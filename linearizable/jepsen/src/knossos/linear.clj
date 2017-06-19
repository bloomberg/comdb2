(ns knossos.linear
  "An implementation of the linear algorithm from
  http://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf"
  (:require [clojure.math.combinatorics :as combo]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.tools.logging :refer [info warn error]]
            [clojure.pprint :refer [cl-format]]
            [knossos.linear.config :as config]
            [knossos.model.memo :as memo :refer [memo]]
            [knossos [core :as core]
                     [history :as history]
                     [memory :as memory]
                     [model :as model]
                     [util :refer :all]
                     [weak-cache-set :as weak-cache-set]
                     [op :as op]])
  (:import [java.util ArrayList
                      Set]
           [knossos.model.memo Wrapper]
           [org.cliffc.high_scale_lib NonBlockingHashSet]))

;; Transitions between configurations

(defn t-call
  "If calls and rets contain no call for a process, that process can invoke an
  operation, generating a new configuration."
  [config op]
  (assoc config :processes (config/call (:processes config) op)))

(defn t-lin
  "A pending call can be linearized by transforming the current state, moving
  the invocation from calls to rets. Returns nil if linearizing this operation
  would be inconsistent. Updates last-lin-op for the config."
  [config op]
  (let [model' (model/step (:model config) op)]
    (when-not (model/inconsistent? model')
      (config/->Config model'
                       (config/linearize (:processes config) op)
                       op))))

(defn t-ret
  "A pending return can be completed. It's legal to provide an invocation or a
  completion here--all that matters is the :process"
  [config op]
  (assoc config :processes (config/return (:processes config) op)))

;; Exploring the automaton

(defn jit-linearizations-cache
  "Construct a cache for memoizing jit-linearizations.

  jit-linearizations is pure.

  Assume we're processing a single ok operation with a given configset. Let c1
  and c2 be two configs from that configset. If c1 and c2 wind up calling (at
  any depth), jit-linearizations for *the same config*, they'll see *the same*
  set of resulting configs, and perform *the same* final linearization and
  final return on each. We can skip one of them.

  So, we construct a concurrent hashset here and use it to prune duplicate
  searches."
  []
  (weak-cache-set/array 1e5))

(defn jit-linearizations
  "Takes an initial configuration, a collection of pending invocations, and a
  final invocation to linearize and return. Explores all orders of pending
  invocations, and returns a sequence of valid configurations with that final
  invocation linearized."
  ([cache config final]
   (jit-linearizations cache final (ArrayList.) config))
  ([^Set cache final ^ArrayList configs config]
   (when (Thread/interrupted)
     (throw (InterruptedException.)))

   ; Record this point
   (if-not (weak-cache-set/add! cache config)
     ; We can skip this execution; it'll appear in some other config's
     ; jit-linearizations
     (do ;(info :dup config)
         configs)

     ; Trivial case: record this configuration.
     (do (.add configs config)
         (let [configs        configs
               final-process  (int (:process final))]
           ; Take all pending ops from the configuration *except* the final one,
           ; try linearizing that op, and if we could linearize it, explore its
           ; successive linearizations too.
           (->> config
                :processes
                config/calls
                (r/filter (fn excluder [op]
                            (not (= final-process (int (:process op))))))
                (rkeep    (fn linearizer [op] (t-lin config op)))
                (reduce   (fn recurrence [configs op]
                            (jit-linearizations cache final configs op))
                        configs)))))))

(defn step-ok!
  "Takes a jit cache, a ConfigSet, a config and an ok operation. If the
  operation has already been linearized, returns a ConfigSet containing only
  `configuration`, but with the ok op removed from the config's rets.

  If the operation hasn't been linearized yet, takes all pending calls from
  other threads and linearizes them in each possible order, creating a set of
  new configurations. Finally, attempts to linearize and return the given
  operation on each of those configs.

  Returns the ConfigSet."
  [cache config-set config ok]
  (let [p (:process ok)]
    (cond
      ; Have we already linearized the ok op in this config?
      (config/returning? (:processes config) p)
      (config/add! config-set (t-ret config ok))

      ; Is this operation pending?
      (config/calling? (:processes config) p)
      (let [jit-configs (jit-linearizations cache config ok)]
        ; Apply ok op to each one and return it.
        (->> jit-configs
             (rkeep (fn final-linearization [config]
                       (when-let [linearized (t-lin config ok)]
                         (t-ret linearized ok))))
             (reduce config/add! config-set))))

    config-set))

(defn previous-ok
  "Given a history and an operation, looks backwards in the history to find
  the previous ok. Returns nil if there was none."
  [history op]
  (assert (op/ok? op))
  (loop [i (dec (:index op))]
    (when-not (neg? i)
      (when-let [op (nth history i)]
        (if (op/ok? op)
          op
          (recur (dec i)))))))

(defn extend-path
  "Given a path and some operations, applies those operations to extend the
  path until hitting an inconsistent point."
  [prefix ops]
  (reduce (fn [path op]
            (let [model (:model (peek path))
                  model' (model/step model op)
                  path'  (conj path {:op op :model model'})]
              (if (model/inconsistent? model')
                (reduced path')
                path')))
          prefix
          ops))

(defn final-paths-for-config
  "Returns a set of final paths for a specific configuration."
  [prefix final config]
  (let [; First, identify the set of pending operations *other* than our
        ; final linearization.
        final-process (:process final)]
    ; Take all pending calls for this configuration
    (->> config
         :processes
         config/calls
         ; Except the final call
         (r/filter (fn excluder [op] (not (= final-process (:process op)))))
         ; Now compute all permutations of those pending ops
         (into [])
         combo/subsets
         (r/mapcat combo/permutations)
         ; followed by the final op
         (r/map #(concat % (list final)))
         ; And extend the prefix along those paths
         (r/map (partial extend-path prefix))
         ; Computing a set
         (foldset))))

(defn final-paths
  "When a search crashes, we have an operation which would not linearize even
  given all pending operations. We also have a series of configurations, each
  of which is comprised of a model and a process state, resulting from the
  completion of the last ok operation.

  We can expand such a configuration into a set of paths, each ending in an
  inconsistent state transition. Normally, when we compute JIT linearizations,
  we drop inconsistent outcomes. Here we want to preserve them--and the paths
  that lead to them. Our aim is to show exhaustively *why* no configuration
  linearized.

  Returns a set of paths, where a path is a sequence of transitions like:

      {:op    some-op
       :model model-resulting-from-applying-op}"
  [history op configs]
  (->> configs
       (map (fn [config]
              (final-paths-for-config [{:op    (:last-op config)
                                        :model (:model config)}]
                                      op
                                      config)))
       (reduce set/union)
       ; And now unwrap memoization
       (map (fn [path]
              (mapv (fn [transition]
                      (let [m (:model transition)]
                        (if (instance? Wrapper m)
                          (assoc transition :model (memo/model m))
                          transition)))
                    path)))
       set))

(def ^:const parallel-threshold
  "How many configs do we need before we start parallelizing?"
  128)

(defn step
  "Advance one step through the history. Takes a configset, returns a new
  configset--or a reduced failure."
  [history state cache configs op]
  (swap! state assoc :configs configs, :op op)
  (cond
    ; If we're invoking an operation, just add it to each config's pending
    ; ops.
    (and (op/invoke? op) (not (:fails? op)))
    (->> configs
         (map #(t-call % op))
         (reduce config/add! (config/set-config-set)))

    ; If we're handling a completion, run each configuration through a
    ; just-in-time linearization, accumulating a new set of configs.
    (op/ok? op)
    (let [cache    (weak-cache-set/clear! cache)
          configs' (if (< (count configs) parallel-threshold)
                     ; Singlethreaded search
                     (let [configs' (config/set-config-set)]
                       (doseq [c configs]
                         (step-ok! cache configs' c op))
                       configs')

                     ; Parallel search
                     (->> configs
                          (pmap (fn par-expand [config]
                                  (step-ok! cache (config/set-config-set)
                                            config op)))
                          (apply concat)
                          ; Merge parallel results
                          (reduce config/add! (config/set-config-set))))]

      (if (empty? configs')
        ; Out of options! Return a reduced debugging state.
        (reduced {:valid?       false
                  :configs      (map config/config->map configs)
                  :final-paths  (final-paths history op configs)
                  :previous-ok  (previous-ok history op)
                  :last-op  (reduce (fn [op config]
                                      (if (or (nil? op)
                                              (< (:index op)
                                                 (:index (:last-op config))))
                                        (:last-op config)
                                        op))
                                    nil
                                    configs)
                  :op           op})
        ; Otherwise, return new config set.
        configs'))

    ; Skip other types of ops
    true
    configs))

(def reporting-interval
  "How long (in ms) to sleep between status updates"
  5000)

(defn reporter!
  "Spawns a reporting thread that periodically logs the state of the analysis.
  State is an atom with :running?, :configs, and :op."
  [state]
  (future
    (with-thread-name "reporter"
      (loop [last-state nil]
        (when (:running? @state)
          (Thread/sleep reporting-interval)
          (let [state @state]
            (when (:running? state)
              (if (not= state last-state)
                (do (info :space (count (:configs state))
                          :cost (->> state
                                     :configs
                                     (map config/estimated-cost)
                                     (reduce +)
                                     (cl-format nil "~,2e"))
                          :op    (:op state))
                    (recur state))
                (recur last-state)))))))))

(defn analysis
  "Given an initial model state and a history, checks to see if the history is
  linearizable. Returns a map with a :valid? bool and a :config configset from
  its final state, plus an offending :op if an operation forced the analyzer to
  discover there are no linearizable paths."
  [model history]
  (let [history (-> history
                    history/complete
                    history/index)
        memo (memo model history)
        history (:history memo)
        configs (-> (:model memo)
                    (config/config history)
                    list
                    config/set-config-set)
        state (atom {:running?  true
                     :configs   configs
                     :op        nil})
        thread (Thread/currentThread)
        mem-watch (memory/on-low-mem!
                    (fn abort []
                      (let [s @state]
                        (warn "Out of memory; aborting search at op"
                              (:index (:op s)) "/" (count history)))
                      (swap! state assoc
                             :running? false
                             :cause    :out-of-memory)
                      (.interrupt thread)))
        cache     (weak-cache-set/nbhs)
        reporter  (reporter! state)]
    ; Perform search
    (try (let [res (reduce (partial step history state cache) configs history)]
           (if (and (map? res) (= false (:valid? res)))
             ; Reduced error
             res
             {:valid?  true
              :configs (map config/config->map res)}))
         (catch InterruptedException e
           (let [{:keys [cause configs op]} @state]
             {:valid?       :unknown
              :cause        cause
              :configs      (map config/config->map configs)
              :previous-ok  (previous-ok history op)
              :last-op      (reduce (fn [op config]
                                      (if (or (nil? op)
                                              (< (:index op)
                                                 (:index (:last-op
                                                           config))))
                                        (:last-op config))
                                      op)
                                    nil
                                    configs)
              :op           op}))
         (finally
           ; Reset memory watchdog and status reporter
           (mem-watch)
           (reset! state {:running? false})))))
