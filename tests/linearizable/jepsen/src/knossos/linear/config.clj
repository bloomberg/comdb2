(ns knossos.linear.config
  "Datatypes for search configurations"
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [potemkin :refer [definterface+ deftype+ defrecord+]]
            [knossos [core :as core]
                     [util :refer :all]
                     [op :as op]]
            [knossos.model.memo :as memo])
    (:import knossos.model.Model
             knossos.model.memo.Wrapper
             java.util.Arrays
             java.util.Set
             java.util.HashSet
             java.lang.reflect.Array))

;; An immutable map of process ids to whether they are calling or returning an
;; op, augmented with a mutable union-find memoized equality test.

(definterface+ Processes
  (calls      "A reducible of called but unlinearized operations."      [ps])
  (^long calls-count "How many calls are outstanding?"                   [ps])

  (call       "Adds an operation being called with the calling state."  [ps op])
  (linearize  "Changes the given operation from calling to returning."  [ps op])
  (return     "Removes an operation from the returned set."             [ps op])

  (idle?      "Is this process doing nothing?"                          [ps p])
  (calling?   "Is this process currently calling an operation?"         [ps p])
  (returning? "Is this process returning an operation?"                 [ps p]))

; Maybe later
; (def ^:const idle      0)
; (def ^:const calling   1)
; (def ^:const returning 2)

;; A silly implementation based on two Clojure maps.
(defrecord MapProcesses [calls rets]
  Processes
  (calls [ps]
    (vals calls))

  (call [ps op]
    (let [p (:process op)]
      (assert (idle? ps p))
      (MapProcesses. (assoc calls p op) rets)))

  (linearize [ps op]
    (let [p (:process op)]
      (assert (calling? ps p))
      (MapProcesses. (dissoc calls p) (assoc rets p op))))

  (return [ps op]
    (let [p (:process op)]
      (assert (returning? ps p))
      (MapProcesses. calls (dissoc rets p))))

  (idle?      [ps p] (not (or (contains? calls p)
                              (contains? rets  p))))
  (calling?   [ps p] (contains? calls p))
  (returning? [ps p] (contains? rets p)))

(defn map-processes
  "A Processes tracker based on Clojure maps."
  []
  (MapProcesses. {} {}))

(deftype MemoizedMapProcesses [callsMap
                               retsMap
                               ^:volatile-mutable ^int hasheq
                               ^:volatile-mutable ^int hashcode]
  Processes
  (calls [ps]
    (vals callsMap))

  (call [ps op]
    (let [p (:process op)]
      (assert (idle? ps p))
      (MemoizedMapProcesses. (assoc callsMap p op) retsMap -1 -1)))

  (linearize [ps op]
    (let [p (:process op)]
      (assert (calling? ps p))
      (MemoizedMapProcesses. (dissoc callsMap p) (assoc retsMap p op) -1 -1)))

  (return [ps op]
    (let [p (:process op)]
      (assert (returning? ps p))
      (MemoizedMapProcesses. callsMap (dissoc retsMap p) -1 -1)))

  (idle?      [ps p] (not (or (contains? callsMap p)
                              (contains? retsMap  p))))
  (calling?   [ps p] (contains? callsMap p))
  (returning? [ps p] (contains? retsMap  p))

  ; I'm assuming calls and rets will never be identical since we move ops
  ; atomically from one to the other, so we shouuuldn't see collisions here?
  ; Maye xor isn't good enough though. Might back off to murmur3.
  clojure.lang.IHashEq
  (hasheq [ps]
    (when (= -1 hasheq)
      (set! hasheq (int (bit-xor (hash callsMap) (hash retsMap)))))
    hasheq)

  Object
  (hashCode [ps]
    (when (= -1 hashcode)
      (set! hashcode (int (bit-xor (.hashCode callsMap) (.hashCode retsMap)))))
    hashcode)

  (equals [this other]
    (or (identical? this other)
        (and (instance? MemoizedMapProcesses other)
             (= (.hashCode this) (.hashCode other))
             (.equals callsMap (.callsMap ^MemoizedMapProcesses other))
             (.equals retsMap  (.retsMap  ^MemoizedMapProcesses other))))))

(defn memoized-map-processes
  "A Processes tracker based on Clojure maps, with memoized hashcode for faster
  hashing and equality."
  []
  (MemoizedMapProcesses. {} {} -1 -1))

; A thousand processes with 32-bit indexes pointing to the operations they
; represent can fit in an array of 4 KB (plus 12 bytes overhead), and be
; compared for equality with no memory indirection. Almost all of those
; processes are likely idle, however--wasted storage.

; A Clojure map with ~8 entries will use an ArrayMap--that's 16*64 bits + 12
; bytes overhead for the array, 8 bytes for the ArrayMap overhead, and each
; Long referred to is 12 bytes overhead + 8 bytes value, for 320 bytes total.
; That's a combined 852 bytes.

; The array representation is ~5x larger, but doesn't require ~10 pointer
; derefs, which might make it easier on caches and pipelines.

; Could we compress the array representation? We could, for instance, take an
; integer array and stripe alternating pairs of process ID and op ID across it,
; sorted by process ID, then use binary search or linear scan to extract the op
; for a given key. That'd pack 8 entries into 12 + ((4 + 4) * 8) = 76 bytes,
; and no pointer indirection for comparison.

; Which of these structures is more expensive to manipulate? With arraymaps
; we're essentially rewriting the whole array structure *anyway*, and with more
; than 8 elements we promote to clojure.lang.APersistentMap, but we're still
; talking about rewriting 256-byte INode arrays at one or two levels of the
; HAMT, plus some minor pointer chasing. Oh AND we have to hash!

; Looking up pending operations *is* more expensive in this approach. We have
; to map the operation index to an actual operation, which is either an array
; lookup (cheap, v. cache-friendly), or a vector lookup (~10x slower but not
; bad). OTOH, iteration over pending ops is reasonably cheap; just read every
; other element from the array and produce a reducible of O(n) or O(log32 n)
; fetches from the history. The history never changes so it should be more
; cache-friendly than the Processes trackers, which mutate *constantly*.

; We'll encode pending ops as positive integers, and returning ops as their
; negative complements, so 0 -> -1, 1 -> -2, 2 -> -3, ...

(defn array-processes-search
  "Given a process ID and an int array like [process-id op-id process-id op-id
  ....], finds the array index of this process ID. (inc idx) will give the
  index of the corresponding process ID. If the process is *not* present,
  returns (dec (- insertion-point)), where insertion point is where the process
  index *would* be, after insertion into the array."
  ([^ints a ^long process]
   (loop [low  0
          high (dec (unchecked-divide-int (alength a) 2))]
     (if (> low high)
       (dec (unchecked-multiply -2 low)) ; Because indices are bounded by
                                         ; MAX_INT / 2, this is safe.
       (let [mid     (quot (unchecked-add-int low high) 2)
             mid-val (aget a (unchecked-multiply 2 mid))]
         (cond (< mid-val process)  (recur (inc mid)  high)
               (< process mid-val)  (recur low        (dec mid))
               true                 (unchecked-multiply 2 mid)))))))

(defn array-processes-assoc
  "Given an array like [process-id op-id process-id op-id ...], an index where
  the process/op pair belong, a process id, and an op id, upserts the process
  id and op id into a copy of the array, maintaining sorted order."
  [^ints a ^long i ^long process ^long op]
  (if (neg? i)
    ; It's not in the array currently; insert it
    (let [i  (int (- (inc i)))
          a' (int-array (unchecked-add 2 (alength a)))]
      ; Copy prefix, insert, copy postfix
      (System/arraycopy a 0 a' 0 i)
      ; Believe it or not we actually burned a ton of time in the aset-int
      ; coercion wrapper
      (aset a' i        process)
      (aset a' (inc i)  op)
      (System/arraycopy a i a' (unchecked-add i 2) (- (alength a) i))
      a')

    ; It's already in the array; just copy and overwrite those indices.
    (let [a' (int-array (alength a))]
      (System/arraycopy a 0 a' 0 (alength a))
      (aset a i       process)
      (aset a' (inc i) op)
      a')))

(defn array-processes-dissoc
  "Like array-processes-assoc, but deletes the elements at i and i+1, returning
  a copy of the array without them."
  [^ints a ^long i]
  (let [a' (int-array (- (alength a) 2))]
    (System/arraycopy a 0       a' 0 i)
    (System/arraycopy a (+ i 2) a' i (- (alength a') i))
    a'))

(deftype ArrayProcesses [^objects history ^ints a]
  Processes
  (calls [ps]
    ; Wish I could find an efficient way to get a reducible out of, say,
    ; (range), range reducers go through chunked seqs instead which is
    ; evidently really pricey!
    (reify clojure.lang.IReduceInit
      (reduce [this f init]
        (let [n (alength a)]
          (loop [acc init
                 i   1]
            (if (<= n i)
              ; done
              acc
              (let [op-index (aget a i)
                    acc' (if (neg? op-index)
                           ; This op has been returned; skip
                           acc
                           ; Fetch op and apply f
                           (f acc (aget history op-index)))]
                (recur acc' (+ i 2)))))))))

  (calls-count [ps]
    (unchecked-divide-int (alength a) 2))

  (call [ps op]
    (let [p  (:process op)
          op (:index op)
          i  (array-processes-search a p)]
      ; The process should not be present yet.
      (assert (neg? i))
      (assert (integer? op))
      (ArrayProcesses. history (array-processes-assoc a i p op))))

  (linearize [ps op]
    (let [p  (:process op)
          op (:index op)
          i  (array-processes-search a p)]
      ; The process should be present and being called.
      (assert (not (neg? i)))
      (assert (not (neg? (aget a (inc i)))))
      (ArrayProcesses. history (array-processes-assoc a i p (dec (- op))))))

  (return [ps op]
    (let [p  (:process op)
          op (:index op)
          i  (array-processes-search a p)]
      ; The process should be present and returning.
      (assert (not (neg? i)))
      (assert (neg? (aget a (inc i))))
      (ArrayProcesses. history (array-processes-dissoc a i))))

  (idle? [ps p]
    (neg? (array-processes-search a p)))

  (calling? [ps p]
    (let [i (array-processes-search a p)]
      (and (not (neg? i))
           (not (neg? (aget a (inc i)))))))

  (returning? [ps p]
    (let [i (array-processes-search a p)]
      (and (not (neg? i))
           (neg? (aget a (inc i))))))

  Object
  (toString [this]
    (str (seq a)))

  (hashCode [ps]
    (Arrays/hashCode a))

  (equals [ps other]
    (and (instance? ArrayProcesses other)
         (Arrays/equals a ^ints (.a ^ArrayProcesses other)))))

(defn array-processes
  "A process tracker backed by a sorted array, closing over the given history.
  History operations must have :index elements identifying their position in
  the history, and integer :process fields."
  [history]
  (assert (every? integer? (map :process (remove op/info? history))))
  (assert (every? integer? (map :index history)))
  (ArrayProcesses. (object-array history) (int-array 0)))

; One particular path through the history, comprised of a model and a tracker
; for process states. Last-lin-op is the last op linearized in this config, and
; is used purely for constructing debugging traces. For performance reasons, it
; does not factor into equality, hashing, or kw lookup/assoc.
(deftype+ Config [model processes last-op]
  clojure.lang.IKeywordLookup
  ; Why can't we just use defrecord? Because defrecord computes hashcodes via
  ; APersistentMap/mapHasheq which pretty darn expensive when we just want to
  ; hash two fields--and there's no way to override hashcode without breaking
  ; defrecord.
  ;
  ; Adapted from https://github.com/clojure/clojure/blob/master/src/clj/clojure/core_deftype.clj
  (getLookupThunk [this k]
    (let [gclass (class this)]
      (condp identical? k
        :model (reify clojure.lang.ILookupThunk
                 (get [thunk gtarget]
                   (if (identical? (class gtarget) gclass)
                     (.-model ^Config gtarget))))
        :processes (reify clojure.lang.ILookupThunk
                     (get [thunk gtarget]
                       (if (identical? (class gtarget) gclass)
                         (.-processes ^Config gtarget))))
        :last-op (reify clojure.lang.ILookupThunk
                       (get [thunk gtarget]
                         (if (identical? (class gtarget) gclass)
                           (.-last-op ^Config gtarget)))))))

  ; Override assoc for performance. I am a terrible human being.
  clojure.lang.IPersistentMap
  (assoc [this k v]
    (condp identical? k
      :model      (Config. v processes last-op)
      :processes  (Config. model v last-op)))

  clojure.lang.IHashEq
  (hasheq [this] (bit-xor (hash model) (hash processes)))

  clojure.lang.IPersistentCollection
  (equiv [this other]
    (boolean
      (or (identical? this other)
          (and (instance? Config other)
               (= model     (.-model     ^Config other))
               (= processes (.-processes ^Config other))))))

  Object
  (hashCode [this]
    (bit-xor (.hashCode model) (.hashCode processes)))

  (equals [this other]
    (or (identical? this other)
        (and (instance? Config other)
             (.equals model     (.-model     ^Config other))
             (.equals processes (.-processes ^Config other)))))

  (toString [this]
    (str "(Config :model " model ", :processes " processes ")")))

(defmethod print-method Config [x ^java.io.Writer w]
  (.write w (str x)))

(defn config
  "An initial configuration around a given model and history."
  [model history]
  (Config. model (array-processes history) nil))

(defn config->map
  "Turns a config into a nice map showing the state of the world at that point."
  [^Config config]
  {:model   (let [m (:model config)]
              (if (instance? Wrapper m)
                (memo/model m)
                m))
   :last-op (:last-op config)
   :pending (into [] (calls (:processes config)))})

(defn gammaish
  "Nemes approximation to the gamma function"
  [n]
  (let [x1 (Math/sqrt (* 2 (/ Math/PI n)))
        x2 (+ n (/ (- (* 12 n)
                      (/ (* 10 n)))))
        x3 (Math/pow (/ x2 Math/E) n)]
    (* x1 x3)))

(defn estimated-cost
  "Roughly how expensive do we think a config is going to be to JIT-linearize?
  We'll estimate based on the cardinality of the pending set."
  [config]
  ; There are n 1-element histories, n * n-1 2-element histories, n * n-1 *
  ; n-3 3-element histories, and so on, so the sum of all histories is n *
  ; n!, which we can estimate using Stirling's approximation.
  (let [n (-> config :processes calls-count)]
    (if (zero? n)
      1
      (inc (* n (gammaish (inc n)))))))

;; Non-threadsafe mutable configuration sets

(definterface+ ConfigSet
  (add! "Add a configuration to a config-set. Returns self.
        You do not need to preserve the return value."
        [config-set config]))

(deftype SetConfigSet [^:unsynchronized-mutable ^Set s]
  ConfigSet
  (add! [this config]
    (.add s config)
    this)

  clojure.lang.Counted
  (count [this] (.size s))

  clojure.lang.Seqable
  (seq [this] (seq s))

  Object
  (toString [this]
    (str "#{" (->> this
                   seq
                   (str/join #", "))
         "}")))

(defmethod print-method SetConfigSet [x ^java.io.Writer w]
  (.write w (str x)))

(defn set-config-set
  "An empty set-backed config set, or one backed by a collection."
  ([] (SetConfigSet. (HashSet.)))
  ([coll] (reduce add! (set-config-set) coll)))
