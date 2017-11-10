(ns comdb2.cli
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
                    [nemesis :as nemesis]]
            [comdb2 [core :as c]
                    [a6 :as a6]
                    [atomic-writes :as aw]
                    [bank :as bank]
                    [dirty-reads :as dr]
                    [g2 :as g2]
                    [register :as register]
                    [sets :as sets]]))

(defn workloads
  "The test workloads that we know how to run. Each workload is a map like

      {:generator         a generator of client ops
       :final-generator   a generator to run after the cluster recovers
       :client            a client to execute those ops
       :checker           a checker
       :model             for the checker}

  Note that workloads are *stateful*, since they include generators; that's
  why this is a function, instead of a constant--we may need a fresh workload
  if we run more than one test."
  []
  {:a6              (a6/workload)
   :atomic-writes   (aw/workload)
   :bank            (bank/workload)
   :dirty-reads     (dr/workload)
   :g2              (g2/workload)
   :register        (register/workload)
   :sets            (sets/workload)})

(defn nemeses
  "The nemeses we can apply to the system. Each nemesis is a map of keyword
  nemesis name to a nemesis. TODO: customizable schedules--right now everything
  is start/stop."
  []
  {:none                    nemesis/noop
   :partition-random-halves (nemesis/partition-random-halves)})

(defn comdb2-test
  "Given options from the CLI, constructs a Jepsen test."
  [opts]
  (let [{:keys [generator
                final-generator
                client
                checker
                model]} (get (workloads) (:workload opts))]
    (c/basic-test
      (merge ; Sort of a hack; we'll use the cluster env var instead of the
             ; command line options for nodes and ssh. TODO: change the option
             ; parsing so we use CLUSTER as a fallback instead of overriding
             ; the command line.
             (dissoc opts :nodes :ssh)
             {:name (name (:workload opts))
              :client client
              :generator generator
              :final-generator final-generator
              :checker checker
              :model model
              :nemesis (get (nemeses) (:nemesis opts))}))))

(def opt-spec
  "Additional command line options"
  [[nil "--workload WORKLOAD" "Test workload to run, e.g. a6"
    :parse-fn keyword
    :missing (str "--workload " (cli/one-of (workloads)))
    :validate [(workloads) (cli/one-of (workloads))]]

   [nil "--nemesis NEMESIS" "Nemesis to use; e.g. partition-random-halves"
    :default :none
    :parse-fn keyword]])

(defn -main
  "Command line runner"
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  comdb2-test
                                         :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
