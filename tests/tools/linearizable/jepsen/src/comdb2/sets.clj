(ns comdb2.sets
  "Commutative set addition, with a final read."
  (:require
    [clojure.tools.logging :refer :all]
    [clojure.core.reducers :as r]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.pprint :refer [pprint]]
    [comdb2.core :as c]
    [dom-top.core :refer [letr with-retry]]
    [jepsen.checker.timeline  :as timeline]
    [jepsen
     [adya :as adya]
     [client :as client]
     [core :as jepsen]
     [checker :as checker]
     [nemesis :as nemesis]
     [util :as util :refer [timeout meh]]
     [generator :as gen]
     [reconnect :as rc]]
    [knossos.op :as op]
    [knossos.model :as model]
    [clojure.java.jdbc :as j]
    [slingshot.slingshot :refer [throw+ try+]]))

(defn set-client
  [conn id]
  (reify client/Client
    (setup! [this test node]
      (set-client (c/connect node) id))

    (invoke! [this test op]
      (c/with-conn [c conn]
        (c/with-txn-prep! c
          (case (:f op)
            :add  (do (c/execute! c [(str "insert into jepsen(id, value) values(" (swap! id inc) ", " (:value op) ")")])
                      (assoc op :type :ok))

            :read (->> (c/query c ["select * from jepsen"])
                       (mapv :value)
                       (into (sorted-set))
                       (assoc op :type :ok, :value))))))

    (teardown! [_ test]
      (rc/close! conn))))

(defn workload
  []
  {:client (set-client nil (atom -1))
   :generator (->> (range)
                   (map (partial array-map
                                 :type :invoke
                                 :f :add
                                 :value))
                   gen/seq
                   (gen/delay 1/10))
   :final-generator (gen/once {:type :invoke, :f :read, :value nil})
   :checker (checker/set)})
