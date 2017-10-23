(ns comdb2.atomic-writes
  "Looks for cases where updates to multiple rows may not be visible
  atomically."
  (:require
    [comdb2.core :as c]
    [clojure.tools.logging :refer :all]
    [clojure.core.reducers :as r]
    [clojure.string :as str]
    [clojure.pprint :refer [pprint]]
    [dom-top.core :refer [letr with-retry]]
    [jepsen.checker.timeline :as timeline]
    [jepsen
     [client :as client]
     [checker :as checker]
     [nemesis :as nemesis]
     [independent :as independent]
     [generator :as gen]
     [reconnect :as rc]]
    [knossos.op :as op]
    [clojure.java.jdbc :as j]
    [slingshot.slingshot :refer [throw+ try+]])
  (:import [knossos.model Model]))

(defrecord AtomicWriteClient [table ids conn]
  client/Client
  (setup! [this test node]
    (assoc this :conn (c/connect node)))

  (invoke! [this test op]
    (c/with-conn [c conn]
      (c/with-timeout
        (j/with-db-transaction [c c {:isolation :serializable}]
          (c/hasql! c)

          (case (:f op)
            :read (->> (c/query c [(str "select * from " table)])
                       (map :value)
                       (assoc op :type :ok, :value))

            :write (do (doseq [id (shuffle ids)]
                         (c/upsert! c table {:id id, :value (:value op)}
                                    [(str "id = " id)]))
                       (assoc op :type :ok)))))))

  (teardown! [this test]
    (rc/close! conn)))


(defn a6-gen
  "Generates a mix of sequential writes and reads."
  []
  (let [v (atom -1)]
    (gen/mix [{:type :invoke, :f :read}
              (fn [_ _] {:type :invoke, :f :write, :value (swap! v inc)})])))

(defn atomic-checker
  "Verifies that every read shows n identical values for all copies."
  [n]
  (reify checker/Checker
    (check [this test model history opts]
      (let [mixed-reads (->> history
                             (r/filter (fn [op]
                                         (let [vs (:value op)]
                                           (and (op/ok? op)
                                                (= :read (:f op))
                                                (not= [] vs)
                                                (or (not= n (count vs))
                                                    (apply not= vs))))))
                             (into []))]
        {:valid? (empty? mixed-reads)
         :mixed-reads mixed-reads}))))


(defn workload
  []
  (let [id-count 5]
    {:client (AtomicWriteClient. "atomic_writes" (vec (range id-count)) nil)
     :generator (a6-gen)
     :checker (atomic-checker id-count)}))
