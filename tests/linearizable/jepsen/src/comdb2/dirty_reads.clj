(ns comdb2.dirty-reads
  "Looks for dirty reads in an insert-only workload."
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

; TODO: Kyle, review the dirty reads test for galera and figure out what this
; was supposed to do

(defrecord DirtyReadsClient [conn n]
  client/Client
  (setup! [this test node]
    (let [conn (c/connect node)]
      (c/with-conn [c conn]
        (if false
          ; This causes reliable connection disconnects and I cannot for the
          ; life of me figure out why
          (dotimes [i n]
            (try
              (c/with-txn-retries
                (Thread/sleep (rand-int 10))
                (c/insert! c :dirty {:id i, :x -1}))
              (catch java.sql.SQLIntegrityConstraintViolationException e nil)))

          (try
            (j/with-db-transaction [c c {:isolation :read-committed}]
              (c/hasql! c)
              (dotimes [i n]
                ; Can't use parameters in a transaction, woooo
                (c/execute! c [(str "insert into dirty (id, x) values ("
                                    i ", " -1 ");")])))
            (catch java.sql.SQLIntegrityConstraintViolationException e
              ; Someone else inserted
              nil))))

    (assoc this :conn conn)))

  (invoke! [this test op]
   (c/with-io-failures op
     (c/with-conn [c conn]
       (c/with-logical-failures op
         (try+
           (c/with-timeout
             (j/with-db-transaction [c c {:isolation :read-committed}]
               (case (:f op)
                 ; skip initial records - not all threads are done initial
                 ; writing, and the initial writes aren't a single transaction so
                 ; we won't see consistent reads
                 :read (->> (c/query c ["select * from dirty where x != -1"])
                            (mapv :x)
                            (assoc op :type :ok, :value))

                 :write (let [x (:value op)
                              order (shuffle (range n))]
                          (doseq [i order]
                            (c/update! c :dirty {:x x} ["id = ?" i])
                            (when (< (rand) 1/10)
                              (throw+ {:type :deliberate-abort})))
                          (assoc op :type :ok)))))
           (catch [:type :deliberate-abort] e
             (assoc op :type :fail, :error :deliberate-abort)))))))


  (teardown! [_ test]
    (rc/close! conn)))

; TODO: better name for this, move it near the dirty-reads client
(defn client
  [n]
  (DirtyReadsClient. nil n))

(defn dirty-reads-checker
  "We're looking for a failed transaction whose value became visible to some
  read."
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [failed-writes (->> history
                               (r/filter op/fail?)
                               (r/filter #(= :write (:f %)))
                               (r/map :value)
                               (into (hash-set)))

            reads (->> history
                       (r/filter op/ok?)
                       (r/filter #(= :read (:f %)))
                       (r/map :value))

            inconsistent-reads (->> reads
                                    (r/filter
                                      (fn [r]
                                        ; Because of COMDB2's weird predicate
                                        ; visibility bug, we might only read a
                                        ; subset of the full table :-(
                                        (or (= r [])
                                            (apply not= r))))
                                    (into []))

            filthy-reads (->> reads
                              (r/filter (partial some failed-writes))
                              (into []))]

        {:valid?              (empty? filthy-reads)
         :inconsistent-reads  inconsistent-reads
         :dirty-reads         filthy-reads}))))

(defn reads
  []
  {:type :invoke, :f :read, :value nil})

(defn writes
  []
  (->> (range)
       (map (partial array-map
                     :type :invoke,
                     :f :write,
                     :value))
       gen/seq))

(defn workload
  []
  (let [n 4]
    {:client (client n)
     :generator (gen/mix [(reads) (writes)])
     :checker (dirty-reads-checker)}))
