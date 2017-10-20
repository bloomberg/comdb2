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

(defrecord DirtyReadsClient [node n]
  client/Client
  (setup! [this test node]
   (warn "setup")
   (j/with-db-connection [c (c/conn-spec node)]
    ; Create table
    (dotimes [i n]
     (try ; TODO: no catch?
      (c/with-txn-retries
       (Thread/sleep (rand-int 10))
       (c/insert! c :dirty {:id i, :x -1})))))

    (assoc this :node node))

  (invoke! [this test op]
   (try
    (j/with-db-transaction [c (c/conn-spec node) {:isolation :serializable}]
     (try ; TODO: no catch?
      (case (:f op)
       ; skip initial records - not all threads are done initial writing, and
       ; the initial writes aren't a single transaction so we won't see
       ; consistent reads
       :read (->> (c/query c ["select * from dirty where x != -1"])
                  (mapv :x)
                  (assoc op :type :ok, :value))

       :write (let [x (:value op)
                    order (shuffle (range n))]
                ; TODO: why is there a discarded read here?
                (doseq [i order]
                  (c/query c ["select * from dirty where id = ?" i]))
                (doseq [i order]
                  (c/update! c :dirty {:x x} ["id = ?" i]))
                (assoc op :type :ok)))))
    (catch java.sql.SQLException e
     ; TODO: why do we know this is a definite failure?
     (assoc op :type :fail :reason (.getMessage e)))))

  (teardown! [_ test]))

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
      (let [failed-writes
            ; Add a dummy failed write so we have something to compare to in the
            ; unlikely case that there's no other write failures
            (merge {:type     :fail
                    :f        :write
                    :value    -12
                    :process  0
                    :time     0}
                   (->> history
                        (r/filter op/fail?)
                        (r/filter #(= :write (:f %)))
                        (r/map :value)
                        (into (hash-set))))

            reads (->> history
                       (r/filter op/ok?)
                       (r/filter #(= :read (:f %)))
                       (r/map :value))

            inconsistent-reads (->> reads
                                    (r/filter seq)
                                    (r/filter (partial apply not=))
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

(defn dirty-reads-test-nemesis
  ([n]
   (dirty-reads-test-nemesis n {}))
  ([n opts]
   (c/basic-test
     (merge
       {:name "dirty reads"
        :concurrency 1
        :client (client n)
        :generator (gen/mix [(reads) (writes)])
        :checker (dirty-reads-checker)
        :time-limit 120}
     opts))))
