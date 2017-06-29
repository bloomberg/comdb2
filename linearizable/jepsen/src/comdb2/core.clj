(ns comdb2.core
 "Tests for Comdb2"
(:require 
  [clojure.tools.logging :refer :all]
  [clojure.core.reducers :as r]
  [clojure.java.io :as io]
  [clojure.string :as str]
  [clojure.pprint :refer [pprint]]
  [jepsen.checker.timeline  :as timeline]
  [jepsen [client :as client]
    [core :as jepsen]
    [db :as db]
    [tests :as tests]
    [control :as c :refer [|]]
    [checker :as checker]
    [nemesis :as nemesis]
    [independent :as independent]
    [util :refer [timeout meh]]
    [generator :as gen]]
  [knossos.op :as op]
  [knossos.model :as model]
  [clojure.java.jdbc :as j]))

(java.sql.DriverManager/registerDriver (com.bloomberg.comdb2.jdbc.Driver.))

(defn db
 [name]
 (reify db/DB
  (setup! [name test node])
  (teardown! [name test node])))

(def conn-spec
 {:classname "com.bloomberg.comdb2.jdbc.Driver"
 :subprotocol "comdb2"
 :subname (.concat (System/getenv "COMDB2_DBNAME") ":dev")})

(defn retriable-transaction-error [e]
 (or (re-find #"not serializable" e) 
     (re-find #"unable to update record rc = 4" e)
     (re-find #"selectv constraints" e)
     (re-find #"Maximum number of retries done." e)))

(defmacro capture-txn-abort
  "Converts aborted transactions to an ::abort keyword"
  [& body]
  `(try ~@body
        (catch java.sql.SQLException e#
         (println "error is " (.getMessage e#))
         (if (retriable-transaction-error (.getMessage e#))
          ::abort
          (throw e#)))))

(defmacro with-txn-retries
  "Retries body on rollbacks."
  [& body]
  `(loop []
     (let [res# (capture-txn-abort ~@body)]
       (if (= ::abort res#)
        (do
         (info "RETRY")
         (recur))
         res#))))

(defmacro with-txn
  "Executes body in a transaction, with a timeout, automatically retrying
  conflicts and handling common errors."
  [op c node & body]
  `(with-txn-retries
      ~@body))

(defrecord BankClient [node n starting-balance]
  client/Client

  (setup! [this test node]
    ; (println "n " (:n this) "test " test "node " node)
    (j/with-db-connection [c conn-spec]
      ; Create initial accts
      (dotimes [i n]
        (try
         (j/insert! c :accounts {:id i, :balance starting-balance})
         (catch java.sql.SQLException e 
          (if (.contains (.getMessage e) "add key constraint duplicate key")
            nil
            (throw e))))))

    (assoc this :node node))

  (invoke! [this test op]
    (with-txn op conn-spec nil
     (j/with-db-transaction [connection conn-spec :isolation :serializable]
      (j/query connection ["set hasql on"])
      (j/query connection ["set max_retries 100000"])

      (try
        (case (:f op)
          :read (->> (j/query connection ["select * from accounts"])
                     (mapv :balance)
                     (assoc op :type :ok, :value))

          :transfer
          (let [{:keys [from to amount]} (:value op)
                b1 (-> connection
                       (j/query ["select * from accounts where id = ?" from]
                         :row-fn :balance)
                       first
                       (- amount))
                b2 (-> connection
                       (j/query ["select * from accounts where id = ?" to]
                         :row-fn :balance)
                       first
                       (+ amount))]
            (cond (neg? b1)
                  (assoc op :type :fail, :value [:negative from b1])

                  (neg? b2)
                  (assoc op :type :fail, :value [:negative to b2])

                  true
                    (do (j/execute! connection ["update accounts set balance = balance - ? where id = ?" amount from])
                        (j/execute! connection ["update accounts set balance = balance + ? where id = ?" amount to])
                        (assoc op :type :ok)))))))))

  (teardown! [_ test]))

(defn bank-client
  "Simulates bank account transfers between n accounts, each starting with
  starting-balance."
  [n starting-balance]
  (BankClient. nil n starting-balance))

(defn bank-read
  "Reads the current state of all accounts without any synchronization."
  [_ _]
  {:type :invoke, :f :read})

(defn bank-transfer
  "Transfers a random amount between two randomly selected accounts."
  [test process]
  (let [n (-> test :client :n)]
    {:type  :invoke
     :f     :transfer
     :value {:from   (rand-int n)
             :to     (rand-int n)
             :amount (rand-int 5)}}))

(def bank-diff-transfer
  "Like transfer, but only transfers between *different* accounts."
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              bank-transfer))

(defn bank-checker
  "Balances must all be non-negative and sum to the model's total."
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [bad-reads (->> history
                           (r/filter op/ok?)
                           (r/filter #(= :read (:f %)))
                           (r/map (fn [op]
                                  (let [balances (:value op)]
                                    (cond (not= (:n model) (count balances))
                                          {:type :wrong-n
                                           :expected (:n model)
                                           :found    (count balances)
                                           :op       op}

                                         (not= (:total model)
                                               (reduce + balances))
                                         {:type :wrong-total
                                          :expected (:total model)
                                          :found    (reduce + balances)
                                          :op       op}))))
                           (r/filter identity)
                           (into []))]
        {:valid? (empty? bad-reads)
         :bad-reads bad-reads}))))

(defn with-nemesis
  "Wraps a client generator in a nemesis that induces failures and eventually
  stops."
  [client]
  (gen/phases
    (gen/phases
      (->> client
           (gen/nemesis
             (gen/seq (cycle [(gen/sleep 0)
                              {:type :info, :f :start}
                              (gen/sleep 10)
                              {:type :info, :f :stop}])))
           (gen/time-limit 30))
      (gen/nemesis (gen/once {:type :info, :f :stop}))
      (gen/sleep 5))))

(defn basic-test
  [opts]
  (merge tests/noop-test
         {:name "comdb2-bank"
          :db (db (System/getenv "COMDB2_DBNAME"))
;          :db (db "marktdb")
          :nemesis (nemesis/partition-random-halves)
          :nodes ["m1" "m2" "m3" "m4" "m5"]
          :ssh {
            :username "root"
            :password "shadow"
            :strict-host-key-checking false
          }}
          (dissoc opts :name :version)))

;(defn connect-to [conn-spec node]
; (merge conn-spec {:subname (str "marktdb:" node)}))

(defn connect-to [conn-spec node]
 conn-spec)

(def nkey (agent 0))

(defn next-key []
 (let [n @nkey]
  (send nkey inc)
  n))

(defn set-client
  [node]
  (reify client/Client
    (setup! [this test node]
     (set-client node))

    (invoke! [this test op]
     (println op)
      (j/with-db-transaction [connection conn-spec :isolation :serializable]

        (j/query connection ["set hasql on"])
        (j/query connection ["set transaction serializable"])
        (when (System/getenv "COMDB2_DEBUG") (j/query connection ["set debug on"]))
;        (j/query connection ["set debug on"])
        (j/query connection ["set max_retries 100000"])
        (with-txn op conn-spec node

        (try
          (case (:f op)
            :add  (do (j/execute! connection [(str "insert into jepsen(id, value) values(" (next-key) ", " (:value op) ")")])
                      (assoc op :type :ok))

            :read (->> (j/query connection ["select * from jepsen"])
                       (mapv :value)
                       (into (sorted-set))
                       (assoc op :type :ok, :value)))))))

    (teardown! [_ test])))

(defn sets-test
 []
 (basic-test
  {:name "set"
  :client (set-client nil)
  :generator (gen/phases
          (->> (range)
           (map (partial array-map
                 :type :invoke
                 :f :add
                 :value))
           gen/seq
           (gen/delay 1/10)
           with-nemesis)
          (->> {:type :invoke, :f :read, :value nil}
           gen/once
           gen/clients))
  :checker (checker/compose
          {:perf (checker/perf)
          :set checker/set})}))


(defn bank-test-nemesis
  [n initial-balance]
  (basic-test
    {:name "bank"
     :concurrency 10
     :model  {:n n :total (* n initial-balance)}
     :client (bank-client n initial-balance)
     :generator (gen/phases
                  (->> (gen/mix [bank-read bank-diff-transfer])
                       (gen/clients)
                       (gen/stagger 1/10)
                       (gen/time-limit 100)
                       with-nemesis)
                  (gen/log "waiting for quiescence")
                  (gen/sleep 10)
                  (gen/clients (gen/once bank-read)))
     :nemesis (nemesis/partition-random-halves)
     :checker (checker/compose
                {:perf (checker/perf)
                :linearizable (independent/checker checker/linearizable)
                 :bank (bank-checker)})}))


(defn bank-test
  [n initial-balance]
  (basic-test
    {:name "bank"
     :concurrency 10
     :model  {:n n :total (* n initial-balance)}
     :client (bank-client n initial-balance)
     :generator (gen/phases
                  (->> (gen/mix [bank-read bank-diff-transfer])
                       (gen/clients)
                       (gen/stagger 1/10)
                       (gen/time-limit 100))
                  (gen/log "waiting for quiescence")
                  (gen/sleep 10)
                  (gen/clients (gen/once bank-read)))
     :nemesis nemesis/noop
     :checker (checker/compose
                {:perf (checker/perf)
                :linearizable (independent/checker checker/linearizable)
                 :bank (bank-checker)})}))

; This is the dirty reads test for Galera

(defrecord DirtyReadsClient [node n]
  client/Client
  (setup! [this test node]
   (warn "setup")
   (j/with-db-connection [c (connect-to conn-spec node)]
    ; Create table
    (dotimes [i n]
     (try
      (with-txn-retries
       (Thread/sleep (rand-int 10))
       (j/insert! c :dirty {:id i, :x -1})))))

    (assoc this :node node))

  (invoke! [this test op]
   (try
    (j/with-db-transaction [c (connect-to conn-spec node) :isolation :serializable]
     (try
      (case (:f op)
       ; skip initial records - not all threads are done initial writing, and the initial writes
       ; aren't a single transaction so we won't see consistent reads
       :read (->> (j/query c ["select * from dirty where x != -1"])
           (mapv :x)
           (assoc op :type :ok, :value))

       :write (let [x (:value op)
           order (shuffle (range n))]
           (doseq [i order]
            (j/query c ["select * from dirty where id = ?" i]))
           (doseq [i order]
            (j/update! c :dirty {:x x} ["id = ?" i]))
           (assoc op :type :ok)))))
    (catch java.sql.SQLException e
     (assoc op :type :fail :reason (.getMessage e)))))

  (teardown! [_ test]))


(defn comdb2-cas-register-client
  "Comdb2 register client"
  [node]
  (reify client/Client
    (setup! [this test node] 
      (j/with-db-connection [c conn-spec]
        (do
          (j/delete! c :register ["1 = 1"])
          (comdb2-cas-register-client node))))

    (invoke! [this test op]

      (j/with-db-connection [c conn-spec]
        (do
          (j/query c ["set hasql on"])
          (j/query c ["set transaction serializable"])
          (when (System/getenv "COMDB2_DEBUG") (j/query c ["set debug on"]))
          (j/query c ["set max_retries 100000"])
          (case (:f op)
            :read 
                    (let [id   (first (:value op))
                          val' (second (:value op))
                          [val uid] (second (j/query c ["select val,uid from register where id = 1"] :as-arrays? true)) ]
                      (info "Worker " (:process op) " READS val " val " uid " uid " PRE-COMMIT")
                      (assoc op :type :ok, :value (independent/tuple 1 val))
                    )

            :write
                 (try
                   (let [updated (first (j/with-db-transaction [c c]
                     (let [id   (first (:value op))
                           val' (second (:value op))
                           [val uid] (second (j/query c ["select val,uid from register where id = 1"] :as-arrays? true))
                           uid' (+ (* 1000 (rand-int 100000)) (:process op))]

                         (do
                                  (if (nil? val)
                                    (do
                                    (info "Worker " (:process op) " INSERTS val " val' " uid " uid' " PRE-COMMIT")
                                    (j/execute! c [(str "insert into register (id, val, uid) values (" id "," val' "," uid' ")")])
                                    )
                                    (do
                                    (info "Worker " (:process op) " WRITES val from " val "-" uid " to " val' "-" uid' " PRE-COMMIT")
                                    (j/execute! c [(str "update register set val=" val' ",uid=" uid' " where 1")])
                                    )
                                  )
                           )
                       )
                      )
                    ) ; first
                   ]
                     (if (zero? updated)
                      (do
                        (info "Worker " (:process op) " WRITE FAILED")
                        (assoc op :type :fail)
                      )
                      (do
                        (info "Worker " (:process op) " WRITE SUCCESS - RETURNING " (second (:value op)))
                        (assoc op :type :ok, :value (independent/tuple 1 (second (:value op))))
                      )
                     )
                   )
                   (catch java.sql.SQLException e
                     (let [error (.getErrorCode e)]
                       (cond 
                         (= error 2) (do 
                                      (info "Worker " (:process op) " FAILED: "(.getMessage e))
                                      (assoc op :type :fail)
                                      )
                         :else (throw e))
                     )
                   )
                 ) ; try / :write case

            :cas
                 (try
                   (let [updated (first (j/with-db-transaction [c c]
                     (let [id   (first (:value op))
                           val' (second (:value op))
                           [val uid] (second (j/query c ["select val,uid from register where id = 1"] :as-arrays? true))
                           uid' (+ (* 1000 (rand-int 100000)) (:process op))]

                         (do
                            (let [[expected-val new-val] val' ]
                                 (do
                                 (info "Worker " (:process op) " CAS FROM " expected-val "-" uid " to " new-val "-" uid')
                                 (j/execute! c [(str "update register set val=" new-val ",uid=" uid' " where id=" id " and val=" expected-val " -- old-uid is " uid)]))))
                          )
                       )
                    )
                  ]
                      (do
                      (info "Worker " (:process op) " TXN RCODE IS " updated)
                      (if (zero? updated)
                                 (do
                                 (info "Worker " (:process op) " CAS FAILED")
                                 (assoc op :type :fail)
                                 )
                                 (do
                                 (info "Worker " (:process op) " CAS SUCCESS")
                                 (assoc op :type :ok, :value (independent/tuple 1 (second (:value op))))
                                 )
                                )
                      )
                      )
                 
                   (catch java.sql.SQLException e
                     (let [error (.getErrorCode e)]
                       (cond 
                         (= error 2) (do 
                                      (info "Worker " (:process op) " FAILED: "(.getMessage e))
                                      (assoc op :type :fail)
                                      )
                         :else (throw e))
                     )
                   )
                )
            )
          )
        )
      )
    (teardown! [_ test])))



; Test on only one register for now
(defn r   [_ _] {:type :invoke, :f :read, :value [1 nil]})
(defn w   [_ _] {:type :invoke, :f :write, :value [1 (rand-int 5)]})
(defn cas [_ _] {:type :invoke, :f :cas, :value [1 [(rand-int 5) (rand-int 5)]]})

(defn client
  [n]
  (DirtyReadsClient. nil n))

(defn dirty-reads-checker
  "We're looking for a failed transaction whose value became visible to some
  read."
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [

            failed-writes  
            ; Add a dummy failed write so we have something to compare to in the
            ; unlikely case that there's no other write failures
                               (merge  {:type :fail, :f :write, :value -12, :process 0, :time 0}
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
                                    (r/filter (partial apply not=))
                                    (into []))
            filthy-reads (->> reads
                              (r/filter (partial some failed-writes))
                              (into []))]
        {:valid? (empty? filthy-reads)
         :inconsistent-reads inconsistent-reads
         :dirty-reads filthy-reads}))))

(def dirty-reads-reads {:type :invoke, :f :read, :value nil})

(def dirty-reads-writes (->> (range)
                 (map (partial array-map
                               :type :invoke,
                               :f :write,
                               :value))
                 gen/seq))

(defn dirty-reads-basic-test
  [opts]
  (merge tests/noop-test
         {:name "dirty-reads"
          :nodes ["m1" "m2" "m3" "m4" "m5"]
          :ssh {
            :username "root"
            :password "shadow"
            :strict-host-key-checking false
          }
          :db   (db (:version opts))
          :nemesis (nemesis/partition-random-halves)}
         (dissoc opts :name :version)))

(defn minutes [seconds] (* 60 seconds))

(defn dirty-reads-tester
  [version n]
  (dirty-reads-basic-test
    {:name "dirty reads"
     :concurrency 1
     :version version
     :client (client n)
     :generator (->> (gen/mix [dirty-reads-reads dirty-reads-writes])
                     gen/clients
                     (gen/time-limit 10))
     :nemesis nemesis/noop
     :checker (checker/compose
                {:perf (checker/perf)
                 :dirty-reads (dirty-reads-checker)
                 :linearizable (independent/checker checker/linearizable)})}))


(defn register-tester
  [opts]
  (basic-test
    (merge
      {:name        "register"
       :client      (comdb2-cas-register-client nil)
       :concurrency 10

       :generator   (gen/phases
                      (->> (gen/mix [w cas cas r])
                           (gen/clients)
                           (gen/stagger 1/10)
                           (gen/time-limit 10))
                      (gen/log "waiting for quiescence")
                      (gen/sleep 10))
       :model       (model/cas-register-comdb2 [1 1])
       :time-limit   100
       :recovery-time  30
       :checker     (checker/compose
                      {:perf  (checker/perf)
                       :linearizable checker/linearizable}) }
      opts)))


(defn register-tester-nemesis
  [opts]
  (basic-test
    (merge
      {:name        "register"
       :client      (comdb2-cas-register-client nil)
       :concurrency 10

       :generator   (gen/phases
                      (->> (gen/mix [w cas cas r])
                           (gen/clients)
                           (gen/stagger 1/10)
                           (gen/time-limit 300)
                           with-nemesis)
                      (gen/log "waiting for quiescence")
                      (gen/sleep 10))
       :model       (model/cas-register-comdb2 [1 1])
       :time-limit   300
       :recovery-time  30
       :checker     (checker/compose
                      {:perf  (checker/perf)
                       :linearizable checker/linearizable}) }
      opts)))

