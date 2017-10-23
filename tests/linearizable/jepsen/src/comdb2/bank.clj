(ns comdb2.bank
 "Verifies snapshot isolation by atomically transferring balances between
 several accounts"
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
   [slingshot.slingshot :refer [throw+ try+]])
 (:import [knossos.model Model]))

(defrecord BankClient [n starting-balance conn]
  client/Client

  (setup! [this test node]
    (let [conn (c/connect node)]
      ; Create initial accts
      (dotimes [i n]
        (with-retry [tries 10]
          (Thread/sleep (rand-int 1000))
          (c/with-conn [c conn]
            (j/with-db-transaction [c c {:isolation :serializable}]
              (c/hasql! c)
              (try
                (info "Inserting" node i)
                ; Can't use insert because parameter substitution is broken
                ; hurraaaaaayyyyyy
                ; (c/insert! c :accounts {:id i, :balance starting-balance})
                (c/execute! c [(str "insert into accounts (id, balance) values ("
                                   i ", " starting-balance ");")])
                (catch java.sql.SQLException e
                  (if (.contains (.getMessage e)
                                 "add key constraint duplicate key")
                    nil
                    (throw e))))))

          (catch java.sql.SQLIntegrityConstraintViolationException e
            ; Someone else inserted
            nil)

          ; I don't know why these nodes close connections on some inserts
          ; and not others, it's the weirdest thing
          (catch java.sql.SQLNonTransientConnectionException e
            (Thread/sleep (rand-int 1000))
            (if (pos? tries)
              (do (info "Retrying insert of" i "on" node)
                  (retry (dec tries)))
              (throw e)))))
      (assoc this :conn conn)))

  (invoke! [this test op]
    (c/with-conn [c conn]
     (j/with-db-transaction [c c {:isolation :serializable}]
       (c/hasql! c)

       (case (:f op)
         :read (->> (c/query c ["select * from accounts"])
                    (mapv :balance)
                    (assoc op :type :ok, :value))

         :transfer
         (let [{:keys [from to amount]} (:value op)
               b1 (-> c
                      (c/query ["select * from accounts where id = ?" from]
                               {:row-fn :balance})
                      first
                      (- amount))
               b2 (-> c
                      (c/query ["select * from accounts where id = ?" to]
                               {:row-fn :balance})
                      first
                      (+ amount))]
           (cond (neg? b1)
                 (assoc op :type :fail, :value [:negative from b1])

                 (neg? b2)
                 (assoc op :type :fail, :value [:negative to b2])

                 true
                 (do (c/execute! c ["update accounts set balance = balance - ? where id = ?" amount from])
                     (c/execute! c ["update accounts set balance = balance + ? where id = ?" amount to])
                     (assoc op :type :ok))))))))

  (teardown! [_ test]
    (rc/close! conn)))

(defn bank-client
  "Simulates bank account transfers between n accounts, each starting with
  starting-balance."
  [n starting-balance]
  (BankClient. n starting-balance nil))

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

(defn workload
  []
  (let [n               10
        initial-balance 100]
    {:model  {:n n :total (* n initial-balance)}
     :client (bank-client n initial-balance)
     :generator (->> (gen/mix [bank-read bank-diff-transfer])
                     (gen/clients)
                     (gen/stagger 1/10))
     :final-generator (gen/once bank-read)
     :checker (bank-checker)}))
