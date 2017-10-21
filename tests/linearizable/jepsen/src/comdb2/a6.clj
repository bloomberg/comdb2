(ns comdb2.a6
 "Looks for A6: a read only snapshot isolation phenomenon."
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

(defn a6-id->key-acct
  "Converts an id number to [key account] tuple."
  [id]
  [(quot id 2) (mod id 2)])

(defn a6-id
  "Build an id out of a key and account."
  [key acct]
  (assert (<= 0 acct 1))
  (+ (* 2 key) acct))

(defn a6-fetch
  "Fetch an account balance."
  [conn table key account-number]
  (-> (c/query conn [(str "select * from " table
                          " where key = ? and account = ?")
                     key account-number])
      first
      :value
      (or 0)))

(defn a6-fetch-by-id
  "Fetch an account balance by ID."
  [conn table key acct]
  (-> (c/query conn [(str "select * from " table " where id = ?")
                     (a6-id key acct)])
      first
      :value
      (or 0)))

(defn a6-upsert!
  "Upsert a record using the specified where clause."
  [conn table key acct value where]
  (c/upsert! conn table
           {:id      (a6-id key acct)
            :key     key
            :account acct
            :value   value}
           where))

(defn a6-upsert-by-id!
  "Upsert a record by id."
  [conn table key acct value]
  (a6-upsert! conn table key acct value ["id = ?" (a6-id key acct)]))

(defn a6-upsert-by-key-acct!
  "Upsert a record by key and account."
  [conn table key acct value]
  (a6-upsert! conn table key acct value ["key = ? and account = ?" key acct]))

(defrecord A6Client [table conn]
  client/Client
  (setup! [this test node]
    (let [conn (c/connect node)]
      (assoc this :conn conn)))

  (invoke! [this test op]
    (c/with-conn [c conn]
      (c/with-timeout
        (let [[k _] (:value op)]
          (j/with-db-transaction [c c {:isolation :serializable}]
            (c/hasql! c)

            (case (:f op)
              :read (let [rows (->> (c/query c [(str "select * from " table
                                                     " where key = ?") k])
                                    (map (juxt :account :value))
                                    (into {}))]
                      (assoc op
                             :type :ok
                             :value (independent/tuple
                                      k
                                      [(rows 0 0)
                                       (rows 1 0)])))

              :deposit  (let [b1 (+ 20 (a6-fetch-by-id c table k 1))]
                          (a6-upsert-by-id! c table k 1 b1)
                          (assoc op
                                 :type :ok
                                 :value (independent/tuple k [nil b1])))

              :withdraw (let [b0  (a6-fetch-by-id c table k 0)
                              b1  (a6-fetch-by-id c table k 1)
                              b0  (- b0 10)   ; Withdraw
                              fee (if (pos? (+ b0 b1)) 0 1)
                              b0  (- b0 fee)] ; Apply fee
                          (a6-upsert-by-id! c table k 0 b0)
                          (assoc op
                                 :type :ok
                                 :value (independent/tuple k [b0 b1])))))))))

  (teardown! [this test]
    (rc/close! conn)))

(defn a6-client
  []
  (A6Client. "a6" nil))

(defn merge-vec
  "Merge two sequences into a vector, overlaying non-nil elements of b on top
  of a."
  [a b]
  (mapv (fn [a b] (if (nil? b) a b)) a b))

(defn a6-check-subhistory
  "Given all completed operations on a single key, check whether they
  interleaved correctly. Returns nil, or an error map if we find something
  weird."
  [history]
  (letr [history (filter op/ok? history)
         rs (filter #(= :read (:f %)) history)
         w  (first (filter #(= :withdraw (:f %)) history))
         d  (first (filter #(= :deposit (:f %)) history))
         _ (when-not (and w d (seq rs)) (return :unknown))
         _ (info "checking\n" (with-out-str (pprint history)))
         legal (into #{}
                     [[0 0]                              ; Initial state
                      (:value w)                         ; Withdrawal state
                      (merge-vec [0 0] (:value d))       ; Deposit on init
                      (merge-vec (:value w) (:value d))]); Deposit on withdrawal
         violations (remove (comp legal :value) rs)]
    (if (seq violations)
      {:illegal-reads violations
       :w w
       :d d}
      :ok)))

(defn a6-checker
  "Check a6 histories. We have a read, an initial state of two accounts with
  value [0 0], and two possible transactions (deposit, withdraw) which could
  interleave in either order. The interleavings are:

  r d w ; Both initial reads return the initial state [0 0]
  r w d

  d r w ; We see the deposit partial state merged on top of [0 0]
  w r d ; We see the withdrawal state, since it's total

  d w r ; We see the withdrawal state, since it's total
  w d r ; We see the deposit merged on top of the withdrawal state

  We compute a legal set of reads, and check to see that the read is one of
  those."
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [results (->> (independent/history-keys history)
                         (reduce (fn [m k]
                                   (let [res (a6-check-subhistory
                                               (independent/subhistory
                                                 k history))]
                                     (condp = res
                                       :ok      (update m :ok-count inc)
                                       :unknown (update m :unknown-count inc)
                                       (-> m
                                           (assoc :valid? false)
                                           (update :error-count inc)
                                           (update :errors assoc k res)))))
                                 {:valid? true
                                  :ok-count       0
                                  :unknown-count  0
                                  :error-count    0
                                  :errors         []}))]
        results))))

(defn a6-gen
  "Generator for SI read-only anomaly operations."
  []
  (independent/concurrent-generator
    5
    (range)
    (fn [k]
      (let [withdrawer (atom nil)
            depositor  (atom nil)
            withdrawn? (atom false)
            deposited? (atom false)]
        (reify gen/Generator
          (op [_ test process]
            (let [t (gen/process->thread test process)]
              ; Withdraw or deposit has completed
              (when (= @withdrawer t) (reset! withdrawn? true))
              (when (= @depositor t)  (reset! deposited? true))
              (cond ; Done
                    (and @withdrawn? @deposited?)
                    nil

                    ; We are going to withdraw
                    (compare-and-set! withdrawer nil t)
                    {:type :invoke, :f :withdraw}

                    ; We are going to deposit
                    (compare-and-set! depositor nil t)
                    {:type :invoke, :f :deposit}

                    ; Otherwise, a read
                    true
                    {:type :invoke, :f :read}))))))))

(defn workload
  []
  {:client    (a6-client)
   :generator (a6-gen)
   :checker     (checker/compose {:a6 (a6-checker)})})
