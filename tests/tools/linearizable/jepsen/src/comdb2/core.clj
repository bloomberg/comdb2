(ns comdb2.core
 "Tests for Comdb2"
 (:require
   [clojure.tools.logging :refer :all]
   [clojure.core.reducers :as r]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.pprint :refer [pprint]]
   [dom-top.core :refer [letr with-retry]]
   [jepsen.checker.timeline  :as timeline]
   [jepsen 
    [adya :as adya]
    [client :as client]
    [core :as jepsen]
    [db :as db]
    [tests :as tests]
    [control :as c]
    [checker :as checker]
    [nemesis :as nemesis]
    [independent :as independent]
    [util :as util :refer [timeout meh]]
    [generator :as gen]
    [reconnect :as rc]]
   [knossos.op :as op]
   [knossos.model :as model]
   [clojure.java.jdbc :as j]
   [slingshot.slingshot :refer [throw+ try+]])
 (:import [knossos.model Model]))

(java.sql.DriverManager/registerDriver (com.bloomberg.comdb2.jdbc.Driver.))

; TODO: break out tests into separate namespaces

(defn cluster-nodes
  "A vector of nodes in the cluster; taken from the CLUSTER env variable."
  []
  (-> (System/getenv "CLUSTER")
      (or "m1 m2 m3 m4 m5")
      (str/split #"\s+")
      vec))

;; JDBC wrappers

(def timeout-delay "Default timeout for operations in ms" 5000)

(defmacro with-prepared-statement-with-timeout
  "Takes a DB conn and a [symbol sql-statement] pair. Constructs a prepared
  statement with a default timeout out of sql-statement, and binds it to
  `symbol`, then evaluates body. Finally, closes the prepared statement."
  [conn [symbol sql] & body]
  `(let [~symbol (j/prepare-statement (j/db-find-connection ~conn)
                                 ~sql
                                 {:timeout (/ timeout-delay 1000)})]
     (try
       ~@body
       (finally
         (.close ~symbol)))))

(defn execute!
  "Like jdbc execute, but includes a default timeout in ms."
  ([conn sql-params]
   (execute! conn sql-params {}))
  ([conn [sql & params] opts]
   (with-prepared-statement-with-timeout conn [s sql]
     (j/execute! conn (into [s] params) opts))))

(defn query
  "Like jdbc query, but includes a default timeout in ms."
  ([conn expr]
   (query conn expr {}))
  ([conn [sql & params] opts]
   (with-prepared-statement-with-timeout conn [s sql]
     (j/query conn (into [s] params) opts))))

(defn insert!
  "Like jdbc insert!, but includes a default timeout."
  [conn table values]
  (j/insert! conn table values {:timeout timeout-delay}))

(defn update!
  "Like jdbc update!, but includes a default timeout."
  [conn table values where]
  (j/update! conn table values where {:timeout timeout-delay}))

(defn upsert!
  "Inserts or updates a value in the database, by attempting an update, and if
  that fails, inserting instead."
  [conn table values where]
  (let [r (update! conn table values where)]
    (info r)
    (if (zero? (first r))
      (insert! conn table values)
      r)))

;; Connection handling

(defn conn-spec
  "JDBC connection spec for a given node."
  [node]
  (info "connecting to" node)
  {:classname   "com.bloomberg.comdb2.jdbc.Driver"
   :subprotocol "comdb2"
   ; One valid subname has a DB name and DB stage: "NAME:STAGE"
   ; Another format is "//NODE/NAME"
   ; I don't know how to do node, name, and stage together.
   ;   :subname (str (System/getenv "COMDB2_DBNAME") ":"
   ;                 (System/getenv "COMDB2_DBSTAGE"))})
   :subname     (str "//" (name node)
                     "/"  (System/getenv "COMDB2_DBNAME"))})

(defn close-conn!
  "Given a JDBC connection, closes it and returns the underlying spec."
  [conn]
  (when-let [c (j/db-find-connection conn)]
    (.close c))
  (dissoc conn :connection))

(defn wait-for-conn
  "I have a hunch connection state is asynchronous in the comdb2 driver, so we
  may need to block a bit for a connection to become ready."
  [conn]
  (with-retry [tries 30]
    (info "Waiting for conn")
    (query conn ["set hasql on"])
    ; I know it says "nontransient" but maaaybe it is???
    (catch java.sql.SQLNonTransientConnectionException e
      (when (neg? tries)
        (throw e))

      (info "Conn not yet available; waiting\n" (with-out-str (pprint conn)))
      (Thread/sleep 1000)
      (retry (dec tries))))
  conn)

(defn connect
  "Constructs and opens a reconnectable JDBC client for a given node."
  [node]
  (rc/open!
    (rc/wrapper
      {:name [:comdb2 node]
       :open (fn open []
               (timeout 5000
                        (throw (RuntimeException.
                                 (str "Timed out connecting to " node)))
                        (let [spec  (conn-spec node)
                              conn  (j/get-connection spec)
                              spec' (j/add-connection spec conn)]
                          (assert spec')
                          spec')))
                          ;(wait-for-conn spec'))))
       :close close-conn!
       :log? true})))

(defmacro with-conn
  "Like jepsen.reconnect/with-conn, but also asserts that the connection has
  not been closed. If it has, throws an ex-info with :type :conn-not-ready.
  Delays by 1 second to allow time for the DB to recover."
  [[c client] & body]
  `(rc/with-conn [~c ~client]
     (when (.isClosed (j/db-find-connection ~c))
       (Thread/sleep 1000)
       (throw (ex-info "Connection not yet ready."
                       {:type :conn-not-ready})))
     ~@body))

(defn hasql!
  "Set up hasql for a transaction."
  [c]
  (query c ["set hasql on"])
  (query c ["set max_retries 100000"]))

;; Error handling

(defmacro with-timeout
  "Like util/timeout, but throws (RuntimeException. \"timeout\") for timeouts.
  Throwing means that when we time out inside a with-conn, the connection state
  gets reset, so we don't accidentally hand off the connection to a later
  invocation with some incomplete transaction."
  [& body]
  `(util/timeout (+ 1000 timeout-delay)
                 (throw (RuntimeException. "timeout"))
                 ~@body))

; TODO: can we unify this with errorCodes?
(defn retriable-transaction-error
  "Given an error string, identifies whether the error is safely retriable."
  [e]
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

;; Test builders

(defn db
  []
  (reify
    db/DB
    (setup! [_ test node])
    (teardown! [_ test node])

    db/LogFiles
    (log-files [_ test node]
      ["/tmp/comdb2.log"])))


(defn basic-test
  [opts]
  (assert (pos? (:time-limit opts)))
  ; Wrap client generator in nemesis, combine final-gen if applicable
  (let [nemesis   (or (:nemesis opts) nemesis/partition-random-halves)
        generator (if (identical? nemesis/noop nemesis)
                    (gen/clients (:generator opts))
                    (gen/nemesis (gen/start-stop 30 10) (:generator opts)))
        generator (gen/phases (->> generator
                                   (gen/time-limit (:time-limit opts)))
                              (gen/log "Healing network")
                              (gen/nemesis (gen/once {:type :info, :f :stop})))
        generator (if-not (:final-generator opts)
                    generator
                    (gen/phases generator
                                (gen/log "Waiting for recovery")
                                (gen/sleep 30)
                                (gen/clients (:final-generator opts))))]
    (merge tests/noop-test
           {:name     "comdb2"
            :db       (db)
            :nemesis  (nemesis/partition-random-halves)
            :nodes    (cluster-nodes)
            :ssh {:username "root"
                  :password "shadow"
                  :strict-host-key-checking false}
            :generator generator
            :checker (checker/compose
                       {:perf (checker/perf)
                        :workload (:checker opts)})}
           (dissoc opts
                   :checker
                   :generator
                   :final-generator))))

(defmacro with-txn-prep!
  "Takes a connection and a body.

  1. Turns on hasql
  2. Sets transaction to serializable
  3. May turn on debug mode
  4. Sets maximum retries

  If this fails because of a connection error, sleeps briefly (to avoid
  hammering a down node with reconnects), and throws a special error, {:type
  :connect-failure-during-txn-prep}, which we know means no side effects have
  transpired. If it succeeds, moves on to execute body.

  TODO: what... should this actually be called? I don't exactly understand how
  these four commands work together, and what their purpose is"
  [c & body]
  `(do (try (query ~c ["set hasql on"])
            (query ~c ["set transaction serializable"])
            (when (System/getenv "COMDB2_DEBUG")
              (query ~c ["set debug on"]))
            (query ~c ["set max_retries 100000"])
            ; I don't think the previous queries actually detect network
            ; faults? Are they maybe executed locally?
            (query ~c ["select 1=1"])

            (catch java.sql.SQLNonTransientConnectionException e#
              (when-not (re-find #"Can't connect to db\."
                                 (.getMessage e#))
                (throw e#))

              (Thread/sleep 5000) ; Give node a chance to wake up again
              (throw+ {:type :connect-failure-during-txn-prep})))
       ~@body))

(defmacro with-logical-failures
  "Takes an op and a body. Executes body, catching known logical (not
  connection!) failures and converting them to failing ops."
  [op & body]
  `(try
     ~@body

     ; Constraint violations are known failures
     (catch java.sql.SQLIntegrityConstraintViolationException e#
       (assoc ~op :type :fail, :error (.getMessage e#)))

     ; And explicitly retriable failures are of course safe
     (catch java.sql.SQLException e#
       (condp = (.getErrorCode e#)
         2  (assoc ~op :type :fail, :error (.getMessage e#))
            (throw e#)))))

(defmacro with-io-failures
  "Takes an op and a body. Executes body, catching Exceptions and converting
  common error codes to failing or info ops. Used *outside* of with-conn, so
  with-conn has a chance to repair connections before we convert exceptions to
  ops."
  [op & body]
  `(try+
     (try
       ~@body
       (catch Exception e#
         (if (= :read (:f ~op))
           ; All read crashes are idempotent, so we convert to failures.
           (assoc ~op :type :fail, :error (.getMessage e#))
           (throw e#))))

     ; We know these failures are idempotent because they happen prior to
     ; actual work
     (catch [:type :connect-failure-during-txn-prep] e#
       (assoc ~op :type :fail, :error :can't-connect))))
