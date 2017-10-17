(ns comdb2.core-test
  (:require [clojure.test :refer :all]
            [comdb2.core :as c]
            [clojure.java.jdbc :as j]
            [jepsen.core :as jepsen]))

; Basic test - does SQL work?
; (deftest a-test
;  (let [db-spec {:classname "com.bloomberg.comdb2.jdbc.Driver"
;            :subprotocol "comdb2"
;            :subname "mikedb:dev"}]
;   (with-db-connection [db-conn db-spec]
;    (is (= (doseq [results (query db-conn ["select 1,2,3,4,5"])] {:1 1, :2 2, :3 3, :4 4, :5 5}))))))

; Do serializable transactions work?

(defn check
  [t]
  (is (= true (:valid? (:results (jepsen/run! t))))))

(deftest ^:test-bank test-bank
  (check (c/bank-test 10 100)))

(deftest ^:test-bank-nemesis test-bank-nemesis
  (check (c/bank-test-nemesis 10 100)))

(deftest ^:sets-test sets-test
  (check (c/sets-test)))

(deftest ^:sets-test-nemesis sets-test-nemesis
 (check (c/sets-test-nemesis {})))

(deftest ^:dirty-reads-test dirty-reads-test
  (check (c/dirty-reads-test-nemesis 4)))

(deftest ^:register-test-nemesis register-test-nemesis
  (check (c/register-tester-nemesis { })))

(deftest ^:register-test register-test
  (check (c/register-tester {})))

(deftest ^:g2-test g2-test
  (check (c/g2-test {})))
