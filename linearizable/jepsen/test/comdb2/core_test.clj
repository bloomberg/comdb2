(ns comdb2.core-test
  (:require [clojure.test :refer :all]
            [comdb2.core :refer :all]
            [clojure.java.jdbc :refer :all]
            [jepsen.core :as jepsen]))

; Basic test - does SQL work?
; (deftest a-test
;  (let [db-spec {:classname "com.bloomberg.comdb2.jdbc.Driver"
;            :subprotocol "comdb2"
;            :subname "mikedb:dev"}]
;   (with-db-connection [db-conn db-spec]
;    (is (= (doseq [results (query db-conn ["select 1,2,3,4,5"])] {:1 1, :2 2, :3 3, :4 4, :5 5}))))))

; Do serializable transactions work?
;(deftest test-bank
; (let [test-spec (bank-test 10 100)]
;  (is (:valid? (:results (jepsen/run! test-spec))))))


;(deftest test-bank-nemesis
; (let [test-spec (bank-test-nemesis 10 100)]
;  (is (:valid? (:results (jepsen/run! test-spec))))))


;(deftest sets-test'
; (is (:valid? (:results (jepsen/run! (sets-test))))))

; (deftest dirty-reads-test
;   (is (:valid? (:results (jepsen/run!  (dirty-reads-tester "6.1" 4))))))
;(deftest false-test'
;  (is (:valid? )))


;(deftest register-test-nemesis
;  (is (:valid? (:results (jepsen/run! (register-tester-nemesis { } ))))))

(deftest register-test
  (is (:valid? (:results (jepsen/run! (register-tester { } ))))))


