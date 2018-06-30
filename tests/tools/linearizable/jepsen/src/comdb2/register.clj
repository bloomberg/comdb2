(ns comdb2.register
  "Linearizable single-register test"
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
     [independent :as independent]
     [util :as util :refer [timeout meh]]
     [generator :as gen]
     [reconnect :as rc]]
    [knossos.op :as op]
    [knossos.model :as model]
    [clojure.java.jdbc :as j]
    [slingshot.slingshot :refer [throw+ try+]]))

(defn comdb2-cas-register-client
  "Comdb2 register client"
  ([]
   (comdb2-cas-register-client "register" nil nil))
  ([table conn uids]
   (reify client/Client
     (setup! [this test node]
       (let [conn (c/connect node)]
         (c/with-conn [c conn]
           (j/delete! c :register ["1 = 1"]))
         (comdb2-cas-register-client table conn (atom -1))))

     (invoke! [this test op]
       (c/with-io-failures op
         (c/with-conn [c conn]
           (c/with-logical-failures op
             (c/with-timeout
               (c/with-txn-prep! c
                 (case (:f op)
                   :read
                   (let [[id val'] (:value op)
                         [val uid] (second
                                     (c/query c [(str "select val,uid from "
                                                      table " where id = ? -- id is " id) id]
                                            {:as-arrays? true}))]
                     (assoc op
                            :type  :ok
                            :uid   uid
                            :value (independent/tuple id val)))

                   :write
                   (let [[id val] (:value op)
                         uid      (swap! uids inc)
                         updated  (first (c/upsert! c table
                                                    {:id id, :val val, :uid uid}
                                                    ["id = ?" id]))]
                     (assert (<= 0 updated 1))
                     (assoc op :type (if (zero? updated) :fail :ok), :uid uid))

                   :cas
                   (let [[id [v v']] (:value op)
                         uid (swap! uids inc)
                         updated (first (c/update! c table
                                                   {:val v', :uid uid}
                                                   ["id = ? and val = ?"
                                                    id v]))]
                     (assert (<= 0 updated 1))
                     (assoc op
                            :type (if (zero? updated) :fail :ok)
                            :uid uid)))))))))

       (teardown! [_ test]
                  (rc/close! conn)))))

; Test on only one register for now
(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn workload
  []
  {:client (comdb2-cas-register-client)
   :generator   (independent/concurrent-generator
                  10
                  (range)
                  (fn [k]
                    (->> (gen/reserve 5 (gen/mix [w cas cas]) r)
                         (gen/stagger 1/10)
                         (gen/limit 200))))
   :model       (model/cas-register)
   :checker     (independent/checker
                  (checker/compose
                    {:timeline (timeline/html)
                     :linearizable (checker/linearizable)}))})
