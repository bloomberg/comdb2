(ns comdb2.g2
  "Looks for anti-dependency cycles in predicates."
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

(defrecord G2Client [ta tb conn]
  client/Client
  (setup! [this test node]
    (let [conn (c/connect node)]
      (assoc this :conn conn)))

  (invoke! [this test op]
    (c/with-conn [c conn]
      (c/with-timeout
        (let [[k [a-id b-id]] (:value op)]
          (case (:f op)
            :insert
            (j/with-db-transaction [c c {:isolation :serializable}]
              (c/hasql! c)
              (letr [order (< (rand) 0.5)
                     as (c/query c [(str "select * from " (if order ta tb)
                                       " where key = ? and value % 3 = 0") k])
                     bs (c/query c [(str "select * from " (if order tb ta)
                                       " where key = ? and value % 3 = 0") k])
                     _ (when (or (seq as) (seq bs))
                         ; The other txn already committed
                         (return (assoc op :type :fail, :error :too-late)))
                     table (if a-id ta tb)
                     id    (or a-id b-id)
                     r     (c/insert! c table {:key   k
                                               :id    id
                                               :value (* 3 (rand-int 10))})]
                (assoc op :type :ok))))))))

  (teardown! [this test]
    (rc/close! conn)))

(defn g2-client [] (G2Client. "g2_a" "g2_b" nil))

(defn workload
  "A test for Adya's G2 phenomenon: an anti-dependency cycle allowed by
  snapshot isolation but prohibited by serializability."
  []
  {:name        "g2"
   :client      (g2-client)
   :generator   (adya/g2-gen)
   :checker     (checker/compose {:g2       (adya/g2-checker)
                                  :timeline (timeline/html)})})
