(ns jepsen.filetest
 (:gen-class)
 (:require
 [knossos 
         [model :as model]
         [linear :as linear]]))

(defn -main
 "Usage: historyfile"
 [& args]

 (if (not= (count args) 1)
  (System/exit 1))

 (let [history (read-string (slurp (first args)))]
  (let [analysis (linear/analysis (model/cas-register) history)]
   (clojure.pprint/pprint analysis)
   (System/exit 
    (if (:valid? analysis) 
     0
     1)))))
