(ns knossos.prioqueue
  "A high-performance priority queue where only approximate ordering is
  required."
  (:require [clojure.tools.logging :refer :all])
  (:import (org.cliffc.high_scale_lib NonBlockingHashMapLong)
           (java.util.concurrent PriorityBlockingQueue
                                 BlockingQueue
                                 TimeUnit)))

; Tuple of a priority and a queue element. We were wasting
; too much time with custom comparators.
(defrecord PenaltyBox [^long prio value]
  java.lang.Comparable
  (compareTo [this other]
    (compare prio (.prio ^PenaltyBox other))))

(defprotocol PrioQueue
  (put!  [q prio element])
  (poll! [q timeout]))

(defrecord StripedBlockingQueue [qs]
  PrioQueue
  (put! [q prio element]
    ; Push onto our preferred queue
    (let [t (mod (.. Thread currentThread getId) (count qs))]
      (.put ^BlockingQueue (nth qs t)
            (PenaltyBox. prio element))))

  (poll! [q timeout]
    (let [n (count qs)
          t (mod (.. Thread currentThread getId) n)]
      (when-let [; Try our preferred queue
                 box (or (.poll ^BlockingQueue (nth qs t)
                                timeout TimeUnit/MILLISECONDS)
                         ; Then rotate through the others
                         (loop [i 0]
                           (when (< i n)
                             (or (.poll ^BlockingQueue
                                        (nth qs (mod (+ t i) n)))
                                 (recur (inc i))))))]
        (.value ^PenaltyBox box)))))


(defn prioqueue []
  (StripedBlockingQueue.
    (->> (repeatedly #(PriorityBlockingQueue. 1))
         (take 12)
         vec)))
