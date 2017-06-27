(ns knossos.weak-cache-set
  "A weakly consistent concurrent cache. We need an efficient way for many
  threads to let each other know what parts of the state space have already
  been explored.

  With thanks to Coda Hale for his advice."
  (:refer-clojure :exclude [contains? add!])
  (:require [potemkin :refer [definterface+ deftype+ defrecord+]]
            [clojure.tools.logging :refer :all])
  (:import (java.util Arrays)
           (sun.misc Unsafe)
           (java.util.concurrent.atomic AtomicReferenceArray)
           (org.cliffc.high_scale_lib NonBlockingHashSet)))

(definterface+ WeakCacheSet
  (add! "Add an element to the cache. Returns true if the cache did not already
        contain x."
        [cache x])
  (clear! "Reset the cache"
          [cache]))

(deftype+ ArrayWeakCacheSet [^objects a]
  WeakCacheSet
  (add! [cache x]
        (let [idx      (mod (hash x) (alength a))
              absent?  (not= x (aget a idx))]
          (aset a idx x)
          absent?))

  (clear! [cache]
          (Arrays/fill a nil)
          cache))

(defn array
  "An unsynchronized array-backed weak cache set holding n elements."
  [n]
  (ArrayWeakCacheSet. (object-array n)))

(deftype+ AtomicArrayWeakCacheSet [^AtomicReferenceArray a]
  WeakCacheSet
  (add! [cache x]
        (let [idx     (mod (hash x) (.length a))
              cur     (.get a idx)
;              _       (when (and (not (nil? cur))
;                               (not= x cur))
;                        (info "collision:" (pr-str x) (pr-str cur)))
              absent? (not= x cur)]
          (.lazySet a idx x)
          absent?))

  (clear! [cache]
          (loop [i 0]
            (if (< i (.length a))
              (.set a i nil)
              (recur (inc i))))
          cache))

(defn atomic-array
  "An unsynchronized array-backed weak cache set holding n elements."
  [^long n]
  (AtomicArrayWeakCacheSet. (AtomicReferenceArray. n)))

(deftype+ NBHSCacheSet [^NonBlockingHashSet s]
  WeakCacheSet
  (add! [cache x]
        (.add s x))
  (clear! [cache]
          (.clear s)
          cache))

(defn nbhs
  "An exact cache based on a nonblocking hashset"
  []
  (NBHSCacheSet. (NonBlockingHashSet.)))
