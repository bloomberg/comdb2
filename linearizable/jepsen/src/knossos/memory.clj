(ns knossos.memory
  "Helps manage memory pressure"
  (:import (java.lang.management ManagementFactory
                                 MemoryPoolMXBean
                                 MemoryNotificationInfo
                                 MemoryType)
           (javax.management NotificationEmitter
                             NotificationListener
                             Notification)))

(def frac 0.9)

(defn on-low-mem!
  "Invokes f when less than frac memory is free after GC. Globally mutates a
  MemoryPoolMXBean. Returns a zero-arity function that, when called, cancels
  the callback."
  [f]
  (let [^MemoryPoolMXBean pool
        (->> (ManagementFactory/getMemoryPoolMXBeans)
             (filter (fn [^MemoryPoolMXBean pool]
                       (and (= (.getType pool) MemoryType/HEAP)
                            (.isUsageThresholdSupported pool))))
             last)]

    ; Set the collection usage threshold
    (->> pool
         .getUsage
         .getMax
         (* frac)
         Math/floor
         (.setCollectionUsageThreshold pool)))

  ; Hook up a listener
  (let [^NotificationEmitter mem (ManagementFactory/getMemoryMXBean)
        ^NotificationListener l
        (reify NotificationListener
          (handleNotification [_ n hb]
            (when (= (.getType n)
                     MemoryNotificationInfo/MEMORY_COLLECTION_THRESHOLD_EXCEEDED)
              (f))))]
    (.addNotificationListener mem l nil nil)

    ; Cancel callback
    (fn cancel [] (.removeNotificationListener mem l))))
