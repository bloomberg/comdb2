(ns knossos.op
  "Operations on operations!

  An operation is comprised of a `process` with a `type` indicating whether it
  is invoking, completing, failing, or noting progress of a function `f`,
  called with an argument `value`. In addition, operations may have an `:index`
  which identifies their position in a history, and carry extra keys.")

(defrecord Op [process type f value ^:int index])

(defn op
  "Constructs a new operation for a history."
  [process type f value]
  (Op. process type f value -1))

(defn invoke
  "Constructs an invocation op."
  [process f value]
  (op process :invoke f value))

(defn ok
  "Constructs an OK op."
  [process f value]
  (op process :ok f value))

(defn fail
  "Constructs a fail op."
  [process f value]
  (op process :fail f value))

(defn info
  "Constructs an info op."
  [process f value]
  (op process :info f value))

(defn ok?
  "Is this op OK?"
  [op]
  (= :ok (:type op)))

(defn invoke?
  "Is this op an invocation?"
  [op]
  (= :invoke (:type op)))

(defn fail?
  "Is this op a failure?"
  [op]
  (= :fail (:type op)))

(defn info?
  "Is this op an informational message?"
  [op]
  (= :info (:type op)))

(defn same-process?
  "Do A and B come from the same process?"
  [a b]
  (= (:process a)
     (:process b)))
