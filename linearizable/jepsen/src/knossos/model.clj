(ns knossos.model
  "A model specifies the behavior of a singlethreaded datatype, transitioning
  from one state to another given an operation."
  (:use     clojure.tools.logging)
  (:require [clojure.set :as set]
            [knossos.op :as op]
            [potemkin :refer [definterface+]]
            [knossos.history :as history]))

(definterface+ Model
  (step [model op]
        "The job of a model is to *validate* that a sequence of operations
        applied to it is consistent. Each invocation of (step model op)
        returns a new state of the model, or, if the operation was
        inconsistent with the model's state, returns a (knossos/inconsistent
        msg). (reduce step model history) then validates that a particular
        history is valid, and returns the final state of the model.

        Models should be a pure, deterministic function of their state and an
        operation's :f and :value."))

(defrecord Inconsistent [msg]
  Model
  (step [this op] this)

  Object
  (toString [this] msg))

(defn inconsistent
  "Represents an invalid termination of a model; e.g. that an operation could
  not have taken place."
  [msg]
  (Inconsistent. msg))

(defn inconsistent?
  "Is a model inconsistent?"
  [model]
  (instance? Inconsistent model))

(defrecord NoOp []
  Model
  (step [m op] m))

(def noop
  "A model which always returns itself, unchanged."
  (NoOp.))

(defrecord Register [value]
  Model
  (step [r op]
    (condp = (:f op)
      :write (Register. (:value op))
      :read  (if (or (nil? (:value op))     ; We don't know what the read was
                     (= value (:value op))) ; Read was a specific value
               r
               (inconsistent
                 (str (pr-str value) "≠" (pr-str (:value op)))))))

  Object
  (toString [r] (pr-str value)))

(defn register
  "A read-write register."
  ([] (Register. nil))
  ([x] (Register. x)))

(defrecord CASRegisterComdb2 [value]
  Model
  (step [r op]
    (do
    (condp = (:f op)
      :write (do (info "STEP " value " -> " op " SUCCESSFUL WRITE " (:value op))
                 (CASRegisterComdb2. (second (:value op))))
      :cas   (let [[cur new] (second (:value op))]
               (if (= cur value)
                 (do (info "STEP " value " -> " op " SUCCESSFUL CAS FROM " cur " TO " new)
                     (CASRegisterComdb2. new))
                 (do (info "STEP " value " -> " op " FAILING CAS BECAUSE CUR IS " cur " AND VALUE IS " value)
                     (inconsistent (str ">> can't CAS " value " from " cur " to " new)))))
      :read  (if (or (nil? (second (:value op)))
                     (= value (second (:value op))))
               (do (info "STEP " value " -> " op " SUCCESSFUL READ, VALUE=" value " :VALUE OP=" (:value op))
                    r)
               (do (info "STEP " value " -> " op " FAILING READ BECAUSE VALUE IS " value " AND :VALUE OP IS " (:value op))
                   (inconsistent (str ">> can't read " (:value op) " from register with value " value)))))))

  Object
  (toString [this] (pr-str value)))

(defn cas-register-comdb2
  "A compare-and-set register"
  ([]      (CASRegisterComdb2. nil))
  ([value] (CASRegisterComdb2. (second value))))

(defrecord CASRegister[value]
  Model
  (step [r op]
    (condp = (:f op)
      :write (CASRegister. (:value op))
      :cas   (let [[cur new] (:value op)]
               (if (= cur value)
                 (CASRegister. new)
                 (inconsistent (str ">> can't CAS " value " from " cur
                                    " to " new))))
      :read  (if (or (nil? (:value op))
                     (= value (:value op)))
               r
               (inconsistent (str ">> can't read " (:value op)
                                  " from register " value)))))
  Object
  (toString [this] (pr-str value)))

(defn cas-register
  "A compare-and-set register"
  ([]      (CASRegister. nil))
  ([value] (CASRegister. value)))

(defrecord Mutex [locked?]
  Model
  (step [r op]
    (condp = (:f op)
      :acquire (if locked?
                 (inconsistent "already held")
                 (Mutex. true))
      :release (if locked?
                 (Mutex. false)
                 (inconsistent "not held"))))

  Object
  (toString [this] (if locked? "locked" "free")))

(defn mutex
  "A single mutex responding to :acquire and :release messages"
  []
  (Mutex. false))

(defrecord MultiRegister []
  Model
  (step [this op]
    (assert (= (:f op) :txn))
    (reduce (fn [state [f k v]]
              ; Apply this particular op
              (case f
                :read  (if (or (nil? v)
                               (= v (get state k)))
                         state
                         (reduced
                           (inconsistent
                             (str (pr-str (get state k)) "≠" (pr-str v)))))
                :write (assoc state k v)))
            this
            (:value op))))

(defn multi-register
  "A register supporting read and write transactions over registers identified
  by keys. Takes a map of initial keys to values. Supports a single :f for ops,
  :txn, whose value is a transaction: a sequence of [f k v] tuples, where :f is
  :read or :write, k is a key, and v is a value. Nil reads are always legal."
  [values]
  (map->MultiRegister values))
