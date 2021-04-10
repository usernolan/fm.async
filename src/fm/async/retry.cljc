(ns fm.async.retry
  (:require
   [clojure.core.async :as async]
   [clojure.spec.alpha :as s]
   [fm.anomaly :as anomaly]
   [fm.core :as fm]))


   ;;;
   ;;; NOTE: specs, default configuration
   ;;;


(s/def ::strategy #{::constant ::linear ::exponential ::tetration})
(s/def ::max-tries integer?) ; NOTE: accepts BigInteger, etc. on JVM only
(s/def ::backoff-ms integer?)

(s/def ::opts
  (s/keys
   :req [::strategy ::max-tries ::backoff-ms]))

(s/def ::f fm/fn?)

(def default-retry-opts
  {::strategy   ::exponential
   ::max-tries  5
   ::backoff-ms 500})


   ;;;
   ;;; NOTE: helpers
   ;;;


(defn throwable-anomaly [f args t]
  (let [a {:fm/fn              f
           ::anomaly/ident     ::anomaly/throwable
           ::anomaly/args      (vec args)
           ::anomaly/throwable t}]
    (if-let [id (get (meta f) :fm/ident)]
      (assoc a :fm/ident id)
      a)))

(defn pow [base exp]
  (#?(:clj Math/pow ; NOTE: can't refer to static method directly on JVM
      :cljs js/Math.pow) base exp))

(defn retry-fn-backoff-ms [{::keys [strategy backoff-ms tries]}]
  (case strategy
    ::constant    backoff-ms
    ::linear      (* tries backoff-ms)
    ::exponential (* (pow 2 (dec tries)) backoff-ms)
    ::tetration   (* (pow tries tries) backoff-ms)))


   ;;;
   ;;; NOTE: api
   ;;;


(fm/defn retry-fn
  ([::f] (retry-fn default-retry-opts f))
  ([::opts ::f]
   (fn [& args]
     (let [opts (assoc opts ::tries 1)]
       (async/go-loop [{::keys [max-tries tries] :as opts} opts]
         (let [x (try (if (seq args) (apply f args) (f))
                      (catch #?(:clj Throwable :cljs :default) t
                        (throwable-anomaly f args t)))]
           (if (and (fm/anomaly? x)
                    (< tries max-tries))
             (let [backoff-ms (retry-fn-backoff-ms opts)]
               (async/<! (async/timeout backoff-ms))
               (recur (update opts ::tries inc)))
             x)))))))
