(ns clj-harvest.harvest
  (:require [clj-harvest.protocol :as p]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [slingshot.slingshot :refer [try+ throw+]]
            [clojure.core.async :refer [<! >! >!! put! go-loop go chan close!
                                        sliding-buffer]]))

(defmacro listen-to
  "Create a go loop that listens for events on event-chan. Each event is 
   processed with event-action-fn . The loop terminates when the channel
   returns nil (i.e. it has been closed). Exceptions while processing 
   events are passed onto an error channel, error-chan. Returns the event
   channel, event-chan.

   Optionally counts success and error actions."
  [event-chan error-chan counting-atom counting-key & body]
  `(let [event-chan# ~event-chan
         error-chan# ~error-chan
         counting-atom# ~counting-atom
         counting-key# ~counting-key]
     (letfn [(inc-completed# [a# k#]
               (swap! a# update-in [k# :completed] (fnil inc 0)))
             (inc-error# [a# k#]
               (swap! a# update-in [k# :errors] (fnil inc 0)))]
       (go-loop []
         (when-let [~'event (<! event-chan#)]
           (try+
            ~@body
            (inc-completed# counting-atom# counting-key#)
            (catch Object err#
              (do
                (>! error-chan# err#)
                (inc-error# counting-atom# counting-key#))))
           (recur))))
     event-chan#))

(defn- make-error-chan
  "Appends errors from an error channel to file, f. Returns the error
   channel, error-chan."
  [f]
  (let [error-chan (chan (sliding-buffer 1000))]
    (go-loop []
      (when-let [error (<! error-chan)]
        (spit f (str error "\n") :append true)
        (recur)))
    error-chan))

(defn make-record-chan [record-fn error-chan stats]
  (listen-to
   (chan 10) error-chan stats :records
   (if event
     (record-fn event)
     (println "Done"))))

;; todo should use thread not go (long running requests to download content)
(defn make-set-spec-chan [oai-conf selection record-chan error-chan stats thread-count]
  (let [set-spec-chan (chan 1000)]
    (dotimes [n thread-count]
      (listen-to
       set-spec-chan error-chan stats :set-specs
       (if event
         (doseq [record (p/record-seq oai-conf (assoc selection :set (:spec event)))
                 :let [put-record (>! record-chan record)]
                 :while put-record])
         (close! record-chan))))
    set-spec-chan))

(defn put-set-specs [set-spec-chan oai-conf selection]
  (doseq [set-spec (p/set-spec-seq oai-conf selection)
          :let [put-result (>!! set-spec-chan set-spec)]
          :while put-result])
  (close! set-spec-chan))

(defn harvest
  "Start a new harvest. Returns a harvest context."
  [oai-conf record-fn & {:keys [selection strategy concurrency]
                         :or {strategy :subsets
                              selection {}
                              concurrency 10}}]
  (let [stats           (atom {})
        error-chan      (make-error-chan "errors.txt")
        record-chan     (make-record-chan record-fn error-chan stats)
        set-spec-chan   (make-set-spec-chan oai-conf selection record-chan
                                            error-chan stats concurrency)
        set-spec-future (condp = strategy
                          :subsets
                          (future (put-set-specs set-spec-chan oai-conf selection))
                          :single-set
                          (future
                            (do
                              (>!! set-spec-chan {:spec (:set selection)})
                              (close! set-spec-chan)))
                          (throw+ {:type ::unknown-strategy}))]
    {:set-spec-future set-spec-future
     :set-spec-chan   set-spec-chan
     :error-chan      error-chan
     :record-chan     record-chan
     :stats           stats}))

(defn halt-harvest
  "Terminate a harvest by closing its channels."
  [harvest-context]
  (close! (:error-chan harvest-context))
  (close! (:record-chan harvest-context))
  (close! (:set-spec-chan harvest-context)))

