(ns clj-harvest.protocol
  (:require [org.httpkit.client :as hc]
            [slingshot.slingshot :refer [throw+]]
            [clojure.data.zip.xml :as xml]
            [clojure.data.zip :as zip]
            [clojure.string :as str]
            [robert.bruce :refer [try-try-again]]
            [decanter.xml :refer [str->doc]]))

(defn set-trace-requests
  [b]
  (def ^:dynamic *trace-requests* b) *trace-requests*)

(set-trace-requests false)

(def error-types
  ["cannotDisseminateFormat"
   "idDoesNotExist"
   "badArgument"
   "badVerb"
   "noMetadataFormats"
   "noRecordsMatch"
   "badResumptionToken"
   "noSetHierarchy"])

;; MUNGE: Crossref OAI-PMH error code literals do not match OAI-PMH spec.
;; Crossref OAI-PMH uses all caps with underscore spacing, OAI-PMH spec
;; explicitly specifies camel case.
(def empty-result-set-error-type "NO_RECORDS_MATCH")

(defn resumable? [response]
  (-> response
      (xml/xml1-> zip/children-auto :resumptionToken xml/text)
      str/blank?
      not))

(defn error? [response]
  (when-let [error (xml/xml1-> response :error)]
    (not= (xml/xml1-> error (xml/attr :code))
          empty-result-set-error-type)))

(defn token [response]
  (xml/xml1-> response zip/children-auto :resumptionToken xml/text))

(defn error [response]
  {:code (xml/xml1-> response :error (xml/attr :code))
   :message (xml/xml1-> response :error xml/text)})

(defn params [oai-conf verb {:keys [type from until set id token]}]
  (let [type-override (or type (:type oai-conf))]
    (cond-> {:verb verb}
      type-override (assoc :metadataPrefix type-override)
      from (assoc :from from)
      until (assoc :until until)
      set (assoc :set set)
      id (assoc :identifier id)
      token (assoc :resumptionToken token))))

(defn headers [{:keys [bearer-token] :as oai-conf}]
  (if bearer-token
    {"Authorization" (str "Bearer " bearer-token)}
    {}))

(defn make-request
  ([oai-conf verb]
   (make-request oai-conf verb {}))
  ([oai-conf verb selection]
   (let [headers (headers oai-conf)
         query-params (params oai-conf (name verb) selection)
         {:keys [status body error]}
         (try-try-again
          {:decay :exponential :tries 10 :sleep 1000
           :return? #(and (= 200 (:status %))
                          (nil? (:error %)))}
          #(do
             (when *trace-requests*
               (println "OAI Request:" query-params headers))
             (-> (:url oai-conf)
                 (hc/get {:query-params query-params :headers headers})
                 deref)))]
     (if (or error (not= 200 status))
       (throw+ {:type ::http-error
                :oai-config oai-conf
                :query-params query-params
                :status status
                :error error
                :body body})
       (let [doc (str->doc body)]
         (if (error? doc)
           (throw+ {:type ::oai-error
                    :oai-config oai-conf
                    :query-params query-params
                    :error (error doc)})
           doc))))))

(defn get-identity [oai-conf]
  (let [doc (make-request oai-conf :Identify)]
    {:name            (xml/xml1-> doc :Identify :repositoryName xml/text)
     :url             (xml/xml1-> doc :Identify :baseURL xml/text)
     :version         (xml/xml1-> doc :Identify :protocolVersion xml/text)
     :admin           (xml/xml1-> doc :Identify :adminEmail xml/text)
     :earliest        (xml/xml1-> doc :Identify :earliestDatestamp xml/text)
     :deletion-policy (xml/xml1-> doc :Identify :deletedRecord xml/text)
     :granularity     (xml/xml1-> doc :Identify :granularity xml/text)}))

(defn get-metadata-formats [oai-conf]
  (let [doc (make-request oai-conf :ListMetadataFormats)
        formats (xml/xml-> doc :ListMetadataFormats :metadataFormat)]
    (map #(hash-map :prefix (xml/xml1-> % :metadataPrefix xml/text)
                    :schema (xml/xml1-> % :schema xml/text)
                    :namespace (xml/xml1-> % :metadataNamespace xml/text))
         formats)))

(defn record-info [record-loc]
  {:id (xml/xml1-> record-loc :header :identifier xml/text)
   :date (xml/xml1-> record-loc :header :datestamp xml/text)
   :set-specs (map xml/text (xml/xml-> record-loc :header :setSpec))
   :metadata (xml/xml1-> record-loc :metadata)})

(defn get-record
  ([oai-conf identifier]
   (get-record oai-conf identifier {}))
  ([oai-conf identifier opts]
   (let [selection (assoc opts :id identifier)
         params (params oai-conf selection)]
     (-> oai-conf
         (make-request :GetRecord params)
         (xml/xml1-> :GetRecord :record)
         record-info))))

(defn oai-seq
  ([oai-conf verb item-element]
   (oai-seq oai-conf verb item-element {}))
  ([oai-conf verb item-element params]
   (oai-seq oai-conf verb item-element params nil))
  ([oai-conf verb item-element params resumption-token]
   (let [resumed-params (if resumption-token
                          (assoc params :token resumption-token)
                          params)
         response (make-request oai-conf verb (params oai-conf resumed-params))
         items (xml/xml-> response verb item-element)]
     (if-not (resumable? response)
       items
       (lazy-cat items (oai-seq oai-conf verb
                                item-element params (token response)))))))

(defn set-spec-seq
  ([oai-conf]
   (set-spec-seq oai-conf {}))
  ([oai-conf selection]
   (map #(hash-map :spec (xml/xml1-> % :setSpec xml/text)
                   :name (xml/xml1-> % :setName xml/text))
        (oai-seq oai-conf :ListSets :set selection))))

(defn identifier-seq
  ([oai-conf]
   (identifier-seq oai-conf {}))
  ([oai-conf selection]
   (map #(hash-map :id (xml/xml1-> % :identifier xml/text)
                   :date (xml/xml1-> % :datestamp xml/text)
                   :set-specs (map xml/text (xml/xml-> % :setSpec)))
        (oai-seq oai-conf :ListIdentifiers :header selection))))

(defn record-seq
  ([oai-conf]
   (record-seq oai-conf {}))
  ([oai-conf selection]
   (map record-info
        (oai-seq oai-conf :ListRecords :record selection))))

(defn conf [url & {:keys [type force-url bearer-token]}]
  (cond-> (get-identity {:url url})
    bearer-token (assoc :bearer-token bearer-token)
    force-url (assoc :url url)
    type (assoc :type type)))
