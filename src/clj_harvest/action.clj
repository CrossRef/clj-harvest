(ns clj-harvest.action
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.data.xml :as xml]))

(defn dump-record-to-disk [dir record]
  (.mkdirs (io/file dir))
  (let [filename (-> record
                     :id
                     (str/replace #"/" "_")
                     (str ".xml"))]
    (spit (io/file dir filename) (:metadata record))))

(defn print-id [record]
  (println (:id record)))

(defn dump-id-to-file [f record]
  (spit (io/file f) (str (:id record) "\n") :append true))
 
