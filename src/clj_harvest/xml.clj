(ns clj-harvest.xml
  (:require [clojure.data.xml :as xml]
            [clojure.zip :as zip]))

(defn str->doc [s]
  (let [rdr (java.io.ByteArrayInputStream. (.getBytes s))]
    (-> rdr xml/parse zip/xml-zip)))
