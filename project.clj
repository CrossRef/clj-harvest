(defproject crossref/clj-harvest "0.0.1"
  :description "An OAI-PMH harvester"
  :url "http://github.com/CrossRef/clj-harvest"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.xml "0.0.8"]
                 [org.clojure/data.zip "0.1.2"]
                 [org.clojure/core.async "0.2.395"]
                 [http-kit "2.2.0"]
                 [slingshot "0.12.2"]
                 [robert/bruce "0.8.0"]])
