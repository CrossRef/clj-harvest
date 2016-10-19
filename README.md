# clj-harvest

An OAI-PMH harvester.

## Usage

In project.clj:

    [crossref/clj-harvest "0.0.1"]

An example:

	(use 'clj-harvest.protocol)
	
	(def oai-conf (conf "http://oai.provider.com/harvest"))
	
	(use 'clj-harvest.harvest)
	
	(harvest oai-conf (fn [record] (println record)))
