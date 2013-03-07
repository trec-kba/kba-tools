#!/usr/bin/python
"""
This is a MapReduce version of toy_kba_system.py.  It illustrates the
input/output for the citation-recommendation-2012 task in TREC KBA.

Unlike toy_kba_system.py, this uses hadoop and mrjob to handle the
opening of gzipped archives and deserialization via JSON.  Also unlike
toy_kba_system.py, this does not create the filter-run instance to
describe the run submission nor does this iterate over date-hour
directories chronologically.  Those crucial steps would need to be
constructured around this.

This is a mapper-only MapReduce job.


Copyright (c) 2012 Computable Insights LLC
released under the MIT X11 License, see license.txt
"""

## import standard libraries and parse command line args
import sys
import json
import time
import urllib

## use this hadoop streaming wrapper from Yelp, see
## https://github.com/Yelp/mrjob/
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

## get our filter algorithm
import toy_kba_algorithm

def log(mesg):
    sys.stderr.write("%s\n" % mesg)
    sys.stderr.flush()

def log_status(mesg):
    log("reporter:status:%s\n" % mesg)

## This is our job class with a mapper method.  No reducer is needed.
class ToyKBA(MRJob):
    ## tell mrjob to expect lines without keys containing just a JSON string
    INPUT_PROTOCOL = JSONValueProtocol
    ## similarly, generate output as json lines without keys
    OUTPUT_PROTOCOL = JSONValueProtocol

    def __init__(self, *args, **kwargs):
        MRJob.__init__(self, *args, **kwargs)

        ## load entities from json file
        log("loading entity list")
        entities = json.load(urllib.urlopen("https://s3.amazonaws.com/trec-kba-2012/entity-urlnames.json"))
        self.entity_representations = toy_kba_algorithm.prepare_entities(entities)

    def mapper(self, key, doc):
        ## mrjob has already deserialize the line to get a stream-item
        ## http://trec-kba.org/schemas/v1.0/stream-item.json

        self.increment_counter('ToyKBA', 'docs', 1)

        try:
            if "cleansed" not in doc["body"]:
                ## This sytem onnly considers docs that have cleansed
                ## text from boilerpipe's ArticleExtractor.
                ## Alternatively, system can use doc["body"]["raw"].
                return

            ## count docs considered
            self.increment_counter('ToyKBA', 'docs-with-cleansed', 1)

            ## get the text
            raw_bytes = doc["body"]["cleansed"].decode("string-escape")
            try:
                ## the content-item instance called "body" has a
                ## guess at character encoding from HTTP headers
                ## and meta tags.
                if doc["body"]["encoding"]:
                    text = raw_bytes.decode(doc["body"]["encoding"], "ignore")
                else:
                    text = raw_bytes
            except Exception, exc:
                log("decoding raw_bytes failed: %s" % exc)
                ## proceed anyway
                text = raw_bytes

            ## instantiate an instance of Scorer from toy_kba_algorithm
            scorer = toy_kba_algorithm.Scorer(text)

            ## give up if the scorer fails
            if not scorer.ready:
                return

            self.increment_counter('ToyKBA', 'docs-can-score', 1)

            ## start filter_result instance 
            filter_result = {
                "stream_id": doc["stream_id"],
                "citations": []  # will populate below
                }

            for entity in self.entity_representations:
                ## run a filter algorithm
                relevance = scorer.compute_relevance(self.entity_representations[entity])

                self.increment_counter('ToyKBA', 'doc-entity-scores', 1)

                ## hardcoded cutoff
                if relevance > 400:
                    ## add to filter_result
                    filter_result["citations"].append(
                        {"entity": entity, "relevance": relevance})

            if len(filter_result["citations"]) > 0:
                ## give the filter result to hadoop to save
                yield None, filter_result

        finally:
            ## update hadoops skip record counters
            self.increment_counter('SkippingTaskCounters','MapProcessedRecords',1)


if __name__ == "__main__":
    ToyKBA.run()
