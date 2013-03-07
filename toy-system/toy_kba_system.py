#!/usr/bin/python
"""
This is a very simple stream filtering system that illustrates the
input/output for the kba-ccr-2013 task in TREC KBA http://trec-kba.org

kba-ccr-2013 has a corpus organized in hourly directories containing
xz-compressed files of thrifts.  This toy system iterates over the
directories in chronological order and generates lines of the format
described on http://trec-kba.org/trec-kba-2013.shtml#submissions

The first line of a submission file must contain a JSON string in the
schema of http://trec-kba.org/schemas/v1.1/filter-run.json which is
updated from 2012.

Copyright (c) 2012-2013 Computable Insights LLC
released under the MIT X11 License, see license.txt
"""

## This simple system has only one configuration, so we hard code the
## filter-run instance and save it to JSON below.  In principle, you
## could add your system configuration parameters to an instance
## document of filter-run and then have your system load that instance
## document instead of creating it as we do here.
filter_run = {
    "$schema": "http://trec-kba.org/schemas/v1.1/filter-run.json",
    "task_id": "kba-ccr-2012",
    "topic_set_id": None,  ## will set this below
    "corpus_id":    None,  ## will set this below
    "team_id": "CompInsights",
    "team_name": "Computable Insights",
    "poc_name": "TREC KBA Organizers", 
    "poc_email": "trec-kba@googlegroups.com",
    "system_id": "toy_1",
    "run_type": "automatic",
    "system_description": "Collapses entity title strings and documents into sets of words and looks for fraction of exact match overlap with entity titles.  Confidence is fraction of entity title words that appear in doc.",
    "system_description_short": "relevance=2,contains_mention=1,confidence=exact_match_name_tokens/num_name_tokens"
    }
## This filter_run dict will be serialized to a .json file below

## import standard libraries and parse command line args
import re
import os
import sys
import json
import time
import streamcorpus

## import the command line parsing library from python 2.7, can be
## installed on early python too.
import argparse
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(dest="entities",   help=".json file containing array of entity IDs")
parser.add_argument(dest="corpus", help="name of directory containing corpus hourly dirs, name must be corpus_id")
parser.add_argument(dest="output", help="filename to create for storing output of this run")
parser.add_argument("--max", dest="max_docs", type=int, default=100, help="limit number of docs we examine")
parser.add_argument("--cutoff", dest="cutoff", type=int, default=400, help="relevance cutoff, measured in thousandths")
args = parser.parse_args()

def log(mesg):
    sys.stderr.write("%s\n" % mesg)
    sys.stderr.flush()

## do not overwrite existing
assert not os.path.exists(args.output), "Output path already exists."
## make dir for output if it has dir
dir = os.path.dirname(args.output)
if dir and not os.path.exists(dir):
    os.makedirs(dir)
output = open(args.output, "wb+")

## get our filter algorithm
import toy_kba_algorithm

## load entities
filter_topics = json.load(open(args.entities))

## set the topic set identifier in filter_run
filter_run["topic_set_id"] = filter_topics["topic_set_id"]

## init our toy algorithm
entities = filter_topics["target_ids"]
entity_representations = toy_kba_algorithm.prepare_entities(entities)

## set the corpus identifier in filter_run
filter_run["corpus_id"] = args.corpus

## prepare to iterate over all hours in corpus in chronological order
date_hour_list = os.listdir(args.corpus)
date_hour_list.sort()

## store some non-required run info of our own design to the
## filter_run dict to store in our submission... not too much, just a
## bit of context for humans.
filter_run["run_info"] = {
    "num_entities": len(entities),
    "num_stream_hours": len(date_hour_list)
    }

## create json string (just one line, no pretty printing!)
filter_run_json_string = json.dumps(filter_run)
## write it as a comment at the first line of the file
output.write("#%s\n" % filter_run_json_string)

## do the run
# keep track of elapsed time
start_time = time.time()
num_entity_doc_compares = 0
num_filter_results = 0
num_docs = 0
num_stream_hours = 0

## iterate over stream in chronological order (see sort above)
for date_hour in date_hour_list:

    ## only go up to max_docs
    if num_docs >= args.max_docs:
        break

    log("Processing " + date_hour)

    ## iterate over all files in this hour
    date_hour_path = os.path.join(args.corpus, date_hour)

    for chunk_file_name in os.listdir(date_hour_path):

        if chunk_file_name == "stats.json":
            continue

        ## only go up to max_docs
        if num_docs >= args.max_docs:
            break

        num_stream_hours += 1

        chunk_path = os.path.join(date_hour_path, chunk_file_name)

        ## read StreamItem instances until we hit prescribed max,
        ## which might take more than one chunk file
        
        for si in streamcorpus.Chunk(path=chunk_path):
            if num_docs == args.max_docs:
                break

            if not si.body.clean_visible:
                ## This sytem only considers docs that have
                ## clean_visible text
                continue

            ## count docs considered
            num_docs += 1

            ## instantiate an instance of Scorer from toy_kba_algorithm
            scorer = toy_kba_algorithm.Scorer(si.body.clean_visible.decode('utf8'))
            
            ## give up if the scorer fails
            if not scorer.ready:
                continue

            for target_id, entity_repr in entity_representations.items():

                ## run a filter algorithm
                confidence, relevance, contains_mention = \
                    scorer.assess_target(entity_repr)
                num_entity_doc_compares += 1

                if confidence > args.cutoff:
                    ## assemble line in the format specified on
                    ## http://trec-kba.org/trec-kba-2013.shtml#submissions
                    rec = [
                        ## sytem identifier
                        filter_run["team_id"], filter_run["system_id"], 
                            
                        ## this task identifier
                        si.stream_id, target_id, 

                        ## algorithm output:
                        confidence, relevance, contains_mention,

                        ## identify the directory containing this chunk file
                        date_hour, 

                        ## default values for SSF run
                        'NULL', -1, -1,
                        ]

                    assert len(rec) == 11

                    output.write('\t'.join(map(str, rec)) + '\n')

                    ## keep count of how many we have save total
                    num_filter_results += 1

            ## print some speed info every 100 entities
            if num_docs % 100 == 0:
                elapsed = time.time() - start_time
                doc_rate = float(num_docs) / elapsed
                scoring_rate = float(num_entity_doc_compares) / elapsed
                log("%d docs, %d scorings in %.1f --> %.3f docs/sec, %.3f compute_relevance/sec" % (
                        num_docs, num_entity_doc_compares, elapsed, doc_rate, scoring_rate))

## store more run info to our official filter_run dict
filter_run["run_info"]["num_entity_doc_compares"] = num_entity_doc_compares
filter_run["run_info"]["num_filter_results"] = num_filter_results
filter_run["run_info"]["elapsed_time"] = time.time() - start_time
filter_run["run_info"]["num_stream_hours"] = num_stream_hours

## create nicely indented json string 
filter_run_json_string = json.dumps(filter_run, indent=4)
## convert to comment lines
filter_run_json_string = re.sub("\n", "\n#", filter_run_json_string)
## add these comment lines to end of output, and close the output
output.write("#%s\n" % filter_run_json_string)
output.close()

print "#%s\n" % filter_run_json_string
print "output is stored in %r" % args.output
print "# done!"
