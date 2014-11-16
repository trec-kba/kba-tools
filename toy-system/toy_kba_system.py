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
    "task_id": "kba-ccr-2013",
    "topic_set_id": None,  ## will set this below
    "corpus_id":    None,  ## will set this below
    "team_id": "CompInsights",
    "team_name": "Computable Insights",
    "poc_name": "TREC KBA Organizers", 
    "poc_email": "trec-kba@googlegroups.com",
    "system_id": "toy_1",
    "run_type": "automatic",
    "system_description": "Entity title strings are used as surface form names, then any document containing one of the surface form names is ranked vital with confidence proportional to length of surface form name, and the longest sentence containing the longest surface form name is treated as a slot fill for all slot types for the given entity type.",
    "system_description_short": "relevance=2, exact name match, longest sentence slot fills",
    }
## This filter_run dict will be serialized to a .json file below

## import standard libraries and parse command line args
import re
import os
import sys
import json
import yaml
import time
import copy
import logging
import streamcorpus

## import the command line parsing library from python 2.7, can be
## installed on early python too.
import argparse
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(dest="mode",   help="'simple' baseline and 'slots' baseline")
parser.add_argument(dest="filter_topics",   help=".json file of filter-topics")
parser.add_argument(dest="profiles",   help=".json (or .yaml) file containing profiles map from target_id to lists of judged documents and a set of slots")
parser.add_argument(dest="corpus", help="name of directory containing XX/YY/stream_id.sc.xz.gpg files")
parser.add_argument(dest="output", help="filename to create for storing output of this run")
parser.add_argument("--max", dest="max_docs", type=int, default=100, help="limit number of docs we examine")
parser.add_argument("--cutoff", dest="cutoff", type=int, default=400, help="relevance cutoff, measured in thousandths")
parser.add_argument("--target-id", default='', help="specific target_id to run")
parser.add_argument("--names-frac", default=False, action='store_true', help="use fraction of name length as confidence")
parser.add_argument("--ssf", default=False, action="store_true", help="generate Streaming Slot Filling (SSF) results instead of the default Cummulative Citation Recommendation (CCR)")
parser.add_argument("--slot-names", default=None, help="path to JSON file mapping entity_type to list of slot_names")
args = parser.parse_args()

logger = logging.getLogger("kba-toy-system")
logger.setLevel("DEBUG")
ch = logging.StreamHandler()
ch.setLevel("DEBUG")
formatter = logging.Formatter("%(asctime)s %(process)d %(levelname)s: %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

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
filter_topics = json.load(open(args.filter_topics))

## set the topic set identifier in filter_run
#filter_run["topic_set_id"] = filter_topics["topic_set_id"]

if args.profiles.endswith('.json'):
    profiles = json.load(open(args.profiles))
elif args.profiles.endswith('.yaml'):
    profiles = yaml.load(open(args.profiles))

## init our toy algorithm
entities = filter_topics["targets"]

if args.mode not in ['slots', 'simple']:
    sys.exit("mode argument must be either 'slots' or 'simple'")

conf_heuristic = toy_kba_algorithm.LEN_FRAC
if not args.names_frac:
    conf_heuristic = toy_kba_algorithm.NAMES_FRAC

recall_filters = {}
for target_id, data in profiles['entities'].iteritems():
    recall_filters[target_id] = []
    for slot_name, values in data['slots'].iteritems():
        if slot_name.isupper() and args.mode == 'slots':
            for val in values:
                recall_filters[target_id].append(val['value'])
        elif args.mode == 'simple' and slot_name == 'canonical_name':                
            recall_filters[target_id].append(values)
            recall_filters[target_id] += values.split()

print recall_filters

slot_names = {}
if args.slot_names:
    slot_names = json.load(open(args.slot_names))

entity_representations = toy_kba_algorithm.prepare_entities(
    entities, recall_filters=recall_filters, 
    slot_names=slot_names,
)
logger.info( json.dumps(entity_representations, indent=4, sort_keys=True) )

## set the corpus identifier in filter_run
corpus_id_parts = args.corpus.split("/")
filter_run["corpus_id"] = corpus_id_parts[-1] or corpus_id_parts[-2]

## store some non-required run info of our own design to the
## filter_run dict to store in our submission... not too much, just a
## bit of context for humans.
filter_run["run_info"] = {
    "num_entities": len(entities),
    }

print_comments = False
if print_comments:
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

if args.target_id:
    ## for parallel mode, just do one
    target_ids = [args.target_id]
else:
    target_ids = [rec['target_id'] for rec in filter_topics['targets']
                  if rec['training_time_range_end']]

for target_id in target_ids:

    ## only go up to max_docs
    if num_docs >= args.max_docs:
        break

    logger.info("Processing " + target_id)

    for citation in profiles['entities'][target_id]['citations']:
        ## only go up to max_docs
        if num_docs >= args.max_docs:
            break

        stream_id = citation['mention_id'].split('#')[0]
        epoch_ticks, doc_id = stream_id.split('-')
        first = doc_id[:2]
        second = doc_id[2:4]

        chunk_path = os.path.join(args.corpus, first, second, stream_id) + '.sc.xz.gpg'

        ## read StreamItem instances until we hit prescribed max,
        ## which might take more than one chunk file

        if not os.path.exists(chunk_path):
            logger.critical('failed to find %s' % chunk_path)
            continue
        
        for si in streamcorpus.Chunk(path=chunk_path):
            if num_docs == args.max_docs:
                break

            if not si.body.clean_visible:
                ## This sytem only considers docs that have
                ## clean_visible text
                logger.critical('giving up for lack of clean_visible')
                continue

            ## count docs considered
            num_docs += 1

            ## instantiate an instance of Scorer from toy_kba_algorithm
            scorer = toy_kba_algorithm.Scorer(si)
            
            ## give up if the scorer fails
            if not scorer.ready:
                logger.critical('failed because scorer is not ready')
                continue

            entity_repr = entity_representations[target_id]

            if 1:
                ## run a filter algorithm
                confidence, relevance, contains_mention = \
                    scorer.assess_target(entity_repr, conf_heuristic)
                num_entity_doc_compares += 1

                if not confidence > args.cutoff:
                    logger.info('dropping line for low conf=%f' % confidence)
                else:
                    ## assemble line in the format specified on
                    ## http://trec-kba.org/trec-kba-2013.shtml#submissions
                    ccr_rec = [
                        ## sytem identifier
                        filter_run["team_id"], filter_run["system_id"], 
                            
                        ## this task identifier
                        si.stream_id, target_id, 

                        ## algorithm output:
                        confidence, relevance, contains_mention,

                        ## identify the directory containing this chunk file
                        '', #date_hour, 

                        ## default values for SSF run
                        "NULL", -1, "0-0",
                        ]

                    if not args.ssf:
                        ## use only the CCR record
                        recs = [ccr_rec]

                    else:
                        ## instead of the CCR record, generate SSF records
                        recs = []

                        if relevance == 2:
                            ## on "vital" ranked docs, attempt Streaming
                            ## Slot Filling (SSF)
                            for row in scorer.fill_slots(entity_repr):

                                ## these fields differ from the base CCR record:
                                ssf_conf, slot_name, slot_equiv_id, byte_range = row

                                ## copy CCR record and insert SSF-specific fields:
                                ssf_rec = copy.deepcopy(ccr_rec)
                                ssf_rec[4]  = ssf_conf
                                ssf_rec[8]  = slot_name
                                ssf_rec[9]  = slot_equiv_id
                                ssf_rec[10] = byte_range

                                recs.append(ssf_rec)

                    logger.debug('saving %d recs' % len(recs))
                    for rec in recs:
                        assert len(rec) == 11, (len(rec), rec)

                        output.write("\t".join(map(str, rec)) + "\n")
                        output.flush()

                        ## keep count of how many we have save total
                        num_filter_results += 1

            ## print some speed info every 100 entities
            if num_docs % 100 == 0:
                elapsed = time.time() - start_time
                doc_rate = float(num_docs) / elapsed
                scoring_rate = float(num_entity_doc_compares) / elapsed
                logger.info("%d docs, %d scorings in %.1f --> %.3f docs/sec, %.3f compute_relevance/sec" % (
                        num_docs, num_entity_doc_compares, elapsed, doc_rate, scoring_rate))

## store more run info to our official filter_run dict
filter_run["run_info"]["num_entity_doc_compares"] = num_entity_doc_compares
filter_run["run_info"]["num_filter_results"] = num_filter_results
filter_run["run_info"]["elapsed_time"] = time.time() - start_time
filter_run["run_info"]["num_stream_hours"] = num_stream_hours

if args.ssf:
    filter_run["task_id"] = "kba-ssf-2013"

## create nicely indented json string 
filter_run_json_string = json.dumps(filter_run, indent=4, sort_keys=True)
## convert to comment lines
filter_run_json_string = re.sub("\n", "\n#", filter_run_json_string)
if print_comments:
    ## add these comment lines to end of output, and close the output
    output.write("#%s\n" % filter_run_json_string)

output.close()

print "#%s\n" % filter_run_json_string
print "output is stored in %r" % args.output
print "# done!"
