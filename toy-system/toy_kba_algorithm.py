#!/usr/bin/python
"""
Implements a very simple scoring function the looks for exact matches
of the entity's name components in the text of the document.


Copyright (c) 2012-2013 Computable Insights LLC
released under the MIT X11 License, see license.txt
"""

## import standard libraries
import re
import sys
import json
import urllib
import string
import hashlib
import logging
import traceback
from streamcorpus import OffsetType

logger = logging.getLogger('kba-toy-system')

slot_names = dict(
    PER = ['Affiliate', 'AssociateOf', 'Contact_Meet_PlaceTime', 'AwardsWon', 
           'DateOfDeath', 'CauseOfDeath', 'Titles', 'FounderOf', 'EmployeeOf'],
    FAC = ['Affiliate', 'Contact_Meet_Entity'],
    ORG = ['Affiliate', 'TopMembers', 'FoundedBy']
    )

## make a unicode translation table to converts all punctuation to white space
strip_punctuation = dict((ord(char), u" ") for char in string.punctuation)

white_space_re = re.compile("(\s|\n|\r)+")

def strip_string(s):
    """
    strips punctuation and repeated whitespace from unicode strings
    """
    return white_space_re.sub(" ", s.translate(strip_punctuation).lower())

def prepare_entities(targets, recall_filters=None):
    """
    Creates a dict keyed on entity URLs with the values set to a
    representation that is efficient for the scorer
    """
    if recall_filters is None:
        recall_filters = {}
    prep = {}
    for target in targets:
        target_id = target['target_id']
        names = []
        longest = 0
        for name in recall_filters.get(target_id, []):
            name = strip_string(name)
            names.append(name)
            longest = max(longest, len(name))

        if not names:
            name = target_id.split('/')[-1]

            name = urllib.unquote(name)
            assert isinstance(name, unicode)

            ## create set of tokens from entity's name
            names = list(set(strip_string(name).split()))

            ## add full name as one of the 'names'
            full_name = strip_string(name)
            names.append(full_name)
            longest = len(full_name)

        assert len(names) > 0, target
            
        prep[target_id] = dict(parts=names, longest=longest, 
                               entity_type=target['entity_type'])

    return prep


class Scorer:
    def __init__(self, si):
        """
        Take StreamItem (si) and prepare to evaluate entity mentions
        """
        try:
            self.text = strip_string(si.body.clean_visible.decode('utf8'))
            self.ready = True
        except Exception, exc:
            ## ignore failures, such as PDFs
            #sys.exit(traceback.format_exc(exc))
            logger.warn("failed to initialize on doc: %s\n" % exc)
            self.ready = False

        if si.body.sentences and 'lingpipe' in si.body.sentences:
            self.sentences = []
            for sent in si.body.sentences['lingpipe']:
                sent_str = strip_string(
                    u' '.join([tok.token.decode('utf8') for tok in sent.tokens]))
                sent_first = sent.tokens[0].offsets[OffsetType.BYTES].first
                last_token_offset = sent.tokens[-1].offsets[OffsetType.BYTES]
                sent_last = last_token_offset.first + last_token_offset.length
                self.sentences.append((sent_str, sent_first, sent_last))
            
        else:
            logger.warn('missing sentences for %s' % si.stream_id)

    def assess_target(self, entity_representation):
        """
        Searches text for parts of entity_name

        :returns tuple(confidence, relevance, contains_mention):

        confidence score is between zero and 1000, which represents a
        float in [0,1] measured in thousandths.

        relevance is an integer in the set [-1, 0, 1, 2], which
        represents "garbage", "neutral", "useful", "vital"

        contains_mention is an integer in the set [0, 1], which
        represents a boolean assertion that the document either
        mentions or does not mention the target entity
        """
        ## look for name parts in text:
        len_longest_observed_name = 0
        self.longest_observed_name = ''
        for name in entity_representation["parts"]:
            if name in self.text:
                if len(name) > len_longest_observed_name:
                    len_longest_observed_name = len(name)
                    ## hold on to this string for SSF below
                    self.longest_observed_name = name

        ## default score is 0
        if len_longest_observed_name == 0:
            ## zero confidence, relevance="garbage", non-mentioning
            return 0, -1, 0

        ## normalize score by length of longest name, which is full_name
        conf_zero_to_one = float(len_longest_observed_name) / entity_representation["longest"]

        ## return score in thousandths
        confidence = int(1000 * conf_zero_to_one)

        relevance = 2  ## hard code "vital" level for this toy system
        
        contains_mention = 1  ## hard code always mentioning for this
                              ## toy system

        return (confidence, relevance, contains_mention)

    def fill_slots(self, entity_representation):
        '''
        simple algorithm for filling all of the slot types for
        entity_type.  Finds the longest sentence containing the
        longest name observed above, and returns that entire sentence
        for every slot type for this entity_type.
        '''        
        longest_sentence = ''
        for sent_str, first, last in self.sentences:
            if self.longest_observed_name in sent_str:
                if len(sent_str) > len(longest_sentence):
                    longest_sentence = sent_str

                    ## construct original byte range for this sentence
                    byte_range = '%d-%d' % (first, last)
        
        if not longest_sentence:
            ## no slot fills
            return
        
        ## most conservative approach to slot alias equivalence is to
        ## treat the sentence itself as the equiv class name; here we
        ## hash the sentence to a shorter string.  Only identical
        ## duplicate sentences will get same slot equiv class -- such
        ## duplicate sentences do occur in the KBA corpus.
        slot_equiv_id = hashlib.md5(longest_sentence.encode('utf8')).hexdigest()

        for slot_name in slot_names[entity_representation['entity_type']]:
            ## for toy system, just assert that longest sentence is
            ## the slot fill for every slot name            
            yield (
                1000, slot_name, slot_equiv_id, 
                ## can uncomment this and comment out byte_range as a
                ## hack to inspect the sentences instead of just
                ## seeing byte ranges
                #longest_sentence,
                byte_range,
                )
        
