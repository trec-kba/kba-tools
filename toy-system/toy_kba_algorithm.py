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
import string
import traceback

## make a unicode translation table to converts all punctuation to white space
strip_punctuation = dict((ord(char), u" ") for char in string.punctuation)

white_space_re = re.compile("\s+")

def strip_string(s):
    """
    strips punctuation and repeated whitespace from unicode strings
    """
    return white_space_re.sub(" ", s.translate(strip_punctuation).lower())

def prepare_entities(entity_urls):
    """
    Creates a dict keyed on entity URLs with the values set to a
    representation that is efficient for the scorer
    """
    prep = {}
    for target_id in entity_urls:
        name = target_id.split('/')[-1]

        ## create set of tokens from entity's name
        parts = list(set(strip_string(name).split()))

        ## add full name as one of the 'names'
        full_name = strip_string(name)
        parts.append(full_name)
        
        ## assemble dict
        prep[name] = {"parts": parts, "longest": len(full_name)}

    return prep


class Scorer:
    def __init__(self, text):
        """
        Takes text (unicode) and prepare to evaluate entity mentions
        """
        try:
            self.text = strip_string(text)
            self.ready = True
        except Exception, exc:
            ## ignore failures, such as PDFs
            #sys.exit(traceback.format_exc(exc))
            sys.stderr.write("failed to initialize on doc: %s\n" % exc)
            self.ready = False

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
        scores = []
        for name in entity_representation["parts"]:
            if name in self.text:
                scores.append(len(name))

        ## default score is 0
        if not scores:
            return 0, -1, 0

        ## normalize score by length of longest name, which is full_name
        conf_zero_to_one = float(max(scores)) / entity_representation["longest"]

        ## return score in thousandths
        confidence = int(1000 * conf_zero_to_one)

        relevance = 2  ## hard code "vital" level for this toy system
        
        contains_mention = 1  ## hard code always mentioning for this
                              ## toy system

        return (confidence, relevance, contains_mention)
