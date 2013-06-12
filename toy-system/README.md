# Toy KBA System

This directory contains a simple example system illustrating the
input/output of the task described here:

    http://trec-kba.org/trec-kba-2013.shtml


The TREC KBA 2013 corpus is stored in the streamcorpus.thrift format,
which makes it easy to load in many languages.  See examples here:

     http://github.com/trec-kba/streamcorpus


This example toy system uses the python streamcorpus module.  This
command will retrieve the streamcorpus python module from pypi over
the Internet and install it on your computer:

    sudo pip install streamcorpus



In this example, the script called "toy_kba_system.py" is the primary
example.  It uses the script called "toy_kba_algorithm.py"

This example illustrates both CCR and SSF runs.  It generates CCR
results by default.  To get SSF style output, add the --ssf flag.

You can run it like this:

python toy_kba_system.py --ssf --max 1000 --cutoff 100 trec-kba-ccr-and-ssf-2013-04-22/trec-kba-ccr-and-ssf-query-topics-2013-04-08.json  s3.amazonaws.com/aws-publicdatasets/trec/kba/kba-streamcorpus-2013-v0_2_0/ filter-run.toy_1.txt 


Note that the default in toy_kba_algorithm for generating surface form
names is to manipulate the target_id URL to get name tokens.  This
results in many short strings, like "the" and "bob", which give this
system very high recall, i.e. too much stuff.  The optional argument
--recall-filters allows you to pass a text file containing a JSON
object that maps target_id to lists of surface form names.  Such lists
of surface form names might be generated from APIs, like Wikipedia
redirects, or by hand.  If one performs research for each entity to
generate these lists of surface form strings, then the run is
considered "manual".


Last modified June 11, 2013 by jrf
