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

You can run it like this:

python toy_kba_system.py --max 10000000 --cutoff 100 filter-topics.sample-trec-kba-targets-2013.json tiny-corpus filter-run.toy_1.txt



Last modified March 7, 2013 by jrf
