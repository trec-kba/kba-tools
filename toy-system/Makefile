

clean:
	rm -rf toy-kba-system

build:  clean
	mkdir toy-kba-system
	cp README.md		toy-kba-system
	cp toy_kba_algorithm.py toy-kba-system
	cp toy_kba_system.py    toy-kba-system
	cp license.txt          toy-kba-system
	#cp toy_kba_mrjob.py     toy-kba-system
	cp -r tiny-corpus toy-kba-system/tiny-corpus
	cp filter-run.toy_1.txt toy-kba-system
	cp filter-topics.sample-trec-kba-targets-2013.json toy-kba-system
	tar czf toy-kba-system.tar.gz toy-kba-system
	rm -rf toy-kba-system
