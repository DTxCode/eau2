/* Authors: Ryan Heminway (heminway.r@husky.neu.edu)
 *           David Tandetnik (tandetnik.da@husky.neu.edu) */
#pragma once
#include "../../src/client/application.cpp"

/******************************************************************
 * Calculate a word count for given file:
 *   1) read the data (on to a single node)
 *   2) produce word counts per homed chunks, in parallel
 *   3) combine the results
 *******************************************************************/
class WordCount: public Application {
public:
	static const size_t BUFSIZE = 1024;
	Key in;
	// KeyBuff kbuf; (???)
	// SIMap all; (???)

	// WordCount(size_t idx, NetworkIfc &net) (???)
	WordCount(size_t idx) : Application(idx) {} 

	/** The master nodes reads the input, then all of the nodes count. */
	void run_() override {
		if (index == 0) {
			FileReader fr;
			delete DataFrame::fromVisitor(&in, &kv, "S", fr);
		}
		local_count();
		reduce();
	}

	/** Returns a key for given node.  These keys are homed on master node
	 *  which then joins them one by one. */
	Key* mk_key(size_t idx) {
		Key * k = kbuf.c(idx).get();
		LOG("Created key " << k->c_str());
		return k;
	}

	/** Compute word counts on the local node and build a data frame. */
	void local_count() {
		DataFrame* words = (kv.waitAndGet(in));
		p("Node ").p(index).pln(": starting local count...");
		SIMap map;
		Adder add(map);
		words->local_map(add);
		delete words;
		Summer cnt(map);
		delete DataFrame::fromVisitor(mk_key(index), &kv, "SI", cnt);
	}

	/** Merge the data frames of all nodes */
	void reduce() {
		if (index != 0) return;
		pln("Node 0: reducing counts...");
		SIMap map;
		Key* own = mk_key(0);
		merge(kv.get(*own), map); 
		for (size_t i = 1; i < arg.num_nodes; ++i) { // merge other nodes
			Key* ok = mk_key(i);
			merge(kv.waitAndGet(*ok), map);
			delete ok;
		}
		p("Different words: ").pln(map.size());
		delete own;
	}

	void merge(DataFrame* df, SIMap& m) {
		Adder add(m);
		df->map(add);
		delete df;
	}
}; // WordcountDemo

