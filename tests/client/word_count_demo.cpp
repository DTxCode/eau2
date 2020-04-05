#include <map>


#include "../../src/store/store.cpp"
#include "../../src/client/application.cpp"
#include "../../src/store/dataframe/fielder.h"
#include "../../src/store/dataframe/rower.h"
#include "../../src/store/key.h"
#include "../../src/store/network/master.h"
#include "../../src/store/store.cpp"

/* Authors: Ryan Heminway (heminway.r@husky.neu.edu)
 *           David Tandetnik (tandetnik.da@husky.neu.edu) */
/*****************************************************************************
 * WordCountFielder records the occurance of each string it sees in a given map
 */
class WordCountFielder : public Fielder {
   public:
    std::map<String*, int>* word_counts;

    WordCountFielder(std::map<String*, int>* word_counts) {
        this->word_counts = word_counts;
    }

    void start(size_t r) {
        // empty
    }

    void accept(bool b) {
        // empty
    }
    void accept(float f) {
        // empty
    }
    void accept(int i) {
        // empty
    }
    void accept(String* s) {
        if (word_counts->count(s)) {
            // count already exists
            int count = word_counts->find(s)->second;
            (*word_counts)[s] = count + 1;
        } else {
            (*word_counts)[s] = 1;
        }
    }

    void done() {
        // empty
    }
};

// Rower that counts occurances of all strings it sees and stores them in a map
class WordCounter : public Rower {
   public:
    std::map<String*, int>* word_counts;

    WordCounter() {
        word_counts = new std::map<String*, int>;
    }

    ~WordCounter() {
        delete word_counts;
    }

    // Returns pointer to the actual map with data
    std::map<String*, int>* get_word_counts() {
        return word_counts;
    }

    bool accept(Row& r) {
        WordCountFielder f(word_counts);

        // f will store counts in word_counts
        r.visit(r.get_idx(), f);

        // return value is ignored
        return false;
    }
};

/******************************************************************
 * Calculate a word count for given file:
 *   1) read the data (on to a single node)
 *   2) produce word counts per homed chunks, in parallel
 *   3) combine the results
 *
 * Assumes given file will be a SoR file of only strings. Malformed 
 * files or non-existent filenames result in undefined behavior. 
 *******************************************************************/
class WordCount : public Application {
   public:
    Key* data_key;
    const char* file_name;

    WordCount(const char* file_name, Store* store) : Application(store) {
        data_key = new Key((char*)"wc-data", 0);
        this->file_name = file_name;

	run_();
    }

    ~WordCount() {
        delete data_key;
    }

    /** The master nodes reads the input, then all of the nodes count. */
    void run_() override {
	printf("WC %zu is running\n", this_node());
        // Node 0 distibutes the data, waits for everyone (including itself) to do their local_maps,
        // and then combines the results with reduce()
        if (this_node() == 0) {
            FILE* input_file = fopen(file_name, "r");
            if (input_file == nullptr) {
                exit_with_msg("Failed to open file to run WordCount on");
            }

            delete DataFrame::fromSorFile(data_key, store, input_file);

            local_count();
            reduce();
        } else {
            local_count();
        }
    }

    // Returns a key for storing a node's partial WordCounts results
    Key* mk_key(size_t idx) {
        size_t needed_buf_size = snprintf(nullptr, 0, "%s%zu", "wc-result-", idx) + 1;
        char key_name[needed_buf_size];
        snprintf(key_name, needed_buf_size, "%s%zu", "wc-result-", idx);

        return new Key(key_name, idx);
    }

    /** Compute word counts on the local node and build a data frame. */
    void local_count() {
	printf("local count on %zu starting\n", this_node());
        DistributedDataFrame* words = store->waitAndGet(data_key);

        // Create rower, apply it with local_map, and get the results in a map
        WordCounter counter;
        words->local_map(counter);
        std::map<String*, int>* word_counts = counter.get_word_counts();

        // Convert map to distributed data frame
        Schema string_int_schema("SI");
        DistributedDataFrame partial_results(store, string_int_schema);
        Row word_count_pair(string_int_schema);

        // loop through map and add entries to 2 column DDF
        for (std::map<String*, int>::iterator it = word_counts->begin(); it != word_counts->end(); it++) {
            String* word = it->first;
            int count = it->second;

            word_count_pair.set(0, word);
            word_count_pair.set(1, count);

            partial_results.add_row(word_count_pair);
        }

        // Save DDF with partial results
        Key* partial_results_key = mk_key(this_node());
        store->put(partial_results_key, &partial_results);

        delete partial_results_key;
        delete words;
    }

    // Merge the results of Wordcounter from all other nodes onto this node
    void reduce() {
        std::map<String*, int> final_word_counts;

        // Loop through nodes in the network and collect their results
        for (size_t node_idx = 0; node_idx < num_nodes(); ++node_idx) {
            Key* partial_results_key = mk_key(node_idx);

            DistributedDataFrame* partial_results = store->waitAndGet(partial_results_key);

            merge(partial_results, &final_word_counts);

            delete partial_results;
            delete partial_results_key;
        }

        // Loop through final map and print counts
        Sys s;
        for (std::map<String*, int>::iterator it = final_word_counts.begin(); it != final_word_counts.end(); it++) {
            String* word = it->first;
            int count = it->second;

            s.p(word->c_str());
            s.p(": ");
            s.p(count);
            s.p("\n");
        }
    }

    // Adds the word counts stored in df to the given map
    void merge(DistributedDataFrame* df, std::map<String*, int>* map) {
        // Loop through DF and updates mappings in map
        for (size_t row_idx = 0; row_idx < df->nrows(); row_idx++) {
            String* word = df->get_string(0, row_idx);
            int new_count = df->get_int(1, row_idx);

            if (map->count(word)) {
                // count already exists
                int cur_count = map->find(word)->second;
                (*map)[word] = cur_count + new_count;
            } else {
                // new word, set count to what was in the DF
                (*map)[word] = new_count;
            }
        }
    }
};


int test_word_count() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = 8888;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, (char*)"127.0.0.1", 8000, master_ip, master_port);
    Store store2(1, (char*)"127.0.0.1", 8001, master_ip, master_port);
    Store store3(2, (char*)"127.0.0.1", 8002, master_ip, master_port);

    //data/wc_data.sor
    std::thread t1([](Store& store1) {
	WordCount wc("tests/test_data/words.sor", &store1);
    }, std::ref(store1));

    std::thread t2([](Store& store2) {
	WordCount wc(nullptr, &store2);
    }, std::ref(store2));

    std::thread t3([](Store& store3) {
	WordCount wc(nullptr, &store3);
    }, std::ref(store3));


    t1.join();
    t2.join();
    t3.join();

    // shutdown system
    s.shutdown();

    // wait for nodes to finish
    while (!store1.is_shutdown()) {
    }
    while (!store2.is_shutdown()) {
    }
    while (!store3.is_shutdown()) {
    }

    return true;
}

int main() {
    assert(test_word_count());
    printf("=================test_word_count PASSED if correct word counts printed above==================\n");
    return 0;
}
