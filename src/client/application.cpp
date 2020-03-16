#pragma once
#include "arguments.h"
#include "sorer.h"

// Application represents the top layer of the distributed system. It reads in data, sends it throughout the network,
// and then lets the user preform K/V operations on it.
class Application {
    Sorer* sorer;
    // KeyValStore = store;

   public:
    Application(FILE* input_file, size_t from, size_t length) {
        sorer = new Sorer(input_file, from, length);
        distribute_data();
        run_();
    }

    void distribute_data() {
        // int num_nodes = store.get_num_nodes();

        // for (size_t i = 0; i< num_nodes; i++) {
        //     ModifiedDataFrame* df = sorer->get_chunk_as_df(i, num_nodes);

        //     Key key = (key_name, i);

        //     store.put(key, serialize(df));
        // }
    }

    virtual void run_() {
    }
};

int main(int argc, char** argv) {
    Arguments args(argc, argv);

    Application app(args.input_file, args.from, args.length);

    app.run_();
}