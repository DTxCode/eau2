/* Authors: Ryan Heminway (heminway.r@husky.neu.edu)
*           David Tandetnik (tandetnik.da@husky.neu.edu) */
#pragma once
#include "../store/store.h"
#include "arguments.h"
#include "sorer.h"

// Application represents the top layer of the distributed system. It reads in data, sends it throughout the network,
// and then lets the user preform K/V operations on it.
class Application {
   public:
    Sorer* sorer;
    Store* store;

    Application(FILE* input_file, size_t from, size_t length, Store* store) {
        this->sorer = nullptr;
        if (input_file) {
            sorer = new Sorer(input_file, from, length);
            // distribute_data_();
        }

        this->store = store;
    }

    ~Application() {
        if (sorer) {
            delete sorer;
        }
    }

    size_t this_node() {
        return store->this_node();
    }

    size_t num_nodes() {
        return store->num_nodes();
    }

    // // Distributes data from sorer in even chunks across all nodes in the network
    // void distribute_data_() {
    //     int num_nodes = store->num_nodes();

    //     for (size_t i = 0; i < num_nodes; i++) {
    //         DataFrame* df = sorer->get_chunk_as_df(i, num_nodes);

    //         Key* key = new Key("", i);  // TODO what are the keys here?

    //         store->put(key, df);
    //     }
    // }

    // OVERRIDE
    // Called on application start
    virtual void run_() = 0;
};

// int main(int argc, char** argv) {
//     Arguments args(argc, argv);

//     Application app(args.input_file, args.from, args.length);

//     app.run_();
// }
