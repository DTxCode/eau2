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
    Store* store;

    Application(Store* store) {
        this->store = store;
    }

    size_t this_node() {
        return store->this_node();
    }

    size_t num_nodes() {
        return store->num_nodes();
    }

    // OVERRIDE
    // Called on application start
    virtual void run_() = 0;

    // Called when application is created. Calls user extended functionality 
    //  in run_
    void run() { run_(); }
};
