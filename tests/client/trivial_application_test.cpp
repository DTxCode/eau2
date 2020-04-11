#pragma once
#include "../../src/store/store.cpp"
#include "../../src/client/application.h"
#include "../../src/store/network/master.h"

// Trivial Application for testing
class Trivial : public Application {
   public:
    Trivial(Store* store) : Application(store) {}

    void run_() {
        size_t SZ = 1000 * 1000;
        float* vals = new float[SZ];
        float sum = 0;

        for (size_t i = 0; i < SZ; ++i) {
            vals[i] = i;
            sum += i;
        }

        Key key("triv", 0);
        DataFrame* df = DataFrame::fromArray(&key, store, SZ, vals);

        assert(df->get_float(0, 1) == 1);

        DataFrame* df2 = store->get(&key);

        for (size_t i = 0; i < SZ; ++i) {
            sum -= df2->get_float(0, i);
        }

        assert(sum == 0);

        delete[] vals;
        delete df;
        delete df2;
    }
};

// Tests putting/getting a DF from the local store
int test_trivial() {
    char* master_ip = "127.0.0.1";
    int master_port = 8888;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store(0, "127.0.0.1", 8000, master_ip, master_port);

    Trivial test(&store);
    test.run_();

    // shutdown system
    s.shutdown();

    // wait for nodes to finish
    while (!store.is_shutdown()) {
    }

    return 1;
}

int main() {
    assert(test_trivial());
    printf("=================trivial_application_test:test_trivial PASSED==================\n");
    return 0;
}
