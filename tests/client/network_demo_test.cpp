#pragma once
#include "../../src/store/store.cpp"
#include "../../src/client/application.cpp"
#include "../../src/store/network/master.h"

class Demo : public Application {
   public:
    Key main("main", 0);
    Key verify("verif", 0);
    Key check("ck", 0);

    Demo(Store* store) : Application(nullptr, 0, 0, store) {}

    void run_() override {
        switch (this_node()) {
            case 0:
                producer();
                break;
            case 1:
                counter();
                break;
            case 2:
                summarizer();
        }
    }

    void producer() {
        size_t SZ = 100 * 1000;
        float* vals = new float[SZ];
        float sum = 0;
        for (size_t i = 0; i < SZ; ++i) {
            vals[i] = i;
            sum += vals[i];
        }

        DataFrame::fromArray(&main, store, SZ, vals);
        DataFrame::fromScalar(&check, store, sum);
    }

    void counter() {
        DataFrame* v = store->waitAndGet(main);
        size_t sum = 0;
        for (size_t i = 0; i < 100 * 1000; ++i) {
            sum += v->get_float(0, i);
        }
        p("The sum is  ").pln(sum);
        DataFrame::fromScalar(&verify, store, sum);
    }

    void summarizer() {
        DataFrame* result = store->waitAndGet(verify);
        DataFrame* expected = store->waitAndGet(check);
        pln(expected->get_float(0, 0) == result->get_float(0, 0) ? "SUCCESS" : "FAILURE");
    }
};

int test_demo() {
    char* master_ip = "127.0.0.1";
    int master_port = 8888;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, "127.0.0.1", 8000, master_ip, master_port);
    Store store2(1, "127.0.0.1", 8001, master_ip, master_port);
    Store store3(2, "127.0.0.1", 8002, master_ip, master_port);

    Demo producer(&store1);
    Demo counter(&store2);
    Demo summarizer(&store3);

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
    assert(test_demo());
    printf("=================test_demo PASSED if SUCCESS printed above==================\n");
    return 0;
}
