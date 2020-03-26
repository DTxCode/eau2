#pragma once
#include <assert.h>
#include "../../src/store/network/master.h"
#include "../../src/store/store.h."

// Tests creating a server and a store, and then putting/getting from the same store
bool test_simple_put() {
    char* master_ip = "127.0.0.1";
    int master_port = 8888;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, "127.0.0.1", 7000, master_ip, master_port);

    // Create key and DF
    Key k("key", 0);

    IntColumn* i_c = new IntColumn(4, 1, 2, 3, 5);
    Schema empty_schema;
    DataFrame df(empty_schema);
    df.add_column(i_c, nullptr);

    // put in store and then get it out
    store1.put_(&k, &df);
    DataFrame* df2 = store1.get_(&k);

    assert(df.get_int(0, 0) == df2->get_int(0, 0));
    assert(df.get_int(0, 1) == df2->get_int(0, 1));
    assert(df.get_int(0, 2) == df2->get_int(0, 2));
    assert(df.get_int(0, 3) == df2->get_int(0, 3));
    assert(df.get_int(0, 4) == df2->get_int(0, 4));

    // shutdown system
    s.shutdown();

    // wait for nodes to finish
    while (!store1.is_shutdown()) {
    }

    return true;
}

// Tests creating a server and two stores, and then having one store put a dataframe on the other store.
bool test_network_put() {
    char* master_ip = "127.0.0.1";
    int master_port = 8888;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, "127.0.0.1", 7000, master_ip, master_port);

    Store store2(1, "127.0.0.1", 7001, master_ip, master_port);

    // Send PUT request from store1 to store2
    Key k("key", 1);  // key to store on another node

    IntColumn* i_c = new IntColumn(4, 1, 2, 3, 5);
    Schema empty_schema;
    DataFrame df(empty_schema);
    df.add_column(i_c, nullptr);

    // put on first node, and then get from second node
    store1.put_(&k, &df);
    DataFrame* df2 = store2.get_(&k);

    assert(df.get_int(0, 0) == df2->get_int(0, 0));
    assert(df.get_int(0, 1) == df2->get_int(0, 1));
    assert(df.get_int(0, 2) == df2->get_int(0, 2));
    assert(df.get_int(0, 3) == df2->get_int(0, 3));
    assert(df.get_int(0, 4) == df2->get_int(0, 4));

    // shutdown system
    s.shutdown();

    // wait for nodes to finish
    while (!store1.is_shutdown()) {
    }
    while (!store2.is_shutdown()) {
    }

    return true;
}

int main() {
    assert(test_simple_put());
    printf("========== test_simple_put PASSED =============\n");
    assert(test_network_put());
    printf("========== test_network_put PASSED =============\n");
}
