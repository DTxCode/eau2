#pragma once
#include "../../src/store/store.cpp"
#include <assert.h>
#include "../../src/store/network/master.h"

// Tests creating a server and a store, and then putting/getting from the same store
bool test_simple_put_get() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = 8888;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store(0, (char*)"127.0.0.1", 8000, master_ip, master_port);

    // Create keys
    Key k1((char*)"bools", 0);
    Key k2((char*)"ints", 0);
    Key k3((char*)"floats", 0);
    Key k4((char*)"strings", 0);

    // Create vals
    bool bools[2] = {true, true};
    int ints[2] = {1, 2};
    float floats[2] = {(float)1.0, (float)2.0};
    String s1("hi"), s2("bye");
    String* strings[2] = {&s1, &s2};

    // put in store and then get it out
    store.put_(&k1, bools, 2);
    store.put_(&k2, ints, 2);
    store.put_(&k3, floats, 2);
    store.put_(&k4, strings, 2);

    bool* bools2 = store.get_bool_array_(&k1);
    int* ints2 = store.get_int_array_(&k2);
    float* floats2 = store.get_float_array_(&k3);
    String* strings2 = store.get_string_array_(&k4);

    assert(bools[1] == bools2[1]);
    assert(ints[1] == ints2[1]);
    assert(floats[1] == floats2[1]);
    assert(strings[1].equals(&strings2[1]));

    //delete strings2[0];
    //delete strings2[1];

    // shutdown system
    s.shutdown();

    // wait for nodes to finish
    while (!store.is_shutdown()) {
    }

    return true;
}

// Tests creating a server and two stores, and then having one store put/get a value to/from the other store.
bool test_network_put_get() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = 7777;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, (char*)"127.0.0.1", 7000, master_ip, master_port);

    Store store2(1, (char*)"127.0.0.1", 7001, master_ip, master_port);

    // Create keys
    Key k1((char*)"bools", 1);
    Key k2((char*)"ints", 1);
    Key k3((char*)"floats", 1);
    Key k4((char*)"strings", 1);

    // Create vals
    bool bools[2] = {true, true};
    int ints[2] = {1, 2};
    float floats[2] = {(float)1.0, (float)2.0};
    String s1("hi"), s2("bye");
    String* strings[2] = {&s1, &s2};

    // put in other store and then get it out
    store1.put_(&k1, bools, 2);
    store1.put_(&k2, ints, 2);
    store1.put_(&k3, floats, 2);
    store1.put_(&k4, strings, 2);

    bool* bools2 = store1.get_bool_array_(&k1);
    int* ints2 = store1.get_int_array_(&k2);
    float* floats2 = store1.get_float_array_(&k3);
    String* strings2 = store1.get_string_array_(&k4);

    assert(bools[1] == bools2[1]);
    assert(ints[1] == ints2[1]);
    assert(floats[1] == floats2[1]);
    assert(strings[1]->equals(strings2[1]));

    delete strings2[0];
    delete strings2[1];

    // shutdown system
    s.shutdown();

    // wait for nodes to finish
    while (!store1.is_shutdown()) {
    }
    while (!store2.is_shutdown()) {
    }

    return true;
}

// Test whether store can put and get a distributed data frame
bool test_network_distributed_df() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = 6777;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, (char*)"127.0.0.1", 6000, master_ip, master_port);

    Store store2(1, (char*)"127.0.0.1", 6001, master_ip, master_port);

    // key to store on another node
    Key k((char*)"key", 1);

    DistributedIntColumn* i_c = new DistributedIntColumn(4, 1, 2, 3, 5);
    Schema empty_schema;
    DistributedDataFrame df(empty_schema);
    df.add_column(i_c, nullptr);

    // Put and get the DDF to/from the other node
    store1.put(&k, &df);
    DistributedDataFrame* df2 = store1.get(&k);

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

// Test whether store can put and waitAndGet a distributed data frame
bool test_network_distributed_df_waitAndGet() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = 5777;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, (char*)"127.0.0.1", 5000, master_ip, master_port);

    Store store2(1, (char*)"127.0.0.1", 5001, master_ip, master_port);

    // key to store on another node
    Key k((char*)"key", 1);

    DistributedIntColumn* i_c = new DistributedIntColumn(4, 1, 2, 3, 5);
    Schema empty_schema;
    DistributedDataFrame df(empty_schema);
    df.add_column(i_c, nullptr);

    // Put and get the DDF to/from the other node
    store1.put(&k, &df);
    DistributedDataFrame* df2 = store1.waitAndGet(&k);

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
    assert(test_simple_put_get());
    printf("========== test_simple_put_get PASSED =============\n");
    assert(test_network_put_get());
    printf("========== test_network_put_get PASSED =============\n");
    assert(test_network_distributed_df());
    printf("========== test_network_distributed_df PASSED =============\n");
    assert(test_network_distributed_df_waitAndGet());
    printf("========== test_network_distributed_df_waitAndGet PASSED =============\n");
}
