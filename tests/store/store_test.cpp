#include "../../src/store/store.cpp"
#include <assert.h>
#include "../../src/store/network/master.h"

// Tests creating a server and a store, and then putting/getting from the same store
bool test_simple_put() {
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
    String strings[2] = {s1, s2};

    // put in store and then get it out
    store.put_(&k1, bools);
    store.put_(&k2, ints);
    store.put_(&k3, floats);
    store.put_(&k4, strings);

    bool* bools2 = store.get_bool_array_(&k1);
    int* ints2 = store.get_int_array_(&k2);
    float* floats2 = store.get_float_array_(&k3);
    String* strings2 = store.get_string_array_(&k4);

    assert(bools[1] == bools2[1]);
    assert(ints[1] == ints2[1]);
    assert(floats[1] == floats2[1]);
    assert(strings[1].equals(&strings2[1]));

    delete[] strings2;

    // shutdown system
    s.shutdown();

    // wait for nodes to finish
    while (!store.is_shutdown()) {
    }

    return true;
}

// // Tests creating a server and two stores, and then having one store put a dataframe on the other store.
// bool test_network_put() {
//     char* master_ip = (char*)"127.0.0.1";
//     int master_port = 7777;
//     Server s(master_ip, master_port);
//     s.listen_for_clients();

//     Store store1(0, (char*)"127.0.0.1", 7000, master_ip, master_port);

//     Store store2(1, (char*)"127.0.0.1", 7001, master_ip, master_port);

//     // Send PUT request from store1 to store2
//     Key k((char*)"key", 1);  // key to store on another node

//     IntColumn* i_c = new IntColumn(4, 1, 2, 3, 5);
//     Schema empty_schema;
//     DataFrame df(empty_schema);
//     df.add_column(i_c, nullptr);

//     // put on first node, and then get from second node
//     store1.put_(&k, &df);
//     DataFrame* df2 = store2.get_(&k);

//     assert(df.get_int(0, 0) == df2->get_int(0, 0));
//     assert(df.get_int(0, 1) == df2->get_int(0, 1));
//     assert(df.get_int(0, 2) == df2->get_int(0, 2));
//     assert(df.get_int(0, 3) == df2->get_int(0, 3));
//     assert(df.get_int(0, 4) == df2->get_int(0, 4));

//     // shutdown system
//     s.shutdown();

//     // wait for nodes to finish
//     while (!store1.is_shutdown()) {
//     }
//     while (!store2.is_shutdown()) {
//     }

//     return true;
// }

// // Tests creating a server and two stores, and then having one store put a dataframe on the other store
// // and then also get it from the other store.
// bool test_network_get() {
//     char* master_ip = (char*)"127.0.0.1";
//     int master_port = 6777;
//     Server s(master_ip, master_port);
//     s.listen_for_clients();

//     Store store1(0, (char*)"127.0.0.1", 6000, master_ip, master_port);

//     Store store2(1, (char*)"127.0.0.1", 6001, master_ip, master_port);

//     // Send PUT request from store1 to store2
//     Key k((char*)"key", 1);  // key to store on another node

//     IntColumn* i_c = new IntColumn(4, 1, 2, 3, 5);
//     Schema empty_schema;
//     DataFrame df(empty_schema);
//     df.add_column(i_c, nullptr);

//     // put on first node, and then get from second node
//     store1.put_(&k, &df);
//     DataFrame* df2 = store1.get_(&k);

//     assert(df.get_int(0, 0) == df2->get_int(0, 0));
//     assert(df.get_int(0, 1) == df2->get_int(0, 1));
//     assert(df.get_int(0, 2) == df2->get_int(0, 2));
//     assert(df.get_int(0, 3) == df2->get_int(0, 3));
//     assert(df.get_int(0, 4) == df2->get_int(0, 4));

//     // shutdown system
//     s.shutdown();

//     // wait for nodes to finish
//     while (!store1.is_shutdown()) {
//     }
//     while (!store2.is_shutdown()) {
//     }

//     return true;
// }

// // Tests creating a server and two stores, and then having one store put a dataframe on the other store
// // and then also get it from the other store using getAndWait
// bool test_network_getAndWait() {
//     char* master_ip = (char*)"127.0.0.1";
//     int master_port = 6777;
//     Server s(master_ip, master_port);
//     s.listen_for_clients();

//     Store store1(0, (char*)"127.0.0.1", 6000, master_ip, master_port);

//     Store store2(1, (char*)"127.0.0.1", 6001, master_ip, master_port);

//     // Send PUT request from store1 to store2
//     Key k((char*)"key", 1);  // key to store on another node

//     IntColumn* i_c = new IntColumn(4, 1, 2, 3, 5);
//     Schema empty_schema;
//     DataFrame df(empty_schema);
//     df.add_column(i_c, nullptr);

//     // put on first node, and then get from second node
//     store1.put_(&k, &df);
//     DataFrame* df2 = store1.getAndWait_(&k);

//     assert(df.get_int(0, 0) == df2->get_int(0, 0));
//     assert(df.get_int(0, 1) == df2->get_int(0, 1));
//     assert(df.get_int(0, 2) == df2->get_int(0, 2));
//     assert(df.get_int(0, 3) == df2->get_int(0, 3));
//     assert(df.get_int(0, 4) == df2->get_int(0, 4));

//     // shutdown system
//     s.shutdown();

//     // wait for nodes to finish
//     while (!store1.is_shutdown()) {
//     }
//     while (!store2.is_shutdown()) {
//     }

//     return true;
// }

int main() {
    assert(test_simple_put());
    printf("========== test_simple_put PASSED =============\n");
    // assert(test_network_put());
    // printf("========== test_network_put PASSED =============\n");
    // assert(test_network_get());
    // printf("========== test_network_get PASSED =============\n");
    // assert(test_network_getAndWait());
    // printf("========== test_network_getAndWait PASSED =============\n");
}
