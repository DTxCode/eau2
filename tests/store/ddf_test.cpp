#include <assert.h>
#include "../../src/store/dataframe/dataframe.h"
#include "../../src/store/network/master.h"
#include "../../src/store/store.cpp"
#include "../test_utils.h"

bool test_ddf_multi_column() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = rand_port();
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, (char*)"127.0.0.1", rand_port(), master_ip, master_port);
    Store store2(1, (char*)"127.0.0.1", rand_port(), master_ip, master_port);

    DistributedIntColumn dist_intc(&store1);
    DistributedBoolColumn dist_boolc(&store2);
    DistributedFloatColumn dist_floatc(&store1);
    DistributedStringColumn dist_stringc(&store2);

    String str("hi");
    for (size_t i = 0; i < 2000; i++) {
        dist_intc.push_back((int)i);
        dist_boolc.push_back(true);
        dist_floatc.push_back((float)i);
        dist_stringc.push_back(&str);
    }

    assert(dist_intc.get(250) == 250);
    assert(dist_floatc.get(250) == (float)250);
    assert(dist_boolc.get(1250));
    assert(!dist_intc.is_missing_dist(100));

    dist_intc.set_missing_dist(1500, true);
    assert(dist_intc.is_missing_dist(1500));

    Schema empty_schema;
    DistributedDataFrame df(&store1, empty_schema);

    df.add_column(&dist_intc);
    df.add_column(&dist_boolc);
    df.add_column(&dist_floatc);
    df.add_column(&dist_stringc);

    df.set(0, 1001, 5);
    assert(5 == df.get_int(0, 1001));

    assert(!(df.is_missing(0, 100)));
    assert(df.is_missing(0, 1500));
    assert(df.get_bool(1, 1000));

    store1.is_done();
    store2.is_done();

    s.shutdown();
    while (!store1.is_shutdown()) {
    }
    while (!store2.is_shutdown()) {
    }

    return true;
}

bool test_ddf_with_missings() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = rand_port();
    Server serv(master_ip, master_port);
    serv.listen_for_clients();

    Store store(0, (char*)"127.0.0.1", rand_port(), master_ip, master_port);

    Schema schema("IFSB");
    
    DistributedDataFrame my_df(&store, schema);
    Row r(schema);

    String str("test");

    r.set_missing(0);
    r.set(1, (float)5.55);
    r.set(2, &str);
    r.set(3, true);

    my_df.add_row(r);
    
    r.set(0, 5);
    r.set_missing(1);
    r.set(2, &str);
    r.set(3, false);

    my_df.add_row(r);
    
    r.set(0, 5);
    r.set(1, (float) 6.66);
    r.set_missing(2);
    r.set(3, false);
    
    my_df.add_row(r);

    assert(my_df.is_missing(0, 0));
    assert(my_df.is_missing(1, 1));
    assert(my_df.is_missing(2, 2));

    store.is_done();
    // shutdown system
    serv.shutdown();

    // wait for nodes to finish
    while (!store.is_shutdown()) {
    }

    return true;
}


int main() {
    assert(test_ddf_multi_column());
    printf("=========== test_ddf_multi_column PASSED =========\n");
    assert(test_ddf_with_missings());
    printf("=========== test_ddf_with_missings PASSED =========\n");

    return 0;
}
