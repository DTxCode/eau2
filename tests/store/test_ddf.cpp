#pragma once
#include <assert.h>

#include "../../src/store/dataframe/dataframe.h"
#include "../../src/store/network/master.h"
#include "../../src/store/store.cpp"

bool test_ddf_multi_column() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = 6543;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, (char*)"127.0.0.1", 6065, master_ip, master_port);
    Store store2(1, (char*)"127.0.0.1", 6064, master_ip, master_port);

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

    s.shutdown();
    while (!store1.is_shutdown()) {
    }
    while (!store2.is_shutdown()) {
    }

    return true;
}

int main() {
    assert(test_ddf_multi_column());
    printf("=========== test_distributed_int_column PASSED =========\n");
    return 0;
}
