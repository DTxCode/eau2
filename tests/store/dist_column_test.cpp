#include <assert.h>
#include "../../src/store/dataframe/column.h"
#include "../../src/store/network/master.h"
#include "../../src/store/store.cpp"

bool test_distributed_int_column() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = 6777;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, (char*)"127.0.0.1", 6000, master_ip, master_port);
    Store store2(1, (char*)"127.0.0.1", 6001, master_ip, master_port);

    DistributedIntColumn dist_intc(&store1);

    for (size_t i = 0; i < 100; i++) {
        dist_intc.push_back(i);
    }

    int val = dist_intc.get(25);

    s.shutdown();
    while (!store1.is_shutdown()) {
    }
    while (!store2.is_shutdown()) {
    }

    assert(val == 25);

    return true;
}

bool test_distributed_bool_column() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = 5777;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, (char*)"127.0.0.1", 5000, master_ip, master_port);
    Store store2(1, (char*)"127.0.0.1", 5001, master_ip, master_port);

    DistributedBoolColumn dist_boolc(&store1);

    for (size_t i = 0; i < 100; i++) {
        dist_boolc.push_back(true);
    }

    bool val = dist_boolc.get(25);

    s.shutdown();
    while (!store1.is_shutdown()) {
    }
    while (!store2.is_shutdown()) {
    }

    assert(val);

    return true;
}

bool test_distributed_float_column() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = 4777;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, (char*)"127.0.0.1", 4000, master_ip, master_port);
    Store store2(1, (char*)"127.0.0.1", 4001, master_ip, master_port);

    DistributedFloatColumn dist_floatc(&store1);

    for (size_t i = 0; i < 10000; i++) {
        dist_floatc.push_back((float)i);
    }

    float val = dist_floatc.get(25);

    s.shutdown();
    while (!store1.is_shutdown()) {
    }
    while (!store2.is_shutdown()) {
    }

    assert(val == 25);

    return true;
}

bool test_distributed_string_column() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = 3777;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, (char*)"127.0.0.1", 3000, master_ip, master_port);
    Store store2(1, (char*)"127.0.0.1", 3001, master_ip, master_port);

    DistributedStringColumn dist_stringc(&store1);

    String str("hi");
    for (size_t i = 0; i < 100; i++) {
        dist_stringc.push_back(&str);
    }

    String* val = dist_stringc.get(25);

    s.shutdown();
    while (!store1.is_shutdown()) {
    }
    while (!store2.is_shutdown()) {
    }

    assert(str.equals(val));

    return true;
}

int main() {
    assert(test_distributed_int_column());
    printf("=========== test_distributed_int_column PASSED =========\n");
    assert(test_distributed_bool_column());
    printf("=========== test_distributed_bool_column PASSED =========\n");
    assert(test_distributed_float_column());
    printf("=========== test_distributed_float_column PASSED =========\n");
    assert(test_distributed_string_column());
    printf("=========== test_distributed_string_column PASSED =========\n");
    return 0;
}
