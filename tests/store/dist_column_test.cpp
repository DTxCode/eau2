#pragma once
#include <assert.h>
#include "../../src/store/dataframe/column.h"
#include "../../src/store/network/master.h"
#include "../../src/store/store.cpp"

bool test_distributed_column() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = 6777;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, (char*)"127.0.0.1", 6000, master_ip, master_port);

    DistributedIntColumn dist_intc(&store1);

    for (size_t i = 0; i < 100000; i++) {
        dist_intc.push_back(i);
    }

    int val = dist_intc.get(125);

    assert(val == 125);

    return true;
}

int main() {
    assert(test_distributed_column());
    printf("=========== test_dist_column PASSED =========\n");
    return 0;
}
