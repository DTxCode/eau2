#pragma once
#include "../../src/store/store.cpp"
#include "../../src/store/dataframe/column.h"
#include <assert.h>
#include "../../src/store/network/master.h"

bool test_distributed_column() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = 6777;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, (char*)"127.0.0.1", 6000, master_ip, master_port);

    DistributedIntColumn dist_intc(&store1);

    printf("here\n");

    for (size_t i = 0; i < 1000000; i++) {
        dist_intc.push_back((i % 17));
    }
    printf("here\n");

    int dummy = dist_intc.get(182322);

    return (dummy >= 0);
}

int main() {
    assert(test_distributed_column());
    printf("=========== test_dist_column PASSED =========\n");
    return 0;
}

