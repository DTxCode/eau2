#pragma once
#include "../../src/store/network/master.h"
#include "../../src/store/store.cpp"
#include "../../src/client/sorer.h"
#include "../../src/store/dataframe/dataframe.h"
#include "iostream"

bool test_ddf_with_missings() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = 8778;
    Server serv(master_ip, master_port);
    serv.listen_for_clients();

    Store store(0, (char*)"127.0.0.1", 7080, master_ip, master_port);

    Schema schema("IFSB");
    
    DistributedDataFrame* my_df = new DistributedDataFrame(&store,schema);
    Row r(schema);


    String* s = new String("");
    assert(s->c_str());

    r.set(0, 5);
    r.set(1, (float)5.55);
    r.set(2, new String("yes"));
    r.set(3, true);

    my_df->add_row(r);
    
    r.set(0, 5);
    r.set(1, (float) 6.66);
    //r.set_missing(2);
    r.set(2, new String(""));
    r.set(3, false);

    my_df->add_row(r);
    
    r.set(0, 5);
    r.set(1, (float) 6.66);
    r.set(2, new String("ahoy"));
    r.set(3, false);
    
    my_df->add_row(r);

    printf("%s\n", my_df->get_string(2, 1)->c_str());

    DistributedStringColumn str_col(&store);
    str_col.push_back(new String(""));
    printf("%s\n", str_col.get(0)->c_str());
    str_col.push_back(new String("hello"));
    printf("%s\n", str_col.get(0)->c_str());

    assert(s->equals(my_df->get_string(2, 1)));

    // shutdown system
    serv.shutdown();

    // wait for nodes to finish
    while (!store.is_shutdown()) {
    }

    return true;
}

int main() {
    assert(test_ddf_with_missings());
    printf("========= PASSED ddf with missings ==========\n");
    return 0;
}

