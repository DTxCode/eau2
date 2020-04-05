// lang:CwC
// Authors: Ryan Heminway (heminway.r@husky.neu.edu), David Tandetnik
#pragma once
#include "../../src/store/network/master.h"
#include "../../src/store/store.cpp"
#include "../../src/client/sorer.h"
#include "../../src/store/dataframe/dataframe.h"
#include "iostream"

// Simple test that reads in a sor file, gets it as a dataframe, and then checks a cell of that dataframe
bool test_sorer_without_missings() {
    String str("");
    assert(str.equals(new String("\0")));

    char* master_ip = (char*)"127.0.0.1";
    int master_port = 8778;
    Server serv(master_ip, master_port);
    serv.listen_for_clients();

    Store store(0, (char*)"127.0.0.1", 7080, master_ip, master_port);
    
    size_t from = 0;
    size_t length = 0;
    char* input_file_name = "data/data.sor";

    // Immediately try to open file
    FILE* input_file = fopen(input_file_name, "r");
    if (input_file == nullptr) {
        exit_with_msg("Failed to open test file");
    }
    printf("here\n");

    // Create Sorer to hand out dataframe from file
    Sorer s(input_file, from, length);
    DistributedDataFrame* my_df = s.get_dataframe(&store);
    
    printf("here\n");


    char* actual = my_df->get_string(2, 2)->c_str();
    char* expected = "bye";

    assert(equal_strings(actual, expected));
    
    // shutdown system
    serv.shutdown();

    // wait for nodes to finish
    while (!store.is_shutdown()) {
    }

    return 1;
}

bool test_sorer_with_missings() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = 8888;
    Server serv(master_ip, master_port);
    serv.listen_for_clients();

    Store store(0, (char*)"127.0.0.1", 8000, master_ip, master_port);
    
    size_t from = 0;
    size_t length = 0;
    char* input_file_name = "data/missing_data.sor";

    // Immediately try to open file
    FILE* input_file = fopen(input_file_name, "r");
    if (input_file == nullptr) {
        exit_with_msg("Failed to open test file");
    }

    // Create Sorer to hand out dataframes from file
    Sorer s(input_file, from, length);
    DistributedDataFrame* my_df = s.get_dataframe(&store);

    Sys sys;
    sys.p(my_df->get_string(2, 0)->c_str());
    sys.p("\n");
    sys.p(my_df->get_string(2, 1)->c_str());
    sys.p("\n");
    sys.p(my_df->get_string(2, 2)->c_str());
    sys.p("\n");
    sys.p(my_df->get_string(2, 3)->c_str());
    sys.p("\n");

    assert(my_df->get_string(2, 3)->equals(new String("1.23")));
    
    // shutdown system
    serv.shutdown();

    // wait for nodes to finish
    while (!store.is_shutdown()) {
    }

    return 1;
}

int main() {
    assert(test_sorer_without_missings());
    printf("========= test_without_missings passed ============");
    assert(test_sorer_with_missings());
    printf("========= test_with_missings passed ============");
    return 0;
}
