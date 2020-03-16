// lang:CwC
// Authors: Ryan Heminway (heminway.r@husky.neu.edu), David Tandetnik
#pragma once
#include "../../src/client/sorer.h"
#include "../../src/store/dataframe/dataframe.h"
#include "iostream"

// Simple test that reads in a sor file, gets a chunk of it as a dataframe, and then checks a cell of that dataframe
bool test_get() {
    size_t from = 0;
    size_t length = 1000;
    char* input_file_name = "tests/test_data/small.sor";

    // Immediately try to open file
    FILE* input_file = fopen(input_file_name, "r");
    if (input_file == nullptr) {
        exit_with_msg("Failed to open test file");
    }

    // Create Sorer to hand out dataframes from file
    Sorer s(input_file, from, length);
    ModifiedDataFrame* my_df = s.get_chunk_as_df(0, 2);

    char* actual = my_df->get_string(2, 2)->c_str();
    char* expected = "bye";

    assert(equal_strings(actual, expected));

    return 1;
}

int main() {
    assert(test_get());
    printf("========= test_get passed ============");
    return 0;
}
