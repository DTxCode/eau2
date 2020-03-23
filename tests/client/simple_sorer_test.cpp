// lang:CwC
// Authors: Ryan Heminway (heminway.r@husky.neu.edu), David Tandetnik
#pragma once
#include "../../src/client/sorer.h"
#include "../../src/store/dataframe/dataframe.h"
#include "iostream"

// Simple test that reads in a sor file, gets a chunk of it as a dataframe, and then checks a cell of that dataframe
bool test_sorer_without_missings() {
    size_t from = 0;
    size_t length = 0;
    char* input_file_name = "data/data.sor";

    // Immediately try to open file
    FILE* input_file = fopen(input_file_name, "r");
    if (input_file == nullptr) {
        exit_with_msg("Failed to open test file");
    }

    // Create Sorer to hand out dataframes from file
    Sorer s(input_file, from, length);
    DataFrame* my_df = s.get_chunk_as_df(0, 10);

    char* actual = my_df->get_string(2, 2)->c_str();
    char* expected = "bye";

    assert(equal_strings(actual, expected));

    return 1;
}

bool test_sorer_with_missings() {
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
    DataFrame* my_df = s.get_chunk_as_df(0, 10);

    char* actual = my_df->get_string(2, 2)->c_str();
    char* expected = "";

    assert(equal_strings(actual, expected));

    return 1;
}

int main() {
    assert(test_sorer_without_missings());
    assert(test_sorer_with_missings());
    printf("========= test_get passed ============");
    return 0;
}
