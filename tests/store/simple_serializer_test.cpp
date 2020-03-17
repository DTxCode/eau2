#pragma once
#include "../../src/store/dataframe/dataframe.h"
#include "../../src/store/serial.h"

// Test that dataframe serialization works as intended
bool test_df_serialize() {
    IntColumn* i_c = new IntColumn(4, 1, 2, 3, 5);
    BoolColumn* b_c = new BoolColumn(4, true, true, false, true);
    StringColumn* s_c = new StringColumn(4, new String("This"), new String("Is"),
        new String("My"), new String("Test"));

    Schema empty_schema;
    DataFrame df(empty_schema);
    df.add_column(i_c, nullptr);
    //df.add_column(b_c, nullptr); TODO finish bool serializing 
    df.add_column(s_c, nullptr);

    Serializer serial;

    char* serialized_df = serial.serialize_dataframe(&df);
    Sys s;
    s.p(serialized_df);
    s.p("\n");

    return true;
}

int main() {
    assert(test_df_serialize());
    printf("========== test_serialize_df PASSED =============\n");
}
