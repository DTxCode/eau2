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
    df.add_column(b_c, nullptr); 
    df.add_column(s_c, nullptr);

    Serializer serial;

    char* serialized_df = serial.serialize_dataframe(&df);
    Sys s;
    //s.p(serialized_df);
    //s.p("\n");

    DataFrame* new_df = serial.deserialize_dataframe(serialized_df);
    
    //return (new_df->get_int(0, 2) == df.get_int(0,2));
    //return (new_df->get_string(1, 2)->equals(df.get_string(1,2)));
    return new_df->get_bool(1, 3);
}

// Test boolean serialization (newly added in Milestone 1)
bool test_bool_serialize() {
    bool b1 = true;
    bool b2 = false;
    Serializer serial;

    char* serialized_b1 = serial.serialize_bool(b1);
    char* serialized_b2 = serial.serialize_bool(b2);

    //Sys s;
    //s.p(serialized_b1);
    //s.p(serialized_b2);

    bool new_b1 = serial.deserialize_bool(serialized_b1);
    bool new_b2 = serial.deserialize_bool(serialized_b2);

    return (new_b1 && !new_b2);
}


int main() {
    assert(test_df_serialize());
    assert(test_bool_serialize());
    printf("========== test_serialize_df PASSED =============\n");
}
