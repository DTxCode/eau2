#pragma once
#include "../../src/store/dataframe/dataframe.h"
#include "../../src/store/serial.cpp"
#include "../../src/store/network/master.h"

// Test that dataframe serialization works as intended
/*bool test_df_serialize() {
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
}*/

// Test Key serialization (newly added in Milestone 3)
bool test_key_serialize() {
    Key k1((char*)"tester", 2);
    Key k2((char*)"ano ther",45);

    Serializer serial;

    char* serialized_k1 = serial.serialize_key(&k1);
    char* serialized_k2 = serial.serialize_key(&k2);

    //Sys s;
    //s.p(serialized_k1);
    //s.p(serialized_k2);
    
    Key* new_k1 = serial.deserialize_key(serialized_k1);
    Key* new_k2 = serial.deserialize_key(serialized_k2);

    bool ret_value = (k1.equals(new_k1) && k2.equals(new_k2));
    delete new_k1;
    delete new_k2;
    return ret_value;
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

bool test_bool_array_serialize() {
    size_t num_bools = 5;
    bool bools1[num_bools];
    for (size_t i = 0; i < num_bools; i++) {
        bools1[i] = true;
    }

    Serializer serial;

    Sys s;
    char* ser_bools1 = serial.serialize_bools(bools1, num_bools);

    //s.p(ser_bools1);

    bool* new_bools1 = serial.deserialize_bools(ser_bools1);
    bool ret_value = true;
    for (size_t i = 0; i < num_bools; i++) {
        ret_value = ret_value && new_bools1[i];
    }

    return ret_value;
}

bool test_int_array_serialize() {
    size_t num_ints = 5;
    int ints1[num_ints];
    for (size_t i = 0; i < num_ints; i++) {
        ints1[i] = 127;
    }

    Serializer serial;

    Sys s;
    char* ser_ints1 = serial.serialize_ints(ints1, num_ints);

    //s.p(ser_ints1);

    int* new_ints1 = serial.deserialize_ints(ser_ints1);
    bool ret_value = true;
    for (size_t i = 0; i < num_ints; i++) {
        ret_value = ret_value && (127 == new_ints1[i]);
    }

    return ret_value;
}

bool test_string_array_serialize() {
    size_t num_strings = 5;
    String* strings1[num_strings];
    for (size_t i = 0; i < num_strings; i++) {
        strings1[i] = new String("testing");;
    }

    Serializer serial;

    Sys s;
    char* ser_strings1 = serial.serialize_strings(strings1, num_strings);

    s.p(ser_strings1);

    String** new_strings1 = serial.deserialize_strings(ser_strings1);
    bool ret_value = true;
    for (size_t i = 0; i < num_strings; i++) {
        ret_value = ret_value && (strcmp(new_strings1[i]->c_str(), "testing") == 0);
    }

    return ret_value;
}

// Simple test, for now just seeing that it compiles and doesnt break
bool test_dist_col_serialize() {
    char* master_ip = "127.0.0.1";
    int master_port = 8888;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store(5, "127.0.0.1", 7000, master_ip, master_port);

    DistributedColumn* d_i = new DistributedIntColumn(&store);

    Serializer serial;
    char* ser_d_i = serial.serialize_dist_col(d_i);
    //printf(ser_d_i);
    //printf("\n");

    DistributedIntColumn* d_i2 = serial.deserialize_dist_int_col(ser_d_i, &store);
    
    // shutdown system
    s.shutdown();

    // wait for nodes to finish
    while (!store.is_shutdown()) {
    }

    return d_i2->size() == 0;
}

bool test_ddf_serialize() {
    char* master_ip = "127.0.0.1";
    int master_port = 8888;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store(5, "127.0.0.1", 7000, master_ip, master_port);

    DistributedColumn* d_i = new DistributedIntColumn(&store);

    for (int i = 0; i < 10000; i++) {
        d_i->push_back(i);
    }

    Schema empty;
    DistributedDataFrame* ddf = new DistributedDataFrame(&store, empty);
    ddf->add_column(d_i, nullptr);

    Serializer serial;
    char* ser_ddf = serial.serialize_distributed_dataframe(ddf);
    printf("%s\n", ser_ddf);

    // shutdown system
    s.shutdown();

    // wait for nodes to finish
    while (!store.is_shutdown()) {
    }

    return true;
}


int main() {
    assert(test_ddf_serialize());
    assert(test_dist_col_serialize());
    assert(test_string_array_serialize());  
    assert(test_int_array_serialize());
    printf("========= serialize_int_array PASSED =============\n");
    assert(test_bool_array_serialize());
    printf("========= serialize_bool_array PASSED =============\n");
    assert(test_key_serialize());
    printf("========= serialize_key PASSED =============\n");
    assert(test_bool_serialize());
    printf("========= serialize_bool PASSED =============\n");
    /*assert(test_df_serialize());
    printf("========== test_serialize_df PASSED =============\n");*/
}
