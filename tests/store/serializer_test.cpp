#include "../../src/store/dataframe/dataframe.h"
#include "../../src/store/serial.cpp"
#include "../../src/store/network/master.h"
#include "../test_utils.h"

// Test Key serialization (newly added in Milestone 3)
bool test_key_serialize() {
    Key k1((char*)"tester", 2);
    Key k2((char*)"ano ther",45);

    Serializer serial;

    char* serialized_k1 = serial.serialize_key(&k1);
    char* serialized_k2 = serial.serialize_key(&k2);
    
    Key* new_k1 = serial.deserialize_key(serialized_k1);
    Key* new_k2 = serial.deserialize_key(serialized_k2);

    assert(k1.equals(new_k1));
    assert(k2.equals(new_k2));

    delete[] serialized_k1;
    delete[] serialized_k2;
    delete new_k1;
    delete new_k2;

    return true;
}

// Test boolean serialization (newly added in Milestone 1)
bool test_bool_serialize() {
    bool b1 = true;
    bool b2 = false;
    Serializer serial;

    char* serialized_b1 = serial.serialize_bool(b1);
    char* serialized_b2 = serial.serialize_bool(b2);

    bool new_b1 = serial.deserialize_bool(serialized_b1);
    bool new_b2 = serial.deserialize_bool(serialized_b2);

    delete[] serialized_b1;
    delete[] serialized_b2;

    return (new_b1 && !new_b2);
}

bool test_bool_array_serialize() {
    size_t num_bools = 5;
    bool bools1[num_bools];
    for (size_t i = 0; i < num_bools; i++) {
        bools1[i] = true;
    }

    Serializer serial;

    char* ser_bools1 = serial.serialize_bools(bools1, num_bools);
    bool* new_bools1 = serial.deserialize_bools(ser_bools1);
    
    bool ret_value = true;
    for (size_t i = 0; i < num_bools; i++) {
        ret_value = ret_value && new_bools1[i];
    }

    delete[] ser_bools1;

    return ret_value;
}

bool test_int_array_serialize() {
    size_t num_ints = 5;
    int ints1[num_ints];
    for (size_t i = 0; i < num_ints; i++) {
        ints1[i] = 127;
    }

    Serializer serial;

    char* ser_ints1 = serial.serialize_ints(ints1, num_ints);
    int* new_ints1 = serial.deserialize_ints(ser_ints1);

    bool ret_value = true;
    for (size_t i = 0; i < num_ints; i++) {
        ret_value = ret_value && (127 == new_ints1[i]);
    }

    delete[] ser_ints1;

    return ret_value;
}

bool test_string_array_serialize() {
    size_t num_strings = 5;
    String* strings[num_strings];
    String s("hi");

    for (size_t i = 0; i < num_strings; i++) {
        strings[i] = &s;
    }

    Serializer serial;

    char* ser_strings = serial.serialize_strings(strings, num_strings);

    String** new_strings = serial.deserialize_strings(ser_strings);

    assert(strings[0]->equals(new_strings[0]));
    assert(strings[1]->equals(new_strings[1]));
    assert(strings[2]->equals(new_strings[2]));
    assert(strings[3]->equals(new_strings[3]));
    assert(strings[4]->equals(new_strings[4]));

    delete[] ser_strings;
    delete new_strings[0];
    delete new_strings[1];
    delete new_strings[2];
    delete new_strings[3];
    delete new_strings[4];
    delete[] new_strings;

    return true;
}

// Simple test, for now just seeing that it compiles and doesnt break
bool test_dist_col_serialize() {
    char* master_ip = (char*) "127.0.0.1";
    int master_port = rand_port();
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store(0, (char*) "127.0.0.1", rand_port(), master_ip, master_port);

    DistributedStringColumn d_s(&store);
    String str("test");
    for (size_t i = 0; i < 2500; i++) {
        d_s.push_back(&str);
    }

    Serializer serial;
    char* ser_d_s = serial.serialize_dist_col(&d_s);

    DistributedStringColumn* d_s2 = serial.deserialize_dist_string_col(ser_d_s, &store);
    
    for (size_t i = 0; i < 2500; i++) {
        assert(d_s2->get(i)->equals(&str));
        assert(!d_s2->is_missing_dist(i));
    }
    
    // shutdown system
    s.shutdown();

    // wait for nodes to finish
    while (!store.is_shutdown()) {
    }

    delete d_s2;
    delete[] ser_d_s;

    return true;
}

bool test_ddf_serialize() {
    char* master_ip = (char*) "127.0.0.1";
    int master_port = rand_port();
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store(0, (char*)"127.0.0.1", rand_port(), master_ip, master_port);

    DistributedIntColumn d_i(&store);
    DistributedFloatColumn d_f(&store);
    DistributedStringColumn d_s(&store);

    String str("hi");
    for (int i = 0; i < 15; i++) {
        d_i.push_back(i);
        d_s.push_back(&str);
        d_f.push_back((float) 5.5);

        assert(!d_i.is_missing_dist(i));
        assert(!d_s.is_missing_dist(i));
        assert(!d_f.is_missing_dist(i));
    }

    Schema empty;
    DistributedDataFrame ddf(&store, empty);
    ddf.add_column(&d_i);
    ddf.add_column(&d_s);
    ddf.add_column(&d_f);
    
    for (size_t i = 0; i < ddf.nrows(); i++) {
        assert(!ddf.is_missing(0, i));
        assert(!ddf.is_missing(1, i));
        assert(!ddf.is_missing(2, i));
    }

    Serializer serial;
    char* ser_ddf = serial.serialize_distributed_dataframe(&ddf);

    DistributedDataFrame* new_ddf = serial.deserialize_distributed_dataframe(ser_ddf, &store);

    assert(new_ddf->get_int(0, 6) == 6);
    assert(new_ddf->get_string(1, 6)->equals(&str));

    // shutdown system
    s.shutdown();

    // wait for nodes to finish
    while (!store.is_shutdown()) {
    }

    delete[] ser_ddf;
    delete new_ddf;

    return true;
}

bool test_schema_serialize() {
    Schema scm("IFBS");

    Serializer ser;
    char* ser_scm = ser.serialize_schema(&scm);

    Schema* new_scm = ser.deserialize_schema(ser_scm);

    Schema scm_tst;
    assert(0 == scm_tst.length());
    assert(0 == new_scm->length());
    assert('I' == new_scm->col_type(0));
    assert('S' == new_scm->col_type(3));

    delete[] ser_scm;
    delete new_scm;

    return true;
}


int main() {
    assert(test_dist_col_serialize());
    printf("========= serialize_dist_col PASSED =============\n");
    assert(test_ddf_serialize());
    printf("========= serialize_ddf PASSED =============\n");
    assert(test_schema_serialize());
    printf("========== serialize schema PASSED ============\n");
    assert(test_string_array_serialize());  
    printf("========= serialize_string_array PASSED =============\n");
    assert(test_int_array_serialize());
    printf("========= serialize_int_array PASSED =============\n");
    assert(test_bool_array_serialize());
    printf("========= serialize_bool_array PASSED =============\n");
    assert(test_key_serialize());
    printf("========= serialize_key PASSED =============\n");
    assert(test_bool_serialize());
    printf("========= serialize_bool PASSED =============\n");
}
