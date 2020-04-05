// lang::CwC
/* Authors: Ryan Heminway (heminway.r@husky.neu.edu)
*           David Tandetnik (tandetnik.da@husky.neu.edu) */
#pragma once
#include <iostream>
#include "serial.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../utils/array.h"
#include "../utils/helper.h"
#include "dataframe/dataframe.h"
#include "store.cpp"
#include "dataframe/schema.h"
#include "network/message.h"

// Serialize a DistributedDataFrame into char*
// Will create same format and contents as Serializing a normal DF, with a pre-pended
// string representing the serialized Store
// "[Serialized Schema]~[Serialized Column 0]~ ... ~[Serialized Column n-1]"
char* Serializer::serialize_distributed_dataframe(DistributedDataFrame* df) { 
    // Track total buffer size we need
    size_t total_str_size = 0;

    // Serialize schema
    char* schema_str = serialize_schema(&df->get_schema());
    total_str_size += strlen(schema_str);

    // Serialize all columns
    size_t cols = df->ncols();
    char** col_strs = new char*[cols];
    for (size_t i = 0; i < cols; i++) {
        col_strs[i] = serialize_dist_col(dynamic_cast<DistributedColumn*>(df->get_col_(i)));
        total_str_size += strlen(col_strs[i]) + 1;  // +1 for tilda below
    }

    char* serial_buffer = new char[total_str_size];

    // Copy schema and column strings into buffer
    strcpy(serial_buffer, schema_str);  
    strcat(serial_buffer, "~");

    // Add serialized column messages to buffer, delimeted by "~"
    for (size_t j = 0; j < cols; j++) {
        strcat(serial_buffer, col_strs[j]);
        // Do not append "~" after last column string
        if (j != cols - 1) {
            strcat(serial_buffer, "~");
        }
        delete[] col_strs[j];
    }
    delete[] col_strs;
    return serial_buffer;
}

// Deserialize a char* buffer into a DistributedDataFrame object
// Expects given msg to have the form:
// "[Serialized Schema]~[Serialized Dist_Column 0]~[...]~[Serialized Dist_Column n-1]"
DistributedDataFrame* Serializer::deserialize_distributed_dataframe(char* msg, Store* store) { 
    char* schema_token = strtok(msg, "~");
    char* columns_token = strtok(nullptr, "\0");
    Schema* schema = deserialize_schema(schema_token);

    // Create array of serialized cols (distinct strings for each col)
    char** serialized_cols = new char*[schema->width()];
    if (!(schema->width() > 0)) {
        return new DistributedDataFrame(store, *schema);
    }
    
    serialized_cols[0] = strtok(columns_token, "~");
    for (size_t i = 1; i < schema->width(); i++) {
        serialized_cols[i] = strtok(nullptr, "~");
    }

    Schema empty_schema;
    // Initialize empty dataframe
    DistributedDataFrame* df = new DistributedDataFrame(store, empty_schema);

    // Deserialize all serialized cols into real columns
    for (size_t i = 0; i < schema->width(); i++) {
        char type = schema->col_type(i);
        if (type == INT_TYPE) {
            df->add_column(deserialize_dist_int_col(serialized_cols[i], store));
        } else if (type == BOOL_TYPE) {
            df->add_column(deserialize_dist_bool_col(serialized_cols[i], store));
        } else if (type == FLOAT_TYPE) {
            df->add_column(deserialize_dist_float_col(serialized_cols[i], store));
        } else {
            df->add_column(deserialize_dist_string_col(serialized_cols[i], store));
        }
    }

    delete schema;
    return df;
}

// Serializes a Distributed Column
// Treats a DistColumn as a set of chunk Keys and a set of Missing keys
// As such, creates msg with format:
// "[Serialized length];[Serialized num_chunks];[Serialized chunk Key 1];[Serialized missing Key 1];...;[Serialized chunk key (num_chunks - 1)];[Serialized missing key (num_chunks - 1)]
char* Serializer::serialize_dist_col(DistributedColumn* col) {
    size_t num_keys = col->num_chunks;

    // Will have a char* for each value. Track them separately
    //  because we do not know their size
    char** chunk_key_strings = new char*[num_keys];
    char** missing_key_strings = new char*[num_keys];

    size_t i;
    size_t total_size = 0;

    char* ser_length = serialize_size_t(col->size());
    char* ser_num_chunks = serialize_size_t(col->num_chunks);

    total_size += strlen(ser_length);
    total_size += strlen(ser_num_chunks);

    // For all key-values, serialize and add to key string tracker arrays
    for (i = 0; i < num_keys; i++) {
        chunk_key_strings[i] = serialize_key(col->chunk_keys[i]);
        missing_key_strings[i] = serialize_key(col->missings_keys[i]);

        // Track (a slight over-estimate due to null terminators) total
        // size required for message
        total_size += strlen(chunk_key_strings[i]);
        total_size += strlen(missing_key_strings[i]);
    }

    // need space for null terminator and all semicolons
    total_size += 2*num_keys + 3;
    char* serial_buffer = new char[total_size];
    if (total_size == 1) {  // Empty case
        strcpy(serial_buffer, "\0");
    } else {
        strcpy(serial_buffer, ser_length);
        strcat(serial_buffer, ";");
        strcat(serial_buffer, ser_num_chunks);
        strcat(serial_buffer, ";");

        // Copy all key-strings in to one buffer
        strcat(serial_buffer, chunk_key_strings[0]);
        strcat(serial_buffer, ";"); 
        strcat(serial_buffer, missing_key_strings[0]);

        //delete[] chunk_key_strings[0];
        for (i = 1; i < num_keys; i++) {
            strcat(serial_buffer, ";"); 
            strcat(serial_buffer, chunk_key_strings[i]);
            strcat(serial_buffer, ";");  
            strcat(serial_buffer, missing_key_strings[i]);
            //delete[] chunk_key_strings[i];
        }
    }

    delete[] missing_key_strings;
    delete[] chunk_key_strings;
    return serial_buffer;
}

// Deserialize a char* msg into a DistributedColumn 
// Expects msg with format: 
// "[Serialized length];[Serialized num_chunks];[Serialized chunk Key 1];[Serialized missing Key 1];...;[Serialized chunk key (num_chunks - 1)];[Serialized missing key (num_chunks - 1)]
DistributedColumn* Serializer::deserialize_dist_col(char* msg, Store* store, char col_type) { 
    char* ser_length = strtok(msg, ";");
    char* ser_num_chunks = strtok(nullptr, ";");
    char* ser_keys = strtok(nullptr, "\0");

    size_t length = deserialize_size_t(ser_length);
    size_t num_chunks = deserialize_size_t(ser_num_chunks);

    Key** chunk_keys = new Key*[num_chunks];
    Key** missings_keys = new Key*[num_chunks];

    // num_chunks should never be 0... so we dont need to handle empty case

    char* chunk_tokens[num_chunks];
    char* missings_tokens[num_chunks];
    // Get all tokens first, because calling deserialize does weird things to token
    chunk_tokens[0] = strtok(ser_keys, ";");
    missings_tokens[0] = strtok(nullptr, ";");
    for (size_t i = 1; i < num_chunks; i++) {
        chunk_tokens[i] = strtok(nullptr, ";");
        missings_tokens[i] = strtok(nullptr, ";");
    }
    
    // Deserialize into chunk and missing keys
    for (size_t i = 0; i < num_chunks; i++) {
        chunk_keys[i] = deserialize_key(chunk_tokens[i]);
        missings_keys[i] = deserialize_key(missings_tokens[i]);
    }

    DistributedColumn* dc;

    if (col_type == INT_TYPE) {
        dc = new DistributedIntColumn(store, chunk_keys, missings_keys, length, num_chunks);
    } else if (col_type == BOOL_TYPE) {
        dc = new DistributedBoolColumn(store, chunk_keys, missings_keys, length, num_chunks);
    } else if (col_type == FLOAT_TYPE) {
        dc = new DistributedFloatColumn(store, chunk_keys, missings_keys, length, num_chunks);
    } else {
        dc = new DistributedStringColumn(store, chunk_keys, missings_keys, length, num_chunks);
    }

    return dc;
}

// Class specific deserialization methods for dist_ columns. 
// Uses generic deserialization for dist_ col.
DistributedIntColumn* Serializer::deserialize_dist_int_col(char* msg, Store* store) { 
    return static_cast<DistributedIntColumn*>(deserialize_dist_col(msg, store, INT_TYPE));
}
DistributedBoolColumn* Serializer::deserialize_dist_bool_col(char* msg, Store* store){
    return static_cast<DistributedBoolColumn*>(deserialize_dist_col(msg, store, BOOL_TYPE));
}
DistributedFloatColumn* Serializer::deserialize_dist_float_col(char* msg, Store* store){ 
    return static_cast<DistributedFloatColumn*>(deserialize_dist_col(msg, store, FLOAT_TYPE));
}
DistributedStringColumn* Serializer::deserialize_dist_string_col(char* msg, Store* store){ 
    return static_cast<DistributedStringColumn*>(deserialize_dist_col(msg, store, STRING_TYPE));
}
    
// Serialize a Message object
char* Serializer::serialize_message(Message* msg) {
    // Leverage built-in method on Message
    return msg->to_string();
}

// Deserialize message into Message object
// Leverage built-in Message constructor which works
// from its to_string() representation (used for serialization)
Message* Serializer::deserialize_message(char* msg) {
    Message* m = new Message(msg);  // Copies msg
    //delete[] msg;
    return m;
}

// Serialize an int to a character array
// Methodology attributed to Stack Overflow post:
// stackoverflow.com/questions/7140943
char* Serializer::serialize_int(int value) {
    char* data;
    // Do a fake write to check how much space we need
    size_t buf_size = snprintf(nullptr, 0, "%d", value) + 1;
    data = new char[buf_size];
    // Do a real write with proper amount of space
    snprintf(data, buf_size, "%d", value);
    return data;
}

// De-serialize a character array in to an integer value
int Serializer::deserialize_int(char* msg) {
    int data;
    sscanf(msg, "%d", &data);
    return data;
}

// Serialize an size_t (unsigned long) to a character array
char* Serializer::serialize_size_t(size_t value) {
    char* data;
    // Do a fake write to check how much space we need
    size_t buf_size = snprintf(nullptr, 0, "%zu", value) + 1;
    data = new char[buf_size];
    // Do a real write with proper amount of space
    snprintf(data, buf_size, "%zu", value);
    return data;
}

// De-serialize a character array in to an size_t value
size_t Serializer::deserialize_size_t(char* msg) {
    size_t data;
    sscanf(msg, "%zu", &data);
    return data;
}

// Serialize a float to a character array
char* Serializer::serialize_float(float value) {
    char* data;
    // Do a fake write to check how much space we need
    size_t buf_size = snprintf(nullptr, 0, "%f", value) + 1;
    data = new char[buf_size];
    // Do a real write with proper amount of space
    snprintf(data, buf_size, "%f", value);
    return data;
}

// De-serialize a character array to a float value
float Serializer::deserialize_float(char* msg) {
    float data;
    sscanf(msg, "%f", &data);
    return data;
}

// Serialize pointer String object to a character array
// Copying c_str representation of the String for safety
char* Serializer::serialize_string(String* value) {
    char* data;
    // Handle nullptr case --> return "\0"
    if (nullptr == value) {
        data = new char[1];
        data[0] = '\0';
        return data;
    }
    // Do a fake write to check how much space we need
    size_t buf_size = snprintf(nullptr, 0, "%s", value->c_str()) + 1;
    data = new char[buf_size];
    strcpy(data, value->c_str());
    data[buf_size - 1] = '\0';
    return data;
}

// De-serialize a character array to a string value
String* Serializer::deserialize_string(char* msg) {
    // Handle case of empty String
    if (msg == nullptr) {
        return new String("");
    }
    return new String(static_cast<const char*>(msg));
}

// Serialize pointer to Key* object
// Creates form "[serialized Key name],[serialized Key home_node]"
char* Serializer::serialize_key(Key* value) {
    char* data;
    // Handle nullptr case --> return "\0"
    if (nullptr == value) {
        data = new char[1];
        data[0] = '\0';
        return data;
    }
    // Do a fake write to check how much space we need
    size_t name_buf_size = snprintf(nullptr, 0, "%s", value->get_name()) + 1;
    char* serialized_home_node = serialize_size_t(value->get_home_node());
    size_t node_buf_size = snprintf(nullptr, 0, "%s", serialized_home_node) + 1;
    data = new char[name_buf_size + node_buf_size + 1];  // + 1 for comma
    strcpy(data, value->get_name());
    strcat(data, ",");
    strcat(data, serialized_home_node);
    delete[] serialized_home_node;
    return data;
}

// Deserialize a char* into a Key object
// Expects char* form of "[serialized Key name],[serialized Key home_node]"
Key* Serializer::deserialize_key(char* msg) {
    char* name_token = strtok(msg, ",");  // Get first val before ,
    char* node_token = strtok(nullptr, ",");
    size_t node_id = deserialize_size_t(node_token);
    return new Key(name_token, node_id);
}

// Serialize StringArray object
// Basically exact same as StringColumn, but some method names prevent
// abstracting over both
char* Serializer::serialize_string_array(StringArray* array) {
    size_t length = array->size();

    if (length == 0) {
        return nullptr;
    }

    // Will have a char* for each value. Track them separately
    //  because we do not know their size
    char** cell_strings = new char*[length];
    size_t i;
    size_t total_size = 0;
    // For all cell-values, serialize and add to cell_strings
    for (i = 0; i < length; i++) {
        cell_strings[i] = serialize_string(array->get(i));

        // Track (a slight over-estimate due to null terminators) total
        // size required for message
        total_size += strlen(cell_strings[i]);
    }

    // size of cell char*s + (length - 1) commas + 1 for null terminator
    total_size += length;
    char* serial_buffer = new char[total_size];

    // Copy all value-strings into one buffer,
    // starting with first value which doesn't need a comma
    strcpy(serial_buffer, cell_strings[0]);
    delete[] cell_strings[0];

    // Loop through any remaining values
    for (i = 1; i < length; i++) {
        strcat(serial_buffer, ",");  // CSV
        strcat(serial_buffer, cell_strings[i]);
        delete[] cell_strings[i];
    }

    delete[] cell_strings;
    return serial_buffer;
}

// Deserialize serialized message to a StringArray
StringArray* Serializer::deserialize_string_array(char* msg) {
    char* token;
    StringArray* fill_array = new StringArray();
    // Tokenize message
    token = strtok(msg, ",");
    // For all tokens in the message, deserialize them
    // and add them to the array via push_back
    while (token) {
        fill_array->push_back(deserialize_string(token));
        token = strtok(nullptr, ",");
    }
    return fill_array;
}

// Serialize a Schema object pointer to a char*
// returned message will have form
// [Serialized Column Types Array]
char* Serializer::serialize_schema(Schema* schema) {
    StringArray col_type_strs;

    // Convert char to string with null-terminator
    char* char_str = new char[2];
    char_str[1] = '\0';

    // Track all column types and names as Strings
    for (size_t i = 0; i < schema->width(); i++) {
        char_str[0] = schema->col_type(i);
        col_type_strs.push_back(new String(char_str));
    }

    // Leverage existing S/D methods for StringArray
    char* col_types = serialize_string_array(&col_type_strs);
    return col_types;
}

// Deserialize a char* msg into a Schema objects
// Expects char* msg format as given by serialize_schema
Schema* Serializer::deserialize_schema(char* msg) {
    StringArray* col_types = deserialize_string_array(msg);
    size_t col_count = col_types->size();
    Schema* fill_schema = new Schema();
    char* col_type_str;
    for (size_t i = 0; i < col_count; i++) {
        col_type_str = col_types->get(i)->c_str();
        fill_schema->add_column(col_type_str[0]);
    }
    return fill_schema;
}

// Serialize a bool to a char* message
// true gets serialized to "1"
// false gets serialized to "0"
char* Serializer::serialize_bool(bool value) {
    char* data;
    data = new char[2];
    // Cast boolean to int (0 or 1), and write to buffer
    snprintf(data, 2, "%d", (int)value);
    return data;
}

// Deserialize a boolean serialized message
// Expects a message in the form "1" or "0"
bool Serializer::deserialize_bool(char* msg) {
    int data;
    sscanf(msg, "%d", &data);
    // For clarity, translate 1 / 0 to true / false
    if (data) {
        return true;
    }
    return false;
}

// Serializer a c-array of bools into a char* formed
// [bool],[bool],[bool] ...
char* Serializer::serialize_bools(bool* bools, size_t num_values) {
    char* data;
    if (nullptr == bools) {
        data = new char[1];
        data[0] = '\0';
        return data;
    }

    size_t length = num_values;

    // 1 char per bool, 1 per comma, 1 for null-terminator
    size_t buf_size = (length * 2) + 1;

    data = new char[buf_size];
    strcpy(data, serialize_bool(bools[0]));

    for (size_t i = 1; i < length; i++) {
        strcat(data, ",");  // CSV
        strcat(data, serialize_bool(bools[i]));
    }
    return data;
}

char* Serializer::serialize_ints(int* ints, size_t num_values) {
    char* data;
    if (nullptr == ints) {
        data = new char[1];
        data[0] = '\0';
        return data;
    }

    size_t length = num_values;
    size_t buf_size = length + 1;
    char** int_tokens = new char*[length];
    for (size_t i = 0; i < length; i++) {
        int_tokens[i] = serialize_int(ints[i]);
        buf_size += strlen(int_tokens[i]);
    }

    data = new char[buf_size];
    strcpy(data, int_tokens[0]);

    for (size_t i = 1; i < length; i++) {
        strcat(data, ",");  // CSV
        strcat(data, int_tokens[i]);
        delete[] int_tokens[i];
    }
    delete[] int_tokens;
    return data;
}

char* Serializer::serialize_floats(float* floats, size_t num_values) {
    char* data;
    if (nullptr == floats) {
        data = new char[1];
        data[0] = '\0';
        return data;
    }

    size_t length = num_values;
    size_t buf_size = length + 1;
    char** float_tokens = new char*[length];
    for (size_t i = 0; i < length; i++) {
        float_tokens[i] = serialize_float(floats[i]);
        buf_size += strlen(float_tokens[i]);
    }

    data = new char[buf_size];
    strcpy(data, float_tokens[0]);

    for (size_t i = 1; i < length; i++) {
        strcat(data, ",");  // CSV
        strcat(data, float_tokens[i]);
        delete[] float_tokens[i];
    }
    delete[] float_tokens;
    return data;
}

char* Serializer::serialize_strings(String** strings, size_t num_values) {
    char* data;
    if (nullptr == strings) {
        data = new char[1];
        data[0] = '\0';
        return data;
    }

    size_t length = num_values;
    size_t buf_size = length + 1;
    char** string_tokens = new char*[length];
    for (size_t i = 0; i < length; i++) {
        string_tokens[i] = serialize_string(strings[i]);
        buf_size += strlen(string_tokens[i]);
    }

    data = new char[buf_size];
    strcpy(data, string_tokens[0]);

    for (size_t i = 1; i < length; i++) {
        strcat(data, ",");  // CSV
        strcat(data, string_tokens[i]);
        delete[] string_tokens[i];
    }
    delete[] string_tokens;
    return data;
}

// Deserializer a char* msg into a c-array of bools
bool* Serializer::deserialize_bools(char* msg) {
    bool bools_temp[strlen(msg)];  // Temporarily oversize the storage
    char* token;
    // Tokenize message
    token = strtok(msg, ",");
    size_t idx = 0;
    // For all tokens in the message, deserialize them based on type
    // and add them to the column via push_back
    while (token) {
        bools_temp[idx] = deserialize_bool(token);
        token = strtok(nullptr, ",");
        idx++;  // After token is created
    }

    bool* bools_final = new bool[idx];
    for (size_t i = 0; i < idx; i++) {
        bools_final[i] = bools_temp[i];
    }

    delete msg;
    return bools_final;
}

int* Serializer::deserialize_ints(char* msg) {
    int ints_temp[strlen(msg)];  // Temporarily oversize the storage
    char* token;
    // Tokenize message
    token = strtok(msg, ",");
    size_t idx = 0;
    // For all tokens in the message, deserialize them based on type
    // and add them to the column via push_back
    while (token) {
        ints_temp[idx] = deserialize_int(token);
        token = strtok(nullptr, ",");
        idx++;  // After token is created
    }

    int* ints_final = new int[idx];
    for (size_t i = 0; i < idx; i++) {
        ints_final[i] = ints_temp[i];
    }

    delete msg;
    return ints_final;
}

float* Serializer::deserialize_floats(char* msg) {
    float floats_temp[strlen(msg)];  // Temporarily oversize the storage
    char* token;
    // Tokenize message
    token = strtok(msg, ",");
    size_t idx = 0;
    // For all tokens in the message, deserialize them based on type
    // and add them to the column via push_back
    while (token) {
        floats_temp[idx] = deserialize_float(token);
        token = strtok(nullptr, ",");
        idx++;  // After token is created
    }

    float* floats_final = new float[idx];
    for (size_t i = 0; i < idx; i++) {
        floats_final[i] = floats_temp[i];
    }

    delete msg;
    return floats_final;
}

String** Serializer::deserialize_strings(char* msg) {
    Sys s;
    size_t num_strings = s.count_char(",", msg) + 1;

    if (num_strings == 0) return nullptr;

    String** strings = new String*[num_strings];

    char* token = strsep(&msg, ",");
    for (size_t i = 0; i < num_strings; i++) {
        strings[i] = deserialize_string(token);
        token = strsep(&msg, ",");
    }

    delete msg;
    return strings;
}

/* DEPRECRATED WITH ADDITION OF DISTRIBUTED_[DataFrame and Column] */

/*

// Serialize a given DataFrame object
// Serialized message will take the form:
// "[Serialized Schema]~[Serialized Column 0]; ... ;[Serialized Column n-1]"
char* Serializer::serialize_dataframe(DataFrame* df) {
    // Track total buffer size we need
    size_t total_str_size = 0;

    // Serialize schema
    char* schema_str = serialize_schema(&df->get_schema());
    total_str_size += strlen(schema_str);

    // Serialize all columns
    size_t cols = df->ncols();
    char** col_strs = new char*[cols];
    for (size_t i = 0; i < cols; i++) {
        // Serialize column based on schema
        Schema s = df->get_schema();
        col_strs[i] = serialize_col(df->get_col_(i), df->get_schema().col_type(i));
        total_str_size += strlen(col_strs[i]) + 1;  // +1 for semicolons below
    }

    char* serial_buffer = new char[total_str_size];

    // Copy schema and column strings into buffer
    strcpy(serial_buffer, schema_str);  //, strlen(schema_str));
    strcat(serial_buffer, "~");

    // Add serialized column messages to buffer, delimeted by ";"
    for (size_t j = 0; j < cols; j++) {
        strcat(serial_buffer, col_strs[j]);
        // Do not append ";" after last column string
        if (j != cols - 1) {
            strcat(serial_buffer, ";");
        }
        delete[] col_strs[j];
    }
    delete[] col_strs;
    return serial_buffer;
}

// Deserialize a char* buffer into a DataFrame object
DataFrame* Serializer::deserialize_dataframe(char* msg) {
    // Keep around a copy of the original message
    char* msg_copy = new char[strlen(msg)];
    strcpy(msg_copy, msg);

    // Tokenize message to get schema section
    char* token = strtok(msg, "~");

    // Deserialize calls strtok, which breaks any subsequent calls using this msg
    // Need to use copies of msg for further tokenizing
    Schema* schema = deserialize_schema(token);
    char** token_copies = new char*[schema->width()];

    Schema empty_schema;

    // Initialize empty dataframe
    DataFrame* df = new DataFrame(empty_schema);

    // Deserialize each column and add it to the dataframe
    for (size_t i = 0; i < schema->width(); i++) {
        // Create a new message copy for this tokenizing
        token_copies[i] = new char[strlen(msg_copy)];
        strcpy(token_copies[i], msg_copy);

        // Move token to be the column strings
        token = strtok(token_copies[i], "~");
        token = strtok(nullptr, "~");

        token = strtok(token, ";");
        // Move token to the correct column string
        for (size_t j = 0; j < i; j++) {
            token = strtok(nullptr, ";");
        }

        char type = schema->col_type(i);
        if (type == INT_TYPE) {
            df->add_column(deserialize_int_col(token));
        } else if (type == BOOL_TYPE) {
            df->add_column(deserialize_bool_col(token));
        } else if (type == FLOAT_TYPE) {
            df->add_column(deserialize_float_col(token));
        } else {
            df->add_column(deserialize_string_col(token));
        }
        //delete[] token_copies[i];
    }

    delete[] token_copies;
    delete[] msg_copy;
    //        delete[] msg;
    delete schema;
    return df;
}


// Generic serialization method for Column type
// Process is same for every column, but serialization method used for
// cell-type is dependent on the given 'type' parameter.
// Providing the wrong type parameter for the given Column results in
// undefined behavior.
// Serialized string format is "[value],[value], ... ,[value],[value]"
// All values, in serialized char* form, are separated by commas
char* Serializer::serialize_col(Column* col, char type) {
    size_t length = col->size();
    // Will have a char* for each value. Track them separately
    //  because we do not know their size
    char** cell_strings = new char*[length];
    size_t i;
    size_t total_size = 0;
    // For all cell-values, serialize and add to cell_strings based on
    // given type of col
    for (i = 0; i < length; i++) {
        // Using static_casts to avoid performance hits from casting
        // This relies on well-formed user input
        if (type == INT_TYPE) {
            IntColumn* typed_col = dynamic_cast<IntColumn*>(col);
            cell_strings[i] = serialize_int(typed_col->get(i));
        } else if (type == FLOAT_TYPE) {
            FloatColumn* typed_col = dynamic_cast<FloatColumn*>(col);
            cell_strings[i] = serialize_float(typed_col->get(i));
        } else if (type == STRING_TYPE) {
            StringColumn* typed_col = dynamic_cast<StringColumn*>(col);
            cell_strings[i] = serialize_string(typed_col->get(i));
        } else if (type == BOOL_TYPE) {
            BoolColumn* typed_col = dynamic_cast<BoolColumn*>(col);
            cell_strings[i] = serialize_bool(typed_col->get(i));
        }

        // Track (a slight over-estimate due to null terminators) total
        // size required for message
        total_size += strlen(cell_strings[i]);
    }
    // size of cell char*s + (length - 1) commas + 1 for null terminator
    total_size += length + 1;
    char* serial_buffer = new char[total_size];
    if (total_size == 1) {  // Empty case
        strcpy(serial_buffer, "\0");
    } else {
        // Copy all value-strings in to one buffer
        strcpy(serial_buffer, cell_strings[0]);
    }
    for (i = 1; i < length; i++) {
        strcat(serial_buffer, ",");  // CSV
        strcat(serial_buffer, cell_strings[i]);
        delete[] cell_strings[i];
    }
    delete[] cell_strings;
    return serial_buffer;
}

// Deserialize a Column serialized message
// Message expected to have form "[value],[value], ... ,[value],[value]"
// Use push_back method to add values to a new column, with the intention
//  of abstracting over implementation details of a column
// Params:
//  msg: char-array message to be deserialized
//  type: char representing type of Column to assume
//  fill_col: pointer to column to fill with values
void Serializer::deserialize_col(char* msg, char type, Column* fill_col) {
    char* token;
    // Tokenize message
    token = strtok(msg, ",");
    // For all tokens in the message, deserialize them based on type
    // and add them to the column via push_back
    while (token) {
        if (type == INT_TYPE) {
            fill_col->push_back(deserialize_int(token));
        } else if (type == FLOAT_TYPE) {
            fill_col->push_back(deserialize_float(token));
        } else if (type == STRING_TYPE) {
            fill_col->push_back(deserialize_string(token));
        } else if (type == BOOL_TYPE) {
            fill_col->push_back(deserialize_bool(token));
        }
        token = strtok(nullptr, ",");
    }
    delete msg;
}

// Serialize an IntColumn
char* Serializer::serialize_int_col(IntColumn* col) {
    return serialize_col(col, INT_TYPE);
}

// Deserialize IntColumn serialized message
IntColumn* Serializer::deserialize_int_col(char* msg) {
    IntColumn* i_c = new IntColumn();
    deserialize_col(msg, INT_TYPE, i_c);
    return i_c;
}

// Serialize a FloatColumn to a character array
char* Serializer::serialize_float_col(FloatColumn* col) {
    return serialize_col(col, FLOAT_TYPE);
}

// Deserialize FloatColumn serialized message
FloatColumn* Serializer::deserialize_float_col(char* msg) {
    FloatColumn* f_c = new FloatColumn();
    deserialize_col(msg, FLOAT_TYPE, f_c);
    return f_c;
}

// Serialize a StringColumn to a character array
char* Serializer::serialize_string_col(StringColumn* col) {
    return serialize_col(col, STRING_TYPE);
}

// Deserialize StringColumn serialized message
StringColumn* Serializer::deserialize_string_col(char* msg) {
    StringColumn* s_c = new StringColumn();
    deserialize_col(msg, STRING_TYPE, s_c);
    return s_c;
}

// Serialize boolean column
// Uses same format as other columns, see serialize_col
char* Serializer::serialize_bool_col(BoolColumn* col) {
    return serialize_col(col, BOOL_TYPE);
}

// Deserialize boolean column from char*
BoolColumn* Serializer::deserialize_bool_col(char* msg) {
    BoolColumn* b_c = new BoolColumn();
    deserialize_col(msg, BOOL_TYPE, b_c);
    return b_c;
}
*/
