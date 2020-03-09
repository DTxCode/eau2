// lang::CwC
// Authors: heminway.r@husky.neu.edu
//          tandetnik.da@husky.neu.edu
#pragma once
#include "column.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>


// Utility class which understands how to transform Classes and 
// data structures used in our DataFrame API to and from a serialized
// character array format. Each Class or data-structure to be serialized
// has its own set of methods which serialize and de-serialize that structure.
// All methods are expected to receive well-formed inputs. Behavior is 
// UNDEFINED if the given arguments to these methods do not align with the 
// expected types. For example, if a user calls "deserialize_int" and passes
// a character array describing a String, the result is undefined. 
class Serializer {
    public:

    // Serialize an IntColumn. Serialized char* for an int column is simply 
    // a message containing all the integers in the column, in order, separated
    // by commas
    char* serialize_int_col(IntColumn* col) {
        size_t length = col->length;
        // Will have a char* for each int
        char** int_strings = new char*[length];
        size_t i;
        size_t total_size = 0;
        for (i = 0; i < length; i++) {
            int_strings[i] = serialize_int(col->get(i));
            // TODO Total_size may be overestimating due to null-terminators
            total_size += strlen(int_strings[i]);
        }
        // size of integer char*s + (length - 1) commas + 1 for null terminator
        total_size += length;
        char* serial_buffer = new char[total_size];
        strcpy(serial_buffer, int_strings[0]);
        // TODO is there a way to combine the loops?
        for (i = 1; i < length; i++) {
            strcat(serial_buffer, ","); // CSV 
            strcat(serial_buffer, int_strings[i]);
            delete[] int_strings[i];
        }
        delete[] int_strings; 
        printf("Serialized char* for IntCol: \n");
        printf("%s\n", serial_buffer);
        return serial_buffer; 
    }

    // Deserialize IntColumn serialized message
    // Message expected to have form "int,int,int,int"
    IntColumn* deserialize_int_col(char* msg) {
        IntColumn* i_c = new IntColumn();
        char* token;
        token = strtok(msg, ",");
        int int_token;
        // Push_back all ints seen in the message
        while (token) {
            int_token = deserialize_int(token);
            i_c->push_back(int_token);
            token = strtok(nullptr, ",");
        }
        return i_c;
    }

    // Serialize an int to a character array
    // Methodology attributed to Stack Overflow post:
    // stackoverflow.com/questions/7140943
    char* serialize_int(int value) {
        char* data;
        // Do a fake write to check how much space we need
        size_t buf_size = snprintf(nullptr, 0, "%d", value) + 1; 
        data = new char[buf_size];
        // Do a real write with proper amount of space
        snprintf(data, buf_size, "%d", value);
        return data;
    }

    // De-serialize a character array in to an integer value
    int deserialize_int(char* msg) {
        int data;
        sscanf(msg, "%d", &data);
        return data; 
    }

    // Serialize a float to a character array
    char* serialize_float(float value) {
        char* data;
        // Do a fake write to check how much space we need
        size_t buf_size = snprintf(nullptr, 0, "%f", value) + 1; 
        data = new char[buf_size];
        // Do a real write with proper amount of space
        snprintf(data, buf_size, "%f", value);
        return data;
    }

    // De-serialize a character array to a float value
    float deserialize_float(char* msg) {
        float data;
        sscanf(msg, "%f", &data);
        printf("Got float: %f", data);
        return data;
    }


};
