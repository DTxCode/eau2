// lang::CwC
// Authors: heminway.r@husky.neu.edu
//          tandetnik.da@husky.neu.edu
#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../utils/array.h"
#include "dataframe/column.h"
#include "dataframe/dataframe.h"
#include "dataframe/schema.h"
#include "network/message.h"

// Utility class which understands how to transform Classes and
// data structures used in our DataFrame API to and from a serialized
// character array format.
// - Each Class or data-structure to be serialized has its own set of
//   methods which serialize and de-serialize that structure.
//   All methods are expected to receive well-formed inputs. Behavior is
//   UNDEFINED if the given arguments to these methods do not align with the
//   expected types. For example, if a user calls "deserialize_int" and passes
//   a character array describing a String, the result is undefined.
// - When possible, this class tries to avoid serializing/deserializing based
//   on implementation details of the given object. For example, when creating
//   a new column from a serialized column message, we call push_back instead
//   of manually creating the internal data structure which holds the values.
// This class is intended to be sub-classed and methods are intended to be
// re-implemented for users who may have different ideas/requirements for
// serialization of these same types. Sub-classes can also maintain the
// design philosophy found here for any additional types not supported here.
class Serializer {
   public:
    // Serialize a given DataFrame object
    // Serialized message will take the form:
    // "[Serialized Schema];;[Serialized Column 0]; ... ;[Serialized Column n-1]"
    virtual char* serialize_dataframe(DataFrame* df) {
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
            col_strs[i] = serialize_col(df->get_col_(i), df->get_schema().col_type(i));
            total_str_size += strlen(col_strs[i]);
        }

        char* serial_buffer = new char[total_str_size];

        // Copy schema and column strings into buffer
        memcpy(serial_buffer, schema_str, strlen(schema_str));
        strcat(serial_buffer, ";;");

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
    virtual DataFrame* deserialize_dataframe(char* msg) {
        // Tokenize message
        char* token = strtok(msg, ";;");

        Schema* schema = deserialize_schema(token);

        Schema empty_schema;

        // Initialize
        DataFrame* df = new DataFrame(empty_schema);

        // Deserialize each column and add it to the dataframe
        for (size_t i = 0; i < schema->width(); i++) {
            token = strtok(nullptr, ";");
            char type = schema->col_type(i);
            if (type == INT_TYPE) {
                df->add_column(deserialize_int_col(token), nullptr);
            } else if (type == BOOL_TYPE) {
                df->add_column(deserialize_bool_col(token), nullptr);
            } else if (type == FLOAT_TYPE) {
                df->add_column(deserialize_float_col(token), nullptr);
            } else {
                df->add_column(deserialize_string_col(token), nullptr);
            }
        }

        delete schema;
        return df;
    }

    // Serialize a Message object
    virtual char* serialize_message(Message* msg) {
        // Leverage built-in method on Message
        return msg->to_string();
    }

    // Deserialize message into Message object
    // Leverage built-in Message constructor which works
    // from its to_string() representation (used for serialization)
    virtual Message* deserialize_message(char* msg) {
        return new Message(msg);
    }

    // Serialize an IntColumn
    virtual char* serialize_int_col(IntColumn* col) {
        return serialize_col(col, INT_TYPE);
    }

    // Deserialize IntColumn serialized message
    virtual IntColumn* deserialize_int_col(char* msg) {
        IntColumn* i_c = new IntColumn();
        deserialize_col(msg, INT_TYPE, i_c);
        return i_c;
    }

    // Serialize a FloatColumn to a character array
    virtual char* serialize_float_col(FloatColumn* col) {
        return serialize_col(col, FLOAT_TYPE);
    }

    // Deserialize FloatColumn serialized message
    virtual FloatColumn* deserialize_float_col(char* msg) {
        FloatColumn* f_c = new FloatColumn();
        deserialize_col(msg, FLOAT_TYPE, f_c);
        return f_c;
    }

    // Serialize a StringColumn to a character array
    virtual char* serialize_string_col(StringColumn* col) {
        return serialize_col(col, STRING_TYPE);
    }

    // Deserialize StringColumn serialized message
    virtual StringColumn* deserialize_string_col(char* msg) {
        StringColumn* s_c = new StringColumn();
        deserialize_col(msg, STRING_TYPE, s_c);
        return s_c;
    }

    // Serialize an int to a character array
    // Methodology attributed to Stack Overflow post:
    // stackoverflow.com/questions/7140943
    virtual char* serialize_int(int value) {
        char* data;
        // Do a fake write to check how much space we need
        size_t buf_size = snprintf(nullptr, 0, "%d", value) + 1;
        data = new char[buf_size];
        // Do a real write with proper amount of space
        snprintf(data, buf_size, "%d", value);
        return data;
    }

    // De-serialize a character array in to an integer value
    virtual int deserialize_int(char* msg) {
        int data;
        sscanf(msg, "%d", &data);
        return data;
    }

    // Serialize a float to a character array
    virtual char* serialize_float(float value) {
        char* data;
        // Do a fake write to check how much space we need
        size_t buf_size = snprintf(nullptr, 0, "%f", value) + 1;
        data = new char[buf_size];
        // Do a real write with proper amount of space
        snprintf(data, buf_size, "%f", value);
        return data;
    }

    // De-serialize a character array to a float value
    virtual float deserialize_float(char* msg) {
        float data;
        sscanf(msg, "%f", &data);
        return data;
    }

    // Serialize pointer String object to a character array
    // Copying c_str representation of the String for safety
    virtual char* serialize_string(String* value) {
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
        // Do a real write with proper amount of space
        snprintf(data, buf_size, "%s", value->c_str());
        return data;
    }

    // De-serialize a character array to a string value
    virtual String* deserialize_string(char* msg) {
        // Handle case of empty String
        if (strcmp(msg, "\0") == 0) {
            return nullptr;
        }
        return new String(static_cast<const char*>(msg));
    }

    // Generic serialization method for Column type
    // Process is same for every column, but serialization method used for
    // cell-type is dependent on the given 'type' parameter.
    // Providing the wrong type parameter for the given Column results in
    // undefined behavior.
    // Serialized string format is "[value],[value], ... ,[value],[value]"
    // All values, in serialized char* form, are separated by commas
    virtual char* serialize_col(Column* col, char type) {
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
                IntColumn* typed_col = static_cast<IntColumn*>(col);
                cell_strings[i] = serialize_int(typed_col->get(i));
            } else if (type == FLOAT_TYPE) {
                FloatColumn* typed_col = static_cast<FloatColumn*>(col);
                cell_strings[i] = serialize_float(typed_col->get(i));
            } else if (type == STRING_TYPE) {
                StringColumn* typed_col = static_cast<StringColumn*>(col);
                cell_strings[i] = serialize_string(typed_col->get(i));
            } else if (type == BOOL_TYPE) {
                BoolColumn* typed_col = static_cast<BoolColumn*>(col);
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
    virtual void deserialize_col(char* msg, char type, Column* fill_col) {
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
    }

    // Serialize StringArray object
    // Basically exact same as StringColumn, but some method names prevent
    // abstracting over both
    virtual char* serialize_string_array(StringArray* array) {
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
    virtual StringArray* deserialize_string_array(char* msg) {
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
    // [Serialized Column Types Array];[Serialized Column Names Array];[Serialized Row Names array]
    virtual char* serialize_schema(Schema* schema) {
        StringArray col_type_strs;
        StringArray col_name_strs;
        StringArray row_name_strs;

        // Convert char to string with null-terminator
        char* char_str = new char[2];
        char_str[1] = '\0';

        // Track all column types and names as Strings
        for (int i = 0; i < schema->width(); i++) {
            char_str[0] = schema->col_type(i);
            col_type_strs.push_back(new String(char_str));
            col_name_strs.push_back(schema->col_name(i));
        }

        // Track all row name Strings
        for (int j = 0; j < schema->length(); j++) {
            row_name_strs.push_back(schema->row_name(j));
        }

        // Leverage existing S/D methods for StringArray
        char* col_types = serialize_string_array(&col_type_strs);
        char* col_names = serialize_string_array(&col_name_strs);
        char* row_names = serialize_string_array(&row_name_strs);

        // Size of sub-strings + 3 for 2 ";"s and a nullterminator
        size_t total_size = 3 + strlen(col_types) + strlen(col_names) + strlen(row_names);
        char* serial_buffer = new char[total_size];
        strcpy(serial_buffer, col_types);
        strcat(serial_buffer, ";");
        strcat(serial_buffer, col_names);
        strcat(serial_buffer, ";");
        strcat(serial_buffer, row_names);
        return serial_buffer;
    }

    // Deserialize a char* msg into a Schema objects
    // Expects char* msg format as given by serialize_schema
    virtual Schema* deserialize_schema(char* msg) {
        char* token;
        Schema* fill_schema = new Schema();
        // Need 3 total copies of the original msg
        // strtok breaks when you call a function that subsequently
        //  calls strtok. As a result, cannot reuse original string
        char* temp_msg1 = new char[strlen(msg)];
        strcpy(temp_msg1, msg);
        char* temp_msg2 = new char[strlen(msg)];
        strcpy(temp_msg2, msg);
        // Tokenize message into ColTypes, ColNames, RowNames arrays
        token = strtok(msg, ";");
        StringArray* col_types = deserialize_string_array(token);
        token = strtok(temp_msg1, ";");
        token = strtok(nullptr, ";");
        StringArray* col_names = deserialize_string_array(token);
        token = strtok(temp_msg2, ";");
        token = strtok(nullptr, ";");
        token = strtok(nullptr, ";");
        StringArray* row_names = deserialize_string_array(token);
        size_t col_count = col_types->size();
        size_t row_count = row_names->size();
        char* col_type_str;
        // Add each column with a type and a name
        for (int i = 0; i < col_count; i++) {
            col_type_str = col_types->get(i)->c_str();
            fill_schema->add_column(col_type_str[0], col_names->get(i));
        }
        // Add each row by name
        for (int j = 0; j < row_count; j++) {
            fill_schema->add_row(row_names->get(j));
        }

        return fill_schema;
    }

    /* For full-implementation of this API, the following stubs must be
        implemented. Some are referenced by generic methods for serialization
        and deserialization */

    // Stub implementation for reference in generic methods
    virtual char* serialize_bool(bool value) { return nullptr; }

    // Stub implementation for reference in generic methods
    virtual bool deserialize_bool(bool value) { return false; }

    // Stub implementation for reference in generic methods
    virtual char* serialize_bool_col(BoolColumn* col) { return nullptr; }

    // Stub implementation for reference in generic methods
    virtual BoolColumn* deserialize_bool_col(char* msg) { return nullptr; }
};
