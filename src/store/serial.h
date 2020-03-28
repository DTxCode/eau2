// lang::CwC
// Authors: heminway.r@husky.neu.edu
//          tandetnik.da@husky.neu.edu
#pragma once
#include <stdio.h>
#include <stdlib.h>

// Forward declarations to avoid includes
class DataFrame;
class Column;
class IntColumn;
class BoolColumn;
class FloatColumn;
class StringColumn;
class StringArray;
class Key;
class Message;
class Schema;
class String;

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
    Serializer() {} 

    virtual ~Serializer() {}

    // Serialize a given DataFrame object
    // Serialized message will take the form:
    // "[Serialized Schema]~[Serialized Column 0]; ... ;[Serialized Column n-1]"
    virtual char* serialize_dataframe(DataFrame* df);

    // Deserialize a char* buffer into a DataFrame object
    virtual DataFrame* deserialize_dataframe(char* msg);

    // Serialize a Message object
    virtual char* serialize_message(Message* msg);

    // Deserialize message into Message object
    // Leverage built-in Message constructor which works
    // from its to_string() representation (used for serialization)
    virtual Message* deserialize_message(char* msg);

    // Serialize an IntColumn
    virtual char* serialize_int_col(IntColumn* col); 

    // Deserialize IntColumn serialized message
    virtual IntColumn* deserialize_int_col(char* msg);

    // Serialize a FloatColumn to a character array
    virtual char* serialize_float_col(FloatColumn* col); 

    // Deserialize FloatColumn serialized message
    virtual FloatColumn* deserialize_float_col(char* msg);

    // Serialize a StringColumn to a character array
    virtual char* serialize_string_col(StringColumn* col);

    // Deserialize StringColumn serialized message
    virtual StringColumn* deserialize_string_col(char* msg);

    // Serialize an int to a character array
    // Methodology attributed to Stack Overflow post:
    // stackoverflow.com/questions/7140943
    virtual char* serialize_int(int value); 

    // De-serialize a character array in to an integer value
    virtual int deserialize_int(char* msg);

    // Serialize an size_t (unsigned long) to a character array
    virtual char* serialize_size_t(size_t value); 
    
    // De-serialize a character array in to an size_t value
    virtual size_t deserialize_size_t(char* msg);

    // Serialize a float to a character array
    virtual char* serialize_float(float value);

    // De-serialize a character array to a float value
    virtual float deserialize_float(char* msg);

    // Serialize pointer String object to a character array
    // Copying c_str representation of the String for safety
    virtual char* serialize_string(String* value);

    // De-serialize a character array to a string value
    virtual String* deserialize_string(char* msg);

    // Serialize pointer to Key* object
    // Creates form "[serialized Key name],[serialized Key home_node]"
    virtual char* serialize_key(Key* value);

    // Deserialize a char* into a Key object
    // Expects char* form of "[serialized Key name],[serialized Key home_node]"
    virtual Key* deserialize_key(char* msg);

    // Generic serialization method for Column type
    // Process is same for every column, but serialization method used for
    // cell-type is dependent on the given 'type' parameter.
    // Providing the wrong type parameter for the given Column results in
    // undefined behavior.
    // Serialized string format is "[value],[value], ... ,[value],[value]"
    // All values, in serialized char* form, are separated by commas
    virtual char* serialize_col(Column* col, char type);

    // Deserialize a Column serialized message
    // Message expected to have form "[value],[value], ... ,[value],[value]"
    // Use push_back method to add values to a new column, with the intention
    //  of abstracting over implementation details of a column
    // Params:
    //  msg: char-array message to be deserialized
    //  type: char representing type of Column to assume
    //  fill_col: pointer to column to fill with values
    virtual void deserialize_col(char* msg, char type, Column* fill_col);

    // Serialize StringArray object
    // Basically exact same as StringColumn, but some method names prevent
    // abstracting over both
    virtual char* serialize_string_array(StringArray* array);

    // Deserialize serialized message to a StringArray
    virtual StringArray* deserialize_string_array(char* msg);

    // Serialize a Schema object pointer to a char*
    // returned message will have form
    // [Serialized Column Types Array];[Serialized Column Names Array];[Serialized Row Names array]
    virtual char* serialize_schema(Schema* schema);

    // Deserialize a char* msg into a Schema objects
    // Expects char* msg format as given by serialize_schema
    virtual Schema* deserialize_schema(char* msg); 

    // Serialize a bool to a char* message
    // true gets serialized to "1"
    // false gets serialized to "0"
    virtual char* serialize_bool(bool value);

    // Deserialize a boolean serialized message
    // Expects a message in the form "1" or "0"
    virtual bool deserialize_bool(char* msg);
    
    // Serialize boolean column
    // Uses same format as other columns, see serialize_col
    virtual char* serialize_bool_col(BoolColumn* col);

    // Deserialize boolean column from char*
    virtual BoolColumn* deserialize_bool_col(char* msg); 

    virtual char* serialize_bools(bool* bools, size_t num_values);
    virtual char* serialize_ints(int* ints, size_t num_values);
    virtual char* serialize_floats(float* floats, size_t num_values);
    virtual char* serialize_strings(String** strings, size_t num_values);

    virtual bool* deserialize_bools(char* msg);
    virtual int* deserialize_ints(char* msg);
    virtual float* deserialize_floats(char* msg);
    virtual String** deserialize_strings(char* msg);
};
