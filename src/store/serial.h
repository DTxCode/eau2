// lang::CwC
// Authors: heminway.r@husky.neu.edu
//          tandetnik.da@husky.neu.edu
#pragma once
#include <stdio.h>
#include <stdlib.h>

// Forward declarations to avoid includes
class DataFrame;
class DistributedDataFrame;
class DistributedColumn;
class DistributedIntColumn;
class DistributedBoolColumn;
class DistributedFloatColumn;
class DistributedStringColumn;
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
class Store;

// Utility to transform Classes and data structures used in the DataFrame API 
// to and from a serialized character array format.
// - Each Class or data-structure to be serialized has its own set of
//   methods in this classwhich serialize and de-serialize that structure.
//   All methods are expected to receive well-formed inputs. Behavior is
//   UNDEFINED if the given arguments to these methods do not align with the
//   expected types. For example, if a user calls "deserialize_int" and passes
//   a character array describing a String, the result is undefined.
// - This class does not manage any memory that is given to or returned by
//   its methods. Users are expected to delete any pointers given to or
//   returned by this class.
// This class is intended to be sub-classed and methods are intended to be
// re-implemented for users who may have different ideas/requirements for
// serialization of these same types. Sub-classes can also maintain the
// design philosophy found here for any additional types not supported here.
class Serializer {
   public:
    Serializer() {}
    virtual ~Serializer() {}

    virtual char* serialize_distributed_dataframe(DistributedDataFrame* df);
    virtual DistributedDataFrame* deserialize_distributed_dataframe(char* msg, Store* store);

    virtual char* serialize_message(Message* msg);
    virtual Message* deserialize_message(char* msg);

    virtual char* serialize_dist_col(DistributedColumn* col);
    virtual DistributedColumn* deserialize_dist_col(char* msg, Store* store, char type);
    virtual DistributedIntColumn* deserialize_dist_int_col(char* msg, Store* store);
    virtual DistributedBoolColumn* deserialize_dist_bool_col(char* msg, Store* store);
    virtual DistributedFloatColumn* deserialize_dist_float_col(char* msg, Store* store);
    virtual DistributedStringColumn* deserialize_dist_string_col(char* msg, Store* store);

    virtual char* serialize_int(int value);
    virtual int deserialize_int(char* msg);
    virtual char* serialize_size_t(size_t value);
    virtual size_t deserialize_size_t(char* msg);
    virtual char* serialize_float(float value);
    virtual float deserialize_float(char* msg);
    virtual char* serialize_string(String* value);
    virtual String* deserialize_string(char* msg);
    virtual char* serialize_key(Key* value);
    virtual Key* deserialize_key(char* msg);
    virtual char* serialize_string_array(StringArray* array);
    virtual StringArray* deserialize_string_array(char* msg);
    virtual char* serialize_schema(Schema* schema);
    virtual Schema* deserialize_schema(char* msg);
    virtual char* serialize_bool(bool value);
    virtual bool deserialize_bool(char* msg);

    virtual char* serialize_bools(bool* bools, size_t num_values);
    virtual char* serialize_ints(int* ints, size_t num_values);
    virtual char* serialize_floats(float* floats, size_t num_values);
    virtual char* serialize_strings(String** strings, size_t num_values);

    virtual bool* deserialize_bools(char* msg);
    virtual int* deserialize_ints(char* msg);
    virtual float* deserialize_floats(char* msg);
    virtual String** deserialize_strings(char* msg);
};
