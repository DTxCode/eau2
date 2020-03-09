#pragma once
#include "array.h"
#include "helper.h"
#include "object.h"
#include "string.h"

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema : public Object {
   public:
    StringArray* col_types;
    StringArray* col_names;
    StringArray* row_names;

    /** Copying constructor */
    Schema(Schema& from) {
        col_names = new StringArray();
        row_names = new StringArray();
        col_types = new StringArray();

        for (size_t col_idx = 0; col_idx < from.width(); col_idx++) {
            String* col_name = from.col_name(col_idx);
            char col_type = from.col_type(col_idx);

            add_column(col_type, col_name);
        }

        // Note: not copying row info
    }

    /** Create an empty schema **/
    Schema() {
        col_names = new StringArray();
        row_names = new StringArray();
        col_types = new StringArray();
    }

    /** Create a schema from a string of types. A string that contains
    * characters other than those identifying the four type results in
    * undefined behavior. The argument is external, a nullptr argument is
    * undefined. **/
    Schema(const char* types) {
        col_names = new StringArray();
        row_names = new StringArray();
        col_types = new StringArray();

        for (size_t i = 0; i < strlen(types); i++) {
            // for each char in types, create a String wrapper for it and add to col_types
            char type = types[i];
            add_column(type, nullptr);
        }
    }

    ~Schema() {
        for (size_t i = 0; i < col_types->size(); i++) {
            delete col_types->get(i);
        }

        delete col_types;
        delete col_names;
        delete row_names;
    }

    /** Add a column of the given type and name (can be nullptr), name
    * is external. Names are expectd to be unique, duplicates result
    * in undefined behavior. */
    void add_column(char typ, String* name) {
        // convert char into String
        String* type_s = new String(&typ);

        col_types->push_back(type_s);
        col_names->push_back(name);
    }

    /** Add a row with a name (possibly nullptr), name is external.  Names are
   *  expectd to be unique, duplicates result in undefined behavior. */
    void add_row(String* name) {
        row_names->push_back(name);
    }

    /** Return name of row at idx; nullptr indicates no name. An idx >= width
    * is undefined. */
    String* row_name(size_t idx) {
        return row_names->get(idx);
    }

    /** Return name of column at idx; nullptr indicates no name given.
    *  An idx >= width is undefined.*/
    String* col_name(size_t idx) {
        return col_names->get(idx);
    }

    /** Return type of column at idx. An idx >= width is undefined. */
    char col_type(size_t idx) {
        String* type_s = col_types->get(idx);

        // type_s should be a String of length 1
        return type_s->at(0);
    }

    /** Given a column name return its index, or -1. */
    int col_idx(const char* name) {
        String* name_s = new String(name);
        size_t index = col_names->index_of(name_s);

        if (index >= col_names->size()) {
            // no such column
            return -1;
        }

        delete name_s;

        return index;
    }

    /** Given a row name return its index, or -1. */
    int row_idx(const char* name) {
        String* name_s = new String(name);
        size_t index = row_names->index_of(name_s);

        if (index >= row_names->size()) {
            // no such row
            return -1;
        }

        delete name_s;

        return index;
    }

    /** The number of columns */
    size_t width() {
        return col_types->size();
    }

    /** The number of rows */
    size_t length() {
        return row_names->size();
    }
};