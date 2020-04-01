#pragma once
#include "../../utils/array.h"
#include "../../utils/helper.h"
#include "../../utils/object.h"
#include "../../utils/string.h"

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns, the type of each column, and the number of rows.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema : public Object {
   public:
    StringArray* col_types;
    size_t num_rows;

    /** Copying constructor */
    Schema(Schema& from) {
        col_types = new StringArray();

        for (size_t col_idx = 0; col_idx < from.width(); col_idx++) {
            char col_type = from.col_type(col_idx);

            add_column(col_type);
        }
        // Doesnt copy row info
        num_rows = 0;
    }

    /** Create an empty schema **/
    Schema() {
        col_types = new StringArray();
        num_rows = 0;
    }

    /** Create a schema from a string of types. A string that contains
    * characters other than those identifying the four type results in
    * undefined behavior. The argument is external, a nullptr argument is
    * undefined. **/
    Schema(const char* types) {
        col_types = new StringArray();

        for (size_t i = 0; i < strlen(types); i++) {
            // for each char in types, create a String wrapper for it and add to col_types
            char type = types[i];
            add_column(type);
        }
        num_rows = 0;
    }

    ~Schema() {
        for (size_t i = 0; i < col_types->size(); i++) {
            delete col_types->get(i);
        }

        delete col_types;
    }

    /** Add a column of the given type and name (can be nullptr), name
    * is external. Names are expectd to be unique, duplicates result
    * in undefined behavior. */
    void add_column(char type) {
        // convert char into String
        String* type_s = new String(&type);

        col_types->push_back(type_s);
    }

    /* Adds a row by simplying increasing the number of rows 
    *  tracked by this schema */
    void add_row() {
        num_rows++;
    }

    /** Return type of column at idx. An idx >= width is undefined. */
    char col_type(size_t idx) {
        String* type_s = col_types->get(idx);

        // type_s should be a String of length 1
        return type_s->at(0);
    }

    /** The number of columns */
    size_t width() {
        return col_types->size();
    }

    /** The number of rows */
    size_t length() {
        return num_rows;
    }
};
