/* Authors: Ryan Heminway (heminway.r@husky.neu.edu)
*           David Tandetnik (tandetnik.da@husky.neu.edu) */
#pragma once
#include "../../utils/object.h"
#include "../../utils/string.h"
#include "fielder.h"
#include "schema.h"

#define INT_TYPE 'I'
#define STRING_TYPE 'S'
#define BOOL_TYPE 'B'
#define FLOAT_TYPE 'F'

union Field {
    int i_val;
    float f_val;
    bool b_val;
    String* s_val;
};

/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality. Rows do not own their data.
 */
class Row : public Object {
   public:
    Field** fields;
    bool* missings;
    char* field_types;
    size_t num_columns;
    size_t row_index;  // Unused, apart for set_idx() and get_idx()

    /** Build a row following a schema. */
    Row(Schema& scm) {
        row_index = -1;  // Default to invalid row number to not overlap with valid row index 0
        num_columns = scm.width();

        // initilize empty fields and field types
        fields = new Field*[num_columns];
        field_types = new char[num_columns];

        // fill in empty fields and field types
        for (size_t i = 0; i < num_columns; i++) {
            fields[i] = new Field();
            field_types[i] = scm.col_type(i);
        }

        // Init missings to be all true in this case
        missings = new bool[num_columns];
        for (size_t i = 0; i < num_columns; i++) {
            missings[i] = true;
        }
    }

    ~Row() {
        for (size_t i = 0; i < num_columns; i++) {
            // Note: no longer taking ownership of string*
            // delete fields[i]->s_val;  // If we're given a string*, set() says we should delete it.
            delete fields[i];
        }

        delete[] field_types;
        delete[] fields;
        delete[] missings;
    }

    // Return whether the element at the given value is a missing value
    // Undefined behavior if the idx is out of bounds
    bool is_missing(size_t col_idx) {
        return missings[col_idx];
    }

    /** Setters: set the given column with the given value. Setting a column with
    * a value of the wrong type is undefined. */
    void set(size_t col, int val) {
        if (col >= num_columns) {
            // trying to set out of bounds column
            return;
        }

        if (field_types[col] != INT_TYPE) {
            // column does not hold ints
            return;
        }

        // Check and update missings
        missings[col] = false;

        // set value in union
        fields[col]->i_val = val;
    }

    void set(size_t col, float val) {
        if (col >= num_columns) {
            // trying to set out of bounds column
            return;
        }

        if (field_types[col] != FLOAT_TYPE) {
            // column does not hold floats
            return;
        }

        // Check and update missings
        missings[col] = false;

        // set value in union
        fields[col]->f_val = val;
    }

    void set(size_t col, bool val) {
        if (col >= num_columns) {
            // trying to set out of bounds column
            return;
        }

        if (field_types[col] != BOOL_TYPE) {
            // column does not hold bools
            return;
        }

        // Check and update missings
        missings[col] = false;

        // set value in union
        fields[col]->b_val = val;
    }

    void set(size_t col, String* val) {
        if (col >= num_columns) {
            // trying to set out of bounds column
            return;
        }

        if (field_types[col] != STRING_TYPE) {
            // column does not hold Strings
            return;
        }

        // Check and update missings
        missings[col] = false;

        // set value in union
        fields[col]->s_val = val;
    }

    // Set the given column index as a missing value, default values are
    // given depending on the schema type for that column, but the value
    // has no meaning other than to be a place-holder
    void set_missing(size_t col_idx) {
        char col_type = field_types[col_idx];
        // get appropriately typed value out of the column, and set it in the row
        if (col_type == INT_TYPE) {
            set(col_idx, 0);
        } else if (col_type == BOOL_TYPE) {
            set(col_idx, false);
        } else if (col_type == FLOAT_TYPE) {
            set(col_idx, (float)0.0);
        } else {
            // Do not allocate memory. Value of missing is not irrelevant
            set(col_idx, nullptr);
        }
        missings[col_idx] = true;
    }

    /** Set/get the index of this row (ie. its position in the dataframe. This is
   *  only used for informational purposes, unused otherwise */
    void set_idx(size_t idx) {
        row_index = idx;
    }

    size_t get_idx() {
        return row_index;
    }

    /** Getters: get the value at the given column. If the column is not
    * of the requested type, the result is undefined. If the value is missing,
    * these methods provide no indication. The returned value is undefined, and
    * the "is_missing" method should be used for sanity checking. */
    int get_int(size_t col) {
        return fields[col]->i_val;
    }

    bool get_bool(size_t col) {
        return fields[col]->b_val;
    }

    float get_float(size_t col) {
        return fields[col]->f_val;
    }

    String* get_string(size_t col) {
        return fields[col]->s_val;
    }

    /** Number of fields in the row. */
    size_t width() {
        return num_columns;
    }

    /** Type of the field at the given position. An idx >= width is  undefined. */
    char col_type(size_t idx) {
        return field_types[idx];
    }

    /** Given a Fielder, visit every field of this row. The first argument is
    * index of the row in the dataframe.
    * Calling this method before the row's fields have been set is undefined. */
    void visit(size_t idx, Fielder& f) {
        f.start(idx);

        // pass each field of this row to the fielder
        for (size_t i = 0; i < num_columns; i++) {
            if (is_missing(i)) {
                printf("This Row has missings!\n");
                continue;  // Do not pass missings to fielder
            }

            char type = field_types[i];

            if (type == INT_TYPE) {
                f.accept(get_int(i));
            } else if (type == FLOAT_TYPE) {
                f.accept(get_float(i));
            } else if (type == BOOL_TYPE) {
                f.accept(get_bool(i));
            } else {
                // string type
                f.accept(get_string(i));
            }
        }

        f.done();
    }
};
