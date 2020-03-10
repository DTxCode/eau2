#pragma once
#include <thread>
#include "../../utils/object.h"
#include "../../utils/string.h"
#include "column.h"
#include "row.h"
#include "rower.h"
#include "schema.h"

#define INT_TYPE 'I'
#define BOOL_TYPE 'B'
#define FLOAT_TYPE 'F'
#define STRING_TYPE 'S'

/****************************************************************************
 * ModifiedDataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 * This ModifiedDataFrame has the ability of executing map in parallel
 */
class ModifiedDataFrame : public Object {
   public:
    Schema* schema;
    Column** columns;

    /** Create a data frame with the same columns as the given df but with no rows or row names */
    ModifiedDataFrame(ModifiedDataFrame& df) {
        schema = new Schema(df.get_schema());
        columns = new Column*[schema->width()];

        set_empty_cols_(schema);
    }

    /** Create a data frame from a schema and columns. All columns are created
    * empty. */
    ModifiedDataFrame(Schema& scm) {
        schema = new Schema(scm);
        columns = new Column*[scm.width()];

        set_empty_cols_(schema);
    }

    ~ModifiedDataFrame() {
        for (size_t col_idx = 0; col_idx < schema->width(); col_idx++) {
            delete columns[col_idx];
        }

        delete[] columns;
        delete schema;
    }

    // Creates and sets empty columns in this dataframe according to the given schema
    void set_empty_cols_(Schema* schema) {
        for (size_t col_idx = 0; col_idx < schema->width(); col_idx++) {
            char col_type = schema->col_type(col_idx);

            if (col_type == INT_TYPE) {
                columns[col_idx] = new IntColumn();
            } else if (col_type == BOOL_TYPE) {
                columns[col_idx] = new BoolColumn();
            } else if (col_type == FLOAT_TYPE) {
                columns[col_idx] = new FloatColumn();
            } else {
                columns[col_idx] = new StringColumn();
            }
        }
    }

    /** Returns the dataframe's schema. Modifying the schema after a dataframe
    * has been created is undefined. */
    Schema& get_schema() {
        return *schema;
    }

    /** Adds a column this dataframe, updates the schema, the new column
    * is external, and appears as the last column of the dataframe, the
    * name is optional and external. A nullptr colum is undefined. */
    void add_column(Column* col, String* name) {
        // If this is not the first column being added, only accept it if
        // it has the same number of rows as what's already in the dataframe
        if (schema->length() != 0 && col->size() != schema->length()) {
            return;
        }

        char col_type = col->get_type();
        // Use copy of given column so that we can delete it later
        Column* col_copy = get_col_copy_(col);

        // add new column information to schema
        size_t old_schema_width = schema->width();
        schema->add_column(col_type, name);
        size_t new_schema_width = schema->width();

        // If this is the first column being added, record new row info
        if (schema->length() == 0) {
            for (size_t row_idx = 0; row_idx < col_copy->size(); row_idx++) {
                schema->add_row(nullptr);
            }
        }

        // increase column array size
        Column** new_columns = new Column*[new_schema_width];
        for (size_t col_idx = 0; col_idx < old_schema_width; col_idx++) {
            new_columns[col_idx] = columns[col_idx];
        }

        // add new column
        new_columns[new_schema_width - 1] = col_copy;

        delete[] columns;
        columns = new_columns;
    }

    // returns a copy of the given column
    Column* get_col_copy_(Column* col) {
        char col_type = col->get_type();

        if (col_type == INT_TYPE) {
            return new IntColumn(col);
        } else if (col_type == BOOL_TYPE) {
            return new BoolColumn(col);
        } else if (col_type == FLOAT_TYPE) {
            return new FloatColumn(col);
        } else {
            return new StringColumn(col);
        }
    }

    /** Return the value at the given column and row. Accessing rows or
    *  columns out of bounds, or request the wrong type is undefined.*/
    int get_int(size_t col, size_t row) {
        return columns[col]->as_int()->get(row);
    }

    bool get_bool(size_t col, size_t row) {
        return columns[col]->as_bool()->get(row);
    }

    float get_float(size_t col, size_t row) {
        return columns[col]->as_float()->get(row);
    }

    String* get_string(size_t col, size_t row) {
        return columns[col]->as_string()->get(row);
    }

    /** Return the offset of the given column name or -1 if no such col. */
    int get_col(String& col) {
        return schema->col_idx(col.c_str());
    }

    /** Return the offset of the given row name or -1 if no such row. */
    int get_row(String& row) {
        return schema->row_idx(row.c_str());
    }

    /** Set the value at the given column and row to the given value.
    * If the column is not  of the right type or the indices are out of
    * bound, the result is undefined. 
    * Runtime error if invalid type or index. */
    void set(size_t col, size_t row, int val) {
        columns[col]->as_int()->set(row, val);
    }

    void set(size_t col, size_t row, bool val) {
        columns[col]->as_bool()->set(row, val);
    }

    void set(size_t col, size_t row, float val) {
        columns[col]->as_float()->set(row, val);
    }

    void set(size_t col, size_t row, String* val) {
        columns[col]->as_string()->set(row, val);
    }

    /** Set the fields of the given row object with values from the columns at
    * the given offset.  If the row is not form the same schema as the
    * dataframe, results are undefined.
    */
    void fill_row(size_t idx, Row& row) {
        // loop over all of the columns
        for (size_t col_idx = 0; col_idx < schema->width(); col_idx++) {
            Column* col = columns[col_idx];
            char col_type = col->get_type();

            // get appropriately typed value out of the column, and set it in the row
            if (col_type == INT_TYPE) {
                int val = col->as_int()->get(idx);
                row.set(col_idx, val);
            } else if (col_type == BOOL_TYPE) {
                bool val = col->as_bool()->get(idx);
                row.set(col_idx, val);
            } else if (col_type == FLOAT_TYPE) {
                float val = col->as_float()->get(idx);
                row.set(col_idx, val);
            } else {
                String* val = col->as_string()->get(idx);
                row.set(col_idx, val);
            }
        }
    }

    /** Add a row at the end of this dataframe. The row is expected to have
   *  the right schema and be filled with values, otherwise undedined.
   * Won't add row if it's not of the right width.  */
    void add_row(Row& row) {
        if (row.width() != schema->width()) {
            // dont add row of incorrect width
            return;
        }

        // rows can't be named at the moment
        schema->add_row(nullptr);

        // loop over all of the columns
        for (size_t col_idx = 0; col_idx < schema->width(); col_idx++) {
            Column* col = columns[col_idx];
            char col_type = col->get_type();

            // get appropriately typed value out of the row, and set it in the column
            // expect col schema to match row schema
            if (col_type == INT_TYPE) {
                int val = row.get_int(col_idx);
                col->as_int()->push_back(val);

            } else if (col_type == BOOL_TYPE) {
                bool val = row.get_bool(col_idx);
                col->as_bool()->push_back(val);

            } else if (col_type == FLOAT_TYPE) {
                float val = row.get_float(col_idx);
                col->as_float()->push_back(val);

            } else {
                String* val = row.get_string(col_idx);
                col->as_string()->push_back(val);
            }
        }
    }

    /** The number of rows in the dataframe. */
    size_t nrows() {
        return schema->length();
    }

    /** The number of columns in the dataframe.*/
    size_t ncols() {
        return schema->width();
    }

    /** Helper function to visit a chunk of rows in order. Row_start and 
        row_end must be valid row indices, or behavior is undefined. **/
    void map_chunk(size_t row_start, size_t row_end, Rower& r) {
        Row* row = new Row(*schema);
        for (size_t row_idx = row_start; row_idx <= row_end; row_idx++) {
            fill_row(row_idx, *row);

            r.accept(*row);

            // Now insert changes back into map (if any)
            for (size_t j = 0; j < ncols(); j++) {
                Column* col = columns[j];
                char col_type = col->get_type();

                // get appropriately typed value out of the row, and set it in the column
                // expect col schema to match row schema
                if (col_type == INT_TYPE) {
                    set(j, row_idx, row->get_int(j));
                } else if (col_type == BOOL_TYPE) {
                    set(j, row_idx, row->get_bool(j));
                } else if (col_type == FLOAT_TYPE) {
                    set(j, row_idx, row->get_float(j));
                } else {
                    set(j, row_idx, row->get_string(j));
                }
            }
        }
    }

    /** Visit rows in order */
    void map(Rower& r) {
        map_chunk(0, nrows() - 1, r);
    }

    /** This method clones the Rower and executes the map in parallel. Join
    used at the end to merge the results */
    void pmap(Rower& r) {
        size_t cpu_cores = 4;  // 1 thread per cpu core
        if (nrows() < cpu_cores) {
            map(r);  // Only parallelize if # of rows makes sense to parallelize
            return;
        }
        std::thread threads[cpu_cores];
        // Need 1 less rower than threads, since we can use the given one
        Rower** rowers = new Rower*[cpu_cores - 1];
        size_t rows_p_thread = nrows() / cpu_cores;
        for (size_t i = 0; i < cpu_cores - 1; i++) {
            rowers[i] = dynamic_cast<Rower*>(r.clone());
            // First (cpu_cores - 1) threads get equal chunk sizes
            threads[i] = std::thread(&ModifiedDataFrame::map_chunk, this, i * rows_p_thread,
                                     ((i + 1) * rows_p_thread) - 1, std::ref(*rowers[i]));
        }
        // Last thread gets rest of the frame as its chunk
        threads[cpu_cores - 1] = std::thread(&ModifiedDataFrame::map_chunk, this,
                                             (cpu_cores - 1) * rows_p_thread,
                                             nrows() - 1, std::ref(r));

        // Once thread is done, integrate its changes back into frame
        for (size_t i = 0; i < cpu_cores; i++) {
            threads[i].join();
        }

        // Join_delete all rowers into original
        for (size_t i = 0; i < cpu_cores - 1; i++) {
            r.join_delete(rowers[i]);
        }
    }

    /** Create a new dataframe, constructed from rows for which the given Rower
    * returned true from its accept method. */
    ModifiedDataFrame* filter(Rower& r) {
        ModifiedDataFrame* new_df = new ModifiedDataFrame(get_schema());

        // pass this row to the Rower and add to new_df is rower accepts it
        Row* row = new Row(*schema);
        for (size_t row_idx = 0; row_idx < nrows(); row_idx++) {
            fill_row(row_idx, *row);

            bool accept = r.accept(*row);
            if (accept) {
                new_df->add_row(*row);
            }
        }

        return new_df;
    }

    /** Print the dataframe in SoR format to standard output. */
    void print() {
        // Use helper for printing
        Sys s;

        for (size_t row_idx = 0; row_idx < nrows(); row_idx++) {
            // Print string representation of this row by looking at col values
            for (size_t j = 0; j < ncols(); j++) {
                Column* col = columns[j];
                char col_type = col->get_type();

                s.p("<");
                if (col_type == INT_TYPE) {
                    int val = get_int(j, row_idx);
                    s.p(val);
                } else if (col_type == BOOL_TYPE) {
                    bool val = get_bool(j, row_idx);
                    s.p(val);
                } else if (col_type == FLOAT_TYPE) {
                    float val = get_float(j, row_idx);
                    if (val >= 0) {
                        s.p("+");
                        s.p(val);
                    } else {
                        s.p("-");
                        s.p(val);
                    }
                } else {
                    String* val = get_string(j, row_idx);
                    s.p("\"");
                    s.p(val->c_str());
                    s.p("\"");
                }
                s.p("> ");
            }

            // Finished with this row, move to next line
            s.pln();
        }
    }
};
