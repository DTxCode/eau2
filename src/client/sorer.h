// lang:CwC
// Authors: Ryan Heminway (heminway.r@husky.neu.edu)
//          David Tandetnik (tandetnik.da@husky.neu.edu)
#pragma once

#include <assert.h>
#include "../store/dataframe/dataframe.h"
#include "../store/dataframe/field.h"
#include "../utils/helper.h"

#include "../store/serial.h"

/* 
* Utility class to parse SoR file into a DataFrame format.
* Sorer class holds the file pointer and can return a DataFrame
* corresponding to a chunk of that file's data. 
*/
class Sorer {
   public:
    FILE* fp;
    size_t from;
    size_t length;
    size_t num_chunks;
    size_t num_columns;
    size_t num_rows;
    size_t rows_per_chunk;
    Schema* schema;

    /* Create a Sorer based on a file pointer.
       PARAMS: 
        file_ptr : FILE pointer to SoR file to process
        from_pt : byte location of where to start reading the file
        length_to_read : number of bytes to read from the file
       Undefined behavior if the given number_chunks_size results in chunks 
       that are too large to store. Always creates at least 1 chunk. */
    Sorer(FILE* file_ptr, size_t from_pt, size_t length_to_read) {
        // TODO how much error handling do we want at this stage
        if (file_ptr == nullptr) {
            exit_with_msg("file_ptr cannot be null");
        }
        fp = file_ptr;
        from = from_pt;
        length = length_to_read;
        // Pre-processing step. Obtain number of columns and the schema
        // of the SoR file
        parse_schema();
        // Get number of rows in file in the region [from, length]
        // for use in calculation get_chunk_as_df
        count_rows();
    }

    ~Sorer() {
        fclose(fp);
        delete schema;
    }

    /* Return the chunk, in the form of a DataFrame, corresponding to the
       given chunk_id. The chunk_id corresponds to the chunk of the file, 
       in order. The id 0 should always be valid and corresponds to the 
       first chunk. */
    DataFrame* get_chunk_as_df(size_t chunk_id, size_t num_chunks) {
        if (num_chunks == 0) {
            exit_with_msg("get_chunk_as_df: num_chunks must be > 0");
        }

        rows_per_chunk = num_rows / num_chunks;
        assert(rows_per_chunk > 0);

        size_t from_row = chunk_id * rows_per_chunk;
        size_t to_row = (chunk_id + 1) * rows_per_chunk;

        // Ensure final chunk does not miss last few rows
        if (chunk_id == (num_chunks - 1)) {
            to_row = num_rows;
        }

        // Moves file pointer to 'from_row' point in file
        go_to_row(from_row);

        DataFrame* df = new DataFrame(*schema);
        Row* row = new Row(*schema);

        char buffer[255];

        // Local row idx
        size_t curr_row = 0;
        size_t col_idx = 0;
        size_t read_idx = 0;
        bool reading_val = false;
        // For whole chunk:
        //  create Row object of each row's data
        //  add Row to df
        while (!feof(fp)) {
            char c = fgetc(fp);
            if (c == '<') {
                reading_val = true;
                read_idx = 0;
            } else if (c == '>') {
                reading_val = false;
                buffer[read_idx] = '\0';

                if (buffer == "") {
                    // TODO handle empty type
                }

                // Based on schema, add data to row
                if (schema->col_type(col_idx) == INT_TYPE) {
                    int val = atoi(trim_whitespace(buffer));
                    row->set(col_idx, val);
                } else if (schema->col_type(col_idx) == FLOAT_TYPE) {
                    float val = (float)atof(trim_whitespace(buffer));
                    row->set(col_idx, val);
                } else if (schema->col_type(col_idx) == STRING_TYPE) {
                    String* val = new String(trim_whitespace(buffer));
                    row->set(col_idx, val);
                } else {  // BOOL_TYPE
                    if (trim_whitespace(buffer) == "1") {
                        row->set(col_idx, true);
                    } else {
                        row->set(col_idx, false);
                    }
                }

                col_idx++;
            } else if (reading_val) {  // Copy value into buffer
                buffer[read_idx] = c;
                read_idx++;
            } else if (c == '\n') {
                // Add row to dataframe
                df->add_row(*row);

                curr_row++;
                col_idx = 0;
            }
            // Stop reading after chunk
            if (from_row + curr_row == to_row) {
                break;
            }
        }

        return df;
    }

    // Move file pointer to the given row_idx in the file
    void go_to_row(size_t row_idx) {
        // move file pointer to next line after "from"
        fseek(fp, from, SEEK_SET);
        while (from != 0 && fgetc(fp) != '\n') {
        }

        // Maximum number of characters possible in a row
        size_t max_row_size = 255 * num_columns;
        char buffer[max_row_size];

        size_t cur_line_idx = 0;
        while (!feof(fp)) {
            if (cur_line_idx == row_idx) {
                break;
            }

            // Read a whole line
            fgets(buffer, max_row_size, fp);
            cur_line_idx++;
        }
    }

    // Count the total number of rows in the file in the given
    // [from, from+length] region
    // TODO could this overflow size_t range?
    void count_rows() {
        // return to beginning of range [from, from+length] of file
        fseek(fp, from, SEEK_SET);

        // Maximum number of characters possible in a row
        size_t max_row_size = 255 * num_columns;
        char buffer[max_row_size];

        size_t cur_line_idx = 0;
        size_t bytes_read = 0;
        while (!feof(fp)) {
            // Read a whole line
            fgets(buffer, max_row_size, fp);
            // Track bytes read
            // TODO Does this count right number of bytes?
            bytes_read += strlen(buffer);
            cur_line_idx++;
            // Only count until from+length
            if (bytes_read >= length) {
                break;
            }
        }

        num_rows = cur_line_idx;
    }

    // Count number of columns in the longest line in first 500 rows
    // Gives number of columns for the schema
    void count_cols() {
        // return to beginning of file
        fseek(fp, 0, SEEK_SET);

        size_t max_fields = 0;
        size_t cur_fields = 0;
        size_t cur_line_idx = 0;
        // Count number of columns in row with most elements in first 500 lines
        // This gives number of columns for schema
        while (!feof(fp)) {
            char c = fgetc(fp);

            if (c == '>') {  // Note: semi-brittle way of counting fields
                cur_fields += 1;
            } else if (c == '\n') {  // New line, reset fields count
                if (cur_fields > max_fields) {
                    max_fields = cur_fields;
                }
                cur_line_idx += 1;
                cur_fields = 0;
            }
            // Only look for schema in first 500 lines
            if (cur_line_idx >= 499) {
                break;
            }
        }

        num_columns = max_fields;
    }

    // Obtain the schema from the first 500 lines of the file
    void parse_schema() {
        count_cols();

        // return to beginning of file
        fseek(fp, 0, SEEK_SET);

        size_t line = 0;
        bool reading_val = false;
        size_t read_idx = 0;
        size_t col_idx = 0;
        char buffer[255] = "";
        char type;
        FIELD_TYPE column_types[num_columns];
        // Default type for every col is BOOL
        for (size_t i = 0; i < num_columns; i++) {
            column_types[i] = BOOL;
        }

        // Read file char by char for first 500 lines, parsing values for col types
        while (!feof(fp)) {
            char c = fgetc(fp);
            if (c == '<') {
                reading_val = true;
                read_idx = 0;
            } else if (c == '>') {
                reading_val = false;
                buffer[read_idx] = '\0';
                FIELD_TYPE val_type = parse_field_type(trim_whitespace(buffer));
                FIELD_TYPE curr_col_type = column_types[col_idx];
                // Type hierarchy when parsing values:
                // If any value is STRING: whole col is STRING
                // If col is not STRING and value is FLOAT: whole col is float
                // if col is not STRING or FLOAT and value is INT: whole col is INT
                // By default, any remaining (unchanged) columns are BOOL
                if (val_type == STRING) {
                    column_types[col_idx] = STRING;
                } else if (val_type == FLOAT && curr_col_type != STRING) {
                    column_types[col_idx] = FLOAT;
                } else if (val_type == INT && curr_col_type != FLOAT && curr_col_type != STRING) {
                    column_types[col_idx] = INT;
                }  // Default case is BOOL

                col_idx++;
            } else if (reading_val) {  // Copy value into buffer
                buffer[read_idx] = c;
                read_idx++;
            } else if (c == '\n') {
                line++;
                col_idx = 0;
            }
            // Only read first 500 lines
            if (line == 500) {
                break;
            }
        }

        schema = new Schema();

        // Create a schema from column types
        for (size_t i = 0; i < num_columns; i++) {
            FIELD_TYPE type = column_types[i];
            if (type == STRING) {
                schema->add_column(STRING_TYPE, nullptr);
            } else if (type == FLOAT) {
                schema->add_column(FLOAT_TYPE, nullptr);
            } else if (type == INT) {
                schema->add_column(INT_TYPE, nullptr);
            } else {
                // TODO missing column defaults to bool type for now
                schema->add_column(BOOL_TYPE, nullptr);
            }
        }
    }
};
