// Authors: Ryan Heminway (heminway.r@husky.neu.edu)
//          David Tandetnik (tandetnik.da@husky.neu.edu)
#pragma once
#include <assert.h>

#include "../store/dataframe/dataframe.h"
#include "../store/dataframe/field.h"
#include "../store/serial.h"
#include "../utils/helper.h"

/* 
* Utility class to parse SoR file into a DataFrame format.
* Sorer class holds the file pointer and can return a DistributedDataFrame
* corresponding to the data in that file.
*/
class Sorer {
   public:
    FILE* fp;
    size_t from;
    size_t length;
    Schema* schema;

    /* Create a Sorer based on a file pointer.
       PARAMS: 
       file_ptr : FILE pointer to SoR file to process
       from_pt : byte location of where to start reading the file
       length_to_read : number of bytes to read from the file
       Undefined behavior if the given number_chunks_size results in chunks 
       that are too large to store. Always creates at least 1 chunk. If 
       length_to_read is 0, assumes the whole file should be read. */
    Sorer(FILE* file_ptr, size_t from_pt, size_t length_to_read) {
        if (file_ptr == nullptr) {
            exit_with_msg("file_ptr cannot be null");
        }
        fp = file_ptr;
        from = from_pt;
        // Read whole file if given length to read is 0
        if (length_to_read == 0) {
            fseek(fp, 0, SEEK_END);
            length = ftell(fp);
        } else {
            length = length_to_read;
        }

        assert(length > 0);
        // Pre-processing step. Obtain number of columns and the schema
        // of the SoR file
        parse_schema();
    }

    ~Sorer() {
        fclose(fp);
        delete schema;
    }

    /* Return the chunk, in the form of a DataFrame, corresponding to the
       given chunk_id. The chunk_id corresponds to the chunk of the file, 
       in order. The id 0 should always be valid and corresponds to the 
       first chunk. */
    DistributedDataFrame* get_dataframe(Store* store) {
        // move file pointer to next line after "from"
        fseek(fp, from, SEEK_SET);
        while (from != 0 && fgetc(fp) != '\n') {
        }

        DistributedDataFrame* df = new DistributedDataFrame(store, *schema);
        Row row(*schema);

        size_t MAX_LINE_LENGTH = 256;
        char buffer[MAX_LINE_LENGTH];
        bzero(buffer, MAX_LINE_LENGTH);

        size_t col_idx = 0;
        size_t read_idx = 0;
        bool reading_val = false;
        size_t bytes_read = 0;
        // For whole file:
        //  create Row object of each row's data
        //  add Row to df
        while (!feof(fp)) {
            char c = fgetc(fp);
            bytes_read++;
            if (c == '<') {
                reading_val = true;
                read_idx = 0;
            } else if (c == '>') {
                reading_val = false;
                buffer[read_idx] = '\0';

                // Handle missings
                if (is_empty_field(buffer, schema->col_type(col_idx))) {
                    row.set_missing(col_idx);
                    // Based on schema, add data to row
                } else if (schema->col_type(col_idx) == INT_TYPE) {
                    int val = atoi(trim_whitespace(buffer));
                    row.set(col_idx, val);
                } else if (schema->col_type(col_idx) == FLOAT_TYPE) {
                    float val = (float)atof(trim_whitespace(buffer));
                    row.set(col_idx, val);
                } else if (schema->col_type(col_idx) == STRING_TYPE) {
                    String val(trim_whitespace(buffer)); 
                    row.set(col_idx, &val);
                } else {  // BOOL_TYPE
                    if (strcmp(trim_whitespace(buffer), "1") == 0) {
                        row.set(col_idx, true);
                    } else {
                        row.set(col_idx, false);
                    }
                }

                col_idx++;
            } else if (reading_val) {  // Copy value into buffer
                buffer[read_idx] = c;
                // printf("Read so far %s\n", buffer);
                read_idx++;
            } else if (c == '\n') {
                // Add row to dataframe
                df->add_row(row);

                col_idx = 0;
                bzero(buffer, MAX_LINE_LENGTH);
            }
            // Stop reading after length_to_read
            if (bytes_read >= length) {
                break;
            }
        }

        return df;
    }

    // checks whether the field represented by the given char* buffer
    // should be an EMPTY (missing) field, given the type of the Column
    bool is_empty_field(char* buffer, char col_type) {
        FIELD_TYPE val_type = parse_field_type(trim_whitespace(buffer));
        FIELD_TYPE col_field_type;

        // Convert character col_type to FIELD_TYPE
        switch (col_type) {
            case INT_TYPE:
                col_field_type = INT;
                break;
            case FLOAT_TYPE:
                col_field_type = FLOAT;
                break;
            case BOOL_TYPE:
                col_field_type = BOOL;
                break;
            default:
                col_field_type = STRING;
                break;
        }

        // Field should be empty (missing) if it is EMPTY or its field_type
        //  does not match column type
        // SPECIAL CASES: field is bool but column is int or float
        //                field is int but column is float
        bool case_1 = (val_type == BOOL && (col_field_type == INT
            || col_field_type == FLOAT));
        bool case_2 = (val_type == INT && (col_field_type == FLOAT));
        bool is_empty;
        is_empty = (val_type == EMPTY) || ((val_type != col_field_type) 
                                           && !case_1 && !case_2);
        return is_empty;
    }

    // Count number of columns in the longest line in first 500 rows
    // Gives number of columns for the schema
    size_t count_cols() {
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

        return max_fields;
    }

    // Obtain the schema from the first 500 lines of the file
    void parse_schema() {
        size_t num_columns = count_cols();

        // return to beginning of file
        fseek(fp, 0, SEEK_SET);

        size_t line = 0;
        bool reading_val = false;
        size_t read_idx = 0;
        size_t col_idx = 0;
        char buffer[255] = "";
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
                schema->add_column(STRING_TYPE);
            } else if (type == FLOAT) {
                schema->add_column(FLOAT_TYPE);
            } else if (type == INT) {
                schema->add_column(INT_TYPE);
            } else {
                schema->add_column(BOOL_TYPE);
            }
        }
    }
};
