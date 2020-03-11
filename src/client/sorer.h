// lang:CwC 
// Authors: Ryan Heminway (heminway.r@husky.neu.edu)
//          David Tandetnik (tandetnik.da@husky.neu.edu)
#pragma once

#include "../store/dataframe/dataframe.h"
#include "field.h"
#include "../utils/helper.h"

/* 
* Utility class to parse SoR file into a DataFrame format.
* Sorer class holds the file pointer and can return a DataFrame
* corresponding to a chunk of that file's data. 
*/
class Sorer {
   public:
    FILE* fp;
    size_t num_chunks;
    size_t num_columns;
    size_t num_rows_per_chunk;
    Schema* schema;

    /* Create a Sorer based on a file pointer.
       PARAMS: 
        file_ptr : FILE pointer to SoR file to process
        max_chunk_size : maximum size of a chunk, in bytes
        from : byte location of where to start reading the file
        length : number of bytes to read from the file
       Undefined behavior if the given max_chunk_size is too large
       to store on a single machine. */
    Sorer(FILE* file_ptr, size_t max_chunk_size, size_t from, size_t length) {
        // TODO how much error handling do we want at this stage
        if (file_ptr == nullptr) {
            exit_with_msg("file_ptr cannot be null");
        }
        fp = file_ptr;
        // Pre-processing step. Obtain number of columns and the schema
        // of the SoR file
        parse_schema(fp);
    }
    
    /* Return the chunk, in the form of a DataFrame, corresponding to the
       given chunk_id. The chunk_id corresponds to the chunk of the file, 
       in order. The id 0 should always be valid and corresponds to the 
       first chunk. */ 
    DataFrame* get_chunk_as_df(size_t chunk_id) {
        
    }

    // Obtain the schema from the first 500 lines of the file
    void parse_schema() {
        num_columns = count_cols();

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
        for (size_t i = 0; i < num_cols; i++) {
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
        for (size_t i = 0; i < num_cols; i++) {
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



