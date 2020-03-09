// lang:CwC
// Authors: Ryan Heminway (heminway.r@husky.neu.edu), David Tandetnik

#include "dataframe.h"
#include "helper.h"
#include "object.h"
#include "string.h"

/*
* Program to parse SoR (schema on read) file and react to command-line
* queries. Possbile command-line arguments and flags are:
* USAGE:
* -f (string) : specify a real, existing path to SoR file to read as a string
* -from (uint) : specify the starting position, in bytes, in the file
* -len (uint) : specify number of bytes to read
* -print_col_type (uint) : request the type of the given column number
    (BOOl, INT, FLOAT, STRING)
* -print_col_idx (uint uint) : request the value at (column index, offset)
* -is_missing_idx (uint uint) : request to check if given (column_index, offset) has 
    a missing value 
*
* Only a SINGLE query is allowed per execution, as seen in design spec
* Similarly, each flag can appear once only
*/
int main(int argc, char** argv) {
    size_t num_flags = 6;   // Number of flags accepted by this program
    FILE* fp;               // File to read from
    size_t from = 0;        // Starting pos in file (in bytes)
    size_t length = 0;      // Length (in bytes) to read
    size_t col_idx = 0;     // Column index for queries
    size_t col_offset = 0;  // Column offset for queries
    // Tracker array for whether the flag has been seen
    // Flags are in order that they are parsed, ex:
    // seen_cmds[0] represents if '-f' flag has been seen
    bool seen_cmds[num_flags] = {0};
    // Tracker # of queries
    size_t queries = 0;

    // Parse command-line arguments
    for (int i = 1; i < argc; i++) {
        char* arg = argv[i];
        // Protect against missing arguments
        if (i + 1 >= argc) {
            exit_with_msg("ERROR: flag must be followed by valid # of args");
        }
        // File specifier
        if (equal_strings(arg, "-f")) {
            if (seen_cmds[0]) {
                exit_with_msg("ERROR: repeated command-line flags");
            }
            // Immediately try to open file
            fp = fopen(argv[i + 1], "r");
            if (fp == nullptr) {
                exit_with_msg("Failed to open file");
            }
            seen_cmds[0] = 1;
            i++;                                   // Skip argument
        } else if (equal_strings(arg, "-from")) {  // From specifier
            if (seen_cmds[1]) {
                exit_with_msg("ERROR: repeated command-line flags");
            }
            from = string_to_int(argv[i + 1]);  // Attempt to convert to size_t
            if (from < 0) {
                exit_with_msg("ERROR: unsigned int arg is malformed");
            }
            seen_cmds[1] = 1;
            i++;
        } else if (equal_strings(arg, "-len")) {  // Length specified
            if (seen_cmds[2]) {
                exit_with_msg("ERROR: repeated command-line flags");
            }
            length = string_to_int(argv[i + 1]);  // Attempt to convert to size_t
            if (length < 0) {
                exit_with_msg("ERROR: unsigned int arg is malformed");
            }
            seen_cmds[2] = 1;
            i++;
        } else if (equal_strings(arg, "-print_col_type")) {  // Column type request
            if (seen_cmds[3]) {
                exit_with_msg("ERROR: repeated command-line flags");
            }
            col_idx = string_to_int(argv[i + 1]);  // Attempt to convert to size_t
            if (col_idx < 0) {
                exit_with_msg("ERROR: unsigned int arg is malformed");
            }
            seen_cmds[3] = 1;
            i++;
            queries++;
        } else if (equal_strings(arg, "-print_col_idx")) {
            if (seen_cmds[4]) {
                exit_with_msg("ERROR: repeated command-line flags");
            }
            if (i + 2 >= argc) {
                exit_with_msg("ERROR: flag must be followed by valid # of args");
            }
            col_idx = string_to_int(argv[i + 1]);  // Attempt to convert to size_t
            if (col_idx < 0) {
                exit_with_msg("ERROR: unsigned int arg is malformed");
            }
            col_offset = string_to_int(argv[i + 2]);
            if (col_offset < 0) {
                exit_with_msg("ERROR: unsigned int arg is malformed");
            }
            seen_cmds[4] = 1;
            i = i + 2;
            queries++;
        } else if (equal_strings(arg, "-is_missing_idx")) {
            if (seen_cmds[5]) {
                exit_with_msg("ERROR: repeated command-line flags");
            }
            if (i + 2 >= argc) {
                exit_with_msg("ERROR: flag must be followed by valid # of args");
            }
            col_idx = string_to_int(argv[i + 1]);  // Attempt to convert to size_t
            if (col_idx < 0) {
                exit_with_msg("ERROR: unsigned int arg is malformed");
            }
            col_offset = string_to_int(argv[i + 2]);
            if (col_offset < 0) {
                exit_with_msg("ERROR: unsigned int arg is malformed");
            }
            seen_cmds[5] = 1;
            i = i + 2;
            queries++;
        } else {  // Unexpected argument
            exit_with_msg("ERROR: Unexpected argument or character");
        }
        if (queries > 1) {
            exit_with_msg("ERROR: Only 1 queries at a time is allowed");
        }
    }
    // Check that they gave us a file, a 'from' point, and a length
    if (!seen_cmds[0]) {
        exit_with_msg("ERROR: no SoR file specified with -f");
    }

    // check that '-from' point is within the file
    fseek(fp, 0, SEEK_END);
    size_t file_size = ftell(fp);
    if (from > file_size) {
        exit_with_msg("ERROR: -from point is past end of the file");
    }
    if (!seen_cmds[2]) {  // User did not specify a length, read whole file
        length = file_size + 1;
    }
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

    // return to beginning of file
    fseek(fp, 0, SEEK_SET);

    // Sanity check the requested column_idx and offset
    if (col_idx >= max_fields) {
        exit_with_msg("ERROR: Requested col idx out of bounds");
    }

    // Create the dataframe from the sor file, the number of fields in the
    // schema, and the from and length
    Dataframe* df = new Dataframe(fp, max_fields, from, length);

    if (seen_cmds[3]) {  // print_col_type query
        pln(df->get_col_type(col_idx));
    } else if (seen_cmds[4]) {  // print_col_idx query
        pln(df->get_value_at(col_idx, col_offset));
    } else if (seen_cmds[5]) {  // is_missing_idx query
        pln(df->missing_at(col_idx, col_offset));
    }

    delete df;
    fclose(fp);

    return 0;
}
