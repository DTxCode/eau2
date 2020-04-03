/* Authors: Ryan Heminway (heminway.r@husky.neu.edu)
*           David Tandetnik (tandetnik.da@husky.neu.edu) */
#pragma once
#include <stdlib.h>
#include <iostream>
#include "../utils/helper.h"

/* Class for parsing command line arguments for a Sorer/Application
    * USAGE:
    * -f (string, required) : specify a  path to SoR file to read 
    * -from (uint, optional, default=0) : specify the starting position, in bytes, in the file
    * -len (uint, optional, default=end of file) : specify number of bytes to read
*/
class Arguments {
   public:
    FILE* input_file;
    size_t from;
    size_t length;

    Arguments(int argc, char** argv) {
        input_file = nullptr;  // File to read
        from = 0;              // Starting pos in file (in bytes)
        length = 0;            // Length (in bytes) to read

        // Tracker array for whether the flag has been seen
        // Flags are in order that they are parsed:
        //  - seen_cmds[0] represents if '-f' flag has been seen
        //  - seen_cmds[1] represents if '-from' flag has been seen
        //  - seen_cmds[2] represents if '-len' flag has been seen
        int num_flags = 3;
        bool seen_cmds[num_flags] = {0};

        // Parse command-line arguments
        for (int i = 1; i < argc; i++) {
            char* arg = argv[i];

            // Protect against missing arguments
            if (i + 1 >= argc) {
                exit_with_msg("ERROR: flag must be followed by valid # of args");
            }

            if (equal_strings(arg, "-f")) {  // File specifier
                if (seen_cmds[0]) {
                    exit_with_msg("ERROR: repeated command-line flag '-f'");
                }

                // Immediately try to open file
                input_file = fopen(argv[i + 1], "r");
                if (input_file == nullptr) {
                    exit_with_msg("Failed to open file");
                }

                seen_cmds[0] = 1;
                i++;
            } else if (equal_strings(arg, "-from")) {  // From specifier
                if (seen_cmds[1]) {
                    exit_with_msg("ERROR: repeated command-line flag '-from'");
                }

                from = string_to_int(argv[i + 1]);  // Attempt to convert to size_t
                if (from < 0) {
                    exit_with_msg("ERROR: unsigned int arg for -from is malformed");
                }

                seen_cmds[1] = 1;
                i++;
            } else if (equal_strings(arg, "-len")) {  // Length specified
                if (seen_cmds[2]) {
                    exit_with_msg("ERROR: repeated command-line flag '-len'");
                }

                length = string_to_int(argv[i + 1]);  // Attempt to convert to size_t
                if (length < 0) {
                    exit_with_msg("ERROR: unsigned int arg for -len is malformed");
                }

                seen_cmds[2] = 1;
                i++;
            } else {  // Unexpected argument
                exit_with_msg("ERROR: Unexpected argument or character");
            }
        }

        // Check that they gave us a file, a 'from' point, and a length
        if (!input_file) {
            exit_with_msg("ERROR: no SoR file specified with -f");
        }

        // check that '-from' point is within the file
        fseek(input_file, 0, SEEK_END);
        size_t file_size = ftell(input_file);
        if (from > file_size) {
            exit_with_msg("ERROR: -from point is past end of the file");
        }

        // User did not specify a length, read whole file
        if (length == 0) {
            length = file_size + 1;
        }
    }
};
