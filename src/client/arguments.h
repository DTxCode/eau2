/* Authors: Ryan Heminway (heminway.r@husky.neu.edu)
*           David Tandetnik (tandetnik.da@husky.neu.edu) */
#pragma once
#include <stdlib.h>
#include <iostream>
#include "../utils/helper.h"

/* Arguments class to process command-line arguments for Linus application.
 * Processes arguments for node ports, addresses, number of nodes, and how
 * many degrees of linus to compute. */
class Arguments {
    public:
        bool start_server;
        char* master_ip;
        int master_port;

        size_t num_nodes;
        int node_id;
        char* node_ip;
        int node_port;

        int degrees;
        char* proj_file;
        char* users_file;
        char* commits_file;

        Arguments(int argc, char** argv) {
            // defaults
            start_server = 0;
            master_ip = (char*) "127.0.0.1";
            master_port = 4444;

            num_nodes = 1;
            node_id = -1; // TODO enforce this is set
            node_ip = (char*) "127.0.0.1";
            node_port = 0; // TODO enforce this is set

            degrees = 1;
            proj_file = (char*) "data/projects_med.sor";
            users_file = (char*) "data/users_med.sor";
            commits_file = (char*) "data/commits_med.sor";

            for (int i = 1; i < argc; i++) {
                char* flag_name = argv[i];
                char* flag_value = argv[i+1];

                if (equal_strings(flag_name, "-start_server")) {
                    start_server = atoi(flag_value); // Note: can't tell between valid 0 and error 0 here

                } else if (equal_strings(flag_name, "-master_ip")) {
                    master_ip = flag_value;

                } else if (equal_strings(flag_name, "-master_port")) {
                    master_port = atoi(flag_value);
                    if (master_port == 0) exit_with_msg("ERROR: invalid master_port");

                } else if (equal_strings(flag_name, "-num_nodes")) {
                    num_nodes = atoi(flag_value);
                    
                } else if (equal_strings(flag_name, "-node_id")) {
                    node_id = atoi(flag_value); // Note can't tell between valid 0 and error 0 here

                } else if (equal_strings(flag_name, "-node_ip")) {
                    node_ip = flag_value;

                } else if (equal_strings(flag_name, "-node_port")) {
                    node_port = atoi(flag_value);
                    if (node_port == 0) exit_with_msg("ERROR: invalid node_port");

                } else if (equal_strings(flag_name, "-degrees")) {
                    degrees = atoi(flag_value);

                } else if (equal_strings(flag_name, "-proj_file")) {
                    proj_file = flag_value;

                } else if (equal_strings(flag_name, "-users_file")) {   
                    users_file = flag_value;

                } else if (equal_strings(flag_name, "-commits_file")) {
                    commits_file = commits_file;

                } else {
                    exit_with_msg("ERROR: Unknown flag given");
                }


                i++;
            }
        }
};


/* Class for parsing command line arguments for a Sorer/Application
    * USAGE:
    * -f (string, required) : specify a  path to SoR file to read 
    * -from (uint, optional, default=0) : specify the starting position, in bytes, in the file
    * -len (uint, optional, default=end of file) : specify number of bytes to read
*/
class SorerArguments {
   public:
    FILE* input_file;
    size_t from;
    size_t length;

    SorerArguments(int argc, char** argv) {
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
