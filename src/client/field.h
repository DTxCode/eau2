// Lang : CwC
// Authors : Ryan Heminway (heminway.r@husky.neu.edu)
//           David Tandetnik (tandetnik.da@husky.neu.edu)

#pragma once

#include <ctype.h>
#include <string.h>

enum FIELD_TYPE { STRING,
                  INT,
                  FLOAT,
                  BOOL,
                  EMPTY,
                  ERROR };

// Assume input is trimmed of whitespaces
// Returns the FIELD_TYPE associated with this value
// If the value is invalid, returns ERROR FIELD_TYPE
FIELD_TYPE parse_field_type(char* value) {
    size_t len = strlen(value);

    if (len == 0) {
        // missing field
        return EMPTY;
    }

    bool surrounded_by_quotes = value[0] == '\"' && value[len - 1] == '\"';
    // Definitly a string
    if (surrounded_by_quotes) {
        return STRING;
    }

    bool has_letter = 0;  // Track whether the value could be a string
    for (size_t i = 0; i < len; i++) {
        if (value[i] == ' ') {
            return ERROR;
        }
        if (isalpha(value[i])) {
            has_letter = 1;
        }
    }
    if (has_letter) {  //Value has no spaces, and has characters --> STRING
        return STRING;
    }

    // At this point, theres no quotes, no spaces, no letters
    // If value contains a point, it then must be a float
    if (contains_char(value, '.')) {
        return FLOAT;
    }

    // At this point, must be bool or int
    if (len == 1 && (value[0] == '0' || value[0] == '1')) {
        return BOOL;
    }

    // Not a bool, not a float, not a string, contains no spaces
    return INT;
}
