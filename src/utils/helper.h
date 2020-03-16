#pragma once
//lang::Cpp

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <climits>
#include <ctime>
#include <iostream>

/** Helper class providing some C++ functionality and convenience
 *  functions. This class has no data, constructors, destructors or
 *  virtual functions. Inheriting from it is zero cost.
 */
class Sys {
   public:
    // Printing functions
    Sys& p(char* c) {
        std::cout << c;
        return *this;
    }
    Sys& p(bool c) {
        std::cout << c;
        return *this;
    }
    Sys& p(float c) {
        std::cout << c;
        return *this;
    }
    Sys& p(int i) {
        std::cout << i;
        return *this;
    }
    Sys& p(size_t i) {
        std::cout << i;
        return *this;
    }
    Sys& p(const char* c) {
        std::cout << c;
        return *this;
    }
    Sys& p(char c) {
        std::cout << c;
        return *this;
    }
    Sys& pln() {
        std::cout << "\n";
        return *this;
    }
    Sys& pln(int i) {
        std::cout << i << "\n";
        return *this;
    }
    Sys& pln(char* c) {
        std::cout << c << "\n";
        return *this;
    }
    Sys& pln(bool c) {
        std::cout << c << "\n";
        return *this;
    }
    Sys& pln(char c) {
        std::cout << c << "\n";
        return *this;
    }
    Sys& pln(float x) {
        std::cout << x << "\n";
        return *this;
    }
    Sys& pln(size_t x) {
        std::cout << x << "\n";
        return *this;
    }
    Sys& pln(const char* c) {
        std::cout << c << "\n";
        return *this;
    }

    // Copying strings
    char* duplicate(const char* s) {
        char* res = new char[strlen(s) + 1];
        strcpy(res, s);
        return res;
    }
    char* duplicate(char* s) {
        char* res = new char[strlen(s) + 1];
        strcpy(res, s);
        return res;
    }

    // Counts frequency of given char in the given string
    size_t count_char(const char* c, char* string) {
        size_t count = 0;
        size_t length = strlen(string);

        for (size_t i = 0; i < length; i++) {
            if (*c == string[i]) {
                count++;
            }
        }

        return count;
    }

    // Function to terminate execution with a message
    void exit_if_not(bool b, char* c) {
        if (b) return;
        p("Exit message: ").pln(c);
        exit(-1);
    }

    // Definitely fail
    //  void FAIL() {
    void myfail() {
        pln("Failing");
        exit(1);
    }

    // Some utilities for lightweight testing
    void OK(const char* m) { pln(m); }
    void t_true(bool p) {
        if (!p) myfail();
    }
    void t_false(bool p) {
        if (p) myfail();
    }
};

void pln(const char* c) {
    std::cout << c << "\n";
}

void exit_with_msg(const char* c) {
    pln(c);
    exit(1);
}

// Helper to compare strings so that we can avoid direct library calls
bool equal_strings(const char* s1, const char* s2) {
    return strcmp(s1, s2) == 0;
}

// Helper to convert the given string to an unsigned int
// returns the unsigned int it finds one, or -1 if it 'fails'
// Fails if string is not an int, its negative, or its too big
long string_to_int(char* int_str) {
    int temp;
    char* endpt;  // For use in strtol func
    temp = strtol(int_str, &endpt, 10);
    if (endpt == int_str) {  // Test if arg is an integer
        return -1;
    }
    temp = atoi(int_str);  // Convert string to int
    if (temp < 0) {
        return -1;
    }
    if (temp > INT_MAX) {
        return -1;
    }
    return temp;
}

// Utility function to get time in milliseconds as a double
double time_now() {
    auto time = std::chrono::high_resolution_clock::now();
    auto duration_time = std::chrono::duration<double>(time.time_since_epoch());
    return duration_time.count();
}

// Helper to remove trailing/leading whitespace from given string
// FOUND ONLINE AT:
// - LINK: https://cboard.cprogramming.com/c-programming/143810-removing-leading-whitespace-line.html
// - DATE: 1/19/20
char* trim_whitespace(char* str) {
    // Trim leading whitespace
    while (isspace(*str)) {
        str++;
    }
    char* end = str + strlen(str) - 1;
    // Trim trailing whitespace
    while ((end > str) && (isspace(*end))) {
        end--;
    }
    // Guarantee str is null-terminated
    *(end + 1) = '\0';

    return str;
}

// Helper to check if a given char array contains a spefic char c
bool contains_char(char* array, char c) {
    for (size_t i = 0; i < strlen(array); i++) {
        if (c == array[i]) {
            return true;
        }
    }

    return false;
}
