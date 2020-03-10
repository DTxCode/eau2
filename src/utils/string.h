#pragma once
// LANGUAGE: CwC
#include <cassert>
#include <cstring>
#include <string>
#include "object.h"

/** An immutable string class that wraps a character array.
 * The character array is zero terminated. The size() of the
 * String does count the terminator character. Most operations
 * work by copy, but there are exceptions (this is mostly to support
 * large strings and avoid them being copied).
 *  author: vitekj@me.com */
class String : public Object {
   public:
    size_t size_;  // number of characters excluding terminate (\0)
    char* cstr_;   // owned; char array

    /** Build a string from a string constant */
    String(char const* cstr, size_t len) {
        size_ = len;
        cstr_ = new char[size_ + 1];
        memcpy(cstr_, cstr, size_ + 1);
        cstr_[size_] = 0;  // terminate
    }
    /** Builds a string from a char*, steal must be true, we do not copy!
     *  cstr must be allocated for len+1 and must be zero terminated. */
    String(bool steal, char* cstr, size_t len) {
        assert(steal && cstr[len] == 0);
        size_ = len;
        cstr_ = cstr;
    }

    String(char const* cstr) : String(cstr, strlen(cstr)) {}

    /** Build a string from another String */
    String(String& from) : Object(from) {
        size_ = from.size_;
        cstr_ = new char[size_ + 1];  // ensure that we copy the terminator
        memcpy(cstr_, from.cstr_, size_ + 1);
    }

    /** Delete the string */
    ~String() { delete[] cstr_; }

    /** Return the number characters in the string (does not count the terminator) */
    size_t size() { return size_; }

    /** Return the raw char*. The result should not be modified or freed. */
    char* c_str() { return cstr_; }

    /** Returns the character at index */
    char at(size_t index) {
        assert(index < size_);
        return cstr_[index];
    }

    /** Compare two strings. */
    bool equals(Object* other) {
        if (other == this) return true;
        String* x = dynamic_cast<String*>(other);
        if (x == nullptr) return false;
        if (size_ != x->size_) return false;
        return strncmp(cstr_, x->cstr_, size_) == 0;
    }

    /** Deep copy of this string */
    String* clone() { return new String(*this); }

    // Returns copy of the internal raw char*
    virtual char* get_string() {
        size_t s_length = strlen(cstr_);
        char* tmp_string = new char[s_length + 1];
        strcpy(tmp_string, cstr_);
        return tmp_string;
    }

    /** This consumes cstr_, the String must be deleted next */
    char* steal() {
        char* res = cstr_;
        cstr_ = nullptr;
        return res;
    }

    /** Compute a hash for this string. */
    size_t hash_me() {
        size_t hash = 0;
        for (size_t i = 0; i < size_; ++i)
            hash = cstr_[i] + (hash << 6) + (hash << 16) - hash;
        return hash;
    }

    // returns concatenation of given string to this string
    virtual String* concat(String* other_string) {
        size_t my_string_length = size();
        size_t other_string_length = other_string->size();

        char new_string[my_string_length + other_string_length + 1];
        strcpy(new_string, cstr_);
        strcat(new_string, other_string->cstr_);

        const char* const_new_string = new_string;

        return new String(const_new_string);
    }

    // returns concatenation of given string to this string
    virtual String* concat(char* other_string) {
        size_t my_string_length = size();
        size_t other_string_length = strlen(other_string);

        char new_string[my_string_length + other_string_length + 1];
        strcpy(new_string, cstr_);
        strcat(new_string, other_string);

        const char* const_new_string = new_string;

        return new String(const_new_string);
    }
};

/** A string buffer builds a string from various pieces.
 *  author: jv */
class StrBuff : public Object {
   public:
    char* val_;  // owned; consumed by get()
    size_t capacity_;
    size_t size_;

    StrBuff() {
        val_ = new char[capacity_ = 10];
        size_ = 0;
    }
    void grow_by_(size_t step) {
        if (step + size_ < capacity_) return;
        capacity_ *= 2;
        if (step + size_ >= capacity_) capacity_ += step;
        char* oldV = val_;
        val_ = new char[capacity_];
        memcpy(val_, oldV, size_);
        delete[] oldV;
    }
    StrBuff& c(const char* str) {
        size_t step = strlen(str);
        grow_by_(step);
        memcpy(val_ + size_, str, step);
        size_ += step;
        return *this;
    }
    StrBuff& c(String& s) { return c(s.c_str()); }
    StrBuff& c(size_t v) { return c(std::to_string(v).c_str()); }  // Cpp

    String* get() {
        assert(val_ != nullptr);  // can be called only once
        grow_by_(1);              // ensure space for terminator
        val_[size_] = 0;          // terminate
        String* res = new String(true, val_, size_);
        val_ = nullptr;  // val_ was consumed above
        return res;
    }
};
