#pragma once
#include "../../utils/object.h"
#include "../../utils/string.h"
#include "stdlib.h"

/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row.
 */
class Fielder : public Object {
   public:
    /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
    virtual void start(size_t r) = 0;

    /** Called for fields of the argument's type with the value of the field. */
    virtual void accept(bool b) = 0;
    virtual void accept(float f) = 0;
    virtual void accept(int i) = 0;
    virtual void accept(String* s) = 0;

    /** Called when all fields have been seen. */
    virtual void done() = 0;
};

/*****************************************************************************
 * CountFielder::
 * A fielder for testing.
 * Counts the number of fields it's seen.
 * Exposes this value via get_count()
 */
class CountFielder : public Fielder {
   public:
    size_t count;

    CountFielder() {
        count = 0;
    }

    /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
    void start(size_t r) {
        // empty
    }

    /** Called for fields of the argument's type with the value of the field. */
    void accept(bool b) { count++; }
    void accept(float f) { count++; }
    void accept(int i) { count++; }
    void accept(String* s) { count++; }

    /** Called when all fields have been seen. */
    void done() {
        // empty
    }

    size_t get_count() {
        return count;
    }
};

/*****************************************************************************
 * IntSumFielder::
 * A fielder for testing.
  * Sums the asbolute value of int fields it's seen.
 * Exposes this value via get_sum()
 */
class IntSumFielder : public Fielder {
   public:
    size_t sum;

    IntSumFielder() {
        sum = 0;
    }

    /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
    void start(size_t r) {
        // empty
    }

    /** Called for fields of the argument's type with the value of the field. */
    void accept(bool b) { return; }
    void accept(float f) { return; }
    void accept(int i) { sum += abs(i); }
    void accept(String* s) { return; }

    /** Called when all fields have been seen. */
    void done() {
        // empty
    }

    size_t get_sum() {
        return sum;
    }
};

/*****************************************************************************
 * TrueCountFielder::
 * A fielder for testing.
 * Counts the number of boolean True fields it's seen.
 * Exposes this value via get_count()
 */
class TrueCountFielder : public Fielder {
   public:
    size_t count;

    TrueCountFielder() {
        count = 0;
    }

    /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
    void start(size_t r) {
        // empty
    }

    /** Called for fields of the argument's type with the value of the field. */
    void accept(bool b) {
        if (b) count++;
    }
    void accept(float f) {
        // empty
    }
    void accept(int i) {
        // empty
    }
    void accept(String* s) {
        // empty
    }

    /** Called when all fields have been seen. */
    void done() {
        // empty
    }

    size_t get_count() {
        return count;
    }
};

/*****************************************************************************
 * MaxSeenFielder::
 * A fielder for testing.
 * Tracks the max int value it has seen.
 * Exposes this value via get_max()
 */
class MaxSeenFielder : public Fielder {
   public:
    int max;

    MaxSeenFielder() {
        max = 0;
    }

    /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
    void start(size_t r) {
        // empty
    }

    /** Called for fields of the argument's type with the value of the field. */
    void accept(bool b) {
        // empty
    }
    void accept(float f) {
        // empty
    }
    void accept(int i) {
        if (i > max) {
            max = i;
        }
    }
    void accept(String* s) {
        // empty
    }

    /** Called when all fields have been seen. */
    void done() {
        // empty
    }

    int get_max() {
        return max;
    }
};
