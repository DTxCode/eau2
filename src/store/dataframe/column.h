#pragma once
#include <stdarg.h>  // va_arg
#include <stdlib.h>
#include "../../utils/object.h"
#include "../../utils/string.h"

#define INT_TYPE 'I'
#define BOOL_TYPE 'B'
#define FLOAT_TYPE 'F'
#define STRING_TYPE 'S'

class IntColumn;
class BoolColumn;
class FloatColumn;
class StringColumn;

/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality. Columns may have missing values, but the missing counts as
 * a value. Missings have default values for each type, but their value
 * has no meaning other than to maintain the type of the element. If 
 * missing values are present, they are counted in the size of column.
 * Missing values are different than unoccupied cells. */
class Column : public Object {
   public:
    size_t length;
    size_t capacity;
    bool* missings;

    // Initialize missings array. Allocate memory and fill with all 'false'
    void init_missings() {
        missings = new bool[capacity];
        for (size_t i = 0; i < capacity; i++) {
            missings[i] = false;
        }
    }
    
    // Return whether the element at the given value is a missing value
    // Undefined behavior if the idx is out of bounds
    bool is_missing(size_t idx) {
        return missings[idx];
    }

    // Assumes capacity has changed. Reallocate missings array and copy
    // missings values
    void resize_missings() {
        bool* new_missings = new bool[capacity];
        for (int i = 0; i < length; i++) {
            new_missings[i] = missings[i];
        }
        delete[] missings;
        missings = new_missings;
    }
        
    /** Type converters: Return same column under its actual type, or
   *  nullptr if of the wrong type.  */
    virtual IntColumn* as_int() { return nullptr; }
    virtual BoolColumn* as_bool() { return nullptr; }
    virtual FloatColumn* as_float() { return nullptr; }
    virtual StringColumn* as_string() { return nullptr; }

    /** Type appropriate push_back methods. Calling the wrong method is
    * undefined behavior. **/
    virtual void push_back(int val) { return; }
    virtual void push_back(bool val) { return; }
    virtual void push_back(float val) { return; }
    virtual void push_back(String* val) { return; }
    // Adds a missing with a default value dependent on the type of the 
    // column 
    virtual void push_back_missing() { return; }

    /** Returns the number of elements in the column. */
    virtual size_t size() { return length; }

    /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.**/
    char get_type() {
        if (nullptr != this->as_int()) {
            return INT_TYPE;
        }
        if (nullptr != this->as_bool()) {
            return BOOL_TYPE;
        }
        if (nullptr != this->as_float()) {
            return FLOAT_TYPE;
        }
        // At this point, must be STRING_TYPE
        return STRING_TYPE;
    }
};

/*************************************************************************
 * IntColumn::
 * Holds int values.
 */
class IntColumn : public Column {
   public:
    int** cells;

    // Create empty int column
    IntColumn() {
        capacity = 10;  // Default of 10 cells
        cells = new int*[capacity];
        length = 0;
        init_missings();
    }

    // Create int column with n entries, listed in order in
    // the succeeding parameters
    IntColumn(int n, ...) {
        capacity = 10;
        // Always make enough space
        if (n > capacity) {
            capacity = n;
        }
        cells = new int*[capacity];
        length = 0;
        va_list vl;
        va_start(vl, n);
        int val;
        for (int i = 0; i < n; i++) {
            val = va_arg(vl, int);
            push_back(val);
        }
        va_end(vl);
        init_missings();
    }

    // Copy constructor. Assumes other column is the same type as this one
    IntColumn(Column* col) {
        capacity = 10;  // Default of 10 cells
        cells = new int*[capacity];
        length = 0;
        init_missings();

        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            // Track other columns missings
            if (col->is_missing(row_idx)) {
                missings[row_idx] = true;
            }
            int row_val = col->as_int()->get(row_idx);
            push_back(row_val);
        }
    }

    // Delete column array and int pointers
    ~IntColumn() {
        for (int i = 0; i < length; i++) {
            delete cells[i];
        }
        delete[] missings;
        delete[] cells;
    }


    // Returns the integer at the given index.
    // Input index out of bounds will cause a runtime error
    int get(size_t idx) {
        return *cells[idx];  // Dereference integer pointer
    }

    // Returns this column as an integer column
    IntColumn* as_int() {
        return this;
    }

    /** Set value at idx. An out of bound idx is undefined.  */
    void set(size_t idx, int val) {
        if (idx >= length) {
            return;
        }
        // Update missing bitmap
        if (missings[idx]) {
            missings[idx] = false;
        }
        *cells[idx] = val;
    }

    // Double the capacity of the array and move (not copy) the integer
    // pointers in to new array
    void resize() {
        capacity = 2 * capacity;
        int** new_cells = new int*[capacity];
        for (int i = 0; i < length; i++) {
            new_cells[i] = cells[i];
        }
        delete[] cells;
        cells = new_cells;
        resize_missings();
    }

    // Add integer to "bottom" of column
    void push_back(int val) {
        if (length == capacity) {
            resize();
        }
        int* new_val = new int(val);
        cells[length] = new_val;
        length++;
    }

    // Add "missing" (0) to bottom of column 
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        int* new_val = new int(0);
        cells[length] = new_val;
        missings[length] = true;
        length++;
    }

};

/*************************************************************************
 * FloatColumn::
 * Holds float values.
 */
class FloatColumn : public Column {
   public:
    float** cells;

    // Create empty column with default capacity 10
    FloatColumn() {
        capacity = 10;
        cells = new float*[capacity];
        length = 0;
        init_missings();
    }

    // Create column with n floats as starting values, in order given
    FloatColumn(int n, ...) {
        capacity = 10;
        if (n > capacity) {
            capacity = n;
        }  // Always make enough space
        cells = new float*[capacity];
        length = 0;
        va_list vl;
        va_start(vl, n);
        float val;
        for (int i = 0; i < n; i++) {
            val = va_arg(vl, double);
            push_back(val);
        }
        va_end(vl);
        init_missings();
    }

    // Copy constructor. Assumes other column is the same type as this one
    FloatColumn(Column* col) {
        capacity = 10;  // Default of 10 cells
        cells = new float*[capacity];
        length = 0;
        init_missings();

        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            // Track other columns missings
            if (col->is_missing(row_idx)) {
                missings[row_idx] = true;
            }
            float row_val = col->as_float()->get(row_idx);
            push_back(row_val);
        }
    }

    // Delete column array and float pointers
    ~FloatColumn() {
        for (int i = 0; i < length; i++) {
            delete cells[i];
        }
        delete[] cells;
        delete[] missings;
    }

    // Double the capacity of the array and move (not copy) the float
    // pointers in to new array
    void resize() {
        capacity = 2 * capacity;
        float** new_cells = new float*[capacity];
        for (int i = 0; i < length; i++) {
            new_cells[i] = cells[i];
        }
        delete[] cells;
        cells = new_cells;
        resize_missings();
    }

    // Get float at index idx. Runtime error if idx is out of bounds
    float get(size_t idx) {
        return *cells[idx];
    }

    // Add given float to "bottom" of the column.
    void push_back(float val) {
        if (length == capacity) {
            resize();
        }
        float* new_val = new float(val);
        cells[length] = new_val;
        length++;
    }

    // Return this column as a FloatColumn
    FloatColumn* as_float() { return this; }

    /** Set value at idx. An out of bound idx is undefined.  */
    void set(size_t idx, float val) {
        if (idx >= size()) {
            return;
        }
        // Update missing bitmap
        if (missings[idx]) {
            missings[idx] = false;
        }
        *cells[idx] = val;
    }
    
    // Add "missing" (0.0) to bottom of column 
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        float* new_val = new float(0.0);
        cells[length] = new_val;
        missings[length] = true;
        length++;
    }
};

/*************************************************************************
 * BoolColumn::
 * Holds bool values.
 */
class BoolColumn : public Column {
   public:
    bool** cells;

    // Create empty column with default capacity 10
    BoolColumn() {
        capacity = 10;
        cells = new bool*[capacity];
        length = 0;
        init_missings();
    }

    // Create column with n bools as starting values, in order given
    BoolColumn(int n, ...) {
        capacity = 10;
        if (n > capacity) {
            capacity = n;
        }  // Always make enough space
        cells = new bool*[capacity];
        length = 0;
        va_list vl;
        va_start(vl, n);
        bool val;
        for (int i = 0; i < n; i++) {
            val = va_arg(vl, int);
            push_back(val);
        }
        va_end(vl);
        init_missings();
    }

    // Copy constructor. Assumes other column is the same type as this one
    BoolColumn(Column* col) {
        capacity = 10;  // Default of 10 cells
        cells = new bool*[capacity];
        length = 0;

        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            // Track other columns missings
            if (col->is_missing(row_idx)) {
                missings[row_idx] = true;
            }
            bool row_val = col->as_bool()->get(row_idx);
            push_back(row_val);
        }
    }

    // Delete column array and bool pointers
    ~BoolColumn() {
        for (int i = 0; i < length; i++) {
            delete cells[i];
        }
        delete[] cells;
        delete[] missings;
    }

    // Double the capacity of the array and move (not copy) the bool
    // pointers in to new array
    void resize() {
        capacity = 2 * capacity;
        bool** new_cells = new bool*[capacity];
        for (int i = 0; i < length; i++) {
            new_cells[i] = cells[i];
        }
        delete[] cells;
        cells = new_cells;
        resize_missings();
    }

    // Get bool at index idx.
    // Runtime error if idx is out of bounds
    bool get(size_t idx) {
        return *cells[idx];
    }

    // Return this column as a BoolColumn
    BoolColumn* as_bool() { return this; }

    /** Set value at idx. An out of bound idx is undefined.  */
    void set(size_t idx, bool val) {
        if (idx >= size()) {
            return;
        }
        // Update missing bitmap
        if (missings[idx]) {
            missings[idx] = false;
        }
        *cells[idx] = val;
    }

    // push the given bool on to the 'bottom' of this column
    void push_back(bool val) {
        if (length == capacity) {
            resize();
        }
        bool* new_val = new bool(val);
        cells[length] = new_val;
        length++;
    }
    
    // Add "missing" (true) to bottom of column 
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        bool* new_val = new bool(true);
        cells[length] = new_val;
        missings[length] = true;
        length++;
    }
};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 */
class StringColumn : public Column {
   public:
    String** cells;

    // Create empty column with default capacity 10
    StringColumn() {
        capacity = 10;
        cells = new String*[capacity];
        length = 0;
        init_missings();
    }

    // Create column with n String*s as starting values, in order given
    StringColumn(int n, ...) {
        capacity = 10;
        if (n > capacity) {
            capacity = n;
        }  // Always make enough space
        cells = new String*[capacity];
        length = 0;
        va_list vl;
        va_start(vl, n);
        String* val;
        for (int i = 0; i < n; i++) {
            val = va_arg(vl, String*);
            push_back(val);
        }
        va_end(vl);
        init_missings();
    }

    // Copy constructor. Assumes other column is the same type as this one
    StringColumn(Column* col) {
        capacity = 10;  // Default of 10 cells
        cells = new String*[capacity];
        length = 0;

        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            // Track other columns missings
            if (col->is_missing(row_idx)) {
                missings[row_idx] = true;
            }
            String* row_val = col->as_string()->get(row_idx);
            push_back(row_val);
        }
    }

    ~StringColumn() {
        // don't delete actual string* because they're not ours
        delete[] cells;
        delete[] missings;
    }

    // Double the capacity of the array and move (not copy) the String
    // pointers in to new array
    void resize() {
        capacity = 2 * capacity;
        String** new_cells = new String*[capacity];
        for (int i = 0; i < length; i++) {
            new_cells[i] = cells[i];
        }
        delete[] cells;
        cells = new_cells;
        resize_missings();
    }

    // Get String* at index idx.
    // Runtime error if idx is out of bounds
    String* get(size_t idx) {
        return cells[idx];
    }

    // Add given String* on to 'bottom' of this column
    void push_back(String* val) {
        if (length == capacity) {
            resize();
        }
        cells[length] = val;
        length++;
    }

    // Return this column as a StringColumn
    StringColumn* as_string() { return this; }

    /** Out of bound idx is undefined. */
    void set(size_t idx, String* val) {
        if (idx >= size()) {
            return;
        }
        // Update missing bitmap
        if (missings[idx]) {
            missings[idx] = false;
        }
        cells[idx] = val;
    }
    
    // Add "missing" ("") to bottom of column 
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        String* new_val = new String("");
        cells[length] = new_val;
        missings[length] = true;
        length++;
    }
};
