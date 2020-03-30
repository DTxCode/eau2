#pragma once
#include <stdarg.h>  // va_arg
#include <stdlib.h>
#include "../../utils/object.h"
#include "../../utils/string.h"
#include "../key.h"
#include "../store.h"

#define INT_TYPE 'I'
#define BOOL_TYPE 'B'
#define FLOAT_TYPE 'F'
#define STRING_TYPE 'S'
#define INTERNAL_CHUNK_SIZE (size_t)100

/*class Store {
    public:
    
    
void put_(Key* k, bool* bs);
void put_(Key* k, int* is);
size_t this_node();
bool* get_bool_array_(Key* k);
int* get_int_array_(Key* k);
    
    };*/

class IntColumn;
class BoolColumn;
class FloatColumn;
class StringColumn;
class DistributedIntColumn;
class DistributedBoolColumn;
class DistributedFloatColumn;
class DistributedStringColumn;

/** INDEXING MATH
*   ~ Desired value at 'logical' index 114
*   ~ Internal array index that stores this index is FLOOR(114 / INTERNAL_CHUNK_SIZE)
*       ~ Index within that internal array is then 114 MOD INTERNAL_CHUNK_SIZE
*   With INTERNAL_CHUNK_SIZE = 50
*   logical index 114 becomes index 14 in the 3rd internal array (index 2) */

/**************************************************************************
 * Column ::
 * * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. 
 * * Columns are mutable, equality is pointr equality. 
 * * Columns may have missing values, but the missing counts as a value. 
 * Missings have default values for each type, but their value
 * has no meaning other than to maintain the type of the element. Users are expected
 * to check if a cell is missing before using a fetched value. If 
 * missing values are present, they are counted in the size of column.
 * Missing values are different than unoccupied cells. 
 * * Internally, values are stored in an array of arrays. Each of the lowest
 * level arrays are of size INTERNAL_CHUNK_SIZE. Each column has 'num_chunks' 
 * of these internal arrays. This structure allows for resizing witout the need
 * for copying all cell values. Instead pointers to each chunk are simply moved
 * into a new outer array. Missings are stored in the same way. */
class Column : public Object {
   public:
    size_t length;      // Count of values(including missings)
    size_t capacity;    // Count of cells available
    size_t num_chunks;  // Count of how many internal 'chunk' arrays were using
    bool** missings;    // Bitmap tracker for missing values

    // Initialize missings array of arrays
    // Allocate memory and fill with all 'false'
    void init_missings() {
        missings = new bool*[num_chunks];
        for (size_t i = 0; i < num_chunks; i++) {
            missings[i] = new bool[INTERNAL_CHUNK_SIZE];
            for (size_t j = 0; j < INTERNAL_CHUNK_SIZE; j++) {
                missings[i][j] = false;
            }
        }
    }

    // Return whether the element at the given value is a missing value
    // Undefined behavior if the idx is out of bounds
    bool is_missing(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        return missings[array_idx][local_idx];
    }

    // Assumes capacity has changed. Reallocate missings array and copy
    // missings values
    void resize_missings() {
        bool** new_missings = new bool*[num_chunks];
        // Can we avoid initializing first N chunks that were copying?
        for (size_t i = 0; i < num_chunks; i++) {
            new_missings[i] = new bool[INTERNAL_CHUNK_SIZE];
        }
        for (size_t i = 0; i < (length / INTERNAL_CHUNK_SIZE); i++) {
            new_missings[i] = missings[i];
        }
        delete[] missings;
        missings = new_missings;
    }

    // Declare the value at idx as missing, does not change or set a value
    // Out of bounds idx is undefined behavior
    void set_missing(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        missings[array_idx][local_idx] = true;
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
class IntColumn : virtual public Column {
   public:
    int** cells;

    // Create empty int column
    IntColumn() {
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        cells = new int*[num_chunks];
        // Array of integer arrays
        for (size_t i = 0; i < num_chunks; i++) {
            cells[i] = new int[INTERNAL_CHUNK_SIZE];
        }
        length = 0;
        init_missings();
    }

    // Create int column with n entries, listed in order in
    // the succeeding parameters
    IntColumn(int n, ...) {
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        // Always make enough space
        if ((size_t)n > capacity) {
            capacity = n;
            num_chunks = (n / INTERNAL_CHUNK_SIZE) + 1;
        }
        cells = new int*[num_chunks];
        // Array of integer arrays
        for (size_t i = 0; i < num_chunks; i++) {
            cells[i] = new int[INTERNAL_CHUNK_SIZE];
        }
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
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        cells = new int*[num_chunks];
        // Array of integer arrays
        for (size_t i = 0; i < num_chunks; i++) {
            cells[i] = new int[INTERNAL_CHUNK_SIZE];
        }
        length = 0;
        init_missings();
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            int row_val = col->as_int()->get(row_idx);
            push_back(row_val);
            // Track other columns missings
            if (col->is_missing(row_idx)) {
                set_missing(row_idx);
            }
        }
    }

    // Delete column array and int pointers
    virtual ~IntColumn() {
        for (size_t i = 0; i < num_chunks; i++) {
            delete[] cells[i];
            delete[] missings[i];
        }
        delete[] missings;
        delete[] cells;
    }

    // Returns the integer at the given index.
    // Input index out of bounds will cause a runtime error
    virtual int get(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        return cells[array_idx][local_idx];
    }

    // Returns this column as an integer column
    virtual IntColumn* as_int() {
        return this;
    }

    /** Set value at idx. An out of bound idx is undefined.  */
    virtual void set(size_t idx, int val) {
        if (idx >= length) {
            return;
        }
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        // Update missing bitmap
        if (is_missing(idx)) {
            missings[array_idx][local_idx] = false;
        }
        cells[array_idx][local_idx] = val;
    }

    // Double the capacity of the array and move (not copy) the integer
    // pointers in to new array
    virtual void resize() {
        num_chunks = 2 * num_chunks;
        capacity = INTERNAL_CHUNK_SIZE * num_chunks;
        int** new_cells = new int*[num_chunks];
        // Array of integer arrays
        for (size_t i = 0; i < num_chunks; i++) {
            new_cells[i] = new int[INTERNAL_CHUNK_SIZE];
        }
        for (size_t i = 0; i < (length / INTERNAL_CHUNK_SIZE); i++) {
            // Move array pointers (internal arrays not being delete)
            new_cells[i] = cells[i];
        }
        delete[] cells;
        cells = new_cells;
        resize_missings();
    }

    // Add integer to "bottom" of column
    virtual void push_back(int val) {
        if (length == capacity) {
            resize();
        }
        size_t array_idx = length / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = length % INTERNAL_CHUNK_SIZE;
        cells[array_idx][local_idx] = val;
        length++;
    }

    // Add "missing" (0) to bottom of column
    virtual void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        size_t array_idx = length / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = length % INTERNAL_CHUNK_SIZE;
        cells[array_idx][local_idx] = 0;
        missings[array_idx][local_idx] = true;
        length++;
    }
};

/*************************************************************************
 * FloatColumn::
 * Holds float values.
 */
class FloatColumn : virtual public Column {
   public:
    float** cells;

    // Create empty column with default capacity 10
    FloatColumn() {
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        cells = new float*[num_chunks];
        // Array of float arrays
        for (size_t i = 0; i < num_chunks; i++) {
            cells[i] = new float[INTERNAL_CHUNK_SIZE];
        }
        length = 0;
        init_missings();
    }

    // Create column with n floats as starting values, in order given
    FloatColumn(int n, ...) {
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        // Always make enough space
        if ((size_t)n > capacity) {
            capacity = n;
            num_chunks = (n / INTERNAL_CHUNK_SIZE) + 1;
        }
        cells = new float*[num_chunks];
        // Array of float arrays
        for (size_t i = 0; i < num_chunks; i++) {
            cells[i] = new float[INTERNAL_CHUNK_SIZE];
        }
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
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        cells = new float*[num_chunks];
        // Array of float arrays
        for (size_t i = 0; i < num_chunks; i++) {
            cells[i] = new float[INTERNAL_CHUNK_SIZE];
        }
        length = 0;
        init_missings();
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            float row_val = col->as_float()->get(row_idx);
            push_back(row_val);
            // Track other columns missings
            if (col->is_missing(row_idx)) {
                set_missing(row_idx);
            }
        }
    }

    // Delete column array and float pointers
    virtual ~FloatColumn() {
        // TODO fix memory leak
	//for (size_t i = 0; i < num_chunks; i++) {
        //    delete[] cells[i];
        //    delete[] missings[i];
        //}
        //delete[] cells;
        //delete[] missings;
    }

    // Double the capacity of the array and move (not copy) the float
    // pointers in to new array
    void resize() {
        num_chunks = 2 * num_chunks;
        capacity = INTERNAL_CHUNK_SIZE * num_chunks;
        float** new_cells = new float*[num_chunks];
        // Array of float arrays
        for (size_t i = 0; i < num_chunks; i++) {
            new_cells[i] = new float[INTERNAL_CHUNK_SIZE];
        }
        for (size_t i = 0; i < (length / INTERNAL_CHUNK_SIZE); i++) {
            // Move array pointers (internal arrays not being deleted)
            new_cells[i] = cells[i];
        }
        delete[] cells;
        cells = new_cells;
        resize_missings();
    }

    // Get float at index idx. Runtime error if idx is out of bounds
    float get(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        return cells[array_idx][local_idx];
    }

    // Add given float to "bottom" of the column.
    void push_back(float val) {
        if (length == capacity) {
            resize();
        }
        size_t array_idx = length / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = length % INTERNAL_CHUNK_SIZE;
        cells[array_idx][local_idx] = val;
        length++;
    }

    // Return this column as a FloatColumn
    FloatColumn* as_float() { return this; }

    /** Set value at idx. An out of bound idx is undefined.  */
    void set(size_t idx, float val) {
        if (idx >= size()) {
            return;
        }
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        // Update missing bitmap
        if (is_missing(idx)) {
            missings[array_idx][local_idx] = false;
        }
        cells[array_idx][local_idx] = val;
    }

    // Add "missing" (0.0) to bottom of column
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        size_t array_idx = length / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = length % INTERNAL_CHUNK_SIZE;
        cells[array_idx][local_idx] = (float)0.0;
        missings[array_idx][local_idx] = true;
        length++;
    }
};

/*************************************************************************
 * BoolColumn::
 * Holds bool values.
 */
class BoolColumn : virtual public Column {
   public:
    bool** cells;

    // Create empty column with default capacity 10
    BoolColumn() {
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        cells = new bool*[num_chunks];
        // Array of bool arrays
        for (size_t i = 0; i < num_chunks; i++) {
            cells[i] = new bool[INTERNAL_CHUNK_SIZE];
        }
        length = 0;
        init_missings();
    }

    // Create column with n bools as starting values, in order given
    BoolColumn(int n, ...) {
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        // Always make enough space
        if ((size_t)n > capacity) {
            capacity = n;
            num_chunks = (n / INTERNAL_CHUNK_SIZE) + 1;
        }
        cells = new bool*[num_chunks];
        // Array of integer arrays
        for (size_t i = 0; i < num_chunks; i++) {
            cells[i] = new bool[INTERNAL_CHUNK_SIZE];
        }
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
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        cells = new bool*[num_chunks];
        // Array of bool arrays
        for (size_t i = 0; i < num_chunks; i++) {
            cells[i] = new bool[INTERNAL_CHUNK_SIZE];
        }
        length = 0;
        init_missings();
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            bool row_val = col->as_bool()->get(row_idx);
            push_back(row_val);
            // Track other columns missings
            if (col->is_missing(row_idx)) {
                set_missing(row_idx);
            }
        }
    }

    // Delete column array and bool pointers
    virtual ~BoolColumn() {
        for (size_t i = 0; i < num_chunks; i++) {
            delete[] cells[i];
            delete[] missings[i];
        }
        delete[] cells;
        delete[] missings;
    }

    // Double the capacity of the array and move (not copy) the bool
    // pointers in to new array
    void resize() {
        num_chunks = 2 * num_chunks;
        capacity = INTERNAL_CHUNK_SIZE * num_chunks;
        bool** new_cells = new bool*[num_chunks];
        // Array of integer arrays
        for (size_t i = 0; i < num_chunks; i++) {
            new_cells[i] = new bool[INTERNAL_CHUNK_SIZE];
        }
        for (size_t i = 0; i < (length / INTERNAL_CHUNK_SIZE); i++) {
            // Move array pointers (internal arrays not being delete)
            new_cells[i] = cells[i];
        }
        delete[] cells;
        cells = new_cells;
        resize_missings();
    }

    // Get bool at index idx.
    // Runtime error if idx is out of bounds
    bool get(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        return cells[array_idx][local_idx];
    }

    // Return this column as a BoolColumn
    BoolColumn* as_bool() { return this; }

    /** Set value at idx. An out of bound idx is undefined.  */
    void set(size_t idx, bool val) {
        if (idx >= length) {
            return;
        }
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        // Update missing bitmap
        if (is_missing(idx)) {
            missings[array_idx][local_idx] = false;
        }
        cells[array_idx][local_idx] = val;
    }

    // push the given bool on to the 'bottom' of this column
    void push_back(bool val) {
        if (length == capacity) {
            resize();
        }
        size_t array_idx = length / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = length % INTERNAL_CHUNK_SIZE;
        cells[array_idx][local_idx] = val;
        length++;
    }

    // Add "missing" (true) to bottom of column
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        size_t array_idx = length / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = length % INTERNAL_CHUNK_SIZE;
        cells[array_idx][local_idx] = true;
        missings[array_idx][local_idx] = true;
        length++;
    }
};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 */
class StringColumn : virtual public Column {
   public:
    String*** cells;

    // Create empty column with default capacity 10
    StringColumn() {
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        cells = new String**[num_chunks];
        // Array of String pointer arrays
        for (size_t i = 0; i < num_chunks; i++) {
            cells[i] = new String*[INTERNAL_CHUNK_SIZE];
        }
        length = 0;
        init_missings();
    }

    // Create column with n String*s as starting values, in order given
    StringColumn(int n, ...) {
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        // Always make enough space
        if ((size_t)n > capacity) {
            capacity = n;
            num_chunks = (n / INTERNAL_CHUNK_SIZE) + 1;
        }
        cells = new String**[num_chunks];
        // Array of String* arrays
        for (size_t i = 0; i < num_chunks; i++) {
            cells[i] = new String*[INTERNAL_CHUNK_SIZE];
        }
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
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        cells = new String**[num_chunks];
        // Array of String* arrays
        for (size_t i = 0; i < num_chunks; i++) {
            cells[i] = new String*[INTERNAL_CHUNK_SIZE];
        }
        length = 0;
        init_missings();
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            String* row_val = col->as_string()->get(row_idx);
            push_back(row_val);
            // Track other columns missings
            if (col->is_missing(row_idx)) {
                set_missing(row_idx);
            }
        }
    }

    virtual ~StringColumn() {
        // don't delete actual string* because they're not ours
        for (size_t i = 0; i < num_chunks; i++) {
            delete[] cells[i];
            delete[] missings[i];
        }
        delete[] missings;
        delete[] cells;
    }

    // Double the capacity of the array and move (not copy) the String
    // pointers in to new array
    void resize() {
        num_chunks = 2 * num_chunks;
        capacity = INTERNAL_CHUNK_SIZE * num_chunks;
        String*** new_cells = new String**[num_chunks];
        // Array of String* arrays
        for (size_t i = 0; i < num_chunks; i++) {
            new_cells[i] = new String*[INTERNAL_CHUNK_SIZE];
        }
        for (size_t i = 0; i < (length / INTERNAL_CHUNK_SIZE); i++) {
            // Move array pointers (internal arrays not being delete)
            new_cells[i] = cells[i];
        }
        delete[] cells;
        cells = new_cells;
        resize_missings();
    }

    // Get String* at index idx.
    // Runtime error if idx is out of bounds
    String* get(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        return cells[array_idx][local_idx];
    }

    // Add given String* on to 'bottom' of this column
    void push_back(String* val) {
        if (length == capacity) {
            resize();
        }
        size_t array_idx = length / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = length % INTERNAL_CHUNK_SIZE;
        cells[array_idx][local_idx] = val;
        length++;
    }

    // Return this column as a StringColumn
    StringColumn* as_string() { return this; }

    /** Out of bound idx is undefined. */
    void set(size_t idx, String* val) {
        if (idx >= length) {
            return;
        }
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        // Update missing bitmap
        if (is_missing(idx)) {
            missings[array_idx][local_idx] = false;
        }
        cells[array_idx][local_idx] = val;
    }

    // Add "missing" ("") to bottom of column
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        size_t array_idx = length / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = length % INTERNAL_CHUNK_SIZE;
        cells[array_idx][local_idx] = new String("");
        missings[array_idx][local_idx] = true;
        length++;
    }
};

/* DistributedColumn:
 * Represents a Column that is stored in a KVS, potentially over multiple nodes
 * Supports all the same operations as Column, but the implementations require
 * communicate with a KVS (Store).
 * DistributedColumn tracks two list of keys, one for cells, one for missings. 
 * Each key represents a chunk of data, whether it is the cell values 
 * or missing values. A Column has an associated store (KVS) that it 
 * communicates with. */
class DistributedColumn : virtual public Column {
   public:
    // Gets length, capacity, num_chunks, missings from Column
    Key** missings_keys;  // Keys to bool chunks that make up missing bitmap
    Key** chunk_keys;     // Keys to each chunk of values in this column
    Store* store;         // KVS
    // local missings array (cache) TODO
    // bool** missings; // Leaving this is so it compiles

    /* All method names appended with '_dist' are purely distributed.
	*  DistributedColumn successors must be very careful in calling
	*  _dist vs normal Column methods. Normal Column methods in the 
	*  distributed scenario have no real meaning. Use with caution */

    // Empty default that will be called automatically by child classes
    DistributedColumn(){}

    DistributedColumn(Store* s, Key** chunk_keys, Key** missings_keys, size_t length, size_t num_chunks) {
        store = s;
        this->length = length;
        this->num_chunks = num_chunks;
        this->capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        this->chunk_keys = chunk_keys;
        this->missings_keys = missings_keys;
    }

    virtual ~DistributedColumn() {
        // delete every key
        for (size_t i = 0; i < num_chunks; i++) {
            delete missings_keys[i];
            delete chunk_keys[i];
        }

        // delete list of keys
        delete[] missings_keys;
        delete[] chunk_keys;
    }

    // Assumes Keys are initialized
    // For each missing key, allocates an array of booleans all set to false initially.
    // Stores this array in the KVS under the pre-determined key for that
    // chunk of missings
    virtual void init_missings_dist() {
        bool missings_chunk[INTERNAL_CHUNK_SIZE];
        for (size_t j = 0; j < INTERNAL_CHUNK_SIZE; j++) {
            missings_chunk[j] = false;
        }

        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(missings_keys[i], missings_chunk, INTERNAL_CHUNK_SIZE);
        }
    }

    // Initialize all keys. After this method. Both Key lists should be
    // populated with 'num_chunks' Key objects. These represent the
    // Keys to the chunks, in order. Each Key will be unique.
    virtual void init_keys_dist() {
        missings_keys = new Key*[num_chunks]; 
        chunk_keys = new Key*[num_chunks];

	set_list_to_nullptrs(missings_keys, num_chunks);
	set_list_to_nullptrs(chunk_keys, num_chunks);

        for (size_t i = 0; i < num_chunks; i++) {
            missings_keys[i] = generate_key_dist(i);
            chunk_keys[i] = generate_key_dist(i);
        }
    }

    void set_list_to_nullptrs(Key** keys, size_t num_keys) {
	for (size_t i =0; i <num_keys; i++) {
		keys[i] = nullptr;
	}
    }

    // Resize Keys arrays to be up-to-date with number of chunks
    virtual void resize_keys_dist() {
        size_t old_num_chunks = num_chunks;
        num_chunks = 2 * num_chunks;
        capacity = INTERNAL_CHUNK_SIZE * num_chunks;

        Key** new_missings_keys = new Key*[num_chunks];
        Key** new_chunk_keys = new Key*[num_chunks];

	set_list_to_nullptrs(new_missings_keys, num_chunks);
	set_list_to_nullptrs(new_chunk_keys, num_chunks);

        // Keep keys we had before
        for (size_t i = 0; i < old_num_chunks; i++) {
            new_missings_keys[i] = missings_keys[i];
            new_chunk_keys[i] = chunk_keys[i];
        }

    	delete[] missings_keys;
        delete[] chunk_keys;
        missings_keys = new_missings_keys;
        chunk_keys = new_chunk_keys;
    
        // Make new ones for new space
        for (size_t j = old_num_chunks; j < num_chunks; j++) {
            missings_keys[j] = generate_key_dist(j);
            chunk_keys[j] = generate_key_dist(j);
        }
    } 

    // Generate a random number, and turn it in to a char* to be used in a Key
    // Ensure that the generated Key does not already exist in our lists of
    // Keys
    virtual Key* generate_key_dist(size_t corresponding_chunk_id) {
        size_t rand_key = rand();  // random number as key

        // Do a fake write to check how much space we need
        size_t buf_size = snprintf(nullptr, 0, "%zu", rand_key) + 1;

        // Do a real write with proper amount of space
        char key[buf_size];
        snprintf(key, buf_size, "%zu", rand_key);

        // Check this key against all existing keys in this column
        for (size_t i = 0; i < num_chunks; i++) {
            if (chunk_keys[i] && chunk_keys[i]->get_name() && strcmp(key, chunk_keys[i]->get_name()) == 0) {
                return generate_key_dist(i);  // Start over, need unique key
            }
            if (missings_keys[i] && missings_keys[i]->get_name() && strcmp(key, missings_keys[i]->get_name()) == 0) {
                return generate_key_dist(i);  // Start over, need unique key
            }
        }

        // We have a unique key, make a Key and return it
        size_t chunk_node = corresponding_chunk_id % store->num_nodes();
        return new Key(key, chunk_node);
    }

    // Assumes capacity has changed. Reallocate missings array and copy
    // missings values
    virtual void resize_missings_dist() {
        bool missings_chunk[INTERNAL_CHUNK_SIZE];
        for (size_t j = 0; j < INTERNAL_CHUNK_SIZE; j++) {
            missings_chunk[j] = false;
        }

        // Put default missings chunk under each new key
        for (size_t i = (length / INTERNAL_CHUNK_SIZE); i < num_chunks; i++) {
            store->put_(missings_keys[i], missings_chunk, INTERNAL_CHUNK_SIZE);
        }
    }

    // Return whether the element at the given value is a missing value
    // Undefined behavior if the idx is out of bounds
    virtual bool is_missing_dist(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        Key* k = missings_keys[array_idx];
        bool* missings = store->get_bool_array_(k);
        bool val = missings[local_idx];
        delete[] missings;  // No longer need it
        return val;
    }

    // Sets the value at idx as missing or not
    // Out of bounds idx is undefined behavior
    virtual void set_missing_dist(size_t idx, bool is_missing) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        //missings[array_idx][local_idx] = true;
        Key* k = missings_keys[array_idx];
        bool* missings = store->get_bool_array_(k);
        missings[local_idx] = is_missing;
        store->put_(k, missings, INTERNAL_CHUNK_SIZE);
        delete[] missings;
    }
};

/*************************************************************************
 * DistributedIntColumn 
 * Holds int values through the use of a KVS (Store).
 */
class DistributedIntColumn : public DistributedColumn, public IntColumn {
   public:
    // Create empty int column
    DistributedIntColumn(Store* s) : IntColumn() {
        store = s;
        length = 0;
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        init_keys_dist();
        init_missings_dist();

        // Put default integer array for each chunk
        int ints[INTERNAL_CHUNK_SIZE];
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], ints, INTERNAL_CHUNK_SIZE);
        }
    }

    // Create int column with n entries, listed in order in
    // the succeeding parameters
    DistributedIntColumn(Store* s, int n, ...) : IntColumn() {
        store = s;
        length = 0;
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        // Always make enough space
        if ((size_t)n > capacity) {
            capacity = n;
            num_chunks = (n / INTERNAL_CHUNK_SIZE) + 1;
        }

        init_keys_dist();
        init_missings_dist();

        // Put default integer array for each chunk
        int ints[INTERNAL_CHUNK_SIZE];
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], ints, INTERNAL_CHUNK_SIZE);
        }

        va_list vl;
        va_start(vl, n);
        int val;
        for (int i = 0; i < n; i++) {
            val = va_arg(vl, int);
            push_back(val);
        }
        va_end(vl);
    }

    // Copy constructor. Assumes other column is the same type as this one
    DistributedIntColumn(Store* s, Column* col) : IntColumn() {
        store = s;
        length = 0;
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        init_keys_dist();
	init_missings_dist();
        
	// Put default integer array for each chunk
        int ints[INTERNAL_CHUNK_SIZE];
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], ints, INTERNAL_CHUNK_SIZE);
        }

        // Copy over data from other column
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            int row_val = col->as_int()->get(row_idx);
            push_back(row_val);

            if (col->is_missing(row_idx)) {
                // row_val we pushed above is fake, mark this cell as missing
                set_missing_dist(row_idx, true);
            }
        }
    }

    // Generic constructor that specifies all values
    DistributedIntColumn(Store* s, Key** chunk_keys, Key** missings_keys, size_t length, size_t num_chunks)
        : DistributedColumn(s, chunk_keys, missings_keys, length, num_chunks) {
    }

    ~DistributedIntColumn() {
        // Memory associated with keys is deleted in DistributedColumn
        // Memory associated with values of keys in store are deleted in Store destructor
        // Memory associated with cells/missing is deleted in normal IntColumn
    }

    // Returns the integer at the given index.
    // Input index out of bounds will cause a runtime error
    int get(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        Key* k = chunk_keys[array_idx];
        int* cells = store->get_int_array_(k);
        int val = cells[local_idx];
        delete[] cells;

        return val;
    }

    /** Set value at idx. An out of bound idx is undefined.  */
    void set(size_t idx, int val) {
        if (idx >= length) {
            return;
        }

        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        Key* k = chunk_keys[array_idx];

        int* cells = store->get_int_array_(k);
        cells[local_idx] = val;
        store->put_(k, cells, INTERNAL_CHUNK_SIZE);

        delete[] cells;

        // We may be overwriting a missing, so mark cell as not-missing
        set_missing_dist(idx, false);
    }

    // Add more keys to our lists of keys to accomodate for more items
    void resize() {
        size_t old_num_chunks = num_chunks;
        resize_keys_dist();
        resize_missings_dist();

        // add default int array to new chunks
        int ints[INTERNAL_CHUNK_SIZE];
        for (size_t i = old_num_chunks; i < num_chunks; i++) {
            store->put_(chunk_keys[i], ints, INTERNAL_CHUNK_SIZE);
        }
    }

    // Add integer to "bottom" of column
    void push_back(int val) {
        if (length == capacity) {
            resize();
        }
        size_t array_idx = length / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = length % INTERNAL_CHUNK_SIZE;
        Key* k = chunk_keys[array_idx];
        int* cells = store->get_int_array_(k);
        cells[local_idx] = val;
        store->put_(k, cells, INTERNAL_CHUNK_SIZE);
        // No need to call set_missing_dist because default is false
        length++;
        delete[] cells;
    }

    // Add "missing" (0) to bottom of column
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        // Default value in store already exists,
        // Mark value as missing and increment length
        set_missing_dist(length, true);
        length++;
    }
};

/*************************************************************************
 * DistributedBoolColumn 
 * Holds bool values through the use of a KVS (Store).
 */
class DistributedBoolColumn : public DistributedColumn, public BoolColumn {
   public:
    // Create empty bool column
    DistributedBoolColumn(Store* s) : BoolColumn() {
        store = s;
        length = 0;
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        init_keys_dist();
        init_missings_dist();

        // Put default bool array for each chunk
        bool bools[INTERNAL_CHUNK_SIZE];
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], bools, INTERNAL_CHUNK_SIZE);
        }
    }

    // Create bool column with n entries, listed in order in
    // the succeeding parameters
    DistributedBoolColumn(Store* s, int n, ...) : BoolColumn() {
        store = s;
        length = 0;
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        // Always make enough space
        if ((size_t)n > capacity) {
            capacity = n;
            num_chunks = (n / INTERNAL_CHUNK_SIZE) + 1;
        }

        init_keys_dist();
        init_missings_dist();

        // Put default bool array for each chunk
        bool bools[INTERNAL_CHUNK_SIZE];
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], bools, INTERNAL_CHUNK_SIZE);
        }

        va_list vl;
        va_start(vl, n);
        bool val;
        for (int i = 0; i < n; i++) {
            val = va_arg(vl, int);
            push_back(val);
        }
        va_end(vl);
    }

    // Copy constructor. Assumes other column is the same type as this one
    DistributedBoolColumn(Store* s, Column* col) : BoolColumn() {
        store = s;
        length = 0;
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        init_keys_dist();
	init_missings_dist();
        
	// Put default bool array for each chunk
        bool bools[INTERNAL_CHUNK_SIZE];
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], bools, INTERNAL_CHUNK_SIZE);
        }

        // Copy over data from other column
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            bool row_val = col->as_bool()->get(row_idx);
            push_back(row_val);

            if (col->is_missing(row_idx)) {
                // row_val we pushed above is fake, mark this cell as missing
                set_missing_dist(row_idx, true);
            }
        }
    }

    // Generic constructor that specifies all values
    DistributedBoolColumn(Store* s, Key** chunk_keys, Key** missings_keys, size_t length, size_t num_chunks)
        : DistributedColumn(s, chunk_keys, missings_keys, length, num_chunks) {
    }

    ~DistributedBoolColumn() {
        // Memory associated with keys is deleted in DistributedColumn
        // Memory associated with values of keys in store are deleted in Store destructor
        // Memory associated with cells/missing is deleted in normal BoolColumn
    }

    // Returns the booleger at the given index.
    // Input index out of bounds will cause a runtime error
    bool get(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        Key* k = chunk_keys[array_idx];
        bool* cells = store->get_bool_array_(k);
        bool val = cells[local_idx];
        delete[] cells;

        return val;
    }

    /** Set value at idx. An out of bound idx is undefined.  */
    void set(size_t idx, bool val) {
        if (idx >= length) {
            return;
        }

        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        Key* k = chunk_keys[array_idx];

        bool* cells = store->get_bool_array_(k);
        cells[local_idx] = val;
        store->put_(k, cells, INTERNAL_CHUNK_SIZE);

        delete[] cells;

        // We may be overwriting a missing, so mark cell as not-missing
        set_missing_dist(idx, false);
    }

    // Add more keys to our lists of keys to accomodate for more items
    void resize() {
        size_t old_num_chunks = num_chunks;
        resize_keys_dist();
        resize_missings_dist();

        // add default bool array to new chunks
        bool bools[INTERNAL_CHUNK_SIZE];
        for (size_t i = old_num_chunks; i < num_chunks; i++) {
            store->put_(chunk_keys[i], bools, INTERNAL_CHUNK_SIZE);
        }
    }

    // Add booleger to "bottom" of column
    void push_back(bool val) {
        if (length == capacity) {
            resize();
        }
        size_t array_idx = length / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = length % INTERNAL_CHUNK_SIZE;
        Key* k = chunk_keys[array_idx];
        bool* cells = store->get_bool_array_(k);
        cells[local_idx] = val;
        store->put_(k, cells, INTERNAL_CHUNK_SIZE);
        // No need to call set_missing_dist because default is false
        length++;
        delete[] cells;
    }

    // Add "missing" (0) to bottom of column
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        // Default value in store already exists,
        // Mark value as missing and increment length
        set_missing_dist(length, true);
        length++;
    }
};

/*************************************************************************
 * DistributedFloatColumn 
 * Holds float values through the use of a KVS (Store).
 */
class DistributedFloatColumn : public DistributedColumn, public FloatColumn {
   public:
    // Create empty float column
    DistributedFloatColumn(Store* s) : FloatColumn() {
        store = s;
        length = 0;
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        init_keys_dist();
        init_missings_dist();

        // Put default float array for each chunk
        float floats[INTERNAL_CHUNK_SIZE];
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], floats, INTERNAL_CHUNK_SIZE);
        }
    }

    // Create float column with n entries, listed in order in
    // the succeeding parameters
    DistributedFloatColumn(Store* s, int n, ...) : FloatColumn() {
        store = s;
        length = 0;
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        // Always make enough space
        if ((size_t)n > capacity) {
            capacity = n;
            num_chunks = (n / INTERNAL_CHUNK_SIZE) + 1;
        }

        init_keys_dist();
        init_missings_dist();

        // Put default float array for each chunk
        float floats[INTERNAL_CHUNK_SIZE];
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], floats, INTERNAL_CHUNK_SIZE);
        }

        va_list vl;
        va_start(vl, n);
        float val;
        for (int i = 0; i < n; i++) {
            val = va_arg(vl, double);
            push_back(val);
        }
        va_end(vl);
    }

    // Copy constructor. Assumes other column is the same type as this one
    DistributedFloatColumn(Store* s, Column* col) : FloatColumn() {
        store = s;
        length = 0;
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;

        init_keys_dist();
	init_missings_dist();

        // Put default float array for each chunk
        float floats[INTERNAL_CHUNK_SIZE];
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], floats, INTERNAL_CHUNK_SIZE);
        }

        // Copy over data from other column
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            float row_val = col->as_float()->get(row_idx);
            push_back(row_val);

            if (col->is_missing(row_idx)) {
                // row_val we pushed above is fake, mark this cell as missing
                set_missing_dist(row_idx, true);
            }
        }
    }

    // Generic constructor that specifies all values
    DistributedFloatColumn(Store* s, Key** chunk_keys, Key** missings_keys, size_t length, size_t num_chunks)
        : DistributedColumn(s, chunk_keys, missings_keys, length, num_chunks) {
    }

    ~DistributedFloatColumn() {
        // Memory associated with keys is deleted in DistributedColumn
        // Memory associated with values of keys in store are deleted in Store destructor
        // Memory associated with cells/missing is deleted in normal IntColumn
    }

    // Returns the float at the given index.
    // Input index out of bounds will cause a runtime error
    float get(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        Key* k = chunk_keys[array_idx];
        float* cells = store->get_float_array_(k);
        float val = cells[local_idx];
        delete[] cells;

        return val;
    }

    /** Set value at idx. An out of bound idx is undefined.  */
    void set(size_t idx, float val) {
        if (idx >= length) {
            return;
        }
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        Key* k = chunk_keys[array_idx];

        float* cells = store->get_float_array_(k);
        cells[local_idx] = val;
        store->put_(k, cells, INTERNAL_CHUNK_SIZE);

        delete[] cells;

        // We may be overwriting a missing, so mark cell as not-missing
        set_missing_dist(idx, false);
    }

    // Add more keys to our lists of keys to accomodate for more items
    void resize() {
        size_t old_num_chunks = num_chunks;
        resize_keys_dist();
        resize_missings_dist();

        // Put default float array for each chunk
        float floats[INTERNAL_CHUNK_SIZE];
        for (size_t i = old_num_chunks; i < num_chunks; i++) {
            store->put_(chunk_keys[i], floats, INTERNAL_CHUNK_SIZE);
        }
    }

    // Add floateger to "bottom" of column
    void push_back(float val) {
        if (length == capacity) {
            resize();
        }
        size_t array_idx = length / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = length % INTERNAL_CHUNK_SIZE;
        Key* k = chunk_keys[array_idx];
        float* cells = store->get_float_array_(k);
        cells[local_idx] = val;
        store->put_(k, cells, INTERNAL_CHUNK_SIZE);
        // No need to call set_missing_dist because default is false
        length++;
        delete[] cells;
    }

    // Add "missing" (0) to bottom of column
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        // Default value in store already exists,
        // Mark value as missing and increment length
        set_missing_dist(length, true);
        length++;
    }
};

/*************************************************************************
 * DistributedStringColumn 
 * Holds String* values through the use of a KVS (Store).
 */
class DistributedStringColumn : public DistributedColumn, public StringColumn {
   public:
    // Create empty String* column
    DistributedStringColumn(Store* s) : StringColumn() {
        store = s;
        length = 0;
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        init_keys_dist();
        init_missings_dist();

        // Put default string array for each chunk
        String* strings[INTERNAL_CHUNK_SIZE] = {};  // {} to ensure all String* are initialized to nullptr
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], strings, INTERNAL_CHUNK_SIZE);
        }
    }

    // Create String* column with n entries, listed in order in
    // the succeeding parameters
    DistributedStringColumn(Store* s, int n, ...) : StringColumn() {
        store = s;
        length = 0;
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        // Always make enough space
        if ((size_t)n > capacity) {
            capacity = n;
            num_chunks = (n / INTERNAL_CHUNK_SIZE) + 1;
        }

        init_keys_dist();
        init_missings_dist();

        // Put default string array for each chunk
        String* strings[INTERNAL_CHUNK_SIZE] = {};
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], strings, INTERNAL_CHUNK_SIZE);
        }

        va_list vl;
        va_start(vl, n);
        String* val;
        for (int i = 0; i < n; i++) {
            val = va_arg(vl, String*);
            push_back(val);
        }
        va_end(vl);
    }

    // Copy constructor. Assumes other column is the same type as this one
    DistributedStringColumn(Store* s, Column* col) : StringColumn() {
        store = s;
        length = 0;
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        init_keys_dist();
	init_missings_dist();
        
	// Put default string array for each chunk
        String* strings[INTERNAL_CHUNK_SIZE] = {};
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], strings, INTERNAL_CHUNK_SIZE);
        }

        // Copy over data from other column
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            String* row_val = col->as_string()->get(row_idx);
            push_back(row_val);

            if (col->is_missing(row_idx)) {
                // row_val we pushed above is fake, mark this cell as missing
                set_missing_dist(row_idx, true);
            }
        }
    }

    // Generic constructor that specifies all values
    DistributedStringColumn(Store* s, Key** chunk_keys, Key** missings_keys, size_t length, size_t num_chunks)
        : DistributedColumn(s, chunk_keys, missings_keys, length, num_chunks) {
    }

    ~DistributedStringColumn() {
        // Memory associated with keys is deleted in DistributedColumn
        // Memory associated with values of keys in store are deleted in Store destructor
        // Memory associated with cells/missing is deleted in normal IntColumn
    }

    // Returns the String*eger at the given index.
    // Input index out of bounds will cause a runtime error
    String* get(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        Key* k = chunk_keys[array_idx];
        String** cells = store->get_string_array_(k);
        String* val = cells[local_idx];
        delete[] cells;

        return val;
    }

    /** Set value at idx. An out of bound idx is undefined.  */
    void set(size_t idx, String* val) {
        if (idx >= length) {
            return;
        }
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        Key* k = chunk_keys[array_idx];

        String** cells = store->get_string_array_(k);
        cells[local_idx] = val;
        store->put_(k, cells, INTERNAL_CHUNK_SIZE);

        delete[] cells;

        // We may be overwriting a missing, so mark cell as not-missing
        set_missing_dist(idx, false);
    }

    // Add more keys to our lists of keys to accomodate for more items
    void resize() {
        size_t old_num_chunks = num_chunks;
        resize_keys_dist();
        resize_missings_dist();

        // Put default string array for each chunk
        String* strings[INTERNAL_CHUNK_SIZE] = {};
        for (size_t i = old_num_chunks; i < num_chunks; i++) {
            store->put_(chunk_keys[i], strings, INTERNAL_CHUNK_SIZE);
        }
    }

    // Add String*eger to "bottom" of column
    void push_back(String* val) {
        if (length == capacity) {
            resize();
        }
        size_t array_idx = length / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = length % INTERNAL_CHUNK_SIZE;
        Key* k = chunk_keys[array_idx];
        String** cells = store->get_string_array_(k);
        cells[local_idx] = val;
        store->put_(k, cells, INTERNAL_CHUNK_SIZE);
        // No need to call set_missing_dist because default is false
        length++;
        delete[] cells;
    }

    // Add "missing" (0) to bottom of column
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        // Default value in store already exists,
        // Mark value as missing and increment length
        set_missing_dist(length, true);
        length++;
    }
};
