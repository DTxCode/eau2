#pragma once
#include <stdarg.h>  // va_arg
#include <stdlib.h>
#include "../../utils/object.h"
#include "../../utils/string.h"
#include "../key.h"
//#include "../store.h"

#define INT_TYPE 'I'
#define BOOL_TYPE 'B'
#define FLOAT_TYPE 'F'
#define STRING_TYPE 'S'
#define INTERNAL_CHUNK_SIZE 10000

class Store {
    public:
    
    
void put_(Key* k, bool* bs);
void put_(Key* k, int* is);
size_t this_node();
bool* get_bool_array_(Key* k);
int* get_int_array_(Key* k);
    
    
    
    
    
    
    };

class IntColumn;
class BoolColumn;
class FloatColumn;
class StringColumn;

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
 

 /* Distributed update:
 * Column tracks two list of keys, one for cells, one for missings. 
 * Each key represents a chunk of data, whether it is the cell values 
 * or missing values. A Column has an associated store (KVS) that it 
 * communicates with. */ 
class Column : public Object {
    // TODO may have off-by-one errors with resizing

   public:
    size_t length;      // Count of values(including missings)
    size_t capacity;    // Count of cells available
    size_t num_chunks;  // Count of how many internal 'chunk' arrays were using
    Key** missings_keys; // Keys to bool chunks that make up missing bitmap
    Key** chunk_keys; // Keys to each chunk of values in this column
    Store* store; // KVS 
    // local missings array (cache) TODO
    bool** missings; // Leaving this is so it compiles

    // Assumes Keys are initialized
    // For each missing key, allocates an array of booleans all set to false
    // Stores this array in the KVS under the pre-determined key for that
    // chunk of missings
    void init_missings() {
        bool** missings = new bool*[num_chunks];
        for (size_t i = 0; i < num_chunks; i++) {
            missings[i] = new bool[INTERNAL_CHUNK_SIZE];
            for (size_t j = 0; j < INTERNAL_CHUNK_SIZE; j++) {
                missings[i][j] = false;
            }
            
            store->put_(missings_keys[i], missings[i]); // TODO put_ method for each array type?
            delete[] missings[i]; // DONT NEED IT AFTER ITS IN STORE
        }
        delete[] missings; // MISSINGS COULD BE STACK ALLOCATED INSTEAD
    }

    // Initialize all keys. After this method. Both Key lists should be 
    // populated with 'num_chunks' Key objects. These represent the 
    // Keys to the chunks, in order. Each Key will be unique. 
    void init_keys() {
        missings_keys = new Key*[num_chunks];
        chunk_keys = new Key*[num_chunks];
        for (size_t i = 0; i < num_chunks; i++) {
            missings_keys[i] = generate_key();
            chunk_keys[i] = generate_key();
        }
    }

    // Resize Keys arrays to be up-to-date with number of chunks
    void resize_keys() {
        Key** new_missings_keys = new Key*[num_chunks];
        Key** new_chunk_keys = new Key*[num_chunks];
        // Keep keys we had before
        for (size_t i = 0; i < (length / INTERNAL_CHUNK_SIZE) + 1; i++) {
            new_missings_keys[i] = missings_keys[i];
            new_chunk_keys[i] = chunk_keys[i];
        }
        // Make new ones for new space
        for (size_t j = (length / INTERNAL_CHUNK_SIZE) + 2; j < num_chunks; j++) {
            new_missings_keys[j] = generate_key();
            new_chunk_keys[j] = generate_key();
        }
        delete[] missings_keys;
        delete[] chunk_keys;
        missings_keys = new_missings_keys;
        chunk_keys = new_chunk_keys;
    }

    // Generate a random number, and turn it in to a char* to be used in a Key
    // Ensure that the generated Key does not already exist in our lists of 
    // Keys
    Key* generate_key() {
        size_t rand_key = rand(); // random number as key
        char* key;
        // Do a fake write to check how much space we need
        size_t buf_size = snprintf(nullptr, 0, "%lu", rand_key) + 1;
        key = new char[buf_size];
        // Do a real write with proper amount of space
        snprintf(key, buf_size, "%lu", rand_key);
        // Check this key against all existing keys in this column
        for (size_t i = 0; i < num_chunks; i++) {
            if (strcmp(key, chunk_keys[i]->get_name()) == 0) {
                return generate_key(); // Start over, need unique key
            }
            if (strcmp(key, missings_keys[i]->get_name()) == 0) {
                return generate_key(); // Start over, need unique key
            }
        }
        // We have a unique key, make a Key and return it
        return new Key(key, store->this_node());
    }

    // Assumes capacity has changed. Reallocate missings array and copy
    // missings values
    void resize_missings() {
        bool** new_missings = new bool*[num_chunks];
        // Can we avoid initializing first N chunks that were copying?
        // TODO this feels very fragile
        for (size_t i = (length / INTERNAL_CHUNK_SIZE) + 1; i < num_chunks; i++) {
            new_missings[i] = new bool[INTERNAL_CHUNK_SIZE];
        }
        for (size_t i = 0; i < (length / INTERNAL_CHUNK_SIZE) + 1; i++) { // TODO other resize methods are off by 1 
            new_missings[i] = missings[i];
        }
        delete[] missings;
        missings = new_missings;
    }
    
    // Return whether the element at the given value is a missing value
    // Undefined behavior if the idx is out of bounds
    bool is_missing(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        Key* k = missings_keys[array_idx];
        bool* missings = store->get_bool_array_(k);
        bool val = missings[local_idx];
        delete[] missings; // No longer need it
        return val;
        //return missings[array_idx][local_idx];
    }


    // Declare the value at idx as missing, does not change or set a value
    // Out of bounds idx is undefined behavior
    void set_missing(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        //missings[array_idx][local_idx] = true;
        Key* k = missings_keys[array_idx];
        bool* missings = store->get_bool_array_(k);
        missings[local_idx] = true;
        store->put_(k, missings);
        delete[] missings;
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
    // int** cells; //TODO should have a local cache for some values

    // Create empty int column
    IntColumn(Store* s) {
        store = s;
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        init_keys();
        init_missings();
        //cells = new int*[num_chunks];
        // Array of integer arrays
        for (size_t i = 0; i < num_chunks; i++) {
            //cells[i] = new int[INTERNAL_CHUNK_SIZE];
            store->put_(chunk_keys[i], new int[INTERNAL_CHUNK_SIZE]);
        }
        length = 0;
    }

    // Create int column with n entries, listed in order in
    // the succeeding parameters
    IntColumn(Store* s, int n, ...) {
        store = s;
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        init_keys();
        init_missings();
        // Always make enough space
        if ((size_t) n > capacity) {
            capacity = n;
            num_chunks = (n / INTERNAL_CHUNK_SIZE) + 1;
        }
        //cells = new int*[num_chunks];
        // Array of integer arrays
        for (size_t i = 0; i < num_chunks; i++) {
            //cells[i] = new int[INTERNAL_CHUNK_SIZE];
            store->put_(chunk_keys[i], new int[INTERNAL_CHUNK_SIZE]);
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
    }

    // Copy constructor. Assumes other column is the same type as this one
    IntColumn(Store* s, Column* col) {
        store = s;
        num_chunks = 10;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        init_missings();
        init_keys();
        //cells = new int*[num_chunks];
        // Array of integer arrays
        for (size_t i = 0; i < num_chunks; i++) {
            //cells[i] = new int[INTERNAL_CHUNK_SIZE];
            store->put_(chunk_keys[i], new int[INTERNAL_CHUNK_SIZE]);
        }
        length = 0;
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
    // TODO what do we own and what do we not?
    // Own Keys that we made, do not own Store 
    ~IntColumn() {
        for (size_t i = 0; i < num_chunks; i++) {
            delete missings_keys[i];
            delete chunk_keys[i];
            //delete[] cells[i];
            //delete[] missings[i];
        }
        delete[] missings_keys;
        delete[] chunk_keys;
        //delete[] missings;
        //delete[] cells;
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

    // Returns this column as an integer column
    IntColumn* as_int() {
        return this;
    }

    /** Set value at idx. An out of bound idx is undefined.  */
    void set(size_t idx, int val) {
        if (idx >= length) {
            return;
        }
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        // Update missing bitmap
        if (is_missing(idx)) {
            set_missing(idx);
        }
        Key* k = chunk_keys[array_idx];
        int* cells = store->get_int_array_(k);
        cells[local_idx] = val;
        store->put_(k, cells);
        delete[] cells;
    }

    // Add more keys to our lists of keys to accomodate for more items
    void resize() {
        size_t old_chunks = num_chunks;
        num_chunks = 2 * num_chunks;
        capacity = INTERNAL_CHUNK_SIZE * num_chunks;
        resize_keys(); // Chunk_keys is now twice the size as before
        resize_missings();
        for (size_t i = old_chunks; i < num_chunks; i++) {
            store->put_(chunk_keys[i], new int[INTERNAL_CHUNK_SIZE]);
            store->put_(missings_keys[i], new bool[INTERNAL_CHUNK_SIZE]);
        }
        
        /*int** new_cells = new int*[num_chunks];
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
        resize_missings();*/
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
        store->put_(k, cells);
        length++;
        delete[] cells; // TODO ?
    }

    // Add "missing" (0) to bottom of column
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        size_t array_idx = length / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = length % INTERNAL_CHUNK_SIZE;
        Key* k = missings_keys[array_idx];
        bool* cells = store->get_bool_array_(k);
        cells[local_idx] = true;
        store->put_(k, cells);
        length++;
        delete[] cells;
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
        if ((size_t) n > capacity) {
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
    ~FloatColumn() {
        for (size_t i = 0; i < num_chunks; i++) {
            delete[] cells[i];
            delete[] missings;
        }
        delete[] cells;
        delete[] missings;
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
class BoolColumn : public Column {
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
        if ((size_t) n > capacity) {
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
    ~BoolColumn() {
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
class StringColumn : public Column {
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
        if ((size_t) n > capacity) {
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

    ~StringColumn() {
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
