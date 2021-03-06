/* Authors: Ryan Heminway (heminway.r@husky.neu.edu)
*           David Tandetnik (tandetnik.da@husky.neu.edu) */
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
 * * Columns are mutable, equality is pointer equality. 
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
    size_t length = 0;                      // Count of values(including missings)
    size_t capacity = INTERNAL_CHUNK_SIZE;  // Count of cells available
    bool* missings_;

    // Default constructor
    Column() {
        missings_ = new bool[INTERNAL_CHUNK_SIZE];
    }

    // Virtual destructor will force child-classes to invoke their own
    // destructors on deletion
    virtual ~Column() {
        delete[] missings_;
    }

    // Return whether the element at the given value is a missing value
    // Undefined behavior if the idx is out of bounds
    virtual bool is_missing(size_t idx) {
        return missings_[idx];
    }

    // Declare the value at idx as missing, does not change or set a value
    // Out of bounds idx is undefined behavior
    virtual void set_missing(size_t idx) {
        missings_[idx] = true;
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
    virtual char get_type() {
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
    int* cells_;

    // Create empty int column
    IntColumn() {
        cells_ = new int[INTERNAL_CHUNK_SIZE]();
    }

    // Copy constructor. Assumes other column is the same type as this one
    IntColumn(Column* col) : IntColumn() {
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            int row_val = col->as_int()->get(row_idx);
            push_back(row_val);
            // Track other columns missings
            if (col->is_missing(row_idx)) {
                set_missing(row_idx);
            }
        }
    }

    // Returns the integer at the given index.
    // Input index out of bounds will cause a runtime error
    virtual int get(size_t idx) {
        return cells_[idx];
    }

    /** Set value at idx. An out of bound idx is undefined.  */
    virtual void set(size_t idx, int val) {
        if (idx >= length) {
            return;
        }
        // Update missing bitmap
        missings_[idx] = false;
        cells_[idx] = val;
    }

    // Delete column array and int pointers
    virtual ~IntColumn() {
        delete[] cells_;
    }

    // Returns this column as an integer column
    virtual IntColumn* as_int() {
        return this;
    }
};

/*************************************************************************
 * FloatColumn::
 * Holds float values.
 */
class FloatColumn : virtual public Column {
   public:
    float* cells_;

    // Create empty column with default capacity 10
    FloatColumn() {
        cells_ = new float[INTERNAL_CHUNK_SIZE]();
    }

    // Copy constructor. Assumes other column is the same type as this one
    FloatColumn(Column* col) : FloatColumn() {
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            float row_val = col->as_float()->get(row_idx);
            push_back(row_val);
            // Track other columns missings
            if (col->is_missing(row_idx)) {
                set_missing(row_idx);
            }
        }
    }

    // Returns the float at the given index.
    // Input index out of bounds will cause a runtime error
    virtual float get(size_t idx) {
        return cells_[idx];
    }

    /** Set value at idx. An out of bound idx is undefined.  */
    virtual void set(size_t idx, float val) {
        if (idx >= length) {
            return;
        }
        // Update missing bitmap
        missings_[idx] = false;
        cells_[idx] = val;
    }

    // Delete column array and float pointers
    virtual ~FloatColumn() {
        delete[] cells_;
    }

    // Return this column as a FloatColumn
    virtual FloatColumn* as_float() { return this; }
};

/*************************************************************************
 * BoolColumn::
 * Holds bool values.
 */
class BoolColumn : virtual public Column {
   public:
    bool* cells_;

    // Create empty column with default capacity 10
    BoolColumn() {
        cells_ = new bool[INTERNAL_CHUNK_SIZE]();
    }

    // Copy constructor. Assumes other column is the same type as this one
    BoolColumn(Column* col) : BoolColumn() {
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            bool row_val = col->as_bool()->get(row_idx);
            push_back(row_val);
            // Track other columns missings
            if (col->is_missing(row_idx)) {
                set_missing(row_idx);
            }
        }
    }

    // Returns the bool at the given index.
    // Input index out of bounds will cause a runtime error
    virtual bool get(size_t idx) {
        return cells_[idx];
    }

    /** Set value at idx. An out of bound idx is undefined.  */
    virtual void set(size_t idx, bool val) {
        if (idx >= length) {
            return;
        }
        // Update missing bitmap
        missings_[idx] = false;
        cells_[idx] = val;
    }

    // Delete column array and bool pointers
    virtual ~BoolColumn() {
        delete[] cells_;
    }

    // Return this column as a BoolColumn
    virtual BoolColumn* as_bool() { return this; }
};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 */
class StringColumn : virtual public Column {
   public:
    String** cells_;

    // Create empty column with default capacity 10
    StringColumn() {
        cells_ = new String*[INTERNAL_CHUNK_SIZE]();
    }

    // Copy constructor. Assumes other column is the same type as this one
    StringColumn(Column* col) : StringColumn() {
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            String* row_val = col->as_string()->get(row_idx);
            push_back(row_val);
            // Track other columns missings
            if (col->is_missing(row_idx)) {
                set_missing(row_idx);
            }
        }
    }

    // Returns the string at the given index.
    // Input index out of bounds will cause a runtime error
    virtual String* get(size_t idx) {
        return cells_[idx];
    }

    /** Set value at idx. An out of bound idx is undefined.  */
    virtual void set(size_t idx, String* val) {
        if (idx >= length) {
            return;
        }
        // Update missing bitmap
        missings_[idx] = false;
        cells_[idx] = val;
    }

    virtual ~StringColumn() {
        delete[] cells_;
    }

    // Return this column as a StringColumn
    virtual StringColumn* as_string() { return this; }
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
    size_t num_chunks = 10;
    // Gets length, capacity, num_chunks, missings from Column
    Key** missings_keys;  // Keys to bool chunks that make up missing bitmap
    Key** chunk_keys;     // Keys to each chunk of values in this column
    Store* store;         // KVS
    size_t cached_chunk_idx = 10;
    size_t cached_missings_idx = 10;

    /* All method names appended with '_dist' are purely distributed.
	*  DistributedColumn successors must be very careful in calling
	*  _dist vs normal Column methods. Normal Column methods in the 
	*  distributed scenario have no real meaning. Use with caution */

    // Empty default that will be called automatically by child classes
    DistributedColumn() {

    }

    // Build a DistColumn from a store, initializing the first set of keys
    // To be called by child classes
    DistributedColumn(Store* s) {
        store = s;
        length = 0;
        capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        init_keys_dist();
        init_missings_dist();
    }

    // Constructor that builds a DistColumn from all its components. 
    // For Interal Use Only
    DistributedColumn(Store* s, Key** chunk_keys, Key** missings_keys, size_t length, size_t num_chunks)  {
        store = s;
        this->length = length;
        this->num_chunks = num_chunks;
        this->capacity = num_chunks * INTERNAL_CHUNK_SIZE;
        this->chunk_keys = chunk_keys;
        this->missings_keys = missings_keys;
        cached_chunk_idx = num_chunks;
        cached_missings_idx = num_chunks;
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
        for (size_t i = 0; i < num_keys; i++) {
            keys[i] = nullptr;
        }
    }

    // Resize Keys arrays to be up-to-date with number of chunks
    virtual void resize_keys_dist() {
        size_t old_num_chunks = num_chunks;
        num_chunks = 2 * num_chunks;
        // For cache reset
        cached_missings_idx = num_chunks;
        cached_chunk_idx = num_chunks;
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
        bool* this_missings = store->get_bool_array_(k);
        bool val = this_missings[local_idx];
        delete[] this_missings;
        return val;

        // TODO fix missings caching (currently has memory leak)
        // Cache this chunk if its not already
        /*if (array_idx != cached_missings_idx) {
            // Free old cache
            delete[] missings_;
            Key* k = missings_keys[array_idx];
            missings_ = store->get_bool_array_(k);
            cached_missings_idx = array_idx;
        }

	    // Use local is_missing
        return is_missing(local_idx);*/
    }

    // Sets the value at idx as missing or not
    // Out of bounds idx is undefined behavior
    virtual void set_missing_dist(size_t idx, bool is_missing) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;

        Key* k = missings_keys[array_idx];
        bool* missings = store->get_bool_array_(k);
        missings[local_idx] = is_missing;

        store->put_(k, missings, INTERNAL_CHUNK_SIZE);

        delete[] missings;
        // Force cache to be reset
        cached_missings_idx = num_chunks;
    }

    // Whether the given row of this distributed column is stored on this node
    bool is_row_local(size_t row_idx) {
        size_t array_idx = row_idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)

        Key* k = chunk_keys[array_idx];
        return k->get_home_node() == store->this_node();
    }
};

/*************************************************************************
 * DistributedIntColumn 
 * Holds int values through the use of a KVS (Store).
 */
class DistributedIntColumn : public DistributedColumn, public IntColumn {
   public:
    // Create empty int column
    DistributedIntColumn(Store* s) : DistributedColumn(s), IntColumn() {
        // Put default integer array for each chunk
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], cells_, INTERNAL_CHUNK_SIZE);
        }
    }

    // Copy constructor. Assumes other column is the same type as this one
    DistributedIntColumn(Store* s, DistributedIntColumn* col) 
        : DistributedColumn(s), IntColumn() {
        // Put default integer array for each chunk
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], cells_, INTERNAL_CHUNK_SIZE);
        }

        // Copy over data from other column
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            int row_val = col->as_int()->get(row_idx);
            push_back(row_val);

            if (col->is_missing_dist(row_idx)) {
                // row_val we pushed above is fake, mark this cell as missing
                set_missing_dist(row_idx, true);
            }
        }
    }

    // Generic constructor that specifies all values
    DistributedIntColumn(Store* s, Key** chunk_keys, Key** missings_keys, size_t length, 
        size_t num_chunks) 
        : DistributedColumn(s, chunk_keys, missings_keys, length, num_chunks) {}

    ~DistributedIntColumn() {
        // Memory associated with keys is deleted in DistributedColumn
        // Memory associated with values of keys in store are deleted in Store destructor
        // Memory associated with cells/missing is deleted in normal IntColumn
    }

    // Returns this column as an integer column
    IntColumn* as_int() {
        return this;
    }

    // Returns the integer at the given index.
    // Input index out of bounds will cause a runtime error
    int get(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        // Load chunk into cache if its not
        if (array_idx != cached_chunk_idx) {
            // Free old cache
            delete[] cells_;
            Key* k = chunk_keys[array_idx];
            cells_ = store->get_int_array_(k);
            cached_chunk_idx = array_idx;
        } 
        // Get value from cache
        return get_local(local_idx);
    }

    // Returns the int at the given index (IN THE LOCAL CACHE)
    // Assumes the local cache is populated
    // Input index out of bounds will cause out of bounds error
    int get_local(size_t idx) {
        return cells_[idx];
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

        // To avoid read/write conflicts with local cache:
        //  Force cache to be re-loaded after a set call
        cached_chunk_idx = num_chunks;
    }

    // Add more keys to our lists of keys to accomodate for more items
    void resize() {
        size_t old_num_chunks = num_chunks;
        resize_keys_dist();
        resize_missings_dist();

        // add default int array to new chunks
        for (size_t i = old_num_chunks; i < num_chunks; i++) {
            store->put_(chunk_keys[i], cells_, INTERNAL_CHUNK_SIZE);
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

        // force cache refresh
        cached_chunk_idx = num_chunks;
    }

    // Add "missing" (0) to bottom of column
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        length++;
        // Default value in store already exists,
        // Mark value as missing and increment length
        set_missing_dist(length - 1, true);

        // force cache refresh
        cached_missings_idx = num_chunks;
    }
};

/*************************************************************************
 * DistributedBoolColumn 
 * Holds bool values through the use of a KVS (Store).
 */
class DistributedBoolColumn : public DistributedColumn, public BoolColumn {
   public:
    // Create empty bool column
    DistributedBoolColumn(Store* s) : DistributedColumn(s), BoolColumn() {
        // Put default bool array for each chunk
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], cells_, INTERNAL_CHUNK_SIZE);
        }
    }

    // Copy constructor. Assumes other column is the same type as this one
    DistributedBoolColumn(Store* s, DistributedBoolColumn* col) 
        : DistributedColumn(s), BoolColumn() {
        // Put default bool array for each chunk
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], cells_, INTERNAL_CHUNK_SIZE);
        }

        // Copy over data from other column
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            bool row_val = col->as_bool()->get(row_idx);
            push_back(row_val);

            if (col->is_missing_dist(row_idx)) {
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

    // Return this column as a BoolColumn
    BoolColumn* as_bool() { return this; }

    // Returns the bool at the given index.
    // Input index out of bounds will cause a runtime error
    bool get(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        // Load chunk into cache if its not
        if (array_idx != cached_chunk_idx) {
            // Free old cache
            delete[] cells_;
            Key* k = chunk_keys[array_idx];
            cells_ = store->get_bool_array_(k);
            cached_chunk_idx = array_idx;
        }
        // Get value from cache
        return get_local(local_idx);
    }

    // Returns the int at the given index (IN THE LOCAL CACHE)
    // Assumes the local cache is populated
    // Input index out of bounds will cause out of bounds error
    bool get_local(size_t idx) {
        return cells_[idx];
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

        // To avoid read/write conflicts with local cache:
        //  Force cache to be re-loaded after a set call
        cached_chunk_idx = num_chunks;
    }

    // Add more keys to our lists of keys to accomodate for more items
    void resize() {
        size_t old_num_chunks = num_chunks;
        resize_keys_dist();
        resize_missings_dist();

        // add default int array to new chunks
        for (size_t i = old_num_chunks; i < num_chunks; i++) {
            store->put_(chunk_keys[i], cells_, INTERNAL_CHUNK_SIZE);
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

        // force cache refresh
        cached_chunk_idx = num_chunks;
    }

    // Add "missing" (true) to bottom of column
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        length++;
        // Default value in store already exists,
        // Mark value as missing and increment length
        set_missing_dist(length - 1, true);

        // force cache refresh
        cached_missings_idx = num_chunks;
    }
};

/*************************************************************************
 * DistributedFloatColumn 
 * Holds float values through the use of a KVS (Store).
 */
class DistributedFloatColumn : public DistributedColumn, public FloatColumn {
   public:
    // Create empty float column
    DistributedFloatColumn(Store* s) : DistributedColumn(s), FloatColumn() {
        // Put default float array for each chunk
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], cells_, INTERNAL_CHUNK_SIZE);
        }
    }

    // Copy constructor. Assumes other column is the same type as this one
    DistributedFloatColumn(Store* s, DistributedFloatColumn* col) 
        : DistributedColumn(s), FloatColumn() {
        // Put default float array for each chunk
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], cells_, INTERNAL_CHUNK_SIZE);
        }

        // Copy over data from other column
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            float row_val = col->as_float()->get(row_idx);
            push_back(row_val);

            if (col->is_missing_dist(row_idx)) {
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

    // Return this column as a FloatColumn
    FloatColumn* as_float() { return this; }

    // Returns the float at the given index.
    // Input index out of bounds will cause a runtime error
    float get(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        // Load chunk into cache if its not
        if (array_idx != cached_chunk_idx) {
            // Free old cache
            delete[] cells_;
            Key* k = chunk_keys[array_idx];
            cells_ = store->get_float_array_(k);
            cached_chunk_idx = array_idx;
        }

        // Get value from cache
        return get_local(local_idx);
    }

    // Returns the float at the given index (IN THE LOCAL CACHE)
    // Assumes the local cache is populated
    // Input index out of bounds will cause out of bounds error
    float get_local(size_t idx) {
        return cells_[idx];
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
        // To avoid read/write conflicts with local cache:
        //  Force cache to be re-loaded after a set call
        cached_chunk_idx = num_chunks;  // Unusable chunk idx
    }

    // Add more keys to our lists of keys to accomodate for more items
    void resize() {
        size_t old_num_chunks = num_chunks;
        resize_keys_dist();
        resize_missings_dist();

        // Put default float array for each chunk
        for (size_t i = old_num_chunks; i < num_chunks; i++) {
            store->put_(chunk_keys[i], cells_, INTERNAL_CHUNK_SIZE);
        }
    }

    // Add float to "bottom" of column
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

        // force cache refresh
        cached_chunk_idx = num_chunks;
    }

    // Add "missing" (0.0) to bottom of column
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        length++;
        // Default value in store already exists,
        // Mark value as missing and increment length
        set_missing_dist(length - 1, true);

        // force cache refresh
        cached_chunk_idx = num_chunks;
    }
};

/*************************************************************************
 * DistributedStringColumn 
 * Holds String* values through the use of a KVS (Store).
 */
class DistributedStringColumn : public DistributedColumn, public StringColumn {
   public:
    // Create empty String* column
    DistributedStringColumn(Store* s) 
    : DistributedColumn(s), StringColumn() {
        // Put default string array for each chunk
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], cells_, INTERNAL_CHUNK_SIZE);
        }
    }

    // Copy constructor. Assumes other column is the same type as this one
    DistributedStringColumn(Store* s, DistributedStringColumn* col) 
        : DistributedColumn(s), StringColumn() {
        // Put default string array for each chunk
        for (size_t i = 0; i < num_chunks; i++) {
            store->put_(chunk_keys[i], cells_, INTERNAL_CHUNK_SIZE);
        }

        // Copy over data from other column
        for (size_t row_idx = 0; row_idx < col->size(); row_idx++) {
            String* row_val = col->as_string()->get(row_idx);
            push_back(row_val);

            if (col->is_missing_dist(row_idx)) {
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
        // Memory associated with cells/missing is deleted in normal StringColumn

        // Normal string column does not delete String* it owns, but Distributed needs to
        // because they represent the cache, not external strings.
        for (size_t i = 0; i < INTERNAL_CHUNK_SIZE; i++) {
            delete cells_[i]; // delete String*
        }

        // delete[] cells_ handled in StringColumn destructor
    }

    // Return this column as a StringColumn
    StringColumn* as_string() { return this; }

    // Returns the String* at the given index.
    // Input index out of bounds will cause a runtime error
    String* get(size_t idx) {
        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        // Load chunk into cache if its not
        if (array_idx != cached_chunk_idx) {
            // Free old cache
            delete_string_cells_(cells_);
            Key* k = chunk_keys[array_idx];
            cells_ = store->get_string_array_(k);
            cached_chunk_idx = array_idx;
        }
        // Get value from cache
        return get_local(local_idx);
    }

    // Returns the String* at the given index (IN THE LOCAL CACHE)
    // Assumes the local cache is populated
    // Input index out of bounds will cause out of bounds error
    String* get_local(size_t idx) {
        return cells_[idx];
    }

    /** Set value at idx. An out of bound idx is undefined.  */
    void set(size_t idx, String* val) {
        if (idx >= length) {
            printf("WARN: Tried to set value in String column out of bounds.\n");
            return;
        }

        size_t array_idx = idx / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = idx % INTERNAL_CHUNK_SIZE;
        Key* k = chunk_keys[array_idx];

        // Get current cells and set new value in store
        String** cells = store->get_string_array_(k);
        String* replaced_value = cells[local_idx];
        cells[local_idx] = val;
        store->put_(k, cells, INTERNAL_CHUNK_SIZE);

        // Put old value back into cells and delete the list
        cells[local_idx] = replaced_value;
        delete_string_cells_(cells); 

        // We may be overwriting a missing, so mark cell as not-missing
        set_missing_dist(idx, false);

        // To avoid read/write conflicts with local cache:
        //  Force cache to be re-loaded after a set call
        cached_chunk_idx = num_chunks;
    }


    // Add more keys to our lists of keys to accomodate for more items
    void resize() {
        size_t old_num_chunks = num_chunks;
        resize_keys_dist();
        resize_missings_dist();

        // Put default string array for each chunk
        for (size_t i = old_num_chunks; i < num_chunks; i++) {
            store->put_(chunk_keys[i], cells_, INTERNAL_CHUNK_SIZE);
        }
    }

    // Add String* to "bottom" of column
    void push_back(String* val) {
        if (length == capacity) {
            resize();
        }

        size_t array_idx = length / INTERNAL_CHUNK_SIZE;  // Will round down (floor)
        size_t local_idx = length % INTERNAL_CHUNK_SIZE;
        Key* k = chunk_keys[array_idx];

        // Get current cells and set new value in store
        String** cells = store->get_string_array_(k);
        String* replaced_value = cells[local_idx];
        cells[local_idx] = val;
        store->put_(k, cells, INTERNAL_CHUNK_SIZE);

        // Put old value back into cells and delete the list
        cells[local_idx] = replaced_value;
        delete_string_cells_(cells); 

        // No need to call set_missing_dist because default is false

        // To avoid read/write conflicts with local cache:
        //  Force cache to be re-loaded after a set call
        cached_chunk_idx = num_chunks;   
        
        length++;
    }

    // Add "missing" (0) to bottom of column
    void push_back_missing() {
        if (length == capacity) {
            resize();
        }
        length++;
        // Default value in store already exists,
        // Mark value as missing and increment length
        set_missing_dist(length - 1, true);

        // force cache refresh
        cached_chunk_idx = num_chunks;
    }

    // Deletes array of string pointers
    void delete_string_cells_(String** cells) {
        for (size_t i = 0; i < INTERNAL_CHUNK_SIZE; i++) {
            delete cells[i]; // delete String*
        }

        delete[] cells;
    }
};
