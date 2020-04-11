/*
* Authors: Ryan Heminway (heminway.r@husky.neu.edu), David Tandetnik
* CS4500 A1 Part 2
*/
#include "../../src/utils/map.h"  // Note: make sure this import comes last
#include "../../src/utils/object.h"
#include "../../src/utils/string.h"

String* keys[5];
Object* o_vals[5];
String* s_vals[5];
Map* map;
bool init = false;

// Initialize lists of Strings and Objects for all tests to use
void init_data() {
    if (init) {  // Only need to init once
        return;
    }
    map = new Map();

    s_vals[0] = new String("hello");
    s_vals[1] = new String("WOrLD");
    s_vals[2] = new String("juice ");
    s_vals[3] = new String("ryan!");
    s_vals[4] = new String("ryan!");

    o_vals[0] = new Object();
    o_vals[0] = new Object();
    o_vals[0] = new Object();
    o_vals[0] = new Object();
    o_vals[0] = new Object();

    keys[0] = new String("key1");
    keys[1] = new String("key2");
    keys[2] = new String("key3");
    keys[3] = new String("key4");
    keys[4] = new String("key5");

    init = true;
}

// Cleanup all data created by init_data
void cleanup() {
    if (!init) {  // Only clean if necessary
        return;
    }
    // Free all heap allocated data
    for (int i = 0; i < 5; i++) {
        delete s_vals[i];
        delete o_vals[i];
        delete keys[i];
    }
    delete map;
    init = false;
}

// Confirm the 'has_key' method returns true when the key should exist
// and false when it shouldnt
// Returns true if test passed, false if it failed
bool test_has_key() {
    init_data();
    map->put(keys[0], o_vals[0]);
    map->put(keys[1], s_vals[1]);
    map->put(keys[2], o_vals[1]);
    bool passed = true;
    passed = passed && map->has_key(keys[0]);
    passed = passed && map->has_key(keys[1]);
    passed = passed && map->has_key(keys[2]);
    passed = passed && !map->has_key(keys[3]);
    passed = passed && !map->has_key(keys[4]);

    cleanup();
    return passed;
}

// Confirm the 'get' method returns the correct values
// Returns true if test passed, false if it failed
bool test_get() {
    init_data();
    map->put(keys[0], o_vals[0]);
    map->put(keys[1], s_vals[1]);
    map->put(keys[2], o_vals[1]);
    bool passed = true;
    passed = passed && (o_vals[0]->equals(map->get(keys[0])));
    passed = passed && (map->size() == 3);
    passed = passed && (s_vals[1]->equals(map->get(keys[1])));
    passed = passed && (map->size() == 3);
    passed = passed && (o_vals[1]->equals(map->get(keys[2])));
    passed = passed && !(o_vals[0]->equals(map->get(keys[2])));
    passed = passed && !(s_vals[1]->equals(map->get(keys[3])));
    passed = passed && (nullptr == map->get(keys[4]));
    passed = passed && (nullptr == map->get(keys[3]));

    cleanup();
    return passed;
}

// Confirm the 'put' method works, assuming 'get' works
// Returns true if test passed, false if it failed
bool test_put() {
    init_data();
    map->put(keys[0], o_vals[0]);
    map->put(keys[3], s_vals[2]);
    map->put(keys[4], s_vals[3]);
    bool passed = true;
    passed = passed && (map->size() == 3);
    passed = passed && map->has_key(keys[0]);
    passed = passed && map->has_key(keys[3]);
    passed = passed && map->has_key(keys[4]);
    passed = passed && !map->has_key(keys[1]);
    passed = passed && !map->has_key(keys[2]);
    passed = passed && (o_vals[0]->equals(map->get(keys[0])));
    passed = passed && (s_vals[2]->equals(map->get(keys[3])));
    passed = passed && (s_vals[3]->equals(map->get(keys[4])));
    passed = passed && !(o_vals[0]->equals(map->get(keys[2])));
    passed = passed && !(s_vals[1]->equals(map->get(keys[3])));
    passed = passed && (nullptr == map->get(keys[1]));
    passed = passed && (nullptr == map->get(keys[2]));

    cleanup();
    return passed;
}

// Confirm remove method returns correct object, assuming put method works
// Returns true if test passes, false if it fails
bool test_remove() {
    init_data();
    map->put(keys[0], o_vals[0]);
    map->put(keys[3], s_vals[2]);
    map->put(keys[4], s_vals[3]);
    bool passed = true;
    passed = passed && (map->size() == 3);
    passed = passed && map->has_key(keys[0]);
    passed = passed && o_vals[0]->equals(map->remove(keys[0]));
    passed = passed && s_vals[2]->equals(map->remove(keys[3]));
    passed = passed && (map->size() == 1);
    passed = passed && !o_vals[0]->equals(map->remove(keys[4]));
    passed = passed && (map->size() == 0);

    cleanup();
    return passed;
}

// Confirm 'size' method returns the number of keys in the map
// Returns true if test passes, false if it fails
bool test_size() {
    init_data();
    map->put(keys[0], o_vals[0]);
    map->put(keys[3], s_vals[2]);
    map->put(keys[4], s_vals[3]);
    bool passed = true;
    passed = passed && (map->size() == 3);
    map->remove(keys[3]);
    passed = passed && (map->size() == 2);
    map->get(keys[1]);
    passed = passed && (map->size() == 2);
    map->remove(keys[0]);
    map->remove(keys[4]);
    passed = passed && (map->size() == 0);
    map->put(keys[4], s_vals[3]);
    passed = passed && (map->size() == 1);

    cleanup();
    return passed;
}

// Main to run all tests
int main(int argc, char** argv) {
    test_get();
    test_has_key();
    test_put();
    test_remove();
    test_size();
}
