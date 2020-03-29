#pragma once

#include <stdlib.h>
#include "list.h"
#include "object.h"
#include "pair.h"

// Represents a mapping from Objects to Objects based on their hashcodes. Much
// of the interface comes from Java's HashMap.
class Map : public Object {
   public:
    // Array of list pointers. Keys are hashed and then modded by the length of this list to find the index of
    // the "bucket" they belong in.
    List** value_lists;
    List* keys_;            // List of keys in this map
    List* values_;          // List of values in this map
    size_t key_space;       // Number of buckets available in the map
    size_t used_key_space;  // Number of buckets with occupancy > 0
    float max_load_factor;  // when (used / key_space) equals this value, rehash

    // Construct an empty map
    Map() {
        key_space = 100;
        used_key_space = 0;
        max_load_factor = 0.75;
        value_lists = create_value_lists_(key_space);
        keys_ = new List();
        values_ = new List();
    }

    // Free memory for map
    ~Map() {
        // Delete pair pointers (we own those)
        delete_entries_();

        delete_value_lists_(value_lists, key_space);

        delete keys_;
        delete values_;
    }

    // Creates array of size key_space where each element is a value_list ("bucket")
    List** create_value_lists_(size_t key_space) {
        List** new_value_lists = new List*[key_space];

        for (size_t i = 0; i < key_space; i++) {
            new_value_lists[i] = new List();
        }

        return new_value_lists;
    }

    // Delete list of value_lists
    void delete_value_lists_(List** value_lists, size_t num_lists) {
        for (size_t i = 0; i < num_lists; i++) {
            // delete individual List()
            delete value_lists[i];
        }

        // delete array of lists
        delete[] value_lists;
    }

    // Deletes all entries (pairs) in this map
    void delete_entries_() {
        for (size_t i = 0; i < key_space; i++) {
            List* value_list = value_lists[i];
            for (size_t j = 0; j < value_list->size(); j++) {
                // Delete the pair pointer
                delete value_list->remove(j);
            }
        }
    }

    // Override. Make a shallow copy of the map (does not clone keys and values)
    virtual Map* clone() {
        Map* new_map = new Map();
        new_map->putAll(this);
        return new_map;
    }

    // Override. Compare to other object
    // Equal maps are two maps with same memory addresses
    // OR two maps with the same elements
    virtual bool equals(Object* other) {
        if (nullptr == other) {
            return false;
        }

        Map* other_map = dynamic_cast<Map*>(other);

        if (nullptr == other_map) {
            return false;
        }

        if (other_map == this) {
            return true;
        }

        if (other_map->size() != size()) {
            return false;
        }

        // Loop over all elements, return false if any key,val pair does not
        // match up between maps
        for (size_t i = 0; i < keys_->size(); i++) {
            Object* key = keys_->get(i);  // list get

            Object* my_val = get(key);
            Object* other_val = other_map->get(key);

            if (!my_val->equals(other_val)) {
                return false;
            }
        }

        return true;
    }

    // Override. Compute hash code for map
    // Hash for a map is the sum of its elements hashes
    virtual size_t hash() {
        size_t hash_ = 0;
        for (size_t i = 0; i < size(); i++) {
            Object* key = keys_->get(i);
            hash_ += key->hash();
        }
        return hash_;
    }

    // Returns true if this map contains the given key
    bool containsKey(Object* key) {
        // Based on indexOf impl: returns >size() if not found
        return (keys_->index_of(key) < keys_->size());
    }

    // Returns true if this map contains the given value
    bool containsValue(Object* value) {
        // Based on indexOf impl: returns >size() if not found
        return (values_->index_of(value) < values_->size());
    }

    // Return the object to which the given key is mapped, or nullptr is one
    // does not exist
    Object* get(Object* key) {
        if (nullptr == key) {
            return nullptr;
        }

        size_t bucket_id = key->hash() % key_space;
        List* value_list = value_lists[bucket_id];

        // Loop through the 'would be' bucket for this key, return a value
        // if one is found with this key
        for (size_t i = 0; i < value_list->size(); i++) {
            Object* entry = value_list->get(i);
            Pair* pair = dynamic_cast<Pair*>(entry);

            // if a key from the pair matches the given key, return the pair val
	    if (pair->get_first()->equals(key)) {
                return pair->get_second();
            }
        }

        return nullptr;
    }

    // Return true if there are no keys in this map, else false
    bool isEmpty() {
        return (keys_->size() == 0);
    }

    // Map the given key to the given value in this map. Returns the previous
    // value associated with that key, or nullptr if none.
    Object* put(Object* key, Object* value) {
        if (nullptr == key) {
            return nullptr;
        }

        if (nullptr == value) {
            return nullptr;
        }

        size_t bucket_id = key->hash() % key_space;
        List* value_list = value_lists[bucket_id];

        Object* replaced = add_to_value_list_(value_list, key, value);

        // Updated existing key. Return the replaced value
        if (replaced) {
            Pair* replaced_pair = dynamic_cast<Pair*>(replaced);
            Object* replaced_value = replaced_pair->get_second();
            delete replaced_pair;

            // Remove the replaced value from list of values and add the new one.
            size_t removed_value_index = values_->index_of(replaced_value);
            values_->remove(removed_value_index);
            values_->push_back(value);

            return replaced_value;
        }

        // Add new key/value to keys and values lists
        keys_->push_back(key);
        values_->push_back(value);

        // If load exceeds max load factor, rehash for performance purposes
        if (((float)used_key_space / key_space) > max_load_factor) {
            // std::cout << "rehashing\n";
            rehash_();
        }

        return nullptr;
    }

    // Adds given key and value as a pair to given value_list ("bucket")
    // Return old value if key already exists
    Object* add_to_value_list_(List* value_list, Object* key, Object* value) {
        size_t value_list_size = value_list->size();
        Pair* new_key_value = new Pair(key, value);
        Object* replaced = nullptr;

        // Add new_key_value to map, checking for if key already exists
        bool key_collision = false;
        for (size_t i = 0; i < value_list_size; i++) {
            Object* exisiting_entry = value_list->get(i);
            Pair* exisiting_pair = dynamic_cast<Pair*>(exisiting_entry);

            // If key collision, replace current value
            if (exisiting_pair->get_first()->equals(key)) {
                replaced = value_list->set(i, new_key_value);
                key_collision = true;
                break;
            }
        }

        if (!key_collision) {
            // No key collisions, place this pair at end of bucket
            value_list->push_back(new_key_value);
        }

        if (value_list->size() == 1) {
            // Created new bucket with this put, so record that used_key_space has increased
            used_key_space++;
        }

        return replaced;
    }

    // Increase the keyspace, and recalculate (and replace) proper buckets for
    // all current elements in the map
    virtual void rehash_() {
        size_t old_key_space = key_space;
        key_space *= 2;
        List** new_value_lists = create_value_lists_(key_space);

        // Loop over all elements in map, moving them to new bucket
        for (size_t i = 0; i < old_key_space; i++) {
            List* value_list = value_lists[i];

            for (size_t j = 0; j < value_list->size(); j++) {
                Pair* p = dynamic_cast<Pair*>(value_list->get(j));

                // Rehash and place in the correct bucket
                Object* key = p->get_first();
                size_t key_hash = key->hash();
                size_t bucket_id = key_hash % key_space;

                new_value_lists[bucket_id]->push_back(p);
            }
        }

        // delete old buckets
        delete_value_lists_(value_lists, old_key_space);

        value_lists = new_value_lists;
    }

    // Copy all the mappings from the given map to this map
    void putAll(Map* map) {
        if (nullptr == map) {
            return;
        }

        // Loop through all keys of given map, get the associated val
        // Place the value into this map with its key
        List* map_keys = map->keys();
        for (size_t i = 0; i < map_keys->size(); i++) {
            Object* map_key = map_keys->get(i);
            Object* map_val = map->get(map_key);
            put(map_key, map_val);
        }

        delete map_keys;
    }

    // Removes the given key's mapping from this map. Returns the value
    // associated with the key, or nullptr if none
    Object* remove(Object* key) {
        if (!key || !containsKey(key)) {
            return nullptr;
        }

        Object* removed_val = nullptr;

        // Key must be in map
        // Loop through all k,v pairs
        size_t bucket_id = key->hash() % key_space;
        List* value_list = value_lists[bucket_id];

        for (size_t j = 0; j < value_list->size(); j++) {
            Pair* pair = dynamic_cast<Pair*>(value_list->get(j));

            // if a key from the pair matches the given, store val for return
            if (pair->get_first()->equals(key)) {
                value_list->remove(j);
                removed_val = pair->get_second();

                // Remove removed key and value
                size_t removed_key_index = keys_->index_of(pair->get_first());
                keys_->remove(removed_key_index);

                size_t removed_value_index = values_->index_of(pair->get_second());
                values_->remove(removed_value_index);

                // Update keyspace if this bucket is now empty
                if (value_list->size() == 0) {
                    used_key_space--;
                }

                break;
            }
        }

        return removed_val;
    }

    // Returns the number of key-value mappings in this map
    size_t size() { return keys_->size(); }

    // Clears all mappings
    void clear() {
        // Delete pair pointers (we own those)
        delete_entries_();

        keys_->clear();
        values_->clear();
        used_key_space = 0;
    }

    // Returns a copy of the list of keys in this map
    List* keys() {
        List* copy_keys = new List();
        copy_keys->add_all(0, keys_);
        return copy_keys;
    }

    // Returns a copy of the list of values in this map
    List* values() {
        List* copy_vals = new List();
        copy_vals->add_all(0, values_);
        return copy_vals;
    }
};
