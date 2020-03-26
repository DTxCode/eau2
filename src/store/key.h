#pragma once
#include <stdlib.h>
#include "../utils/helper.h"
#include "../utils/object.h"

// Represents a Key in a KeyValue Store
// Does not own or copy any of its data
class Key : public Object {
   public:
    char* name;
    size_t home_node;

    // Constructs a key from the given name and home_node. Uses a copy of the given name.
    Key(char* name, size_t home_node) {
        Sys s;
        this->name = s.duplicate(name);
        this->home_node = home_node;
    }

    ~Key() {
        delete[] name;
    }

    char* get_name() {
        return name;
    }

    size_t get_home_node() {
        return home_node;
    }

    size_t hash_me() {
        size_t name_length = strlen(name);
        return name_length * (home_node + 1 + name_length);
    }

    Key* clone() {
        return new Key(name, home_node);
    }

    bool equals(Object* other) {
	if (other == nullptr) return false;
	if (other == this) return true;

	Key* other_key = dynamic_cast<Key*>(other);

	if (other_key == nullptr) return false;

	if (equal_strings(other_key->name, name) && other_key->home_node == home_node) return true;


	return false;
	}
};
