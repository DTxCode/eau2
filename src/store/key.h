#pragma once
#include <stdlib.h>
#include "../utils/object.h"

// Represents a Key in a KeyValue Store
// Does not own or copy any of its data
class Key : public Object {
   public:
    char* name;
    size_t home_node;

    Key(char* name, size_t home_node) {
        this->name = name;
        this->home_node = home_node;
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
};
