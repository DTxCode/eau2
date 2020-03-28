#pragma once
#include <stdlib.h>
#include <mutex>
#include "../utils/map.h"
#include "../utils/string.h"
#include "key.h"
#include "network/node.h"

// Represents a KeyValue with local data as well as the capability to fetch data from other KeyValue stores.
// USAGE:
//   - User calls public Store.get() and Store.put() that work on Distributed DataFrames only
//   - Distributed DataFrame calls private Store.get_() and Store.put_() that work on normal Dataframes only
class Store : public Node {
   public:
    Map* map;
    std::mutex map_lock;
    size_t node_id;

    Store(size_t node_id, char* my_ip_address, int my_port, char* server_ip_address, int server_port);

    size_t this_node();
    size_t num_nodes();

    void put_(Key* k, bool* bools);
    void put_(Key* k, int* ints);
    void put_(Key* k, float* floats);
    void put_(Key* k, String* strings);

    bool* get_bool_array_(Key* k);
    int* get_int_array_(Key* k);
    float* get_float_array_(Key* k);
    String* get_string_array_(Key* k);
};