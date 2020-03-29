#pragma once
#include "store.h"
#include <mutex>
#include "../utils/map.h"
#include "dataframe/dataframe.h"
#include "key.h"
#include "network/message.h"
#include "network/node.h"
#include "serial.cpp"

#define GETANDWAIT_SLEEP 25

Store::Store(size_t node_id, char *my_ip_address, int my_port, char *server_ip_address, int server_port) : Node(my_ip_address, my_port, server_ip_address, server_port) {
    this->node_id = node_id;
    map = new Map();
}

Store::~Store() {
    // Delete both keys and values from our map
    map_lock.lock();
    List *keys = map->keys();
    for (size_t i = 0; i < keys->size(); i++) {
        Object *key = keys->get(i);
        Object *val = map->get(key);

        delete key;
        delete val;
    }

    delete keys;
    delete map;
    map_lock.unlock();
}

// Returns the ID of this node
size_t Store::this_node() {
    return node_id;
}

// Returns number of nodes in the network
size_t Store::num_nodes() {
    known_nodes_lock.lock();
    size_t num_nodes = known_nodes->size();
    known_nodes_lock.unlock();

    return num_nodes;
}

// Stores the given DistributedDataFrame in the store, possibly on another node.
// Does not modify or delete given vales
void Store::put(Key *k, DistributedDataFrame *df) {
    char *value = serializer->serialize_distributed_dataframe(df);

    put_char_(k, value);

    delete[] value;
}

// Saves the given char* to the given key. For internal use only.
// Uses copies of given key/value (does not modify or delete them)
void Store::put_char_(Key *key, char *value) {
    size_t key_home = key->get_home_node();

    if (key_home == node_id) {
        // Value belongs on this node
        String *val = new String(value);  // deleted in destructor
        map_lock.lock();
        map->put(key->clone(), val);
        map_lock.unlock();
    } else {
        // Value belongs on another node
        send_put_request_(key, value);
    }
}

/*
    The following put_ methods save the given arrays under the given key, possibly on another node/
    They are helper method for DistributedColumns. Not meant to be used by end users.
    Uses copies of given key/array (does not modify or delete them)
*/
void Store::put_(Key *k, bool *bools, size_t num) {
    char *value = serializer->serialize_bools(bools, num);

    put_char_(k, value);

    delete[] value;
}

void Store::put_(Key *k, int *ints, size_t num) {
    char *value = serializer->serialize_ints(ints, num);

    put_char_(k, value);

    delete[] value;
}

void Store::put_(Key *k, float *floats, size_t num) {
    char *value = serializer->serialize_floats(floats, num);

    put_char_(k, value);

    delete[] value;
}

void Store::put_(Key *k, String **strings, size_t num) {
    char *value = serializer->serialize_strings(strings, num);

    put_char_(k, value);

    delete[] value;
}

// Asks another node to PUT the given key/value
void Store::send_put_request_(Key *key, char *value) {
    char *key_str = key->get_name();
    size_t key_home = key->get_home_node();

    known_nodes_lock.lock();
    String *other_node_address = known_nodes->get(key_home);
    known_nodes_lock.unlock();

    char *other_node_host = network->get_host_from_address(other_node_address);
    int other_node_port = network->get_port_from_address(other_node_address);

    // Create PUT message to send to the other node, consisting of the format
    // [KEY_STRING]~[VALUE]
    char msg[strlen(key_str) + 1 + strlen(value)];
    sprintf(msg, "%s~%s", key_str, value);

    Message *response = send_msg(other_node_host, other_node_port, PUT, msg);

    if (response->msg_type != ACK) {
        printf("Node %zu did not get successful ACK for its PUT request to node %zu\n", node_id, key_home);
        exit(1);
    }

    delete response;
    delete[] other_node_host;
}

// Gets the given key for a DistributedDataFrame from the store, possibly from another node.
// If key doesn't exist, returns nullptr.
// Does not modify or delete given key
DistributedDataFrame *Store::get(Key *k) {
    char *serialized_df = get_char_(k);

    if (serialized_df == nullptr) {
        return nullptr;
    }

    DistributedDataFrame *df = serializer->deserialize_distributed_dataframe(serialized_df);

    delete[] serialized_df;

    return df;
}

/*
    The following get_ methods save the given arrays under the given key, possibly on another node.
    They are helper method for DistributedColumns. Not meant to be used by end users.
    Uses copies of given key (does not modify or delete it)
*/
bool *Store::get_bool_array_(Key *k) {
    char *serialized_array = get_char_(k);

    if (serialized_array == nullptr) {
        return nullptr;
    }

    bool *bools = serializer->deserialize_bools(serialized_array);

    delete[] serialized_array;

    return bools;
}

int *Store::get_int_array_(Key *k) {
    char *serialized_array = get_char_(k);

    if (serialized_array == nullptr) {
        return nullptr;
    }

    int *ints = serializer->deserialize_ints(serialized_array);

    delete[] serialized_array;

    return ints;
}

float *Store::get_float_array_(Key *k) {
    char *serialized_array = get_char_(k);

    if (serialized_array == nullptr) {
        return nullptr;
    }

    float *floats = serializer->deserialize_floats(serialized_array);

    delete[] serialized_array;

    return floats;
}

String **Store::get_string_array_(Key *k) {
    char *serialized_array = get_char_(k);

    if (serialized_array == nullptr) {
        return nullptr;
    }

    String **strings = serializer->deserialize_strings(serialized_array);

    delete[] serialized_array;

    return strings;
}

// Gets a copy of the value associated with the given key, possibly from another node,
// and returns as a char*. If key doesn't exist, returns nullptr.
// For internal use only.
char *Store::get_char_(Key *key) {
    size_t key_home = key->get_home_node();

    if (key_home == node_id) {
        map_lock.lock();
        Object *value_obj = map->get(key);
        map_lock.unlock();

        if (value_obj == nullptr) {
            // Key does not exist
            return nullptr;
        }

        // All objects in the map should be string*
        String *val_string = dynamic_cast<String *>(value_obj);
        char *value = val_string->c_str();

        return duplicate(value);
    } else {
        // Value maybe lives on another node
        char *value = send_get_request_(key);
        return value;
    }
}

// Asks another node to GET the value associated with the given key
// Returns a heap-allocated value, or nullptr
char *Store::send_get_request_(Key *key) {
    char *key_str = key->get_name();
    size_t key_home = key->get_home_node();

    known_nodes_lock.lock();
    String *other_node_address = known_nodes->get(key_home);
    known_nodes_lock.unlock();

    char *other_node_host = network->get_host_from_address(other_node_address);
    int other_node_port = network->get_port_from_address(other_node_address);

    // Send GET request to another node with key we're asking for
    Message *response = send_msg(other_node_host, other_node_port, GET, key_str);

    if (response->msg_type == NACK) {
        // key does not exist
        return nullptr;
    } else if (response->msg_type != ACK) {
        printf("Node %zu did not get successful NACK or ACK for its GET request to node %zu\n", node_id, key_home);
        exit(1);
    }

    char *serialized_value = duplicate(response->msg);

    delete response;
    delete[] other_node_host;

    return serialized_value;
}

// Gets the given key for a DistributedDataFrame from the store, possibly from another node.
// If key doesn't exist, blocks until it does. Never returns nullptr.
// Does not modify or delete given key
DistributedDataFrame *Store::waitAndGet(Key *k) {
    DistributedDataFrame *df = get(k);

    // TODO ways to improve this:
    // - for local get, could do waitAndNotify method instead of this busy loop
    // - for network get, use above method + timeout
    while (df == nullptr) {
        std::this_thread::sleep_for(std::chrono::milliseconds(GETANDWAIT_SLEEP));
        df = get(k);
    }

    return df;
}

// OVERRIDE
// Handler for all messages that come into this node
// Some sort of reply is expected to be written to the given socket
// By default, sends empty ACK back to the message sender and prints a "message-received" string
void Store::handle_message(int connected_socket, Message *msg) {
    printf("Node %s:%d got message from another node with type %d and contents \"%s\"\n", my_ip_address, my_port, msg->msg_type, msg->msg);

    if (msg->msg_type == PUT) {
        handle_put_(connected_socket, msg);
    } else if (msg->msg_type == GET) {
        handle_get_(connected_socket, msg);
    } else {
        printf("WARN: Store got a message from another node with unknown message type %d\n", msg->msg_type);
    }
}

// Called when this store gets a PUT request from another node
void Store::handle_put_(int connected_socket, Message *msg) {
    char *msg_contents = msg->msg;

    // put together Key
    char *key_str = strtok(msg_contents, "~");
    Key key(key_str, node_id);  // This node got a PUT request, so the key must live on this node.

    // put together value_str
    char *val_str = strtok(nullptr, "\0");

    // save to map
    put_char_(&key, val_str);

    // Send ACK
    Message ack(my_ip_address, my_port, ACK, (char *)"");
    network->write_msg(connected_socket, &ack);
}

// Called when this store gets a GET request from another node
void Store::handle_get_(int connected_socket, Message *msg) {
    // Message consists of just the key
    char *key_str = msg->msg;
    Key key(key_str, node_id);  // This node got a GET request, so the key must live on this node.

    char *serialized_value = get_char_(&key);

    if (serialized_value == nullptr) {
        // Value doesn't exist, so send NACK
        Message nack(my_ip_address, my_port, NACK, (char *)"");
        network->write_msg(connected_socket, &nack);
    } else {
        // Send ACK with serialized value
        Message ack(my_ip_address, my_port, ACK, serialized_value);
        network->write_msg(connected_socket, &ack);

        delete[] serialized_value;
    }
}

// The following formArray methods store `count` `vals` in a single column in a DistributedDataFrame.
// Saves that DDF in store under key and returns it.
// Count must be less than or equal to the number of floats in vals
DistributedDataFrame *DataFrame::fromArray(Key *key, Store *store, size_t count, float *vals) {
    DistributedFloatColumn col(store);

    for (size_t i = 0; i < count; i++) {
        col.push_back(vals[i]);
    }

    return fromDistributedColumn(key, store, &col);
}

DistributedDataFrame *DataFrame::fromArray(Key *key, Store *store, size_t count, bool *vals) {
    DistributedBoolColumn col(store);

    for (size_t i = 0; i < count; i++) {
        col.push_back(vals[i]);
    }

    return fromDistributedColumn(key, store, &col);
}

DistributedDataFrame *DataFrame::fromArray(Key *key, Store *store, size_t count, int *vals) {
    DistributedIntColumn col(store);

    for (size_t i = 0; i < count; i++) {
        col.push_back(vals[i]);
    }

    return fromDistributedColumn(key, store, &col);
}

DistributedDataFrame *DataFrame::fromArray(Key *key, Store *store, size_t count, String **vals) {
    DistributedStringColumn col(store);

    for (size_t i = 0; i < count; i++) {
        col.push_back(vals[i]);
    }

    return fromDistributedColumn(key, store, &col);
}

// Stores copy of col in store under key
DistributedDataFrame *DataFrame::fromDistributedColumn(Key *key, Store *store, DistributedColumn *col) {
    Schema *empty_schema = new Schema();
    DistributedDataFrame *df = new DistributedDataFrame(store, *empty_schema);

    // add column to DF
    df->add_column(col, nullptr);

    // add DF to store under key
    store->put(key, df);

    return df;
}

// The following fromScalar methods store `val` in a single cell in a DistributedDataFrame.
// Saves that DDF in store under key and returns it.
DistributedDataFrame *DataFrame::fromScalar(Key *key, Store *store, float val) {
    DistributedFloatColumn col(store, 1, val);

    return fromDistributedColumn(key, store, &col);
}

DistributedDataFrame *DataFrame::fromScalar(Key *key, Store *store, bool val) {
    DistributedBoolColumn col(store, 1, val);

    return fromDistributedColumn(key, store, &col);
}

DistributedDataFrame *DataFrame::fromScalar(Key *key, Store *store, int val) {
    DistributedIntColumn col(store, 1, val);

    return fromDistributedColumn(key, store, &col);
}

DistributedDataFrame *DataFrame::fromScalar(Key *key, Store *store, String *val) {
    DistributedStringColumn col(store, 1, val);

    return fromDistributedColumn(key, store, &col);
}
