#pragma once
#include <mutex>
#include "../utils/map.h"
#include "dataframe/dataframe.h"
#include "key.h"
#include "network/message.h"
#include "network/node.h"
#include "serial.h"

#define GETANDWAIT_SLEEP 25

// Represents a KeyValue with local data as well as the capability to fetch data from other KeyValue stores.
// USAGE:
//   - User calls public Store.get() and Store.put() that work on Distributed DataFrames only
//   - Distributed DataFrame calls private Store.get_() and Store.put_() that work on normal Dataframes only
class Store : public Node {
   public:
    Map *map;
    std::mutex map_lock;
    size_t node_id;

    Store(size_t node_id, char *my_ip_address, int my_port, char *server_ip_address, int server_port) : Node(my_ip_address, my_port, server_ip_address, server_port) {
        this->node_id = node_id;
        map = new Map();
    }

    ~Store() {
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
    size_t this_node() {
        return node_id;
    }

    // Returns number of nodes in the network
    size_t num_nodes() {
        known_nodes_lock.lock();
        size_t num_nodes = known_nodes->size();
        known_nodes_lock.unlock();

        return num_nodes;
    }

    // Saves the given Dataframe under the given key, possibly on another node
    // Helper method for Distributed DataFrames. Not meant to be used by end users.
    // Uses copies of given key/DF (does not modify or delete them)
    void put_(Key *key, DataFrame *df) {
        char *value = serializer->serialize_dataframe(df);

        put_char_(key, value);

        delete[] value;
    }

    // Saves the given char* to the given key. For internal use only.
    // Uses copies of given key/value (does not modify or delete them)
    void put_char_(Key *key, char *value) {
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

    // Asks another node to PUT the given key/value
    void send_put_request_(Key *key, char *value) {
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

    // Gets the value associated with the given key, possibly from another node,
    // and returns as a DataFrame. If key doesn't exist, returns nullptr.
    // Helper method for Distributed DataFrames. Not meant to be used by end users.
    DataFrame *get_(Key *key) {
        char *serialized_df = get_char_(key);

        if (serialized_df == nullptr) {
            return nullptr;
        }

        DataFrame *df = serializer->deserialize_dataframe(serialized_df);

        delete[] serialized_df;

        return df;
    }

    // Gets a copy of the value associated with the given key, possibly from another node,
    // and returns as a char*. If key doesn't exist, returns nullptr.
    // For internal use only.
    char *get_char_(Key *key) {
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
    char *send_get_request_(Key *key) {
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

    // Gets the value associated with the given key, possibly from another node,
    // and returns as a DataFrame. If key doesn't exist, the method blocks until it does.
    // Helper method for Distributed DataFrames. Not meant to be used by end users.
    DataFrame *getAndWait_(Key *key) {
        DataFrame *df = get_(key);
        
	std::this_thread::sleep_for(std::chrono::milliseconds(5));
 
        // TODO ways to improve this:
        // - for local get, could do waitAndNotify method instead of this busy loop
        // - for network get, use above method + timeout
        while (df == nullptr) {
            std::this_thread::sleep_for(std::chrono::milliseconds(GETANDWAIT_SLEEP));
            df = get_(key);
        }

        return df;
    }

    // OVERRIDE
    // Handler for all messages that come into this node
    // Some sort of reply is expected to be written to the given socket
    // By default, sends empty ACK back to the message sender and prints a "message-received" string
    void
    handle_message(int connected_socket, Message *msg) {
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
    void handle_put_(int connected_socket, Message *msg) {
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
    void handle_get_(int connected_socket, Message *msg) {
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
};

// Stores count vals in a single column in a DataFrame. Saves that DF in store under key and returns it.
// Count must be less than or equal to the number of floats in vals
DataFrame *DataFrame::fromArray(Key *key, Store *store, size_t count, float *vals) {
    Schema *empty_schema = new Schema();
    DataFrame *df = new DataFrame(*empty_schema);

    FloatColumn col;

    for (size_t i = 0; i < count; i++) {
        col.push_back(vals[i]);
    }

    // add column to DF
    df->add_column(&col, nullptr);

    // add DF to store under key
    store->put_(key, df);

    return df;
}
