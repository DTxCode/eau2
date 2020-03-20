#pragma once
#include "../utils/map.h"
#include "dataframe/dataframe.h"
#include "key.h"
#include "network/message.h"
#include "network/node.h"
#include "serial.h"

// Represents a KeyValue with local data as well as the capability to fetch data from other KeyValue stores.
class Store : public Node {
   public:
    Map *map;
    size_t node_id;

    Store(size_t node_id, char *my_ip_address, int my_port, char *server_ip_address, int server_port) : Node(my_ip_address, my_port, server_ip_address, server_port) {
        // connect to the network
        register_and_listen();

        this->node_id = node_id;
        map = new Map();
    }

    ~Store() {
        // We added the String* to map, so we have to delete them
        List *keys = map->keys();
        for (size_t i = 0; i < keys->size(); i++) {
            Object *key = keys->get(i);
            Object *val = map->get(key);

            delete val;
        }

        delete keys;
        delete map;
    }

    // Returns the ID of this node
    size_t this_node() {
        return node_id;
    }

    // Returns number of nodes in the network
    size_t num_nodes() {
        return known_nodes->size();
    }

    // Saves the given Dataframe under the given key, possibly on another node
    // Does not own/delete any of the given data
    void put(Key *key, DataFrame *df) {
        char *value = serializer->serialize_dataframe(df);

        put_(key, value);

        delete[] value;
    }

    // Saves the given char* to the given key. For internal use only.
    // Does not own/delete any of the given data
    void put_(Key *key, char *value) {
        char *key_str = key->get_name();
        size_t key_home = key->get_home_node();

        if (key_home == node_id) {
            // Value belongs on this node
            String *val = new String(value);  // deleted in destructor
            map->put(key, val);
        } else {
            // Value belongs on another node
            // - use key_home to index into known_nodes list
            // - send that node a PUT message with the value string
            String *other_node_address = known_nodes->get(key_home);
            char *other_node_host = network->get_host_from_address(other_node_address);
            int other_node_port = network->get_port_from_address(other_node_address);

            // Create PUT message to send to the other node, consisting of the format
            // [KEY_STRING]~[VALUE]
            char msg[strlen(key_str) + 1 + strlen(value)];
            sprintf(msg, "%s~%s", key_str, value);

            Message *response = send_msg(other_node_host, other_node_port, PUT, msg);

            if (response->msg_type != ACK) {
                printf("Node %d did not get successful ACK for its PUT request to node %d\n", node_id, key_home);
                exit(1);
            }

            delete response;
            delete[] other_node_host;
        }
    }

    // Gets the value associated with the given key, possibly from another node,
    // and returns as a Distributed DataFrame. If key doesn't exist, returns nullptr.
    DataFrame *get(Key *key) {
        size_t key_home = key->get_home_node();

        if (key_home == node_id) {
            Object *val = map->get(key);

            if (val == nullptr) {
                // Key does not exist
                return nullptr;
            }

            // All objects in the map should be string*
            String *df_string = dynamic_cast<String *>(val);

            // Deserialize string
            DataFrame *df = serializer->deserialize_dataframe(df_string->c_str());

            return df;
        } else {
            // Value maybe lives on another node
            // TODO
            // - use key_home to index into known_nodes list
            // - send that node a GET message with the key->get_name()
            // - deserailize result into a DF
            exit_with_msg("Unimplemented: get a key from another node");
        }
    }

    // Same as get(), but if the key doesn't exist, blocks until it does.
    DataFrame *getAndWait(Key *key) {
        size_t key_home = key->get_home_node();
        char *key_name = key->get_name();

        if (key_home == node_id) {
            Object *val;

            while (val == nullptr) {
                val = map->get(key);
            }

            // All objects in the map should be string*
            String *df_string = dynamic_cast<String *>(val);

            // Deserialize string
            DataFrame *df = serializer->deserialize_dataframe(df_string->c_str());

            return df;
        } else {
            // Value maybe lives on another node
            // TODO
            // - use key_home to index into known_nodes list
            // - send that node a GETWAIT message with the key->get_name()
            // - deserailize result into a DF
            exit_with_msg("Unimplemented: getAndWait a key from another node");
        }
    }

    // OVERRIDE
    // Handler for all messages that come into this node
    // Some sort of reply is expected to be written to the given socket
    // By default, sends empty ACK back to the message sender and prints a "message-received" string
    void handle_message(int connected_socket, Message *msg) {
        printf("Node %s:%d got message from another node with type %d and contents \"%s\"\n", my_ip_address, my_port, msg->msg_type, msg->msg);

        if (msg->msg_type == PUT) {
            // handle put request
            char *msg_contents = msg->msg;

            // put together Key
            char *key_str = strtok(msg_contents, "~");
            Key key(key_str, node_id);  // This node got a PUT request, so the key must live on this node.

            // put together value_str
            char *val_str = strtok(nullptr, "\0");

            put_(key, val_str);
        }

        // Send ACK
        Message ack(my_ip_address, my_port, ACK, "");
        network->write_msg(connected_socket, &ack);
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
    store->put(key, df);

    return df;
}
