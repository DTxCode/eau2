#pragma once
#include "../utils/map.h"
#include "dataframe/dataframe.h"
#include "key.h"
#include "network/message.h"
#include "network/node.h"

// Represents a KeyValue with local data as well as the capability to fetch data from other KeyValue stores.
class Store : public Node {
   public:
    Map *map;
    size_t node_id;
    Serializer *serializer;

    Store(size_t node_id, char *my_ip_address, int my_port, char *server_ip_address, int server_port) : Node(my_ip_address, my_port, server_ip_address, server_port) {
        this->node_id = node_id;
        Map = new Map();
        serializer = new Serializer();
    }

    ~Store() {
        // Not deleting values in map?
        delete map;
        delete serializer;
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
        size_t key_home = key->get_home_node();
        char *value = "";  //TODO:serializer->serialize_df(df);

        if (key_home == node_id) {
            // Value belongs on this node
            map->put(key->get_name(), value);
        } else {
            // Value belongs on another node
            // TODO
            // - use key_home to index into known_nodes list
            // - send that node a PUT message with the value string
            exit_with_msg("Unimplemented: put a key to another node");
        }
    }

    // Gets the value associated with the given key, possibly from another node,
    // and returns as a DataFrame. If key doesn't exist, returns nullptr.
    DataFrame *get(Key *key) {
        size_t key_home = key->get_home_node();

        if (key_home == node_id) {
            Object *val = map->get(key->get_name());

            if (val == nullptr) {
                // Key does not exist
                return nullptr;
            }

            DataFrame *df = nullptr;  // TODO: serializer->deserailize_df(val);
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
                val = map->get(key_name);
            }

            DataFrame *df = nullptr;  // TODO: serializer->deserailize_df(val);
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

        // Send ACK
        Message ack(my_ip_address, my_port, ACK, "");
        network->write_msg(connected_socket, &ack);
    }
};