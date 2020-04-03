#pragma once
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
//#include <stropts.h>
#include <sys/socket.h>
#include <unistd.h>
#include <mutex>
#include <thread>
#include "../../utils/array.h"
#include "../serial.h"
#include "message.h"
#include "network.h"

/*
    Represents a Node in a network. Needs to register with a Server to join the network.
    Afterwards it can communicate with the server as well as other Nodes.
*/
class Node {
   public:
    char *my_ip_address;
    int my_port;
    char *server_ip_address;
    int server_port;
    Network *network;
    StringArray *known_nodes;  // List of other nodes in the network. Set to nullptr until the first
                               // directory update from the server

    std::thread *listener;  // thread that listens for incoming connections
    bool registered;        // Whether this node is registered with the master server
    bool shutting_down;     // Whether this node is shutting down. Used by (infinite) spawned thread to know to shut down
    Serializer *serializer;
    std::mutex known_nodes_lock;

    Node(char *my_ip_address, int my_port, char *server_ip_address, int server_port) {
        this->my_ip_address = my_ip_address;
        this->my_port = my_port;
        this->server_ip_address = server_ip_address;
        this->server_port = server_port;
        known_nodes = nullptr;
        listener = nullptr;
        network = new Network();
        registered = false;
        shutting_down = false;
        serializer = new Serializer();
        register_and_listen();
    }

    ~Node() {
        if (listener && listener->joinable()) {
            // object is being destroyed before a shutdown signal from the server
            // Making sure to cleanup listener thread
            shutting_down = true;
            listener->join();
        }

        // Grab known_nodes lock
        known_nodes_lock.lock();
        // Cleanup existing directory list, if it exists
        if (known_nodes != nullptr) {
            for (size_t i = 0; i < known_nodes->size(); i++) {
                delete known_nodes->get(i);
            }

            delete known_nodes;
        }
        known_nodes_lock.unlock();

        delete listener;
        delete network;
        delete serializer;
    }

    // Sends the given message to the given target
    // Expects a response to be written back in the same connection (hangs otherwise)
    // Caller is responsible for deleting returned Message object.
    // Returns nullptr if this method is called before node is registered.
    Message *send_msg(char *target_ip_address, int target_port, MessageType msg_type, char *msg_contents) {
        if (!registered) {
            return nullptr;
        }

        Message msg(my_ip_address, my_port, msg_type, msg_contents);

        Message *response = network->send_and_receive_msg(&msg, target_ip_address, target_port);

        return response;
    }

    // Register this Node with the server and then
    // listen for connections from other Nodes, or the master server
    void register_and_listen() {
        // Start listening
        int listening_socket = network->bind_and_listen(my_ip_address, my_port);
        listener = new std::thread(&Node::listen_, this, listening_socket);

        // Register Node with server
        register_node_();

        // wait for first directory update before returning
        // TODO replace with waitAndNotify
        while (known_nodes == nullptr) {
        }
    }

    // Registers this Node with the master server
    void register_node_() {
        // Put together register message in the format [HOST]:[PORT]
        // + 10 should leave enough space for ":[PORT]", since max port number ~= 66,000
        char msg_to_send[strlen(my_ip_address) + 10];
        sprintf(msg_to_send, "%s:%d", my_ip_address, my_port);

        // Send registration message to master
        Message register_msg(my_ip_address, my_port, REGISTER, msg_to_send);

        Message *response = network->send_and_receive_msg(&register_msg, server_ip_address, server_port);

        if (response->msg_type == ACK) {
            // registered successfully
            registered = true;
            printf("Node at %s:%d registered succesfully\n", my_ip_address, my_port);
            delete response;
        } else {
            // error with registration
            delete response;
            printf("Node failed to register with the master server");
            exit(1);
        }
    }

    // Listens and processes incoming connections in an infinite loop
    void listen_(int listening_socket) {
        printf("Node at %s is listening on port %d...\n", my_ip_address, my_port);

        // Process connections to this Node as they come in
        while (1) {
            if (shutting_down) {
                // Parent process is shutting down
                break;
            }

            int connection = network->check_for_connections(listening_socket);
            if (connection < 0) {
                // No open connection at the moment
                continue;
            }

            // msg should never be null
            Message *msg = network->read_msg(connection);

            if (msg->msg_type == DIRECTORY) {
                // Node got a directory update from the server
                update_directory_(connection, msg);
            } else if (msg->msg_type == SHUTDOWN) {
                // Node was told to shutdown by the server
                shutdown_(connection);
                close(connection);
                delete msg;
                break;
            } else {
                // Node got a message from another node
                handle_message(connection, msg);
            }

            close(connection);
            delete msg;
        }

        close(listening_socket);
    }

    // Processes an update from the server with the list of connected nodes in the network
    void update_directory_(int connected_socket, Message *msg) {
        printf("Node at %s:%d got a directory update from the server\n", my_ip_address, my_port);

        // Get new directory list as a string
        char *new_directory = msg->msg;

        // Cleanup existing directory list, if it exists
        known_nodes_lock.lock();
        if (known_nodes != nullptr) {
            for (size_t i = 0; i < known_nodes->size(); i++) {
                delete known_nodes->get(i);
            }

            delete known_nodes;
        }

        // Save new directory list
        known_nodes = serializer->deserialize_string_array(new_directory);
        known_nodes_lock.unlock();

        // Send ACK
        Message ack(my_ip_address, my_port, ACK, (char *)"");
        network->write_msg(connected_socket, &ack);
    }

    // Called when server tells this node to shut down
    // Cleans up run-time flags, but does not delete memory (handled by destructor)
    void shutdown_(int connected_socket) {
        printf("Node at %s:%d is shutting down\n", my_ip_address, my_port);

        // Send ACK
        Message ack(my_ip_address, my_port, ACK, (char *)"");
        network->write_msg(connected_socket, &ack);

        shutting_down = true;
        registered = false;
    }

    // OVERRIDE
    // Handler for all messages that come into this node
    // Some sort of reply is expected to be written to the given socket
    // By default, sends empty ACK back to the message sender and prints a "message-received" string
    virtual void handle_message(int connected_socket, Message *msg) {
        // Send ACK
        Message ack(my_ip_address, my_port, ACK, (char *)"");
        network->write_msg(connected_socket, &ack);

        printf("Node %s:%d got message from another node with type %d and contents \"%s\"\n", my_ip_address, my_port, msg->msg_type, msg->msg);
    }

    // Whether this node is shut down
    bool is_shutdown() {
        return shutting_down && !registered;
    }
};
