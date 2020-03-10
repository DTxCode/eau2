#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <thread>
#include "../utils/array.h"
#include "../utils/string.h"
#include "network.h"
#include "serial.h"

/*
    Represents a Server in a network. Clients connect to this server in order in order to register/join the network.
*/
class Server {
   public:
    char *my_ip_address;
    int my_port;
    StringArray *registered_nodes;
    Network *network;
    std::thread *listener;  // Thread that listens for incoming connections
    bool shutting_down;     // Whether this server is shutting down. Used by (infinite) spawned thread to know to shut down
    Serializer *serializer;

    Server(char *my_ip_address, int my_port) {
        this->my_ip_address = my_ip_address;
        this->my_port = my_port;
        registered_nodes = new StringArray();
        network = new Network();
        listener = nullptr;
        shutting_down = false;
        serializer = new Serializer();
    }

    ~Server() {
        for (size_t i = 0; i < registered_nodes->size(); i++) {
            delete registered_nodes->get(i);
        }

        if (listener != nullptr) {
            delete listener;
        }

        delete registered_nodes;
        delete network;
        delete serializer;
    }

    // Sends a shutdown signal to all nodes in the network and stops the server
    // Does not delete memeory - handled by destructor
    void shutdown() {
        // Stop the server's listener thread
        shutting_down = true;

        if (listener->joinable()) {
            listener->join();
        }

        printf("Server is shutting down itself and %zu other nodes...\n", registered_nodes->size());
        Message shutdown_msg(my_ip_address, my_port, SHUTDOWN, "");

        // Loop over all the nodes this server knows about
        for (size_t node_idx = 0; node_idx < registered_nodes->size(); node_idx++) {
            String *node = registered_nodes->get(node_idx);
            char *node_host = network->get_host_from_address(node);
            int node_port = network->get_port_from_address(node);

            printf("... sending shutdown message to %s:%d\n", node_host, node_port);
            Message *response = network->send_and_receive_msg(&shutdown_msg, node_host, node_port);

	    delete[] node_host;

            if (response->msg_type != ACK) {
                printf("WARN: Did not get back ACK from node after sending it shutdown signal");
            }

	    delete response;
        }
    }

    // Starts a thread that loops forever, listening for clients
    // Main process returns after listening has started
    void listen_for_clients() {
        int listening_socket = network->bind_and_listen(my_ip_address, my_port);
        printf("Server at %s is listening on port %d...\n", my_ip_address, my_port);
        listener = new std::thread(&Server::listen_, this, listening_socket);
    }

    // Listens for incoming connections in an infinite loop
    void listen_(int listening_socket) {
        // Process connections to this server as they come in
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

            process_client_registration_(connection, msg);

            delete msg;
        }

        close(listening_socket);
    }

    // Register client that's connected on given socket
    void process_client_registration_(int connection, Message *registration_msg) {
        printf("Server got registration request from node: %s\n", registration_msg->msg);

        // Send ACK back to node
        Message ack(my_ip_address, my_port, ACK, "");
        network->write_msg(connection, &ack);
        close(connection);

        // Record new node's ip address
        String *new_node = new String(registration_msg->msg);
        registered_nodes->push_back(new_node);

        // Update all nodes with new directory info
        update_clients_();
    }

    // Send my list of known node IPs to all of the individual node
    void update_clients_() {
        // Encode server's list of known nodes to json array
        char *registered_nodes_string = serializer->serialize_string_array(registered_nodes);

        Message directory_msg(my_ip_address, my_port, DIRECTORY, registered_nodes_string);

        // Loop over all the nodes this server knows about
        for (size_t node_idx = 0; node_idx < registered_nodes->size(); node_idx++) {
            String *node = registered_nodes->get(node_idx);
            char *node_host = network->get_host_from_address(node);
            int node_port = network->get_port_from_address(node);

            Message *response = network->send_and_receive_msg(&directory_msg, node_host, node_port);

            delete[] node_host;

            if (response->msg_type != ACK) {
                printf("WARN: Did not get back ACK from node after sending it directory update");
            }
	   
	    delete response;
	}

        delete[] registered_nodes_string;
    }
};
