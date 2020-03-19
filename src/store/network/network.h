#pragma once
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <stropts.h>
#include <sys/socket.h>
#include <unistd.h>
#include "../../utils/helper.h"
#include "../../utils/string.h"
#include "message.h"

#define MAX_MESSAGE_CHUNK_SIZE 512
#define LISTEN_TIMEOUT 200
#define MAX_INCOMING_CONNECTIONS 20

/*
    Network wraps important network functionality into helpful methods.
    Useful for both clients and servers.
*/
class Network {
   public:
    size_t max_message_chunk_size;
    size_t listen_timeout;
    size_t max_incoming_connections;
    Sys* sys;  // helper

    Network() {
        max_message_chunk_size = MAX_MESSAGE_CHUNK_SIZE;
        listen_timeout = LISTEN_TIMEOUT;
        max_incoming_connections = MAX_INCOMING_CONNECTIONS;
        sys = new Sys();
    }

    ~Network() {
        delete sys;
    }

    // Extracts port from the given address
    // Expect given address to have format [HOST]:[PORT]
    int get_port_from_address(String* address) {
        // address_copy is heap-allocated
        char* address_copy = address->get_string();

        // first call to strtok will "chop off" the host
        // second call should return the port
        strtok(address_copy, ":");
        char* port_string = strtok(nullptr, ":");

        // Convert port to integer
        int port = atoi(port_string);
        if (port == 0) {
            printf("ERROR could not get port from given address %s", address_copy);
            exit(1);
        }

        delete[] address_copy;

        return port;
    }

    // Extracts host from the given address
    // Expect given address to have format [HOST]:[PORT]
    char* get_host_from_address(String* address) {
        // Strtok modifies original string, so work with a copy of it
        // address_copy is heap-allocated
        char* address_copy = address->get_string();

        // first call to strtok will return just the string before the ":"
        char* host = strtok(address_copy, ":");

        // make dupe so that we can delete[] the original copy
        char* host_dup = sys->duplicate(host);
        delete[] address_copy;

        return host_dup;
    }

    // Bind the given address/port to this process and listen for incoming connections
    // Returns non-blocking socket that represents a queue of no more than 'max_pending_connections' connections.
    // Can be passed to accept()
    int bind_and_listen(char* ip_address, int port) {
        // Create socket that this server will listen on
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
            perror("ERROR opening socket");
            exit(1);
        }

        /* Initialize socket structure */
        struct sockaddr_in serv_addr;
        bzero((char*)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = inet_addr(ip_address);
        serv_addr.sin_port = htons(port);

        /* Assign serv_addr struct to the socket that was created.*/
        if (bind(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
            perror("ERROR binding address to socket");
            exit(1);
        }

        // Start listening on sockfd for incoming requests
        listen(sockfd, max_incoming_connections);

        return sockfd;
    }

    // Sends the given message to the given ip_address/port and returns the response
    Message* send_and_receive_msg(Message* msg, char* target_ip_address, int target_port) {
        // Connect to target
        int sock = connect_to_(target_ip_address, target_port);

        // Write and read
        write_msg(sock, msg);
        Message* response = read_msg(sock);

        close(sock);

        return response;
    }

    // Reads a Message from the given socket
    // Socket should be open and read-ready, else this call hangs.
    Message* read_msg(int socket) {
        char* msg_string = read_from_socket_(socket);
        Message* msg = new Message(msg_string);
        delete[] msg_string;
        return msg;
    }

    // Writes the given Message to the given socket
    // Socket should be open and write-ready, else this call hangs
    void write_msg(int socket, Message* msg) {
        char* msg_string = msg->to_string();

        write_to_socket_(socket, msg_string);

        delete[] msg_string;
    }

    // Checks for incoming connections on the given socket that are ready to be read from.
    // Returns an open read-ready socket if there is one, else returns -1
    int check_for_connections(int socket) {
        struct sockaddr_in cli_addr;
        unsigned int clilen = sizeof(cli_addr);

        // Check if listening_socket has a socket ready to read from
        struct pollfd fd = {.fd = socket, .events = POLLIN};
        poll(&fd, 1, listen_timeout);

        // If something is ready to be read, accept the message
        if (fd.revents & POLLIN) {
            int connection_socket = accept(socket, (struct sockaddr*)&cli_addr, &clilen);
            if (connection_socket < 0) {
                perror("ERROR on accept from socket");
                exit(1);
            }

            return connection_socket;
        }

        return -1;
    }

    // Returns all data possible to be read from the given socket
    // Expects message to be prepended with "[LENGTH];", where LENGTH is the length of the message that follows
    char* read_from_socket_(int socket) {
        char buffer[max_message_chunk_size];
        bzero(buffer, max_message_chunk_size);

        // first extract how many bytes we're expecting from this socket
        // read size - 1 to always ensure there's a null terminator
        int bytes_read = read(socket, buffer, max_message_chunk_size - 1);

        if (bytes_read < 0) {
            perror("ERROR reading from socket");
            exit(1);
        }

        // rest of the message should have this length
        char* message_length_string = strtok(buffer, ";");
        size_t message_length = atoi(message_length_string);  // Assumes success because we formatted the message

        // Initialize msg and bytes_read, then loop until bytes_read >= message_length
        char* message = strtok(nullptr, "\0");  // The actual message
        String* msg = new String(message);
        bytes_read = msg->size();

        while (bytes_read < message_length) {
            bzero(buffer, max_message_chunk_size);

            // read size - 1 to always ensure there's a null terminator
            int new_bytes_read = read(socket, buffer, max_message_chunk_size - 1);

            if (new_bytes_read < 0) {
                perror("ERROR reading from socket");
                exit(1);
            }

            bytes_read += new_bytes_read;

            // record new data
            String* new_msg = msg->concat(buffer);
            delete msg;
            msg = new_msg;
        }

        // Pull out and return cstr inside of String*
        char* msg_string = msg->get_string();
        delete msg;

        return msg_string;
    }

    // Writes given message to the given socket.
    // Prepends given ip address and port to the message in the format ADDRESS:PORT;msg
    void write_to_socket_(int socket, char* msg_to_send) {
        // Prepend message length to message
        size_t length = strlen(msg_to_send);
        size_t num_digits_in_length = snprintf(NULL, 0, "%d", length);

        // + 1 for semicolon between length and message, + 1 for null terminator
        char prepended_message[num_digits_in_length + 1 + length + 1];

        sprintf(prepended_message, "%d;%s", length, msg_to_send);

        // Send message to server
        int bytes_written = write(socket, prepended_message, strlen(prepended_message));

        if (bytes_written < 0) {
            perror("ERROR writing to socket");
            exit(1);
        }
    }

    // Returns a socket connected to the given IP address and port
    int connect_to_(char* target_ip_address, int target_port) {
        // Create socket
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            perror("ERROR: cannot open socket");
            exit(1);
        }

        // Create target server struct
        struct hostent* server = gethostbyname(target_ip_address);
        if (server == NULL) {
            printf("ERROR: cannot connect to target server %s:%d\n", target_ip_address, target_port);
            exit(1);
        }

        // Create serv_addr struct
        struct sockaddr_in serv_addr;
        bzero((char*)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char*)server->h_addr, (char*)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(target_port);

        // Connect to the server
        if (connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
            printf("ERROR could not connect to server with IP %s and port %d", target_ip_address, target_port);
            exit(1);
        }

        return sockfd;
    }
};