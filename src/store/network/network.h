/* Authors: Ryan Heminway (heminway.r@husky.neu.edu)
*           David Tandetnik (tandetnik.da@husky.neu.edu) */
#pragma once
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
//#include <stropts.h>
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

        char* entry;

        // first call to strtok will "chop off" the host
        // second call should return the port
        strtok_r(address_copy, ":", &entry);
        char* port_string = strtok_r(nullptr, ":", &entry);

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

        char* entry;
        // first call to strtok will return just the string before the ":"
        char* host = strtok_r(address_copy, ":", &entry);

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
        serv_addr.sin_addr.s_addr = INADDR_ANY; //inet_addr(ip_address);
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

        assert(response != nullptr);

        return response;
    }

    // Reads a Message from the given socket
    // Socket should be open and read-ready, else this call hangs.
    Message* read_msg(int socket) {
        char* msg_string = read_from_socket_(socket);
        Message* msg = new Message(msg_string);
        delete[] msg_string;

        assert(msg != nullptr);

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
        struct timespec timeout = {.tv_sec = 0, .tv_nsec = (long) listen_timeout * 1000000};
        
        ppoll(&fd, 1, &timeout, nullptr);

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

    // Reads a size_t from the socket, and then reads that many bytes more from the socket.
    char* read_from_socket_(int socket) {
        size_t msg_size = 0;

        if (read(socket, &msg_size, sizeof(size_t)) < (long) sizeof(size_t)) {
            perror("ERROR reading msg size from socket\n");
            exit(1);
        }

        char* msg = new char[msg_size + 1];
        msg[msg_size] = '\0';
        
        size_t bytes_read = 0;
        while (bytes_read < msg_size) {
            size_t new_bytes_read = read(socket, msg + bytes_read, msg_size - bytes_read);

            if (new_bytes_read < 0) {
                printf("ERROR reading from socket");
                exit(1);
            } else if (new_bytes_read == 0) {
                printf("ERROR: read_from_socket got EOF before receiving all expected data. Expected %zu bytes. Got %zu with contents %s\n",
                            msg_size, bytes_read, msg);
                exit(1);
            }

            bytes_read += new_bytes_read;
        }

        // printf("DEBUG: read_from_socket returning %s\n", msg);
        assert(strlen(msg) == msg_size);
        assert(msg[msg_size] == '\0');

        return msg;
    }

    // Writes given message to the given socket.
    // First writes a size_t with the length of the message, then writes the message
    void write_to_socket_(int socket, char* msg_to_send) {
        size_t length = strlen(msg_to_send);

        if (write(socket, &length, sizeof(size_t)) < (long) sizeof(size_t)) {
            printf("ERROR writing msg size to socket. Full message was %s\n", msg_to_send);
            exit(1);
        };

        if (write(socket, msg_to_send, strlen(msg_to_send)) < (long) strlen(msg_to_send)) {
            printf("ERROR writing msg to socket. Full message was %s\n", msg_to_send);
            exit(1);
        };

        // printf("DEBUG: write_to_socket_ wrote msg %s\n", msg_to_send);
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
            printf("ERROR could not connect to server with IP %s and port %d\n", target_ip_address, target_port);
            exit(1);
        }

        return sockfd;
    }
};
