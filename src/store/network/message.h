/* Authors: Ryan Heminway (heminway.r@husky.neu.edu)
*           David Tandetnik (tandetnik.da@husky.neu.edu) */
#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../utils/helper.h"

enum MessageType {
    ACK,
    NACK,
    REGISTER,
    DIRECTORY,
    SHUTDOWN,
    PUT,
    GET
};

// Represents a Message sent between nodes/servers in a network
class Message {
   public:
    char* sender_ip_address;
    int sender_port;
    MessageType msg_type;
    char* msg;

    // Constructs a Message with the given fields
    Message(char* sender_ip_address, int sender_port, MessageType msg_type, char* msg) {
        Sys s;
        this->sender_ip_address = s.duplicate(sender_ip_address);
        this->sender_port = sender_port;
        this->msg_type = msg_type;
        this->msg = s.duplicate(msg);
    }

    ~Message() {
        delete[] sender_ip_address;
        delete[] msg;
    }

    // Constructs a Message from the given stringified Message
    // Expects given string to have format [SENDER IP ADDRESS]:[SENDER PORT];[MESSAGE TYPE];[MESSAGE]
    Message(char* message_string) {
        // printf("DEBUG: Constructing message from string: %s\n", message_string);

        assert(message_string);
       
        Sys s;

        // Use duplicate because strtok mutates its string
        char* msg_duplicate = s.duplicate(message_string);

        char* entry; // Used for strtok_r thread safety
        
        sender_ip_address = s.duplicate(strtok_r(msg_duplicate, ":", &entry));
        sender_port = atoi(strtok_r(nullptr, ";", &entry));
        
        char* msg_type_string = strtok_r(nullptr, ";", &entry);
        msg_type = static_cast<MessageType>(atoi(msg_type_string));

        // May be nullptr. If so, set to empty string
        msg = strtok_r(nullptr, "\0", &entry);
        if (msg == nullptr) {
            msg = new char[1];
            msg[0] = '\0';
        } else {
            msg = s.duplicate(msg);
        }

        assert(sender_ip_address);
        assert(sender_port);
        assert(msg_type >= 0);
        assert(msg);

        delete[] msg_duplicate;

        // printf("DEBUG: Created Message with ip %s port %d type %d msg %s. Msg has length %zu\n", sender_ip_address, sender_port, msg_type, msg, strlen(msg));
    }

    // Returns a string representation of this Message in the format
    // [SENDER IP ADDRESS]:[SENDER PORT];[MESSAGE TYPE];[MESSAGE]
    char* to_string() {
        size_t buf_size = snprintf(nullptr, 0, "%s:%d;%d;%s", sender_ip_address, sender_port, msg_type, msg) + 1;
        char* string = new char[buf_size];
        snprintf(string, buf_size, "%s:%d;%d;%s", sender_ip_address, sender_port, msg_type, msg);

        assert(string[buf_size-1] == '\0');

        return string;
    }
};
