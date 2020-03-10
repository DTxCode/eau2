#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../utils/helper.h"

enum MessageType {
    ACK,
    REGISTER,
    DIRECTORY,
    SHUTDOWN
};

// Represents a Message sent between nodes/servers in a network
class Message {
   public:
    char* sender_ip_address;
    int sender_port;
    MessageType msg_type;
    char* msg;

    // Constructs a Message with the given fields
    // NOTE: `msg` cannot contain any semicolons!
    Message(char* sender_ip_address, int sender_port, MessageType msg_type, char* msg) {
        this->sender_ip_address = sender_ip_address;
        this->sender_port = sender_port;
        this->msg_type = msg_type;
        this->msg = msg;
    }

    // Constructs a Message from the given stringified Message
    // Expects given string to have format [SENDER IP ADDRESS]:[SENDER PORT];[MESSAGE TYPE];[MESSAGE]
    Message(char* message_string) {
        if (message_string == nullptr) {
            printf("ERROR cannot create Message from empty string\n");
            exit(1);
        }

        // Use duplicate because strtok mutates its string
        Sys s;
	char* msg_duplicate = s.duplicate(message_string);

        sender_ip_address = strtok(msg_duplicate, ":");
        if (strlen(sender_ip_address) == 0) {
            printf("ERROR failed to parse Message.sender_ip_address from the given string: %s", message_string);
            exit(1);
        }

        // Cast port to int
        char* port_string = strtok(nullptr, ";");
        sender_port = atoi(port_string);
        if (sender_port == 0) {
            printf("ERROR failed to parse Message.sender_port from the given string: %s\n", message_string);
            exit(1);
        }

        // Cast message type to enum
        char* msg_type_string = strtok(nullptr, ";");
        msg_type = static_cast<MessageType>(atoi(msg_type_string));

        // can be empty string
        msg = strtok(nullptr, ";");
    }

    // Returns a string representation of this Message in the format
    // [SENDER IP ADDRESS]:[SENDER PORT];[MESSAGE TYPE];[MESSAGE]
    char* to_string() {
        // + 10 should give enough space for the part of new_msg corresponding to ":PORT;"
        // since port numbers are no more than ~66,000
        // + 5 should give enough space for ";MESSAGE_TYPE;"
        char* string = new char[strlen(sender_ip_address) + 10 + 5 + strlen(msg)];

        sprintf(string, "%s:%d;%d;%s", sender_ip_address, sender_port, msg_type, msg);

        return string;
    }
};
