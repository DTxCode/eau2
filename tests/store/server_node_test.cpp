#include <assert.h>
#include "../../src/store/store.cpp"
#include "../../src/store/network/master.h"
#include "../../src/store/network/message.h"
#include "../../src/store/network/node.h"
#include "../test_utils.h"

// A Node for testing
class TestNode : public Node {
   public:
    int received_messages;

    TestNode(char* my_address, int my_port, char* master_address, int master_port) : Node(my_address, my_port, master_address, master_port) {
        received_messages = 0;
    }

    void handle_message(int connected_socket, Message* msg) {
        printf("Node %s:%d got message from another node with type %d and contents \"%s\"\n", my_ip_address, my_port, msg->msg_type, msg->msg);

        // Send ACK
        Message ack(my_ip_address, my_port, ACK, (char*) "");
        network->write_msg(connected_socket, &ack);

        received_messages += 1;
    }

    // Returns how many messages this node has receieved
    int get_received_messages() {
        return received_messages;
    }
};

// Tests creating a server and two nodes, and then having a node send another node a simple message
bool test_simple_message() {
    char* master_ip = (char*) "127.0.0.1";
    int master_port = rand_port();
    Server s(master_ip, master_port);
    s.listen_for_clients();

    int port_1 = rand_port();
    TestNode node1((char*) "127.0.0.1", port_1, master_ip, master_port);
    node1.register_and_listen();

    int port_2 = rand_port();
    TestNode node2((char*) "127.0.0.1", port_2, master_ip, master_port);
    node2.register_and_listen();

    // Send empty ACK message from node1 to node2
    Message* response = node1.send_msg((char*) "127.0.0.1", port_2, ACK, (char*)"Hello from node 1!");
    delete response;

    node1.is_done();
    node2.is_done();

    // shutdown system
    s.shutdown();

    // wait for nodes to finish
    // TODO replace with waitAndNotify
    while (!node1.is_shutdown()) {
    }
    while (!node2.is_shutdown()) {
    }

    assert(node1.get_received_messages() == 0);
    assert(node2.get_received_messages() == 1);

    return true;
}

int main() {
    assert(test_simple_message());
    printf("========== test_simple_message PASSED =============\n");
}
