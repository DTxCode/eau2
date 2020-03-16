#include <assert.h>
#include "../../src/store/master.h"
#include "../../src/store/message.h"
#include "../../src/store/node.h"

// A Node for testing
class TestNode : public Node {
   public:
    int received_messages;

    TestNode(char* my_address, int my_port, char* master_address, int master_port) : Node(my_address, my_port, master_address, master_port) {
        received_messages = 0;
    }

    void handle_message(int connected_socket, Message* msg) {
        // Send ACK
        Message ack(my_ip_address, my_port, ACK, "");
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
    char* master_ip = "127.0.0.1";
    int master_port = 8888;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    TestNode node1("127.0.0.1", 7000, master_ip, master_port);
    node1.register_and_listen();

    TestNode node2("127.0.0.1", 7001, master_ip, master_port);
    node2.register_and_listen();

    // Send empty ACK message from node1 to node2
    Message* response = node1.send_msg("127.0.0.1", 7001, ACK, "Hello from node 1!");
    delete response;

    // shutdown system
    s.shutdown();

    // wait for nodes to finish
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
