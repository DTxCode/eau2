build:
	g++ -Wall -pthread src/client/main.cpp -o app

test:
	g++ -Wall -pthread -g tests/store/server_node_test.cpp -o network && ./network

valgrind:
	valgrind --leak-check=full --track-origins=yes ./network # TODO: change executable
