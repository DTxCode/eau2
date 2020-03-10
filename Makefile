build:
	g++ -Wall -pthread src/client/main.cpp -o app

test:
	g++ -Wall -pthread tests/store/server_node_test.cpp -o network && ./network

valgrind:
	
