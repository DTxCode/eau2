build:
	g++ -Wall src/client/main.cpp -o app

test:
	g++ -Wall tests/store/server_node_test.cpp -o network && ./network

valgrind:
