build:
	g++ -std=c++11 -Wall -pthread -g tests/client/linus_demo.cpp -o linus 

run:
	./linus -node_id 0 -node_port 4000 -degrees 1 -start_server 1
	./linus -node_id 1 -node_port 4001 -degrees 1 

# Run all tests
test: test-dist-column test-server-node test-serializer test-store test-ddf test-map
	echo "All tests passed!"

### Client

# Linus Demo
test-linus:
	g++ -std=c++11 -Wall -pthread -g tests/client/linus_demo.cpp -o linus_demo
	./linus_demo

# WordCount Demo
test-word-count:
	g++ -std=c++11 -Wall -pthread -g tests/client/word_count_demo.cpp -o word_count_demo
	./word_count_demo

# Network Demo Application test
test-client-with-network:
	g++ -std=c++11 -Wall -pthread -g tests/client/network_demo_test.cpp -o network_demo
	./network_demo

# Trivial Application Test
test-client-trivial:
	g++ -std=c++11 -Wall -pthread -g tests/client/trivial_application_test.cpp -o trivial_app_test
	./trivial_app_test

valgrind-client-trivial:
	g++ -std=c++11 -Wall -pthread -g tests/client/trivial_application_test.cpp -o trivial_app_test
	valgrind --leak-check=full --track-origins=yes ./trivial_app_test

# Simple sorer test
test-client-sorer:
	g++ -std=c++11 -Wall -pthread -g tests/client/simple_sorer_test.cpp -o simple_sorer_test
	./simple_sorer_test

valgrind-client-sorer:
	g++ -std=c++11 -Wall -pthread -g tests/client/simple_sorer_test.cpp -o simple_sorer_test
	valgrind --leak-check=full --track-origins=yes ./simple_sorer_test

### Store

# Distributed Column test
test-dist-column:
	g++ -std=c++11 -Wall -pthread -g tests/store/dist_column_test.cpp -o col_test
	./col_test

valgrind-dist-column:
	g++ -std=c++11 -Wall -pthread -g tests/store/dist_column_test.cpp -o col_test
	valgrind --leak-check=full --track-origins=yes ./col_test

# Server-node test
test-server-node:
	g++ -std=c++11 -Wall -pthread -g tests/store/server_node_test.cpp -o server_node_test
	./server_node_test

valgrind-server-node:
	g++ -std=c++11 -Wall -pthread -g tests/store/server_node_test.cpp -o server_node_test
	valgrind --leak-check=full --track-origins=yes ./server_node_test

# Simple serializer test
test-serializer:
	g++ -std=c++11 -Wall -pthread -g tests/store/serializer_test.cpp -o serial_test
	./serial_test

valgrind-serializer:
	g++ -std=c++11 -Wall -pthread -g tests/store/serializer_test.cpp -o serial_test
	valgrind --leak-check=full --track-origins=yes ./serial_test

# Store test
test-store:
	g++ -std=c++11 -Wall -pthread -g tests/store/store_test.cpp -o store_test
	./store_test

valgrind-store:
	g++ -std=c++11 -Wall -pthread -g tests/store/store_test.cpp -o store_test
	valgrind --leak-check=full --track-origins=yes ./store_test

# DDF test
test-ddf:
	g++ -std=c++11 -Wall -pthread -g tests/store/ddf_test.cpp -o ddf_test
	./ddf_test

valgrind-ddf:
	g++ -std=c++11 -Wall -pthread -g tests/store/ddf_test.cpp -o ddf_test
	valgrind --leak-check=full --track-origins=yes ./ddf_test


### Utils Tests

# Map test
test-map:
	g++ -std=c++11 -Wall -pthread -g tests/utils/map_test.cpp -o map_test
	./map_test

valgrind-map:
	g++ -std=c++11 -Wall -pthread -g tests/utils/map_test.cpp -o map_test
	valgrind --leak-check=full --track-origins=yes ./map_test 
