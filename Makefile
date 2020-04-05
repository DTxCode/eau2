build:
	g++ -std=c++11 -Wall -pthread src/client/application.cpp -o app

run:
	./app -from 0 -len 1000 -f ./data/data.sor

### Client

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

# Server-node test
test-server-node:
	g++ -std=c++11 -Wall -pthread -g tests/store/server_node_test.cpp -o server_node_test
	./server_node_test

valgrind-server-node:
	g++ -std=c++11 -Wall -pthread -g tests/store/server_node_test.cpp -o server_node_test
	valgrind --leak-check=full --track-origins=yes ./server_node_test

# Simple serializer test
test-simple-serializer:
	g++ -std=c++11 -Wall -pthread -g tests/store/simple_serializer_test.cpp -o serial_test
	./serial_test

valgrind-simple-serializer:
	g++ -std=c++11 -Wall -pthread -g tests/store/simple_serializer_test.cpp -o serial_test
	valgrind --leak-check=full --track-origins=yes ./serial_test

# Store test
test-store:
	g++ -std=c++11 -Wall -pthread -g tests/store/store_test.cpp -o store_test
	./store_test

valgrind-store:
	g++ -std=c++11 -Wall -pthread -g tests/store/store_test.cpp -o store_test
	valgrind --leak-check=full --track-origins=yes ./store_test

# Distributed Column test
test-column:
	g++ -std=c++11 -Wall -pthread -g tests/store/dist_column_test.cpp -o col_test
	./col_test

test-ddf:
	g++ -std=c++11 -Wall -pthread -g tests/store/test_ddf.cpp -o ddf_test
	./ddf_test
	g++ -std=c++11 -Wall -pthread -g tests/store/test_missings_col.cpp -o missing_col_test
	./missing_col_test
