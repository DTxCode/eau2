build:
	g++ -std=c++11 -Wall -pthread src/client/application.cpp -o app

run:
	./app -from 0 -len 1000 -f ./data/data.sor

# Client
test-client:
	g++ -std=c++11 -Wall -pthread -g tests/client/simple_sorer_test.cpp -o simple_sorer_test
	./simple_sorer_test

valgrind-client:
	g++ -std=c++11 -Wall -pthread -g tests/client/simple_sorer_test.cpp -o simple_sorer_test
	valgrind --leak-check=full --track-origins=yes ./simple_sorer_test

# Store
test-store:
	g++ -std=c++11 -Wall -pthread -g tests/store/simple_serializer_test.cpp -o serial_test
	./serial_test
	g++ -std=c++11 -Wall -pthread -g tests/store/server_node_test.cpp -o server_node_test
	./server_node_test

valgrind-store:
	g++ -std=c++11 -Wall -pthread -g tests/store/server_node_test.cpp -o server_node_test
	valgrind --leak-check=full --track-origins=yes ./server_node_test
