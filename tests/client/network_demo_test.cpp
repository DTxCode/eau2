/* Authors: Ryan Heminway (heminway.r@husky.neu.edu)
*           David Tandetnik (tandetnik.da@husky.neu.edu) */
#include "../../src/store/store.cpp"
#include "../../src/client/application.h"
#include "../../src/store/network/master.h"
#include "../../src/store/key.h"
#include "../test_utils.h"

class Demo : public Application {
   public:
    Key* main;
    Key* verify;
    Key* check;

    Demo(Store* store) : Application(store) {
        main = new Key((char*)"main" ,0);
	    verify = new Key((char*)"verif", 0);
	    check = new Key((char*)"check", 0);
    }

    ~Demo() {
	delete main;
	delete verify;
	delete check;
    }

    void run_() {
        switch (this_node()) {
            case 0:
                producer();
                break;
            case 1:
                counter();
                break;
            case 2:
                summarizer();
        }
    }

    void producer() {
        size_t SZ = 1 * 1000;
        float* vals = new float[SZ];
        float sum = 0;
        for (size_t i = 0; i < SZ; ++i) {
            vals[i] = (float)i;
            sum += vals[i];
        }

        delete DataFrame::fromArray(main, store, SZ, vals);
        delete DataFrame::fromScalar(check, store, sum);
        delete[] vals;
    }

    void counter() {
        DataFrame* v = store->waitAndGet(main);
        float sum = 0;
        for (size_t i = 0; i < 1 * 1000; ++i) {
            sum += v->get_float(0, i);
        }
	    printf("The sum is %f\n", sum);
        delete DataFrame::fromScalar(verify, store, sum);
        delete v;
    }

    void summarizer() {
        DataFrame* result = store->waitAndGet(verify);
        DataFrame* expected = store->waitAndGet(check);
        if(expected->get_float(0, 0) == result->get_float(0, 0)){
		    printf("SUCCESS\n");
	    } else {
		    printf("FAILURE\n");
    	}
        delete result;
        delete expected;
    }
};

int test_demo() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = rand_port();
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, (char*)"127.0.0.1", rand_port(), master_ip, master_port);
    Store store2(1, (char*)"127.0.0.1", rand_port(), master_ip, master_port);
    Store store3(2, (char*)"127.0.0.1", rand_port(), master_ip, master_port);

    Demo producer(&store1);
    producer.run_();
    Demo counter(&store2);
    counter.run_();
    Demo summarizer(&store3);
    summarizer.run_();
    
    store1.is_done();
    store2.is_done();
    store3.is_done();

    // shutdown system
    s.shutdown();

    // wait for nodes to finish
    while (!store1.is_shutdown()) {
    }
    while (!store2.is_shutdown()) {
    }
    while (!store3.is_shutdown()) {
    }


    return true;
}

int main() {
    assert(test_demo());
    printf("=================test_demo PASSED if SUCCESS printed above==================\n");
    return 0;
}
