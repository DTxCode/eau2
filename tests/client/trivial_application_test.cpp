#include "../../src/client/application.cpp"

// Trivial Application for testing
class Trivial : public Application {
   public:
    Trivial(Store* store) : Application(nullptr, 0, 0, store) {}

    void run_() {
        size_t SZ = 1000 * 1000;
        double* vals = new double[SZ];
        double sum = 0;

        for (size_t i = 0; i < SZ; ++i) {
            vals[i] = i;
            sum += i;
        }

        Key key("triv", 0);
        DataFrame* df = DataFrame::fromArray(&key, &store, SZ, vals);

        assert(df->get_double(0, 1) == 1);

        DataFrame* df2 = store->get(key);

        for (size_t i = 0; i < SZ; ++i) {
            sum -= df2->get_double(0, i);
        }

        assert(sum == 0);

        delete df;
        delete df2;
    }
};

// Tests putting/getting a DF from the local store
int test_trivial() {
    char* master_ip = "127.0.0.1";
    int master_port = 8888;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store(0, "127.0.0.1", 8000, master_ip, master_port);

    Trivial test(&store);

    return 1;
}

void main() {
    assert(test_trivial());
    printf("=================trivial_application_test:test_trivial PASSED==================\n");
}