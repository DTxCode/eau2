#include "../../src/client/arguments.h"
#include "../../src/store/dataframe/rower.h"
#include "../../src/store/store.cpp"
#include "../../src/store/network/master.h"

class TrueCountRower : public Rower {
   public:
    TrueCountRower() {}

    int count = 0;

    int get_count() { return count; }

    bool accept(Row& r) {
        IntSumFielder f;
        r.visit(r.get_idx(), f);
        count += f.get_sum();
        return true;
    }
};


int main(int argc, char** argv) { 
    Arguments args(argc, argv);
    Server* s;

    // Node 0 start server
    if (args.node_id == 0) {
        s = new Server(args.master_ip, args.master_port);
        s->listen_for_clients();
    }

    Store store(args.node_id, args.node_ip, args.node_port, args.master_ip, args.master_port);
    
    while (store.num_nodes() != args.num_nodes) {}

    // Node 0 load data into store
    if (args.node_id == 0) {
        /*bool* bools = new bool[500];
        for (size_t i = 0; i < 500; i++) {
            bools[i] = false;
        }
        bools[50] = true;
        bools[100] = true;
        bools[150] = true;
        bools[350] = true;*/
        int* ints = new int[500];
        for (size_t i = 0; i < 500; i++) {
            ints[i] = 0;
        }
        ints[50] = 1;
        ints[100] = 1;
        ints[150] = 1;
        ints[350] = 1;

        Key* k = new Key((char*) "test", 0);
        delete DataFrame::fromArray(k, &store, 500, ints);
        delete[] ints;
        printf("Node 0 finished uploading data\n");
    }

    Key* new_k = new Key((char*) "test", 0);
    DataFrame* df = dynamic_cast<DataFrame*>(store.waitAndGet(new_k));
    TrueCountRower r;
    df->local_map(r);
    printf("Node %d found %d TRUES!\n", args.node_id, r.get_count());
    delete df;
    Key* nK;
    if (args.node_id == 0) {
        nK = new Key((char*) "results-0", 0);
    } else {
        nK = new Key((char*) "results-1", 1);
    }
    delete DataFrame::fromScalar(nK, &store, r.get_count());

    if (args.node_id == 0) {
        size_t count_true = 0;
        DataFrame* df_1 = dynamic_cast<DataFrame*>(store.waitAndGet(new Key((char*) "results-0", 0)));
        count_true += df_1->get_int(0, 0);
        delete df_1;
        if (args.num_nodes > 1) {
            DataFrame* df_2 = dynamic_cast<DataFrame*>(store.waitAndGet(new Key((char*) "results-1", 1)));
            count_true += df_2->get_int(0, 0);
            delete df_2;
        }

        printf("Got a total of %zu TRUES!\n", count_true);
        
        s->shutdown();
        delete s;
    }

    while(!store.is_shutdown()) {}

    return 0;

}

