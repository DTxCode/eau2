Ryan Heminway
David Tandetnik        
Eau2 System Architecture and Design

Introduction

    The eau2 system aims to provide key-value storage over a distributed set of nodes acting as the backbone for applications which process large amounts of data. 
With the requirement of a cluster of compute nodes, users can run the eau2 system to store data across the cluster but interact with it as if it was local.
Eau2 only accepts input data formatted based on schema-on-read (SoR) requirements, and that data is read-only once it is stored in the system. 
For queries, the eau2 system provides the data in the form of a dataframe which allows for easy data aggregation and access. 


Architecture

    Conceptually, there are three layers of the eau2 system. At the bottom is the key-value storage system. Data is stored across multiple nodes, 
so this layer also includes a network protocol which allows the key-value stores on each node to interact with each other. 
Each key consists of a string and a home node while each value is a serialized blob of data. Communication between the stores allows the user to view the 
key-value storage as a single system holding all the data, without any knowledge of the underlying distribution. The layer above provides abstractions 
like distributed columns and distributed dataframes. These utility classes are useful for data aggregation and are easier to work with than the raw data. 
At the top sits the application layer. This layer provides all the functionality necessary to run the eau2 system and interact with the lower levels. 
The user builds on top of this layer to leverage the underlying key-value store as the backbone of their program. 


Implementation

    The bottom layer will be implemented using one main class, for now referenced as Store. This class will have two main components, a Map and a Network. 
The map is the normal key-value storage structure, mapping Keys to Strings. Keys are a new type which represent a tuple of a String and an integer 
to denote the home node identifier of the data. Strings are used to hold serialized blobs of data. The Network object on the other hand contains 
all the necessary logic for registering and communicating with the other nodes in the cluster. Overall, the Store layer will have a simple public API
supporting methods such as ‘get’, ‘getAndWait’, and ‘put’ which all work with DistributedDataFrames. There is also a set of analogous private API 
methods that work on chunks of DistributedColumns, described below.
    In the middle layer we have the useful abstraction classes. Specifically, the DistributedDataFrame class and the DistributedColumn class. 
The DistributedDataFrame is a queryable table of data which supports aggregate operations such as ‘map’ and ‘filter’. All data stored in a DistributedDataFrame 
is stored in columnar format with all data in each column being of a single type. The types supported by a DataFrame are the four SoR types: INT, BOOL, FLOAT, 
and STRING. Unlike normal DataFrames, DistributedDataFrames access their data as needed from other nodes across the network under-the-hood. The DistributedColumn 
class has the API of a normal column, however it is implemented such that the values it stores are distributed in chunks across the cluster of nodes 
using the Store’s private API. This abstraction allows this class to represent much more data than the average column stored in local memory.
    For the top layer, we provide a single Application class. This class contains a ‘run’ method which must be invoked on all nodes in the 
cluster to start up the system. This class has the Store as a field, providing the user which extends the Application class with access to all data 
stored on the system. Additionally, this class will contain any utility methods required for building a program on top of it. One example is the Application 
API method ‘this_node’ which returns the index of the current node. 


Use Cases

// One node saves some data to the store, the other takes the max of the data and saves that to 
// the store too.
class Demo : public Application {
Public:
    Key df(“data”, 0);
    Key max(“max”, 1);


    Demo(size_t idx): Application(idx) {}


    void run_() { 
        switch(this_node()) {
        case 0: producer(); break;
        case 1: maxer();
        }
     }


    void producer() { 
        float vals = new float[3];
        vals[0] = 1;
        vals[1] = 10;
        vals[2] = 100;
    
       DataFrame::fromArray(&df, store, 3, vals);
}


    void maxer() {
        DataFrame* frame = kv.getAndWait(df);
        int max = frame->get_int(0, 0);
        for (size_t i = 1; i < frame->nrows(); i++) {
            if (kv->get(0, i) > max) max = store->get(0, i);
        }
        DataFrame::fromScalar(&max, store, max);
    }
};           
	

Status

    At this point, our system is capable of processing an application across multiple nodes, including the Linus demo. We have 
however only experimented with relatively small files (less than half a GB). Our next steps include testing the system further with 
more data and more nodes. All of our tests pass and are clean of memory issues.
