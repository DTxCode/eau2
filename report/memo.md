Ryan Heminway,
David Tandetnik        
Eau2 System Architecture and Design

# Introduction

  The eau2 system aims to provide key-value storage over a distributed set of nodes acting as the backbone for applications which process large amounts of data.
With the requirement of a cluster of compute nodes, users can run the eau2 system to store data across the cluster but interact with it as if it was local.
Eau2 only accepts input data formatted based on schema-on-read (SoR) requirements, and that data is read-only once it is stored in the system.
For queries, the eau2 system provides the data in the form of a dataframe which allows for easy data aggregation and access. 

# Architecture

  Conceptually, there are three layers of the eau2 system.
  At the bottom is the key-value storage system. Data is stored across multiple nodes, so this layer also includes a network protocol which allows the key-value stores on each node to interact with each other.
  Each key consists of a string and a home node while each value is a serialized blob of data. Communication between the stores allows the user to view the key-value storage as a single system holding all the data, without any knowledge of the underlying distribution.
  The layer above provides abstractions like distributed columns and distributed dataframes. These utility classes are useful for data aggregation and are easier to work with than the raw data.
  At the top sits the application layer. This layer provides all the functionality necessary to run the eau2 system and interact with the lower levels.
  The user builds on top of this layer to leverage the underlying key-value store as the backbone of their program. 

# Implementation

  The bottom layer will be implemented using one main class, for now referenced as Store. This class will have two main components, a Map and a Network.
  The map is the normal key-value storage structure, mapping Keys to Strings. Keys are a new type which represent a tuple of a String and an integer to denote the home node identifier of the data.
  Strings are used to hold serialized blobs of data. The Network object on the other hand contains all the necessary logic for registering and communicating with the other nodes in the cluster.
  Overall, the Store layer will have a simple public API supporting methods such as ‘get’, ‘getAndWait’, and ‘put’ which all work with DistributedDataFrames.
  There is also a set of analogous private API methods that work on chunks of DistributedColumns, described below.
  In the middle layer we have the useful abstraction classes.
  Specifically, the DistributedDataFrame class and the DistributedColumn class.
  The DistributedDataFrame is a queryable table of data which supports aggregate operations such as ‘map’ and ‘filter’.
  All data stored in a DistributedDataFrame is stored in columnar format with all data in each column being of a single type.
  The types supported by a DataFrame are the four SoR types: INT, BOOL, FLOAT, and STRING. Unlike normal DataFrames, DistributedDataFrames access their data as needed from other nodes across the network under-the-hood.
  The DistributedColumn class has the API of a normal column, however it is implemented such that the values it stores are distributed in chunks across the cluster of nodes using the Store’s private API.
  This abstraction allows this class to represent much more data than the average column stored in local memory.
  For the top layer, we provide a single Application class. This class contains a ‘run_’ method which must be invoked on all nodes in the cluster to start up the system.
  This class has the Store as a field, providing the user which extends the Application class with access to all data stored on the system.
  Additionally, this class will contain any utility methods required for building a program on top of it. One example is the Application API method ‘this_node’ which returns the index of the current node. 

# Use Cases (SEE PDF)

# Open Questions

Any recommended resources for getting started with AWS? 

# Status

At this point, our system is getting very close to completion, with a couple areas left to cleanup.
We currently have no problems handling any task on a single node.
That includes the WordCount demo from Milestone 4, and the Linus demo from Milestone 5. 
We do, however, run into issues when running with multiple threads or multiple nodes.
We have tried with threads, as well as running with multiple executables on the same network, both with similar results.
For example, we can run the Linus demo with two nodes on a network and it usually hangs indefinitely or crashes due to a segmentation fault.
In rare cases, it will run to completion but the results are wrong.
These networking issues will be the focus of our efforts from now until our final code walk (10 days away).
We have network and store tests which run with multiple nodes without problems, so we are still trying to find the root of the problems that we see when running larger application demos. 
This week, we have focused on cleaning up memory leaks. 
We were able to make a lot of progress in this domain, at one point having every main test running with 0 leaks.
This revealed a bunch of bugs in our serialization protocol, and some bad practices in our Column structures which we cleaned up. 
Overall, we are happier with the state of the codebase this week, but we are still running into similar networking issues as we did last week. 
    Simply put, here is where we stand for Milestone 5.
    We have created our own small test files for users, projects, and commits.
    We have very small test files with <10 lines per file, and we have a medium sized dataset with ~200 commits and 10+ users and projects.
    Both of these datasets can be run on a single node, and we can verify that the results are correct. To test this for yourself, run ‘make build & make run’.
    Unfortunately, we have nothing that works for datasets that require multiple nodes.
    Additionally, as requested, we have created an overarching target to run all tests.
    Run ‘make test’ to run all of our unit tests. 
    As of our submission, all tests are passing when we run them. With this week’s progress cleaning up memory leaks, we will shift our focus to network issues. 
