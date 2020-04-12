#include "../../src/store/network/master.h"
#include "../../src/client/application.h"
#include "../../src/store/dataframe/dataframe.h"
#include "../../src/store/store.cpp"
#include "../../src/store/dataframe/rower.h"

/** Authors:
*	Ryan Heminway (heminway.r@husky.neu.edu)
*   David Tandetnik (tandetnik.da@husky.neu.edu) */
/**
 * The input data is a processed extract from GitHub.
 *
 * projects:  I x S   --  The first field is a project id (or pid).
 *                    --  The second field is that project's name.
 *                    --  In a well-formed dataset the largest pid
 *                    --  is equal to the number of projects.
 *
 * users:    I x S    -- The first field is a user id, (or uid).
 *                    -- The second field is that user's name.
 *
 * commits: I x I x I -- The fields are pid, uid, uid', each row represent
 *                    -- a commit to project pid, written by user uid
 *                    -- and committed by user uid',
 **/

/**************************************************************************
 * A bit set contains size() booleans that are initialize to false and can
 * be set to true with the set() method. The test() method returns the
 * value. Does not grow.
 ************************************************************************/
class Set {
   public:
    bool* vals_;  // owned; data
    size_t size_; // number of elements

    /** Creates a set of the same size as the dataframe. */ 
    Set(DataFrame* df) : Set(df->nrows()) {}

    /** Creates a set of the given size. */
    Set(size_t sz) :  vals_(new bool[sz]), size_(sz) {
        for(size_t i = 0; i < size_; i++)
            vals_[i] = false; 
    }

    ~Set() { delete[] vals_; }

    /** Add idx to the set. If idx is out of bound, ignore it.  Out of bound
     *  values can occur if there are references to pids or uids in commits
     *  that did not appear in projects or users.
     */
    void set(size_t idx) {
        if (idx >= size_ ) return; // ignoring out of bound writes
        vals_[idx] = true;       
    }

    /** Is idx in the set?  See comment for set(). */
    bool test(size_t idx) {
        if (idx >= size_) return true; // ignoring out of bound reads
        return vals_[idx];
    }

    size_t size() { return size_; }

    /** Performs set union in place. */
    void union_(Set& from) {
        for (size_t i = 0; i < from.size_; i++) 
            if (from.test(i))
                set(i);
    }
};


/*******************************************************************************
 * A SetUpdater is a rower that gets the first column of the data frame and
 * sets the corresponding value in the given set.
 ******************************************************************************/
class SetUpdater : public Rower {
   public:
    Set& set_; // set to update

    SetUpdater(Set& set): set_(set) {}

    /** Assume a row with at least one column of type I. Assumes that there
     * are no missing. Reads the value and sets the corresponding position.
     * The return value is irrelevant here. */
    bool accept(Row & row) { 
        set_.set(row.get_int(0));  
        return false; 
    }
};

/*****************************************************************************
 * A SetWriter copies all the values present in the set into a one-column
 * dataframe. The data contains all the values in the set. The dataframe has
 * at least one integer column.
 ****************************************************************************/
class SetWriter: public Writer {
   public:
    Set& set_; // set to read from
    int i_ = 0;  // position in set

    SetWriter(Set& set): set_(set) { }

    /** Skip over false values and stop when the entire set has been seen */
    bool done() {
        while (i_ < set_.size_ && set_.test(i_) == false) ++i_;
        return i_ == set_.size_;
    }

    bool accept(Row & row) { 
        row.set(0, i_++);
        return true;
    }
};

/***************************************************************************
 * The ProjectTagger is a reader that is mapped over commits, and marks all
 * of the projects to which a collaborator of Linus committed as an author.
 * The commit dataframe has the form:
 *    pid x uid x uid
 * where the pid is the identifier of a project and the uids are the
 * identifiers of the author and committer. If the author is a collaborator
 * of Linus, then the project is added to the set. If the project was
 * already tagged then it is not added to the set of newProjects.
 *************************************************************************/
class ProjectsTagger : public Rower {
   public:
    Set& uSet; // set of collaborator 
    Set& pSet; // set of projects of collaborators
    Set newProjects;  // newly tagged collaborator projects

    ProjectsTagger(Set& uSet, Set& pSet, DataFrame* proj):
        uSet(uSet), pSet(pSet), newProjects(proj) {}

    /** The data frame must have at least two integer columns. The newProject
     * set keeps track of projects that were newly tagged (they will have to
     * be communicated to other nodes). */
    bool accept(Row & row) override {
        int pid = row.get_int(0);
        int uid = row.get_int(1);
        if (uSet.test(uid)) 
            if (!pSet.test(pid)) {
                pSet.set(pid);
                newProjects.set(pid);
            }
        return false;
    }
};

/***************************************************************************
 * The UserTagger is a rower that is mapped over commits, and marks all of
 * the users which commmitted to a project to which a collaborator of Linus
 * also committed as an author. The commit dataframe has the form:
 *    pid x uid x uid
 * where the pid is the idefntifier of a project and the uids are the
 * identifiers of the author and committer. 
 *************************************************************************/
class UsersTagger : public Rower {
   public:
    Set& pSet;
    Set& uSet;
    Set newUsers;

    UsersTagger(Set& pSet,Set& uSet, DataFrame* users):
        pSet(pSet), uSet(uSet), newUsers(users->nrows()) { }

    bool accept(Row & row) override {
        int pid = row.get_int(0);
        int uid = row.get_int(1);
        if (pSet.test(pid)) 
            if(!uSet.test(uid)) {
                uSet.set(uid);
                newUsers.set(uid);
            }
        return false;
    }
};

/*************************************************************************
 * This computes the collaborators of Linus Torvalds.
 * is the linus example using the adapter.  And slightly revised
 *   algorithm that only ever trades the deltas.
 **************************************************************************/
class Linus : public Application {
   public:
    size_t DEGREES = 1;  // How many degrees of separation from linus?
    int LINUS = 1; //4967;   // The uid of Linus (offset in the user df)
    const char* PROJ = "data/projects_small.sor";
    const char* USER = "data/users_small.sor";
    const char* COMM = "data/commits_small.sor";
    DataFrame* projects; //  pid x project name
    DataFrame* users;  // uid x user name
    DataFrame* commits;  // pid x uid x uid 
    Set* uSet; // Linus' collaborators
    Set* pSet; // projects of collaborators

    Linus(Store* store): Application(store) {}

    /** Compute DEGREES of Linus.  */
    void run_() override {
        readInput();
        for (size_t i = 0; i < DEGREES; i++) step(i);
    }

    // Return a key for storing a node's partial results
    Key* mk_key(char* str, size_t stage, size_t node_id) {
        size_t needed_buf_size = snprintf(nullptr, 0, "%s-%zu-%zu", str, stage, node_id) + 1;
        char key_name[needed_buf_size];
        snprintf(key_name, needed_buf_size, "%s-%zu-%zu", str, stage, node_id);

        return new Key(key_name, node_id);
    }

    /** Node 0 reads three files, cointainng projects, users and commits, and
     *  creates thre dataframes. All other nodes wait and load the three
     *  dataframes. Once we know the size of users and projects, we create
     *  sets of each (uSet and pSet). We also output a data frame with a the
     *  'tagged' users. At this point the dataframe consists of only
     *  Linus. **/
    void readInput() {
        Key* pK = new Key((char*) "projs", 0);
        Key* uK = new Key((char*) "usrs", 0);
        Key* cK = new Key((char*) "comts", 0);
        if (this_node() == 0) {
            printf("Reading...\n");
            projects = DataFrame::fromSorFile(pK, store, (char*) PROJ);
            printf("%zu projects\n", projects->nrows());
            users = DataFrame::fromSorFile(uK, store, (char*) USER);
            printf("%zu users\n", users->nrows());
            commits = DataFrame::fromSorFile(cK, store, (char*) COMM);
            printf("%zu commits\n", commits->nrows());
            // This dataframe contains the id of Linus.
            delete DataFrame::fromScalar(new Key((char*)"users-0-0", 0), store, LINUS);
        } else {
            projects = dynamic_cast<DataFrame*>(store->waitAndGet(pK));
            users = dynamic_cast<DataFrame*>(store->waitAndGet(uK));
            commits = dynamic_cast<DataFrame*>(store->waitAndGet(cK));
        }
        // All users and all projects set to false initially
        uSet = new Set(users);
        pSet = new Set(projects);
    }


    /** Performs a step of the linus calculation. It operates over the three
     *  datafrrames (projects, users, commits), the sets of tagged users and
     *  projects, and the users added in the previous round. */
    void step(size_t stage) {
        printf("Stage: %zu\n", stage);
        // Key of the shape: users-stage-0
        Key* uK = mk_key("users", stage, 0);

        // A df with all the users added on the previous round
        DataFrame* newUsers = dynamic_cast<DataFrame*>(store->waitAndGet(uK));
        // users is DF of all user info    
        Set delta(users);
        SetUpdater upd(delta); 
        // For all users in newUsers, set that user bit to True in delta 
        newUsers->map(upd); 
        delete newUsers;
        // At this point delta is updated with the newUsers

        // Mark all projects touched by the users in delta as projects related to linus
        ProjectsTagger ptagger(delta, *pSet, projects);
        // IMPORTANT: Will only update with projects on this node
        commits->local_map(ptagger);
        // Merge results from other nodes
        merge(ptagger.newProjects, "projects", stage);
        // Add new projects to set of projects related to linus
        pSet->union_(ptagger.newProjects); 

        // Now mark all users who contributed to any of the new projects
        UsersTagger utagger(ptagger.newProjects, *uSet, users);
        // IMPORTANT: Will only update with users on this node
        commits->local_map(utagger);
        // Merge results from other nodes
        merge(utagger.newUsers, "users", stage + 1);
        // Add new users to set of users related to linus
        uSet->union_(utagger.newUsers); 

        printf("After stage %zu : \n", stage);
        printf("   tagged projects: %zu\n", pSet->size());
        printf("   tagged users: %zu\n", uSet->size());
    }

    /** Gather updates to the given set from all the nodes in the systems.
     * The union of those updates is then published as dataframe.  The key
     * used for the otuput is of the form "name-stage-0" where name is either
     * 'users' or 'projects', stage is the degree of separation being
     * computed.
     */ 
    void merge(Set& set, char const* name, int stage) {
        if (this_node() == 0) {
            for (size_t i = 1; i < num_nodes(); ++i) {
                // STEP (2)
                Key* nK = mk_key((char*) name, stage, i);
                // New elements
                DataFrame* delta = dynamic_cast<DataFrame*>(store->waitAndGet(nK));
                printf("  received delta of %zu elements from node %zu\n", 
                        delta->nrows(), i);
                // Update the given set with new elements
                SetUpdater upd(set);
                delta->map(upd);
                delete delta;
            }
            // STEP (3)
            printf("    storing %zu merged elements \n", set.size());
            // Create a new DF from the updated set 
            SetWriter writer(set);
            Key* k = mk_key((char*) name, stage, 0);
            delete DataFrame::fromWriter(k, store, "I", writer);
        } else {
            // STEP (1) 
            printf("   sending %zu elements to master node \n", set.size());
            // Put a DF in KVS with updated elements (for master node to process)
            SetWriter writer(set);
            Key* k = mk_key((char*) name, stage, this_node());
            delete DataFrame::fromWriter(k, store, "I", writer);

            // STEP (4)
            // Get the merged result from master node
            Key* mK = mk_key((char*) name, stage, 0);
            DataFrame* merged = dynamic_cast<DataFrame*>(store->waitAndGet(mK));
            printf("    receiving %zu merged elements \n", merged->nrows());
            SetUpdater upd(set);
            merged->map(upd);
            delete merged;
        }
    }
}; // Linus

bool test_linus() {
    char* master_ip = (char*)"127.0.0.1";
    int master_port = 8888;
    Server s(master_ip, master_port);
    s.listen_for_clients();

    Store store1(0, (char*)"127.0.0.1", 8000, master_ip, master_port);
   //  Store store2(1, (char*)"127.0.0.1", 8001, master_ip, master_port);
   // Store store3(2, (char*)"127.0.0.1", 8002, master_ip, master_port);

    Linus linus(&store1);
    linus.run();

    // shutdown system
    s.shutdown();

    // wait for nodes to finish
    while (!store1.is_shutdown()) {
    }

    return true;
}

int main() {
    assert(test_linus());
    printf("=================test_linus PASSED if correct number of collaborators printed==================\n");
    return 0;
}
