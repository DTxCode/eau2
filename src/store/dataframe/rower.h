#pragma once
#include "../../utils/object.h"
#include "row.h"

#define INT_TYPE 'I'
#define STRING_TYPE 'S'
#define BOOL_TYPE 'B'
#define FLOAT_TYPE 'F'

/*******************************************************************************
 *  Rower::
 *  An interface for iterating through each row of a data frame. The intent
 *  is that this class should subclassed and the accept() method be given
 *  a meaningful implementation. Rowers can be cloned for parallel execution.
 */
class Rower : public Object {
   public:
    /** This method is called once per row. The row object is on loan and
      should not be retained as it is likely going to be reused in the next
      call. The return value is used in filters to indicate that a row
      should be kept. */
    virtual bool accept(Row& r) { return true; }

    /** Once traversal of the data frame is complete the rowers that were
      split off will be joined.  There will be one join per split. The
      original object will be the last to be called join on. The join method
      is reponsible for cleaning up memory. */
    virtual void join_delete(Rower* other) {}
};

// BoolFlipRower is a rower that flips the boolean values of any rows it sees.
// Used for testing map.
class BoolFlipRower : public Rower {
   public:
    bool accept(Row& r) {
        // If we see a boolean in this row, replace it with its opposite value.
        for (size_t i = 0; i < r.width(); i++) {
            char type = r.col_type(i);

            if (type == BOOL_TYPE) {
                bool b = r.get_bool(i);
                r.set(i, !b);
            }
        }

        // Unused by map
        return true;
    }
};

// TrueRower is a rower that accepts rows which have a boolean set to True within them
// Used for testing filter.
class TrueRower : public Rower {
   public:
    bool accept(Row& r) {
        TrueCountFielder f;

        r.visit(r.get_idx(), f);

        size_t num_trues = f.get_count();

        if (num_trues > 0) {
            return true;
        }

        return false;
    }
};

// Rower that executes a for loop with the nuber of iterations
// equal to 100 * (sum of integers in given row + 1)
class LoopRower : public Rower {
   public:
    virtual bool accept(Row& r) {
        IntSumFielder f;
        r.visit(r.get_idx(), f);

        size_t sum = f.get_sum() + 1;

        size_t dummy = 0;
        for (size_t i = 0; i < (100 * sum); i++) {
            // Do something useful
            dummy++;
        }

        return true;
    }

    // Nothing to join, just delete given rower
    virtual void join_delete(Rower* other) {
        delete other;
    }

    // Returns a new loop rower pointer
    // This class has no unique fields
    virtual LoopRower* clone() {
        return new LoopRower();
    }
};

// Rower that tracks the maximum integer seen, or 0
class MaxRower : public Rower {
   public:
    int max = 0;
    virtual bool accept(Row& r) {
        MaxSeenFielder f;
        r.visit(r.get_idx(), f);

        if (f.get_max() > max) {
            max = f.get_max();
        }

        return true;
    }

    // Join the two max values and delete given rower
    virtual void join_delete(Rower* other) {
        MaxRower* m = dynamic_cast<MaxRower*>(other);
        if (m->get_max() > max) {
            max = m->get_max();
        }
        delete other;
    }

    // Return maximum int seen by this rower
    int get_max() { return max; }

    // Returns a new max rower pointer with same max as this
    virtual MaxRower* clone() {
        MaxRower* m = new MaxRower();
        m->max = get_max();
        return m;
    }
};
