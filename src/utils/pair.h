#include "object.h"

// Represents a pair
class Pair : public Object {
   public:
    Object* first;
    Object* second;

    Pair(Object* f, Object* s) {
        first = f;
        second = s;
    }

    ~Pair() {}

    // Get the reference to the first item in this pair
    Object* get_first() {
        return first;
    }

    // Get the reference to the second item in this pair
    Object* get_second() {
        return second;
    }
};