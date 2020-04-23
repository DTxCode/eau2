// lang:CwC
#pragma once
#include <stdlib.h>
#include "object.h"

class List : public Object {
   public:
    // pointer to list of objects
    Object** list;
    size_t capacity;
    size_t length;

    List() {
        int START_CAPACITY = 10;
        length = 0;
        list = new Object*[START_CAPACITY];
        capacity = START_CAPACITY;
    }

    ~List() {
        delete[] list;  // Shouldnt delete elements
    }

    void increase_capacity() {
        size_t new_capacity = capacity * 2;
        Object** new_list = new Object*[new_capacity];

        // copy over data to new list
        for (size_t i = 0; i < length; i++) {
            new_list[i] = list[i];
        }

        delete[] list;
        list = new_list;
        capacity = new_capacity;
    }

    // Appends e to end
    virtual void push_back(Object* e) {
        if (length == capacity) {
            // no space for new element, need to increase list size
            increase_capacity();
        }

        list[length] = e;
        length += 1;
    }

    // Inserts e at i
    virtual void add(size_t i, Object* e) {
        if (i > length) {
            // Insert index out of bounds
            printf("ERROR: tried to add element to list at out-of-bounds index\n");
            exit(1);
        }

        if (length == capacity) {
            // no space for new element, need to increase list size
            increase_capacity();
        }

        // copy elements after i forward one spot
        Object* object_to_shift = list[i];
        for (int j = i; (size_t)j < length; j++) {
            Object* next_object_to_shift = list[j + 1];
            list[j + 1] = object_to_shift;
            object_to_shift = next_object_to_shift;
        }

        // set element at i now that there's space for it
        list[i] = e;

        length += 1;
    }

    // Inserts all of elements in c into this list at i
    virtual void add_all(size_t i, List* c) {
        size_t c_length = c->length;

        for (int j = 0; (size_t)j < c_length; j++) {
            int index_to_insert = i + j;
            Object* object_to_insert = c->get(j);

            add(index_to_insert, object_to_insert);
        }
    }

    // Removes all of elements from this list
    virtual void clear() {
        delete[] list;
        list = new Object*[capacity];  // Note: not reducing capacity on clear()
        length = 0;
    }

    // Returns the element at index
    virtual Object* get(size_t index) {
        if (index >= length) {
            // index out of bounds
            printf("ERROR: tried to get element from list at out-of-bounds index\n");
            exit(1);
        }

        return list[index];
    }

    // Removes the element at i and returns it
    virtual Object* remove(size_t i) {
        if (i >= length) {
            // remove index out of bounds
            printf("ERROR: tried to remove element from list at out-of-bounds index\n");
            exit(1);
        }

        Object* removed_element = list[i];

        // shift elements after the one to remove to the left
        for (int j = i + 1; (size_t)j < length; j++) {
            list[j - 1] = list[j];
        }

        length -= 1;
        return removed_element;
    }

    // Replaces the element at i with e and returns the previous value
    virtual Object* set(size_t i, Object* e) {
        if (i >= length) {
            // set index out of bounds
            printf("ERROR: tried to set element from list at out-of-bounds index\n");
            exit(1);
        }

        Object* replaced_element = list[i];
        list[i] = e;

        return replaced_element;
    }

    // Return the number of elements in the collection
    virtual size_t size() {
        return length;
    }

    // Returns the index of the first occurrence
    // of o, or >size() if not there
    virtual size_t index_of(Object* o) {
        for (size_t i = 0; i < length; i++) {
            Object* element = list[i];

            if (element->equals(o)) {
                return i;
            }
        }

        return size() + 1;
    }

    // Returns the index of the first occurrence
    // of o, or >size() if not there. Uses pointer equality!
    virtual size_t index_of_pointer(Object* o) {
        for (size_t i = 0; i < length; i++) {
            Object* element = list[i];

            if (element == o) {
                return i;
            }
        }

        return size() + 1;
    }

    // Compares o with this list for equality.
    virtual bool equals(Object* o) {
        List* other_list = dynamic_cast<List*>(o);

        if (other_list == nullptr) {
            return false;
        }

        if (other_list->length != length) {
            return false;
        }

        for (size_t i = 0; i < length; i++) {
            Object* other_element = other_list->get(i);
            Object* my_element = list[i];

            if (!other_element->equals(my_element)) {
                return false;
            }
        }

        return true;
    }

    // Returns the hash code value for this list.
    virtual size_t hash() {
        size_t hash = 1;
        for (size_t i = 0; i < length; i++) {
            Object* o = list[i];
            hash *= o->hash();
        }

        if (size() > 0) {
            hash *= get(0)->hash();
        }

        return hash;
    }
};
