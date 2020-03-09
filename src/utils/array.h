#pragma once

#include <stdlib.h>
#include "object.h"
#include "string.h"

/**
 * An array of objects with a dynamic size
 */
class ObjectArray : public Object {
   public:
    Object** array;    // Array of object pointers
    size_t capacity_;  // Number of 'slots' in the current array
    size_t size_;      // Number of 'filled slots' in the current array

    /**
    * Creates a new Object array.
    */
    ObjectArray() : Object() {
        capacity_ = 10;
        size_ = 0;
        array = new Object*[capacity_];
        for (size_t i = 0; i < capacity_; i++) {
            array[i] = nullptr;  // Init to all null
        }
    }

    // Destruct this array
    virtual ~ObjectArray() {
        delete[] array;  // Delete array pointer, NOT each object in it
    }

    /*
    * Double the capacity of the array. Create a new internal array
    * and copy all elements over.
    */
    virtual void increase_capacity() {
        size_t new_cap = capacity_ * 2;
        Object** new_array = new Object*[new_cap];

        for (size_t i = 0; i < new_cap; i++) {
            if (i < size_) {
                new_array[i] = array[i];  // Copy old elements
            } else {
                new_array[i] = nullptr;  // Null out new space
            }
        }

        capacity_ = new_cap;
        delete[] array;
        array = new_array;
    }

    /*
    * Push back the given object onto the array
    */
    virtual void push_back(Object* obj) {
        if (size_ == capacity_) {  // Resize array if its full
            increase_capacity();
        }

        array[size_] = obj;
        size_++;
    }

    /**
     * Provides a hash of this array. If this array is equal to another array, the hash codes should be equal
     * @return The hash code of the array
     */
    virtual size_t hash() {
        size_t hash = 1;
        // Multiply all object hashes together
        for (size_t i = 0; i < capacity_; i++) {
            Object* obj = array[i];

            if (obj != nullptr) {
                hash *= obj->hash();
            }
        }
        // To avoid permutations of arrays being equal, also multiple again
        // by first objects hash
        Object* obj = array[0];
        if (obj != nullptr) {
            hash *= obj->hash();
        }

        return hash;
    }

    /**
         * Returns true if this array is equal to another object. If the object is an array they are equal if it has
         * the same elements in the same order as this array.
         * @param other The object to compare this object to
         * @return true if the other object is equal to this object; false otherwise
         */
    virtual bool equals(Object* other) {
        if (nullptr == other) {
            return false;
        }

        ObjectArray* otherArray = dynamic_cast<ObjectArray*>(other);

        if (nullptr == otherArray) {
            // Not an ObjectArray
            return false;
        }

        if (this == otherArray) {
            return true;
        }

        if (size_ != otherArray->size()) {
            return false;
        }

        // Go through array, checking each element to other array's element
        // for equality
        for (size_t i = 0; i < size_; i++) {
            Object* mine = array[i];
            Object* other = otherArray->get(i);

            if (mine == nullptr && other != nullptr) {
                return false;
            } else if (mine != nullptr && other == nullptr) {
                return false;
            } else if (mine != nullptr) {
                // both mine and other are not nullptrs
                if (!mine->equals(other)) {
                    return false;
                }
            }
        }

        // Same size, same elements. Equal array
        return true;
    }

    /**
    * @return The size of the array
    */
    size_t size() {
        return size_;
    }

    /**
    * Returns the element at the given index. Indices are bound checked.
    * @param i The index of the element to return
    * @return The element at i
    */
    Object* get(size_t i) {
        if (i >= size_) {
            return nullptr;
        }

        return array[i];
    }

    /**
     * Sets the object at the given index. Indices are bound checked. Cannot set out of size bounds
     * @param obj The object that should be set
     * @param i
     */
    void set(size_t i, Object* obj) {
        if (i >= size_) {
            return;
        }

        array[i] = obj;
    }

    /**
         * Returns the first occurrence of the given object in the array using the object's equal() method.
         * If the object is not found, size() is returned.
         * @param obj The object to find in the array
         * @return The first index of the object. size() if not found
         */
    size_t index_of(Object* obj) {
        if (nullptr == obj) {
            return size();
        }

        // Check each element for equality
        for (size_t i = 0; i < size_; i++) {
            Object* mine = array[i];

            if (obj->equals(mine)) {
                return i;
            }
        }

        return size();
    }
};

/**
 * An array of strings with a dynamic size
 */
class StringArray : public Object {
   public:
    ObjectArray* array;  // Object array to store Strings

    /**
         * Creates a new String array.
         */
    StringArray() : Object() {
        array = new ObjectArray();
    }

    // Destroy string array
    virtual ~StringArray() {
        delete array;
    }

    /**
         * Push back the given string onto the array
         */
    virtual void push_back(String* obj) {
        array->push_back(obj);
    }

    /**
         * Provides a hash of this array. If this array is equal to another array, the hash codes should be equal
         * @return The hash code of the array
         */
    virtual size_t hash() {
        return array->hash();
    }

    /**
         * Returns true if this array is equal to another object. If the object is an array they are equal if it has
         * the same elements in the same order as this array.
         * @param other The object to compare this object to
         * @return true if the other object is equal to this object; false otherwise
         */
    virtual bool equals(Object* other) {
        if (nullptr == other) {
            return false;
        }

        StringArray* otherArray = dynamic_cast<StringArray*>(other);

        if (nullptr == otherArray) {
            // Not an StringArray
            return false;
        }

        if (this == otherArray) {
            return true;
        }

        if (array->size() != otherArray->size()) {
            return false;
        }

        // Go through array, checking each element to other array's element
        // for equality
        for (size_t i = 0; i < array->size(); i++) {
            String* mine = dynamic_cast<String*>(array->get(i));
            Object* other = otherArray->get(i);

            if (mine == nullptr && other != nullptr) {
                return false;
            } else if (mine != nullptr && other == nullptr) {
                return false;
            } else if (mine != nullptr) {
                // both mine and other are not nullptrs
                if (!mine->equals(other)) {
                    return false;
                }
            }
        }

        // Same size, same elements. Equal array
        return true;
    }

    /**
         * @return The size of the array
         */
    size_t size() {
        return array->size();
    }

    /**
         * Returns the element at the given index. Indices are bound checked.
         * @param i The index of the element to return
         * @return The element at i
         */
    String* get(size_t i) {
        Object* obj = array->get(i);
        if (nullptr == obj) {
            return nullptr;
        }

        return dynamic_cast<String*>(obj);
    };

    /**
         * Sets the object at the given index. Indices are bound checked. Cannot set out of size bounds
         * @param givenString The object that should be placed in the array
         * @param i
         */
    void set(size_t i, String* givenString) {
        array->set(i, givenString);
    }

    /**
         * Returns the first occurrence of the given object in the array using the object's equal() method.
         * If the object is not found, size() is returned.
         * @param givenString The object to find in the array
         * @return The first index of the object. size() if not found
         */
    size_t index_of(String* givenString) {
        return array->index_of(givenString);
    }
};
