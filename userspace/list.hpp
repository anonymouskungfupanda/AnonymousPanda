#pragma once

#include <cassert>
#include <cstdint>

template <typename T>
struct list_container {
    list_container<T> *next, *prev;
    T *data;
};

template <typename T>
struct list_iterator {
    list_container<T>* p;
    list_iterator(list_container<T>* p) : p(p) {}
    bool operator!=(list_iterator rhs) { return p != rhs.p; }
    T& operator*() { return *p->data; }
    void operator++() {
        p = p->next;
    }
    void operator--() {
        p = p->prev;
    }
    list_iterator<T> operator++(int) {
        p = p->next;
        return *this;
    }
    list_iterator<T> operator--(int) {
        p = p->prev;
        return *this;
    }
};

template <typename T>
struct list {

private:

    list_container<T> *head, *tail;
    uint64_t size;

public:

    list_iterator<T> begin() { return list_iterator(head); }
    list_iterator<T> end() { return list_iterator(tail); }

    uint64_t get_size() const {
        return size;
    }

    uint64_t const index_of(list_container<T> *l) {

        uint64_t ret = size - 1;
        for (list_container<T> *temp = tail ;; temp = temp->prev) {
            if (temp == l)
                return ret;
            ret--;
        }

        assert(false);
        return -1;
    }

    void add_front(list_container<T> *l) {

        if (head == nullptr) {
            head = tail = l;
        }

        else {
            head->prev = l;
            l->next = head;
            head = l;
        }

        ++size;
    }

    void push_back(list_container<T> *l) {

        if (tail == nullptr) {
            head = tail = l;
        }

        else {
            tail->next = l;
            l->prev = tail;
            tail = l;
        }

        ++size;
    }

    void remove(list_container<T> *l) {

        list_container<T> *next = l->next;
        list_container<T> *prev = l->prev;

        if (next == nullptr && prev == nullptr) {
            head = tail = nullptr;
        }

        else if (next == nullptr) {
            prev->next = nullptr;
            tail = prev;
        }

        else if (prev == nullptr) {
            next->prev = nullptr;
            head = next;
        }

        else {
            next->prev = prev;
            prev->next = next;
        }

        l->next = l->prev = nullptr;
        --size;
    }

    list_container<T> *pop_front() {

        list_container<T> *ret = head;

        if (head != nullptr)
            remove(head);
        else return nullptr;

        return ret;
    }

    list_container<T> *get_head() const {
        return head;
    }

    list_container<T> *get_tail() const {
        return tail;
    }

    list() : head(nullptr), tail(nullptr), size(0) {}

};

