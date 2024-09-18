#pragma once

#include <cstdint>
#include <vector>

/*
 * There are various reasons why I decided not to use std::bitset
 */
struct bitmap {

private:

    uint32_t bit_count;
    std::vector<uint32_t> arr;
    unsigned long weight;

    /* To reduce code for bitmap searching */
    std::pair<uint32_t, bool> const search_arr(bool invert, uint32_t start);

public:

    const std::size_t BITS_PER_BYTE = 8;
    bitmap(uint32_t bits);

    /* Bit manipulation */
    void clear_bit(uint32_t bit);
    void set_bit(uint32_t bit);
    bool const test_bit(uint32_t bit);

    /* Searching */
    std::pair<uint32_t, bool> const find_first_zero_bit();
    std::pair<uint32_t, bool> const find_next_zero_bit(uint32_t start);
    std::pair<uint32_t, bool> const find_first_one_bit();
    std::pair<uint32_t, bool> const find_next_one_bit(uint32_t start);

    void dump_bits(const char *path);
    unsigned long get_weight();

};

