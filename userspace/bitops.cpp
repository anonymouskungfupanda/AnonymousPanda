#include <iostream>
#include <fstream>
#include <cassert>
#include "bitops.hpp"

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

/*
 * Checks endianness at compile time so that bitmap dumps are
 *      always in the same order
 */
constexpr bool is_little_endian() {

    union {
        uint16_t magic = 0xBEEF;
        uint8_t bytes[2];
    };

    if (bytes[0] == 0xEF && bytes[1] == 0xBE)
        return true;

    /* Big-endian */
    return false;
}

/*
 * We agreed on msb being written first
 */
inline uint32_t to_big_endian(uint32_t x) {

    uint32_t ret = 0;

    if (is_little_endian()) {
        ret |= ((x & 0x000000FF) << 0x18);
        ret |= ((x & 0x0000FF00) << 0x08);
        ret |= ((x & 0x00FF0000) >> 0x08);
        ret |= ((x & 0xFF000000) >> 0x18);
    }

    return ret;
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

bitmap::bitmap(uint32_t bits) : bit_count(bits) {
    const size_t bits_per_byte = 8;

    /* This is just some math to round up to the nearest 32 bits */
    const size_t dwords = (bits/sizeof(uint32_t)) + (bits%sizeof(uint32_t) != 0);
    const size_t alloc_size = (dwords/bits_per_byte) + (dwords%bits_per_byte != 0);
    arr = std::vector<uint32_t>(alloc_size);

    for (size_t i = 0; i < arr.size(); i++)
        arr[i] = 0;

    weight = 0;
}

std::pair<uint32_t, bool> const bitmap::search_arr(bool find_zero, uint32_t start) {

    /* Search by checking every 32 bits at once */
    // for (size_t i = 0; i < arr.size(); i++) {
    //     uint32_t dword = invert ? ~arr[i] : arr[i];
    //     if (dword != 0) {
    //         uint32_t bit_pos = (i * sizeof(uint32_t) * BITS_PER_BYTE) + __builtin_ffs(dword) - 1;
    //         if (bit_pos < bit_count)
    //             return { bit_pos, true };
    //     }
    // }

    // This is slow, but it's also very simple. So I like it
    for (uint32_t i = start; i < bit_count; i++) {
        bool bit_set = test_bit(i);

        // Equivalent to (find_zero AND clear bit) OR (find_one AND set bit)
        if (find_zero != bit_set)
            return { i, true };
    }
    return { 0, false }; // Not found
}

/*
 * both of these returns an std::pair, the first field being the location of the bit
 *      if it was found. The second argument is true if the bit was found
 */
std::pair<uint32_t, bool> const bitmap::find_first_zero_bit() {
    return search_arr(true, 0);
}
std::pair<uint32_t, bool> const bitmap::find_next_zero_bit(uint32_t start) {
    return search_arr(true, start);
}


std::pair<uint32_t, bool> const bitmap::find_first_one_bit() {
    return search_arr(false, 0);
}
std::pair<uint32_t, bool> const bitmap::find_next_one_bit(uint32_t start) {
    return search_arr(false, start);
}

void bitmap::clear_bit(uint32_t bit) {
    assert(bit < bit_count);

    const size_t offset = bit % (sizeof(uint32_t) * BITS_PER_BYTE);
    const size_t index  = bit / (sizeof(uint32_t) * BITS_PER_BYTE);

    if ((arr[index] & (1 << offset)) != 0)
        weight = weight - 1;
    arr[index] &= ~(1 << offset);

}

void bitmap::set_bit(uint32_t bit) {
    assert(bit < bit_count);

    const size_t offset = bit % (sizeof(uint32_t) * BITS_PER_BYTE);
    const size_t index  = bit / (sizeof(uint32_t) * BITS_PER_BYTE);

    if ((arr[index] & (1 << offset)) == 0)
        weight = weight + 1;

    arr[index] |= (1 << offset);
}

bool const bitmap::test_bit(uint32_t bit)
{
    assert(bit < bit_count);

    const size_t offset = bit % (sizeof(uint32_t) * BITS_PER_BYTE);
    const size_t index  = bit / (sizeof(uint32_t) * BITS_PER_BYTE);

    return arr[index] & (1 << offset);
}

void bitmap::dump_bits(const char *path) {

    std::ofstream dump_file(path, std::ios::out | std::ios::binary);

    if (!dump_file.is_open()) {
        std::cout << "Could not open: " << path << "\n";
        return;
    }

    for (auto it = arr.rbegin(); it != arr.rend(); it++) {
        uint32_t bits = to_big_endian(*it);
        dump_file.write(reinterpret_cast<const char *>(&bits), sizeof(bits));
    }

    dump_file.close();
}

unsigned long bitmap::get_weight() {

    return weight;

}
