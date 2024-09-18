#pragma once

#include <cstdint>
#include <iostream>
#include <memory>
#include "io.hpp"


struct zone {

public:

    uint32_t write_pointer;         /* Current write pointer location in sectors */
    uint64_t zone_size_sectors;     /* Zone size in sectors */
    uint64_t zone_cap_sectors;      /* Zone capacity in sectors */


public:

    uint32_t get_wp();              /* Write pointer getter */

    void init(uint64_t zn_size, uint64_t zn_cap);
    void append(uint32_t size_bytes);
    void reset();

};


/* Cache device */
struct zbd {

public:

    std::unique_ptr<zone[]> zones;  /* Representation of individual zone metadata */
    uint64_t zone_size_sectors;     /* Zone size in sectors */
    uint64_t zone_cap_sectors;      /* Zone capacity in sectors */
    uint32_t zone_count;

    zbd(uint64_t zn_size, uint64_t zn_cap, uint32_t nr_zones);
    ~zbd();

    /* Read and write to cache device */
    bool write(io_info &io);
    bool read(io_info &io);

    /* Dump metadata to a csv */
    void dump_meta(const char *path);

};

