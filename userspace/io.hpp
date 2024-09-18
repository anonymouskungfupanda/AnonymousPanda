#pragma once

#include <cstdint>
#include <utility>
#include <vector>

struct io_info {
    uint32_t size_sectors;                  /* Size in sectors */
    uint64_t sector;                        /* Sector number */
    bool     is_read;                       /* True if read, false if write */
};

/* Needed for usage as a KEY in std::Map */
const bool operator<(const io_info &a, const io_info &b);

