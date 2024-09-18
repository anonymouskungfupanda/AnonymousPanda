#include "io.hpp"

const bool operator<(const io_info &a, const io_info &b) {

    if (a.sector != b.sector)
        return a.sector < b.sector;

    else if (a.is_read != b.is_read)
        return a.is_read < b.is_read;

    else return a.size_sectors < b.size_sectors;
}

