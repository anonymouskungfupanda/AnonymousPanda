#include <iostream>
#include <fstream>
#include <cassert>
#include "zbd.hpp"
#include "dmcache.hpp"


/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

uint32_t zone::get_wp() {

    return write_pointer;

};

void zone::init(uint64_t zn_size, uint64_t zn_cap) {

    zone_size_sectors = zn_size;
    zone_cap_sectors  = zn_cap;

    reset();

}

void zone::append(uint32_t size_sectors) {

    write_pointer += size_sectors;

    /* Wrote past zone capacity */
    assert(write_pointer <= zone_cap_sectors);

}

void zone::reset() {

    write_pointer = 0;

}


/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

zbd::zbd(uint64_t zn_size, uint64_t zn_cap, uint32_t nr_zones) {

    zones = std::unique_ptr<zone[]>(new zone[nr_zones]);
    zone_size_sectors = zn_size;
    zone_cap_sectors  = zn_cap;
    zone_count        = nr_zones;

    for (uint64_t i = 0; i < nr_zones; i++)
        zones[i].init(zn_size, zn_cap);

}

zbd::~zbd() {

}

/*
 * NOTE: Zone appends use the start sector, this function respects that. It
 *      will update io.sector with where it was written if successful.
 */
bool zbd::write(io_info &io) {

    uint64_t zone_idx = io.sector / zone_size_sectors;
    //uint64_t zone_idx = io.sector / zone_cap_sectors;
    uint64_t zone_wp  = zones[zone_idx].get_wp();
    uint64_t offset   = io.sector % zone_size_sectors;

    // If this happens, it indicates that there is an issue with the code
    assert(zone_wp != zone_cap_sectors + 1);
    assert(offset == 0);

    DBUG("[ZBD WRITE] zone_idx:", zone_idx, " zone_wp:", zone_wp, " offset:", offset, "\n");

    zones[zone_idx].append(io.size_sectors);
    assert(io.size_sectors == 8);
    io.sector = (zone_idx * zone_size_sectors) + zone_wp;
    return true;
}

bool zbd::read(io_info &io) {
    uint64_t zone_idx = io.sector / zone_size_sectors;
    uint64_t zone_wp  = zones[zone_idx].get_wp();
    uint64_t offset   = io.sector % zone_size_sectors;

    // Cannot read past write pointer
    assert(offset + io.size_sectors <= zone_wp);

    return true;
}

void zbd::dump_meta(const char *path) {

    std::ofstream dump_file(path);

    dump_file << "Zone index, Write Pointer\n";
    for (uint32_t i = 0; i < zone_count; i++) {
        dump_file << i << ", " << zones[i].get_wp() << '\n';
    }

    dump_file.close();
}

