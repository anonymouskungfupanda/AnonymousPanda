#include "dmcache.hpp"
#include "list.hpp"
#include <cassert>
#include <cstdint>

static inline uint64_t get_diff(std::uint64_t a, std::uint64_t b) {
    return (a < b) ? (b - a) : (a - b);
}

void dmcache::track_sseqr_scan_stats(list_container<sseqr_range> *scan)
{
    std::uint64_t start_addr = scan->data->start_addr;
    std::uint64_t end_addr = scan->data->end_addr;

    /* The scan length is the distance between the start sector and end
     * sector of the scan passed in */
    std::uint64_t scan_length = (end_addr > start_addr) ?
        end_addr - start_addr : start_addr - end_addr;

    if (scan_length > seq_io_stats.max_scan_length)
        seq_io_stats.max_scan_length = scan_length;

    if (scan_length < seq_io_stats.min_scan_length)
        seq_io_stats.min_scan_length = scan_length;

    seq_io_stats.scan_length_sum += scan_length;
    ++seq_io_stats.num_scans;
}

bool dmcache::update_sseqr_window(io_info &io)
{
    bool ret = false;

    list_container<sseqr_range> *scan = nullptr;
    std::uint64_t diff = 0;

    std::uint64_t start_addr = io.sector;
    std::uint64_t end_addr = start_addr + io.size_sectors;

    /* Try to find an existing scan for this io */

#if(SSEQR_OPTIMIZATION==SSEQR_OPTIMIZATION_MAP)

    /* Here, the start LBA is used to search for an existing start, end LBA pair
     * with an equivalent ending LBA */
    if (auto search = sseqr_hash.find(start_addr); search != sseqr_hash.end()) {
        assert(search->second->data->in_use);
        search->second->data->total_ios++;
        scan = search->second;
    }

#elif(SSEQR_OPTIMIZATION==SSEQR_OPTIMIZATION_TREE)

    /* This finds a key-pair in the map which has the first key which is greater than or
     * equal to `start_addr`. Either this element, or the one that comes before it will
     * be the key pair that has the closest key to `start_addr`, as the first one which is
     * less than `start_addr` might actually be closer. */
    auto search = sseqr_hash.lower_bound(start_addr);

    /* Here, we try to check the element before the lower bound in case the element with
     * the smaller key-pair has the closer key as compared to the lower bound. */
    if (search != sseqr_hash.end())
    {
        /* Avoid decrementing to undefined behavior */
        if (search != sseqr_hash.begin()) {
            auto prev = std::prev(search);
            if (prev != sseqr_hash.end() &&
                    get_diff(prev->first, start_addr) < get_diff(search->first, start_addr)) {
                search = prev;
            }
        }

        diff = get_diff(search->first, start_addr);
        if (diff <= sseqr_accept_range)
            scan = search->second;
    }

    /* If the start_addr is larger than any key across all key-pairs, then we need to look
     * at the last element in the map and determine if it is close enough. */
    else if (!sseqr_hash.empty())
    {
        auto last = sseqr_hash.rbegin();
        diff = get_diff(last->first, start_addr);

        if (diff <= sseqr_accept_range)
            scan = last->second;
    }

#else

    list_container<sseqr_range> *iter = sseqr_ranges.get_tail();
    while (iter != nullptr) {

        diff = (iter->data->end_addr > start_addr) ?
            iter->data->end_addr - start_addr : start_addr - iter->data->end_addr;

        /* Don't iterate over scans that aren't in use */
        if (!iter->data->in_use)
            break;

        /* Found */
        if (diff <= sseqr_accept_range) {
            assert(iter->data->in_use);
            iter->data->total_ios++;
            scan = iter;
            break;
        }

        /* Keep looking */
        else iter = iter->prev;
    }

#endif

    /* If it can't find one, re-use the oldest. Since the old range is no longer
     * being used, we can just update the start and end to reflect that of the io */
    if (scan == nullptr) {
        scan = sseqr_ranges.get_head();
        scan->data->in_use     = true;

        /* Track the stats before changing them in the scan struct */
        UPDATE_SSEQR_HASH(scan->data->end_addr, end_addr, scan);
        track_sseqr_scan_stats(scan);

        scan->data->start_addr = start_addr;
        scan->data->end_addr   = end_addr;
        scan->data->total_ios  = 1; // TODO: Do we need and/or use this?

        /* Mark the start of the sequential scan - this scan is new so it is not long */
        scan->data->t_s        = get_timestamp();
        scan->data->is_long_scan  = false;
    }

    /* Next, calculate the difference between the end of the scan and the start of the
     * io. There are two main scenarios to consider caused by a non-zero accept range:
     *  - The IO ends a little ways after the end of the scan, in which case we need to
     *    update the end of the scan to reflect it's new ending sector
     *  - The IO starts a little ways before the start of the scan, in which case we need
     *    to update the start of the scan to reflect it's new ending sector */
    else {

        if (diff <= sseqr_accept_range && scan->data->end_addr < end_addr) {
            UPDATE_SSEQR_HASH(scan->data->end_addr, end_addr, scan);
            scan->data->end_addr = end_addr;
        }

        /* The hash table for the optimization is not updated here, as only ending LBAs
         * are used as keys. In this case, the start LBA of the scan changes, so there is
         * no point in updating the map */
        else if (diff <= sseqr_accept_range && scan->data->start_addr > start_addr) {
            scan->data->start_addr = start_addr;
        }
    }

    /* Move scan to the front of the list */
    sseqr_ranges.remove(scan);
    sseqr_ranges.push_back(scan);

    /* Determine the sequentiality of the cacheblock */
    if (sequential_flagging) {
        ret = IS_ACCESS_SEQUENTIAL(scan, sseqr_admit_dist);

        if (ret && !scan->data->is_long_scan) {
            scan->data->is_long_scan = true;
            scan->data->t_x = get_timestamp();
        }
    }

    else
        ret = SSEQR_SHOULD_ADMIT(scan, sseqr_admit_dist);

    return ret;
}
