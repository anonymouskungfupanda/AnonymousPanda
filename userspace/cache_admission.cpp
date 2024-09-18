#include "dmcache_gc.hpp"
#include "dmcache.hpp"
#include "list.hpp"
#include <cassert>
#include <cstdint>

/* Just for show for the time being */
#define READ_STAGE_DATA() ( nullptr )

/*
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 */

void dmcache::admit_to_cache(io_info &io) {

    uint64_t offset = io.sector & block_mask;
    uint64_t key = (io.sector - offset) >> block_shift;

    /* Either take an unused cacheblock or evict one */
    std::pair<bool, cacheblock *> req = request_cache_block();
    cacheblock *block = req.second;

#if GC_ENABLED

    // Maintain this crazy stat
    if (gc->previously_relocated_blocks_evicted_via_lru.find(key) !=
            gc->previously_relocated_blocks_evicted_via_lru.end()) {
        gc->relocation_misses_after_lru_eviction++;
        gc->previously_relocated_blocks_evicted_via_lru.erase(key);

        auto &b_stats = relocated_block_stats[key];
        b_stats.relocation_misses_after_lru_eviction++;
    }

    // Every block should start with these flags cleared
    block->set_relocated(false);
    block->set_can_count_unique_hit(false);
    block->set_relocation_read_hit(false);
    block->set_relocation_write_hit(false);

    // If this cache miss was preceded by an eviction, inc
    // the counter and remove that source address
    auto iter = gc->evicted_blocks.find(key);
    if (iter != gc->evicted_blocks.end()) {
        gc->eviction_misses_count++;
        gc->evicted_blocks.erase(iter);
    }
#endif

    /* Keep track of garbage sector for logging */
    uint64_t garbage_sector =
        (req.first) ? block_idx_to_sector_addr(block->cache_block_addr) : -1;

    /* Cache invalidation - only remove it from the cache if it was evicted and
     * not if the cacheblock was taken from the empty list */
    if (req.first) {
        ++stats.replaces;
        map.erase(block->src_block_addr);
    } else
        ++stats.inserts;

    /* Cache insertion */
    block->src_block_addr = key;
    map[key] = block;

    if (cache_dev_is_zoned) {
        // Both types of misses cause a write. Actually, the only I/O that doesn't
        // cause a write is a read hit. Fun fact

        /* Update dmcache's metadata */
        prep_for_write(block, req.first, false, false);
    }

    /* This is an incomplete IO. Associate the information in the io_info struct
     * with the cacheblock so that when the complete event is recieved everything
     * can be updated appropriately */
    incomplete_io[io] = {
        .op_type = io.is_read ? incomplete_io_meta::OP_READ_MISS
            : incomplete_io_meta::OP_WRITE_MISS,
        .sector1 = io.sector,
        .sector2 = garbage_sector,
        .counter = FETCH_INC(timestamp_counter),
        .block_ptr = block,
    };

    /* This needs to be called manually */
    handle_blk_ta_complete(io);
}

bool dmcache::cache_admission_addr_staging(io_info &io) {

    uint64_t offset = io.sector & block_mask;
    uint64_t key = (io.sector - offset) >> block_shift;

    /* lookup in the staging area */
    auto it = staging_map.find(key);
    if (it != staging_map.end()) {
        staged_block *block = it->second;
        block->freq_of_access++;

        /* Update LRU */
        staging_lru.remove(&block->list);

        // Admit the block to the cache.
        if (block->freq_of_access > RWSS_ACCESS_THRESHOLD) {
            staging_lru.add_front(&block->list);
            return true;
        } else {
            staging_lru.push_back(&block->list);
        }

    } else {

        std::pair<bool, staged_block *> req = request_staging_area();
        staged_block *block = req.second;
        block->freq_of_access = 1;

        if (req.first) {
            staging_stats.replaces++;
            staging_map.erase(block->src_block_addr);
        } else
            staging_stats.inserts++;

        /* Insertion in the staging area*/
        block->src_block_addr = key;
        staging_map[key] = block;
    }

    if (io.is_read)
        stats.read_misses++;
    else
        stats.write_misses++;

    stats.cache_misses++;

    return false;
}

bool dmcache::cache_admission_data_staging(io_info &io) {
    uint64_t offset = io.sector & block_mask;
    uint64_t key = (io.sector - offset) >> block_shift;

    /* lookup in the staging area */
    auto it = staging_map.find(key);
    if (it != staging_map.end()) {
        staged_block *block = it->second;
        block->freq_of_access++;

        /* Update LRU */
        staging_lru.remove(&block->list);

        /* IO is read, data is staged. 
         * - Serve the request from the staging area
         * - update cache hit stats
         * - if the block->freq_of_access > threshold: admit this to the cache:
         * - Ideally remove this from the staging area*/
        if (block->type == staged_block::DATA_REF && io.is_read) {
            stats.cache_hits++;
            stats.read_hits++;

            if (block->freq_of_access > RWSS_ACCESS_THRESHOLD) {
                staging_lru.add_front(&block->list);
                admit_to_cache(io);
            }
            else staging_lru.push_back(&block->list);

            /* To avoid the cache miss call */
            return false;
        }

        /* IO is read, data is not staged. 
         * - Bring the data into the staging area
         * - Count as miss */
        else if (block->type == staged_block::REF_ONLY && io.is_read) {
            block->data = READ_STAGE_DATA();
            if (block->freq_of_access > RWSS_ACCESS_THRESHOLD) {
                staging_lru.add_front(&block->list);
                return true;
            }
            staging_lru.push_back(&block->list);
        }

        /* IO is write.
         * - Check block->freq_of_Access > threahold
         * - if no: update the block->freq_of_Access
         * - if yes return true:*/
        else {
            if (block->freq_of_access > RWSS_ACCESS_THRESHOLD) {
                staging_lru.add_front(&block->list);
                return true;
            }
            staging_lru.push_back(&block->list);
        }

    } else {

        std::pair<bool, staged_block *> req = request_staging_area();
        staged_block *block = req.second;
        block->freq_of_access = 1;

        if (req.first) {
            staging_stats.replaces++;
            staging_map.erase(block->src_block_addr);
        } else
            staging_stats.inserts++;

        /* Insertion in the staging area*/
        block->src_block_addr = key;
        staging_map[key] = block;

        /* IO is read, stage data and reference */
        if (io.is_read) {
            block->type = staged_block::DATA_REF;
            block->data = READ_STAGE_DATA();
        }

        /* IO is write, stage only reference and write to cache */
        else {
            block->type = staged_block::REF_ONLY;
        }
    }

    if (io.is_read)
        stats.read_misses++;
    else
        stats.write_misses++;

    stats.cache_misses++;
    return false;
}

bool dmcache::cache_admission_sseqr(io_info &io)
{
    bool ret = true;

    /* Since the scan will be moved to the tail of the list at the end of the
     * update_sseqr_window function, the correct scan should be at the tail */
    list_container<sseqr_range> *scan = sseqr_ranges.get_tail();
    ret = SSEQR_SHOULD_ADMIT(scan, sseqr_admit_dist);

    /* The stats for reads and writes are increased in the caller. We want to
     * seperate the stats for what actually enters the cache vs what is skipped
     * due to sequential access patterns (because if we don't then hit rate will
     * be utterly horrible) so if this io is not admitted, we should decrease
     * the stats here to keep the numbers seperate */
    if (!ret && io.is_read) {
        ++seq_io_stats.reads_skipped;
        --stats.reads;
    }
    else if (!ret) {
        ++seq_io_stats.writes_skipped;
        --stats.writes;
    }

    return ret;
}

