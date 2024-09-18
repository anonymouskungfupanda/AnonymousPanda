#include <filesystem>
#include <algorithm>
#include <iostream>
#include <cassert>
#include <cstdint>
#include <fstream>
#include <vector>
#include <string>
#include <tuple>
#include <iomanip>

#include "blktrace.hpp"
#include "dmcache_gc.hpp"
#include "dmcache.hpp"
#include "dmsetup_ios.hpp"
#include "io.hpp"
#include "list.hpp"
#include "rt_analysis.hpp"
#include "tqdm.hpp"

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

void dmcache::log_gc_count_vs_io_count() {
    std::string filename = "temp.log";

    static std::ofstream logger_file(filename, std::ios::app);
    logger_file << (stats.reads + stats.writes) << ","
        << gc->gc_garbage_count << ","
        << temp << "\n";

    temp = 0;
    logger_file.flush();
}

dmcache::dmcache(const std::unordered_map<std::string, std::string> &arg_map,
        std::vector<std::ifstream> &trace_files,const std::string &trace_name) {

    char* home_path = getenv("HOME");
    if (!home_path) {
        std::cout << "If you are not on Linux, you must set the HOME environment variable and "
                     "provide a config file!" << std::endl;
        exit(1);
    }

    std::string conf_path(home_path), line;
    conf_path += "/.dm-cache-sim.conf";
    std::ifstream conf_file(conf_path);

    if (!conf_file.is_open()) {
        std::cout << "Could not open config file " + conf_path << std::endl;
        exit(1);
    }

    std::cout << "\nConfiguring (Commandline arguments take precedence over config file arguments)\n";

    /* Parse configurables from config file */
    while (std::getline(conf_file, line))
        parse_conf_option(line);

    /* Parse command line arguments - take precedence over config file */
    parse_argv_options(arg_map);

    /* Cache admission enabled isn't a config file option, so set it here */
    if (arg_map.contains("cache_admission_enabled")) {

        if (std::stoi(arg_map.at("cache_admission_enabled")) == 0)
            cache_admission_enabled = false;

        else {
            std::cout << " - cache admission is enabled\n";
            cache_admission_enabled = true;
        }
    }

    std::cout << "Done\n" << std::endl;

    block_shift     = __builtin_ffs(block_size) - 1;
    block_mask      = block_size - 1;
    blocks_per_zone = zone_cap_sectors / block_size;

    seq_io_stats = {
        .reads_skipped      = 0             ,
        .writes_skipped     = 0             ,
        .max_scan_length    = 0             ,
        .min_scan_length    = UINT64_MAX    ,
        .scan_length_sum    = 0             ,
        .num_scans          = 0             ,
    };
    sseqr_accept_range *= block_size;
    sseqr_admit_dist   *= block_size;

    if (cache_dev_is_zoned) {

        /* With ZBD setting enabled, the cache_size opteion is overwritten and calculated
         * Using the other relevant ZBD aruments provided */
        cache_size          = nr_zones * blocks_per_zone;
        org_cache_size      = cache_size;
    
        if (cache_admission_enabled) {
            /* Theoritically, it can be assumed that staging area size is inifinite. However, 
            * for the sake of simulation, we limit it a large value. */
            staging_area_size   = STAGING_AREA_SIZE;
        }

        /* dmcache does this, so I'm doing it too */
        start_seq_write_zones = 0;

        bm_full_zones    = new bitmap(nr_zones);

        auto res = bm_full_zones->find_first_zero_bit();
        assert(res.second); // The bitmap should be empty, but still

        wp_block            = 0;
        zone_batch_size     = 0;
        current_write_head  = 0;

        cache_dev_max_open_zones = cache_dev_max_active_zones = 14;
        active_zones_max = cache_dev_max_open_zones == 0 ? nr_zones : cache_dev_max_open_zones;

        // Start out the current_zones array as maxed ints. This will allow zone 0 to be
        // selected as the first zone when we call reset_zbd_write_heads()
        for (int i = 0; i < NUM_WRITE_HEADS; i++)
            current_zones[i] = (uint32_t)-1;

        reset_zbd_write_heads();

        cache_device = new zbd(zone_size_sectors, zone_cap_sectors, nr_zones);      

    }

#if GC_ENABLED
    // Creating this GC instance may change DMC state. For example, it
    // may reduce the value of cache_size, which is used below to
    // determine how many cacheblocks should be made. Be careful!
    gc = new dmcacheGC(this, arg_map);
#endif

    init_dmcache_lru();
    ENSURE_LRU_STATES();

    if (cache_admission_enabled && 
        (cache_admission == &dmcache::cache_admission_addr_staging ||
         cache_admission == &dmcache::cache_admission_data_staging)) {
        /* Initializing cache blocks */
        for (uint64_t i = 0; i < staging_area_size; i++) {
            staged_block *block = new staged_block {
                .src_block_addr = 0,
                .freq_of_access = 0,
            };
            block->list.data = block;
            staging_lru.add_front(&block->list);
        }
    }

    if ( (cache_admission_enabled && cache_admission == &dmcache::cache_admission_sseqr) || (sequential_flagging) ) {
        /* Initializing sseqr window */
        for (uint64_t i = 0; i < sseqr_window_size; i++) {
            sseqr_range *block = new sseqr_range {
                .in_use     = false ,
                .start_addr = 0     ,
                .end_addr   = 0     ,
                .total_ios  = 0     ,
            };
            block->list.data = block;
            sseqr_ranges.add_front(&block->list);
        }
    }

    /* Pointer to trace event vector */
    if (use_fio_traces)
        fio_parser = std::make_unique<fio_trace_parser>(
#ifndef STREAMLINED_TRACE_INPUT
                trace_files
#endif
        );
    else
        parser = new blktrace_parser(trace_files);

    print_dmcache_configuration();

    /* For automation logging */
    trace_file_name = trace_name;
}

dmcache::~dmcache() {

    // Final check to make sure the states were maintained
    ENSURE_LRU_STATES();

    // Compute average # of accesses for all blocks
    uint64_t total_accesses = 0;
    uint64_t total_accesses_post_relocaiton = 0;
    for (auto pair : block_stats) {
        total_accesses += pair.second.accesses;
        total_accesses_post_relocaiton += pair.second.post_relocation_accesses;
    }

    if (!block_stats.empty()) {

        uint64_t total_accesses_pre_relocaiton = total_accesses - total_accesses_post_relocaiton;
        avg_access_stats = {
            .avg_num_accesses_all_blocks = (double)total_accesses / block_stats.size(),
            .avg_num_accesses_before_relocation = (double)total_accesses_pre_relocaiton / block_stats.size(),
            .avg_num_accesses_after_relocation = (double)total_accesses_post_relocaiton / block_stats.size(),
            .ratio_avg_accesses_before_over_after = (double)total_accesses_pre_relocaiton / total_accesses_post_relocaiton
        };

#ifndef STREAMLINED_TRACE_INPUT
        std::cout << "Average # of accesses (ALL blocks): " 
            << avg_access_stats.avg_num_accesses_all_blocks << std::endl;
        std::cout << "Average # of accesses before relocation (ALL blocks): " 
            << avg_access_stats.avg_num_accesses_before_relocation << std::endl;
        std::cout << "Average # of accesses after relocation (ALL blocks): " 
            << avg_access_stats.avg_num_accesses_after_relocation << std::endl;
        std::cout << "Ratio of average # accesses before relocation / average # accesses after relocation (ALL blocks): "
            << avg_access_stats.ratio_avg_accesses_before_over_after << std::endl;
#endif

    }

#ifdef STREAMLINED_TRACE_INPUT
    print_dmcache_results_stats();
    if (automation_logging)
        do_automation_logging();
#else
    do_automation_print(std::cerr);
#endif

#if GC_ENABLED
    delete gc;
#endif
    if (cache_dev_is_zoned && dump_zbd_metadata) {
        cache_device->dump_meta("/tmp/zbd_dump.csv");
    }

    if (cache_dev_is_zoned && dump_bitmaps)
        bm_full_zones->dump_bits("/tmp/full_zones_bm.bin");
    
    if (op_logging_enabled) {
        op_logger_file->close();
        delete op_logger_file;
    }

    if (dump_access_pattern) {
        ap_gc_iters_logger_file->close();
        ap_ios_logger_file->close();
        if (ap_flush_logger_file != nullptr) {
            ap_flush_logger_file->close();
            delete ap_flush_logger_file;
        }
        delete ap_gc_iters_logger_file;
        delete ap_ios_logger_file;
    }

    if (stats_logging_enabled) {
        stats_logger_file->close();

        #if GC_ENABLED
            for (uint64_t i = 0; i < nr_zones; i++) {
                *zone_stats_logger_file << gc->per_zone_stats[i]->reclaim_count;

                if (i != nr_zones - 1)
                    *zone_stats_logger_file << ",";
            }
            zone_stats_logger_file->close();
            delete zone_stats_logger_file;

            per_zone_stats_logger_file->close();
            delete per_zone_stats_logger_file;

            rwss_t_logger_file->close();
            delete rwss_t_logger_file;

            cblock_stats_logger_file->close();
            delete cblock_stats_logger_file;

            relocation_stats_logger_file->close();
            delete relocation_stats_logger_file;
        #endif

        delete stats_logger_file;
    }

    list_container<cacheblock> *block = nullptr;
    while ((block = lru.pop_front()) != nullptr) {
        delete block->data;
    }

    list_container<staged_block> *staged_block = nullptr;
    while ((staged_block = staging_lru.pop_front()) != nullptr) {
        delete staged_block->data;
    }

    if (cache_dev_is_zoned)
        delete bm_full_zones;

    delete cache_device;

    if (!use_fio_traces)
        delete parser;
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */



void dmcache::cacheblock_update_state(struct cacheblock *cacheblock)
{
    if (is_state(cacheblock->state, WRITEBACK)) { /* Write back finished */
        cacheblock->state = VALID;
    } else if (is_state(cacheblock->state, WRITETHROUGH)) {

        //cache invalidate
        map.erase(cacheblock->src_block_addr);
        cacheblock->state = INVALID;

    } else { /* Cache insertion finished */
        set_state(cacheblock->state, VALID);
        clear_state(cacheblock->state, RESERVED);
    }
}

void dmcache::reset_zbd_write_heads()
{
    int i;
    uint64_t new_zone_idx;

    for (i = 0; i < NUM_WRITE_HEADS; i++) {

        new_zone_idx = select_new_empty_zone();
        DBUG("New Empty Zone Selected:", new_zone_idx, "\n");
        assert(new_zone_idx < nr_zones);
        current_zones[i] = new_zone_idx;
        active_zones_insert(new_zone_idx);

        /* Initialize the deafult value of max_ts and min_ts as the current_timestamp*/
#if GC_ENABLED
        if(gc) {
            gc->per_zone_stats[new_zone_idx]->max_timestamp = get_timestamp();
            gc->per_zone_stats[new_zone_idx]->min_timestamp = get_timestamp();
        }
#endif

    }
    assert(active_zones_count != active_zones_max);

#if DEBUG_MODE
    active_zones_print();
#endif

}

void dmcache::finish_zone_append(struct cacheblock *cacheblock)
{
    uint64_t zone_idx, zone_block_offset;

    zone_idx            = block_idx_to_zone_idx(cacheblock->cache_block_addr);
    zone_block_offset   = cacheblock->cache_block_addr - zone_idx_to_block_idx(zone_idx);

    if (zone_block_offset + 1 >= blocks_per_zone) {

        // This was the last write operation for the zone, so we shall mark it as full
        // and remove it from the active zones list
        DBUG("Marking zone ", zone_idx, " as full. and removin it from active zone list\n");
        bm_full_zones->set_bit(zone_idx);
        active_zones_remove(zone_idx);
    }
}

void dmcache::prep_for_write(cacheblock *block, bool mark_old_gcable, bool is_gc_active, bool cache_hit)
{
    uint64_t old_block_addr = block->cache_block_addr;
    block->cache_block_addr = zone_idx_to_block_idx(current_zones[current_write_head]);
    // TODO add assert to check if the block if and after GC is the same

    // Update lru_state book keeping - when this function is called from either cache_hit
    // or cache_miss, the state of the block is guarunteed to be after the threshold. For
    // relocation, it may be before or after. Hence, the counter corresponding to whatever
    // the existing lru_state of the block is incremented to account for both cases.
    if (block->lru_state == cacheblock::before_thresh)
        gc->per_zone_stats[current_zones[current_write_head]]->num_before++;

    // Update ts_score book keeping - we can get away with doing this here because we
    // know which zone this block is being appended to, and also because the timestamp
    // should have been updated BEFORE calling this function
    const auto [do_contribute, blk_ts_shift] = should_contribute_ts_score_bk(block);
    uint64_t old_ts_score = gc->per_zone_stats[current_zones[current_write_head]]->ts_score;

    if (do_contribute)
        block->ts_score = limit_blk_ts_score_bk(block) >> blk_ts_shift;

    else
        block->ts_score = 0;

    gc->per_zone_stats[current_zones[current_write_head]]->ts_score += block->ts_score;
    assert(old_ts_score <= gc->per_zone_stats[current_zones[current_write_head]]->ts_score);

    //zone_batch_size++;
    //if (zone_batch_size >= WRITE_HEADS_BLOCK_SIZE) 
    {

        /* This is a time to select a new zone */
        current_write_head++;

        if (current_write_head >= NUM_WRITE_HEADS) { 
            /* We have passed through the whole array. Start over and increment wp_block*/

            wp_block += WRITE_HEADS_BLOCK_SIZE;
            current_write_head = 0;

            if (wp_block == blocks_per_zone) {
                
                DBUG("Curent Zone:", current_zones[0], " becomes full after this write. cache_block_addr", block->cache_block_addr, "\n");
                DBUG("Select a new zone & reset write heads\n");
                reset_zbd_write_heads();
                wp_block = 0;
// GC is now activated by gc_mark_garbage, since it is based on garbage amount
// #if GC_ENABLED 
//                 DBUG("GC Zone Metadata update In-progress. GC May/MayNot be triggered.\n");
//                 if (!is_gc_active) 
//                     gc->gc_zone_metadata_updated(this);

//                 DBUG("GC Zone Metadata update Complete. cache_block_addr", block->cache_block_addr, "\n");          
// #endif
            }
        }

        //zone_batch_size = 0;
    }

#if GC_ENABLED

    gc->pre_gc_allocates        = stats.allocates;
    gc->pre_gc_garbage_count    = gc->gc_garbage_count;
    gc->num_evicted_periodical  = 0;
    gc->num_freed_periodical    = 0;

    if (mark_old_gcable)
        gc->gc_mark_garbage(this, old_block_addr);

    gc->post_gc_allocates       = stats.allocates;
    gc->post_gc_garbage_count   = gc->gc_garbage_count;

    if (gc->gc_trigger_mode == FREE_SPACE_TRIGGER && !is_gc_active && gc->should_start_gc(this))
        gc->do_gc(this);

#endif
}

uint64_t dmcache::select_new_empty_zone() {

    /* bool denotes if it found one or not, int specifies where */
    std::pair<uint32_t, bool> ret = bm_full_zones->find_first_zero_bit();

    // In the real dm-cache code, this loop checks active zones. But we can check
    // current zones instead since we are in the simulator and everything is instant

#if DEBUG_MODE
    active_zones_print();
#endif

    DBUG("Selecting new empty zone. ret.first", ret.first, " ret.second", ret.second, "current_zone\n");

    while(ret.second && ret.first < nr_zones && 
            active_zones_contains(ret.first)) { 
            //std::find(std::begin(current_zones), std::end(current_zones), ret.first) != std::end(current_zones)) {
            ret = bm_full_zones->find_next_zero_bit(ret.first + 1);
            DBUG("Again !! Selecting new empty zone. ret.first:", ret.first, " ret.second:", ret.second, "\n");
    }

    /* Maybe? TODO: Some more active zones limit checks occur here, don't know if emulating
     *      this is necessary or not */

    // No new empty zone was found. This is a problem
    assert(ret.second);

    return ret.first;
}

void dmcache::handle_blk_ta_complete(io_info &io) {
    /* Fetch the data stored during the queue event */
    const incomplete_io_meta &meta  = incomplete_io[io];
    cacheblock *const block         = meta.block_ptr;

    /* It is not necessary to track every single IO. If we aren't keeping track, just
     *      break out of this function without doing anything else */
    if (!incomplete_io.erase(io))
        return;

    switch (meta.op_type) {

        case incomplete_io_meta::OP_READ_HIT: {
            /* TODO */
        } break;

        case incomplete_io_meta::OP_WRITE_HIT: {
            if (cache_dev_is_zoned) {
                io_info internal_io = io;
                internal_io.sector = block_idx_to_sector_addr(block->cache_block_addr);

                /* Update simulated zbd metadata, and update cacheblock_addr */
                cache_device->write(internal_io);
                block->cache_block_addr = sector_addr_to_block_idx(internal_io.sector);

#if GC_ENABLED
                gc->gc_cacheblock_metadata_updated(this, block);
#endif /* GC_ENABLED*/

                cacheblock_update_state(block);
                if (cache_dev_is_zoned) 
                    finish_zone_append(block);
            }

            if (op_logging_enabled) 
                log_write_hit(io.sector,
                    meta.lru_index,
                    block_idx_to_sector_addr(meta.sector2),
                    block_idx_to_sector_addr(block->cache_block_addr),
                    meta.counter);

        } break;

        case incomplete_io_meta::OP_READ_MISS:
        case incomplete_io_meta::OP_WRITE_MISS: {

            if (cache_dev_is_zoned) {

                io_info internal_io = io;
                internal_io.sector = block_idx_to_sector_addr(block->cache_block_addr);

                /* Update simulated zbd metadata */
                cache_device->write(internal_io);
                block->cache_block_addr = sector_addr_to_block_idx(internal_io.sector);

#if GC_ENABLED
                gc->gc_cacheblock_metadata_updated(this, block);
#endif /* GC_ENABLED*/

                cacheblock_update_state(block);
                if (cache_dev_is_zoned) 
                    finish_zone_append(block);

            }

            if (op_logging_enabled) {

                if (io.is_read) { /* Read miss */
                    log_read_miss(io.sector,
                        block_idx_to_sector_addr(block->cache_block_addr),
                        meta.sector2,
                        meta.counter);

                } else { /* Write miss */
                    log_write_miss(io.sector,
                        block_idx_to_sector_addr(block->cache_block_addr),
                        meta.sector2,
                        meta.counter);
                }
            }

        } break;
    }

    if (stats_logging_enabled)
        log_stats();
}

void dmcache::update_wss_rwss(uint64_t offset) {

    using s = cacheblock::lru_states;

	// Check if the offset exists in wss.	
	if (wss.find(offset) == wss.end()) {
        wss.insert(offset);
        
        if(gc->gc_relocation_mode == RELOCATE_ON_DYNAMIC_RWSS) {
            // Track the unique access in the window 
            if(wss_t.find(offset) == wss_t.end())
                wss_t.insert(offset);
        }
    }
    else {
        if (rwss.find(offset) == rwss.end())
            rwss.insert(offset);

        if(gc->gc_relocation_mode == RELOCATE_ON_DYNAMIC_RWSS) {
            // As the rwws_t is cleared every window, we need to update the rwss_t
            if (rwss_t.find(offset) == rwss_t.end())
                rwss_t.insert(offset);
        }
    }

#if GC_ENABLED
    if(gc->gc_relocation_mode == RELOCATE_ON_DYNAMIC_RWSS) {

        if (get_timestamp() % gc->rwsst_window_interval == 0) {
            window_number++;

            gc->window_intr_idx++;
            double hit_rate = (double)stats.cache_hits / (double)(stats.cache_hits + stats.cache_misses);
            double waf      = (((stats.cache_hits + stats.cache_misses) - stats.read_hits) + gc->relocated_cacheblocks_count) / (double)stats.writes;

            if(victim_stats_logging_enabled)
                log_rwss_t(rwss.size(), gc->relocation_lru_thresh, hit_rate, waf);

            ENSURE_LRU_STATES();

            // assert(evicted_t.size() <= rwss_t.size());
            // uint64_t new_relocation_lru_thresh = gc->rwss_percentage * (rwss_t.size() - evicted_t.size());
            uint64_t new_relocation_lru_thresh = gc->rwss_percentage * rwss_t.size();

#if USE_GROUND_TRUTH_RWSS_T
            if ((gc->window_intr_idx - 1) >= gc->baseline_rwss_t_data[TARGET_DEVICE].rwss_t.size()) 
                new_relocation_lru_thresh = gc->rwss_percentage * gc->baseline_rwss_t_data[TARGET_DEVICE].rwss_t.back();
            else 
                new_relocation_lru_thresh = gc->rwss_percentage * gc->baseline_rwss_t_data[TARGET_DEVICE].rwss_t[(gc->window_intr_idx - 1)];
#endif
            //std::cout << gc->window_intr_idx << "- Window [" << get_timestamp() << ":" << get_timestamp() + gc->rwsst_window_interval << "]" << new_relocation_lru_thresh << std::endl;

            if (max_lru_thresh_enabled && new_relocation_lru_thresh >= stats.allocates) {
                new_relocation_lru_thresh = stats.allocates * max_lru_thresh_percent;
                lru_thresh_readjustments += 1;
            }
            move_lru_thresh(new_relocation_lru_thresh);

            rwss_t_threshold_vec.push_back(gc->relocation_lru_thresh);
            evicted_t.clear();
            rwss_t.clear();
            wss_t.clear(); // Also clear up the wss_t

            ENSURE_LRU_STATES();
        }
    }
#endif /* GC_ENABLED */
}

bool dmcache::handle_blk_ta_queue(io_info &io)
{
    bool ret = false, is_sequential = false;

    uint64_t offset = io.sector & block_mask;
    uint64_t key    = (io.sector - offset) >> block_shift;

    FETCH_INC(timestamp_counter);
    update_wss_rwss(key);
    if ( (cache_admission_enabled && cache_admission == &dmcache::cache_admission_sseqr)
            || (sequential_flagging) ) {
        is_sequential = update_sseqr_window(io);
        if (is_sequential) ++seq_io_stats.total_flagged;
    }

    if (io.is_read) {
        stats.reads++;
    } else {
        stats.writes++;
    }

    /* Per cacheblock statistics for total accesses */
    auto& b_stats = block_stats[key];
    b_stats.accesses++;

    /* Cache lookup */
    auto it = map.find(key);
    if (it != map.end()) {

        cache_hit(it->second, io, is_sequential);
        ret = true;

    } else {

        bool allow_admission = true;

        if (cache_admission_enabled)
            allow_admission = (this->*cache_admission)(io);

        /* We do not know which cacheblock is going to be used to handle this
         * cache miss, so we need to pass "is_sequential" along so that it can
         * be updated once it finds a cacheblock */
        if (allow_admission)
            cache_miss(io, is_sequential);

        else
            ap_log_io(io.sector, 0, is_sequential);

        ret = allow_admission;
    }

    return ret;
}

std::pair<bool, staged_block *>dmcache::request_staging_area() {
    
    bool evicted = false;
    
    list_container<staged_block> *ret = staging_lru.pop_front();
    staging_lru.push_back(ret);

    if (staging_stats.allocates < staging_area_size) {
        staging_stats.allocates++;
    } else {
        evicted = true;
    }

    return { evicted, ret->data };
}


std::pair<bool, cacheblock *>dmcache::request_cache_block()
{
    list_container<cacheblock> *ret = lru.get_head();
    bool evicted = false;

#if GC_ENABLED
    advance_lru_thresh(ret);
#endif

    lru.pop_front();
    lru.push_back(ret);

    if (stats.allocates < cache_size) {
        stats.allocates++;
        ret->data->freq_of_access++;
    }

    else {

        if (is_state(ret->data->state, DIRTY)) {
            clear_state(ret->data->state, DIRTY);
            stats.dirty_blocks--;
            stats.write_backs++;
        }

        if (ret->data->get_relocated()) {
            if (!ret->data->get_relocation_read_hit()) {
                gc->relocations_no_read_hit++;
                if (!ret->data->get_relocation_write_hit()) {
                    gc->relocations_no_read_write_hit++;
                }
            }

            if (!ret->data->get_relocation_write_hit())
                gc->relocations_no_write_hit++;

            gc->previously_relocated_blocks_evicted_via_lru.insert(ret->data->src_block_addr);
        }

        /* Update lru_state and ts_score book keeping */
        uint64_t zone_idx = block_idx_to_zone_idx(ret->data->cache_block_addr);

        if (ret->data->lru_state == cacheblock::before_thresh)
            gc->per_zone_stats[zone_idx]->num_before--;

        /* Decrease ts_score of zone by block's previous contribution */
        assert(gc->per_zone_stats[zone_idx]->ts_score >= ret->data->ts_score);
        gc->per_zone_stats[zone_idx]->ts_score -= ret->data->ts_score;

        ret->data->freq_of_access = 0;
        ret->data->freq_of_access++;
        evicted = true;
    }

    ret->data->is_reaccessed = false;
    return { evicted, ret->data };
}

void dmcache::update_zone_timestamp_stats(struct cacheblock *block) {
    uint64_t zone_idx = block_idx_to_zone_idx(block->cache_block_addr);

    if (block->last_access >= gc->per_zone_stats[zone_idx]->max_timestamp)
        gc->per_zone_stats[zone_idx]->max_timestamp = block->last_access;

    if (block->last_access <= gc->per_zone_stats[zone_idx]->min_timestamp)
        gc->per_zone_stats[zone_idx]->min_timestamp = block->last_access;
}

void dmcache::cache_hit(struct cacheblock *block, io_info &io, bool is_sequential)
{
    uint32_t offset = (uint32_t)(io.sector & block_mask);
    uint64_t lru_index = 0, old_cache = 0; /* For logging */
    RT_START(cache_hit);

    uint64_t prev_ts = block->last_access;
    block->last_access = get_timestamp();

    /* We keep track of the sseqr scan this IO contributes to here, as it
     * is easiest to do it here. The sseqr_range list is an LRU so we can
     * get the scan by accessing the most recent element */
    if (sseqr_ranges.get_size() > 0)
        block->scan = sseqr_ranges.get_tail()->data;
    else block->scan = nullptr;

    /* Note, is_sequential and is_reaccess are updated here */
    update_ts_score_bk(block, prev_ts, is_sequential, io.is_read);

#if GC_ENABLED
    update_zone_timestamp_stats(block);
    advance_lru_thresh(&block->list);
#endif

    /* This is slow, so only do it if logging is enabled */
    if (op_logging_enabled)
        lru_index = lru.index_of(&block->list);
       
    /* Update LRU */
    lru.remove(&block->list);
    lru.push_back(&block->list);

    if (io.is_read) {   /* Read hit */

        if (cache_dev_is_zoned) {
            io_info internal_io = io;
            internal_io.sector  = block_idx_to_sector_addr(block->cache_block_addr) + offset;
            cache_device->read(internal_io);
        }

        if (op_logging_enabled) 
            log_read_hit(io.sector, lru_index,
                         block_idx_to_sector_addr(block->cache_block_addr), get_timestamp());

        stats.read_hits++;
        block->freq_of_access++;
    } else { /* Write hit */
        if (cache_dev_is_zoned) {
            /* Save this for logging purposes */
            old_cache = block->cache_block_addr;

            /* Update dmcache's metadata, it will always invalidate the old block */
            prep_for_write(block, true, false, true);

            /* This is an incomplete IO. Associate the information in the io_info struct with the cacheblock
             *      so that when the complete event is recieved everything can be updated appropriately */
            incomplete_io[io] = {
                .op_type   = incomplete_io_meta::OP_WRITE_HIT ,
                .sector1   = io.sector                    ,
                .sector2   = old_cache                    ,
                .lru_index = lru_index                    ,
                .counter   = get_timestamp()              ,
                .block_ptr = block                        ,
            };
        } 

        /* For regular cache device only, just for logging */
        else if (op_logging_enabled) 
                    log_write_hit( io.sector, lru_index,
                                    block_idx_to_sector_addr(old_cache),
                                    block_idx_to_sector_addr(block->cache_block_addr),
                                    get_timestamp());

        stats.write_hits++;
    }

    #if GC_ENABLED
        if (block->get_relocated()) {
            auto& other_b_stats = block_stats[block->src_block_addr];
            other_b_stats.post_relocation_accesses++;
            auto& b_stats = relocated_block_stats[block->src_block_addr];
            if (block->get_can_count_unique_hit()) {
                gc->unique_relocation_hits_count++;
                block->set_can_count_unique_hit(false);
            }
            if (io.is_read) {
                block->set_relocation_read_hit(true);
                b_stats.relocation_read_hits++;
                gc->relocation_read_hits_count++;
            } else {
                block->set_relocation_write_hit(true);
                b_stats.relocation_write_hits++;
                gc->relocation_write_hits_count++;
            }
            gc->relocation_hits_count++;
        }
    #endif

    stats.cache_hits++;

    ap_log_io(io.sector, block->ts_score, is_sequential);
    RT_STOP(cache_hit);
}

void dmcache::cache_miss(io_info &io, bool is_sequential)
{
    uint64_t offset = io.sector & block_mask;
    uint64_t key = (io.sector - offset) >> block_shift;
    RT_START(cache_miss);

    /* Either take an unused cacheblock or evict one. Also note that
     * is_reaccess is updated here */
    std::pair<bool, cacheblock *> req = request_cache_block();
    req.second->last_access = get_timestamp();

    cacheblock *block = req.second;
    block->is_sequential = is_sequential;

    /* We keep track of the sseqr scan this IO contributes to here, as it
     * is easiest to do it here. The sseqr_range list is an LRU so we can
     * get the scan by accessing the most recent element */
    if (sseqr_ranges.get_size() > 0)
        block->scan = sseqr_ranges.get_tail()->data;
    else block->scan = nullptr;

    #if GC_ENABLED
        update_zone_timestamp_stats(block);

        // Maintain this crazy stat
        if (gc->previously_relocated_blocks_evicted_via_lru.find(key) != 
            gc->previously_relocated_blocks_evicted_via_lru.end()) 
        {
            gc->relocation_misses_after_lru_eviction++;
            gc->previously_relocated_blocks_evicted_via_lru.erase(key);

            auto& b_stats = relocated_block_stats[key];
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
    uint64_t garbage_sector = (req.first) ?
        block_idx_to_sector_addr(block->cache_block_addr) : -1;

    /* Cache invalidation - only remove it from the cache if it was evicted and not if the
     *      cacheblock was taken from the empty list */
    if (req.first) {
        ++stats.replaces;
        map.erase(block->src_block_addr);
    }
    else ++stats.inserts;

    /* Cache insertion */
    block->src_block_addr = key;
    map[key] = block;

    if (cache_dev_is_zoned)
        prep_for_write(block, req.first, false, false);

    /* This is an incomplete IO. Associate the information in the io_info struct with the cacheblock
     *  so that when the complete event is recieved everything can be updated appropriately */
    incomplete_io[io] = {
        .op_type   = io.is_read ? incomplete_io_meta::OP_READ_MISS : incomplete_io_meta::OP_WRITE_MISS ,
        .sector1   = io.sector                    ,
        .sector2   = garbage_sector               ,
        .counter   = get_timestamp() ,
        .block_ptr = block                        ,
    };

    if (io.is_read)
        ++stats.read_misses;
    else ++stats.write_misses;

    ++stats.cache_misses;

    ap_log_io(io.sector, block->ts_score, is_sequential);
    RT_STOP(cache_miss);
}

void dmcache::run() {

    std::cout << "++++++++++ Starting DMZCache Userspace Simulator +++++++++" << std::endl;

    std::tuple<bool, io_info, blktrace_act> next_trace;

    /* Use the base device size to generate the dmsetup I/Os */
    uint64_t dmsetup_ios[261];
    init_dmsetup_ios(dmsetup_ios, base_dev_size);

    /* NOTE: Enabling this might overcomplicate things */
    if (!emulate_dmsetup) goto skip_dmsetup;

    /* dmsetup create emulation - if the cache device is read, these
     *      sectors appear to be read then re-read */
    for (int i = 0; i < (cache_dev_is_zoned ? 1 : 2); i++) {
        for (int j = 0; j < 261; j++) {
            io_info io = {
                .size_sectors = block_size     ,
                .sector       = dmsetup_ios[j] ,
                .is_read      = true           ,
            };
            handle_blk_ta_queue(io);
        }
    }

skip_dmsetup:

    /* Now process the actual traces */
    if (use_fio_traces) {

#ifndef STREAMLINED_TRACE_INPUT

        size_t tupleLength = fio_parser->get_total_lines();
        std::tuple<bool, io_info> trace;
        trace = fio_parser->get();

        auto TQDM = tq::trange(tupleLength);
        TQDM.set_prefix("Processing FIO Trace:");
        uint64_t count = 1, processed_ios = 0;
        for (int a : TQDM){

            if (std::get<0>(trace)) {
                io_info &io = std::get<1>(trace);

                /* Cache Admission (CloudCache)
                ADDRESS STAGING:
                    - Use a small portion of the main memory as the staging area for the referenced address.  
                    - A block is admitted into the cache only after it has been accessed N times, 
                        no matter whether they are read or writes.
                    - As per CloudCache the optimal value of N is 1 or 2.
                    - The size of the staging area is bounded and when it gets full the staged address are evicted using LRU. 
                */

                // We will run the completion only if the queue actually processed
                if (count >= skip)
                    if (handle_blk_ta_queue(io)) {
                        handle_blk_ta_complete(io);
                        ++processed_ios;
                    }

                trace = fio_parser->get();
                TQDM << count << "/" << tupleLength;
                count++;

                if (processed_ios >= limit)
                    break;
            }
        }
        std::cout << std::endl;

#else

        std::tuple<bool, io_info> trace;
        trace = fio_parser->get();

        while (std::get<0>(trace)) {
            io_info &io = std::get<1>(trace);
            if (handle_blk_ta_queue(io))
                handle_blk_ta_complete(io);
	    // log_gc_count_vs_io_count();
            trace = fio_parser->get();
            //count++; //I dont think we need this. 
        }

#endif

        /*
        while (std::get<0>(trace)) {
            io_info &io = std::get<1>(trace);

            //std::cout << io.sector << std::endl;
            handle_blk_ta_queue(io);
            handle_blk_ta_complete(io);

            trace = fio_parser->get();
        }*/
    } else {

        next_trace = parser->get(block_size);
        while (std::get<0>(next_trace)) {

            io_info &io = std::get<1>(next_trace);
            assert(io.size_sectors == 8);

            switch (std::get<2>(next_trace)) {

                case __BLK_TA_QUEUE:
                    handle_blk_ta_queue(io);
                    break;

                case __BLK_TA_COMPLETE:
                    handle_blk_ta_complete(io);
                    break;

                default:
                    assert(false);

            }

            next_trace = parser->get(block_size);
        }
    }
}
