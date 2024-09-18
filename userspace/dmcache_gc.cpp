#include <algorithm>
#include <optional>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <random>
#include <string>
#include <tuple>
#include "dmcache.hpp"
#include "dmcache_gc.hpp"
#include "rt_analysis.hpp"

#if GC_ENABLED
dmcache *dmc;

void dmcacheGC::log_gc_stats(uint64_t start_garbage, uint64_t end_garbage, uint64_t reclaims) {
    std::string filename = "gc_stuff.log";

    static std::ofstream logger_file(filename, std::ios::app);
    logger_file << gc_iter_count << "\t"
        << start_garbage << "\t"
        << end_garbage << "\t"
        << reclaims << "\n";
    logger_file.flush();
}

std::vector<std::string> split(const std::string &str, char delim) {
    std::vector<std::string> tokens;
    size_t start = 0, end = 0;
    while ((end = str.find(delim, start)) != std::string::npos) {
        tokens.push_back(str.substr(start, end - start));
        start = end + 1;
    }
    tokens.push_back(str.substr(start));
    return tokens;
}

#if 0
/* For arguments like relocation mode where the value of the argument contains two values
 *      seperated by a delimiter. Example: on_ts_recency:5 */
std::pair<std::string, std::optional<std::string>> parse_argument(const std::string &arg, const char delim) {

    std::string::size_type delim_pos = arg.find_first_of(delim);

    /* No second value found */
    if (std::string::npos == delim_pos)
        return { arg, { } };

    std::string second = arg.substr(delim_pos + 1, arg.size());
    std::string first = arg.substr(0, delim_pos);

    return { first, second };
}
#endif

std::pair<std::string, std::vector<std::string>> parse_argument(const std::string &arg, char delim) {
    std::vector<std::string> parts = split(arg, delim);
    if (parts.empty()) {
        return { "", {} };
    } else if (parts.size() == 1) {
        return { parts[0], {} };
    } else {
        return { parts[0], {parts.begin() + 1, parts.end()} };
    }
}

dmcacheGC::dmcacheGC(dmcache *dmcache, const std::unordered_map<std::string, std::string> &arg_map) {   

    dmc                          = dmcache;
    gc_free_zones_target         = 0;
    gc_garbage_count             = 0;
    uint64_t gc_end_zones_target = 0;
    gc_iter_count                = 0;
    is_gc_active                 = false;
    gc_relocation_mode           = GC_RELOCATION_MODE;

#if USE_GROUND_TRUTH_RWSS_T
    init_device_ground_truth_threshold();
#endif

    /* Used to calculate actual index once we know the cache size */
    double   arg_relocation_thresh = RELOCATION_LRU_THRESHOLD;
    uint64_t arg_window_interval   = RWSS_WINDOW_INTERVAL;

    // Reading GC block selection policy from either cmd args or default macro
    if (arg_map.contains("gc_policy")) {
        const std::string &arg = arg_map.at("gc_policy");
        auto parsed = parse_argument(arg, ':');

        if (arg == "relocation")
            gc_mode = RELOCATION_GC;

        else if (parsed.first == "eviction") {
            gc_mode = EVICTION_GC;

            if (!parsed.second.empty()) {
                const std::string &s = parsed.second.front();
                char *end;
                arg_relocation_thresh = std::stod(s.c_str());

                if (arg_relocation_thresh < 1.0)
                    relocation_lru_thresh = (arg_relocation_thresh * dmc->cache_size);
                else
                    relocation_lru_thresh = (dmc->cache_size - 1);
            }
        }

        else
            assert (false);
    }
    else gc_mode = GC_MODE;

    // Reading GC triggering mode from either cmdline args or default macro
    if (arg_map.contains("gc_trigger")) {
        const std::string &arg = arg_map.at("gc_trigger");

        if (arg == "garbage")
            gc_trigger_mode = GARBAGE_TRIGGER;

        else if (arg == "free_space")
            gc_trigger_mode = FREE_SPACE_TRIGGER;

        else
            assert (false);
    }
    else
        gc_trigger_mode = GC_TRIGGER_MODE;

    // Reading GC zone selection policy from either cmd args or default macro
    if (arg_map.contains("vs_policy")) {
        const std::string &arg = arg_map.at("vs_policy");

        if (arg == "gc_count")
            gc_victim_sel_mode = GREEDY_SELECTION;

        else if (arg == "ts_score")
            gc_victim_sel_mode = TS_RECENCY_SELECTION;

        else if (arg == "ts_seq_score")
            gc_victim_sel_mode = TS_SEQUENTIAL_SELECTION;

        else if (arg == "lru_score")
            gc_victim_sel_mode = LRU_RECENCY_SELECTION;

        else
            assert(false);
    }
    else gc_victim_sel_mode = VICTIM_SELECTION_POLICY;

    // Reading relocation mode from either cmd args or default macro
    if (gc_mode == RELOCATION_GC && arg_map.contains("relocation_mode")) {

        const std::string &arg = arg_map.at("relocation_mode");
        auto parsed = parse_argument(arg, ':');
        char *end;

        if (parsed.first == "everything") {
            gc_relocation_mode = RELOCATE_EVERYTHING;

            if (!parsed.second.empty()) {
                const std::string &timestamp_str = parsed.second.front();
                char* end;
                arg_relocation_thresh = std::stod(timestamp_str.c_str());
            }
        }

        else if (parsed.first == "hit_count") {
            gc_relocation_mode = RELOCATE_ON_HIT_COUNT;
        }

        else if (parsed.first == "hit_ratio") {
            gc_relocation_mode = RELOCATE_ON_HIT_RATIO;
        }
        else if (parsed.first == "ts_recency") {
            if (!parsed.second.empty()) {
                //relocation_timestamp_thresh = std::strtoull(parsed.second.value().c_str(), &end, 10);
                const std::string &timestamp_str = parsed.second.front();
                char* end;
                relocation_timestamp_thresh = std::strtoull(timestamp_str.c_str(), &end, 10);
                gc_relocation_mode          = RELOCATE_ON_TS_RECENCY;
            }
        }
        else if (parsed.first == "lru_recency") {
            if (!parsed.second.empty()) {
                /* Note: This is saved as a double for the time being. The double will be used along with the
                 *      cache size (once it's calculated) to determine the actual index of the lru threshold */
                const std::string &timestamp_str = parsed.second.front();
                char* end;
                arg_relocation_thresh   = std::stod(timestamp_str.c_str());
                gc_relocation_mode      = RELOCATE_ON_LRU_RECENCY;
            }
        }
        else if (parsed.first == "rwss") {
            //rwss:<RWSS of the workload>:<rwss_percentage>
            if (!parsed.second.empty()) {
                const std::string &timestamp_str = parsed.second.front();
                char* end;
                arg_relocation_thresh  = std::strtoull(timestamp_str.c_str(), &end, 10);

                if (parsed.second.size() >= 2) {
                    const std::string& rwss_per = parsed.second[1];
                    rwss_percentage = std::stod(rwss_per.c_str());
                }
                gc_relocation_mode = RELOCATE_ON_STATIC_RWSS;
            }
        }
        else if (parsed.first == "rwss_t") {
            //rwss_t:<RWSS of the workload>:<rwss_percentage>
            if (!parsed.second.empty()) {
                const std::string &timestamp_str = parsed.second.front();
                char* end;

                arg_relocation_thresh = std::strtoull(timestamp_str.c_str(), &end, 10);
                if (parsed.second.size() >= 2) {
                    const std::string& rwss_per = parsed.second[1];
                    rwss_percentage = std::stod(rwss_per.c_str());
                }
                
                gc_relocation_mode = RELOCATE_ON_DYNAMIC_RWSS;
            }
        } else
            gc_relocation_mode = GC_RELOCATION_MODE;
    }

    if (gc_mode == RELOCATION_GC && gc_relocation_mode == RELOCATE_ON_DYNAMIC_RWSS && arg_map.contains("window_config")) {
        //window_interval:<RWSS of the workload>:<window_percentage>
        const std::string &arg = arg_map.at("window_config");
        auto parsed            = parse_argument(arg, ':');
        char *end;

        if (parsed.first == "window_interval") {
            if (!parsed.second.empty()) {

                const std::string &timestamp_str = parsed.second.front();
                char* end;
                arg_window_interval   = std::stod(timestamp_str.c_str());
                if (parsed.second.size() >= 2) {
                    const std::string& rwss_per = parsed.second[1];
                    window_percentage = std::stod(rwss_per.c_str());
                }
            }
        }
    }

    if (arg_map.contains("ensure_min_freed")) {
        const std::string &arg = arg_map.at("ensure_min_freed");
        ensure_min_freed_percent = std::stod(arg.c_str());
        ensure_min_freed_enabled = true;
    }

    // Reading GC configuration from either cmd args or default macro
    if (arg_map.contains("gc_configuration")) {
        char *end;
        const std::string &arg = arg_map.at("gc_configuration");

        gc_op_percentage = std::strtoull(arg.c_str(), &end, 10);
        assert (*(end++) == '/');
        
        gc_op_acceptable_free_space_percentage = std::strtoull(end, &end, 10);
        assert (*(end++) == '-');
        
        gc_op_stop_start_gap_percentage = std::strtoull(end, &end, 10);
        assert (*(end++) == '-');
        
        gc_op_start_block_gap_percentage = std::strtoull(end, &end, 10);
        assert(*end == 0);

    }
    else {

        gc_op_percentage                        = GC_OP_PERCENTAGE;
        gc_op_acceptable_free_space_percentage  = GC_OP_ACCEPTABLE_FREE_SPACE_PERCENTAGE;
        gc_op_stop_start_gap_percentage         = GC_OP_STOP_START_GAP_PERCENTAGE;
        gc_op_start_block_gap_percentage        = GC_OP_START_BLOCK_GAP_PERCENTAGE;

    }

    bm_gcable_blocks = new bitmap(dmc->nr_zones * dmc->blocks_per_zone);

    // Max number of cacheblocks we could store, if we used 100% of the cache space
    uint64_t block_capacity = dmc->nr_zones * dmc->blocks_per_zone;
    //std::cout << "block_capacity:" << block_capacity << std::endl;

    // The number of cacheblocks we are reserving for GC operation
    uint64_t op_capacity = (block_capacity * gc_op_percentage + 50) / 100;
    //std::cout << "op_capacity:" << op_capacity << std::endl;

    // Required minimum free space for GC to operate properly
    uint64_t required_free = (BLOCK_IOS_FREE_ZONES * dmc->blocks_per_zone);
    //std::cout << "required_free:" << required_free << std::endl;

    /* If GC_MODE is set to RELOCATION_GC, GC will require atleast one zone for relocation; 
        and other zone is used to write the incoming io. Hence nr_zones should be atleast 2*/
    assert(dmc->nr_zones >= 2);

#if 0
    /*
      - If the OP capacity is less than the number of blocks per zone, then we need to increase the OP capacity
        to at least the number of blocks per zone. This is because GC works at the zone level, and we need to
        be able to store at least one zone in the OP space.
      - But, this does not consider the garbage percentage. If the OP capacity is less than the number of blocks
        per zone, then we need to increase the OP capacity to at least the number of blocks per zone + the garbage
        percentage, such that GC has enough space to reloacte cacheblocks 
    */

    //uint64_t percentage_after_garbage   = 100 - gc_op_acceptable_garbage_percentage;
    uint64_t min_free_space_percentage  = gc_op_acceptable_garbage_percentage;
    uint64_t min_op_capacity            = (dmc->blocks_per_zone * 100 + (min_free_space_percentage - 1)) / min_free_space_percentage;


    if (op_capacity < min_op_capacity) {
        std::cout << "WARNING: Configured OP capacity (" << op_capacity << ") automatically raised to " << min_op_capacity << std::endl;
        op_capacity = min_op_capacity;
    }
#endif

    assert(gc_op_acceptable_free_space_percentage + gc_op_stop_start_gap_percentage + gc_op_start_block_gap_percentage == 100);

    if (op_capacity > required_free)
        op_capacity = op_capacity - required_free;
    else 
        op_capacity = required_free;

    uint64_t min_op_capacity = dmc->blocks_per_zone + ((100 - gc_op_acceptable_free_space_percentage) * dmc->blocks_per_zone) / 100;
    if (op_capacity < min_op_capacity) {
        std::cout << "WARNING: Configured OP capacity (" << op_capacity << ") automatically raised to " << min_op_capacity << std::endl;
        op_capacity = min_op_capacity;
    }

    // The remaining OP space is the same as block io trigger. The acceptable garbage percentage sets
    // the stopping point for GC in reference to this remaining space.
    // gc_trigger_stop = (gc_trigger_block_io * gc_op_acceptable_garbage_percentage + 50) / 100;
    gc_trigger_stop = (op_capacity * gc_op_acceptable_free_space_percentage + 50) / 100;

 #if 0
     // Under test
     if (gc_relocation_mode == RELOCATE_ON_DYNAMIC_RWSS)
         gc_trigger_stop     = std::max(gc_trigger_stop, dmc->blocks_per_zone);
 #endif

    // Calculate the start trigger for GC. It is the stop trigger amount + the gap percentage
    //gc_trigger_start = gc_trigger_stop + (gc_trigger_block_io * gc_op_stop_start_gap_percentage + 50) / 100;
    gc_trigger_start    = gc_trigger_stop + (op_capacity * gc_op_stop_start_gap_percentage + 50) / 100;
    gc_trigger_block_io = op_capacity - gc_trigger_start;

    // Set cache size
    dmc->cache_size = block_capacity - op_capacity;
    assert(op_capacity >= required_free);

    switch(gc_relocation_mode) {

        case RELOCATE_ON_LRU_RECENCY:
        case RELOCATE_EVERYTHING: {
            if (arg_relocation_thresh < 1.0)
                relocation_lru_thresh = (arg_relocation_thresh * dmc->cache_size); 
            else
                relocation_lru_thresh = (dmc->cache_size - 1);
            break;
        }
        case RELOCATE_ON_STATIC_RWSS: {
            // arg_relocation_thresh here denotes the RWSS of the entire workload
            rwss_workload         = arg_relocation_thresh; 
            relocation_lru_thresh = arg_relocation_thresh * rwss_percentage;
            relocation_lru_thresh = std::min(relocation_lru_thresh, dmc->cache_size - 1);
            break;
        }
        case RELOCATE_ON_DYNAMIC_RWSS: {
            rwsst_window_interval = arg_window_interval * window_percentage;

            // The initial value of relocation_lru_thresh is equal to rwsst_window_interval.
            // Here the absolute value of arg_window_interval and arg_relocation_thresh is ideally the same. 
            relocation_lru_thresh = rwsst_window_interval;
            relocation_lru_thresh = std::min(relocation_lru_thresh, dmc->cache_size - 1);
            // relocation_lru_thresh = 0; // - Uncomment to start at zero

            // rwss_t(t) = rwss_percentage * rwss_r(t-1)
            dmc->rwss_t_threshold_vec.push_back(relocation_lru_thresh);
            window_intr_idx       = 1;

#if USE_GROUND_TRUTH_RWSS_T
            relocation_lru_thresh = rwss_percentage * baseline_rwss_t_data[TARGET_DEVICE].rwss_t[window_intr_idx-1];
#endif
            //std::cout << window_intr_idx << "- Window [" << dmc->get_timestamp() << ":" << dmc->get_timestamp() + rwsst_window_interval << "]" << relocation_lru_thresh << std::endl;
            break;
        }
    }

    zbd_cacheblocks = new cacheblock*[dmc->nr_zones * dmc->blocks_per_zone];
    assert(zbd_cacheblocks != NULL);

    // eviction_buffer = static_cast<char*>(std::calloc(dmc->block_size, sizeof(char)));

    /* -------- GC Statistics -------- */
    per_zone_stats = new zone_stats*[dmc->nr_zones]();
    gc_reset_stats(dmc);
}

dmcacheGC::~dmcacheGC() {
    std::string filename = "per_zone_stats.log";
    static std::ofstream logger_file(filename, std::ios::app);

    std::cout << "TOTAL ZONE RESETS: " << total_zone_resets << std::endl;
    std::cout << "TOTAL GARBAGE CREATED: " << gc_total_garbage_count << std::endl;

    for (uint64_t zone_idx = 0; zone_idx < dmc->nr_zones; zone_idx++) {
        logger_file << zone_idx << "\t"
            << per_zone_stats[zone_idx]->reclaim_count << "\t"
            << per_zone_stats[zone_idx]->blocks_relocated << "\t"
            << per_zone_stats[zone_idx]->blocks_flushed << "\n";
    }

    if (dmc->dump_bitmaps) 
        bm_gcable_blocks->dump_bits("/tmp/gcable_blocks_bm.bin");

    // Count all the blocks that are left over and were never evicted
    for (cacheblock& block : dmc->lru) {
        if (block.get_relocated()) {
            if (!block.get_relocation_read_hit()) {
                relocations_no_read_hit++;
                if (!block.get_relocation_write_hit()) {
                    relocations_no_read_write_hit++;
                }
            }

            if (!block.get_relocation_write_hit())
                relocations_no_write_hit++;
        }   
    }

    // Compute average # of accesses for relocated blocks
    uint64_t total_accesses = 0;
    for (uint64_t src_address : all_relocated_blocks)
        total_accesses += dmc->block_stats[src_address].accesses;

    if (!all_relocated_blocks.empty()) {
        dmc->avg_access_stats.avg_num_accesses_relocated_blocks =
            (double)total_accesses / all_relocated_blocks.size();
#ifndef STREAMLINED_TRACE_INPUT
        std::cout << "Average # of accesses (relocated blocks): " 
            << dmc->avg_access_stats.avg_num_accesses_relocated_blocks << std::endl;
#endif
    }
    
    if (dmc->stats_logging_enabled)
        dmc->log_stats();

    for (uint64_t index = 0; index < dmc->nr_zones; index++)
        delete per_zone_stats[index];

    delete[] zbd_cacheblocks;
    delete bm_gcable_blocks;
    delete per_zone_stats;
}

void dmcacheGC::gc_reset_stats(dmcache *dmc){

    uint64_t index;
    for (index = 0; index < dmc->nr_zones; index++) {
        per_zone_stats[index] = new zone_stats; 
        per_zone_stats[index]->reclaim_count    = 0;
        per_zone_stats[index]->max_timestamp    = 0;
        per_zone_stats[index]->min_timestamp    = 0;
        per_zone_stats[index]->blocks_flushed   = 0;
        per_zone_stats[index]->ts_score         = 0;
        per_zone_stats[index]->num_before       = 0;
        per_zone_stats[index]->blocks_relocated = 0;
    }

    relocated_cacheblocks_count         = 0;
    relocated_unique_cacheblocks_count  = 0;
    evicted_cacheblocks_count           = 0;
}

void dmcacheGC::gc_print_stats(dmcache *dmc)
{
    uint64_t index, total_zone_reclaims;
    index = total_zone_reclaims = 0;

    DBUG_GC("dmcache GC Statistics:\n");
    DBUG_GC("Cacheblocks Relocated:", relocated_cacheblocks_count, "\n");
    DBUG_GC("Unique Cacheblocks Relocated:", relocated_unique_cacheblocks_count, "\n");
    DBUG_GC("Zone Reclaims:\n");
    for (index = 0; index < dmc->nr_zones; index++) {
        total_zone_reclaims += per_zone_stats[index]->reclaim_count;
        DBUG_GC("Zone-", index, ":", per_zone_stats[index]->reclaim_count, "\n");
    }
    DBUG_GC("Total Reclaims:", total_zone_reclaims, "\n");
}

uint64_t dmcacheGC::gc_count_free_zones(struct dmcache* dmc) {
    uint64_t nr_free_zones;
    nr_free_zones = dmc->nr_zones - dmc->bm_full_zones->get_weight();
    // Count only compltely empty zones by subtracting active_zones_count
    nr_free_zones -= dmc->active_zones_count;
    return nr_free_zones;
}

// Reclaim space by selecting an optimal zone which will be reset after 
// evicting all cacheblocks contained within
void dmcacheGC::reclaim_zone(dmcache* dmc, uint64_t starting_block_idx) {

    uint64_t block_idx = 0;
    struct cacheblock* victim_cacheblock; 
    RT_START(gc_reclaim);
    uint64_t zone_idx = dmc->block_idx_to_zone_idx(starting_block_idx);

    /* This must be called before restoring the per_zone stats */
    auto additional_evictions = calc_additional_evictions(dmc, zone_idx);
    // Uncomment this for Locality Based Zone selection 
    // auto additional_evictions = calc_additional_valid_blocks_evictions(dmc, zone_idx);

    per_zone_stats[zone_idx]->gcable_count = 0;
    per_zone_stats[zone_idx]->ts_score     = 0;
    per_zone_stats[zone_idx]->num_before   = 0;

    /* Relocate or evict based on the GC policy*/
    switch (gc_mode) {
        case RELOCATION_GC: {
            for (block_idx = starting_block_idx; block_idx < starting_block_idx + dmc->blocks_per_zone; block_idx++) {
                bool relocation_allowed = true;
                if ((victim_cacheblock = gc_fetch_cacheblock(dmc, block_idx))) {

                    switch (gc_relocation_mode) {

                        case RELOCATE_EVERYTHING:
                            break;

                        case RELOCATE_ON_HIT_COUNT: {

                            /*
                                - First, check if the victim_cacheblock is present in the local mapping for relocated blocks.
                                - If present: 
                                    (a) For the current src_blk_addr in the fetched cacheblock, find its global stats from relocation_stats map
                                    (b) Decide based on the global stats (relocation_read_hits) whether to relocate or evict. 
                                - If not present: relocate the cacheblock
                            */
                            
                            auto it = dmc->relocated_map.find(victim_cacheblock->cache_block_addr);
                            if (it != dmc->relocated_map.end()) {

                                auto block_stats = dmc->relocated_block_stats.find(it->second->src_block_addr);
                                if (block_stats != dmc->relocated_block_stats.end() && 
                                    block_stats->second.relocation_read_hits < RELOCATION_FREQ_THRESHOLD)
                                        relocation_allowed = false;
                            }
                            break;
                        }

                        case RELOCATE_ON_HIT_RATIO: {

                            auto it = dmc->relocated_map.find(victim_cacheblock->cache_block_addr);
                            if (it != dmc->relocated_map.end()) {

                                auto block_stats = dmc->relocated_block_stats.find(it->second->src_block_addr);
                                if (block_stats != dmc->relocated_block_stats.end() && 
                                    (double)block_stats->second.relocation_read_hits/block_stats->second.relocation_count < EFFECTIVE_RELOCATION_RATIO) 
                                        relocation_allowed = false;
                            }
                            break;
                        }

                        case RELOCATE_ON_TS_RECENCY: {

                            auto it = dmc->relocated_map.find(victim_cacheblock->cache_block_addr);
                            if (it != dmc->relocated_map.end() && (dmc->get_timestamp() - relocation_timestamp_thresh) > it->second->last_access)
                                relocation_allowed = false;
                            break;
                        }

                        case RELOCATE_ON_STATIC_RWSS:
                        case RELOCATE_ON_DYNAMIC_RWSS:
                        case RELOCATE_ON_LRU_RECENCY: {

                            auto block_lru_state = victim_cacheblock->lru_state;
                            if (cacheblock::lru_states::before_thresh == block_lru_state)
                                relocation_allowed = false;

                            break;
                        }

                        default: assert(false);
                    }

                    if (relocation_allowed && !additional_evictions.contains(block_idx)) {

                        gc_relocate_cacheblock(dmc, victim_cacheblock);
                        
                        num_evicted_periodical++;

                        // Update the total relocation count for the cacheblock
                        relocated_cacheblocks_count++;
                        per_zone_stats[dmc->block_idx_to_zone_idx(starting_block_idx)]->blocks_relocated++;

                        // Update the relocation count for the cacheblock
                        auto& b_stats = dmc->relocated_block_stats[victim_cacheblock->src_block_addr];
                        b_stats.relocation_count++;

                        // Add the relocated cacheblock to the relocated cacheblock list
                        dmc->relocated_map[victim_cacheblock->cache_block_addr] = victim_cacheblock;

                        // Update the unique relocation count for the cacheblock
                        if (b_stats.relocation_count == 1)
                            relocated_unique_cacheblocks_count++;

						/* 
						   - If the victim_cacheblock is a unique block in the last window; then it MUST be saved.
						   - If the victim_cacheblocks is a reused block in the last window; then it MUST be saved.

						   if (block exits in rwss_t)
								no_reused_blocks_t_saved++;
						   else if (block exits in wss_t)
								no_unused_blocks_t_saved++;
						*/

						if (dmc->rwss_t.find(victim_cacheblock->src_block_addr) == dmc->rwss_t.end())
							dmc->rwss_t_stats.no_reused_blocks_t_saved++;
						else if (dmc->wss_t.find(victim_cacheblock->src_block_addr) == dmc->wss_t.end())
							dmc->rwss_t_stats.no_unused_blocks_t_saved++;

                    } else {
                        gc_evict_cacheblock(dmc, block_idx);
                        evicted_cacheblocks_count++;
                        num_evicted_periodical++;

                        per_zone_stats[dmc->block_idx_to_zone_idx(starting_block_idx)]->blocks_flushed++;

                        dmc->relocated_map.erase(victim_cacheblock->cache_block_addr);

						/* 
                            - If the victim_cacheblock is a unique block in the last window; then it MUST be saved.
                            - If the victim_cacheblocks is a reused block in the last window; then it MUST be saved.

                            if (block exits in rwss_t)
                                no_reused_blocks_t_flushed++;
                            else if (block exits in wss_t)
                                no_unused_blocks_t_flushed++;

                        */
                       if (dmc->rwss_t.find(victim_cacheblock->src_block_addr) == dmc->rwss_t.end())
                            dmc->rwss_t_stats.no_reused_blocks_t_flushed++;
                       else if (dmc->wss_t.find(victim_cacheblock->src_block_addr) == dmc->wss_t.end())
                            dmc->rwss_t_stats.no_unused_blocks_t_flushed++;
                    }
                }
            }
            break;
        }
        case EVICTION_GC: {
            for (block_idx = starting_block_idx; block_idx < starting_block_idx + dmc->blocks_per_zone; block_idx++) {
                if (bm_gcable_blocks->test_bit(block_idx))
                    continue;

                gc_evict_cacheblock(dmc, block_idx);
                evicted_cacheblocks_count++;
                num_evicted_periodical++;
            }
            break;
        }
    }
    RT_STOP(gc_reclaim);
}

std::set<uint64_t> dmcacheGC::calc_additional_valid_blocks_evictions(dmcache *dmc, uint64_t zone_idx) const {


    std::set<uint64_t> ret = {  };

    uint64_t amt_freed = 0;
    uint64_t req_freed = (dmc->blocks_per_zone - per_zone_stats[zone_idx]->gcable_count)  * ensure_min_freed_percent;


    if (gc_relocation_mode == RELOCATE_EVERYTHING) {

        for (uint64_t block_idx = 0; block_idx < dmc->blocks_per_zone; block_idx++) {
            uint64_t full_addr = dmc->zone_idx_to_block_idx(zone_idx) + block_idx;
            cacheblock *block = zbd_cacheblocks[full_addr];

            if (block == nullptr)
                continue;
            else amt_freed++;

            ret.insert(full_addr);
            if (amt_freed == req_freed)
                break;
        }
    }
    return ret;
}

std::set<uint64_t> dmcacheGC::calc_additional_evictions(dmcache *dmc, uint64_t zone_idx) const {
    std::set<uint64_t> ret = {  };

    uint64_t amt_freed = 0;
    uint64_t req_freed = dmc->blocks_per_zone * ensure_min_freed_percent;

    if (gc_mode == EVICTION_GC)
	return { };

    if (gc_relocation_mode == RELOCATE_EVERYTHING) {
        amt_freed = per_zone_stats[zone_idx]->gcable_count;
    }
    else if (gc_relocation_mode == RELOCATE_ON_DYNAMIC_RWSS) {
        amt_freed = per_zone_stats[zone_idx]->gcable_count + per_zone_stats[zone_idx]->num_before;
    }

    /* If there is a sufficient amount of blocks freed, we don't need to evict any additional blocks */
    if (!ensure_min_freed_enabled || amt_freed >= req_freed)
        return { };

    if (gc_relocation_mode == RELOCATE_ON_DYNAMIC_RWSS) {
	    std::vector<std::pair<uint64_t, cacheblock *>> blocks_after_thresh = { };
	    blocks_after_thresh.reserve(dmc->blocks_per_zone);

	    for (uint64_t block_idx = 0; block_idx < dmc->blocks_per_zone; block_idx++) {
		uint64_t full_addr = dmc->zone_idx_to_block_idx(zone_idx) + block_idx;
		cacheblock *block = zbd_cacheblocks[full_addr];

		/* We should only consider blocks which are potential relocation candidates */
		if (block != nullptr && block->lru_state == cacheblock::after_thresh)
		    blocks_after_thresh.push_back( { full_addr , block } );
	    }

	    /* We want the smaller ones at the front */
	    std::sort(blocks_after_thresh.begin(), blocks_after_thresh.end(), [](auto a, auto b) {
		return a.second->last_access < b.second->last_access;
	    });

	    for (uint64_t i = 0; i < req_freed - amt_freed; i++) {
		ret.insert(blocks_after_thresh.at(i).first);
	    }
    }
    else if (gc_relocation_mode == RELOCATE_EVERYTHING) {
	uint64_t count = 0;

        for (uint64_t block_idx = 0; block_idx < dmc->blocks_per_zone; block_idx++) {
	    uint64_t full_addr = dmc->zone_idx_to_block_idx(zone_idx) + block_idx;
	    cacheblock *block = zbd_cacheblocks[full_addr];

	    if (block == nullptr)
		continue;
	    else count++;

	    ret.insert(full_addr);
	    if (count >= req_freed - amt_freed)
		break;
	}
    }

    return ret;
}

void dmcacheGC::update_gc_metadata(dmcache* dmc, uint64_t starting_block_idx,
                                    uint64_t gc_zone_idx, uint64_t garbage_count) {
    uint64_t block_idx = 0;

    DBUG_GC("[GC] Resetting zone:", gc_zone_idx, "\n");
    // Clear the zone by resetting the write pointer            
    dmc->cache_device->zones[gc_zone_idx].reset();
    ++total_zone_resets;

    // Update bitmaps so that the zone is now empty
    for (block_idx = starting_block_idx; block_idx < starting_block_idx + dmc->blocks_per_zone; block_idx++) {
        bm_gcable_blocks->clear_bit(block_idx);

        assert(block_idx < dmc->nr_zones*dmc->blocks_per_zone);
        zbd_cacheblocks[block_idx] = NULL;                                                                                               
    }

    // Update the full zone bitmap
    dmc->bm_full_zones->clear_bit(gc_zone_idx);

    // Decrement the gc_garbage_count count based on the amount of garbage we just reclaimed
    gc_garbage_count        -= garbage_count;
    num_freed_periodical    += garbage_count;

    // Update zone statistics
    per_zone_stats[gc_zone_idx]->reclaim_count++;
}

uint64_t dmcacheGC::calc_free_space(dmcache *dmc) {

    // org_cache_cache includes OPS
    const uint64_t total_available_space = dmc->org_cache_size;
    const uint64_t total_valid_blocks    = dmc->stats.allocates;

#if(0)
    uint64_t sum = 0;
    for (size_t i = 0; i < dmc->nr_zones; i++) {
        auto zone = dmc->cache_device->zones.get()[i];
        sum += zone.write_pointer;
    }
    assert(sum == (total_valid_blocks + gc_garbage_count - 1) * dmc->block_size);
#endif

    /* Both garbage and valid blocks consume space on the device */
    return total_available_space - (total_valid_blocks + gc_garbage_count);
}

bool dmcacheGC::should_continue_gc(dmcache *dmc)
{
    if (gc_trigger_mode == GARBAGE_TRIGGER)
        return gc_garbage_count >= gc_trigger_stop;

    else {
        std::uint64_t free_space = calc_free_space(dmc);
        return free_space <= gc_trigger_stop;
    }
}

bool dmcacheGC::should_start_gc(dmcache *dmc)
{
    if (gc_trigger_mode == GARBAGE_TRIGGER)
        return gc_garbage_count >= gc_trigger_start;
    else {
        std::uint64_t free_space = calc_free_space(dmc);
        return free_space <= gc_trigger_start;
    }
}

void dmcacheGC::do_gc(dmcache* dmc)
{
    struct cacheblock* cache; 
    bool first_iter = false;
    uint64_t starting_block_idx, block_idx, zone_idx, garbage_count;
    starting_block_idx = block_idx = zone_idx = garbage_count = 0;
    uint64_t start_garbage = gc_garbage_count, reclaim_count = 0;
    uint64_t found_vz_with_zero_reclaim = 0;

    dmc->ap_log_gc();

    while (should_continue_gc(dmc)) {
        is_gc_active = true;

        switch (gc_victim_sel_mode) {
            case RANDOM_SELECTION:          std::tie(zone_idx, garbage_count) = gc_choose_zone_random(dmc);            break;
            case GREEDY_SELECTION:          std::tie(zone_idx, garbage_count) = gc_choose_zone_greedy(dmc);            break;
            case TS_SCORE_SELECTION:        std::tie(zone_idx, garbage_count) = gc_choose_zone_ts_score_opt(dmc);      break;
            case TS_RECENCY_SELECTION:      std::tie(zone_idx, garbage_count) = gc_choose_zone_ts_score_opt(dmc);      break;
            case TS_SEQUENTIAL_SELECTION:   std::tie(zone_idx, garbage_count) = gc_choose_zone_ts_score_opt(dmc);      break;
            case LOCALITY_SELECTION:        std::tie(zone_idx, garbage_count) = gc_choose_zone_ts_score_tradeoff(dmc); break;
            case LRU_RECENCY_SELECTION:     std::tie(zone_idx, garbage_count) = gc_choose_zone_lru_score(dmc);         break;
            default: assert(false);
        }

        // No full zones available, skip this GC run
        if (zone_idx == (uint64_t)-1) {
            if (first_iter) gc_iter_count++;
            return;
        }
        reclaim_count += 1;

        uint64_t possible_reclaims = garbage_count;
        if (gc_relocation_mode == RELOCATE_ON_DYNAMIC_RWSS) {
            possible_reclaims += per_zone_stats[zone_idx]->num_before;
        }

        uint64_t possible_migration = dmc->blocks_per_zone - possible_reclaims;
        uint64_t current_free_space = calc_free_space(dmc);

        //assert(possible_migration < current_free_space);

        if (possible_reclaims == 0 && gc_mode != EVICTION_GC) {
            found_vz_with_zero_reclaim++;
#if 1
            // Under test
            std::cout << "WARNING: LRU readjust threshold enabled : 0.0025" << std::endl;
            uint64_t dist               = (dmc->zone_cap_sectors / dmc->block_size) * std::stof("0.0025");
            dmc->lru_readjust_dist      = dist;
            dmc->lru_readjust_enabled   = true;
#else
            if (found_vz_with_zero_reclaim == 20) { 
                std::cout<<"PROBLEM !!!!!!!! in GC-IER:"<< gc_iter_count << std::endl;
                exit(1);
            }
#endif

        }

        starting_block_idx = dmc->zone_idx_to_block_idx(zone_idx);

        if(dmc->victim_stats_logging_enabled) 
            dmc->log_victim_zone_stats(starting_block_idx);

        uint64_t old_thresh = relocation_lru_thresh;
        if (dmc->lru_readjust_enabled && possible_reclaims == 0) {

            dmc->readjust_lru_thresh(zone_idx);
            assert(per_zone_stats[zone_idx]->num_before == dmc->lru_readjust_dist);

            reclaim_zone(dmc, starting_block_idx);
            dmc->move_lru_thresh(old_thresh);

            /* Can't hurt, right? */
            assert(old_thresh == relocation_lru_thresh);
        }
        else
            reclaim_zone(dmc, starting_block_idx);

        update_gc_metadata(dmc, starting_block_idx, zone_idx, garbage_count);
        first_iter = true;

    }

    //log_gc_stats(start_garbage, gc_garbage_count, reclaim_count);

    // Resume I/Os now that space has been cleared
    if (block_ios.load(std::memory_order_relaxed)) {
        block_ios.store(false);
        DBUG_GC("BLOCK I/Os disabled!\n");
    }

    if (dmc->stats_logging_enabled)
        dmc->log_cacheblock_stats();

    is_gc_active = false;
    gc_iter_count++;

    if (gc_mode)
        gc_print_stats(dmc);

}
 
void dmcacheGC::gc_evict_cacheblock(dmcache *dmc, uint64_t block_idx) {
    uint64_t cacheblock_src_addr=0, cacheblock_cache_addr=0;
    bool cacheblock_is_dirty = false;
    struct cacheblock* cache;

    cache = zbd_cacheblocks[block_idx];
    assert(cache);

    if (cache->cache_block_addr == block_idx) {
        cacheblock_is_dirty     = is_state(cache->state, DIRTY);
        cacheblock_src_addr     = cache->src_block_addr;
        cacheblock_cache_addr   = cache->cache_block_addr;

        dmc->ap_log_flush(gc_iter_count, dmc->block_idx_to_sector_addr(cache->src_block_addr));

        // Keep track of evicted blocks in rwss_t
        if (gc_relocation_mode == RELOCATE_ON_DYNAMIC_RWSS && dmc->rwss_t.contains(cache->src_block_addr))
            dmc->evicted_t.insert(cache->src_block_addr);

        //cache invalidate
        dmc->map.erase(cache->src_block_addr);
        
        // Keep track of evicted blocks for the evicted_misses counter
        auto pair = evicted_blocks.insert(cache->src_block_addr);
        assert(pair.second);

        cache->state = INVALID;

        /* Update LRU */
        dmc->retreat_lru_thresh(&cache->list);
        dmc->lru.remove(&cache->list);
        dmc->lru.add_front(&cache->list);

        cache->is_reaccessed = false;
        cache->freq_of_access = 0;
        dmc->stats.allocates--;

        if (dmc->stats_logging_enabled)
            dmc->log_stats();
    }

    /* In the original dmcache kernel module. First the cacheblock is read into the memory.
     * Then from the memory, the block is written to the source device. Also, we need to maintain dirty blocks counter. 
     * We do not need to clear the DIRTY flag because cache_invalidate() call above clears it for us.
     */ 
    if (cacheblock_is_dirty) {
        --(dmc->stats.dirty_blocks);
        dmc->stats.write_backs++;
    }
}

// Fetches a cacheblock for GC. The returned cacheblock will be in the RESERVED state, owned by GC
// Returns NULL if the desired cacheblock is already RESERVED and does not need to be relocated by GC
struct cacheblock* dmcacheGC::gc_fetch_cacheblock(dmcache *dmc, uint64_t block_idx) {

    struct cacheblock* cache;

    if (bm_gcable_blocks->test_bit(block_idx))
        return NULL;

    assert(block_idx <= dmc->nr_zones*dmc->blocks_per_zone);

    cache = zbd_cacheblocks[block_idx];
    assert(cache != NULL);

    // "obtain" the cacheblock by marking it as RESERVED, as long as it is not already RESERVED
    // If the cacheblock was already RESERVED, we should not mess with it. Other code is already moving it
    //spin_lock(&cache->lock);
    //if (!is_state(cache->state, RESERVED)) 
    {

        //set_state(cache->state, RESERVED);
        //spin_unlock(&cache->lock);
        //DBUG("[GC] cache_block_addr:", cache->cache_block_addr, " Expected:", block_idx, "\n");

        if (cache->cache_block_addr != block_idx) {
            DBUG_GC("ERROR!! Expected Address:", block_idx, " Actual Address:", cache->cache_block_addr, "\n");
            exit(1);
        }
        return cache;
    }
    //spin_unlock(&cache->lock);

    return NULL;
}

int dmcacheGC::do_relocation_io(dmcache *dmc, struct relocation_job* job) {

    if(!job->is_read)
        dmc->prep_for_write(job->cacheblock, false, true, false);

    // There is actually no need to simulate the read IO; being a simulator. We can directly call the write IO. 
    job->bi_sector = dmc->block_idx_to_sector_addr(job->cacheblock->cache_block_addr);
    relocation_endio(dmc, job);

    return 0;
}

void dmcacheGC::relocation_endio(dmcache *dmc, struct relocation_job* job) {

    if (job->is_read) {
        // READ is finished, requeue for ZONE_APPEND now
        job->is_read = false;
        do_relocation_io(dmc, job);
    } else {
        // ZONE_APPEND is finished. Store the actual write location in the cacheblock
        do_relocation_complete(dmc, job);
    }
}

int dmcacheGC::do_relocation_complete(dmcache *dmc, struct relocation_job* job) {

    io_info internal_io = {
                .size_sectors = dmc->block_size,
                .sector       = dmc->block_idx_to_sector_addr(job->cacheblock->cache_block_addr),
                .is_read      = false,
            };

    /* Update simulated zbd metadata */
    job->cache_device->write(internal_io);
    job->cacheblock->cache_block_addr = dmc->sector_addr_to_block_idx(internal_io.sector);
    
    // Return the cacheblock to the VALID state and update associated metadata
    gc_cacheblock_metadata_updated(dmc, job->cacheblock);
    dmc->cacheblock_update_state(job->cacheblock);
    dmc->finish_zone_append(job->cacheblock);

    if (dmc->stats_logging_enabled)
        dmc->log_stats();

    return 0;
}

void dmcacheGC::gc_relocate_cacheblock(dmcache *dmc, struct cacheblock *cache) {

    struct relocation_job job;
    all_relocated_blocks.insert(cache->src_block_addr);

    // Mark the block as relocated because we are relocating it :)
    // Used for the relocation_hits statistic
    cache->set_relocated(true);
    cache->set_can_count_unique_hit(true);

    // job = new_relocation_job(dmc, cache);
    job = {
        .cacheblock   = cache ,
        .is_read      = true  ,
        .cache_device = dmc->cache_device
    };
    do_relocation_io(dmc, &job);
}

void dmcacheGC::gc_mark_garbage(dmcache *dmc, uint64_t block_idx) {

    bm_gcable_blocks->set_bit(block_idx);
    gc_total_garbage_count++;
    gc_garbage_count++;
    pre_gc_garbage_count++;

    assert(block_idx <= dmc->nr_zones*dmc->blocks_per_zone);
    per_zone_stats[dmc->block_idx_to_zone_idx(block_idx)]->gcable_count++;
    zbd_cacheblocks[block_idx] = NULL;

    // Garbge amount has changed, check GC triggers
    if (gc_garbage_count >= gc_trigger_block_io) {
        block_ios = true; // atomic_set(&gc->block_ios, 1);
    }

    // if (gc_trigger_count(dmc) >= gc_trigger_start) {
    if (should_start_gc(dmc)) {
        DBUG_GC("[GC] gc_garbage_count:", gc_garbage_count, " gc_trigger_start:", gc_trigger_start, "\n");

        /* Dump cacheblock stats for each zone if enabled */
        if (dmc->stats_logging_enabled) {
            dmc->log_cacheblock_stats();

            // Log relocation cacheblock stats
            for (auto pair : dmc->relocated_block_stats) {
                auto block = dmc->map.find(pair.first);
                if (block != dmc->map.end()) {
                    *dmc->relocation_stats_logger_file << gc_iter_count << ","
                                                    << pair.first << ","
                                                    << block->second->cache_block_addr << ","
                                                    << pair.second.relocation_count << ","
                                                    << pair.second.relocation_read_hits << ","
                                                    << pair.second.relocation_write_hits << ","
                                                    << pair.second.relocation_misses_after_lru_eviction << "\n";
                }
            }
        }

        do_gc(dmc);
    }
}

void dmcacheGC::gc_cacheblock_metadata_updated(dmcache* dmc, struct cacheblock* cacheblock) {

    assert(cacheblock->cache_block_addr < dmc->nr_zones*dmc->blocks_per_zone);
    zbd_cacheblocks[cacheblock->cache_block_addr] = cacheblock;
}

void dmcacheGC::gc_zone_metadata_updated(dmcache* dmc) {
    // uint64_t nr_free_zones = gc_count_free_zones(dmc);

    // This fn is always called directly after new zones are selected. So it is
    // safe to assume that we have ~NUM_WRITE_HEADS of available space
    //if (nr_free_zones + NUM_WRITE_HEADS <= BLOCK_IOS_FREE_ZONES) 
    
    // There is no need to include NUM_WRITE_HEADS as nr_free_zones already took care of it 
    // if (nr_free_zones <= BLOCK_IOS_FREE_ZONES) {
    //     block_ios = true; // atomic_set(&gc->block_ios, 1);
    // }

    if (gc_garbage_count >= gc_trigger_block_io) {
        block_ios = true; // atomic_set(&gc->block_ios, 1);
    }

    /* Decide if we should trigger GC to run based upon the remaining space of the cache device
       There is no need to include NUM_WRITE_HEADS as nr_free_zones already took care of it 
    */

    // if (gc_trigger_count(dmc) >= gc_trigger_start) {
    if (should_start_gc(dmc)) {
        DBUG_GC("[GC] gc_garbage_count:", gc_garbage_count, " gc_trigger_start:", gc_trigger_start, "\n");
        do_gc(dmc);
    }
}

#if 0
int is_block_ios_enabled(struct dm_target* ti) {

    struct cache_c* dmc = (struct cache_c*) ti->private;
    struct gc_c* gc = dmc->gc;
    return atomic_read(&gc->block_ios);
}
#endif


#else

void do_gc(dmcache* dmc) {}
void gc_cacheblock_metadata_updated(dmcache* dmc, struct cacheblock* cacheblock) {}
void gc_mark_garbage(dmcache* dmc, uint64_t block_idx) {}
void gc_zone_metadata_updated(dmcache* dmc) {}
void gc_reset(dmcache* dmc) {}
int is_block_ios_enabled(struct dm_target* ignored) { return 0; }

#endif
