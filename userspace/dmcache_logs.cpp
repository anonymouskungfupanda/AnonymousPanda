#include <filesystem>
#include "dmcache_gc.hpp"
#include "dmcache.hpp"

void dmcache::ap_log_gc()
{
    if (!dump_access_pattern)
        return;

    assert(ap_gc_iters_logger_file != nullptr);
    *ap_gc_iters_logger_file << get_timestamp() << "\n";
}

void dmcache::ap_log_io(uint64_t sector, uint64_t score, bool is_sequential)
{
    if (!dump_access_pattern)
        return;

    assert(ap_ios_logger_file != nullptr);
    *ap_ios_logger_file << sector << ","
        << get_timestamp() << ","
        << score << ","
        << is_sequential << "\n";
}

void dmcache::ap_log_flush(uint64_t iter, uint64_t sector)
{
    if (!dump_access_pattern)
        return;

    /* Leaving this enabled tends to eat up a LOT of disk space, hence why I have the return here. If
     * you want to work with the flushed blocks logs, I'd suggest having an if to check for a specific
     * range (e.g. GC iterations 10-20 or something) and return on all iterations outside of this range
     * to avoid consuming a bunch of space. */
    return;

    if (ap_current_gc_iter != iter || ap_flush_logger_file == nullptr) {
        if (ap_flush_logger_file != nullptr) {
            ap_flush_logger_file->close();
            delete ap_flush_logger_file;
        }
        ap_flush_logger_file = new std::ofstream("/tmp/ap_flush_" + std::to_string(iter) + ".log",
		std::ios_base::out);
        ap_current_gc_iter = iter;
    }

    assert(ap_flush_logger_file != nullptr);
    *ap_flush_logger_file << sector << "\n";
}



void dmcache::print_dmcache_configuration() {

    std::cout << "------------ DMZCache Simulator Configuration ------------" << std::endl;
    std::cout << "- Block Size: " << block_size << std::endl;
    std::cout << "- Total number of Zones: " << nr_zones << std::endl;
    std::cout << "- Cacheblocks per Zone: " << blocks_per_zone << std::endl;
    std::cout << "- Max Open Zones: " << cache_dev_max_open_zones << std::endl;
    std::cout << "- Max Active Zones: " << cache_dev_max_active_zones << std::endl;
    std::cout << "- Zone Size in sectors: " << zone_size_sectors << std::endl;
    std::cout << "- Zone Capacity in sectors: " << zone_cap_sectors << std::endl;
    std::cout << "- Total available Cache Size (without OPS): " << (nr_zones * blocks_per_zone) << std::endl;
    //std::cout << "- Number of Write Heads: " << NUM_WRITE_HEADS << std::endl;
    //std::cout << "- Write Head Block Size: " << WRITE_HEADS_BLOCK_SIZE << std::endl;
    std::cout << "- Cache Admission Enabled: " << (cache_admission_enabled ? "Yes" : "No") << std::endl;
    std::cout << "- GC Enabled: " << (GC_ENABLED ? "Yes" : "No") << std::endl;
    
    if (cache_admission_enabled) {
        std::cout << "----------------- Cache Admission Configuration -----------------" << std::endl;
        std::cout << "- RWSS(N); N: " << RWSS_ACCESS_THRESHOLD << std::endl;
        std::cout << "- Staging Area Size: \t" << staging_area_size << "\n";
    }
    
#if GC_ENABLED
        std::cout << "--------------- DMZCache-GC Configuration ---------------" << std::endl;
        std::cout << "- GC Victim Selection Mode: " << VICTIM_SELECTION_MODE_STRING(gc->gc_victim_sel_mode) << std::endl;
        std::cout << "- GC Policy: " << (gc->gc_mode ? "RELOCATION" : "EVICTION") << std::endl;

        if (gc->gc_mode == RELOCATION_GC) {
            std::cout << "- Relocation Mode: " << RELOCATION_MODE_STRING(gc->gc_relocation_mode) << std::endl;
            DISPLAY_THRESHOLD_FOR_MODE(gc->gc_relocation_mode);
        }

        std::cout << "- GC Configuration: " << gc->gc_op_percentage << "/"
                  << gc->gc_op_acceptable_free_space_percentage << "-"
                  << gc->gc_op_stop_start_gap_percentage << "-"
                  << gc->gc_op_start_block_gap_percentage << std::endl;

        std::cout << "- Total available Cache Size (after OPS): " << cache_size << std::endl;
        std::cout << "- GC Trigger Start: " << gc->gc_trigger_start << std::endl;
        std::cout << "- GC Trigger Stop: "  << gc->gc_trigger_stop << std::endl;
        std::cout << "- GC Trigger Block: " << gc->gc_trigger_block_io << std::endl;

#endif /* (GC_ENABLED) */
    std::cout << "----------------------------------------------------------" << std::endl;
}

void dmcache::print_dmcache_results_stats() {

    std::cout << "----------------- Workload Statistics -----------------" << std::endl;
    std::cout << "- Workload WSS:   \t" << (double) (wss.size() * 4)/ (1024*1024) << " GB \n";
    std::cout << "- Workload RWSS:  \t" << (double) (rwss.size() * 4)/ (1024*1024) << " GB \n";
    
    if (gc->gc_relocation_mode == RELOCATE_ON_DYNAMIC_RWSS) {
        double mean;
        double std_deviation;
        double variance = 0;

        if(rwss_t_threshold_vec.empty()){
            std::cout << "- Threshold Mean: \t 0 \t Std Dev:0 \t Variance:0 \n";
        }else {
            auto const count = static_cast<float>(rwss_t_threshold_vec.size());
            mean             =  std::reduce(rwss_t_threshold_vec.begin(), rwss_t_threshold_vec.end()) / count;

            for (uint64_t index = 0; index < rwss_t_threshold_vec.size(); index++)
                variance += pow(rwss_t_threshold_vec[index] - mean, 2);
            
            variance     /= rwss_t_threshold_vec.size();
            std_deviation = sqrt(variance);

            std::cout << "- Threshold Mean: \t" << mean << "\t Std Dev:" << std_deviation << "\t Variance:" << variance << "\n";
        }
    }

    std::cout << "----------------- DMZCache Simulation Statistics -----------------" << std::endl;
    std::cout << "- Reads:   \t" << stats.reads        << "\n";
    std::cout << "- Writes:  \t" << stats.writes       << "\n";
    std::cout << "- Hits:    \t" << stats.cache_hits   << "\n";
    std::cout << "--  Read:  \t" << stats.read_hits    << "\n";
    std::cout << "--  Write: \t" << stats.write_hits   << "\n";
    std::cout << "- Misses:  \t" << stats.cache_misses << "\n";
    std::cout << "--  Read:  \t" << stats.read_misses  << "\n";
    std::cout << "--  Write: \t" << stats.write_misses << "\n";
    std::cout << "- Replaces:\t" << stats.replaces     << "\n";
    std::cout << "- Inserts: \t" << stats.inserts      << "\n";
    std::cout << "- Cache Usage: \t" << (double)(stats.inserts + stats.replaces) / (double)(stats.reads + stats.writes) << "\n";
    std::cout << "- Hit Rate:\t" << (double)stats.cache_hits / (double)(stats.cache_hits + stats.cache_misses) << "\n";
    std::cout << "--  Read Hit Rate:\t" << (double)stats.read_hits / (double)(stats.read_hits + stats.read_misses) << "\n";
    std::cout << "--  Write Hit Rate:\t" << (double)stats.write_hits / (double)(stats.write_hits + stats.write_misses) << "\n";

    #if GC_ENABLED
        std::cout << "----------------- DMZCache-GC Statistics -----------------" << std::endl;
        std::cout << "- WAF:     \t" << (((stats.cache_hits + stats.cache_misses) - stats.read_hits) + gc->relocated_cacheblocks_count) / (double)stats.writes << "\n";
        std::cout << "- GC Iterations: \t\t" << gc->gc_iter_count << "\n";
        std::cout << "- Total Relocated Cacheblocks: \t\t" << gc->relocated_cacheblocks_count << "\n";
        std::cout << "- Unique Relocated Cacheblocks: \t" << gc->relocated_unique_cacheblocks_count << "\n";
        std::cout << "- Unique Relocation hits: \t\t" << gc->unique_relocation_hits_count << "\n";
        std::cout << "- Relocation hits both Read and Write: \t" << gc->relocation_hits_count << "\n";
        std::cout << "- Relocation hits Read: \t\t" << gc->relocation_read_hits_count << "\n";
        std::cout << "- Relocation hits Write: \t\t" << gc->relocation_write_hits_count << "\n";
        std::cout << "- Relocations which resulted in 0 READ hits: \t\t" << gc->relocations_no_read_hit << "\n";
        std::cout << "- Relocations which resulted in 0 WRITE hits: \t\t" << gc->relocations_no_write_hit << "\n";
        std::cout << "- Relocations which results in neither READ nor WRITE hits: \t\t" << gc->relocations_no_read_write_hit << "\n";
        std::cout << "- The # of times a block was relocated, then evicted, and then resulted in a cache miss:\t" << gc->relocation_misses_after_lru_eviction << "\n";
        std::cout << "- Total Evicted Cacheblocks: \t\t" << gc->evicted_cacheblocks_count << "\n";
        std::cout << "- Cache misses due to evicted cacheblocks: \t" << gc->eviction_misses_count << "\n";

		if(gc->gc_relocation_mode == RELOCATE_ON_DYNAMIC_RWSS) {
            std::cout << "- Total Number of Reused cacheblocks saved: \t\t" << rwss_t_stats.no_reused_blocks_t_saved << "\n";
            std::cout << "- Total NUmber of Unused cacheblocks saved: \t\t" << rwss_t_stats.no_unused_blocks_t_saved << "\n";
            std::cout << "- Total Number of Reused cacheblocks flushed: \t\t" << rwss_t_stats.no_reused_blocks_t_flushed << "\n";
            std::cout << "- Total Number of Unused cacheblocks flushed: \t\t" << rwss_t_stats.no_unused_blocks_t_flushed << "\n";
        }

    #else
        std::cout << "- WAF:     \t" << ((stats.cache_hits + stats.cache_misses) - stats.read_hits) / (double)stats.writes << "\n";
    #endif
    
    if (cache_admission_enabled) {
        std::cout << "----------------- Cache Admission Statistics -----------------" << std::endl;
        std::cout << "- Allocates:                \t" << stats.allocates            << "\n";
        std::cout << "- Inserts:                  \t" << staging_stats.inserts      << "\n";
        std::cout << "- Replaces:                 \t" << staging_stats.replaces     << "\n";
        std::cout << "- Sequential Reads Skipped: \t" << seq_io_stats.reads_skipped << "\n";
        std::cout << "- Sequential Writes Skipped:\t" << seq_io_stats.writes_skipped << "\n";
    }

    std::cout << "----------------------------------------------------------" << std::endl;
}

void dmcache::log_stats() 
{
    if (++stats_logging_counter >= stats_logging_frequency) {
        stats_logging_counter = 0;

        uint64_t normal_writes = (stats.cache_hits + stats.cache_misses) - stats.read_hits;
        //write_hits + read_misses

        #if GC_ENABLED
            uint64_t total_writes = normal_writes + gc->relocated_cacheblocks_count;
        #else
            uint64_t total_writes = normal_writes;
        #endif

        *stats_logger_file << stats.reads << ","
                           << stats.writes << ","
                           << stats.cache_hits << ","
                           << stats.cache_misses << ","
                           << stats.read_misses << ","
                           << stats.write_misses << ","
                           << stats.read_hits << ","
                           << stats.write_hits << ","
                           << stats.allocates << ","
                           << stats.replaces << ","
                           << stats.inserts << ","
                           << stats.dirty_blocks << ","
                           << stats.write_backs << ","
                           << stats.cache_hits / (double)(stats.cache_hits + stats.cache_misses) << ","
                           << total_writes / (double)stats.writes;

        #if GC_ENABLED
            *stats_logger_file << ","
                               << gc->is_gc_active << ","
                               << (normal_writes + gc->relocated_cacheblocks_count) / (double)normal_writes << ","
                               << gc->gc_garbage_count << ","
                               << nr_zones * blocks_per_zone - gc->gc_garbage_count << ","
                               << gc->relocated_cacheblocks_count << ","
                               << gc->relocated_unique_cacheblocks_count << ","
                               << gc->evicted_cacheblocks_count << ","
                               << gc->relocation_hits_count << ","
                               << gc->unique_relocation_hits_count << ","
                               << gc->relocation_read_hits_count << ","
                               << gc->relocation_write_hits_count << ","
                               << gc->relocations_no_read_hit << ","
                               << gc->relocations_no_write_hit << ","
                               << gc->relocations_no_read_write_hit << ","
                               << gc->eviction_misses_count << ","
                               << gc->post_gc_allocates << ","
                               << gc->pre_gc_allocates << ","
                               << gc->post_gc_garbage_count << ","
                               << gc->pre_gc_garbage_count << ","
                               << gc->num_evicted_periodical << ","
                               << gc->num_freed_periodical << ","
                               << gc->relocation_misses_after_lru_eviction;
        #endif

        *stats_logger_file << "\n";
    }
}

void dmcache::do_automation_logging() {

    namespace fs = std::filesystem;
    std::fstream file;

    /* If the file already exists, just open it and append to the end */
    if (fs::exists(automation_logging_filename)) {
        file = std::fstream(automation_logging_filename, std::ios::out | std::ios::app);
        file.seekp(0, std::ios::end);
    }

    /* If the file doesn't exist, create it */
    else {
        file = std::fstream(automation_logging_filename, std::ios::out);
        file << "Trace File\t"
            << "nr_zones\t"
            << "zone_size (GB)\t"
            << "zone_cap (GB)\t"
            << "GC_Config\t"
            << "POLICY\t"
            << "Cache Admission (N=0: No Cache Admission, N=1: Address Staging, N=2: Data Staging)\t"
            << "hit_rate\t"
            << "Read (Hit Rate)\t"
            << "Write (Hit Rate)\t"
            << "WAF\t"
            << "Cache Usage\t"
            << "Reads\t"
            << "Writes\t"
            << "Cache Hits\t"
            << "Read Hits\t"
            << "Write Hits\t"
            << "Cache Misses\t"
            << "Read Misses\t"
            << "Write Misses\t"
            << "Replace\t"
            << "Insert\t"
            << "Relocated Cacheblocks\t"
            << "Unique Relocated Cacheblocks\t"
            << "Unique Relocation Hits\t"
            << "Relocation Hits (Read & Write)\t"
            << "Relocation Hits (Read)\t"
            << "Relocation Hits (Write)\t"
            << "Relocations in 0 READ Hits\t"
            << "Relocations in 0 WRITE Hits\t"
            << "Relocations in neither READ nor WRITE Hits\t"
            << "Relocated Block -> Evicted -> Cache_miss\t"
            << "Evicted Cacheblocks\t"
            << "Cache_miss due to evicted cacheblocks\t"
            << "Average # of accesses (ALL blocks)\t"
            << "Average # of accesses before relocation (ALL blocks)\t"
            << "Average # of accesses after relocation (ALL blocks)\t"
            << "Ratio of average # accesses before relocation / average # accesses after relocation (ALL blocks)\t"
            << "Average # of accesses (relocated blocks)\t"
            << "GC iterations\t"
            << "Average threshold values for Dynamic RWSS\t"
            << "std_deviation\t"
            << "variance\t"
            << "no_reused_blocks_t_saved\t"
            << "no_unused_blocks_t_saved\t"
            << "no_reused_blocks_t_flushed\t"
            << "no_unused_blocks_t_flushed\t"
            << "total_zone_resets\t"
            << "total_garbage_created\t"
            << "lru_thresh_readjustments\t"
            << "max_scan_length\t"
            << "min_scan_length\t"
            << "avg_scan_length\t"
            << "num_scans\t"
            << "total_flagged_seq\n";
    }

    do_automation_print(file);
}

void dmcache::do_automation_print(std::ostream &out) {

    double zone_size_gigs   = ((double)zone_size_sectors * 512.0) / 1073741824.0;
    double zone_cap_gigs    = ((double)zone_cap_sectors * 512.0) / 1073741824.0;
    int staging_policy      = 0;
    
    double rwss_t_mean      = 0;
    double rwss_t_std_dev   = 0;
    double rwss_t_variance  = 0;

    if (!cache_admission_enabled) {
        staging_policy = 0;
    }
    else if (cache_admission == &dmcache::cache_admission_addr_staging) {
        staging_policy = 1;
    }
    else if (cache_admission == &dmcache::cache_admission_data_staging) {
        staging_policy = 2;
    }

    if(!rwss_t_threshold_vec.empty()){

        auto const count = static_cast<float>(rwss_t_threshold_vec.size());
        rwss_t_mean      =  std::reduce(rwss_t_threshold_vec.begin(), rwss_t_threshold_vec.end()) / count;

        for (uint64_t index = 0; index < rwss_t_threshold_vec.size(); index++)
            rwss_t_variance += pow(rwss_t_threshold_vec[index] - rwss_t_mean, 2);
        
        rwss_t_variance     /= rwss_t_threshold_vec.size();
        rwss_t_std_dev = sqrt(rwss_t_variance);
    }

    out << trace_file_name << "\t"
        << nr_zones << "\t"
        << zone_size_gigs << "\t"
        << zone_cap_gigs << "\t"

        << gc->gc_op_percentage << "/"
            << gc->gc_op_acceptable_free_space_percentage << "-"
            << gc->gc_op_stop_start_gap_percentage     << "-"
            << gc->gc_op_start_block_gap_percentage    << "\t"

        << (gc->gc_mode ? "RELOCATION" : "FLUSH") << "\t"
        << staging_policy << "\t"
        << (double)stats.cache_hits / (double)(stats.cache_hits + stats.cache_misses) << "\t"
        << (double)stats.read_hits / (double)(stats.read_hits + stats.read_misses) << "\t"
        << (double)stats.write_hits / (double)(stats.write_hits + stats.write_misses) << "\t"
        << (((stats.cache_hits + stats.cache_misses) - stats.read_hits) + gc->relocated_cacheblocks_count) / (double)stats.writes << "\t"
        << (double)(stats.inserts + stats.replaces) / (double)(stats.reads + stats.writes) << "\t"
        << stats.reads << "\t"
        << stats.writes << "\t"
        << stats.cache_hits << "\t"
        << stats.read_hits << "\t"
        << stats.write_hits << "\t"
        << stats.cache_misses << "\t"
        << stats.read_misses << "\t"
        << stats.write_misses << "\t"
        << stats.replaces << "\t"
        << stats.inserts << "\t"
        << gc->relocated_cacheblocks_count << "\t"
        << gc->relocated_unique_cacheblocks_count << "\t"
        << gc->unique_relocation_hits_count << "\t"
        << gc->relocation_hits_count << "\t"
        << gc->relocation_read_hits_count << "\t"
        << gc->relocation_write_hits_count << "\t"
        << gc->relocations_no_read_hit << "\t"
        << gc->relocations_no_write_hit << "\t"
        << gc->relocations_no_read_write_hit << "\t"
        << gc->relocation_misses_after_lru_eviction << "\t"
        << gc->evicted_cacheblocks_count << "\t"
        << gc->eviction_misses_count << "\t"
        << avg_access_stats.avg_num_accesses_all_blocks << "\t"
        << avg_access_stats.avg_num_accesses_before_relocation << "\t"
        << avg_access_stats.avg_num_accesses_after_relocation << "\t"
        << avg_access_stats.ratio_avg_accesses_before_over_after << "\t"
        << avg_access_stats.avg_num_accesses_relocated_blocks << "\t"
        << gc->gc_iter_count << "\t"
        << rwss_t_mean << "\t"
        << rwss_t_std_dev << "\t"
		<< rwss_t_variance << "\t"
        << rwss_t_stats.no_reused_blocks_t_saved << "\t"
        << rwss_t_stats.no_unused_blocks_t_saved << "\t"
        << rwss_t_stats.no_reused_blocks_t_flushed << "\t"
        << rwss_t_stats.no_unused_blocks_t_flushed << "\t"
        << gc->total_zone_resets << "\t"
        << gc->gc_total_garbage_count << "\t"
        << lru_thresh_readjustments << "\t"
        << seq_io_stats.max_scan_length << "\t"
        << seq_io_stats.min_scan_length << "\t"
        << ((float)(seq_io_stats.scan_length_sum) / (float)(seq_io_stats.num_scans)) << "\t"
        << seq_io_stats.num_scans << "\t"
        << seq_io_stats.total_flagged << "\n";
}

void dmcache::log_cacheblock_stats()
{
    for (const auto& entry : map) {
        uint64_t key        = entry.first;
        cacheblock* value   = entry.second;
        uint64_t zone_idx   = block_idx_to_zone_idx(value->cache_block_addr);

        *cblock_stats_logger_file << gc->is_gc_active << ","
                                  << gc->gc_iter_count << ","
                                  << zone_idx << ","
                                  << value->cache_block_addr << ","
                                  << value->freq_of_access;

        *cblock_stats_logger_file << "\n";
    }
}

void dmcache::log_victim_zone_stats(uint64_t starting_block_idx) {

    uint64_t index, total_zone_reclaims;
    index = total_zone_reclaims = 0;
    uint64_t zone_idx = block_idx_to_zone_idx(starting_block_idx);

    *per_zone_stats_logger_file << gc->gc_iter_count << ","
                                << zone_idx << ","
                                << gc->per_zone_stats[zone_idx]->reclaim_count << ","
                                << gc->per_zone_stats[zone_idx]->gcable_count << ","
                                << gc->per_zone_stats[zone_idx]->max_timestamp << ","
                                << gc->per_zone_stats[zone_idx]->min_timestamp << ","
                                << gc->per_zone_stats[zone_idx]->blocks_flushed;
    *per_zone_stats_logger_file << "\n";
    per_zone_stats_logger_file->flush();
}

void dmcache::log_rwss_t(uint64_t rwss, uint64_t relocation_lru_thresh, double hit_rate, double waf) {

    *rwss_t_logger_file << rwss << ","
                        << relocation_lru_thresh << ","
                        << hit_rate << ","
                        << waf;
    *rwss_t_logger_file << "\n";
    rwss_t_logger_file->flush();
}

void dmcache::log_write_miss(uint64_t io, uint64_t cache, uint64_t garbage, uint64_t timestamp) {

    if (garbage == (uint64_t)-1)
        *op_logger_file
            << timestamp << ","
            << io
            << ",WRITE,MISS,"
            << cache << ","
            << -1 << ",0\n";

    else
        *op_logger_file
            << timestamp << ","
            << io
            << ",WRITE,MISS,"
            << cache << ","
            << garbage << ",0\n";
}

void dmcache::log_write_hit(uint64_t io, uint64_t lru_idx, uint64_t old_cache, uint64_t new_cache, uint64_t timestamp) {

    *op_logger_file
        << timestamp << ","
        << io
        << ",WRITE,HIT,"
        << lru_idx << ","
        << old_cache << ","
        << new_cache << "\n";
}

void dmcache::log_read_miss(uint64_t io, uint64_t cache, uint64_t garbage, uint64_t timestamp) {

    if (garbage == (uint64_t)-1)
        *op_logger_file
            << timestamp << ","
            << io
            << ",READ,MISS,"
            << cache << ","
            << -1 << ",0\n";

    else
        *op_logger_file
            << timestamp << ","
            << io
            << ",READ,MISS,"
            << cache << ","
            << garbage << ",0\n";
}

void dmcache::log_read_hit(uint64_t io, uint64_t lru_idx, uint64_t cache, uint64_t timestamp) {

    *op_logger_file
        << timestamp << ","
        << io
        << ",READ,HIT,"
        << lru_idx << ","
        << cache << ",0\n";
}
