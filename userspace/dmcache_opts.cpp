#include <iostream>
#include <fstream>
#include "dmcache.hpp"
#include "dmcache_gc.hpp"

/*
 * In case more options are to be added here in the future:
 *
 *  ... another dmcache::option should be added to this array. The 'name'
 * is what the option is named in the configuration file, and the function
 * pointed to by 'func' will be called for the corresponding name. Within
 * the function pointed to by 'func', you can access what the option is set
 * to in the config file with the 'value' parameter and access/change any
 * members from the dmcache struct with the 'dmc' pointer.
 */
const dmcache::option dmcache::opts_table[] = {

    /* Enable or disable dmsetup emulation */
    {
        [](dmcache *dmc, const std::string &value) {
            if (std::stoi(value) == 1) {
                std::cout << " - dmsetup will be emulated\n";
                dmc->emulate_dmsetup = true;
            }
        },
        "emulate_dmsetup"
    },

    /* Enable or disable the fio trace reader */
    {
        [](dmcache *dmc, const std::string &value) {
            if (std::stoi(value) == 1) {
                std::cout << " - Input will be interpreted as a Fio V2 trace\n";
                dmc->use_fio_traces = true;
            }
        },
        "use_fio_traces"
    },

    /* Base device size in sectors */
    {
        [](dmcache *dmc, const std::string &value) {
            std::cout << " - base device size is" << value << " sectors\n";
            dmc->base_dev_size = std::stoi(value);
        },
        "base_size"
    },

    /* Zoned block cache device or regular cache device (1 or 0) */
    {
        [](dmcache *dmc, const std::string &value) {
            if (std::stoi(value) == 1) {
                std::cout << " - Simulated cache device will be ZBD\n";
                dmc->cache_dev_is_zoned = true;
            }
            else std::cout << " - Simulated cache device will be regular\n";
        },
        "zbd_cache"
    },

    /* Block size in sectors */
    {
        [](dmcache *dmc, const std::string &value) {
            std::cout << " - block size is" << value << " sectors\n";
            dmc->block_size = std::stoi(value);
        },
        "block_size"
    },

    /* Cache size in blocks, only actually used for regular cache as the cache
     *      size will just be derrived from other options for zbd */
    {
        [](dmcache *dmc, const std::string &value) {
            std::cout << " - cache size is" << value << " blocks\n";
            dmc->cache_size = std::stol(value);
        },
        "cache_size"
    },

    /* Number of zones, only used for zbd cache */
    {
        [](dmcache *dmc, const std::string &value) {
            std::cout << " - zbd has" << value << " zones\n";
            dmc->nr_zones = std::stoi(value);
        },
        "nr_zones"
    },

    /* Zone size in sectors only used for zbd cache */
    {
        [](dmcache *dmc, const std::string &value) {
            std::cout << " - zone size is" << value << " sectors\n";
            dmc->zone_size_sectors = std::stol(value);
        },
        "zone_size"
    },

    /* Zone capacity in sectors only used for zbd cache */
    {
        [](dmcache *dmc, const std::string &value) {
            std::cout << " - zone capacity is" << value << " sectors\n";
            dmc->zone_cap_sectors = std::stol(value);
        },
        "zone_cap"
    },

    /* Dump zbd write pointers to a file (1 or 0) */
    {
        [](dmcache *dmc, const std::string &value) {
            if (std::stoi(value) == 1) {
                std::cout << " - simulated zbd metadata will be dumped\n";
                dmc->dump_zbd_metadata = true;
            }
        },
        "dump_zbd_metadata"
    },

    /* Dump bitmap contents to a file (1 or 0) */
    {
        [](dmcache *dmc, const std::string &value) {
            if (std::stoi(value) == 1) {
                std::cout << " - bitmap contents will be dumped\n";
                dmc->dump_bitmaps = true;
            }
        },
        "dump_bitmaps"
    },

    /* This is subject to change (1 or 0) */
    {
        [](dmcache *dmc, const std::string &value) {
            if (std::stoi(value) == 1) {
                std::cout << " - operation_logger is enabled\n";
                dmc->op_logger_file = new std::ofstream("./operation_logs.csv", std::ios_base::out);
                dmc->op_logging_enabled = true;
            }
        },
        "operation_logging"
    },

    /* Log stats periodically during simulation */
    {
        [](dmcache *dmc, const std::string &value) {
            if (std::stoi(value) == 1) {
                std::cout << " - statistics logging is enabled\n";
                dmc->stats_logger_file = new std::ofstream("./stats_log.csv", std::ios_base::out);
                *dmc->stats_logger_file << "reads,writes,cache_hits,cache_misses,read_misses,write_misses,"
                    "read_hits,write_hits,allocates,replaces,insesrts,dirty_blocks,write_backs,hit_rate,WAF";
                dmc->stats_logging_enabled = true;

                #if GC_ENABLED
                    dmc->zone_stats_logger_file = new std::ofstream("./zone_stats_log.csv", std::ios_base::out);
                    std::string zone_header;
                    zone_header.reserve(dmc->nr_zones * 2); 

                    for (uint64_t i = 0; i < dmc->nr_zones; ++i) {
                        if (i > 1) {
                            zone_header.push_back(',');
                        }
                        zone_header += std::to_string(i);
                    }

                    *dmc->zone_stats_logger_file << zone_header;
                    *dmc->zone_stats_logger_file << "\n";

                    *dmc->per_zone_stats_logger_file << zone_header;
                    *dmc->per_zone_stats_logger_file << "\n";

                    *dmc->stats_logger_file << ",is_gc_active,GC_WAF,#_garbage_blocks,#_free_blocks,#_blocks_relocated,#_unique_blocks_relocated,"
                    "#_blocks_evicted,relocation_hits,unique_relocation_hits,relocation_read_hits_count,relocation_write_hits_count,"
                    "relocations_no_read_hit,relocations_no_write_hit,relocations_no_read_write_hit,eviction_misses,post_gc_allocates,pre_gc_allocates,"
                    "post_gc_garbage_count,pre_gc_garbage_count,#_evicted_periodical,#_freed_periodical,relocation_misses_after_lru_eviction";
                    
                    dmc->cblock_stats_logger_file = new std::ofstream("./cblock_stats_log.csv", std::ios_base::out);
                    *dmc->cblock_stats_logger_file << "is_gc_active,gc_iter_count,zone_idx,cacheblockaddr,hotness";
                    *dmc->cblock_stats_logger_file << "\n";

                    dmc->relocation_stats_logger_file = new std::ofstream("./relocation_stats_log.csv", std::ios_base::out);
                    *dmc->relocation_stats_logger_file << "gc_iter_count,src_block_addr,cache_block_addr,relocation_count,relocation_read_hits,relocation_write_hits,relocation_misses_after_lru_eviction";
                    *dmc->relocation_stats_logger_file << "\n";
                #endif

                *dmc->stats_logger_file << "\n";
            }
        },
        "statistics_logging"
    },

    /* Log stats periodically during simulation */
    {
        [](dmcache *dmc, const std::string &value) {
            if (std::stoi(value) == 1) {
                std::cout << " - victim statistics logging is enabled\n";
                dmc->per_zone_stats_logger_file     = new std::ofstream("./victim_stats_log.csv", std::ios_base::out);
                dmc->rwss_t_logger_file             = new std::ofstream("./rwss_t_log.csv", std::ios_base::out);

                dmc->victim_stats_logging_enabled = true;

                *dmc->per_zone_stats_logger_file << "gc_iter_count,zone_idx,reclaim_count,gcable_count,max_timestamp,min_timestamp,blocks_flushed,total_recency";
                *dmc->per_zone_stats_logger_file << "\n";

                *dmc->rwss_t_logger_file << "rwss,rwss_t,hit_rate,waf";
                *dmc->rwss_t_logger_file << "\n";
            }
        },
        "victim_stats_logging"
    },
    {
        [](dmcache *dmc, const std::string &value) {
            dmc->automation_logging_filename = std::string(value.c_str() + 1);
            dmc->automation_logging = true;
            std::cout << " - automation logging to" << value << " is enabled\n";
        },
        "automation_logging"
    },

    {
        [](dmcache *dmc, const std::string &value) {
            if (std::stoi(value) == 1) {
                dmc->automation_streaming = true;
                std::cout << " - automation streaming to stderr is enabled\n";
            }
        },
        "automation_streaming"
    },

    /* Control frequency of statistics logging */
    {
        [](dmcache *dmc, const std::string &value) {
            dmc->stats_logging_frequency = std::stol(value);
            std::cout << " - statistics will be logged per " << dmc->stats_logging_frequency << " io ops\n";
        },
        "statistics_logging_frequency"
    },

    /* Control staging policy */
    {
        [](dmcache *dmc, const std::string &value) {
            if (value == " addr") {
                dmc->cache_admission = &dmcache::cache_admission_addr_staging;
                std::cout << " - will use address staging for cache_admission\n";
            }
            else if (value == " data") {
                dmc->cache_admission = &dmcache::cache_admission_data_staging;
                std::cout << " - will use data staging for cache_admission\n";
            }
            else if (value == " sseqr") {
                dmc->cache_admission = &dmcache::cache_admission_sseqr;
                std::cout << " - will use sseqr for cache_admission\n";
            }
        },
        "staging_policy"
    },

    /* Control maximum value of relocation_lru_thresh based on num_allocates */
    {
        [](dmcache *dmc, const std::string &value) {
            dmc->max_lru_thresh_enabled = true;
            dmc->max_lru_thresh_percent = std::stof(value);
            std::cout << " - using max_lru_thresh_percent of " << dmc->max_lru_thresh_percent << "\n";
        },
        "max_lru_thresh"
    },

    /* Control lru readjustment distance using a percentage of zone capacity */
    {
        [](dmcache *dmc, const std::string &value) {
            uint64_t dist = (dmc->zone_cap_sectors / dmc->block_size) * std::stof(value);
            dmc->lru_readjust_dist = dist;
            dmc->lru_readjust_enabled = true;
            std::cout << " - using threshold readjust distance of " << value << "\n";
        },
        "readjust_dist_percent"
    },

    /* Both of these are for trace segmenting */
    {
        [](dmcache *dmc, const std::string &value) {
             char *end;
             dmc->skip = std::strtoull(value.c_str(), &end, 10);
             std::cout << " - skipping first " << value << " IOs\n";
        },
        "skip"
    },

    /* Both of these are for trace segmenting */
    {
        [](dmcache *dmc, const std::string &value) {
             char *end;
             dmc->limit = std::strtoull(value.c_str(), &end, 10);
             std::cout << " - limit set to " << value << " IOs\n";
        },
        "limit"
    },

    /* For SSEQR configuration */
    {
        [](dmcache *dmc, const std::string &value) {
            std::stringstream ss(value);
            std::string tmp;

            if (std::getline(ss, tmp, ':')) {
                dmc->sseqr_window_size = std::stoull(tmp);
            }

            if (std::getline(ss, tmp, ':')) {
                dmc->sseqr_accept_range = std::stoull(tmp);
            }

            if (std::getline(ss, tmp, ':')) {
                dmc->sseqr_admit_dist = std::stoull(tmp);
            }

            // The last two parameters are later multiplied by block size in the dmc constructor
            std::cout << " - SSEQR config set to (" << dmc->sseqr_window_size << ", " << dmc->sseqr_accept_range
                << ", " << dmc->sseqr_admit_dist << "), units (I/O count, blocks, blocks)\n";
        },
        "sseqr_config"
    },

    {
        [](dmcache *dmc, const std::string &value) {
            if (std::stoi(value) == 1) {
                dmc->sequential_flagging = true;
                std::cout << " - Cacheblocks will be marked as sequential\n";
            }
        },
        "seq_flagging"
    },

    /* For access pattern analysis */
    {
        [](dmcache *dmc, const std::string &value) {
            std::cout << " - access pattern analysis logs will be generated\n";
            std::string name = value.substr(1, value.length());

            dmc->ap_gc_iters_logger_file = new std::ofstream(name + "_ap_gc.log", std::ios_base::out);
            dmc->ap_ios_logger_file      = new std::ofstream(name + "_ap_io.log", std::ios_base::out);
            dmc->ap_flush_logger_file    = nullptr; // Set on each GC iteration
            dmc->dump_access_pattern = true;
        },
        "ap_analysis"
    },
};

/*
 * The parameter line is the entire line from the configuration file to be parsed. Returns
 *      one if the argument comes from argv for the time being.
 */
void dmcache::parse_conf_option(const std::string &line) {

    std::string param = line.substr(0, line.find_first_of(' ')), value;
    value = line.substr(line.find_last_of(' '), line.length() - 1);

    for (auto opt : opts_table) {
        if (param == opt.opt_name)
            opt.func(this, value);
    }
}

/*
 * This iterates the entries in the arg_map and calls the corresponding function in the opts_table
 *      if it exists. This is NOT to be called before parse_conf_option to ensure that command line
 *      arguments take precedence over config file values.
 */
void dmcache::parse_argv_options(const std::unordered_map<std::string, std::string> &arg_map)
{
    for (auto t : arg_map) {
        std::string value = " " + t.second;
        for (auto opt : opts_table) {
            if (t.first == opt.opt_name)
                opt.func(this, value);
        }
    }
}
