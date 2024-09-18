#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <algorithm>
#include <fstream>
#include <tuple>
#include <map>
#include <queue>
#include <atomic>
#include <iostream>
#include <sstream>
#include <set>
#include "blktrace.hpp"
#include "bitops.hpp"
#include "list.hpp"
#include "zbd.hpp"
#include "io.hpp"
#include "fio_trace_parser.hpp"

/* Change to 1 to enable the checks for the lru_states. Be warned, it's slow */
#if(0)
#define ENSURE_LRU_STATES() validate_lru_states_or_exit(__func__, __LINE__)
#else /* ENSURE_LRU_STATES */
#define ENSURE_LRU_STATES()
#endif /* ENSURE_LRU_STATES */

#define CACHE_ADMISSION_ENABLED     0       /* Enables the Cache Admission Policy*/
#define RWSS_ACCESS_THRESHOLD       1       /* The number of accesses a block must have before it is considered for cache admission */
#define STAGING_AREA_SIZE           1102848000

#define SSEQR_OPTIMIZATION_NONE     0
#define SSEQR_OPTIMIZATION_MAP      1       /* Use hash map lookup for end sector, but lose functionality of accept range */
#define SSEQR_OPTIMIZATION_TREE     2       /* Use binary tree lookup for end sector, keep non-zero accept range functionality */
#define SSEQR_OPTIMIZATION          SSEQR_OPTIMIZATION_TREE

/* States of a cache block */
#define INVALID         0
#define VALID           1           /* Valid */
#define RESERVED        2           /* Allocated but data not in place yet */
#define DIRTY           4           /* Locally modified */
#define WRITEBACK       8           /* In the process of write back */
#define WRITETHROUGH    16          /* In the process of write through */
#define WRITEALLOCATE   32          /* In the process of write allocate */

#define is_state(x, y)      (x & y)
#define set_state(x, y)     (x |= y)
#define clear_state(x, y)   (x &= ~y)
#define put_state(x, y)     (x = y)

#define NUM_WRITE_HEADS         1
#define WRITE_HEADS_BLOCK_SIZE  1

#define FETCH_INC(val) val++

#if((SSEQR_OPTIMIZATION==SSEQR_OPTIMIZATION_MAP) || (SSEQR_OPTIMIZATION==SSEQR_OPTIMIZATION_TREE))
#define UPDATE_SSEQR_HASH(old_end_addr, new_end_addr, scan)     \
    do {                                                        \
        sseqr_hash.erase(old_end_addr);                         \
        sseqr_hash[new_end_addr] = scan;                        \
    } while (0)
#else
#define UPDATE_SSEQR_HASH(...)
#endif

#define SSEQR_SHOULD_ADMIT(scan, admit_dist) \
    ( scan->data->end_addr - scan->data->start_addr <= admit_dist )
#define IS_ACCESS_SEQUENTIAL(scan, admit_dist) \
    ( scan->data->end_addr - scan->data->start_addr >= admit_dist )

struct sseqr_range;
struct dmcacheGC;   

struct relocation_stats {
    uint64_t relocation_count = 0;                      /* Tracks the number of times this block was relocated */
    uint64_t relocation_read_hits = 0;                  /* Tracks the number of times this block experienced a read hit after being relocated */
    uint64_t relocation_write_hits = 0;                 /* Tracks the number of times this block experienced a write hit after being relocated */
    uint64_t relocation_misses_after_lru_eviction = 0;  /* Tracks the number of times a block was relocated, then evicted, and then resulted in a cache miss */
};

struct cacheblock_stats {
    uint64_t accesses = 0;
    uint64_t post_relocation_accesses = 0;
};

struct cacheblock {

    list_container<cacheblock> list;                /* For keeping track of this block in the lru */
    uint32_t src_block_addr;                        /* Block idx of the cacheblock on the source dev */
    uint32_t cache_block_addr;                      /* Block idx of the cacheblock on the cache dev */
    uint32_t freq_of_access;                        /* Tracks the frequency of access for this block */
    unsigned short state;   	                    /* State of a block */

    // Moved into the flags variable so they only take 1 byte in total
    // bool relocated = false;                         /* True if this block was relocated. Used for stats */
    // bool can_count_unique_hit = false;              /* Tracks if this block has been counted for unique relocation hits already */
    // bool relocation_read_hits;                  /* Tracks the number of times this block experienced a read hit after being relocated */
    // bool relocation_write_hits;                 /* Tracks the number of times this block experienced a write hit after being relocated */

    enum lru_states {
        before_thresh ,
        on_thresh     ,
        after_thresh  ,
    };

    uint64_t last_access = 0;
    uint64_t ts_score = 0;

    lru_states lru_state;
    unsigned char flags = 0;

    bool is_sequential = false;
    bool is_reaccessed = false;

    sseqr_range *scan = nullptr;

private:

    inline void set(bool value, int idx) {
        if (value) {
            flags |= (1 << idx);
        } else {
            flags &= ~(1 << idx);
        }
    }

    inline bool get(int idx) {
        return flags & (1 << idx);
    }

public:
    inline void set_relocated(bool value) {
        set(value, 0);
    }
    inline void set_can_count_unique_hit(bool value) {
        set(value, 1);
    }
    inline void set_relocation_read_hit(bool value) {
        set(value, 2);
    }
    inline void set_relocation_write_hit(bool value) {
        set(value, 3);
    }

    inline bool get_relocated() {
        return get(0);
    }
    inline bool get_can_count_unique_hit() {
        return get(1);
    }
    inline bool get_relocation_read_hit() {
        return get(2);
    }
    inline bool get_relocation_write_hit() {
        return get(3);
    }
};

struct staged_block {
    list_container<staged_block> list;              /* For keeping track of this block in the lru */
    uint64_t src_block_addr;                        /* Block idx of the cacheblock on the source dev */
    uint64_t freq_of_access;                        /* Tracks the frequency of access for this block */

    enum staged_block_type {
        DATA_REF ,
        REF_ONLY ,
    };
    staged_block_type type;
    void *data = nullptr;                           /* Just for show for the time being. Will remain a nullptr  */
};

struct sseqr_range {
    list_container<sseqr_range> list;
    bool in_use = false;

    uint64_t start_addr = 0;
    uint64_t end_addr   = 0;
    uint64_t total_ios  = 0;

    bool is_long_scan      = false;                    /* Has the range of this scan (end_sect - start_sect) exceeded X? */
    uint64_t t_s        = 0;                        /* The timestamp at which the scan begins */
    uint64_t t_x        = 0;                        /* The timestamp at which the scan becomes a long scan */
};

struct dmcache {

public:
    void log_gc_count_vs_io_count();
    uint64_t temp = 0;
    std::set<uint64_t> wss;                                      /* WSS of the workload being parsed*/
    std::set<uint64_t> rwss;                                     /* RWSS of the workload being parsed*/
    std::set<uint64_t> wss_t;                                    /* WSS of the workload in the last t time units*/
    std::set<uint64_t> rwss_t;                                   /* RWSS of the workload in the last t time units*/
    std::set<uint64_t> evicted_t;                                /* The set of blocks evicted, which were part of the RWSS
                                                                  * in the last t time units */
    std::vector<uint64_t> rwss_t_threshold_vec;                  /* Vector of changing threshold over the window interval*/

    std::unique_ptr<fio_trace_parser> fio_parser;               /* For reading Fio traces, if selected */
    std::uint64_t skip = 0, limit = 0;

    blktrace_parser *parser;                                    /* For obtaining io_info structs from the trace */
    /* NOTE: dmsetup emulation often complicated things and sometimes behaved differently across
     *      different setups */
    bool emulate_dmsetup = false;                               /* Enable or disable dmsetup emulation */
    bool use_fio_traces = false;                                /* Enable or disable the fio trace reader */

    /* The struct and map below are just for keeping track of certain metadata and also which cacheblock
     * is associated with a particular IO so that when the complete event comes through, all the
     * cacheblock metadata and logging can be updated/logged appropriately 
     */
    struct incomplete_io_meta {
        enum OPERATION_TYPE {
            OP_READ_HIT   ,
            OP_WRITE_HIT  ,
            OP_READ_MISS  ,
            OP_WRITE_MISS ,
        } op_type;
        uint64_t sector1, sector2, lru_index, counter;
        cacheblock *block_ptr;
    };
    
    std::map<io_info, incomplete_io_meta> incomplete_io;
    std::unordered_map<uint64_t, cacheblock*> map;                              /* Takes place of the radix tree */
    std::unordered_map<uint64_t, staged_block*> staging_map;                    /* Acts as a staging area for Cache */
    std::unordered_map<uint64_t, relocation_stats> relocated_block_stats;       /* Stores cacheblock stats per src address */
    std::unordered_map<uint64_t, cacheblock*> relocated_map;                    /* Stores cacheblocks that have been relocated */ 
    std::unordered_map<uint64_t, cacheblock_stats> block_stats;                 /* Global per cacheblock statistics */

#if((SSEQR_OPTIMIZATION==SSEQR_OPTIMIZATION_MAP) || (SSEQR_OPTIMIZATION==SSEQR_OPTIMIZATION_TREE))
    /* To avoid traversing nodes in the list to find a start end pair of LBAs
     * which fit a particular sequence, we can store the end sector and perform
     * a lookup on the start sectors of incoming blocks. By doing this, the
     * second SSEQR argument (sseqr_accept_range) can no longer be used. In
     * effect, using this optimization forces this argument to be zero */
    std::map<std::uint64_t, list_container<sseqr_range>*> sseqr_hash;
#endif

    bool sequential_flagging = false;
    std::uint64_t sseqr_window_size  = 8;
    std::uint64_t sseqr_accept_range = 8;
    std::uint64_t sseqr_admit_dist   = 16;
    list<sseqr_range> sseqr_ranges;

    std::pair<bool, std::uint8_t> should_contribute_ts_score_bk(const cacheblock *block) const;
    std::uint64_t limit_blk_ts_score_bk(const cacheblock *block) const;
    bool cache_admission_enabled = CACHE_ADMISSION_ENABLED; /* Enables the Cache Admission Policy */

    list<cacheblock> lru;                               /* Representation of the LRU list for the cacheblocks */
    list<staged_block> staging_lru;                     /* Representation of the LRU list for the cache admission */
    zbd* cache_device;                                  /* Representation of the cache device */

    uint64_t base_dev_size;                             /* Base device size in sectors */
    uint64_t org_cache_size;                            /* Cache size in sectors including OP */
    uint64_t cache_size;                                /* Cache size in sectors excluding OP */
    uint64_t staging_area_size;                         /* Staging area size in sectors */
    uint32_t bits;                                      /* Cache size in bits */
    uint32_t block_size;                                /* Cache block_size */
    uint32_t block_shift;                               /* Cache block size in bits */
    uint32_t block_mask;                                /* Cache block mask */

    bool cache_dev_is_zoned = false;                    /* Determines whether to emulate ZBD or regular */
    uint64_t nr_zones;                                  /* Total number of zones of the cache device */

    uint64_t start_seq_write_zones;                     /* Starting sector for SWR or SWP zones */
    uint64_t zone_size_sectors;                         /* Cache device zone size in sectors */
    uint64_t zone_cap_sectors;                          /* Cache device zone capacity in sectors */
    uint64_t blocks_per_zone;                           /* Number of usable cache blocks per zone */

    bitmap *bm_full_zones;                              /* Bitmap indicating full zones */

    uint32_t current_write_head;                        /* Index of the current zone being used for writing */
    uint32_t current_zones[NUM_WRITE_HEADS];            /* Array of current zones which is currently being used for writing by the multi heads*/

    uint32_t zone_batch_size;     
    uint64_t wp_block;              	                /* Counter used to track how much space is used in the current zone */

    uint64_t cache_dev_max_open_zones;                  /* Max. Number of Open Zones */
    uint64_t cache_dev_max_active_zones;                /* Max. Number of Active Zones */

    uint64_t active_zones_max;                          /* Stores the maximum number of active zones */
    uint64_t active_zones_count;                        /* Stores the current number of active zones */
    std::vector<uint64_t> active_zones;                 /* Active zone queue*/

    void init_dmcache_lru();
    void validate_lru_states_or_exit(const char *func, int lineno) const;

    /* Both of these functions exist to update the state information within each cacheblock
     *      regarding where they are relative to the lru_thresh in the lru. It will also
     *      "advance" or "retreat" the lru_thresh pointer when appropriate.
     *      ...
     * NOTE: Both of these are to be called only before doing any lru modifications */
    bool advance_lru_thresh(list_container<cacheblock> *block);
    bool retreat_lru_thresh(list_container<cacheblock> *block);

    void move_blk_lru_bk(list_container<cacheblock> *block, cacheblock::lru_states new_state);
    void move_lru_thresh(uint64_t new_pos);

    void readjust_lru_thresh(uint64_t zone_idx);
    uint64_t lru_readjust_dist = 0;
    bool lru_readjust_enabled = false;

    list_container<cacheblock> *lru_thresh;             /* Pointer to the cacheblock in the LRU denoting the eviction threshold */
    uint64_t window_number = 0;

    float max_lru_thresh_percent = 0.90f;
    bool max_lru_thresh_enabled = false;

    /* BEWARE: This is increased for both max_lru_thresh and lru_readjust_dist. Do not run these two
     *   at the same time */
    uint64_t lru_thresh_readjustments = 0;

    bool dump_zbd_metadata = false;                     /* Wether or not to dump zbd metadata in destructor */
    bool dump_bitmaps      = false;                     /* Wether or not to dump bitmap contents to a bin file */

    std::ofstream *zone_stats_logger_file;              /* For logging zone stats towards the end of the test*/
    std::ofstream *per_zone_stats_logger_file;          /* For logging per zone stats for each victim selection; per gc iterations*/
    std::ofstream *cblock_stats_logger_file;            /* For logging cacheblock stats */
    std::ofstream *relocation_stats_logger_file;        /* For logging relocated cacheblock stats */
    std::ofstream *rwss_t_logger_file;                  /* For logging dynamic rwss*/

    /* For plotting sector against timestamp */
    std::ofstream *ap_gc_iters_logger_file;
    std::ofstream *ap_ios_logger_file;
    std::ofstream *ap_flush_logger_file;
    uint64_t ap_current_gc_iter = 0;
    bool dump_access_pattern = false;

    void ap_log_io(uint64_t sector, uint64_t score, bool is_sequential);
    void ap_log_flush(uint64_t iter, uint64_t sector);
    void ap_log_gc();

    struct {
        uint64_t reads          = 0;
        uint64_t writes         = 0;
        uint64_t cache_hits     = 0;
        uint64_t cache_misses   = 0;
        uint64_t read_misses    = 0;
        uint64_t write_misses   = 0;
        uint64_t read_hits      = 0;
        uint64_t write_hits     = 0;
        uint64_t allocates      = 0;
        uint64_t replaces       = 0;
        uint64_t inserts        = 0;
        uint64_t dirty_blocks   = 0;
        uint64_t write_backs    = 0;
    } stats;

    struct {
        uint64_t reads_skipped  = 0;
        uint64_t writes_skipped = 0;

        uint64_t max_scan_length = 0;
        uint64_t min_scan_length = 0;

        /* These are useful for calculating
         * the average scan length */
        uint64_t scan_length_sum = 0;
        uint64_t num_scans       = 0;

        uint64_t total_flagged   = 0;
    } seq_io_stats;

    struct {
        uint64_t reads          = 0;
        uint64_t writes         = 0;
        uint64_t allocates      = 0;
        uint64_t replaces       = 0;
        uint64_t inserts        = 0;
    } staging_stats;

    struct {
        double avg_num_accesses_all_blocks          = 0;
        double avg_num_accesses_before_relocation   = 0;
        double avg_num_accesses_after_relocation    = 0;
        double ratio_avg_accesses_before_over_after = 0;
        double avg_num_accesses_relocated_blocks    = 0;
    } avg_access_stats;

    struct {
        uint64_t no_reused_blocks_t_saved    = 0;
        uint64_t no_unused_blocks_t_saved    = 0;
        uint64_t no_reused_blocks_t_flushed  = 0; 
        uint64_t no_unused_blocks_t_flushed  = 0;
    } rwss_t_stats;
    
    bool stats_logging_enabled          = false;
    bool victim_stats_logging_enabled   = false;

    /* For dumping all stats to a single CSV or stderr across multiple runs of the simulator */
    std::string automation_logging_filename, trace_file_name;
    bool automation_logging = false, automation_streaming = false;

    void do_automation_print(std::ostream &out);
    void do_automation_logging();

    std::pair<bool, cacheblock *>request_cache_block();
    std::pair<bool, staged_block *>request_staging_area();

    struct dmcacheGC* gc;
    void print_dmcache_configuration();
    void print_dmcache_results_stats();
    void log_stats();
    void log_cacheblock_stats();
    void log_victim_zone_stats(uint64_t starting_block_idx);
    void log_rwss(uint64_t rwss);
    void log_rwss_t(uint64_t rwss, uint64_t relocation_lru_thresh, double hit_rate, double waf);
    void cacheblock_update_state(struct cacheblock *cacheblock);
    void finish_zone_append(struct cacheblock *cacheblock);
    void reset_zbd_write_heads();
    void prep_for_write(cacheblock *block, bool mark_old_gcable, bool is_gc_active, bool cache_hit);
    uint64_t select_new_empty_zone();
    void handle_blk_ta_complete(io_info &io);           /* Process a __BLK_TA_COMPLETE event */
    bool handle_blk_ta_queue(io_info &io);              /* Process a __BLK_TA_QUEUE event */
    void cache_hit(struct cacheblock *block, io_info &io, bool is_sequential);
    void cache_miss(io_info &io, bool is_sequential);
    void gc_zone_metadata_updated(dmcache* dmc);

    bool (dmcache::*cache_admission)(io_info &io) = &dmcache::cache_admission_addr_staging;
    bool cache_admission_addr_staging(io_info &io);
    bool cache_admission_data_staging(io_info &io);
    bool cache_admission_sseqr(io_info &io); // Selective Sequential Rejection
    bool update_sseqr_window(io_info &io);
    void track_sseqr_scan_stats(list_container<sseqr_range> *scan);
    void admit_to_cache(io_info &io);
    void update_zone_timestamp_stats(struct cacheblock *block);
    void update_wss_rwss(uint64_t offset); 
    void update_zone_recency(uint64_t zone_idx, int tagged_for_garbage);
    void update_ts_score_bk(struct cacheblock *block, uint64_t prev_ts, bool prev_seq, bool is_read);

    /* These conversions are almost an exact copy paste from the original dm-cache */
    inline uint64_t sector_addr_to_block_idx(uint64_t addr) const {

        if (cache_dev_is_zoned) {
            uint32_t zone_idx = sector_addr_to_zone_idx(addr);
            uint64_t zone_starting_sector = zone_idx_to_sector_addr(zone_idx);
            return ((addr - zone_starting_sector) >> block_shift) + zone_idx_to_block_idx(zone_idx);
        }

        /* For non-ZBD cache devices */
        return addr >> block_shift;
    }
 
    inline uint32_t sector_addr_to_zone_idx(uint64_t addr) const {

        return (addr - start_seq_write_zones) / zone_size_sectors;
    }
    inline uint64_t block_idx_to_sector_addr(uint64_t idx) const {

        if (cache_dev_is_zoned) {
            uint64_t zone_block_offset = idx % blocks_per_zone;
            return zone_idx_to_sector_addr(block_idx_to_zone_idx(idx)) + (zone_block_offset << block_shift);
        }

        /* For non-ZBD cache devices */
        return idx << block_shift;
    }

    inline uint64_t block_idx_to_zone_idx(uint64_t idx) const {
        return idx / blocks_per_zone;
    }

    inline uint64_t zone_idx_to_sector_addr(uint32_t idx) const {
        return start_seq_write_zones + zone_size_sectors * idx;
    }

    inline uint64_t zone_idx_to_block_idx(uint32_t idx) const {
        return blocks_per_zone * idx;
    }

    inline uint64_t get_active_zones_count() {
        // iterate through the active zones and count them
        uint64_t count = 0;
        auto it = active_zones.begin();
        for (; it != active_zones.end(); ++it)
            count++;
        return count;
    }

    inline uint64_t read_active_zone() {
        if(!active_zones.empty())
            return active_zones.front();
        else
            return -1;
    }

    inline bool active_zones_insert(uint64_t zone_idx) {
        if (active_zones_count >= active_zones_max)
            return false;

        active_zones.push_back(zone_idx);
        active_zones_count++;
        return true;
    }

    inline void active_zones_remove(uint64_t zone_idx) {
        auto it = std::find(active_zones.begin(), active_zones.end(), zone_idx);
        if (it != active_zones.end()) {
            active_zones.erase(it);
            active_zones_count--;
        }
    }

    inline bool active_zones_contains(uint64_t zone_idx) {
        auto it = std::find(active_zones.begin(), active_zones.end(), zone_idx);
        if (it != active_zones.end())
            return true;
        else
            return false;
    }

    inline void active_zones_print() {
        auto it = active_zones.begin();
        std::cout << "Active Zones: ";
        for (; it != active_zones.end(); ++it)
            std::cout << *it << " " ;
        std::cout << std::endl;
    }

    inline uint64_t get_timestamp() const {
        return timestamp_counter;
    }

    dmcache(const std::unordered_map<std::string, std::string> &arg_map, std::vector<std::ifstream> &trace_files, const std::string &trace_name);
    ~dmcache();

    void run(); /* Will begin processing the traces */

private:

    /* For operation logging*/
    uint64_t timestamp_counter = 0;
    void log_write_miss(uint64_t io, uint64_t cache, uint64_t garbage, uint64_t timestamp);
    void log_write_hit(uint64_t io, uint64_t lru_idx, uint64_t old_cache, uint64_t new_cache, uint64_t timestamp);
    void log_read_miss(uint64_t io, uint64_t cache, uint64_t garbage, uint64_t timestamp);
    void log_read_hit(uint64_t io, uint64_t lru_idx, uint64_t cache, uint64_t timestamp);

    bool op_logging_enabled = false;
    std::ofstream *op_logger_file;

    std::ofstream *stats_logger_file;
    long stats_logging_frequency = 1;
    long stats_logging_counter = 0;

    /*For configuration file parsing*/
    struct option {
        void (*func)(dmcache *dmc, const std::string &value);
        const char *opt_name;
    };

    void parse_argv_options(const std::unordered_map<std::string, std::string> &arg_map);
    void parse_conf_option(const std::string &line);
    static const option opts_table[];

};

/* Debugging macros */
#define DEBUG_MODE 0

#if DEBUG_MODE
#define DBUG(...) \
do { \
    std::stringstream ss; \
    ss << "DEBUG: " << __FILE__ << ":" << __LINE__ << ": "; \
    display_debug_message(ss.str(), ##__VA_ARGS__); \
} while(0)
#else
#define DBUG(...)
#endif

template<typename... Args>
void display_debug_message(const std::string& msg, Args&&... args) {
    std::stringstream ss;
    ss << msg;
    int dummy[sizeof...(args)] = { (ss << std::forward<Args>(args), 0)... };
    std::cout << ss.str() << std::flush;
}
