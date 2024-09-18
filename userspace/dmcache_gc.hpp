#ifndef GC_GUARD
#define GC_GUARD

#include "bitops.hpp"
#include "dmcache.hpp"
#include <iostream>
#include <thread>
#include <unistd.h>
#include <numeric>
#include <unordered_map>
#include <unordered_set>
#include <cmath>

/* Garbage collection */
#define GC_ENABLED 1 // Enable/disable GC

/* 0: Eviction GC - NOTE: Not tested in a long time. Probably doesn't work anymore
   1: Relocation GC */
#define EVICTION_GC 0
#define RELOCATION_GC 1
#define GC_MODE RELOCATION_GC

/* 0: RANDOM_SELECTION      - Choose a random full zone
 * 1: GREEDY_SELECTION      - Choose the most full zone
 * 2: GREEDY_INV_SELECTION  - Choose the least full zone */
#define RANDOM_SELECTION            0
#define GREEDY_SELECTION            1
#define TS_SCORE_SELECTION          2
#define TS_RECENCY_SELECTION        3
#define LRU_RECENCY_SELECTION       4
#define LOCALITY_SELECTION          5
#define TS_SEQUENTIAL_SELECTION     6
#define VICTIM_SELECTION_POLICY GREEDY_SELECTION

#define GARBAGE_TRIGGER           0
#define FREE_SPACE_TRIGGER        1
#define GC_TRIGGER_MODE FREE_SPACE_TRIGGER

#define RELOCATE_EVERYTHING       0                 /* No threshold*/
#define RELOCATE_ON_HIT_COUNT     1                 /* Static Threshold:  set wrt to the hit count of the relocated cacheblocks */
#define RELOCATE_ON_HIT_RATIO     2                 /* Static Threshold:  set wrt to the hit ratio of the relocated cacheblocks */
#define RELOCATE_ON_TS_RECENCY    3                 /* Static Threshold:  set wrt to the global timestamp */
#define RELOCATE_ON_LRU_RECENCY   4                 /* Static Threshold:  set wrt to the lru size */
#define RELOCATE_ON_STATIC_RWSS   5                 /* Static Threshold:  set as the RWSS of the entire workload */
#define RELOCATE_ON_DYNAMIC_RWSS  6                 /* Dynamic Threshold: window interval is set as the RSS of the entire workload and rwss_(t) = rwss_t(t-1) */
#define GC_RELOCATION_MODE RELOCATE_EVERYTHING 

#define USE_GROUND_TRUTH_RWSS_T             0       /* Instead of predicting the threshold values; use the ground truth */
#define RELOCATION_FREQ_THRESHOLD           1
#define RELOCATION_TIMESTAMP_THRESHOLD      10
#define RELOCATION_LRU_THRESHOLD            1
#define EFFECTIVE_RELOCATION_RATIO          0.01

#define RWSS_PERCENTAGE             1
#define WINDOW_PERCENTAGE           1

#define RWSS_WINDOW_INTERVAL        42854787         /* At every nth of this window size; th threshold is reset to the dynamic RWSS */
#define ENTIRE_WORKLOAD_RWSS        42854787         /* The entire workload RWSS */

// Controls the overprovisioned space for GC
#define GC_OP_PERCENTAGE 10

// validate GC_OP_PERCENTAGE  total_blocks * 70%  * 50%

// Controls the gap sizes within the OP space. These 3 values must add to 100%
#define GC_OP_ACCEPTABLE_GARBAGE_PERCENTAGE 10
#define GC_OP_ACCEPTABLE_FREE_SPACE_PERCENTAGE 90
#define GC_OP_STOP_START_GAP_PERCENTAGE 0
#define GC_OP_START_BLOCK_GAP_PERCENTAGE 90


/* When there are this many free zones left, I/O blocking will be enabled */
#define BLOCK_IOS_FREE_ZONES 0
#define GC_RELOCATION_ASYNC_LIMIT 16                /* The max iodepth used when performing cacheblock relocation in GC */

// Macro to convert the relocation mode to a string message
#define VICTIM_SELECTION_MODE_STRING(mode)                       \
    ((mode) == RANDOM_SELECTION ? "RANDOM" :                     \
    ((mode) == GREEDY_SELECTION ? "GREEDY" :                     \
    ((mode) == TS_SCORE_SELECTION ? "TS_SCORE_SELECTION" :       \
    ((mode) == TS_RECENCY_SELECTION ? "TS_RECENCY" :             \
    ((mode) == LRU_RECENCY_SELECTION ? "LRU_RECENCY" : "UNKNOWN_MODE")))))

// Macro to convert the relocation mode to a string message
#define RELOCATION_MODE_STRING(mode)                                    \
    ((mode) == RELOCATE_EVERYTHING ? "RELOCATE_EVERYTHING" :            \
    ((mode) == RELOCATE_ON_HIT_COUNT ? "RELOCATE_ON_HIT_COUNT" :        \
    ((mode) == RELOCATE_ON_HIT_RATIO ? "RELOCATE_ON_HIT_RATIO" :        \
    ((mode) == RELOCATE_ON_TS_RECENCY ? "RELOCATE_ON_TS_RECENCY" :      \
    ((mode) == RELOCATE_ON_LRU_RECENCY ? "RELOCATE_ON_LRU_RECENCY" :    \
    ((mode) == RELOCATE_ON_STATIC_RWSS ? "RELOCATE_ON_STATIC_RWSS" :    \
    ((mode) == RELOCATE_ON_DYNAMIC_RWSS ? "RELOCATE_ON_DYNAMIC_RWSS" : "UNKNOWN_MODE")))))))

// Macro to print the appropriate threshold value for a given mode
#define DISPLAY_THRESHOLD_FOR_MODE(mode) \
    do { \
        std::cout << "- Threshold values: "; \
        if (mode == RELOCATE_EVERYTHING)            std::cout << "N/A"; \
        else if (mode == RELOCATE_ON_HIT_COUNT)     std::cout << "RELOCATION_FREQ_THRESHOLD:" << RELOCATION_FREQ_THRESHOLD; \
        else if (mode == RELOCATE_ON_HIT_RATIO)     std::cout << "EFFECTIVE_RELOCATION_RATIO:" << EFFECTIVE_RELOCATION_RATIO; \
        else if (mode == RELOCATE_ON_TS_RECENCY)    std::cout << "RELOCATION_TIMESTAMP_THRESHOLD:" << gc->relocation_timestamp_thresh; \
        else if (mode == RELOCATE_ON_LRU_RECENCY)   std::cout << "RELOCATION_LRU_THRESHOLD:" << gc->relocation_lru_thresh; \
        else if (mode == RELOCATE_ON_STATIC_RWSS)   std::cout << "WORKLOAD_RWSS:" << gc->rwss_workload << " RWSS_PERCENTAGE:" << gc->rwss_percentage; \
        else if (mode == RELOCATE_ON_DYNAMIC_RWSS)  std::cout << "WINDOW INTERVAL:" << gc->rwsst_window_interval  << " WINDOW_PERCENTAGE:"  << gc->window_percentage <<" RWSS_PERCENTAGE:" << gc->rwss_percentage; \
        else std::cout << "UNKNOWN_THRESHOLD"; \
        std::cout << std::endl; \
    } while(0)

// ----------- Forward declarations -----------
// Defined in dm-cache.h
struct dmcache;
struct cacheblock;

typedef struct {
    int     idx;
    char    name[10];
    std::vector<uint64_t> rwss_t;
}baseline_rwss_t;

typedef enum {
    DEV_44_WS_100,
    DEV_44_WS_75,
    DEV_44_WS_50,
    DEV_44_WS_25,
    DEV_44_WS_12_5,
    DEV_369_WS_100,
    DEV_369_WS_75,
    DEV_369_WS_50,
    DEV_369_WS_25,
    DEV_369_WS_12_5,
    DEV_714_WS_100,
    DEV_714_WS_75,
    DEV_714_WS_50,
    DEV_714_WS_25,
    DEV_714_WS_12_5,
    DEV_792_WS_100,
    DEV_792_WS_75,
    DEV_792_WS_50,
    DEV_792_WS_25,
    DEV_792_WS_12_5,
    DEV_79_WS_100,
    DEV_79_WS_75,
    DEV_79_WS_50,
    DEV_79_WS_25,
    DEV_79_WS_12_5,
    MAX_DEV_NUM
}DEVICE_NUMBER;

#define TARGET_DEVICE DEV_44_WS_100

struct zone_stats {
    uint64_t reclaim_count;                         /* How many times has this zone been reclaimed by GC */
    uint64_t gcable_count;                          /* Number of grabage blocks exist in this current zone */
    uint64_t ts_score;

    uint64_t threshold;
    uint64_t num_before;

    uint64_t max_timestamp;                         /* The maximum ts of the valid data blocks in a zone */
    uint64_t min_timestamp;                         /* The minimum ts of the valid data blocks in a zone */

    uint64_t blocks_flushed;                        /* How many number of valid blocks flushed in a zone; during relocation mode*/
    uint64_t blocks_relocated;

    double avg_garbage;
    double avg_ts_score;
    double zone_score;
};

struct relocation_job
{
    struct cacheblock* cacheblock;
    bool is_read;                                   /* True if read, false if write */
    uint64_t bi_sector;
    zbd* cache_device;                              /* Representation of the cache device */
};

struct dmcacheGC {

public:

    baseline_rwss_t *baseline_rwss_t_data;          /* Baseline rwss_t values for device workloads*/
    uint64_t total_zone_resets = 0;

    bool is_gc_active;                              /* True if GC is currently active */
    int gc_iter_count;                              /* The number of times GC has been run */
    bitmap* bm_gcable_blocks;                       /* Bitmap indicating garbage collectable blocks */
    uint64_t gc_free_zones_target;                  /* The number of zones that the GC would like to keep free */
    struct cacheblock **zbd_cacheblocks;            /* Array containing cacheblock metadata, relative to it's location on the cache device */
    // void *eviction_buffer;                          /* Memory buffer for GC to perform cacheblock flush operations during eviction */
    std::atomic<bool> block_ios{false};             /* Atomic boolean value for determining if I/O operations should be blocked */

    /* ------ GC configuration ----- */

    uint64_t gc_op_acceptable_garbage_percentage;
    uint64_t gc_op_stop_start_gap_percentage;
    uint64_t gc_op_start_block_gap_percentage;
    int gc_op_percentage;

    uint64_t gc_op_acceptable_free_space_percentage;

    int gc_mode;
    int gc_victim_sel_mode                      = VICTIM_SELECTION_POLICY;
    int gc_relocation_mode;
    int gc_trigger_mode;
    int window_intr_idx;
    double rwss_percentage                      = RWSS_PERCENTAGE;
    double window_percentage                    = WINDOW_PERCENTAGE;
    
    /* -------- GC Triggers -------- */
    uint64_t gc_total_garbage_count = 0;
    uint64_t gc_garbage_count;                      /* The total number of gcable cacheblocks */
    uint64_t gc_trigger_start;                      /* Once the garbage count reaches this amount, GC will begin reclaiming zones */  
    uint64_t gc_trigger_stop;                       /* Once GC is able to reduce garbage to this amount, GC will stop reclaiming zones */
    uint64_t gc_trigger_block_io;                   /* After the garbge count exceeds this amount, new I/O operations will be blocked */
    inline uint64_t gc_trigger_count(dmcache *dmc); /* Returns either garbage count or amount of free space */

    uint64_t calc_free_space(dmcache *dmc);  /* TODO: Use counter based method instead, reduce likelyhood of bugs in kernel */
    bool should_continue_gc(dmcache *dmc);
    bool should_start_gc(dmcache *dmc);

    /* ----- Relocation Config ----- */
    std::uint64_t relocation_timestamp_thresh   = RELOCATION_TIMESTAMP_THRESHOLD;
    std::uint64_t relocation_lru_thresh;
    std::uint64_t rwss_workload;                        /* The RWSS of the entire workload*/
    std::uint64_t rwsst_window_interval         = RWSS_WINDOW_INTERVAL;

    /* -------- GC Statistics -------- */
    struct zone_stats **per_zone_stats;
    uint64_t relocated_cacheblocks_count        = 0;    /* Total number of cacheblocks relocated */
    uint64_t relocated_unique_cacheblocks_count = 0;    /* Total number of unique cacheblocks relocated */
    uint64_t evicted_cacheblocks_count          = 0;    /* Total number of cacheblocks evicted */
    uint64_t unique_relocation_hits_count       = 0;    /* Total number of unique relocation hits*/
    uint64_t relocation_hits_count              = 0;    /* Total number of relocation hits both Read and Write*/
    uint64_t relocation_read_hits_count         = 0;    /* Total number of relocation hits Read*/
    uint64_t relocation_write_hits_count        = 0;    /* Total number of relocation hits Write*/
    uint64_t relocations_no_read_hit            = 0;    /* Relocations which resulted in 0 READ hits */
    uint64_t relocations_no_write_hit           = 0;    /* Relocations which resulted in 0 WRITE hits */
    uint64_t relocations_no_read_write_hit      = 0;    /* Relocations which resulted in 0 hits (none at all) */
    uint64_t relocation_misses_after_lru_eviction = 0;  /* Tracks the number of times a block was relocated, then evicted, and then resulted in a cache miss */
    uint64_t eviction_misses_count              = 0;
    uint64_t post_gc_allocates                  = 0;
    uint64_t pre_gc_allocates                   = 0;
    uint64_t post_gc_garbage_count              = 0;
    uint64_t pre_gc_garbage_count               = 0;
    uint64_t num_evicted_periodical             = 0;
    uint64_t num_freed_periodical               = 0;

    std::unordered_set<uint64_t> evicted_blocks;
    std::unordered_set<uint64_t> previously_relocated_blocks_evicted_via_lru;
    std::unordered_set<uint64_t> all_relocated_blocks; /* Set containing any src address that has ever been relocated */
    
    std::tuple<uint64_t, uint64_t> gc_choose_zone_greedy(dmcache *dmc);
    std::tuple<uint64_t, uint64_t> gc_choose_zone_random(dmcache *dmc);
    std::tuple<uint64_t, uint64_t> gc_choose_zone_ts_score(dmcache *dmc);
    std::tuple<uint64_t, uint64_t> gc_choose_zone_ts_score_tradeoff(dmcache *dmc);
    /* opt is short for optimization, 'gc_choose_zone_ts_score_opt' uses book keeping
     * optimization and is otherwise equivalent to 'gc_choose_zone_ts_score' */
    std::tuple<uint64_t, uint64_t> gc_choose_zone_ts_score_opt(dmcache *dmc);
    std::tuple<uint64_t, uint64_t> gc_choose_zone_lru_score(dmcache *dmc);
    std::tuple<uint64_t, uint64_t> gc_choose_zone_lru_recency(dmcache *dmc);

    void init_device_ground_truth_threshold();
    void gc_reset_stats(dmcache *dmc);
    void gc_print_stats(dmcache *dmc);
    uint64_t gc_count_free_zones(struct dmcache* dmc);
    void gc_zone_metadata_updated(dmcache* dmc);
    void do_gc(dmcache* dmc);
    void gc_cacheblock_metadata_updated(dmcache* dmc, struct cacheblock* cacheblock);
    void gc_mark_garbage(dmcache* dmc, uint64_t block_idx);
    void gc_evict_cacheblock(dmcache* dmc, uint64_t block_idx);
    struct cacheblock* gc_fetch_cacheblock(dmcache *dmc, uint64_t block_idx);

    int do_relocation_io(dmcache* dmc, struct relocation_job* job);
    void relocation_endio(dmcache* dmc, struct relocation_job* bio);
    int do_relocation_complete(dmcache* dmc, struct relocation_job* job);
    void gc_relocate_cacheblock(dmcache *dmc, struct cacheblock *cache);

    void set_gc_mode(dmcache *dmc);
    void update_gc_metadata(dmcache* dmc, uint64_t starting_block_idx,
                                    uint64_t gc_zone_idx, uint64_t gc_zone_idx_gcable_count);
    bool is_gc_required(dmcache* dmc, bool gc_mode);
    void reclaim_zone(dmcache* dmc, uint64_t starting_block_idx);

    /* This is intended to be used with the free space trigger when the integration of the
     * block selection and zone selection policy could possibly lead to infinite looping
     * caused by picking a full zone and relocating everything. This ensures that there must
     * be a certain amount of free space freed per iteration minimum.
     * ...
     * As of now, this is only intended to be enabled when using ts_score zone selection with
     * dynamic rwss_t block selection. Additional blocks added are always evicted only if they
     * are after the threshold, and are chosen based on smallest timestamp.  */
    std::set<uint64_t> calc_additional_evictions(dmcache *dmc, uint64_t zone_idx) const;
    std::set<uint64_t> calc_additional_valid_blocks_evictions(dmcache *dmc, uint64_t zone_idx) const;
    bool ensure_min_freed_enabled = false;
    float ensure_min_freed_percent;

    void vs_log(std::uint64_t zone_idx, std::vector<std::pair<uint64_t, uint64_t>> scores, dmcache *dmc);
    void validate_ts_score(std::uint64_t zone_idx, std::uint64_t test_score, dmcache *dmc);

    dmcacheGC(dmcache *dmc, const std::unordered_map<std::string, std::string> &arg_map);
    ~dmcacheGC();  

    inline double calc_zone_heat_score(uint64_t garbage, uint64_t before, uint64_t after) {

    constexpr double garbage_weight =  0.0f;
    constexpr double before_weight  =  1.0f;
    constexpr double after_weight   =  2.0f;

    return (garbage * garbage_weight)
         + (before  * before_weight)
         + (after   * after_weight);
    }

    void log_gc_stats(uint64_t start_garbage, uint64_t end_garbage, uint64_t reclaims);
};
void gc_mark_garbage(dmcache* dmc, uint64_t block_idx);

/* Debugging macros */
#define DEBUG_GC 0

#if DEBUG_GC 
#define DBUG_GC(...) \
do { \
    std::stringstream ss; \
    ss << "DEBUG-GC: " << __FILE__ << ":" << __LINE__ << ": "; \
    display_debug_message(ss.str(), ##__VA_ARGS__); \
} while(0)
#else
#define DBUG_GC(...)
#endif
#endif
