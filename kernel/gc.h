#ifndef GC_GUARD
#define GC_GUARD

#include <asm/atomic.h>
#include <linux/bio.h>
#include <linux/slab.h>
#include <linux/spinlock.h>
#include <linux/workqueue.h>

/* Garbage collection */
#define GC_ENABLED 1                        /* Enable or disable Garbage Collection*/

#define DEBUG_GC 0
#define GC_STATS_DEBUG 0
#define GC_PER_ZONE_DEBUG 0
#define GC_PER_ITER_DEBUG 0

/* 0: Eviction GC
 * 1: Relocation GC */
#define EVICTION_GC 0
#define RELOCATION_GC 1
#define GC_MODE RELOCATION_GC 

#define RELOCATE_EVERYTHING       0                 /* No threshold*/
#define RELOCATE_ON_DYNAMIC_RWSS  1                 /* Dynamic Threshold*/ 
#define GC_RELOCATION_MODE RELOCATE_EVERYTHING 

/* 0: RANDOM_SELECTION              - Choose a random full zone
 * 1: GARBAGE_BASED_SELECTION       - Choose the zone with most garbage zone*/
#define RANDOM_SELECTION            0
#define GARBAGE_BASED_SELECTION     1
#define LOCALITY_BASED_SELECTION    2
#define VICTIM_SELECTION_POLICY GARBAGE_BASED_SELECTION 

#define GC_MIN_JOBS         2048

//#define WINDOW_INTERVAL 1145844	// Device:44           
//#define WINDOW_INTERVAL 13229368 	// Device:792 
//#define WINDOW_INTERVAL 9463403 	// Device:714
#define WINDOW_INTERVAL 1000              /* To be updated as per the trace*/
#define RWSS_PERCENTAGE 1

#define ZBD_CACHEBLOCKS_RADIX_TREE 0  /* 0 - Use C array for cacheblock lookup | 1 - Use Radix tree for GC cacheblock lookup */

#define GC_RELOCATION_ASYNC_LIMIT 4096      /* The max iodepth used when performing cacheblock relocation in GC */
#define GC_FLUSH_ASYNC_LIMIT 128          /* The max iodepth used when performing cacheblock flush in GC */
#define BLOCK_IOS_FREE_ZONES 2            /* When there are this many free zones left, I/O blocking will be enabled */

//+++++++++ Garbage Trigger Macros +++++++++++
// Controls the overprovisioned space for GC
#define GC_OP_PERCENTAGE 10
// Controls the gap sizes within the OP space. These 3 values must add to 100%
#define GC_OP_ACCEPTABLE_FREE_SPACE_PERCENTAGE 90 
#define GC_OP_STOP_START_GAP_PERCENTAGE 0
#define GC_OP_START_BLOCK_GAP_PERCENTAGE 10
//++++++++++++++++++++++++++++++++++++++++++++

// Macro to convert the relocation mode to a string message
#define VICTIM_SELECTION_MODE_STRING(mode)                       \
    ((mode) == RANDOM_SELECTION ? "Random Zone Selection" :                     \
    ((mode) == GARBAGE_BASED_SELECTION ? "Garbage based Zone Selection" :                     \
    ((mode) == LOCALITY_BASED_SELECTION ? "Locality based Zone Selection" : "UNKNOWN_MODE")))

// Macro to display gc moder
#define DISPLAY_GC_INFO(gc) \
    do { \
        if (gc->gc_mode) { \
            DMINFO("- GC Policy: Save"); \
            DMINFO("- GC Block Selection Mode: %s", \
                    ((gc->gc_relocation_mode == RELOCATE_EVERYTHING) ? "Save Everything" : \
                     ((gc->gc_relocation_mode == RELOCATE_ON_DYNAMIC_RWSS) ? "Locality based Block Selection" : "UNKNOWN_MODE"))); \
        } else { \
            DMINFO("- GC Policy: Flush"); \
        } \
    } while (0)


#define GC_STATS_LOCK()      spin_lock(&gc->gc_stats_lock);
#define GC_STATS_UNLOCK()    spin_unlock(&gc->gc_stats_lock);

//++++++++ Macros to manipulate GC statistics +++++++++++
#define GC_STATS_INC(x)         atomic64_inc(&(gc)->x) 
#define GC_STATS_DEC(x)         atomic64_dec(&(gc)->x)
#define GC_STATS_GET(x)        (uint64_t)atomic64_read((&(gc)->x))
#define GC_STATS_SET(x,y)       atomic64_set(&(gc)->x, (y)) 
#define GC_STATS_SUB(x,y)       atomic64_sub((y), (&(gc)->x))

#define ZONE_STATS_LOCK(id)       spin_lock(&gc->per_zone_stats[id].zone_lock)
#define ZONE_STATS_UNLOCK(id)     spin_unlock(&gc->per_zone_stats[id].zone_lock)

//++++++++ Macros to manipulate per zone statistics +++++++++++
#define ZONE_STATS_INC(id, x)     atomic64_inc(&(gc)->per_zone_stats[id].x)
#define ZONE_STATS_DEC(id, x)     atomic64_dec(&(gc)->per_zone_stats[id].x)
#define ZONE_STATS_GET(id, x)     atomic64_read(&(gc)->per_zone_stats[id].x)
#define ZONE_STATS_SET(id, x, y)  atomic64_set(&(gc)->per_zone_stats[id].x, (y))
#define ZONE_STATS_SUB(id, x, y)  atomic64_sub((y), (&(gc)->per_zone_stats[id].x))
#define ZONE_STATS_ADD(id, x, y)  atomic64_add((y), (&(gc)->per_zone_stats[id].x))


//++++++++ Debug Macros  +++++++++++
#if(0)
#define VALIDATE_TS_SCORE(zone_idx, test_score, dmc) \
    validate_ts_score(zone_idx, test_score, dmc)
#else
#define VALIDATE_TS_SCORE(...)
#endif

// ----------- Forward declarations -----------
// Defined in dm-cache.h
struct cache_c;
struct cacheblock;
struct kcached_job;

struct victim_zone {
    u64 zone_idx;
    s64 zone_garbage_count;
};

struct zone_stats {
    spinlock_t zone_lock;
    atomic64_t pending_writes; 
    atomic64_t reclaim_count;                   /* How many times has this zone been reclaimed by GC */
    atomic64_t gcable_count;                    /* Number of grabage blocks exist in this current zone */
    atomic64_t ts_score;                        /* Summation of logical timestamp of all valid blocks in a zone*/
    atomic64_t threshold;                       /* Threshold used in the block selection policy */
    atomic64_t num_before;                      /* Number of valid blocks in a zone which lies before the threshold*/
    atomic64_t min_timestamp;                   /* The min timestamp of the valid blocks in a zone*/
    atomic64_t max_timestamp;                   /* The max timestamp of the valid blocks in a zone*/
    atomic64_t cacheblocks_relocated;           /* Number of cacheblocks relocated */
    atomic64_t cacheblocks_flushed;             /* Number of cacheblocks flushed */
};

struct relocation_job
{
    struct cacheblock* cacheblock;
    struct page_list* pages;
    struct cache_c* dmc;
    struct list_head list;
    unsigned char rw;
};

struct writeback_job {
    struct cacheblock* cacheblock;
    struct page_list* pages;
    struct cache_c* dmc;
    struct list_head list;
};

struct gc_c
{
    int gc_mode;                 	            /* GC_MODE: Relocation or Flush*/
    int gc_trigger_mode;                        /* GC Trigger Mode */
    int gc_relocation_mode;			            /* GC Relocation Mode */
    int gc_victim_sel_mode;			            /* GC Victim Selection Mode */
    u64 gc_iter_count;                          /* Total number of gc iterations */
    u64 fg_gc_iter_count;                       /* Total number of foreground gc iterations*/
    u64 skip_gc_iter_count;                     /* Total number of GC iteration skipped as no full zones found */
    struct cache_c* dmc;			            /* struct dmc references*/
    struct bio_set relocation_bio_set;
    struct workqueue_struct *gc_wq, *gc_jobs_wq; /* Workqueue for background garbage collection*/
    struct work_struct gc_work, gc_jobs_work;    /* Work for background garbage collection*/

    struct kmem_cache* gc_job_cache;
    mempool_t* gc_job_pool;

    struct bio_set writeback_bio_set;
    struct workqueue_struct *gc_writeback_jobs_wq;  
    struct work_struct gc_writeback_jobs_work;     
    struct kmem_cache* gc_writeback_job_cache;
    mempool_t* gc_writeback_job_pool;

    spinlock_t gc_job_lock;
    struct list_head gc_pages_jobs;
    struct list_head gc_io_jobs;
    struct list_head gc_complete_jobs;

    struct list_head gc_writeback_jobs;

    mempool_t* dmc_job_mempool;                 /* Mempool for gc relocation jobs */
    atomic_t nr_relocation_jobs;                /* Number of gc relocation jobs */
    atomic_t nr_writeback_jobs;
    wait_queue_head_t destroyq;                 /* Wait queue for I/O completion */

    unsigned long *bm_gcable_blocks;            /* Bitmap indicating garbage collectable blocks */

    spinlock_t zbd_cacheblocks_lock;            /* Lock to protect zbd_cacheblocks array */
#if ZBD_CACHEBLOCKS_RADIX_TREE
    struct radix_tree_root zbd_cacheblocks;
#else
    struct cacheblock **zbd_cacheblocks;        /* Array containing cacheblock metadata, relative to it's location on the cache device */
#endif

    atomic_t relocation_counter;                /* Counter which keeps track of the current number of simultaneous relocation operations */
    wait_queue_head_t relocation_wq;            /* Waitqueue which allows GC to sleep until relocation operations have completed */

    atomic_t nr_writeback;                      /* Counter which keeps track of the current number of simultaneous flush operations */
    wait_queue_head_t writeback_wq;             /* Waitqueue for writeback operations to have completed */

    struct bio *eviction_bio; 
    void *eviction_buffer;                      /* Memory buffer for GC to perform cacheblock flush operations during eviction */

    atomic_t          block_ios;                /* Atomic boolean value for determining if I/O operations should be blocked */

    /* ------ GC configuration ----- */
    unsigned long gc_op_acceptable_free_space_percentage;
    unsigned long gc_op_stop_start_gap_percentage;
    unsigned long gc_op_start_block_gap_percentage;
    int gc_op_percentage;

    /* -------- GC Triggers -------- */
    u64 gc_trigger_start;                       /* Once the garbage count reaches this amount, GC will begin reclaiming zones */  
    u64 gc_trigger_stop;                        /* Once GC is able to reduce garbage to this amount, GC will stop reclaiming zones */
    u64 gc_trigger_block_io;                    /* After the garbge count exceeds this amount, new I/O operations will be blocked */

    /* Block Selection Parameters*/
    unsigned long long rwsst_window_interval;
    unsigned long long relocation_lru_thresh;
    unsigned long long rwss_percentage;

    /* -------- GC Statistics -------- */
    spinlock_t gc_stats_lock;
    atomic_t  is_gc_active;                     /* Atomic boolean to determinf if GC is in progress */
    atomic64_t gc_garbage_count;                /* The total number of gcable cacheblocks */
    atomic64_t relocated_cacheblocks_count;     /* Total number of cacheblocks relocated */
    atomic64_t evicted_cacheblocks_count;       /* Total number of cacheblocks evicted */

    struct zone_stats* per_zone_stats;
};

void dmz_gc_debug_update(struct cache_c *dmc, u64 io_no, u64 total_garbage_in_cache);
void dmz_gc_dump_data(void); 
int gc_jobs_init(struct gc_c* gc);
void init_gc(struct cache_c* dmc);
void gc_triggers_init(struct gc_c* gc);
void do_gc(struct work_struct *ignored);
void gc_cacheblock_metadata_updated(struct cache_c* dmc, struct cacheblock* cacheblock);
void gc_mark_garbage(struct cache_c* dmc, u64 block_idx);
void gc_zone_metadata_updated(struct cache_c* dmc);
void reclaim_zone(struct cache_c *dmc, u64 starting_block_idx); 
void update_gc_metadata(struct cache_c *dmc, uint64_t starting_block_idx,uint64_t gc_zone_idx, s64 garbage_count); 
void gc_reset(struct cache_c* dmc);
void gc_dtr(struct cache_c* dmc);
void gc_jobs_exit(struct gc_c* gc);
void check_and_queue_gc_work(struct gc_c *gc);
void check_and_set_trigger_block_ios(struct gc_c *gc);

int is_block_ios_enabled(struct dm_target* ignored);

/*****************************************************************
*  Logging & Stats Functions
******************************************************************/
void reset_dmcache_gc_stats(struct cache_c* dmc);
void log_dmcache_gc_stats(struct cache_c* dmc);

/*****************************************************************
*  Debuggig Functions
******************************************************************/
void init_dmz_gc_debug(void);
void dmz_gc_debug_update(struct cache_c *dmc, u64 io_no, u64 total_garbage_in_cache);
void per_zone_debug_update(struct cache_c *dmc, u64 zone_idx, u64 reclaim_count, u64 blocks_relcoated, u64 blocks_flushed);
void per_gc_iter_debug_update(struct cache_c *dmc, s64 gc_iter, s64 start_garbage, s64 end_garbage, u64 no_reclaim_per_gc_iter);
void dmz_gc_dump_debug_logs(void); 
bool validate_ts_score(u64 zone_idx, u64 test_score, struct cache_c *dmc);
#endif
