#ifndef DM_CACHE_GUARD
#define DM_CACHE_GUARD

#include <asm/atomic.h>
#include <asm/checksum.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/list.h>
#include <linux/blkdev.h>
#include <linux/bio.h>
#include <linux/slab.h>
#include <linux/hash.h>
#include <linux/spinlock.h>
#include <linux/workqueue.h>
#include <linux/pagemap.h>
#include <linux/version.h>
#include <linux/hrtimer.h>
#include <linux/sched/clock.h>
#include <linux/dm-io.h>
#include <linux/dm-kcopyd.h>
#include <linux/delay.h>
#include <linux/bsearch.h>
#include <linux/vmalloc.h>

#include <linux/blkzoned.h>
#include <linux/proc_fs.h>

#define DM_MSG_PREFIX "cache"
#define DMC_PREFIX "dm-cache: "

#define WiPS_HACK               0
#define NUM_WRITE_HEADS         1
#define WRITE_HEADS_BLOCK_SIZE  1 
#define NUM_FIO_JOBS            12

#define PENDING_WRITES_VALIDATION 0

#define ZBD_SINGLE_WRITE_HEAD 0
#define ZBD_MULTI_WRITE_HEAD  1
//#define DMC_ZBD_WRITE_MODE ZBD_MULTI_WRITE_HEAD
#define DMC_ZBD_WRITE_MODE ZBD_SINGLE_WRITE_HEAD 

/* Tracepoints */
#define DO_TRACEPOINTS 0
#if DO_TRACEPOINTS
#define dmcache_trace(name, args...) trace_##name(args)
#else
#define dmcache_trace(name, args...)
#endif

/* Target features */
#ifdef CONFIG_BLK_DEV_ZONED 
#define CACHE_TARGET_FEATURES \
	DM_TARGET_MIXED_ZONED_MODEL
#else
#define CACHE_TARGET_FEATURES NULL
#endif

#define DMCACHE_SUCCESS 0
#define DMCACHE_FAILURE 1

/* Default cache parameters */
#define DEFAULT_CACHE_SIZE  65536
#define DEFAULT_CACHE_ASSOC 1024
#define DEFAULT_BLOCK_SIZE  8
#define CONSECUTIVE_BLOCKS  512

/* Write policy */
#define WRITE_THROUGH   0
#define WRITE_BACK  1
#define DEFAULT_WRITE_POLICY WRITE_THROUGH

/* Number of pages for I/O */
//#define DMCACHE_COPY_PAGES  4096
#define DMCACHE_COPY_PAGES  262144
#define DMCACHE_MIN_IOS     1024
#define MIN_JOBS            1024

/* States of a cache block */
#define INVALID         0
#define VALID           1   /* Valid */
#define RESERVED        2   /* Allocated but data not in place yet */
#define DIRTY           4   /* Locally modified */
#define WRITEBACK       8   /* In the process of write back */
#define WRITETHROUGH    16  /* In the process of write through */
#define WRITEALLOCATE   32  /* In the process of write allocate */

#define is_state(x, y)      (x & y)
#define set_state(x, y)     (x |= y)
#define clear_state(x, y)   (x &= ~y)
#define put_state(x, y)     (x = y)


//Atomic Operations
#define dmc_atomic_inc(x)     atomic64_inc(&(dmc)->x) 
#define dmc_atomic_dec(x)     atomic64_dec(&(dmc)->x)
#define dmc_atomic_get(x)     (uint64_t)atomic64_read((&(dmc)->x))
#define dmc_atomic_set(x,y)   atomic64_set(&(dmc)->x, (y)) 

#define dmcstats_inc(x)       atomic64_inc(&(dmc->stats).x)
#define dmcstats_dec(x)       atomic64_dec(&(dmc->stats).x)
#define dmcstats_get(x)       (uint64_t)atomic64_read((&(dmc->stats).x))
#define dmcstats_set(x,y)     atomic64_set(&(dmc->stats).x, (y)) 


/* Change to 1 to enable the checks for the lru_states. Be warned, it's slow */
#if(1)
#define ENSURE_LRU_STATES() validate_lru_states_or_exit(dmc, __func__, __LINE__)
#else /* ENSURE_LRU_STATES */
#define ENSURE_LRU_STATES()
#endif /* ENSURE_LRU_STATES */

#define FETCH_INC(val) val++

struct gc_c;

enum lru_states {
	BEFORE_THRESH,
	ON_THRESH,
	AFTER_THRESH,
};

struct cache_stats {

    /* Stats */
    atomic64_t total_io; 
    atomic64_t reads;                /* Number of reads */
    atomic64_t writes;               /* Number of writes */
    atomic64_t cache_hits;           /* Number of cache hits */
    atomic64_t replace;              /* Number of cache replacements */
    atomic64_t writeback;            /* Number of replaced dirty blocks */
    atomic64_t dirty;                /* Number of submitted dirty blocks */
    atomic64_t cache_misses;         /* Number of cache Misses */
    atomic64_t read_misses;          /* Number of Cache Read Misses*/
    atomic64_t write_misses;         /* Number of Cache Write Misses*/
    atomic64_t read_hits;            /* Number of Cache Read Hits*/
    atomic64_t invalidates;          /* Number of Cache Invalidates*/
    atomic64_t inserts;              /* Number of cache inserted*/
    atomic64_t allocate;             /* Number of cache allocated*/
    atomic64_t forwarded;            /* Number of cache forwarded. FIXME: Not used anywhere*/
    atomic64_t dirty_blocks;         /* Number of Dirty Blocks*/
    atomic64_t garbage;               /* Number of Garbage Blocks created by the workload*/
};

/* Cache context */
struct cache_c {
    char src_dev_name[100];
    char cache_dev_name[100];

    struct dm_target *global_ti;    	 /* global dm_target to hold global cache */ 
    struct dm_dev *src_dev;     	     /* Source device */
    struct dm_dev *cache_dev;   	     /* Cache device */
    struct dm_kcopyd_client *kcp_client; /* Kcopyd client for writing back data */
    struct bio_set bio_set;              /* Mempool for bios and bvecs */

    spinlock_t cache_lock;              /* Lock to protect the radix tree */
    struct radix_tree_root *cache;  	/* Radix Tree for cache blocks */
    sector_t cache_size;       		    /* Cache size in sectors excluding OPS */
    sector_t org_cache_size;       		/* Cache size in sectors including OPS */
    unsigned int bits;      		    /* Cache size in bits */
    unsigned int assoc;     		    /* Cache associativity */
    unsigned int block_size;    	    /* Cache block size */
    unsigned int block_shift;   	    /* Cache block size in bits */
    unsigned int block_mask;    	    /* Cache block mask */
    unsigned int consecutive_shift; 	/* Consecutive blocks size in bits */
    unsigned long counter;     		    /* Logical timestamp of last access */
    unsigned int write_policy;  	    /* Cache write policy */

    spinlock_t page_lock;        		/* Lock to protect page allocation/deallocation */
    struct page_list *pages;    	    /* Pages for I/O */
    unsigned int nr_pages;      	    /* Number of pages */
    unsigned int nr_free_pages; 	    /* Number of free pages */
    wait_queue_head_t destroyq; 	    /* Wait queue for I/O completion */
    wait_queue_head_t wait_writeback;   /* Wait queue for I/O completion */
    atomic_t nr_jobs;       		    /* Number of I/O jobs */
    struct dm_io_client *io_client;   	/* Client memory pool*/

#ifdef CONFIG_BLK_DEV_ZONED
    /* ZBD Support */
    bool cache_dev_is_zoned;                    /* True: cache_dev is a ZBD */
    sector_t start_seq_write_zones;             /* Starting sector for SWR or SWP zones*/
    unsigned int nr_zones;                      /* Total number of zones of the cache device used by dmcache */
    unsigned int total_nr_zones;                /* Total numbers of zones available on the cache device */

    unsigned int cache_dev_max_open_zones;      /* Max. Number of Open Zones */
    unsigned int cache_dev_max_active_zones;    /* Max. Number of Active Zones */

    sector_t zsize_sectors;            	        /* Cache Device Zone size in sectors */
    sector_t zcap_sectors;           	        /* Cache Device Zone Capacity in sectors */
    sector_t blocks_per_zone;         	        /* Number of usable cache blocks per zone */

    struct mutex zbd_write_loc_mutex;           /* Mutex to protect the reserve the write location */ 

#if DMC_ZBD_WRITE_MODE == ZBD_SINGLE_WRITE_HEAD
    unsigned int current_zone;      	        /* Index of the zone which is currently being used for writing */
#else
    unsigned int current_write_head;            /* Index of the current zone being used for writing */
    unsigned int current_zones[NUM_WRITE_HEADS];/* Array of current zones which is currently being used for writing by the multi heads*/
#endif

#if WiPS_HACK
    pid_t pid_list[NUM_FIO_JOBS];
    pid_t current_pid;
    unsigned int pid_index;
    unsigned int wp_block[NUM_WRITE_HEADS];
#else

    unsigned int zone_batch_size;     
    unsigned int wp_block;              	    /* Counter used to track how much space is used in the current zone */
#endif

    unsigned long *bm_full_zones;      	        /* Bitmap indicating full zones */

    spinlock_t   active_zones_lock;             /* Lock to protect the active zones list */
    unsigned int active_zones_max;              /* Stores the maximum number of active zones */
    unsigned int active_zones_count;            /* Stores the current number of active zones */
    struct list_head active_zones_containers;   /* Used to store preallocated containers that
                                                 * aren't being used */
    
    struct bio_list deferred_bios;              /* List of deferred bios when disk space is low*/
    spinlock_t def_bio_list_lock;               /* Lock to protect the deferred bio_list */

#endif /*CONFIG_BLK_DEV_ZONED */

	
    // Workqueue and Job pool
    mempool_t *_job_pool;                       /* Mempool for kcached jobs */
    spinlock_t _job_lock;                       /* Lock to prevent the job operations */
    struct kmem_cache* _job_cache;

    struct list_head _complete_jobs;
    struct list_head _pages_jobs;
    struct list_head _io_jobs;

    struct workqueue_struct *_kcached_wq;       /*Workqueue for kcached*/
    struct work_struct _kcached_work;           /*kcached work item */
    struct work_struct deferred_bio_work;       /*deferred bio work item */

    /* LRU List */
    struct list_head *lru;
    spinlock_t lru_lock;
    struct cacheblock *lru_thresh;  

    /* --- Block Selection --- */
    atomic64_t timestamp_counter;				/* A logical timestamp counter */

    /* WSS and RWSS */
    spinlock_t locality_lock;                   /* Lock to protect the idata structures around Locality */
    struct radix_tree_root *wss;              	/* Radix Tree for tracking WSS  */
    struct radix_tree_root *rwss_t;   	        /* Radix Tree for tracking RWSS in a certain window interval*/
    u64 working_set;                            /* Working Set Size*/
    u64 reuse_working_set;                      /* Reuse Working Set Size */
    u64 reuse_working_set_t;                    /* Reuse Working Set Size in a certain window interval*/

    struct cache_stats stats;                   /* cache_c stats */

    struct gc_c* gc;
};

struct locality_ctx {
    u64 timestamp;
    u64 wss;
    u64 rwss_t;
};

/* Cache block metadata structure */
struct cacheblock {
    spinlock_t lock;                    /* Lock to protect operations on the bio list */
    u64 src_block_addr;                 /* Block idx of the cacheblock on the source dev*/
    u64 cache_block_addr;               /* Block idx of the cacheblock on the cache dev*/

    unsigned short state;   	        /* State of a block */
	enum lru_states lru_state;          /* State of the block wrt threshold on the LRU*/
    struct bio_list bios;   	        /* List of pending bios */
    s64 last_access;                    /* Timestamp based logical counter representing when the block was last accessed*/

    struct list_head list;
};

/* Structure for a kcached job */
struct kcached_job {
    struct list_head list;
    struct cache_c *dmc;                /* Pointer to the cache_c instance to which this job is related */
    struct bio *bio;    	            /* Original bio */
    struct dm_io_region src;
    struct dm_io_region dest;
    struct cacheblock *cacheblock;
    int rw;
    bool original_bio_data_dir;         /* Original Bio data_dir 0: READ 1: WRITE */
    unsigned int nr_pages;
    struct page_list *pages;
};

#ifdef CONFIG_BLK_DEV_ZONED

struct dmc_report_zones_args {
    sector_t zone_capacity;           	/* Zone Capacity */
    bool  zone_cap_mismatch;          	/* True: Zone Size != Zone Capacity */
    //TODO: Extend this struct as per the req
};

struct active_zone_container {
    unsigned int zone_idx;
    struct list_head list;
    struct rb_node node;
};

#endif /*CONFIG_BLK_DEV_ZONED */

#define DMC_DEBUG 0
#define DMC_DBUG_MODE_LOW 2
#define DMC_DBUG_MODE_HIGH 4

#define DMC_DBUG_MODE DMC_DBUG_MODE_LOW

#if DMC_DEBUG && DMC_DBUG_MODE == DMC_DBUG_MODE_LOW 
    #define DEBUG_DMC( s, arg...) DMINFO(s, ##arg)
    #define DEBUG_DMC_JOB( s, arg...) DMINFO(s, ##arg)
    #define DEBUG_DMC_BIO( s, arg...) DMINFO(s, ##arg)
    #define DEBUG_DMC_ZBD( s, arg...) DMINFO(s, ##arg)
    #define DEBUG_DMC_GC( s, arg...) DMINFO(s, ##arg)
    #define DEBUG_DMC_ACTIVE_ZONES( s, arg...) DMINFO(s, ##arg)
#elif DMC_DEBUG && DMC_DBUG_MODE == DMC_DBUG_MODE_HIGH 
    #define DEBUG_DMC( s, arg...) DMINFO(s, ##arg)
    #define DEBUG_DMC_JOB( s, arg...) 
    #define DEBUG_DMC_BIO( s, arg...) 
    #define DEBUG_DMC_ZBD( s, arg...)
    #define DEBUG_DMC_GC( s, arg...)
    #define DEBUG_DMC_ACTIVE_ZONES( s, arg...)
#else
    #define DEBUG_DMC( s, arg...) 
    #define DEBUG_DMC_JOB( s, arg...) 
    #define DEBUG_DMC_BIO( s, arg...) 
    #define DEBUG_DMC_ZBD( s, arg...)
    #define DEBUG_DMC_GC( s, arg...)
    #define DEBUG_DMC_ACTIVE_ZONES( s, arg...)
#endif

#define DMC_INFO(s, ...) pr_info("###[%s]###" s, __FUNCTION__, ##__VA_ARGS__)

/*****************************************************************
 *  LRU Management Functions
 ******************************************************************/
void init_dmcache_lru(struct cache_c *dmc);
void validate_lru_states_or_exit(struct cache_c *dmc, const char *func, int lineno);
/* 	Both of these functions exist to update the state information within each cacheblock regarding where they are
 *  relative to the lru_thresh in the lru. It will also "advance" or "retreat" the lru_thresh pointer when 
 *  appropriate.
 * NOTE: Both of these are to be called only before doing any lru modifications */
bool advance_lru_thresh(struct cache_c *dmc, struct cacheblock *block, bool tagged_for_garbage);
bool retreat_lru_thresh(struct cache_c *dmc, struct cacheblock *block);
void move_lru_thresh(struct cache_c *dmc, u64 new_pos);
void move_blk_lru_bk(struct cache_c *dmc, struct cacheblock *block, enum lru_states new_state);

/*****************************************************************
 *  Functions
 ******************************************************************/
int kcached_get_pages(struct cache_c *dmc, unsigned int nr, struct page_list **pages);
void kcached_put_pages(struct cache_c *dmc, struct page_list *pl);
void zbd_prepare_for_write(struct cache_c *dmc, struct cacheblock *cacheblock, bool mark_old_location_as_gcable);
void cacheblock_update_state(struct cache_c *dmc, struct cacheblock *cacheblock);
void finish_zone_append(struct cache_c *dmc, struct cacheblock *cacheblock);
void flush_bios(struct cache_c *dmc, struct cacheblock *cacheblock);

static inline unsigned long *bitmap_kvmalloc(unsigned int nbits, gfp_t flags)
{
    return kvmalloc_array(BITS_TO_LONGS(nbits), sizeof(unsigned long), flags);
}

/*****************************************************************
 *  Address conversion functions - USE ONLY WITH CACHE INDEXES/ADDRESSES
 ******************************************************************/

static inline u64 sector_addr_to_block_idx(struct cache_c *dmc, sector_t addr);
static inline unsigned int sector_addr_to_zone_idx(struct cache_c *dmc, sector_t addr);
static inline sector_t block_idx_to_sector_addr(struct cache_c *dmc, u64 idx);
static inline unsigned int block_idx_to_zone_idx(struct cache_c *dmc, u64 idx);
static inline sector_t zone_idx_to_sector_addr(struct cache_c *dmc, unsigned int idx);
static inline u64 zone_idx_to_block_idx(struct cache_c *dmc, unsigned int idx);

static inline u64 sector_addr_to_block_idx(struct cache_c *dmc, sector_t addr)
{
    if (dmc->cache_dev_is_zoned) {
        unsigned int zone_idx = sector_addr_to_zone_idx(dmc, addr);
        sector_t zone_starting_sector = zone_idx_to_sector_addr(dmc, zone_idx);
        return ((addr - zone_starting_sector) >> dmc->block_shift) + zone_idx_to_block_idx(dmc, zone_idx);
    }

    // Non ZBD cache dev
    return addr >> dmc->block_shift;
}
static inline unsigned int sector_addr_to_zone_idx(struct cache_c *dmc, sector_t addr)
{
    return (addr - dmc->start_seq_write_zones) / dmc->zsize_sectors;
}

static inline sector_t block_idx_to_sector_addr(struct cache_c *dmc, u64 idx)
{
    if (dmc->cache_dev_is_zoned) {
        u64 zone_block_offset = idx % dmc->blocks_per_zone;
        return zone_idx_to_sector_addr(dmc, block_idx_to_zone_idx(dmc, idx)) + (zone_block_offset << dmc->block_shift);
    }

    // Non ZBD cache dev
    return idx << dmc->block_shift;
}
static inline unsigned int block_idx_to_zone_idx(struct cache_c *dmc, u64 idx)
{
    return idx / dmc->blocks_per_zone;
}

static inline sector_t zone_idx_to_sector_addr(struct cache_c *dmc, unsigned int idx)
{
    return dmc->start_seq_write_zones + dmc->zsize_sectors * idx;
}
static inline u64 zone_idx_to_block_idx(struct cache_c *dmc, unsigned int idx)
{
    return dmc->blocks_per_zone * idx;
}

/*****************************************************************
*  Timestamp & Bookeeping Functions
******************************************************************/
void update_ts_score_bk(struct cache_c *dmc, struct cacheblock *block, u64 prev_ts, bool is_read); 
void update_curr_zone_stats(struct cache_c *dmc, struct cacheblock *block);

/*****************************************************************
*  Workqueue and Job Functions
******************************************************************/
void wake_deferred_bio_work(struct cache_c *dmc);
void cache_invalidate(struct cache_c *dmc, struct cacheblock *cache, bool has_cache_lock);
void update_zone_timestamp_stats(struct cache_c *dmc, struct cacheblock *block); 

/*****************************************************************
*  Logging & Stats Functions
******************************************************************/
void log_dmcache_configuration(struct cache_c* dmc); 
void log_dmcache_stats(struct cache_c *dmc);
void reset_dmcache_stats(struct cache_c *dmc);

#endif
