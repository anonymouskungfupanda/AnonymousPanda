#include "dm.h"
#include "gc.h"
#include "dm-cache.h"

#include <linux/random.h>

#if GC_ENABLED

static void do_relocation_work(struct work_struct* work);
static int do_relocation_pages(struct relocation_job* job);
static int do_relocation_io(struct relocation_job* job);
static int do_relocation_complete(struct relocation_job* job);

static void do_writeback_work(struct work_struct* work);
static int do_writeback(struct writeback_job *job);

void gc_triggers_init(struct gc_c* gc) {

    u64 block_capacity, op_capacity, required_free;

    gc->gc_op_percentage                        = GC_OP_PERCENTAGE;
    gc->gc_op_acceptable_free_space_percentage  = GC_OP_ACCEPTABLE_FREE_SPACE_PERCENTAGE;
    gc->gc_op_stop_start_gap_percentage         = GC_OP_STOP_START_GAP_PERCENTAGE;
    gc->gc_op_start_block_gap_percentage        = GC_OP_START_BLOCK_GAP_PERCENTAGE;

	/* Max number of cacheblocks we could store, if we used 100% of the cache space */
    block_capacity = gc->dmc->nr_zones * gc->dmc->blocks_per_zone;

    /* The number of cacheblocks we are reserving for GC operation */
    op_capacity = (block_capacity * gc->gc_op_percentage + 50) / 100;

    /* Required minimum free space for GC to operate properly */
    required_free = (BLOCK_IOS_FREE_ZONES * gc->dmc->blocks_per_zone);

    /* Set the block io trigger. It is the opposite of the required free space */
    BUG_ON(op_capacity < required_free);

    /* If GC_MODE is set to RELOCATION_GC, GC will require atleast one zone for relocation; 
        and other zone is used to write the incoming io. Hence nr_zones should be atleast 2*/
    BUG_ON(gc->dmc->nr_zones < 2);
    
#if 0
    /*
     * - If the OP capacity is less than the number of blocks per zone, then we need to increase the OP capacity
     *   to at least the number of blocks per zone. This is because GC works at the zone level, and we need to
     *   be able to store at least one zone in the OP space.
     * - But, this does not consider the garbage percentage. If the OP capacity is less than the number of blocks
     *   per zone, then we need to increase the OP capacity to at least the number of blocks per zone + the garbage
     *   percentage, such that GC has enough space to reloacte cacheblocks 
    */

    if (gc->gc_mode == RELOCATION_GC) {
        u64 percentage_after_garbage   = 100 - gc->gc_op_acceptable_garbage_percentage;
        u64 min_op_capacity            = (gc->dmc->blocks_per_zone * 100 + (percentage_after_garbage - 1)) / percentage_after_garbage;

        if (op_capacity < min_op_capacity) {
			DMWARN ("WARNING: Configured OP capacity (%llu) automatically raised to (%llu)", op_capacity, min_op_capacity);
            op_capacity = min_op_capacity;
        }
    }
#endif

    BUG_ON(gc->gc_op_acceptable_free_space_percentage + gc->gc_op_stop_start_gap_percentage + gc->gc_op_start_block_gap_percentage != 100);

    if (op_capacity > required_free)
        op_capacity = op_capacity - required_free;
    else
        op_capacity = required_free;

#if 1
	uint64_t min_op_capacity = gc->dmc->blocks_per_zone + ((100 - gc->gc_op_acceptable_free_space_percentage) * gc->dmc->blocks_per_zone) / 100;
	if (op_capacity < min_op_capacity) {
		DMWARN ("WARNING: Configured OP capacity (%llu) automatically raised to (%llu)", op_capacity, min_op_capacity);
		op_capacity = min_op_capacity;
	}
#endif


    //assert(gc_trigger_block_io > dmc->blocks_per_zone);

    /* The remaining OP space is the same as block io trigger. The acceptable garbage percentage sets
     * the stopping point for GC in reference to this remaining space.
     */
    gc->gc_trigger_stop = (op_capacity * gc->gc_op_acceptable_free_space_percentage + 50) / 100;

    gc->gc_trigger_stop = max(gc->gc_trigger_stop, gc->dmc->blocks_per_zone);


    /* Calculate the start trigger for GC. It is the stop trigger amount + the gap percentage. */
    gc->gc_trigger_start    = gc->gc_trigger_stop + (op_capacity * gc->gc_op_stop_start_gap_percentage + 50) / 100;   
    //gc->gc_trigger_block_io = op_capacity - required_free;
    gc->gc_trigger_block_io = op_capacity - gc->gc_trigger_start;

    /* Set cache size, also update the value of the lru threshold */
    gc->dmc->cache_size = block_capacity - op_capacity;

    //BUG_ON(op_capacity < required_free);

    /* The initial value of relocation_lru_thresh is set as rwss_window_interval
     * gc->rwss_window_interval is set as WINDOW_INTERVAL which is equal to certain % 
     * of RWSS for the entire workload
     */
    gc->relocation_lru_thresh = gc->rwsst_window_interval;
    gc->relocation_lru_thresh = min(gc->relocation_lru_thresh, gc->dmc->cache_size - 1);
}

int gc_jobs_init(struct gc_c* gc)
{
    gc->gc_job_cache = kmem_cache_create("gc-jobs", sizeof(struct relocation_job),
                                    __alignof__(struct relocation_job),0, NULL);
    if (!gc->gc_job_cache)
        return -ENOMEM;

    gc->gc_writeback_job_cache = kmem_cache_create("gc-writeback-jobs", sizeof(struct writeback_job),
                                    __alignof__(struct writeback_job),0, NULL);
    if (!gc->gc_writeback_job_cache)
        return -ENOMEM;

    gc->gc_job_pool = mempool_create(GC_MIN_JOBS, mempool_alloc_slab, mempool_free_slab, gc->gc_job_cache);
    if (!gc->gc_job_pool) {
        kmem_cache_destroy(gc->gc_job_cache);
        return -ENOMEM;
    }

    gc->gc_writeback_job_pool = mempool_create(GC_MIN_JOBS, mempool_alloc_slab, mempool_free_slab, gc->gc_writeback_job_cache);
    if (!gc->gc_writeback_job_pool) {
        kmem_cache_destroy(gc->gc_writeback_job_cache);
        return -ENOMEM;
    }

    gc->gc_jobs_wq = alloc_workqueue("gc-jobs", WQ_MEM_RECLAIM, 0);
    if (!gc->gc_jobs_wq) {
        DMERR("failed to start gc-jobs");
        return -ENOMEM;
    }
    INIT_WORK(&gc->gc_jobs_work, do_relocation_work);

    gc->gc_writeback_jobs_wq = alloc_workqueue("gc-writeback-jobs", WQ_MEM_RECLAIM, 0);
    if (!gc->gc_writeback_jobs_wq) {
        DMERR("failed to start gc-jobs");
        return -ENOMEM;
    }
    INIT_WORK(&gc->gc_writeback_jobs_work, do_writeback_work);
    return 0;
}

void gc_jobs_exit(struct gc_c* gc)
{
    BUG_ON(!list_empty(&gc->gc_pages_jobs));
    BUG_ON(!list_empty(&gc->gc_io_jobs));
    BUG_ON(!list_empty(&gc->gc_complete_jobs));
    BUG_ON(!list_empty(&gc->gc_writeback_jobs));

    mempool_destroy(gc->gc_job_pool);
    mempool_destroy(gc->gc_writeback_job_pool);

    kmem_cache_destroy(gc->gc_job_cache);
    kmem_cache_destroy(gc->gc_writeback_job_cache);

    gc->gc_job_pool         = NULL;
    gc->gc_job_cache        = NULL;
    gc->gc_writeback_job_pool   = NULL;
    gc->gc_writeback_job_cache  = NULL;

    destroy_workqueue(gc->gc_jobs_wq);
    destroy_workqueue(gc->gc_writeback_jobs_wq);
}

inline void wake_writeback_wq(struct gc_c* gc)
{
    queue_work(gc->gc_writeback_jobs_wq, &gc->gc_writeback_jobs_work);
}

inline void wake(struct gc_c* gc)
{
    queue_work(gc->gc_jobs_wq, &gc->gc_jobs_work);
}


static inline void push_writeback_job(struct list_head* jobs, struct writeback_job* job)
{
    struct cache_c* dmc = job->dmc;
    struct gc_c* gc     = dmc->gc;
    unsigned long flags;

    spin_lock_irqsave(&gc->gc_job_lock, flags);
    list_add_tail(&job->list, jobs);
    spin_unlock_irqrestore(&gc->gc_job_lock, flags);
}

static inline void push(struct list_head* jobs, struct relocation_job* job)
{
    struct cache_c* dmc = job->dmc;
    struct gc_c* gc = dmc->gc;
    unsigned long flags;

    spin_lock_irqsave(&gc->gc_job_lock, flags);
    list_add_tail(&job->list, jobs);
    spin_unlock_irqrestore(&gc->gc_job_lock, flags);
}


static inline struct writeback_job* pop_writeback_job(struct list_head *jobs, struct cache_c* dmc)
{
    struct gc_c* gc        = dmc->gc;
    struct writeback_job* job  = NULL;
    unsigned long flags;

    spin_lock_irqsave(&gc->gc_job_lock, flags);
    if (!list_empty(jobs)) {
        job = list_entry(jobs->next, struct writeback_job, list);
        list_del(&job->list);
    }
    spin_unlock_irqrestore(&gc->gc_job_lock, flags);

    return job;
}

static inline struct relocation_job* pop(struct list_head *jobs, struct cache_c* dmc)
{
    struct gc_c* gc = dmc->gc;
    struct relocation_job* job = NULL;
    unsigned long flags;

    spin_lock_irqsave(&gc->gc_job_lock, flags);
    if (!list_empty(jobs)) {
        job = list_entry(jobs->next, struct relocation_job, list);
        list_del(&job->list);
    }
    spin_unlock_irqrestore(&gc->gc_job_lock, flags);

    return job;
}


static void queue_writeback_job(struct writeback_job* job)
{
    struct cache_c* dmc = job->dmc;
    struct gc_c* gc = dmc->gc;

    atomic_inc(&gc->nr_writeback_jobs);
    push_writeback_job(&gc->gc_writeback_jobs, job);
    wake_writeback_wq(gc);
}

static void queue_relocation_job(struct relocation_job* job)
{
    struct cache_c* dmc = job->dmc;
    struct gc_c* gc = dmc->gc;

    atomic_inc(&gc->nr_relocation_jobs);
    push(&gc->gc_pages_jobs, job);
    wake(gc);
}

static int process_writeback_jobs(struct list_head *jobs, int (*fn) (struct writeback_job*), struct cache_c* dmc)
{
    struct writeback_job *job;
    int r, count = 0;

    while ((job = pop_writeback_job(jobs, dmc))) {
        r = fn(job);

        if (r > 0) {
            push_writeback_job(jobs, job);
            break;
        } else if (unlikely(r < 0)) {
            DMERR("process_jobs: Job processing error");
        }
        count++;
    }
    return count;
}


static int process_jobs(struct list_head *jobs, int (*fn) (struct relocation_job*), struct cache_c* dmc)
{
    struct relocation_job *job;
    int r, count = 0;

    while ((job = pop(jobs, dmc))) {
        r = fn(job);

        if (r > 0) {
            /*
             * We couldn't service this job ATM, so
             * push this job back onto the list.
             */
            push(jobs, job);
            break;
        } else if (unlikely(r < 0)) {
            /* error this rogue job */
            DMERR("process_jobs: Job processing error");
        }

        count++;
    }

    return count;
}

static void do_writeback_work(struct work_struct* work)
{
    struct gc_c* gc     = container_of(work, struct gc_c, gc_writeback_jobs_work);
    struct cache_c* dmc = gc->dmc;

    process_writeback_jobs(&gc->gc_writeback_jobs, do_writeback, dmc);

}

static void do_relocation_work(struct work_struct* work)
{
    struct gc_c* gc     = container_of(work, struct gc_c, gc_jobs_work);
    struct cache_c* dmc = gc->dmc;

    process_jobs(&gc->gc_pages_jobs, do_relocation_pages, dmc);
    process_jobs(&gc->gc_io_jobs, do_relocation_io, dmc);
    process_jobs(&gc->gc_complete_jobs, do_relocation_complete, dmc);
}

static int do_relocation_pages(struct relocation_job* job)
{
    struct cache_c* dmc = job->dmc;
    struct gc_c* gc = dmc->gc;

    // Always fetch exactly 1 page, because 4096K block size
    int r = kcached_get_pages(dmc, 1, &job->pages);
    if (r == -ENOMEM) /* can't complete now */
        return 1;

    /* this job is ready for io */
    push(&gc->gc_io_jobs, job);
    return 0;
}

static void relocation_endio(struct bio* bio)
{
    struct relocation_job* job  = bio->bi_private;
    struct cache_c* dmc         = job->dmc;
    struct gc_c* gc             = dmc->gc;

    if (job->rw == READ) {
        // READ is finished, requeue for ZONE_APPEND now
        job->rw = WRITE;
        push(&gc->gc_io_jobs, job);
    } else {
        // ZONE_APPEND is finished. Store the actual write location in the cacheblock
        job->cacheblock->cache_block_addr = sector_addr_to_block_idx(dmc, bio->bi_iter.bi_sector);
        push(&gc->gc_complete_jobs, job);
    }

    // Release the bio used for this READ/WRITE
    bio_put(bio);
    wake(gc);
}

static int do_relocation_io(struct relocation_job* job)
{
    struct cache_c* dmc = job->dmc;
    struct gc_c* gc = dmc->gc;
    blk_opf_t op_flag;
    struct bio *relocation_bio;

    // We need 1 io_vec because we are using exactly 1 page for 4096K cacheblock size

    if (job->rw == READ) { 
        // Read job
        op_flag = REQ_OP_READ;
    } else { 
        // Write job
        op_flag = REQ_OP_ZONE_APPEND;
        zbd_prepare_for_write(dmc, job->cacheblock, false);
        update_curr_zone_stats(dmc, job->cacheblock);
    }

    relocation_bio = bio_alloc_bioset(dmc->cache_dev->bdev, 1, op_flag, GFP_KERNEL,  &gc->relocation_bio_set);

    if(bio_add_page(relocation_bio, job->pages->page, PAGE_SIZE, 0) != PAGE_SIZE) {
	bio_put(relocation_bio);
	DMERR("Relocation bio page allocation failed");
	return -1;
    }

    bio_set_dev(relocation_bio, dmc->cache_dev->bdev);
    relocation_bio->bi_end_io         = relocation_endio;
    relocation_bio->bi_private        = job;
    relocation_bio->bi_iter.bi_size   = to_bytes(dmc->block_size);
    relocation_bio->bi_iter.bi_sector = block_idx_to_sector_addr(dmc, job->cacheblock->cache_block_addr);

    submit_bio_noacct(relocation_bio);
    return 0;
}

static int do_relocation_complete(struct relocation_job* job)
{
    struct cache_c* dmc = job->dmc;
    struct gc_c* gc     = dmc->gc;

    kcached_put_pages(dmc, job->pages);

    /* Return the cacheblock to the VALID state and update associated metadata */
    gc_cacheblock_metadata_updated(dmc, job->cacheblock);
    cacheblock_update_state(dmc, job->cacheblock);
    finish_zone_append(dmc, job->cacheblock);
    flush_bios(dmc, job->cacheblock);

    mempool_free(job, gc->gc_job_pool);
    if (atomic_dec_and_test(&gc->nr_relocation_jobs))
        wake_up(&gc->destroyq);

    /* Wake up GC, who may be waiting on this request */
    atomic_dec(&gc->relocation_counter);
    wake_up_all(&gc->relocation_wq);

    return 0;
}

static int do_writeback(struct writeback_job* job)
{
    struct dm_io_request iorq;
    struct dm_io_region region;

    struct cache_c* dmc 	= job->dmc;
    struct gc_c* gc     	= dmc->gc;
    struct cacheblock* cache 	= job->cacheblock;
    struct bio *writeback_bio 	= bio_alloc_bioset(NULL, 1, REQ_OP_READ, GFP_KERNEL,  &gc->writeback_bio_set);


    iorq.bi_opf         = REQ_OP_READ;
    iorq.mem.type       = DM_IO_BIO;
    iorq.mem.ptr.bio    = writeback_bio; 
    iorq.notify.fn      = NULL;
    iorq.client         = dmc->io_client;

    region.bdev         = dmc->cache_dev->bdev;
    region.count        = dmc->block_size;
    region.sector       = block_idx_to_sector_addr(dmc, cache->cache_block_addr);

    // Read the cache block into memory
    bio_set_dev(writeback_bio, dmc->cache_dev->bdev);
    dm_io(&iorq, 1, &region, NULL, IOPRIO_DEFAULT);

    iorq.bi_opf         = REQ_OP_WRITE;
    region.bdev         = dmc->src_dev->bdev;
    region.sector       = cache->src_block_addr << dmc->block_shift;

    // Write from memory to the source device
    bio_set_dev(writeback_bio, dmc->src_dev->bdev);
    writeback_bio->bi_opf = REQ_OP_WRITE;
    dm_io(&iorq, 1, &region, NULL, IOPRIO_DEFAULT);

    // Maintain dirty blocks counter. We do not need to clear the DIRTY flag because
    // The cache_invalidate() call above clears it for us
    dmcstats_dec(dirty_blocks);

    mempool_free(job, gc->gc_writeback_job_pool);
    if (atomic_dec_and_test(&gc->nr_writeback_jobs))
        wake_up(&gc->destroyq);

    atomic_dec(&gc->nr_writeback);
    wake_up_all(&gc->writeback_wq);

    return 0;
}


static struct relocation_job* new_relocation_job(struct cache_c* dmc, struct cacheblock* cacheblock)
{
    struct gc_c* gc             = dmc->gc;
    struct relocation_job* job  = mempool_alloc(gc->gc_job_pool, GFP_KERNEL);
    job->cacheblock             = cacheblock;
    job->dmc                    = dmc;
    job->rw                     = READ;
    return job;
}


static struct writeback_job* new_writeback_job(struct cache_c* dmc, struct cacheblock* cacheblock)
{
    struct gc_c* gc             = dmc->gc;
    struct writeback_job* job       = mempool_alloc(gc->gc_writeback_job_pool, GFP_KERNEL);
    job->cacheblock             = cacheblock;
    job->dmc                    = dmc;
    return job;
}

inline unsigned int gc_count_free_zones(struct cache_c* dmc)
{
    unsigned int nr_free_zones;
    nr_free_zones = dmc->nr_zones - bitmap_weight(dmc->bm_full_zones, dmc->nr_zones);

    /* Count only compltely empty zones by subtracting active_zones_count */
    spin_lock(&dmc->active_zones_lock);
    nr_free_zones -= dmc->active_zones_count;
    spin_unlock(&dmc->active_zones_lock);

    return nr_free_zones;
}


void init_gc(struct cache_c* dmc)
{
    struct gc_c* gc = kvmalloc(sizeof(struct gc_c), GFP_KERNEL);
    int ret;
    gc->dmc             = dmc;
    dmc->gc             = gc;
    BUG_ON(!gc);

    gc->gc_mode               = GC_MODE;
    gc->gc_relocation_mode    = GC_RELOCATION_MODE; 
    gc->gc_victim_sel_mode    = VICTIM_SELECTION_POLICY;
    gc->rwsst_window_interval = WINDOW_INTERVAL;
    gc->rwss_percentage       = RWSS_PERCENTAGE;

    GC_STATS_SET(gc_garbage_count, 0);

#if DEBUG_GC
    init_dmz_gc_debug();
#endif

    gc_jobs_init(gc);

    spin_lock_init(&gc->gc_job_lock);
    INIT_LIST_HEAD(&gc->gc_pages_jobs);
    INIT_LIST_HEAD(&gc->gc_io_jobs);
    INIT_LIST_HEAD(&gc->gc_complete_jobs);
    INIT_LIST_HEAD(&gc->gc_writeback_jobs);

    atomic_set(&gc->nr_relocation_jobs, 0);
    init_waitqueue_head(&gc->destroyq);

    gc->dmc_job_mempool  = dmc->_job_pool;
    gc->bm_gcable_blocks = bitmap_kvmalloc((dmc->nr_zones * dmc->blocks_per_zone), GFP_KERNEL);

    /* Initialize the GC Triggers*/
    gc_triggers_init(gc);

    spin_lock_init(&gc->zbd_cacheblocks_lock);

#if ZBD_CACHEBLOCKS_RADIX_TREE
    INIT_RADIX_TREE(&gc->zbd_cacheblocks, GFP_NOWAIT);
#else
    gc->zbd_cacheblocks = (struct cacheblock**) kvmalloc_array(dmc->nr_zones*dmc->blocks_per_zone, sizeof(struct cacheblock*), GFP_KERNEL);
    BUG_ON(gc->zbd_cacheblocks == NULL);
#endif

    /* Allocate locks and data structures used for async cacheblock relocation in GC */
    ret = bioset_init(&gc->relocation_bio_set, 2*GC_RELOCATION_ASYNC_LIMIT, 0, BIOSET_NEED_BVECS);
    BUG_ON(ret);
    atomic_set(&gc->relocation_counter, 0);
    init_waitqueue_head(&gc->relocation_wq);

    ret = bioset_init(&gc->writeback_bio_set, 2*GC_FLUSH_ASYNC_LIMIT, 0, BIOSET_NEED_BVECS);
    BUG_ON(ret);
    atomic_set(&gc->nr_writeback, 0);
    init_waitqueue_head(&gc->writeback_wq);

#if 0
    unsigned short bio_pages_needeed = (to_bytes(dmc->block_size) + PAGE_SIZE - 1) / PAGE_SIZE;

    gc->eviction_bio = bio_alloc(GFP_KERNEL, bio_pages_needeed);
    for (i = 0; i < bio_pages_needeed; i++)
	    bio_add_page(gc->eviction_bio, alloc_page(GFP_KERNEL), PAGE_SIZE, 0);

    gc->eviction_buffer = kvzalloc(to_bytes(dmc->block_size), GFP_KERNEL);
#endif

    /* Configure metatada for I/O blocking. 
     * Used when cache space is low and GC needs exclusive cache access */
    atomic_set(&gc->block_ios, false);

    /* Create a new workqueue for running background GC, which will reclaim dirty zones. */
    //gc->gc_wq = alloc_workqueue("dmcache-gc", WQ_MEM_RECLAIM, 0);
    gc->gc_wq = create_singlethread_workqueue("dmcache-gc");

    INIT_WORK(&gc->gc_work, do_gc);

    /* -------- GC Statistics -------- */
    spin_lock_init(&gc->gc_stats_lock);
    int i;
    gc->per_zone_stats = kvmalloc_array(dmc->nr_zones, sizeof(struct zone_stats), GFP_KERNEL);
    for(i=0; i<dmc->nr_zones; i++)
        spin_lock_init(&gc->per_zone_stats[i].zone_lock);

    reset_dmcache_gc_stats(dmc);
}

// Fetches a cacheblock for GC. The returned cacheblock will be in the RESERVED state, owned by GC
// Returns NULL if the desired cacheblock is already RESERVED and does not need to be relocated by GC
static inline struct cacheblock* gc_fetch_cacheblock(struct cache_c *dmc, u64 block_idx)
{
    struct gc_c* gc = dmc->gc;
    struct cacheblock* cache;

    spin_lock(&gc->zbd_cacheblocks_lock);
    if (test_bit(block_idx, gc->bm_gcable_blocks)) {
        spin_unlock(&gc->zbd_cacheblocks_lock);
        return NULL;
    }
#if ZBD_CACHEBLOCKS_RADIX_TREE
    cache = radix_tree_lookup(&gc->zbd_cacheblocks, block_idx);
    BUG_ON(cache == NULL);
#else
    BUG_ON(block_idx > dmc->nr_zones*dmc->blocks_per_zone);
    cache = gc->zbd_cacheblocks[block_idx];
#endif

    spin_unlock(&gc->zbd_cacheblocks_lock);

    if (cache == NULL)
        DMERR("Wanted Address: %llu", block_idx);

    // "obtain" the cacheblock by marking it as RESERVED, as long as it is not already RESERVED
    // If the cacheblock was already RESERVED, we should not mess with it. Other code is already moving it
    spin_lock(&cache->lock);
    if (!is_state(cache->state, RESERVED)) {
        set_state(cache->state, RESERVED);
        spin_unlock(&cache->lock);
        //DMINFO("CAddr: %llu, Expected: %llu", cache->cache_block_addr, block_idx);
        if (unlikely(cache->cache_block_addr != block_idx)) {
            DMERR("Expected Address: %llu, Actual Address: %llu", block_idx, cache->cache_block_addr);
            BUG();
        }
        return cache;
    }
    spin_unlock(&cache->lock);

    return NULL;
}

static inline void gc_flush_cacheblock(struct cache_c *dmc, struct cacheblock *cache)
{
    struct gc_c* gc = dmc->gc;
    struct writeback_job* job;
    bool cacheblock_is_dirty = false;

    if (cache && cache->cache_block_addr) {
        spin_lock(&cache->lock);
        if (!is_state(cache->state, RESERVED)) {
            cacheblock_is_dirty 	= is_state(cache->state, DIRTY);

            cache_invalidate(dmc, cache, true);
            spin_unlock(&cache->lock);

            spin_lock(&dmc->lru_lock);
            /* Update LRU */
#if GC_ENABLED
            retreat_lru_thresh(dmc, cache);
#endif
            list_move(&cache->list, dmc->lru);
            spin_unlock(&dmc->lru_lock);
            dmcstats_dec(allocate);
        }else
            spin_unlock(&cache->lock);
    }

    if(cacheblock_is_dirty) {
        atomic_inc(&gc->nr_writeback);
        job = new_writeback_job(dmc, cache);
        BUG_ON(job == NULL);
        //queue_writeback_job(job);
    }
}

static inline void gc_relocate_cacheblock(struct cache_c *dmc, struct cacheblock *cache)
{
    struct gc_c* gc = dmc->gc;
    struct relocation_job* job;

    // Wait until we are below the async I/O limit to proceed with submitting the relocation request
    wait_event(gc->relocation_wq, atomic_read(&gc->relocation_counter) < GC_RELOCATION_ASYNC_LIMIT);
    //while (atomic_read(&gc->relocation_counter) >= GC_RELOCATION_ASYNC_LIMIT) { usleep_range(1, 100); }

#if 0
    if ((temp = atomic_inc_return(&gc->relocation_counter)) >= GC_RELOCATION_ASYNC_LIMIT)
        DMINFO_LIMIT("Peak Reached: %d", temp);
#else
    atomic_inc(&gc->relocation_counter);
#endif

    job = new_relocation_job(dmc, cache);
    BUG_ON(job == NULL);
    queue_relocation_job(job);
}


void update_gc_metadata(struct cache_c *dmc, uint64_t starting_block_idx,
                                    uint64_t gc_zone_idx, s64 garbage_count) 
{
    u64 block_idx   = 0;
    struct gc_c* gc = dmc->gc;

    /* Busy wait until all async I/O is complete*/
    wait_event(gc->relocation_wq, atomic_read(&gc->relocation_counter) == 0);
    //wait_event(gc->writeback_wq, atomic_read(&gc->nr_writeback) == 0);

    /* Decrement the gc_garbage_count count based on the amount of garbage we just reclaimed*/
    //GC_STATS_LOCK();
    //GC_STATS_SUB(gc_garbage_count, garbage_count);
    //GC_STATS_UNLOCK();

    ZONE_STATS_LOCK(gc_zone_idx);
    //ZONE_STATS_SET(gc_zone_idx, gcable_count, 0);
    ZONE_STATS_INC(gc_zone_idx, reclaim_count);
    ZONE_STATS_UNLOCK(gc_zone_idx);

    /* Clear the zone by resetting the write pointer */            
    blkdev_zone_mgmt(dmc->cache_dev->bdev, REQ_OP_ZONE_RESET, 
            zone_idx_to_sector_addr(dmc, gc_zone_idx), dmc->zsize_sectors);

    spin_lock(&gc->zbd_cacheblocks_lock);

    /* Update bitmaps so that the zone is now empty */
    for (block_idx = starting_block_idx; block_idx < starting_block_idx + dmc->blocks_per_zone; block_idx++) {
        if(test_bit(block_idx, gc->bm_gcable_blocks)) {
            clear_bit(block_idx, gc->bm_gcable_blocks);
            GC_STATS_LOCK();
            GC_STATS_DEC(gc_garbage_count);
            GC_STATS_UNLOCK();
        }

#if ZBD_CACHEBLOCKS_RADIX_TREE
        radix_tree_delete(&gc->zbd_cacheblocks, block_idx);
#else
        BUG_ON(block_idx > dmc->nr_zones*dmc->blocks_per_zone);
        gc->zbd_cacheblocks[block_idx] = NULL;
#endif
    }

    spin_unlock(&gc->zbd_cacheblocks_lock);

    /* Update the full zone bitmap*/
    clear_bit(gc_zone_idx, dmc->bm_full_zones);
}

void reclaim_zone(struct cache_c *dmc, u64 starting_block_idx) 
{

    struct cacheblock* victim_cacheblock;
    struct gc_c* gc        = dmc->gc;
    u64 victim_zone_idx    = block_idx_to_zone_idx(dmc, starting_block_idx);
    u64 block_idx;

    ZONE_STATS_LOCK(victim_zone_idx);
    ZONE_STATS_SET(victim_zone_idx, gcable_count, 0);
	ZONE_STATS_SET(victim_zone_idx, ts_score, 0);
	ZONE_STATS_SET(victim_zone_idx, num_before, 0);
    ZONE_STATS_UNLOCK(victim_zone_idx);

	/* Relocate or evict based on the GC policy*/
    switch (gc->gc_mode) {
        case RELOCATION_GC: {
            for (block_idx = starting_block_idx; block_idx < starting_block_idx + dmc->blocks_per_zone; block_idx++) {
                bool relocation_allowed = true;

                if ((victim_cacheblock = gc_fetch_cacheblock(dmc, block_idx))) {

                    switch (gc->gc_relocation_mode) {

                        case RELOCATE_EVERYTHING:       break;	
                        case RELOCATE_ON_DYNAMIC_RWSS: 
                            if(victim_cacheblock->lru_state == BEFORE_THRESH)
                                relocation_allowed = false;
                            break;
                    }

                    if(relocation_allowed) {
                        gc_relocate_cacheblock(dmc, victim_cacheblock);
                        atomic64_inc(&gc->relocated_cacheblocks_count);

                        ZONE_STATS_LOCK(victim_zone_idx);
                        ZONE_STATS_INC(victim_zone_idx, cacheblocks_relocated);
                        ZONE_STATS_UNLOCK(victim_zone_idx);

                    }else {
                        /* If the cacheblock is to be flushed. There is no need for the block
                         * to be in RESERVED state */
                        spin_lock(&victim_cacheblock->lock);
                        if (is_state(victim_cacheblock->state, RESERVED))
                            clear_state(victim_cacheblock->state, RESERVED);
                        spin_unlock(&victim_cacheblock->lock);

                        gc_flush_cacheblock(dmc, victim_cacheblock); 

                        atomic64_inc(&gc->evicted_cacheblocks_count);
                        ZONE_STATS_LOCK(victim_zone_idx);
                        ZONE_STATS_INC(victim_zone_idx, cacheblocks_flushed);
                        ZONE_STATS_UNLOCK(victim_zone_idx);
                    }
                }
            }
            break;
        }

        case EVICTION_GC: {
            for (block_idx = starting_block_idx; block_idx < starting_block_idx + dmc->blocks_per_zone; block_idx++) {

                if (test_bit(block_idx, gc->bm_gcable_blocks))              
                    continue;

                spin_lock(&dmc->gc->zbd_cacheblocks_lock);
#if ZBD_CACHEBLOCKS_RADIX_TREE
                victim_cacheblock = radix_tree_lookup(&gc->zbd_cacheblocks, block_idx);
                BUG_ON(victim_cacheblock == NULL);
#else
                victim_cacheblock = dmc->gc->zbd_cacheblocks[block_idx];
#endif
                spin_unlock(&dmc->gc->zbd_cacheblocks_lock);

                gc_flush_cacheblock(dmc, victim_cacheblock); 

                atomic64_inc(&gc->evicted_cacheblocks_count);

                ZONE_STATS_LOCK(victim_zone_idx);
                ZONE_STATS_INC(victim_zone_idx, cacheblocks_flushed);
                ZONE_STATS_UNLOCK(victim_zone_idx);
            }
            break;
        }	
    }
}

static inline unsigned int gc_choose_zone_random(struct cache_c *dmc, 
                                                 struct victim_zone* target_zone) {
    unsigned short random_number;
    unsigned int zone_idx;
    struct gc_c* gc = dmc->gc;

    do {
        get_random_bytes_wait(&random_number, sizeof(unsigned short));
        zone_idx = ((random_number * dmc->nr_zones) + ((unsigned short)-1)/2) / ((unsigned short)-1);
    } while (!test_bit(zone_idx, dmc->bm_full_zones));

    target_zone->zone_idx           = zone_idx;
    ZONE_STATS_LOCK(zone_idx);
    target_zone->zone_garbage_count = ZONE_STATS_GET(zone_idx, gcable_count);
    ZONE_STATS_UNLOCK(zone_idx);
    return 0;
}

static inline unsigned int gc_choose_zone_on_garbage(struct cache_c *dmc, 
                                                 struct victim_zone* target_zone) {
    struct gc_c* gc = dmc->gc;
    unsigned int zone_idx;
    unsigned int best_zone_for_gc   = 0;
    sector_t zone_gcable_count      = 0;
    sector_t best_zone_gcable_count = 0;
    bool found_first_full_zone      = false;

    for (zone_idx = 0; zone_idx < dmc->nr_zones; zone_idx++) {

        /* Only consider full zones for GC */
        if (!test_bit(zone_idx, dmc->bm_full_zones))
            continue;

        /* Count the number of GCable blocks in each zone*/
        ZONE_STATS_LOCK(zone_idx);
        zone_gcable_count = ZONE_STATS_GET(zone_idx, gcable_count);
        ZONE_STATS_UNLOCK(zone_idx);

        // Jackpot! We have found a zone which is all GCable
        if (zone_gcable_count == dmc->blocks_per_zone) {
            DEBUG_DMC_GC("Jackpot zone!! All GCable blocks");
            target_zone->zone_idx           = zone_idx;
            target_zone->zone_garbage_count = zone_gcable_count;
            return 0;
        }

        /* Keep track of the best zone we've found thus far */
        if (!found_first_full_zone || zone_gcable_count > best_zone_gcable_count) {
            found_first_full_zone   = true;
            best_zone_for_gc        = zone_idx;
            best_zone_gcable_count  = zone_gcable_count;
        }
    }

    /* No full zones found.*/ 
    if (!found_first_full_zone) { 
        target_zone->zone_idx            = -1;
        target_zone->zone_garbage_count  = 0;
        return 0;
    }

    target_zone->zone_idx             = best_zone_for_gc; 
    target_zone->zone_garbage_count   = best_zone_gcable_count;

    return 0;
}


bool validate_ts_score(u64 zone_idx, u64 test_score, struct cache_c *dmc)
{
    u64 true_score = 0;
    u64 block_idx; 
    u64 starting_block_idx = zone_idx_to_block_idx(dmc, zone_idx);
    struct cacheblock* cacheblock;

    for(block_idx=starting_block_idx; block_idx < dmc->blocks_per_zone; block_idx++) {
        spin_lock(&dmc->gc->zbd_cacheblocks_lock);
#if ZBD_CACHEBLOCKS_RADIX_TREE
        cacheblock = radix_tree_lookup(&dmc->gc->zbd_cacheblocks, block_idx);
        BUG_ON(cacheblock == NULL);
#else
        cacheblock = dmc->gc->zbd_cacheblocks[block_idx];
#endif
        spin_unlock(&dmc->gc->zbd_cacheblocks_lock);

        if(cacheblock != NULL)
            true_score += cacheblock->last_access;
    }

    if(true_score == test_score)
        return true;

    DMWARN("WARNING: TS Score Validation Failed!! Expected:%llu, Found:%llu", true_score, test_score);
    BUG_ON(true);

}

static inline unsigned int gc_choose_zone_on_locality(struct cache_c *dmc, 
                                                 struct victim_zone* target_zone) {
    struct gc_c* gc = dmc->gc;
    unsigned int zone_idx;
    sector_t zone_gcable_count      = 0;
    u64 ts_score_summation, best_ts_score_summation, victim_zone_idx, victim_zone_gcable_count; 
    zone_idx = ts_score_summation = best_ts_score_summation = victim_zone_idx = 0;
    bool found_first_full_zone      = false;

    for (zone_idx = 0; zone_idx < dmc->nr_zones; zone_idx++) {

        /* Get the timestamp summation using the bookeeping stats*/ 
        ZONE_STATS_LOCK(zone_idx);
        ts_score_summation  = ZONE_STATS_GET(zone_idx, ts_score);
        zone_gcable_count   = ZONE_STATS_GET(zone_idx, gcable_count);
        ZONE_STATS_UNLOCK(zone_idx);

        /* Only consider full zones for GC */
        if (!test_bit(zone_idx, dmc->bm_full_zones) || zone_gcable_count == 0)
            continue;

        VALIDATE_TS_SCORE(zone_idx, ts_score_summation, dmc);

        /* Keep track of the best zone we've found thus far */
        if (!found_first_full_zone || ts_score_summation < best_ts_score_summation) {
            found_first_full_zone    = true;
            victim_zone_idx          = zone_idx;
            best_ts_score_summation  = ts_score_summation;
            victim_zone_gcable_count = zone_gcable_count;
        }
    }

    /* No full zones found.*/ 
    if (!found_first_full_zone) { 
        target_zone->zone_idx            = -1;
        target_zone->zone_garbage_count  = 0;
        return 0;
    }

    target_zone->zone_idx             = victim_zone_idx; 
    target_zone->zone_garbage_count   = victim_zone_gcable_count; 

    return 0;

}

inline s64 calc_free_space(struct gc_c *gc)
{
    struct cache_c* dmc = gc->dmc;
    s64 total_garbage_in_cache;
    s64 total_available_space = dmc->org_cache_size;
    s64 total_valid_blocks    = dmcstats_get(allocate);

    GC_STATS_LOCK();
    total_garbage_in_cache = GC_STATS_GET(gc_garbage_count);
    GC_STATS_UNLOCK();

    /* Both garbage and valid blocks consume space on the device */
    return total_available_space - (total_valid_blocks + total_garbage_in_cache);
}

void check_and_set_trigger_block_ios(struct gc_c *gc)
{
    s64 free_space = calc_free_space(gc);
    if (!atomic_read(&gc->block_ios) && free_space <= gc->gc_trigger_block_io){ 
        atomic_set(&gc->block_ios, true);
        DMWARN("BLOCK I/Os enabled!");
    }
}

inline void check_and_unset_trigger_block_ios(struct cache_c *dmc)
{
    struct gc_c* gc = dmc->gc;

    // Resume I/Os now that space has been cleared
    if (atomic_read(&gc->block_ios)) {
        atomic_set(&gc->block_ios, false);
        wake_deferred_bio_work(dmc);
        DMWARN("BLOCK I/Os disabled!");
        gc->fg_gc_iter_count++;
    }
}

inline void check_and_inc_gc_iter_count(struct gc_c *gc) 
{
    if(atomic_read(&gc->is_gc_active)){
        atomic_set(&gc->is_gc_active, false);
        gc->gc_iter_count++;
    }
}

void check_and_queue_gc_work(struct gc_c *gc)
{
    s64 free_space = calc_free_space(gc);
    if (!atomic_read(&gc->is_gc_active) && free_space <= gc->gc_trigger_start)
        queue_work(gc->gc_wq, &gc->gc_work);
}

inline int find_no_of_full_zones(struct gc_c *gc) 
{
    struct cache_c* dmc = gc->dmc;
    return bitmap_weight(dmc->bm_full_zones, dmc->nr_zones);
}

inline bool should_continue_gc(struct gc_c *gc)
{
    s64 free_space = calc_free_space(gc);
    
#if 0
    printk("[TS:%lld] [FS:%lld] [stop:%lld] [block:%lld] [FZ:%d] [GC:%d]\n", gc->dmc->org_cache_size, free_space,
            gc->gc_trigger_stop, gc->gc_trigger_block_io, find_no_of_full_zones(gc), 
            free_space <= gc->gc_trigger_stop);
#endif

    return free_space <= gc->gc_trigger_stop;
}

void do_gc(struct work_struct* work) {

    struct gc_c* gc     = container_of(work, struct gc_c, gc_work);
    struct cache_c *dmc = gc->dmc;
    struct victim_zone target_zone;

    u64 starting_block_idx = 0;

#if GC_PER_ITER_DEBUG
    s64 gc_iter_start_garbage,gc_iter_end_garbage; 
    u64 no_reclaim_per_gc_iter =0;
    gc_iter_start_garbage = gc_iter_end_garbage = 0;
    GC_STATS_LOCK();
    gc_iter_start_garbage = GC_STATS_GET(gc_garbage_count);
    GC_STATS_UNLOCK();
#endif

    while (should_continue_gc(gc)) {

        atomic_set(&gc->is_gc_active, true);

	    switch (VICTIM_SELECTION_POLICY) {
		    case RANDOM_SELECTION:          gc_choose_zone_random(dmc, &target_zone);       break;
		    case GARBAGE_BASED_SELECTION:   gc_choose_zone_on_garbage(dmc, &target_zone);   break;
            case LOCALITY_BASED_SELECTION:  gc_choose_zone_on_locality(dmc, &target_zone);  break;
		    default: BUG_ON(true);
	    }

#if VICTIM_SELECTION_POLICY == GARBAGE_BASED_SELECTION
        if (target_zone.zone_idx == (uint64_t)-1) {
            DMWARN("WARN: No full Zones Found.\n");
            gc->skip_gc_iter_count++;
            break;
        }
#elif VICTIM_SELECTION_POLICY == LOCALITY_BASED_SELECTION
        if (target_zone.zone_idx == (uint64_t)-1 || target_zone.zone_garbage_count == 0)  {
            DMWARN("WARN: No full Zones found with garbage.\n");
            gc->skip_gc_iter_count++;
            break;
        }
#endif
	    starting_block_idx = zone_idx_to_block_idx(dmc, target_zone.zone_idx);
	    reclaim_zone(dmc, starting_block_idx);
	    update_gc_metadata(dmc, starting_block_idx, target_zone.zone_idx, target_zone.zone_garbage_count);
#if GC_PER_ITER_DEBUG
        no_reclaim_per_gc_iter++;
#endif
    }

#if GC_PER_ITER_DEBUG
    if(atomic_read(&gc->is_gc_active)){

        GC_STATS_LOCK();
        gc_iter_end_garbage = GC_STATS_GET(gc_garbage_count);
        GC_STATS_UNLOCK();

        per_gc_iter_debug_update(dmc, gc->gc_iter_count, gc_iter_start_garbage, gc_iter_end_garbage, no_reclaim_per_gc_iter);
    }
#endif

    check_and_inc_gc_iter_count(gc);
    check_and_unset_trigger_block_ios(dmc);
}

void gc_cacheblock_metadata_updated(struct cache_c* dmc, struct cacheblock* cacheblock)
{
    struct gc_c* gc = dmc->gc;

    spin_lock(&gc->zbd_cacheblocks_lock);

#if ZBD_CACHEBLOCKS_RADIX_TREE
    radix_tree_insert(&gc->zbd_cacheblocks, cacheblock->cache_block_addr, cacheblock);
#else
    BUG_ON(cacheblock->cache_block_addr > dmc->nr_zones*dmc->blocks_per_zone);
    gc->zbd_cacheblocks[cacheblock->cache_block_addr] = cacheblock;
#endif

    spin_unlock(&gc->zbd_cacheblocks_lock);
}

void gc_mark_garbage(struct cache_c* dmc, u64 block_idx)
{
    struct gc_c* gc = dmc->gc;
    u64 zone_idx    = block_idx_to_zone_idx(dmc, block_idx);

    set_bit(block_idx, gc->bm_gcable_blocks);

    GC_STATS_LOCK();
    GC_STATS_INC(gc_garbage_count);
    GC_STATS_UNLOCK();

    ZONE_STATS_LOCK(zone_idx);
    ZONE_STATS_INC(zone_idx, gcable_count);
    ZONE_STATS_UNLOCK(zone_idx);

    spin_lock(&gc->zbd_cacheblocks_lock);

#if ZBD_CACHEBLOCKS_RADIX_TREE
    radix_tree_delete(&gc->zbd_cacheblocks, block_idx);
#else
    BUG_ON(block_idx > dmc->nr_zones*dmc->blocks_per_zone);
    gc->zbd_cacheblocks[block_idx] = NULL;
#endif

    spin_unlock(&gc->zbd_cacheblocks_lock);

    //check_and_set_trigger_block_ios(gc);
    //check_and_queue_gc_work(gc);
}

void gc_zone_metadata_updated(struct cache_c* dmc) {

    struct gc_c* gc = dmc->gc;

    check_and_set_trigger_block_ios(gc);
    check_and_queue_gc_work(gc);
}


void gc_reset(struct cache_c* dmc)
{
    struct gc_c* gc = dmc->gc;
    bitmap_zero(gc->bm_gcable_blocks, dmc->nr_zones*dmc->blocks_per_zone);
}

void gc_dtr(struct cache_c* dmc)
{
    struct gc_c* gc = dmc->gc;

    // Stop the background GC thread
    cancel_work_sync(&gc->gc_work);
    flush_workqueue(gc->gc_wq);
    destroy_workqueue(gc->gc_wq);

    // Wait for completion of all relocation jobs
    wait_event(gc->destroyq, !atomic_read(&gc->nr_relocation_jobs));

    kvfree(gc->bm_gcable_blocks);

#if !ZBD_CACHEBLOCKS_RADIX_TREE
    kvfree(gc->zbd_cacheblocks);
#endif

#if GC_ENABLED
    bioset_exit(&gc->relocation_bio_set);
    bioset_exit(&gc->writeback_bio_set);
#endif

    gc_jobs_exit(gc);

    kvfree(gc->per_zone_stats);
    kvfree(gc);
}

int is_block_ios_enabled(struct dm_target* ti)
{
    struct cache_c* dmc = (struct cache_c*) ti->private;
    struct gc_c* gc = dmc->gc;
    return atomic_read(&gc->block_ios);
}

#else

void init_gc(struct cache_c* dmc) {}
void do_gc(struct work_struct *ignored) {}
void gc_cacheblock_metadata_updated(struct cache_c* dmc, struct cacheblock* cacheblock) {}
void gc_mark_garbage(struct cache_c* dmc, u64 block_idx) {}
void gc_zone_metadata_updated(struct cache_c* dmc) {}
void gc_flush_bios(void) {}
void gc_reset(struct cache_c* dmc) {}
void gc_dtr(struct cache_c* dmc) {}

int is_block_ios_enabled(struct dm_target* ignored) { return 0; }
#endif
