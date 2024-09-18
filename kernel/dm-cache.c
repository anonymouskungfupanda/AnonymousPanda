#include "dm.h"
#include "gc.h"
#include "dm-cache.h"
#include "dm-error.h"

#if DO_TRACEPOINTS
#define CREATE_TRACE_POINTS
#include <trace/events/dm-trace.h>
#endif

/*****************************************************************
 *  Shared structures
 ******************************************************************/

#if WiPS_HACK
size_t FindIndex(struct cache_c *dmc, pid_t target_pid)
{
    size_t index = 0;

    while ( index < NUM_FIO_JOBS && dmc->pid_list[index] != target_pid ) {
        ++index;
    }

    return ( index == NUM_FIO_JOBS ? -1 : index );
}

void set_curret_write_head(struct cache_c *dmc)
{
    int index;

    dmc->current_pid = task_pid_nr(current);

    index = FindIndex(dmc, dmc->current_pid);

    if(index < 0) {
        dmc->pid_list[dmc->pid_index] = dmc->current_pid;
        index = dmc->pid_index;
        dmc->pid_index++;
    }

    if(index <= (NUM_FIO_JOBS - 2))
        dmc->current_write_head = 0;
    else 
        dmc->current_write_head = 1;
}
#endif

static void destroy_radix_tree(struct cache_c* dmc);
static void cache_flush(struct cache_c *dmc);
static int cache_read_miss(struct cache_c *dmc, struct bio* bio, struct cacheblock *cache);
static int cache_write_miss(struct cache_c *dmc, struct bio* bio, struct cacheblock *cache);
static void io_callback(unsigned long error, void *context);
static void write_back(struct cache_c *dmc, struct cacheblock *cache, unsigned int length);
static int cache_hit(struct cache_c *dmc, struct bio* bio, struct cacheblock *cache);
static void process_deferred_bios(struct work_struct *work);
static void queue_kcached_job(struct kcached_job *job);

/*****************************************************************
 *  Functions for managing the kcached job & workqueue 
 ******************************************************************/
static struct kcached_job *new_kcached_job(struct cache_c *dmc, struct bio* bio, sector_t request_block, 
                                           struct cacheblock *cache);
static inline struct kcached_job *pop(struct list_head *jobs, struct cache_c *dmc);
static inline void push(struct list_head *jobs, struct kcached_job *job);
static void do_work(struct work_struct *work); 
static int do_complete(struct kcached_job *job); 
static int do_store(struct kcached_job *job);
static void wake_kcached_work(struct cache_c *dmc) {

    queue_work(dmc->_kcached_wq, &dmc->_kcached_work);
}

void wake_deferred_bio_work(struct cache_c *dmc) {

    queue_work(dmc->_kcached_wq, &dmc->deferred_bio_work);
}

/*****************************************************************
 *  Functions for managing the active zone data structure
 ******************************************************************/

static struct rb_root active_zones_tree = RB_ROOT;

static inline unsigned int active_zones_count(struct cache_c *dmc)
{
    return dmc->active_zones_count;
}

//static inline bool active_zones_insert(unsigned int zone_idx)
static inline bool active_zones_insert(struct cache_c *dmc, unsigned int zone_idx)
{
    struct rb_node **new = &active_zones_tree.rb_node, *parent = NULL;
    struct active_zone_container *cmp, *ins;

    //if (shared_cache->active_zones_count >= shared_cache->active_zones_max)
    if (dmc->active_zones_count >= dmc->active_zones_max)
        return false;

    /* obtain an unused container */
    ins = list_first_entry(&dmc->active_zones_containers, struct active_zone_container, list);
    ins->zone_idx = zone_idx;
    list_del(&ins->list);

    while (*new) {
        parent  = *new;
        cmp     = rb_entry(*new, struct active_zone_container, node);

        /* this shouldn't happen */
        if (cmp->zone_idx == zone_idx)
            goto err;

        if (cmp->zone_idx < zone_idx)
            new = &((*new)->rb_left);
        else
            new = &((*new)->rb_right);
    }

    rb_link_node(&ins->node, parent, new);
    rb_insert_color(&ins->node, &active_zones_tree);

    ++dmc->active_zones_count;
    return true;

err:
    DMWARN("Warning: Attempting to insert a zone that is already active\n");
    return false;
}

/* A helper function for active_zones_remove and active_zones_contains */
static struct active_zone_container *active_zone_search(unsigned int zone_idx) 
{
    struct rb_node *cur = active_zones_tree.rb_node;
    struct active_zone_container *res;

    while (cur) {
        res = rb_entry(cur, struct active_zone_container, node);

        /* index found */
        if (res->zone_idx == zone_idx)
            return res;

        if (res->zone_idx < zone_idx)
            cur = cur->rb_left;
        else
            cur = cur->rb_right;
    }

    /* not found */
    return NULL;
}

static inline void active_zones_remove(struct cache_c *dmc, unsigned int zone_idx)
{
    struct active_zone_container *cur = active_zone_search(zone_idx);
    if (cur) {
        rb_erase(&cur->node, &active_zones_tree);
        --dmc->active_zones_count;

        /* place container back in unused list */
        list_add_tail(&cur->list, &dmc->active_zones_containers);
    }
}

static inline bool active_zones_contains(unsigned int zone_idx)
{
    struct active_zone_container *cur = active_zone_search(zone_idx);

    // False if NULL, true otherwise
    return cur;
}

static inline void active_zones_print(void)
{
    /*unsigned int i;     -- I will return to this shortly
    DEBUG_DMC_ACTIVE_ZONES("START ARRAY!");
    for (i = 0; i < shared_cache->active_zones_count; i++)
        DEBUG_DMC_ACTIVE_ZONES("%u", shared_cache->active_zones_array[i]);
    DEBUG_DMC_ACTIVE_ZONES("END ARRAY!");*/
}

/****************************************************************************
 *  Wrapper functions for using the new dm_io API
 ****************************************************************************/

static inline char* get_command(struct cache_c *dmc, int cmd)
{
	if(cmd == READ) 
        return "READ"; 
    else if(dmc->cache_dev_is_zoned)
            return "ZONE_APPEND";
    else
        return "WRITE";
}

static void dmcache_bio_error_handler(struct bio *bio, blk_status_t status)
{
    DMWARN("Bio Execution Failed due to: %s", blk_status_to_str(status));
    switch(status)
    {
        case BLK_STS_ZONE_OPEN_RESOURCE:
            break;
        case BLK_STS_ZONE_ACTIVE_RESOURCE:
            break;
        case BLK_STS_IOERR:
            /* */
            DMERR("Bio failed for Cacheblock request: %llu",
                    ((struct kcached_job *) bio->bi_private)->cacheblock->cache_block_addr);
            DMERR("Bio failed : bio->bi_iter.bi_sector %llu", bio->bi_iter.bi_sector);
            break;
    }
}

/****************************************************************************
 * Bookeeping functions 
 ****************************************************************************/
/*
 * bk is short for book-keeping
 *  - The last_access of the block must be updated before calling this function.
 *  - This is only intended to be called in the cache_hit path. The ts_score maintenence for
 *    the cache miss path for both read and write is handled in request_cache_block and also
 *    zbd_prep_for_write.
 */
void update_ts_score_bk(struct cache_c *dmc, struct cacheblock *block, u64 prev_ts, bool is_read) 
{
	struct gc_c *gc = dmc->gc;
	u64 zone_idx    = block_idx_to_zone_idx(dmc, block->cache_block_addr);

	ZONE_STATS_LOCK(zone_idx);
    ZONE_STATS_SUB(zone_idx, ts_score, prev_ts); 

	/* Because read hits don't call prep_for_write, this needs to be done here
	 * otherwise the blocks score won't be added back to the ts_score of the
	 * zone the block is placed in 
	 */

	if (is_read)
		ZONE_STATS_ADD(zone_idx, ts_score, block->last_access);
    
	ZONE_STATS_UNLOCK(zone_idx);
}

/* This f() is called after a write request is prepared and ready to be issued*/
void update_curr_zone_stats(struct cache_c *dmc, struct cacheblock *block)
{
	struct gc_c *gc = dmc->gc;
	u64 zone_idx    = block_idx_to_zone_idx(dmc, block->cache_block_addr);

	ZONE_STATS_LOCK(zone_idx);

    /* To increment the pending writes for the zone, this will be decreased 
     * after the metadata is updated after the write request is complete */

    ZONE_STATS_INC(zone_idx, pending_writes); 

	/* Update lru_state book keeping - when this function is called from either cache_hit
	 * or cache_miss, the state of the block is guarunteed to be after the threshold. For
	 * relocation, it may be before or after. Hence, the counter corresponding to whatever
	 * the existing lru_state of the block is incremented to account for both cases.*/

	if (block->lru_state == BEFORE_THRESH)
		ZONE_STATS_INC(zone_idx, num_before);

    /* Update ts_score book keeping - we can get away with doing this here because we  
	 * know which zone this block is being appended to, and also because the timestamp
	 * should have been updated BEFORE calling this function 
	 */
	ZONE_STATS_ADD(zone_idx, ts_score, block->last_access);
	ZONE_STATS_UNLOCK(zone_idx);
}

void update_zone_timestamp_stats(struct cache_c *dmc, struct cacheblock *block) 
{
    struct gc_c *gc = dmc->gc;
    u64 zone_idx = block_idx_to_zone_idx(dmc, block->cache_block_addr);

    ZONE_STATS_LOCK(zone_idx);

    if (block->last_access >= ZONE_STATS_GET(zone_idx, max_timestamp))
        ZONE_STATS_SET(zone_idx, max_timestamp, block->last_access);

    if (block->last_access <= ZONE_STATS_GET(zone_idx, min_timestamp))
        ZONE_STATS_SET(zone_idx, min_timestamp, block->last_access);

    ZONE_STATS_UNLOCK(zone_idx);
}


/****************************************************************************
 * I/O callback functions 
 ****************************************************************************/

static void io_callback(unsigned long error, void *context)
{
    struct kcached_job *job = (struct kcached_job *) context;
    struct cache_c *dmc     = job->dmc;
    struct bio *bio         = job->bio; /* Currently, this bio is the original bio (context of dmcache) */
    struct bio *clone_bio;

    if (error) {
        /* TODO */
        DMERR("io_callback: io error");
        return;
    }

    if (job->rw == READ) {

        job->rw = WRITE;

        /* Clone the bio and prep for the do_store */
    	clone_bio = bio_alloc_bioset(job->dest.bdev, 1, REQ_OP_ZONE_APPEND, GFP_NOIO, &dmc->bio_set);
	
        BUG_ON(clone_bio == NULL);

        if(bio_add_page(clone_bio, job->pages->page, PAGE_SIZE, 0) != PAGE_SIZE) {
            bio_put(clone_bio);
            DMERR("Clone bio page allocation failed");
            return;
        }

        bio_copy_data(clone_bio, bio);

        // Update the job's bio context to the clone_bio
        clone_bio->bi_ioprio = bio->bi_ioprio;
        job->bio = clone_bio;

        // End the Orginal bio
        bio_endio(bio);

        push(&job->dmc->_io_jobs, job);
    } else {   
        push(&job->dmc->_complete_jobs, job);
    }

    wake_kcached_work(job->dmc);
}

// Called after the WRITE (do_store()) portion of a WRITE HIT/MISS is finished
static void write_bi_end_io(struct bio *bio)
{
	struct kcached_job  *job = (struct kcached_job *) bio->bi_private;
	struct cache_c      *dmc = job->dmc;

	if(likely(bio->bi_status == BLK_STS_OK))
	{
		if(dmc->cache_dev_is_zoned) 
			job->cacheblock->cache_block_addr = sector_addr_to_block_idx(dmc, bio->bi_iter.bi_sector);

		// Release the cloned bio and end the original bio
		bio_put(bio);
		bio_endio(job->bio);
		// Finish the job using workqueue 
		queue_kcached_job(job);
	}
}

// Called after the WRITE (do_store()) portion of a READ MISS is finished
static void read_bi_end_io(struct bio *bio)
{
	int error = 0;

	struct kcached_job  *job = (struct kcached_job *) bio->bi_private;
	struct cache_c      *dmc = job->dmc;

	if(likely(bio->bi_status == BLK_STS_OK))
	{
		if(dmc->cache_dev_is_zoned) 
			job->cacheblock->cache_block_addr = sector_addr_to_block_idx(dmc, bio->bi_iter.bi_sector);

		// Releases the clone bio. The original bio was already ended before the write started
		bio_put(bio);
		io_callback(error, (void *)job);
	} else {
		//Error Handling for ZBD Cache Device
		DMWARN("Bio Execution Failed!! Error Recovery will be attempted !!");
		bio_put(bio);
		dmcache_bio_error_handler(bio, bio->bi_status);
	}
}


/****************************************************************************
 * Functions for handling pages used by async I/O.
 * The data asked by a bio request may not be aligned with cache blocks, in
 * which case additional pages are required for the request that is forwarded
 * to the server. A pool of pages are reserved for this purpose.
 ****************************************************************************/
static struct page_list *alloc_pl(void)
{
    struct page_list *pl;

    pl = kmalloc(sizeof(*pl), GFP_KERNEL);
    if (!pl)
        return NULL;

    pl->page = alloc_page(GFP_KERNEL);
    if (!pl->page) {
        kfree(pl);
        return NULL;
    }

    return pl;
}

static void free_pl(struct page_list *pl)
{
    __free_page(pl->page);
    kfree(pl);
}

static void drop_pages(struct page_list *pl)
{
    struct page_list *next;

    while (pl) {
        next = pl->next;
        free_pl(pl);
        pl = next;
    }
}

int kcached_get_pages(struct cache_c *dmc, unsigned int nr, struct page_list **pages)
{
    struct page_list *pl;

    spin_lock(&dmc->page_lock);
    if (dmc->nr_free_pages < nr) {
        DMERR("kcached_get_pages: No free pages: %u<%u",
                dmc->nr_free_pages, nr);
        spin_unlock(&dmc->page_lock);
        return -ENOMEM;
    }

    dmc->nr_free_pages -= nr;
    for (*pages = pl = dmc->pages; --nr; pl = pl->next);

    dmc->pages 	= pl->next;
    pl->next 	= NULL;

    spin_unlock(&dmc->page_lock);

    return 0;
}

void kcached_put_pages(struct cache_c *dmc, struct page_list *pl)
{
    struct page_list *cursor;

    spin_lock(&dmc->page_lock);
    for (cursor = pl; cursor->next; cursor = cursor->next)
        dmc->nr_free_pages++;

    dmc->nr_free_pages++;
    cursor->next = dmc->pages;
    dmc->pages = pl;

    spin_unlock(&dmc->page_lock);
}

static int alloc_bio_pages(struct cache_c *dmc, unsigned int nr)
{
    unsigned int i;
    struct page_list *pl = NULL, *next;

    for (i = 0; i < nr; i++) {
        next = alloc_pl();
        if (!next) {
            if (pl)
                drop_pages(pl);
            return -ENOMEM;
        }
        next->next = pl;
        pl = next;
    }

    kcached_put_pages(dmc, pl);
    dmc->nr_pages += nr;

    return 0;
}

static void free_bio_pages(struct cache_c *dmc)
{
    BUG_ON(dmc->nr_free_pages != dmc->nr_pages);
    drop_pages(dmc->pages);
    dmc->pages = NULL;
    dmc->nr_free_pages = dmc->nr_pages = 0;
}

/****************************************************************************
 * Workqueue and kcached job fucntions
 ****************************************************************************/

static int jobs_wq_init(struct cache_c *dmc)
{
    dmc->_job_cache = kmem_cache_create("kcached-jobs",sizeof(struct kcached_job),
                                    __alignof__(struct kcached_job),0, NULL);
    if (!dmc->_job_cache)
        return -ENOMEM;

    dmc->_job_pool = mempool_create(MIN_JOBS, mempool_alloc_slab, mempool_free_slab, dmc->_job_cache);
    if (!dmc->_job_pool) {
        kmem_cache_destroy(dmc->_job_cache);
        return -ENOMEM;
    }

    spin_lock_init(&dmc->_job_lock);
    INIT_LIST_HEAD(&dmc->_complete_jobs);
    INIT_LIST_HEAD(&dmc->_io_jobs);

    dmc->_kcached_wq = alloc_workqueue("kcached", WQ_MEM_RECLAIM, 0);
    if (!dmc->_kcached_wq) {
        DMERR("failed to start kcached");
        return -ENOMEM;
    }

    INIT_WORK(&dmc->_kcached_work, do_work);
    INIT_WORK(&dmc->deferred_bio_work, process_deferred_bios);

    DMINFO("dmcache: kcached jobs Initialized");

    return DMCACHE_SUCCESS;
}

static void jobs_wq_exit(struct cache_c *dmc)
{
    BUG_ON(!list_empty(&dmc->_complete_jobs));
    BUG_ON(!list_empty(&dmc->_io_jobs));

    mempool_destroy(dmc->_job_pool);
    kmem_cache_destroy(dmc->_job_cache);
    dmc->_job_pool  = NULL;
    dmc->_job_cache = NULL;

    destroy_workqueue(dmc->_kcached_wq);
}

/****************************************************************************
 * Functions to push and pop a job onto the head of a given job list.
 ****************************************************************************/
static inline struct kcached_job *pop(struct list_head *jobs, struct cache_c *dmc)
{
    struct kcached_job *job = NULL;
    unsigned long flags;

    spin_lock_irqsave(&dmc->_job_lock, flags);

    if (!list_empty(jobs)) {
        job = list_entry(jobs->next, struct kcached_job, list);
        list_del(&job->list);
    }
    spin_unlock_irqrestore(&dmc->_job_lock, flags);

    return job;
}

static inline void push(struct list_head *jobs, struct kcached_job *job)
{
    unsigned long flags;
    struct cache_c *dmc = job->dmc; 

    spin_lock_irqsave(&dmc->_job_lock, flags);
    list_add_tail(&job->list, jobs);
    spin_unlock_irqrestore(&dmc->_job_lock, flags);
}

/****************************************************************************
 * Functions for asynchronously fetching data from source device and storing
 * data in cache device. Because the requested data may not align with the
 * cache blocks, extra handling is required to pad a block request and extract
 * the requested data from the results.
 ****************************************************************************/

static int cache_read_sync(struct kcached_job *job) {

	struct dm_io_request iorq;
	int ret = 0;

	atomic_inc(&job->dmc->nr_jobs);
	ret = kcached_get_pages(job->dmc, job->nr_pages, &job->pages);

	if (ret == -ENOMEM) /* can't complete now */
		return -1;

	/* The request is aligned to cache block */
	iorq.bi_opf         = REQ_OP_READ | REQ_SYNC;
	//iorq.bi_op_flags    = 0;
	iorq.mem.type       = DM_IO_BIO;
	iorq.mem.ptr.bio    = job->bio;
	iorq.notify.fn      = io_callback;
	iorq.client         = job->dmc->io_client;
	iorq.notify.context = (void *)job;

	dm_io(&iorq, 1, &job->src, NULL, IOPRIO_DEFAULT);

	return DM_MAPIO_SUBMITTED;
}


/* Original bio request is WRITE*/
static int cache_write_sync(struct kcached_job *job) {

	struct cache_c *dmc = job->dmc;
	struct bio *bio     = job->bio;
	struct bio  *clone_bio;

	clone_bio = bio_alloc_clone(job->dest.bdev, bio, GFP_NOIO, &dmc->bio_set);
	BUG_ON(clone_bio == NULL);

	if(dmc->cache_dev_is_zoned)
		clone_bio->bi_opf = REQ_OP_ZONE_APPEND;
	else 
		clone_bio->bi_opf = REQ_OP_WRITE;

	/* If cache_dev is a ZBD device, the sector here represents the stating sector of the zone */
	bio_set_dev(clone_bio, job->dest.bdev);
	clone_bio->bi_iter.bi_sector  = job->dest.sector;
	clone_bio->bi_private         = (void *)job;
	clone_bio->bi_end_io          = write_bi_end_io;

	submit_bio_noacct(clone_bio);

	return DM_MAPIO_SUBMITTED;
}


/* only used for async write on cache read miss */
static int do_store(struct kcached_job *job)
{
	struct bio *clone_bio  	      = job->bio;

	clone_bio->bi_iter.bi_size    = PAGE_SIZE;
	clone_bio->bi_end_io          = read_bi_end_io;

	if(job->dmc->cache_dev_is_zoned) 
		clone_bio->bi_opf = REQ_OP_ZONE_APPEND;
	else
		clone_bio->bi_opf = REQ_OP_WRITE;

	/* If cache_dev is a ZBD device, the sector here represents the stating sector of the zone */
	bio_set_dev(clone_bio, job->dest.bdev);
	clone_bio->bi_iter.bi_sector  = job->dest.sector;
	clone_bio->bi_private         = (void *)job;

	submit_bio_noacct(clone_bio);

	return DM_MAPIO_SUBMITTED;
}

/* Flush the bios that are waiting for this cache insertion or write back.*/
void flush_bios(struct cache_c *dmc, struct cacheblock *cacheblock) {

    struct bio_list* bio_list = &cacheblock->bios;
    struct bio *bio, *n;

    spin_lock(&cacheblock->lock);
    // Normal device
    if (!dmc->cache_dev_is_zoned) { 
        bio = bio_list_get(bio_list);
        spin_unlock(&cacheblock->lock);
        while (bio) {
            n = bio->bi_next;
            bio->bi_next = NULL;
            DMINFO("Flush bio: %llu->%llu (%u bytes)",
                    (unsigned long long)cacheblock->src_block_addr, 
                    (unsigned long long)bio->bi_iter.bi_sector, 
                    (unsigned)bio->bi_iter.bi_size);

            dmcache_trace(dmcache_submit_bio_noacct, bio);
            submit_bio_noacct(bio);
            bio = n;
        }
        return;
    }

    // ---------- ZNS cache device ----------

    // Figure out how many bios we can yank out of the bio list and submit in one go
    bio = bio_list->head;
    while (bio_list->head && (bio_list->head->bi_bdev == dmc->cache_dev->bdev && 
            bio_data_dir(bio_list->head) == READ)) 
        bio_list->head = bio_list->head->bi_next;
    if (bio_list->head && bio_list->head != bio_list->tail) {
        struct bio* temp = bio_list->head->bi_next;
        bio_list->head->bi_next = NULL;
        bio_list->head = temp;
    } else {
        bio_list->head = bio_list->tail = NULL;
    }
    spin_unlock(&cacheblock->lock);

    // Now submit those previously mentioned bios    
    while (bio) {
        n = bio->bi_next;
        bio->bi_next = NULL;

        // Some requests are completely untouched. Just resubmit them
        if (bio->bi_bdev != dmc->cache_dev->bdev) {
            if (bio_data_dir(bio) == READ)
                cache_read_miss(dmc, bio, cacheblock);
            else
                cache_write_miss(dmc, bio, cacheblock);
        } else {
            // Fix other requests so that that they will READ/WRITE correctly
            if (bio_data_dir(bio) == READ) {
                // Update old READ bio to the new location of the cacheblock

                bio->bi_iter.bi_sector &= dmc->block_mask; /* Keep sector offset bits */
                bio->bi_iter.bi_sector |= block_idx_to_sector_addr(dmc, cacheblock->cache_block_addr)
                                                        & ~dmc->block_mask;

                // DMINFO("Flush READ bio: cache idx: %llu, bio idx: %llu", 
                //     cacheblock->cache_block_addr, sector_addr_to_block_idx(dmc, bio->bi_iter.bi_sector));

                submit_bio_noacct(bio);
            } else {
                // If we need to write, we have to move the cacheblock again and wait 
                // to continue executing the rest of the bios in the bio list.
                //DMINFO("Flush WRITE bio: cache idx: %llu", cacheblock->cache_block_addr);
                cache_hit(dmc, bio, cacheblock);
            }
        }

        bio = n;
    }
}

void finish_zone_append(struct cache_c *dmc, struct cacheblock *cacheblock)
{
    unsigned int zone_idx;
    sector_t zone_block_offset;
	struct gc_c *gc = dmc->gc;
    u64 pending_writes;

    zone_idx            = block_idx_to_zone_idx(dmc, cacheblock->cache_block_addr);
    zone_block_offset   = cacheblock->cache_block_addr - zone_idx_to_block_idx(dmc, zone_idx);

    ZONE_STATS_LOCK(zone_idx);
    ZONE_STATS_DEC(zone_idx, pending_writes);
    pending_writes = ZONE_STATS_GET(zone_idx, pending_writes);
    ZONE_STATS_UNLOCK(zone_idx);

#if PENDING_WRITES_VALIDATION
    if (pending_writes == 0) {
#else
    if (zone_block_offset + 1 >= dmc->blocks_per_zone) {
#endif

        // TODO decide during cache_ctr if zone finish commands are necessary and if so trigger this code
        if (0) {
            // We must manually issue a zone finish command as there is some extra space
            DEBUG_DMC_ZBD("Issuing finish command for zone %u", zone_idx);
        }

        /* This was the last write operation for the zone, so we shall mark it as full
         * and remove it from the active zones list */
        DEBUG_DMC_ACTIVE_ZONES("Removing zone %u from the active zone list", zone_idx);
        set_bit(zone_idx, dmc->bm_full_zones);

        spin_lock(&dmc->active_zones_lock);
        active_zones_remove(dmc, zone_idx);
        DEBUG_DMC_ACTIVE_ZONES("There are %u zones remaining:", active_zones_count(dmc));
        //active_zones_print();
        spin_unlock(&dmc->active_zones_lock);
    }
}

void cacheblock_update_state(struct cache_c *dmc, struct cacheblock *cacheblock)
{
    spin_lock(&cacheblock->lock);
    if (unlikely(is_state(cacheblock->state, WRITEBACK))) { 
        /* Write back finished */
        cacheblock->state = VALID;
    } else if (unlikely(is_state(cacheblock->state, WRITETHROUGH))) { 
        cache_invalidate(dmc, cacheblock, true);
    } else { 
        /* Cache insertion finished */
        set_state(cacheblock->state, VALID);
        clear_state(cacheblock->state, RESERVED);
    }
    spin_unlock(&cacheblock->lock);
}

// Called at the end of the READ MISS
static int do_complete(struct kcached_job *job)
{
    struct cache_c *dmc = job->dmc;

    if (job->nr_pages > 0)
        kcached_put_pages(job->dmc, job->pages);

    /* ---- BEGIN DO NOT CHANGE ORDER ---- */
#if GC_ENABLED
    if(dmc->cache_dev_is_zoned)
        gc_cacheblock_metadata_updated(dmc, job->cacheblock);
#endif /* GC_ENABLED*/

    cacheblock_update_state(dmc, job->cacheblock);

    if (dmc->cache_dev_is_zoned)
        finish_zone_append(dmc, job->cacheblock);

    flush_bios(dmc, job->cacheblock);
    /* ---- END DO NOT CHANGE ORDER ---- */

    mempool_free(job, dmc->_job_pool);

    if (atomic_dec_and_test(&dmc->nr_jobs))
        wake_up(&dmc->destroyq);

    return 0;
}

/*
 * Run through a list for as long as possible.  Returns the count
 * of successful jobs.
 */
static int process_jobs(struct list_head *jobs, struct cache_c *dmc, int (*fn) (struct kcached_job *))
{
    struct kcached_job *job;
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

static void do_work(struct work_struct *work)
{
    struct cache_c *dmc = container_of(work, struct cache_c, _kcached_work); 
    process_jobs(&dmc->_complete_jobs, dmc, do_complete);
    process_jobs(&dmc->_io_jobs, dmc, do_store);
}

#if 1
static void queue_kcached_job(struct kcached_job *job) {

    struct cache_c *dmc = job->dmc;

    atomic_inc(&dmc->nr_jobs);
    if (job->nr_pages > 0) /* Request pages */
        push(&dmc->_pages_jobs, job);
    // else if (job->original_bio_data_dir == READ) /* Read jobs use the wq instead of calling do_fetch() directly */
    //     push(&dmc->_io_jobs, job);
    else /* Write jobs call do_store() directly, and are only queued for the completion step */
        push(&dmc->_complete_jobs, job);

    wake_kcached_work(dmc);
}
#endif

static int kcached_init(struct cache_c *dmc)
{
    int ret;

    spin_lock_init(&dmc->page_lock);
    spin_lock_init(&dmc->lru_lock);

    dmc->pages      = NULL;
    dmc->nr_pages   = dmc->nr_free_pages = 0;
    ret             = alloc_bio_pages(dmc, DMCACHE_COPY_PAGES);
    if (ret) {
        DMERR("kcached_init: Could not allocate bio pages");
        return ret;
    }

    init_waitqueue_head(&dmc->destroyq);
    init_waitqueue_head(&dmc->wait_writeback);
    atomic_set(&dmc->nr_jobs, 0);

    return DMCACHE_SUCCESS;
}

static void kcached_client_destroy(struct cache_c *dmc)
{
    /* Wait for completion of all jobs submitted by this client. */
    wait_event(dmc->destroyq, !atomic_read(&dmc->nr_jobs));

    free_bio_pages(dmc);
}


/****************************************************************************
 * Functions for writing back dirty blocks.
 * We leverage kcopyd to write back dirty blocks because it is convenient to
 * use and it is not reasonble to reimplement the same function here. But we
 * need to reserve pages for both kcached and kcopyd. TODO: dynamically change
 * the number of reserved pages.
 ****************************************************************************/
static void copy_callback(struct cache_c *dmc, int read_err, unsigned int write_err, void *context)
{
    struct cacheblock *cacheblock   = (struct cacheblock *) context;
    flush_bios(dmc, cacheblock);

    dmcstats_dec(dirty_blocks);
    if (dmcstats_get(dirty_blocks) == 0) {
        wake_up(&dmc->wait_writeback);
    }
}

static void copy_block(struct cache_c *dmc, struct dm_io_region src,
        struct dm_io_region dest, struct cacheblock *cacheblock)
{
    DMINFO("Copying: %llu:%llu->%llu:%llu",
            (unsigned long long)src.sector, (unsigned long long)src.count * 512, 
            (unsigned long long)dest.sector, (unsigned long long)dest.count * 512);
    dm_kcopyd_copy(dmc->kcp_client, &src, 1, &dest, 0, (dm_kcopyd_notify_fn) copy_callback, cacheblock);
}

static void write_back(struct cache_c *dmc, struct cacheblock *cache, unsigned int length)
{
    struct dm_io_region src, dest;

    DMINFO("Write back block (%llu, %u)",(unsigned long long)cache->cache_block_addr, 
            (unsigned)length);

    src.bdev 	= dmc->cache_dev->bdev;
    src.sector 	= block_idx_to_sector_addr(dmc, cache->cache_block_addr);
    src.count 	= dmc->block_size * length;

    dest.bdev	= dmc->src_dev->bdev;
    dest.sector = cache->src_block_addr << dmc->block_shift;
    dest.count 	= dmc->block_size * length;

    set_state(cache->state, WRITEBACK);
    dmcstats_set(dirty_blocks, (dmcstats_get(dirty_blocks) - length));
    copy_block(dmc, src, dest, cache);
}


/****************************************************************************
 *  Functions for implementing the various cache operations.
 ****************************************************************************/

/****************************************************************************
 * Insert a block into the cache (in the frame specified by cache_block).
 ****************************************************************************/
static int cache_insert(struct cache_c *dmc, sector_t block, struct cacheblock *cache)
{
    set_state(cache->state, RESERVED);

    cache->src_block_addr = block >> dmc->block_shift;

    spin_lock(&dmc->cache_lock);
    radix_tree_insert(dmc->cache, block >> dmc->block_shift, (void *) cache);
    spin_unlock(&dmc->cache_lock);

    return 1;
}

/****************************************************************************
 * Invalidate a block (specified by cache_block) in the cache.
 ****************************************************************************/
void cache_invalidate(struct cache_c *dmc, struct cacheblock *cache, bool has_cache_lock)
{
    if (has_cache_lock) {
        cache->state = INVALID;
    } else {
        spin_lock(&cache->lock);
        cache->state = INVALID;
        spin_unlock(&cache->lock);
    }
    
    spin_lock(&dmc->cache_lock);
    radix_tree_delete(dmc->cache, cache->src_block_addr);
    spin_unlock(&dmc->cache_lock);
}

/****************************************************************************
 * Handle a cache hit:
 *  For READ, serve the request from cache is the block is ready; otherwise,
 *  queue the request for later processing.
 *  For write, invalidate the cache block if write-through. If write-back,
 *  serve the request from cache if the block is ready, or queue the request
 *  for later processing if otherwise.
 ****************************************************************************/
static int cache_hit(struct cache_c *dmc, struct bio* bio, struct cacheblock *cache)
{
    unsigned int offset   = (unsigned int)(bio->bi_iter.bi_sector & dmc->block_mask);
    struct kcached_job *job;

#if VICTIM_SELECTION_POLICY == LOCALITY_BASED_SELECTION
    /* Update the timestamp related stats*/
    u64 prev_ts           = cache->last_access;
    cache->last_access    = dmc_atomic_get(timestamp_counter);
    update_ts_score_bk(dmc, cache, prev_ts, bio_data_dir(bio) == READ);
    update_zone_timestamp_stats(dmc, cache);
#endif

    spin_lock(&dmc->lru_lock);

#if GC_ENABLED
    advance_lru_thresh(dmc, cache,!(bio_data_dir(bio) == READ));
#endif

    list_move_tail(&cache->list, dmc->lru);
    spin_unlock(&dmc->lru_lock);

    dmcstats_inc(cache_hits);

    if (bio_data_dir(bio) == READ) { 

        /* READ Hit */
        dmcstats_inc(read_hits);

        spin_lock(&cache->lock);
        bio_set_dev(bio, dmc->cache_dev->bdev);
        bio->bi_iter.bi_sector = block_idx_to_sector_addr(dmc, cache->cache_block_addr) + offset;

        if (is_state(cache->state, VALID)) { 
            /* Valid cache block */
            spin_unlock(&cache->lock);
            return DM_MAPIO_REMAPPED;
        }

        /* Cache block is not ready yet. Add to the bio list */
        bio_list_add(&cache->bios, bio);
        spin_unlock(&cache->lock);

        return DM_MAPIO_SUBMITTED;
    } else { 
        /* WRITE hit */
        dmcstats_inc(invalidates);

        spin_lock(&cache->lock);
        /* Write delay */
        if (!is_state(cache->state, DIRTY)) {
            set_state(cache->state, DIRTY);
            dmcstats_inc(dirty_blocks);
        }

        /* Cache block not ready yet. Add to the bio list */
        if (is_state(cache->state, RESERVED)) {
            bio_set_dev(bio, dmc->cache_dev->bdev);
            bio->bi_iter.bi_sector = block_idx_to_sector_addr(dmc, cache->cache_block_addr) + offset;
            bio_list_add(&cache->bios, bio);
            spin_unlock(&cache->lock);
            return DM_MAPIO_SUBMITTED;
        }

        /* Serve the request from cache */
        if(dmc->cache_dev_is_zoned) {
            set_state(cache->state, RESERVED);

            spin_unlock(&cache->lock);

            zbd_prepare_for_write(dmc, cache, true);

#if GC_ENABLED
            update_curr_zone_stats(dmc, cache);
#endif

            job = new_kcached_job(dmc, bio, 0, cache);

            /* Do not push the write hit job in the workqueue. The device mapper SHOULD NOT wait 
             * to submit any IO if there is no need for it. As the I/O submission is synchronous 
             * in the dmcache thread.
             */
            cache_write_sync(job);

            return DM_MAPIO_SUBMITTED;
        }

        /* Regular Cache Device */
        bio_set_dev(bio, dmc->cache_dev->bdev);
        bio->bi_iter.bi_sector = block_idx_to_sector_addr(dmc, cache->cache_block_addr) + offset;

        spin_unlock(&cache->lock);

        return DM_MAPIO_REMAPPED;
    }
}

static struct kcached_job *new_kcached_job(struct cache_c *dmc, struct bio* bio, sector_t request_block, 
                                            struct cacheblock *cache)
{
    struct dm_io_region src, dest;
    struct kcached_job *job;

    src.bdev 	= dmc->src_dev->bdev;
    src.sector 	= request_block;
    src.count 	= dmc->block_size;

    dest.bdev 	= dmc->cache_dev->bdev;
    dest.sector	= block_idx_to_sector_addr(dmc, cache->cache_block_addr);
    dest.count 	= src.count;

    job 	    = mempool_alloc(dmc->_job_pool, GFP_KERNEL);
    job->dmc 	= dmc;
    job->bio 	= bio;
    job->src 	= src;
    job->dest 	= dest;
    job->cacheblock = cache;
    job->rw     = job->original_bio_data_dir = bio_data_dir(bio);

    /* Write jobs do not create a new bio. So they don't need any pages.
     * Read jobs on the other hand DO create a new bio. So they need a page*/
    job->nr_pages = job->rw == WRITE ? 0 : 1;

    return job;
}


static inline int select_new_empty_zone(struct cache_c *dmc)
{
    unsigned int selected_zone_idx;

    /* Find a zone which is suitable for writing. It is possible that we will "find" a zone that is
     * already open for writing. So in that case, we will perform a repeated search
     */
    selected_zone_idx = find_first_zero_bit(dmc->bm_full_zones, dmc->nr_zones);
    while (selected_zone_idx < dmc->nr_zones && active_zones_contains(selected_zone_idx)) {
        selected_zone_idx = find_next_zero_bit(dmc->bm_full_zones, dmc->nr_zones, selected_zone_idx + 1);
    }

    return selected_zone_idx;
}

#if WiPS_HACK
static inline void reset_current_write_head(struct cache_c *dmc)
{
    int i;
    unsigned int new_zone_idx;
    unsigned long flags;

    spin_lock_irqsave(&dmc->active_zones_lock, flags);

    new_zone_idx            = select_new_empty_zone(dmc);
    BUG_ON(new_zone_idx >= dmc->nr_zones);
    dmc->current_zones[dmc->current_write_head] = new_zone_idx;
    DEBUG_DMC_ACTIVE_ZONES("Adding zone %u to the active zones list", new_zone_idx);
    active_zones_insert(dmc, new_zone_idx);
    BUG_ON(active_zones_count(dmc) == dmc->active_zones_max);

    spin_unlock_irqrestore(&dmc->active_zones_lock, flags);
}
#endif

#if DMC_ZBD_WRITE_MODE == ZBD_MULTI_WRITE_HEAD
static inline void reset_zbd_write_heads(struct cache_c *dmc)
{
    int i;
    unsigned int new_zone_idx;

    spin_lock(&dmc->active_zones_lock);
    for (i = 0; i < NUM_WRITE_HEADS; i++) 
    {
        new_zone_idx = select_new_empty_zone(dmc);
        BUG_ON(new_zone_idx >= dmc->nr_zones);
        dmc->current_zones[i] = new_zone_idx;
        DEBUG_DMC_ACTIVE_ZONES("Adding zone %u to the active zones list", new_zone_idx);
        active_zones_insert(dmc, new_zone_idx);
    }
    BUG_ON(active_zones_count(dmc) == dmc->active_zones_max);
    spin_unlock(&dmc->active_zones_lock);


#if WiPS_HACK
    for (i = 0; i < NUM_WRITE_HEADS; i++) 
        dmc->wp_block[i] = 0;

    for (i=0; i< NUM_FIO_JOBS; i++)
        dmc->pid_list[i] = -1;

    dmc->pid_index = 0;
#endif


}
#endif

void zbd_prepare_for_write(struct cache_c *dmc, struct cacheblock *cacheblock, bool mark_old_location_as_gcable)
{
    unsigned int zero_bit_location;
    unsigned long flags;

#if GC_ENABLED
    /* Mark the old location of this cacheblock as garbage. 
     * Check GT Triggers and queue GC work if required */
    if (mark_old_location_as_gcable) {
        dmcstats_inc(garbage);
        gc_mark_garbage(dmc, cacheblock->cache_block_addr);
    }

    check_and_set_trigger_block_ios(dmc->gc);
    check_and_queue_gc_work(dmc->gc);
#endif

    /* Grab the write_loc_mutex so we can reserve some space to perform the write*/
    mutex_lock(&dmc->zbd_write_loc_mutex);

#if DMC_ZBD_WRITE_MODE == ZBD_SINGLE_WRITE_HEAD
    // Increment the write pointer and leave if we can complete this operation without switching zones
    dmc->wp_block++;

    if (dmc->wp_block <= dmc->blocks_per_zone) {

        // On the last cacheblock of each zone, check if we need to enable blocked I/Os
        if (dmc->wp_block == dmc->blocks_per_zone)
            gc_zone_metadata_updated(dmc);

        // Perform the write to the current zone
        cacheblock->cache_block_addr = zone_idx_to_block_idx(dmc, dmc->current_zone);
        mutex_unlock(&dmc->zbd_write_loc_mutex);
        return;
    }

    //DMWARN("Beware serving this bio request will require a zone transition. Zone %d is full", dmc->current_zone);

    spin_lock_irqsave(&dmc->active_zones_lock, flags);

#if 0
    // Busy wait if it is unsafe to open another zone. One of the active zones will probably finish writing shortly
    while (active_zones_count(dmc) == dmc->active_zones_max) {
        mutex_unlock(&dmc->active_zones_lock);
        DMWARN_LIMIT("Can't have any more active zones!!");
        usleep_range(1, 100);
        mutex_lock(&dmc->active_zones_lock);
    }
#endif

    /* Ideally the scenario where there are no active zone are available should not be allowed to occur.
     * For now, we replace the busy wait when it is unsafe to open another zone with a BUG_ON() */
    BUG_ON(active_zones_count(dmc) == dmc->active_zones_max);

    zero_bit_location = select_new_empty_zone(dmc);
    BUG_ON(zero_bit_location >= dmc->nr_zones);

    DEBUG_DMC_ACTIVE_ZONES("Adding zone %u to the active zones list", zero_bit_location);
    active_zones_insert(dmc, zero_bit_location);
    
    spin_unlock_irqrestore(&dmc->active_zones_lock, flags);

    // Transition the zone
    dmc->current_zone = zero_bit_location;
    dmc->wp_block     = 1;

    // Perform the write to the newly selected zone zone
    cacheblock->cache_block_addr = zone_idx_to_block_idx(dmc, dmc->current_zone);
        
#else
    // Multi write Head Mode

#if WiPS_HACK
    set_curret_write_head(dmc);
#endif

    cacheblock->cache_block_addr = zone_idx_to_block_idx(dmc, dmc->current_zones[dmc->current_write_head]);

#if WiPS_HACK
    dmc->wp_block[dmc->current_write_head]++;

    if (dmc->wp_block[dmc->current_write_head] == dmc->blocks_per_zone) {
        reset_current_write_head(dmc);
        dmc->wp_block[dmc->current_write_head] 	 = 0;
    }

#else
    dmc->zone_batch_size++;

    if (dmc->zone_batch_size >= WRITE_HEADS_BLOCK_SIZE)
    {
        /* This is a time to select a new zone */
        dmc->current_write_head++;

        if (dmc->current_write_head >= NUM_WRITE_HEADS) { 
            /* We have passed through the whole array. Start over and increment wp_block*/

            dmc->wp_block += WRITE_HEADS_BLOCK_SIZE;
            dmc->current_write_head = 0;

            if (dmc->wp_block == dmc->blocks_per_zone) {
                reset_zbd_write_heads(dmc);
                dmc->wp_block = 0;

                gc_zone_metadata_updated(dmc);
            }
        }

        dmc->zone_batch_size = 0;
    }
#endif

 
#endif /* DMC_ZBD_WRITE_MODE == ZBD_SINGLE_WRITE_HEAD */

    mutex_unlock(&dmc->zbd_write_loc_mutex);
}

static struct cacheblock* request_cache_block(struct cache_c *dmc, bool *was_cacheblock_evicted)
{
    struct cacheblock *cache;
	struct gc_c *gc = dmc->gc;

    DEBUG_DMC("DM_REQUEST: Allocatate:%llu", dmcstats_get(allocate));

    spin_lock(&dmc->lru_lock);
    cache = list_first_entry(dmc->lru, struct cacheblock, list);

#if GC_ENABLED
    advance_lru_thresh(dmc, cache, false);
#endif
    list_move_tail(&cache->list, dmc->lru);
    spin_unlock(&dmc->lru_lock);

    // Take item from empty List
    if(dmcstats_get(allocate) < dmc->cache_size) {
        dmcstats_inc(allocate);
        *was_cacheblock_evicted = false;
    } else {
        // Below its limit

        // TODO We must perform a flush to the source device BEFORE doing this
        // Fixing the dirty block counter
        spin_lock(&cache->lock);
        if (is_state(cache->state, DIRTY)) {
            clear_state(cache->state, DIRTY);
            // TODO Add lock because this is accessed from GC also
            dmcstats_dec(dirty_blocks);
        }
        spin_unlock(&cache->lock);
        *was_cacheblock_evicted = true;

#if GC_ENABLED
		// Update lru_state and ts_score book keeping
		u64 zone_idx = block_idx_to_zone_idx(dmc, cache->cache_block_addr);
		ZONE_STATS_LOCK(zone_idx);
		if (cache->lru_state == BEFORE_THRESH)
			ZONE_STATS_DEC(zone_idx, num_before);
		ZONE_STATS_SUB(zone_idx, ts_score, cache->last_access); 
		ZONE_STATS_UNLOCK(zone_idx);
#endif

    }

    return cache;
}

/****************************************************************************
 * Handle a read cache miss:
 *  Update the metadata; fetch the necessary block from source device;
 *  store data to cache device.
 ****************************************************************************/
static int cache_read_miss(struct cache_c *dmc, struct bio* bio, struct cacheblock* cache) {
    unsigned int offset;
    struct kcached_job *job;
    sector_t request_block;

    /* Default to true, so that when called from flush_bios(), we mark the old cacheblock as dirty */
    bool was_cacheblock_evicted = true;

    offset 	        = (unsigned int)(bio->bi_iter.bi_sector & dmc->block_mask);
    request_block   = bio->bi_iter.bi_sector - offset;

    /* If this is being called from cache_map(), we need to get a cacheblock using LRU
     * Otherwise, this may be being called from flush_bios(), in which case we already have a cacheblock*/
    if (!cache) 
        cache = request_cache_block(dmc, &was_cacheblock_evicted);

#if VICTIM_SELECTION_POLICY == LOCALITY_BASED_SELECTION
    /* Update the timestamp related stats*/
    cache->last_access = dmc_atomic_get(timestamp_counter);
    update_zone_timestamp_stats(dmc, cache);
#endif

    spin_lock(&cache->lock);
    if(is_state(cache->state, RESERVED) || is_state(cache->state, WRITEBACK)) {
        bio_list_add(&cache->bios, bio);
        spin_unlock(&cache->lock);
        return DM_MAPIO_SUBMITTED;
    }

    if (is_state(cache->state, VALID)) {
        dmcstats_inc(replace);
        cache_invalidate(dmc, cache, true);
    } else {
        dmcstats_inc(inserts);
    }

    /* Update the cache metadata */
    cache_insert(dmc, request_block, cache); 
    spin_unlock(&cache->lock);

    /* Reserve write location for ZBD cache device */
    if (dmc->cache_dev_is_zoned) {
        zbd_prepare_for_write(dmc, cache, was_cacheblock_evicted);

#if GC_ENABLED
        update_curr_zone_stats(dmc, cache);
#endif
    }

    job = new_kcached_job(dmc, bio, request_block, cache);

    cache_read_sync(job);

    return DM_MAPIO_SUBMITTED;
}

/****************************************************************************
 * Handle a write cache miss:
 *  If write-through, forward the request to source device.
 *  If write-back, update the metadata; fetch the necessary block from source
 *  device; write to cache device.
 ****************************************************************************/
static int cache_write_miss(struct cache_c *dmc, struct bio* bio, struct cacheblock *cache) 
{
    unsigned int offset;
    struct kcached_job *job;
    sector_t request_block;

    /* Default to true, so that when called from flush_bios(), we mark the old cacheblock as dirty*/
    bool was_cacheblock_evicted = true;

    offset          = (unsigned int)(bio->bi_iter.bi_sector & dmc->block_mask);
    request_block   = bio->bi_iter.bi_sector - offset;

    /* If this is being called from cache_map(), we need to get a cacheblock using LRU
     * Otherwise, this may be being called from flush_bios(), in which case we already have a cacheblock*/
    if (!cache) 
        cache = request_cache_block(dmc, &was_cacheblock_evicted);

#if VICTIM_SELECTION_POLICY == LOCALITY_BASED_SELECTION
    /* Update the timestamp related stats*/
    cache->last_access = dmc_atomic_get(timestamp_counter);
    //update_zone_timestamp_stats(dmc, cache);
#endif

    spin_lock(&cache->lock);
    if(is_state(cache->state, RESERVED) || is_state(cache->state, WRITEBACK)) {
        bio_list_add(&cache->bios, bio);
        spin_unlock(&cache->lock);
        return DM_MAPIO_SUBMITTED;
    }

    if (is_state(cache->state, VALID)) {
        dmcstats_inc(replace);
        cache_invalidate(dmc, cache, true);
    } else { 
        dmcstats_inc(inserts);
    }

    // if (dmc->write_policy == WRITE_BACK) {
    //     set_state(cache->state, DIRTY);
    //     dmcstats_inc(dirty_blocks);
    // }

    // Write hit and write miss should both cause cacheblock state to go to DIRTY
    if (!is_state(cache->state, DIRTY)) {
        set_state(cache->state, DIRTY);
        dmcstats_inc(dirty_blocks);
        DEBUG_DMC("[Cache-HIT-Write] Mark the cacheblock DIRTY"); 
    }

    cache_insert(dmc, request_block, cache); /* Update metadata first */
    spin_unlock(&cache->lock);

    if (dmc->cache_dev_is_zoned) {
        zbd_prepare_for_write(dmc, cache, was_cacheblock_evicted);
#if GC_ENABLED
        update_curr_zone_stats(dmc, cache);
#endif
    }

    job = new_kcached_job(dmc, bio, request_block, cache);

    /* Do not push the write miss job in the workqueue. The device mapper SHOULD NOT wait to submit any IO 
     * if there is no need for it. As the I/O submission is synchronous in the dmcache thread.
     */
    cache_write_sync(job);

    return DM_MAPIO_SUBMITTED;
}

/* Handle cache misses */
static int cache_miss(struct cache_c *dmc, struct bio* bio) 
{
    
    dmcstats_inc(cache_misses);
    if (bio_data_dir(bio) == READ){
        dmcstats_inc(read_misses);
        return  cache_read_miss(dmc, bio, NULL);
    } else {
        dmcstats_inc(write_misses);
        return cache_write_miss(dmc, bio, NULL);
    }
}

/****************************************************************************
 *  Functions for deferring bios due to low space.
 ****************************************************************************/

static void defer_bio(struct cache_c *dmc, struct bio *bio)
{
    spin_lock(&dmc->def_bio_list_lock);
    bio_list_add(&dmc->deferred_bios, bio);
    spin_unlock(&dmc->def_bio_list_lock);
}

static void process_deferred_bios(struct work_struct *work)
{
    struct cache_c *dmc = container_of(work, struct cache_c, deferred_bio_work);
	struct bio_list bios;
	struct bio *bio;

	bio_list_init(&bios);

	spin_lock(&dmc->def_bio_list_lock);
	bio_list_merge(&bios, &dmc->deferred_bios);
	bio_list_init(&dmc->deferred_bios);
	spin_unlock(&dmc->def_bio_list_lock);

	while ((bio = bio_list_pop(&bios))) {
        DEBUG_DMC_BIO("Flush GC bio: Bio bi_sector: %llu", bio->bi_iter.bi_sector);
        submit_bio_noacct(bio);
	}
}

/****************************************************************************
 *  Functions for implementing the operations on a cache mapping.
 ****************************************************************************/

#if GC_ENABLED
static void update_wss_rwss(struct cache_c *dmc, sector_t request_block) {

    u64 offset = request_block >> dmc->block_shift;
    void **wss_ctx, *rwss_ctx;

    struct radix_tree_iter iter;
    struct cacheblock* cache;
    void** slot;

    spin_lock(&dmc->locality_lock);
    wss_ctx  = radix_tree_lookup(dmc->wss, offset);

    if (wss_ctx != NULL) {
        rwss_ctx = radix_tree_lookup(dmc->rwss_t, offset);
        if (rwss_ctx == NULL) {
            dmc->reuse_working_set_t++;
            radix_tree_insert(dmc->rwss_t, offset, (void*)(xa_mk_value(offset)));
        }
    } else {
        dmc->working_set++;
        radix_tree_insert(dmc->wss, offset, (void*)(xa_mk_value(offset)));
    }


    if(dmc->gc->gc_relocation_mode == RELOCATE_ON_DYNAMIC_RWSS) {

        if(dmc_atomic_get(timestamp_counter) % dmc->gc->rwsst_window_interval == 0) {

            u64 new_relocation_lru_thresh = dmc->gc->rwss_percentage * dmc->reuse_working_set_t;

            printk("Window Update: Old-Thresh:%lld New-Thresh:%lld WSS:%lld, RWSS-T:%lld\n",
                    dmc->gc->relocation_lru_thresh, new_relocation_lru_thresh, dmc->working_set, dmc->reuse_working_set_t);

            spin_lock(&dmc->lru_lock);
            move_lru_thresh(dmc, new_relocation_lru_thresh);
            spin_unlock(&dmc->lru_lock);

            /* Reset rwss_t tracker */
            dmc->reuse_working_set_t  = 0;

            // Delete the Radix tree 
            radix_tree_for_each_slot(slot, dmc->rwss_t, &iter, 0) {
                cache = *slot;
                radix_tree_iter_delete(dmc->rwss_t, &iter, slot);
            }
        }
    }

    spin_unlock(&dmc->locality_lock);

}
#endif


/* Decide the mapping and perform necessary cache operations for a bio request.*/
static int cache_map(struct dm_target *ti, struct bio *bio) {

    struct cache_c *dmc = (struct cache_c*)ti->private;
    struct cacheblock *cache;
    sector_t request_block, offset;

#if GC_ENABLED
    if (dmc->cache_dev_is_zoned && is_block_ios_enabled(ti)) {
        defer_bio(dmc, bio);
        return DM_MAPIO_SUBMITTED;
    }
#endif

    offset 	       = bio->bi_iter.bi_sector & dmc->block_mask;
    request_block  = bio->bi_iter.bi_sector - offset;

#if GC_ENABLED
    dmc_atomic_inc(timestamp_counter);
    update_wss_rwss(dmc, request_block);
#endif

#if GC_STATS_DEBUG
    u64 total_garbage_in_cache;
    GC_STATS_LOCK();
    total_garbage_in_cache = GC_STATS_GET(gc_garbage_count);
    GC_STATS_UNLOCK();
    dmz_gc_debug_update(dmc, dmc_atomic_get(timestamp_counter), total_garbage_in_cache);
#endif

    if (bio_data_dir(bio) == READ)
        dmcstats_inc(reads);
    else
        dmcstats_inc(writes);

    spin_lock(&dmc->cache_lock);
    cache = radix_tree_lookup(dmc->cache, request_block >> dmc->block_shift);
    spin_unlock(&dmc->cache_lock);

    if (cache != NULL) // Cache hit; server request from cache 
        return cache_hit(dmc, bio, cache);
    else // Cache miss; replacement block is found 
        return cache_miss(dmc, bio);
}


struct meta_dmc {
    sector_t size;
    unsigned int block_size;
    unsigned int assoc;
    unsigned int write_policy;
    unsigned int chksum;
};

static void put_src_devices(void)
{
    /* TODO
     * Implement cleaning in case of error 
     * and putting all src_dev is required
     */
    return;
}

#ifdef CONFIG_BLK_DEV_ZONED

/* Return a corresponding zone number of a sector*/
static inline int dmc_zone_no(sector_t zsize_sectors, sector_t sector)
{
    return sector >> ilog2(zsize_sectors);
}

static int dmc_report_zone_cb(struct blk_zone *zone, unsigned int idx, void *data)
{
	struct dmc_report_zones_args *rz_args = data;

	if (zone->type == BLK_ZONE_TYPE_CONVENTIONAL)
		return 0;

	/* FIXME: Rethink this */
	//set_bit(idx, rz_args->bm_zones);

	rz_args->zone_capacity = zone->capacity;

	if (zone->len != zone->capacity && !rz_args->zone_cap_mismatch)
		rz_args->zone_cap_mismatch = true;

	return 0;
}

static int dmc_report_one_zone_cb(struct blk_zone *zone, unsigned int idx, void *data)
{
    memcpy(data, zone, sizeof(struct blk_zone));
    return 0;
}

/* This fucntion will report a single zone and return the wp of the zone.*/
static inline int dmc_report_zone_wp(struct block_device *bdev, sector_t start)
{
    struct blk_zone zone;
    int err;

    err = blkdev_report_zones(bdev, start, 1, dmc_report_one_zone_cb, &zone);
    if(err != 1) {
        DMERR("Report Zone Failed !!");
        return err;
    }

    if(zone.type != BLK_ZONE_TYPE_SEQWRITE_REQ)
        return 0;

    return zone.wp;
}
#endif /*CONFIG_BLK_DEV_ZONED */


/****************************************************************************
 * Construct a cache mapping.
 *  arg[0]: path to source device
 *  arg[1]: path to cache device
 *  arg[2]: cache block size (in sectors)
 *  arg[3]: cache size (in blocks)
 *  arg[4]: Number of zones for ZBD Cache device
 *  arg[5]: write caching policy
 ****************************************************************************/
static int cache_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
    int ret = -EINVAL;
    int count;
    struct cache_c *dmc;
    struct sysinfo sys_info;
    sector_t order, data_size, dev_size;
    unsigned long long cache_size;

#ifdef CONFIG_BLK_DEV_ZONED
    struct   dmc_report_zones_args zone_args;
#endif /*CONFIG_BLK_DEV_ZONED */

    DMINFO("Name: %s", dm_table_device_name(ti->table));

    // Read all the command line parameters passed to the dmcache device driver
    if (argc < 2) {
        ti->error = "dm-cache: Need at least 2 arguments (src dev and cache dev)";
        goto bad;
    }

    dmc = kvmalloc(sizeof(*dmc), GFP_KERNEL);
    if (dmc == NULL) {
        ti->error   = "dm-cache: Failed to allocate cache context";
        ret         = ENOMEM;
        goto bad1;
    }

    // Adding source device: arg[0]: path to source device
    ret = dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &dmc->src_dev);
    if (ret) {
        ti->error = "dm-cache: Source device lookup failed";
        goto bad1;
    }

    DMINFO("Registering Source device %s:%llu",
            dmc->src_dev->name,(long long unsigned int)dmc->src_dev->bdev->bd_dev);

    // Adding cache device: arg[1]: path to cache device
    DMINFO("Registering Cache Device:%s",argv[1]);

    //Copy the device names
    strcpy(dmc->src_dev_name, argv[0]);
    strcpy(dmc->cache_dev_name, argv[1]);

    ret = dm_get_device(ti, argv[1], dm_table_get_mode(ti->table), &dmc->cache_dev);
    if (ret) {
        ti->error = "dm-cache: Cache device lookup failed";
        goto bad2;
    }
    
    // Setup the workqueues
    ret = jobs_wq_init(dmc); 
    if (ret) {
        ti->error = "dm-cache: Failed to initialize jobs pool & workqueue";
        goto bad2;
    }

    dmc->io_client = dm_io_client_create();
    if (IS_ERR(dmc->io_client)) {
        ret         = PTR_ERR(dmc->io_client);
        ti->error   = "Failed to create io client\n";
        goto bad3;
    }

    dmc->kcp_client = dm_kcopyd_client_create(NULL);
    if (dmc->kcp_client == NULL) {
        ti->error = "Failed to initialize kcopyd client\n";
        goto bad4;
    }

    ret = kcached_init(dmc);
    if (ret) {
        ti->error = "Failed to initialize kcached";
        goto bad5;
    }

    // Create the bioset
    ret = bioset_init(&dmc->bio_set, 1024, 0, BIOSET_NEED_BVECS);
    if (ret) {
        ti->error = "Failed to create bio_set";
        goto bad6;
    }

    // Read: arg[2]: cache block size (in sectors)
    if (argc >= 3) {
        if (sscanf(argv[2], "%u", &dmc->block_size) != 1) {
            ti->error   = "dm-cache: Invalid block size";
            ret         = -EINVAL;
            goto bad6;
        }
        if (!dmc->block_size || (dmc->block_size & (dmc->block_size - 1))) {
            ti->error   = "dm-cache: Invalid block size";
            ret         = -EINVAL;
            goto bad6;
        }
    } else {
        dmc->block_size = DEFAULT_BLOCK_SIZE;
    }

    dmc->block_shift 	= ffs(dmc->block_size) - 1;
    dmc->block_mask 	= dmc->block_size - 1;

    // arg[3]: cache size (in blocks)
    if (argc >= 4) {
        if (sscanf(argv[3], "%llu", &cache_size) != 1) {
            ti->error   = "dm-cache: Invalid cache size";
            ret         = -EINVAL;
            goto bad6;
        }
        dmc->cache_size = (sector_t) cache_size;
    } else {
        dmc->cache_size = DEFAULT_CACHE_SIZE;
    }

    // arg[4]: Number of zones for ZBD Cache device
    if (argc >= 5) {
        if (sscanf(argv[4], "%u", &dmc->nr_zones) != 1) {
            ti->error   = "dm-cache: Invalid Number of zones";
            ret         = -EINVAL;
            goto bad6;
        }
    } else
        dmc->assoc = DEFAULT_CACHE_ASSOC;

    //dev_size  = to_sector(dmc->cache_dev->bdev->bd_inode->i_size);
    dev_size  = bdev_nr_sectors(dmc->cache_dev->bdev);
    data_size = dmc->cache_size * dmc->block_size;
    dmc->bits = ffs(dmc->cache_size) - 1;

    if (data_size > dev_size) {
        DMERR("Requested cache size exeeds the cache device's capacity (%llu > %llu)",
              (unsigned long long) data_size,
              (unsigned long long) dev_size);
        ti->error = "dm-cache: Invalid cache size";
        ret       = -EINVAL;
        goto bad6;
    }

    // arg[5]: write caching policy
    if (argc >= 6) {
        if (sscanf(argv[5], "%u", &dmc->write_policy) != 1) {
            ti->error   = "dm-cache: Invalid cache write policy";
            ret         = -EINVAL;
            goto bad6;
        }
        if (dmc->write_policy != 0 && dmc->write_policy != 1 && dmc->write_policy != 2) {
            ti->error   = "dm-cache: Invalid cache write policy";
            ret         = -EINVAL;
            goto bad6;
        }
    } else {
        dmc->write_policy = DEFAULT_WRITE_POLICY;
    }

    // Check system information
    order = dmc->cache_size * sizeof(struct cacheblock) + sizeof(*dmc->lru);
    si_meminfo(&sys_info);
    DMINFO("Free memory before: %lu needed:%llu",sys_info.freeram,
           (unsigned long long) (order>0 ? (order>>10)/4 : 0));

    if(sys_info.freeram < (order>>10)/4){
        DMERR("Requested cache size needs (%llukB) free Memory, Free memory (%lukB)",(unsigned long long) order>>10,
              sys_info.freeram * 4);
        ti->error   = "Not enough Memory to allocate dm-cache metadata";
        ret         = -ENOMEM;
        goto bad6;
    }
#ifdef CONFIG_BLK_DEV_ZONED

    /* ZBD Support*/
    dmc->cache_dev_is_zoned = bdev_is_zoned(dmc->cache_dev->bdev);
    dmc->cache_dev_is_zoned ? DMINFO("cache device is a zoned device") : 
                              DMINFO("Cache Device is not a zoned device");

    if (dmc->cache_dev_is_zoned) 
    {
        dmc->start_seq_write_zones = 0; /* FIXME: Ideally should not be set to 0 instead 
                                           should befetched from the cache_device */
        // Sanitize dmc->nr_zones
        dmc->total_nr_zones = bdev_nr_zones(dmc->cache_dev->bdev);
        if(dmc->nr_zones)
            dmc->nr_zones = min(dmc->nr_zones, dmc->total_nr_zones);
        else
            dmc->nr_zones = dmc->total_nr_zones; 

        //Maximum Ative & Open Zone Limits from the ZBD 
        dmc->cache_dev_max_open_zones   = bdev_max_open_zones(dmc->cache_dev->bdev);
        dmc->cache_dev_max_active_zones = bdev_max_active_zones(dmc->cache_dev->bdev);

        //Zone Size
        dmc->zsize_sectors = bdev_zone_sectors(dmc->cache_dev->bdev);

        //Make sure dmcache can fit in dmc->nr_zones in the given cache_size
        //TODO: Add a warning message if the nr_zones is updated.
        dmc->nr_zones = min(dmc->nr_zones, (unsigned int)((dmc->cache_size * dmc->block_size)/dmc->zsize_sectors));

        zone_args.zone_cap_mismatch = false;
        ret = blkdev_report_zones(dmc->cache_dev->bdev, 0, dmc->nr_zones, dmc_report_zone_cb, &zone_args);
        if (ret < 0)
            DMERR("Error: Failed to Report Zones!");

        //Zone Capacity
        dmc->zcap_sectors    = zone_args.zone_capacity;
        dmc->blocks_per_zone = dmc->zcap_sectors / dmc->block_size;

        // Fix cache size so that it actually represents the number of cache blocks we can store in the given zones
        dmc->cache_size     = dmc->nr_zones * dmc->blocks_per_zone;
        dmc->org_cache_size = dmc->cache_size;

        mutex_init(&dmc->zbd_write_loc_mutex);

        /* Prepare the data structure for managing active or in-use zones. 
         * We will limit the number of active zones to match the hardware max open zones limit.
         */
        spin_lock_init(&dmc->active_zones_lock);
        dmc->active_zones_max = dmc->cache_dev_max_open_zones == 0 ? dmc->nr_zones : dmc->cache_dev_max_open_zones;
        DEBUG_DMC_ACTIVE_ZONES("Active zone max:%d", dmc->active_zones_max);
        INIT_LIST_HEAD(&dmc->active_zones_containers);
        for (count = 0; count < dmc->active_zones_max; count++) {
            struct active_zone_container *new = kvmalloc(sizeof(struct active_zone_container), GFP_KERNEL);
            list_add(&new->list, &dmc->active_zones_containers);
        }

        //Bitmap Initialization
        dmc->bm_full_zones    = bitmap_kvmalloc(dmc->nr_zones, GFP_KERNEL);
#if !WiPS_HACK
        dmc->wp_block         = 0;
        dmc->zone_batch_size  = 0;
#endif

#if DMC_ZBD_WRITE_MODE == ZBD_SINGLE_WRITE_HEAD
        dmc->current_zone       = find_first_zero_bit(dmc->bm_full_zones, dmc->nr_zones);
        dmc->active_zones_count = 0;
        active_zones_insert(dmc, dmc->current_zone);
#else
        reset_zbd_write_heads(dmc);
#endif

        // Issue reset command for all zones on the cache device so that we start in a known state
        //blkdev_zone_mgmt(dmc->cache_dev->bdev, REQ_OP_ZONE_RESET, dmc->start_seq_write_zones, dmc->nr_zones*dmc->block_size, GFP_KERNEL);

#if GC_ENABLED
        init_gc(dmc);
#endif

    }
#endif /*CONFIG_BLK_DEV_ZONED */

    /* Allocating all the space for Metadata  */

    spin_lock_init(&dmc->cache_lock);

    dmc->cache = (struct radix_tree_root *)kvmalloc(sizeof(*dmc->cache), GFP_KERNEL);
    if (!dmc->cache) {
        ti->error   = "Unable to allocate memory";
        ret         = -ENOMEM;
        goto bad6;
    }
    INIT_RADIX_TREE(dmc->cache, GFP_NOWAIT);

    dmc->lru = (struct list_head *)kvmalloc(sizeof(*dmc->lru), GFP_KERNEL);
    if(dmc->lru == NULL){
        ti->error   = "Unable to allocate memory for LRU list";
        ret         = -ENOMEM;
        goto bad7;
    }
    INIT_LIST_HEAD(dmc->lru);

    spin_lock_init(&dmc->def_bio_list_lock);
    bio_list_init(&dmc->deferred_bios);

    dmc->wss    = (struct radix_tree_root *)kvmalloc(sizeof(*dmc->wss), GFP_KERNEL);
    if(dmc->wss == NULL) {
        ti->error   = "Unable to allocate memory for WSS Tree";
        ret         = -ENOMEM;
        goto bad8;
    }
    INIT_RADIX_TREE(dmc->wss, GFP_NOWAIT);

    dmc->rwss_t = (struct radix_tree_root *)kvmalloc(sizeof(*dmc->rwss_t), GFP_KERNEL);
    if(dmc->rwss_t == NULL) {
        ti->error   = "Unable to allocate memory for RWSS Tree";
        ret         = -ENOMEM;
        goto bad9;
    }
    INIT_RADIX_TREE(dmc->rwss_t, GFP_NOWAIT);

    DMINFO("Allocate %lluKB (%luB per) mem for %llu-entry cache" \
            "(capacity:%lluMB, associativity:%u, block size:%u " \
            "sectors(%uKB), %s)",
            (unsigned long long) order >> 10, (unsigned long) sizeof(struct cacheblock),
            (unsigned long long) dmc->cache_size,
            (unsigned long long) data_size >> (20-SECTOR_SHIFT),
            dmc->assoc, dmc->block_size,
            dmc->block_size >> (10-SECTOR_SHIFT),
            dmc->write_policy == 1 ? "write-back" : 
            (dmc->write_policy == 0 ? "write-through":"write-allocate"));


    spin_lock_init(&dmc->locality_lock);
    init_dmcache_lru(dmc);
    ENSURE_LRU_STATES();

    ti->max_io_len  = dmc->block_size;
    ti->private     = dmc;

    log_dmcache_configuration(dmc);

    return DMCACHE_SUCCESS;

bad9:
    kvfree(dmc->wss);
bad8:
    kvfree(dmc->lru);
bad7:
    kvfree(dmc->cache);
bad6:
    bioset_exit(&dmc->bio_set);
    kcached_client_destroy(dmc);
bad5:
    dm_kcopyd_client_destroy(dmc->kcp_client);
bad4:
    dm_io_client_destroy(dmc->io_client);
bad3:
    dm_put_device(ti,dmc->cache_dev);
    dm_put_device(ti,dmc->src_dev);
bad2:
    put_src_devices();
    kfree(dmc);
bad1:
    //kfree(virtual_mapping);
bad:
    return ret;
}

static void cache_flush(struct cache_c *dmc)
{
    struct cacheblock *cache;
    struct list_head *temp;

    DMINFO("Flush dirty blocks (%llu) ...", dmcstats_get(dirty_blocks));

    // TODO - Verify that no lock is required since this is called in cache_dtr only
    //down(&dmc->lru_lock);
    list_for_each(temp, dmc->lru) {
        cache = list_entry(temp, struct cacheblock, list);
        if(is_state(cache->state, DIRTY))
            write_back(dmc,cache, 1);
    }
    //up(&dmc->lru_lock);
}

// Delete radix tree. Does not delete cacheblocks
// No locks. But this should not be called while 
// the radix tree is in use anyway
static void destroy_radix_tree(struct cache_c* dmc)
{
    struct radix_tree_iter iter;
    struct cacheblock* cache;
    void** slot;

    radix_tree_for_each_slot(slot, dmc->cache, &iter, 0) {
        cache = *slot;
        radix_tree_iter_delete(dmc->cache, &iter, slot);
    }

    kvfree(dmc->cache);
}

/*
 * Destroy the cache mapping.
 */
static void cache_dtr(struct dm_target *ti)
{
    struct cache_c *dmc = (struct cache_c *) ti->private;
    struct active_zone_container *tmp;
    unsigned int count;

#if DEBUG_GC
	dmz_gc_dump_debug_logs(); 
#endif

    /* The below code is yet to be tested */
    DMINFO("Pre-wait on destroyq");
    if (atomic_read(&dmc->nr_jobs)) {
        DMINFO("Waiting on destroyq");
        wait_event(dmc->destroyq, !atomic_read(&dmc->nr_jobs));
    }

    // Stop the background GC thread and clean up GC data structures
    if (dmc->cache_dev_is_zoned)
        gc_dtr(dmc);

    DMINFO("Pre-wait on writeback. Dirty block count: %llu", dmcstats_get(dirty_blocks));
    // TODO - Fix dirty_blocks counter. Currently cache flush is disabled for ease of testing other things
    if (0 && dmcstats_get(dirty_blocks) > 0) {
        cache_flush(dmc);
        DMINFO("Waiting on writeback");
        wait_event(dmc->wait_writeback, (dmcstats_get(dirty_blocks) ==  0));
    }

    /*
     * Wait for jobs to complete before put the device
     */
    DMINFO("Finished waiting!");

    kcached_client_destroy(dmc);
    bioset_exit(&dmc->bio_set);
    dm_kcopyd_client_destroy(dmc->kcp_client);

    if (dmcstats_get(reads) + dmcstats_get(writes) > 0)
	    DMINFO("stats: reads(%llu), writes(%llu), cache hits(%llu, 0.%llu)," \
			    "replacement(%llu), replaced dirty blocks(%llu), " \
			    "flushed dirty blocks(%llu)",
			    dmcstats_get(reads), dmcstats_get(writes), 
                dmcstats_get(cache_hits),
			    dmcstats_get(cache_hits) * 100 / (dmcstats_get(reads) + dmcstats_get(writes)),
			    dmcstats_get(replace), dmcstats_get(writeback), 
                dmcstats_get(dirty));

    //destroy_radix_tree(dmc);

    kvfree((void *)dmc->lru);
    dm_io_client_destroy(dmc->io_client);
    dm_put_device(ti,dmc->cache_dev);
    dm_put_device(ti,dmc->src_dev);

    if (dmc->cache_dev_is_zoned) {
        kvfree(dmc->bm_full_zones);

        /* freeing active zones data structures */
        for (count = 0; count < dmc->active_zones_count; count++) {
            tmp = rb_entry(active_zones_tree.rb_node, struct active_zone_container,node);
            active_zones_remove(dmc, tmp->zone_idx);
        }
        for (count = 0; count < dmc->active_zones_max; count++) {
            tmp = list_first_entry(&dmc->active_zones_containers, struct active_zone_container, list);
            list_del(&tmp->list);
            kvfree(tmp);
        }
    }

    jobs_wq_exit(dmc);

    kvfree(dmc);
}


static void reset_dmcache(struct cache_c *dmc)
{
    struct cacheblock *cache;
    struct list_head *temp;
    struct xarray* new_cache;
    unsigned int count;
    u64 index;

    // Create new radix tree
    new_cache = (struct radix_tree_root *)kvmalloc(sizeof(*dmc->cache), GFP_KERNEL);
    INIT_RADIX_TREE(new_cache, GFP_NOWAIT);

    // Delete the existing radix tree and swap the new one in
    spin_lock(&dmc->cache_lock);
    destroy_radix_tree(dmc);
    dmc->cache = new_cache;
    spin_unlock(&dmc->cache_lock);

    // Reset the zones & zone metadata
    if (dmc->cache_dev_is_zoned) {
        // Clear all zones by resetting the write pointer
        blkdev_zone_mgmt(dmc->cache_dev->bdev, REQ_OP_ZONE_RESET, 
                         dmc->start_seq_write_zones, 
                         dmc->zsize_sectors*dmc->nr_zones);

        // Reset the bitmap
        bitmap_zero(dmc->bm_full_zones, dmc->nr_zones);

#if GC_ENABLED
        gc_reset(dmc);
#endif /* GC_ENABLED*/

        // Reset the active zones
        spin_lock(&dmc->active_zones_lock);
        for (count = 0; count < dmc->nr_zones; count++)
            active_zones_remove(dmc, count);

        BUG_ON(active_zones_count(dmc) != 0);
        spin_unlock(&dmc->active_zones_lock);

        // Reset metadata
#if DMC_ZBD_WRITE_MODE == ZBD_SINGLE_WRITE_HEAD
        dmc->current_zone       = find_first_zero_bit(dmc->bm_full_zones, dmc->nr_zones);
#else
        reset_zbd_write_heads(dmc);
#endif

#if !WiPS_HACK
        //dmc->current_write_head = 0;
        dmc->wp_block 		    = 0;
        dmc->zone_batch_size    = 0;
#endif
    }

    // Reset the dmcache statistics
    reset_dmcache_stats(dmc);

    // We will treat each cacheblock as if it has never been used before
    dmcstats_set(allocate, 0);

    // Reset the cacheblocks state to INVALID
    index = dmc->cache_size - 1;
    list_for_each(temp, dmc->lru) {
        cache = list_entry(temp, struct cacheblock, list);
        cache->state = INVALID;
        cache->cache_block_addr = index;

        // Overflow means something bad has happened
        BUG_ON(index == (u64)-1);
        index--;
    }
}

/*
 * Report cache status:
 *  Output cache stats upon request of device status;
 *  Output cache configuration upon request of table status.
 */
static void cache_status(struct dm_target *ti, status_type_t type, unsigned status_flags, 
                         char *result, unsigned maxlen)
{
    struct cache_c *dmc = (struct cache_c*)ti->private;

    switch (type) {
        case STATUSTYPE_INFO:

            log_dmcache_stats(dmc);

            break;
        case STATUSTYPE_TABLE:

            DMINFO("conf: capacity(%lluM), associativity(%u), block size(%uK), %s",
                    (unsigned long long) dmc->cache_size * dmc->block_size >> 11,
                    dmc->assoc, dmc->block_size>>(10-SECTOR_SHIFT),
                    dmc->write_policy == 1 ? "write-back":
                    (dmc->write_policy == 0 ?"write-through":"write-allocate"));
            break;
        default:
            break;
    }
}

static int cache_message(struct dm_target *ti, unsigned argc, char **argv,
           char *result, unsigned maxlen)
{
    struct cache_c *dmc = (struct cache_c *)ti->private;
    DMINFO("dm-cache: Message Received: %s", argv[0]);

    if (argc != 1 && argc != 2)
        goto error;

    if (strcmp(argv[0], "reset_stats") == 0) {
        DMINFO ("RESET! dm-cache statistics");
        reset_dmcache_stats(dmc);
        return 1;
    } else if (strcmp(argv[0], "reset_dmcache") == 0) {
        DMINFO ("RESET! dm-cache metadata");
        reset_dmcache(dmc);
        return 1;
    }
error:
    DMWARN ("Unrecognised message received <%d>%s  ",argc,argv[0]);
    return -EINVAL;
}

static int cache_iterate_devices(struct dm_target *ti, iterate_devices_callout_fn fn, void* data)
{
    int ret                   = 0;
    struct cache_c *dmc       = (struct cache_c*)ti->private;
    struct dm_dev *dest_dev   = dmc->cache_dev;
    struct dm_dev *source_dev = dmc->src_dev;

    ret = fn(ti, dest_dev, 0, bdev_nr_sectors(dest_dev->bdev), data);
    if (!ret)
        ret = fn(ti, source_dev, 0, ti->len, data);
    return ret;
}

static void cache_io_hints(struct dm_target *ti, struct queue_limits *limits)
{
    /* Still need to verify what exactly this is doing */
    //limits->zoned = BLK_ZONED_NONE;
    limits->zoned = 0; 
}


/****************************************************************************
 *  Functions for manipulating a cache target.
 ****************************************************************************/

static struct target_type cache_target = {
    .name            = "cache"               ,
    .version         = {1, 0, 1}             ,
    .features        = CACHE_TARGET_FEATURES ,
    .module          = THIS_MODULE           ,
    .ctr             = cache_ctr             ,
    .dtr             = cache_dtr             ,
    .map             = cache_map             ,
    .status          = cache_status          ,
    .message         = cache_message         ,
    .io_hints        = cache_io_hints        ,
    .iterate_devices = cache_iterate_devices ,
    .busy            = is_block_ios_enabled,
};

/*
 * Initiate a cache target.
 */
static int __init dm_cache_init(void)
{
    int ret;

    ret = dm_register_target(&cache_target);
    if (ret < 0) {
        DMERR("cache: register failed %d", ret);
    }

    DMINFO("Device Mapper Target: dmcache registered successfully!!");
    return ret;
}

/*
 * Destroy a cache target.
 */
static void __exit dm_cache_exit(void)
{
    dm_unregister_target(&cache_target);
    DMINFO("Device Mapper Target: dmcache unregistered successfully!!");
}

module_init(dm_cache_init);
module_exit(dm_cache_exit);

MODULE_DESCRIPTION(DM_NAME " cache target");
MODULE_AUTHOR("Anonymous");
MODULE_LICENSE("GPL");
