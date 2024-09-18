#include "dm.h"
#include "gc.h"
#include "dm-cache.h"

#if GC_ENABLED
#if DEBUG_GC

#if GC_STATS_DEBUG
static DEFINE_SPINLOCK(dmz_gc_debug_lock);
static LIST_HEAD(dmz_gc_debug_list);
struct file* dmz_gc_debug_fp    = NULL;
loff_t dmz_gc_debug_fp_pos      = 0;
#endif

#if GC_PER_ZONE_DEBUG
static DEFINE_SPINLOCK(per_zone_debug_lock);
static LIST_HEAD(per_zone_debug_list);
struct file* per_zone_debug_fp  = NULL;
loff_t per_zone_debug_fp_pos    = 0;
#endif

#if GC_PER_ZONE_DEBUG
static DEFINE_SPINLOCK(per_gc_iter_debug_lock);
static LIST_HEAD(per_gc_iter_debug_list);
struct file* per_gc_iter_debug_fp  = NULL;
loff_t per_gc_iter_debug_fp_pos    = 0;
#endif

struct dmz_gc_debug_list_entry
{
    char buffer[63];
    unsigned char length;
    struct list_head list;
};

void init_dmz_gc_debug(void)
{
	unsigned long flags;
	struct file* file;

#if GC_STATS_DEBUG
    file = filp_open("/var/log/dmz_gc_debug.log", O_RDWR | O_CREAT, 0644);
	//file = filp_open("/home/local/ASUAD/kjha9/dmz_gc_debug.log", O_RDWR | O_CREAT, 0644);
	spin_lock_irqsave(&dmz_gc_debug_lock, flags);
	dmz_gc_debug_fp = file;
	dmz_gc_debug_fp_pos = 0;
	spin_unlock_irqrestore(&dmz_gc_debug_lock, flags);
#endif
    
#if GC_PER_ZONE_DEBUG
    file = filp_open("/var/log/per_zone_debug.log", O_RDWR | O_CREAT, 0644);
	spin_lock_irqsave(&per_zone_debug_lock, flags);
	per_zone_debug_fp       = file;
	per_zone_debug_fp_pos   = 0;
	spin_unlock_irqrestore(&per_zone_debug_lock, flags);
#endif

#if GC_PER_ITER_DEBUG
    file = filp_open("/var/log/per_gc_iter_debug.log", O_RDWR | O_CREAT, 0644);
	spin_lock_irqsave(&per_gc_iter_debug_lock, flags);
	per_gc_iter_debug_fp       = file;
	per_gc_iter_debug_fp_pos   = 0;
	spin_unlock_irqrestore(&per_gc_iter_debug_lock, flags);
#endif

}

#if GC_STATS_DEBUG
/* IO#, Total_Garbage_Count, Trigger_GC */
void dmz_gc_debug_update(struct cache_c *dmc, u64 io_no, u64 total_garbage_in_cache)
{
    unsigned long flags;

    spin_lock_irqsave(&dmz_gc_debug_lock, flags);
    if (dmz_gc_debug_fp) {
        struct dmz_gc_debug_list_entry* entry = kvmalloc(sizeof(*entry), GFP_NOWAIT);
        BUG_ON(!entry);
        
        entry->length = snprintf(entry->buffer, sizeof(entry->buffer), "%llu,%llu,%d\n", 
            io_no, total_garbage_in_cache, atomic_read(&dmc->gc->is_gc_active));

        BUG_ON(entry->length > sizeof(entry->buffer));
        list_add_tail(&entry->list, &dmz_gc_debug_list);
    }
    spin_unlock_irqrestore(&dmz_gc_debug_lock, flags);

}
#endif

#if GC_PER_ZONE_DEBUG
/* Zone-idx, reclaim_count, blocks_relocated, blocks_flushed */
void per_zone_debug_update(struct cache_c *dmc, u64 zone_idx, u64 reclaim_count, 
                           u64 blocks_relcoated, u64 blocks_flushed)
{
    unsigned long flags;

    spin_lock_irqsave(&per_zone_debug_lock, flags);
    if (per_zone_debug_fp) {
        struct dmz_gc_debug_list_entry* entry = kvmalloc(sizeof(*entry), GFP_NOWAIT);
        BUG_ON(!entry);
        
        entry->length = snprintf(entry->buffer, sizeof(entry->buffer), "%llu,%llu,%llu,%llu\n", 
            zone_idx, reclaim_count, blocks_relcoated, blocks_flushed);

        BUG_ON(entry->length > sizeof(entry->buffer));
        list_add_tail(&entry->list, &per_zone_debug_list);
    }
    spin_unlock_irqrestore(&per_zone_debug_lock, flags);

}
#endif

#if GC_PER_ITER_DEBUG
/*gc_iter, start_garbage, end_garbage no_reclaim_per_iter*/ 
void per_gc_iter_debug_update(struct cache_c *dmc, s64 gc_iter, s64 start_garbage, s64 end_garbage, 
                             u64 no_reclaim_per_iter)
{
    unsigned long flags;

    spin_lock_irqsave(&per_gc_iter_debug_lock, flags);
    if (per_gc_iter_debug_fp) {
        struct dmz_gc_debug_list_entry* entry = kvmalloc(sizeof(*entry), GFP_NOWAIT);
        BUG_ON(!entry);
        
        entry->length = snprintf(entry->buffer, sizeof(entry->buffer), "%llu,%llu,%llu,%llu\n", 
            gc_iter, start_garbage, end_garbage, no_reclaim_per_iter);

        BUG_ON(entry->length > sizeof(entry->buffer));
        list_add_tail(&entry->list, &per_gc_iter_debug_list);
    }
    spin_unlock_irqrestore(&per_gc_iter_debug_lock, flags);

}
#endif
void dmz_gc_dump_debug_logs(void) 
{

	unsigned long flags;
	struct dmz_gc_debug_list_entry* entry;

#if GC_STATS_DEBUG
    spin_lock_irqsave(&dmz_gc_debug_lock, flags);
    while (!list_empty(&dmz_gc_debug_list)) {
        entry = list_entry(dmz_gc_debug_list.next, struct dmz_gc_debug_list_entry, list);
        list_del(&entry->list);
        spin_unlock_irqrestore(&dmz_gc_debug_lock, flags);
        kernel_write(dmz_gc_debug_fp, entry->buffer, entry->length, &dmz_gc_debug_fp_pos);
        kvfree(entry);
        spin_lock_irqsave(&dmz_gc_debug_lock, flags);
    }
    filp_close(dmz_gc_debug_fp, NULL);
    spin_unlock_irqrestore(&dmz_gc_debug_lock, flags);
#endif

#if GC_PER_ZONE_DEBUG
    spin_lock_irqsave(&per_zone_debug_lock, flags);
    while (!list_empty(&per_zone_debug_list)) {
        entry = list_entry(per_zone_debug_list.next, struct dmz_gc_debug_list_entry, list);
        list_del(&entry->list);
        spin_unlock_irqrestore(&per_zone_debug_lock, flags);
        kernel_write(per_zone_debug_fp, entry->buffer, entry->length, &per_zone_debug_fp_pos);
        kvfree(entry);
        spin_lock_irqsave(&per_zone_debug_lock, flags);
    }
    filp_close(per_zone_debug_fp, NULL);
    spin_unlock_irqrestore(&per_zone_debug_lock, flags);
#endif

#if GC_PER_ITER_DEBUG
    spin_lock_irqsave(&per_gc_iter_debug_lock, flags);
    while (!list_empty(&per_gc_iter_debug_list)) {
        entry = list_entry(per_gc_iter_debug_list.next, struct dmz_gc_debug_list_entry, list);
        list_del(&entry->list);
        spin_unlock_irqrestore(&per_gc_iter_debug_lock, flags);
        kernel_write(per_gc_iter_debug_fp, entry->buffer, entry->length, &per_gc_iter_debug_fp_pos);
        kvfree(entry);
        spin_lock_irqsave(&per_gc_iter_debug_lock, flags);
    }
    filp_close(per_gc_iter_debug_fp, NULL);
    spin_unlock_irqrestore(&per_gc_iter_debug_lock, flags);
#endif
}
#endif
#endif


