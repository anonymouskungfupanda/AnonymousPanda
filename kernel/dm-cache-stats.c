#include "dm.h"
#include "gc.h"
#include "dm-cache.h"

void log_dmcache_configuration(struct cache_c* dmc) 
{
    struct gc_c* gc = dmc->gc;

    DMINFO("------------ DM-ZCache Configuration ------------");
    DMINFO("- Source Device:%s, Cache Device:%s", dmc->src_dev_name, dmc->cache_dev_name);
    DMINFO("- Block Size: %d", dmc->block_size);
    DMINFO("- Total number of Zones used:[%d/%d]",dmc->nr_zones, dmc->total_nr_zones);
    DMINFO("- Cacheblocks per Zone: %lld", dmc->blocks_per_zone);
    DMINFO("- Max Open Zones: %d", dmc->cache_dev_max_open_zones);
    DMINFO("- Max Active Zones: %d", dmc->cache_dev_max_active_zones);
    DMINFO("- Zone Size in sectors: %lld", dmc->zsize_sectors);
    DMINFO("- Zone Capacity in sectors: %lld", dmc->zcap_sectors);
    DMINFO("- Total available Cache Size (without OPS): %lld", (dmc->nr_zones * dmc->blocks_per_zone));
    DMINFO("- GC Enabled: %s", (GC_ENABLED ? "Yes" : "No"));

#if GC_ENABLED
    DMINFO("--------------- DM-ZCache-GC Configuration ---------------");
    DMINFO("- GC Victim Selection Mode: %s", VICTIM_SELECTION_MODE_STRING(gc->gc_victim_sel_mode));
    DISPLAY_GC_INFO(gc);
    DMINFO("- GC Configuration: %d/%ld-%ld-%ld", gc->gc_op_percentage,
            gc->gc_op_acceptable_free_space_percentage, gc->gc_op_stop_start_gap_percentage, gc->gc_op_start_block_gap_percentage);
    if(gc->gc_mode == RELOCATION_GC)
        DMINFO("- GC Relocation IO Depth: %d", GC_RELOCATION_ASYNC_LIMIT);
    if(gc->gc_relocation_mode == RELOCATE_ON_DYNAMIC_RWSS) {
        DMINFO("- Window Size:%lld", gc->rwsst_window_interval);
        DMINFO("- RWSS Relaxation Threshold:%lld", gc->rwss_percentage);
    }
    DMINFO("- Total available Cache Size (after OPS): %llu", dmc->cache_size);
    DMINFO("- GC Trigger Start: %lld", gc->gc_trigger_start);
    DMINFO("- GC Trigger Stop: %lld", gc->gc_trigger_stop);
    DMINFO("- GC Trigger Block: %lld", gc->gc_trigger_block_io);
#endif
    DMINFO("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
}

void log_dmcache_stats(struct cache_c *dmc)
{
    DMINFO("+++++++++++++++++ DM-ZCache: Statistics ++++++++++++++++++");
    DMINFO("- Reads:%llu", dmcstats_get(reads));
    DMINFO("- Writes:%llu", dmcstats_get(writes));
    DMINFO("- Cache Hits:%llu", dmcstats_get(cache_hits));
    DMINFO("-- Read Hit:%llu",  dmcstats_get(read_hits));
    DMINFO("-- Write Hit:%llu", dmcstats_get(cache_hits) - dmcstats_get(read_hits));
    DMINFO("- Cache Miss:%llu", dmcstats_get(cache_misses));
    DMINFO("-- Read Miss:%llu", dmcstats_get(read_misses));
    DMINFO("-- Write Miss:%llu", dmcstats_get(write_misses));
    DMINFO("- Inserts:%llu", dmcstats_get(inserts));
    DMINFO("- Allocate:%llu", dmcstats_get(allocate));
    DMINFO("- Invalidates:%llu", dmcstats_get(invalidates));
    DMINFO("- Forwarded:%llu", dmcstats_get(forwarded));
    DMINFO("- Replace:%llu", dmcstats_get(replace));
    DMINFO("- Dirty:%llu", dmcstats_get(dirty_blocks));
    DMINFO("- Garbage:%llu", dmcstats_get(garbage));
    DMINFO("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

#if GC_ENABLED
    log_dmcache_gc_stats(dmc);
#endif
}


void log_dmcache_gc_stats(struct cache_c* dmc)
{
    struct gc_c* gc = dmc->gc;
    int idx;
    u64 total_zone_reclaims = 0;

    DMINFO("++++++++++++++++++ DM_ZCACHE-GC: Statistics +++++++++++++++++++");
    DMINFO("Total GC Iterations: %llu", gc->gc_iter_count);
    DMINFO("Total Foregroud GC Iterations:%llu", gc->fg_gc_iter_count);
    DMINFO("Total GC Iterations skipped:%llu", gc->skip_gc_iter_count);
    DMINFO("Cacheblocks Relocated: %llu", atomic64_read(&gc->relocated_cacheblocks_count));
    DMINFO("Cacheblocks Flushed: %llu", atomic64_read(&gc->evicted_cacheblocks_count));


    DMINFO("Zone Reclaims:");
    for (idx = 0; idx < dmc->nr_zones; idx++) {
        total_zone_reclaims += atomic64_read(&gc->per_zone_stats[idx].reclaim_count);

#if GC_PER_ZONE_DEBUG
        per_zone_debug_update(dmc, idx, atomic64_read(&gc->per_zone_stats[idx].reclaim_count), 
                                   atomic64_read(&gc->per_zone_stats[idx].cacheblocks_relocated),
                                   atomic64_read(&gc->per_zone_stats[idx].cacheblocks_flushed));
#endif

    }

    DMINFO("Total Reclaims: %llu", total_zone_reclaims);
    DMINFO("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
}


void reset_dmcache_stats(struct cache_c *dmc)
{
    dmcstats_set(reads,0);
    dmcstats_set(writes,0);
    dmcstats_set(cache_hits,0);
    dmcstats_set(cache_misses,0);
    dmcstats_set(read_misses,0);
    dmcstats_set(write_misses,0);
    dmcstats_set(read_hits,0);
    dmcstats_set(inserts,0);
    dmcstats_set(invalidates,0);
    //dmcstats_set(allocate,0); THIS RIGHT HERE. THIS IS BAD DO NOT UNCOMMENT
    dmcstats_set(forwarded,0);
    dmcstats_set(replace,0);

    // Not safe to reset this statistic, because it is used to implement functionality
    //map_dev->dirty_blocks   = 0;

#if GC_ENABLED
    reset_dmcache_gc_stats(dmc);
#endif
}

void reset_dmcache_gc_stats(struct cache_c* dmc)
{
    struct gc_c* gc = dmc->gc;
    u64 zone_idx;

    for (zone_idx = 0;zone_idx < dmc->nr_zones; zone_idx++) {
        ZONE_STATS_SET(zone_idx, gcable_count, 0);
        ZONE_STATS_SET(zone_idx, reclaim_count, 0);
        ZONE_STATS_SET(zone_idx, num_before, 0);
        ZONE_STATS_SET(zone_idx, max_timestamp, 0);
        ZONE_STATS_SET(zone_idx, min_timestamp, 0);
        ZONE_STATS_SET(zone_idx, ts_score, 0);
        ZONE_STATS_SET(zone_idx, cacheblocks_relocated, 0);
        ZONE_STATS_SET(zone_idx, cacheblocks_flushed, 0);
    }

    atomic64_set(&gc->relocated_cacheblocks_count, 0);
    atomic64_set(&gc->evicted_cacheblocks_count, 0);
}
