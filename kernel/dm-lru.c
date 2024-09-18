#include "dm.h"
#include "gc.h"
#include "dm-cache.h"

void move_blk_lru_bk(struct cache_c *dmc, struct cacheblock *block, enum lru_states new_state) {

    const u64 zone_idx = block_idx_to_zone_idx(dmc, block->cache_block_addr);
    enum lru_states prev_state = block->lru_state;
    struct gc_c *gc = dmc->gc;

    /* If these two are the same, then the same counter will just be decreased and then
     * increased right after. This is not necessary, so don't bother. */
    if (new_state == prev_state) {
        return;
    }

    if (is_state(block->state, VALID)) {

        ZONE_STATS_LOCK(zone_idx);
        /* Decrease the counter for the old state */
        if (prev_state == BEFORE_THRESH)
            atomic64_dec(&dmc->gc->per_zone_stats[zone_idx].num_before);

        /* Increase the counter for the new state */
        if (new_state == BEFORE_THRESH)
            atomic64_inc(&dmc->gc->per_zone_stats[zone_idx].num_before);
        ZONE_STATS_UNLOCK(zone_idx);
    }

    /* NOTE: The scenario of "on_thresh" is pretty much neglected. If the block was the
     * threshold, no counder is decreased. If the block is becoming the threshold, no
     * counter is increased. It shouldn't really matter. */
    block->lru_state = new_state;
}

void init_dmcache_lru(struct cache_c *dmc) {

	int cur_lru_state = AFTER_THRESH;
	u64 zone_idx, index;
	u64 seq_offset = dmc->cache_size-1;

	BUG_ON(dmc->cache_size > (uint32_t)-1);
	// Initializing cache blocks
	for (index = 0; index < dmc->cache_size; index++) {
        struct cacheblock* block = kvmalloc((sizeof(struct cacheblock)), GFP_KERNEL);
        bio_list_init(&block->bios);
        block->state 				= INVALID;
        block->cache_block_addr 	= seq_offset--;
        spin_lock_init(&block->lock);
        list_add(&block->list, dmc->lru);

#if GC_ENABLED
		zone_idx = block_idx_to_zone_idx(dmc, index);
		// Keep track of the threshold block
		if (dmc->gc->relocation_lru_thresh == index) {
			cur_lru_state 		= BEFORE_THRESH; 
			block->lru_state 	= ON_THRESH; 
			dmc->lru_thresh 	= block;
			continue;
		}
		block->lru_state = cur_lru_state;
#endif
	}
}

void validate_lru_states_or_exit(struct cache_c *dmc, const char *func, int lineno) {

	int cur_lru_state = AFTER_THRESH;
    uint64_t count = 0;
    struct list_head *lru_node;
    struct cacheblock *cache;

    spin_lock(&dmc->lru_lock);
    list_for_each_prev(lru_node, dmc->lru) {
        cache = list_entry(lru_node, struct cacheblock, list);
        if ((count++) == (dmc->gc->relocation_lru_thresh)) {
            // Validate the position and state of the threshold
            if (cache->lru_state != ON_THRESH || cache != dmc->lru_thresh) {
                goto fail_and_exit;
            }
            cur_lru_state = BEFORE_THRESH;
            continue;
        }

        // Validate nonthreshold node states
        if (cache->lru_state != cur_lru_state) {
            goto fail_and_exit;
        }
    }
    spin_unlock(&dmc->lru_lock);
    return;

fail_and_exit:
    spin_unlock(&dmc->lru_lock);
    DMERR("[exited]: failed to maintain lru_state, in function %s line %d\n", func, lineno);
}



bool advance_lru_thresh(struct cache_c *dmc, struct cacheblock *block, bool tagged_for_garbage) 
{
    struct cacheblock *node;
    int block_state  = block->lru_state;

    /* If we are at the tail, we do not need to do anything because we cannot advance any further. 
     * Nothing changes if the threshold is the block being accessed, and it is impossible for the 
     * block to come after. */

    if(list_next_entry(block, list) == NULL) {

        move_blk_lru_bk(dmc, dmc->lru_thresh, BEFORE_THRESH); 
        // dmc->lru_thresh->lru_state  = BEFORE_THRESH; 
        move_blk_lru_bk(dmc, block, ON_THRESH);
        // block->lru_state            = ON_THRESH;

        /* Transition lru_thresh */
        dmc->lru_thresh = block;
        return false;
    }

    /* It is assumed that on_thresh contributes to the after_threshold count */
    if(block_state == BEFORE_THRESH) {

        /* State updates */
        node                        = list_next_entry(dmc->lru_thresh, list);
        move_blk_lru_bk(dmc, node, ON_THRESH); 
        // node->lru_state             = ON_THRESH; 
        move_blk_lru_bk(dmc, dmc->lru_thresh, BEFORE_THRESH); 
        // dmc->lru_thresh->lru_state  = BEFORE_THRESH; 
        move_blk_lru_bk(dmc, block, AFTER_THRESH); 
        // block->lru_state            = AFTER_THRESH; 

        /* Transition lru_thresh */
        dmc->lru_thresh             = node;
        return true;
    }

    if(block_state == ON_THRESH) {

        /* State updates */
        node = list_next_entry(dmc->lru_thresh, list);
        move_blk_lru_bk(dmc, node, ON_THRESH); 
        // node->lru_state             = ON_THRESH; 
        move_blk_lru_bk(dmc, dmc->lru_thresh, AFTER_THRESH);
        // dmc->lru_thresh->lru_state  = AFTER_THRESH;

        /* Transition lru_thresh */
        dmc->lru_thresh = node;
        return true;
    }

    return false;
}


bool retreat_lru_thresh(struct cache_c *dmc, struct cacheblock *block) {

    struct cacheblock *node;
    int block_state    = block->lru_state;

    /* If we are at the head, we do not need to do anything because we cannot retreat any further. 
     * Nothing changes if the threshold is the block being accessed, and it is impossible for the 
     * block to come before. */

    if(list_prev_entry(dmc->lru_thresh, list) == NULL) {

        move_blk_lru_bk(dmc, dmc->lru_thresh, AFTER_THRESH);
        // dmc->lru_thresh->lru_state = AFTER_THRESH;
        move_blk_lru_bk(dmc, block, ON_THRESH);
        // block->lru_state           = ON_THRESH;

        /* Transition lru_thresh */
        dmc->lru_thresh = block;
        return false;
    }

    if(block_state == AFTER_THRESH) {

        /* State updates */
        node                         = list_prev_entry(dmc->lru_thresh, list);
        move_blk_lru_bk(dmc, node, ON_THRESH); 
        // node->lru_state              = ON_THRESH; 
        move_blk_lru_bk(dmc, dmc->lru_thresh, AFTER_THRESH);
        // dmc->lru_thresh->lru_state   = AFTER_THRESH;

        /* Transition lru_thresh */
        dmc->lru_thresh              = node;
        return true;
    }

    if(block_state == ON_THRESH) {

        /* State updates */
        node                         = list_prev_entry(dmc->lru_thresh, list);
        move_blk_lru_bk(dmc, node, ON_THRESH);
        // node->lru_state              = ON_THRESH;
        move_blk_lru_bk(dmc, dmc->lru_thresh, BEFORE_THRESH);
        // dmc->lru_thresh->lru_state   = BEFORE_THRESH;

        /* Transition lru_thresh */
        dmc->lru_thresh              = node;
        return true;
    }

    return false;
}

void move_lru_thresh(struct cache_c *dmc, u64 new_pos) {

    struct cacheblock *curr_thresh = dmc->lru_thresh;
    u64 cur_pos = dmc->gc->relocation_lru_thresh, diff, index;

    /* New position is closer to the tail, shift right */
    if (cur_pos > new_pos) {
        diff    = cur_pos - new_pos;
        index   = 0;

        list_for_each_entry_from(curr_thresh,dmc->lru, list) {
            if(index < diff) {
                move_blk_lru_bk(dmc, curr_thresh, BEFORE_THRESH);
                // curr_thresh->lru_state  = BEFORE_THRESH;
                index++;
            }else {
                break;
            }
        }
    }

    /* New position is closer to the head, shift left */
    if (new_pos > cur_pos) {
        diff    = new_pos - cur_pos;
        index   = 0;

        //head_node = list_first_entry(dmc->lru, struct cacheblock, list);
        list_for_each_entry_from_reverse(curr_thresh,dmc->lru, list) {
            if(index < diff) {
                move_blk_lru_bk(dmc, curr_thresh, AFTER_THRESH);
                // curr_thresh->lru_state = AFTER_THRESH;
                index++;
            }else {
                break;
            }
        }
    }

    /* Finally, both cases need to update the new threshold node */
    dmc->gc->relocation_lru_thresh  = new_pos;
    move_blk_lru_bk(dmc, curr_thresh, ON_THRESH);
    // curr_thresh->lru_state          = ON_THRESH;
    dmc->lru_thresh                 = curr_thresh;
}
