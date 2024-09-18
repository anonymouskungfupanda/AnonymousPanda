#include "dmcache_gc.hpp"
#include "dmcache.hpp"
#include "list.hpp"

void dmcache::init_dmcache_lru() {

    cacheblock::lru_states cur_lru_state = cacheblock::after_thresh;
    assert(cache_size < (uint32_t)-1);
    uint64_t zone_idx;
    
    /* Initializing cache blocks */
    for (uint32_t index = 0; index < cache_size; index++) {

        cacheblock *block = new cacheblock {
            .src_block_addr         = 0,
            .cache_block_addr       = index,
            .state                  = INVALID,
            .flags = 0
        };
        block->list.data = block;
        lru.add_front(&block->list);

#if GC_ENABLED
        zone_idx = block_idx_to_zone_idx(index);

        /* Keep track of the threshold block */
        if (gc->relocation_lru_thresh == index) {
            cur_lru_state       = cacheblock::before_thresh;
            block->lru_state    = cacheblock::on_thresh;
            lru_thresh          = &block->list;

            //on_thresh is also considered as after_thresh; a closed boundary
            continue;
        }

        block->lru_state = cur_lru_state;
#endif
    }

    uint64_t max_thresh = stats.allocates * max_lru_thresh_percent;
    if (max_lru_thresh_enabled && gc->relocation_lru_thresh > max_thresh) {
        move_lru_thresh(max_thresh);
    }
}

void dmcache::validate_lru_states_or_exit(const char *func, int lineno) const {

    /*
     * Check 1: Verify the lru_states of the blocks in the lru list
     */
    using s = cacheblock::lru_states;
    auto cur_lru_state = s::after_thresh;
    std::uint64_t count = 0;

    for (auto node = lru.get_tail(); node != nullptr; node = node->prev) {
        if ((count++) == (gc->relocation_lru_thresh)) {
            /* Validate the position and state of the threshold */
            if (node->data->lru_state != s::on_thresh || node != lru_thresh)
                goto bad_lru_state;
            cur_lru_state = s::before_thresh;
            continue;
        }
        /* Validate non-threshold node states */
        if (node->data->lru_state != cur_lru_state)
            goto bad_lru_state;
    }

    /*
     * Check 2: Verify the book-kept lru_state values for each zone
     */
    for (uint64_t zone_idx = 0; zone_idx < nr_zones; zone_idx++) {

        /* Test values */
        uint64_t test_before = gc->per_zone_stats[zone_idx]->num_before;

        uint64_t true_before = 0, true_after = 0;
        for (uint64_t block_idx = 0; block_idx < blocks_per_zone; block_idx++) {
            uint64_t full_addr = zone_idx_to_block_idx(zone_idx) + block_idx;
            cacheblock const *block = gc->zbd_cacheblocks[full_addr];

            if (block == nullptr)
                continue;

            else if (block->lru_state == s::before_thresh)
                ++true_before;
        }

        if (true_before != test_before) {
            std::cout << "Zone " << zone_idx << ": true_before: " << true_before << " test_before: " << test_before << std::endl;
            goto bad_bk;
        }
    }

    return;

bad_lru_state:
    std::cout << "[exited]: failed to maintain lru_state, in function " << func << " line " << lineno << std::endl;
    exit(1);

bad_bk:
    std::cout << "[exited]: failed to maintain book-kept lru_state, in function " << "func " << func << " line " << lineno << std::endl;
    exit(1);
}

bool dmcache::advance_lru_thresh(list_container<cacheblock> *block) {

    using s = cacheblock::lru_states;
    auto thresh_state = lru_thresh->data->lru_state;
    auto block_state  = block->data->lru_state;

    /* If we are at the tail, we do not need to do anything because we cannot
     * advance any further. Nothing changes if the threshold is the block being
     * accessed, and it is impossible for the block to come after. */
    if (lru_thresh->next == nullptr) {

        /* State updates */
        move_blk_lru_bk(lru_thresh, s::before_thresh);
        move_blk_lru_bk(block, s::on_thresh);

        /* Transition lru_thresh */
        lru_thresh = block;
        return false;
    }

    /* It is assumed that on_thresh contributes to the after_threshold count */
    if (block_state == s::before_thresh) {

        /* State updates */
        move_blk_lru_bk(lru_thresh->next, s::on_thresh);
        move_blk_lru_bk(lru_thresh, s::before_thresh);
        move_blk_lru_bk(block, s::after_thresh);

        /* Transition lru_thresh */
        lru_thresh = lru_thresh->next;

        return true;
    }

    if (block_state == s::on_thresh) {

        /* State updates */
        move_blk_lru_bk(lru_thresh->next, s::on_thresh);
        move_blk_lru_bk(lru_thresh, s::after_thresh);

        /* Transition lru_thresh */
        lru_thresh = lru_thresh->next;

        return true;
    }

    return false;
}

bool dmcache::retreat_lru_thresh(list_container<cacheblock> *block) {

    using s = cacheblock::lru_states;

    auto thresh_state   = lru_thresh->data->lru_state;
    auto block_state    = block->data->lru_state;

    /* If we are at the head, we do not need to do anything because we cannot
     * retreat any further. Nothing changes if the threshold is the block being
     * accessed, and it is impossible for the block to come before. */
    if (lru_thresh->prev == nullptr) {

        /* State updates */
        move_blk_lru_bk(lru_thresh, s::after_thresh);
        move_blk_lru_bk(block, s::on_thresh);

        /* Transition lru_thresh */
        lru_thresh = block;
        return false;
    }

    /* This code path is not applicable for garbage blocks */    

    if (block_state == s::after_thresh) {

        /* State updates */
        move_blk_lru_bk(lru_thresh->prev, s::on_thresh);
        move_blk_lru_bk(lru_thresh, s::after_thresh);
        move_blk_lru_bk(block, s::before_thresh);

        /* Transition lru_thresh */
        lru_thresh = lru_thresh->prev;
        return true;
    }

    if (block_state == s::on_thresh) {

        /* State updates */
        move_blk_lru_bk(lru_thresh->prev, s::on_thresh);
        move_blk_lru_bk(lru_thresh, s::before_thresh);

        /* Transition lru_thresh */
        lru_thresh = lru_thresh->prev;

        return true;
    }

    return false;
}

void dmcache::readjust_lru_thresh(uint64_t zone_idx)
{
    using s = cacheblock::lru_states;
    lru_thresh_readjustments++;

    while (gc->per_zone_stats[zone_idx]->num_before < lru_readjust_dist) {
        move_blk_lru_bk(lru_thresh, s::before_thresh);
        move_blk_lru_bk(lru_thresh->next, s::on_thresh);
        lru_thresh = lru_thresh->next;
    }
}

void dmcache::move_lru_thresh(std::uint64_t new_pos) {

    using s = cacheblock::lru_states;

    std::uint64_t cur_pos = gc->relocation_lru_thresh, dif;
    list_container<cacheblock> *cur_thresh = lru_thresh;

    /* New position is closer to the tail, shift right */
    if (cur_pos > new_pos) {
        dif = cur_pos - new_pos;

        for (std::uint64_t i = 0; cur_thresh->next != nullptr && i < dif; i++) {
            move_blk_lru_bk(cur_thresh, s::before_thresh);
            cur_thresh = cur_thresh->next;
        }
    }

    /* New position is closer to the head, shift left */
    if (new_pos > cur_pos) {
        dif = new_pos - cur_pos;

        for (std::uint64_t i = 0; cur_thresh->prev != nullptr && i < dif; i++) {
            move_blk_lru_bk(cur_thresh, s::after_thresh);
            cur_thresh = cur_thresh->prev;
        }
    }

    /* Finally, both cases need to update the new threshold node */
    gc->relocation_lru_thresh = new_pos;
    move_blk_lru_bk(cur_thresh, s::on_thresh);
    lru_thresh = cur_thresh;
}
