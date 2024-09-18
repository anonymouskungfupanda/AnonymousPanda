#include <algorithm>
#include <cassert>
#include <cstdint>
#include <utility>
#include <cmath>
#include <tuple>
#include "dmcache_gc.hpp"
#include "dmcache.hpp"

#define CASE_ORIGINAL   0

/*
 * Change CASE_NO for any ts_score variation, leave the hash
 * define commented to use original ts_score.
 */
#define CASE_NO     CASE_ORIGINAL

#define TAB_SEQ_F_MISS(caseno)  \
    ( ts_score_cases[caseno][1][0] )
#define TAB_SEQ_T_MISS(caseno)  \
    ( ts_score_cases[caseno][0][0] )
#define TAB_SEQ_F_HIT(caseno)   \
    ( ts_score_cases[caseno][1][1] )
#define TAB_SEQ_T_HIT(caseno)   \
    ( ts_score_cases[caseno][0][1] )

/*
 * The format of these tables is as follows:
 * {
 *      { SEQ_T_MISS,   SEQ_T_HIT   },
 *      { SEQ_F_MISS,   SEQ_F_HIT   },
 * }
 * Each space has a corresponding macro to access it's spot in the
 * table to avoid accidentally using the wrong indices.
 */
static const std::pair<bool, std::uint8_t> ts_score_cases[][2][2] =
{
    {   /* Case 0 - (Original) */
        { { true  , 0  } , { true  , 0  } },
        { { true  , 0  } , { true  , 0  } },
    },

    {   /* Case 1 */
        { { false , 0  } , { true  , 0  } },
        { { false , 0  } , { true  , 0  } },
    },

    {   /* Case 2 */
        { { false , 0  } , { false , 0  } },
        { { false , 0  } , { true  , 0  } },
    },

    {   /* Case 3 */
        { { false , 0 }  , { false , 0  } },
        { { true  , 0 }  , { true  , 0  } },
    },

    {   /* Case 4 */
        { { false , 0 }  , { true  , 0  } },
        { { true  , 0 }  , { true  , 0  } },
    },

    {   /* Case 5 */
        { { false , 0  } , { true  , 1  } },
        { { false , 0  } , { true  , 0  } },
    },

    {   /* Case 6 */
        { { true  , 1 }  , { true  , 0  } },
        { { true  , 0 }  , { true  , 0  } },
    },

    {   /* Case 7 */
        { { true  , 2 }  , { true  , 0  } },
        { { true  , 0 }  , { true  , 0  } },
    },

    {   /* Case 8 */
        { { true  , 3 }  , { true  , 0  } },
        { { true  , 0 }  , { true  , 0  } },
    },

    {   /* Case 9 */
        { { true  , 4 }  , { true  , 0  } },
        { { true  , 0 }  , { true  , 0  } },
    },
};

/*
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 */

std::pair<bool, std::uint8_t> dmcache::should_contribute_ts_score_bk(const cacheblock *block) const
{

#ifndef CASE_NO

    /* No case, use original ts_score */
    return { true , 0 };

#else // CASE_NO

    /* If sequential flagging is disabled, treat like normal ts_score */
    if (!sequential_flagging)
        return { true , 0 };

    bool is_sequential = block->is_sequential;
    bool is_reaccessed = block->is_reaccessed;
    auto caseno = CASE_NO;

    if (is_sequential && is_reaccessed)
        return TAB_SEQ_T_HIT(caseno);

    else if (is_sequential && !is_reaccessed)
        return TAB_SEQ_T_MISS(caseno);

    else if (!is_sequential && is_reaccessed)
        return TAB_SEQ_F_HIT(caseno);

    else if (!is_sequential && !is_reaccessed)
        return TAB_SEQ_F_MISS(caseno);

#endif // CASE_NO

    assert(false);
}

std::uint64_t dmcache::limit_blk_ts_score_bk(const cacheblock *block) const
{

    /* Set to 1 for clamping to start_ts for sequential scans */
#if(0)

    if (block->is_sequential) {
        assert(block->last_access >= block->scan->start_ts);
        return std::min(block->last_access, block->scan->start_ts);
    }

#endif

    /* Set to 1 for ts sequential decay for sequential scans */
#if(0)

    if (block->is_sequential) {

        const std::uint64_t t_s = block->scan->t_s;
        const std::uint64_t t_x = block->scan->t_x;
        const float alpha = 0.99999;

        assert(block->scan->is_long_scan);
        assert(t_x <= block->last_access);
        assert(t_s < t_x);

        const std::uint64_t exp  = block->last_access - t_x;
        const std::uint64_t diff = t_x - t_s;
        return ( std::pow(alpha, exp) * diff ) + t_s;
    }

#endif

    return block->last_access;
}

/*
 * bk is short for book-keeping
 *  - This is supposed to update the counters in a zone as one of it's blocks potentially crosses
 *    over the threshold (or it's lru_state value changes)
 */
[[gnu::noinline]] // If compiling with -O3 don't delete
void dmcache::move_blk_lru_bk(list_container<cacheblock> *block, cacheblock::lru_states new_state)
{
    uint64_t zone_idx = block_idx_to_zone_idx(block->data->cache_block_addr);
    enum cacheblock::lru_states prev_state = block->data->lru_state;

    /* If these two are the same, then the same counter will just be decreased and then
     * increased right after. This is not necessary, so don't bother. */
    if (new_state == prev_state)
        return;

    if (is_state(block->data->state, VALID)) {

        /* Decrease the counter for the old state */
        if (prev_state == cacheblock::before_thresh)
            gc->per_zone_stats[zone_idx]->num_before--;

        /* Increase the counter for the new state */
        if (new_state == cacheblock::before_thresh)
            gc->per_zone_stats[zone_idx]->num_before++;

    }

    /* NOTE: The scenario of "on_thresh" is pretty much neglected. If the block was the
     * threshold, no counder is decreased. If the block is becoming the threshold, no
     * counter is increased. It shouldn't really matter. */
    block->data->lru_state = new_state;
}

/*
 * bk is short for book-keeping
 *  - The last_access of the block must be updated before calling this function.
 *  - This is only intended to be called in the cache_hit path. The ts_score maintenence for
 *    the cache miss path for both read and write is handled in request_cache_block and also
 *    dmcache::prep_for_write.
 */
void dmcache::update_ts_score_bk(struct cacheblock *block, uint64_t prev_ts, bool is_seq, bool is_read)
{
    uint64_t zone_idx = block_idx_to_zone_idx(block->cache_block_addr);

    /* Decrement the ts_score if the block, in it's previous state, contributed
     * to the ts_score of the zone. */
    assert(gc->per_zone_stats[zone_idx]->ts_score >= block->ts_score);
    gc->per_zone_stats[zone_idx]->ts_score -= block->ts_score;

    /* TODO: This might not be the best place to do this in terms of keeping the
     * code readable, however these two fields *****MUST***** only be updated
     * between the decrement and increment. */
    block->is_sequential = is_seq;
    block->is_reaccessed = true;

    auto [do_contribute, blk_ts_shift] = should_contribute_ts_score_bk(block);
    uint64_t old_ts_score = gc->per_zone_stats[zone_idx]->ts_score;

    /* At this time, we only consider reads. Writes cannot be handled at this time
     * as we do not know which zone the data will be appended to and it is possible
     * for data in one zone to be invalidated and re-written to another */
    if (is_read && do_contribute)
        block->ts_score = limit_blk_ts_score_bk(block) >> blk_ts_shift;

    /* Either the block is not in a contribution state, or the IO is a write. If
     * the IO is a write, the contribution score will be calcualted and re-added in
     * dmcache::zbd_prep_for_write() */
    else
        block->ts_score = 0;

    gc->per_zone_stats[zone_idx]->ts_score += block->ts_score;
    assert(old_ts_score <= gc->per_zone_stats[zone_idx]->ts_score);
}
