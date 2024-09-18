#include <filesystem>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <fstream>
#include <random>
#include <iomanip>
#include <utility>

#include "rt_analysis.hpp"
#include "dmcache_gc.hpp"
#include "dmcache.hpp"

#if(0)
#define VS_LOG(zone_idx, scores, dmc) vs_log(zone_idx, scores, dmc)
#else
#define VS_LOG(...)
#endif

#if(0)
#define VALIDATE_TS_SCORE(zone_idx, test_score, dmc) \
    validate_ts_score(zone_idx, test_score, dmc)
#else
#define VALIDATE_TS_SCORE(...)
#endif

void dmcacheGC::validate_ts_score(std::uint64_t zone_idx, std::uint64_t test_score, dmcache *dmc) {
    uint64_t true_score = 0;

    for (uint64_t block_idx = 0; block_idx < dmc->blocks_per_zone; block_idx++) {
        uint64_t full_addr = dmc->zone_idx_to_block_idx(zone_idx) + block_idx;
        cacheblock const *block = zbd_cacheblocks[full_addr];

        if (block != nullptr)
            true_score += block->ts_score;
    }

    if (true_score == test_score)
        return;

    std::cout << "True: " << true_score << ", Test: " << test_score << std::endl;
    assert(false);
}

struct heat_score_metrics {
    uint64_t garbage, before, after;
};

void
dmcacheGC::vs_log(std::uint64_t zone_idx, std::vector<std::pair<uint64_t, uint64_t>> scores, dmcache *dmc) {
    std::string filename = "vs_analysis.log";
    bool write_header = false;

    if (!std::filesystem::exists(filename))
        write_header = true;

    static std::ofstream logger_file(filename, std::ios::app);
    if (write_header) {
        logger_file << "total_iters\t"
            << "window_num\t"
            << "cache_size\t"
            << "allocates\t"
            << "replacements\t"
            << "size_of_wss_t\t"
            << "size_of_rwss_t\t"
            << "size_of_evicted_t\t"
            << "relocation_lru_thresh\t"
            << "index_of_lru_thresh\t"
            << "total_garbage\t"
            << "total_free_space\t"
            << "curr_zone\t"
            << "curr_zone_wp\t"
            << "victim_index\t"
            << "victim_garbage\t"
            << "victim_valid\t"
            << "num_before\t"
            << "num_after\t"
            << "victim_min_ts\t"
            << "victim_max_ts\t"
            << "victim_avg_ts\t"
            << "victim_ts_score\n";
    }
    std::uint64_t min_ts = std::numeric_limits<std::uint64_t>::max(), max_ts = 0;
    std::uint64_t garbage_count = 0, valid_count = 0, summation = 0, num_seq = 0;
    std::uint64_t num_before = 0, num_after = 0;
    double scaled_total = 0.0f;

    for (std::uint64_t block_idx = 0; block_idx < dmc->blocks_per_zone; block_idx++) {
        uint64_t full_addr = dmc->zone_idx_to_block_idx(zone_idx) + block_idx;
        cacheblock const *block = zbd_cacheblocks[full_addr];

        if (block == nullptr) {
            ++garbage_count;
            continue;
        }
        else ++valid_count;

        if (block->is_sequential)
            ++num_seq;

        if (block->lru_state == cacheblock::before_thresh)
            ++num_before;
        else ++num_after;

        std::uint64_t cur_ts = block->last_access;
        summation += cur_ts;

        if (cur_ts < min_ts) min_ts = cur_ts;
        if (cur_ts > max_ts) max_ts = cur_ts;
    }

    std::uint64_t lru_thresh_idx = dmc->lru.index_of(dmc->lru_thresh);
    logger_file << gc_iter_count << "\t"
        << dmc->window_number << "\t"
        << dmc->cache_size << "\t"
        << dmc->stats.allocates << "\t"
        << dmc->stats.replaces << "\t"
        << dmc->wss_t.size() << "\t"
        << dmc->rwss_t.size() << "\t"
        << dmc->evicted_t.size() << "\t"
        << relocation_lru_thresh << "\t"
        << lru_thresh_idx << "\t"
        << gc_garbage_count << "\t"
        << calc_free_space(dmc) << "\t"
        << dmc->current_zones[dmc->current_write_head] << "\t"
        << dmc->wp_block << "\t"
        << zone_idx << "\t"
        << garbage_count << "\t"
        << valid_count << "\t"
        << num_before << "\t"
        << num_after << "\t"
        << min_ts << "\t"
        << max_ts << "\t"
        << summation / dmc->blocks_per_zone << "\t"
        << per_zone_stats[zone_idx]->ts_score << "\t";

    for (auto p : scores) {
        num_before = num_after = garbage_count = valid_count = num_seq = 0;
        auto [zone_idx, score] = p;

        for (std::uint64_t block_idx = 0; block_idx < dmc->blocks_per_zone; block_idx++) {
            uint64_t full_addr = dmc->zone_idx_to_block_idx(zone_idx) + block_idx;
            cacheblock const *block = zbd_cacheblocks[full_addr];

            if (block == nullptr) {
                ++garbage_count;
                continue;
            }
            else ++valid_count;

            if (block->is_sequential)
                ++num_seq;

            if (block->lru_state == cacheblock::before_thresh)
                ++num_before;
            else ++num_after;
        }

        logger_file << "(" << zone_idx << ", " << garbage_count << ", " << valid_count << ", "
            << num_before << ", " << num_after << ", " << num_seq << ", " << score << ")" << "\t";
    }
    logger_file << "\n";
    logger_file.flush();
}

inline double calc_heat_score(const heat_score_metrics &m) {

    constexpr double garbage_weight =  0.0f;
    constexpr double before_weight  =  1.0f;
    constexpr double after_weight   =  2.0f;

    return (m.garbage * garbage_weight)
         + (m.before  * before_weight)
         + (m.after   * after_weight);
}

// Choose a random zone which will be freed during GC
std::tuple<uint64_t, uint64_t> dmcacheGC::gc_choose_zone_random(dmcache *dmc) {

    std::uint32_t prev_index = 0, weight = dmc->bm_full_zones->get_weight();
    std::random_device rd;

    RT_START(gc_select);
    std::mt19937 gen(rd());

    /* No full zones found. Return max u64 to indicate to caller */
    if (weight == 0) {
        RT_STOP(gc_select);
        return std::make_tuple((uint64_t)-1, 0);
    }

    /* Get a collection of all full zones to pick from */
    std::vector<std::uint32_t> full_zones(weight);
    for (std::uint32_t i = 0; i < weight; i++) {
        full_zones[i] = dmc->bm_full_zones->find_next_one_bit(prev_index).first;
        prev_index = 1 + full_zones[i];
    }

    /* Pick a random full zone */
    std::uniform_int_distribution<std::size_t> dist(0, weight - 1);
    std::size_t idx = dist(gen);
    assert(idx < weight);

    std::uint64_t zone_idx = full_zones[idx];
    assert(dmc->bm_full_zones->test_bit(zone_idx));

    RT_STOP(gc_select);
    return std::make_tuple(zone_idx, per_zone_stats[zone_idx]->gcable_count);
}

// Choose the optimal zone which will be freed during GC
std::tuple<uint64_t, uint64_t> dmcacheGC::gc_choose_zone_greedy(dmcache *dmc) {

    uint64_t zone_idx, block_idx, zone_gcable_count, best_zone_gcable_count, best_zone_for_gc; 
    zone_idx = block_idx = zone_gcable_count = best_zone_gcable_count = best_zone_for_gc = 0;
    std::vector<std::pair<std::uint64_t, std::uint64_t>> zone_scores;
    bool found_first_full_zone = false;
    RT_START(gc_select);

#if DEBUG_MODE
    dmc->active_zones_print();
#endif

    for (zone_idx = 0; zone_idx < dmc->nr_zones; zone_idx++) {
        // Only consider full zones for GC
        if(!dmc->bm_full_zones->test_bit(zone_idx))
            continue;

        // Count the number of GCable blocks in each zone
        zone_gcable_count = per_zone_stats[zone_idx]->gcable_count;
        zone_scores.push_back({ zone_idx, zone_gcable_count });

        // Jackpot! We have found a zone which is all GCable
        if (zone_gcable_count == dmc->blocks_per_zone) {
            DBUG_GC("[GC] Jackpot zone:", zone_idx ,"!! All GCable blocks\n");
            RT_STOP(gc_select);

            VS_LOG(zone_idx, zone_scores, dmc);
            return std::make_tuple(zone_idx, zone_gcable_count);
        }

        DBUG_GC("[GC] zone_idx:", zone_idx, " zone_gcable_count:", zone_gcable_count, " zone_nongcable_count:", (dmc->blocks_per_zone - zone_gcable_count), "\n");
        // Keep track of the best zone we've found thus far
        if (!found_first_full_zone || zone_gcable_count > best_zone_gcable_count) {
        //if (/*!found_first_full_zone ||*/ zone_gcable_count > best_zone_gcable_count) {
            found_first_full_zone   = true;
            best_zone_for_gc        = zone_idx;
            best_zone_gcable_count  = zone_gcable_count;
        }
    }

    // No full zones found. Return max u64 to indicate to caller
    // assert(found_first_full_zone);
    if (!found_first_full_zone) {
        RT_STOP(gc_select);
        return std::make_tuple((uint64_t)-1, 0);
    }
    DBUG_GC("[GC] best_zone_for_gc:", best_zone_for_gc, " best_zone_gcable_count:", best_zone_gcable_count, "\n");
    RT_STOP(gc_select);

    //return best_zone_for_gc;
    VS_LOG(best_zone_for_gc, zone_scores, dmc);
    return std::make_tuple(best_zone_for_gc, best_zone_gcable_count);
}

std::tuple<uint64_t, uint64_t> dmcacheGC::gc_choose_zone_lru_score(dmcache *dmc) {

    using s = cacheblock::lru_states;
    bool found_first = false;
    
    std::vector<std::pair<std::uint64_t, std::uint64_t>> zone_scores;
    double best_score = std::numeric_limits<double>::max();
    heat_score_metrics best_metrics = { 0 };
    uint64_t best_zone_idx = 0;

    for (uint64_t zone_idx = 0; zone_idx < dmc->nr_zones; zone_idx++) {
        heat_score_metrics cur_metrics = { 0 };
        double cur_score = 0;

        /* Only consider full zones */
        if (!dmc->bm_full_zones->test_bit(zone_idx))
            continue;

        for (uint64_t block_idx = 0; block_idx < dmc->blocks_per_zone; block_idx++) {
            uint64_t full_addr = dmc->zone_idx_to_block_idx(zone_idx) + block_idx;
            cacheblock const *block = zbd_cacheblocks[full_addr];

            if (block == nullptr)
                ++cur_metrics.garbage;

            else if (block->lru_state == s::before_thresh)
                ++cur_metrics.before;

            else if (block->lru_state != s::before_thresh)
                ++cur_metrics.after;
        }
        assert(cur_metrics.garbage == per_zone_stats[zone_idx]->gcable_count);
        cur_score = calc_heat_score(cur_metrics);
        zone_scores.push_back({ zone_idx , cur_score });

        if (cur_score < best_score) {
            best_metrics = cur_metrics;
            best_zone_idx = zone_idx;
            best_score = cur_score;
        }
        found_first = true;
    }

    if (!found_first)
        return { (uint64_t)(-1) , 0 };

    VS_LOG(best_zone_idx, zone_scores, dmc);
    return { best_zone_idx , best_metrics.garbage };
}

std::tuple<uint64_t, uint64_t> dmcacheGC::gc_choose_zone_ts_score_tradeoff(dmcache *dmc) {

    uint64_t summation, zone_idx, best_zone_for_gc; 
    summation = zone_idx = best_zone_for_gc = 0;
    bool found_first_full_zone = false;
    std::vector<std::pair<std::uint64_t, std::uint64_t>> zone_scores;

    double avg_garbage; 
    double avg_ts_score; 

    double zone_score, best_zone_score;
    zone_score = best_zone_score = 0.0;

    uint64_t total_ts_score_sum = 0;
    uint64_t total_garbage_sum = 0;


    for (zone_idx = 0; zone_idx < dmc->nr_zones; zone_idx++) {

        if(!dmc->bm_full_zones->test_bit(zone_idx))
            continue;

        total_ts_score_sum += per_zone_stats[zone_idx]->ts_score;
        total_garbage_sum  += per_zone_stats[zone_idx]->gcable_count;
    }

    for (zone_idx = 0; zone_idx < dmc->nr_zones; zone_idx++) {

        if(!dmc->bm_full_zones->test_bit(zone_idx))
            continue;

        avg_garbage  = (double)per_zone_stats[zone_idx]->gcable_count/total_garbage_sum;
        avg_ts_score = (double)per_zone_stats[zone_idx]->ts_score/total_ts_score_sum;

        zone_score = (double)avg_garbage*(1-avg_ts_score);
        summation  = per_zone_stats[zone_idx]->ts_score;

        per_zone_stats[zone_idx]->avg_garbage  = avg_garbage;
        per_zone_stats[zone_idx]->avg_ts_score = avg_ts_score;
        per_zone_stats[zone_idx]->zone_score   = zone_score;


        //std::cout<<"ZONE:" << zone_idx << "Avg Garbg:" << avg_garbage << "Avg TS:" << avg_ts_score << "Zone Score:" << zone_score << std::endl;

        zone_scores.push_back({ zone_idx, summation });
        //select a zone with a higher zone_score 
        if (!found_first_full_zone || zone_score > best_zone_score) {
            found_first_full_zone = true;
            best_zone_for_gc = zone_idx;
            best_zone_score = zone_score;
        }
    }

    if (!found_first_full_zone) {
        RT_STOP(gc_select);
        return std::make_tuple((uint64_t)-1, 0);
    }

    VS_LOG(best_zone_for_gc, zone_scores, dmc);
    return std::make_tuple(best_zone_for_gc, per_zone_stats[best_zone_for_gc]->gcable_count);
}

std::tuple<uint64_t, uint64_t> dmcacheGC::gc_choose_zone_ts_score_opt(dmcache *dmc)
{
    uint64_t zone_idx, block_idx, summation, best_summation, best_zone_for_gc; 
    zone_idx = block_idx = summation = best_summation = best_zone_for_gc = 0;
    std::vector<std::pair<std::uint64_t, std::uint64_t>> zone_scores;
    bool found_first_full_zone = false;
    RT_START(gc_select);

#if DEBUG_MODE
    dmc->active_zones_print();
#endif

    for (zone_idx = 0; zone_idx < dmc->nr_zones; zone_idx++) {
        // Only consider full zones for GC
        if(!dmc->bm_full_zones->test_bit(zone_idx))
            continue;

        // Count the number of GCable blocks in each zone
        summation = per_zone_stats[zone_idx]->ts_score;
        VALIDATE_TS_SCORE(zone_idx, summation, dmc);
        zone_scores.push_back({ zone_idx, summation });

        DBUG_GC("[GC] zone_idx:", zone_idx, " zone_gcable_count:", zone_gcable_count, " zone_nongcable_count:", (dmc->blocks_per_zone - zone_gcable_count), "\n");
        // Keep track of the best zone we've found thus far
        if (!found_first_full_zone || summation < best_summation) {
        //if (/*!found_first_full_zone ||*/ zone_gcable_count > best_zone_gcable_count) {
            found_first_full_zone   = true;
            best_zone_for_gc        = zone_idx;
            best_summation          = summation;
        }
    }

    // No full zones found. Return max u64 to indicate to caller
    // assert(found_first_full_zone);
    if (!found_first_full_zone) {
        RT_STOP(gc_select);
        return std::make_tuple((uint64_t)-1, 0);
    }
    DBUG_GC("[GC] best_zone_for_gc:", best_zone_for_gc, " best_zone_gcable_count:", best_zone_gcable_count, "\n");
    RT_STOP(gc_select);

    //return best_zone_for_gc;
    VS_LOG(best_zone_for_gc, zone_scores, dmc);
    return std::make_tuple(best_zone_for_gc, per_zone_stats[best_zone_for_gc]->gcable_count);
}

/*
 * Old, gc_choose_zone_ts_score is equivalent and optimized and is to be used instead
 */
std::tuple<uint64_t, uint64_t> dmcacheGC::gc_choose_zone_ts_score(dmcache *dmc) {

    using s = cacheblock::lru_states;

    std::vector<std::pair<std::uint64_t, std::uint64_t>> zone_scores;
    uint64_t best_score = std::numeric_limits<uint64_t>::max();
    uint64_t best_zone_idx = 0, gcable_count = 0;
    bool found_first = false;

    for (uint64_t zone_idx = 0; zone_idx < dmc->nr_zones; zone_idx++) {
        uint64_t cur_score = 0, cur_gcable_count = 0;

        /* Only consider full zones */
        if (!dmc->bm_full_zones->test_bit(zone_idx))
            continue;

        for (uint64_t block_idx = 0; block_idx < dmc->blocks_per_zone; block_idx++) {
            uint64_t full_addr = dmc->zone_idx_to_block_idx(zone_idx) + block_idx;
            cacheblock const *block = zbd_cacheblocks[full_addr];

            if (block == nullptr)
                ++cur_gcable_count;

            else
                cur_score += block->last_access;
        }
        zone_scores.push_back({ zone_idx , cur_score });

        if (cur_score < best_score) {
            gcable_count = cur_gcable_count;
            best_zone_idx = zone_idx;
            best_score = cur_score;
        }
        found_first = true;
    }

    if (!found_first)
        return { (uint64_t)(-1) , 0 };

    VS_LOG(best_zone_idx, zone_scores, dmc);
    return { best_zone_idx , gcable_count };
}

