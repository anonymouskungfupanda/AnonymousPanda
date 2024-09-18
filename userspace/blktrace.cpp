#include <fstream>
#include <string>
#include <utility>
#include <vector>
#include <algorithm>
#include "blktrace.hpp"
#include "io.hpp"

blktrace_parser::blktrace_parser(std::vector<std::ifstream> &trace_files) {

    for (std::ifstream &trace_file : trace_files) {

        while (trace_file.peek() != std::char_traits<char>::eof()) {

            trace cur_trace;
            trace_file.read((char *)&cur_trace.t, sizeof(blk_io_trace));

            /* There is a chance that certain events will contain extra data after, the amount
             *      denoted by pdu_len. If extra data exists, read it */
            if (cur_trace.t.pdu_len != 0) {
                cur_trace.extra_data = new u8[cur_trace.t.pdu_len];
                trace_file.read((char *)cur_trace.extra_data, cur_trace.t.pdu_len);
            }
            else cur_trace.extra_data = 0;

            trace_events.push_back(cur_trace);
        }
    }

    /* Lastly, these trace events might be all scattered across each of the files, so sort them based on
     *      the time field in the blk_io_trace struct */
    std::sort(trace_events.begin(), trace_events.end(), [](const trace &a, const trace &b) {
        return a.t.time < b.t.time;
    });

    /* Start at the beginning */
    it = trace_events.begin();
}

blktrace_parser::~blktrace_parser() {

    for (auto trace_event : trace_events) {
        if (trace_event.t.pdu_len != 0)
            delete[] trace_event.extra_data;
    }

}

/*
 * Obtain the next IO / event type from the trace
 */
std::tuple<bool, io_info, blktrace_act> blktrace_parser::get(uint32_t block_size) {

    const uint32_t sectors_to_bytes = 512;
    const uint32_t block_size_bytes = block_size * sectors_to_bytes;

    while (it != trace_events.end()) {

        blktrace_cat cat = (blktrace_cat)(it->t.action >> BLK_TC_SHIFT);
        blktrace_act act = (blktrace_act)(it->t.action & ACT_MASK);

        /* Ignore if event is not read or write */
        if (!(cat & BLK_TC_READ) && !(cat & BLK_TC_WRITE)) {
            ++it;
            continue;
        }

        /* Event is a queue event */
        if (act == __BLK_TA_QUEUE) {

            io_info io = { 0 };

            /*
             * The current hack for IO splitting:
             *
             *      Upon looking through blkparse, when an IO larger than the block size
             *  is queued, there are various __BLK_TA_SPLIT events to split it into smaller
             *  IOs. Below, if the size for the queued IO is greater than the block size, it
             *  will modify the trace event in-place to mimic the splitting. It will do this
             *  in-place modification until an IO for each block in the queued IO has been
             *  returned. If the queued IO is not larger than the block size, then no split
             *  events appear in blktrace so it is not necessary.
             *
             * TODO: Maybe
             *
             *      I'm unsure if this hack will at any point affect the order in which the
             * smaller IOs will be processed. If, at some point in the future, we observe that
             * the smaller IOs are being processed out of order then we might have to use the
             * split events to split the IOs instead of doing things this way.
             *
             */

            /* IO splitting is necessary */
            if (it->t.bytes > block_size_bytes) {

                /* Only use block_size in sectors for the size */
                io = {
                    .size_sectors = block_size               ,
                    .sector       = it->t.sector             ,
                    .is_read      = (cat & BLK_TC_READ) != 0 ,
                };

                /* In-place modification */
                it->t.sector = it->t.sector + block_size;
                it->t.bytes = it->t.bytes - block_size_bytes;

            }

            /* IO splitting is either finished or wasn't needed in the first place */
            else {

                io = {
                    .size_sectors = it->t.bytes / sectors_to_bytes   ,
                    .sector       = it->t.sector                     ,
                    .is_read      = (cat & BLK_TC_READ) != 0x0       ,
                };

                ++it;
            }

            return { true, io, __BLK_TA_QUEUE };

        }

        /* Event is a complete event */
        else if (act == __BLK_TA_COMPLETE) {

            io_info io = {
                .size_sectors = it->t.bytes / sectors_to_bytes ,
                .sector       = it->t.sector                   ,
                .is_read      = (cat & BLK_TC_READ) != 0x0     ,
            };

            ++it;

            return { true, io, __BLK_TA_COMPLETE };

        }

        /* Ignore everything that isn't a queue or complete event */
        else ++it;

    }

    /* None left, __BLK_TA_QUEUE is irrelevant */
    return { false, { 0 }, __BLK_TA_QUEUE };
}

