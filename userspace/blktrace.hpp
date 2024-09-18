#pragma once

#include <cstdint>
#include <fstream>
#include <vector>
#include <tuple>
#include "io.hpp"

#define BLK_IO_TRACE_MAGIC      0x65617400
#define BLK_IO_TRACE_VERSION    0x07

#define u64 uint64_t
#define u32 uint32_t
#define u16 uint16_t
#define u8  uint8_t

/*
 * Trace categories
 */
enum blktrace_cat {
    BLK_TC_READ     = 1 << 0,   /* reads */
    BLK_TC_WRITE    = 1 << 1,   /* writes */
    BLK_TC_FLUSH    = 1 << 2,   /* flush */
    BLK_TC_SYNC     = 1 << 3,   /* sync */
    BLK_TC_QUEUE    = 1 << 4,   /* queueing/merging */
    BLK_TC_REQUEUE  = 1 << 5,   /* requeueing */
    BLK_TC_ISSUE    = 1 << 6,   /* issue */
    BLK_TC_COMPLETE = 1 << 7,   /* completions */
    BLK_TC_FS       = 1 << 8,   /* fs requests */
    BLK_TC_PC       = 1 << 9,   /* pc requests */
    BLK_TC_NOTIFY   = 1 << 10,  /* special message */
    BLK_TC_AHEAD    = 1 << 11,  /* readahead */
    BLK_TC_META     = 1 << 12,  /* metadata */
    BLK_TC_DISCARD  = 1 << 13,  /* discard requests */
    BLK_TC_DRV_DATA = 1 << 14,  /* binary driver data */
    BLK_TC_FUA      = 1 << 15,  /* fua requests */
    BLK_TC_END      = 1 << 15,  /* we've run out of bits! */
};

/*
 * Basic trace actions
 */
enum blktrace_act {
    __BLK_TA_QUEUE = 1,         /* queued */
    __BLK_TA_BACKMERGE,         /* back merged to existing rq */
    __BLK_TA_FRONTMERGE,        /* front merge to existing rq */
    __BLK_TA_GETRQ,             /* allocated new request */
    __BLK_TA_SLEEPRQ,           /* sleeping on rq allocation */
    __BLK_TA_REQUEUE,           /* request requeued */
    __BLK_TA_ISSUE,             /* sent to driver */
    __BLK_TA_COMPLETE,          /* completed by driver */
    __BLK_TA_PLUG,              /* queue was plugged */
    __BLK_TA_UNPLUG_IO,         /* queue was unplugged by io */
    __BLK_TA_UNPLUG_TIMER,      /* queue was unplugged by timer */
    __BLK_TA_INSERT,            /* insert request */
    __BLK_TA_SPLIT,             /* bio was split */
    __BLK_TA_BOUNCE,            /* bio was bounced */
    __BLK_TA_REMAP,             /* bio was remapped */
    __BLK_TA_ABORT,             /* request aborted */
    __BLK_TA_DRV_DATA,          /* driver-specific binary data */
    __BLK_TA_CGROUP = 1 << 8,   /* from a cgroup*/
};

#define ACT_MASK            (31)
#define BLK_TC_SHIFT        (16)
#define BLK_TC_ACT(act)     ((act) << BLK_TC_SHIFT)

/*
 * Used for the actual trace events themselves
 */
struct blk_io_trace {
    u32 magic;              /* MAGIC << 8 | version */
    u32 sequence;           /* event number */
    u64 time;               /* in nanoseconds */
    u64 sector;             /* disk offset */
    u32 bytes;              /* transfer length */
    u32 action;             /* what happened */
    u32 pid;                /* who did it */
    u32 device;             /* device identifier (dev_t) */
    u32 cpu;                /* on what cpu did it happen */
    u16 error;              /* completion error */
    u16 pdu_len;            /* length of data after this trace */
};

/*
 * This doesn't come from blktrace, this is just to keep extra data that appears at the end
 *      of a trace with the corresponding blk_io_trace struct
 */
struct trace {
    blk_io_trace t;
    /* All "t.pdu_len" bytes of data after the blk_io_trace struct will be kept here */
    u8 *extra_data;
};

struct blktrace_parser {

public:

    blktrace_parser(std::vector<std::ifstream> &trace_files);
    ~blktrace_parser();

    /* Obtain the next IO / event type from the trace */
    std::tuple<bool, io_info, blktrace_act> get(uint32_t block_size);

private:

    std::vector<trace> trace_events;
    std::vector<trace>::iterator it;

};

