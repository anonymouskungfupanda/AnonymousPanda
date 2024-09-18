#undef TRACE_SYSTEM
#define TRACE_SYSTEM dmcache

#if !defined(_TRACE_SCHED_H) || defined(TRACE_HEADER_MULTI_READ)
#define _TRACE_DMCACHE_H

#include <linux/blk_types.h>
#include <linux/types.h>
#include <linux/tracepoint.h>

/* ---------------------------------------------- */

DECLARE_EVENT_CLASS(dmcache_bio_template,

    TP_PROTO(struct bio *_bio),

    TP_ARGS(_bio),

    TP_STRUCT__entry(
        /* This is subject to change as I can imagine we might want to keep track of more
	 * relevant information, this is mainly just to make sure things work */
	__field( sector_t, bi_sector )
    ),

    TP_fast_assign(
        __entry->bi_sector = _bio->bi_iter.bi_sector;
    ),

    TP_printk("bi_sector: %llu",
        (unsigned long long)__entry->bi_sector   
    )

);

DEFINE_EVENT(dmcache_bio_template, dmcache_submit_bio_noacct,

    TP_PROTO(struct bio *_bio),

    TP_ARGS(_bio)

);

/* ---------------------------------------------- */

#endif /* _TRACE_DMCACHE_H */

/* This part must be outside protection */
#include <trace/define_trace.h>

