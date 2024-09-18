CFLAGS= -std=c++2a -Wall -o dm-cache-sim.out
SRCS=main.cpp io.cpp blktrace.cpp dmcache.cpp dmcache_opts.cpp zbd.cpp bitops.cpp dmcache_gc.cpp dmcache_gt.cpp fio_trace_parser.cpp dmcache_logs.cpp cache_admission.cpp dmcache_lru.cpp victim_selection.cpp book-keeping.cpp seq.cpp
CC=g++

all: $(SRCS)
	$(CC) -Wno-unused-variable $(SRCS) $(CFLAGS)

debug: $(SRCS)
	$(CC) -g -Wno-unused-variable $(SRCS) $(CFLAGS)

dmcache: $(SRCS)
	$(CC) -DGC_ENABLED=0 -g -Wno-unused-variable $(SRCS) $(CFLAGS)

dmcachegc: $(SRCS)
	$(CC) -DGC_ENABLED=1 -g -Wno-unused-variable $(SRCS) $(CFLAGS)

streamer: $(SRCS)
	$(CC) -O3 -DGC_ENABLED=1 -DSTREAMLINED_TRACE_INPUT -g -Wno-unused-variable $(SRCS) $(CFLAGS)

clean:
	rm -f dm-cache-sim.out
