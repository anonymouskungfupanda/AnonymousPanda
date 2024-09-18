obj-m += dmcache.o
EXTRA_CFLAGS := -msoft-float -msse -msse2 -msse3 -msse4
dmcache-objs := gc.o dm-lru.o dm-cache-stats.o dm-debug.o

dmcache-y := dm-cache.o

all:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) modules

clean:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) clean
