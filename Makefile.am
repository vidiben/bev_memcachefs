AM_CFLAGS = -Wall
bin_PROGRAMS = memcachefs
memcachefs_SOURCES = memcachefs.c handle.c
noinst_HEADERS = memcachefs.h handle.h
memcachefs_LDFLAGS = -L. -lfuse -lmemcache
man_MANS = memcachefs.1
EXTRA_DIST = $(man_MANS) debian/changelog debian/compat debian/control \
	debian/copyright debian/rules
