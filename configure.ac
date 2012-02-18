#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT
AM_INIT_AUTOMAKE(memcachefs, 0.5)
AC_CONFIG_SRCDIR([memcachefs.c])
AM_CONFIG_HEADER(config.h)

# Checks for programs.
AC_PROG_CC
#AM_PROG_CC_C_O
AC_PROG_LIBTOOL

#AC_DEFINE(FUSE_USE_VERSION, 22, [Fuse API Version])
AC_DEFINE(FUSE_USE_VERSION, 25, [Fuse API Version])
AC_DEFINE(_FILE_OFFSET_BITS,64,[Use 64 bits file offsets])

# Checks for libraries.
AC_CHECK_LIB([fuse], [main])
AC_CHECK_LIB(memcache, [main])
AC_CHECK_LIB(pthread, pthread_create)

# Checks for header files.
AC_PATH_X
AC_HEADER_STDC
AC_CHECK_HEADERS([stdio.h stdlib.h string.h unistd.h])
AC_CHECK_HEADERS([errno.h fcntl.h libgen.h limits.h])
AC_CHECK_HEADERS([sys/stat.h sys/types.h sys/socket.h])
AC_CHECK_HEADERS([netinet/in.h arpa/inet.h netdb.h])
AC_CHECK_HEADERS(pthread.h)
AC_CHECK_HEADERS(fuse/fuse.h,, AC_MSG_ERROR([Please install fuse development package]))
AC_CHECK_HEADERS(memcache.h,, AC_MSG_ERROR([Please install libmemcache development package]))

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST

# Checks for library functions.
AC_CHECK_FUNCS([strcspn strdup strspn strstr])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
