/*
 * memcachefs.c - memcache filesystem
 * Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
 * $Id: memcachefs.c,v 1.9 2007/08/10 17:23:21 hamano Exp $
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fuse/fuse.h>
#include <memcache.h>
#include "memcachefs.h"
#include "handle.h"

/* default options */
memcachefs_opt_t opt = {
    .host = NULL,
    .port = "11211",
    .verbose = 0,
    .maxhandle = 10,
};

handle_pool_t *pool;

static int memcachefs_connect()
{
    int sock;
    struct in_addr addr;
    struct hostent *host;
    struct sockaddr_in serv_addr;
    unsigned short port;

    memset((char *)&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family=AF_INET;

    if((addr.s_addr = inet_addr(opt.host))==-1){
        host = gethostbyname(opt.host);
        if(!host){
            return -1;
        }
        memcpy(&addr, (struct in_addr *)*host->h_addr_list,
               sizeof(struct in_addr));
    }

    port = atoi(opt.port);
    if(port <= 0){
        return -1;
    }
    serv_addr.sin_port = htons(port);
    serv_addr.sin_addr = addr;

    if((sock = socket(AF_INET,SOCK_STREAM, 0))<0){
        return -1;
    }
    if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr))==-1){
        close(sock);
        return -1;
    }
    return sock;
}

static int memcachefs_cachedump(int sock, char item_index,
                                fuse_fill_dir_t filler, void *filler_buf)
{
    char cmd[256];
    char *buf;
    char *ptr;
    int memlimit = 2*1024*1024; // see memcached source
    int len;
    char line[514];
    char *line_start;
    char *line_end;
    char *key;
    char *key_end;

    buf = malloc(memlimit);
    if(!buf){
        return -1;
    }

    snprintf(cmd, 256, "stats cachedump %d 0\r\n", item_index);
    len = write(sock, cmd, strlen(cmd));
    if(len != len){
        free(buf);
        return -1;
    }
    ptr = buf;
    do{
        len = read(sock, ptr, 512);
        if(len < 0){
            return -1;
        }
        ptr[len] = '\0';
        ptr += len;
    }while(len == 512);

    line_start = buf;
    line_end = buf;
    while(*line_start != '\0'){
        line_end = strchr(line_start, '\n');
        len = line_end - line_start;
        len = (len >= 514)?514:len;
        memcpy(line, line_start, len);
        line[len] = '\0';
        if(!strncmp(line, "ITEM ", 5)){
            key = strchr(line, ' ') + 1;
            key_end = strchr(key, ' ');
            *key_end = '\0';
            filler(filler_buf, key, NULL, 0);
        }else{
            break;
        }
        line_start = line_end + 1;
    }

    free(buf);
    return 0;
}

static int memcachefs_getattr(const char *path, struct stat *stbuf)
{
    handle_t *handle;
    char *key;
    size_t keylen;
    void *val;
    size_t vallen;

    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\")\n", __func__, path);
    }
    memset(stbuf, 0, sizeof(struct stat));

    if(!strcmp(path, "/")){
        stbuf->st_ino = 1;
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_uid = fuse_get_context()->uid;
        stbuf->st_gid = fuse_get_context()->gid;
        stbuf->st_nlink = 1;
        stbuf->st_atime = 0;
        stbuf->st_mtime = 0;
        stbuf->st_size = 0;
        return 0;
    }

    handle = handle_get(pool);
    if(!handle){
        return -EMFILE;
    }

    key = (char *)path + 1;
    keylen = strlen(key);
    val = (char*)mc_aget2(handle->mc, key, keylen, &vallen);
    handle_release(pool, handle->index);
    if(!val){
        return -ENOENT;
    }
    stbuf->st_mode = S_IFREG | 0666;
    stbuf->st_uid = fuse_get_context()->uid;
    stbuf->st_gid = fuse_get_context()->gid;
    stbuf->st_nlink = 1;
    stbuf->st_size = vallen;
    free(val);
    return 0;
}

static int memcachefs_opendir(const char *path, struct fuse_file_info *fi)
{
    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\")\n", __func__, path);
    }
    return 0;
}

static int memcachefs_readdir(const char *path, void *buf,
                              fuse_fill_dir_t filler, off_t offset,
                              struct fuse_file_info *fi)
{
    (void) offset;
    (void) fi;
    int sock;
    static char *cmd = "stats items\r\n";
    char buf_items[4096]; // see item.c of memcached
    size_t len;
    char line[256];
    char *line_start;
    char *line_end;
    char item_index;
    char *tmp;

    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\")\n", __func__, path);
    }
    if(strcmp(path, "/")){
        return -ENOENT;
    }

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    sock = memcachefs_connect();
    if(sock < 0){
        fprintf(stderr, "error: can't connect to %s:%s\n", opt.host, opt.port);
        return -EIO;
    }
    len = write(sock, cmd, strlen(cmd));
    len = read(sock, buf_items, sizeof(buf_items));
    buf_items[len] = '\0';

    line_start = buf_items;
    line_end = buf_items;
    while(*line_start != '\0'){
        line_end = strchr(line_start, '\n');
        len = line_end - line_start;
        len = (len >= 256)?256:len;
        memcpy(line, line_start, len);
        line[len] = '\0';
        line_start = line_end + 1;

        if(!strncmp(line, "STAT items:", 11)){
            tmp = (char*)(line + 11);
            item_index = atoi(tmp);
            tmp = strchr(tmp, ':');
            if(strncmp(tmp, ":number", 7)){
                continue;;
            }
            memcachefs_cachedump(sock, item_index, filler, buf);
        }else if(!strncmp(line, "END", 3)){
            break;
        }
    }
    close(sock);
    return 0;
}

static int memcachefs_releasedir(const char *path, struct fuse_file_info *fi)
{
    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\")\n", __func__, path);
    }
    return 0;
}

static int memcachefs_mknod(const char *path, mode_t mode, dev_t rdev)
{
    int ret;
    handle_t *handle;
    char *key;
    size_t keylen;

    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\", 0%o)\n", __func__, path, mode);
    }
    if(!S_ISREG(mode)){
        return -ENOSYS;
    }

    handle = handle_get(pool);
    if(!handle){
        return -EMFILE;
    }
    key = (char *)path + 1;
    keylen = strlen(key);
    ret = mc_set(handle->mc, key, keylen, "", 0, 0, 0);
    handle_release(pool, handle->index);
    if(ret){
        return -EIO;
    }

    return 0;
}

static int memcachefs_mkdir(const char *path, mode_t mode){
    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\", 0%o)\n", __func__, path, mode);
    }
    return -ENOSYS;
}

static int memcachefs_unlink(const char *path)
{
    int ret;
    handle_t *handle;
    char *key;
    size_t keylen;

    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\")\n", __func__, path);
    }
    handle = handle_get(pool);
    if(!handle){
        return -EMFILE;
    }
    key = (char *)path + 1;
    keylen = strlen(key);
    ret = mc_delete(handle->mc, key, keylen, 0);
    handle_release(pool, handle->index);
    if(ret){
        return -EIO;
    }

    return 0;
}

static int memcachefs_chmod(const char* path, mode_t mode)
{
    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\", 0%3o)\n", __func__, path, mode);
    }
    return -ENOSYS;
}

static int memcachefs_chown(const char *path, uid_t uid, gid_t gid)
{
    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\", %d, %d)\n", __func__, path, uid, gid);
    }
    return -ENOSYS;
}

static int memcachefs_truncate(const char* path, off_t length)
{
    int ret;
    handle_t *handle;
    char *key;
    size_t keylen;

    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\", %lld)\n", __func__, path, length);
    }
    if(length != 0){
        return -ENOSYS;
    }

    handle = handle_get(pool);
    if(!handle){
        return -EMFILE;
    }

    key = (char *)path + 1;
    keylen = strlen(key);
    ret = mc_set(handle->mc, key, keylen, "", 0, 0, 0);
    handle_release(pool, handle->index);
    if(ret){
        return -EIO;
    }

    return 0;
}

static int memcachefs_utime(const char *path, struct utimbuf *time)
{
    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\")\n", __func__, path);
    }
    return 0;
}

static int memcachefs_open(const char *path, struct fuse_file_info *fi)
{
    handle_t *handle;
    char *key;
    size_t keylen;
    void *val;
    size_t vallen;

    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\")\n", __func__, path);
    }

    handle = handle_get(pool);
    if(!handle){
        return -EMFILE;
    }
    fi->fh = handle->index;

    key = (char *)path + 1;
    keylen = strlen(key);
    val = mc_aget2(handle->mc, key, keylen, &vallen);
    if(!val){
        return -EIO;
    }
    memcpy(handle->buf, val, vallen);
    handle->buf_len = vallen;

    return 0;
}

static int memcachefs_read(const char *path, char *buf, size_t size,
                           off_t offset, struct fuse_file_info *fi)
{
    size_t len;

    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\" %zu@%llu)\n", __func__, path, size, offset);
    }

    len = pool->handles[fi->fh]->buf_len - offset;
    len = (len < size)?len:size;
    memcpy(buf, pool->handles[fi->fh]->buf + offset, len);

    return len;
}

static int memcachefs_write(const char *path, const char *buf, size_t size,
                            off_t offset, struct fuse_file_info *fi)
{
    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\" %zu@%lld)\n", __func__, path, size, offset);
    }

    // The memcached can be stored up to 1M - 1 or less byte.
    if(offset + size >= pool->handles[fi->fh]->buf_size){
        return -EFBIG;
    }

    memcpy(pool->handles[fi->fh]->buf + offset, buf, size);
    if(pool->handles[fi->fh]->buf_len < offset + size){
        pool->handles[fi->fh]->buf_len = offset + size;
    }

    return size;
}

static int memcachefs_flush(const char *path, struct fuse_file_info *fi)
{
    int ret;
    handle_t *handle;
    char *key;
    size_t keylen;

    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\")\n", __func__, path);
    }

    handle = pool->handles[fi->fh];

    key = (char *)path + 1;
    keylen = strlen(key);
    ret = mc_set(handle->mc, key, keylen, handle->buf, handle->buf_len , 0, 0);
    if(ret){
        return -EIO;
    }

    return 0;
}

static int memcachefs_release(const char *path, struct fuse_file_info *fi)
{
    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\")\n", __func__, path);
    }

    handle_release(pool, fi->fh);

    return 0;
}

static int memcachefs_fsync(const char *path, int i, struct fuse_file_info *fi)
{
    int ret;
    handle_t *handle;
    char *key;
    size_t keylen;

    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\", %d)\n", __func__, path, i);
    }
    handle = pool->handles[fi->fh];

    key = (char *)path + 1;
    keylen = strlen(key);
    ret = mc_set(handle->mc, key, keylen, handle->buf, handle->buf_len , 0, 0);
    if(ret){
        return -EIO;
    }

    return 0;
}

static int memcachefs_link(const char *from, const char *to)
{
    if(opt.verbose){
        fprintf(stderr, "%s(%s, %s)\n", __func__, from, to);
    }
    return -ENOSYS;
}

static int memcachefs_symlink(const char *from, const char *to)
{
    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\" -> \"%s\")\n", __func__, from, to);
    }
    return -ENOSYS;
}

static int memcachefs_readlink(const char *path, char *buf, size_t size)
{
    if(opt.verbose){
        fprintf(stderr, "%s(\"%s\")\n", __func__, path);
    }
    return -ENOSYS;
}

static int memcachefs_rename(const char *from, const char *to)
{
    int ret;
    handle_t *handle;
    char *key;
    size_t keylen;
    void *val;
    size_t vallen;

    if(opt.verbose){
        fprintf(stderr, "%s(%s -> %s)\n", __func__, from, to);
    }
    handle = handle_get(pool);

    key = (char *)from + 1;
    keylen = strlen(key);
    val = (char*)mc_aget2(handle->mc, key, keylen, &vallen);
    if(!val){
        handle_release(pool, handle->index);
        return -ENOENT;
    }

    key = (char *)to + 1;
    keylen = strlen(key);
    ret = mc_set(handle->mc, key, keylen, val, vallen, 0, 0);
    if(ret){
        handle_release(pool, handle->index);
        return -EIO;
    }

    key = (char *)from + 1;
    keylen = strlen(key);
    ret = mc_delete(handle->mc, key, keylen, 0);
    if(ret){
        handle_release(pool, handle->index);
        return -EIO;
    }

    free(val);
    return 0;
}

static struct fuse_operations memcachefs_oper = {
    .getattr    = memcachefs_getattr,
    .opendir    = memcachefs_opendir,
    .readdir    = memcachefs_readdir,
    .releasedir = memcachefs_releasedir,
    .mknod      = memcachefs_mknod,
    .mkdir      = memcachefs_mkdir,
    .unlink     = memcachefs_unlink,
    .rmdir      = memcachefs_unlink,
    .chmod      = memcachefs_chmod,
    .chown      = memcachefs_chown,
    .truncate   = memcachefs_truncate,
    .utime      = memcachefs_utime,
    .open       = memcachefs_open,
    .read       = memcachefs_read,
    .write      = memcachefs_write,
    .flush      = memcachefs_flush,
    .release    = memcachefs_release,
    .fsync      = memcachefs_fsync,
    .link       = memcachefs_link,
    .symlink    = memcachefs_symlink,
    .readlink   = memcachefs_readlink,
    .rename     = memcachefs_rename,
};

void usage(){
    fprintf(stderr, "Usage: memcachefs host[:port] mountpoint\n");
}

static int memcachefs_opt_proc(void *data, const char *arg, int key,
                               struct fuse_args *outargs)
{
    char *str;

    if(key == FUSE_OPT_KEY_OPT){
        if(!strcmp(arg, "-v")){
            opt.verbose = 1;
        }else if(!strncmp(arg, "maxhandle=", strlen("maxhandle="))){
            str = strchr(arg, '=') + 1;
            opt.maxhandle = atoi(str);
        }else{
            fuse_opt_add_arg(outargs, arg);
       }
    }else if(key == FUSE_OPT_KEY_NONOPT){
        if(!opt.host){
            opt.host = (char*)arg;
            str = strchr(arg, ':');
            if(str){
                *str = '\0';
                str++;
                opt.port = str;
            }
        }else{
            fuse_opt_add_arg(outargs, arg);
        }
    }
    return 0;
}

/*
 * main
 */
int main(int argc, char *argv[])
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    fuse_opt_parse(&args, &opt, NULL, memcachefs_opt_proc);

    if(!opt.host){
        usage();
        return EXIT_SUCCESS;
    }

    pool = handle_pool_new(&opt);
    if(!pool){
        perror("malloc()");
        return EXIT_FAILURE;
    }

    if(opt.verbose){
        fprintf(stderr, "mounting to %s:%s\n", opt.host, opt.port);
    }

    fuse_main(args.argc, args.argv, &memcachefs_oper);
    fuse_opt_free_args(&args);
    handle_pool_free(pool);
    return EXIT_SUCCESS;
}
