/*
 * handle.c
 * Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
 * $Id: handle.c,v 1.2 2007/08/10 17:23:21 hamano Exp $
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <memcache.h>
#include "memcachefs.h"
#include "handle.h"

static pthread_mutex_t handles_mutex = PTHREAD_MUTEX_INITIALIZER;

handle_pool_t *handle_pool_new(memcachefs_opt_t *opt)
{
    int i;
    int ret;
    handle_pool_t *pool;

    pool = (handle_pool_t*)malloc(sizeof(handle_pool_t));
    if(!pool){
        return NULL;
    }
    pool->num = opt->maxhandle;
    pool->handles = (handle_t**)malloc(sizeof(handle_t*) * pool->num);
    if(!pool->handles){
        return NULL;
    }
    for(i=0; i<pool->num; i++){
        pool->handles[i] = (handle_t*)malloc(sizeof(handle_t));
        if(!pool->handles[i]){
            return NULL;
        }
        memset(pool->handles[i], 0, sizeof(handle_t));
        pool->handles[i]->index = i;
        pool->handles[i]->buf_size = 1024 * 1024;
        pool->handles[i]->buf = (char*)malloc(pool->handles[i]->buf_size);
        pool->handles[i]->mc = mc_new();
        ret = mc_server_add(pool->handles[i]->mc, opt->host, opt->port);
    }
    return pool;
}

void handle_pool_free(handle_pool_t *pool)
{
    int i;
    for(i=0; i<pool->num; i++){
        free(pool->handles[i]->buf);
        mc_free(pool->handles[i]->mc);
        free(pool->handles[i]);
    }
    free(pool->handles);
    free(pool);
}

handle_t *handle_get(handle_pool_t *pool)
{
    int i;
    handle_t *ret = NULL;

    pthread_mutex_lock(&handles_mutex);
    for(i=0; i< pool->num; i++){
        if(!pool->handles[i]->use){
            ret = pool->handles[i];
            ret->use = 1;
            break;
        }
    }
    pthread_mutex_unlock(&handles_mutex);

    return ret;
}

void handle_release(handle_pool_t *pool, unsigned int index)
{
    pthread_mutex_lock(&handles_mutex);
    pool->handles[index]->use = 0;
    pthread_mutex_unlock(&handles_mutex);
    return;
}
