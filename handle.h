/*
 * handle.h
 * Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
 * $Id: handle.h,v 1.1 2007/08/07 15:26:13 hamano Exp $
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

typedef struct{
    int index;
    int use;
    struct memcache *mc;
    char *buf;
    size_t buf_len;
    size_t buf_size;
}handle_t;

typedef struct{
    handle_t **handles;
    size_t num;
}handle_pool_t;

handle_pool_t *handle_pool_new(memcachefs_opt_t *opt);
void handle_pool_free(handle_pool_t *);
handle_t *handle_get(handle_pool_t *pool);
void handle_release(handle_pool_t *pool, unsigned int index);
