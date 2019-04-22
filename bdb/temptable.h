/*
   Copyright 2019 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */


#ifndef __temptable_h__
#define __temptable_h__

/* temptables */

#include <stdint.h>

typedef struct bdb_state_tag bdb_state_type;

struct bdb_temp_hash;
typedef struct bdb_temp_hash bdb_temp_hash;

enum { BDB_TEMP_TABLE_DONT_USE_INMEM = 1 };
struct temp_table;
struct temp_cursor;
struct temp_table *bdb_temp_table_create(bdb_state_type *bdb_state,
                                         int *bdberr);
struct temp_table *bdb_temp_list_create(bdb_state_type *bdb_state, int *bdberr);
struct temp_table *bdb_temp_hashtable_create(bdb_state_type *bdb_state,
                                             int *bdberr);
struct temp_table *bdb_temp_table_create_flags(bdb_state_type *bdb_state,
                                               int flags, int *bdberr);

int bdb_temp_table_close(bdb_state_type *bdb_state, struct temp_table *table,
                         int *bdberr);

int bdb_temp_table_truncate(bdb_state_type *bdb_state, struct temp_table *tbl,
                            int *bdberr);

struct temp_cursor *bdb_temp_table_cursor(bdb_state_type *bdb_state,
                                          struct temp_table *table,
                                          void *usermem, int *bdberr);
int bdb_temp_table_close_cursor(bdb_state_type *bdb_state,
                                struct temp_cursor *cursor, int *bdberr);

int bdb_temp_table_insert(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                          void *key, int keylen, void *data, int dtalen,
                          int *bdberr);
int bdb_temp_table_update(bdb_state_type *bdb_state, struct temp_cursor *cur,
                          void *key, int keylen, void *data, int dtalen,
                          int *bdberr);
int bdb_temp_table_delete(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                          int *bdberr);

int bdb_temp_table_first(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                         int *bdberr);
int bdb_temp_table_last(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                        int *bdberr);
int bdb_temp_table_next(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                        int *bdberr);
int bdb_temp_table_prev(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                        int *bdberr);
int bdb_temp_table_move(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                        int how, int *bdberr);
int bdb_temp_table_next_norewind(bdb_state_type *bdb_state,
                                 struct temp_cursor *cursor, int *bdberr);
int bdb_temp_table_prev_norewind(bdb_state_type *bdb_state,
                                 struct temp_cursor *cursor, int *bdberr);

void *bdb_temp_table_get_usermem(struct temp_table *table);
void bdb_temp_table_set_user_data(struct temp_cursor *cur, void *usermem);

int bdb_temp_table_keysize(struct temp_cursor *cursor);
int bdb_temp_table_datasize(struct temp_cursor *cursor);
void *bdb_temp_table_key(struct temp_cursor *cursor);
void *bdb_temp_table_data(struct temp_cursor *cursor);

typedef int (*tmptbl_cmp)(void *, int, const void *, int, const void *);
void bdb_temp_table_set_cmp_func(struct temp_table *table, tmptbl_cmp);

int bdb_temp_table_find(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                        const void *key, int keylen, void *unpacked,
                        int *bdberr);
int bdb_temp_table_find_exact(bdb_state_type *bdb_state,
                              struct temp_cursor *cursor, void *key, int keylen,
                              int *bdberr);
void bdb_temp_table_reset_datapointers(struct temp_cursor *cur);

void *bdb_temp_table_get_cur(struct temp_cursor *skippy);

void bdb_get_cache_stats(bdb_state_type *bdb_state, uint64_t *hits,
                         uint64_t *misses, uint64_t *reads, uint64_t *writes,
                         uint64_t *thits, uint64_t *tmisses);
void bdb_thread_event(bdb_state_type *bdb_state, int event);

void bdb_stripe_get(bdb_state_type *bdb_state);
void bdb_stripe_done(bdb_state_type *bdb_state);

int bdb_count(bdb_state_type *bdb_state, int *bdberr);

struct bdb_temp_hash *bdb_temp_hash_create(bdb_state_type *bdb_state,
                                           char *tmpname, int *bdberr);
struct bdb_temp_hash *bdb_temp_hash_create_cache(bdb_state_type *bdb_state,
                                                 int cachekb, char *tmpname,
                                                 int *bdberr);
int bdb_temp_hash_destroy(bdb_temp_hash *h);
int bdb_temp_hash_insert(bdb_temp_hash *h, void *key, int keylen, void *dta,
                         int dtalen);
int bdb_temp_hash_lookup(bdb_temp_hash *h, void *key, int keylen, void *dta,
                         int *dtalen, int maxlen);



#endif
