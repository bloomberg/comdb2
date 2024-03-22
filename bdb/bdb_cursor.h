/*
   Copyright 2015 Bloomberg Finance L.P.

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

/* api to use bdb cursors */

#ifndef __bdb_cursor_h__
#define __bdb_cursor_h__

#include <limits.h>

#include "bdb_api.h"
#include <list.h>

struct cursor_tran;
typedef struct cursor_tran cursor_tran_t;

typedef struct temp_cursor tmpcursor_t;
typedef struct temp_table tmptable_t;

/* NOTE: this typing break a bit the bdbcursor_t encapsulation,
   but it allows us opening only one side (real or shadow)
   and be faster since we know what we are doing in sqlglue.c
 */
enum bdb_open_type {
    BDB_OPEN_BOTH = 0,
    BDB_OPEN_BOTH_CREATE = 1,
    BDB_OPEN_REAL = 2,
    BDB_OPEN_SHAD = 3,
};

/**
 * - BDB_SET: try to position to exact row, return IX_NOTFND if not possible
 *(DB_SET like)
 * - BDB_NEXT: try to position on a next row (DB_SET_RANGE like)
 * - BDB_PREV: try to position on a previous row (LAST_DUP like)
 *
 */
enum { BDB_SET, BDB_NEXT, BDB_PREV };

struct bdb_cursor_impl_tag;
typedef struct bdb_cursor_impl_tag bdb_cursor_impl_t;

typedef struct bdb_cursor_ifn {
    bdb_cursor_impl_t *impl;

    /* Link to the next open cursor. */
    LINKC_T(struct bdb_cursor_ifn) lnk;

    /* move operations */
    int (*first)(struct bdb_cursor_ifn *cur, int *bdberr);
    int (*next)(struct bdb_cursor_ifn *cur, int *bdberr);
    int (*prev)(struct bdb_cursor_ifn *cur, int *bdberr);
    int (*last)(struct bdb_cursor_ifn *cur, int *bdberr);
    int (*find)(struct bdb_cursor_ifn *cur, void *key, int keylen, int dirLeft,
                int *bdberr);
    int (*find_last_dup)(struct bdb_cursor_ifn *, void *key, int keylen,
                         int keymax, bias_info *, int *bdberr);

    /* updates */
    int (*insert)(struct bdb_cursor_ifn *cur, unsigned long long genid,
                  void *data, int datalen, void *datacopy, int datacopylen,
                  int *bdberr);
    int (*delete)(struct bdb_cursor_ifn *cur, int *bdberr);

    /* accessor functions */
    void *(*data)(struct bdb_cursor_ifn *cur);
    int (*datalen)(struct bdb_cursor_ifn *cur);
    int (*rrn)(struct bdb_cursor_ifn *cur);
    unsigned long long (*genid)(struct bdb_cursor_ifn *cur);
    int (*dbnum)(struct bdb_cursor_ifn *cur);
    void *(*datacopy)(struct bdb_cursor_ifn *cur);
    uint8_t (*ver)(struct bdb_cursor_ifn *cur);
    void (*get_found_data)(struct bdb_cursor_ifn *, int *rrn,
                           unsigned long long *genid, int *datalen, void **data,
                           uint8_t *ver);
    void *(*collattr)(struct bdb_cursor_ifn *cur);
    int (*collattrlen)(struct bdb_cursor_ifn *cur);

    /* lock/unlock */
    int (*unlock)(struct bdb_cursor_ifn *cur, int *bdberr);
    int (*lock)(struct bdb_cursor_ifn *cur, cursor_tran_t *curtran, int how,
                int *bdberr);
    int (*set_curtran)(struct bdb_cursor_ifn *cur, cursor_tran_t *curtran);

    /* Get pageorder information. */
    int (*getpageorder)(struct bdb_cursor_ifn *cur);

    /* Update my shadows. */
    int (*updateshadows)(struct bdb_cursor_ifn *cur, int *bdberr);
    int (*updateshadows_pglogs)(struct bdb_cursor_ifn *cur, unsigned *inpgno,
                                unsigned char *infileid, int *bdberr);

    void *(*getshadowtran)(struct bdb_cursor_ifn *cur);

    /* Set NULL in shadows. */
    int (*setnullblob)(struct bdb_cursor_ifn *cur, unsigned long long genid,
                       int dbnum, int blobno, int *bdberr);

    /* close */
    int (*close)(struct bdb_cursor_ifn *cur, int *bdberr);

    /* pause */
    int (*pause)(struct bdb_cursor_ifn *cur, int *bdberr);

    /* Pause all */
    int (*pauseall)(void *arg);

    /* Pause arg */
    void *pausearg;

    /* Count */
    int (*count)(void *arg);

    /* Count arg */
    void *countarg;

} bdb_cursor_ifn_t;

/**
 * Bdbcursor creator (behold)
 *
 */
bdb_cursor_ifn_t *
bdb_cursor_open(bdb_state_type *bdb_state, cursor_tran_t *curtran,
                tran_type *shadow_tran, int ixnum, enum bdb_open_type type,
                void *shadadd, /* THIS WILL GO AWAY */
                int pageorder, int rowlocks, int *holding_pagelocks_flag,
                int (*pause_pagelock_cursors)(void *), void *pausearg,
                int (*count_pagelock_cursors)(void *), void *countarg, int trak,
                int *bdberr, int snapcur);

int bdb_cursor_process_skip(bdb_state_type *bdb_state, tran_type *tran,
                            /* upcall */
                            void *arg0, void *arg1,
                            int (*upcall)(void *, void *, int,
                                          unsigned long long),
                            int *bdberr);

int bdb_cursor_process_sdata(bdb_state_type *bdb_state, tran_type *tran,
                             /* upcall */
                             void *arg0, void *arg1,
                             int (*upcall)(void *, void *, int, void *, int,
                                           unsigned long long),
                             int *bdberr);

int bdb_set_check_shadows(tran_type *shadow_tran);

/**
 *  Create/destroy a curtran
 *
 */
cursor_tran_t *bdb_get_cursortran(bdb_state_type *bdb_state, uint32_t flags,
                                  int *bdberr);
int bdb_put_cursortran(bdb_state_type *bdb_state, cursor_tran_t *curtran,
                       uint32_t flags, int *bdberr);

/**
 * Return lockerid in use by provided curtran
 *
 */
uint32_t bdb_get_lid_from_cursortran(cursor_tran_t *curtran);

/**
 * Parse a string containing an enable/disable feature and
 * return a proper return code for it
 *
 */
int bdb_osql_trak(char *sql, unsigned int *status);

int bdb_free_curtran_locks(bdb_state_type *bdb_state, cursor_tran_t *curtran,
                           int *bdberr);

int bdb_curtran_has_waiters(bdb_state_type *bdb_state, cursor_tran_t *curtran);

unsigned int bdb_curtran_get_lockerid(cursor_tran_t *curtran);

int bdb_bkfill_shadows_pglogs_from_active_ltrans(bdb_state_type *bdb_state,
                                                 tran_type *shadow_tran,
                                                 int *bdberr);

int bdb_get_lsn_context_from_timestamp(bdb_state_type *bdb_state,
                                       int32_t timestamp, void *ret_lsn,
                                       unsigned long long *ret_context,
                                       int *bdberr);

int bdb_get_context_from_lsn(bdb_state_type *bdb_state, void *lsnp,
                             unsigned long long *ret_context, int *bdberr);

int bdb_direct_count(bdb_cursor_ifn_t *, int ixnum, int64_t *count, int is_snapcur, uint32_t last_commit_lsn_file, uint32_t last_commit_lsn_offset, uint32_t last_checkpoint_lsn_file, uint32_t last_checkpoint_lsn_offset);

#endif
