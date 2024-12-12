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

#ifndef __CURSOR_LL_H_
#define __CURSOR_LL_H_

#include "bdb_cursor.h"

#include <build/db.h>

u_int32_t get_cursor_flags(const bdb_cursor_impl_t * const cur, const uint8_t cursor_is_pausible);

enum berkdb_t {
    BERKDB_UNK = 0,
    BERKDB_REAL = 1,         /* persistent */
    BERKDB_SHAD = 2,         /* shadow, for open: if exist */
    BERKDB_SHAD_CREATE = 3,  /* shadow, for open: create if does not exist */
    BERKDB_REAL_ROWLOCKS = 4 /* persistent, use row locking */

};

typedef struct bdb_realdb_tag {

    /* berkdb objects */
    /*DB          *db;*/
    DBC *dbc;

    /* DBTs, using below buffers */
    DBT data; /* owns the buffer for data & key */
    DBT key;
    uint8_t ver;

    /* bulk api requirements; enabled only if tmpbulklen>0 */
    DBT bulk; /* owns the buffer for itself */
    int use_bulk;
    void *bulkptr;
    void *lastdta;
    void *lastkey;
    int lastdtasize;
    int lastkeysize;

    /* ODH and inplace updates:
       there is no easy way to prevent this due to current layering.
       for now, set this if you need odh processing */
    int use_odh;
    DBT odh;       /* NO buffer; points into odh_tmp, bulk, or data */
    char *odh_tmp; /* used with compress feature, damn odh api */

    int pagenum; /* Page number and index of the most recent move. */
    int pageidx;
    DB_LSN pagelsn;
    int need_update_shadows;

    unsigned char fileid[DB_FILE_ID_LEN];
    int got_fileid;

    /* debugging */
    int id; /* used for dbging, the current berkdb_counter val */

} bdb_realdb_tag_t;

typedef struct bdb_rowlocks_tag {
    void *dtamem;         /* memory for data */
    void *dta;            /* current payload */
    int dtalen;           /* size of payload */
    int lastdtasize;      /* size of the last data payload */
    void *keymem;         /* memory for key */
    void *key;            /* current key */
    int lastkeysize;      /* size of the last key payload */
    int keylen;           /* size of key */
    uint8_t ver;          /* ver of current payload */
    void *lastkey;        /* remembered key for re-position */
    int positioned;       /* cursor is positioned on something */
    int paused;           /* set to 1 if we're paused */
    int lock_mode;        /* set to 1 if we're not acquiring rowlocks */
    int have_lock;        /* we are holding the lock */
    DBC *pagelock_cursor; /* berkeley cursor backing this ll cursor */
    DBCPS pagelock_pause; /* hold paused state of pagelock_cursor */
    DB *db;               /* db the cursor is on */
    DB_LOCK lk;           /* row lock on the current row */
    int page;             /* Page of last successful move */
    int index;            /* Index of last successful move */
    unsigned char fileid[DB_FILE_ID_LEN];
    unsigned long long genid; /* genid locked */

    /* Bulk state */
    int numnexts;
    int use_bulk;
    DBT bulk;
    void *bulkptr;
    void *bulkkey;
    int bulkkeysz;
    void *bulkdata;
    int bulkdatasz;
    int use_odh; /* Uncompress bulk into here */
    DBT odh;
    char *odh_tmp;

    int eof;       /* Flag which marks the end of the file */
    DB_LSN lwmlsn; /* Low water mark */
    DB_LSN pagelsn;
    int force_rowlocks; /* Force this cursor to grab rowlocks */
    int active;         /* This is the current active cursor */

    int last_op;
    int last_rc;
    int last_bdberr;
} bdb_rowlocks_tag_t;

typedef struct bdb_shaddb_tag {

    tmptable_t *tbl;  /* this is a pointer in the shadow_tran; NO OWNERSHIP */
    tmpcursor_t *cur; /* we own this cursor */
    void *dta;        /* data */
    int dtalen;       /* length of the data */
    uint8_t ver;      /* version of this record */
    int use_odh;      /* shadow odh tag. */
    char *odh_tmp;    /* unpack odh */
    int tmpsz;        /* size of odh_tmp */
} bdb_shaddb_tag_t;

typedef struct bdb_berkdb_impl {

    enum berkdb_t type;     /* 0, real, 1 shadow */
    int idx;                /* generic index */
    bdb_cursor_impl_t *cur; /* owner cursor */

    /* connect to each operation */
    union {
        bdb_realdb_tag_t rl;
        bdb_shaddb_tag_t sd;
        bdb_rowlocks_tag_t row;
    } u;

    int trak; /* enable this for a lower level view of btree ops */
    bdb_state_type *bdb_state;
    int num_nexts;
    int num_nexts_since_open;
    int num_nexts_since_deadlock;
    int num_nexts_since_stale;

    /* hack to fix read committed when real points nowhere;
       this tries to offer some independence from lower level cursor move
       semantics
       (example: a next on uninitialized -> first; or hitting past right side ->
       last; and so on)
       if outoforder = 1,  don't use this for relative moves, repositioning
       first is required
     */
    int outoforder;
    uint8_t at_eof;
} bdb_berkdb_impl_t;

struct bdb_berkdb_impl;
typedef struct bdb_berkdb {

    struct bdb_berkdb_impl *impl;

    int (*unlock)(struct bdb_berkdb *pberkdb, int *bdberr);
    int (*lock)(struct bdb_berkdb *pberkdb, struct cursor_tran *curtran,
                int *bdberr);
    int (*move)(struct bdb_berkdb *berkdb, int dir, int *bdberr);
    int (*first)(struct bdb_berkdb *berkdb, int *bdberr);
    int (*next)(struct bdb_berkdb *berkdb, int *bdberr);
    int (*prev)(struct bdb_berkdb *berkdb, int *bdberr);
    int (*last)(struct bdb_berkdb *berkdb, int *bdberr);
    int (*close)(struct bdb_berkdb *berkdb, int *bdberr);
    int (*get_pagelsn)(struct bdb_berkdb *berkdb, DB_LSN *lsn, int *bdberr);
    int (*dta)(struct bdb_berkdb *berkdb, char **dta, int *bdberr);
    int (*dtasize)(struct bdb_berkdb *berkdb, int *dtasize, int *bdberr);
    int (*key)(struct bdb_berkdb *berkdb, char **key, int *bdberr);
    int (*keysize)(struct bdb_berkdb *berkdb, int *keysize, int *bdberr);
    int (*ver)(struct bdb_berkdb *berkdb, uint8_t *ver, int *bdberr);
    int (*find)(struct bdb_berkdb *berkdb, void *key, int keysize, int how,
                int *bdberr);
    int (*insert)(struct bdb_berkdb *berkdb, char *key, int keylen, char *dta,
                  int dtalen, int *bdberr);
    int (*delete)(struct bdb_berkdb *berkdb, int *bdberr);
    void (*trak)(struct bdb_berkdb *berkdb, int status);
    int (*get_everything)(struct bdb_berkdb *, char **dta, int *dtasize,
                          char **key, int *keysize, uint8_t *ver, int *bdberr);
    int (*is_at_eof)(struct bdb_berkdb *);
    int (*pageindex)(struct bdb_berkdb *, int *page, int *idx, int *bdberr);
    int (*fileid)(struct bdb_berkdb *, void *pfileid, int *bdberr);

    /* properties interpreted by higher level */
    int (*outoforder_set)(struct bdb_berkdb *berkdb, int status);
    int (*outoforder_get)(struct bdb_berkdb *berkdb);
    int (*prevent_optimized)(struct bdb_berkdb *berkdb);
    int (*allow_optimized)(struct bdb_berkdb *berkdb);
    int (*pause)(struct bdb_berkdb *berkdb, int *bdberr);
    int (*get_skip_stat)(struct bdb_berkdb *berkdb, u_int64_t *nextcount,
                         u_int64_t *skipcount);
    int (*defer_update_shadows)(struct bdb_berkdb *berkdb);
} bdb_berkdb_t;

#endif
