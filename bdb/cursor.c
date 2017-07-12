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

/*
   THIS IS THE FILE FORMERLY KNOWN AS BDB_SQL.C

   it's cursor.c now to reflect the new intent.  this file
   implements the bdb_cursor object.  this object is
   our cursor abstraction.  it should be able to be backed
   by real berkeley cursors, by the "fetch.c" codepath (formerly 'ix mode'),
   by remote databases through marhsalling, whatever.  calling code in db
   should use bdb_cursors and open them with the correct intent.

   for now, none of this exists.

   all routines in this file DO NOT GET the bdb lock.
   they assume it is ALREADY BEING HELD by a cutran or tran
   or whatever you allocated already
 */

/*
   NOTES ON THE NEW BDB CURSOR
Assumptions:
- using data/datalen/genid
will give you the data and its genid for both data and
indexes
- data == NULL means not positioned (or empty) btree
- data/datalen/genid are changed only by a successful
move in the btree/shadow
- a cursor points to a row in the btree or its shadow,
as long as there was a successful move in the past
(ie. btree was not empty);
 */

/* TODO:
   1) additional pointer to index-inline data
   2) allocate DBTs to match row sizes
 */
#ifdef __sun
   /* for PTHREAD_STACK_MIN on Solaris */
#  define __EXTENSIONS__
#endif

#ifdef NEWSI_STAT
#include <time.h>
#include <sys/time.h>
#include <util.h>
#endif

#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <alloca.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <limits.h>
#include <strings.h>
#include <assert.h>
#include <poll.h>
#include <netinet/in.h>

#include <db.h>
#include <fsnap.h>
#include <ctrace.h>

#include "net.h"
#include "bdb_int.h"
#include "bdb_cursor.h"
#include "locks.h"
#include "bdb_osqlcur.h"
#include "bdb_osqllog.h"
#include "bdb_osqltrn.h"
#include <dlfcn.h>
#include <list.h>
#include <plhash.h>
#include "logmsg.h"

#include "genid.h"
#define MERGE_DEBUG (0)

struct datacopy_info {
    void *datacopy;
    int size;
};

static char hex(unsigned char a)
{
    if (a < 10)
        return '0' + a;
    return 'a' + (a - 10);
}
/* Return a hex string */
static char *tohex(char *output, char *key, int keylen)
{
    int i = 0;
    char byte[3];

    output[0] = '\0';
    byte[2] = '\0';

    for (i = 0; i < keylen; i++) {
        snprintf(byte, sizeof(byte), "%c%c", hex(((unsigned char)key[i]) / 16),
                 hex(((unsigned char)key[i]) % 16));
        strcat(output, byte);
    }

    return output;
}

static void hexdump(loglvl lvl, char *key, int keylen)
{
    if (key == NULL || keylen == 0) {
        logmsg(LOGMSG_ERROR, "NULL(%d)\n", keylen);
        return;
    }
    char *mem;
    char *output;

    mem = alloca((2 * keylen) + 2);
    output = tohex(mem, key, keylen);

    logmsg(lvl, "%s\n", output);
}

static void hexdumpbuf(char *key, int keylen, char **buf)
{
    char *mem;
    char *output;

    mem = malloc((2 * keylen) + 2);
    output = tohex(mem, key, keylen);

    *buf = output;
}

pthread_mutex_t pr_lk = PTHREAD_MUTEX_INITIALIZER;

int lkprintf(loglvl lvl, const char *fmt, ...)
{
    va_list args;
    int rc;

    pthread_mutex_lock(&pr_lk);

    va_start(args, fmt);
    logmsgv(lvl, fmt, args);
    va_end(args);

    pthread_mutex_unlock(&pr_lk);

    return 0;
}

/* switch a cursor to a different dta file
   we add ONE virtual data stripe,
   indexed dtastripe (where add/upd are stored)
 */
#define IS_VALID_DTA(id)                                                       \
    ((id) >= 0 && (((id) < cur->state->attr->dtastripe) ||                     \
                   ((id) == cur->state->attr->dtastripe && cur->addcur)))

hash_t *logfile_pglogs_repo = NULL;
static unsigned first_logfile;
static unsigned last_logfile;
static int logfile_pglogs_repo_ready = 0;
static pthread_mutex_t logfile_pglogs_repo_mutex;

#ifdef NEWSI_STAT
struct timeval logfile_relink_time;
struct timeval logfile_insert_time;
struct timeval client_insert_time;
struct timeval client_relink_time;
struct timeval ltran_insert_time;
struct timeval ltran_relink_time;
struct timeval txn_insert_time;
struct timeval txn_relink_time;
struct timeval logical_undo_time;
pthread_mutex_t newsi_stat_mutex;
#endif

struct temp_table *bdb_gbl_timestamp_lsn;
pthread_mutex_t bdb_gbl_timestamp_lsn_mutex;
tmpcursor_t *bdb_gbl_timestamp_lsn_cur;
int bdb_gbl_timestamp_lsn_ready = 0;
extern int gbl_newsi_use_timestamp_table;

hash_t *bdb_gbl_ltran_pglogs_hash;
pthread_mutex_t bdb_gbl_ltran_pglogs_mutex;
int bdb_gbl_ltran_pglogs_hash_ready = 0;
int bdb_gbl_ltran_pglogs_hash_processed = 0;

extern pthread_mutex_t bdb_gbl_recoverable_lsn_mutex;
extern DB_LSN bdb_gbl_recoverable_lsn;
extern int32_t bdb_gbl_recoverable_timestamp;

static hash_t *bdb_asof_ltran_hash;
extern pthread_mutex_t bdb_asof_current_lsn_mutex;
extern DB_LSN bdb_asof_current_lsn;
extern DB_LSN bdb_latest_commit_lsn;
extern uint32_t bdb_latest_commit_gen;
extern pthread_cond_t bdb_asof_current_lsn_cond;

static int bdb_switch_stripe(bdb_cursor_impl_t *cur, int dtafile, int *bdberr);
static int bdb_cursor_find_merge(bdb_cursor_impl_t *cur, void *key, int keylen,
                                 int *bdberr);
static int bdb_cursor_move(bdb_cursor_impl_t *cur, int how, int *bdberr);
static int bdb_btree_merge(bdb_cursor_impl_t *cur, int stripe_rl, int page_rl,
                           int index_rl, char *data_rl, int datalen_rl,
                           char *data_sd, int datalen_sd, char *key_rl,
                           int keylen_rl, char *key_sd, int keylen_sd,
                           unsigned long long genid_rl,
                           unsigned long long genid_sd, uint8_t ver_rl,
                           int how);
static int bdb_btree_update_shadows(bdb_cursor_impl_t *cur, int how,
                                    int *bdberr);
static int bdb_btree_update_shadows_with_trn_pglogs(bdb_cursor_impl_t *cur,
                                                    db_pgno_t *inpgno,
                                                    unsigned char *infileid,
                                                    int *bdberr);

/* local bdb cursor functionality */
static int bdb_cursor_first(bdb_cursor_ifn_t *cur, int *bdberr);
static int bdb_cursor_next(bdb_cursor_ifn_t *cur, int *bdberr);
static int bdb_cursor_prev(bdb_cursor_ifn_t *cur, int *bdberr);
static int bdb_cursor_last(bdb_cursor_ifn_t *cur, int *bdberr);
static int bdb_cursor_find(bdb_cursor_ifn_t *cur, void *key, int keylen,
                           int dirLeft, int *bdberr);
static int bdb_cursor_find_last_dup(bdb_cursor_ifn_t *, void *key, int keylen,
                                    int keymax, bias_info *, int *bdberr);
static int bdb_cursor_close(bdb_cursor_ifn_t *cur, int *bdberr);
static int bdb_cursor_getpageorder(bdb_cursor_ifn_t *pcur_ifn);
static int bdb_cursor_update_shadows(bdb_cursor_ifn_t *pcur_ifn, int *bdberr);
static void *bdb_cursor_get_shadowtran(bdb_cursor_ifn_t *pcur_ifn);
static int bdb_cursor_update_shadows_with_pglogs(bdb_cursor_ifn_t *pcur_ifn,
                                                 unsigned *inpgno,
                                                 unsigned char *infileid,
                                                 int *bdberr);
static int bdb_cursor_set_null_blob_in_shadows(bdb_cursor_ifn_t *pcur_ifn,
                                               unsigned long long genid,
                                               int dbnum, int blobno,
                                               int *bdberr);
static int bdb_cursor_pause(bdb_cursor_ifn_t *pcur_ifn, int *bdberr);
static void *bdb_cursor_data(bdb_cursor_ifn_t *cur);
static int bdb_cursor_datalen(bdb_cursor_ifn_t *cur);
static unsigned long long bdb_cursor_genid(bdb_cursor_ifn_t *cur);
static int bdb_cursor_rrn(bdb_cursor_ifn_t *cur);
static int bdb_cursor_dbnum(bdb_cursor_ifn_t *cur);
static void *bdb_cursor_datacopy(bdb_cursor_ifn_t *cur);
static uint8_t bdb_cursor_ver(bdb_cursor_ifn_t *cur);
static void bdb_cursor_found_data(struct bdb_cursor_ifn *cur, int *rrn,
                                  unsigned long long *genid, int *datalen,
                                  void **data, uint8_t *ver);
static void *bdb_cursor_collattr(bdb_cursor_ifn_t *cur);
static int bdb_cursor_collattrlen(bdb_cursor_ifn_t *cur);

static int bdb_cursor_insert(bdb_cursor_ifn_t *cur, unsigned long long genid,
                             void *data, int datalen, void *datacopy,
                             int datacopylen, int *bdberr);
static int bdb_cursor_delete(bdb_cursor_ifn_t *cur, int *bdberr);

static int bdb_cursor_unlock(bdb_cursor_ifn_t *cur, int *bdberr);
static int bdb_cursor_lock(bdb_cursor_ifn_t *cur, cursor_tran_t *curtran,
                           int how, int *bdberr);
static int bdb_cursor_set_curtran(bdb_cursor_ifn_t *cur,
                                  cursor_tran_t *curtran);
static inline int calculate_discard_pages(bdb_cursor_impl_t *cur);

static void *unpack_datacopy_odh(bdb_cursor_ifn_t *cur, uint8_t *to,
                                 int to_size, uint8_t *from, int from_size,
                                 uint8_t *ver);

static int bdb_cursor_reposition(bdb_cursor_ifn_t *pcur_ifn, int how,
                                 int *bdberr);
static int bdb_cursor_reposition_noupdate(bdb_cursor_ifn_t *pcur_ifn,
                                          bdb_berkdb_t *berkdb, char *key,
                                          int keylen, int how, int *bdberr);
static int bdb_cursor_move_and_skip_int(bdb_cursor_impl_t *cur,
                                        bdb_berkdb_t *berkdb, int how,
                                        int retrieved, int update_shadows,
                                        int *bdberr);

static inline int berkdb_get_genid_from_dtakey(bdb_cursor_impl_t *cur,
                                               void *dta, void *key,
                                               unsigned long long *genid,
                                               int *bdberr)
{
    void *val;
    if (cur->type == BDBC_DT)
        val = key;
    else
        val = dta;

#ifdef _SUN_SOURCE
    memcpy(genid, val, 8);
#else
    *genid = *(unsigned long long *)val;
#endif

    return 0;
}

static char *tellmehow(int how)
{
    switch (how) {
    case DB_NEXT:
        return "NEXT";
    case DB_PREV:
        return "PREV";
    case DB_FIRST:
        return "FIRST";
    case DB_LAST:
        return "LAST";
    default:
        return "UNKNOWN";
    }
}

#if MERGE_DEBUG
enum { BDB_SHOW_RL = 1, BDB_SHOW_SD = 2, BDB_SHOW_BOTH = 3 };

static void print_cursor_keys(bdb_cursor_impl_t *cur, int which)
{
    int bdberr;
    if (which & BDB_SHOW_RL && cur->rl) {
        logmsg(LOGMSG_USER, "ptr in RL: \n");
        char *loc_key_rl = NULL;
        int loc_keysize_rl = 0;
        cur->rl->key(cur->rl, &loc_key_rl, &bdberr);
        cur->rl->keysize(cur->rl, &loc_keysize_rl, &bdberr);

        hexdump(loc_key_rl, loc_keysize_rl);
        logmsg(LOGMSG_USER, "\n");
    }
    if (which & BDB_SHOW_SD && cur->sd) {
        logmsg(LOGMSG_USER, "ptr in SD: \n");
        char *loc_key_sd = NULL;
        int loc_keysize_sd = 0;
        cur->sd->key(cur->sd, &loc_key_sd, &bdberr);
        cur->sd->keysize(cur->sd, &loc_keysize_sd, &bdberr);

        hexdump(loc_key_sd, loc_keysize_sd);
        logmsg(LOGMSG_USER, "\n");
    }
}
#endif

static inline int calculate_discard_pages(bdb_cursor_impl_t *cur)
{
    bdb_state_type *bdb_state = cur->state;
    DB_ENV *dbenv;
    db_pgno_t numpages;
    u_int32_t pagesize;
    u_int32_t gb;
    u_int32_t bytes;
    u_int64_t tablesize = 0;
    u_int64_t bpoolsize;
    u_int32_t utilization;
    DB *db = NULL;
    int ii;

    /* Short circuit if we can consume the entire bufferpool. */
    if (bdb_state->attr->tablescan_cache_utilization == 100)
        return 0;

    /* Grab the dbenv. */
    dbenv = cur->state->dbenv;

    /* Calculate the buffer pool size. */
    dbenv->get_cachesize(dbenv, &gb, &bytes, NULL);
#define GIGABYTE (1024 * 1024 * 1024ULL) // for alex: 1024 ^ 3
    bpoolsize = gb * GIGABYTE + bytes;

    /* Calculate the table size. */
    for (ii = 0; ii < cur->state->attr->dtastripe; ii++) {
        db = bdb_state->dbp_data[0][ii];
        db->get_numpages(db, &numpages);
        db->get_pagesize(db, &pagesize);
        tablesize += (numpages * pagesize);
    }

    /* Calculate what the table will utilize. */
    utilization = (100 * tablesize) / bpoolsize;

    return (utilization > bdb_state->attr->tablescan_cache_utilization);
}

/* Make sure an expanding recordsize doesn't spill onto overflow pages. */
static inline int allow_pageorder_tablescan(bdb_cursor_impl_t *cur)
{
    bdb_state_type *bdb_state = cur->state;
    DB *db = bdb_state->dbp_data[0][0];
    u_int32_t pagesize;
    u_int32_t recordsize;

    /* Verify that it's enabled for this table */
    if (bdb_state->disable_page_order_tablescan)
        return 0;

    /* Disable for snapshot / serializable for now */
    if (cur->shadow_tran &&
        (cur->shadow_tran->tranclass == TRANCLASS_SNAPISOL ||
         cur->shadow_tran->tranclass == TRANCLASS_SERIALIZABLE))
        return 0;

    /* Short circuit if this check is disabled. */
    if (cur->state->attr->disable_pageorder_recsz_chk)
        return 1;

    /* Retrive the pagesize for this table. */
    db->get_pagesize(db, &pagesize);

    /* Subtract maximum possible header: base + cksum + crypto + align + fudge.
     */
    pagesize -= (26 + 20 + 16 + 2 + 8);

    /* Start with record size. */
    recordsize = bdb_state->lrl;

    /* Add space for the genid key. */
    recordsize += sizeof(unsigned long long);

    /* Add space for the index for each + fudge: the index is actually a short.
     */
    recordsize += (2 * sizeof(int));

    /* Allow pageorder if we can fit at least 4 records on a page. */
    return (pagesize / recordsize) >= 4;
}

/* Trace flag */
extern int gbl_skip_ratio_trace;

/* Print page-order skip trace */
static inline int pageorder_skip_trace(bdb_cursor_impl_t *cur)
{
    u_int64_t nextcount;
    u_int64_t skipcount;
    u_int64_t ratio = 0;
    static int lastpr = 0;
    int interval = 1;
    int now;
    int rc;

    /* Get time */
    now = time(NULL);

    /* Return if too early */
    if ((now - lastpr) < interval)
        return 0;

    /* Remember time */
    lastpr = now;

    /* Disabled globally */
    if (!cur->state->attr->page_order_tablescan) {
        logmsg(LOGMSG_USER, "%s: global page order tablescan is disabled\n",
                __func__);
        return 0;
    }

    /* Disabled for table */
    if (cur->state->disable_page_order_tablescan) {
        logmsg(LOGMSG_USER, "%s: page order tablescan is disabled for table '%s'.\n",
                __func__, cur->state->name);
        return 0;
    }

    /* Check cursor */
    if (!cur->rl) {
        logmsg(LOGMSG_USER, "%s: NULL cursor for table '%s'\n", __func__,
                cur->state->name);
        return 0;
    }

    /* Check max threshold */
    if (cur->state->attr->disable_pgorder_threshold >= 100) {
        logmsg(LOGMSG_USER, "%s: threshold is 100%% for table '%s'\n", __func__,
                cur->state->name);
        return 0;
    }

    /* Irrelavant skip-stats */
    if (rc = cur->rl->get_skip_stat(cur->rl, &nextcount, &skipcount)) {
        logmsg(LOGMSG_USER, "%s: get_skip_stat returned %d\n", __func__, rc);
        return 0;
    }

    /* Skip to next ratio */
    if (nextcount > 0)
        ratio = (100 * skipcount) / nextcount;

    /* Sanity */
    assert(ratio >= 0 && ratio <= 100);

    /* Not enough pages */
    if (nextcount < cur->state->attr->disable_pgorder_min_nexts) {
        logmsg(LOGMSG_USER, "%s: below next threshold for table '%s'\n", __func__,
                cur->state->name);
    }

    logmsg(LOGMSG_USER, "Table scan for table '%s' skipcount = %llu nextcount = %llu "
            "ratio = %llu (threshold = %d)\n",
            cur->state->name, skipcount, nextcount, ratio,
            cur->state->attr->disable_pgorder_threshold);

    return 0;
}

/* Analyze page-order skips at the end of a datafile traversal */
static inline int verify_pageorder_tablescan(bdb_cursor_impl_t *cur)
{
    u_int64_t nextcount;
    u_int64_t skipcount;
    u_int64_t ratio = 0;
    int disable = 0;
    int now;
    int rc;

    /* Print trace if enabled */
    if (gbl_skip_ratio_trace)
        pageorder_skip_trace(cur);

    /* Page-order tablescan already disabled */
    if (!cur->state->attr->page_order_tablescan)
        return 0;

    /* Return immediately if this is disabled */
    if (cur->state->disable_page_order_tablescan)
        return 0;

    /* Disable switch disabled */
    if (!cur->rl || cur->state->attr->disable_pgorder_threshold >= 100)
        return 0;

    /* Irrelavant skip-stats */
    if (rc = cur->rl->get_skip_stat(cur->rl, &nextcount, &skipcount))
        return 0;

    /* Skip to next ratio */
    if (nextcount > 0)
        ratio = (100 * skipcount) / nextcount;

    /* Sanity */
    assert(ratio >= 0 && ratio <= 100);

    /* Not enough pages */
    if (nextcount < cur->state->attr->disable_pgorder_min_nexts)
        return 0;

    /* Ratio of skips to nexts */
    if (ratio > cur->state->attr->disable_pgorder_threshold) {
        logmsg(LOGMSG_WARN, "Disable page-order tablescan for table %s skipcount = "
                "%llu nextcount = %llu ratio = %llu%% threshold = %d%%\n",
                cur->state->name, skipcount, nextcount, ratio,
                cur->state->attr->disable_pgorder_threshold);

        /* Disable pageorder tablescan */
        cur->state->disable_page_order_tablescan = 1;
        cur->pageorder = 0;
        cur->discardpages = 0;
        return 1;
    }

    return 0;
}

int bdb_set_check_shadows(tran_type *shadow_tran)
{
    shadow_tran->check_shadows = 1;
    return 0;
}

/**
 * ixnum == -1 of BDBC_DT, ixnum >= 0 for BDBC_IX
 *
 */
bdb_cursor_ifn_t *bdb_cursor_open(
    bdb_state_type *bdb_state, cursor_tran_t *curtran, tran_type *shadow_tran,
    int ixnum, enum bdb_open_type type, void *shadadd, int pageorder,
    int rowlocks, int *holding_pagelocks_flag,
    int (*pause_pagelock_cursors)(void *), void *pausearg,
    int (*count_cursors)(void *), void *countarg, int trak, int *bdberr)
{
    extern int gbl_force_old_cursors;
    bdb_cursor_ifn_t *pcur_ifn = NULL;
    bdb_cursor_impl_t *cur = NULL;
    int maxdta = 0;
    int maxkey = 0;
    int rc = 0;
    int dbnum = 0;

    /* this bdb state has to be a table, not an env */
    if (!bdb_state->parent) {
        logmsg(LOGMSG_ERROR, "%s: calling this for parent bdb_state\n", __func__);
        cheap_stack_trace();
        *bdberr = BDBERR_BADARGS;
        return NULL;
    }

    /* we expect to always be running with a curtran these days */
    if (!curtran || !bdb_get_lid_from_cursortran(curtran)) {
        logmsg(LOGMSG_ERROR, "%s: no curtran provided!!!\n", __func__);
        cheap_stack_trace();
        *bdberr = BDBERR_BADARGS;
        return NULL;
    }

    dbnum = get_dbnum_by_handle(bdb_state);
    if (dbnum == -1) {
        *bdberr = BDBERR_BADARGS;
        return NULL;
    }

    pcur_ifn = calloc(1, sizeof(bdb_cursor_ifn_t) + sizeof(bdb_cursor_impl_t));
    if (!pcur_ifn) {
        logmsg(LOGMSG_ERROR, "%s: malloc %d\n", __func__, sizeof(bdb_cursor_impl_t));
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }
    cur = (bdb_cursor_impl_t *)((char *)pcur_ifn + sizeof(bdb_cursor_ifn_t));

    /* set this to 0 to get repeatable reads in row lock mode */
    cur->state = bdb_state;
    cur->curtran = curtran;
    cur->shadow_tran = shadow_tran;
    cur->dbnum = dbnum;
    cur->used_rl = 1;
    cur->used_sd = 1;
    cur->pagelockflag = holding_pagelocks_flag;
    cur->laststripe = cur->lastpage = cur->lastindex = -1;

    rowlocks = cur->rowlocks = 0;

    cur->upd_shadows_count = 0;

    cur->trak = trak | ((shadow_tran) ? shadow_tran->trak : 0);

    if (cur->trak && shadow_tran) {
        logmsg(LOGMSG_USER, "Cur %p opened as tranclass %d startgenid %llx\n", cur,
                cur->shadow_tran->tranclass, cur->shadow_tran->startgenid);
    } else if (cur->trak && !shadow_tran) {
        logmsg(LOGMSG_USER, "Cur %p opened with no shadow tran\n", cur);
    }

    if (bdb_state->isopen == 0)
        return NULL;

    if (ixnum >= 0) {
        cur->idx = ixnum;
        cur->type = BDBC_IX;
        if (bdb_state->ixdta[ixnum]) {
            cur->datacopy =
                malloc(bdb_state->lrl + 2 * sizeof(unsigned long long));
            if (!cur->datacopy) {
                logmsg(LOGMSG_ERROR, "%s: malloc %d\n", __func__,
                        bdb_state->lrl + 2 * sizeof(unsigned long long));
                *bdberr = BDBERR_MALLOC;
                return NULL;
            }
        }
    } else {
        cur->idx = 0;
        cur->type = BDBC_DT;
    }
    /* aparently the limits must be the same for index and data
       (obviously they don't have to)
       this will fix the datacopy issue until we fix the length
       to more accurate values
     */
    maxdta = MAXRECSZ;
    maxkey = MAXKEYSZ;

    /* gear up the bdb cursor
       Do it here, before trying to open berkdb objects
       that will might try to use cur->ifn
     */
    pcur_ifn->impl = cur;
    pcur_ifn->first = bdb_cursor_first;
    pcur_ifn->next = bdb_cursor_next;
    pcur_ifn->prev = bdb_cursor_prev;
    pcur_ifn->last = bdb_cursor_last;
    pcur_ifn->find = bdb_cursor_find;
    pcur_ifn->find_last_dup = bdb_cursor_find_last_dup;

    pcur_ifn->insert = bdb_cursor_insert;
    pcur_ifn->delete = bdb_cursor_delete;
    pcur_ifn->data = bdb_cursor_data;
    pcur_ifn->datalen = bdb_cursor_datalen;
    pcur_ifn->genid = bdb_cursor_genid;
    pcur_ifn->rrn = bdb_cursor_rrn;
    pcur_ifn->dbnum = bdb_cursor_dbnum;
    pcur_ifn->datacopy = bdb_cursor_datacopy;
    pcur_ifn->ver = bdb_cursor_ver;
    pcur_ifn->get_found_data = bdb_cursor_found_data;
    pcur_ifn->collattr = bdb_cursor_collattr;
    pcur_ifn->collattrlen = bdb_cursor_collattrlen;

    pcur_ifn->unlock = bdb_cursor_unlock;
    pcur_ifn->lock = bdb_cursor_lock;
    pcur_ifn->set_curtran = bdb_cursor_set_curtran;
    pcur_ifn->getpageorder = bdb_cursor_getpageorder;

    pcur_ifn->updateshadows = bdb_cursor_update_shadows;
    pcur_ifn->updateshadows_pglogs = bdb_cursor_update_shadows_with_pglogs;

    pcur_ifn->getshadowtran = bdb_cursor_get_shadowtran;

    pcur_ifn->setnullblob = bdb_cursor_set_null_blob_in_shadows;
    pcur_ifn->pause = bdb_cursor_pause;
    pcur_ifn->pauseall = pause_pagelock_cursors;
    pcur_ifn->pausearg = pausearg;
    pcur_ifn->count = count_cursors;
    pcur_ifn->countarg = countarg;

    pcur_ifn->close = bdb_cursor_close;

    cur->ifn = pcur_ifn;

    /* Determine the page-order flags at the first open. */
    if (cur->type == BDBC_DT && cur->idx == 0 && pageorder &&
        allow_pageorder_tablescan(cur)) {
        cur->pageorder = 1;
        cur->discardpages = calculate_discard_pages(cur);
        if (cur->trak) {
            logmsg(LOGMSG_USER, "Cur %p opened in page-order mode.\n", cur);
        }
    }

    if (type == BDB_OPEN_REAL || type == BDB_OPEN_BOTH) {
        cur->rl = bdb_berkdb_open(cur, BERKDB_REAL, maxdta, maxkey, bdberr);
        if (!cur->rl) {
            logmsg(LOGMSG_ERROR, "%s: fail to create index berkdb\n", __func__);
            if (cur->datacopy)
                free(cur->datacopy);
            free(pcur_ifn);
            return NULL;
        }
        if (cur->trak) {
            logmsg(LOGMSG_USER, "Cur %p opened cur->rl in bdb_cursor_open\n", cur);
        }
    }

    /* we only open if there is already one (we'll open when we insert, if any)
     */
    if (type == BDB_OPEN_SHAD || type == BDB_OPEN_BOTH) {
        int openhow = BERKDB_SHAD;

        /* Always create cur->sd for non-page order snapisol or you can lose
         * data in cursor_move_merge. */
        if (cur->shadow_tran &&
            (cur->shadow_tran->tranclass == TRANCLASS_SNAPISOL ||
             cur->shadow_tran->tranclass == TRANCLASS_SERIALIZABLE))
            openhow = BERKDB_SHAD_CREATE;

        /* open the cursor now */
        cur->sd = bdb_berkdb_open(cur, openhow, maxdta, maxkey, bdberr);
        if (*bdberr) {
            logmsg(LOGMSG_ERROR, "%s: fail to create shadow index berkdb\n",
                    __func__);
            if (cur->rl) {
                int newbdberr;
                rc = cur->rl->close(cur->rl, &newbdberr);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "%s: bdb_berkdb_close failed %d %d\n",
                            __func__, rc, newbdberr);
                }
            }

            if (cur->datacopy)
                free(cur->datacopy);
            free(pcur_ifn);
            return NULL;
        }
        if (cur->trak) {
            logmsg(LOGMSG_USER, "Cur %p opened cur->sd in bdb_cursor_open\n", cur);
        }
    } else if (cur->trak) {
        logmsg(LOGMSG_USER, "Cur %p did not open cur->sd because type is %d\n", cur,
                type);
    }

    /* A cursor into the virtual stripe. */
    cur->addcur = shadadd;

    /* Each cursor needs it's own handle to traverse the virtual stripe. */

    /* Check if this requires odh logic. */
    if (cur->type == BDBC_DT) {
        /* Allocate the addcur odh area. */
        cur->addcur_odh = malloc(MAXRECSZ);

        /* Mark as an odh cursor. */
        cur->addcur_use_odh = 1;
    }

    /* Add this to the list of cursors under this txn. */
    if (cur->shadow_tran &&
        (cur->shadow_tran->tranclass == TRANCLASS_SNAPISOL ||
         cur->shadow_tran->tranclass == TRANCLASS_SERIALIZABLE)) {
        listc_abl(&cur->shadow_tran->open_cursors, pcur_ifn);
    }

    /* Start with a NULL skip-list. */
    cur->vs_stab = NULL;
    cur->vs_skip = NULL;
#if 0
   cur->cstripe = NULL; cur->cscur = NULL;
#endif

    return pcur_ifn;
}

int timestamp_lsn_keycmp(void *_, int key1len, const void *key1, int key2len,
                         const void *key2)
{
    int len;
    int rc;
    struct timestamp_lsn_key *pkey1;
    struct timestamp_lsn_key *pkey2;
    assert(key1len == key2len);
    pkey1 = (struct timestamp_lsn_key *)key1;
    pkey2 = (struct timestamp_lsn_key *)key2;
    len = key1len;

    if (pkey1->timestamp != pkey2->timestamp) {
        if (pkey1->timestamp < pkey2->timestamp) {
            return -1;
        } else {
            return 1;
        }
    } else if (pkey1->lsn.file != pkey2->lsn.file) {
        if (pkey1->lsn.file < pkey2->lsn.file) {
            return -1;
        } else {
            return 1;
        }
    } else {
        if (pkey1->lsn.offset < pkey2->lsn.offset) {
            return -1;
        } else if (pkey1->lsn.offset == pkey2->lsn.offset) {
            return 0;
        } else {
            return 1;
        }
    }

    return 0;
}

static pool_t *shadows_fileid_pglogs_queue_pool = NULL;
static pool_t *pglogs_queue_cursor_pool = NULL;
static pool_t *ltran_pglogs_key_pool = NULL;
static pool_t *shadows_asof_cursor_pool = NULL;
static pool_t *pglogs_commit_list_pool = NULL;
static pool_t *pglogs_queue_key_pool = NULL;
static pool_t *pglogs_key_pool = NULL;
static pool_t *pglogs_logical_key_pool = NULL;
static pool_t *pglogs_lsn_list_pool = NULL;
static pool_t *pglogs_lsn_commit_list_pool = NULL;
static pool_t *pglogs_relink_key_pool = NULL;
static pool_t *pglogs_relink_list_pool = NULL;

static LISTC_T(struct commit_list) pglogs_commit_list;
static pthread_mutex_t shadows_fileid_pglogs_queue_lk;
static pthread_mutex_t pglogs_queue_cursor_lk;
static pthread_mutex_t ltran_pglogs_key_lk;
static pthread_mutex_t shadows_asof_cursor_lk;
static pthread_mutex_t pglogs_commit_list_pool_lk;
static pthread_mutex_t pglogs_queue_key_pool_lk;
static pthread_mutex_t pglogs_key_pool_lk;
static pthread_mutex_t pglogs_logical_key_pool_lk;
static pthread_mutex_t pglogs_lsn_list_pool_lk;
static pthread_mutex_t pglogs_lsn_commit_list_pool_lk;
static pthread_mutex_t pglogs_relink_key_pool_lk;
static pthread_mutex_t pglogs_relink_list_pool_lk;
static pthread_mutex_t pglogs_queue_lk;
static hash_t *pglogs_fileid_hash;

static pthread_mutex_t del_queue_lk = PTHREAD_MUTEX_INITIALIZER;

static struct shadows_fileid_pglogs_queue *
allocate_shadows_fileid_pglogs_queue()
{
    struct shadows_fileid_pglogs_queue *q;
    Pthread_mutex_lock(&shadows_fileid_pglogs_queue_lk);
    q = pool_getablk(shadows_fileid_pglogs_queue_pool);
    Pthread_mutex_unlock(&shadows_fileid_pglogs_queue_lk);
#ifdef NEWSI_DEBUG_POOL
    q->pool = shadows_fileid_pglogs_queue_pool;
#endif
    return q;
}

void return_shadows_fileid_pglogs_queue(struct shadows_fileid_pglogs_queue *q)
{
    Pthread_mutex_lock(&shadows_fileid_pglogs_queue_lk);
#ifdef NEWSI_DEBUG_POOL
    assert(q->pool == shadows_fileid_pglogs_queue_pool);
#endif
    pool_relablk(shadows_fileid_pglogs_queue_pool, q);
    Pthread_mutex_unlock(&shadows_fileid_pglogs_queue_lk);
}

static struct pglogs_queue_cursor *allocate_pglogs_queue_cursor(void)
{
    struct pglogs_queue_cursor *c;
    Pthread_mutex_lock(&pglogs_queue_cursor_lk);
    c = pool_getablk(pglogs_queue_cursor_pool);
    Pthread_mutex_unlock(&pglogs_queue_cursor_lk);
#ifdef NEWSI_DEBUG_POOL
    c->pool = pglogs_queue_cursor_pool;
#endif
    return c;
}

void return_pglogs_queue_cursor(struct pglogs_queue_cursor *c)
{
    Pthread_mutex_lock(&pglogs_queue_cursor_lk);
#ifdef NEWSI_DEBUG_POOL
    assert(c->pool == pglogs_queue_cursor_pool);
#endif
    pool_relablk(pglogs_queue_cursor_pool, c);
    Pthread_mutex_unlock(&pglogs_queue_cursor_lk);
}

static struct ltran_pglogs_key *allocate_ltran_pglogs_key(void)
{
    struct ltran_pglogs_key *k;
    Pthread_mutex_lock(&ltran_pglogs_key_lk);
    k = pool_getablk(ltran_pglogs_key_pool);
    Pthread_mutex_unlock(&ltran_pglogs_key_lk);
#ifdef NEWSI_DEBUG_POOL
    k->pool = ltran_pglgos_key_pool;
#endif
    return k;
}

static void return_ltran_pglogs_key(struct ltran_pglogs_key *k)
{
    Pthread_mutex_lock(&ltran_pglogs_key_lk);
#ifdef NEWSI_DEBUG_POOL
    assert(k->pool == ltran_pglogs_key_pool);
#endif
    pool_relablk(ltran_pglogs_key_pool, k);
    Pthread_mutex_unlock(&ltran_pglogs_key_lk);
}

static struct shadows_asof_cursor *allocate_asof_cursor(void)
{
    struct shadows_asof_cursor *a;
    Pthread_mutex_lock(&shadows_asof_cursor_lk);
    a = pool_getablk(shadows_asof_cursor_pool);
    Pthread_mutex_unlock(&shadows_asof_cursor_lk);
#ifdef NEWSI_DEBUG_POOL
    a->pool = shadows_asof_cursor_pool;
#endif
    return a;
}

static void return_asof_cursor(struct shadows_asof_cursor *a)
{
    Pthread_mutex_lock(&shadows_asof_cursor_lk);
#ifdef NEWSI_DEBUG_POOL
    assert(a->pool == shadows_asof_cursor_pool);
#endif
    pool_relablk(shadows_asof_cursor_pool, a);
    Pthread_mutex_unlock(&shadows_asof_cursor_lk);
}

static struct commit_list *allocate_pglogs_commit_list(void)
{
    struct commit_list *q;
    Pthread_mutex_lock(&pglogs_commit_list_pool_lk);
    q = pool_getablk(pglogs_commit_list_pool);
    Pthread_mutex_unlock(&pglogs_commit_list_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    q->pool = pglogs_commit_list_pool;
#endif
    return q;
}

static void return_pglogs_commit_list(struct commit_list *c)
{
    Pthread_mutex_lock(&pglogs_commit_list_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    assert(r->pool == pglogs_commit_list_pool);
#endif
    pool_relablk(pglogs_commit_list_pool, c);
    Pthread_mutex_unlock(&pglogs_commit_list_pool_lk);
}

struct shadows_pglogs_queue_key *allocate_shadows_pglogs_queue_key(void)
{
    struct shadows_pglogs_queue_key *q;
    Pthread_mutex_lock(&pglogs_queue_key_pool_lk);
    q = pool_getablk(pglogs_queue_key_pool);
    Pthread_mutex_unlock(&pglogs_queue_key_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    q->pool = pglogs_queue_key_pool;
#endif
    return q;
}

void return_pglogs_queue_key(struct shadows_pglogs_queue_key *qk)
{
    Pthread_mutex_lock(&pglogs_queue_key_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    assert(r->pool == pglogs_queue_key_pool);
#endif
    pool_relablk(pglogs_queue_key_pool, qk);
    Pthread_mutex_unlock(&pglogs_queue_key_pool_lk);
}

struct shadows_pglogs_key *allocate_shadows_pglogs_key(void)
{
    struct shadows_pglogs_key *r;
    Pthread_mutex_lock(&pglogs_key_pool_lk);
    r = pool_getablk(pglogs_key_pool);
    Pthread_mutex_unlock(&pglogs_key_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    r->pool = pglogs_key_pool;
#endif
    return r;
}

struct shadows_pglogs_logical_key *allocate_shadows_pglogs_logical_key(void)
{
    struct shadows_pglogs_logical_key *r;
    Pthread_mutex_lock(&pglogs_logical_key_pool_lk);
    r = pool_getablk(pglogs_logical_key_pool);
    Pthread_mutex_unlock(&pglogs_logical_key_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    r->pool = pglogs_logical_key_pool;
#endif
    return r;
}

struct lsn_list *allocate_lsn_list(void)
{
    struct lsn_list *r;
    Pthread_mutex_lock(&pglogs_lsn_list_pool_lk);
    r = pool_getablk(pglogs_lsn_list_pool);
    Pthread_mutex_unlock(&pglogs_lsn_list_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    r->pool = pglogs_lsn_list_pool;
#endif
    return r;
}

void deallocate_lsn_list(struct lsn_list *r)
{
    Pthread_mutex_lock(&pglogs_lsn_list_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    assert(r->pool == pglogs_lsn_list_pool);
#endif
    pool_relablk(pglogs_lsn_list_pool, r);
    Pthread_mutex_unlock(&pglogs_lsn_list_pool_lk);
}

struct lsn_commit_list *allocate_lsn_commit_list(void)
{
    struct lsn_commit_list *r;
    Pthread_mutex_lock(&pglogs_lsn_commit_list_pool_lk);
    r = pool_getablk(pglogs_lsn_commit_list_pool);
    Pthread_mutex_unlock(&pglogs_lsn_commit_list_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    r->pool = pglogs_lsn_commit_list_pool;
#endif
    return r;
}

struct pglogs_relink_key *allocate_pglogs_relink_key(void)
{
    struct pglogs_relink_key *r;
    Pthread_mutex_lock(&pglogs_relink_key_pool_lk);
    r = pool_getablk(pglogs_relink_key_pool);
    Pthread_mutex_unlock(&pglogs_relink_key_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    r->pool = pglogs_relink_key_pool;
#endif
    return r;
}

struct relink_list *allocate_relink_list(void)
{
    struct relink_list *r;
    Pthread_mutex_lock(&pglogs_relink_list_pool_lk);
    r = pool_getablk(pglogs_relink_list_pool);
    Pthread_mutex_unlock(&pglogs_relink_list_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    r->pool = pglogs_relink_list_pool;
#endif
    return r;
}

void deallocate_relink_list(struct relink_list *r)
{
    Pthread_mutex_lock(&pglogs_relink_list_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    assert(r->pool == pglogs_relink_list_pool);
#endif
    pool_relablk(pglogs_relink_list_pool, r);
    Pthread_mutex_unlock(&pglogs_relink_list_pool_lk);
}

static int return_relinks(void *obj, void *arg)
{
    char *list = (char *)obj + DB_FILE_ID_LEN * sizeof(unsigned char) +
                 sizeof(db_pgno_t);
    void *ent;
    Pthread_mutex_lock(&pglogs_relink_list_pool_lk);
    while ((ent = listc_rtl(list)) != NULL) {
#ifdef NEWSI_DEBUG_POOL
        assert(((struct relink_list *)ent)->pool == pglogs_relink_list_pool);
#endif
        pool_relablk(pglogs_relink_list_pool, ent);
    }
    Pthread_mutex_unlock(&pglogs_relink_list_pool_lk);
    Pthread_mutex_lock(&pglogs_relink_key_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    assert(((struct pglogs_relink_key *)obj)->pool == pglogs_relink_key_pool);
#endif
    pool_relablk(pglogs_relink_key_pool, obj);
    Pthread_mutex_unlock(&pglogs_relink_key_pool_lk);
    return 0;
}

static int return_logical_txn_pglogs(void *obj, void *arg)
{
    char *list = (char *)obj + DB_FILE_ID_LEN * sizeof(unsigned char) +
                 sizeof(db_pgno_t);
    void *ent;
    Pthread_mutex_lock(&pglogs_lsn_commit_list_pool_lk);
    while ((ent = listc_rtl(list)) != NULL) {
#ifdef NEWSI_DEBUG_POOL
        assert(((struct lsn_commit_list *)ent)->pool ==
               pglogs_lsn_commit_list_pool);
#endif
        pool_relablk(pglogs_lsn_commit_list_pool, ent);
    }
    Pthread_mutex_unlock(&pglogs_lsn_commit_list_pool_lk);
    Pthread_mutex_lock(&pglogs_logical_key_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    assert(((struct shadows_pglogs_logical_key *)obj)->pool ==
           pglogs_logical_key_pool);
#endif
    pool_relablk(pglogs_logical_key_pool, obj);
    Pthread_mutex_unlock(&pglogs_logical_key_pool_lk);
    return 0;
}

static int return_pglogs_key(void *obj, void *arg)
{
    struct shadows_pglogs_key *r = (struct shadows_pglogs_key *)obj;
    Pthread_mutex_lock(&pglogs_key_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    assert(r->pool == pglogs_key_pool);
#endif
    pool_relablk(pglogs_key_pool, r);
    Pthread_mutex_unlock(&pglogs_key_pool_lk);
    return 0;
}

static int return_txn_pglogs(void *obj, void *arg)
{
    char *list = (char *)obj + DB_FILE_ID_LEN * sizeof(unsigned char) +
                 sizeof(db_pgno_t);
    void *ent;
    Pthread_mutex_lock(&pglogs_lsn_list_pool_lk);
    while ((ent = listc_rtl(list)) != NULL) {
#ifdef NEWSI_DEBUG_POOL
        assert(((struct lsn_list *)ent)->pool == pglogs_lsn_list_pool);
#endif
        pool_relablk(pglogs_lsn_list_pool, ent);
    }
    Pthread_mutex_unlock(&pglogs_lsn_list_pool_lk);
    Pthread_mutex_lock(&pglogs_key_pool_lk);
#ifdef NEWSI_DEBUG_POOL
    assert(((struct shadows_pglogs_key *)obj)->pool == pglogs_key_pool);
#endif
    pool_relablk(pglogs_key_pool, obj);
    Pthread_mutex_unlock(&pglogs_key_pool_lk);
    return 0;
}

void bdb_return_relinks(hash_t *hashtbl)
{
    hash_for(hashtbl, return_relinks, hashtbl);
    hash_clear(hashtbl);
    hash_free(hashtbl);
}

void bdb_return_logical_pglogs_hashtbl(hash_t *hashtbl)
{
    hash_for(hashtbl, return_logical_txn_pglogs, hashtbl);
    hash_clear(hashtbl);
    hash_free(hashtbl);
}

void bdb_return_txn_pglogs_hashtbl(hash_t *hashtbl)
{
    hash_for(hashtbl, return_txn_pglogs, hashtbl);
    hash_clear(hashtbl);
    hash_free(hashtbl);
}

void bdb_return_pglogs_key_hashtbl(hash_t *hashtbl)
{
    hash_for(hashtbl, return_pglogs_key, hashtbl);
    hash_clear(hashtbl);
    hash_free(hashtbl);
}

int bdb_gbl_pglogs_mem_init(bdb_state_type *bdb_state)
{
    //   int stepup = 4096;
    int stepup = 0;
    if (!gbl_new_snapisol)
        return 0;

    shadows_fileid_pglogs_queue_pool = pool_setalloc_init(
        sizeof(struct shadows_fileid_pglogs_queue), stepup, malloc, free);
    pglogs_queue_cursor_pool = pool_setalloc_init(
        sizeof(struct pglogs_queue_cursor), stepup, malloc, free);
    ltran_pglogs_key_pool = pool_setalloc_init(sizeof(struct ltran_pglogs_key),
                                               stepup, malloc, free);
    shadows_asof_cursor_pool = pool_setalloc_init(
        sizeof(struct shadows_asof_cursor), stepup, malloc, free);
    pglogs_commit_list_pool =
        pool_setalloc_init(sizeof(struct commit_list), stepup, malloc, free);
    pglogs_queue_key_pool = pool_setalloc_init(
        sizeof(struct shadows_pglogs_queue_key), stepup, malloc, free);
    pglogs_key_pool = pool_setalloc_init(sizeof(struct shadows_pglogs_key),
                                         stepup, malloc, free);
    pglogs_logical_key_pool = pool_setalloc_init(
        sizeof(struct shadows_pglogs_logical_key), stepup, malloc, free);
    pglogs_lsn_list_pool =
        pool_setalloc_init(sizeof(struct lsn_list), stepup, malloc, free);
    pglogs_lsn_commit_list_pool = pool_setalloc_init(
        sizeof(struct lsn_commit_list), stepup, malloc, free);
    pglogs_relink_key_pool = pool_setalloc_init(
        sizeof(struct pglogs_relink_key), stepup, malloc, free);
    pglogs_relink_list_pool =
        pool_setalloc_init(sizeof(struct relink_list), stepup, malloc, free);

    pthread_mutex_init(&shadows_fileid_pglogs_queue_lk, NULL);
    pthread_mutex_init(&pglogs_queue_cursor_lk, NULL);
    pthread_mutex_init(&ltran_pglogs_key_lk, NULL);
    pthread_mutex_init(&shadows_asof_cursor_lk, NULL);
    pthread_mutex_init(&pglogs_commit_list_pool_lk, NULL);
    pthread_mutex_init(&pglogs_queue_key_pool_lk, NULL);
    pthread_mutex_init(&pglogs_key_pool_lk, NULL);
    pthread_mutex_init(&pglogs_lsn_list_pool_lk, NULL);
    pthread_mutex_init(&pglogs_logical_key_pool_lk, NULL);
    pthread_mutex_init(&pglogs_lsn_commit_list_pool_lk, NULL);
    pthread_mutex_init(&pglogs_relink_key_pool_lk, NULL);
    pthread_mutex_init(&pglogs_relink_list_pool_lk, NULL);

    return 0;
}

int bdb_insert_pglogs_logical_int(hash_t *pglogs_hashtbl, unsigned char *fileid,
                                  db_pgno_t pgno, DB_LSN lsn,
                                  DB_LSN commit_lsn);

static int insert_ltran_pglog(bdb_state_type *bdb_state,
                              unsigned long long logical_tranid,
                              unsigned char *fileid, db_pgno_t pgno, DB_LSN lsn,
                              DB_LSN commitlsn)
{
    struct ltran_pglogs_key *ltran_ent = NULL, ltran_key;
    int bdberr = 0;
    int rc = 0;

    ltran_key.logical_tranid = logical_tranid;
    Pthread_mutex_lock(&bdb_gbl_ltran_pglogs_mutex);
    if ((ltran_ent = hash_find(bdb_gbl_ltran_pglogs_hash, &ltran_key)) ==
        NULL) {
        ltran_ent = allocate_ltran_pglogs_key();
        if (!ltran_ent) {
            Pthread_mutex_unlock(&bdb_gbl_ltran_pglogs_mutex);
            logmsg(LOGMSG_ERROR, "%s: fail malloc ltran_ent\n", __func__);
            return -1;
        }
        ltran_ent->logical_tranid = logical_tranid;
        ltran_ent->pglogs_hashtbl = hash_init_o(
            offsetof(struct shadows_pglogs_logical_key, fileid),
            DB_FILE_ID_LEN * sizeof(unsigned char) + sizeof(db_pgno_t));
        ltran_ent->relinks_hashtbl = hash_init_o(
            offsetof(struct pglogs_relink_key, fileid),
            DB_FILE_ID_LEN * sizeof(unsigned char) + sizeof(db_pgno_t));
        pthread_mutex_init(&ltran_ent->pglogs_mutex, NULL);
        ltran_ent->logical_commit_lsn.file = 0;
        ltran_ent->logical_commit_lsn.offset = 1;
        hash_add(bdb_gbl_ltran_pglogs_hash, ltran_ent);
    }

    Pthread_mutex_lock(&ltran_ent->pglogs_mutex);

    rc = bdb_insert_pglogs_logical_int(ltran_ent->pglogs_hashtbl, fileid, pgno,
                                       lsn, commitlsn);

    if (rc)
        abort();

    Pthread_mutex_unlock(&ltran_ent->pglogs_mutex);
    Pthread_mutex_unlock(&bdb_gbl_ltran_pglogs_mutex);
    return 0;
}

int bdb_relink_gbl_ltran_pglogs(void *bdb_state, unsigned char *fileid,
                                db_pgno_t pgno, db_pgno_t prev_pgno,
                                db_pgno_t next_pgno, DB_LSN lsn);

int bdb_update_logfile_pglogs_from_queue(
    void *bdb_state, unsigned char *fid,
    struct shadows_pglogs_queue_key *queuekey);

int bdb_relink_logfile_pglogs(void *bdb_state, unsigned char *fileid,
                              db_pgno_t pgno, db_pgno_t prev_pgno,
                              db_pgno_t next_pgno, DB_LSN lsn);

static int copy_queue_key_to_global(bdb_state_type *bdb_state,
                                    struct shadows_fileid_pglogs_queue *queue,
                                    struct shadows_pglogs_queue_key *key)
{
    int ret;
#ifdef ASOF_TRACE
    static unsigned long long count = 0;
    static int lastpr;
    int now;

    count++;
    if ((now = time(NULL)) - lastpr) {
        char *typestr;
        switch (key->type) {
        case PGLOGS_QUEUE_PAGE:
            typestr = "queue-page";
            break;
        case PGLOGS_QUEUE_RELINK:
            typestr = "queue-relink";
            break;
        default:
            typestr = "???";
            break;
        }
        logmsg(LOGMSG_USER, "%s: processing %s commit-lsn [%d][%d] count %llu\n", __func__,
               typestr, key->commit_lsn.file, key->commit_lsn.offset, count);
        lastpr = now;
    }
#endif

    switch (key->type) {
    case PGLOGS_QUEUE_PAGE:
        if (key->logical_tranid) {
            ret = insert_ltran_pglog(bdb_state, key->logical_tranid,
                                     queue->fileid, key->pgno, key->lsn,
                                     key->commit_lsn);
        } else {
            ret = bdb_update_logfile_pglogs_from_queue(bdb_state, queue->fileid,
                                                       key);
        }
        break;

    case PGLOGS_QUEUE_RELINK:
        if (key->logical_tranid) {
            ret = bdb_relink_gbl_ltran_pglogs(bdb_state, queue->fileid,
                                              key->pgno, key->prev_pgno,
                                              key->next_pgno, key->lsn);
        } else {
            ret = bdb_relink_logfile_pglogs(bdb_state, queue->fileid, key->pgno,
                                            key->prev_pgno, key->next_pgno,
                                            key->lsn);
        }
        break;

    default:
        abort();
        break;
    }

    if (ret)
        abort();

    return 0;
}

static int collect_queue_fileids(void *obj, void *arg)
{
    struct pglogs_queue_heads *qh = (struct pglogs_queue_heads *)arg;
    struct shadows_fileid_pglogs_queue *queue =
        (struct shadows_fileid_pglogs_queue *)obj;
    memcpy(qh->fileids[qh->index++], queue->fileid, DB_FILE_ID_LEN);
    return 0;
}

int bdb_oldest_outstanding_ltran(bdb_state_type *bdb_state, int *ltran_count,
                                 DB_LSN *oldest_ltran);

int bdb_pglogs_min_lsn(bdb_state_type *bdb_state, DB_LSN *outlsn)
{
    DB_LSN orig_lsn, lsn, txn_lsn = {0};
    int count;

    bdb_oldest_active_lsn(bdb_state, &lsn);
    orig_lsn = lsn;

    bdb_oldest_outstanding_ltran(bdb_state, &count, &txn_lsn);
    if (count > 0) {
        if (log_compare(&txn_lsn, &lsn) < 0)
            lsn = txn_lsn;
    }

    *outlsn = lsn;
    return 0;
}

static struct shadows_fileid_pglogs_queue *
retrieve_fileid_pglogs_queue(unsigned char *fileid, int create)
{
    unsigned char test_fileid[DB_FILE_ID_LEN] = {0};
    struct shadows_fileid_pglogs_queue *fileid_queue;

    pthread_mutex_lock(&pglogs_queue_lk);
    if (((fileid_queue = hash_find(pglogs_fileid_hash, fileid)) == NULL) &&
        create) {
        fileid_queue = allocate_shadows_fileid_pglogs_queue();
        fileid_queue->deleteme = 0;
        memcpy(fileid_queue->fileid, fileid, DB_FILE_ID_LEN);
        pthread_rwlock_init(&fileid_queue->queue_lk, NULL);
        listc_init(&fileid_queue->queue_keys,
                   offsetof(struct shadows_pglogs_queue_key, lnk));
        if (memcmp(fileid, test_fileid, DB_FILE_ID_LEN) == 0)
            abort();
        hash_add(pglogs_fileid_hash, fileid_queue);
    }

    pthread_mutex_unlock(&pglogs_queue_lk);
    return fileid_queue;
}

int bdb_clean_pglog_queue(bdb_state_type *bdb_state,
                          struct shadows_fileid_pglogs_queue *queue,
                          DB_LSN minlsn)
{
    struct shadows_pglogs_queue_key *qe, *prev_qe, *del_qe = NULL;

    // Grab this in write mode
    // Consumers will grab in read-mode until they anchor against the list by
    // finding an LSN that is greater than their start LSN.
    pthread_rwlock_wrlock(&queue->queue_lk);
    qe = LISTC_BOT(&queue->queue_keys);

    while (qe && (prev_qe = qe->lnk.prev)) {
        if (qe->type == PGLOGS_QUEUE_PAGE &&
            (log_compare(&qe->commit_lsn, &minlsn) < 0)) {
            del_qe = qe;
            break;
        }
        qe = prev_qe;
    }

    if (!del_qe) {
        pthread_rwlock_unlock(&queue->queue_lk);
        return 0;
    }

    /* Remove from the TOP of the list and return */
    do {
        qe = listc_rtl(&queue->queue_keys);
        return_pglogs_queue_key(qe);
    } while (qe != del_qe);

    pthread_rwlock_unlock(&queue->queue_lk);

    return 0;
}

int bdb_clean_pglogs_queues(bdb_state_type *bdb_state)
{
    struct pglogs_queue_heads qh;
    DB_LSN lsn;
    int count, i;

    pthread_mutex_lock(&del_queue_lk);
    bdb_pglogs_min_lsn(bdb_state, &lsn);

    pthread_mutex_lock(&pglogs_queue_lk);

    if (!pglogs_fileid_hash) {
        pthread_mutex_unlock(&pglogs_queue_lk);
        pthread_mutex_unlock(&del_queue_lk);
        return 0;
    }

    hash_info(pglogs_fileid_hash, NULL, NULL, NULL, NULL, &count, NULL, NULL);

    qh.fileids = malloc(count * sizeof(unsigned char *));
    for (i = 0; i < count; i++)
        qh.fileids[i] = malloc(sizeof(unsigned char) * DB_FILE_ID_LEN);

    qh.index = 0;

    hash_for(pglogs_fileid_hash, collect_queue_fileids, &qh);
    pthread_mutex_unlock(&pglogs_queue_lk);

    for (i = 0; i < count; i++) {
        struct shadows_fileid_pglogs_queue *queue; //= qh.queue_heads[i];
        unsigned char *fileid = qh.fileids[i];

        if (!(queue = retrieve_fileid_pglogs_queue(fileid, 0)))
            abort();

        bdb_clean_pglog_queue(bdb_state, queue, lsn);
        free(qh.fileids[i]);
    }

    free(qh.fileids);
    pthread_mutex_unlock(&del_queue_lk);
    return 0;
}

// Must be called holding the logfile_pglogs_repo_mutex
static struct logfile_pglogs_entry *
retrieve_logfile_pglogs(unsigned int filenum, int create)
{
    struct logfile_pglogs_entry *e;

    if ((e = hash_find(logfile_pglogs_repo, &filenum)) == NULL && create) {
        e = malloc(sizeof(struct logfile_pglogs_entry));
        e->filenum = filenum;
        e->pglogs_hashtbl = hash_init_o(
            offsetof(struct shadows_pglogs_logical_key, fileid),
            DB_FILE_ID_LEN * sizeof(unsigned char) + sizeof(db_pgno_t));
        e->relinks_hashtbl = hash_init_o(
            offsetof(struct pglogs_relink_key, fileid),
            DB_FILE_ID_LEN * sizeof(unsigned char) + sizeof(db_pgno_t));
        pthread_mutex_init(&e->pglogs_mutex, NULL);
        hash_add(logfile_pglogs_repo, e);

        if (!first_logfile)
            first_logfile = filenum;

        if (last_logfile < filenum)
            last_logfile = filenum;
    }
    return e;
}

void bdb_delete_logfile_pglogs(bdb_state_type *bdb_state, int filenum)
{
    int i;
    pthread_mutex_lock(&logfile_pglogs_repo_mutex);
    for (i = last_logfile; i <= filenum; i++) {
        struct logfile_pglogs_entry *e;
        if (e = hash_find(logfile_pglogs_repo, &i)) {
            hash_del(logfile_pglogs_repo, e);
            pthread_mutex_lock(&e->pglogs_mutex);
            bdb_return_logical_pglogs_hashtbl(e->pglogs_hashtbl);
            bdb_return_relinks(e->relinks_hashtbl);
            pthread_mutex_destroy(&e->pglogs_mutex);
            free(e);
        }
    }
    first_logfile = (filenum + 1);
    pthread_mutex_unlock(&logfile_pglogs_repo_mutex);
}

int transfer_ltran_pglogs_to_gbl(bdb_state_type *bdb_state,
                                 unsigned long long logical_tranid,
                                 DB_LSN logical_commit_lsn)
{
    int rc = 0;
    int bdberr = 0;
    int allocate = 0;
    struct ltran_pglogs_key *ltran_ent = NULL;
    struct logfile_pglogs_entry *l_entry;
    struct ltran_pglogs_key key;
    void *hash_cur;
    unsigned int hash_cur_buk;
    struct shadows_pglogs_logical_key *pglogs_ent = NULL;
    struct shadows_pglogs_logical_key *logfile_pglogs_ent = NULL;
    struct lsn_commit_list *lsn_ent = NULL;
    unsigned filenum;
#ifdef NEWSI_DEBUG
    struct lsn_commit_list *bot = NULL;
#endif
#ifdef NEWSI_STAT
    struct timeval before, after, diff;
    gettimeofday(&before, NULL);
#endif

    if (!bdb_gbl_ltran_pglogs_hash_ready)
        return 0;

    assert(logical_tranid);

    key.logical_tranid = logical_tranid;

    /* get the committed logical transaction */
    Pthread_mutex_lock(&bdb_gbl_ltran_pglogs_mutex);
    if ((ltran_ent = hash_find(bdb_gbl_ltran_pglogs_hash, &key)) != NULL) {
        hash_del(bdb_gbl_ltran_pglogs_hash, ltran_ent);
    }
    Pthread_mutex_unlock(&bdb_gbl_ltran_pglogs_mutex);

    if (ltran_ent == NULL)
        return 0;

    filenum = logical_commit_lsn.file;

    /* get the logfile we are going to transfer to */
    if (logfile_pglogs_repo_ready) {
        Pthread_mutex_lock(&logfile_pglogs_repo_mutex);
        l_entry = retrieve_logfile_pglogs(filenum, 1);
        Pthread_mutex_lock(&l_entry->pglogs_mutex);
        Pthread_mutex_unlock(&logfile_pglogs_repo_mutex);

        Pthread_mutex_lock(&ltran_ent->pglogs_mutex);

        /* for each recorded page */
        pglogs_ent =
            hash_first(ltran_ent->pglogs_hashtbl, &hash_cur, &hash_cur_buk);
        while (pglogs_ent) {
            /* get the same page in the global structure */
            if ((logfile_pglogs_ent =
                     hash_find(l_entry->pglogs_hashtbl, pglogs_ent)) == NULL) {
                /* add one if not exist */
                logfile_pglogs_ent = allocate_shadows_pglogs_logical_key();
                if (!logfile_pglogs_ent) {
                    Pthread_mutex_unlock(&ltran_ent->pglogs_mutex);
                    Pthread_mutex_unlock(&l_entry->pglogs_mutex);
                    logmsg(LOGMSG_ERROR, "%s: fail malloc logfile_pglogs_ent\n",
                            __func__);
                    return -1;
                }
                memcpy(logfile_pglogs_ent, pglogs_ent,
                       sizeof(struct shadows_pglogs_logical_key));
                listc_init(&logfile_pglogs_ent->lsns,
                           offsetof(struct lsn_commit_list, lnk));
                hash_add(l_entry->pglogs_hashtbl, logfile_pglogs_ent);
            }
            /* for each recorded lsn */
            while ((lsn_ent = listc_rtl(&pglogs_ent->lsns)) != NULL) {
                lsn_ent->commit_lsn = logical_commit_lsn;
#ifdef NEWSI_DEBUG
                bot = LISTC_BOT(&logfile_pglogs_ent->lsns);
                if (bot)
                    assert((log_compare(&bot->commit_lsn,
                                        &lsn_ent->commit_lsn) < 0) ||
                           (log_compare(&bot->commit_lsn,
                                        &lsn_ent->commit_lsn) == 0 &&
                            log_compare(&bot->lsn, &lsn_ent->lsn) <= 0));
#endif
                listc_abl(&logfile_pglogs_ent->lsns, lsn_ent);
            }

            pglogs_ent =
                hash_next(ltran_ent->pglogs_hashtbl, &hash_cur, &hash_cur_buk);
        }

        Pthread_mutex_unlock(&ltran_ent->pglogs_mutex);
        Pthread_mutex_unlock(&l_entry->pglogs_mutex);
    }

    pthread_mutex_destroy(&ltran_ent->pglogs_mutex);
    bdb_return_logical_pglogs_hashtbl(ltran_ent->pglogs_hashtbl);
    bdb_return_relinks(ltran_ent->relinks_hashtbl);
    return_ltran_pglogs_key(ltran_ent);

#ifdef NEWSI_STAT
    gettimeofday(&after, NULL);
    timeval_diff(&before, &after, &diff);
    Pthread_mutex_lock(&newsi_stat_mutex);
    timeval_add(&logfile_insert_time, &diff, &logfile_insert_time);
    Pthread_mutex_unlock(&newsi_stat_mutex);
#endif

    return 0;
}

static inline void set_del_lsn(const unsigned char *func, unsigned int line,
                               DB_LSN *old_del_lsn, DB_LSN *new_del_lsn)
{
    *old_del_lsn = *new_del_lsn;
}

static void *pglogs_asof_thread(void *arg)
{
    bdb_state_type *bdb_state = (bdb_state_type *)arg;
    static int fileid_max_count = 0;
    struct pglogs_queue_heads qh = {0};
    struct commit_list *lcommit, *bcommit, *next;
    int pollms, ret;

    while (1) {
        // Remove list
        int count, i, dont_poll = 0, drain_limit;
        DB_LSN new_asof_lsn, lsn, del_lsn = {0};
        // drain_limit = bdb_state->attr->asof_thread_drain_limit;

        // Get commit list
        pthread_mutex_lock(&bdb_asof_current_lsn_mutex);
        new_asof_lsn = bdb_asof_current_lsn;
        set_del_lsn(__func__, __LINE__, &del_lsn, &bdb_asof_current_lsn);
        assert(new_asof_lsn.file);
        lcommit = LISTC_TOP(&pglogs_commit_list);
        bcommit = LISTC_BOT(&pglogs_commit_list);
        pglogs_commit_list.top = pglogs_commit_list.bot = NULL;
        pglogs_commit_list.count = 0;
        pthread_mutex_unlock(&bdb_asof_current_lsn_mutex);

        bdb_pglogs_min_lsn(bdb_state, &lsn);
        if (log_compare(&lsn, &del_lsn) < 0)
            set_del_lsn(__func__, __LINE__, &del_lsn, &lsn);

        pthread_mutex_lock(&pglogs_queue_lk);
        hash_info(pglogs_fileid_hash, NULL, NULL, NULL, NULL, &count, NULL,
                  NULL);

        // Realloc if I need to
        if (count > fileid_max_count) {
            unsigned char **fids;
            int j;
            qh.fileids = realloc(qh.fileids, count * sizeof(unsigned char *));
            for (j = fileid_max_count; j < count; j++)
                qh.fileids[j] = malloc(sizeof(unsigned char) * DB_FILE_ID_LEN);
            fileid_max_count = count;
        }

        qh.index = 0;

        // Collect the fileids
        hash_for(pglogs_fileid_hash, collect_queue_fileids, &qh);
        pthread_mutex_unlock(&pglogs_queue_lk);

        for (i = 0; i < qh.index; i++) {
            struct shadows_fileid_pglogs_queue *queue;
            unsigned char *fileid = qh.fileids[i];
            struct shadows_pglogs_queue_key *prev, *top, *current, *last;
            struct shadows_asof_cursor *cur;

            int do_current = 0;

            if (!(queue = retrieve_fileid_pglogs_queue(fileid, 0)))
                abort();

            if ((cur = hash_find(bdb_asof_ltran_hash, fileid)) == NULL) {
                cur = allocate_asof_cursor();
                memcpy(cur->fileid, fileid, DB_FILE_ID_LEN);
                cur->cur = NULL;
                hash_add(bdb_asof_ltran_hash, cur);
            }

            pthread_rwlock_rdlock(&queue->queue_lk);
            last = LISTC_BOT(&queue->queue_keys);
            top = LISTC_TOP(&queue->queue_keys);

            if ((current = cur->cur) == NULL) {
                current = top;
                do_current = 1;
            }

            pthread_rwlock_unlock(&queue->queue_lk);

            // Commits are queued in log_put_int_int while holding the log
            // region
            // lock.  transfer_pglogs_to_queues happens later.  We could remove
            // this
            // code if the order were switched.
            if (last) {
                struct shadows_pglogs_queue_key *fnd = last;

                while (fnd && fnd->type != PGLOGS_QUEUE_PAGE)
                    fnd = fnd->lnk.prev;

                if (fnd && log_compare(&fnd->commit_lsn, &del_lsn) < 0) {
                    assert(fnd->commit_lsn.file);
                    set_del_lsn(__func__, __LINE__, &del_lsn, &fnd->commit_lsn);
                }
            }

            if (do_current && current) {
                copy_queue_key_to_global(bdb_state, queue, current);
            }

            count = 0;
            while (current != last) {
                prev = current;
                current = current->lnk.next;

                count++;

                copy_queue_key_to_global(bdb_state, queue, current);
            }

            // Don't poll if there's more to drain
            if (last != LISTC_BOT(&queue->queue_keys))
                dont_poll = 1;

            cur->cur = current;
        }

        // Loop again to delete
        for (i = 0; i < qh.index; i++) {
            struct shadows_fileid_pglogs_queue *queue;
            struct shadows_asof_cursor *cur;
            unsigned char *fileid = qh.fileids[i];

            if (!(queue = retrieve_fileid_pglogs_queue(fileid, 0)))
                abort();

            if (queue->deleteme &&
                (cur = hash_find(bdb_asof_ltran_hash, fileid)) &&
                (cur->cur == LISTC_BOT(&queue->queue_keys))) {
                struct shadows_pglogs_queue_key *qe;
                pthread_mutex_lock(&pglogs_queue_lk);
                hash_del(pglogs_fileid_hash, queue);
                pthread_mutex_unlock(&pglogs_queue_lk);
                while (qe = listc_rtl(&queue->queue_keys))
                    return_pglogs_queue_key(qe);
                return_shadows_fileid_pglogs_queue(queue);
                assert(cur);
                hash_del(bdb_asof_ltran_hash, cur);
                return_asof_cursor(cur);
            } else
                bdb_clean_pglog_queue(bdb_state, queue, del_lsn);
        }

        // This should be the largest commit-lsn
        if (bcommit) {
            new_asof_lsn = bcommit->commit_lsn;
            assert(log_compare(&new_asof_lsn, &bdb_asof_current_lsn) >= 0);
        }

        while (lcommit) {
            assert(log_compare(&new_asof_lsn, &lcommit->commit_lsn) >= 0);

            if (lcommit->logical_tranid) {
                ret = transfer_ltran_pglogs_to_gbl(
                    bdb_state, lcommit->logical_tranid, lcommit->commit_lsn);
                if (ret)
                    abort();
            }
            next = lcommit->lnk.next;
            return_pglogs_commit_list(lcommit);
            lcommit = next;
        }

        // Update the global asof lsn
        pthread_mutex_lock(&bdb_asof_current_lsn_mutex);
        assert(log_compare(&new_asof_lsn, &bdb_asof_current_lsn) >= 0);
        bdb_asof_current_lsn = new_asof_lsn;
        assert(bdb_asof_current_lsn.file);
        pthread_cond_broadcast(&bdb_asof_current_lsn_cond);
        pthread_mutex_unlock(&bdb_asof_current_lsn_mutex);

#ifdef ASOF_TRACE
        static int lastpr = 0;
        int now;

        if ((now = time(NULL)) - lastpr) {
           logmsg(LOGMSG_INFO, "%s setting asof lsn to [%d][%d]\n", __func__,
                   new_asof_lsn.file, new_asof_lsn.offset);
            lastpr = now;
        }
#endif

        if (!dont_poll) {
            pollms = bdb_state->attr->asof_thread_poll_interval_ms <= 0
                         ? 500
                         : bdb_state->attr->asof_thread_poll_interval_ms;
            poll(NULL, 0, pollms);
        }
    }

    return NULL;
}

int bdb_gbl_pglogs_init(bdb_state_type *bdb_state)
{
    int rc, i, bdberr;
    pthread_t thread_id;
    pthread_attr_t thd_attr;

    if (gbl_new_snapisol_asof) {
        rc = bdb_checkpoint_list_init();
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to init checkpoint list\n", __func__);
            return rc;
        }

        pthread_mutex_init(&bdb_gbl_recoverable_lsn_mutex, NULL);

        logfile_pglogs_repo = hash_init_o(
            offsetof(struct logfile_pglogs_entry, filenum), sizeof(int));

        if (!logfile_pglogs_repo) {
            logmsg(LOGMSG_ERROR, "%s: failed to init logfile_pglogs_repo\n",
                    __func__);
            return ENOMEM;
        }

        pthread_mutex_init(&logfile_pglogs_repo_mutex, NULL);
        first_logfile = last_logfile = 0;

        if (gbl_newsi_use_timestamp_table) {
            bdb_gbl_timestamp_lsn = bdb_temp_table_create(bdb_state, &bdberr);
            if (bdb_gbl_timestamp_lsn == NULL) {
                logmsg(LOGMSG_ERROR, "%s: failed to init bdb_gbl_timestamp_lsn\n",
                        __func__);
                return -1;
            }
            bdb_gbl_timestamp_lsn_cur = bdb_temp_table_cursor(
                bdb_state, bdb_gbl_timestamp_lsn, NULL, &bdberr);
            if (bdb_gbl_timestamp_lsn == NULL) {
                logmsg(LOGMSG_ERROR, 
                        "%s: failed to init cursor for bdb_gbl_timestamp_lsn\n",
                        __func__);
                return -1;
            }
            bdb_temp_table_set_cmp_func(bdb_gbl_timestamp_lsn,
                                        timestamp_lsn_keycmp);
            bdb_gbl_timestamp_lsn_ready = 1;
        }

        logfile_pglogs_repo_ready = 1;

        bdb_asof_ltran_hash =
            hash_init_o(offsetof(struct shadows_asof_cursor, fileid),
                        DB_FILE_ID_LEN * sizeof(unsigned char));
        pthread_mutex_init(&bdb_asof_current_lsn_mutex, NULL);
        pthread_cond_init(&bdb_asof_current_lsn_cond, NULL);
        listc_init(&pglogs_commit_list, offsetof(struct commit_list, lnk));
    }

#ifdef NEWSI_STAT
    bzero(&logfile_relink_time, sizeof(struct timeval));
    bzero(&logfile_insert_time, sizeof(struct timeval));
    bzero(&client_relink_time, sizeof(struct timeval));
    bzero(&client_insert_time, sizeof(struct timeval));
    bzero(&ltran_relink_time, sizeof(struct timeval));
    bzero(&ltran_insert_time, sizeof(struct timeval));
    bzero(&txn_insert_time, sizeof(struct timeval));
    bzero(&txn_relink_time, sizeof(struct timeval));
    bzero(&logical_undo_time, sizeof(struct timeval));
    pthread_mutex_init(&newsi_stat_mutex, NULL);
#endif

    /* Init pglogs queue */
    pthread_mutex_init(&pglogs_queue_lk, NULL);
    pglogs_fileid_hash =
        hash_init_o(offsetof(struct shadows_fileid_pglogs_queue, fileid),
                    sizeof(((struct shadows_fileid_pglogs_queue *)0)->fileid));

    bdb_gbl_ltran_pglogs_hash =
        hash_init_o(offsetof(struct ltran_pglogs_key, logical_tranid),
                    sizeof(unsigned long long));
    pthread_mutex_init(&bdb_gbl_ltran_pglogs_mutex, NULL);

    bdb_gbl_ltran_pglogs_hash_ready = 1;

    if (!gbl_new_snapisol_asof) {
        bdb_gbl_ltran_pglogs_hash_processed = 1;
    }
    else {
        pthread_attr_init(&thd_attr);
        pthread_attr_setstacksize(&thd_attr, 4 * 1024); /* 4K */
        pthread_attr_setdetachstate(&thd_attr, PTHREAD_CREATE_DETACHED);

        rc = __recover_logfile_pglogs(bdb_state->dbenv);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to bkfill gbl pglogs, new begin-as-of "
                            "snapshot might not work\n",
                    __func__);
            Pthread_mutex_lock(&bdb_gbl_recoverable_lsn_mutex);
            bdb_get_current_lsn(bdb_state, &(bdb_gbl_recoverable_lsn.file),
                                &(bdb_gbl_recoverable_lsn.offset));
            bdb_gbl_recoverable_timestamp = (int32_t)time(NULL);
            Pthread_mutex_unlock(&bdb_gbl_recoverable_lsn_mutex);
            logmsg(LOGMSG_ERROR, "set gbl_recoverable_lsn as [%d][%d]\n",
                   bdb_gbl_recoverable_lsn.file,
                   bdb_gbl_recoverable_lsn.offset);
        }
        bdb_get_current_lsn(bdb_state, &bdb_asof_current_lsn.file,
                            &bdb_asof_current_lsn.offset);
        bdb_gbl_ltran_pglogs_hash_processed = 1;

        rc = pthread_create(&thread_id, &thd_attr, pglogs_asof_thread,
                            (void *)bdb_state);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "pglogs_asof_thread pthread_create error");
            exit(1);
        }
    }

    return 0;
}

int bdb_txn_pglogs_init(void *bdb_state, void **pglogs_hashtbl,
                        void **relinks_hashtbl, pthread_mutex_t *mutexp)
{
    if (!gbl_new_snapisol)
        return 0;

    *pglogs_hashtbl =
        hash_init_o(offsetof(struct shadows_pglogs_key, fileid),
                    DB_FILE_ID_LEN * sizeof(unsigned char) + sizeof(db_pgno_t));
    if (*pglogs_hashtbl == NULL)
        return ENOMEM;

    *relinks_hashtbl =
        hash_init_o(offsetof(struct pglogs_relink_key, fileid),
                    DB_FILE_ID_LEN * sizeof(unsigned char) + sizeof(db_pgno_t));
    if (*relinks_hashtbl == NULL)
        return ENOMEM;

    pthread_mutex_init(mutexp, NULL);

    return 0;
}

int bdb_txn_pglogs_close(void *instate, void **pglogs_hashtbl,
                         void **relinks_hashtbl, pthread_mutex_t *mutexp)
{
    bdb_state_type *bdb_state = (bdb_state_type *)instate;
    if (!gbl_new_snapisol)
        return 0;

    bdb_return_txn_pglogs_hashtbl(*pglogs_hashtbl);
    bdb_return_relinks(*relinks_hashtbl);
    pthread_mutex_destroy(mutexp);

    return 0;
}

int bdb_insert_pglogs_int(hash_t *pglogs_hashtbl, unsigned char *fileid,
                          db_pgno_t pgno, DB_LSN lsn)
{
    int rc = 0;
    struct shadows_pglogs_key key;
    struct shadows_pglogs_key *pglogs_ent = NULL;
    struct lsn_list *lsnent = NULL;
#ifdef NEWSI_DEBUG
    struct lsn_list *bot = NULL;
#endif

    if (pgno == 0)
        return 0;

    memcpy(key.fileid, fileid, DB_FILE_ID_LEN);
    key.pgno = pgno;

    /* find the page in the hash */
    if ((pglogs_ent = hash_find(pglogs_hashtbl, &key)) == NULL) {
        pglogs_ent = allocate_shadows_pglogs_key();
        if (!pglogs_ent)
            return ENOMEM;
        memcpy(pglogs_ent->fileid, fileid, DB_FILE_ID_LEN);
        pglogs_ent->pgno = pgno;
        listc_init(&pglogs_ent->lsns, offsetof(struct lsn_list, lnk));
        hash_add(pglogs_hashtbl, pglogs_ent);
    }

    lsnent = allocate_lsn_list();
    if (!lsnent)
        abort();
    lsnent->lsn = lsn;
#ifdef NEWSI_DEBUG
    bot = LISTC_BOT(&pglogs_ent->lsns);
    if (bot)
        assert(log_compare(&bot->lsn, &lsnent->lsn) <= 0);
#endif
    listc_abl(&pglogs_ent->lsns, lsnent);

    return 0;
}

int bdb_insert_pglogs_logical_int(hash_t *pglogs_hashtbl, unsigned char *fileid,
                                  db_pgno_t pgno, DB_LSN lsn, DB_LSN commit_lsn)
{
    int rc = 0;
    struct shadows_pglogs_logical_key key;
    struct shadows_pglogs_logical_key *pglogs_ent = NULL;
    struct lsn_commit_list *lsnent = NULL;
#ifdef NEWSI_DEBUG
    struct lsn_commit_list *bot = NULL;
#endif

    if (pgno == 0)
        return 0;

    memcpy(key.fileid, fileid, DB_FILE_ID_LEN);
    key.pgno = pgno;

    /* find the page in the hash */
    if ((pglogs_ent = hash_find(pglogs_hashtbl, &key)) == NULL) {
        pglogs_ent = allocate_shadows_pglogs_logical_key();
        if (!pglogs_ent)
            return ENOMEM;
        memcpy(pglogs_ent->fileid, fileid, DB_FILE_ID_LEN);
        pglogs_ent->pgno = pgno;
        listc_init(&pglogs_ent->lsns, offsetof(struct lsn_commit_list, lnk));
        hash_add(pglogs_hashtbl, pglogs_ent);
    }

    lsnent = allocate_lsn_commit_list();
    if (!lsnent)
        abort();
    lsnent->lsn = lsn;
    lsnent->commit_lsn = commit_lsn;
#ifdef NEWSI_DEBUG
    bot = LISTC_BOT(&pglogs_ent->lsns);
    if (bot)
        assert((log_compare(&bot->commit_lsn, &lsnent->commit_lsn) < 0) ||
               (log_compare(&bot->commit_lsn, &lsnent->commit_lsn) == 0 &&
                log_compare(&bot->lsn, &lsnent->lsn) <= 0));
#endif
    listc_abl(&pglogs_ent->lsns, lsnent);
    /* printf("%s: added lsn [%u][%u] addr %p to hash %p, ent %p list %p\n",
       __func__,
       lsn.file, lsn.offset, lsnent, pglogs_hashtbl, pglogs_ent,
       &pglogs_ent->lsns); */

    return 0;
}

int bdb_insert_relinks_int(hash_t *relinks_hashtbl, unsigned char *fileid,
                           db_pgno_t pgno, db_pgno_t prev_pgno,
                           db_pgno_t next_pgno, DB_LSN lsn)
{
    int rc = 0;
    struct relink_list *rlent;
    struct pglogs_relink_key *relinks_ent;
    struct pglogs_relink_key key;
    memcpy(key.fileid, fileid, DB_FILE_ID_LEN);
#ifdef NEWSI_DEBUG
    struct relink_list *bot = NULL;
#endif

    if (pgno == 0)
        return 0;

    memcpy(key.fileid, fileid, DB_FILE_ID_LEN);

    if (prev_pgno) {
        key.pgno = prev_pgno;
        /* get the page */
        if ((relinks_ent = hash_find(relinks_hashtbl, &key)) == NULL) {
            relinks_ent = allocate_pglogs_relink_key();
            if (!relinks_ent) {
                logmsg(LOGMSG_ERROR, "%s: fail malloc relinks_ent\n", __func__);
                return ENOMEM;
            }
            memcpy(relinks_ent->fileid, fileid, DB_FILE_ID_LEN);
            relinks_ent->pgno = prev_pgno;
            listc_init(&relinks_ent->relinks,
                       offsetof(struct relink_list, lnk));
            hash_add(relinks_hashtbl, relinks_ent);
        }
        /* add this relink to the page */
        rlent = allocate_relink_list();
        if (!rlent)
            abort();
        rlent->inh = pgno;
        rlent->lsn = lsn;
#ifdef NEWSI_DEBUG
        bot = LISTC_BOT(&relinks_ent->relinks);
        if (bot)
            assert(log_compare(&bot->lsn, &rlent->lsn) <= 0);
#endif
        listc_abl(&relinks_ent->relinks, rlent);
    }

    if (next_pgno) {
        key.pgno = next_pgno;
        /* get the page */
        if ((relinks_ent = hash_find(relinks_hashtbl, &key)) == NULL) {
            relinks_ent = allocate_pglogs_relink_key();
            if (!relinks_ent) {
                logmsg(LOGMSG_ERROR, "%s: fail malloc relinks_ent\n", __func__);
                return ENOMEM;
            }
            memcpy(relinks_ent->fileid, fileid, DB_FILE_ID_LEN);
            relinks_ent->pgno = next_pgno;
            listc_init(&relinks_ent->relinks,
                       offsetof(struct relink_list, lnk));
            hash_add(relinks_hashtbl, relinks_ent);
        }
        /* add this relink to the page */
        rlent = allocate_relink_list();
        if (!rlent)
            abort();
        rlent->inh = pgno;
        rlent->lsn = lsn;
#ifdef NEWSI_DEBUG
        bot = LISTC_BOT(&relinks_ent->relinks);
        if (bot)
            assert(log_compare(&bot->lsn, &rlent->lsn) <= 0);
#endif
        listc_abl(&relinks_ent->relinks, rlent);
    }

    return 0;
}

int bdb_shadows_pglogs_key_list_init(void **listp, int n)
{
    static struct page_logical_lsn_key *key = NULL;
    static int nelements = 0;

    assert(listp);

    if (n == 0) {
        *listp = NULL;
        return 0;
    }

    if (n <= nelements) {
        (*listp) = key;
        return 0;
    }

    (*listp) = malloc(n * sizeof(struct page_logical_lsn_key));

    if (*listp == NULL) {
        logmsg(LOGMSG_ERROR, 
                "%s:%d failed to malloc for page_logical_lsn_key list\n",
                __func__, __LINE__);
        abort();
    }

    if (key)
        free(key);
    key = (*listp);
    nelements = n;

    return 0;
}

int bdb_update_add_pglogs_key_list(int i, void **listp, db_pgno_t pgno,
                                   unsigned char *fileid, DB_LSN lsn,
                                   DB_LSN commit_lsn)
{
    struct page_logical_lsn_key *keylist =
        *(struct page_logical_lsn_key **)listp;
    keylist[i].pgno = pgno;
    memcpy(keylist[i].fileid, fileid, DB_FILE_ID_LEN);
    keylist[i].commit_lsn = commit_lsn;
    keylist[i].lsn = lsn;
    return 0;
}

int bdb_update_ltran_pglogs_hash(void *bdb_state, void *pglogs,
                                 unsigned int nkeys,
                                 unsigned long long logical_tranid,
                                 int is_logical_commit,
                                 DB_LSN logical_commit_lsn)
{
    int rc = 0;
    int bdberr = 0;
    struct ltran_pglogs_key *ltran_ent = NULL;
    struct ltran_pglogs_key ltran_key;
    struct shadows_pglogs_logical_key *pglogs_ent;
    struct lsn_commit_list *lsnent;
    int i;
    struct page_logical_lsn_key *keylist =
        (struct page_logical_lsn_key *)pglogs;

#ifdef NEWSI_STAT
    struct timeval before, after, diff;
    gettimeofday(&before, NULL);
#endif

    if (!bdb_gbl_ltran_pglogs_hash_ready || (!is_logical_commit && !nkeys))
        return 0;

    assert(logical_tranid);

    ltran_key.logical_tranid = logical_tranid;

    Pthread_mutex_lock(&bdb_gbl_ltran_pglogs_mutex);

    if ((ltran_ent = hash_find(bdb_gbl_ltran_pglogs_hash, &ltran_key)) ==
        NULL) {
        if (!nkeys) {
            Pthread_mutex_unlock(&bdb_gbl_ltran_pglogs_mutex);
            return 0;
        }
        ltran_ent = allocate_ltran_pglogs_key();
        if (!ltran_ent) {
            Pthread_mutex_unlock(&bdb_gbl_ltran_pglogs_mutex);
            logmsg(LOGMSG_ERROR, "%s: fail malloc ltran_ent\n", __func__);
            return -1;
        }
        ltran_ent->logical_tranid = logical_tranid;
        ltran_ent->pglogs_hashtbl = hash_init_o(
            offsetof(struct shadows_pglogs_logical_key, fileid),
            DB_FILE_ID_LEN * sizeof(unsigned char) + sizeof(db_pgno_t));
        ltran_ent->relinks_hashtbl = hash_init_o(
            offsetof(struct pglogs_relink_key, fileid),
            DB_FILE_ID_LEN * sizeof(unsigned char) + sizeof(db_pgno_t));
        pthread_mutex_init(&ltran_ent->pglogs_mutex, NULL);
        ltran_ent->logical_commit_lsn.file = 0;
        ltran_ent->logical_commit_lsn.offset = 1;
        hash_add(bdb_gbl_ltran_pglogs_hash, ltran_ent);
    }

    if (is_logical_commit) {
        ltran_ent->logical_commit_lsn = logical_commit_lsn;
    }

    Pthread_mutex_lock(&ltran_ent->pglogs_mutex);
    /* for each recorded page-lsn pair */
    /* keylist was generated by master in reverse lsn order, so process it in
     * reverse order*/
    for (i = nkeys - 1; i >= 0; i--) {
        rc = bdb_insert_pglogs_logical_int(
            ltran_ent->pglogs_hashtbl, keylist[i].fileid, keylist[i].pgno,
            keylist[i].lsn, keylist[i].commit_lsn);
        if (rc)
            abort();
        /* fprintf(stderr, "%s: Inserted logical lsn [%d][%d] commit lsn
           [%d][%d] to active logical tranid %llx\n",
              __func__, keylist[i].lsn.file, keylist[i].lsn.offset,
              keylist[i].commit_lsn.file, keylist[i].commit_lsn.offset,
           logical_tranid); */
    }
    Pthread_mutex_unlock(&ltran_ent->pglogs_mutex);

    Pthread_mutex_unlock(&bdb_gbl_ltran_pglogs_mutex);

#ifdef NEWSI_STAT
    gettimeofday(&after, NULL);
    timeval_diff(&before, &after, &diff);
    Pthread_mutex_lock(&newsi_stat_mutex);
    timeval_add(&ltran_insert_time, &diff, &ltran_insert_time);
    Pthread_mutex_unlock(&newsi_stat_mutex);
#endif

    return 0;
}

#ifdef NEWSI_STAT
void bdb_print_logfile_pglogs_stat()
{
    logmsg(LOGMSG_USER, "logfile_insert_time: %.3fms\n",
           (double)logfile_insert_time.tv_sec * 1000 +
               (double)logfile_insert_time.tv_usec / 1000);
    logmsg(LOGMSG_USER, "logfile_relink_time: %.3fms\n",
           (double)logfile_relink_time.tv_sec * 1000 +
               (double)logfile_relink_time.tv_usec / 1000);
    logmsg(LOGMSG_USER, "client_insert_time: %.3fms\n",
           (double)client_insert_time.tv_sec * 1000 +
               (double)client_insert_time.tv_usec / 1000);
    logmsg(LOGMSG_USER, "client_relink_time: %.3fms\n",
           (double)client_relink_time.tv_sec * 1000 +
               (double)client_relink_time.tv_usec / 1000);
    logmsg(LOGMSG_USER, "ltran_insert_time: %.3fms\n",
           (double)ltran_insert_time.tv_sec * 1000 +
               (double)ltran_insert_time.tv_usec / 1000);
    logmsg(LOGMSG_USER, "ltran_relink_time %.3fms\n",
           (double)ltran_relink_time.tv_sec * 1000 +
               (double)ltran_relink_time.tv_usec / 1000);
    logmsg(LOGMSG_USER, "txn_insert_time: %.3fms\n",
           (double)txn_insert_time.tv_sec * 1000 +
               (double)txn_insert_time.tv_usec / 1000);
    logmsg(LOGMSG_USER, "txn_relink_time %.3fms\n",
           (double)txn_relink_time.tv_sec * 1000 +
               (double)txn_relink_time.tv_usec / 1000);
    logmsg(LOGMSG_USER, "logical_undo_time %.3fms\n",
           (double)logical_undo_time.tv_sec * 1000 +
               (double)logical_undo_time.tv_usec / 1000);
}
#endif

int bdb_update_logfile_pglogs_from_queue(
    void *bdb_state, unsigned char *fid,
    struct shadows_pglogs_queue_key *queuekey)
{
    int rc = 0;
    int count = 0;
    int bdberr;
    int i, allocate = 0;
    unsigned filenum;
    struct logfile_pglogs_entry *l_entry;
    struct shadows_pglogs_logical_key *pglogs_ent = NULL;
    struct lsn_commit_list *lsnent = NULL;
    DB_LSN logical_commit_lsn = queuekey->commit_lsn;

#ifdef NEWSI_STAT
    struct timeval before, after, diff;
    gettimeofday(&before, NULL);
#endif

    if (!logfile_pglogs_repo_ready)
        return 0;

    filenum = queuekey->lsn.file;

    Pthread_mutex_lock(&logfile_pglogs_repo_mutex);
    l_entry = retrieve_logfile_pglogs(filenum, 1);
    Pthread_mutex_lock(&l_entry->pglogs_mutex);
    Pthread_mutex_unlock(&logfile_pglogs_repo_mutex);

    rc = bdb_insert_pglogs_logical_int(l_entry->pglogs_hashtbl, fid,
                                       queuekey->pgno, queuekey->lsn,
                                       logical_commit_lsn);

    if (rc)
        abort();

    Pthread_mutex_unlock(&l_entry->pglogs_mutex);

#ifdef NEWSI_STAT
    gettimeofday(&after, NULL);
    timeval_diff(&before, &after, &diff);
    Pthread_mutex_lock(&newsi_stat_mutex);
    timeval_add(&logfile_insert_time, &diff, &logfile_insert_time);
    Pthread_mutex_unlock(&newsi_stat_mutex);
#endif

    return 0;
}

int bdb_update_logfile_pglogs(void *bdb_state, void *pglogs, unsigned int nkeys,
                              DB_LSN logical_commit_lsn)
{
    int rc = 0;
    int count = 0;
    int bdberr;
    int allocate = 0;
    int i;
    struct logfile_pglogs_entry *l_entry;
    struct page_logical_lsn_key *keylist =
        (struct page_logical_lsn_key *)pglogs;
    unsigned filenum, fileidx;
    struct shadows_pglogs_logical_key *pglogs_ent = NULL;
    struct lsn_commit_list *lsnent = NULL;

#ifdef NEWSI_STAT
    struct timeval before, after, diff;
    gettimeofday(&before, NULL);
#endif

    if (!logfile_pglogs_repo_ready || !nkeys)
        return 0;

    filenum = logical_commit_lsn.file;

    Pthread_mutex_lock(&logfile_pglogs_repo_mutex);
    l_entry = retrieve_logfile_pglogs(filenum, 1);
    Pthread_mutex_lock(&l_entry->pglogs_mutex);
    Pthread_mutex_unlock(&logfile_pglogs_repo_mutex);

    /* for each recorded page-lsn pair */
    /* keylist was generated by master in reverse lsn order, so process it in
     * reverse order*/
    for (i = nkeys - 1; i >= 0; i--) {
        rc = bdb_insert_pglogs_logical_int(l_entry->pglogs_hashtbl,
                                           keylist[i].fileid, keylist[i].pgno,
                                           keylist[i].lsn, logical_commit_lsn);
        if (rc)
            abort();
    }
    Pthread_mutex_unlock(&l_entry->pglogs_mutex);

#ifdef NEWSI_STAT
    gettimeofday(&after, NULL);
    timeval_diff(&before, &after, &diff);
    Pthread_mutex_lock(&newsi_stat_mutex);
    timeval_add(&logfile_insert_time, &diff, &logfile_insert_time);
    Pthread_mutex_unlock(&newsi_stat_mutex);
#endif

    return 0;
}

int bdb_update_timestamp_lsn(void *bdb_state, int32_t timestamp, DB_LSN lsn,
                             unsigned long long context)
{
    int rc = 0;
    int bdberr = 0;
    struct timestamp_lsn_key tlkey;
    tlkey.timestamp = timestamp;
    tlkey.lsn = lsn;
    tlkey.context = context;

    if (!bdb_gbl_timestamp_lsn_ready)
        return 0;

    Pthread_mutex_lock(&bdb_gbl_timestamp_lsn_mutex);
    rc = bdb_temp_table_insert(bdb_state, bdb_gbl_timestamp_lsn_cur, &tlkey,
                               sizeof(struct timestamp_lsn_key), NULL, 0,
                               &bdberr);
    /* fprintf(stderr, "%s: inserted timestamp %lld, lsn [%d][%d]\n", __func__,
     * timestamp, lsn.file, lsn.offset); */
    Pthread_mutex_unlock(&bdb_gbl_timestamp_lsn_mutex);

    return rc;
}

void bdb_delete_timestamp_lsn(bdb_state_type *bdb_state, int32_t timestamp)
{
    int rc = 0;
    int bdberr = 0;
    struct timestamp_lsn_key *foundkey;

    if (!bdb_gbl_timestamp_lsn_ready)
        return;

    Pthread_mutex_lock(&bdb_gbl_timestamp_lsn_mutex);
    rc = bdb_temp_table_first(bdb_state, bdb_gbl_timestamp_lsn_cur, &bdberr);
    while (!rc) {
        foundkey = bdb_temp_table_key(bdb_gbl_timestamp_lsn_cur);
        if (foundkey->timestamp > timestamp)
            break;
        /* fprintf(stderr, "%s: deleted timestamp %lld, lsn [%d][%d]\n",
           __func__,
              foundkey->timestamp, foundkey->lsn.file, foundkey->lsn.offset); */
        rc = bdb_temp_table_delete(bdb_state, bdb_gbl_timestamp_lsn_cur,
                                   &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:%d failed to delete timestamp_lsn_key in temp "
                            "table. rc %d bdberr %d\n",
                    __func__, __LINE__, rc, bdberr);
            abort();
        }
        rc =
            bdb_temp_table_first(bdb_state, bdb_gbl_timestamp_lsn_cur, &bdberr);
    }
    Pthread_mutex_unlock(&bdb_gbl_timestamp_lsn_mutex);
}

static int bdb_update_relinks_fileid_queues(void *bdb_state,
                                            unsigned char *fileid,
                                            db_pgno_t pgno, db_pgno_t prev_pgno,
                                            db_pgno_t next_pgno, DB_LSN lsn)
{
    struct shadows_fileid_pglogs_queue *fileid_queue = NULL;
    struct shadows_pglogs_queue_key *qe;

    fileid_queue = retrieve_fileid_pglogs_queue(fileid, 1);
    qe = allocate_shadows_pglogs_queue_key();
    qe->type = PGLOGS_QUEUE_RELINK;
    qe->prev_pgno = prev_pgno;
    qe->next_pgno = next_pgno;
    qe->pgno = pgno;
    qe->lsn = lsn;
    qe->commit_lsn = (DB_LSN){.file = 0, .offset = 0};

    pthread_rwlock_wrlock(&fileid_queue->queue_lk);
    listc_abl(&fileid_queue->queue_keys, qe);
    pthread_rwlock_unlock(&fileid_queue->queue_lk);
    return 0;
}

static int bdb_update_pglogs_fileid_queues(
    void *bdb_state, unsigned long long logical_tranid, int is_logical_commit,
    DB_LSN commit_lsn, struct page_logical_lsn_key *keylist, unsigned int nkeys)
{
    int j, count;
    struct shadows_fileid_pglogs_queue *fileid_queue = NULL;
    struct shadows_pglogs_queue_key **qearray = NULL, *qe, *chk;
    struct page_logical_lsn_key *key;

    if (nkeys <= 256)
        qearray = (struct shadows_pglogs_queue_key **)alloca(
            nkeys * sizeof(struct shadows_pglogs_queue_key *));
    else
        qearray = (struct shadows_pglogs_queue_key **)malloc(
            nkeys * sizeof(struct shadows_pglogs_queue_key *));

    for (j = 0; j < nkeys; j++) {
        key = &keylist[j];
        qearray[j] = qe = allocate_shadows_pglogs_queue_key();
        qe->logical_tranid = logical_tranid;
        qe->type = PGLOGS_QUEUE_PAGE;
        qe->prev_pgno = qe->next_pgno = 0;
        qe->pgno = key->pgno;
        qe->lsn = key->lsn;
        qe->commit_lsn = key->commit_lsn;

        if (log_compare(&key->commit_lsn, &commit_lsn))
            abort();
    }

    for (j = nkeys - 1; j >= 0; j--) {
        key = &keylist[j];

        if (!fileid_queue ||
            memcmp(fileid_queue->fileid, key->fileid, DB_FILE_ID_LEN)) {
            if (fileid_queue)
                pthread_rwlock_unlock(&fileid_queue->queue_lk);
            fileid_queue = retrieve_fileid_pglogs_queue(key->fileid, 1);
            pthread_rwlock_wrlock(&fileid_queue->queue_lk);
        }

        // Sanity
        if (chk = fileid_queue->queue_keys.bot)
            assert(log_compare(&qearray[j]->commit_lsn, &chk->commit_lsn) >= 0);

        listc_abl(&fileid_queue->queue_keys, qearray[j]);
    }

    if (fileid_queue)
        pthread_rwlock_unlock(&fileid_queue->queue_lk);

    if (nkeys > 256)
        free(qearray);

    if (gbl_new_snapisol_asof && (!logical_tranid || is_logical_commit)) {
        struct commit_list *lcommit = allocate_pglogs_commit_list();
        lcommit->commit_lsn = commit_lsn;
        lcommit->logical_tranid = logical_tranid;
        pthread_mutex_lock(&bdb_asof_current_lsn_mutex);
        listc_abl(&pglogs_commit_list, lcommit);
        pthread_mutex_unlock(&bdb_asof_current_lsn_mutex);
    }

    // lkprintf(stderr, "enque_global: enqueued commit_lsn [%d][%d]\n",
    // commit_lsn.file, commit_lsn.offset);

    return 0;
}

/* Called when the file itself is deleted. */
int bdb_remove_fileid_pglogs_queue(bdb_state_type *bdb_state,
                                   unsigned char *fileid, char *name)
{
    struct shadows_fileid_pglogs_queue *fileid_queue = NULL;
    struct shadows_pglogs_queue_key *qe;

    pthread_mutex_lock(&del_queue_lk);
    pthread_mutex_lock(&pglogs_queue_lk);

    if (pglogs_fileid_hash &&
        (fileid_queue = hash_find(pglogs_fileid_hash, fileid))) {
        // asof thread will delete this
        if (gbl_new_snapisol_asof) {
            fileid_queue->deleteme = 1;
            fileid_queue = NULL;
        } else
            hash_del(pglogs_fileid_hash, fileid_queue);
    }

    pthread_mutex_unlock(&pglogs_queue_lk);
    pthread_mutex_unlock(&del_queue_lk);

    if (fileid_queue) {
        while (qe = listc_rtl(&fileid_queue->queue_keys))
            return_pglogs_queue_key(qe);
        return_shadows_fileid_pglogs_queue(fileid_queue);
    }

    return 0;
}

struct pglog_queue_heads {
    int index;
    struct shadows_fileid_pglogs_queue **queue_heads;
};

int bdb_transfer_pglogs_to_queues(void *bdb_state, void *pglogs,
                                  unsigned int nkeys, int is_logical_commit,
                                  unsigned long long logical_tranid,
                                  DB_LSN logical_commit_lsn, int32_t timestamp,
                                  unsigned long long context)
{
    int rc = 0;
    int bdberr = 0;
    struct page_logical_lsn_key *keylist =
        (struct page_logical_lsn_key *)pglogs;

    if (!gbl_new_snapisol || !bdb_gbl_ltran_pglogs_hash_ready || !bdb_gbl_ltran_pglogs_hash_processed)
        return 0;

    rc = bdb_update_timestamp_lsn(bdb_state, timestamp, logical_commit_lsn,
                                  context);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s failed to update bdb_gbl_timestamp_lsn, rc = %d\n",
                __func__, rc);
        return rc;
    }

    bdb_update_pglogs_fileid_queues(bdb_state, logical_tranid,
                                    is_logical_commit, logical_commit_lsn,
                                    keylist, nkeys);

    return 0;
}

static int transfer_txn_pglogs_to_ltran(hash_t *pglogs_hashtbl,
                                        DB_LSN commit_lsn,
                                        int is_logical_commit,
                                        unsigned long long logical_tranid)
{
    struct ltran_pglogs_key *ltran_ent = NULL;
    struct ltran_pglogs_key ltran_key;
    void *hash_cur;
    unsigned int hash_cur_buk;
    struct shadows_pglogs_key *pglogs_ent = NULL;
    struct shadows_pglogs_logical_key *ltran_pglogs_ent = NULL;
    struct lsn_list *lsnent = NULL;
    struct lsn_commit_list *add_lsnent = NULL;
#ifdef NEWSI_DEBUG
    struct lsn_commit_list *bot = NULL;
#endif
#ifdef NEWSI_STAT
    struct timeval before, after, diff;
    gettimeofday(&before, NULL);
#endif

    if (!bdb_gbl_ltran_pglogs_hash_ready)
        return 0;

    assert(logical_tranid);

    ltran_key.logical_tranid = logical_tranid;

    Pthread_mutex_lock(&bdb_gbl_ltran_pglogs_mutex);

    if ((ltran_ent = hash_find(bdb_gbl_ltran_pglogs_hash, &ltran_key)) ==
        NULL) {
        ltran_ent = malloc(sizeof(struct ltran_pglogs_key));
        if (!ltran_ent) {
            Pthread_mutex_unlock(&bdb_gbl_ltran_pglogs_mutex);
            logmsg(LOGMSG_ERROR, "%s: fail malloc ltran_ent\n", __func__);
            return -1;
        }
        ltran_ent->logical_tranid = logical_tranid;
        ltran_ent->pglogs_hashtbl = hash_init_o(
            offsetof(struct shadows_pglogs_logical_key, fileid),
            DB_FILE_ID_LEN * sizeof(unsigned char) + sizeof(db_pgno_t));
        ltran_ent->relinks_hashtbl = hash_init_o(
            offsetof(struct pglogs_relink_key, fileid),
            DB_FILE_ID_LEN * sizeof(unsigned char) + sizeof(db_pgno_t));
        pthread_mutex_init(&ltran_ent->pglogs_mutex, NULL);
        ltran_ent->logical_commit_lsn.file = 0;
        ltran_ent->logical_commit_lsn.offset = 1;
        hash_add(bdb_gbl_ltran_pglogs_hash, ltran_ent);
    }

    if (is_logical_commit)
        ltran_ent->logical_commit_lsn = commit_lsn;

    Pthread_mutex_unlock(&bdb_gbl_ltran_pglogs_mutex);

    Pthread_mutex_lock(&ltran_ent->pglogs_mutex);

    /* for each recorded page */
    pglogs_ent = hash_first(pglogs_hashtbl, &hash_cur, &hash_cur_buk);
    while (pglogs_ent) {
        /* get the same page in the global structure */
        if ((ltran_pglogs_ent =
                 hash_find(ltran_ent->pglogs_hashtbl, pglogs_ent)) == NULL) {
            /* add one if not exist */
            ltran_pglogs_ent = allocate_shadows_pglogs_logical_key();
            if (!ltran_pglogs_ent)
                abort();
            memcpy(ltran_pglogs_ent->fileid, pglogs_ent->fileid,
                   DB_FILE_ID_LEN);
            ltran_pglogs_ent->pgno = pglogs_ent->pgno;
            listc_init(&ltran_pglogs_ent->lsns,
                       offsetof(struct lsn_commit_list, lnk));
            hash_add(ltran_ent->pglogs_hashtbl, ltran_pglogs_ent);
        }
        /* for each recorded lsn */
        LISTC_FOR_EACH(&pglogs_ent->lsns, lsnent, lnk)
        {
            add_lsnent = allocate_lsn_commit_list();
            if (!add_lsnent)
                abort();
            add_lsnent->lsn = lsnent->lsn;
            add_lsnent->commit_lsn = commit_lsn;
#ifdef NEWSI_DEBUG
            bot = LISTC_BOT(&ltran_pglogs_ent->lsns);
            if (bot)
                assert((log_compare(&bot->commit_lsn, &add_lsnent->commit_lsn) <
                        0) ||
                       (log_compare(&bot->commit_lsn,
                                    &add_lsnent->commit_lsn) == 0 &&
                        log_compare(&bot->lsn, &add_lsnent->lsn) <= 0));
#endif
            listc_abl(&ltran_pglogs_ent->lsns, add_lsnent);
        }

        pglogs_ent = hash_next(pglogs_hashtbl, &hash_cur, &hash_cur_buk);
    }

    Pthread_mutex_unlock(&ltran_ent->pglogs_mutex);

#ifdef NEWSI_STAT
    gettimeofday(&after, NULL);
    timeval_diff(&before, &after, &diff);
    Pthread_mutex_lock(&newsi_stat_mutex);
    timeval_add(&ltran_insert_time, &diff, &ltran_insert_time);
    Pthread_mutex_unlock(&newsi_stat_mutex);
#endif

    return 0;
}

static int transfer_txn_relinks_to_queues(void *bdb_state,
                                          unsigned long long logical_tranid,
                                          hash_t *relinks_hashtbl,
                                          DB_LSN commit_lsn)
{
    void *hash_cur;
    struct shadows_fileid_pglogs_queue *fileid_queue = NULL;
    struct pglogs_relink_key *relinks_ent = NULL;
    struct shadows_pglogs_queue_key *qe, *chk;
    struct relink_list *rlent = NULL;
    unsigned int hash_cur_buk;

    relinks_ent = hash_first(relinks_hashtbl, &hash_cur, &hash_cur_buk);
    while (relinks_ent) {
        if (!fileid_queue ||
            memcmp(fileid_queue->fileid, relinks_ent->fileid, DB_FILE_ID_LEN)) {
            if (fileid_queue)
                pthread_rwlock_unlock(&fileid_queue->queue_lk);
            fileid_queue = retrieve_fileid_pglogs_queue(relinks_ent->fileid, 1);
            pthread_rwlock_wrlock(&fileid_queue->queue_lk);
        }

        LISTC_FOR_EACH(&relinks_ent->relinks, rlent, lnk)
        {
            qe = allocate_shadows_pglogs_queue_key();
            qe->logical_tranid = logical_tranid;
            qe->type = PGLOGS_QUEUE_RELINK;
            qe->prev_pgno = 0;
            qe->next_pgno = rlent->inh;
            qe->pgno = relinks_ent->pgno;
            qe->lsn = rlent->lsn;
            qe->commit_lsn = commit_lsn;

            // Sanity
            if (chk = fileid_queue->queue_keys.top)
                assert(log_compare(&qe->commit_lsn, &chk->commit_lsn) >= 0);

            listc_abl(&fileid_queue->queue_keys, qe);
        }

        relinks_ent = hash_next(relinks_hashtbl, &hash_cur, &hash_cur_buk);
    }

    if (fileid_queue)
        pthread_rwlock_unlock(&fileid_queue->queue_lk);

    return 0;
}

static int transfer_txn_pglogs_to_queues(void *bdb_state,
                                         unsigned long long logical_tranid,
                                         hash_t *pglogs_hashtbl,
                                         DB_LSN commit_lsn)
{
    void *hash_cur;
    struct shadows_fileid_pglogs_queue *fileid_queue = NULL;
    struct shadows_pglogs_key *pglogs_ent = NULL;
    struct shadows_pglogs_queue_key *qe, *chk;
    struct lsn_list *lsnent = NULL;
    unsigned int hash_cur_buk;

    pglogs_ent = hash_first(pglogs_hashtbl, &hash_cur, &hash_cur_buk);
    while (pglogs_ent) {
        if (!fileid_queue ||
            memcmp(fileid_queue->fileid, pglogs_ent->fileid, DB_FILE_ID_LEN)) {
            if (fileid_queue)
                pthread_rwlock_unlock(&fileid_queue->queue_lk);
            fileid_queue = retrieve_fileid_pglogs_queue(pglogs_ent->fileid, 1);
            pthread_rwlock_wrlock(&fileid_queue->queue_lk);
        }

        LISTC_FOR_EACH(&pglogs_ent->lsns, lsnent, lnk)
        {
            qe = allocate_shadows_pglogs_queue_key();
            qe->logical_tranid = logical_tranid;
            qe->type = PGLOGS_QUEUE_PAGE;
            qe->prev_pgno = qe->next_pgno = 0;
            qe->pgno = pglogs_ent->pgno;
            qe->lsn = lsnent->lsn;
            qe->commit_lsn = commit_lsn;

            if (chk = fileid_queue->queue_keys.top)
                assert(log_compare(&qe->commit_lsn, &chk->commit_lsn) >= 0);
            listc_abl(&fileid_queue->queue_keys, qe);
        }

        pglogs_ent = hash_next(pglogs_hashtbl, &hash_cur, &hash_cur_buk);
    }

    if (fileid_queue)
        pthread_rwlock_unlock(&fileid_queue->queue_lk);

    return 0;
}

int bdb_update_txn_pglogs(void *bdb_state, void *pglogs_hashtbl,
                          pthread_mutex_t *mutexp, db_pgno_t pgno,
                          unsigned char *fileid, DB_LSN lsn)
{
    int rc;
    struct shadows_pglogs_key *pglogs_ent;
    struct shadows_pglogs_key key;
#ifdef NEWSI_STAT
    struct timeval before, after, diff;
    gettimeofday(&before, NULL);
#endif

    if (!gbl_new_snapisol)
        return 0;

    memcpy(key.fileid, fileid, DB_FILE_ID_LEN);
    key.pgno = pgno;
    Pthread_mutex_lock(mutexp);

    rc = bdb_insert_pglogs_int(pglogs_hashtbl, fileid, pgno, lsn);
    if (rc)
        abort();

    Pthread_mutex_unlock(mutexp);

#ifdef NEWSI_STAT
    gettimeofday(&after, NULL);
    timeval_diff(&before, &after, &diff);
    Pthread_mutex_lock(&newsi_stat_mutex);
    timeval_add(&txn_insert_time, &diff, &txn_insert_time);
    Pthread_mutex_unlock(&newsi_stat_mutex);
#endif

    return 0;
}

int bdb_relink_gbl_ltran_pglogs(void *bdb_state, unsigned char *fileid,
                                db_pgno_t pgno, db_pgno_t prev_pgno,
                                db_pgno_t next_pgno, DB_LSN lsn)
{
    int rc = 0;
    void *hash_cur;
    unsigned int hash_cur_buk;
    struct ltran_pglogs_key *ltran_ent = NULL;
    struct relink_list *rlent;
    struct pglogs_relink_key *relinks_ent;
    struct pglogs_relink_key key;

#ifdef NEWSI_STAT
    struct timeval before, after, diff;
    gettimeofday(&before, NULL);
#endif

    if (!bdb_gbl_ltran_pglogs_hash_ready)
        return 0;

    memcpy(key.fileid, fileid, DB_FILE_ID_LEN);

    Pthread_mutex_lock(&bdb_gbl_ltran_pglogs_mutex);

    ltran_ent = hash_first(bdb_gbl_ltran_pglogs_hash, &hash_cur, &hash_cur_buk);
    while (ltran_ent) {
        Pthread_mutex_lock(&ltran_ent->pglogs_mutex);

        rc = bdb_insert_relinks_int(ltran_ent->relinks_hashtbl, fileid, pgno,
                                    prev_pgno, next_pgno, lsn);
        if (rc)
            abort();

        Pthread_mutex_unlock(&ltran_ent->pglogs_mutex);
        ltran_ent =
            hash_next(bdb_gbl_ltran_pglogs_hash, &hash_cur, &hash_cur_buk);
    }

    Pthread_mutex_unlock(&bdb_gbl_ltran_pglogs_mutex);

#ifdef NEWSI_STAT
    gettimeofday(&after, NULL);
    timeval_diff(&before, &after, &diff);
    Pthread_mutex_lock(&newsi_stat_mutex);
    timeval_add(&ltran_relink_time, &diff, &ltran_relink_time);
    Pthread_mutex_unlock(&newsi_stat_mutex);
#endif

    return 0;
}

int bdb_relink_logfile_pglogs(void *bdb_state, unsigned char *fileid,
                              db_pgno_t pgno, db_pgno_t prev_pgno,
                              db_pgno_t next_pgno, DB_LSN lsn)
{
    int rc = 0, allocate = 0;
    unsigned filenum, last_filenum;
    struct logfile_pglogs_entry *l_entry;

    if (!logfile_pglogs_repo_ready)
        return 0;

#ifdef NEWSI_STAT
    struct timeval before, after, diff;
    gettimeofday(&before, NULL);
#endif

    filenum = lsn.file;
    Pthread_mutex_lock(&logfile_pglogs_repo_mutex);
    l_entry = retrieve_logfile_pglogs(filenum, 1);
    Pthread_mutex_lock(&l_entry->pglogs_mutex);
    Pthread_mutex_unlock(&logfile_pglogs_repo_mutex);

    rc = bdb_insert_relinks_int(l_entry->relinks_hashtbl, fileid, pgno,
                                prev_pgno, next_pgno, lsn);
    if (rc)
        abort();

    Pthread_mutex_unlock(&l_entry->pglogs_mutex);

#ifdef NEWSI_STAT
    gettimeofday(&after, NULL);
    timeval_diff(&before, &after, &diff);
    Pthread_mutex_lock(&newsi_stat_mutex);
    timeval_add(&logfile_relink_time, &diff, &logfile_relink_time);
    Pthread_mutex_unlock(&newsi_stat_mutex);
#endif

    return 0;
}

extern int gbl_disable_new_snapisol_overhead;

int bdb_relink_pglogs(void *bdb_state, unsigned char *fileid, db_pgno_t pgno,
                      db_pgno_t prev_pgno, db_pgno_t next_pgno, DB_LSN lsn)
{
    int rc = 0;

    if (!gbl_new_snapisol || !bdb_gbl_ltran_pglogs_hash_ready)
        return 0;

    if (gbl_disable_new_snapisol_overhead)
        return 0;

#ifdef NEWSI_STAT
    struct timeval before, after, diff;
    gettimeofday(&before, NULL);
#endif

    rc = bdb_update_relinks_fileid_queues(bdb_state, fileid, pgno, prev_pgno,
                                          next_pgno, lsn);

#ifdef NEWSI_STAT
    gettimeofday(&after, NULL);
    timeval_diff(&before, &after, &diff);
    Pthread_mutex_lock(&newsi_stat_mutex);
    timeval_add(&txn_relink_time, &diff, &txn_relink_time);
    Pthread_mutex_unlock(&newsi_stat_mutex);
#endif

    return 0;
}

int bdb_relink_txn_pglogs(void *bdb_state, void *relinks_hashtbl,
                          pthread_mutex_t *mutexp, unsigned char *fileid,
                          db_pgno_t pgno, db_pgno_t prev_pgno,
                          db_pgno_t next_pgno, DB_LSN lsn)
{
    int rc = 0;

    if (!gbl_new_snapisol || !bdb_gbl_ltran_pglogs_hash_ready)
        return 0;

    if (gbl_disable_new_snapisol_overhead)
        return 0;

#ifdef NEWSI_STAT
    struct timeval before, after, diff;
    gettimeofday(&before, NULL);
#endif

    Pthread_mutex_lock(mutexp);

    rc = bdb_insert_relinks_int(relinks_hashtbl, fileid, pgno, prev_pgno,
                                next_pgno, lsn);
    if (rc)
        abort();

    Pthread_mutex_unlock(mutexp);

#ifdef NEWSI_STAT
    gettimeofday(&after, NULL);
    timeval_diff(&before, &after, &diff);
    Pthread_mutex_lock(&newsi_stat_mutex);
    timeval_add(&txn_relink_time, &diff, &txn_relink_time);
    Pthread_mutex_unlock(&newsi_stat_mutex);
#endif

    return 0;
}

#include "nodemap.h"

int bdb_push_pglogs_commit(void *in_bdb_state, DB_LSN commit_lsn, uint32_t gen,
                           unsigned long long ltranid, int push)
{
    bdb_state_type *bdb_state = (bdb_state_type *)in_bdb_state;
    struct commit_list *lcommit = NULL;
    extern int gbl_durable_set_trace;
    char *master, *eid;

    if (gbl_new_snapisol_asof && push) {
        lcommit = allocate_pglogs_commit_list();
        lcommit->commit_lsn = commit_lsn;
        lcommit->logical_tranid = ltranid;
    }

    pthread_mutex_lock(&bdb_asof_current_lsn_mutex);
    if (lcommit)
        listc_abl(&pglogs_commit_list, lcommit);
    bdb_latest_commit_lsn = commit_lsn;
    bdb_latest_commit_gen = gen;
    if (gbl_durable_set_trace)
        logmsg(LOGMSG_USER, "Set commit lsn to [%d][%d] generation %u\n", commit_lsn.file,
                commit_lsn.offset, gen);

    pthread_mutex_unlock(&bdb_asof_current_lsn_mutex);

    bdb_state->dbenv->get_rep_master(bdb_state->dbenv, &master);
    bdb_state->dbenv->get_rep_eid(bdb_state->dbenv, &eid);

    // We are under the log lock here.  If we are writing logs, there has to
    // be a master
    static time_t lastpr = 0;
    time_t now;
    int doprint = 0;
    unsigned long long master_cnt = 0;
    unsigned long long notmaster_cnt = 0;
    
    if (gbl_durable_set_trace && ((now = time(NULL)) > lastpr)) {
        doprint = 1;
        lastpr = now;
    }

    if (!strcmp(master, eid)) {
        seqnum_type *seqnum = &bdb_state->seqnum_info->seqnums[nodeix(eid)];
        Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));
        seqnum->lsn = commit_lsn;
        seqnum->commit_generation = seqnum->generation = gen;
        Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));
        bdb_set_commit_lsn_gen(bdb_state, &commit_lsn, gen);
        master_cnt++;
        if (doprint) {
            logmsg(LOGMSG_USER, "%s: setting seqnum_info on master to [%d][%d] "
                    "gen [%d] master-count=%llu not-master-count=%llu\n", 
                    __func__, commit_lsn.file, commit_lsn.offset, gen, 
                    master_cnt, notmaster_cnt);
        }
    }
    else {
        notmaster_cnt++;
        if (doprint) {
            logmsg(LOGMSG_USER, "%s: NOT setting seqnum_info on replicant, lsn is [%d][%d] "
                    "gen [%d] master-count=%llu not-master-count=%llu\n", 
                    __func__, commit_lsn.file, commit_lsn.offset, gen, 
                    master_cnt, notmaster_cnt);
        }
    }

    return 0;
}

int bdb_latest_commit(bdb_state_type *bdb_state, DB_LSN *latest_lsn,
                      uint32_t *latest_gen)
{
    pthread_mutex_lock(&bdb_asof_current_lsn_mutex);
    *latest_lsn = bdb_latest_commit_lsn;
    *latest_gen = bdb_latest_commit_gen;
    pthread_mutex_unlock(&bdb_asof_current_lsn_mutex);
    return 0;
}

#include <dbinc_auto/txn_auto.h>
#include <dbinc/db_swap.h>

/* Called for a single-node cluster: the latest lsn is durable */
void bdb_durable_lsn_for_single_node(void *in_bdb_state)
{
    bdb_state_type *bdb_state = (bdb_state_type *)in_bdb_state;
    DB_LSN lsn, found_lsn = {0};
    DBT data = {0};
    DB_LOGC *logc;
    u_int32_t rectype, generation;
    int ret;

    if ((ret = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0)) != 0)
        abort();

    for (ret = logc->get(logc, &lsn, &data, DB_LAST); ret == 0;
         ret = logc->get(logc, &lsn, &data, DB_PREV)) {
        LOGCOPY_32(&rectype, data.data);
        switch (rectype) {
        case DB___txn_regop:
        case DB___txn_regop_gen:
        case DB___txn_regop_rowlocks:
            found_lsn = lsn;
            goto done;
            break;
        }
    }
done:

    if (logc) {
        logc->close(logc, 0);
    }

    if (found_lsn.file == 0)
        found_lsn = lsn;

    bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &generation);
    bdb_state->dbenv->set_durable_lsn(bdb_state->dbenv, &found_lsn, generation);
}

int bdb_latest_commit_is_durable(void *in_bdb_state)
{
    extern int gbl_durable_replay_test;
    bdb_state_type *bdb_state = (bdb_state_type *)in_bdb_state;
    uint32_t durable_gen;
    uint32_t latest_gen;
    DB_LSN durable_lsn;
    DB_LSN latest_lsn;

    bdb_latest_commit(bdb_state, &latest_lsn, &latest_gen);
    bdb_state->dbenv->get_durable_lsn(bdb_state->dbenv, &durable_lsn,
                                      &durable_gen);

    if (latest_gen < durable_gen)
        return 0;

    if ((latest_gen == durable_gen) &&
        log_compare(&durable_lsn, &latest_lsn) < 0)
        return 0;

    if (gbl_durable_replay_test && (0 == (rand() % 20)))
        return 0;

    return 1;
}

int bdb_transfer_txn_pglogs(void *bdb_state, void *pglogs_hashtbl,
                            void *relinks_hashtbl, pthread_mutex_t *mutexp,
                            DB_LSN commit_lsn, uint32_t flags,
                            unsigned long long logical_tranid,
                            int32_t timestamp, unsigned long long context)
{
    int rc, bdberr;
    int count = 0;
    int is_logical_commit = (flags & DB_TXN_LOGICAL_COMMIT);
    int get_repo_lock = !(flags & DB_TXN_DONT_GET_REPO_MTX);

    if (!gbl_new_snapisol || !bdb_gbl_ltran_pglogs_hash_ready)
        return 0;

    if (gbl_disable_new_snapisol_overhead)
        return 0;

    if (context) {
        rc =
            bdb_update_timestamp_lsn(bdb_state, timestamp, commit_lsn, context);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s failed to update bdb_gbl_timestamp_lsn, rc = %d\n",
                    __func__, rc);
            return rc;
        }
    }

    if (rc = transfer_txn_pglogs_to_queues(bdb_state, logical_tranid,
                                           pglogs_hashtbl, commit_lsn))
        abort();

    if (rc = transfer_txn_relinks_to_queues(bdb_state, logical_tranid,
                                            relinks_hashtbl, commit_lsn))
        abort();

    return 0;
}

int bdb_get_lsn_context_from_timestamp(bdb_state_type *bdb_state,
                                       int32_t timestamp, void *ret_lsn,
                                       unsigned long long *ret_context,
                                       int *bdberr)
{
    int rc = -1;
    struct timestamp_lsn_key tlkey;
    struct timestamp_lsn_key *foundkey;

    ((DB_LSN *)ret_lsn)->file = 0;
    ((DB_LSN *)ret_lsn)->offset = 1;
    if (ret_context)
        *ret_context = 0;

    if (!bdb_gbl_timestamp_lsn_ready)
        return get_lsn_context_from_timestamp(bdb_state->dbenv, timestamp,
                                              ret_lsn, ret_context);

    tlkey.timestamp = timestamp + 1;
    tlkey.lsn.file = 0;
    tlkey.lsn.offset = 0;

    Pthread_mutex_lock(&bdb_gbl_timestamp_lsn_mutex);

    bdb_temp_table_find(bdb_state, bdb_gbl_timestamp_lsn_cur, &tlkey,
                        sizeof(struct timestamp_lsn_key), NULL, bdberr);
    foundkey = bdb_temp_table_key(bdb_gbl_timestamp_lsn_cur);
    if (foundkey && foundkey->timestamp > timestamp) {
        bdb_temp_table_move(bdb_state, bdb_gbl_timestamp_lsn_cur, DB_PREV,
                            bdberr);
        foundkey = bdb_temp_table_key(bdb_gbl_timestamp_lsn_cur);
    } else {
        bdb_temp_table_last(bdb_state, bdb_gbl_timestamp_lsn_cur, bdberr);
        foundkey = bdb_temp_table_key(bdb_gbl_timestamp_lsn_cur);
    }

    if (foundkey) {
        rc = 0;
        ((DB_LSN *)ret_lsn)->file = foundkey->lsn.file;
        ((DB_LSN *)ret_lsn)->offset = foundkey->lsn.offset;
        if (ret_context)
            *ret_context = foundkey->context;
    }

    Pthread_mutex_unlock(&bdb_gbl_timestamp_lsn_mutex);

    return rc;
}

int bdb_get_context_from_lsn(bdb_state_type *bdb_state, void *lsnp,
                             unsigned long long *ret_context, int *bdberr)
{
    int rc = -1;
    struct timestamp_lsn_key *foundkey;

    *ret_context = 0;

    if (!bdb_gbl_timestamp_lsn_ready)
        return get_context_from_lsn(bdb_state->dbenv, *(DB_LSN *)lsnp,
                                    ret_context);

    Pthread_mutex_lock(&bdb_gbl_timestamp_lsn_mutex);

    rc = bdb_temp_table_last(bdb_state, bdb_gbl_timestamp_lsn_cur, bdberr);
    while (!rc) {
        foundkey = bdb_temp_table_key(bdb_gbl_timestamp_lsn_cur);
        if (log_compare(&foundkey->lsn, (DB_LSN *)lsnp) <= 0) {
            *ret_context = foundkey->context;
            Pthread_mutex_unlock(&bdb_gbl_timestamp_lsn_mutex);
            return 0;
        }
        rc = bdb_temp_table_move(bdb_state, bdb_gbl_timestamp_lsn_cur, DB_PREV,
                                 bdberr);
    }

    Pthread_mutex_unlock(&bdb_gbl_timestamp_lsn_mutex);

    abort();
}

static int bdb_cursor_update_shadows(bdb_cursor_ifn_t *pcur_ifn, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    int rc;

    rc = bdb_btree_update_shadows(cur, -1, bdberr);

    return rc;
}

static void *bdb_cursor_get_shadowtran(bdb_cursor_ifn_t *pcur_ifn)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;

    return cur->shadow_tran;
}

static int bdb_cursor_update_shadows_with_pglogs(bdb_cursor_ifn_t *pcur_ifn,
                                                 unsigned *inpgno,
                                                 unsigned char *infileid,
                                                 int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    int rc;

    rc =
        bdb_btree_update_shadows_with_trn_pglogs(cur, inpgno, infileid, bdberr);

    return rc;
}

/*
 * This is needed to resolve a race condition which occurs when a cursor is in
 * pageorder mode.  If a record with a blob is updated, the ondisk portion of
 * the record could be placed in the virtual stripe while the untouched blob
 * part of the record is still read from the blob file.  Because the cursor
 * is on the virtual stripe, it can't rely on page-locks: the blob can be
 * updated after the cursor runs update_shadows but before the cursor calls
 * fetch_blobs_into_sqlite_mem.  If there is an inplace-update which updates
 * or creates a not-null blob, we can detect it by comparing the update id of
 * what we found against the update id of what we're looking for: the code runs
 * update_shadows on the spot when it finds an update-ids in the data-file which
 * is larger than what it's looking for.  This routine accomodates the case
 * where both the datafile and the shadow contain NULL.  The first time we see a
 * NULL in both the data and blob file, we are forced to run update_shadows to
 * make sure that the blob hasn't been deleted out from under us.  If both of
 * them are still NULL, we call this routine so that we're not forced to spin.
 *
 * This is called only when fetch sees that the data was retrieved from the
 * shadow rather than the data file.
 */
static int bdb_cursor_set_null_blob_in_shadows(bdb_cursor_ifn_t *pcur_ifn,
                                               unsigned long long genid,
                                               int dbnum, int blobno,
                                               int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    int rc = 0;

    if (!cur->shadow_tran ||
        (cur->shadow_tran->tranclass != TRANCLASS_SNAPISOL &&
         cur->shadow_tran->tranclass != TRANCLASS_SERIALIZABLE))
        return 0;
    rc = bdb_osql_set_null_blob_in_shadows(cur, cur->shadow_tran->osql, genid,
                                           dbnum, blobno, bdberr);
    return rc;
}

static int bdb_cursor_getpageorder(bdb_cursor_ifn_t *pcur_ifn)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    return cur->pageorder;
}

static int bdb_cursor_first(bdb_cursor_ifn_t *pcur_ifn, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    int rc;

    rc = bdb_cursor_move(cur, DB_FIRST, bdberr);

    return rc;
}

static int bdb_cursor_last(bdb_cursor_ifn_t *pcur_ifn, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    int rc;

    rc = bdb_cursor_move(cur, DB_LAST, bdberr);

    return rc;
}

static int bdb_cursor_next(bdb_cursor_ifn_t *pcur_ifn, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    int rc;

again:
    rc = bdb_cursor_move(cur, DB_NEXT, bdberr);

    /* must stand on the last row */
    if (rc == IX_PASTEOF) {

        /* Short circuit getlast logic immediately if this is a data file */
        if (cur->type == BDBC_DT) {
            return IX_PASTEOF;
        }

        /* we still have the lock page, if we were there */
        rc = pcur_ifn->last(pcur_ifn, bdberr);
        if (rc < 0) {
            if (*bdberr == BDBERR_DEADLOCK)
                *bdberr = BDBERR_DEADLOCK_ON_LAST;

            return rc;
        }
        if (rc == IX_EMPTY)
            return IX_EMPTY;

        if (rc)
            ctrace(
                "%s: unexpected return, I should have had that lock, %d %d!\n",
                __func__, rc, *bdberr);
        return IX_PASTEOF;
    }

    return rc;
}

static int bdb_cursor_prev(bdb_cursor_ifn_t *pcur_ifn, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    int rc;

/* The YAST test does an 'order by rowid desc' */
/* assert(cur->type != BDBC_DT); */

again:
    rc = bdb_cursor_move(cur, DB_PREV, bdberr);

    /* must stand on the last row */
    if (rc == IX_PASTEOF) {
        /* we still have the lock page, if we were there */
        rc = pcur_ifn->first(pcur_ifn, bdberr);
        if (rc < 0) {
            if (*bdberr == BDBERR_DEADLOCK)
                *bdberr = BDBERR_DEADLOCK_ON_LAST;

            return rc;
        }
        if (rc == IX_EMPTY)
            return IX_EMPTY;

        if (rc)
            ctrace(
                "%s: unexpected return, I should have had that lock, %d %d!\n",
                __func__, rc, *bdberr);
        return IX_PASTEOF;
    }

    return rc;
}

/* when key found in RL lastkeylen will be > 0 and
 * lastkey will be not NULL
 */
inline static int key_found_in_rl(bdb_cursor_impl_t *cur)
{
    return cur->lastkeylen > 0;
}

/**
 * RETURNS (please try to keep this up to date):
 *    - IX_FND       : found the exact key we are looking for
 *    - IX_PASTEOF   : cannot find this record or smaller
 *                     (cursor is positioned to first that can be in ANY ORDER
 *VS key)
 *    - IX_EMPTY     : no rows
 *    - IX_NOTFND    : found a record that is SMALLER than the key
 *    < 0            : error, bdberr set
 *
 * If dirLeft is set (Direction is LEFT), we are moving left (PREV) on the btree
 */
static int bdb_cursor_find_last_dup(bdb_cursor_ifn_t *pcur_ifn, void *key,
                                    int keylen, int keymax, bias_info *bias,
                                    int *bdberr)
{
    int dirLeft = bias->dirLeft;
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    bdb_state_type *bdb_state;
    char *newkey;
    int newkeylen;
    int rc = 0;

    bdb_state = cur->state;

    *bdberr = 0;

    if (cur->trak) {
        logmsg(LOGMSG_USER, "Cur %p %s find_last_dup len=%d data[8]=%llx\n", cur,
                (cur->type == BDBC_DT) ? "data" : "index", keylen,
                *(long long *)key);
    }

    /* are we invalidated? relock */
    if (cur->invalidated) {
        assert(cur->rl != NULL);

        cur->invalidated = 0;

        rc = cur->rl->lock(cur->rl, cur->curtran, bdberr);
        if (rc)
            return rc;
    }

    if (unlikely(bias->truncated)) {
        /* Already adjusted in types.c */
        newkeylen = keylen;
        newkey = key;
    } else {
        /* Add FF at the end of the key */
        newkeylen = keylen + 1;
        newkey = alloca(newkeylen);
        memcpy(newkey, key, keylen);
        newkey[keylen] = 0xff;
    }

    if (cur->type == BDBC_DT) {
        logmsg(LOGMSG_ERROR, "%s: calling this for data file???\n", __func__);
        cheap_stack_trace();
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    rc = bdb_cursor_find_merge(cur, newkey, newkeylen, bdberr);
    if (rc < 0)
        return rc;

    if (rc == IX_PASTEOF ||
        rc == IX_NOTFND) /*reuse of ll functions generate also IX_NOTFND*/
    {
        rc = pcur_ifn->last(pcur_ifn, bdberr);
        if (rc < 0)
            return rc;

        if (rc == IX_EMPTY)
            return IX_EMPTY;
    } else if (rc == IX_FND) {
        if (!dirLeft) {
            /* we are going Right because we got here from SeekGT,
             * and rc == IX_FIND so we found GT: don't do the prevs
             * unnecessarily.
             *
             * Note: we were looking for newkey which is larger than key
             * so whatever found (in data) has to be >= key. */
            if (unlikely(bias->truncated)) {
                // SQLite compare using cooked values
                // Assert below doesn't apply (key may be desc)
                rc = bias->cmp(bias, cur->data);
            } else if ((rc = memcmp(cur->data, key, keylen)) < 0) {
                abort(); /* can never have data < key */
            }

            /* if data == key (rc == 0), since we are looking for GT, return
             * IX_NOTFND */
            return (rc == 0) ? IX_NOTFND : IX_FND;
        }

        /* direction is LEFT, whatever we found is greater than SeekLE wants
         * so need to get PREV, which will be what SeekLE wants or smaller
         */
        if (cur->used_sd == 1 && key_found_in_rl(cur) &&
            cur->sd->is_at_eof(cur->sd)) {
            /* if we are at eof, tree might be empty */
            rc = cur->sd->last(cur->sd, bdberr);
            if (rc == IX_FND)
                cur->used_sd = 0;
        } else if (cur->used_rl == 1 && !key_found_in_rl(cur) &&
                   cur->rl->is_at_eof(cur->rl)) {
            /* if we are at eof, tree might be empty */
            rc = cur->rl->last(cur->rl, bdberr);
            if (rc == IX_FND)
                cur->used_rl = 0;
        }

        struct datacopy_info datacopy_bck;

        /* patch for datacopy, since this will reset the pointers if
           it fails */
        if (cur->type == BDBC_IX && cur->state->ixdta[cur->idx]) {
            datacopy_bck = *(struct datacopy_info *)(cur->datacopy);
        }

        rc = bdb_cursor_move(cur, DB_PREV, bdberr);
        if (rc < 0)
            return rc;
        /* we are still positioned on the last record */
        if (rc == IX_NOTFND || rc == IX_PASTEOF) {
            /* restore the previous position */
            if (cur->type == BDBC_IX && cur->state->ixdta[cur->idx]) {
                *(struct datacopy_info *)cur->datacopy = datacopy_bck;
            }

            return IX_PASTEOF;
        }
    } else {
        logmsg(LOGMSG_ERROR, "%s: bdb_cursor_find_merge rc=%d???\n", __func__, rc);
        cheap_stack_trace();
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (!dirLeft) {
        // we get here if bias OP_SeekGT and we did a last, now we need to do
        // PREV
        // which means that we are changing direction
        if (cur->used_sd == 0)
            cur->used_sd = 1;
        if (cur->used_rl == 0)
            cur->used_rl = 1;
    }

    /* wiper mode */
    do {
        /* IX_FND */
        rc = bias->truncated ? bias->cmp(bias, cur->data)
                             : memcmp(cur->data, newkey, keylen);
        if (rc <= 0)
            break;

        rc = bdb_cursor_move(cur, DB_PREV, bdberr);
        if (rc < 0)
            return rc;
        if (rc == IX_NOTFND || rc == IX_PASTEOF)
            return IX_PASTEOF;

    } while (rc == IX_FND);

    rc = bias->truncated ? bias->cmp(bias, cur->data)
                         : memcmp(cur->data, key, keylen);
    return (rc == 0) ? IX_FND : IX_NOTFND;
}

/**
 * Switch berkdb stripe in use
 *
 */
static int bdb_switch_stripe(bdb_cursor_impl_t *cur, int dtafile, int *bdberr)
{
    bdb_berkdb_t *newberkdb_rl = NULL;
    bdb_berkdb_t *newberkdb_sd = NULL;
    int rc = 0;

    *bdberr = 0;
    if (!IS_VALID_DTA(dtafile))
        return IX_NOTFND;

    if (cur->idx == dtafile)
        return 0;

    /*
    cur->idx = dtafile;
    */

    if (dtafile == cur->state->attr->dtastripe) {
        /* add stripe is simple, close the shared persistent one and return */
        if (!cur->addcur) {
            if (cur->trak) {
                logmsg(LOGMSG_USER, "Cur %p switch_stripe returning NOTFND because "
                                "there's no addcur stripe.\n",
                        cur);
            }
            return IX_NOTFND; /* cursor still position on the last row here, if
                                 any */
        }
        cur->idx = dtafile;

        /* close real if exists */
        if (cur->rl) {
            verify_pageorder_tablescan(cur);
            rc = cur->rl->close(cur->rl, bdberr);
            if (rc)
                return rc;
            cur->rl = NULL;
        }

        /* close shadow if exists; virtual stripe has no shadow */
        if (cur->sd) {
            rc = cur->sd->close(cur->sd, bdberr);
            if (rc)
                return rc;
            cur->sd = NULL;
        }

        return IX_FND;
    } else {
        if (cur->rl)
            verify_pageorder_tablescan(cur);

        cur->idx = dtafile;

        newberkdb_rl = bdb_berkdb_open(cur, cur->rowlocks ? BERKDB_REAL_ROWLOCKS
                                                          : BERKDB_REAL,
                                       MAXRECSZ, MAXKEYSZ, bdberr);
        if (!newberkdb_rl)
            return -1;

        if (cur->shadow_tran) {
            /* if shadow tran around, get a shadow also if exist*/
            newberkdb_sd =
                bdb_berkdb_open(cur, BERKDB_SHAD, MAXRECSZ, MAXKEYSZ, bdberr);
            if (!newberkdb_sd && *bdberr)
                return -1;
        }
    }

    /* switch real berkdb */
    if (cur->rl) {
        rc = cur->rl->close(cur->rl, bdberr);
        if (rc)
            return rc;
        cur->rl = NULL;
    }

    if (newberkdb_rl)
        cur->rl = newberkdb_rl;

    /* switch the shadow */
    if (cur->sd) {
        rc = cur->sd->close(cur->sd, bdberr);
        if (rc)
            return rc;
        cur->sd = NULL;
    }

    if (newberkdb_sd)
        cur->sd = newberkdb_sd;

    return IX_FND;
}

static int berkdb_get_genid(bdb_cursor_impl_t *cur, bdb_berkdb_t *berkdb,
                            unsigned long long *genid, int *bdberr)
{
    char *val = NULL;
    int vallen = 0;
    int rc = 0;

    if (cur->type == BDBC_DT) {
        rc = berkdb->key(berkdb, &val, bdberr);
        if (rc < 0)
            return rc;
        rc = berkdb->keysize(berkdb, &vallen, bdberr);
        if (rc < 0)
            return rc;
    } else {
        rc = berkdb->dta(berkdb, &val, bdberr);
        if (rc < 0)
            return rc;
        rc = berkdb->dtasize(berkdb, &vallen, bdberr);
        if (cur->state->ixdta[cur->idx]) {
            /* datacopy */
            assert(vallen >= sizeof(*genid));
            vallen = sizeof(*genid);
        }
        if (rc < 0)
            return rc;
    }

    assert(vallen >= sizeof(*genid));

#ifdef _SUN_SOURCE
    memcpy(genid, val, 8);
#else
    *genid = *(unsigned long long *)val;
#endif

    return 0;
}

/**
 * If the cursor had to close its berkeley cursor (losing the locks as well)
 * this function will recreate the berkeley cursor
 *
 * For a relative move, the cursor needs to be reposition on the old place
 * We could fail to find the old record, but this is ok as long as we get
 * the "next" record we needed in the first place to move to (when this
 * happens, retrieved is set to 1)
 *
 */
static int bdb_cursor_revalidate(bdb_cursor_impl_t *cur, bdb_berkdb_t *berkdb,
                                 int how, int *retrieved, int *bdberr)
{
    int rc = 0;

    if (!cur->invalidated)
        return 0;

    *retrieved = 0;

    /* absolute moves needs just to lock
       relative moves are actually finds */
    switch (how) {
    case DB_FIRST:
    case DB_LAST:
        cur->invalidated = 0;
        rc = berkdb->lock(berkdb, cur->curtran, bdberr);
        if (rc == BDBERR_DEADLOCK) {
            rc = -1;
            *bdberr = BDBERR_DEADLOCK;
        }

        return rc;

    case DB_NEXT:
    case DB_PREV:
        /* this call resets invalidated field */
        rc = cur->ifn->lock(cur->ifn, cur->curtran,
                            (how == DB_NEXT) ? BDB_NEXT : BDB_PREV, bdberr);
        if (rc == IX_NOTFND) {
            /* we missed the actual row, but got the one we're
               looking for */
            *retrieved = 1;
            rc = IX_FND;
        }

        /* errors or PASTEOF are returned as such */
        assert(rc <= 0 || rc == IX_FND || rc == IX_EMPTY || rc == IX_PASTEOF ||
               rc == BDBERR_DEADLOCK);

        if (rc == BDBERR_DEADLOCK) {
            rc = -1;
            *bdberr = BDBERR_DEADLOCK;
        }

        return rc;
    }
    return rc;
}

/**
 * A cursor merging code has the following assumption:
 * - if we move NEXT-like, at any time the current position has the smallest key
 * from both real and shadow pair
 * - if we move PREV-like, the current position is the biggest key
 *
 * This assumption is not valid when either real or shadow btree has no more
 *data
 * and we continue moving in the other btree.   When this happens, the first
 *btree
 * is marked out-of-order.  Any time we try to do a merge, we need to reposition
 * the out-of-order btree, even though this will fail.  Since both btree can be
 * updated in between moves, this repositioning has to be tried at every move
 *
 */
static int bdb_cursor_reorder(bdb_cursor_impl_t *cur, bdb_berkdb_t *berkdb,
                              char *key, int keylen, int how, int *retrieved,
                              int *bdberr)
{
    int rc = 0;

    assert(how == DB_PREV || how == DB_NEXT);
    if (cur->type == BDBC_IX)
        rc = bdb_cursor_reposition_noupdate(cur->ifn, berkdb, key, keylen, how,
                                            bdberr);
    else
        rc = bdb_cursor_reposition_noupdate(cur->ifn, berkdb,
                                            (char *)&cur->genid,
                                            sizeof(cur->genid), how, bdberr);
    if (rc < 0)
        return rc;

    assert(rc == IX_FND || rc == IX_NOTFND || rc == IX_PASTEOF ||
           rc == IX_EMPTY);

    if (rc == IX_FND || rc == IX_NOTFND) {
        /* found something in the right direction */

        berkdb->outoforder_set(berkdb, 0);
        /* if we got a neighboring record in the desired direction
           we are set; we just need to skip the incoming move */
        if (rc == IX_NOTFND) {
            *retrieved = 1;
            rc = IX_FND;
        }
    }

    return rc;
}

/* moves both shadow and real btrees */
static int bdb_cursor_move_and_skip(bdb_cursor_impl_t *cur,
                                    bdb_berkdb_t *berkdb, char *key, int keylen,
                                    int how, int *bdberr)
{
    int retrieved = 0; /* marked if re-locking gets the record we're
                          looking for */
    int rc = 0;

    assert(how == DB_FIRST || how == DB_NEXT || how == DB_PREV ||
           how == DB_LAST);

    /* if we are invalidated, get back a berkeley db cursor */
    if (cur->invalidated && cur->rl == berkdb) {
        rc = bdb_cursor_revalidate(cur, berkdb, how, &retrieved, bdberr);
        if (rc)
            return rc;
        assert(cur->invalidated == 0);
    }

    /* here the cursor is valid; is it not positioned as well and we're moving
     * relatively? */
    if (how != DB_FIRST && how != DB_LAST && berkdb->outoforder_get(berkdb)) {
        rc = bdb_cursor_reorder(cur, berkdb, key, keylen, how, &retrieved,
                                bdberr);
        if (rc < 0)
            return rc;

#if MERGE_DEBUG
        fprintf(stderr, "%d %s:%d reordering rc=%d %llx\n", pthread_self(),
                __FILE__, __LINE__, rc, *(unsigned long long *)key);
#endif

        /* if we have failed to reposition the cursor and this is a relative
         * move,
         * we're done here */
        if (berkdb->outoforder_get(berkdb))
            return rc;
    }

    /* do the dance */
    rc = bdb_cursor_move_and_skip_int(cur, berkdb, how, retrieved,
                                      /*update_shadows*/ 1, bdberr);
    if (rc < 0)
        return rc;
#if MERGE_DEBUG
    fprintf(stderr, "%d %s:%d bdb_cursor_move_and_skip_int rc=%d\n",
            pthread_self(), __FILE__, __LINE__, rc);
#endif

    /* now update the out-of-order flag */
    if (rc == IX_FND) {
        berkdb->outoforder_set(berkdb, 0);
    } else {
        berkdb->outoforder_set(berkdb, 1);
    }

    return rc;
}

static int update_pglogs_from_global_queues(bdb_cursor_impl_t *cur,
                                            unsigned char *fileid, int *bdberr);

/**
 * RETURNS:
 * - IX_FND       : found a record
 * - IX_PASTEOF   : move past the end of btree, left or right (for DB_NEXT and
 *DB_PREV only)
 * - IX_EMPTY     : no rows, or all rows marked deleted
 * - <0           : error, bdberr set
 *
 */
static int bdb_cursor_move_and_skip_int(bdb_cursor_impl_t *cur,
                                        bdb_berkdb_t *berkdb, int how,
                                        int retrieved, int update_shadows,
                                        int *bdberr)
{
    unsigned long long genid = 0;
    int howcrt = how;
    int tmpheader = 0;
    int rc = 0;
    int rc2 = 0;
    db_pgno_t pgno = 0;
    db_pgno_t prev_pgno = 0;
    unsigned char fileid[DB_FILE_ID_LEN];

    assert(how == DB_NEXT || how == DB_PREV || how == DB_FIRST ||
           how == DB_LAST);

    if (gbl_new_snapisol && cur->rl == berkdb && update_shadows) {
        rc2 = cur->rl->fileid(cur->rl, fileid, bdberr);
        if (rc2 < 0) {
            logmsg(LOGMSG_FATAL, "get fileid failed\n");
            abort();
        }
    }

    do {

        if (!retrieved) {
            rc = berkdb->move(berkdb, howcrt, bdberr);
            if (rc < 0)
                return rc;

#if MERGE_DEBUG
            fprintf(stderr, "%d %s:%d berkdb->move rc=%d\n", pthread_self(),
                    __FILE__, __LINE__, rc);
#endif
        } else {
            /* Move the cursor for the next iteration. */
            retrieved = 0;
        }

        if (gbl_new_snapisol && cur->rl == berkdb && update_shadows) {
            rc2 = cur->rl->pageindex(cur->rl, &pgno, NULL, bdberr);
            if (rc2 < 0) {
                logmsg(LOGMSG_FATAL, "get pgno failed\n");
                abort();
            }
            if (prev_pgno != pgno || pgno == 0) {
                rc2 = bdb_btree_update_shadows_with_trn_pglogs(cur, &pgno,
                                                               fileid, bdberr);
                if (rc2 < 0) {
                    logmsg(LOGMSG_FATAL, "bdb_btree_update_shadows_with_trn_pglogs failed\n");
                    abort();
                }
                prev_pgno = pgno;
            }
        }

        if (rc == IX_NOTFND || rc == IX_EMPTY) /* exhausted all rows, if any */
        {
            if (how == DB_FIRST || how == DB_LAST)
                rc = IX_EMPTY;
            else
                rc = IX_PASTEOF;
            break;
        }

        /* If we've found something, see if we should skip it. */
        if (IX_FND == rc) {
            rc = berkdb_get_genid(cur, berkdb, &genid, bdberr);
            if (rc)
                return rc;

            rc = bdb_tran_deltbl_isdeleted(cur->ifn, genid,
                                           (cur->sd == berkdb) ? 1 : 0, bdberr);
            if (rc < 0)
                return -1;
        }

        /* are we done? */
        if (rc == IX_FND) {
            /* This is updated inside of bdb_move_merge. */
            break;
        }

#if MERGE_DEBUG
        fprintf(stderr, "%s ignored %llx\n",
                (cur->sd == berkdb) ? "shadow" : "real", genid);
#endif

        /* ok, the found row is marked deleted, try to get
           the next one preserving the direction */
        switch (howcrt) {
        case DB_FIRST:
            howcrt = DB_NEXT;
            break;
        case DB_LAST:
            howcrt = DB_PREV;
            break;
        }
    } while (1);

    return rc;
}

/* find and skip for both real and shadow */
static int bdb_cursor_find_and_skip(bdb_cursor_impl_t *cur,
                                    bdb_berkdb_t *berkdb, void *key, int keylen,
                                    int how, int keylen_incremented,
                                    int update_shadows, int *bdberr)
{
    unsigned long long genid = 0;
    int howcrt = how;
    int rc = 0;
    int rc2 = 0;
    db_pgno_t pgno = 0;
    db_pgno_t prev_pgno = 0;
    unsigned char fileid[DB_FILE_ID_LEN];
    int done_update_shadows;

    if (gbl_new_snapisol && cur->rl == berkdb && update_shadows) {
        rc2 = cur->rl->fileid(cur->rl, fileid, bdberr);
        if (rc2 < 0) {
            printf("get fileid failed\n");
            abort();
        }
    }

    /* are we invalidated? relock */
    if (cur->invalidated && cur->rl == berkdb) {
        assert(cur->rl != NULL);

        cur->invalidated = 0;

        rc = cur->rl->lock(cur->rl, cur->curtran, bdberr);
        if (rc)
            return rc;
    }

    do {
        done_update_shadows = 0;

        rc = berkdb->find(berkdb, key, keylen, howcrt, bdberr);
#if MERGE_DEBUG
        fprintf(stderr, "%d %s:%d find() how=%d returned rc=%d\n",
                pthread_self(), __FILE__, __LINE__, how, rc);
#endif
        if (rc < 0)
            return rc;

        if (gbl_new_snapisol && cur->rl == berkdb && update_shadows) {
            char *key_tmp;
            int keylen_tmp;
            int cmprc = 1;

            if (cur->type == BDBC_DT) {
                berkdb->key(berkdb, &key_tmp, bdberr);
                berkdb->keysize(berkdb, &keylen_tmp, bdberr);
                cmprc = memcmp(key_tmp, key,
                               keylen < keylen_tmp ? keylen : keylen_tmp);
            }

            /* only update shadow if we dont find what we want */
            if (cur->type != BDBC_DT || howcrt == DB_FIRST ||
                howcrt == DB_LAST || rc != IX_FND || cmprc != 0) {
                done_update_shadows = 1;

                rc2 = cur->rl->pageindex(cur->rl, &pgno, NULL, bdberr);
                if (rc2 < 0) {
                    logmsg(LOGMSG_FATAL, "get pgno failed\n");
                    abort();
                }
                if (prev_pgno != pgno || pgno == 0) {
                    rc2 = bdb_btree_update_shadows_with_trn_pglogs(
                        cur, &pgno, fileid, bdberr);
                    if (rc2 < 0) {
                        logmsg(LOGMSG_FATAL, "bdb_btree_update_shadows_with_trn_pglogs "
                               "failed\n");
                        abort();
                    }
                    prev_pgno = pgno;
                }
            }
        }

        if (rc == IX_EMPTY) /* exhausted all rows, if any */
            break;
        if (rc == IX_NOTFND) {
            /*
            fprintf( stderr, "%d %s:%d returing %d\n",
                  pthread_self(), __FILE__, __LINE__, IX_PASTEOF);
             */

            rc = IX_PASTEOF;
            break;
        }
        if (rc == IX_PASTEOF)
            break;
        /* hack around temptable semantic of returning IX_FND when
           looking for something bigger than anything inside, and returning
           the last row */
        if (rc == IX_FND && cur->sd == berkdb) {
            char *key_tmp;
            int keylen_tmp;
            rc = berkdb->key(berkdb, &key_tmp, bdberr);
            if (rc < 0)
                return rc;
            rc = berkdb->keysize(berkdb, &keylen_tmp, bdberr);
            if (rc < 0)
                return rc;

            /*
               If this is an index cursor it is not pgorder (pgorder not
               supported on indexes).
               If this is a dta cursor, it is not pgorder (as we've put it in
               addcur).
               If this is a blob cursor, it is not pgorder (pgorder not
               supported on blobs).
            */
            assert(cur->pageorder == 0);

            int cmprc;
            if (keylen_incremented && keylen > keylen_tmp) {
                assert(keylen <= (keylen_tmp + 1));
                cmprc = memcmp(key_tmp, key, keylen_tmp);
            } else {
                assert(keylen <= keylen_tmp);
                cmprc = memcmp(key_tmp, key, keylen);
            }

            if (cmprc < 0) {
                /* "last" gave me a row that is smaller than me, ouch */
                rc = IX_PASTEOF;
                break;
            }
        }

        rc = berkdb_get_genid(cur, berkdb, &genid, bdberr);
        if (rc)
            return rc;

        rc = bdb_tran_deltbl_isdeleted(cur->ifn, genid,
                                       (cur->sd == berkdb) ? 1 : 0, bdberr);
        if (rc < 0)
            return -1;

        /* The found row is marked deleted, update shadow */
        if (rc && gbl_new_snapisol && cur->rl == berkdb && update_shadows &&
            !done_update_shadows) {
            rc2 = cur->rl->pageindex(cur->rl, &pgno, NULL, bdberr);
            if (rc2 < 0) {
                logmsg(LOGMSG_FATAL, "get pgno failed\n");
                abort();
            }
            if (prev_pgno != pgno || pgno == 0) {
                rc2 = bdb_btree_update_shadows_with_trn_pglogs(cur, &pgno,
                                                               fileid, bdberr);
                if (rc2 < 0) {
                    logmsg(LOGMSG_FATAL, "bdb_btree_update_shadows_with_trn_pglogs failed\n");
                    abort();
                }
                prev_pgno = pgno;
            }
        }

        /* ok, the found row is marked deleted, try to get
           the next one preserving the direction */
        switch (howcrt) {
        case DB_SET_RANGE:
            howcrt = DB_NEXT;
            break;
        case DB_SET: {
            if (rc != IX_FND)
                return IX_NOTFND; /* exact match, failed */
        }
        }
    } while (rc != IX_FND);

    return rc;
}

/**
 *
 *  MERGING ALGORITHM
 *
 *  - we merge between a real and a shadow btree, if any
 *  - we describe the most complicated form, in snapshot/serializable mode
 *  steps are skipped for simpler transactions
 *  - this is used for both move and find
 *
 *  1) check if the real row was consumed, and do a move if so
 *     - note: a real move will use skip list if one is present to skip
 *       deleted rows
 *  2) check if there are logs to update my snapshot, and apply them
 *     if so;
 *     - if any log was applied, create shadow berkdb cursors if we
 *       don't have one
 *     - relative moves (next, prev) need to reposition the
 *       shadow cursor as close as possible to original
 *       real position (to account for possible new inserted
 *       shadow rows)
 *     - absolute moves (i.e find, first, last) do not need to
 *       sync the shadow cursor
 *   3) check if the shadow row was consumed, and if so, move shadow
 *   4) merge shadow and real row (lower key is preferred); the row
 *      preferred is marked as consumed;
 *     - note: if shadow and real are the same, the last consumed
 *       real key is checked (this account for shadow dups of real
 *       data)
 *
 */

/* this is a shadowed btree merging FIND function */
static int bdb_cursor_find_merge(bdb_cursor_impl_t *cur, void *key, int keylen,
                                 int *bdberr)
{

    /* Variables which contain 'real' data from the actual table. */
    char *dta_rl = NULL;
    int dtasize_rl = 0;
    char *key_rl = NULL;
    int keysize_rl = 0;
    unsigned long long genid_rl;
    uint8_t ver_rl;
    int got_rl;

    /* Variables containing 'page-order' data from the virtual stripe. */
    char *dta_po = NULL;
    int dtasize_po = 0;
    char *key_po = NULL;
    int keysize_po = 0;
    unsigned long long genid_po;
    uint8_t ver_po;
    int got_po;

    /* Variables containing 'shadow' data from the shadow-tables. */
    char *dta_sd = NULL;
    int dtasize_sd = 0;
    char *key_sd = NULL;
    int keysize_sd = 0;
    unsigned long long genid_sd;
    int got_sd = 0;

    /* Utility variables. */
    unsigned long long *genid_ck;
    int rc = 0;
    struct odh odh;

    /* NOTE:
       - when we do an absolute move (find), we need to reset "used"
     */
    cur->used_rl = 0;
    cur->used_sd = 0;

step1:
    got_rl = 0;
    got_po = 0;

    /* STEP 1*/
    if (cur->rl) /* shadow data will have no real */
    {
        /*
         * DB_SET_RANGE doesn't work with page-order cursors: the semantics
         * will sometimes cause a cursor to move off-page.  Non-page order
         * cursors call the normal flavor of __bam_c_next and get a reasonable
         * answer.  Page-order cursors can move offpage to (curpage + 1) and
         * get a ridiculous answer.  Worse is that since the cursor can move to
         * a different page, we no longer have a lock on the original page, and
         * we no longer know if the log-updates should fall on the right or left
         * side of the cursor.
         */
        int how = cur->pageorder ? DB_SET : DB_SET_RANGE;

        rc = bdb_cursor_find_and_skip(cur, cur->rl, key, keylen, how, 0, 1,
                                      bdberr);

#if MERGE_DEBUG
        fprintf(stderr, "%d %s:%d rc=%d used_rl=%d [%d]\n", pthread_self(),
                __FILE__, __LINE__, rc, cur->used_rl, how);

        print_cursor_keys(cur, BDB_SHOW_RL);
#endif

        if (rc < 0)
            return rc;
        /* a failed absolute move is always consumed */
        if (rc != IX_FND) {
            if (cur->trak) {
                char *buf;
                hexdumpbuf(key, keylen, &buf);
                logmsg(LOGMSG_USER, "Cur %p did not find 0x%s in real table, "
                                "rc=%d; setting used_rl=1\n",
                        cur, buf, rc);
                free(buf);
            }
            cur->used_rl = 1;
        } else {
            cur->used_rl = 0;
            cur->rl->outoforder_set(cur->rl, 0);

            if (likely(cur->rl->get_everything)) {
                rc = cur->rl->get_everything(cur->rl, &dta_rl, &dtasize_rl,
                                             &key_rl, &keysize_rl, &ver_rl,
                                             bdberr);
            } else {
                rc = cur->rl->dta(cur->rl, &dta_rl, bdberr);
                if (rc < 0)
                    return rc;
                rc = cur->rl->dtasize(cur->rl, &dtasize_rl, bdberr);
                if (rc < 0)
                    return rc;
                rc = cur->rl->key(cur->rl, &key_rl, bdberr);
                if (rc < 0)
                    return rc;
                rc = cur->rl->keysize(cur->rl, &keysize_rl, bdberr);
                if (rc < 0)
                    return rc;
                rc = cur->rl->ver(cur->rl, &ver_rl, bdberr);
                if (rc < 0)
                    return rc;
            }
            if (rc < 0)
                return rc;
            berkdb_get_genid_from_dtakey(cur, dta_rl, key_rl, &genid_rl,
                                         bdberr);
            got_rl = 1;

            if (cur->trak) {
                logmsg(LOGMSG_USER, "Cur %p found 0x", cur);
                hexdump(LOGMSG_USER, key_rl, keysize_rl);
                logmsg(LOGMSG_USER, " data 0x");
                hexdump(LOGMSG_USER, dta_rl, dtasize_rl);
                logmsg(LOGMSG_USER, " in real table.\n");
            }
        }
    } else {
        if (cur->trak) {
            char *buf;
            hexdumpbuf(key, keylen, &buf);
            logmsg(LOGMSG_USER, "Cur %p didn't check real table for 0x%s because "
                            "cur->rl is NULL\n",
                    cur, buf);
            free(buf);
        }
    }

    /*
     * The 'switch_stripe' routine used for cursor-moves will close the real and
     * shadow cursors when it switches to the virtual stripe.   Page-ordered
     * cursors (unlike normal cursors) use the virtual table as their shadow
     * table rather than the 'actual' shadow-table (which they won't have).  So
     * if this is a normal cursor, the merge at the end of this routine will be
     * between the real and shadow tables, or will just be the data in the
     * virtual table if the cursor has moved past the last stripe.  If this is a
     * page-order cursor, the merge at the end of this routine will be between
     * the real-cursor and the virtual stripe.
     */

    /* STEP 2 - this could open 'addcur'. */
    rc = bdb_btree_update_shadows(cur, DB_SET, bdberr);
    if (rc < 0)
        return rc;

    /* Check the virtual stripe */
    if (cur->addcur && cur->type == BDBC_DT && 0 == got_rl && cur->pageorder) {
        /* Temptable find-exact semantics require that I malloc the key. */
        void *fndkey = malloc(keylen);

        /* Copy key. */
        memcpy(fndkey, key, keylen);

        /* Find my record. */
        rc = bdb_temp_table_find_exact(cur->state, cur->addcur, fndkey, keylen,
                                       bdberr);
        if (rc < 0)
            return rc;

        /* TODO - functionize this. */
        if (rc == IX_FND) {
            genid_ck = bdb_temp_table_key(cur->addcur);

            /**
             * Check for a 'real' genid in the addcur table.  This will only
             * occur
             * if the cursor is a page-order cursor, and if this record was
             * added
             * via the update-shadows step from bdb_osql_log_run_optimized or
             * bdb_osql_log_run_unoptimzed.  Handle both cases inline here.
             */
            if (!is_genid_synthetic(*genid_ck)) {
                bdb_osql_log_addc_ptr_t *addptr;
                char *dta;
                int dtalen;

                /* Sanity check. */
                assert(cur->pageorder != 0);

                /* Retrieve the header. */
                addptr = bdb_temp_table_data(cur->addcur);

                /* If it's on the skip-list, go to the next. */
                if (bdb_tran_deltbl_isdeleted(cur->ifn, *genid_ck, 0, bdberr))
                    return IX_NOTFND;

                /* Retrieve the size. */
                dtalen = bdb_temp_table_datasize(cur->addcur);

                /* Retrieve the actual row if this is the optimized codepath. */
                if (dtalen > 0 && bdb_osql_log_is_optim_data(addptr)) {
                    bdb_osql_log_addc_ptr_t *newptr;
                    int rowlen;
                    unsigned long long *pgenid;

                    /* Rebuild the row from the logfiles. */
                    rc = bdb_osql_log_get_optim_data_addcur(
                        cur->state, &addptr->lsn, (void **)&newptr, &rowlen,
                        bdberr);
                    if (rc < 0)
                        return rc;

                    /* Update flags. */
                    newptr->flag = 0;

                    /* Update lsn. */
                    newptr->lsn = addptr->lsn;

                    /* Update the temp table. */
                    rc =
                        bdb_temp_table_update(cur->state, cur->addcur, genid_ck,
                                              8, newptr, rowlen, bdberr);
                    if (rc < 0)
                        return rc;

                    /* Malloc pgenid for find_exact. */
                    pgenid = (unsigned long long *)malloc(sizeof(*pgenid));

                    /* Copy it. */
                    *pgenid = *genid_ck;

                    /* Re-find the newly inserted position */
                    rc = bdb_temp_table_find_exact(cur->state, cur->addcur,
                                                   (char *)pgenid, 8, bdberr);
                    if (rc != IX_FND) {
                        logmsg(LOGMSG_ERROR, "%s: fail to retrieve back the "
                                        "updated row rc=%d bdberr=%d\n",
                                __func__, rc, *bdberr);
                        rc = -1; /* we have to find this row back */
                    }

                    /* Retrieve the header. */
                    addptr = bdb_temp_table_data(cur->addcur);

                    /* Retrieve the size. */
                    dtalen = bdb_temp_table_datasize(cur->addcur);
                }

                /* The payload is just after the header. */
                dta = (char *)addptr + sizeof(*addptr);

                /* Remove length of the header. */
                dtalen -= sizeof(*addptr);

                /* Retrieve odh-unpacked row data. */
                if (cur->addcur_use_odh) {
                    if (bdb_unpack(cur->state, dta, dtalen, cur->addcur_odh,
                                   MAXRECSZ, &odh, NULL)) {
                        *bdberr = BDBERR_UNPACK;
                        return -1;
                    }

                    dta_po = odh.recptr;
                    dtasize_po = odh.length;
                    ver_po = odh.csc2vers;
                }
                /* No ondisk headers. */
                else {
                    dta_po = dta;
                    dtasize_po = dtalen;
                    ver_po = cur->state->version;
                }

                key_po = bdb_temp_table_key(cur->addcur);
                keysize_po = bdb_temp_table_keysize(cur->addcur);
                genid_po = *(unsigned long long *)key_po;
                got_po = 1;

                if (cur->trak) {
                    logmsg(LOGMSG_USER, "Cur %p found 0x", cur);
                    hexdump(LOGMSG_USER, key_po, keysize_po);
                    logmsg(LOGMSG_USER, " data 0x");
                    hexdump(LOGMSG_USER, dta_po, dtasize_po);
                    logmsg(LOGMSG_USER, " in virtual stripe.\n");
                }
            } else {
                /* Synthetic genids are handled in the db layer. */
                abort();
            }
        }
    }

    /* STEP 2.5 */
    /* this is a mid-step required by snapisol mode that have
       no protection from rowlock logical transactions;
       if STEP 1 landed on a row with genid older than our start
       time, but which was committed by a younger transaction
       (which should be skipped), we need to go back and redo
       step 1.  You won't find the same record again because
       'find_and_skip' already checks for deltbl_isdeleted
       records. */
    if (got_rl) {
        rc = bdb_tran_deltbl_isdeleted(cur->ifn, genid_rl, 0, bdberr);
        if (rc < 0)
            return rc;

        if (rc == 1) {
            if (cur->used_rl == 0) {
                if (cur->trak) {
                    logmsg(LOGMSG_USER, 
                        "%ld %s:%d tran %p skipping %llx, startgenid=%llx\n",
                        pthread_self(), __FILE__, __LINE__, cur->shadow_tran,
                        bdb_genid_to_host_order(genid_rl),
                        bdb_genid_to_host_order(cur->shadow_tran->startgenid));
                }
            }

            /* Cleanup. */
            dta_rl = NULL;
            dtasize_rl = 0;
            key_rl = NULL;
            keysize_rl = 0;
            dta_po = NULL;
            dtasize_po = 0;
            key_po = NULL;
            keysize_po = 0;

            goto step1;
        }
    }

    /* STEP 3 */
    if (cur->sd && !cur->shadow_tran->check_shadows)
        cur->used_sd = 1;

    if (!cur->pageorder && cur->sd && cur->shadow_tran->check_shadows) {
#if MERGE_DEBUG
        print_cursor_keys(cur, BDB_SHOW_SD);
#endif
        rc = bdb_cursor_find_and_skip(cur, cur->sd, key, keylen, DB_SET_RANGE,
                                      0, 0, bdberr);
        if (rc < 0)
            return rc;

        /* a failed absolute move is always consumed */
        if (rc != IX_FND) {
            cur->used_sd = 1;
#if MERGE_DEBUG
            fprintf(stderr,
                    "NOT FOUND rc=%d; left pointing at the last row key_sd=\n",
                    rc);
            print_cursor_keys(cur, BDB_SHOW_SD);
            fprintf(stderr, " -- marked SD used\n");
#endif
        } else {
            cur->used_sd = 0;
            cur->sd->outoforder_set(cur->sd, 0);

            rc = cur->sd->dta(cur->sd, &dta_sd, bdberr);
            if (rc < 0)
                return rc;
            rc = cur->sd->dtasize(cur->sd, &dtasize_sd, bdberr);
            if (rc < 0)
                return rc;
            rc = cur->sd->key(cur->sd, &key_sd, bdberr);
            if (rc < 0)
                return rc;
            rc = cur->sd->keysize(cur->sd, &keysize_sd, bdberr);
            if (rc < 0)
                return rc;

            rc = berkdb_get_genid_from_dtakey(cur, dta_sd, key_sd, &genid_sd,
                                              bdberr);
            if (rc)
                return rc;

            if (cur->trak) {
                logmsg(LOGMSG_USER, "Cur %p found 0x", cur);
                hexdump(LOGMSG_USER, key_sd, keysize_sd);
                logmsg(LOGMSG_USER, " data 0x");
                hexdump(LOGMSG_USER, dta_sd, dtasize_sd);
                logmsg(LOGMSG_USER, " in shadow table.\n");
            }
#if MERGE_DEBUG
            printf("FOUND:\n\tkeylen=%d\n\tkey=\"", keysize_sd);
            hexdump(key_sd, keysize_sd);
            printf("\"\n\tdatalen=%d\n\tdata=\"", dtasize_sd);
            hexdump(dta_sd, dtasize_sd);
            printf("\"\n");
#endif
        }
    } else {
        if (cur->trak) {
            char *buf;
            hexdumpbuf(key, keylen, &buf);
            logmsg(LOGMSG_USER, "Cur %p did not check shadows for 0x%s, cur->sd=%p "
                            "check_shadows=%d\n",
                    cur, buf, cur->sd,
                    (cur->shadow_tran != NULL) ? cur->shadow_tran->check_shadows
                                               : 0);
            free(buf);
        }
    }

    /* STEP 4 */

    /*   Page order cursors merge on dta_rl and dta_po.  At most one of them
         will have data. */
    if (cur->pageorder && cur->type == BDBC_DT) {
#if MERGE_DEBUG
        printf("find bdb_btree_merge RL_PO\n");
        if (dta_rl) {
            printf("dta_rl == \n");
            hexdump(dta_rl, dtasize_rl);
        } else {
            printf("dta_rl == NULL\n");
        }
        if (dta_po) {
            printf("dta_po == \n");
            hexdump(dta_po, dtasize_po);
        } else {
            printf("dta_po == NULL\n");
        }
        if (key_rl) {
            printf("key_rl == \n");
            hexdump(key_rl, keysize_rl);
        } else {
            printf("key_rl == NULL\n");
        }
        if (key_po) {
            printf("key_po == \n");
            hexdump(key_po, keysize_po);
        } else {
            printf("key_po == NULL\n");
        }
#endif
        return bdb_btree_merge(cur, -1, -1, -1, dta_rl, dtasize_rl, dta_po,
                               dtasize_po, /* use this to set cur->data */
                               key_rl, keysize_rl, key_po,
                               keysize_po,         /* keys */
                               genid_rl, genid_po, /* genids */
                               ver_rl, DB_NEXT);   /* bias */

    }
    /* Others use the real (dta_rl), or virtual (dta_po) tables, but not both.
       */
    else if (dta_po && cur->type == BDBC_DT) {
#if MERGE_DEBUG
        printf("find bdb_btree_merge PO_SD\n");
        if (dta_po) {
            printf("dta_po == \n");
            hexdump(dta_po, dtasize_po);
        } else {
            printf("dta_po == NULL\n");
        }
        if (dta_sd) {
            printf("dta_sd == \n");
            hexdump(dta_sd, dtasize_sd);
        } else {
            printf("dta_sd == NULL\n");
        }
        if (key_po) {
            printf("key_po == \n");
            hexdump(key_po, keysize_po);
        } else {
            printf("key_po == NULL\n");
        }
        if (key_sd) {
            printf("key_sd == \n");
            hexdump(key_sd, keysize_sd);
        } else {
            printf("key_sd == NULL\n");
        }
#endif
        /* The '_sd' variables will be NULL for this case. */
        return bdb_btree_merge(cur, -1, -1, -1, dta_po, dtasize_po, dta_sd,
                               dtasize_sd, /* use this to set cur->data */
                               key_po, keysize_po, key_sd,
                               keysize_sd,         /* keys */
                               genid_po, genid_sd, /* genids */
                               ver_po, DB_NEXT);   /* bias */
    }
    /* Normal case, before reaching the virtual stripe. */
    else if (cur->type == BDBC_DT) {
#if MERGE_DEBUG
        printf("find bdb_btree_merge BDBC_DT\n");
        if (dta_rl) {
            printf("dta_rl == \n");
            hexdump(dta_rl, dtasize_rl);
        } else {
            printf("dta_rl == NULL\n");
        }
        if (dta_sd) {
            printf("dta_sd == \n");
            hexdump(dta_sd, dtasize_sd);
        } else {
            printf("dta_sd == NULL\n");
        }
        if (key_rl) {
            printf("key_rl == \n");
            hexdump(key_rl, keysize_rl);
        } else {
            printf("key_rl == NULL\n");
        }
        if (key_sd) {
            printf("key_sd == \n");
            hexdump(key_sd, keysize_sd);
        } else {
            printf("key_sd == NULL\n");
        }
#endif
        return bdb_btree_merge(cur, -1, -1, -1, dta_rl, dtasize_rl, dta_sd,
                               dtasize_sd, /* use this to set cur->data */
                               key_rl, keysize_rl, key_sd,
                               keysize_sd,         /* keys */
                               genid_rl, genid_sd, /* genids */
                               ver_rl, DB_NEXT);   /* bias */
    }
    /* Index case. */
    else {
        assert(cur->type == BDBC_IX);
#if MERGE_DEBUG
        /*
        if (cur->state->ixdta[cur->idx] && dta_rl)
        {
           struct datacopy_info *info = (struct datacopy_info *) cur->datacopy;
           info->datacopy = dta_rl;
           info->size = dtasize_rl;
           cur->unpacked_datacopy = NULL;
        }
        */
        printf("find bdb_btree_merge BDBC_IX\n");
        if (dta_rl) {
            printf("dta_rl == \n");
            hexdump(dta_rl, dtasize_rl);
        } else {
            printf("dta_rl == NULL\n");
        }
        if (dta_sd) {
            printf("dta_sd == \n");
            hexdump(dta_sd, dtasize_sd);
        } else {
            printf("dta_sd == NULL\n");
        }
        if (key_rl) {
            printf("key_rl == \n");
            hexdump(key_rl, keysize_rl);
        } else {
            printf("key_rl == NULL\n");
        }
        if (key_sd) {
            printf("key_sd == \n");
            hexdump(key_sd, keysize_sd);
        } else {
            printf("key_sd == NULL\n");
        }
#endif
        return bdb_btree_merge(cur, -1, -1, -1, dta_rl, dtasize_rl, dta_sd,
                               dtasize_sd, /* use this to set cur->data */
                               key_rl, keysize_rl, key_sd,
                               keysize_sd,         /* keys */
                               genid_rl, genid_sd, /* genids */
                               0, DB_NEXT);        /* bias */
    }
}

static void set_datacopy(bdb_cursor_impl_t *cur, void *dta, int len)
{
    struct datacopy_info *info;

    if (cur->type == BDBC_IX && cur->state->ixdta[cur->idx]) {
        info = (struct datacopy_info *)cur->datacopy;
        info->datacopy = dta;
        info->size = len;
        cur->unpacked_datacopy = NULL;
        /*fprintf(stderr, "SETTING %p dta=%p sz=%d\n", cur, info->datacopy,
         * info->size);*/
    }
}

/* this is a shadowed btree merging function */
static int bdb_cursor_move_merge(bdb_cursor_impl_t *cur, int how, int *bdberr)
{

    char *dta_rl = NULL;
    int dtasize_rl = 0;
    char *key_rl = NULL;
    int keysize_rl = 0;
    char *dta_sd = NULL;
    int dtasize_sd = 0;
    char *key_sd = NULL;
    int keysize_sd = 0;
    unsigned long long *genid_ck;
    unsigned long long genid_rl;
    unsigned long long genid_sd;
    char last_key[MAXRECSZ];
    int last_keylen = 0;
    int stripe_rl = -1;
    int page_rl = -1;
    int index_rl = -1;
    int rc = 0;
    uint8_t ver_rl = 0;
    int got_rl;
    int crt_how;
    struct odh odh;

    /* NOTE:
       - when we do a relative move (next, prev), it is
       important to know which of the berkdb_bt is used/pointed to
       This one will have to move again and not the other one!
       - when we do an absolute move (last, next), we need to do
       a reset of used
     */
    if (how != DB_NEXT && how != DB_PREV) {
        cur->used_rl = 0;
        cur->used_sd = 0;
    }
    /* How is 'DB_NEXT' or 'DB_PREV'. */
    else {
        /*
           One another note:
           Since cur->data points to the buffer inside berkdb rl,
           any move on that berkdb affects the position at which
           the cursor points.  This is NOT good if we need to
           reposition the shadow!
           Store the position here

           You have to copy this immediately: the update-shadows step can modify
           the shadow's 'outoforder' flag and force a lookup, but it's too late
           to
           copy then.
         */
        assert(cur->datalen <= sizeof(last_key));
        last_keylen = cur->datalen;
        memcpy(last_key, cur->data, last_keylen);
        if (cur->type == BDBC_IX &&
            bdb_keycontainsgenid(cur->state, cur->idx)) {
            memcpy(last_key + last_keylen, &cur->genid, sizeof(cur->genid));
        }
    }

step1:
    got_rl = 0;

    /* STEP 1 */
    if (cur->rl) {
#if MERGE_DEBUG
        fprintf(stderr, "%d %s:%d used_rl=%d cur->genid=%llx [%d]\n",
                pthread_self(), __FILE__, __LINE__, cur->used_rl, cur->genid,
                how);
#endif
        if ((how != DB_NEXT && how != DB_PREV) || cur->used_rl) {

            rc = bdb_cursor_move_and_skip(cur, cur->rl, cur->data, cur->datalen,
                                          how, bdberr);
            if (rc < 0)
                return rc;

#if MERGE_DEBUG
            fprintf(stderr, "%d %s:%d rc=%d used_rl=%d [%d]\n", pthread_self(),
                    __FILE__, __LINE__, rc, cur->used_rl, how);

            print_cursor_keys(cur, BDB_SHOW_RL);
#endif

            /* a failed absolute move is always consumed */
            if (!cur->used_rl) {
                if (rc != IX_FND)
                    cur->used_rl = 1;
            } else {
                if (rc == IX_FND)
                    cur->used_rl = 0;
            }
        } else
            rc = IX_FND; /* we did not move, reuse same position */

        if (rc == IX_FND) {
#if 0
         cur->outoforder_rl = 0;
#endif
            if (likely(cur->rl->get_everything)) {
                rc = cur->rl->get_everything(cur->rl, &dta_rl, &dtasize_rl,
                                             &key_rl, &keysize_rl, &ver_rl,
                                             bdberr);
            } else {
                rc = cur->rl->dta(cur->rl, &dta_rl, bdberr);
                if (rc < 0)
                    return rc;
                rc = cur->rl->dtasize(cur->rl, &dtasize_rl, bdberr);
                if (rc < 0)
                    return rc;
                rc = cur->rl->key(cur->rl, &key_rl, bdberr);
                if (rc < 0)
                    return rc;
                rc = cur->rl->keysize(cur->rl, &keysize_rl, bdberr);
                if (rc < 0)
                    return rc;
                rc = cur->rl->ver(cur->rl, &ver_rl, bdberr);
                if (rc < 0)
                    return rc;
            }
            if (rc < 0)
                return rc;
            berkdb_get_genid_from_dtakey(cur, dta_rl, key_rl, &genid_rl,
                                         bdberr);

            /* Get page and index. */
            rc = cur->rl->pageindex(cur->rl, &page_rl, &index_rl, bdberr);
            if (rc < 0)
                return rc;

            /* Get current stripe. */
            stripe_rl = cur->idx;

            got_rl = 1;
        }
        /* Print information. */
        if (cur->trak) {
            logmsg(LOGMSG_USER, 
                    "Cur %p idx-real-move how=%d rc=%d srch: keylen=%d key=0x",
                    cur, how, rc, last_keylen);
            hexdump(LOGMSG_USER, last_key, last_keylen);

            if (IX_FND == rc) {
                logmsg(LOGMSG_USER, " found: keylen=%d key=0x", keysize_rl);
                hexdump(LOGMSG_USER, key_rl, keysize_rl);
                logmsg(LOGMSG_USER, " stripe %d page %d idx %d", cur->idx, page_rl,
                        index_rl);
            } else {
                logmsg(LOGMSG_USER, " (not found)");
            }
            logmsg(LOGMSG_USER, "\n");
        }
    }

    /* are we on the shadow "add" stripe ? */
    if (!cur->rl && !got_rl && cur->type == BDBC_DT &&
        cur->idx == cur->state->attr->dtastripe && cur->addcur) {
        crt_how = how;
        int skip;

        /* Update shadows could have added something to addcur.  A simple
         * 'next' will return the wrong record if this happened.  If
         * something was added, reposition the cursor. */
        if ((crt_how == DB_NEXT || crt_how == DB_PREV) && cur->repo_addcur &&
            cur->agenid != 0) {
            unsigned long long *agenid = malloc(sizeof(*agenid));

            /* Set the reposition genid. */
            *agenid = cur->agenid;

            /* Reposition the cursor. */
            rc = bdb_temp_table_find_exact(cur->state, cur->addcur,
                                           (char *)agenid, 8, bdberr);

            /* Things shouldn't be disappearing from addcur. */
            assert(rc == IX_FND);

            if (cur->trak) {
                logmsg(LOGMSG_USER, "Cur %p repositioned to genid %llx.\n", cur,
                        cur->agenid);
            }
        }
    next:
        skip = 0;
        rc = bdb_temp_table_move(cur->state, cur->addcur, crt_how, bdberr);
        if (rc)
            return rc;

        genid_ck = bdb_temp_table_key(cur->addcur);

        cur->agenid = *genid_ck;

        cur->repo_addcur = 0;

        if (cur->trak) {
            logmsg(LOGMSG_USER, "Cur %p processing addcur %p genid %llx.\n", cur,
                    cur->addcur, *genid_ck);
        }

        /**
         * Check for a 'real' genid in the addcur table.  This will only occur
         * if the cursor is a page-order cursor, and if this record was added
         * via the update-shadows step from bdb_osql_log_run_optimized or
         * bdb_osql_log_run_unoptimzed.  Handle both cases inline here.
         */
        if (!is_genid_synthetic(*genid_ck)) {
            bdb_osql_log_addc_ptr_t *addptr;
            char *dta;
            int dtalen;

            /* Sanity check. */
            assert(cur->pageorder != 0);

            /* Sanity check 2. */
            assert(how == DB_FIRST || how == DB_NEXT || how == DB_LAST);

            /* Retrieve the header. */
            addptr = bdb_temp_table_data(cur->addcur);

            /* Change first to next. */
            if (crt_how == DB_FIRST)
                crt_how = DB_NEXT;

            /* Change last to prev. */
            if (crt_how == DB_LAST)
                crt_how = DB_PREV;

            /* If we've already seen this record this scan, go to the next. */
            if (cur->vs_skip) {
                unsigned long long *srec;
                int memc = 0;

                /* We can rely on a simple 'next' until update_shadows inserts a
                 * record into the vs_skip table.  Update-shadows will light the
                 * 'new_skip' flag when it does this. */
                if (cur->new_skip) {
                    /* Print a message. */
                    if (cur->trak) {
                        logmsg(LOGMSG_USER, "Cur %p reposition vs_skip to %llx.\n",
                                cur, *genid_ck);
                    }

                    /* Temp_table semantics require that I malloc the key. */
                    srec = (unsigned long long *)malloc(sizeof(*srec));

                    /* Copy the current key. */
                    memcpy(srec, genid_ck, sizeof(*srec));

                    /* Find this record. */
                    int find_rc =
                        bdb_temp_table_find(cur->state, cur->vs_skip,
                                            (char *)srec, 8, NULL, bdberr);

                    /* Temp_table semantics also require that we free key if not
                     * found */
                    if (find_rc != IX_FND)
                        free(srec);
                    if (find_rc) {
                        if (cur->trak) {
                            logmsg(LOGMSG_USER, 
                                    "Cur %p vs_skip does not contain genid %llx.\n",
                                    cur, *genid_ck);
                        }
                    }

                    /* Grab the latest genid. */
                    srec = bdb_temp_table_key(cur->vs_skip);

                    /* Work around temptable semantics- next record will be
                     * invalid. */
                    if (NULL == srec || memcmp(srec, genid_ck, 8) < 0) {
                        cur->last_skip = 1;
                    } else {
                        cur->last_skip = 0;
                    }

                    /* We don't have to reposition next time. */
                    cur->new_skip = 0;
                }

                /* Grab the current temptable key. */
                if (cur->last_skip) {
                    srec = NULL;
                } else {
                    srec = bdb_temp_table_key(cur->vs_skip);
                }

                /* Print current vs_skip key. */
                if (cur->trak) {
                    if (srec) {
                        logmsg(LOGMSG_USER, "Cur %p found vs_skip key %llx.\n", cur,
                                *srec);
                    } else {
                        logmsg(LOGMSG_USER, "Cur %p vs_skip key is NIL.\n", cur);
                    }
                }

                /* If srec exists it should be greater than or equal to
                 * genid_ck.
                 * If this fails it means that there was a record in the
                 * cursor's
                 * 'vs_skip' table which doesn't exist in addcur. */
                if (srec) {
                    memc = memcmp(srec, genid_ck, 8);
                    assert(memc >= 0);
                }

                /* Skip this virtual-stripe record: we've already seen it. */
                if (srec && 0 == memc) {
                    /* Go to the next record in the cursor's skip-list. */
                    bdb_temp_table_next(cur->state, cur->vs_skip, bdberr);

                    /* Fall-through and dump trace if trak'ing is enabled. */
                    if (cur->trak) {
                        logmsg(LOGMSG_USER, "Cur %p skipping genid %llx in addcur "
                                        "for this scan.\n",
                                cur, *srec);
                    }

                    goto next;
                }
            }

            /* If it's on the skip-list, go to the next. */
            if (bdb_tran_deltbl_isdeleted(cur->ifn, *genid_ck, 0, bdberr)) {
                if (cur->trak) {
                    logmsg(LOGMSG_USER, "Cur %p: genid %llx in addcur %p is on the "
                                    "is-deleted list.\n",
                            cur, *genid_ck, cur->addcur);
                }
                goto next;
            }

            if (cur->trak) {
                logmsg(LOGMSG_USER, 
                        "Cur %p: looking at genid %llx from addcur %p.\n", cur,
                        *genid_ck, cur->addcur);
            }

            /* Retrieve the size. */
            dtalen = bdb_temp_table_datasize(cur->addcur);

            /* Retrieve the actual row if this is the optimized codepath. */
            if (dtalen > 0 && bdb_osql_log_is_optim_data(addptr)) {
                bdb_osql_log_addc_ptr_t *newptr;
                int rowlen;
                unsigned long long *pgenid;

                /* Rebuild the row from the logfiles. */
                rc = bdb_osql_log_get_optim_data_addcur(
                    cur->state, &addptr->lsn, (void **)&newptr, &rowlen,
                    bdberr);
                if (rc < 0)
                    return rc;

                /* Update flags. */
                newptr->flag = 0;

                /* Update lsn. */
                newptr->lsn = addptr->lsn;

                /* update the temp table. */
                rc = bdb_temp_table_update(cur->state, cur->addcur, genid_ck, 8,
                                           newptr, rowlen, bdberr);
                if (rc < 0)
                    return rc;

                /* Malloc pgenid for find_exact. */
                pgenid = (unsigned long long *)malloc(sizeof(*pgenid));

                /* Copy it. */
                *pgenid = *genid_ck;

                /* Re-find the newly inserted position */
                rc = bdb_temp_table_find_exact(cur->state, cur->addcur,
                                               (char *)pgenid, 8, bdberr);
                if (rc != IX_FND) {
                    logmsg(LOGMSG_ERROR, "%s: fail to retrieve back the updated row "
                                    "rc=%d bdberr=%d\n",
                            __func__, rc, *bdberr);
                    rc = -1; /* we have to find this row back */
                }

                /* Retrieve the header. */
                addptr = bdb_temp_table_data(cur->addcur);

                /* Retrieve the size. */
                dtalen = bdb_temp_table_datasize(cur->addcur);
            }

            /* The payload is just after the header. */
            dta = (char *)addptr + sizeof(*addptr);

            /* Remove length of the header. */
            dtalen -= sizeof(*addptr);

            /* Retrieve odh-unpacked row data. */
            if (cur->addcur_use_odh) {
                if (bdb_unpack(cur->state, dta, dtalen, cur->addcur_odh,
                               MAXRECSZ, &odh, NULL)) {
                    *bdberr = BDBERR_UNPACK;
                    return -1;
                }

                dta_rl = odh.recptr;
                dtasize_rl = odh.length;
                ver_rl = odh.csc2vers;

            }
            /* No ondisk headers. */
            else {
                dta_rl = dta;
                dtasize_rl = dtalen;
                ver_rl = cur->state->version;
            }

            key_rl = bdb_temp_table_key(cur->addcur);
            keysize_rl = bdb_temp_table_keysize(cur->addcur);
            genid_rl = *(unsigned long long *)key_rl;
        } else {
            dta_rl = bdb_temp_table_data(cur->addcur);
            dtasize_rl = bdb_temp_table_datasize(cur->addcur);
            key_rl = bdb_temp_table_key(cur->addcur);
            keysize_rl = bdb_temp_table_keysize(cur->addcur);
            genid_rl = *(unsigned long long *)key_rl;
            ver_rl = cur->state->version;
        }

        /* Will always be larger than a log-update. */
        stripe_rl = cur->idx;

        /* If we're tracking, print what record is being skipped. */
        if (skip) {
            if (cur->trak) {
                logmsg(LOGMSG_USER, "cur %p skipping page-order key 0x", cur);
                hexdump(LOGMSG_USER, key_rl, keysize_rl);
                logmsg(LOGMSG_USER, " dta 0x");
                hexdump(LOGMSG_USER, dta_rl, dtasize_rl);
                logmsg(LOGMSG_USER, "\n");
            }

            dta_rl = NULL;
            dtasize_rl = 0;
            key_rl = NULL;
            keysize_rl = 0;
            dta_sd = NULL;
            dtasize_sd = 0;
            stripe_rl = -1;
            page_rl = -1;
            index_rl = -1;

            goto next;
        }
    }

    /* STEP 2 */
    /*
     * You could have just found a record in ADDCUR which will have it's blob
     * updated independantly.  If you are on the ADDCUR table, you do not have
     * any page-locks.  A blob-update can slip in just after update_shadows
     * is called, but before the blob-retrieval.
     *
     * What do I do with this?  Force an out-of-band FIND on the data-file
     * against the the masked genid so that I will block on the pagelock.  If
     * I get the lock before the update, then the blob in the blobfile must be
     * correct.  If I block on the lock, then the replication stream will
     * update the blob, and add logical records to the global queue before I
     * finally acquire the pagelock- which means that update_shadows will find
     * the logs it needs to reconstruct the blob.
     *
     * I can optimize a bit: I only need to do this if the blob doesn't yet
     * exist in my shadows.
     */
    rc = bdb_btree_update_shadows(cur, how, bdberr);
    if (rc < 0)
        return rc;

    /* STEP 2.5 */
    /* this is a mid-step required by snapisol mode that have
       no protection from rowlock logical transactions;
       if STEP 1 landed on a row with genid older than our start
       time, but which was committed by a younger transaction
       (which should be skipped), we need to go back and redo
       step 1 */
    if (got_rl) {
        rc = bdb_tran_deltbl_isdeleted(cur->ifn, genid_rl, 0, bdberr);
        if (rc < 0)
            return rc;

        if (rc == 1) {
            if (cur->used_rl == 0) {
                cur->used_rl = 1; /* please move again */
            }

            /* Cleanup. */
            dta_rl = NULL;
            dtasize_rl = 0;
            key_rl = NULL;
            keysize_rl = 0;
            stripe_rl = -1;
            page_rl = -1;
            index_rl = -1;

            goto step1;
        }
    }

    /* STEP 3 */
    if (cur->sd && !cur->shadow_tran->check_shadows)
        cur->used_sd = 1;

    if (cur->sd && cur->shadow_tran->check_shadows) {
#if MERGE_DEBUG
        fprintf(stderr, "%d %s:%d used_sd=%d cur->genid=%llx [%d]\n",
                pthread_self(), __FILE__, __LINE__, cur->used_sd, cur->genid,
                how);
        print_cursor_keys(cur, BDB_SHOW_BOTH);
#endif
        if ((how != DB_NEXT && how != DB_PREV) || cur->used_sd) {

            rc = bdb_cursor_move_and_skip(cur, cur->sd, last_key, last_keylen,
                                          how, bdberr);
            if (rc < 0)
                return rc;

#if MERGE_DEBUG
            fprintf(stderr, "%d %s:%d rc=%d used_sd=%d [%d]\n", pthread_self(),
                    __FILE__, __LINE__, rc, cur->used_sd, how);

            print_cursor_keys(cur, BDB_SHOW_BOTH);
#endif

#if 0 
         /* the shadows are based on temp tables, which have the good
            habit of flipping over when not initialized;
            make sure this is not retrieving out of order rows
            
            ASSERTION: when moving NEXT, the current position is
            smaller or equal than both real and shadow rows
            - for PREV, current position is bigger or equal than
            both rows

            NOTE: I feel like this should be revised
          */
         if (rc == IX_FND && cur->type == BDBC_IX && (how == DB_NEXT || how == DB_PREV))
         {
            rc = cur->sd->key( cur->sd, &key_sd, bdberr);
            if (rc<0)
               return rc;
            rc = cur->sd->keysize( cur->sd, &keysize_sd, bdberr);
            if (rc<0)
               return rc;

            assert (keysize_sd >= cur->datalen);
           rc = memcmp( key_sd, cur->data, cur->datalen);

            if (how== DB_NEXT)
            {
               if (rc < 0)
                  rc = IX_PASTEOF;  /*end of it */
               else
                  rc = IX_FND;      /*restore*/
            }
            else if( how == DB_PREV)
            {
               if (rc > 0)
                  rc = IX_PASTEOF;  /*end of it */
               else
                  rc = IX_FND;      /*restore*/
            }
         }
#endif

            /* a failed absolute move is always consumed */
            if (!cur->used_sd) {
                if (rc != IX_FND)
                    cur->used_sd = 1;
            } else {
                if (rc == IX_FND)
                    cur->used_sd = 0;
            }
        } else
            rc = IX_FND; /* we did not move, reuse same position */

        if (rc == IX_FND) {
            rc = cur->sd->dta(cur->sd, &dta_sd, bdberr);
            if (rc < 0)
                return rc;
            rc = cur->sd->dtasize(cur->sd, &dtasize_sd, bdberr);
            if (rc < 0)
                return rc;
            rc = cur->sd->key(cur->sd, &key_sd, bdberr);
            if (rc < 0)
                return rc;
            rc = cur->sd->keysize(cur->sd, &keysize_sd, bdberr);
            if (rc < 0)
                return rc;

            rc = berkdb_get_genid_from_dtakey(cur, dta_sd, key_sd, &genid_sd,
                                              bdberr);
            if (rc)
                return rc;
        }

        /* Print information. */
        if (cur->trak) {
            logmsg(LOGMSG_USER, 
                "Cur %p %s idx-shad-move how=%d rc=%d srch: keylen=%d key=0x",
                cur, cur->pageorder ? "pageorder(?) " : "", how, rc,
                last_keylen);
            hexdump(LOGMSG_USER, last_key, last_keylen);

            if (IX_FND == rc) {
                logmsg(LOGMSG_USER, " found: keylen=%d dta=0x", keysize_sd);
                hexdump(LOGMSG_USER, key_sd, keysize_sd);
            } else {
                logmsg(LOGMSG_USER, " (not found)");
            }
            logmsg(LOGMSG_USER, "\n");
        }
    }

    /* STEP 4 */
    if (cur->type == BDBC_DT) {
#if MERGE_DEBUG
        printf("move bdb_btree_merge BDBC_DT\n");
        if (dta_rl) {
            printf("dta_rl == \n");
            hexdump(dta_rl, dtasize_rl);
        } else {
            printf("dta_rl == NULL\n");
        }
        if (dta_sd) {
            printf("dta_sd == \n");
            hexdump(dta_sd, dtasize_sd);
        } else {
            printf("dta_sd == NULL\n");
        }
        if (key_rl) {
            printf("key_rl == \n");
            hexdump(key_rl, keysize_rl);
        } else {
            printf("key_rl == NULL\n");
        }
        if (key_sd) {
            printf("key_sd == \n");
            hexdump(key_sd, keysize_sd);
        } else {
            printf("key_sd == NULL\n");
        }
#endif
        return bdb_btree_merge(cur, stripe_rl, page_rl, index_rl, dta_rl,
                               dtasize_rl, dta_sd, dtasize_sd, /* datas */
                               key_rl, keysize_rl, key_sd,
                               keysize_sd,         /* keys */
                               genid_rl, genid_sd, /* genids */
                               ver_rl, how);       /* bias */
    } else {
#if MERGE_DEBUG
        printf("move bdb_btree_merge BDBC_IX\n");
        if (dta_rl) {
            printf("dta_rl == \n");
            hexdump(dta_rl, dtasize_rl);
        } else {
            printf("dta_rl == NULL\n");
        }
        if (dta_sd) {
            printf("dta_sd == \n");
            hexdump(dta_sd, dtasize_sd);
        } else {
            printf("dta_sd == NULL\n");
        }
        if (key_rl) {
            printf("key_rl == \n");
            hexdump(key_rl, keysize_rl);
        } else {
            printf("key_rl == NULL\n");
        }
        if (key_sd) {
            printf("key_sd == \n");
            hexdump(key_sd, keysize_sd);
        } else {
            printf("key_sd == NULL\n");
        }
#endif
        /*
        if (cur->state->ixdta[cur->idx]  && dta_rl)
        {
           struct datacopy_info *info = (struct datacopy_info *) cur->datacopy;
           info->datacopy = dta_rl;
           info->size = dtasize_rl;
           cur->unpacked_datacopy = NULL;
        }
        */
        return bdb_btree_merge(cur, stripe_rl, page_rl, index_rl, dta_rl,
                               dtasize_rl, dta_sd, dtasize_sd, /* datas */
                               key_rl, keysize_rl, key_sd,
                               keysize_sd,         /* keys */
                               genid_rl, genid_sd, /* genids */
                               0, how);            /* bias */
    }
}

static int bdb_cursor_move_int(bdb_cursor_impl_t *cur, int how, int *bdberr)
{
    int rc = IX_NOTFND;
    int nextstripe = cur->idx;
    int crt_how = how;

    cur->nsteps++;

    if (cur->trak) {
        logmsg(LOGMSG_USER, "Cur %p %s move %s stripe %d\n", cur,
                (cur->type == BDBC_DT) ? "data" : "index", tellmehow(how),
                cur->idx);
    }

    /* Increment cursor-version. */
    if (cur->type == BDBC_DT && how == DB_FIRST && cur->pageorder) {
        /* Create my vs_stab temp_table if it doesn't exist. */
        if (cur->vs_skip == NULL) {
            cur->vs_stab = bdb_temp_table_create(cur->state, bdberr);
        }
        /* Otherwise truncate it. */
        else {
            bdb_temp_table_close_cursor(cur->state, cur->vs_skip, bdberr);
            bdb_temp_table_truncate(cur->state, cur->vs_stab, bdberr);
        }

        /* Get a cursor to the new (or truncated) table. */
        cur->vs_skip =
            bdb_temp_table_cursor(cur->state, cur->vs_stab, NULL, bdberr);

        /* Cursor-move should do a full search. */
        cur->new_skip = 1;

        /* Set lastpage and laststripe to -1. */
        cur->laststripe = cur->lastpage = cur->lastindex = -1;

#if 0
       if( cur->cstripe == NULL )
       {
          cur->cstripe = bdb_temp_table_create( cur->state, bdberr );
       }
       else
       {
          bdb_temp_table_close_cursor( cur->state, cur->cscur, bdberr );
          bdb_temp_table_truncate( cur->state, cur->cstripe, bdberr );
       }

       cur->cscur = bdb_temp_table_cursor( cur->state, cur->cstripe, 
             NULL, bdberr );
#endif
    }

    /* THIS IS NOT EXACTLY THE LEVEL OF ABSTRACTION I WANTED,
       BUT IT MAKES SENSE SINCE I DO NOT WANT TO LOOK IN ALL
       STRIPES SIMPLY IGNORING THE FACT THAT THE KEY IS GENID
       AND THIS TELLS ME WHICH STRIPE I NEED */
    if (cur->type == BDBC_DT && (how == DB_FIRST || how == DB_LAST)) {
        int switch_stripes = 0;
        int dtafile =
            (how == DB_FIRST) ? 0 : (cur->state->attr->dtastripe -
                                     ((cur->addcur) ? 0 : 1)); /* last stripe */

        if (cur->data) {
            /* cursor is positioned */
            if (dtafile != cur->idx) {
                switch_stripes = 1;
            }
        } else {
            switch_stripes = 1;
        }
        if (switch_stripes) {
            if (!cur->invalidated || dtafile == cur->state->attr->dtastripe) {
                cur->invalidated = 0;
                rc = bdb_switch_stripe(cur, dtafile, bdberr);
                if (rc)
                    return rc;
            } else {
                /* Set the dtafile.  We'll recreate cursors below in
                 * bdb_cursor_revalidate(). */
                cur->idx = dtafile;

                /* Close & reopen the shadow so the real stripe and shadow
                 * stripes are in-sync */
                if (cur->sd) {
                    rc = cur->sd->close(cur->sd, bdberr);
                    cur->sd = NULL;
                    if (rc)
                        return (rc);
                }

                if (cur->shadow_tran) {
                    cur->sd = bdb_berkdb_open(cur, BERKDB_SHAD, MAXRECSZ,
                                              MAXKEYSZ, bdberr);
                    if (!cur->sd && *bdberr) {
                        return -1;
                    }
                }
            }
        }
        nextstripe = dtafile;
    }

    /* did we deadlock during a stripe jump ? that's a tricky one
       [
       I deadlock during a next that jumps on a new stripe:
       - the "next" is dynamically converted to a "first"
       - "first" deadlocks;
       - during recovery I cannot "lock" myself since the unlocked
       btree is not the one which is cached by bdbcursor
       ]
     */
    if (cur->type == BDBC_DT) {
        if (cur->used_rl == 0 && cur->used_sd == 0 && cur->invalidated == 1) {
            if (how == DB_NEXT)
                crt_how = DB_FIRST;
            else if (how == DB_PREV)
                crt_how = DB_LAST;
        }
    }

    do {
        /* both real data and index files are ending up here */
        rc = bdb_cursor_move_merge(cur, crt_how, bdberr);
        if (rc < 0)
            return rc;

        if (rc == IX_FND)
            return IX_FND;

        if (cur->type == BDBC_IX) {
            return (rc == IX_NOTFND) ? IX_PASTEOF : rc;
        }

        if (rc == IX_PASTEOF || rc == IX_EMPTY || rc == IX_NOTFND) {
            switch (how) {
            case DB_FIRST:
                nextstripe++;
                break;
            case DB_NEXT:
                nextstripe++;
                crt_how = DB_FIRST;
                break;
            case DB_LAST:
                nextstripe--;
                break;
            case DB_PREV:
                nextstripe--;
                crt_how = DB_LAST;
                break;
            default:
                *bdberr = BDBERR_BADARGS;
                return -1;
            }

            if (!IS_VALID_DTA(nextstripe))
                return (how == DB_FIRST || how == DB_LAST) ? IX_EMPTY
                                                           : IX_PASTEOF;

#if 0
         if (nextstripe == cur->state->attr->dtastripe)
            printf("Welcome to virtual world!\n");
#endif
            /* If you've exhausted records in the stripe but decide to call
             * 'release locks' while updating the shadows the invalidated
             * flag gets set.  This switch-stripe will instantiate a cursor
             * without clearing the invalidated flag. */
            cur->invalidated = 0;
            rc = bdb_switch_stripe(cur, nextstripe, bdberr);
            if (rc < 0)
                return rc;
            if (rc)
                return rc; /* current index */
        }
    } while (1);
}

static int bdb_cursor_move(bdb_cursor_impl_t *cur, int how, int *bdberr)
{
    int rc, cnt = 0, max = cur->state->attr->max_rowlocks_reposition, now;
    bdb_cursor_ifn_t *pcur_ifn = cur->ifn;
    static int lastpr = 0;

again:
    rc = bdb_cursor_move_int(cur, how, bdberr);
    if (-1 == rc && BDBERR_NEED_REPOSITION == *bdberr) {
        if (++cnt < max) {
            bdb_cursor_unlock(pcur_ifn, bdberr);
            goto again;
        }
        *bdberr = BDBERR_DEADLOCK;
    }

    return rc;
}

/**
 * RETURNS (please try to keep this up to date):
 *    - IX_FND       : found the exact key we are looking for
 *    - IX_PASTEOF   : cannot find this record or bigger
 *                     (cursor is positioned to last that can be in ANY ORDER VS
 *key)
 *    - IX_EMPTY     : no rows
 *    - IX_NOTFND    : found a record that is BIGGER than the key
 *    < 0            : error, bdberr set
 *
 * If dirLeft is set (Direction is LEFT), we are moving left (PREV) on the btree
 */
static int bdb_cursor_find_int(bdb_cursor_ifn_t *pcur_ifn, void *key,
                               int keylen, int dirLeft, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    bdb_state_type *bdb_state;
    int rc;
    int dtafile = -1;
    unsigned long long genid;

    bdb_state = cur->state;

    *bdberr = 0;

    if (cur->trak) {
        logmsg(LOGMSG_USER, "Cur %p %s find len=%d data[8]=%llx\n", cur,
                (cur->type == BDBC_DT) ? "data" : "index", keylen,
                *(long long *)key);
    }

    /* are we invalidated? relock */
    if (cur->invalidated) {
        assert(cur->rl != NULL);

        cur->invalidated = 0;

        rc = cur->rl->lock(cur->rl, cur->curtran, bdberr);
        if (rc)
            return rc;
    }

    /* THIS IS NOT EXACTLY THE LEVEL OF ABSTRACTION I WANTED,
        BUT IT MAKES SENSE SINCE I DO NOT WANT TO LOOK IN ALL
        STRIPES SIMPLY IGNORING THE FACT THAT THE KEY IS GENID
        AND THIS TELLS ME WHICH STRIPE I NEED */
    if (cur->type == BDBC_DT) {
        unsigned long long genid;

        assert(keylen == sizeof(genid));

        memcpy(&genid, key, sizeof(genid));
        dtafile = get_dtafile_from_genid(genid);
        if (dtafile != cur->idx) {
            rc = bdb_switch_stripe(cur, dtafile, bdberr);
            if (rc)
                return rc;
        }
    }

    rc = bdb_cursor_find_merge(cur, key, keylen, bdberr);

    if (rc < 0)
        return rc;

    if (rc == IX_FND) {
        if (cur->type == BDBC_IX) {

            if (bdb_keycontainsgenid(cur->state, cur->idx))
                assert(keylen <= (cur->datalen + sizeof(cur->genid)));
            else
                assert(keylen <= cur->datalen);

            if (dirLeft) { // set via is bias == SeekLT, we are travelling left
                if (cur->used_sd == 1 && key_found_in_rl(cur) &&
                    cur->sd->is_at_eof(cur->sd)) {
                    /* if we are at eof, tree might be empty */
                    rc = cur->sd->last(cur->sd, bdberr);
                    if (rc == IX_FND)
                        cur->used_sd = 0;
                } else if (cur->used_rl == 1 && !key_found_in_rl(cur) &&
                           cur->rl->is_at_eof(cur->rl)) {
                    /* if we are at eof, tree might be empty */
                    rc = cur->rl->last(cur->rl, bdberr);
                    if (rc == IX_FND)
                        cur->used_rl = 0;
                } else if (cur->used_rl == 0)
                    cur->used_rl = 1;
                else if (cur->used_sd == 0)
                    cur->used_sd = 1;
            }
            /* use keylen, i.e. partial keys */
            if (memcmp(cur->data, key, keylen))
                return IX_NOTFND;
        } else if (cur->type == BDBC_DT) {
            /* to my surprise, this code path is also possible,
               every time we use rowid in sql */
            assert(keylen == sizeof(unsigned long long));

            if (memcmp(&cur->genid, key, keylen))
                return IX_NOTFND;
        }
        return IX_FND;
    }
#if 0
   /* Handled inside of cursor_find_merge */
   /* 
    * If we haven't checked the addcur stripe, check it now.  Enable only for
    * pageorder tablescan mode.
    */
   else if (
        rc == IX_NOTFND && 
        cur->type == BDBC_DT && 
        cur->addcur && 
        cur->idx != cur->state->attr->dtastripe &&
        cur->pageorder != 0
   )
   {
      rc = bdb_switch_stripe(cur, cur->state->attr->dtastripe, bdberr);
      if (rc) 
         return rc;
      goto again;
   }
#endif
    else if (rc == IX_NOTFND) {
        /* reusing btree_merge function generates IX_NOTFND instead of
         * IX_PASTEOF when a btree is empty */
        rc = IX_PASTEOF;
    }

    assert(rc == IX_PASTEOF);

    /* Don't do a 'last' on a data file */
    if (cur->type == BDBC_DT) {
        return IX_PASTEOF;
    }

    rc = pcur_ifn->last(pcur_ifn, bdberr);
    if (rc < 0)
        return rc;

    if (rc == IX_EMPTY)
        return IX_EMPTY;

    /* found a row before the key, last in shadowed btree */
    return IX_PASTEOF;
}

static int bdb_cursor_find(bdb_cursor_ifn_t *pcur_ifn, void *key, int keylen,
                           int dirLeft, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    int rc, cnt = 0, max = cur->state->attr->max_rowlocks_reposition, now;
    static int lastpr = 0;

again:
    rc = bdb_cursor_find_int(pcur_ifn, key, keylen, dirLeft, bdberr);
    if (-1 == rc && BDBERR_NEED_REPOSITION == *bdberr) {
        if (++cnt < max) {
            bdb_cursor_unlock(pcur_ifn, bdberr);
            goto again;
        }
        *bdberr = BDBERR_DEADLOCK;
    }

    return rc;
}

static int bdb_cursor_close(bdb_cursor_ifn_t *pcur_ifn, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    int rc = 0;

    *bdberr = 0;

    if (cur->trak) {
        logmsg(LOGMSG_USER, "Cur %p closed\n", cur);
    }

    if (cur->skip) {
        rc = bdb_temp_table_close_cursor(cur->state, cur->skip, bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: error closing skip table %d %d\n", __func__,
                    rc, *bdberr);
        }
        cur->skip = NULL;
    }

    if (cur->vs_skip) {
        rc = bdb_temp_table_close_cursor(cur->state, cur->vs_skip, bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: error closing vs_skip cursor %d %d\n",
                    __func__, rc, *bdberr);
        }

        rc = bdb_temp_table_close(cur->state, cur->vs_stab, bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: error closing vs_stab table %d %d\n", __func__,
                    rc, *bdberr);
        }

        cur->vs_skip = NULL;
        cur->vs_stab = NULL;
    }

#if 0
   if (cur->cstripe)
   {
      rc = bdb_temp_table_close_cursor( cur->state, cur->cscur, bdberr );
      if (rc)
      {
         fprintf( stderr, "%s: error closing cscur cursor %d %d\n",
               __func__, rc, *bdberr);
      }

      rc = bdb_temp_table_close( cur->state, cur->cstripe, bdberr );
      if (rc) 
      {
         fprintf( stderr, "%s: error closing cstripe table %d %d\n",
               __func__, rc, *bdberr);
      }

      cur->cscur = NULL;
      cur->cstripe = NULL;
   }
#endif

    /* close shadows as well*/
    if (cur->addcur) {
        /* We own this cursor: destroy it. */
        rc = bdb_temp_table_close_cursor(cur->state, cur->addcur, bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: error closing add table %d %d\n", __func__, rc,
                    *bdberr);
        }
        cur->addcur = NULL;
    }

    if (cur->rl) {
        verify_pageorder_tablescan(cur);
        rc = cur->rl->close(cur->rl, bdberr);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s:%d rc=%d bdberr=%d\n", __FILE__, __LINE__, rc,
                    *bdberr);
    }
    if (cur->sd) {
        rc = cur->sd->close(cur->sd, bdberr);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s:%d rc=%d bdberr=%d\n", __FILE__, __LINE__, rc,
                    *bdberr);
    }

    if (cur->lastkey)
        free(cur->lastkey);
    if (cur->datacopy)
        free(cur->datacopy);
    if (cur->addcur_odh)
        free(cur->addcur_odh);

    /* Remove from shadow-tran's open cursor list. */
    if (cur->shadow_tran &&
        (cur->shadow_tran->tranclass == TRANCLASS_SNAPISOL ||
         cur->shadow_tran->tranclass == TRANCLASS_SERIALIZABLE)) {
        listc_rfl(&cur->shadow_tran->open_cursors, pcur_ifn);
    }

    memset(cur, 0xff, sizeof(bdb_cursor_impl_t));
    free(pcur_ifn);
    return (*bdberr) ? -1 : 0;
}

static void *bdb_cursor_data(bdb_cursor_ifn_t *cur) { return cur->impl->data; }

static void *bdb_cursor_datacopy(bdb_cursor_ifn_t *cur)
{
    bdb_cursor_impl_t *c = cur->impl;

    if (c->unpacked_datacopy) {
        /* datacopy has been consumed once by get_data.
         * which means this row has been converted to the latest
         * version already. make this reflect in subsequent calls
         * to bdb_cursor_ver() */
        c->ver = c->state->version;
        return c->unpacked_datacopy;
    }

    bdb_state_type *bdb_state = c->state;
    struct datacopy_info info = *(struct datacopy_info *)c->datacopy;
    int size = info.size;
    uint8_t *from = info.datacopy;
    if (size == 0)
        return NULL;

    /* datacopy starts with genid. skip it */
    from += sizeof(unsigned long long);
    size -= sizeof(unsigned long long);

    if (bdb_state->ondisk_header && bdb_state->datacopy_odh &&
        (c->type == BDBC_DT || !is_genid_synthetic(c->genid))) {
        c->unpacked_datacopy = unpack_datacopy_odh(
            cur, c->datacopy, bdb_state->lrl, from, size, &c->ver);
    } else {
        c->unpacked_datacopy = from;
        c->ver = c->state->version;
    }
    return c->unpacked_datacopy;
}

static void bdb_cursor_found_data(struct bdb_cursor_ifn *cur, int *rrn,
                                  unsigned long long *genid, int *datalen,
                                  void **data, uint8_t *ver)
{
    *rrn = cur->impl->rrn;
    *genid = cur->impl->genid;
    *datalen = cur->impl->datalen;
    *data = cur->impl->data;
    *ver = cur->impl->ver;

    if (cur->impl->trak) {
        logmsg(LOGMSG_USER, 
                "Cur %p retrieving %s rrn=%d genid=%llx len=%d ver=%d dta=0x",
                cur->impl, cur->impl->type == BDBC_IX ? "index" : "data", *rrn,
                *genid, *datalen, *ver);
        hexdump(LOGMSG_USER, *data, *datalen);
        logmsg(LOGMSG_ERROR, "\n");
    }
}

static uint8_t bdb_cursor_ver(bdb_cursor_ifn_t *cur) { return cur->impl->ver; }

static int bdb_cursor_datalen(bdb_cursor_ifn_t *cur)
{
    return cur->impl->datalen;
}

static unsigned long long bdb_cursor_genid(bdb_cursor_ifn_t *cur)
{
    return cur->impl->genid;
}

static int bdb_cursor_rrn(bdb_cursor_ifn_t *cur) { return cur->impl->rrn; }

static int serial_update_lastkey(bdb_cursor_impl_t *cur, char *key, int keylen)
{
    if (!cur->shadow_tran)
        return 0;

    assert((keylen > 0 && key != NULL) || (keylen == 0 && key == NULL));
    assert(keylen < MAXKEYSZ);

    if (!cur->lastkey) {
        cur->lastkey = malloc(MAXKEYSZ);
        if (!cur->lastkey) {
            logmsg(LOGMSG_ERROR, "%s malloc %d\n", __func__, MAXKEYSZ);
            return -1;
        }
    }

    memcpy(cur->lastkey, key, keylen);
    cur->lastkeylen = keylen;

    return 0;
}

static int bdb_btree_merge(bdb_cursor_impl_t *cur, int stripe_rl, int page_rl,
                           int index_rl, char *pdata_rl, int pdatalen_rl,
                           char *pdata_sd, int pdatalen_sd, char *key_rl,
                           int keylen_rl, char *key_sd, int keylen_sd,
                           unsigned long long genid_rl,
                           unsigned long long genid_sd, uint8_t ver_rl, int how)
{
    char *data_rl;
    int datalen_rl;
    char *data_sd;
    int datalen_sd;
    int rc = -1;
    void *data = NULL;
    int len = 0;
    int rrn = 0;
    unsigned long long genid = 0;
    int fidlen = (DB_FILE_ID_LEN * 2) + 1;
    unsigned char fileid[DB_FILE_ID_LEN] = {0};
    char hex_fid[(DB_FILE_ID_LEN * 2) + 1];

    if (cur->trak) {
        int bdberr = 0;
        cur->rl->fileid(cur->rl, fileid, &bdberr);
        hex_fid[fidlen - 1] = '\0';
        tohex(hex_fid, fileid, DB_FILE_ID_LEN);
    }

    bdb_state_type *bdb_state;

    bdb_state = cur->state;

    /* Should be the child bdb state, not the parent. */
    assert(bdb_state->parent != NULL);

    cur->collattr_len = 0;
    cur->collattr = NULL;

    if (cur->type == BDBC_IX) {
        data_rl = key_rl;
        datalen_rl = keylen_rl;
        data_sd = key_sd;
        datalen_sd = keylen_sd;
    } else {
        data_rl = pdata_rl;
        datalen_rl = pdatalen_rl;
        data_sd = pdata_sd;
        datalen_sd = pdatalen_sd;
    }

    /* Non-genid databases shouldn't make it this far. I REALLY hope it's
       caught before it makes it this far. */
    if (cur->state->attr->genids == 0)
        return -1;

    /* Not supported for non-dtastripe. Tough luck. */
    if (cur->state->attr->dtastripe == 0)
        return -1;

    if (!data_rl && !data_sd) {
        /*

        fprintf( stderr, "%d Empty cursor %p [%d]\n",
              pthread_self(), cur, how);
         */
        rc = serial_update_lastkey(cur, NULL, 0);
        if (rc)
            return rc;

        set_datacopy(cur, NULL, 0);

        cur->lastpage = INT_MAX;

        if (cur->trak) {
            lkprintf(LOGMSG_USER,
                     "shadtrn %p cur %p fid %s merging stripe %d %s (empty)\n",
                     cur->shadow_tran, cur, hex_fid, cur->idx,
                     cur->type == BDBC_IX ? "index" : "data");
        }
        switch (how) {
        case DB_FIRST:
        case DB_LAST:
            return IX_EMPTY;
        case DB_NEXT:
        case DB_PREV:
            return IX_NOTFND;
        case DB_SET_RANGE:
            return IX_PASTEOF;
        case DB_SET:
            return IX_NOTFND;
        }
        /* reset datacopy */

        /* we did not move anywhere, don't touch data/datalen/used-s */
    }

    cur->rrn = 2;

    /* only data in real? */
    if (!data_sd) {
        /*
        fprintf( stderr, "%d Real only %p genid_rl=%llx [%d] %u\n",
              pthread_self(), cur, genid_rl, how, (*(unsigned
        int*)&data_rl[1])&0x7FFFFFFF);
         */

        if (cur->trak) {
            char *mem = alloca((2 * datalen_rl) + 2);
            tohex(mem, data_rl, datalen_rl);
            lkprintf(LOGMSG_ERROR, "shadtrn %p cur %p fid %s merging stripe %d %s "
                             "(real) len=%d dta=0x%s\n",
                     cur->shadow_tran, cur, hex_fid, cur->idx,
                     cur->type == BDBC_IX ? "index" : "data", datalen_rl, mem);
        }
        rc = serial_update_lastkey(cur, key_rl, keylen_rl);
        if (rc)
            return rc;

        set_datacopy(cur, pdata_rl, pdatalen_rl);

        cur->used_rl = 1;

        cur->data = data_rl;
        cur->datalen = datalen_rl;
        cur->laststripe = stripe_rl;
        cur->lastpage = page_rl;
        cur->lastindex = index_rl;
        cur->ver = ver_rl;
        if (cur->type == BDBC_IX && bdb_keycontainsgenid(cur->state, cur->idx))
            cur->datalen -= sizeof(unsigned long long);
        cur->genid = genid_rl;

        if (cur->type == BDBC_IX && !cur->state->ixdta[cur->idx] &&
            pdatalen_rl > sizeof(unsigned long long)) {
            cur->collattr = pdata_rl + sizeof(unsigned long long);
            cur->collattr_len = pdatalen_rl - sizeof(unsigned long long);
        }

        return IX_FND;
    }

    /* only data in shadow? */
    if (!data_rl) {
        /*
        fprintf( stderr, "%d Shadow only %p genid_sd=%llx [%d] %u\n",
              pthread_self(), cur, genid_sd, how, (*(unsigned
        int*)&data_sd[1])&0x7FFFFFFF);
         */

        if (cur->trak) {
            char *mem = alloca((2 * datalen_sd) + 2);
            tohex(mem, data_sd, datalen_sd);
            lkprintf(LOGMSG_USER, "shadtrn %p cur %p fid %s merging stripe %d %s "
                             "(shadow) len=%d dta=0x%s\n",
                     cur->shadow_tran, cur, hex_fid, cur->idx,
                     cur->type == BDBC_IX ? "index" : "data", datalen_sd, mem);
        }
        rc = serial_update_lastkey(cur, NULL, 0);
        if (rc)
            return rc;

        set_datacopy(cur, pdata_sd, pdatalen_sd);

        cur->used_sd = 1;
        cur->data = data_sd;
        cur->datalen = datalen_sd;

        if (cur->type == BDBC_IX)
            cur->datalen -= sizeof(unsigned long long);
        cur->genid = genid_sd;

        /* This is a synthetic row- it's version will be the 'current' version.
         */
        cur->ver = bdb_state->version;

        if (cur->type == BDBC_IX && !cur->state->ixdta[cur->idx] &&
            pdatalen_sd > sizeof(unsigned long long)) {
            cur->collattr = pdata_sd + sizeof(unsigned long long);
            cur->collattr_len = pdatalen_sd - sizeof(unsigned long long);
        }

        return IX_FND;
    }

    /* Can't be pageorder. */
    assert(!cur->pageorder);

    /* both real and shadow */
    /* NOTE: for now, index shadows are suffixed by genids; data shadows are not
     */
    assert((cur->type == BDBC_IX &&
            (keylen_sd == keylen_rl + sizeof(unsigned long long))) ||
           (cur->type == BDBC_IX && keylen_sd == keylen_rl &&
            bdb_keycontainsgenid(cur->state, cur->idx)) ||
           (cur->type == BDBC_DT && (keylen_sd == keylen_rl)));

    rc = memcmp(key_rl, key_sd,
                keylen_sd -
                    ((cur->type == BDBC_IX) ? sizeof(unsigned long long) : 0));

    /* If this is a dup-key which matches, the sort continues on the genid. */
    if (rc == 0 && cur->type == BDBC_IX &&
        !is_genid_synthetic(
            *(unsigned long long *)&key_sd[keylen_sd -
                                           sizeof(unsigned long long)]) &&
        bdb_keycontainsgenid(cur->state, cur->idx)) {
        /* I'm using memcmp rather than bdb_cmp_genids because this should be
         * btree sort-order. */
        rc = memcmp(&genid_rl, &genid_sd, sizeof(unsigned long long));
    }

    if (rc == 0 || (((rc < 0) && (how == DB_NEXT || how == DB_FIRST)) ||
                    ((rc > 0) && (how == DB_PREV || how == DB_LAST)))) {
        /* if same key, skip shadow (mark used)
         IFF the genid is not synthetic; otherwise we would
         be skipping locally added rows that identically match
         an existing row
         */
        if (rc == 0) {
            if (cur->type != BDBC_IX) {
                cur->used_sd = 1;
            }

            /* Only consume dup-indices if the genid matches. */
            if (cur->type == BDBC_IX &&
                !is_genid_synthetic(
                    *(unsigned long long *)&key_sd
                        [keylen_sd - sizeof(unsigned long long)])) {
                /* Assert that the genids are the same for unique keys. */
                if (!bdb_keycontainsgenid(cur->state, cur->idx)) {
                    assert(genid_rl == genid_sd);
                }
                /* The memcmp above ensures that genid_rl == genid_sd if rc = 0.
                 */
                cur->used_sd = 1;
            }
        }

        if (cur->trak) {
            char *real_mem = alloca((2 * datalen_rl) + 2);
            char *shadow_mem = alloca((2 * datalen_sd) + 2);

            tohex(real_mem, data_rl, datalen_rl);
            tohex(shadow_mem, data_sd, datalen_sd);

            lkprintf(LOGMSG_USER, "shadtrn %p cur %p fid %s merging stripe %d %s "
                             "(both->real) len=%d dta=0x%s vs len=%d "
                             "dta=0x%s\n",
                     cur->shadow_tran, cur, hex_fid, cur->idx,
                     cur->type == BDBC_IX ? "index" : "data", datalen_rl,
                     real_mem, datalen_sd, shadow_mem);
        }
        rc = serial_update_lastkey(cur, key_rl, keylen_rl);
        if (rc)
            return rc;

        set_datacopy(cur, pdata_rl, pdatalen_rl);

        cur->used_rl = 1;
        /* pick real */
        cur->data = data_rl;
        cur->datalen = datalen_rl;
        cur->laststripe = stripe_rl;
        cur->lastpage = page_rl;
        cur->lastindex = index_rl;
        cur->ver = ver_rl;

        if (cur->type == BDBC_IX && bdb_keycontainsgenid(cur->state, cur->idx))
            cur->datalen -= sizeof(unsigned long long);

        cur->genid = genid_rl;

        if (cur->type == BDBC_IX && !cur->state->ixdta[cur->idx] &&
            pdatalen_sd > sizeof(unsigned long long)) {
            cur->collattr = pdata_rl + sizeof(unsigned long long);
            cur->collattr_len = pdatalen_rl - sizeof(unsigned long long);
        }

        /*
        fprintf( stderr, "%d Both get Real %p genid_rl = %llx genid_sd=%llx [%d]
        %u\n",
              pthread_self(), cur, genid_rl, genid_sd, how, (*(unsigned
        int*)&data_rl[1])&0x7FFFFFFF);
         */

    } else if (((rc > 0) && (how == DB_NEXT || how == DB_FIRST)) ||
               ((rc < 0) && (how == DB_PREV || how == DB_LAST))) {
        if (cur->trak) {
            char *real_mem = alloca((2 * datalen_rl) + 2);
            char *shadow_mem = alloca((2 * datalen_sd) + 2);

            tohex(real_mem, data_rl, datalen_rl);
            tohex(shadow_mem, data_sd, datalen_sd);

            lkprintf(LOGMSG_USER, "shadtrn %p cur %p fid %s merging stripe %d %s "
                             "(both->shadow) len=%d dta=0x%s vs len=%d "
                             "dta=0x%s\n",
                     cur->shadow_tran, cur, hex_fid, cur->idx,
                     cur->type == BDBC_IX ? "index" : "data", datalen_sd,
                     shadow_mem, datalen_rl, real_mem);
        }
        rc = serial_update_lastkey(cur, NULL, 0);
        if (rc)
            return rc;

        set_datacopy(cur, pdata_sd, pdatalen_sd);

        cur->used_sd = 1;
        cur->data = data_sd;
        cur->datalen = datalen_sd;
        cur->ver = bdb_state->version;
        if (cur->type == BDBC_IX)
            cur->datalen -= sizeof(unsigned long long);
        cur->genid = genid_sd;

        if (cur->type == BDBC_IX && !cur->state->ixdta[cur->idx] &&
            pdatalen_sd > sizeof(unsigned long long)) {
            cur->collattr = pdata_sd + sizeof(unsigned long long);
            cur->collattr_len = pdatalen_sd - sizeof(unsigned long long);
        }

        /*
        fprintf( stderr, "%d Both get Shadow %p genid_rl = %llx genid_sd=%llx
        [%d] %u\n",
              pthread_self(), cur, genid_rl, genid_sd, how, (*(unsigned
        int*)&data_sd[1])&0x7FFFFFFF);
         */

    } else {
        logmsg(LOGMSG_ERROR, "dbt_to_cursor: huh?");
        return -1;
    }

    return IX_FND;
}

/* this is a bit heavy handed, but it will prove the point
   if this is correct
 */
static int wait_for_recovering(bdb_state_type *bdb_state)
{

    int rc = 0;
    int retries = 0;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    /* 10 seconds waiting for recovery */
    do {
        rc = berkdb_is_recovering(bdb_state->dbenv);
        if (!rc)
            break;
        poll(NULL, 0, 5);
        retries++;
    } while (retries < 2000 && rc);

    return rc;
}

/********************************************************************/

/**
 * Always update the shadow
 * For index, this is going to the paring shadow
 * TODO: For data, this is always going to the last stripe
 *
 */
static int bdb_cursor_insert(bdb_cursor_ifn_t *pcur_ifn,
                             unsigned long long genid, void *data, int datalen,
                             void *datacopy, int datacopylen, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    char newkey[MAXKEYSZ]; /* we need to generate a new key */
    char newdata[MAXRECSZ];
    char *newpayload;
    int newpayloadlen;
    int rc = 0;

    if (cur->type == BDBC_DT) {
        logmsg(LOGMSG_ERROR, "%s:%d NOT implemented!\n", __FILE__, __LINE__);
        return -1;
    }

    if (!cur->sd) {
        cur->sd = bdb_berkdb_open(cur, BERKDB_SHAD_CREATE, MAXRECSZ, MAXKEYSZ,
                                  bdberr);
        if (!cur->sd)
            return -1;
    }

    if (datalen + sizeof(genid) > sizeof(newkey)) {
        logmsg(LOGMSG_ERROR, "%s: key too long\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    memcpy(newkey, data, datalen);
    memcpy(&newkey[datalen], &genid, sizeof(genid));

    if (cur->state->ixdta[cur->idx]) {
        assert(datacopy != NULL && datacopylen > 0);
        assert(sizeof(genid) + datacopylen <= MAXRECSZ);

        memcpy(newdata, (char *)&genid, sizeof(genid));
        memcpy(newdata + sizeof(genid), datacopy, datacopylen);

        newpayload = &newdata[0];
        newpayloadlen = sizeof(genid) + datacopylen;
    } else {
        newpayload = (char *)&genid;
        newpayloadlen = sizeof(genid);
    }

    rc = cur->sd->insert(cur->sd, newkey, datalen + sizeof(genid), newpayload,
                         newpayloadlen, bdberr);

#if 0
   fprintf( stderr, "INSERT %s %d %llx %d :\n",
         (cur->type==BDBC_IX)?"IX":"DT", cur->idx, genid, datalen+sizeof(genid));
   hexdump(newkey, datalen+sizeof(genid));
   fprintf( stderr, "\n");
#endif

    return rc;
}

/**
 * Delete an index from shadow
 * Cursor is already positioned on the record
 * Works for indexes
 * TODO: data (when addcur is merged in bdb)
 *
 */
static int bdb_cursor_delete(bdb_cursor_ifn_t *pcur_ifn, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    int rc = 0;

    if (cur->type == BDBC_DT) {
        logmsg(LOGMSG_ERROR, "%s:%d NOT implemented!\n", __FILE__, __LINE__);
        return -1;
    }

    if (!cur->sd) {
        logmsg(LOGMSG_ERROR, "%s:%d: BUG no shadow??\n", __FILE__, __LINE__);
        *bdberr = BDBERR_BUG_KILLME;
        return -1;
    }

    rc = cur->sd->delete (cur->sd, bdberr);

    return rc;
}

/********************************************************************/

int bdb_check_pageno(bdb_state_type *p_bdb_state, uint32_t pgno)
{

    int i = 0, j = 0, k = 0;

    if (p_bdb_state->parent)
        p_bdb_state = p_bdb_state->parent;

    for (i = 0; i < p_bdb_state->numchildren; i++) {
        /* for each dta file */
        bdb_state_type *bdb_state = p_bdb_state->children[i];

        if (bdb_state) {
            for (j = 0; j < bdb_state->numdtafiles; j++) {
                /* for each stripe */
                for (k = 0; k < bdb_state->attr->dtastripe; k++) {
                    /* this could be more smart, treat meta, dta and
                       blob based on attr; but I only want a simple DB set
                       walk:(
                    */
                    if (!bdb_state->dbp_data[j][k])
                        continue;
                    __db_check_all_btree_cursors(bdb_state->dbp_data[j][k],
                                                 pgno);
                }
            }
            /* for each index */
            for (j = 0; j < bdb_state->numix; j++) {
                if (!bdb_state->dbp_ix[j])
                    continue;
                __db_check_all_btree_cursors(bdb_state->dbp_ix[j], pgno);
            }
        }
    }

    return 0;
}

/**
 * Returns the dbnum for a bdbcursor
 *
 */
static int bdb_cursor_dbnum(bdb_cursor_ifn_t *cur) { return cur->impl->dbnum; }

/**
 * Release the persistent cursors/page locks associated with bdbcursor
 * If cursor is invalidated, this means we're done
 * Note: an outoforder cursor needs to unlock as well, since it is gonna
 * change the curtran.
 */
static int bdb_cursor_unlock(bdb_cursor_ifn_t *pcur_ifn, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;

    *bdberr = 0;

    if (cur->invalidated)
        return 0;

    if (cur->rl) {
        cur->invalidated = 1;
        if (cur->trak) {
            logmsg(LOGMSG_USER, "Cur %p unlocked\n", cur);
        }
        return cur->rl->unlock(cur->rl, bdberr);
    }

    return 0;
}

/**
 * Recreate a cursor for this bdbcursor after an unlock
 * If the cursor was not positioned anywhere, return after cursor recreation.
 * Otherwise, reposition the new cursor to the previous location; if row does
 *not exist
 * anymore, "how" tells you how to react:
 * - BDB_SET: return IX_NOTFND (DB_SET like)
 * - BDB_NEXT: try to position on a next row (DB_SET_RANGE like)
 * - BDB_PREV: try to position on a previous row (LAST_DUP like) *
 *
 * RETURNS:
 * - IX_FND       : found the previous record
 * - IX_NOTFND    : could not found record, sitting on the next/prev record
 *depeding on the direction
 * - IX_PASTEOF   : no record found, neither a following one in the next/prev
 *direction (for next/prev)
 * - IX_EMPTY     : no rows
 * - <0           : error, bdberr set
 *
 */
static int bdb_cursor_lock(bdb_cursor_ifn_t *pcur_ifn, cursor_tran_t *curtran,
                           int how, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    int rc = 0;

    *bdberr = 0;
    cur->curtran = curtran;

    cur->invalidated = 0;

    if (cur->rl) {
        /* get cursor */
        rc = cur->rl->lock(cur->rl, curtran, bdberr);
        if (rc)
            return rc;

        /* if the cursor was not set anywhere,
           no need to reposition anywhere */
        if (cur->rl->outoforder_get(cur->rl))
            return rc;

        rc = bdb_cursor_reposition(pcur_ifn, how, bdberr);

        if (cur->trak) {
            logmsg(LOGMSG_USER, "Cur %p reposition returns %d\n", cur, rc);
        }
    }
    return rc;
}

/** compare a key with a berkdb key */
static int compare_keys(bdb_cursor_impl_t *cur, int *bdberr)
{
    char *key;
    int keysize;
    int rc;

    rc = cur->sd->key(cur->sd, &key, bdberr);
    if (rc)
        return rc;

    rc = cur->sd->keysize(cur->sd, &keysize, bdberr);
    if (rc)
        return rc;

    assert(keysize >= cur->lastkeylen); /* > for datacopy indexes */

    return memcmp(key, cur->lastkey, cur->lastkeylen) == 0;
}

/**
 * Reposition a cursor on an updated shadow to fix a relative move
 * Called from bdb_btree_update_shadows (see this for more explanations)
 *
 */
static int bdb_btree_update_shadow_cursor_next(bdb_cursor_impl_t *cur,
                                               int created, int *bdberr)
{
    int rc = 0;

    /* All Scenarios for shadow cursor in NEXT situation:

       1) If "created"

     ** this was created in the meantime; need to start considering steps
     ** from the last position of the real key since everything before it
     ** was already consumed.  between the last real position and the current
     ** real position there could be rows that were "shadowed"(move to shadow)
     ** I need to consider them using for my shadow;
     ** Note: even last position might already be in the shadow

 Steps:
 - find and compare with lastkey
 - if the same, mark consumed (will move)
 - if different (is HIGHER), mark UNCONSUMED (will use)

 2) If not "created", and consumed ( shadow is <= last real key, or not set)

     ** if shadow not set, need a find to last key;
     ** If shadow is set, between the shadow position and the last
     ** real position there could be new rows inserted, which were
     ** already consumed by the real moves; skip them with a find

 Steps:
 - find and compare with lastkey
 - if the same, left marked consumed (will move)
 - if different (is HIGHER), marked UNCONSUMED (will use)

 3) If not "created", and not consumed (shadow is > last real key, since
 merging mark shadow consumed if equal)

     ** shadow is set in this case; between the current not yet consumed
     ** shadow and last position might have new shadowed rows that were
     ** skiped by this real move AND the previous shadow mode (since
     ** were added in between previous shadow move and this real move)
     ** need to find "back" and use them if any

 Steps:
 - find and compare with lastkey
 - if the same, mark consumed (will move)
 - if different (is HIGHER), mark UNCONSUMED (will use)

 Conclusion:
 a common algo is possible (this does not optimize certain cases
 but will do for now):

 - find last key in the shadow and compare with real last key
 - if the same, mark consumed (will move in STEP 3 of merging)
 - if different, mark unconsumed (will skip step3 of merging and use in step 4)

 More simple than you would expect.

     */

    /* NOTE: this is broken, bdb_temp_table_find returns also smaller keys, blah
     */

    rc = cur->sd->find(cur->sd, cur->lastkey, cur->lastkeylen, DB_SET_RANGE,
                       bdberr);
    if (rc < 0)
        return rc;

    if (rc == IX_FND) {
        rc = compare_keys(cur, bdberr);
        if (rc < 0)
            return rc;

        cur->used_sd = rc;
    } else if (rc == IX_NOTFND) {
        /* will use in step 4 of merging (step3 is skipped) */
        cur->used_sd = 0;
    } else {
        /* IX_PASTEOF */
        cur->used_sd = 1;
    }

    return 0;
}

static int bdb_btree_update_shadow_cursor_prev(bdb_cursor_impl_t *cur,
                                               int created, int *bdberr)
{
    int rc = 0;

    /* All Scenarios for shadow cursor in PREV situation:
       - left as homework, pretty much the same idea
     */

    /* argh, cannot reuse last_dup for bdbcursor here */
    char tmpkey[MAXKEYSZ + 1]; /* last dup */
    int tmpkeylen;
    char *usekey = NULL;
    int usekeylen = 0;

    if (cur->type == BDBC_IX) {
        /* uniq search (double genid for dup indexes, but I do not care */
        memcpy(tmpkey, cur->lastkey, cur->lastkeylen);
        memcpy(tmpkey + cur->lastkeylen, &cur->genid, sizeof(cur->genid));
        usekeylen = cur->lastkeylen + sizeof(cur->genid);
        usekey = tmpkey;
    } else {
        usekey = cur->lastkey;
        usekeylen = cur->lastkeylen;
    }
    rc = cur->sd->find(cur->sd, usekey, usekeylen, DB_SET_RANGE, bdberr);
    if (rc < 0)
        return rc;

    if (rc == IX_FND) {
        rc = compare_keys(cur, bdberr);
        if (rc < 0)
            return rc;

        cur->used_sd = rc;
    } else if (rc == IX_NOTFND) {
        /* we point exact to the right to the intended row */
        rc = cur->sd->prev(cur->sd, bdberr);
        if (rc < 0)
            return rc;

        if (rc == IX_FND) {
            cur->used_sd = 0;
        } else if (rc == IX_PASTEOF || rc == IX_NOTFND) {
            cur->used_sd = 1;
        }
    } else {
        /* IX_PASTEOF */
        rc = cur->sd->last(cur->sd, bdberr);
        if (rc < 0)
            return rc;

        if (rc == IX_FND) {
            cur->used_sd = 0;
        } else {
            logmsg(LOGMSG_ERROR, "%s argh?\n", __func__);
            *bdberr = BDBERR_BUG_KILLME;
            return -1;
        }
    }

    return 0;
}

static int bdb_copy_logfile_pglogs_to_shadow_tran(bdb_state_type *bdb_state,
                                                  tran_type *shadow_tran,
                                                  db_pgno_t *inpgno,
                                                  unsigned char *infileid,
                                                  int *bdberr)
{
    int rc = 0;

    struct shadows_pglogs_logical_key key;
    struct shadows_pglogs_logical_key *pglogs_ent = NULL;
    struct shadows_pglogs_key *client_pglogs_ent = NULL;
    struct lsn_commit_list *lsnent = NULL;
    struct lsn_list *add_lsnent = NULL;
    struct lsn_list *add_before_lsnent = NULL;

    struct pglogs_relink_key relink_key;
    struct pglogs_relink_key *relinks_ent = NULL;
    struct pglogs_relink_key *client_relinks_ent = NULL;
    struct relink_list *rlent = NULL;
    struct relink_list *add_rlent = NULL;
    struct relink_list *add_before_rlent = NULL;

    unsigned filenum, last_filenum;

    bzero(&key, sizeof(struct shadows_pglogs_logical_key));

    assert(inpgno);
    assert(infileid);

    key.pgno = *inpgno;
    memcpy(key.fileid, infileid, DB_FILE_ID_LEN);
    key.pgno = (key.pgno == 0) ? 1 : key.pgno;

    relink_key.pgno = key.pgno;
    memcpy(relink_key.fileid, infileid, DB_FILE_ID_LEN);

    Pthread_mutex_lock(&logfile_pglogs_repo_mutex);
    filenum = first_logfile;
    last_filenum = last_logfile;
    Pthread_mutex_unlock(&logfile_pglogs_repo_mutex);

    for (; filenum <= last_filenum; ++filenum) {
        struct logfile_pglogs_entry *l_entry;
        Pthread_mutex_lock(&logfile_pglogs_repo_mutex);
        l_entry = retrieve_logfile_pglogs(filenum, 0);

        // No writes ..
        if (!l_entry) {
            Pthread_mutex_unlock(&logfile_pglogs_repo_mutex);
            continue;
        }

        Pthread_mutex_lock(&l_entry->pglogs_mutex);
        Pthread_mutex_unlock(&logfile_pglogs_repo_mutex);

        /* copy pglogs */
        /* for each recorded page */
        pglogs_ent = hash_find(l_entry->pglogs_hashtbl, &key);
        if (pglogs_ent) {
            /* get the same page in the global structure */
            if ((client_pglogs_ent = hash_find(shadow_tran->pglogs_hashtbl,
                                               pglogs_ent)) == NULL) {
                /* add one if not exist */
                client_pglogs_ent = allocate_shadows_pglogs_key();
                if (!client_pglogs_ent) {
                    logmsg(LOGMSG_FATAL, "%s: fail malloc client_pglogs_ent\n",
                            __func__);
                    abort();
                }
                memcpy(client_pglogs_ent->fileid, pglogs_ent->fileid,
                       DB_FILE_ID_LEN);
                client_pglogs_ent->pgno = pglogs_ent->pgno;
                listc_init(&client_pglogs_ent->lsns,
                           offsetof(struct lsn_list, lnk));
                hash_add(shadow_tran->pglogs_hashtbl, client_pglogs_ent);
            }
            /* for each recorded lsn */
            LISTC_FOR_EACH_REVERSE(&pglogs_ent->lsns, lsnent, lnk)
            {
                if (log_compare(&lsnent->commit_lsn, &shadow_tran->asof_lsn) <=
                    0)
                    break;
                if (log_compare(&lsnent->commit_lsn, &shadow_tran->birth_lsn) <=
                    0) {
                    add_lsnent = allocate_lsn_list();
                    if (!add_lsnent)
                        abort();
                    add_lsnent->lsn = lsnent->lsn;
                    /* add in order */
                    LISTC_FOR_EACH(&client_pglogs_ent->lsns, add_before_lsnent,
                                   lnk)
                    {
                        if (log_compare(&add_lsnent->lsn,
                                        &add_before_lsnent->lsn) <= 0) {
                            listc_add_before(&client_pglogs_ent->lsns,
                                             add_lsnent, add_before_lsnent);
                            break;
                        }
                    }
                    if (add_before_lsnent == NULL) {
                        listc_abl(&client_pglogs_ent->lsns, add_lsnent);
                    }
                }
            }
        }

        /* copy relinks */
        /* for each recorded page */
        relinks_ent = hash_find(l_entry->relinks_hashtbl, &relink_key);
        if (relinks_ent) {
            /* get the same page in the global structure */
            if ((client_relinks_ent = hash_find(shadow_tran->relinks_hashtbl,
                                                relinks_ent)) == NULL) {
                /* add one if not exist */
                client_relinks_ent = allocate_pglogs_relink_key();
                if (!client_relinks_ent) {
                    logmsg(LOGMSG_FATAL, "%s: fail malloc client_relinks_ent\n",
                            __func__);
                    abort();
                }
                memcpy(client_relinks_ent, relinks_ent,
                       sizeof(struct pglogs_relink_key));
                listc_init(&client_relinks_ent->relinks,
                           offsetof(struct relink_list, lnk));
                hash_add(shadow_tran->relinks_hashtbl, client_relinks_ent);
            }
            /* for each recorded lsn */
            LISTC_FOR_EACH_REVERSE(&relinks_ent->relinks, rlent, lnk)
            {
                if (log_compare(&rlent->lsn, &shadow_tran->asof_ref_lsn) <= 0)
                    break;
                if (log_compare(&rlent->lsn, &shadow_tran->birth_lsn) <= 0) {
                    add_rlent = allocate_relink_list();
                    if (!add_rlent)
                        abort();
                    add_rlent->inh = rlent->inh;
                    add_rlent->lsn = rlent->lsn;
                    /* add in order */
                    LISTC_FOR_EACH(&client_relinks_ent->relinks,
                                   add_before_rlent, lnk)
                    {
                        if (log_compare(&add_rlent->lsn,
                                        &add_before_rlent->lsn) <= 0) {
                            listc_add_before(&client_relinks_ent->relinks,
                                             add_rlent, add_before_rlent);
                            break;
                        }
                    }
                    if (add_before_rlent == NULL) {
                        listc_abl(&client_relinks_ent->relinks, add_rlent);
                    }
                }
            }
        }
        Pthread_mutex_unlock(&l_entry->pglogs_mutex);
    }

    return rc;
}

static int bdb_btree_update_shadows_for_page(bdb_cursor_impl_t *cur,
                                             db_pgno_t *inpgno,
                                             unsigned char *infileid,
                                             DB_LSN upto, int *dirty,
                                             int *bdberr)
{
    int rc = 0;
    DB_LSN maxlsn = {0};
    struct shadows_pglogs_key key;
    struct shadows_pglogs_key *hashent = NULL;
    struct shadows_pglogs_key *pglogs_ent = NULL;
    struct lsn_list *lsnent = NULL;

    struct pglogs_relink_key relink_key;
    struct pglogs_relink_key *relinks_ent = NULL;
    struct relink_list *rlent = NULL;

    bzero(&key, sizeof(struct shadows_pglogs_key));
    bzero(&relink_key, sizeof(struct pglogs_relink_key));

    assert(inpgno);
    assert(infileid);
    key.pgno = *inpgno;
    memcpy(key.fileid, infileid, DB_FILE_ID_LEN);
    key.pgno = (key.pgno == 0) ? 1 : key.pgno;

    relink_key.pgno = key.pgno;
    memcpy(relink_key.fileid, infileid, DB_FILE_ID_LEN);

    update_pglogs_from_global_queues(cur, infileid, bdberr);

    if (cur->shadow_tran->asof_hashtbl &&
        ((hashent = hash_find(cur->shadow_tran->asof_hashtbl, &key)) == NULL)) {
        rc = bdb_copy_logfile_pglogs_to_shadow_tran(
            cur->state, cur->shadow_tran, inpgno, infileid, bdberr);
        if (!rc) {
            hashent = allocate_shadows_pglogs_key();
            if (!hashent) {
                logmsg(LOGMSG_ERROR, "%s: fail malloc hashent\n", __func__);
                return -1;
            }
            memcpy(hashent, &key, sizeof(struct shadows_pglogs_key));
            hash_add(cur->shadow_tran->asof_hashtbl, hashent);
        } else {
            logmsg(LOGMSG_ERROR, "%s failed to copy gbl pglogs to shadow tran\n",
                    __func__);
            return -1;
        }
    }

    assert(cur->shadow_tran->pglogs_hashtbl != NULL);
    assert(cur->shadow_tran->relinks_hashtbl != NULL);

    pglogs_ent = hash_find(cur->shadow_tran->pglogs_hashtbl, &key);
    if (pglogs_ent == NULL)
        goto do_relink;

    while ((lsnent = listc_rtl(&pglogs_ent->lsns)) != NULL) {
        if (log_compare(&lsnent->lsn, &maxlsn) > 0)
            maxlsn = lsnent->lsn;

        if (upto.file == 0 || upto.offset == 1 ||
            (log_compare(&lsnent->lsn, &upto) <= 0)) {
            rc = bdb_osql_update_shadows_with_pglogs(cur, lsnent->lsn,
                                                     cur->shadow_tran->osql,
                                                     dirty, cur->trak, bdberr);
            if (rc) {
                logmsg(LOGMSG_FATAL, "%s:%d failed to apply logical logs to shadow table\n",
                        __func__, __LINE__);
                abort();
            }
            deallocate_lsn_list(lsnent);
            /* printf("%s: freed lsn addr %p on hash %p ent %p list %p\n",
               __func__,
               lsnent, cur->shadow_tran->pglogs_hashtbl, pglogs_ent,
               &pglogs_ent->lsns); */
        } else {
            listc_atl(&pglogs_ent->lsns, lsnent);
            break;
        }
    }

do_relink:
    relinks_ent = hash_find(cur->shadow_tran->relinks_hashtbl, &relink_key);
    if (relinks_ent == NULL)
        goto out;

    while ((rlent = listc_rtl(&relinks_ent->relinks)) != NULL) {
        if (upto.file == 0 || upto.offset == 1 ||
            (log_compare(&rlent->lsn, &upto) <= 0)) {
            /* recursively call update shadows for relinked pages */
            rc = bdb_btree_update_shadows_for_page(cur, &rlent->inh, infileid,
                                                   rlent->lsn, dirty, bdberr);
            if (rc)
                abort();
            deallocate_relink_list(rlent);
        } else {
            listc_atl(&relinks_ent->relinks, rlent);
            break;
        }
    }

out:
#if 0
   {
      int len = (DB_FILE_ID_LEN * 2) + 1;
      char hex_fid[(DB_FILE_ID_LEN * 2) + 1];
      hex_fid[len - 1] = '\0';
      tohex(hex_fid, infileid, DB_FILE_ID_LEN);

      if (maxlsn.file)
      {
         lkprintf(stderr, "shadtrn %p cur %p upd_shad_for_page: updated %s page %d to lsn [%d][%d]\n",
               cur->shadow_tran, cur, hex_fid, *inpgno, maxlsn.file, maxlsn.offset);
      }
      else
      {
         lkprintf(stderr, "shadtrn %p cur %p upd_shad_for_page: nothing to update for %s page %d\n", 
               cur->shadow_tran, cur, hex_fid, *inpgno);
      }
   }
#endif

    return 0;
}

static int bdb_btree_update_shadows_with_trn_pglogs(bdb_cursor_impl_t *cur,
                                                    db_pgno_t *inpgno,
                                                    unsigned char *infileid,
                                                    int *bdberr)
{
    int rc = 0;
    int dirty_shadow = 0;
    DB_LSN upto;

    /* here, for snapshot/serializable sessions, we need to resync shadows. */
    if (!cur->shadow_tran ||
        (cur->shadow_tran->tranclass != TRANCLASS_SNAPISOL &&
         cur->shadow_tran->tranclass != TRANCLASS_SERIALIZABLE)) {
        if (cur->trak) {
            if (!cur->shadow_tran) {
                logmsg(LOGMSG_USER, "Cur %p skipping update shadows because "
                                "shadow_tran is NULL\n",
                        cur);
            } else if (cur->shadow_tran->tranclass != TRANCLASS_SNAPISOL &&
                       cur->shadow_tran->tranclass != TRANCLASS_SERIALIZABLE) {
                logmsg(LOGMSG_USER, "Cur %p skipping update shadows because it is "
                                "the wrong tranclass (%d)\n",
                        cur, cur->shadow_tran->tranclass);
            }
        }
        return 0;
    }

    upto.file = 0;
    upto.offset = 1;

#ifdef NEWSI_STAT
    struct timeval before, after, diff;
    gettimeofday(&before, NULL);
#endif

    update_pglogs_from_global_queues(cur, infileid, bdberr);

    Pthread_mutex_lock(&cur->shadow_tran->pglogs_mutex);
    rc = bdb_btree_update_shadows_for_page(cur, inpgno, infileid, upto,
                                           &dirty_shadow, bdberr);
    Pthread_mutex_unlock(&cur->shadow_tran->pglogs_mutex);
    if (rc)
        abort();

#ifdef NEWSI_STAT
    gettimeofday(&after, NULL);
    timeval_diff(&before, &after, &diff);
    Pthread_mutex_lock(&newsi_stat_mutex);
    timeval_add(&logical_undo_time, &diff, &logical_undo_time);
    Pthread_mutex_unlock(&newsi_stat_mutex);
#endif

    if (!dirty_shadow)
        goto done;

    /* NOTE:
       1) for relative moves,next/prev, we need to resync cur->sd with the new
       shadow image; this accounts for the situation when the shadow is updated
       after cursor creation
       2) absolute moves (first, last, find are not affected by this)

       - the lastkey contains genid if duped index, so no conflicts
       - direction matters; last_dup for prev, find for next
     */
    bdb_cursor_ifn_t *open_pcur_ifn = NULL;
    bdb_cursor_impl_t *open_cur = NULL;

    LISTC_FOR_EACH(&cur->shadow_tran->open_cursors, open_pcur_ifn, lnk)
    {
        open_cur = open_pcur_ifn->impl;

        /* we updated the shadows (i.e. inserted stuff into them) */
        if (!open_cur->sd && open_cur->idx < open_cur->state->attr->dtastripe) {
            /*
             * New data.  It is time to get my shadow cursor.  Use 'CREATE'
             * here:
             * page-order code adds to 'addcur', which doesn't trigger
             * shadow-table
             * creation.
             */
            open_cur->sd = bdb_berkdb_open(open_cur, BERKDB_SHAD_CREATE,
                                           MAXRECSZ, MAXKEYSZ, bdberr);
            if (!open_cur->sd) {
                logmsg(LOGMSG_ERROR, "%s: bdb_berkdb_open %d\n", __func__, *bdberr);
                rc = -1;
                goto done;
            }
        }

        if (open_cur->lastkeylen) {
            /*
             Lets try the new logic: when we update the shadow, we can mark them
             outoforder
             and force a repositioning
             */
            if (open_cur->sd) {
                open_cur->sd->outoforder_set(open_cur->sd, 1);
                open_cur->used_sd = 1; /* we need to reposition */
            }
        }
    }

done:
    return rc;
}

extern int gbl_update_shadows_interval;

static struct pglogs_queue_cursor *
retrieve_queue_cursor_fileid(bdb_cursor_impl_t *cur, unsigned char *fileid,
                             int *bdberr)
{
    struct pglogs_queue_cursor *plogq;

    if (!(plogq = hash_find(cur->shadow_tran->pglogs_queue_hash, fileid))) {
        plogq = allocate_pglogs_queue_cursor();
        memcpy(plogq->fileid, fileid, DB_FILE_ID_LEN);
        if (!(plogq->queue = retrieve_fileid_pglogs_queue(fileid, 1)))
            abort();
        plogq->last = NULL;
        hash_add(cur->shadow_tran->pglogs_queue_hash, plogq);
    }

    return plogq;
}

static struct pglogs_queue_cursor *retrieve_queue_cursor(bdb_cursor_impl_t *cur,
                                                         int *bdberr)
{
    unsigned char fileid[DB_FILE_ID_LEN];
    struct pglogs_queue_cursor *plogq;

    if (!cur->rl || cur->rl->fileid(cur->rl, fileid, bdberr))
        return NULL;

    return retrieve_queue_cursor_fileid(cur, fileid, bdberr);
}

static int should_update(tran_type *shadow_tran,
                         struct shadows_pglogs_queue_key *key)
{
    int i;

    if (log_compare(&key->commit_lsn, &shadow_tran->birth_lsn) > 0)
        return 1;

    if (!key->logical_tranid ||
        log_compare(&key->commit_lsn, &shadow_tran->oldest_txn_at_start) < 0)
        return 0;

    for (i = 0; i < shadow_tran->bkfill_txn_count; i++)
        if (key->logical_tranid == shadow_tran->bkfill_txn_list[i])
            return 1;

    return 0;
}

static int update_pglogs_from_queue(tran_type *shadow_tran,
                                    unsigned char *fileid,
                                    struct shadows_pglogs_queue_key *key)
{
    int ret = 0;

    switch (key->type) {
    case PGLOGS_QUEUE_PAGE:
        if (should_update(shadow_tran, key)) {
            ret = bdb_insert_pglogs_int(shadow_tran->pglogs_hashtbl, fileid,
                                        key->pgno, key->lsn);
        }
        break;

    case PGLOGS_QUEUE_RELINK:
        ret = bdb_insert_relinks_int(shadow_tran->relinks_hashtbl, fileid,
                                     key->pgno, key->prev_pgno, key->next_pgno,
                                     key->lsn);
        break;

    default:
        logmsg(LOGMSG_ERROR, "%s unknown key->type %d (free'd memory?) for %p\n", __func__,
               key->type, key);
        fsnapf(stdout, fileid, DB_FILE_ID_LEN);
        abort();
        break;
    }

    if (ret)
        abort();

    return 0;
}

static int update_pglogs_from_global_queues_int(
    bdb_cursor_impl_t *cur, struct pglogs_queue_cursor *qcur, int *bdberr)
{
    struct shadows_pglogs_queue_key *current, *prev, *last;
    DB_LSN last_lsn = {0};

    pthread_rwlock_rdlock(&qcur->queue->queue_lk);
    last = LISTC_BOT(&qcur->queue->queue_keys);

    // Walk backwards: this seems terrible.. maybe at txn registration time do
    // a hash_for to create point all of these at the end of the list?
    if ((prev = current = qcur->last) == NULL) {
        DB_LSN *start_lsn = cur->shadow_tran->oldest_txn_at_start.file
                                ? &cur->shadow_tran->oldest_txn_at_start
                                : &cur->shadow_tran->birth_lsn;
        int found_greater = 0;

        current = last;

        while (current && (current->type != PGLOGS_QUEUE_PAGE ||
                           log_compare(&current->commit_lsn, start_lsn) > 0)) {
            if (current->type == PGLOGS_QUEUE_PAGE)
                found_greater = 1;
            prev = current;
            current = current->lnk.prev;
        }

        if (!current && prev && prev->type == PGLOGS_QUEUE_PAGE) {
            update_pglogs_from_queue(cur->shadow_tran, qcur->fileid, prev);
            last_lsn = prev->commit_lsn;
            current = prev;
        }

        if (!found_greater)
            current = NULL;
    }

    pthread_rwlock_unlock(&qcur->queue->queue_lk);

    if (current)
        assert(last);

    // No locking: things above my birth_lsn can't disappear
    while (current && current != last) {
        current = current->lnk.next;
        update_pglogs_from_queue(cur->shadow_tran, qcur->fileid, current);
        if (current->type == PGLOGS_QUEUE_PAGE)
            last_lsn = current->commit_lsn;
    }

    qcur->last = current;

#if 0
   if (last_lsn.file)
   {
      int len = (DB_FILE_ID_LEN * 2) + 1;
      char hex_fid[(DB_FILE_ID_LEN * 2) + 1];
      hex_fid[len - 1] = '\0';
      tohex(hex_fid, qcur->fileid, DB_FILE_ID_LEN);
      lkprintf(stderr, "shadtrn %p cur %p upd_pglogs_from_queue: %s updated to [%d][%d]\n",
            cur->shadow_tran, cur, hex_fid, last_lsn.file, last_lsn.offset);
   }
#endif

    return 0;
}

static int update_pglogs_from_global_queues(bdb_cursor_impl_t *cur,
                                            unsigned char *fileid, int *bdberr)
{
    struct pglogs_queue_cursor *qcur = cur->queue_cursor;
    unsigned char myfileid[DB_FILE_ID_LEN];
    int ret;

    // Simple case: cached cursor points to the fileid we want to update
    if (qcur && (!fileid || !memcmp(qcur->fileid, fileid, DB_FILE_ID_LEN)))
        return update_pglogs_from_global_queues_int(cur, qcur, bdberr);

    // Retrieve the qcur
    if (fileid)
        qcur = retrieve_queue_cursor_fileid(cur, fileid, bdberr);
    else
        qcur = cur->queue_cursor = retrieve_queue_cursor(cur, bdberr);

    if (!qcur)
        abort();

    // Update pagelogs for this fileid
    if (ret = update_pglogs_from_global_queues_int(cur, qcur, bdberr))
        abort();

    // Cache cursor
    if (!cur->queue_cursor && cur->rl &&
        !cur->rl->fileid(cur->rl, myfileid, bdberr) &&
        !memcmp(myfileid, qcur->fileid, DB_FILE_ID_LEN))
        cur->queue_cursor = qcur;

    return 0;
}

/**
 * Update the shadow by applying the bdblogs
 * Creates a shadow cursor if there was an update and no cursor yet
 * Calls bdb_btree_update_shadow_cursor to reposition the cursor on
 * the updated image
 */
static int bdb_btree_update_shadows(bdb_cursor_impl_t *cur, int how,
                                    int *bdberr)
{
    int dirty_shadow = 0;
    int rc = 0;

    if (gbl_new_snapisol) {
        if (!cur->shadow_tran ||
            (cur->shadow_tran->tranclass != TRANCLASS_SNAPISOL &&
             cur->shadow_tran->tranclass != TRANCLASS_SERIALIZABLE)) {
            if (cur->trak) {
                if (!cur->shadow_tran) {
                    logmsg(LOGMSG_USER, "Cur %p skipping update shadows because "
                                    "shadow_tran is NULL\n",
                            cur);
                } else if (cur->shadow_tran->tranclass != TRANCLASS_SNAPISOL &&
                           cur->shadow_tran->tranclass !=
                               TRANCLASS_SERIALIZABLE) {
                    logmsg(LOGMSG_USER, "Cur %p skipping update shadows because it "
                                    "is the wrong tranclass (%d)\n",
                            cur, cur->shadow_tran->tranclass);
                }
            }
            return 0;
        }

        return update_pglogs_from_global_queues(cur, NULL, bdberr);
    }

    if (gbl_update_shadows_interval &&
        ++cur->upd_shadows_count < gbl_update_shadows_interval && cur->rl &&
        cur->rl->defer_update_shadows(cur->rl)) {
        if (cur->trak) {
            logmsg(LOGMSG_USER, "Cur %p deferring update shadows\n", cur);
        }
        return 0;
    }

    cur->upd_shadows_count = 0;

    /* here, for snapshot/serializable sessions, we need to resync shadows. */
    if (!cur->shadow_tran ||
        (cur->shadow_tran->tranclass != TRANCLASS_SNAPISOL &&
         cur->shadow_tran->tranclass != TRANCLASS_SERIALIZABLE)) {
        if (cur->trak) {
            if (!cur->shadow_tran) {
                logmsg(LOGMSG_USER, "Cur %p skipping update shadows because "
                                "shadow_tran is NULL\n",
                        cur);
            } else if (cur->shadow_tran->tranclass != TRANCLASS_SNAPISOL &&
                       cur->shadow_tran->tranclass != TRANCLASS_SERIALIZABLE) {
                logmsg(LOGMSG_USER, "Cur %p skipping update shadows because it is "
                                "the wrong tranclass (%d)\n",
                        cur, cur->shadow_tran->tranclass);
            }
        }
        return 0;
    }

    /* we have shadows, recom or snapisol/serial.   We have to do this even if
     * we're on the virtual stripe
     * to support optimized blobs */
    rc = bdb_osql_update_shadows(cur->ifn, cur->shadow_tran->osql,
                                 &dirty_shadow, LOG_APPLY, bdberr);
    if (rc)
        return rc;

    if (!dirty_shadow)
        return 0;

    /* NOTE:
       1) for relative moves,next/prev, we need to resync cur->sd with the new
       shadow image; this accounts for the situation when the shadow is updated
       after cursor creation
       2) absolute moves (first, last, find are not affected by this)

       - the lastkey contains genid if duped index, so no conflicts
       - direction matters; last_dup for prev, find for next
     */
    bdb_cursor_ifn_t *open_pcur_ifn = NULL;
    bdb_cursor_impl_t *open_cur = NULL;

    LISTC_FOR_EACH(&cur->shadow_tran->open_cursors, open_pcur_ifn, lnk)
    {
        open_cur = open_pcur_ifn->impl;

        /* we updated the shadows (i.e. inserted stuff into them) */
        if (!open_cur->sd && open_cur->idx < open_cur->state->attr->dtastripe) {
            /*
             * New data.  It is time to get my shadow cursor.  Use 'CREATE'
             * here:
             * page-order code adds to 'addcur', which doesn't trigger
             * shadow-table
             * creation.
             */
            open_cur->sd = bdb_berkdb_open(open_cur, BERKDB_SHAD_CREATE,
                                           MAXRECSZ, MAXKEYSZ, bdberr);
            if (!open_cur->sd) {
                logmsg(LOGMSG_ERROR, "%s: bdb_berkdb_open %d\n", __func__, *bdberr);
                return -1;
            }
        }

        if (open_cur->lastkeylen) {
            /*
             Lets try the new logic: when we update the shadow, we can mark them
             outoforder
             and force a repositioning
             */
            if (open_cur->sd) {
                open_cur->sd->outoforder_set(open_cur->sd, 1);
                open_cur->used_sd = 1; /* we need to reposition */
            }
        }
    }

    return rc;
}

/* Exposed pause function */
static int bdb_cursor_pause(bdb_cursor_ifn_t *pcur_ifn, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    if (cur->rl) {
        return cur->rl->pause(cur->rl, bdberr);
    } else {
        return 0;
    }
}

/**
 * Mark a cursor invalid, forcing a re-locking when the cursor moves
 *
 */
static int bdb_cursor_set_curtran(bdb_cursor_ifn_t *pcur_ifn,
                                  cursor_tran_t *curtran)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;

    /* save the curtran for later */
    cur->curtran = curtran;

    /* We don't set this flag if there's no cursor */
    if (cur->rl && cur->invalidated == 0) {
        logmsg(LOGMSG_ERROR, "%s: setting curtran while cursor is NOT invalidated???\n",
                __func__);
    }

    return 0;
}

/**
 * Parse a string containing an enable/disable feature and
 * return a proper return code for it
 *
 */
int bdb_osql_trak(char *sql, unsigned int *status)
{
    if (strncasecmp(sql, "ALL", 3) == 0) {
        *status |= SQL_DBG_ALL;
        return 0;
    }
    if (strncasecmp(sql, "BDBLOG", 6) == 0) {
        *status |= SQL_DBG_BDBLOG;
        return 0;
    }
    if (strncasecmp(sql, "BDBALLLOG", 9) == 0) {
        *status |= SQL_DBG_BDBALLLOG;
        return 0;
    }
    if (strncasecmp(sql, "BDBTRN", 6) == 0) {
        *status |= SQL_DBG_BDBTRN;
        return 0;
    }
    if (strncasecmp(sql, "BDBALLTRN", 9) == 0) {
        *status |= SQL_DBG_BDBALLTRN;
        bdb_osql_trn_trak(SQL_DBG_BDBALLTRN);

        return 0;
    }
    if (strncasecmp(sql, "BDBALLLOG", 9) == 0) {
        *status |= SQL_DBG_BDBALLLOG;
        return 0;
    }
    if (strncasecmp(sql, "BDBSHADOW", 6) == 0) {
        *status |= SQL_DBG_SHADOW;
        return 0;
    }
    return -1;
}

char const *cursortype(int type)
{
    if (type == BDBC_IX)
        return "IX";
    if (type == BDBC_DT)
        return "DT";
    return "Unknown";
}

static void *unpack_datacopy_odh(bdb_cursor_ifn_t *cur, uint8_t *to,
                                 int to_size, uint8_t *from, int from_size,
                                 uint8_t *ver)
{
    bdb_cursor_impl_t *berkdb = cur->impl;
    bdb_realdb_tag_t *bt = &berkdb->rl->impl->u.rl;
    bdb_rowlocks_tag_t *brl = &berkdb->rl->impl->u.row;
    bdb_state_type *bdb_state = berkdb->state;
    int use_bulk;
    int rc;
    void *bulk_dta = NULL;
    struct odh odh;

    if (berkdb->rowlocks) {
        use_bulk = brl->use_bulk;
        bulk_dta = brl->dtamem;
    } else {
        use_bulk = bt->use_bulk;
        bulk_dta = bt->data.data;
    }

    rc = bdb_unpack(bdb_state, from, from_size, to, to_size, &odh, NULL);

    if (rc) {
        abort();
    }

    *ver = odh.csc2vers;

    /* Copy it to avoid vtag_to_ondisk overruns. */
    if (use_bulk && odh.length < bdb_state->lrl) {
        memcpy(bulk_dta, odh.recptr, odh.length);
        return bulk_dta;
    } else {
        return odh.recptr;
    }
}

static const char *curtypetostr(int type)
{
    switch (type) {
    default:
    case BDBC_UN:
        return "unknown?????";
    case BDBC_IX:
        return "index";
    case BDBC_DT:
        return "dta";
    case BDBC_SK:
        return "shadow";
    case BDBC_BL:
        return "blob";
    }
}

/**
 * Reposition a cursor
 * This function is dedicated to shadow merging and repositioning
 * case, which is used only from index merging
 *
 * RETURNS:
 * IX_FND      - if the record cached in cur->key exists and found
 * IX_NOTFND   - if the record cached in -"- does not exist, but we found the
 *next record in order
 * IX_PASTEOF  - if we reached the end of sequence
 * IX_EMPTY    - if no rows
 * <0          - error, and bdberr is set accordingly
 *
 */
static int bdb_cursor_reposition_noupdate_int(bdb_cursor_ifn_t *pcur_ifn,
                                              bdb_berkdb_t *berkdb, char *key,
                                              int keylen, int how, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    char *data = NULL;
    int rc = 0;

    assert(how == DB_NEXT || how == DB_PREV);

#if 0
   assert( cur->type == BDBC_IX);
#endif

    if (cur->type == BDBC_IX && bdb_keycontainsgenid(cur->state, cur->idx))
        keylen += sizeof(unsigned long long);

    switch (how) {
    case DB_NEXT: {
        rc = bdb_cursor_find_and_skip(cur, berkdb, key, keylen, DB_SET_RANGE, 0,
                                      1, bdberr);
        if (rc < 0)
            return rc;

        if (rc == IX_FND) {
            /* is this the key we really want, or the next one? */
            char *found_key = NULL;
            int found_keylen = 0;

            rc = berkdb->key(berkdb, &found_key, bdberr);
            if (rc < 0)
                return rc;
            rc = berkdb->keysize(berkdb, &found_keylen, bdberr);
            if (rc < 0)
                return rc;

            assert(keylen <= found_keylen);

            rc = memcmp(key, found_key, keylen);

            assert(rc <= 0);

            if (rc < 0) {
                rc = IX_NOTFND; /* found the next in order */
            } else if (rc > 0) {
                rc = IX_PASTEOF; /* last gave us a smaller row */
            } else {
                rc = IX_FND; /* bingo */
            }

            /*
            fprintf( stderr, "%d %s:%d returning rc=%d keylen=%d key=%llx
            found_key=%llx\n",
                  pthread_self(), __FILE__, __LINE__, rc, keylen,
                  *(unsigned long long*)key, *(unsigned long long*)found_key
                  );
             */
        }
        /*
        else
           fprintf( stderr, "%d %s:%d returning rc=%d keylen=%d key=%llx\n",
                 pthread_self(), __FILE__, __LINE__, rc, keylen,
                 *(unsigned long long*)key);
         */
        break;
    }
    case DB_PREV: {
        char *keycopy = alloca(keylen);
        memcpy(keycopy, key, keylen);

#if MERGE_DEBUG
        print_cursor_keys(cur, BDB_SHOW_BOTH);
#endif

        rc = bdb_cursor_find_and_skip(cur, berkdb, key, keylen, DB_SET_RANGE,
                                      1 /* keylen incremented */, 1, bdberr);
#if MERGE_DEBUG
        fprintf(
            stderr,
            "%d %s:%d rc=%d, used_sd=%d, used_rl=%d, cur->genid=%llx [%d]\n",
            pthread_self(), __FILE__, __LINE__, rc, cur->used_sd, cur->used_rl,
            cur->genid, how);
#endif

        if (rc < 0)
            return rc;
        if (rc == IX_PASTEOF ||
            rc == IX_NOTFND) /*reuse of ll functions generate also IX_NOTFND*/
        {
            if (((cur->rl == berkdb && cur->used_rl) ||
                 (cur->sd == berkdb && cur->used_sd))) {
#if MERGE_DEBUG
                fprintf(
                    stderr,
                    "%d %s:%d we used this value so for PREV need"
                    "to \nreturn rc=%d, berkdb is %s, used_sd=%d, used_rl=%d, "
                    "sd_eof=%d, rl_eof=%d, key=%x [%d]\n",
                    pthread_self(), __FILE__, __LINE__, rc,
                    (cur->rl == berkdb ? "RL" : "SD"), cur->used_sd,
                    cur->used_rl, cur->sd->is_at_eof(cur->sd),
                    cur->rl->is_at_eof(cur->rl), key, how);
#endif
                break;
            }

            rc = bdb_cursor_move_and_skip_int(cur, berkdb, DB_LAST, 0, 1,
                                              bdberr);
            if (rc < 0)
                return rc;

            if (rc == IX_EMPTY)
                break;
        } else if (rc == IX_FND) {

            rc = bdb_cursor_move_and_skip_int(cur, berkdb, DB_PREV, 0, 1,
                                              bdberr);
            if (rc < 0)
                return rc;

            /* we are still positioned on the last record */
            if (rc == IX_PASTEOF)
                break;
        } else if (rc == IX_EMPTY) {
            break;
        } else {
            logmsg(LOGMSG_ERROR, "%s: ugh rc=%d???\n", __func__, rc);
            cheap_stack_trace();
            *bdberr = BDBERR_BADARGS;
            return -1;
        }

        /* wiper mode; at this point berkdb is poiting to some row */
        do {
            /* IX_FND */
            /* THE CURSOR POSITION IS NOT YET UPDATED, NEED TO RETRIEVE THE DATA
             * DIRECT FROM BERKDB */
            rc = berkdb->key(berkdb, &data, bdberr);
            if (rc < 0)
                return rc;

            rc = memcmp(data, keycopy, keylen);
            if (rc <= 0)
                break;

            rc = bdb_cursor_move_and_skip_int(cur, berkdb, DB_PREV, 0, 1,
                                              bdberr);
            if (rc < 0)
                return rc;
            if (rc == IX_PASTEOF)
                break;

        } while (rc == IX_FND);

        if (rc <= 0) {
            if (rc < 0)
                rc = IX_NOTFND;
            else
                rc = IX_FND;
        } else {
            assert(rc == IX_PASTEOF);
        }

    } break;
    }

    return rc;
}

static int bdb_cursor_reposition_noupdate(bdb_cursor_ifn_t *pcur_ifn,
                                          bdb_berkdb_t *berkdb, char *key,
                                          int keylen, int how, int *bdberr)
{
    int rc;
    berkdb->prevent_optimized(berkdb);
    rc = bdb_cursor_reposition_noupdate_int(pcur_ifn, berkdb, key, keylen, how,
                                            bdberr);
    berkdb->allow_optimized(berkdb);
    return rc;
}

/**
 * Reposition a cursor
 *
 */
static int bdb_cursor_reposition(bdb_cursor_ifn_t *pcur_ifn, int how,
                                 int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    int rc = 0;
    char key[MAXKEYSZ];
    int keysize = 0;
    unsigned long long original_genid = 0;

    if (cur->used_rl == 0 && cur->used_sd == 0)
        return 0;

    if (cur->type == BDBC_DT) {
        /* these are unique */
        memcpy(key, (char *)&cur->genid, sizeof(cur->genid));
        keysize = sizeof(cur->genid);
    } else {
        /* if these are dup indexes, we know the genid
           followed the data row
           use that so that we do not have dups in the following lookups
         */
        memcpy(key, cur->data, cur->datalen);
        keysize = cur->datalen;

        if (bdb_keycontainsgenid(cur->state, cur->idx)) {
            /*keysize += sizeof(unsigned long long); Uh*/
            memcpy(key + cur->datalen, (char *)&cur->genid, sizeof(cur->genid));
        }

        /* we also need to store the genid for later match,
           see comment after switch-find */
        original_genid = cur->genid;
    }

    /* we have not more worries about dups here */
    switch (how) {
    case BDB_SET:
        abort();
        break;

    case BDB_NEXT:
    case BDB_PREV:
        rc = bdb_cursor_reposition_noupdate(
            pcur_ifn, cur->rl, key, keysize,
            (how == BDB_NEXT) ? DB_NEXT : DB_PREV, bdberr);
        break;

    default:
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* if found something:
       - for indexes need to check if it is the same
       - for data, with no inplace updates rows are time ordered
       therefore I don't do nothing (until ipu are supported)
     */
    if (cur->type == BDBC_IX && rc == IX_FND) {
        if (cur->genid != original_genid) {
            /* we did not really re-find this
               it is not sure at this point if returning an
               replacement instead of the next row is what we need,
               but intuitively it seems the correct thing to do
             */
            rc = IX_NOTFND;
        }
    }

    return rc;
}

static void *bdb_cursor_collattr(bdb_cursor_ifn_t *cur)
{
    return cur->impl->collattr;
}

static int bdb_cursor_collattrlen(bdb_cursor_ifn_t *cur)
{
    return cur->impl->collattr_len;
}

struct count_arg {
    DB *db;
    int64_t count;
    int rc;
};

static void *db_count(void *varg)
{
    int rc;
    struct count_arg *arg = varg;

    DBT k = {0};
    k.data = alloca(MAXKEYSZ);
    k.ulen = MAXKEYSZ;
    k.flags = DB_DBT_USERMEM;

    DBT v = {0};
    v.data = alloca(128 * 1024);
    v.ulen = 128 * 1024;
    v.flags = DB_DBT_USERMEM;

    DB *db = arg->db;
    DBC *dbc;
    if ((rc = db->cursor(db, NULL, &dbc, 0)) != 0) {
        arg->rc = rc;
        return NULL;
    }
    int64_t count = 0;
    while ((rc = dbc->c_get(dbc, &k, &v, DB_NEXT | DB_MULTIPLE_KEY)) == 0) {
        uint8_t *kk, *vv;
        uint32_t ks, vs;
        void *bulk;
        DB_MULTIPLE_INIT(bulk, &v);
        DB_MULTIPLE_KEY_NEXT(bulk, &v, kk, ks, vv, vs);
        while (bulk) {
            ++count;
            DB_MULTIPLE_KEY_NEXT(bulk, &v, kk, ks, vv, vs);
        }
    }
    dbc->c_close(dbc);
    arg->rc = rc;
    arg->count = count;
    return NULL;
}

int gbl_parallel_count = 0;
int bdb_direct_count(bdb_cursor_ifn_t *cur, int ixnum, int64_t *rcnt)
{
    int64_t count = 0;
    int parallel_count;
    bdb_state_type *state = cur->impl->state;
    DB **db;
    int stripes;
    pthread_attr_t attr;
    if (ixnum < 0) { // data
        db = state->dbp_data[0];
        stripes = state->attr->dtastripe;
        parallel_count = gbl_parallel_count;
        // max page 64K
        // allocate twice that + 4K, in case page compressed "really" well
        pthread_attr_init(&attr);
#ifdef PTHREAD_STACK_MIN
        pthread_attr_setstacksize(&attr, PTHREAD_STACK_MIN + 132 * 1024);
#endif
    } else { // index
        db = &state->dbp_ix[ixnum];
        stripes = 1;
        parallel_count = 0;
    }
    struct count_arg args[stripes];
    pthread_t thds[stripes];
    for (int i = 0; i < stripes; ++i) {
        args[i].db = db[i];
        if (parallel_count) {
            pthread_create(&thds[i], &attr, db_count, &args[i]);
        } else {
            db_count(&args[i]);
        }
    }
    int rc = 0;
    void *ret;
    for (int i = 0; i < stripes; ++i) {
        if (parallel_count) {
            pthread_join(thds[i], &ret);
        }
        if (args[i].rc == DB_LOCK_DEADLOCK) {
            rc = BDBERR_DEADLOCK;
            break;
        } else if (args[i].rc != DB_NOTFOUND) {
            rc = -1;
            break;
        }
        rc = 0;
        count += args[i].count;
    }
    if (parallel_count) {
        pthread_attr_destroy(&attr);
    }
    if (rc == 0) *rcnt = count;
    return rc;
}
