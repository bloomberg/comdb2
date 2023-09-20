/*
   Copyright 2015, 2017, Bloomberg Finance L.P.

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

#include "sqlglue.h"

#include "sqloffload.h"
#include "analyze.h"

#ifndef DEBUG_TYPES
#include "types.c"
#endif

/*
  low level code needed to support sql
 */

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <stdarg.h>
#include <stddef.h>
#include <limits.h>
#include <ctype.h>
#include <alloca.h>
#include <strings.h>
#include <stdarg.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <unistd.h>

#include <zlib.h>

#include <plbitlib.h>
#include <tcputil.h>
#include <sockpool.h>

#include <cdb2api.h>

#include <plhash.h>
#include <list.h>
#include <ctrace.h>
#include <epochlib.h>

#include <sbuf2.h>

#include <bdb_api.h>
#include <bdb_cursor.h>
#include <bdb_fetch.h>
#include <bdb_int.h>
#include <time.h>

#include "comdb2.h"
#include "crc32c.h"
#include "types.h"
#include "util.h"
#include <fsnapf.h>

#include "flibc.h"

#include "sql.h"
#include <sqliteInt.h>
#include <vdbeInt.h>
#include <sqlite_btree.h>
#include <os.h>
#include <sqlite3.h>

#include "dbinc/debug.h"
#include "sqlinterfaces.h"

#include "osqlsqlthr.h"
#include "osqlshadtbl.h"
#include "bdb/bdb_schemachange.h"

#include <genid.h>
#include <strbuf.h>
#include <thread_malloc.h>
#include "fdb_fend.h"
#include "fdb_access.h"
#include "bdb_osqlcur.h"

#include "debug_switches.h"
#include "logmsg.h"
#include "reqlog.h"
#include "locks.h"
#include "eventlog.h"
#include "db_access.h"
#include "str0.h"
#include "comdb2_atomic.h"
#include "sc_util.h"
#include "comdb2_query_preparer.h"
#include <portmuxapi.h>
#include "cdb2_constants.h"
#include <translistener.h>

int gbl_delay_sql_lock_release_sec = 5;

unsigned long long get_id(bdb_state_type *);
static void unlock_bdb_cursors(struct sql_thread *thd, bdb_cursor_ifn_t *bdbcur,
                               int *bdberr);

struct temp_cursor;
struct temp_table;
extern int gbl_partial_indexes;
#define SQLITE3BTREE_KEY_SET_INS(IX) (clnt->ins_keys |= (1ULL << (IX)))
#define SQLITE3BTREE_KEY_SET_DEL(IX) (clnt->del_keys |= (1ULL << (IX)))
extern int gbl_expressions_indexes;
extern int gbl_debug_tmptbl_corrupt_mem;

// Lua threads share temp tables.
// Don't create new btree, use this one (tmptbl_clone)
static __thread struct temptable *tmptbl_clone = NULL;

uint32_t gbl_sql_temptable_count;
int gbl_throttle_txn_chunks_msec = 0;

void free_cached_idx(uint8_t **cached_idx)
{
    int i;
    if (!cached_idx)
        return;
    for (i = 0; i < MAXINDEX; i++) {
        if (cached_idx[i]) {
            free(cached_idx[i]);
            cached_idx[i] = NULL;
        }
    }
}

static void sqlite3MakeSureDbHasErr(sqlite3 *db, int rc){
    if ((db == NULL) || (db->errCode != SQLITE_OK)) return;
    db->errCode = (rc != SQLITE_OK) ? rc : SQLITE_ERROR;
}

extern int sqldbgflag;
extern int gbl_notimeouts;
extern int gbl_move_deadlk_max_attempt;
extern int gbl_fdb_track;
extern int gbl_selectv_rangechk;
extern int gbl_enable_internal_sql_stmt_caching;

unsigned long long gbl_sql_deadlock_reconstructions = 0;
unsigned long long gbl_sql_deadlock_failures = 0;

extern int sqldbgflag;
extern int gbl_dump_sql_dispatched; /* dump all sql strings dispatched */

static int ddguard_bdb_cursor_find(struct sql_thread *thd, BtCursor *pCur,
                                   bdb_cursor_ifn_t *cur, void *key, int keylen,
                                   int is_temp_bdbcur, int bias, int *bdberr);
static int ddguard_bdb_cursor_find_last_dup(struct sql_thread *, BtCursor *,
                                            bdb_cursor_ifn_t *, void *key,
                                            int keylen, int keymax, bias_info *,
                                            int *bdberr);
static int ddguard_bdb_cursor_move(struct sql_thread *thd, BtCursor *pCur,
                                   int flags, int *bdberr, int how,
                                   struct ireq *iq_do_prefault,
                                   int freshcursor);
static int is_sql_update_mode(int mode);
static int queryOverlapsCursors(struct sqlclntstate *clnt, BtCursor *pCur);

enum { AUTHENTICATE_READ = 1, AUTHENTICATE_WRITE = 2 };

static int chunk_transaction(BtCursor *pCur, struct sqlclntstate *clnt,
                             struct sql_thread *thd);

CurRange *currange_new()
{
    CurRange *rc = (CurRange *)malloc(sizeof(CurRange));
    rc->tbname = NULL;
    rc->idxnum = -2;
    rc->lkey = NULL;
    rc->rkey = NULL;
    rc->lflag = 0;
    rc->rflag = 0;
    rc->lkeylen = 0;
    rc->rkeylen = 0;
    rc->islocked = 0;
    return rc;
}

void currangearr_init(CurRangeArr *arr)
{
    arr->size = 0;
    arr->cap = CURRANGEARR_INIT_CAP;
    arr->file = 0;
    arr->offset = 0;
    arr->hash = NULL;
    arr->ranges = malloc(sizeof(CurRange *) * arr->cap);
}

void currangearr_append(CurRangeArr *arr, CurRange *r)
{
    currangearr_double_if_full(arr);
    assert(r);
    arr->ranges[arr->size++] = r;
}

CurRange *currangearr_get(CurRangeArr *arr, int n)
{
    if (n >= arr->size || n < 0) {
        return NULL; // out of range
    }
    return arr->ranges[n];
}

void currangearr_double_if_full(CurRangeArr *arr)
{
    if (arr->size >= arr->cap) {
        arr->cap *= 2;
        arr->ranges = realloc(arr->ranges, sizeof(CurRange *) * arr->cap);
    }
}

static int currange_cmp(const void *p, const void *q)
{
    CurRange *l = *(CurRange **)p;
    CurRange *r = *(CurRange **)q;
    int rc;
    assert(l);
    assert(r);
    if (!l->tbname)
        return 1;
    if (!r->tbname)
        return -1;
    rc = strcmp(l->tbname, r->tbname);
    if (rc)
        return rc;
    if (l->islocked || r->islocked)
        return r->islocked - l->islocked;
    if (l->idxnum != r->idxnum)
        return l->idxnum - r->idxnum;
    if (l->lflag)
        return -1;
    if (r->lflag)
        return 1;
    if (l->lkey && r->lkey) {
        rc = memcmp(l->lkey, r->lkey,
                    (l->lkeylen < r->lkeylen ? l->lkeylen : r->lkeylen));
        if (rc)
            return rc;
        else
            return l->lkeylen - r->lkeylen;
    }
    return 0;
}

static inline void currangearr_sort(CurRangeArr *arr)
{
    qsort((void *)arr->ranges, arr->size, sizeof(CurRange *), currange_cmp);
}

static void currangearr_merge_neighbor(CurRangeArr *arr)
{
    int i, j;
    j = 0;
    i = 1;
    int n = arr->size;
    CurRange *p, *q;
    void *tmp;
    if (!n)
        return;
    while (i < n) {
        p = arr->ranges[j];
        q = arr->ranges[i];
        if (strcmp(p->tbname, q->tbname) == 0) {
            if (p->idxnum == q->idxnum) {
                assert(p->rflag || p->rkey);
                if ((q->lflag) ||
                    (p->rflag ||
                     memcmp(q->lkey, p->rkey,
                            (q->lkeylen < p->rkeylen ? q->lkeylen
                                                     : p->rkeylen)) <= 0)) {
                    // coalesce
                    if (p->rflag || q->rflag) {
                        p->rflag = 1;
                        if (p->rkey) {
                            free(p->rkey);
                            p->rkey = NULL;
                        }
                        p->rkeylen = 0;
                    } else if (memcmp(p->rkey, q->rkey,
                                      (p->rkeylen < q->rkeylen ? p->rkeylen
                                                               : q->rkeylen)) <
                               0) {
                        tmp = q->rkey;
                        q->rkey = p->rkey;
                        p->rkey = tmp;
                    }
                    currange_free(q);
                    arr->ranges[i] = NULL;
                    if (p->lflag && p->rflag)
                        p->islocked = 1;
                    i++;
                    continue;
                }
            } else {
                if (p->islocked) {
                    // merge
                    currange_free(q);
                    arr->ranges[i] = NULL;
                    i++;
                    continue;
                }
            }
        }
        j++;
        if (j != i) {
            // move record
            arr->ranges[j] = arr->ranges[i];
        }
        i++;
    }
    arr->size = j + 1;
}

static inline void currangearr_coalesce(CurRangeArr *arr)
{
    currangearr_sort(arr);
    currangearr_merge_neighbor(arr);
    currangearr_sort(arr);
    currangearr_merge_neighbor(arr);
}

void currangearr_build_hash(CurRangeArr *arr)
{
    if (arr->size == 0)
        return;
    hash_t *range_hash =
        hash_init_strptr(offsetof(struct serial_tbname_hash, tbname));
    for (int i = 0; i < arr->size; i++) {
        struct serial_tbname_hash *th;
        struct serial_index_hash *ih;
        CurRange *r = arr->ranges[i];
        if ((th = hash_find(range_hash, &(r->tbname))) == NULL) {
            th = malloc(sizeof(struct serial_tbname_hash));
            th->tbname = strdup(r->tbname);
            th->islocked = r->islocked;
            th->begin = i;
            th->end = i;
            th->idx_hash = hash_init_o(
                offsetof(struct serial_index_hash, idxnum), sizeof(int));
            ih = malloc(sizeof(struct serial_index_hash));
            ih->idxnum = r->idxnum;
            ih->begin = i;
            ih->end = i;
            hash_add(th->idx_hash, ih);
            hash_add(range_hash, th);
        } else {
            th->end = i;
            if ((ih = hash_find(th->idx_hash, &(r->idxnum))) == NULL) {
                ih = malloc(sizeof(struct serial_index_hash));
                ih->begin = i;
                ih->end = i;
                ih->idxnum = r->idxnum;
                hash_add(th->idx_hash, ih);
            } else {
                ih->end = i;
                ;
            }
        }
    }
    arr->hash = range_hash;
}

static int free_idxhash(void *obj, void *arg)
{
    struct serial_index_hash *ih = (struct serial_index_hash *)obj;
    free(ih);
    return 0;
}

static int free_rangehash(void *obj, void *arg)
{
    struct serial_tbname_hash *th = (struct serial_tbname_hash *)obj;
    free(th->tbname);
    hash_for(th->idx_hash, free_idxhash, NULL);
    hash_clear(th->idx_hash);
    hash_free(th->idx_hash);
    free(th);
    return 0;
}
void currangearr_free(CurRangeArr *arr)
{
    if (!arr)
        return;
    int i;
    for (i = 0; i < arr->size; i++) {
        currange_free(arr->ranges[i]);
        arr->ranges[i] = NULL;
    }
    free(arr->ranges);
    if (arr->hash) {
        hash_for(arr->hash, free_rangehash, NULL);
        hash_clear(arr->hash);
        hash_free(arr->hash);
    }
    free(arr);
}

void currange_free(CurRange *cr)
{
    if (cr->tbname) {
        free(cr->tbname);
        cr->tbname = NULL;
    }
    if (cr->lkey) {
        free(cr->lkey);
        cr->lkey = NULL;
    }
    if (cr->rkey) {
        free(cr->rkey);
        cr->rkey = NULL;
    }
    free(cr);
}

void currangearr_print(CurRangeArr *arr)
{
    if (arr == NULL)
        return;
    CurRange *cr;
    int i;
    if (arr) {
        logmsg(LOGMSG_USER, "!!! SIZE: %d !!!\n", arr->size);
        logmsg(LOGMSG_USER, "!!! LSN: [%d][%d] !!!\n", arr->file, arr->offset);
        for (i = 0; i < arr->size; i++) {
            cr = arr->ranges[i];
            logmsg(LOGMSG_USER, "------------------------\n");
            if (cr->tbname) {
                logmsg(LOGMSG_USER, "!!! tbname: %s !!!\n", cr->tbname);
            }
            logmsg(LOGMSG_USER, "!!! islocked: %d !!!\n", cr->islocked);
            logmsg(LOGMSG_USER, "!!! idxnum: %d !!!\n", cr->idxnum);
            logmsg(LOGMSG_USER, "!!! lflag: %d !!!\n", cr->lflag);
            if (cr->lkey) {
               logmsg(LOGMSG_USER, "!!! lkeylen: %d !!!\n", cr->lkeylen);
                fsnapf(stdout, cr->lkey, cr->lkeylen);
            }
           logmsg(LOGMSG_USER, "!!! rflag: %d !!!\n", cr->rflag);
            if (cr->rkey) {
                logmsg(LOGMSG_USER, "!!! rkeylen: %d !!!\n", cr->rkeylen);
                fsnapf(stdout, cr->rkey, cr->rkeylen);
            }
        }
    }
}

/* return 0 if authenticated, else -1 */
int authenticate_cursor(BtCursor *pCur, int how)
{
    int rc;

    /* check if the cursor is doing a remote accesss, and if so
       check for explicit whitelisting */
    if (pCur->bt && pCur->bt->is_remote && pCur->fdbc) {

        rc = pCur->fdbc->access(pCur, (how == AUTHENTICATE_READ)
                                          ? ACCESS_REMOTE_READ
                                          : ACCESS_REMOTE_WRITE);
        if (rc) {
            logmsg(LOGMSG_WARN, "%s: remote access denied mode=%d\n", __func__,
                    how);
            return -1;
        }
    }

    if (!gbl_uses_password)
        return 0;

    /* always allow read access to sqlite_master */
    if (pCur->rootpage == RTPAGE_SQLITE_MASTER) {
        if (how == AUTHENTICATE_READ)
            return 0;
        logmsg(LOGMSG_ERROR, "%s: query requires write access to ftable???\n",
                __func__);
        return -1;
    }

    return 0;
}

int peer_dropped_connection_sbuf(SBUF2 *sb)
{
    if (!sb)
        return 0;

    int rc;
    struct pollfd fd = {0};
    fd.fd = sbuf2fileno(sb);
    fd.events = POLLIN;
    if ((rc = poll(&fd, 1, 0)) >= 0) {
        if (fd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            return 1;
        }
        return 0;
    } else if (errno == EINTR || errno == EAGAIN) {
        return 0;
    }
    return 1;
}

static int skip_clnt_check(struct sqlclntstate *clnt)
{
    return clnt == NULL || clnt->skip_peer_chk || clnt->in_sqlite_init;
}

int peer_dropped_connection(struct sqlclntstate *clnt)
{
    if (skip_clnt_check(clnt)) {
        return 0;
    }
    return clnt->plugin.peer_check(clnt);
}

int throttle_num = 0;
int calls_per_second;

void set_throttle(int num) { throttle_num = num; }

int throttle_sleep_time = 1;

int get_throttle(void) { return throttle_num; }

void throttle(void)
{
    calls_per_second++;
    if (throttle_num > 0) {
        throttle_num--;
        poll(NULL, 0, throttle_sleep_time);
    }
}

int get_calls_per_sec(void) { return calls_per_second; }

void reset_calls_per_sec(void) { calls_per_second = 0; }

static void handle_failed_recover_deadlock(struct sqlclntstate *clnt,
                                    int recover_deadlock_rcode)
{
    char *str;
    int rc;
    clnt->ready_for_heartbeats = 0;
    assert(bdb_lockref() == 0);
    switch (recover_deadlock_rcode) {
    case SQLITE_COMDB2SCHEMA:
        str = "Database schema has changed";
        rc = CDB2ERR_SCHEMA;
        break;
    case SQLITE_CLIENT_CHANGENODE:
        str = "Client api shoudl retry request";
        rc = CDB2ERR_CHANGENODE;
        break;
    default:
        str = "Failed to reaquire locks on deadlock";
        rc = recover_deadlock_rcode;
        break;
    }
    errstat_set_rcstrf(&clnt->osql.xerr, rc, str);
    if (clnt->recover_ddlk_fail) {
        clnt->recover_ddlk_fail(clnt, str);
    } else {
        struct Vdbe *vdbe = (struct Vdbe *)clnt->dbtran.pStmt;
        sqlite3_mutex_enter(sqlite3_db_mutex(vdbe->db));
        sqlite3VdbeError(vdbe, "%s", str);
        sqlite3_mutex_leave(sqlite3_db_mutex(vdbe->db));
    }
    logmsg(LOGMSG_DEBUG, "%s %s\n", __func__, str);
}

static inline int check_recover_deadlock(struct sqlclntstate *clnt)
{
    int rc;

    if ((rc = clnt->recover_deadlock_rcode)) {
        assert(bdb_lockref() == 0);
        handle_failed_recover_deadlock(clnt, rc);
        logmsg(LOGMSG_ERROR, "%s: failing on recover_deadlock error\n",
                __func__);
    }
    return rc < 0 ? SQLITE_BUSY : rc;
}

static int is_sqlite_db_init(BtCursor *pCur)
{
    sqlite3 *db = NULL;
    if (pCur->vdbe) {
        db = pCur->vdbe->db;
    }
    if (db && db->init.busy) {
        return 1;
    }
    return 0;
}

int check_sql_client_disconnect(struct sqlclntstate *clnt, char *file, int line)
{
    extern int gbl_epoch_time;
    extern int gbl_watchdog_disable_at_start;
    if (gbl_watchdog_disable_at_start)
        return 0;
    if (gbl_epoch_time && (gbl_epoch_time - clnt->last_check_time > 5)) {
        clnt->last_check_time = gbl_epoch_time;
        if (!gbl_notimeouts && peer_dropped_connection(clnt)) {
            logmsg(LOGMSG_INFO, "Peer dropped connection %s:%d\n", file, line);
            clnt->thd->sqlthd->stop_this_statement = 1;
            return 1;
        }
    }
    return 0;
}
/*
   This is called every time the db does something (find/next/etc. on a cursor).
   The query is aborted if this returns non-zero.
 */
int gbl_debug_sleep_in_sql_tick;
int gbl_debug_sleep_in_analyze;
static int sql_tick(struct sql_thread *thd, int no_recover_deadlock)
{
    int rc;
    extern int gbl_epoch_time;

    if (thd == NULL)
        return 0;

    gbl_sqltick++;

    struct sqlclntstate *clnt = thd->clnt;
    if (skip_clnt_check(clnt)) {
        return 0;
    }

    Pthread_mutex_lock(&clnt->sql_tick_lk);

    /* Increment per-clnt sqltick */
    ++clnt->sqltick;

    if (gbl_debug_sleep_in_sql_tick || (gbl_debug_sleep_in_analyze && clnt->is_analyze))
        sleep(1);

    /* statement cancelled? done */
    if (thd->stop_this_statement) {
        rc = SQLITE_ABORT;
        goto done;
    }

    if (clnt->statement_timedout) {
        rc = SQLITE_TIMEDOUT;
        goto done;
    }

    if ((rc = check_recover_deadlock(clnt)))
        goto done;

    if (clnt->in_sqlite_init == 0) {
        if (no_recover_deadlock == 0) {
            if ((gbl_epoch_time - clnt->last_sent_row_sec) >= gbl_delay_sql_lock_release_sec) {

                rc = clnt_check_bdb_lock_desired(clnt);

            } else if (gbl_sql_random_release_interval && !(rand() % gbl_sql_random_release_interval)) {

                rc = recover_deadlock(thedb->bdb_env, thd, NULL, 0);

                if ((rc = check_recover_deadlock(clnt)))
                    goto done;

                logmsg(LOGMSG_DEBUG, "%s recovered deadlock\n", __func__);

                clnt->deadlock_recovered++;
            }
        }
    }

    if (check_sql_client_disconnect(clnt, __FILE__, __LINE__)) {
        rc = SQLITE_ABORT;
        goto done;
    }

    if (clnt->limits.maxcost && (thd->cost > clnt->limits.maxcost)) {
        rc = SQLITE_COST_TOO_HIGH;
        goto done;
    }

done:
    Pthread_mutex_unlock(&clnt->sql_tick_lk);
    return rc;
}

pthread_key_t query_info_key;
static uint32_t gbl_query_id = 1;

int comdb2_sql_tick()
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    return sql_tick(thd, 0);
}

int comdb2_sql_tick_no_recover_deadlock()
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    return sql_tick(thd, 1);
}

void sql_get_query_id(struct sql_thread *thd)
{
    if (thd) {
        thd->id = ATOMIC_ADD32(gbl_query_id, 1);
    }
}

static unsigned int query_path_component_hash(const void *key, int len)
{
    const struct query_path_component *q = key;
    const char *name;

    name = q->lcl_tbl_name;

    struct myx {
        int ix;
        char name[1];
    } * x;
    int sz = offsetof(struct myx, name) + strlen(name) + 1;
    x = alloca(sz);
    x->ix = q->ix;
    strcpy(x->name, name);
    return hash_default_fixedwidth((void *)x, sz);
}

static int query_path_component_cmp(const void *key1, const void *key2, int len)
{
    const struct query_path_component *q1 = key1, *q2 = key2;
    if (q1->ix != q2->ix) {
        return q1->ix - q2->ix;
    }
    if (!q1->rmt_db[0] && !q2->rmt_db[0]) {
        // both local
        return strncmp(q1->lcl_tbl_name, q2->lcl_tbl_name,
                       sizeof(q1->lcl_tbl_name));
    } else if (!q1->rmt_db[0]) {
        // mismatch
        return -1;
    } else if (!q2->rmt_db[0]) {
        // mismatch
        return 1;
    } else {
        int rc = strncmp(q1->rmt_db, q2->rmt_db, sizeof(q1->rmt_db));
        if (rc) return rc;

        return strncmp(q1->lcl_tbl_name, q2->lcl_tbl_name,
                       sizeof(q1->lcl_tbl_name));
    }
}

struct sql_thread *start_sql_thread(void)
{
    struct sql_thread *thd = calloc(1, sizeof(struct sql_thread));
    if (!thd) {
        logmsg(LOGMSG_ERROR, "%s: calloc failed\n", __func__);
        return NULL;
    }
    listc_init(&thd->query_stats, offsetof(struct query_path_component, lnk));
    thd->query_hash = hash_init_user(query_path_component_hash,
                                     query_path_component_cmp, 0, 0);
    Pthread_mutex_init(&thd->lk, NULL);
    Pthread_setspecific(query_info_key, thd);
    Pthread_mutex_lock(&gbl_sql_lock);
    listc_abl(&thedb->sql_threads, thd);
    Pthread_mutex_unlock(&gbl_sql_lock);

    thd->crtshard = 0;

    return thd;
}

static int free_queryhash(void *obj, void *arg)
{
    struct query_path_component *qh = (struct query_path_component *)obj;
    free(qh);
    return 0;
}

/* sql thread pool relies on this being safe to call even if we didn't
 * register with start_sql_thread */
void done_sql_thread(void)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (thd) {
        Pthread_mutex_lock(&gbl_sql_lock);
        listc_rfl(&thedb->sql_threads, thd);
        Pthread_mutex_unlock(&gbl_sql_lock);
        Pthread_mutex_destroy(&thd->lk);
        Pthread_setspecific(query_info_key, NULL);
        if (thd->buf) {
            free(thd->buf);
            thd->buf = NULL;
        }
        if (thd->query_hash) {
            hash_for(thd->query_hash, free_queryhash, NULL);
            hash_clear(thd->query_hash);
            hash_free(thd->query_hash);
            thd->query_hash = 0;
        }
        destroy_sqlite_master(thd->rootpages, thd->rootpage_nentries);
        free(thd);
    }
    bdb_temp_table_maybe_reset_priority_thread(thedb->bdb_env, 1);
}

static int ondisk_to_sqlite_tz(struct dbtable *db, struct schema *s, void *inp,
                               int rrn, unsigned long long genid, void *outp,
                               int maxout, int nblobs, void **blob,
                               size_t *blobsz, size_t *bloboffs, int *reqsize,
                               const char *tzname, BtCursor *pCur)
{
    unsigned char *out = (unsigned char *)outp, *in = (unsigned char *)inp;
    struct field *f;
    int fnum;
    int i;
    int rc = 0;
    Mem *m = NULL;
    u32 *type = NULL;
    int datasz = 0;
    int hdrsz = 0;
    unsigned int sz;
    unsigned char *hdrbuf, *dtabuf;
    int ncols = 0;
    int nField;
    int rec_srt_off = gbl_sort_nulls_correctly ? 0 : 1;

    /* Raw index optimization */
    if (pCur && pCur->nCookFields >= 0)
        nField = pCur->nCookFields;
    else
        nField = s->nmembers;

    m = (Mem *)alloca(sizeof(Mem) * (nField + 1)); // Extra 1 for genid

    type = (u32 *)alloca(sizeof(u32) * (nField + 1));

#ifdef debug_raw
    printf("convert => %s %s %d / %d\n", db->tablename, s->tag, nField,
           s->nmembers);
#endif

    *reqsize = 0;

    for (fnum = 0; fnum < nField; fnum++) {
        memset(&m[fnum], 0, sizeof(Mem));
        rc = get_data(pCur, s, in, fnum, &m[fnum], 1, tzname);
        if (rc)
            goto done;
        type[fnum] =
            sqlite3VdbeSerialType(&m[fnum], SQLITE_DEFAULT_FILE_FORMAT, &sz);
        datasz += sz;
        hdrsz += sqlite3VarintLen(type[fnum]);
    }
    ncols = fnum;

    if (!db->dtastripe)
        genid = rrn;

    if (genid) {
        m[fnum].u.i = genid;
        m[fnum].flags = MEM_Int;

        type[fnum] =
            sqlite3VdbeSerialType(&m[fnum], SQLITE_DEFAULT_FILE_FORMAT, &sz);
        datasz += sz;
        hdrsz += sqlite3VarintLen(type[fnum]);
        ncols++;
        /*fprintf( stderr, "%s:%d type=%d size=%d datasz=%d hdrsz=%d
          ncols->%d\n",
          __FILE__, __LINE__, type, sz, datasz, hdrsz, ncols);*/
    }

    /* to account for size of header in header */
    hdrsz += sqlite3VarintLen(hdrsz);

    /*
       fprintf( stderr, "%s:%d hdrsz=%d ncols=%d maxout=%d\n",
       __FILE__, __LINE__, hdrsz, ncols, maxout);*/

    hdrbuf = out;
    dtabuf = out + hdrsz;

    /* enough room? */
    if (maxout > 0 && (datasz + hdrsz) > maxout) {
        logmsg(LOGMSG_ERROR, "AAAAA!?!?\n");
        rc = -2;
        *reqsize = datasz + hdrsz;
        goto done;
    }

    /* put header size in header */

    sz = sqlite3PutVarint(hdrbuf, hdrsz);
    hdrbuf += sz;

    for (fnum = 0; fnum < ncols; fnum++) {
        // TODO: verify that this works as before
        sz = sqlite3VdbeSerialPut(dtabuf, &m[fnum], type[fnum]);
        dtabuf += sz;
        sz = sqlite3PutVarint(hdrbuf, type[fnum]);
        hdrbuf += sz;
        assert(hdrbuf <= (out + hdrsz));
    }
    /* return real length */
    *reqsize = datasz + hdrsz;
    rc = 0;

done:
    /* revert back the flipped fields */
    for (i = 0; i < nField; i++) {
        f = &s->member[i];
        if (f->flags & INDEX_DESCEND) {
            xorbuf(in + f->offset + rec_srt_off, f->len - rec_srt_off);
        }
    }
    return rc;
}

/* Convert comdb2 record to sqlite format. Return 0 on success, -1 on error,
   -2 if buffer not big enough (reqsize contains required size in this case) */
static int ondisk_to_sqlite(struct dbtable *db, struct schema *s, void *inp, int rrn,
                            unsigned long long genid, void *outp, int maxout,
                            int nblobs, void **blob, size_t *blobsz,
                            size_t *bloboffs, int *reqsize)
{
    return ondisk_to_sqlite_tz(db, s, inp, rrn, genid, outp, maxout, nblobs,
                               blob, blobsz, bloboffs, reqsize, NULL, NULL);
}

/* Convert a sequence of Mem * to a serialized sqlite row */
int sqlite3_unpacked_to_packed(Mem *mems, int nmems, char **ret_rec,
                               int *ret_rec_len)
{
    char *rec, *crt;
    int sz, remsz, total_data_sz, total_header_sz;
    int fnum;
    u32 type;
    u32 len;

    /* compute output row size, header + data */
    total_data_sz = 0;
    total_header_sz = 0;
    for (fnum = 0; fnum < nmems; fnum++) {
        type = sqlite3VdbeSerialType(&mems[fnum], SQLITE_DEFAULT_FILE_FORMAT,
                                     &len);
        total_data_sz += sqlite3VdbeSerialTypeLen(type);
        total_header_sz += sqlite3VarintLen(type);
    }
    // adding header length to total_header_sz may change header length of total_header_sz, so calculate sqlite3VarintLen() twice
    int header_length = sqlite3VarintLen(total_header_sz);
    total_header_sz += sqlite3VarintLen(total_header_sz + header_length);

    /* create the sqlite row */
    rec = (char *)calloc(1, total_header_sz + total_data_sz);
    if (!rec) {
        return -1;
    }

    crt = rec;
    remsz = total_header_sz + total_data_sz;

    sz = sqlite3PutVarint((unsigned char *)crt, total_header_sz);
    crt += sz;
    remsz -= sz;

    /* serialize headers */
    for (fnum = 0; fnum < nmems; fnum++) {
        sz = sqlite3PutVarint((unsigned char *)crt,
                              sqlite3VdbeSerialType(&mems[fnum],
                                                    SQLITE_DEFAULT_FILE_FORMAT,
                                                    &len));
        crt += sz;
        remsz -= sz;
    }
    for (fnum = 0; fnum < nmems; fnum++) {
        sz = sqlite3VdbeSerialPut(
            (unsigned char *)crt, &mems[fnum],
            sqlite3VdbeSerialType(&mems[fnum], SQLITE_DEFAULT_FILE_FORMAT,
                                  &len));
        crt += sz;
        remsz -= sz;
    }

    *ret_rec = rec;
    *ret_rec_len = total_header_sz + total_data_sz;

    if (remsz != 0) {
        logmsg(LOGMSG_ERROR, "%s: remsz %d != 0\n", __func__, remsz);
        abort();
    }

    return 0;
}

/* Called by convert_failure_reason_str() to decode the sql specific part. */
int convert_sql_failure_reason_str(const struct convert_failure *reason,
                                   char *out, size_t outlen)
{
    if (reason->source_sql_field_flags & MEM_Null) {
        return snprintf(out, outlen, " from SQL NULL");
    } else if (reason->source_sql_field_flags & MEM_Int) {
        return snprintf(out, outlen, " from SQL integer '%lld'",
                        reason->source_sql_field_info.ival);
    } else if (reason->source_sql_field_flags & MEM_Real) {
        return snprintf(out, outlen, " from SQL real '%f'",
                        reason->source_sql_field_info.rval);
    } else if (reason->source_sql_field_flags & MEM_Str) {
        return snprintf(out, outlen, " from SQL string of length %d",
                        (int)reason->source_sql_field_info.slen);
    } else if (reason->source_sql_field_flags & MEM_Blob) {
        return snprintf(out, outlen, " from SQL blob of length %d",
                        (int)reason->source_sql_field_info.blen);
    } else if (reason->source_sql_field_flags & MEM_Datetime) {
        return snprintf(out, outlen, " from SQL datetime");
    } else if (reason->source_sql_field_flags & MEM_Interval) {
        return snprintf(
            out, outlen,
            " from SQL interval (day-second, year-month or decimal)");
    }
    return 0;
}

struct mem_info {
    struct schema *s;
    Mem *m;
    int null;
    int *nblobs;
    struct field_conv_opts_tz *convopts;
    const char *tzname;
    blob_buffer_t *outblob;
    int maxblobs;
    struct convert_failure *fail_reason;
    int fldidx;
};

static int mem_to_ondisk(void *outbuf, struct field *f, struct mem_info *info,
                         bias_info *bias_info)
{
    Mem *m = info->m;
    struct schema *s = info->s;
    int null = info->null;
    int *nblobs = info->nblobs;
    struct field_conv_opts_tz *convopts = info->convopts;
    const char *tzname = info->tzname;
    blob_buffer_t *outblob = info->outblob;
    int maxblobs = info->maxblobs;
    struct convert_failure *fail_reason = info->fail_reason;
    int rec_srt_off = gbl_sort_nulls_correctly ? 0 : 1;
    int outdtsz;
    int rc = 0;
    uint8_t *out = (uint8_t *)outbuf;

    if (m->flags & MEM_Null) {
        /* for local replicants, we need to supply a value here. */
        if (gbl_replicate_local && strcasecmp(f->name, "comdb2_seqno") == 0) {
            long long val;
            int outsz;
            const struct field_conv_opts outopts = {0};

            val = get_unique_longlong(thedb);
            rc = CLIENT_to_SERVER(&val, sizeof(unsigned long long), CLIENT_INT,
                                  0, NULL, NULL, out + f->offset, f->len,
                                  f->type, 0, &outsz, &outopts, NULL);
        } else
            set_null(out + f->offset, f->len);

        goto done;
    }

    /* if target field is DATETIME or DATETIMEUS, prepare timezone name for
     * conversion */
    if (f->type == SERVER_DATETIME || f->type == SERVER_DATETIMEUS) {
        if (convopts && tzname && tzname[0]) {
            strncpy0(convopts->tzname, tzname, sizeof(convopts->tzname));
            convopts->flags |= FLD_CONV_TZONE;
        }
    }

    if ((f->type == SERVER_BLOB || f->type == SERVER_BLOB2 ||
         f->type == SERVER_VUTF8) &&
        m->n > MAXBLOBLENGTH) {
        rc = -1;
        if (fail_reason) {
            fail_reason->reason = CONVERT_FAILED_BLOB_SIZE;
        }
        return rc;
    }

    if (m->flags & MEM_Master) {
        set_resolve_master(out + f->offset, f->len);
        rc = 0;
    }

    if (m->flags & MEM_Int) {
        i64 i = flibc_htonll(m->u.i);
        rc = CLIENT_to_SERVER(
            &i, sizeof(i), CLIENT_INT, null, (struct field_conv_opts *)convopts,
            NULL /*blob */, out + f->offset, f->len, f->type, 0, &outdtsz,
            &f->convopts, NULL /*&outblob[nblobs] blob */);
    } else if (m->flags & MEM_Real) {
        double r = flibc_htond(m->u.r);
        rc =
            CLIENT_to_SERVER(&r, sizeof(r), CLIENT_REAL, null,
                             (struct field_conv_opts *)convopts, NULL /*blob */,
                             out + f->offset, f->len, f->type, 0, &outdtsz,
                             &f->convopts, NULL /*&outblob[nblobs] blob */);
    } else if (m->flags & MEM_Str) {
        blob_buffer_t *vutf8_outblob = NULL;

        /* if we are converting to a blob (vutf8), make sure it's in
         * range */
        if (f->type == SERVER_VUTF8) {
            int blobix = f->blob_index;
            if (blobix == -1 || blobix >= maxblobs) {
                rc = -1;
                if (fail_reason) {
                    fail_reason->reason = CONVERT_FAILED_BAD_BLOB_PROGRAMMER;
                }
                return rc;
            }
            vutf8_outblob = &outblob[blobix];
        }

        if (m->n >= f->len && f->type == SERVER_BCSTR &&
            (convopts->flags & FLD_CONV_TRUNCATE)) {
            // if string is longer than field
            // and find-by-truncate is enabled
            convopts->step = (f->flags & INDEX_DESCEND) ? 0 : 1;
            bias_info->truncated = 1;
        }
        rc = CLIENT_to_SERVER(m->z, m->n, CLIENT_PSTR2, null,
                              (struct field_conv_opts *)convopts,
                              NULL /*blob */, out + f->offset, f->len, f->type,
                              0, &outdtsz, &f->convopts, vutf8_outblob);

        if (gbl_report_sqlite_numeric_conversion_errors &&
            (f->type == SERVER_BINT || f->type == SERVER_UINT ||
             f->type == SERVER_BREAL)) {
            double rValue;
            char *s;
            struct sql_thread *thd = pthread_getspecific(query_info_key);
            struct sqlclntstate *clnt;
            if (thd) {
                clnt = thd->clnt;
                if (m->n > 0 && m->z[m->n - 1] != 0) {
                    s = alloca(m->n + 1);
                    memcpy(s, m->z, m->n);
                    s[m->n] = 0;
                } else
                    s = m->z;

                if (sqlite3AtoF(s, &rValue, sqlite3Strlen30(s), SQLITE_UTF8) ==
                        0 &&
                    rc != -1) {
                    static int once = 1;
                    if (once) {
                        logmsg(LOGMSG_ERROR, "debug: sqlite<->comdb2 numeric "
                                        "conversion mismatch\n");
                        once = 0;
                    }
                    ctrace("!sqlite3IsNumber \"%.*s\" %s\n", m->n, m->z,
                           clnt->sql);
                }
            }
        }

        if (f->type == SERVER_VUTF8)
            (*nblobs)++;
    } else if (m->flags & MEM_Datetime) {
        /* datetime blob */
        if (!tzname || !tzname[0])
            rc = -1;
        else
            switch (m->du.dt.dttz_prec) {
            case DTTZ_PREC_MSEC: {
                if (f->type != SERVER_DATETIMEUS ||
                    !gbl_forbid_datetime_promotion || m->du.dt.dttz_conv) {
                    if (f->type == SERVER_DATETIMEUS &&
                        gbl_forbid_datetime_ms_us_s2s) {
                        // Since s2s would reject the conversion, we cheat a bit
                        // here.
                        server_datetimeus_t sdt;
                        bzero(&sdt, sizeof(sdt));
                        sdt.flag = 8; /*data_bit */
                        sdt.sec = flibc_htonll(m->du.dt.dttz_sec);
                        *((char *)&sdt.sec) ^= 0x80;
                        sdt.usec = htonl(m->du.dt.dttz_frac * 1000);

                        rc = SERVER_to_SERVER(
                            &sdt, sizeof(sdt), SERVER_DATETIMEUS, NULL, NULL, 0,
                            out + f->offset, f->len, f->type, 0, &outdtsz,
                            &f->convopts, NULL /*&outblob[nblobs]*/);
                    } else {
                        server_datetime_t sdt;
                        bzero(&sdt, sizeof(sdt));
                        sdt.flag = 8; /*data_bit */
                        sdt.sec = flibc_htonll(m->du.dt.dttz_sec);
                        *((char *)&sdt.sec) ^= 0x80;
                        sdt.msec = htons(m->du.dt.dttz_frac);

                        rc = SERVER_to_SERVER(
                            &sdt, sizeof(sdt), SERVER_DATETIME, NULL, NULL, 0,
                            out + f->offset, f->len, f->type, 0, &outdtsz,
                            &f->convopts, NULL /*&outblob[nblobs]*/);
                    }
                } else {
                    rc = -1;
                }
                break;
            }
            case DTTZ_PREC_USEC: {
                if (f->type != SERVER_DATETIME ||
                    !gbl_forbid_datetime_truncation || m->du.dt.dttz_conv) {
                    if (f->type == SERVER_DATETIME &&
                        gbl_forbid_datetime_ms_us_s2s) {
                        server_datetime_t sdt;
                        bzero(&sdt, sizeof(sdt));
                        sdt.flag = 8; /*data_bit */
                        sdt.sec = flibc_htonll(m->du.dt.dttz_sec);
                        *((char *)&sdt.sec) ^= 0x80;
                        sdt.msec = htons(m->du.dt.dttz_frac / 1000);

                        rc = SERVER_to_SERVER(
                            &sdt, sizeof(sdt), SERVER_DATETIME, NULL, NULL, 0,
                            out + f->offset, f->len, f->type, 0, &outdtsz,
                            &f->convopts, NULL /*&outblob[nblobs]*/);
                    } else {
                        server_datetimeus_t sdt;
                        bzero(&sdt, sizeof(sdt));
                        sdt.flag = 8; /*data_bit */
                        sdt.sec = flibc_htonll(m->du.dt.dttz_sec);
                        *((char *)&sdt.sec) ^= 0x80;
                        sdt.usec = htonl(m->du.dt.dttz_frac);

                        rc = SERVER_to_SERVER(
                            &sdt, sizeof(sdt), SERVER_DATETIMEUS, NULL, NULL, 0,
                            out + f->offset, f->len, f->type, 0, &outdtsz,
                            &f->convopts, NULL /*&outblob[nblobs]*/);
                    }
                } else {
                    rc = -1;
                }
                break;
            }
            default:
                rc = -1;
                break;
            }
    } else if (m->flags & MEM_Interval) {
        switch (m->du.tv.type) {
        case INTV_YM_TYPE: {
            cdb2_client_intv_ym_t ym;
            ym.sign = htonl(m->du.tv.sign);
            ym.years = htonl(m->du.tv.u.ym.years);
            ym.months = htonl(m->du.tv.u.ym.months);
            rc = CLIENT_to_SERVER(&ym, sizeof(ym), CLIENT_INTVYM, null, NULL,
                                  NULL, out + f->offset, f->len, f->type, 0,
                                  &outdtsz, &f->convopts,
                                  NULL /*&outblob[nblobs]*/);
            break;
        }
        case INTV_DS_TYPE: {
            if (f->type != SERVER_INTVDSUS || !gbl_forbid_datetime_promotion ||
                m->du.tv.u.ds.conv) {
                cdb2_client_intv_ds_t ds;
                ds.sign = htonl(m->du.tv.sign);
                ds.days = htonl(m->du.tv.u.ds.days);
                ds.hours = htonl(m->du.tv.u.ds.hours);
                ds.mins = htonl(m->du.tv.u.ds.mins);
                ds.sec = htonl(m->du.tv.u.ds.sec);

                if (f->type == SERVER_INTVDSUS &&
                    gbl_forbid_datetime_promotion) {
                    // Since c2s would reject the conversion, we cheat a bit
                    // here.
                    ds.msec = htonl(m->du.tv.u.ds.frac * 1000);
                    rc = CLIENT_to_SERVER(
                        &ds, sizeof(ds), CLIENT_INTVDSUS, null, NULL, NULL,
                        out + f->offset, f->len, f->type, 0, &outdtsz,
                        &f->convopts, NULL /*&outblob[nblobs]*/);
                } else {
                    ds.msec = htonl(m->du.tv.u.ds.frac);
                    rc = CLIENT_to_SERVER(&ds, sizeof(ds), CLIENT_INTVDS, null,
                                          NULL, NULL, out + f->offset, f->len,
                                          f->type, 0, &outdtsz, &f->convopts,
                                          NULL /*&outblob[nblobs]*/);
                }
            } else {
                rc = -1;
            }
            break;
        }
        case INTV_DSUS_TYPE: {
            if (f->type != SERVER_INTVDS || !gbl_forbid_datetime_truncation ||
                m->du.tv.u.ds.conv) {
                cdb2_client_intv_dsus_t ds;
                ds.sign = htonl(m->du.tv.sign);
                ds.days = htonl(m->du.tv.u.ds.days);
                ds.hours = htonl(m->du.tv.u.ds.hours);
                ds.mins = htonl(m->du.tv.u.ds.mins);
                ds.sec = htonl(m->du.tv.u.ds.sec);
                if (f->type == SERVER_INTVDS &&
                    gbl_forbid_datetime_truncation) {
                    // Since c2s would reject the conversion, we cheat a bit
                    // here.
                    ds.usec = htonl(m->du.tv.u.ds.frac / 1000);
                    rc = CLIENT_to_SERVER(&ds, sizeof(ds), CLIENT_INTVDS, null,
                                          NULL, NULL, out + f->offset, f->len,
                                          f->type, 0, &outdtsz, &f->convopts,
                                          NULL /*&outblob[nblobs]*/);
                } else {
                    ds.usec = htonl(m->du.tv.u.ds.frac);
                    rc = CLIENT_to_SERVER(
                        &ds, sizeof(ds), CLIENT_INTVDSUS, null, NULL, NULL,
                        out + f->offset, f->len, f->type, 0, &outdtsz,
                        &f->convopts, NULL /*&outblob[nblobs]*/);
                }
            } else {
                rc = -1;
            }
            break;
        }
        case INTV_DECIMAL_TYPE: {
            if (f->type == SERVER_DECIMAL) {
                rc = sqlite_to_decimal_ondisk(&m->du.tv.u.dec, out + f->offset,
                                              f->len);
                outdtsz = f->len;
            } else {
                server_decimal128_t tmp;
                if ((rc = sqlite_to_decimal_ondisk(&m->du.tv.u.dec, &tmp,
                                                   sizeof(tmp))) == 0) {
                    rc = SERVER_to_SERVER(&tmp, sizeof(tmp), SERVER_DECIMAL,
                                          NULL, NULL, 0, out + f->offset,
                                          f->len, f->type, 0, &outdtsz,
                                          &f->convopts, NULL);
                }
            }
            break;
        }
        default:
            rc = -1;
            break;
        }
    } else if (m->flags & MEM_Blob) {
        int blobix = f->blob_index;
        /* if we are converting to a blob, make sure it's in range */
        if (f->type == SERVER_BLOB2 || f->type == SERVER_BLOB ||
            f->type == SERVER_VUTF8) {
            if (blobix == -1 || blobix >= maxblobs) {
                rc = -1;
                if (fail_reason) {
                    fail_reason->reason = CONVERT_FAILED_BAD_BLOB_PROGRAMMER;
                }
                return rc;
            }
        }

        if (m->n >= f->len && f->type == SERVER_BYTEARRAY &&
            (convopts->flags & FLD_CONV_TRUNCATE)) {
            /* if the SQLite BLOB is longer than the bytearray field
               and find-by-truncate is enabled. */
            convopts->step = (f->flags & INDEX_DESCEND) ? 0 : 1;
            bias_info->truncated = 1;
        }

        rc =
            CLIENT_to_SERVER(m->z, m->n, CLIENT_BYTEARRAY, null,
                             (struct field_conv_opts *)convopts, NULL /*blob */,
                             out + f->offset, f->len, f->type, 0, &outdtsz,
                             &f->convopts, &outblob[blobix] /*blob */);
        if (f->type == SERVER_BLOB2 || f->type == SERVER_BLOB ||
            f->type == SERVER_VUTF8)
            (*nblobs)++;
    }

done:
    if (f->flags & INDEX_DESCEND) {
        xorbuf(((char *)out + f->offset) + rec_srt_off, f->len - rec_srt_off);
    }

    if (rc && fail_reason) {
        fail_reason->reason = CONVERT_FAILED_INCOMPATIBLE_VALUES;
        fail_reason->source_sql_field_flags = m->flags;
        fail_reason->target_schema = s;
        fail_reason->target_field_idx = info->fldidx;
        if (m->flags & MEM_Int) {
            fail_reason->source_sql_field_info.ival = m->u.i;
        } else if (m->flags & MEM_Real) {
            fail_reason->source_sql_field_info.rval = m->u.r;
        } else if (m->flags & MEM_Str) {
            fail_reason->source_sql_field_info.slen = m->n;
        } else if (m->flags & MEM_Blob) {
            fail_reason->source_sql_field_info.blen = m->n;
        }
    }

    return rc ? -1 : 0;
}

int sqlite_to_ondisk(struct schema *s, const void *inp, int len, void *outp,
                     const char *tzname, blob_buffer_t *outblob, int maxblobs,
                     struct convert_failure *fail_reason, BtCursor *pCur)
{
    Mem m;
    unsigned int hdrsz, type;
    int hdroffset = 0, dataoffset = 0;
    unsigned char *in = (unsigned char *)inp;
    unsigned char *out = (unsigned char *)outp;

    struct field *f;
    int fld = 0;
    int rc;

    int clen = 0; /* converted sofar */
    int nblobs = 0;

    struct mem_info info;
    struct field_conv_opts_tz convopts = {.flags = 0};

    info.s = s;
    info.fail_reason = fail_reason;
    info.tzname = tzname;
    info.m = &m;
    info.nblobs = &nblobs;
    info.convopts = &convopts;
    info.outblob = outblob;
    info.maxblobs = maxblobs;
    info.fail_reason = fail_reason;

    if (fail_reason)
        init_convert_failure_reason(fail_reason);

    hdroffset = sqlite3GetVarint32(in, &hdrsz);
    dataoffset = hdrsz;
    while (hdroffset < hdrsz) {
        hdroffset += sqlite3GetVarint32(in + hdroffset, &type);
        info.null = (type == 0);
        f = &s->member[fld];
        dataoffset += sqlite3VdbeSerialGet(in + dataoffset, type, &m);

        info.fldidx = fld;

        if ((rc = mem_to_ondisk(out, f, &info, NULL)) != 0)
            return rc;

        clen += f->len;
        fld++;

        /* sqlite3BtreeMoveto is sometimes called with len that
         * doesn't account for the record number at the end
         * of the record. Don't error out under that condition. */
        if (fld >= s->nmembers)
            break;
    }
    return clen;
}

static int stat1_find(char *namebuf, struct schema *schema, struct dbtable *db,
                      int ixnum, void *trans)
{
    if (gbl_create_mode)
        return 0;

    struct ireq iq;
    int rc;
    int rrn;
    int stat_ixnum;
    char key[MAXKEYLEN];
    int keylen;
    char fndkey[MAXKEYLEN];
    unsigned long long genid;
    struct schema *statschema;
    struct field *f;

    init_fake_ireq(thedb, &iq);
    iq.usedb = get_dbtable_by_name("sqlite_stat1");
    if (!iq.usedb)
        return -1;

    /* From comdb2_stats1.csc2:
     ** keys {
     **     "0" = tbl + idx
     ** }
     */
    bzero(key, sizeof(key));
    keylen = 0;
    stat_ixnum = 0;
    statschema = iq.usedb->ixschema[stat_ixnum];

    f = &statschema->member[0]; /* tbl */
    set_data(key + f->offset, db->tablename, strlen(db->tablename) + 1);
    keylen += f->len;

    f = &statschema->member[1]; /* idx */
    set_data(key + f->offset, namebuf, strlen(namebuf) + 1);
    keylen += f->len;

    rc = ix_find_flags(&iq, trans, stat_ixnum, key, keylen, fndkey, &rrn,
                       &genid, NULL, 0, 0, IX_FIND_IGNORE_INCOHERENT);

    if (rc == IX_FND || rc == IX_FNDMORE) {
        /* found old style names; continue using them */
        return 1;
    }

    return 0;
}

static int using_old_style_name(char *namebuf, int len, struct schema *schema,
                                struct dbtable *db, int ixnum, void *trans)
{
    snprintf(namebuf, len, "%s_ix_%d", db->tablename, ixnum);
    return stat1_find(namebuf, schema, db, ixnum, trans);
}

void form_new_style_name(char *namebuf, int len, struct schema *schema,
                         const char *csctag, const char *dbname)
{
    char buf[16 * 1024];
    int fieldctr;
    int current = 0;
    unsigned int crc;

    SNPRINTF(buf, sizeof(buf), current, "%s", dbname)
    if (schema->flags & (SCHEMA_DATACOPY | SCHEMA_PARTIALDATACOPY)) {
        SNPRINTF(buf, sizeof(buf), current, "%s", "DATACOPY")

        if (schema->flags & SCHEMA_PARTIALDATACOPY) {
            struct schema *partial_datacopy = schema->partial_datacopy;

            SNPRINTF(buf, sizeof(buf), current, "%s", "(")
            for (fieldctr = 0; fieldctr < partial_datacopy->nmembers; fieldctr++) {
                if (fieldctr > 0) {
                    SNPRINTF(buf, sizeof(buf), current, "%s", ", ")
                }

                SNPRINTF(buf, sizeof(buf), current, "%s", partial_datacopy->member[fieldctr].name)
            }
            SNPRINTF(buf, sizeof(buf), current, "%s", ")")
        }
    }

    if (schema->flags & SCHEMA_DUP)
        SNPRINTF(buf, sizeof(buf), current, "%s", "DUP")

    if (schema->flags & SCHEMA_RECNUM)
        SNPRINTF(buf, sizeof(buf), current, "%s", "RECNUM")

    if (schema->flags & SCHEMA_UNIQNULLS)
        SNPRINTF(buf, sizeof(buf), current, "%s", "UNIQNULLS")

    for (fieldctr = 0; fieldctr < schema->nmembers; ++fieldctr) {
        SNPRINTF(buf, sizeof(buf), current, "%s", schema->member[fieldctr].name)
        if (schema->member[fieldctr].flags & INDEX_DESCEND)
            SNPRINTF(buf, sizeof(buf), current, "%s", "DESC")
    }

done:
    crc = crc32(0, (unsigned char *)buf, current);
    snprintf(namebuf, len, "$%s_%X", csctag, crc);
}

/*
** Given a comdb2 index, this routine will decide whether to
** advertise its name as tablename_ix_ixnum or the new style
** $csctag_hash to SQLite (new style has preceeding $).
**
** To start using the new style names, simply
** (1) fastinit sqlite_stat1
** (2) bounce the db and
** (3) run analyze
**
** The index name to be adv. to sqlite is returned in namebuf.
** A valid name is always returned.
**
** Return value:
** <0: Error (stat1 not found?)
**  0: No stats for this index.
**  1: Found stat with old style names.
**  2: Found stat with new style names.
*/
int sql_index_name_trans(char *namebuf, int len, struct schema *schema,
                         struct dbtable *db, int ixnum, void *trans)
{
    int rc;
    rc = using_old_style_name(namebuf, len, schema, db, ixnum, trans);
    if (rc > 0) {
        /* found old style entry; keep using it */
        return rc;
    }

    form_new_style_name(namebuf, len, schema, schema->csctag, db->tablename);
    rc = stat1_find(namebuf, schema, db, ixnum, trans);
    if (rc > 0)
        return 2;
    return rc;
}

/*
** Given a comdb2 field, this routine will print the human readable
** version of the name to dstr. It will also take care of allocating
** memory, so callers are required to free() dstr after they are done
** using it. This routine can also switch on in_default or out_default
** depending on what you want.
*/
char *sql_field_default_trans(struct field *f, int is_out)
{
    int default_type;
    void *this_default;
    unsigned int this_default_len;
    char *cval = NULL;
    unsigned char *bval = NULL;
    int rc = 0;
    int null;
    int outsz;
    char *dstr = NULL;

    default_type = is_out ? f->out_default_type : f->in_default_type;
    this_default = is_out ? f->out_default : f->in_default;
    this_default_len = is_out ? f->out_default_len : f->in_default_len;

    switch (default_type) {
    case SERVER_UINT: {
        unsigned long long uival = 0;
        rc = SERVER_UINT_to_CLIENT_UINT(
            this_default, this_default_len, NULL, NULL, &uival,
            sizeof(unsigned long long), &null, &outsz, NULL, NULL);
        uival = flibc_htonll(uival);
        if (rc == 0)
            dstr = sqlite3_mprintf("%llu", uival);
        break;
    }
    case SERVER_BINT: {
        long long ival = 0;
        rc = SERVER_BINT_to_CLIENT_INT(this_default, this_default_len, NULL,
                                       NULL, &ival, sizeof(long long), &null,
                                       &outsz, NULL, NULL);
        ival = flibc_htonll(ival);
        if (rc == 0)
            dstr = sqlite3_mprintf("%lld", ival);
        break;
    }
    case SERVER_BREAL: {
        double dval = 0;
        rc = SERVER_BREAL_to_CLIENT_REAL(this_default, this_default_len, NULL,
                                         NULL, &dval, sizeof(double), &null,
                                         &outsz, NULL, NULL);
        dval = flibc_htond(dval);
        if (rc == 0)
            dstr = sqlite3_mprintf("%f", dval);
        break;
    }
    case SERVER_BCSTR: {
        cval = sqlite3_malloc(this_default_len + 1);
        rc = SERVER_BCSTR_to_CLIENT_CSTR(this_default, this_default_len, NULL,
                                         NULL, cval, this_default_len + 1,
                                         &null, &outsz, NULL, NULL);
        if (rc == 0)
            dstr = sqlite3_mprintf("'%q'", cval);
        break;
    }
    case SERVER_BYTEARRAY: {
        bval = sqlite3_malloc(this_default_len - 1);
        rc = SERVER_BYTEARRAY_to_CLIENT_BYTEARRAY(
            this_default, this_default_len, NULL, NULL, bval,
            this_default_len - 1, &null, &outsz, NULL, NULL);

        dstr = sqlite3_malloc((this_default_len * 2) + 3);
        dstr[0] = 'x';
        dstr[1] = '\'';
        int i;
        for (i = 0; i < this_default_len - 1; i++)
            snprintf(&dstr[i * 2 + 2], 3, "%02x", bval[i]);
        dstr[i * 2 + 2] = '\'';
        dstr[i * 2 + 3] = 0;
        break;
    }
    case SERVER_DATETIME: {
        struct field_conv_opts_tz outopts = {0};
        cval = sqlite3_malloc(CLIENT_DATETIME_EXT_LEN);
        strcpy(outopts.tzname, "UTC");
        outopts.flags |= FLD_CONV_TZONE;
        rc = SERVER_DATETIME_to_CLIENT_CSTR(this_default, this_default_len,
                                            NULL, NULL, cval, 1024, &null,
                                            &outsz, (struct field_conv_opts*) &outopts, NULL);
        if (rc == 0) {
            if (null) {
                dstr = sqlite3_mprintf("%q", "CURRENT_TIMESTAMP");
            } else {
                dstr = sqlite3_mprintf("'%q'", cval);
            }
        }
        break;
    }
    case SERVER_DATETIMEUS: {
        struct field_conv_opts_tz outopts = {0};
        cval = sqlite3_malloc(CLIENT_DATETIME_EXT_LEN);
        strcpy(outopts.tzname, "UTC");
        outopts.flags |= FLD_CONV_TZONE;
        rc = SERVER_DATETIMEUS_to_CLIENT_CSTR(this_default, this_default_len,
                                              NULL, NULL, cval, 1024, &null,
                                              &outsz, (struct field_conv_opts*) &outopts, NULL);
        if (rc == 0) {
            if (null) {
                dstr = sqlite3_mprintf("%q", "CURRENT_TIMESTAMP");
            } else {
                dstr = sqlite3_mprintf("'%q'", cval);
            }
        }
        break;
    }
    case SERVER_FUNCTION: {
        dstr = sqlite3_mprintf("%s", this_default);
        break;
    }
    case SERVER_SEQUENCE: {
        dstr = sqlite3_mprintf("%q", "nextsequence");
        break;
    }
    /* no defaults for blobs or vutf8 */
    default:
        logmsg(LOGMSG_ERROR, "Unknown default type %d in schema: column '%s' of type %d\n",
               default_type, f->name, f->type);
        return NULL;
    }

    if (rc) {
        return NULL;
    }

    if (cval)
        sqlite3_free(cval);
    if (bval)
        sqlite3_free(bval);
    return dstr;
}

static void create_sqlite_stat_sqlmaster_record(struct dbtable *tbl)
{
    if (tbl->sql)
        free(tbl->sql);
    for (int i = 0; i < tbl->nsqlix; i++) {
        free(tbl->ixsql[i]);
        tbl->ixsql[i] = NULL;
    }
    switch (tbl->tablename[11]) {
    case '1':
        tbl->sql = strdup("create table sqlite_stat1(tbl,idx,stat);");
        break;
    case '2':
        tbl->sql =
            strdup("create table sqlite_stat2(tbl,idx,sampleno,sample);");
        break;
    case '4':
        tbl->sql =
            strdup("create table sqlite_stat4(tbl,idx,neq,nlt,ndlt,sample);");
        break;
    default:
        abort();
    }
    if (tbl->ixsql) {
        free(tbl->ixsql);
        tbl->ixsql = NULL;
    }
    tbl->ixsql = calloc(sizeof(char *), tbl->nix);
    tbl->nsqlix = 0;
    tbl->ix_expr = 0;
    tbl->ix_partial = 0;
    tbl->ix_blob = 0;
}

int create_datacopy_arrays()
{
    int rc = 0;
    for (int table = 0; table < thedb->num_dbs && rc == 0; table++) {
        rc = create_datacopy_array(thedb->dbs[table]);
        if (rc)
            return rc;
    }

    return 0;
}

int create_datacopy_array(struct dbtable *tbl)
{
    struct schema *schema = tbl->schema;

    if (schema == NULL) {
        logmsg(LOGMSG_ERROR, "No .ONDISK tag for table %s.\n", tbl->tablename);
        return -1;
    }

    if (is_sqlite_stat(tbl->tablename)) {
        return 0;
    }

    for (int ixnum = 0; ixnum < tbl->nix; ixnum++) {

        schema = tbl->schema->ix[ixnum];
        struct schema *ondisk = tbl->schema;
        if (schema == NULL) {
            logmsg(LOGMSG_ERROR, "No index %d schema for table %s\n", ixnum, tbl->tablename);
            return -1;
        }

        if (!(schema->flags & (SCHEMA_DATACOPY | SCHEMA_PARTIALDATACOPY))) {
            continue;
        } else if (schema->flags & SCHEMA_PARTIALDATACOPY) {
            ondisk = schema->partial_datacopy;
        }

        int datacopy_pos = 0;
        for (int ondisk_i = 0; ondisk_i < ondisk->nmembers; ++ondisk_i) {
            int skip = 0;
            struct field *ondisk_field = &ondisk->member[ondisk_i];

            for (int schema_i = 0; schema_i < schema->nmembers; ++schema_i) {
                if (strcmp(ondisk_field->name, schema->member[schema_i].name) == 0) {
                    skip = 1;
                    break;
                }
            }
            if (skip)
                continue;

            if (datacopy_pos == 0) {
                size_t need = ondisk->nmembers * sizeof(schema->datacopy[0]);
                if (schema->datacopy)
                    free(schema->datacopy);
                schema->datacopy = (int *)malloc(need);
                if (schema->datacopy == NULL) {
                    logmsg(LOGMSG_ERROR, "Could not allocate memory for datacopy array\n");
                    return -1;
                }
            }
            schema->datacopy[datacopy_pos] = ondisk_i;
            ++datacopy_pos;
        }
    }
    return 0;
}

/* This creates SQL statements that correspond to a table's schema. These
   statements are used to bootstrap sqlite. */
static int create_sqlmaster_record(struct dbtable *tbl, void *tran)
{
    int field;
    char namebuf[128];
    char *tablename = tbl->sqlaliasname ? tbl->sqlaliasname : tbl->tablename;

    struct schema *schema = tbl->schema;
    if (schema == NULL) {
        logmsg(LOGMSG_ERROR, "No .ONDISK tag for table %s.\n", tablename);
        return -1;
    }

    if (is_sqlite_stat(tablename)) {
        create_sqlite_stat_sqlmaster_record(tbl);
        return 0;
    }

    strbuf *sql = strbuf_new();
    strbuf_clear(sql);
    strbuf_appendf(sql, "create table \"%s\"(", tablename);

    /* Fields */
    for (field = 0; field < schema->nmembers; field++) {
        char *type = sqltype(&schema->member[field], namebuf, sizeof(namebuf));
        if (type == NULL) {
            logmsg(LOGMSG_ERROR,
                   "Unsupported type in schema: column '%s' [%d] "
                   "table %s\n",
                   schema->member[field].name, field, tablename);
            strbuf_free(sql);
            return -1;
        }
        strbuf_appendf(sql, "\"%s\" %s", schema->member[field].name, type);
        /* add defaults for write sql */
        if (schema->member[field].in_default) {
            strbuf_append(sql, " DEFAULT");
            char *dstr = sql_field_default_trans(&schema->member[field], 0);

            if (dstr) {
                strbuf_appendf(sql, " %s", dstr);
                sqlite3_free(dstr);
            } else {
                logmsg(LOGMSG_ERROR,
                       "Failed to convert default value column '%s' table "
                       "%s type %d\n",
                       schema->member[field].name, tablename,
                       schema->member[field].type);
                strbuf_free(sql);
                return -1;
            }
        }

        if (field != schema->nmembers - 1)
            strbuf_append(sql, ", ");
    }

    /* CHECK constraints */
    for (int i = 0; i < tbl->n_check_constraints; i++) {
        strbuf_append(sql, ",");
        if (tbl->check_constraints[i].consname) {
            strbuf_appendf(sql, " constraint '%s'",
                           tbl->check_constraints[i].consname);
        }
        strbuf_appendf(sql, " check (%s)", tbl->check_constraints[i].expr);
    }

    strbuf_append(sql, ");");
    if (tbl->sql)
        free(tbl->sql);
    tbl->sql = strdup(strbuf_buf(sql));
    if (tbl->nix > 0) {
        for (int i = 0; i < tbl->nsqlix; i++) {
            free(tbl->ixsql[i]);
            tbl->ixsql[i] = NULL;
        }
        if (tbl->ixsql) {
            free(tbl->ixsql);
            tbl->ixsql = NULL;
        }
        tbl->ixsql = calloc(sizeof(char *), tbl->nix);
    }
    ctrace("%s\n", strbuf_buf(sql));
    tbl->nsqlix = 0;

    /* Indices */
    for (int ixnum = 0; ixnum < tbl->nix; ixnum++) {
        strbuf_clear(sql);

        schema = tbl->schema->ix[ixnum];
        if (schema == NULL) {
            logmsg(LOGMSG_ERROR, "No index %d schema for table %s\n", ixnum,
                   tablename);
            strbuf_free(sql);
            return -1;
        }

        sql_index_name_trans(namebuf, sizeof(namebuf), schema, tbl, ixnum,
                             tran);
        if (schema->sqlitetag)
            free(schema->sqlitetag);
        schema->sqlitetag = strdup(namebuf);
        if (schema->sqlitetag == NULL) {
            logmsg(LOGMSG_ERROR,
                   "%s malloc (strdup) failed - wanted %zu bytes\n", __func__,
                   strlen(namebuf));
            abort();
        }

        /* We lie to sqlite about the uniqueness of the indexes. */
        strbuf_append(sql, "create index ");

        strbuf_appendf(sql, "\"%s\" on \"%s\" (", namebuf, tablename);
        for (field = 0; field < schema->nmembers; field++) {
            if (field > 0)
                strbuf_append(sql, ", ");
            if (schema->member[field].isExpr) {
                if (!gbl_expressions_indexes) {
                    logmsg(LOGMSG_ERROR, "EXPRESSIONS INDEXES FOUND IN SCHEMA! PLEASE FIRST "
                            "ENABLE THE EXPRESSIONS INDEXES FEATURE.\n");
                    if (tbl->iq)
                        reqerrstr(tbl->iq, ERR_SC,
                                  "Please enable indexes on expressions.");
                    strbuf_free(sql);
                    return -1;
                }
                strbuf_appendf(sql, "(%s)", schema->member[field].name);
            } else {
                strbuf_appendf(sql, "\"%s\"", schema->member[field].name);
            }
            if (schema->member[field].flags & INDEX_DESCEND)
                strbuf_append(sql, " DESC");
        }

        if (schema->flags & (SCHEMA_DATACOPY | SCHEMA_PARTIALDATACOPY)) {
            struct schema *ondisk = tbl->schema;
            if (schema->flags & SCHEMA_PARTIALDATACOPY) {
                ondisk = schema->partial_datacopy;
            }
            int first = 1;
            /* Add all fields from ONDISK to index */
            for (int ondisk_i = 0; ondisk_i < ondisk->nmembers; ++ondisk_i) {
                int skip = 0;
                struct field *ondisk_field = &ondisk->member[ondisk_i];

                /* skip fields already in index */
                for (int schema_i = 0; schema_i < schema->nmembers;
                     ++schema_i) {
                    if (strcmp(ondisk_field->name,
                               schema->member[schema_i].name) == 0) {
                        skip = 1;
                        break;
                    }
                }
                if (skip)
                    continue;

                strbuf_appendf(sql, ", \"%s\"", ondisk_field->name);
                /* stop optimizer by adding dummy collation */
                if (first == 1) {
                    strbuf_append(sql, " collate DATACOPY");
                    first = 0;
                }
            }
        }

        strbuf_append(sql, ")");
        if (schema->where) {
            if (!gbl_partial_indexes) {
                logmsg(LOGMSG_ERROR, "PARTIAL INDEXES FOUND IN SCHEMA! PLEASE FIRST "
                                "ENABLE THE PARTIAL INDEXES FEATURE.\n");
                if (tbl->iq)
                    reqerrstr(tbl->iq, ERR_SC,
                              "Please enable partial indexes.");
                strbuf_free(sql);
                return -1;
            }
            strbuf_appendf(sql, " where (%s)", schema->where + 6);
        }
        strbuf_append(sql, ";");
        if (tbl->ixsql[ixnum])
            free(tbl->ixsql[ixnum]);
        if (field > 0) {
            tbl->ixsql[ixnum] = strdup(strbuf_buf(sql));
            ctrace("  %s\n", strbuf_buf(sql));
            tbl->nsqlix++;
        } else {
            tbl->ixsql[ixnum] = NULL;
        }
    }

    logmsg(LOGMSG_DEBUG, "sql: %s\n", strbuf_buf(sql));
    strbuf_free(sql);
    return 0;
}

/* create and write SQL statements. uses ondisk schema */
int create_sqlmaster_records(void *tran)
{
    int table;
    int rc = 0;
    sql_mem_init(NULL);
    for (table = 0; table < thedb->num_dbs && rc == 0; table++) {
        rc = create_sqlmaster_record(thedb->dbs[table], tran);
    }

    sql_mem_shutdown(NULL);
    return rc;
}

char *sqltype(struct field *f, char *buf, int len)
{
    int dlen;

    dlen = f->len;

    switch (f->type) {
    case SERVER_UINT:
    case SERVER_BINT:
        dlen--;
    case CLIENT_UINT:
    case CLIENT_INT:
        switch (dlen) {
        case 2:
            snprintf(buf, len, "smallint");
            break;
        case 4:
            snprintf(buf, len, "int");
            break;
        case 8:
            snprintf(buf, len, "largeint");
            break;
        default:
            return NULL;
        }
        return buf;

    case SERVER_BREAL:
        dlen--;
    case CLIENT_REAL:
        switch (dlen) {
        case 4:
            snprintf(buf, len, "smallfloat");
            break;
        case 8:
            snprintf(buf, len, "double");
            break;
        default:
            return NULL;
        }
        return buf;

    case SERVER_BCSTR:
        dlen--;
    case CLIENT_CSTR:
    case CLIENT_PSTR2:
    case CLIENT_PSTR:
        snprintf(buf, len, "char(%d)", dlen);
        return buf;

    case SERVER_BYTEARRAY:
        dlen--;
    case CLIENT_BYTEARRAY:
        snprintf(buf, len, "blob(%d)", dlen);
        return buf;

    case CLIENT_BLOB:
    case SERVER_BLOB:
    case CLIENT_BLOB2:
    case SERVER_BLOB2:
        snprintf(buf, len, "blob");
        return buf;

    case CLIENT_VUTF8:
    case SERVER_VUTF8:
        snprintf(buf, len, "varchar");
        return buf;

    case CLIENT_DATETIME:
        logmsg(LOGMSG_FATAL, "THIS SHOULD NOT RUN! !");
        exit(1);

    case CLIENT_DATETIMEUS:
        logmsg(LOGMSG_FATAL, "THIS SHOULD NOT RUN! !");
        exit(1);

    case SERVER_DATETIME:
        snprintf(buf, len, "datetime");
        return buf;

    case SERVER_DATETIMEUS:
        snprintf(buf, len, "datetimeus");
        return buf;

    case CLIENT_INTVYM:
    case SERVER_INTVYM:
        snprintf(buf, len, "interval month");
        return buf;

    case CLIENT_INTVDS:
    case SERVER_INTVDS:
        snprintf(buf, len, "interval sec");
        return buf;

    case CLIENT_INTVDSUS:
    case SERVER_INTVDSUS:
        snprintf(buf, len, "interval usec");
        return buf;

    case SERVER_DECIMAL:
        snprintf(buf, len, "decimal");
        return buf;
    }

    return NULL;
}

/* Calculate space needed to store a sqlite version of a record for
   a given schema */
int schema_var_size(struct schema *sc)
{
    int i;
    int sz = 0;
    for (i = 0; i < sc->nmembers; i++) {
        switch (sc->member[i].type) {
        case SERVER_DATETIME:
        case SERVER_DATETIMEUS:
            sz += sizeof(dttz_t) + 7;
            break;
        case SERVER_INTVYM:
        case SERVER_INTVDS:
        case SERVER_INTVDSUS:
        case SERVER_DECIMAL:
            sz += sizeof(intv_t) + 7;
            break;
        default:
            sz += sc->member[i].len + 7;
            /* 6: 1 for the ondisk indicator byte, 5 for max size
             * of (weird sqlite) integer to represent type in header,
             * 1 for good luck */
            break;
        }
    }
    sz += sqlite3VarintLen(sz); /* for the length of header */
    sz += 12; /* max bytes needed by rrn/genid (comdb2 8 byte rrns/genid +
               * header byte + sqlite type byte) */
    return sz;
}

struct schema_mem {
    struct schema *sc;
    Mem *min;
    Mem *mout;
};

/**
** Updates comdb2 dbtable with any scalar funcs that may be
** used by its indexes. This information is read from sqlite.
*/
int resolve_sfuncs_for_table(sqlite3 *db, struct dbtable *tbl)
{
    int rc = 0;
    assert(tbl->lua_sfuncs == NULL);
    rc = sqlite3_table_index_funcs(db, NULL, tbl->tablename, &tbl->lua_sfuncs, &tbl->num_lua_sfuncs);
    if (!rc && (tbl->ix_func = (int)(tbl->num_lua_sfuncs > 0) != 0))
        assert(tbl->lua_sfuncs);
    return rc;
}

/* This function let's us skip the syntax check if there is no SQL
 * in the CSC2 schema.
 */
static int do_syntax_check(struct dbtable *tbl)
{
    return ((gbl_partial_indexes && tbl->ix_partial) ||
            (gbl_expressions_indexes && tbl->ix_expr) ||
            (tbl->n_check_constraints > 0))
               ? 1
               : 0;
}

#define INDEXES_THREAD_MEMORY 1048576
/* Force an update on sqlite_master to perform a syntax check for
 * partial index and CHECK constraint expressions.
 * Also, used to resolve scalar functions used by the dbtable
 */
int sql_syntax_check(struct ireq *iq, struct dbtable *db)
{
    int rc = 0;
    int resolve_rc = 0;
    sqlite3 *hndl = NULL;
    struct schema_mem sm = {0};
    const char *temp = "select 1 from sqlite_master limit 1";
    char *err = NULL;
    int got_curtran = 0;
    master_entry_t *ents = NULL;
    int nents = 0;

    if (!do_syntax_check(db)) {
        return rc;
    }

    sql_mem_init(NULL);
    thread_memcreate(INDEXES_THREAD_MEMORY);

    rc = create_sqlmaster_record(db, NULL);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to create sqlmaster record\n",
               __func__);
        sql_mem_shutdown(NULL);
        return -1;
    }
    ents = create_master_entry_array(&db, 1, thedb->view_hash, &nents);
    if (!ents) {
        logmsg(LOGMSG_ERROR, "%s: failed to create master entries\n", __func__);
        sql_mem_shutdown(NULL);
        return -1;
    }

    struct sqlclntstate clnt;
    start_internal_sql_clnt(&clnt);
    clnt.sql = (char *)temp;

    /* schema_mems is used to pass db->schema to is_comdb2_index_blob so we can
     * mark db->schema->ix_blob if the index expression has blob fields */
    sm.sc = db->schema;
    clnt.verify_indexes = 1;
    clnt.schema_mems = &sm;

    struct sql_thread *sqlthd = start_sql_thread();
    sql_get_query_id(sqlthd);
    clnt.debug_sqlclntstate = pthread_self();
    sqlthd->clnt = &clnt;

    get_copy_rootpages_custom(sqlthd, ents, nents);

    destroy_sqlite_master(ents, nents);

    struct sqlthdstate thd = {0};
    rc = sqlite3_open_serial("db", &hndl, &thd);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: sqlite3_open failed\n", __func__);
        goto done;
    }

    rc = get_curtran(thedb->bdb_env, &clnt);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: td %p unable to get a CURSOR transaction, rc = %d!\n", __func__,
               (void *)pthread_self(), rc);
        goto done;
    }
    got_curtran = 1;

    if ((rc = sqlite3_exec(hndl, clnt.sql, NULL, NULL, &err)) == 0) {
        if ((resolve_rc = resolve_sfuncs_for_table(hndl, db) != 0)) {
            logmsg(LOGMSG_ERROR, "%s: resolve_sfuncs_for_table failed with rc=%d\n", __func__, resolve_rc);
        }
    }

done:
    if (err) {
        logmsg(LOGMSG_ERROR, "Sqlite syntax check error: \"%s\"\n", err);
        if (iq)
            reqerrstr(iq, ERR_SC, "%s", err);
        sqlite3_free(err);
    }
    if (got_curtran && put_curtran(thedb->bdb_env, &clnt))
        logmsg(LOGMSG_ERROR, "%s: failed to close curtran\n", __func__);
    if (hndl)
        sqlite3_close_serial(&hndl);

    end_internal_sql_clnt(&clnt);
    done_sql_thread();
    sql_mem_shutdown(NULL);
    return rc;
}

/*
** Populates scalar functions for each db if used in index
*/
int resolve_sfuncs_for_db(struct dbenv* thedb)
{
    int rc = 0;
    sqlite3 * hndl;
    struct sqlclntstate clnt;
    struct sql_thread *sqlthd;
    struct sqlthdstate thd = {0};
    int got_curtran = 0;
    const char * sql = "select 1 from sqlite_master limit 1";

    thread_memcreate(INDEXES_THREAD_MEMORY);

    start_internal_sql_clnt(&clnt);
    clnt.sql = (char *)sql;

    sqlthd = start_sql_thread();
    sql_get_query_id(sqlthd);
    sqlthd->clnt = &clnt;

    assert_lock_schema_lk();
    get_copy_rootpages_nolock(sqlthd);

    if ((rc = sqlite3_open_serial("db", &hndl, &thd) != 0)) {
        logmsg(LOGMSG_ERROR, "%s: sqlite3_open failed\n", __func__);
        goto done;
    }

    if ((rc = get_curtran(thedb->bdb_env, &clnt) != 0)) {
        logmsg(LOGMSG_ERROR, "%s: unable to get a CURSOR transaction, rc = %d!\n", __func__, rc);
        goto done;
    }
    got_curtran = 1;

    for (int tbl_idx = 0; tbl_idx < thedb->num_dbs; ++tbl_idx) {
        struct dbtable *tbl = thedb->dbs[tbl_idx];
        if (!is_sqlite_stat(tbl->tablename))
            resolve_sfuncs_for_table(hndl, thedb->dbs[tbl_idx]);
    }

done:
    if (got_curtran && put_curtran(thedb->bdb_env, &clnt))
        logmsg(LOGMSG_ERROR, "%s: failed to close curtran\n", __func__);
    if (hndl)
        sqlite3_close_serial(&hndl);

    end_internal_sql_clnt(&clnt);
    done_sql_thread();
    return rc;
}

struct session_tbl {
    int dirty;
    char *name;
    TAILQ_ENTRY(session_tbl) entry;
};

static struct session_tbl *get_session_tbl(struct sqlclntstate *clnt, const char *tbl_name)
{
    if (clnt->dbtran.mode == TRANLEVEL_SOSQL || is_sqlite_stat(tbl_name)) {
        return NULL;
    }
    struct session_tbl *tbl = NULL;
    TAILQ_FOREACH(tbl, &clnt->session_tbls, entry) {
        if (strcmp(tbl_name, tbl->name) == 0)  {
            break;
        }
    }
    if (!tbl) {
        tbl = calloc(1, sizeof(struct session_tbl));
        tbl->name = strdup(tbl_name);
        TAILQ_INSERT_TAIL(&clnt->session_tbls, tbl, entry);
    }
    return tbl;
}

void clear_session_tbls(struct sqlclntstate *clnt)
{
    struct session_tbl *tbl, *tmp;
    TAILQ_FOREACH_SAFE(tbl, &clnt->session_tbls, entry, tmp) {
        TAILQ_REMOVE(&clnt->session_tbls, tbl, entry);
        free(tbl->name);
        free(tbl);
    }
}

static int move_is_nop(BtCursor *pCur, int *pRes)
{
    if (*pRes != 1 || pCur->cursor_class != CURSORCLASS_INDEX) {
        return 0;
    }
    struct schema *s = pCur->db->ixschema[pCur->sc->ixnum];
    if (s->flags & SCHEMA_DUP) return 0;
    if (pCur->clnt->dbtran.mode == TRANLEVEL_SOSQL) return 1;
    if (pCur->session_tbl && !pCur->session_tbl->dirty) return 1;
    return 0;
}

/*
   GLUE BETWEEN SQLITE AND COMDB2.

   This is the sqlite btree layer and other pieces of the sqlite backend
   re-implemented and/or stubbed out.  sqlite3Btree* calls are provided here.
   The sqlite library "thinks" it is running un-modified.

   Please only expose interfaces in this file that are ACTUAL sqlite interfaces
   needed to host the library.
 */

#define UNIMPLEMENTED 99999

typedef int (*xCmpPacked)(KeyInfo *, int l1, const void *k1, int l2,
                          const void *k2);

static inline int i64cmp(const i64 *key1, const i64 *key2)
{
    if (*key1 > *key2)
        return 1;
    if (*key1 < *key2)
        return -1;
    return 0;
}

#include "vdbecompare.c"

/**
 * Detects if a cursor is stalling and optimize certain moves
 * Mark done if the move is done
 *
 * This is a helper to the other cursor_move functions, which
 * are part of a cursor's method function block.
 */
static int cursor_move_preprop(BtCursor *pCur, int *pRes, int how, int *done)
{
    struct sql_thread *thd = pCur->thd;
    int rc = SQLITE_OK;

    /* skip cost if this is not doing anything! */
    if ((pCur->next_is_eof && how == CNEXT) ||
        (pCur->prev_is_eof && how == CPREV)) {
        *pRes = 1;
        pCur->eof = 1;
        *done = 1;
        return SQLITE_OK;
    }

    /* add to cost */
    switch (how) {
    case CFIRST:
    case CLAST:
        /*printf("tbl %s first/last cost %f\n", pCur->db ? pCur->db->tablename :
         * "<temp>", pCur->find_cost); */
        thd->cost += pCur->find_cost;
        pCur->nfind++;
        break;

    case CPREV:
    case CNEXT:
        /*printf("tbl %s next/prev cost %f\n", pCur->db ? pCur->db->tablename :
         * "<temp>", pCur->move_cost); */
        thd->cost += pCur->move_cost;
        pCur->nmove++;
        break;
    }

    if (!is_sqlite_db_init(pCur)) {
        rc = sql_tick(thd, 0);
        if (rc) {
            *done = 1;
            return rc;
        }
    }

    int inprogress;
    if (thd->clnt->is_analyze &&
        ((inprogress = get_schema_change_in_progress(__func__, __LINE__)) ||
                      get_analyze_abort_requested() ||
                      db_is_exiting())) {
        if (inprogress)
            logmsg(LOGMSG_ERROR, 
                    "%s: Aborting Analyze because schema_change_in_progress\n",
                    __func__);
        if (get_analyze_abort_requested())
            logmsg(LOGMSG_ERROR, 
                    "%s: Aborting Analyze because of send analyze abort\n",
                    __func__);
        if (db_is_exiting())
            logmsg(LOGMSG_ERROR,
                    "%s: Aborting Analyze because db is exiting\n",
                    __func__);
        *done = 1;
        rc = -1;
        return SQLITE_BUSY;
    }

    pCur->next_is_eof = 0;
    pCur->prev_is_eof = 0;

    /* reset blobs if we read any */
    if (pCur->blobs.numcblobs > 0)
        free_blob_status_data(&pCur->blobs);

    *done = 0;

    return SQLITE_OK;
}

/**
 * Helper function for class dependent function cursor_move_table()
 */
static void extract_stat_record(struct dbtable *db, uint8_t *in, uint8_t *out,
                                int *outlen)
{
    struct schema *s = db->schema;
    int n = is_stat2(db->tablename) ? 3 : 2;
    // for stat4 we have n (samplelen) = 2

    /* extract len */
    struct field *f = &s->member[n];
#ifdef _SUN_SOURCE
    int t;
    memcpy(&t, (int *)(in + f->offset + 1), sizeof(int));
    *outlen = ntohl(t);
#else
    *outlen = ntohl(*(int *)(in + f->offset + 1));
#endif

    /* overwrite the data with packed record */
    f = &s->member[n + 1];
    in = in + f->offset + 1;
    memmove(out, in, *outlen);
}

static void genid_hash_add(BtCursor *cur, int rrn, unsigned long long genid)
{
    /* remove old (latest one is the one to use) */
    struct key {
        int rrn;
        struct dbtable *db;
    } key = {0};

    if (cur->db->dtastripe) {
        logmsg(LOGMSG_ERROR, "genid_hash_add called w/ dtastripe\n");
        return;
    }
    key.rrn = rrn;
    key.db = cur->db;

    /* printf("genid_hash_add(cur %d rrn %d genid %08llx rc %d)\n",
     * cur->cursorid, rrn, genid, rc); */
    int rc = bdb_temp_hash_insert(cur->bt->genid_hash, &key, sizeof(struct key),
                                  &genid, sizeof(genid));
    if (rc)
        logmsg(LOGMSG_ERROR, "bdb_temp_hash_insert returned rc = %d\n", rc);
}

static int get_matching_genid(BtCursor *cur, int rrn, unsigned long long *genid)
{
    int rc;
    int len;
    struct key {
        int rrn;
        struct dbtable *db;
    } key = {0};

    if (cur->db->dtastripe) {
        logmsg(LOGMSG_ERROR, "get_matching_genid called w/ dtastripe\n");
        return -1;
    }
    key.rrn = rrn;
    key.db = cur->db;

    rc = bdb_temp_hash_lookup(cur->bt->genid_hash, &key, sizeof(key), genid,
                              &len, sizeof(unsigned long long));
    if (rc)
        return -1;
    return 0;
}

static int cursor_move_table(BtCursor *pCur, int *pRes, int how)
{
    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt;
    int bdberr = 0;
    int done = 0;
    int rc = SQLITE_OK;
    int outrc = SQLITE_OK;
    uint8_t ver;

    if (access_control_check_sql_read(pCur, thd)) {
        return SQLITE_ACCESS;
    }

    rc = cursor_move_preprop(pCur, pRes, how, &done);
    if (done) {
        return rc;
    }

    clnt = thd->clnt;

    /* If no tablescans are allowed (sqlite_stats* tables are exempt), return an error */
    if (!clnt->limits.tablescans_ok && pCur->db && !(is_sqlite_stat(pCur->db->tablename)))
        return SQLITE_NO_TABLESCANS;

    /* Set had_tablescans flag if we're asked to warn of tablescans. */
    if (clnt->limits.tablescans_warn)
        thd->had_tablescans = 1;

    outrc = SQLITE_OK;
    *pRes = 0;
    if (thd)
        thd->nmove++;

    bdberr = 0;
    rc = ddguard_bdb_cursor_move(thd, pCur, 0, &bdberr, how, NULL, 0);
    switch(bdberr) {
    case BDBERR_NOT_DURABLE: return SQLITE_CLIENT_CHANGENODE;
    case BDBERR_TRANTOOCOMPLEX: return SQLITE_TRANTOOCOMPLEX;
    case BDBERR_TRAN_CANCELLED: return SQLITE_TRAN_CANCELLED;
    case BDBERR_NO_LOG: return SQLITE_TRAN_NOLOG;
    case BDBERR_DEADLOCK:
        logmsg(LOGMSG_ERROR, "%s: too much contention, retried %d times [%llx]\n", __func__,
               gbl_move_deadlk_max_attempt, clnt->osql.rqid);
        ctrace("%s: too much contention, retried %d times [%llx]\n", __func__, gbl_move_deadlk_max_attempt,
               clnt->osql.rqid);
        return SQLITE_DEADLOCK;
    }

    if (rc == IX_FND || rc == IX_NOTFND) {
        void *buf;
        int sz;

        if (unlikely(pCur->is_btree_count)) {
            if (pCur->is_recording)
                pCur->genid = pCur->bdbcur->genid(pCur->bdbcur);
        } else {
            /*
               pCur->rrn = pCur->bdbcur->rrn(pCur->bdbcur);
               pCur->genid = pCur->bdbcur->genid(pCur->bdbcur);
               sz = pCur->bdbcur->datalen(pCur->bdbcur);
               buf = pCur->bdbcur->data(pCur->bdbcur);
               ver = pCur->bdbcur->ver(pCur->bdbcur);
             */
            pCur->bdbcur->get_found_data(pCur->bdbcur, &pCur->rrn, &pCur->genid,
                                         &sz, &buf, &ver);
            vtag_to_ondisk_vermap(pCur->db, buf, &sz, ver);
            if (sz > getdatsize(pCur->db)) {
                /* This shouldn't happen, but check anyway */
               logmsg(LOGMSG_ERROR, "%s: incorrect datsize %d\n", __func__, sz);
                return SQLITE_INTERNAL;
            }
            if (pCur->writeTransaction) {
                memcpy(pCur->dtabuf, buf, sz);
            } else {
                pCur->dtabuf = buf;
            }
        }
    }

    if (rc == IX_EMPTY) {
        pCur->empty = 1;
        *pRes = 1;
    } else if (rc == IX_PASTEOF) {
        pCur->eof = 1;
        *pRes = 1;
    } else if (is_good_ix_find_rc(rc)) {
        *pRes = 0;

        pCur->empty = 0;

        if (unlikely(pCur->cursor_class == CURSORCLASS_STAT24) &&
            !pCur->is_btree_count)
            extract_stat_record(pCur->db, pCur->dtabuf, pCur->dtabuf,
                                &pCur->dtabuflen);

        if (!pCur->db->dtastripe)
            genid_hash_add(pCur, pCur->rrn, pCur->genid);

        if (!gbl_selectv_rangechk) {
            if ((rc == IX_FND || rc == IX_FNDMORE) && pCur->is_recording &&
                clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
                rc = osql_record_genid(pCur, thd, pCur->genid);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "%s: failed to record genid %llx (%llu)\n",
                            __func__, pCur->genid, pCur->genid);
                }
            }
        }

        rc = 0;
    } else if (rc == IX_ACCESS) {
        outrc = SQLITE_ACCESS;
    } else if (rc) {
        logmsg(LOGMSG_ERROR, "%s dir %d rc %d bdberr %d\n", __func__, how, rc, bdberr);
        outrc = SQLITE_INTERNAL;
    }

#ifdef DEBUG_TRAN
    if (gbl_debug_sql_opcodes) {
        fprintf(stdout, "MOVE [%s] : genid=%llx pRes=%d how=%d rc=%d\n",
                pCur->db->tablename, pCur->genid, *pRes, how, outrc);
        fflush(stdout);
    }
#endif

    return outrc;
}

static int cursor_move_index(BtCursor *pCur, int *pRes, int how)
{
    struct sql_thread *thd = pCur->thd;
    struct ireq iq = {0};
    int bdberr = 0;
    int done = 0;
    int rc = SQLITE_OK;
    int outrc = SQLITE_OK;
    struct sqlclntstate *clnt = thd->clnt;

    if (access_control_check_sql_read(pCur, thd)) {
        return SQLITE_ACCESS;
    }

    rc = cursor_move_preprop(pCur, pRes, how, &done);
    if (done) {
        return rc;
    }

    iq.dbenv = thedb;
    iq.is_fake = 1;
    iq.usedb = pCur->db;
    iq.opcode = OP_FIND;

    outrc = SQLITE_OK;
    *pRes = 0;
    if (thd)
        thd->nmove++;

    if (how == CNEXT) {
        pCur->num_nexts++;

        if (gbl_prefaulthelper_sqlreadahead && pCur->num_nexts == gbl_sqlreadaheadthresh) {
            pCur->num_nexts = 0;
            readaheadpf(&iq, pCur->db, pCur->ixnum, pCur->fndkey,
                        getkeysize(pCur->db, pCur->ixnum), gbl_sqlreadahead);
        }
    }

    bdberr = 0;
    rc = ddguard_bdb_cursor_move(thd, pCur, 0, &bdberr, how, &iq, 0);
    switch(bdberr) {
    case BDBERR_NOT_DURABLE: return SQLITE_CLIENT_CHANGENODE;
    case BDBERR_TRANTOOCOMPLEX: return SQLITE_TRANTOOCOMPLEX;
    case BDBERR_TRAN_CANCELLED: return SQLITE_TRAN_CANCELLED;
    case BDBERR_NO_LOG: return SQLITE_TRAN_NOLOG;
    case BDBERR_DEADLOCK:
        logmsg(LOGMSG_ERROR, "%s too much contention, retried %d times.\n", __func__,
                gbl_move_deadlk_max_attempt);
        ctrace("%s: too much contention, retried %d times [%llx]\n", __func__,
               gbl_move_deadlk_max_attempt,
               (thd->clnt && thd->clnt->osql.rqid)
                   ? thd->clnt->osql.rqid
                   : 0);
        return SQLITE_DEADLOCK;
    }

    if (rc == IX_FND || rc == IX_NOTFND) {
        void *buf;
        int sz;
        uint8_t _;

        if (unlikely(pCur->is_btree_count)) {
            if (pCur->is_recording)
                pCur->genid = pCur->bdbcur->genid(pCur->bdbcur);
        } else {

            /*
               pCur->rrn = pCur->bdbcur->rrn(pCur->bdbcur);
               pCur->genid = pCur->bdbcur->genid(pCur->bdbcur);
               sz = pCur->bdbcur->datalen(pCur->bdbcur);
               buf = pCur->bdbcur->data(pCur->bdbcur);
             */
            pCur->bdbcur->get_found_data(pCur->bdbcur, &pCur->rrn, &pCur->genid,
                                         &sz, &buf, &_);

            if (sz > getkeysize(pCur->db, pCur->ixnum)) {
                logmsg(LOGMSG_ERROR, "%s: incorrect size %d\n", __func__, sz);
                return SQLITE_INTERNAL;
            }

            if (pCur->writeTransaction) {
                memcpy(pCur->ondisk_key, buf, sz);
                pCur->lastkey = pCur->ondisk_key;
            } else {
                /*
                 ** I don't think we need to copy to ondisk_key
                 ** memcpy(pCur->ondisk_key, buf, sz);
                 */
                pCur->lastkey = buf;
            }
        }
    }

    if (rc == IX_EMPTY) {
        pCur->empty = 1;
        *pRes = 1;
    } else if (rc == IX_PASTEOF) {
        pCur->eof = 1;
        *pRes = 1;
    } else if (is_good_ix_find_rc(rc)) {
        *pRes = 0;

        /* convert key (append rrn) */
        if (!pCur->db->dtastripe)
            genid_hash_add(pCur, pCur->rrn, pCur->genid);

        pCur->empty = 0;

        if (!gbl_selectv_rangechk) {
            if ((rc == IX_FND || rc == IX_FNDMORE) && pCur->is_recording &&
                thd->clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {

                rc = osql_record_genid(pCur, thd, pCur->genid);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "%s: failed to record genid %llx (%llu)\n",
                            __func__, pCur->genid, pCur->genid);
                }
            }
        }

        if (unlikely(pCur->is_btree_count))
            return outrc;

        /* if this cursor is on a key, convert key */
        rc = ondisk_to_sqlite_tz(pCur->db, pCur->sc, pCur->lastkey /* in */,
                                 pCur->rrn, pCur->genid, pCur->keybuf /* out */,
                                 pCur->keybuf_alloc, 0, NULL, NULL, NULL,
                                 &pCur->keybuflen, clnt->tzname, pCur);

        if (rc) {
            /* keys are always fixed length, so -2 should be impossible */
           logmsg(LOGMSG_ERROR, "%s: ondisk_to_sqlite_tz error rc = %d\n", __func__, rc);
            outrc = SQLITE_INTERNAL;
        }
    } else if (rc == IX_ACCESS) {
        outrc = SQLITE_ACCESS;
    } else if (rc) {
        logmsg(LOGMSG_ERROR, "%s dir %d rc %d bdberr %d\n", __func__, how, rc, bdberr);
        outrc = SQLITE_INTERNAL;
    }

    return outrc;
}

static int tmptbl_cursor_move(BtCursor *pCur, int *pRes, int how)
{
    int bdberr = 0;
    int done = 0;
    int rc = SQLITE_OK;

    rc = cursor_move_preprop(pCur, pRes, how, &done);
    if (done)
        return rc;

    switch (how) {
    case CFIRST:
        rc = bdb_temp_table_first(thedb->bdb_env, pCur->tmptable->cursor,
                                  &bdberr);
        break;

    case CLAST:
        rc = bdb_temp_table_last(thedb->bdb_env, pCur->tmptable->cursor,
                                 &bdberr);
        break;

    case CPREV:
        rc = bdb_temp_table_prev_norewind(thedb->bdb_env,
                                          pCur->tmptable->cursor, &bdberr);
        break;

    case CNEXT:
        rc = bdb_temp_table_next_norewind(thedb->bdb_env,
                                          pCur->tmptable->cursor, &bdberr);
        break;
    }

    if (rc == IX_PASTEOF || rc == IX_EMPTY) {
        rc = SQLITE_OK;
        *pRes = 1;
    } else if (rc) {
        logmsg(LOGMSG_ERROR, "%s:bdb_temp_table_MOVE error rc = %d\n", __func__, rc);
        rc = SQLITE_INTERNAL;
    } else {
        rc = SQLITE_OK;
        *pRes = 0;
        pCur->empty = 0;
    }
    return rc;
}

static int cursor_move_compressed(BtCursor *pCur, int *pRes, int how)
{
    int done = 0;
    int rc = SQLITE_OK;

    rc = cursor_move_preprop(pCur, pRes, how, &done);
    if (done)
        return rc;

    switch (how) {
    case CFIRST:
        rc = sampler_first(pCur->sampler);
        break;

    case CLAST:
        rc = sampler_last(pCur->sampler);
        break;

    case CPREV:
        rc = sampler_prev(pCur->sampler);
        break;

    case CNEXT:
        rc = sampler_next(pCur->sampler);
        break;
    }

    if (rc == IX_PASTEOF || rc == IX_EMPTY) {
        rc = SQLITE_OK;
        *pRes = 1;
    }

    else if (rc) {
        logmsg(LOGMSG_ERROR, "%s:bdb_temp_table_MOVE error rc = %d\n", __func__, rc);
        rc = SQLITE_INTERNAL;
    }

    else {
        void *tmp = sampler_key(pCur->sampler);

        rc = ondisk_to_sqlite_tz(pCur->db, pCur->sc, tmp, 2, 0, pCur->keybuf,
                                 pCur->keybuf_alloc, 0, NULL, NULL, NULL,
                                 &pCur->keybuflen, pCur->clnt->tzname, pCur);

        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: ondisk_to_sqlite_tz error rc = %d\n", __func__, rc);
            rc = SQLITE_INTERNAL;
        } else {
            rc = SQLITE_OK;
            *pRes = 0;
            pCur->empty = 0;
        }
    }
    return rc;
}

static int cursor_move_master(BtCursor *pCur, int *pRes, int how)
{
    /* skip preprop. if we're called from sqlite3_open_serial
     * and if peer_dropped_connection is true, we'll get NO SQL ENGINE and
     * a wasted thread apparently.
    int done = 0;
    int rc = cursor_move_preprop(pCur, pRes, how, &done, 0);
    if (done)
        return rc;
     */

    pCur->eof = 0; /* reset. set again if we really are past eof */
    struct sql_thread *thd = pCur->thd;

    /* local table */
    switch (how) {
    case CNEXT:
        if (((pCur->tblpos + 1 >= thd->rootpage_nentries) && !pCur->keyDdl) ||
            (pCur->tblpos + 1 > thd->rootpage_nentries)) {
            *pRes = 1;
            pCur->eof = 1;
            return SQLITE_OK;
        }
        /*NOTE: if there is siderow, tblpos = 0 and returns *pRes == nretries */
        *pRes = 0;
        pCur->tblpos++;
        break;

    case CPREV:
        if (pCur->tblpos < 0) {
            *pRes = 1;
            pCur->eof = 1;
            return SQLITE_OK;
        }
        *pRes = 0;
        pCur->tblpos--;
        break;

    case CFIRST:
        pCur->tblpos = 0;
        if (thd->rootpage_nentries <= 0 && !pCur->keyDdl) {
            pCur->eof = 1;
            *pRes = 1;
        } else
            *pRes = 0;
        /*NOTE: if there is siderow, tblpos = 0 and returns *pRes == 0 */
        break;

    case CLAST:
        if (pCur->keyDdl) {
            /* position on side row */
            pCur->tblpos = thd->rootpage_nentries;
        } else {
            pCur->tblpos = thd->rootpage_nentries - 1;
        }
        if (pCur->tblpos >= 0) {
            *pRes = 0;
            pCur->eof = 1;
        } else
            *pRes = 0;
        break;

    default:
        logmsg(LOGMSG_ERROR, "%s: unknown \"how\" operation %d\n", __func__, how);
        return SQLITE_INTERNAL;
    }
    return SQLITE_OK;
}

/* TODO review index base! */
static int cursor_move_remote(BtCursor *pCur, int *pRes, int how)
{
    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = thd->clnt;
    int done = 0;
    int rc = 0;

    /* check authentication */
    if (authenticate_cursor(pCur, AUTHENTICATE_READ) != 0)
        return SQLITE_ACCESS;

    assert(pCur->fdbc != NULL);

    rc = cursor_move_preprop(pCur, pRes, how, &done);
    if (done) {
        return rc;
    }

    *pRes = 0;
    if (thd)
        thd->nmove++;

    rc = pCur->fdbc->move(pCur, how);

    if (rc == IX_FND || rc == IX_FNDMORE) {
        char *data;
        int datalen;

        *pRes = 0;
        pCur->empty = 0;
        /* copy data into the cursor */

        pCur->fdbc->get_found_data(pCur, &pCur->genid, &datalen, &data);
        pCur->rrn = 2;
        if (pCur->ixnum == -1) {
            if (pCur->writeTransaction) {
                if (pCur->dtabuf_alloc < datalen) {
                    pCur->dtabuf = realloc(pCur->dtabuf, datalen);
                    if (!pCur->dtabuf) {
                        logmsg(LOGMSG_ERROR, "%s: failed malloc %d bytes\n",
                                __func__, datalen);
                        return -1;
                    }
                    pCur->dtabuf_alloc = datalen;
                }
                memcpy(pCur->dtabuf, data, datalen);
            } else {
                pCur->dtabuf = data;
            }
        } else {
            if (pCur->keybuf_alloc < datalen) {
                pCur->keybuf = realloc(pCur->keybuf, datalen);
                if (!pCur->keybuf) {
                    logmsg(LOGMSG_ERROR, "%s: failed malloc %d bytes\n", __func__,
                            datalen);
                    return -1;
                }
                pCur->keybuf_alloc = datalen;
            }
            pCur->keybuflen = datalen;
            memcpy(pCur->keybuf, data, datalen);
        }

        /* mark end of stream */
        /* NOTE: local cache doesn't provide advanced eof termination,
         *  so don't stop on IX_FND
         */
        if (rc == IX_FND && !(is_sqlite_stat(pCur->fdbc->name(pCur)))) {
            if (how == CNEXT || how == CFIRST) {
                pCur->next_is_eof = 1;
            } else if (how == CPREV || how == CLAST) {
                pCur->prev_is_eof = 1;
            }
        }
    } else if (rc == IX_PASTEOF || rc == IX_NOTFND || rc == IX_EMPTY) {
        pCur->empty = 1;
        *pRes = 1;
    } else if (rc == IX_ACCESS) {
        return SQLITE_ACCESS;
    } else if (rc == SQLITE_SCHEMA_REMOTE) {
        clnt->osql.error_is_remote = 1;
        clnt->osql.xerr.errval = CDB2ERR_ASYNCERR;

        errstat_set_strf(&clnt->osql.xerr,
                         "schema change table \"%s\" from db \"%s\"",
                         pCur->fdbc->dbname(pCur), pCur->fdbc->tblname(pCur));

        fdb_clear_sqlite_cache(pCur->sqlite, pCur->fdbc->dbname(pCur),
                               pCur->fdbc->tblname(pCur));

        return SQLITE_SCHEMA_REMOTE;
    } else if (rc == FDB_ERR_FDB_VERSION) {
        /* lower level handles the retries here */
        abort();
    } else {
        assert(rc != 0);
        logmsg(LOGMSG_ERROR, "%s dir %d rc %d\n", __func__, how, rc);
        return SQLITE_INTERNAL;
    }

    return SQLITE_OK;
}

static inline int sqlite3VdbeCompareRecordPacked(KeyInfo *pKeyInfo, int k1len,
                                                 const void *key1, int k2len,
                                                 const void *key2)
{
    if (!pKeyInfo) {
        return i64cmp(key1, key2);
    }

    UnpackedRecord *rec;

    rec = sqlite3VdbeAllocUnpackedRecord(pKeyInfo);
    if (rec == 0) {
        logmsg(LOGMSG_ERROR, "Error rec is zero, returned from "
                        "sqlite3VdbeAllocUnpackedRecord()\n");
        return 0;
    }
    sqlite3VdbeRecordUnpack(pKeyInfo, k2len, key2, rec);

    int cmp = sqlite3VdbeRecordCompare(k1len, key1, rec);
    sqlite3DbFree(pKeyInfo->db, rec);
    return cmp;
}

unsigned long long release_locks_on_si_lockwait_cnt = 0;
/* Release pagelocks if the replicant is waiting on this sql thread */
static int cursor_move_postop(BtCursor *pCur)
{
    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = thd->clnt;
    extern int gbl_sql_release_locks_on_si_lockwait;
    extern int gbl_locks_check_waiters;
    int rc = 0;

    if (gbl_locks_check_waiters && gbl_sql_release_locks_on_si_lockwait &&
        (clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
         clnt->dbtran.mode == TRANLEVEL_SERIAL)) {
        extern int gbl_sql_random_release_interval;
        if (bdb_curtran_has_waiters(thedb->bdb_env, clnt->dbtran.cursor_tran)) {
            rc = release_locks("replication is waiting on si-session");
            release_locks_on_si_lockwait_cnt++;
        } else if (gbl_sql_random_release_interval &&
                   !(rand() % gbl_sql_random_release_interval)) {
            rc = release_locks("random release cursor_move_postop");
            release_locks_on_si_lockwait_cnt++;
        }
    }

    return rc;
}

int temp_table_cmp(KeyInfo *pKeyInfo, int k1len, const void *key1, int k2len,
                   const void *key2)
{
    if (k2len >= 0)
        return sqlite3VdbeCompareRecordPacked(pKeyInfo, k1len, key1, k2len,
                                              key2);
    else
        return sqlite3VdbeRecordCompare(k1len, key1, (UnpackedRecord *)key2);
}

/* This is OP_MakeRecord from vdbe.c. */
void sqlite3VdbeRecordPack(UnpackedRecord *unpacked, Mem *pOut)
{
    u8 *zNewRecord;  /* A buffer to hold the data for the new record */
    Mem *pRec;       /* The new record */
    u64 nData;       /* Number of bytes of data space */
    int nHdr;        /* Number of bytes of header space */
    i64 nByte;       /* Data space required for this record */
    int nZero;       /* Number of zero bytes at the end of the record */
    int nVarint;     /* Number of bytes in a varint */
    u32 serial_type; /* Type field */
    Mem *pData0;     /* First field to be combined into the record */
    Mem *pLast;      /* Last field of the record */
    int nField;      /* Number of fields in the record */
    int file_format; /* File format to use for encoding */
    int i;           /* Space used in zNewRecord[] */
    u32 len;         /* Length of a field */

    nData = 0; /* Number of bytes of data space */
    nHdr = 0;  /* Number of bytes of header space */
    nByte = 0; /* Data space required for this record */
    nZero = 0; /* Number of zero bytes at the end of the record */
    pData0 = unpacked->aMem;
    nField = unpacked->nField;
    pLast = &pData0[nField - 1];
    file_format = SQLITE_DEFAULT_FILE_FORMAT;

    /* Loop through the elements that will make up the record to figure
     ** out how much space is required for the new record.
     */
    for (pRec = pData0; pRec <= pLast; pRec++) {
        serial_type = sqlite3VdbeSerialType(pRec, file_format, &len);
        nData += len;
        nHdr += sqlite3VarintLen(serial_type);
        if (pRec->flags & MEM_Zero) {
            /* Only pure zero-filled BLOBs can be input to this Opcode.
             ** We do not allow blobs with a prefix and a zero-filled tail. */
            nZero += pRec->u.nZero;
        } else if (len) {
            nZero = 0;
        }
    }

    /* Add the initial header varint and total the size */
    nHdr += nVarint = sqlite3VarintLen(nHdr);
    if (nVarint < sqlite3VarintLen(nHdr)) {
        nHdr++;
    }
    nByte = nHdr + nData - nZero;

    /* Make sure the output register has a buffer large enough to store
     ** the new record. The output register (pOp->p3) is not allowed to
     ** be one of the input registers (because the following call to
     ** sqlite3VdbeMemGrow() could clobber the value before it is used).
     */
    if (sqlite3VdbeMemGrow(pOut, (int)nByte, 0)) {
        /* goto no_mem; */
    }
    zNewRecord = (u8 *)pOut->z;

    /* Write the record */
    i = putVarint32(zNewRecord, nHdr);
    for (pRec = pData0; pRec <= pLast; pRec++) {
        serial_type = sqlite3VdbeSerialType(pRec, file_format, &len);
        i += putVarint32(&zNewRecord[i], serial_type); /* serial type */
    }
    for (pRec = pData0; pRec <= pLast; pRec++) { /* serial data */
        serial_type = sqlite3VdbeSerialType(pRec, file_format, &len);
        i += sqlite3VdbeSerialPut(&zNewRecord[i], pRec, serial_type);
    }
    assert(i == nByte);

    pOut->n = (int)nByte;
    pOut->flags = MEM_Blob;
    pOut->xDel = 0;
    if (nZero) {
        pOut->u.nZero = nZero;
        pOut->flags |= MEM_Zero;
    }
    pOut->enc = SQLITE_UTF8; /* In case the blob is ever converted to text */
}

static int sqlite_unpacked_to_ondisk(BtCursor *pCur, UnpackedRecord *rec,
                                     struct convert_failure *fail_reason,
                                     bias_info *bias_info)
{
    int rc = 0;
    int clen = 0;
    struct field *f;
    int fields;
    int sign;

    struct field_conv_opts_tz convopts = {0};
    convopts.flags = gbl_large_str_idx_find ? FLD_CONV_TRUNCATE : 0;

    struct mem_info info = {0};
    info.s = pCur->db->ixschema[pCur->ixnum];
    info.tzname = pCur->clnt->tzname;
    info.fail_reason = fail_reason;
    info.convopts = &convopts;

    /* assumption: this is called only for index btrees */
    if (pCur->ixnum == -1) {
        logmsg(LOGMSG_ERROR, "%s: bug,bug,bug\n", __func__);
        abort();
    }

    fields = rec->nField < info.s->nmembers ? rec->nField : info.s->nmembers;
    for (int i = 0; i < fields; ++i) {
        f = &info.s->member[i];
        info.m = &rec->aMem[i];
        info.fldidx = i;
        rc = mem_to_ondisk(pCur->ondisk_key, f, &info, bias_info);
        if (f->type == SERVER_DECIMAL) {
            /* this is an index, we need to remove quantums before trying to
             * match */
            decimal_quantum_get((char *)pCur->ondisk_key + f->offset, f->len, &sign);
        }
        if (rc)
            break;
        clen += f->len;
    }
    if (rc)
        return rc;

    return clen;
}

void xdump(void *b, int len)
{
    unsigned char *c;
    int i;

    c = (unsigned char *)b;
    for (i = 0; i < len; i++)
        logmsg(LOGMSG_USER, "%02x", c[i]);
}

sqlite3_int64 sqlite3BtreeMaxRecordSize(BtCursor *pCur){
    return 2147483647; /* see vdbeMemFromBtreeResize in vdbemem.c */
}

/*
 ** Return the currently defined page size
 */
int sqlite3BtreeGetPageSize(Btree *pBt)
{
    /* only used to get data in chunks. default to 4K */
    reqlog_logf(pBt->reqlogger, REQL_TRACE, "GetPageSize(pBt %d)       = %d\n",
                pBt->btreeid, 4096);
    return 4096;
}

int sqlite3BtreeIsReadonly(Btree *p) { return 0; }

/*
 ** Delete all information from the single table that pCur is open on.
 ** This routine only work for pCur on an ephemeral table.
 */
int sqlite3BtreeClearTableOfCursor(BtCursor *pCur)
{
    return sqlite3BtreeClearTable(pCur->bt, pCur->rootpage, 0);
}

/*
 ** This call is a no-op if no write-transaction is currently active on pBt.
 **
 ** Otherwise, sync the database file for the btree pBt. zMaster points to
 ** the name of a master journal file that should be written into the
 ** individual journal file, or is NULL, indicating no master journal file
 ** (single database transaction).
 **
 ** When this is called, the master journal should already have been
 ** created, populated with this journal pointer and synced to disk.
 **
 ** Once this is routine has returned, the only thing required to commit
 ** the write-transaction for this database file is to delete the journal.
 */
int sqlite3BtreeSync(Btree *pBt, const char *zMaster)
{
    /* backend does this, just return that all went well */
    int rc = SQLITE_OK;
    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                "Sync(pBt %d, journal %s)      = %s\n", pBt->btreeid,
                zMaster ? zMaster : "NULL", sqlite3ErrStr(rc));
    return rc;
}

/*
 ** Commit the statment subtransaction currently in progress.  If no
 ** subtransaction is active, this is a no-op.
 */
int sqlite3BtreeCommitStmt(Btree *pBt)
{
    /* no subtransactions */
    int rc = SQLITE_OK;
    reqlog_logf(pBt->reqlogger, REQL_TRACE, "CommitStmt(bt=%d)      = %s\n",
                pBt->btreeid, sqlite3ErrStr(rc));
    return rc;
}

static int temptable_free(void *obj, void *arg)
{
    struct temptable *tmp = obj;
    Btree *pBt = arg;
    if (tmp->owner == pBt) {
        int bdberr;
        int rc = bdb_temp_table_close(thedb->bdb_env, tmp->tbl, &bdberr);
        if (rc == 0) {
            ATOMIC_ADD32(gbl_sql_temptable_count, -1);
        } else {
            logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_close(%p) rc %d\n",
                   __func__, tmp->tbl, rc);
        }
    }
    free(tmp);
    return 0;
}

/*
 ** Close an open database and invalidate all cursors.
 */
int sqlite3BtreeClose(Btree *pBt)
{
    int rc = SQLITE_OK;
    BtCursor *pCur;
    struct sql_thread *thd;

    /* go through cursor list for db.  close any still open (abort? commit?),
     * deallocate pBt */

    if (pBt->genid_hash) {
        bdb_temp_hash_destroy(pBt->genid_hash);
        pBt->genid_hash = NULL;
    }

    BtCursor *tmp;
    LISTC_FOR_EACH_SAFE(&pBt->cursors, pCur, tmp, lnk)
    {
        rc = sqlite3BtreeCloseCursor(pCur);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: sqlite3BtreeCloseCursor failed, pCur=%p, rc=%d\n",
                   __func__, pCur, rc);
            /* Don't stop, or will leak cursors that will lock pages forever */
        }
    }
    assert(listc_size(&pBt->cursors) == 0);

    if (pBt->is_temporary) {
        hash_for(pBt->temp_tables, temptable_free, pBt);
        hash_free(pBt->temp_tables);
        pBt->temp_tables = NULL;
    }

    /* Reset thd pointers */
    thd = pthread_getspecific(query_info_key);
    if (thd) {
        if (pBt->is_temporary && thd->bttmp == pBt)
            thd->bttmp = NULL;
        else if (thd->bt == pBt)
            thd->bt = NULL;
    }

    if (pBt->free_schema && pBt->schema) {
        pBt->free_schema(pBt->schema);
        free(pBt->schema);
    }

    reqlog_logf(pBt->reqlogger, REQL_TRACE, "Close(pBt %d)      = %s\n",
                pBt->btreeid, sqlite3ErrStr(rc));

    if (pBt->zFilename) {
        free(pBt->zFilename);
        pBt->zFilename = NULL;
    }
    if (unlikely(gbl_debug_tmptbl_corrupt_mem)) {
        memset(pBt, 0xcdb2, sizeof(*pBt));
    }
    free(pBt);

    return rc;
}

/*
 ** Rollback the active statement subtransaction.  If no subtransaction
 ** is active this routine is a no-op.
 **
 ** All cursors will be invalidated by this operation.  Any attempt
 ** to use a cursor that was open at the beginning of this operation
 ** will result in an error.
 */
int sqlite3BtreeRollbackStmt(Btree *pBt)
{
    /* no subtransactions */
    int rc = SQLITE_OK;
    reqlog_logf(pBt->reqlogger, REQL_TRACE, "Rollback(pBt %d)      = %s\n",
                pBt->btreeid, sqlite3ErrStr(rc));
    return rc;
}

/*
 ** Change the default pages size and the number of reserved bytes per page.
 **
 ** The page size must be a power of 2 between 512 and 65536.  If the page
 ** size supplied does not meet this constraint then the page size is not
 ** changed.
 **
 ** Page sizes are constrained to be a power of two so that the region
 ** of the database file used for locking (beginning at PENDING_BYTE,
 ** the first byte past the 1GB boundary, 0x40000000) needs to occur
 ** at the beginning of a page.
 **
 ** If parameter nReserve is less than zero, then the number of reserved
 ** bytes per page is left unchanged.
 */
int sqlite3BtreeSetPageSize(Btree *pBt, int pageSize, int nReserve, int eFix)
{
    /* NOT NEEDED */
    /* backend takes care of this */
    int rc = SQLITE_OK;
    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                "GetPageSize(pBt %d pagesize %d reserve %d)       = %s\n",
                pBt->btreeid, pageSize, nReserve, sqlite3ErrStr(rc));
    return rc;
}

/*
 ** Change the way data is synced to disk in order to increase or decrease
 ** how well the database resists damage due to OS crashes and power
 ** failures.  Level 1 is the same as asynchronous (no syncs() occur and
 ** there is a high probability of damage)  Level 2 is the default.  There
 ** is a very low but non-zero probability of damage.  Level 3 reduces the
 ** probability of damage to near zero but with a write performance reduction.
 */
int sqlite3BtreeSetSafetyLevel(Btree *pBt, int level, int fullsync)
{
    /* backend takes care of this */
    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                "SetSafetyLevel(pBt %d, level %d, fullsync %d)     = %s\n",
                pBt->btreeid, level, fullsync, sqlite3ErrStr(SQLITE_OK));
    return SQLITE_OK;
}

/*
 ** Open a database file.
 **
 ** zFilename is the name of the database file.  If zFilename is NULL
 ** a new database with a random name is created.  This randomly named
 ** database file will be deleted when sqlite3BtreeClose() is called.
 */
int sqlite3BtreeOpen(
    sqlite3_vfs *pVfs,     /* dummy */
    const char *zFilename, /* name of the file containing the btree database */
    sqlite3 *pSqlite,      /* Associated database handle */
    Btree **ppBtree,       /* pointer to new btree object written here */
    int flags,             /* options */
    int vfsFlags)          /* Flags passed through to VFS open */
{
    int rc = SQLITE_OK;
    static int id = 0;
    Btree *bt;
    struct sql_thread *thd;
    struct reqlogger *logger;

    if (gbl_fdb_track)
        logmsg(LOGMSG_USER, "XXXXXXXXXXXXX Opening \"%s\"\n",
               (zFilename) ? zFilename : "NULL");

    thd = pthread_getspecific(query_info_key);
    logger = thrman_get_reqlogger(thrman_self());

    bt = calloc(1, sizeof(Btree));
    if (!bt) {
        logmsg(LOGMSG_ERROR, "sqlite3BtreeOpen(\"%s\"): out of memory\n", zFilename);
        rc = SQLITE_INTERNAL;
        goto done;
    }

    if (zFilename) {
        bt->zFilename = strdup(zFilename);
    }

    if (zFilename && strcmp(zFilename, "db") == 0) {
        /* local database */

        bt->reqlogger = logger;
        bt->btreeid = id++;

        bt->is_temporary = 0;
        listc_init(&bt->cursors, offsetof(BtCursor, lnk));

        *ppBtree = bt;
    } else if (!zFilename || strcmp(zFilename, ":memory:") == 0) {
        /* temporary connection (for temp tables and such) */
        bt->reqlogger = thrman_get_reqlogger(thrman_self());
        bt->btreeid = id++;
        bt->is_temporary = 1;
        int masterPgno;
        assert(tmptbl_clone == NULL);
        rc = sqlite3BtreeCreateTable(bt, &masterPgno, BTREE_INTKEY);
        if (rc != SQLITE_OK) goto done;
        assert(masterPgno == 1); /* sqlite_temp_master root page number */
        listc_init(&bt->cursors, offsetof(BtCursor, lnk));
        if (flags & BTREE_UNORDERED) {
            bt->is_hashtable = 1;
        }
        thd->bttmp = bt;
        *ppBtree = bt;
    } else if (zFilename) {
        /* TODO: maybe we should enforce unicity ? when attaching same dbs from
         * multiple sql threads */

        /* remote database */

        bt->reqlogger = logger;
        bt->btreeid = id++;
        listc_init(&bt->cursors, offsetof(BtCursor, lnk));
        bt->is_remote = 1;
        /* NOTE: this is a lockless pointer; at the time of setting this, we got
        a lock in sqlite3AddAndLockTable, so it should be good. The sqlite
        engine will keep this structure around after fdb tables are changed.
        While fdb will NOT go away, its tables can dissapear or change schema.
        Cached schema in Table object needs to be matched against fdb->tbl and
        make sure they are consistent before doing anything on the attached fdb
        */
        bt->fdb = get_fdb(zFilename);
        if (!bt->fdb) {
            logmsg(LOGMSG_ERROR, "%s: fdb not available for %s ?\n", __func__,
                    zFilename);
            free(bt);
            bt = NULL;
            rc = SQLITE_ERROR;
        }

        *ppBtree = bt;
    } else {
        logmsg(LOGMSG_FATAL, "%s no database name\n", __func__);
        abort();
    }

done:
    reqlog_logf(logger, REQL_TRACE,
                "Open(file %s, tree %d flags %d)     = %s\n",
                zFilename ? zFilename : "NULL",
                *ppBtree ? (*ppBtree)->btreeid : -1, flags, sqlite3ErrStr(rc));
    return rc;
}

int sql_set_transaction_mode(sqlite3 *db, struct sqlclntstate *clnt, int mode)
{
    int i;

    /* snapshot/serializable protection */
    if ((mode == TRANLEVEL_SERIAL || mode == TRANLEVEL_SNAPISOL) &&
        !(gbl_rowlocks || gbl_snapisol)) {
        logmsg(LOGMSG_ERROR, "%s REQUIRES MODIFICATIONS TO THE LRL FILE\n",
                (mode == TRANLEVEL_SNAPISOL) ? "SNAPSHOT ISOLATION"
                                             : "SERIALIZABLE");
        return -1;
    }

    for (i = 0; i < db->nDb; i++)
        if (db->aDb[i].pBt) {
            if (clnt->osql.count_changes) {
                db->flags |= SQLITE_CountRows;
            } else {
                db->flags &= ~SQLITE_CountRows;
            }
        }

    return 0;
}

/*
 ** Change the limit on the number of pages allowed in the cache.
 **
 ** The maximum number of cache pages is set to the absolute
 ** value of mxPage.  If mxPage is negative, the pager will
 ** operate asynchronously - it will not stop to do fsync()s
 ** to insure data is written to the disk surface before
 ** continuing.  Transactions still work if synchronous is off,
 ** and the database cannot be corrupted if this program
 ** crashes.  But if the operating system crashes or there is
 ** an abrupt power failure when synchronous is off, the database
 ** could be left in an inconsistent and unrecoverable state.
 ** Synchronous is on by default so database corruption is not
 ** normally a worry.
 */
int sqlite3BtreeSetCacheSize(Btree *pBt, int mxPage)
{
    /* backend handles this */
    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                "SetCacheSize(pBt %d, maxpage %d)      = %s\n", pBt->btreeid,
                mxPage, sqlite3ErrStr(SQLITE_OK));
    return SQLITE_OK;
}

/*
 ** Return the full pathname of the underlying database file.
 */
const char *sqlite3BtreeGetFilename(Btree *pBt)
{
    /* nothing should use this: fake out lib by returning value
     * indicating that we are dealing with an in-memory database */
    char *filename = "";

    reqlog_logf(pBt->reqlogger, REQL_TRACE, "GetFilename(%d)     = \"%s\"\n",
                pBt->btreeid, filename);
    return filename;
}

/*
 ** Return the pathname of the directory that contains the database file.
 */
const char *sqlite3BtreeGetDirname(Btree *pBt)
{
    /* again, should be unused: treat all dbs as in-memory */
    char *dirname = "";

    reqlog_logf(pBt->reqlogger, REQL_TRACE, "GetDirname(%d)     = %s\n",
                pBt->btreeid, dirname);
    return dirname;
}

/* Move the cursor to the last entry in the table.  Return SQLITE_OK
 ** on success.  Set *pRes to 0 if the cursor actually points to something
 ** or set *pRes to 1 if the table is empty.
 */
int sqlite3BtreeLast(BtCursor *pCur, int *pRes)
{
    int rc;

    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = thd->clnt;
    CurRangeArr **append_to;
    if (pCur->range) {
        if (pCur->range->idxnum == -1 && pCur->range->islocked == 0) {
            currange_free(pCur->range);
            pCur->range = currange_new();
        } else if (pCur->range->islocked || pCur->range->lkey ||
                   pCur->range->rkey || pCur->range->lflag ||
                   pCur->range->rflag) {
            if (!pCur->is_recording ||
                (clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE &&
                 gbl_selectv_rangechk)) {
                append_to =
                    (pCur->is_recording) ? &(clnt->selectv_arr) : &(clnt->arr);
                if (!*append_to) {
                    *append_to = malloc(sizeof(CurRangeArr));
                    currangearr_init(*append_to);
                }
                currangearr_append(*append_to, pCur->range);
            } else {
                currange_free(pCur->range);
            }
            pCur->range = currange_new();
        }
    }

    rc = pCur->cursor_move(pCur, pRes, CLAST);

    if (pCur->range && pCur->db) {
        if (!pCur->range->tbname) {
            pCur->range->tbname = strdup(pCur->db->tablename);
        }
        pCur->range->idxnum = pCur->ixnum;
        pCur->range->rflag = 1;
        if (pCur->range->rkey) {
            free(pCur->range->rkey);
            pCur->range->rkey = NULL;
        }
        pCur->range->rkeylen = 0;
        if (pCur->ixnum == -1 || *pRes == 1) {
            pCur->range->islocked = 1;
            pCur->range->lflag = 1;
            if (pCur->range->lkey) {
                free(pCur->range->lkey);
                pCur->range->lkey = NULL;
            }
            pCur->range->lkeylen = 0;
        }
    }

    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                "Last(pCur %d, exists %s)        = %s\n", pCur->cursorid,
                rc ? "dunno" : *pRes ? "no" : "yes", sqlite3ErrStr(rc));
    return rc;
}

/* print error and goto done if client has readonly set
 * but sql is writing to normal (non-tmp) btrees */
#define CHECK_CLNT_READONLY_BUT_SQL_IS_WRITING(clnt, pCur)                                                \
    do {                                                                                                  \
        if (clnt->is_readonly &&                                                                          \
            (pCur->cursor_class != CURSORCLASS_TEMPTABLE || !clnt->isselect) &&                           \
            (pCur->rootpage != RTPAGE_SQLITE_MASTER)) {                                                   \
            errstat_set_strf(&clnt->osql.xerr, "connection/database in read-only mode");                  \
            rc = SQLITE_ACCESS;                                                                           \
            goto done;                                                                                    \
        }                                                                                                 \
    } while(0);

/*
 ** Delete the entry that the cursor is pointing to.  The cursor
 ** is left pointing at a random location.
 */
int sqlite3BtreeDelete(BtCursor *pCur, int usage)
{
    int rc = SQLITE_OK;
    int bdberr = 0;
    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = thd->clnt;

    /* only record for temp tables - writes for real tables are recorded in
     * block code */
    if (thd && pCur->db == NULL) {
        thd->nwrite++;
        thd->cost += pCur->write_cost;
        pCur->nwrite++;
    }

    CHECK_CLNT_READONLY_BUT_SQL_IS_WRITING(clnt, pCur);

    /* if this is part of an analyze skip the delete - we'll do
     * the entire update part in one shot later when the analyze is done */
    if (clnt->is_analyze && is_sqlite_stat(pCur->db->tablename)) {
        rc = SQLITE_OK;
        goto done;
    }

    assert(0 == pCur->is_sampled_idx);

    if (pCur->bt->is_temporary) {
        rc = pCur->cursor_del(thedb->bdb_env, pCur->tmptable->cursor, &bdberr,
                              pCur);
        if (rc) {
            logmsg(LOGMSG_ERROR, "sqlite3BtreeDelete:bdb_temp_table_delete error rc = %d\n",
                   rc);
            rc = SQLITE_INTERNAL;
        }
    } else {
        /* check authentication */
        if (authenticate_cursor(pCur, AUTHENTICATE_WRITE) != 0) {
            rc = SQLITE_ACCESS;
            goto done;
        }

        /* sanity check, this should never happen */
        if (!is_sql_update_mode(clnt->dbtran.mode)) {
            logmsg(LOGMSG_ERROR, "%s: calling delete in the wrong sql mode %d\n",
                    __func__, clnt->dbtran.mode);
            rc = UNIMPLEMENTED;
            goto done;
        }

        if (gbl_partial_indexes && pCur->ixnum != -1)
            SQLITE3BTREE_KEY_SET_DEL(pCur->ixnum);

        /*
           Sqlite hack does not generate index delete plan unless this is index
        driven
        ignore deletes on keys, the delete is a table scan
        if (pCur->ixnum != -1 && !(pCur->open_flags & OPFLAG_FORDELETE)) {
           rc = SQLITE_OK;
           goto done;
        }*/
        /* skip the deletes on data btree, if the delete is a index scan */
        if ((pCur->ixnum == -1 && (pCur->open_flags & OPFLAG_FORDELETE)) ||
            (usage & OPFLAG_AUXDELETE)) {
            rc = SQLITE_OK;
            goto done;
        }

        if (pCur->bt == NULL || pCur->bt->is_remote == 0) {

            if ((queryOverlapsCursors(clnt, pCur) == 1) ||
                /* We ignore the failure for REPLACE as same record could conflict
                 * for more that one unique indexes.
                 */
                pCur->vdbe->oeFlag == OE_Replace) {
                rc = bdb_tran_deltbl_isdeleted_dedup(pCur->bdbcur, pCur->genid, 0, &bdberr);
                if (rc == 1) {
                    rc = SQLITE_OK;
                    goto done;
                } else if (rc < 0) {
                    logmsg(LOGMSG_ERROR, "%s:bdb_tran_deltbl_isdeleted error rc = %d bdberr=%d\n", __func__, rc,
                           bdberr);
                    rc = SQLITE_INTERNAL;
                    goto done;
                }
            }

            if (clnt->dbtran.maxchunksize > 0 &&
                clnt->dbtran.mode == TRANLEVEL_SOSQL &&
                clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
                if ((rc = chunk_transaction(pCur, clnt, thd)) != SQLITE_OK)
                    goto done;
            }

            if (is_sqlite_stat(pCur->db->tablename)) {
                clnt->ins_keys = -1ULL;
                clnt->del_keys = -1ULL;
            }

            if (gbl_expressions_indexes && pCur->ixnum != -1 &&
                pCur->db->ix_expr) {
                int keysize = getkeysize(pCur->db, pCur->ixnum);
                assert(clnt->idxDelete[pCur->ixnum] == NULL);
                clnt->idxDelete[pCur->ixnum] = malloc(keysize);
                if (clnt->idxDelete[pCur->ixnum] == NULL) {
                    logmsg(LOGMSG_ERROR, "%s:%d malloc %d failed\n", __func__,
                            __LINE__, keysize);
                    rc = SQLITE_NOMEM;
                    goto done;
                }
                if (keysize != pCur->bdbcur->datalen(pCur->bdbcur)) {
                    logmsg(LOGMSG_ERROR, "%s:%d malformed index %d\n", __func__,
                            __LINE__, pCur->ixnum);
                    rc = SQLITE_ERROR;
                    goto done;
                }
                memcpy(clnt->idxDelete[pCur->ixnum],
                       pCur->bdbcur->data(pCur->bdbcur), keysize);
            }

            /* this is part of a offload request;
             * we ship the offsql_del_rpl_t back
             * to the master
             */
            rc = osql_delrec(pCur, thd);
            clnt->effects.num_deleted++;
            clnt->log_effects.num_deleted++;
            clnt->nrows++;
        } else {
            /* make sure we have a distributed transaction and use that to
             * update remote */
            uuid_t tid;
            fdb_tran_t *trans = fdb_trans_begin_or_join(
                clnt, pCur->bt->fdb, (char *)tid, 0 /*TODO*/);

            if (!trans) {
                logmsg(LOGMSG_ERROR, 
                        "%s:%d failed to create or join distributed transaction!\n",
                       __func__, __LINE__);
                return SQLITE_INTERNAL;
            }

            if ((queryOverlapsCursors(clnt, pCur) == 1) ||
                /* We ignore the failure for REPLACE as same record could conflict
                 * for more that one unique indexes.
                 */
                pCur->vdbe->oeFlag == OE_Replace) {
                rc = fdb_is_genid_deleted(trans, pCur->genid);
                if (rc == 1) {
                    rc = SQLITE_OK;
                    goto done;
                } else if (rc < 0) {
                    rc = SQLITE_INTERNAL;
                    goto done;
                }
            }

            if (gbl_expressions_indexes && pCur->ixnum != -1 &&
                pCur->fdbc->tbl_has_expridx(pCur)) {
                assert(clnt->idxDelete[pCur->ixnum] == NULL);
                clnt->idxDelete[pCur->ixnum] =
                    malloc(sizeof(int) + pCur->keybuflen);
                if (clnt->idxDelete[pCur->ixnum] == NULL) {
                    logmsg(LOGMSG_ERROR, "%s:%d malloc %zu failed\n", __func__,
                           __LINE__, sizeof(int) + pCur->keybuflen);
                    rc = SQLITE_NOMEM;
                    goto done;
                }
                *((int *)clnt->idxDelete[pCur->ixnum]) = pCur->keybuflen;
                memcpy((unsigned char *)clnt->idxDelete[pCur->ixnum] +
                           sizeof(int),
                       pCur->keybuf, pCur->keybuflen);
            }

            if (is_sqlite_stat(pCur->fdbc->name(pCur))) {
                clnt->ins_keys = -1ULL;
                clnt->del_keys = -1ULL;
            }
            rc = pCur->fdbc->delete (pCur, clnt, trans, pCur->genid);
            clnt->effects.num_deleted++;
            clnt->nrows++;
        }
        clnt->ins_keys = 0ULL;
        clnt->del_keys = 0ULL;
        if (gbl_expressions_indexes) {
            free_cached_idx(clnt->idxInsert);
            free_cached_idx(clnt->idxDelete);
        }
        if (rc == SQLITE_DDL_MISUSE)
        {
            sqlite3_mutex_enter(sqlite3_db_mutex(pCur->vdbe->db));
            sqlite3VdbeError(pCur->vdbe,
                             "Transactional DDL Error: Overlapping Tables");
            sqlite3_mutex_leave(sqlite3_db_mutex(pCur->vdbe->db));
        }
    }

done:
    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE, "Delete(pCur %d)     "
                                                 "   = %s\n",
                pCur->cursorid, sqlite3ErrStr(rc));
    return rc;
}

/* Move the cursor to the first entry in the table.  Return SQLITE_OK
 ** on success.  Set *pRes to 0 if the cursor actually points to something
 ** or set *pRes to 1 if the table is empty.
 */
int sqlite3BtreeFirst(BtCursor *pCur, int *pRes)
{
    int rc;

    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = thd->clnt;
    CurRangeArr **append_to;
    if (pCur->range) {
        if (pCur->range->idxnum == -1 && pCur->range->islocked == 0) {
            currange_free(pCur->range);
            pCur->range = currange_new();
        } else if (pCur->range->islocked || pCur->range->lkey ||
                   pCur->range->rkey || pCur->range->lflag ||
                   pCur->range->rflag) {
            if (!pCur->is_recording ||
                (clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE &&
                 gbl_selectv_rangechk)) {
                append_to =
                    (pCur->is_recording) ? &(clnt->selectv_arr) : &(clnt->arr);
                if (!*append_to) {
                    *append_to = malloc(sizeof(CurRangeArr));
                    currangearr_init(*append_to);
                }
                currangearr_append(*append_to, pCur->range);
            } else {
                currange_free(pCur->range);
            }
            pCur->range = currange_new();
        }
    }

    rc = pCur->cursor_move(pCur, pRes, CFIRST);

    if (pCur->range && pCur->db) {
        if (!pCur->range->tbname) {
            pCur->range->tbname = strdup(pCur->db->tablename);
        }
        pCur->range->idxnum = pCur->ixnum;
        pCur->range->lflag = 1;
        if (pCur->range->lkey) {
            free(pCur->range->lkey);
            pCur->range->lkey = NULL;
        }
        pCur->range->lkeylen = 0;
        if (pCur->ixnum == -1 || *pRes == 1) {
            pCur->range->islocked = 1;
            pCur->range->rflag = 1;
            if (pCur->range->rkey) {
                free(pCur->range->rkey);
                pCur->range->rkey = NULL;
            }
            pCur->range->rkeylen = 0;
        }
    }

    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                "First(pCur %d, exists %s)        = %s\n", pCur->cursorid,
                rc ? "dunno" : *pRes ? "no" : "yes", sqlite3ErrStr(rc));
    return rc;
}

/*
 ** Copy the complete content of pBtFrom into pBtTo.  A transaction
 ** must be active for both files.
 **
 ** The size of file pBtFrom may be reduced by this operation.
 ** If anything goes wrong, the transaction on pBtFrom is rolled back.
 */
int sqlite3BtreeCopyFile(Btree *pBtTo, Btree *pBtFrom)
{
    /* should never see this */
    int rc = SQLITE_OK;
    reqlog_logf(pBtTo->reqlogger, REQL_TRACE,
                "CopyFile(pBtTo %d, pBtFrom %d)      = %s\n", pBtTo->btreeid,
                pBtFrom->btreeid, sqlite3ErrStr(rc));
    return rc;
}

/*
 ** Change the 'auto-vacuum' property of the database. If the 'autoVacuum'
 ** parameter is non-zero, then auto-vacuum mode is enabled. If zero, it
 ** is disabled. The default value for the auto-vacuum property is
 ** determined by the SQLITE_DEFAULT_AUTOVACUUM macro.
 */
int sqlite3BtreeSetAutoVacuum(Btree *pBt, int autoVacuum)
{
    int rc = SQLITE_OK;
    /* don't care about this, return ok */
    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                "SetAutoVacuum(pBt %d, auto %s)      = %s\n", pBt->btreeid,
                autoVacuum ? "on" : "off", sqlite3ErrStr(rc));
    return rc;
}

/*
 ** Erase all information in a table and add the root of the table to
 ** the freelist.  Except, the root of the principle table (the one on
 ** page 1) is never added to the freelist.
 **
 ** This routine will fail with SQLITE_LOCKED if there are any open
 ** cursors on the table.
 **
 ** If AUTOVACUUM is enabled and the page at iTable is not the last
 ** root page in the database file, then the last root page
 ** in the database file is moved into the slot formerly occupied by
 ** iTable and that last slot formerly occupied by the last root page
 ** is added to the freelist instead of iTable.  In this say, all
 ** root pages are kept at the beginning of the database file, which
 ** is necessary for AUTOVACUUM to work right.  *piMoved is set to the
 ** page number that used to be the last root page in the file before
 ** the move.  If no page gets moved, *piMoved is set to 0.
 ** The last root page is recorded in meta[3] and the value of
 ** meta[3] is updated by this procedure.
 */
int sqlite3BtreeDropTable(Btree *pBt, int iTable, int *piMoved)
{
    *piMoved = 0;
    if (!pBt->is_temporary)
        return UNIMPLEMENTED;
    struct temptable *tmp = hash_find(pBt->temp_tables, &iTable);
    if (tmp->owner == pBt) {
        int bdberr;
        int rc = bdb_temp_table_close(thedb->bdb_env, tmp->tbl, &bdberr);
        if (rc == 0) {
            ATOMIC_ADD32(gbl_sql_temptable_count, -1);
        } else {
            logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_close(%p) rc %d\n",
                   __func__, tmp->tbl, rc);
        }
    }
    hash_del(pBt->temp_tables, tmp);
    free(tmp);
    return SQLITE_OK;
}

/*
** Step the cursor to the back to the previous entry in the database.
** Return values:
**
**     SQLITE_OK     success
**     SQLITE_DONE   the cursor is already on the first element of the table
**     otherwise     some kind of error occurred
**
** The main entry point is sqlite3BtreePrevious().  That routine is optimized
** for the common case of merely decrementing the cell counter BtCursor.aiIdx
** to the previous cell on the current page.  The (slower) btreePrevious()
** helper routine is called when it is necessary to move to a different page
** or to restore the cursor.
**
** If bit 0x01 of the F argument to sqlite3BtreePrevious(C,F) is 1, then
** the cursor corresponds to an SQL index and this routine could have been
** skipped if the SQL index had been a unique index.  The F argument is a
** hint to the implement.  The native SQLite btree implementation does not
** use this hint, but COMDB2 does.
*/
int sqlite3BtreePrevious(BtCursor *pCur, int flags)
{
    int fndlen;
    void *buf;
    int *pRes = &flags;

    if (pCur->empty || move_is_nop(pCur, pRes)) {
        return SQLITE_DONE;
    }

    int rc = pCur->cursor_move(pCur, pRes, CPREV);
    if( *pRes==1 ) rc = SQLITE_DONE;

    if (pCur->range && pCur->db && !pCur->range->islocked) {
        if (pCur->ixnum == -1) {
            pCur->range->islocked = 1;
            pCur->range->lflag = 1;
            pCur->range->rflag = 1;
            if (pCur->range->rkey) {
                free(pCur->range->rkey);
                pCur->range->rkey = NULL;
            }
            if (pCur->range->lkey) {
                free(pCur->range->lkey);
                pCur->range->lkey = NULL;
            }
            pCur->range->lkeylen = 0;
            pCur->range->rkeylen = 0;
        } else if (*pRes == 1 || rc != SQLITE_OK) {
            pCur->range->lflag = 1;
            if (pCur->range->lkey) {
                free(pCur->range->lkey);
                pCur->range->lkey = NULL;
            }
            pCur->range->lkeylen = 0;
        } else if (pCur->bdbcur && pCur->range->lflag == 0) {
            fndlen = pCur->bdbcur->datalen(pCur->bdbcur);
            buf = pCur->bdbcur->data(pCur->bdbcur);
            if (pCur->range->lkey) {
                free(pCur->range->lkey);
                pCur->range->lkey = NULL;
            }
            pCur->range->lkey = malloc(fndlen);
            pCur->range->lkeylen = fndlen;
            memcpy(pCur->range->lkey, buf, fndlen);
        }
        if (pCur->range->lflag && pCur->range->rflag)
            pCur->range->islocked = 1;
    }

    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                "Previous(pCur %d, valid %s)      = %s\n", pCur->cursorid,
                *pRes ? "yes" : "no", sqlite3ErrStr(rc));
    return rc;
}

/*
 ** Return non-zero if a statement transaction is active.
 */
int sqlite3BtreeIsInStmt(Btree *pBt)
{
    /* don't care for now. if one was started, return that one is in progress */
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    int rc = (thd && thd->clnt) ? thd->clnt->intrans : 0;

    reqlog_logf(pBt->reqlogger, REQL_TRACE, "IsInTrans(pBt %d)      = %s\n",
                pBt->btreeid, rc ? "yes" : "no");
    return rc;
}

/*
 ** Return the currently defined page size
 */
int sqlite3BtreeGetReserve(Btree *pBt)
{
    /* space wasted in a page: don't care. how about a byte? */
    reqlog_logf(pBt->reqlogger, REQL_TRACE, "GetReserve(pBt %d)       = %d\n",
                pBt->btreeid, 1);
    return 1;
}

/*
From: D. Richard Hipp <DRH@HWACI.COM>
To: ALEX SCOTTI, BLOOMBERG/ 731 LEXIN
Subject: Re: sqlite3Btree questions
Date: 04/24, 2009  15:27:16

On Apr 24, 2009, at 3:08 PM, ALEX SCOTTI, BLOOMBERG/ 731 LEXIN wrote:

> i think i found a bug in our sqlite3btree implementation.
> my table has 1 integer column, "a" with an index on it.
> i store the values 1,2,3,4,5,6,7,8,9 in the table.
> "select * from alex1 where a <= 4 order by a desc" works as i would
> expect.
> "select * from alex1 where a <= 9999 order by a desc" is broken,
> giving me no results.
>
> i tracked down the issue to what seems to be a misunderstanding in
> our implementation of sqlite3BtreeEof().  what seems to happen is
> sqlite3BtreeMoveto() is called first, and looks up 9999, positioning
> the cursor at the last entry in the index.  then sqlite3BtreeEof()
> gets called and we erroneously(?) return true, terminating the query.
>
> are there semantics here that i didnt get right?

sqlite3BtreeEof() should only return TRUE if....

(1) the previous sqlite3BtreeNext() moved off the end of the table.
(2) the previous sqlite3BtreePrev() moved off the beginning of the
table.
(3) the table is empty (contains no rows)

It should always return FALSE after an sqlite3BtreeMoveto() unless the
table is empty.

The name sqlite3BtreeEof() is inappropriate, I think.  We should have
called it something closer to sqlite3BtreeInvalidRow().  It should
return true if the cursor is not currently pointing at a valid row in
the table.

D. Richard Hipp
drh@hwaci.com

 */

/*
 ** Return TRUE if the cursor is not pointing at an entry of the table.
 **
 ** TRUE will be returned after a call to sqlite3BtreeNext() moves
 ** past the last entry in the table or sqlite3BtreePrev() moves past
 ** the first entry.  TRUE is also returned if the table is empty.
 */
int sqlite3BtreeEof(BtCursor *pCur)
{
    int rc = /*pCur->eof || */ pCur->empty;
    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                " sqlite3BtreeEof(pCur %d)       = %d\n", pCur->cursorid, rc);
    return rc;
}

/*
 ** Read part of the key associated with cursor pCur.  Exactly
 ** "amt" bytes will be transfered into pBuf[].  The transfer
 ** begins at "offset".
 **
 ** Return SQLITE_OK on success or an error code if anything goes
 ** wrong.  An error is returned if "offset+amt" is larger than
 ** the available payload.
 */
int sqlite3BtreeKey(BtCursor *pCur, u32 offset, u32 amt, void *pBuf)
{
    int rc = SQLITE_OK;
    int reqsz;
    unsigned char *buf;

    /* Pretty sure this never gets called !*/
    if (pCur->bt && !pCur->bt->is_temporary &&
        pCur->rootpage == RTPAGE_SQLITE_MASTER)
        abort();

    // rc = SQLITE_ERROR;
    // the following code is slated for removal
    if (pCur->is_sampled_idx) {
        /* this is in ondisk format- i want to convert */
        buf = sampler_key(pCur->sampler);
        rc = ondisk_to_sqlite_tz(pCur->db, pCur->sc, pCur->ondisk_key, 2, 0,
                                 pCur->keybuf, pCur->keybuflen, 0, NULL, NULL,
                                 NULL, &reqsz, NULL, pCur);
        if (rc) {
           logmsg(LOGMSG_ERROR, "%s: ondisk_to_sqlite returns %d\n", __func__, rc);
        } else {
            memcpy(pBuf, (char *)pCur->keybuf + offset, amt);
        }
    } else if (pCur->bt->is_temporary) {
        buf = bdb_temp_table_key(pCur->tmptable->cursor);
        memcpy(pBuf, buf + offset, amt);
    } else if (pCur->ixnum == -1) {
        /* this is genid */
        assert(amt == sizeof(pCur->genid));
        memcpy(pBuf, &pCur->genid, sizeof(pCur->genid));
    } else {
        memcpy(pBuf, ((char *)pCur->keybuf) + offset, amt);
    }

    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                "Key(pCur %d, offset %d)      = %s\n", pCur->cursorid, offset,
                sqlite3ErrStr(rc));
    reqlog_loghex(pCur->bt->reqlogger, REQL_TRACE, pBuf, amt);
    reqlog_logl(pCur->bt->reqlogger, REQL_TRACE, "\n");
    return rc;
}

/*
 ** For the entry that cursor pCur is point to, return as
 ** many bytes of the key or data as are available on the local
 ** b-tree page.  Write the number of available bytes into *pAmt.
 **
 ** The pointer returned is ephemeral.  The key/data may move
 ** or be destroyed on the next call to any Btree routine.
 **
 ** These routines is used to get quick access to key and data
 ** in the common case where no overflow pages are used.
 */
const void *sqlite3BtreeDataFetch(BtCursor *pCur, u32 *pAmt)
{
    void *out = NULL;
    *pAmt = 0;

    assert(0 == pCur->is_sampled_idx);

    if (pCur->bt->is_temporary) {
        *pAmt = bdb_temp_table_datasize(pCur->tmptable->cursor);
        return bdb_temp_table_data(pCur->tmptable->cursor);
    } else if (pCur->rootpage == RTPAGE_SQLITE_MASTER) {
        struct sql_thread *thd = pCur->thd;
        if (pCur->bt && pCur->bt->is_remote) {
            if (fdb_master_is_local(pCur)) {
                *pAmt = fdb_get_sqlite_master_entry_size(
                    pCur->bt->fdb, pCur->crt_sqlite_master_row);
                out = fdb_get_sqlite_master_entry(pCur->bt->fdb,
                                                  pCur->crt_sqlite_master_row);
            } else {
                *pAmt = pCur->fdbc->datalen(pCur);
                out = pCur->fdbc->data(pCur);
            }
        } else if (pCur->tblpos == thd->rootpage_nentries) {
            assert(pCur->keyDdl);
            *pAmt = pCur->nDataDdl;
            out = pCur->dataDdl;
        } else {
            *pAmt = get_sqlite_entry_size(thd, pCur->tblpos);
            out = get_sqlite_entry(thd, pCur->tblpos);
        }
    } else if (pCur->bt->is_remote) {

        assert(pCur->fdbc);

        *pAmt = pCur->fdbc->datalen(pCur);
        out = pCur->fdbc->data(pCur);

    } else {
        if (pCur->ixnum == -1) {
            *pAmt = pCur->dtabuflen;
            out = pCur->dtabuf;
        } else {
            *pAmt = pCur->sqlrrnlen;
            out = pCur->sqlrrn;
        }
    }

    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                "DataFetch(pCur %d pAmt %d)      = %p\n", pCur->cursorid, *pAmt,
                out);
    if (out) {
        reqlog_loghex(pCur->bt->reqlogger, REQL_TRACE, out, *pAmt);
        reqlog_logl(pCur->bt->reqlogger, REQL_TRACE, "\n");
    }
    return (const void *)out;
}

/*
 ** Change the busy handler callback function.
 */
int sqlite3BtreeSetBusyHandler(Btree *pBt, BusyHandler *pHandler)
{
    /* we always block, no need for this */
    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                "SetBusyHandler(pBt %d, handler %p)      = %s\n", pBt->btreeid,
                pHandler, sqlite3ErrStr(SQLITE_OK));
    return SQLITE_OK;
}

/*
 ** Set size to the size of the buffer needed to hold the value of
 ** the key for the current entry.  If the cursor is not pointing
 ** to a valid entry, size is set to 0.
 **
 ** For a table with the INTKEY flag set, this routine returns the key
 ** itself, not the number of bytes in the key.
 */
i64 sqlite3BtreeIntegerKey(BtCursor *pCur)
{
    int rc = SQLITE_OK;
    i64 size = 0;

    /* keys and data widths are always fixed. if pointing to a
     * valid key, return lrl or ixlen, otherwise (at eof) return 0 */
    if (pCur->empty) {
        size = 0;
        /* sampled (previously misnamed compressed) index is a temptable
           of ondisk-format records */
    } else if (pCur->is_sampled_idx) {
        size = pCur->keybuflen;
    } else if (pCur->bt->is_temporary) {
        if (pCur->ixnum == -1) {
            void *buf;
            int sz;
            buf = bdb_temp_table_key(pCur->tmptable->cursor);
            sz = bdb_temp_table_keysize(pCur->tmptable->cursor);
            switch (sz) {
            case sizeof(int): /* rrn */
                size = (i64) * (int *)buf;
                break;
            case sizeof(unsigned long long): /* genid */
                size = (i64) * (unsigned long long *)buf;
                break;
            default:
                logmsg(LOGMSG_ERROR, "sqlite3BtreeIntegerKey: incorrect sz %d\n", sz);
                rc = SQLITE_INTERNAL;
                break;
            }
        } else {
            size = bdb_temp_table_keysize(pCur->tmptable->cursor);
        }
    } else if (pCur->rootpage == RTPAGE_SQLITE_MASTER) {
        if (pCur->bt->is_remote) {
            if (!fdb_master_is_local(pCur)) {
                logmsg(LOGMSG_ERROR, "%s: wrong assumption, master is not "
                                "local\n",
                        __func__);
                rc = SQLITE_INTERNAL;
            }
            size = fdb_get_sqlite_master_entry_size(
                pCur->bt->fdb, pCur->crt_sqlite_master_row);
        } else {
            struct sql_thread *thd = pCur->thd;
            if (pCur->tblpos == thd->rootpage_nentries) {
                assert(pCur->keyDdl);
                size = pCur->keyDdl;
            } else
                size = get_sqlite_entry_size(thd, pCur->tblpos);
        }
    } else if (pCur->ixnum == -1) {
        if (pCur->bt->is_remote || pCur->db->dtastripe)
            memcpy(&size, &pCur->genid, sizeof(unsigned long long));
        else
            size = pCur->rrn;
    } else {
        size = pCur->keybuflen;
    }

    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                "KeySize(pCur %d, size %lld)      = %s\n", pCur->cursorid,
                (long long)size, sqlite3ErrStr(rc));
    return size;
}

/*
 ** Set size to the number of bytes of data in the entry the
 ** cursor currently points to.  Always return SQLITE_OK.
 ** Failure is not possible.  If the cursor is not currently
 ** pointing to an entry (which can happen, for example, if
 ** the database is empty) then size is set to 0.
 */
u32 sqlite3BtreePayloadSize(BtCursor *pCur)
{
    u32 size = 0;

    assert(0 == pCur->is_sampled_idx);

    if (pCur->empty) {
        /* no data in btree */
        size = 0;
    } else if (pCur->bt->is_temporary) {
        size = bdb_temp_table_datasize(pCur->tmptable->cursor);
    } else if (pCur->rootpage == RTPAGE_SQLITE_MASTER) {
        if (pCur->bt && pCur->bt->is_remote) {
            if (fdb_master_is_local(pCur))
                size = fdb_get_sqlite_master_entry_size(
                    pCur->bt->fdb, pCur->crt_sqlite_master_row);
            else
                size = pCur->fdbc->datalen(pCur);
        } else {
            struct sql_thread *thd = pCur->thd;
            if (pCur->tblpos == thd->rootpage_nentries) {
                assert(pCur->keyDdl);
                size = pCur->nDataDdl;
            } else {
                size = get_sqlite_entry_size(thd, pCur->tblpos);
            }
        }
    } else if (pCur->bt->is_remote) {
        assert(pCur->fdbc);
        size = pCur->fdbc->datalen(pCur);
    } else {
        if (pCur->ixnum == -1) { /* data file.  data is actual data */
            size = pCur->dtabuflen;
        } else { /* key file. data is record number */
            size = pCur->sqlrrnlen;
        }
    }
/* UNIMPLEMENTED */

    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                "DataSize(pCur %d, size %d)      = %s\n", pCur->cursorid, size,
                sqlite3ErrStr(SQLITE_OK));
    return size;
}

/*
 ** Return the pathname of the journal file for this database. The return
 ** value of this routine is the same regardless of whether the journal file
 ** has been created or not.
 */
const char *sqlite3BtreeGetJournalname(Btree *pBt)
{
    char *jname = "";
    /* UNIMPLEMENTED */
    reqlog_logf(pBt->reqlogger, REQL_TRACE, "GetJournalname(%d)     = \"%s\"\n",
                pBt->btreeid, jname);
    return NULL;
}

void get_current_lsn(struct sqlclntstate *clnt)
{
    struct dbtable *db =
        &thedb->static_table; /* this is not used but required */
    if (db) {
        bdb_get_current_lsn(db->handle, &(clnt->file), &(clnt->offset));
    } else {
        logmsg(LOGMSG_ERROR, "get_current_lsn: ireq has no bdb handle\n");
        abort();
    }
    if (clnt->arr) {
        clnt->arr->file = clnt->file;
        clnt->arr->offset = clnt->offset;
    }
    if (clnt->selectv_arr) {
        clnt->selectv_arr->file = clnt->file;
        clnt->selectv_arr->offset = clnt->offset;
    }
}

static int get_snapshot(struct sqlclntstate *clnt, int *f, int *o)
{
    return clnt->plugin.get_snapshot(clnt, f, o);
}

int initialize_shadow_trans(struct sqlclntstate *clnt, struct sql_thread *thd)
{
    int rc = SQLITE_OK;
    struct ireq iq;
    int error = 0;
    int snapshot_file = 0;
    int snapshot_offset = 0;

    if (!clnt->snapshot) {
       get_snapshot(clnt, &snapshot_file, &snapshot_offset);
    }

    init_fake_ireq(thedb, &iq);
    iq.usedb = &thedb->static_table; /* this is not used but required */

    switch (clnt->dbtran.mode) {
    default:
        logmsg(LOGMSG_ERROR, "%s: unknown mode %d\n", __func__, clnt->dbtran.mode);
        return SQLITE_INTERNAL;
        break;
    case TRANLEVEL_SNAPISOL:
        clnt->dbtran.shadow_tran = trans_start_snapisol(
            &iq, clnt->bdb_osql_trak, clnt->snapshot, snapshot_file,
            snapshot_offset, &error, clnt->is_hasql_retry);

        if (!clnt->dbtran.shadow_tran) {
            logmsg(LOGMSG_ERROR, "%s:trans_start_snapisol error %d\n", __func__,
                   error);
            if (!error) {
                rc = SQLITE_INTERNAL;
            } else if (error == BDBERR_NOT_DURABLE) {
                rc = SQLITE_CLIENT_CHANGENODE;
            } else if (error == BDBERR_TRANTOOCOMPLEX) {
                rc = SQLITE_TRANTOOCOMPLEX;
            } else if (error == BDBERR_NO_LOG) {
                rc = SQLITE_TRAN_NOLOG;
            } else {
                rc = error;
            }
            return rc;
        }

        break;
    /* we handle communication with a blockprocess when all is over */

    case TRANLEVEL_SERIAL:
        /*
         * Serial needs to create a transaction, so that two
         * subsequent selects part of the same transaction see
         * the same data (inserts are easily skipped, but deletes
         * and updates will have visible effects otherwise
         */
        clnt->dbtran.shadow_tran = trans_start_serializable(
            &iq, clnt->bdb_osql_trak, clnt->snapshot, snapshot_file,
            snapshot_offset, &error, clnt->is_hasql_retry);

        if (!clnt->dbtran.shadow_tran) {
            logmsg(LOGMSG_ERROR, "%s:trans_start_serializable error\n", __func__);
            if (!error) {
                rc = SQLITE_INTERNAL;
            } else if (error == BDBERR_NOT_DURABLE) {
                rc = SQLITE_CLIENT_CHANGENODE;
            } else if (error == BDBERR_NO_LOG) {
                rc = SQLITE_TRAN_NOLOG;
            } else {
                rc = error;
            }
            return rc;
        }

        break;
    /* we handle communication with a blockprocess when all is over */

    case TRANLEVEL_RECOM:
        /* create our special bdb transaction
         * (i.e. w/out berkdb transaction */
        clnt->dbtran.shadow_tran =
            trans_start_readcommitted(&iq, clnt->bdb_osql_trak);

        if (!clnt->dbtran.shadow_tran) {
           logmsg(LOGMSG_ERROR, "%s:trans_start_readcommitted error\n", __func__);
           return SQLITE_INTERNAL;
        }

        break;
    /* we handle communication with a blockprocess when all is over */

    case TRANLEVEL_SOSQL:
        /* this is the first update of the transaction, open a
         * block processor on the master */
        clnt->dbtran.shadow_tran =
            trans_start_socksql(&iq, clnt->bdb_osql_trak);
        if (!clnt->dbtran.shadow_tran) {
           logmsg(LOGMSG_ERROR, "%s:trans_start_socksql error\n", __func__);
           return SQLITE_INTERNAL;
        }

        clnt->osql.sock_started = 0;

        break;
    }

    return rc;
}

int start_new_transaction(struct sqlclntstate *clnt, struct sql_thread *thd)
{
    int rc;

    clnt->ins_keys = 0ULL;
    clnt->del_keys = 0ULL;

    if (gbl_expressions_indexes) {
        free_cached_idx(clnt->idxInsert);
        free_cached_idx(clnt->idxDelete);
    }

    if (clnt->arr) {
        currangearr_free(clnt->arr);
        clnt->arr = NULL;
    }
    if (clnt->selectv_arr) {
        currangearr_free(clnt->selectv_arr);
        clnt->selectv_arr = NULL;
    }

    if (clnt->dbtran.mode == TRANLEVEL_SERIAL) {
        clnt->arr = malloc(sizeof(CurRangeArr));
        currangearr_init(clnt->arr);
    }
    if (gbl_selectv_rangechk) {
        clnt->selectv_arr = malloc(sizeof(CurRangeArr));
        currangearr_init(clnt->selectv_arr);
    }

    get_current_lsn(clnt);

    if (clnt->ctrl_sqlengine == SQLENG_STRT_STATE)
        sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_INTRANS_STATE);

    clnt->intrans = 1;
    clear_session_tbls(clnt);

#ifdef DEBUG_TRAN
    if (gbl_debug_sql_opcodes) {
        logmsg(LOGMSG_ERROR,
               "%p starts transaction tid=%d mode=%d intrans=%d\n", clnt,
               pthread_self(), clnt->dbtran.mode, clnt->intrans);
    }
#endif
    if ((rc = initialize_shadow_trans(clnt, thd)) != 0) {
        sql_debug_logf(clnt, __func__, __LINE__,
                       "initialize_shadow_tran returns %d\n", rc);
        return rc;
    }

    uuidstr_t us;
    char rqidinfo[40];
    snprintf(rqidinfo, sizeof(rqidinfo), "rqid=%016llx %s appsock %" PRIxPTR,
             clnt->osql.rqid, comdb2uuidstr(clnt->osql.uuid, us),
             (intptr_t)clnt->appsock_id);
    thrman_setid(thrman_self(), rqidinfo);

    return 0;
}

/*
 ** Attempt to start a new transaction. A write-transaction
 ** is started if the second argument is nonzero, otherwise a read-
 ** transaction.  If the second argument is 2 or more and exclusive
 ** transaction is started, meaning that no other process is allowed
 ** to access the database.  A preexisting transaction may not be
 ** upgrade to exclusive by calling this routine a second time - the
 ** exclusivity flag only works for a new transaction.
 **
 ** A write-transaction must be started before attempting any
 ** changes to the database.  None of the following routines
 ** will work unless a transaction is started first:
 **
 **      sqlite3BtreeCreateTable()
 **      sqlite3BtreeCreateIndex()
 **      sqlite3BtreeClearTable()
 **      sqlite3BtreeDropTable()
 **      sqlite3BtreeInsert()
 **      sqlite3BtreeDelete()
 **      sqlite3BtreeUpdateMeta()
 **
 ** If wrflag is true, then nMaster specifies the maximum length of
 ** a master journal file name supplied later via sqlite3BtreeSync().
 ** This is so that appropriate space can be allocated in the journal file
 ** when it is created..
 */

int sqlite3BtreeBeginTrans(Vdbe *vdbe, Btree *pBt, int wrflag, int *pSchemaVersion)
{
    int rc = SQLITE_OK;
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;

#ifdef DEBUG_TRAN
    if (gbl_debug_sql_opcodes) {
        logmsg(LOGMSG_ERROR, "%s %d %d\n", __func__, clnt->intrans,
               clnt->ctrl_sqlengine);
    }
#endif

    /* already have a transaction, keep using it until it commits/aborts */
    if (clnt->intrans || clnt->in_sqlite_init ||
        (clnt->ctrl_sqlengine != SQLENG_STRT_STATE &&
         clnt->ctrl_sqlengine != SQLENG_NORMAL_PROCESS)) {
        rc = SQLITE_OK;
        goto done;
    }

    if (wrflag && clnt->origin) {
        if (gbl_check_sql_source && !allow_write_from_remote(clnt->origin)) {
            sqlite3_mutex_enter(sqlite3_db_mutex(vdbe->db));
            sqlite3VdbeError(vdbe, "write from node %d not allowed",
                             clnt->conninfo.node);
            sqlite3_mutex_leave(sqlite3_db_mutex(vdbe->db));
            rc = SQLITE_ACCESS;
            goto done;
        }
    }

    /* we get here:
     * - we are not in an sql transaction (old style, intrans) AND
     * - the sql state is EITHER:
     * - SQLENG_STRT_STATE (saw a begin)
     * - SQLENG_NORMAL_PROCESS (singular requests)
     *
     * once out of it, sql state is:
     * - unchanged, if read-only stmt
     * - SQLENG_INTRANS_STATE otherwise
     * (this will block any more access here until after a commit/rollback)
     */

    if (pBt->is_temporary) {
        goto done;
    }

    if (clnt->dbtran.mode <= TRANLEVEL_RECOM && wrflag == 0) { // read-only
        if (clnt->has_recording == 0 ||                        // not selectv
            clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS) { // singular selectv
            rc = SQLITE_OK;
            goto done;
        }
    }

    /* UPSERT: If we were asked to perform some action on conflict
     * (ON CONFLICT DO UPDATE/REPLACE and not DO NOTHING), then its a good
     * idea to switch at least to READ COMMITTED if we are running in some
     * lower isolation level in a standalone (autocommit) transaction.
     */
    if ((clnt->dbtran.mode < TRANLEVEL_RECOM) &&
        (clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS) &&
        (vdbe && comdb2ForceVerify(vdbe) == 1)) {
        logmsg(LOGMSG_DEBUG, "%s: switched to %s from %s\n", __func__,
               tranlevel_tostr(TRANLEVEL_RECOM),
               tranlevel_tostr(clnt->dbtran.mode));
        clnt->translevel_changed = 1;
        clnt->dbtran.mode = TRANLEVEL_RECOM;
    }

    rc = start_new_transaction(clnt, thd);

done:
    if (rc == SQLITE_OK && pSchemaVersion) {
        sqlite3BtreeGetMeta(pBt, BTREE_SCHEMA_VERSION, (u32 *)pSchemaVersion);
    }

    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                "BeginTrans(pBt %d, wrflag %d)      = %s (rc=%d)\n",
                pBt->btreeid, wrflag, sqlite3ErrStr(rc), rc);
    return rc;
}

extern int gbl_early_verify;
extern int gbl_osql_send_startgen;
int gbl_forbid_incoherent_writes;

/*
 ** Commit the transaction currently in progress.
 **
 ** This will release the write lock on the database file.  If there
 ** are no active cursors, it also releases the read lock.
 */

void abort_dbtran(struct sqlclntstate *clnt);

int sqlite3BtreeCommit(Btree *pBt)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;
    int rc = SQLITE_OK;
    int irc = 0;
    int bdberr = 0;

#ifdef DEBUG_TRAN
    if (gbl_debug_sql_opcodes) {
        uuidstr_t us;
        fprintf(
            stderr,
            "sqlite3BtreeCommit intrans=%d sqleng=%d openeng=%d rqid=%llx %s\n",
            clnt->intrans, clnt->ctrl_sqlengine, clnt->in_sqlite_init,
            clnt->osql.rqid, comdb2uuidstr(clnt->osql.uuid, us));
    }
#endif

    if (clnt->arr)
        currangearr_coalesce(clnt->arr);
    if (clnt->selectv_arr)
        currangearr_coalesce(clnt->selectv_arr);

    if (!clnt->intrans || clnt->in_sqlite_init ||
        (!clnt->in_sqlite_init && clnt->ctrl_sqlengine != SQLENG_FNSH_STATE &&
         clnt->ctrl_sqlengine != SQLENG_NORMAL_PROCESS &&
         clnt->ctrl_sqlengine != SQLENG_FNSH_ABORTED_STATE)) {
        rc = SQLITE_OK;
        goto done;
    }

    clnt->recno = 0;

    /* reset the state of the sqlengine */
    if (clnt->ctrl_sqlengine == SQLENG_FNSH_STATE ||
        clnt->ctrl_sqlengine == SQLENG_FNSH_ABORTED_STATE)
        sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                SQLENG_NORMAL_PROCESS);

    int64_t rows = clnt->log_effects.num_updated +
                   clnt->log_effects.num_deleted +
                   clnt->log_effects.num_inserted;
    if (rows && gbl_forbid_incoherent_writes && !clnt->had_lease_at_begin) {
        abort_dbtran(clnt);
        errstat_cat_str(&clnt->osql.xerr, "failed write from incoherent node");
        clnt->osql.xerr.errval = ERR_BLOCK_FAILED + ERR_VERIFY;
        return SQLITE_ABORT;
    }
    clnt->intrans = 0;
    /* This is the last chunk; count it in. */
    if (clnt->dbtran.maxchunksize > 0 && clnt->dbtran.mode == TRANLEVEL_SOSQL)
        ++clnt->dbtran.nchunks;
    clnt->dbtran.crtchunksize = clnt->dbtran.maxchunksize = 0;

#ifdef DEBUG_TRAN
    if (gbl_debug_sql_opcodes) {
        uuidstr_t us;
        fprintf(stderr, "%p commits transaction %d %d rqid=%llx %s\n", clnt,
                pthread_self(), clnt->dbtran.mode, clnt->osql.rqid,
                comdb2uuidstr(clnt->osql.uuid, us));
    }
#endif

    switch (clnt->dbtran.mode) {
    default:

        logmsg(LOGMSG_ERROR, "%s: unknown mode %d\n", __func__, clnt->dbtran.mode);
        rc = SQLITE_INTERNAL;
        goto done;

    case TRANLEVEL_RECOM:

        /*
         * Because we don't see begin/commit here, this is processed only
         * for the standalone updates!
         * for recom, we take care of the idx shadows first
         * -we wipe em out
         */
        if (clnt->dbtran.shadow_tran) {
            rc = recom_commit(clnt, thd, clnt->tzname, 0);

            if (!rc) {
                irc = trans_commit_shadow(clnt->dbtran.shadow_tran, &bdberr);
            } else {
                irc = trans_abort_shadow((void **)&clnt->dbtran.shadow_tran,
                                         &bdberr);
            }
            if (irc) {
                logmsg(LOGMSG_ERROR, "%s: commit failed rc=%d bdberr=%d\n", __func__,
                        irc, bdberr);
            }
            clnt->dbtran.shadow_tran = NULL;
        }

        /* UPSERT: Restore the isolation level back to what it was. */
        if (clnt->translevel_changed) {
            clnt->dbtran.mode = TRANLEVEL_SOSQL;
            clnt->translevel_changed = 0;
            logmsg(LOGMSG_DEBUG, "%s: switched back to %s\n", __func__,
                   tranlevel_tostr(clnt->dbtran.mode));
        }

        break;

    case TRANLEVEL_SNAPISOL:
        if (clnt->dbtran.shadow_tran) {
            rc = snapisol_commit(clnt, thd, clnt->tzname);
            if (!rc) {
                irc = trans_commit_shadow(clnt->dbtran.shadow_tran, &bdberr);
            } else {
                irc = trans_abort_shadow((void **)&clnt->dbtran.shadow_tran,
                                         &bdberr);
            }
            if (irc) {
                logmsg(LOGMSG_ERROR, "%s:%d %s shadow failed rc=%d bdberr=%d\n",
                       __func__, __LINE__, rc ? "abort" : "commit", irc,
                       bdberr);
            }
            clnt->dbtran.shadow_tran = NULL;
        }
        break;

    case TRANLEVEL_SERIAL:

        /*
         * Because we don't see begin/commit here, this is processed only
         * for the standalone updates!
         * for snapisol, serial, we take care of the idx shadows first
         * -we wipe em out
         */
        if (clnt->dbtran.shadow_tran) {
            rc = serial_commit(clnt, thd, clnt->tzname);
            if (!rc) {
                if (clnt->dbtran.shadow_tran) { // TODO: NULL possible here?  Maybe from serial_commit?
                    irc =
                        trans_commit_shadow(clnt->dbtran.shadow_tran, &bdberr);
                }
            } else {
                irc = trans_abort_shadow((void **)&clnt->dbtran.shadow_tran,
                                         &bdberr);
            }
            if (irc) {
                logmsg(LOGMSG_ERROR, "%s:%d %s shadow failed rc=%d bdberr=%d\n",
                       __func__, __LINE__, rc ? "abort" : "commit", irc,
                       bdberr);
            }
            clnt->dbtran.shadow_tran = NULL;
        }
        break;

    case TRANLEVEL_SOSQL:
        if (!clnt->skip_peer_chk && gbl_early_verify && !clnt->early_retry && gbl_osql_send_startgen) {
            if (clnt->start_gen != bdb_get_rep_gen(thedb->bdb_env))
                clnt->early_retry = EARLY_ERR_GENCHANGE;
        }
        if (gbl_selectv_rangechk)
            rc = selectv_range_commit(clnt);
        if (rc) {
            irc = osql_sock_abort(clnt, OSQL_SOCK_REQ);
            if (irc) {
                logmsg(LOGMSG_ERROR,
                       "%s: failed to abort sorese transaction irc=%d\n",
                       __func__, irc);
            }
            clnt->early_retry = 0;
            rc = SQLITE_ABORT;
        } else if (clnt->early_retry) {
            irc = osql_sock_abort(clnt, OSQL_SOCK_REQ);
            if (irc) {
                logmsg(LOGMSG_ERROR,
                       "%s: failed to abort sorese transaction irc=%d\n",
                       __func__, irc);
            }
            if (clnt->early_retry == EARLY_ERR_VERIFY) {
                clnt->osql.xerr.errval = ERR_BLOCK_FAILED + ERR_VERIFY;
                errstat_cat_str(&(clnt->osql.xerr),
                                "unable to update record rc = 4");
            } else if (clnt->early_retry == EARLY_ERR_SELECTV) {
                clnt->osql.xerr.errval = ERR_CONSTR;
                errstat_cat_str(&(clnt->osql.xerr),
                                "constraints error, no genid");
            } else if (clnt->early_retry == EARLY_ERR_GENCHANGE) {
                clnt->osql.xerr.errval = ERR_BLOCK_FAILED + ERR_VERIFY;
                errstat_cat_str(&(clnt->osql.xerr),
                                "verify error on master swing");
            }
            if (clnt->early_retry) {
                clnt->early_retry = 0;
                rc = SQLITE_ABORT;
            }
        } else {
            rc = osql_sock_commit(clnt, OSQL_SOCK_REQ, TRANS_CLNTCOMM_NORMAL);
            osqlstate_t *osql = &thd->clnt->osql;
            if (osql->xerr.errval == COMDB2_SCHEMACHANGE_OK) {
                osql->xerr.errval = 0;
            }
        }
        break;

    }

    clnt->ins_keys = 0ULL;
    clnt->del_keys = 0ULL;

    if (gbl_expressions_indexes) {
        free_cached_idx(clnt->idxInsert);
        free_cached_idx(clnt->idxDelete);
    }

    if (clnt->arr) {
        currangearr_free(clnt->arr);
        clnt->arr = NULL;
    }
    if (clnt->selectv_arr) {
        currangearr_free(clnt->selectv_arr);
        clnt->selectv_arr = NULL;
    }

    reset_clnt_flags(clnt);

done:
    reqlog_logf(pBt->reqlogger, REQL_TRACE, "Commit(pBt %d)      = %s\n",
                pBt->btreeid, sqlite3ErrStr(rc));
    return rc;
}

int rollback_tran(struct sql_thread *thd, struct sqlclntstate *clnt)
{
    int rc = SQLITE_OK;

    switch (clnt->dbtran.mode) {
    default:
        logmsg(LOGMSG_ERROR, "%s: unknown mode %d\n", __func__,
               clnt->dbtran.mode);
        rc = SQLITE_INTERNAL;
        break;

    case TRANLEVEL_RECOM:
        if (clnt->dbtran.shadow_tran) {
            rc = recom_abort(clnt);
            if (rc)
                logmsg(LOGMSG_ERROR, "%s: recom abort rc=%d??\n", __func__, rc);
        }
        break;

    case TRANLEVEL_SNAPISOL:
        if (clnt->dbtran.shadow_tran) {
            rc = snapisol_abort(clnt);
            if (rc)
                logmsg(LOGMSG_ERROR, "%s: recom abort rc=%d??\n", __func__, rc);
        }
        break;

    case TRANLEVEL_SERIAL:
        if (clnt->dbtran.shadow_tran) {
            rc = serial_abort(clnt);
            if (rc)
                logmsg(LOGMSG_ERROR, "%s: recom abort rc=%d??\n", __func__, rc);
        }
        break;

    case TRANLEVEL_SOSQL:
        rc = osql_sock_abort(clnt, OSQL_SOCK_REQ);
        break;
    }

    return rc;
}

/*
 ** Rollback the transaction in progress.  All cursors will be
 ** invalided by this operation.  Any attempt to use a cursor
 ** that was open at the beginning of this operation will result
 ** in an error.
 **
 ** This will release the write lock on the database file.  If there
 ** are no active cursors, it also releases the read lock.
 */
int sqlite3BtreeRollback(Btree *pBt, int dummy, int writeOnlyDummy)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;
    int rc = SQLITE_OK;

    /*
     * fprintf(stderr, "sqlite3BtreeRollback %d %d\n", clnt->intrans,
     * clnt->ctrl_sqlengine );
     */

    if (!clnt || !clnt->intrans || clnt->in_sqlite_init ||
        (clnt->ctrl_sqlengine != SQLENG_FNSH_RBK_STATE &&
         clnt->ctrl_sqlengine != SQLENG_NORMAL_PROCESS)) {
        rc = SQLITE_OK;
        goto done;
    }

    /* this is happening if the sql errored and the
     * sql engine is rollbacking the transaction */
    if (clnt->ctrl_sqlengine == SQLENG_FNSH_RBK_STATE ||
        clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE)
        sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                SQLENG_NORMAL_PROCESS);

    clnt->intrans = 0;
    clear_session_tbls(clnt);

    /* UPSERT: Restore the isolation level back to what it was. */
    if (clnt->dbtran.mode == TRANLEVEL_RECOM && clnt->translevel_changed) {
        clnt->dbtran.mode = TRANLEVEL_SOSQL;
        clnt->translevel_changed = 0;
        logmsg(LOGMSG_DEBUG, "%s: switched back to %s\n", __func__,
               tranlevel_tostr(clnt->dbtran.mode));
    }

    rc = rollback_tran(thd, clnt);

    clnt->ins_keys = 0ULL;
    clnt->del_keys = 0ULL;

    if (gbl_expressions_indexes) {
        free_cached_idx(clnt->idxInsert);
        free_cached_idx(clnt->idxDelete);
    }

    if (clnt->arr) {
        currangearr_free(clnt->arr);
        clnt->arr = NULL;
    }
    if (clnt->selectv_arr) {
        currangearr_free(clnt->selectv_arr);
        clnt->selectv_arr = NULL;
    }

    reset_clnt_flags(clnt);

done:
    reqlog_logf(pBt->reqlogger, REQL_TRACE, "Rollback(pBt %d)      = %s\n",
                pBt->btreeid, sqlite3ErrStr(rc));
    return rc;
}

/*
 ** Return non-zero if a transaction is active.
 */
int sqlite3BtreeIsInTrans(Btree *pBt)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    int rc = (thd && thd->clnt) ? thd->clnt->intrans : 0;

/* UNIMPLEMENTED */
/* FIXME TODO XXX #if 0'ed out the foll */
#if 0
   reqlog_logf(pBt->reqlogger, REQL_TRACE, "IsInTrans(pBt %d)      = %s\n",
         pBt->btreeid, rc ? "yes" : "no");
#endif
    return rc;
}

/*
 ** Return the value of the 'auto-vacuum' property. If auto-vacuum is
 ** enabled 1 is returned. Otherwise 0.
 */
int sqlite3BtreeGetAutoVacuum(Btree *pBt)
{
    /* no autovacuum */
    int rc = 0;
    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                "GetAutoVacuum(pBt %d)        = %s\n", pBt->btreeid,
                rc ? "yes" : "no");
    return rc;
}

/*
** Temp tables were not designed to be shareable.
** Use this lock for synchoronizing access to shared
** temp table between Lua threads.
*/
static __thread pthread_mutex_t *tmptbl_lk = NULL;
void comdb2_set_tmptbl_lk(pthread_mutex_t *lk)
{
    tmptbl_lk = lk;
}

/*
 ** Create a new BTree table.  Write into *piTable the page
 ** number for the root page of the new table.
 **
 ** The type of type is determined by the flags parameter.  Only the
 ** following values of flags are currently in use.  Other values for
 ** flags might not work:
 **
 **     BTREE_INTKEY                    Used for SQL tables with rowid keys
 **     BTREE_BLOBKEY                   Used for SQL indices
 */
int sqlite3BtreeCreateTable(Btree *pBt, int *piTable, int flags)
{
    int bdberr = 0;
    int rc = SQLITE_OK;
    struct sql_thread *thd;
    if ((thd = pthread_getspecific(query_info_key)) == NULL) {
        rc = SQLITE_INTERNAL;
        logmsg(LOGMSG_ERROR, "%s rc: %d\n", __func__, rc);
        return rc;
    }
    if (!pBt->is_temporary) { /* must go through comdb2 to do this */
        rc = UNIMPLEMENTED;
        logmsg(LOGMSG_ERROR, "%s rc: %d\n", __func__, rc);
        goto done;
    }
    if (!thd->clnt->limits.temptables_ok) {
        rc = SQLITE_NO_TEMPTABLES;
        goto done;
    }
    if (pBt->temp_tables == NULL) {
        pBt->temp_tables = hash_init_i4(offsetof(struct temptable, rootpage));
    }
    struct temptable *pNewTbl = calloc(1, sizeof(struct temptable));
    if (pNewTbl == NULL) {
        logmsg(LOGMSG_ERROR, "%s: calloc(%zu) failed\n", __func__,
               sizeof(struct temptable));
        rc = SQLITE_NOMEM;
        goto done;
    }
    pNewTbl->owner = pBt;
    pNewTbl->flags = flags;
    pNewTbl->lk = tmptbl_lk;
    pNewTbl->rootpage = ++pBt->num_temp_tables;
    if (pBt->is_hashtable) {
        pNewTbl->tbl = bdb_temp_hashtable_create(thedb->bdb_env, &bdberr);
        if (pNewTbl->tbl != NULL) ATOMIC_ADD32(gbl_sql_temptable_count, 1);
    } else if (tmptbl_clone) {
        pNewTbl->lk = tmptbl_clone->lk;
        pNewTbl->tbl = tmptbl_clone->tbl;
        pNewTbl->owner = tmptbl_clone->owner;
    } else {
        pNewTbl->tbl = bdb_temp_table_create(thedb->bdb_env, &bdberr);
        if (pNewTbl->tbl != NULL) ATOMIC_ADD32(gbl_sql_temptable_count, 1);
    }
    if (pNewTbl->tbl == NULL) {
        --pBt->num_temp_tables;
        free(pNewTbl);
        logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_create failed: %d\n", __func__,
               bdberr);
        rc = SQLITE_INTERNAL;
        goto done;
    }
    hash_add(pBt->temp_tables, pNewTbl);
    thd->had_temptables = 1;
    *piTable = pNewTbl->rootpage;
done:
    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                "CreateTable(pBt %d, root %d, flags %d)      = %s\n",
                pBt->btreeid, *piTable, flags, sqlite3ErrStr(rc));
    return rc;
}

/*
 ** Read the meta-information out of a database file.  Meta[0]
 ** is the number of free pages currently in the database.  Meta[1]
 ** through meta[15] are available for use by higher layers.  Meta[0]
 ** is read-only, the others are read/write.
 **
 ** The schema layer numbers meta values differently.  At the schema
 ** layer (and the SetCookie and ReadCookie opcodes) the number of
 ** free pages is not visible.  So Cookie[0] is the same as Meta[1].
 */
#if 0
static u32 metadata[16] = {
   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};
#endif
void sqlite3BtreeGetMeta(Btree *pBt, int idx, u32 *pMeta)
{
    if (idx < 0 || idx > 15) {
        logmsg(LOGMSG_ERROR, "sqlite3BtreeGetMeta: unknown index idx = %d\n", idx);
        goto done;
    }

    /* Value of BTREE_SCHEMA_VERSION is 1.*/
    idx = idx - 1;

#if 0
   from main.c
      **
      Meta values are as follows:**meta[0] Schema cookie.
      Changes with each schema change. **
      meta[1] File format of schema layer. **
      meta[2] Size of the page cache. **
      meta[3] Use freelist if 0. Autovacuum if greater than zero. ** meta[4]
      Db text encoding.1:UTF - 8 3:UTF - 16 LE 4:UTF -
      16 BE ** meta[5] The user cookie.Used by the application.
#endif
    switch (idx) {
    case 0:
        /* This is my cookie.  There are many like it, but this one is mine */
        *pMeta = 0;
        break;
    case 1:
        *pMeta = 4;
        break;
    case 2:
        *pMeta = 4;
        break;
    case 3:
        *pMeta = 4;
        break;
    case 4:
        *pMeta = SQLITE_UTF8;
        break;
    default:
        *pMeta = 0;
        break;
    }

done:
    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                "GetMeta(pBt %d, idx %d, data %d)      = SQLITE_OK\n",
                pBt->btreeid, idx, *pMeta);
}

static int
cursor_find_remote(BtCursor *pCur,            /* The cursor to be moved */
                   struct sqlclntstate *clnt, /* clnt state, mostly for error */
                   UnpackedRecord *pIdxKey,   /* Unpacked index key */
                   i64 intKey,                /* The table key */
                   int bias,                  /* OP_Seek* ops and frieds */
                   int *pRes)                 /* Write search results here */
{
    Mem *key;
    Mem genidkey;
    int nfields;
    int rc = SQLITE_OK;

    /* check authentication */
    if (authenticate_cursor(pCur, AUTHENTICATE_READ) != 0)
        return SQLITE_ACCESS;

    assert(pCur->fdbc != NULL);
    if (pCur->ixnum == -1) {
        bzero(&genidkey, sizeof(genidkey));
        genidkey.u.i = intKey;
        genidkey.flags = MEM_Int;
        key = &genidkey;
        nfields = 1;
    } else {
        assert(pIdxKey != NULL);
        key = &pIdxKey->aMem[0];
        nfields = pIdxKey->nField;
    }

    if (pCur->ixnum == -1) {
        rc = pCur->fdbc->find(pCur, key, nfields, bias);
    } else {
        /* index, we have dups, are we looking for last ?*/
        if (pIdxKey->default_rc < 0) {
            rc = pCur->fdbc->find_last(pCur, key, nfields, bias);
        } else {
            rc = pCur->fdbc->find(pCur, key, nfields, bias);
        }
    }

    if (rc == IX_FND ||
        rc == IX_FNDMORE /*|| (pCur->ixnum >=0 && rc == IX_NOTFND)*/) {
        char *data;
        int datalen;

        pCur->fdbc->get_found_data(pCur, &pCur->genid, &datalen, &data);
        pCur->rrn = 2;

        if (pCur->ixnum == -1) {
            *pRes = 0; /* this is IX_FND */

            if (pCur->writeTransaction) {
                if (pCur->ondisk_buf) /* not allocated for deletes */
                {
                    memcpy(pCur->ondisk_buf, data, datalen);
                }
            } else {
                pCur->ondisk_buf = data;
            }
        } else {
            /*
               if (pCur->writeTransaction) {
               memcpy(pCur->keybuf, data, datalen);
               } else {
               pCur->fndkey = data;
               }
             */
            if (pCur->keybuf_alloc < datalen) {
                pCur->keybuf = realloc(pCur->keybuf, datalen);
                if (!pCur->keybuf) {
                    logmsg(LOGMSG_ERROR, "%s: failed malloc %d bytes\n", __func__,
                            datalen);
                    return -1;
                }
                pCur->keybuf_alloc = datalen;
            }
            memcpy(pCur->keybuf, data, datalen);
            pCur->keybuflen = datalen;

/* NEED TO REDO THIS; the data sent by remote will be sqlite format */
#if 0
         abort();
         /* indexes are also converted to sqlite */
         pCur->lastkey = pCur->fndkey;
         rc = ondisk_to_sqlite_tz;
#endif
            *pRes = sqlite3VdbeRecordCompare(pCur->keybuflen, pCur->keybuf,
                                             pIdxKey);
        }

        if (rc == IX_FND /*last record */) {
            switch (bias) {
            case OP_SeekRowid:
            case OP_IfNoHope:
            case OP_NotFound:
            case OP_Found:
                pCur->next_is_eof = 1;
                break;
            case OP_SeekGT:
            case OP_SeekGE:
            case OP_DeferredSeek:
            case OP_NotExists:
                pCur->next_is_eof = 1;
                break;
            case OP_SeekLT:
            case OP_SeekLE:
                pCur->prev_is_eof = 1;
                break;
            default:
                logmsg(LOGMSG_ERROR, "%s: unsupported find rc=%d\n", __func__, bias);
                abort();
                break;
            }
        }
    } else if (rc == IX_EMPTY) {
        pCur->empty = 1;
        *pRes = -1;
        return SQLITE_OK;
    } else if (rc == IX_PASTEOF) {
        pCur->eof = 1;
        *pRes = -1;
        return SQLITE_OK;
    } else if (rc == SQLITE_SCHEMA_REMOTE) {
        clnt->osql.error_is_remote = 1;
        clnt->osql.xerr.errval = CDB2ERR_ASYNCERR;

        errstat_set_strf(&clnt->osql.xerr,
                         "schema change table \"%s\" from db \"%s\"",
                         pCur->fdbc->dbname(pCur), pCur->fdbc->tblname(pCur));

        fdb_clear_sqlite_cache(pCur->sqlite, pCur->fdbc->dbname(pCur),
                               pCur->fdbc->tblname(pCur));

        return SQLITE_SCHEMA_REMOTE;
    } else if (rc == FDB_ERR_FDB_VERSION) {
        /* lower level handles the retries here */
        abort();
    } else {
        *pRes = -1;
        assert(rc != 0);
        logmsg(LOGMSG_ERROR, "%s find bias=%d rc %d\n", __func__, bias, rc);
        return SQLITE_INTERNAL;
    }
    return SQLITE_OK;
}

static int bias_cmp(bias_info *info, void *found)
{
    BtCursor *cur = info->cur;
    ondisk_to_sqlite_tz(cur->db, cur->sc, found, cur->rrn, cur->genid,
                        cur->keybuf, cur->keybuf_alloc, 0, NULL, NULL, NULL,
                        &cur->keybuflen, cur->clnt->tzname, cur);
    return sqlite3VdbeRecordCompare(cur->keybuflen, cur->keybuf,
                                    info->unpacked);
}

/* Move the cursor so that it points to an entry near the key
** specified by pIdxKey or intKey.   Return a success code.
**
** For INTKEY tables, the intKey parameter is used.  pIdxKey
** must be NULL.  For index tables, pIdxKey is used and intKey
** is ignored.
**
** If an exact match is not found, then the cursor is always
** left pointing at a leaf page which would hold the entry if it
** were present.  The cursor might point to an entry that comes
** before or after the key.
**
** An integer is written into *pRes which is the result of
** comparing the key with the entry to which the cursor is
** pointing.  The meaning of the integer written into
** *pRes is as follows:
**
**     *pRes<0      The cursor is left pointing at an entry that
**                  is smaller than intKey/pIdxKey or if the table is empty
**                  and the cursor is therefore left point to nothing.
**
**     *pRes==0     The cursor is left pointing at an entry that
**                  exactly matches intKey/pIdxKey.
**
**     *pRes>0      The cursor is left pointing at an entry that
**                  is larger than intKey/pIdxKey.
**
*/
int sqlite3BtreeMovetoUnpacked(BtCursor *pCur, /* The cursor to be moved */
                               UnpackedRecord *pIdxKey, /* Unpacked index key */
                               i64 intKey,              /* The table key */
                               int bias, /* used to detect the vdbe operation */
                               int *pRes) /* Write search results here */
{
    int rc = SQLITE_OK;
    void *buf = NULL;
    int fndlen;
    int bdberr;
    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = pCur->clnt;
    unsigned long long genid;
    int verify = 0;

    if (debug_switch_pause_moveto()) {
        logmsg(LOGMSG_USER, "Waiting 15 sec\n");
        poll(NULL, 0, 15000);
    }

    /* verification error if not found */
    if (gbl_early_verify &&
        (bias == OP_NotExists || bias == OP_SeekRowid || bias == OP_NotFound ||
         bias == OP_IfNoHope) &&
        *pRes != 0) {
        verify = 1;
        *pRes = 0;
    }

    /* check authentication */
    if (authenticate_cursor(pCur, AUTHENTICATE_READ) != 0) {
        rc = SQLITE_ACCESS;
        goto done;
    }

    /* we may move the cursor in a way that would invalidate any serialized
     * cursor we may have */
    bdb_cursor_ser_invalidate(&pCur->cur_ser);

    if (access_control_check_sql_read(pCur, thd)) {
        rc = SQLITE_ACCESS;
        goto done;
    }

    /* skip the moveto on databtree is marked for delete,
       this is a nop */
    /* for partial indexes,
       I need to move to check if I need to delete a partial index */
    if (likely(bdb_attr_get(thedb->bdb_attr, BDB_ATTR_ONE_PASS_DELETE)) && /* One-pass optimization is ON */
        (pCur->ixnum == -1) && (pCur->open_flags & OPFLAG_FORDELETE) && /* Data is open for deletion */
        (bias != OP_DeferredSeek) && /* Not a DeferredSeek or FinishSeek */
        (!gbl_partial_indexes || !pCur->db->ix_partial) /* Partial index is disabled or table does not have any */
        ) {
        rc = SQLITE_OK;
        goto done;
    }

    if (thd)
        thd->nfind++;

    thd->cost += pCur->find_cost;

    /* assert that this isn't called for sampled (previously misnamed
     * compressed) */
    assert(0 == pCur->is_sampled_idx);

    if (!is_sqlite_db_init(pCur)) {
        rc = sql_tick(thd, 0);
        if (rc)
            return rc;
    }

    pCur->nfind++;

    if (pCur->blobs.numcblobs > 0)
        free_blob_status_data(&pCur->blobs);

    pCur->eof = 0;
    pCur->empty = 0;
    pCur->next_is_eof = 0;
    pCur->prev_is_eof = 0;

    if (pCur->bt->is_temporary) {
        if (pIdxKey) {
            if (bdb_is_hashtable(pCur->tmptable->tbl)) {
                Mem mem = {{0}};
                sqlite3VdbeRecordPack(pIdxKey, &mem);
                rc = bdb_temp_table_find(thedb->bdb_env, pCur->tmptable->cursor,
                                         mem.z, mem.n, NULL, &bdberr);
                sqlite3VdbeMemRelease(&mem);
            } else {
                rc = pCur->cursor_find(thedb->bdb_env, pCur->tmptable->cursor,
                                       NULL, 0, pIdxKey, &bdberr, pCur);
            }
        } else {
            rc =
                pCur->cursor_find(thedb->bdb_env, pCur->tmptable->cursor,
                                  &intKey, sizeof(intKey), NULL, &bdberr, pCur);
        }
        if (!is_good_ix_find_rc(rc)) {
            if (rc == IX_EMPTY) {
                rc = SQLITE_OK;
                pCur->empty = 1;
                *pRes = -1;
                return rc;
            } else if (rc == IX_PASTEOF) {
                rc = SQLITE_OK;
                pCur->eof = 1;
                *pRes = -1;
                return rc;
            }
            logmsg(LOGMSG_ERROR, "sqlite3BtreeMoveto:bdb_temp_table_find error rc=%d\n", rc);
            rc = SQLITE_INTERNAL;
            goto done;
        }
        if (pIdxKey)
            *pRes = sqlite3VdbeRecordCompare(
                bdb_temp_table_keysize(pCur->tmptable->cursor),
                bdb_temp_table_key(pCur->tmptable->cursor), pIdxKey);
        else
            *pRes = i64cmp(bdb_temp_table_key(pCur->tmptable->cursor), &intKey);

    } else if (pCur->rootpage == RTPAGE_SQLITE_MASTER) {

        /* this is a find in a sqlite_master, ignore for now
        */
        if (pCur->keyDdl != intKey) {
            logmsg(LOGMSG_ERROR, 
                "%s: cached ddl row different than lookup row %llx %llx???\n",
                __func__, pCur->genid, intKey);
        }
        pCur->tblpos =
            thd->rootpage_nentries; /* position ourselves on the side row */
        *pRes = 0;
        goto done;

    } else if (cur_is_remote(pCur)) { /* remote cursor */

        /* filter the supported operations */
        if (bias != OP_SeekLT && bias != OP_SeekLE && bias != OP_SeekGE &&
            bias != OP_SeekGT && bias != OP_NotExists && bias != OP_Found &&
            bias != OP_IfNoHope && bias != OP_NotFound &&
            bias != OP_IdxDelete && bias != OP_SeekRowid && bias != OP_DeferredSeek) {
            logmsg(LOGMSG_ERROR, "%s: unsupported remote cursor operation op=%d\n",
                    __func__, bias);
            rc = SQLITE_INTERNAL;
        }
        /* hack for partial indexes */
        else if (bias == OP_IdxDelete && pCur->ixnum != -1) {
            /* hack for partial indexes and indexes on expressions */
            if (gbl_expressions_indexes && pCur->fdbc->tbl_has_expridx(pCur)) {
                Mem mem = {{0}};
                sqlite3VdbeRecordPack(pIdxKey, &mem);
                assert(clnt->idxDelete[pCur->ixnum] == NULL);
                clnt->idxDelete[pCur->ixnum] = malloc(sizeof(int) + mem.n);
                if (clnt->idxDelete[pCur->ixnum] == NULL) {
                    logmsg(LOGMSG_ERROR, "%s:%d malloc %zu failed\n", __func__,
                           __LINE__, sizeof(int) + mem.n);
                    rc = SQLITE_NOMEM;
                    goto done;
                }
                *((int *)clnt->idxDelete[pCur->ixnum]) = mem.n;
                memcpy((unsigned char *)clnt->idxDelete[pCur->ixnum] +
                           sizeof(int),
                       mem.z, mem.n);
                sqlite3VdbeMemRelease(&mem);
            }
            *pRes = 0;
            rc = SQLITE_OK;
        } else {
            rc = cursor_find_remote(pCur, clnt, pIdxKey, intKey, bias, pRes);
        }

    } else if (pCur->ixnum == -1) {
        /* Data. nKey has rrn */
        i64 nKey;
        uint8_t ver;

        /* helper simulate verify sql error -> deadlock */
        {
            static int deadlock_race = 0;
            if (debug_switch_simulate_verify_error()) {
                if (!deadlock_race) {
                    deadlock_race++;
                    while (debug_switch_simulate_verify_error()) {
                        poll(NULL, 0, 10);
                    }
                } else {
                    deadlock_race++;
                }
            }
            if (debug_switch_reset_deadlock_race()) {
                deadlock_race = 0;
            }
        }

        /* TODO: we already found the data record.  find some way to map between
         * index/data cursors and don't do extra data fetches unless we
         * move the cursor */
        pCur->eof = 0;
        pCur->empty = 0;
        reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                    "looking for record %lld\n", intKey);

        if (pCur->db->dtastripe) {
            genid = intKey;
            nKey = 2;
        } else {
            nKey = intKey;
            rc = get_matching_genid(pCur, (int)nKey, &genid);
            if (rc) {
                rc = SQLITE_OK;
                *pRes = -1;
                goto done;
            }
        }

#if 0
        if (sqldbgflag & 3)
            printf("# find %s rrn %lld ", pCur->db->tablename, nKey);
#endif

        if (is_genid_synthetic(genid)) {
            rc = osql_get_shadowdata(pCur, genid, &buf, &fndlen, &bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, 
                        "%s: error fetching shadow data for genid %llu\n",
                        __func__, genid);
            }
            pCur->rrn = 2;
            pCur->genid = genid;
        } else {
            rc = ddguard_bdb_cursor_find(thd, pCur, pCur->bdbcur, &genid,
                                         sizeof(genid), 0, bias, &bdberr);

            if ((pCur->ixnum == -1 && rc == IX_FND) ||
                (pCur->ixnum >= 0 && rc != IX_EMPTY && rc >= 0)) {
                /*
                   pCur->rrn = pCur->bdbcur->rrn(pCur->bdbcur);
                   pCur->genid = pCur->bdbcur->genid(pCur->bdbcur);
                   fndlen = pCur->bdbcur->datalen(pCur->bdbcur);
                   buf = pCur->bdbcur->data(pCur->bdbcur);
                   ver = pCur->bdbcur->ver(pCur->bdbcur);
                 */
                pCur->bdbcur->get_found_data(pCur->bdbcur, &pCur->rrn,
                                             &pCur->genid, &fndlen, &buf, &ver);
                vtag_to_ondisk(pCur->db, buf, &fndlen, ver, pCur->genid);
            }
        }

        if (!rc) {
            /* we need this??? */
            if (fndlen != getdatsize(pCur->db)) {
                logmsg(LOGMSG_ERROR, "sqlite3BtreeMoveto: incorrect fndlen %d\n", fndlen);
                rc = SQLITE_INTERNAL; /* YO, this is IX_NOTFND !!!! */
                goto done;
            }
            if (pCur->writeTransaction) {
                memcpy(pCur->ondisk_buf, buf, fndlen);
            } else {
                pCur->ondisk_buf = buf;
            }
        }

        if ((rc == IX_NOTFND) || (rc == IX_PASTEOF)) {
            rc = SQLITE_OK;
            *pRes = -1;
            pCur->eof = 1;
        } else if (rc == IX_EMPTY) {
            rc = SQLITE_OK;
            *pRes = -1; /* return next rrn? i don't think this matters */
            pCur->empty = 1;
        } else if (is_good_ix_find_rc(rc)) {
            *pRes = 0;
            if (unlikely(pCur->cursor_class == CURSORCLASS_STAT24)) {
                extract_stat_record(pCur->db, buf, pCur->dtabuf,
                                    &pCur->dtabuflen);
            } else {
                /* DTA FILE - DONT CONVERT */
                if (pCur->writeTransaction) {
                    memcpy(pCur->dtabuf, pCur->ondisk_buf,
                           getdatsize(pCur->db));
                } else {
                    pCur->dtabuf = pCur->ondisk_buf;
                }
            }
            pCur->sqlrrnlen =
                sqlite3PutVarint((unsigned char *)pCur->sqlrrn, pCur->rrn);

            if (!gbl_selectv_rangechk) {
                if ((rc == IX_FND || rc == IX_FNDMORE) && pCur->is_recording &&
                    thd->clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
                    rc = osql_record_genid(pCur, thd, pCur->genid);
                    if (rc) {
                        logmsg(LOGMSG_ERROR, 
                                "%s: failed to record genid %llx (%llu)\n",
                                __func__, pCur->genid, pCur->genid);
                    }
                }
            }
            rc = 0;

#if 0
         if (sqldbgflag & 3) {
            printf(" record=");
            printrecord(pCur->ondisk_buf, pCur->db->schema, 0);
         }
         if (sqldbgflag & 3)
            printf("\n");
#endif

        } else { /* ix_find_by_rrn_and_genid || bdb_cursor_find */

            /* HERE WE ACTUALLY CAN GET A DEADLOCK!!!!! 03142008dh */
            if (rc == BDBERR_DEADLOCK /*ix_find? */
                || bdberr == BDBERR_DEADLOCK /*cursor */) {

                logmsg(LOGMSG_INFO, "sqlite3BtreeMoveto: deadlock bdberr = %d\n", bdberr);
                rc = SQLITE_DEADLOCK;

            } else {
                if (bdberr == BDBERR_TRANTOOCOMPLEX) {
                    rc = SQLITE_TRANTOOCOMPLEX;
                } else if (bdberr == BDBERR_TRAN_CANCELLED) {
                    rc = SQLITE_TRAN_CANCELLED;
                } else if (bdberr == BDBERR_NO_LOG) {
                    rc = SQLITE_TRAN_NOLOG;
                } else if (bdberr == BDBERR_NOT_DURABLE) {
                    rc = SQLITE_CLIENT_CHANGENODE;
                } else {
                   logmsg(LOGMSG_ERROR, "sqlite3BtreeMoveto: error rc=%d bdberr = %d\n", rc,
                           bdberr);
                    rc = SQLITE_INTERNAL;
                }
            }

#if 0
         if (sqldbgflag & 3)
            printf(" no match\n");
#endif
        }
    } else { /* find by key */
        int ondisk_len;
        struct convert_failure *fail_reason = &thd->clnt->fail_reason;

        struct bias_info info = {.bias = bias,
                                 .truncated = 0,
                                 .cmp = bias_cmp,
                                 .cur = pCur,
                                 .unpacked = pIdxKey};

/* here, if we are a splinter, we update the key properly */
#if 0
        if(thd->crtshard >=  1) {
            struct db *db = thedb->dbs[pCur->tblnum];
            shard_limits_t*shards = db->shards[pCur->ixnum];

            /* if the key is before our start, update the search to start
               if the key is bigger than start if next shard (if any), return EOF
               otherwise use the actual key */
            if(thd->crtshard>1) {
                /*adjust left */
                if(sqlite3RecordCompareExprList(pIdxKey, 
                            &shards->mems[thd->crtshard-2]) < 0) {
                    /* replace pIdxKey with shards->mems */
                    /* THIS IS HACK! leaks and crashes
                    pIdxKey->aMem = shards->mems; */
                }
            }
            if(thd->crtshard-1 < shards->limits->nExpr) {
                /*adjust right */
                if(sqlite3RecordCompareExprList(pIdxKey, 
                            &shards->mems[thd->crtshard-1])>=0) {
                    /* the result is eof */
                }
            }
            
        }
#endif
        ondisk_len = rc =
            sqlite_unpacked_to_ondisk(pCur, pIdxKey, fail_reason, &info);
        if (rc < 0) {
            char errs[128];
            convert_failure_reason_str(&thd->clnt->fail_reason,
                                       pCur->db->tablename, "SQLite format",
                                       ".ONDISK", errs, sizeof(errs));
            reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                        "Moveto: sqlite_unpacked_to_ondisk failed [%s]\n",
                        errs);
            sqlite3_mutex_enter(sqlite3_db_mutex(pCur->vdbe->db));
            sqlite3VdbeError(pCur->vdbe, errs, (char *)0);
            sqlite3_mutex_leave(sqlite3_db_mutex(pCur->vdbe->db));
            rc = SQLITE_ERROR;
            goto done;
        }

        /* hack for partial indexes and indexes on expressions */
        if (bias == OP_IdxDelete && pCur->ixnum != -1) {
            if (gbl_expressions_indexes && pCur->db->ix_expr) {
                assert(clnt->idxDelete[pCur->ixnum] == NULL);
                clnt->idxDelete[pCur->ixnum] = malloc(ondisk_len);
                if (clnt->idxDelete[pCur->ixnum] == NULL) {
                    logmsg(LOGMSG_ERROR, "%s:%d malloc %d failed\n", __func__,
                            __LINE__, ondisk_len);
                    rc = SQLITE_NOMEM;
                    goto done;
                }
                memcpy(clnt->idxDelete[pCur->ixnum], pCur->ondisk_key,
                       ondisk_len);
            }
            *pRes = 0;
            rc = SQLITE_OK;
            goto done;
        }

        assert(ondisk_len >= 0);

        /* find last dup? */
        if (pIdxKey->default_rc < 0) {
            rc = ddguard_bdb_cursor_find_last_dup(
                thd, pCur, pCur->bdbcur, pCur->ondisk_key, ondisk_len,
                pCur->db->ix_keylen[pCur->ixnum], &info, &bdberr);
            if (is_good_ix_find_rc(rc)) {
                uint8_t _;
                pCur->bdbcur->get_found_data(pCur->bdbcur, &pCur->rrn,
                                             &pCur->genid, &fndlen, &buf, &_);
                if (fndlen != getkeysize(pCur->db, pCur->ixnum)) {
                    logmsg(LOGMSG_ERROR, "sqlite3BtreeMoveto: getkeysize return "
                           "incorrect length\n");
                    rc = SQLITE_INTERNAL;
                    goto done;
                }
                if (pCur->writeTransaction) {
                    memcpy(pCur->fndkey, buf, fndlen);
                } else {
                    pCur->fndkey = buf;
                }
            }
        } else {
            rc = ddguard_bdb_cursor_find(thd, pCur, pCur->bdbcur,
                                         pCur->ondisk_key, ondisk_len, 0, bias,
                                         &bdberr);
            if (is_good_ix_find_rc(rc)) {
                uint8_t _;
                pCur->bdbcur->get_found_data(pCur->bdbcur, &pCur->rrn,
                                             &pCur->genid, &fndlen, &buf, &_);
                if (fndlen != getkeysize(pCur->db, pCur->ixnum)) {
                    logmsg(LOGMSG_ERROR, "sqlite3BtreeMoveto: getkeysize return "
                           "incorrect length\n");
                    rc = SQLITE_INTERNAL;
                    goto done;
                }
                if (pCur->writeTransaction) {
                    memcpy(pCur->fndkey, buf, fndlen);
                } else {
                    pCur->fndkey = buf;
                }
            }
        }

        if (rc == IX_EMPTY) {
#if 0
            if (sqldbgflag & 3)
                printf(" no data\n");
#endif
            *pRes = -1;
            rc = SQLITE_OK;
            pCur->empty = 1;
            goto done;
        }

        /* Note: this code is to close a hole where we look for a value that's
         * larger than anything in the db, find something smaller, then larger
         * values
         * get added before we try to find the next matching record. There's a
         * corresponding problem in the 'go left' case if we try to find records
         * smaller than the smallest value in the db, but there's no cheap way
         * to
         * figure out of the record is the first record in the db. Serializable
         * papers over this with correct skip logic, but for now the left case
         * isn't
         * handled.
         */
        /* NOTE: last dup is returning EOF here :
         * table contains 1, 2, 3; select * from table where id>0 !
         */
        if (rc == IX_PASTEOF && !(pIdxKey->default_rc < 0)) {
            pCur->next_is_eof = 1;
        }

        if (is_good_ix_find_rc(rc)) {
            int goodrc = (rc == IX_FND || rc == IX_FNDMORE);
#if 0
         if (sqldbgflag & 3) {
            printf("found rrn %d key ", pCur->rrn);
            printrecord(pCur->fndkey, pCur->db->ixschema[pCur->ixnum], 0);
            printf(" data ");
            printrecord(pCur->ondisk_buf, pCur->db->schema, 0);
         }
#endif
            pCur->eof = 0;
            pCur->lastkey = pCur->fndkey;
            rc = ondisk_to_sqlite_tz(pCur->db, pCur->sc, pCur->fndkey,
                                     pCur->rrn, pCur->genid, pCur->keybuf,
                                     pCur->keybuf_alloc, 0, NULL, NULL, NULL,
                                     &pCur->keybuflen, clnt->tzname, pCur);
            if (rc) {
                reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                            "Moveto: ondisk_to_sqlite failed\n");
                logmsg(LOGMSG_ERROR, "Moveto: ondisk_to_sqlite failed\n");
                rc = SQLITE_INTERNAL;
                goto done;
            }

            if (!pCur->db->dtastripe) {
                genid_hash_add(pCur, pCur->rrn, pCur->genid);
            }

            if (!gbl_selectv_rangechk) {
                if (goodrc && pCur->is_recording &&
                    thd->clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
                    rc = osql_record_genid(pCur, thd, pCur->genid);
                    if (rc) {
                        logmsg(LOGMSG_ERROR, 
                                "%s: failed to record genid %llx (%llu)\n",
                                __func__, pCur->genid, pCur->genid);
                    }
                }
            }

            if (likely(info.truncated == 0)) {
                /* Comdb2 keys are memcmp'able. Lets put that to use.. */
                int cmplen = ondisk_len < fndlen ? ondisk_len : fndlen;
                *pRes = memcmp(pCur->fndkey, pCur->ondisk_key, cmplen);
            } else {
                /*
                ** Strings were truncated for find
                ** Compare found key with complete original search key
                */
                *pRes = sqlite3VdbeRecordCompare(pCur->keybuflen, pCur->keybuf,
                                                 pIdxKey);
            }
            if (*pRes == 0 && bias != OP_SeekGT) {
                *pRes = pIdxKey->default_rc;
            }
            rc = SQLITE_OK;
            goto done;
        } else { /* ix_find_xxx || bdb_cursor_find */

            /* HERE WE ACTUALLY CAN GET A DEADLOCK!!!!! 03142008dh */
            if (rc == BDBERR_DEADLOCK /*ix_find? */
                || bdberr == BDBERR_DEADLOCK /*cursor */) {

                if (bdberr == BDBERR_DEADLOCK) {
                    uuidstr_t us;
                    ctrace(
                        "%s: too much contention, retried %d times [%llx %s]\n",
                        __func__, gbl_move_deadlk_max_attempt,
                        (thd->clnt && thd->clnt->osql.rqid)
                            ? thd->clnt->osql.rqid
                            : 0,
                        (thd->clnt && thd->clnt->osql.rqid)
                            ? comdb2uuidstr(thd->clnt->osql.uuid, us)
                            : "invalid-uuid");
                }
                /*
                 * printf("sqlite3BtreeMoveto: deadlock bdberr = %d\n", bdberr);
                 */
                rc = SQLITE_DEADLOCK;
            } else {
                if (bdberr == BDBERR_TRANTOOCOMPLEX) {
                    rc = SQLITE_TRANTOOCOMPLEX;
                } else if (bdberr == BDBERR_TRAN_CANCELLED) {
                    rc = SQLITE_TRAN_CANCELLED;
                } else if (bdberr == BDBERR_NO_LOG) {
                    rc = SQLITE_TRAN_NOLOG;
                } else {
                    logmsg(LOGMSG_ERROR, "sqlite3BtreeMoveto: error rc=%d bdberr = %d\n", rc,
                           bdberr);
                    rc = SQLITE_INTERNAL;
                }
            }

            goto done;
        }
    }

done:
    /* early verification error */
    if (verify && !pCur->bt->is_temporary &&
        pCur->rootpage != RTPAGE_SQLITE_MASTER && *pRes != 0 &&
        pCur->vdbe->readOnly == 0 && pCur->ixnum == -1) {
        int irc = is_genid_recorded(thd, pCur, genid);
        if (irc < 0)
            logmsg(LOGMSG_ERROR, "%s: failed to check early verify genid\n",
                   __func__);
        else if (irc == 1)
            clnt->early_retry = EARLY_ERR_SELECTV;
        else
            clnt->early_retry = EARLY_ERR_VERIFY;
    }

    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                "Moveto(pCur %d, found %s)     = %s\n", pCur->cursorid,
                *pRes == 0 ? "yes" : *pRes < 0 ? "less" : "more",
                sqlite3ErrStr(rc));
    return rc;
}

/*
 ** For the entry that cursor pCur is point to, return as
 ** many bytes of the key or data as are available on the local
 ** b-tree page.  Write the number of available bytes into *pAmt.
 **
 ** The pointer returned is ephemeral.  The key/data may move
 ** or be destroyed on the next call to any Btree routine.
 **
 ** These routines is used to get quick access to key and data
 ** in the common case where no overflow pages are used.
 */
const void *sqlite3BtreeKeyFetch(BtCursor *pCur, u32 *pAmt)
{
    void *out = NULL;
    if (pCur->is_sampled_idx) {
        out = pCur->keybuf;
        *pAmt = pCur->keybuflen;
        goto done;
    }
    if (pCur->bt->is_temporary) {
        out = bdb_temp_table_key(pCur->tmptable->cursor);
        *pAmt = bdb_temp_table_keysize(pCur->tmptable->cursor);
        goto done;
    }
    out = pCur->keybuf;
    *pAmt = pCur->keybuflen;
done:
    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                "KeyFetch(pCur %d pAmt %d)      = %p\n", pCur->cursorid, *pAmt,
                out);
    reqlog_loghex(pCur->bt->reqlogger, REQL_TRACE, out, *pAmt);
    reqlog_logl(pCur->bt->reqlogger, REQL_TRACE, "\n");
    return out;
}

/*
** Read part of the payload for the row at which that cursor pCur is currently
** pointing.  "amt" bytes will be transferred into pBuf[].  The transfer
** begins at "offset".
**
** pCur can be pointing to either a table or an index b-tree.
** If pointing to a table btree, then the content section is read.  If
** pCur is pointing to an index b-tree then the key section is read.
**
** For sqlite3BtreePayload(), the caller must ensure that pCur is pointing
** to a valid row in the table.  For sqlite3BtreePayloadChecked(), the
** cursor might be invalid or might need to be restored before being read.
**
** Return SQLITE_OK on success or an error code if anything goes
** wrong.  An error is returned if "offset+amt" is larger than
** the available payload.
**
*******************************************************************************
** NOTE: Given the current architectures of SQLite (i.e. it only calls into
**       this function from one place) and comdb2 (i.e. it does not expose the
**       concept of overflow pages to SQLite), it seems highly unlikely this
**       function will be called; however, if there are changs to SQLite in the
**       future that call into this function from more places, that may change.
*******************************************************************************
*/
int sqlite3BtreePayload(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  int rc;
  const void *pSrc;
  u32 nSrc = 0;
  u64 nextOffset;

  pSrc = sqlite3BtreePayloadFetch(pCur, &nSrc);
  if( pSrc==0 ){
    rc = SQLITE_ERROR;
    goto done;
  }
  assert( nSrc>0 );
  nextOffset = (u64)offset + amt;
  if( nextOffset>nSrc ){
    rc = SQLITE_ERROR;
    goto done;
  }
  memcpy(pBuf, pSrc + offset, amt);
  rc = SQLITE_OK;

done:
  reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
              "Payload(pCur %d offset %d amt %d)     = %d\n", pCur->cursorid,
              offset, amt, rc);
  reqlog_loghex(pCur->bt->reqlogger, REQL_TRACE, pBuf, amt);
  reqlog_logl(pCur->bt->reqlogger, REQL_TRACE, "\n");
  return rc;
}

/*
** For the entry that cursor pCur is point to, return as
** many bytes of the key or data as are available on the local
** b-tree page.  Write the number of available bytes into *pAmt.
**
** The pointer returned is ephemeral.  The key/data may move
** or be destroyed on the next call to any Btree routine,
** including calls from other threads against the same cache.
** Hence, a mutex on the BtShared should be held prior to calling
** this routine.
**
** These routines is used to get quick access to key and data
** in the common case where no overflow pages are used.
*/
const void *sqlite3BtreePayloadFetch(BtCursor *pCur, u32 *pAmt)
{
  assert( pCur );
  if( pCur->ixnum==-1 ){
    return sqlite3BtreeDataFetch(pCur, pAmt);
  }else{
    return sqlite3BtreeKeyFetch(pCur, pAmt);
  }
}

/* add the costs of the sorter to the thd costs */
void addVdbeToThdCost(int type, int *data)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (thd == NULL)
        return;

    switch (type) {
      case VDBESORTER_WRITE:
        thd->cost += CDB2_TEMP_WRITE_COST;
        break;
      case VDBESORTER_FIND:
        thd->cost += CDB2_TEMP_FIND_COST;
        break;
      case VDBESORTER_MOVE:
        thd->cost += CDB2_TEMP_MOVE_COST;
        break;
    }

    ++(*data);
}

/* append the costs of the sorter to the thd query stats */
void addVdbeSorterCost(const VdbeSorter *pSorter)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (thd == NULL)
        return;

    struct query_path_component fnd = {{0}}, *qc;

    if (NULL == (qc = hash_find(thd->query_hash, &fnd))) {
        qc = calloc(sizeof(struct query_path_component), 1);
        hash_add(thd->query_hash, qc);
        listc_abl(&thd->query_stats, qc);
    }

    qc->nfind += pSorter->nfind;
    qc->nnext += pSorter->nmove;
    /* note: we record writes in record routines on the master */
    qc->nwrite += pSorter->nwrite;
}

/*
 ** Close a cursor.  The read lock on the database file is released
 ** when the last cursor is closed.
 */
int sqlite3BtreeCloseCursor(BtCursor *pCur)
{
    int rc = SQLITE_OK;
    int cursorid;
    int bdberr = 0;
    /* Not sure about this. Can pCur->thd be different from thread-specific */
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    /* If we are using sqldb of other thread, then the other thread will close
     * the db. */
    if (thd == NULL)
        return 0;
    struct sqlclntstate *clnt = thd->clnt;

    if (pCur->range) {
        if (pCur->range->idxnum == -1 && pCur->range->islocked == 0) {
            currange_free(pCur->range);
            pCur->range = NULL;
        } else if (pCur->range->islocked || pCur->range->lkey ||
                   pCur->range->rkey || pCur->range->lflag ||
                   pCur->range->rflag) {
            if (!pCur->is_recording ||
                (clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE &&
                 gbl_selectv_rangechk)) {
                CurRangeArr **append_to = (pCur->is_recording) ? &(clnt->selectv_arr) : &(clnt->arr);
                if (!*append_to) {
                    *append_to = malloc(sizeof(CurRangeArr));
                    currangearr_init(*append_to);
                }
                currangearr_append(*append_to, pCur->range);
            } else {
                currange_free(pCur->range);
            }
            pCur->range = NULL;
        } else {
            currange_free(pCur->range);
            pCur->range = NULL;
        }
    }

    if (pCur->blobs.numcblobs > 0)
        free_blob_status_data(&pCur->blobs);

    /* update cursor use counts.  don't lock for now.
     * analyze shouldnt' affect cursor stats */
    if (pCur->db && !clnt->is_analyze) {
        if (pCur->ixnum != -1)
            pCur->db->sqlixuse[pCur->ixnum] += (pCur->nfind + pCur->nmove);
    }

    if (thd && thd->query_hash) {
        if (pCur->cursor_class == CURSORCLASS_SQLITEMASTER ||
            (pCur->db && is_sqlite_stat(pCur->db->tablename))) {
            goto skip;
        }
        struct query_path_component fnd = {{0}}, *qc = NULL;
        if (pCur->bt && pCur->bt->is_remote) {
            if (!pCur->fdbc)
                goto skip; /* failed during cursor creation */
            strncpy0(fnd.rmt_db, pCur->fdbc->dbname(pCur), sizeof(fnd.rmt_db));
            strncpy0(fnd.lcl_tbl_name, pCur->fdbc->tblname(pCur),
                     sizeof(fnd.lcl_tbl_name));
        } else if (pCur->db) {
            strncpy0(fnd.lcl_tbl_name, pCur->db->tablename,
                     sizeof(fnd.lcl_tbl_name));
        }
        fnd.ix = pCur->ixnum;

        if ((qc = hash_find(thd->query_hash, &fnd)) == NULL) {
            qc = calloc(sizeof(struct query_path_component), 1);
            if (pCur->bt && pCur->bt->is_remote) {
                strncpy0(qc->rmt_db, pCur->fdbc->dbname(pCur),
                         sizeof(qc->rmt_db));
                strncpy0(qc->lcl_tbl_name, pCur->fdbc->tblname(pCur),
                         sizeof(qc->lcl_tbl_name));
            } else if (pCur->db) {
                strncpy0(qc->lcl_tbl_name, pCur->db->tablename,
                         sizeof(qc->lcl_tbl_name));
            }
            qc->ix = pCur->ixnum;
            hash_add(thd->query_hash, qc);
            listc_abl(&thd->query_stats, qc);
        }

        if (qc) {
            qc->nfind += pCur->nfind;
            qc->nnext += pCur->nmove;
            /* note: we record writes in record routines on the master */
            qc->nwrite += pCur->nwrite;
            qc->nblobs += pCur->nblobs;
        }
    }

skip:
    cursorid = pCur->cursorid;
    if (pCur->bt && pCur->bt->is_remote &&
        pCur->rootpage != RTPAGE_SQLITE_MASTER) /* sqlite_master is local */
    {
        /* release the fdb cursor */
        if (pCur->fdbc) {
            /* free memory allocated in cursor_move_remote(). */
            if (pCur->writeTransaction)
                free(pCur->dtabuf);
            free(pCur->keybuf);

            rc = pCur->fdbc->close(pCur);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: failed fdb_cursor_close rc=%d\n", __func__,
                        rc);
                rc = SQLITE_INTERNAL;
            }
        }
    } else {
        if (pCur->rootpage >= RTPAGE_START) {
            if (pCur->writeTransaction) {
                free(pCur->ondisk_buf);
                free(pCur->fndkey);
            }
            free(pCur->ondisk_key);
        }
        if (pCur->writeTransaction) {
            free(pCur->dtabuf);
        }
        free(pCur->keybuf);

        if (pCur->is_sampled_idx) {
            rc = sampler_close(pCur->sampler);
            pCur->sampler = NULL;
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_close_cursor rc %d\n",
                       __func__, bdberr);
                rc = SQLITE_INTERNAL;
                goto done;
            }
        } else if (pCur->bt && pCur->bt->is_temporary) {
            if( pCur->cursor_close ){
                rc = pCur->cursor_close(thedb->bdb_env, pCur, &bdberr);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "bdb_temp_table_close_cursor rc %d\n", bdberr);
                    rc = SQLITE_INTERNAL;
                    goto done;
                }
            }
            free(pCur->tmptable);
            pCur->tmptable = NULL;
        }

        if (pCur->bdbcur) {
            /* opened a real cursor? close it */
            bdberr = 0;
            rc = pCur->bdbcur->close(pCur->bdbcur, &bdberr);
            if (bdberr == BDBERR_DEADLOCK) {
                /* For now, We do not recover from deadlocks during cursor close
                 */
                rc = SQLITE_DEADLOCK;
            }
        }
        if (rc) {
            logmsg(LOGMSG_ERROR, "bdb_cursor_close: rc %d\n", bdberr);
            rc = SQLITE_INTERNAL;
            /*
             * Keep going, we do want to leak BtCursors when
             * we close a Btree!!!
             * goto done;
             */
        }
    }

    if (thd) {
        Pthread_mutex_lock(&thd->lk);
        if (pCur->on_list)
            listc_rfl(&pCur->bt->cursors, pCur);
        Pthread_mutex_unlock(&thd->lk);
    }

/* We don't allocate BtCursor anymore */
/* free(pCur); */
done:
    reqlog_logf(pCur->reqlogger, REQL_TRACE, "CloseCursor(pCur %d)      = %s\n",
                cursorid, sqlite3ErrStr(rc));

    return rc;
}

/*
 ** Delete all information from a single table in the database.  iTable is
 ** the page number of the root of the table.  After this routine returns,
 ** the root page is empty, but still exists.
 **
 ** This routine will fail with SQLITE_LOCKED if there are any open
 ** read cursors on the table.  Open write cursors are moved to the
 ** root of the table.
 */
int sqlite3BtreeClearTable(Btree *pBt, int iTable, int *pnChange)
{
    /* So here's Uncle Mike's lesson learned #6943925: if you have
     * a routine that "should never be called", make darn sure it's
     * never called... */
    int rc = SQLITE_OK;
    int bdberr;
    int ixnum;
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;

    if (pnChange)
        *pnChange = 0;

    if (pBt->is_temporary) {
        struct temptable *pTbl = hash_find(pBt->temp_tables, &iTable);
        if (pTbl == NULL) {
            logmsg(LOGMSG_ERROR, "%s: table %d not found\n",
                   __func__, iTable);
            rc = SQLITE_INTERNAL;
            goto done;
        }
        rc = bdb_temp_table_truncate(thedb->bdb_env, pTbl->tbl, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, 
                    "sqlite3BtreeClearTable: bdb_temp_table_clear error rc = %d\n",
                    rc);
            rc = SQLITE_INTERNAL;
            goto done;
        }
    } else {
        struct dbtable *db = get_sqlite_db(thd, iTable, &ixnum);
        if (ixnum != -1) {
            rc = SQLITE_OK;
            goto done;
        }
        /* If we are in analyze, lie.  Otherwise we end up with an empty,
         * and then worse, half-filled stat table during the analyze. */
        if (clnt->is_analyze && is_sqlite_stat(db->tablename)) {
            rc = SQLITE_OK;
            goto done;
        }
        rc = db->n_constraints == 0 ? osql_cleartable(thd, db->tablename) : -1;
        if (rc) {
            logmsg(LOGMSG_ERROR, "sqlite3BtreeClearTable: error rc = %d\n", rc);
            rc = SQLITE_INTERNAL;
            goto done;
        }
    }
done:
    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                "ClearTable(pBt %d iTable %d)      = %s\n", pBt->btreeid,
                iTable, sqlite3ErrStr(rc));
    return rc;
}

/*
 ** Write meta-information back into the database.  Meta[0] is
 ** read-only and may not be written.
 */
int sqlite3BtreeUpdateMeta(Btree *pBt, int idx, u32 iMeta)
{
    int rc = SQLITE_OK;
    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                "UpdateMeta(pBt %d, idx %d, data %d)      = %s\n", pBt->btreeid,
                idx, iMeta, sqlite3ErrStr(rc));
    return rc;
}

unsigned long long get_rowid(BtCursor *pCur)
{
    if (pCur->genid == 0) {
        logmsg(LOGMSG_FATAL, "get_rowid will return 0\n");
        exit(1);
    }

    return pCur->genid;
}

/*
 ** ADDON:
 ** this function is also called for recom/snapisol/serial
 ** in this case, if the genid is synthetic, it needs to pull the blobs from
 ** the SHADOW BLOB; we do this here
 */
static int fetch_blob_into_sqlite_mem(BtCursor *pCur, struct schema *sc,
                                      int fnum, Mem *m, void *dta)
{
    struct ireq iq;
    blob_status_t blobs;
    int blobnum;
    struct field *f;
    int rc;
    int bdberr;
    int nretries = 0;
    struct sql_thread *thd = pCur->thd;
    struct schema *pd = NULL;

    if (sc->flags & SCHEMA_PARTIALDATACOPY_ACTUAL) {
        pd = sc;
    }

    if (!pCur->have_blob_descriptor) {
        gather_blob_data_byname(pCur->db, ".ONDISK",
                                &pCur->blob_descriptor, pd);
        pCur->have_blob_descriptor = 1;
    }

    f = &sc->member[fnum];
    blobnum = f->blob_index + 1;

    pCur->nblobs++;
    if (thd) {
        thd->cost += pCur->blob_cost;
    }

again:
    memcpy(&blobs, &pCur->blob_descriptor, sizeof(blobs));

    if (is_genid_synthetic(pCur->genid)) {
        rc = osql_fetch_shadblobs_by_genid(pCur, &blobnum, &blobs, &bdberr);
    } else {
        bdb_fetch_args_t args = {0};
        /* Tell bdb to use sqlite's malloc routines. */
        args.fn_malloc = sqlite3GlobalConfig.m.xMalloc;
        args.fn_free = sqlite3GlobalConfig.m.xFree;
        rc = bdb_fetch_blobs_by_rrn_and_genid_cursor(
            pCur->db->handle, pCur->rrn, pCur->genid, 1, &blobnum,
            blobs.bloblens, blobs.bloboffs, (void **)blobs.blobptrs,
            pCur->bdbcur, &args, &bdberr);
    }

    if (rc) {
        if (bdberr == BDBERR_DEADLOCK) {
            nretries++;
            if ((rc = recover_deadlock(thedb->bdb_env, thd, NULL, 0)) != 0) {
                if (!gbl_rowlocks)
                    logmsg(LOGMSG_ERROR, "%s: %p failed dd recovery, rc %d\n",
                           __func__, (void *)pthread_self(), rc);
                if (rc < 0)
                    return SQLITE_BUSY;
                else
                    return rc;
            }
            if (nretries >= gbl_maxretries) {
                logmsg(LOGMSG_ERROR, "too much contention fetching "
                                     "tbl %s blob %s tried %d times\n",
                       pCur->db->tablename, f->name, nretries);
                return SQLITE_DEADLOCK;
            }
            goto again;
        }
        return SQLITE_DEADLOCK;
    }

    /* Happens more frequently in index mode, but can happen in cursor mode
     * after a deadlock (because we close all our cursors) */
    init_fake_ireq(thedb, &iq);
    iq.usedb = pCur->db;

    if (check_one_blob_consistency(&iq, iq.usedb, ".ONDISK", &blobs,
                                   dta, f->blob_index, 0, pd)) {
        free_blob_status_data(&blobs);
        nretries++;
        if (nretries >= gbl_maxblobretries) {
            logmsg(LOGMSG_ERROR, "inconsistent blob genid %llx, blob index %d\n",
                    pCur->genid, f->blob_index);
            return SQLITE_CORRUPT;
        }
        goto again;
    }

#if 0 
   int patch = 0;
   if (is_genid_synthetic(pCur->genid)) 
   {
      patch = blobnum-1;
   }
#endif

    if (blobs.blobptrs[0] == NULL) {
        m->z = NULL;
        m->flags = MEM_Null;
    } else {
        m->zMalloc = m->z = blobs.blobptrs[0];
        m->szMalloc = m->n = blobs.bloblens[0];

        if (f->type == SERVER_VUTF8) {
            /* Already nul-terminated. */
            m->flags = (MEM_Term | MEM_Str);
            if (m->n > 0)
                --m->n; /* sqlite string lengths do not include NULL */
        } else
            m->flags = MEM_Blob;
    }

    return 0;
}

int is_datacopy(BtCursor *pCur, int *fnum)
{
    int nmembers = pCur->sc->nmembers;
    /*if (nmembers == 0) return 0;*/
    if (pCur->ixnum >= 0 && pCur->db->ix_datacopy[pCur->ixnum] &&
        *fnum >= nmembers) {
        /* Make fnum point to correct column as if rec is .ONDISK */
        *fnum = pCur->sc->datacopy[*fnum - nmembers];
        return 1;
    }
    return 0;
}

int get_data(BtCursor *pCur, struct schema *sc, uint8_t *in, int fnum, Mem *m,
             uint8_t flip_orig, const char *tzname)
{
    int null;
    i64 ival = 0;
    int outdtsz = 0;
    int rc = 0;
    struct field *f = &(sc->member[fnum]);
    void *record = in;
    uint8_t *in_orig = in = in + f->offset;

    if (f->flags & INDEX_DESCEND) {
        if (gbl_sort_nulls_correctly) {
            in_orig[0] = ~in_orig[0];
        }
        if (flip_orig) {
            xorbufcpy((char *)&in[1], (char *)&in[1], f->len - 1);
        } else {
            switch (f->type) {
            case SERVER_BINT:
            case SERVER_UINT:
            case SERVER_BREAL:
            case SERVER_DATETIME:
            case SERVER_DATETIMEUS:
            case SERVER_INTVYM:
            case SERVER_INTVDS:
            case SERVER_INTVDSUS:
            case SERVER_DECIMAL: {
                /* This way we don't have to flip them back. */
                uint8_t *p = alloca(f->len);
                p[0] = in[0];
                xorbufcpy((char *)&p[1], (char *)&in[1], f->len - 1);
                in = p;
                break;
            }
            default:
                /* For byte and cstring, set the MEM_Xor flag and
                 * sqlite will flip the field */
                break;
            }
        }
    }

    null = stype_is_null(in);
    if (null) { /* this field is null, we dont need to fetch */
        m->z = NULL;
        m->n = 0;
        m->flags = MEM_Null;
        goto done;
    }

#ifdef _LINUX_SOURCE
    struct field_conv_opts convopts = {.flags = FLD_CONV_LENDIAN};
#else
    struct field_conv_opts convopts = {.flags = 0};
#endif

    switch (f->type) {
    case SERVER_UINT:
        rc = SERVER_UINT_to_CLIENT_INT(
            in, f->len, NULL /*convopts */, NULL /*blob */, &ival, sizeof(ival),
            &null, &outdtsz, &convopts, NULL /*blob */);
        m->u.i = ival;
        if (rc == -1)
            goto done;
        if (null)
            m->flags = MEM_Null;
        else
            m->flags = MEM_Int;
        break;

    case SERVER_BINT:
        rc = SERVER_BINT_to_CLIENT_INT(
            in, f->len, NULL /*convopts */, NULL /*blob */, &ival, sizeof(ival),
            &null, &outdtsz, &convopts, NULL /*blob */);
        m->u.i = ival;
        if (rc == -1)
            goto done;
        if (null) {
            m->flags = MEM_Null;
        } else {
            m->flags = MEM_Int;
        }
        break;

    case SERVER_BREAL: {
        double dval = 0;
        rc = SERVER_BREAL_to_CLIENT_REAL(
            in, f->len, NULL /*convopts */, NULL /*blob */, &dval, sizeof(dval),
            &null, &outdtsz, &convopts, NULL /*blob */);
        m->u.r = dval;
        if (rc == -1)
            goto done;
        if (null)
            m->flags = MEM_Null;
        else
            m->flags = MEM_Real;
        break;
    }
    case SERVER_BCSTR:
        /* point directly at the ondisk string */
        m->z = (char *)&in[1]; /* skip header byte in front */
        if (flip_orig || !(f->flags & INDEX_DESCEND)) {
            m->n = cstrlenlim((char *)&in[1], f->len - 1);
        } else {
            m->n = cstrlenlimflipped(&in[1], f->len - 1);
        }
        m->flags = MEM_Str | MEM_Ephem;
        break;

    case SERVER_BYTEARRAY:
        /* just point to bytearray directly */
        m->z = (char *)&in[1];
        m->n = f->len - 1;
        m->flags = MEM_Blob | MEM_Ephem;
        break;

    case SERVER_DATETIME:
        if (debug_switch_support_datetimes()) {
            assert(sizeof(server_datetime_t) == f->len);

            db_time_t sec = 0;
            unsigned short msec = 0;

            bzero(&m->du.dt, sizeof(dttz_t));

            /* TMP BROKEN DATETIME */
            if (in[0] == 0) {
                memcpy(&sec, &in[1], sizeof(sec));
                memcpy(&msec, &in[1] + sizeof(db_time_t), sizeof(msec));
                sec = flibc_ntohll(sec);
                msec = ntohs(msec);
                m->du.dt.dttz_sec = sec;
                m->du.dt.dttz_frac = msec;
                m->du.dt.dttz_prec = DTTZ_PREC_MSEC;
                m->flags = MEM_Datetime;
                m->tz = (char *)tzname;
            } else {
                rc = SERVER_BINT_to_CLIENT_INT(
                    in, sizeof(db_time_t) + 1, NULL /*convopts */,
                    NULL /*blob */, &(m->du.dt.dttz_sec),
                    sizeof(m->du.dt.dttz_sec), &null, &outdtsz, &convopts,
                    NULL /*blob */);
                if (rc == -1)
                    goto done;

                memcpy(&msec, &in[1] + sizeof(db_time_t), sizeof(msec));
                msec = ntohs(msec);
                m->du.dt.dttz_frac = msec;
                m->du.dt.dttz_prec = DTTZ_PREC_MSEC;
                m->flags = MEM_Datetime;
                m->tz = (char *)tzname;
            }

        } else {
            /* previous broken case, treat as bytearay */
            m->z = (char *)&in[1];
            m->n = f->len - 1;
            if (stype_is_null(in))
                m->flags = MEM_Null;
            else
                m->flags = MEM_Blob;
        }
        break;

    case SERVER_DATETIMEUS:
        if (debug_switch_support_datetimes()) {
            assert(sizeof(server_datetimeus_t) == f->len);
            if (stype_is_null(in)) {
                m->flags = MEM_Null;
            } else {

                db_time_t sec = 0;
                unsigned int usec = 0;

                bzero(&m->du.dt, sizeof(dttz_t));

                /* TMP BROKEN DATETIME */
                if (in[0] == 0) {

                    /* HERE COMES A BROKEN RECORD */
                    memcpy(&sec, &in[1], sizeof(sec));
                    memcpy(&usec, &in[1] + sizeof(db_time_t), sizeof(usec));
                    sec = flibc_ntohll(sec);
                    usec = ntohl(usec);
                    m->du.dt.dttz_sec = sec;
                    m->du.dt.dttz_frac = usec;
                    m->du.dt.dttz_prec = DTTZ_PREC_USEC;
                    m->flags = MEM_Datetime;
                    m->tz = (char *)tzname;
                } else {
                    rc = SERVER_BINT_to_CLIENT_INT(
                        in, sizeof(db_time_t) + 1, NULL /*convopts */,
                        NULL /*blob */, &(m->du.dt.dttz_sec),
                        sizeof(m->du.dt.dttz_sec), &null, &outdtsz, &convopts,
                        NULL /*blob */);
                    if (rc == -1)
                        goto done;

                    memcpy(&usec, &in[1] + sizeof(db_time_t), sizeof(usec));
                    usec = ntohl(usec);
                    m->du.dt.dttz_frac = usec;
                    m->du.dt.dttz_prec = DTTZ_PREC_USEC;
                    m->flags = MEM_Datetime;
                    m->tz = (char *)tzname;
                }
            }
        } else {
            /* previous broken case, treat as bytearay */
            m->z = (char *)&in[1];
            m->n = f->len - 1;
            m->flags = MEM_Blob;
        }
        break;

    case SERVER_INTVYM: {
        cdb2_client_intv_ym_t ym;
        rc = SERVER_INTVYM_to_CLIENT_INTVYM(in, f->len, NULL, NULL, &ym,
                                            sizeof(ym), &null, &outdtsz, NULL,
                                            NULL);
        if (rc)
            goto done;

        if (null) {
            m->flags = MEM_Null;
        } else {
            m->flags = MEM_Interval;
            m->du.tv.type = INTV_YM_TYPE;
            m->du.tv.sign = ntohl(ym.sign);
            m->du.tv.u.ym.years = ntohl(ym.years);
            m->du.tv.u.ym.months = ntohl(ym.months);
        }
        break;
    }

    case SERVER_INTVDS: {
        cdb2_client_intv_ds_t ds;

        rc = SERVER_INTVDS_to_CLIENT_INTVDS(in, f->len, NULL, NULL, &ds,
                                            sizeof(ds), &null, &outdtsz, NULL,
                                            NULL);
        if (rc)
            goto done;

        if (null)
            m->flags = MEM_Null;
        else {

            m->flags = MEM_Interval;
            m->du.tv.type = INTV_DS_TYPE;
            m->du.tv.sign = ntohl(ds.sign);
            m->du.tv.u.ds.days = ntohl(ds.days);
            m->du.tv.u.ds.hours = ntohl(ds.hours);
            m->du.tv.u.ds.mins = ntohl(ds.mins);
            m->du.tv.u.ds.sec = ntohl(ds.sec);
            m->du.tv.u.ds.frac = ntohl(ds.msec);
            m->du.tv.u.ds.prec = DTTZ_PREC_MSEC;
        }
        break;
    }

    case SERVER_INTVDSUS: {
        cdb2_client_intv_dsus_t ds;

        rc = SERVER_INTVDSUS_to_CLIENT_INTVDSUS(in, f->len, NULL, NULL, &ds,
                                                sizeof(ds), &null, &outdtsz,
                                                NULL, NULL);
        if (rc)
            goto done;

        if (null)
            m->flags = MEM_Null;
        else {
            m->flags = MEM_Interval;
            m->du.tv.type = INTV_DSUS_TYPE;
            m->du.tv.sign = ntohl(ds.sign);
            m->du.tv.u.ds.days = ntohl(ds.days);
            m->du.tv.u.ds.hours = ntohl(ds.hours);
            m->du.tv.u.ds.mins = ntohl(ds.mins);
            m->du.tv.u.ds.sec = ntohl(ds.sec);
            m->du.tv.u.ds.frac = ntohl(ds.usec);
            m->du.tv.u.ds.prec = DTTZ_PREC_USEC;
        }
        break;
    }

    case SERVER_BLOB2: {
        int len;
        /* get the length of the vutf8 string */
        memcpy(&len, &in[1], 4);
        len = ntohl(len);

        /* TODO use types.c's enum for header length */
        /* if the string is small enough to be stored in the record */
        if (len <= f->len - 5) {
            /* point directly at the ondisk string */
            /* TODO use types.c's enum for header length */
            m->z = (char *)&in[5];

            /* m->n is the blob length */
            m->n = (len > 0) ? len : 0;

            /*fprintf(stderr, "m->n = %d\n", m->n); */
            m->flags = MEM_Blob;
        } else
            rc = fetch_blob_into_sqlite_mem(pCur, sc, fnum, m, record);

        break;
    }

    case SERVER_VUTF8: {
        int len;
        /* get the length of the vutf8 string */
        memcpy(&len, &in[1], 4);
        len = ntohl(len);

        /* TODO use types.c's enum for header length */
        /* if the string is small enough to be stored in the record */
        if (len <= f->len - 5) {
            /* point directly at the ondisk string */
            /* TODO use types.c's enum for header length */
            m->z = (char *)&in[5];

            /* sqlite string lengths do not include NULL */
            m->n = (len > 0) ? len - 1 : 0;

            /*fprintf(stderr, "m->n = %d\n", m->n); */
            m->flags = MEM_Str | MEM_Ephem;
        } else
            rc = fetch_blob_into_sqlite_mem(pCur, sc, fnum, m, record);
        break;
    }

    case SERVER_BLOB: {
        int len;
        memcpy(&len, &in[1], 4);
        len = ntohl(len);
        if (stype_is_null(in)) {
            m->flags = MEM_Null;
        } else if (len == 0) {
            /* this blob is zerolen, we should'nt need to fetch*/
            m->z = NULL;
            m->flags = MEM_Blob;
            m->n = 0;
        } else {
            rc = fetch_blob_into_sqlite_mem(pCur, sc, fnum, m, record);
        }
        break;
    }
    case SERVER_DECIMAL:
        m->flags = MEM_Interval;
        m->du.tv.type = INTV_DECIMAL_TYPE;
        m->du.tv.sign = 0;

        /* if this is an index, try to extract the quantum */
        if (pCur->ixnum >= 0 && pCur->db->ix_collattr[pCur->ixnum] > 0) {
            char *payload;
            int payloadsz;
            short ch;
            int sign;
            char *new_in; /* we need to preserve the original key from
                             quantums, if any */

            new_in = alloca(f->len);
            memcpy(new_in, in, f->len);

            sign = -1;

            if (bdb_attr_get(thedb->bdb_attr,
                             BDB_ATTR_REPORT_DECIMAL_CONVERSION)) {
                logmsg(LOGMSG_USER, "Dec set quantum IN:\n");
                hexdump(LOGMSG_USER, new_in, f->len);
                logmsg(LOGMSG_USER, "\n");
            }

            if (pCur->bdbcur) {
                payload = pCur->bdbcur->collattr(pCur->bdbcur);
                payloadsz = pCur->bdbcur->collattrlen(pCur->bdbcur);

                if (payload && payloadsz > 0) {
                    ch = field_decimal_quantum(
                        pCur->db, pCur->db->ixschema[pCur->ixnum], fnum,
                        payload, payloadsz,
                        ((4 * pCur->db->ix_collattr[pCur->ixnum]) == payloadsz)
                            ? &sign
                            : NULL);

                    decimal_quantum_set(new_in, f->len, &ch,
                                        (sign == -1) ? NULL : &sign);
                } else {
                    decimal_quantum_set(new_in, f->len, NULL, NULL);
                }

            } else {
                /* This code path is only hit for analyze (or I suppose if
                 * anyone tries
                 * the no cursor setting again).  Choose an arbitrary scale. */
                decimal_quantum_set(new_in, f->len, NULL, NULL);
            }

            if (bdb_attr_get(thedb->bdb_attr,
                             BDB_ATTR_REPORT_DECIMAL_CONVERSION)) {
                logmsg(LOGMSG_USER, "Dec set quantum OUT:\n");
                hexdump(LOGMSG_USER, new_in, f->len);
                logmsg(LOGMSG_USER, "\n");
            }

            in = (unsigned char *)new_in;
        } else if (pCur->ixnum >= 0 && pCur->db->ix_datacopy[pCur->ixnum] && f->idx != -1) {
            // if f->idx == -1 then "in" is already the datacopy record
            struct field *fidx = &(pCur->db->schema->member[f->idx]);
            assert(f->len == fidx->len);
            in = pCur->bdbcur->datacopy(pCur->bdbcur) + fidx->offset;
        }

        decimal_ondisk_to_sqlite(in, f->len, (decQuad *)&m->du.tv.u.dec, &null);

        break;
    default:
        logmsg(LOGMSG_ERROR, "get_data_int: unhandled type %d\n", f->type);
        break;
    }

done:
    if (flip_orig)
        return rc;

    if (f->flags & INDEX_DESCEND) {
        if (gbl_sort_nulls_correctly) {
            in_orig[0] = ~in_orig[0];
        }
        switch (f->type) {
        case SERVER_BCSTR:
        case SERVER_BYTEARRAY:
            m->flags |= MEM_Xor;
            break;
        }
    }

    return rc;
}

int get_datacopy(BtCursor *pCur, int fnum, Mem *m)
{
    uint8_t *in;
    struct schema *s;

    s = pCur->db->schema;
    in = pCur->bdbcur->datacopy(pCur->bdbcur);
    if (s->ix[pCur->ixnum]->flags & SCHEMA_PARTIALDATACOPY) {
        s = s->ix[pCur->ixnum]->partial_datacopy;
    } else if (!is_genid_synthetic(pCur->genid)) {
        uint8_t ver = pCur->bdbcur->ver(pCur->bdbcur);
        vtag_to_ondisk_vermap(pCur->db, in, NULL, ver);
    }

    return get_data(pCur, s, in, fnum, m, 0, pCur->clnt->tzname);
}

static int
sqlite3BtreeCursor_analyze(Btree *pBt,      /* The btree */
                           int iTable,      /* Root page of table to open */
                           int wrFlag,      /* 1 to write. 0 read-only */
                           xCmpPacked xCmp, /* Key Comparison func */
                           void *pArg,      /* First arg to xCompare() */
                           BtCursor *cur,   /* Write new cursor here */
                           struct sql_thread *thd)
{
    int key_size;
    int sz;
    struct sqlclntstate *clnt = thd->clnt;
    struct dbtable *db;

    assert(iTable >= RTPAGE_START);
    /* INVALID: assert(iTable < (thd->rootpage_nentries + RTPAGE_START)); */

    db = get_sqlite_db(thd, iTable, &cur->ixnum);

    cur->cursor_class = CURSORCLASS_TEMPTABLE;
    cur->cursor_move = cursor_move_compressed;

    cur->sampler = analyze_get_sampler(clnt, db->tablename, cur->ixnum);

    cur->db = db;
    cur->tableversion = cur->db->tableversion;
    cur->sc = cur->db->ixschema[cur->ixnum];
    cur->rootpage = iTable;
    cur->bt = pBt;
    cur->is_sampled_idx = 1;
    cur->nCookFields = -1;

    key_size = getkeysize(cur->db, cur->ixnum);
    cur->ondisk_key = malloc(key_size + sizeof(int));
    if (!cur->ondisk_key) {
        logmsg(LOGMSG_ERROR, "%s:malloc ondisk_key sz %zu failed\n", __func__,
               key_size + sizeof(int));
        return SQLITE_INTERNAL;
    }
    cur->ondisk_keybuf_alloc = key_size;
    sz = schema_var_size(cur->sc);
    cur->keybuf = malloc(sz);
    if (!cur->keybuf) {
        logmsg(LOGMSG_ERROR, "%s: keybuf malloc %d failed\n", __func__, sz);
        free(cur->ondisk_key);
        return SQLITE_INTERNAL;
    }
    cur->lastkey = NULL;
    cur->keybuflen = sz;
    cur->keybuf_alloc = sz;

    return SQLITE_OK;
}

static int tmptbl_cursor_del(bdb_state_type *bdb_state, struct temp_cursor *cur,
                             int *bdberr, BtCursor *_)
{
    return bdb_temp_table_delete(bdb_state, cur, bdberr);
}

static int tmptbl_cursor_put(bdb_state_type *bdb_state, struct temp_table *tbl,
                             void *key, int keylen, void *data, int dtalen,
                             void *unpacked, int *bdberr, BtCursor *_)
{
    return bdb_temp_table_put(bdb_state, tbl, key, keylen, data, dtalen,
                              unpacked, bdberr);
}

static int tmptbl_cursor_close(bdb_state_type *bdb_state, BtCursor *btcursor,
                               int *bdberr)
{
    return bdb_temp_table_close_cursor(bdb_state, btcursor->tmptable->cursor,
                                       bdberr);
}

static int tmptbl_cursor_find(bdb_state_type *bdb_state,
                              struct temp_cursor *cur, const void *key,
                              int keylen, void *unpacked, int *bdberr,
                              BtCursor *_)
{
    return bdb_temp_table_find(bdb_state, cur, key, keylen, unpacked, bdberr);
}

static unsigned long long tmptbl_cursor_rowid(struct temp_table *tbl,
                                              BtCursor *_)
{
    return bdb_temp_table_new_rowid(tbl);
}

static int tmptbl_cursor_count(BtCursor *btcursor, i64 *count)
{
    int bdberr;
    int rc = bdb_temp_table_first(thedb->bdb_env, btcursor->tmptable->cursor,
                                  &bdberr);
    if (rc == IX_PASTEOF || rc == IX_EMPTY) {
        *count = 0;
        return SQLITE_OK;
    }

    i64 cnt = 1;
    while ((rc = bdb_temp_table_next_norewind(thedb->bdb_env,
                                              btcursor->tmptable->cursor,
                                              &bdberr)) == IX_FND) {
        ++cnt;
    }

    if (rc == IX_PASTEOF) {
        *count = cnt;
        return SQLITE_OK;
    }

    return SQLITE_INTERNAL;
}

static int lk_tmptbl_cursor_move(BtCursor *btcursor, int *pRes, int how)
{
    Pthread_mutex_lock(btcursor->tmptable->lk);
    int rc = tmptbl_cursor_move(btcursor, pRes, how);
    Pthread_mutex_unlock(btcursor->tmptable->lk);
    return rc;
}

static int lk_tmptbl_cursor_del(bdb_state_type *bdb_state,
                                struct temp_cursor *cur, int *bdberr,
                                BtCursor *btcursor)
{
    Pthread_mutex_lock(btcursor->tmptable->lk);
    int rc = tmptbl_cursor_del(bdb_state, cur, bdberr, btcursor);
    Pthread_mutex_unlock(btcursor->tmptable->lk);
    return rc;
}

static int lk_tmptbl_cursor_put(bdb_state_type *bdb_state,
                                struct temp_table *tbl, void *key, int keylen,
                                void *data, int dtalen, void *unpacked,
                                int *bdberr, BtCursor *btcursor)
{
    Pthread_mutex_lock(btcursor->tmptable->lk);
    int rc = tmptbl_cursor_put(bdb_state, tbl, key, keylen, data, dtalen,
                               unpacked, bdberr, btcursor);
    Pthread_mutex_unlock(btcursor->tmptable->lk);
    return rc;
}

static int lk_tmptbl_cursor_close(bdb_state_type *bdb_state, BtCursor *btcursor,
                                  int *bdberr)
{
    Pthread_mutex_lock(btcursor->tmptable->lk);
    int rc = tmptbl_cursor_close(bdb_state, btcursor, bdberr);
    Pthread_mutex_unlock(btcursor->tmptable->lk);
    return rc;
}

static int lk_tmptbl_cursor_find(bdb_state_type *bdb_state,
                                 struct temp_cursor *cur, const void *key,
                                 int keylen, void *unpacked, int *bdberr,
                                 BtCursor *btcursor)
{
    Pthread_mutex_lock(btcursor->tmptable->lk);
    int rc = tmptbl_cursor_find(bdb_state, cur, key, keylen, unpacked, bdberr,
                                btcursor);
    Pthread_mutex_unlock(btcursor->tmptable->lk);
    return rc;
}

static unsigned long long lk_tmptbl_cursor_rowid(struct temp_table *tbl,
                                                 BtCursor *btcursor)
{
    Pthread_mutex_lock(btcursor->tmptable->lk);
    unsigned long long rowid = tmptbl_cursor_rowid(tbl, btcursor);
    Pthread_mutex_unlock(btcursor->tmptable->lk);
    return rowid;
}

static int lk_tmptbl_cursor_count(BtCursor *btcursor, i64 *count)
{
    Pthread_mutex_lock(btcursor->tmptable->lk);
    int rc = tmptbl_cursor_count(btcursor, count);
    Pthread_mutex_unlock(btcursor->tmptable->lk);
    return rc;
}

static int
sqlite3BtreeCursor_temptable(Btree *pBt,      /* The btree */
                             int iTable,      /* Root page of table to open */
                             int wrFlag,      /* 1 to write. 0 read-only */
                             xCmpPacked xCmp, /* Key Comparison func */
                             void *pArg,      /* First arg to xCompare() */
                             BtCursor *cur,   /* Write new cursor here */
                             struct sql_thread *thd)
{
    int bdberr = 0;
    struct temptable *src = hash_find(pBt->temp_tables, &iTable);
    if (src == NULL) {
        logmsg(LOGMSG_ERROR, "%s: table %d not found\n",
               __func__, iTable);
        return SQLITE_INTERNAL;
    }

    assert(cur->tmptable == NULL);
    cur->tmptable = calloc(1, sizeof(struct temptable));

    if (!cur->tmptable) {
        logmsg(LOGMSG_ERROR, "%s: calloc sizeof(struct temptable) failed\n",
                __func__);
        return SQLITE_INTERNAL;
    }

    cur->cursor_class = CURSORCLASS_TEMPTABLE;
    assert(src->tbl);
    cur->tmptable->tbl = src->tbl;
    if (src->lk) {
        cur->tmptable->lk = src->lk;
        cur->cursor_move = lk_tmptbl_cursor_move;
        cur->cursor_del = lk_tmptbl_cursor_del;
        cur->cursor_put = lk_tmptbl_cursor_put;
        cur->cursor_close = lk_tmptbl_cursor_close;
        cur->cursor_find = lk_tmptbl_cursor_find;
        cur->cursor_rowid = lk_tmptbl_cursor_rowid;
        cur->cursor_count = lk_tmptbl_cursor_count;
    } else {
        cur->cursor_move = tmptbl_cursor_move;
        cur->cursor_del = tmptbl_cursor_del;
        cur->cursor_put = tmptbl_cursor_put;
        cur->cursor_close = tmptbl_cursor_close;
        cur->cursor_find = tmptbl_cursor_find;
        cur->cursor_rowid = tmptbl_cursor_rowid;
        cur->cursor_count = tmptbl_cursor_count;
    }

    if (cur->tmptable->lk)
        Pthread_mutex_lock(cur->tmptable->lk);
    cur->tmptable->cursor = bdb_temp_table_cursor(
        thedb->bdb_env, cur->tmptable->tbl, pArg, &bdberr);
    bdb_temp_table_set_cmp_func(cur->tmptable->tbl, (tmptbl_cmp)xCmp);
    if (cur->tmptable->lk)
        Pthread_mutex_unlock(cur->tmptable->lk);

    if (cur->tmptable->cursor == NULL) {
        logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_cursor failed: %d\n",
               __func__, bdberr);
        free(cur->tmptable);
        cur->tmptable = NULL;
        return SQLITE_INTERNAL;
    }

    /* if comments above are to be believed
     * this will be set the same on each cursor
     * that is opened for a table */
    cur->rootpage = iTable;
    cur->bt = pBt;
    if (src->flags & BTREE_INTKEY)
        cur->ixnum = -1;
    else
        /* mark as index (-1 means not index, others are ignored) */
        /* this also covers the flags == 0 case for OP_AggReset */
        cur->ixnum = 0;

    return SQLITE_OK;
}

static int sqlite3BtreeCursor_master(
    Btree *pBt,                /* The btree */
    int iTable,                /* Root page of table to open */
    int wrFlag,                /* 1 to write. 0 read-only */
    xCmpPacked xCmp,           /* Key Comparison func */
    void *pArg,                /* First arg to xCompare() */
    BtCursor *cur,             /* Write new cursor here */
    struct sql_thread *thd,    /* need some sideinfo from sqlite3 */
    unsigned long long keyDdl, /* passed cached side row to the new cursor; this
                                  will be clean once the CREATE VIEW plan
                                  will be fixed by Dr. Hipp */
    char *dataDdl, int nDataDdl)
{
    Mem m;
    int sz = 0;
    u32 len;

    cur->cursor_class = CURSORCLASS_SQLITEMASTER;
    if (pBt->is_remote) {
        cur->cursor_move = fdb_cursor_move_master;
    } else {
        cur->cursor_move = cursor_move_master;
    }

    cur->tblpos = 0;
    cur->db = NULL;
    cur->tableversion = 0;

    /* buffer just contains rrn */
    m.flags = MEM_Int;
    m.u.i = (i64)INT_MAX;
    sqlite3VdbeSerialType(&m, SQLITE_DEFAULT_FILE_FORMAT, &len);
    sz += len;                   /* need this much for the data */
    sz += sqlite3VarintLen(len); /* need this much for the header */

    cur->keybuflen = sz;
    cur->keybuf = malloc(sz);
    if (!cur->keybuf) {
        logmsg(LOGMSG_ERROR, "%s: malloc %d for keybuf\n", __func__, sz);
        return SQLITE_INTERNAL;
    }
    cur->bt = pBt;
    cur->move_cost = cur->find_cost = cur->write_cost = 0;

    cur->keyDdl = keyDdl;
    cur->dataDdl = dataDdl;
    cur->nDataDdl = nDataDdl;

    return SQLITE_OK;
}

/* analyze that should run against a compressed index */
static inline int has_compressed_index(int iTable, BtCursor *cur,
                                       struct sql_thread *thd)
{
    int ixnum;
    int rc;
    struct sqlclntstate *clnt = thd->clnt;
    struct dbtable *db;

    if (!clnt->is_analyze) {
        return 0;
    }

    assert(iTable >= RTPAGE_START);
    /* INVALID: assert(iTable < (thd->rootpage_nentries + RTPAGE_START)); */

    db = get_sqlite_db(thd, iTable, &ixnum);
    if (!db) return -1;

    rc = analyze_is_sampled(clnt, db->tablename, ixnum);
    return rc;
}

static int rootpcompare(const void *p1, const void *p2)
{
#if 0
    int i = *(int *)p1;
    int j = *(int *)p2;

    if (i>j)
        return 1;
    if (i<j)
        return -1;
    return 0;
#endif
    Table *tp1 = *(Table **)p1;
    Table *tp2 = *(Table **)p2;

    return strcmp(tp1->zName, tp2->zName);
}

int gbl_debug_systable_locks = 0;
#ifdef ASSERT_SYSTABLE_LOCKS
int gbl_assert_systable_locks = 1;
#else
int gbl_assert_systable_locks = 0;
#endif
extern pthread_rwlock_t views_lk;

static int sqlite3LockStmtTables_int(sqlite3_stmt *pStmt, int after_recovery)
{
    if (pStmt == NULL)
        return 0;

    Vdbe *p = (Vdbe *)pStmt;
    int rc = 0;
    int bdberr = 0;
    int prev = -1;
    Table **tbls = p->tbls;
    int nTables = p->numTables;
    int iTable;
    int nRemoteTables = 0;
    int remote_schema_changed = 0;
    int dups = 0;
    struct dbtable *db;
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;

    if (NULL == clnt->dbtran.cursor_tran) {
        return 0;
    }

    if ((p->vTableFlags & PREPARE_ACQUIRE_VIEWSLK) && !clnt->dbtran.views_lk_held) {
        Pthread_rwlock_rdlock(&views_lk);
        clnt->dbtran.views_lk_held = 1;
    }

    for (int i = 0; i < p->numVTableLocks; i++) {
        if ((rc = bdb_lock_tablename_read_fromlid(thedb->bdb_env, p->vTableLocks[i],
                                                  bdb_get_lid_from_cursortran(clnt->dbtran.cursor_tran))) != 0) {
            logmsg(LOGMSG_ERROR, "%s lock %s returns %d\n", __func__, p->vTableLocks[i], rc);
            return rc;
        }
    }

    if (gbl_debug_systable_locks && p->numVTableLocks > 0) {
        if ((rc = bdb_lock_tablename_read_fromlid(thedb->bdb_env, "_comdb2_systables",
                                                  bdb_get_lid_from_cursortran(clnt->dbtran.cursor_tran))) != 0) {
            logmsg(LOGMSG_ERROR, "%s lock _comdb2_systables returns %d\n", __func__, rc);
            return rc;
        }
    }

    if (nTables == 0)
        return 0;

    /* sort and dedup */
    qsort(tbls, nTables, sizeof(Table *), rootpcompare);

    prev = -1;
    nRemoteTables = 0;

    for (int i = 0; i < nTables; i++) {
        Table *tab = tbls[i];
        iTable = tab->tnum;

        /* INVALID: assert(iTable < thd->rootpage_nentries + RTPAGE_START); */

        if (iTable < RTPAGE_START)
            continue;

        if (prev >= 0 && prev == iTable) {
            /* don't lock same table twice */
            dups++;
            if (dups >= 3) {
                /* we need at least 3 cursors targetting the same
                table; one table, for delete, and two independent index lookups
                */
                clnt->is_overlapping = 1;
            }
            continue;
        }
        dups = 1;
        prev = iTable;

        db = get_sqlite_db(thd, iTable, NULL);

        if (!db) {
            if (after_recovery && !fdb_table_exists(iTable)) {
                logmsg(LOGMSG_ERROR, "%s: no such table: %s\n", __func__,
                       tab->zName);
                sqlite3_mutex_enter(sqlite3_db_mutex(p->db));
                sqlite3VdbeError(p, "table \"%s\" was schema changed",
                                 tab->zName);
                sqlite3VdbeTransferError(p);
                sqlite3MakeSureDbHasErr(p->db, SQLITE_OK);
                sqlite3_mutex_leave(sqlite3_db_mutex(p->db));
                return SQLITE_SCHEMA;
            }
            nRemoteTables++;
            continue;
        }

        /* here we are locking a table and make sure no schema change happens
           during the run
           Perfect place for a checking requests comming from remote databases

           NOTE: initial code FDB_VER_LEGACY did not do schema change version
           tracking
         */
        if (clnt->fdb_state.remote_sql_sb &&
            clnt->fdb_state.code_release >= FDB_VER_CODE_VERSION) {
            /*assert(nTables == 1);   WRONG: currently our sql includes one
             * table and only one table */

            unsigned long long version;
            int short_version;

            rc = bdb_table_version_select_verbose(db->tablename, NULL, &version,
                                                  &bdberr, 0);
            if (rc || bdberr) {
                logmsg(LOGMSG_ERROR, "%s error version=%llu rc=%d bdberr=%d\n",
                       __func__, version, rc, bdberr);
                version = -1ULL;
            }
            short_version = fdb_table_version(version);
            if (gbl_fdb_track) {
                logmsg(LOGMSG_ERROR, "%s: table \"%s\" has version %llu (%u), "
                                     "checking against %u\n",
                       __func__, db->tablename, version, short_version,
                       clnt->fdb_state.version);
            }

            if (short_version != clnt->fdb_state.version) {
                clnt->fdb_state.xerr.errval = SQLITE_SCHEMA;
                /* NOTE: first word of the error string is the actual version,
                   expected on the other side; please do not change */
                errstat_set_strf(&clnt->fdb_state.xerr,
                                 "%llu Stale version local %u != received %u",
                                 version, short_version,
                                 clnt->fdb_state.version);

                /* local table was schema changed in the middle, we need to pass
                 * back an error */
                return SQLITE_SCHEMA;
            }
        }

        if ((rc = bdb_lock_table_read_fromlid(db->handle, bdb_get_lid_from_cursortran(clnt->dbtran.cursor_tran))) !=
            0) {
            logmsg(LOGMSG_ERROR, "%s lock table %s returns %d\n", __func__, tab->zName, rc);
            return rc;
        }

        /* BIG NOTE:
         * A sqlite engine can safely access a dbtable once it acquires the read lock
         * for the table. This is done under schema change lock, and after that the
         * table cannot be dropped or altered until the read lock is released.
         * 
         * In a normal case, for each statement, the table read locks are acquired under the 
         * curtran locker id, and are released when the statement execution is over,
         * just before closing the curtran.  * This protects the table for the duration 
         * of statement's execution.  This is different in chunk transaction, as explained below.  
         *
         * For client transactions ("begin";write_table1;write_table2;commit"), after write_table1
         * is done, "table1" can be altered or even dropped by a concurrent schema change.
         * This is ok for transaction isolation modes weaker than snapshot.
         *
         * For snapshot and serial isolation, once a table is accessed, it become part of the
         * snapshot.  For example, in ("begin"; write_table1_once; write_table1_twice; "commit"),
         * if table is schema changed after "write_table1_once" finishes, the snapshot is broken
         * and we need to fail the transaction.
         *
         * In transaction chunk mode, during a single statement execution we release the table locks 
         * once the chunk is committed. This allows the table to be dropped/altered concurrently
         * with sqlite statement execution.  In order to continue sqlite execution safely 
         * (next chunk), we need to reacquire the table locks.  We also need to make sure the
         * table was not changed (altered or drop&add).
         */

        /* if chunk mode, check version (see comment above) */
        if (clnt->dbtran.maxchunksize > 0) {
            if (db->tableversion != tab->version) {
                logmsg(LOGMSG_ERROR,
                       "%s table %s schema changed during chunk transaction %llu %d\n",
                       __func__, db->tablename, db->tableversion, tab->version);
                return SQLITE_SCHEMA;
            }
        }

        /* in snapshot and stronger isolations, check cached table versions */
        if (clnt->dbtran.shadow_tran &&
            (clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
             clnt->dbtran.mode == TRANLEVEL_SERIAL)) {
            /* make sure btrees have not changed since the transaction started
             */
            rc = bdb_osql_check_table_version(
                db->handle, clnt->dbtran.shadow_tran, 0, &bdberr);
            if (rc != 0) {
                /* fprintf(stderr, "bdb_osql_check_table_version failed rc=%d
                   bdberr=%d\n",
                   rc, bdberr);*/
                sqlite3_mutex_enter(sqlite3_db_mutex(p->db));
                sqlite3VdbeError(p, "table \"%s\" was schema changed",
                                 db->tablename);
                sqlite3VdbeTransferError(p);
                sqlite3MakeSureDbHasErr(p->db, SQLITE_OK);
                sqlite3_mutex_leave(sqlite3_db_mutex(p->db));

                return SQLITE_SCHEMA;
            }
        }

        if (after_recovery) {
            /* NOTE: returning here error is very low level, branching many code
               paths.
               Leave version checking for phase 2, for now we make sure that
               longer term sql
               processing is still protected after the recovery */
            if (0) {
                sqlite3_mutex_enter(sqlite3_db_mutex(p->db));
                sqlite3VdbeError(
                    p, "table \"%s\" was schema changed during recovery",
                    db->tablename);
                sqlite3VdbeTransferError(p);
                sqlite3_mutex_leave(sqlite3_db_mutex(p->db));

                return SQLITE_SCHEMA;
            }
        } else {
            /* we increment only on the initial table locking */
            db->nsql++; /* per table nsql stats */
        }

        reqlog_add_table(thrman_get_reqlogger(thrman_self()), db->tablename);
    }

    if (!after_recovery)
        clnt->dbtran.pStmt = pStmt;

    /* we don't release remote table locks during sql recovery */
    if (!after_recovery && nRemoteTables > 0) {
        clnt->dbtran.lockedRemTables =
            (fdb_tbl_ent_t **)calloc(sizeof(fdb_tbl_ent_t *), nRemoteTables);
        if (!clnt->dbtran.lockedRemTables) {
            logmsg(LOGMSG_ERROR, "%s: malloc!\n", __func__);
            return SQLITE_ABORT;
        }
        clnt->dbtran.nLockedRemTables = nRemoteTables;

        nRemoteTables = 0;
        prev = -1;
        for (int i = 0; i < nTables; i++) {
            Table *tab = tbls[i];
            iTable = tab->tnum;

            if (iTable < 2)
                continue;

            if (prev >= 0 && prev == iTable) {
                /* don't lock same table twice */
                continue;
            }
            prev = iTable;

            db = get_sqlite_db(thd, iTable, NULL);

            if (db) {
                /* local table */
                continue;
            }

            /* this is a remote table; we need to acquire remote locks */
            rc = fdb_lock_table(pStmt, clnt, tab,
                                &clnt->dbtran.lockedRemTables[nRemoteTables]);
            if (rc == SQLITE_SCHEMA_REMOTE) {
                assert(clnt->dbtran.lockedRemTables[nRemoteTables] == NULL);
                remote_schema_changed = 1;
            } else if (rc) {
                clnt->dbtran.nLockedRemTables =
                    nRemoteTables; /* we only grabbed that many locks so far */

                logmsg(LOGMSG_ERROR, 
                       "Failed to lock remote table cache for \"%s\" rootp %d\n",
                       tab->zName, iTable);

                sqlite3_mutex_enter(sqlite3_db_mutex(p->db));
                sqlite3VdbeError(
                    p,
                    "Failed to lock remote table cache for \"%s\" rootp %d\n",
                    tab->zName, iTable);
                sqlite3VdbeTransferError(p);
                sqlite3_mutex_leave(sqlite3_db_mutex(p->db));

                return SQLITE_ABORT;
            }

            nRemoteTables++;
        }

        /* do we have stall/missing remote tables ? */
        if (remote_schema_changed)
            return SQLITE_SCHEMA_REMOTE;
    }

    return 0;
}

int sqlite3LockStmtTables(sqlite3_stmt *pStmt)
{
    return sqlite3LockStmtTables_int(pStmt, 0);
}

int sqlite3LockStmtTablesRecover(sqlite3_stmt *pStmt)
{
    return sqlite3LockStmtTables_int(pStmt, 1);
}

void sql_remote_schema_changed(struct sqlclntstate *clnt, sqlite3_stmt *pStmt)
{
    Vdbe *p = (Vdbe *)pStmt;
    Table **tbls = p->tbls;
    int nTables = p->numTables;
    int iTable;
    int nRemoteTables = 0;
    int prev;
    struct dbtable *pdb;

    nRemoteTables = 0;
    prev = -1;
    struct sql_thread *thd = pthread_getspecific(query_info_key);

    for (int i = 0; i < nTables; i++) {
        Table *tab = tbls[i];

        iTable = tab->tnum;

        /* INVALID: assert(iTable < thd->rootpage_nentries + RTPAGE_START); */

        if (iTable < RTPAGE_START)
            continue;

        if (prev >= 0 && prev == iTable) {
            /* don't lock same table twice */
            continue;
        }
        prev = iTable;

        pdb = get_sqlite_db(thd, iTable, NULL);

        if (!pdb) {
            Db *db = &p->db->aDb[tab->iDb];

            /* if the descriptive entry is missing, it means the lock was not
             * acquired due to stale table */
            if (clnt->dbtran.lockedRemTables[nRemoteTables] == NULL) {
                /* this table had stale information, free its sqlite schema
                 * cache */
                fdb_clear_sqlite_cache(p->db, db->zDbSName, tab->zName);
            }

            nRemoteTables++;
            continue;
        }
    }
}

/**
 * Remotes don't use a berkdb lock to block access to a resource
 * In order to avoid losing the lock every time berkdb desides to
 * take a break, I am using a counter.
 *
 * This special routine takes care of decrementing the counter
 * when session is over (i.e. when curtran is released at the end)
 *
 */
int sqlite3UnlockStmtTablesRemotes(struct sqlclntstate *clnt)
{
    int rc = 0;

    for (int i = 0; i < clnt->dbtran.nLockedRemTables; i++) {
        /* missing lock */
        if (clnt->dbtran.lockedRemTables[i] == NULL)
            continue;

        /* this is a remote table; we need to release remote locks */
        rc = fdb_unlock_table(clnt->dbtran.lockedRemTables[i]);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Failed to unlock remote table cache for \"%s\"\n",
                    fdb_table_entry_tblname(clnt->dbtran.lockedRemTables[i]));
        }
    }

    /* This is required to recover from deadlocks.*/
    if (clnt->dbtran.lockedRemTables)
        free(clnt->dbtran.lockedRemTables);

    clnt->dbtran.lockedRemTables = NULL;
    clnt->dbtran.nLockedRemTables = 0;
    return 0;
}

static int
sqlite3BtreeCursor_remote(Btree *pBt,      /* The btree */
                          int iTable,      /* Root page of table to open */
                          int wrFlag,      /* 1 to write. 0 read-only */
                          xCmpPacked xCmp, /* Key Comparison func */
                          void *pArg,      /* First arg to xCompare() */
                          BtCursor *cur,   /* Write new cursor here */
                          struct sql_thread *thd)
{
    extern int gbl_ssl_allow_remsql;
    struct sqlclntstate *clnt = thd->clnt;
    fdb_tran_t *trans;
    fdb_t *fdb;
    uuid_t tid;

    assert(pBt != 0);

    /* this doesn't get a lock, by this time we have acquired a table lock here
     */
    fdb = get_fdb(pBt->zFilename);
    if (!pBt->fdb) {
        logmsg(LOGMSG_ERROR, "%s: failed to retrieve db \"%s\"\n", __func__,
                pBt->zFilename);
        return SQLITE_INTERNAL;
    }
    assert(fdb == pBt->fdb);

    cur->cursor_class = CURSORCLASS_REMOTE;
    cur->cursor_move = cursor_move_remote;

    /* Reset previous fdb error (if any). */
    clnt->fdb_state.xerr.errval = 0;

    if (clnt->intrans) {
        int rc = osql_sock_start_deferred(clnt);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to start sosql, rc=%d\n", __func__,
                   rc);
            return rc;
        }
    }

    /* set a transaction id if none is set yet */
    if ((iTable >= RTPAGE_START) && !fdb_is_sqlite_stat(fdb, cur->rootpage)) {
        /* I would like to open here a transaction if this is
           an actual update */
        if (!clnt->isselect /* TODO: maybe only create one if we write to remote && fdb_write_is_remote()*/) {
            trans =
                fdb_trans_begin_or_join(clnt, fdb, (char *)tid, 0 /* TODO */);
        } else {
            trans = fdb_trans_join(clnt, fdb, (char *)tid);
        }
    } else {
        *(unsigned long long *)tid = 0ULL;
        trans = NULL;
    }

    if (trans)
        Pthread_mutex_lock(&clnt->dtran_mtx);

    int usessl = clnt->plugin.has_ssl(clnt) && gbl_ssl_allow_remsql;
    cur->fdbc =
        fdb_cursor_open(clnt, cur, cur->rootpage, trans, &cur->ixnum, usessl);
    if (!cur->fdbc) {
        if (trans)
            Pthread_mutex_unlock(&clnt->dtran_mtx);
        return SQLITE_ERROR;
    }

    if (gbl_fdb_track) {
        uuidstr_t cus, tus;
        unsigned char *pStr = (unsigned char *)cur->fdbc->id(cur);
        logmsg(LOGMSG_USER,
               "%s Created cursor cid=%s with tid=%s rootp=%d "
               "db:tbl=\"%s:%s\"\n",
               __func__, (pStr) ? comdb2uuidstr(pStr, cus) : "UNK",
               comdb2uuidstr(tid, tus), iTable, pBt->zFilename,
               cur->fdbc->name(cur));
    }

    if (trans)
        Pthread_mutex_unlock(&clnt->dtran_mtx);

    if (gbl_expressions_indexes && !clnt->isselect &&
        cur->fdbc->tbl_has_expridx(cur)) {
        if (!clnt->idxInsert)
            clnt->idxInsert = calloc(MAXINDEX, sizeof(uint8_t *));
        if (!clnt->idxDelete)
            clnt->idxDelete = calloc(MAXINDEX, sizeof(uint8_t *));
        if (!clnt->idxInsert || !clnt->idxDelete) {
            logmsg(LOGMSG_ERROR, "%s:%d malloc failed\n", __func__, __LINE__);
            return SQLITE_NOMEM;
        }
    }

    return 0;
}

static inline int use_rowlocks(struct sqlclntstate *clnt)
{
    return 0;
}

static int
sqlite3BtreeCursor_cursor(Btree *pBt,      /* The btree */
                          int iTable,      /* Root page of table to open */
                          int wrFlag,      /* 1 to write. 0 read-only */
                          xCmpPacked xCmp, /* Key Comparison func */
                          void *pArg,      /* First arg to xCompare() */
                          BtCursor *cur,   /* Write new cursor here */
                          struct sql_thread *thd)
{
    struct sqlclntstate *clnt = thd->clnt;
    size_t key_size;
    int rowlocks = use_rowlocks(clnt);
    int rc = SQLITE_OK;
    int bdberr = 0;
    u32 len;
    int sz = 0;
    struct schema *sc;
    void *shadow_tran = NULL;

    assert(iTable >= RTPAGE_START);
    /* INVALID: assert(iTable < thd->rootpage_nentries + RTPAGE_START); */

    cur->db = get_sqlite_db(thd, iTable, &cur->ixnum);
    assert(cur->db);
    if (cur->db == NULL) {
        /* this shouldn't happen */
       logmsg(LOGMSG_ERROR, "sqlite3BtreeCursor: no cur->db\n");
        return SQLITE_INTERNAL;
    }
    cur->tableversion = cur->db->tableversion;

    /* initialize the shadow, if any  */
    cur->shadtbl = osql_get_shadow_bydb(thd->clnt, cur->db);

    if (cur->ixnum == -1) {
        if (is_stat2(cur->db->tablename) || is_stat4(cur->db->tablename)) {
            cur->cursor_class = CURSORCLASS_STAT24;
        } else
            cur->cursor_class = CURSORCLASS_TABLE;
        cur->cursor_move = cursor_move_table;
        cur->sc = cur->db->schema;
    } else {
        cur->cursor_class = CURSORCLASS_INDEX;
        cur->cursor_move = cursor_move_index;
        cur->sc = cur->db->ixschema[cur->ixnum];
        cur->nCookFields = -1;
    }

    reqlog_usetable(pBt->reqlogger, cur->db->tablename);

    /* check one time if we have blobs when we open the cursor,
     * so we dont need to run this code for every row if we dont even
     * have them */
    rc = gather_blob_data_byname(cur->db, ".ONDISK", &cur->blobs, NULL);
    if (rc) {
       logmsg(LOGMSG_ERROR, "sqlite3BtreeCursor: gather_blob_data error rc=%d\n", rc);
        return SQLITE_INTERNAL;
    }
    cur->numblobs = cur->blobs.numcblobs;
    if (cur->blobs.numcblobs)
        free_blob_status_data(&cur->blobs);

    cur->tblnum = cur->db->dbs_idx;
    cur->session_tbl = get_session_tbl(clnt, cur->db->tablename);

    cur->ondisk_dtabuf_alloc = getdatsize(cur->db);
    if (cur->writeTransaction) {
        cur->ondisk_buf = calloc(1, getdatsize(cur->db));
        if (!cur->ondisk_buf) {
            logmsg(LOGMSG_ERROR, "%s: malloc (getdatsize(cur->db)=%d) failed\n",
                    __func__, getdatsize(cur->db));
            return SQLITE_INTERNAL;
        }
    } else {
        cur->ondisk_buf = NULL;
    }

    /* for tablescans, key is a genid make sure we have room for either a
     * genid or the first key so that we can look up either way */
    if (cur->ixnum == -1) {
        key_size = getkeysize(cur->db, 0);
        if (sizeof(unsigned long long) > key_size || key_size == (size_t)-1)
            key_size = sizeof(unsigned long long);
    } else
        key_size = getkeysize(cur->db, cur->ixnum);
    cur->ondisk_key = malloc(key_size + sizeof(int));
    if (!cur->ondisk_key) {
        logmsg(LOGMSG_ERROR, "%s:malloc ondisk_key sz %zu failed\n", __func__,
               key_size + sizeof(int));
        free(cur->ondisk_buf);
        return SQLITE_INTERNAL;
    }
    cur->ondisk_keybuf_alloc = key_size;
    if (cur->writeTransaction) {
        cur->fndkey = malloc(key_size + sizeof(int));
        if (!cur->fndkey) {
            logmsg(LOGMSG_ERROR, "%s:malloc fndkey sz %zu failed\n", __func__,
                   key_size + sizeof(int));
            free(cur->ondisk_buf);
            free(cur->ondisk_key);
            return SQLITE_INTERNAL;
        }
    } else {
        cur->fndkey = NULL;
    }

    /* find sqlite-format buffer sizes we'll need */

    /* data buffer */
    sz = schema_var_size(cur->db->schema);
    if (cur->writeTransaction) {
        cur->dtabuf = malloc(sz);
        if (!cur->dtabuf) {
            logmsg(LOGMSG_ERROR, "%s:malloc dtabuf sz %d failed\n", __func__, sz);
            free(cur->fndkey);
            free(cur->ondisk_buf);
            free(cur->ondisk_key);
            return SQLITE_INTERNAL;
        }
    } else {
        cur->dtabuf = NULL;
    }
    cur->dtabuflen = sz;
    cur->dtabuf_alloc = sz;

    memset(&cur->blobs, 0, sizeof(blob_status_t));

    /* key */
    if (cur->ixnum == -1) {
        Mem m;
        /* buffer just contains rrn */
        m.flags = MEM_Int;
        m.u.i = (i64)INT_MAX;
        sqlite3VdbeSerialType(&m, SQLITE_DEFAULT_FILE_FORMAT, &len);
        sz += len;                   /* need this much for the data */
        sz += sqlite3VarintLen(len); /* need this much for the header */
    } else {
        sc = cur->db->ixschema[cur->ixnum];
        sz = schema_var_size(sc);
    }
    cur->keybuf = malloc(sz);
    if (!cur->keybuf) {
        logmsg(LOGMSG_ERROR, "%s: keybuf malloc %d failed\n", __func__, sz);
        free(cur->dtabuf);
        free(cur->fndkey);
        free(cur->ondisk_buf);
        free(cur->ondisk_key);
        return SQLITE_INTERNAL;
    }
    cur->keybuflen = sz;
    cur->keybuf_alloc = sz;

    if (clnt->dbtran.mode == TRANLEVEL_SOSQL ||
        clnt->dbtran.mode == TRANLEVEL_RECOM ||
        clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
        clnt->dbtran.mode == TRANLEVEL_SERIAL) {
        shadow_tran = clnt->dbtran.shadow_tran;
    }

    enum bdb_open_type open_type;
    if (shadow_tran && (clnt->dbtran.mode != TRANLEVEL_SOSQL)) {
        open_type = (wrFlag) ? BDB_OPEN_BOTH_CREATE : BDB_OPEN_BOTH;
    } else {
        open_type = BDB_OPEN_REAL;
    }
    cur->bdbcur = bdb_cursor_open(
        cur->db->handle, clnt->dbtran.cursor_tran, shadow_tran, cur->ixnum,
        open_type, (clnt->dbtran.mode == TRANLEVEL_SOSQL)
                       ? NULL
                       : osql_get_shadtbl_addtbl_newcursor(cur),
        clnt->pageordertablescan, rowlocks,
        rowlocks ? &clnt->holding_pagelocks_flag : NULL,
        rowlocks ? pause_pagelock_cursors : NULL, rowlocks ? (void *)thd : NULL,
        rowlocks ? count_pagelock_cursors : NULL, rowlocks ? (void *)thd : NULL,
        clnt->bdb_osql_trak, &bdberr);
    if (cur->bdbcur == NULL) {
        logmsg(LOGMSG_ERROR, "%s: bdb_cursor_open rc %d\n", __func__, bdberr);
        if (bdberr == BDBERR_DEADLOCK)
            rc = SQLITE_DEADLOCK;
        else
            rc = SQLITE_INTERNAL;
        return rc;
    }

    if (gbl_expressions_indexes && !clnt->isselect && cur->db->ix_expr) {
        if (!clnt->idxInsert)
            clnt->idxInsert = calloc(MAXINDEX, sizeof(uint8_t *));
        if (!clnt->idxDelete)
            clnt->idxDelete = calloc(MAXINDEX, sizeof(uint8_t *));
        if (!clnt->idxInsert || !clnt->idxDelete) {
            logmsg(LOGMSG_ERROR, "%s:%d malloc failed\n", __func__, __LINE__);
            rc = SQLITE_NOMEM;
        }
    }

    return rc;
}

/*
 ** Create a new cursor for the BTree whose root is on the page
 ** iTable.  The act of acquiring a cursor gets a read lock on
 ** the database file.
 **
 ** If wrFlag==0, then the cursor can only be used for reading.
 ** If wrFlag==1, then the cursor can be used for reading or for
 ** writing if other conditions for writing are also met.  These
 ** are the conditions that must be met in order for writing to
 ** be allowed:
 **
 ** 1:  The cursor must have been opened with wrFlag==1
 **
 ** 2:  No other cursors may be open with wrFlag==0 on the same table
 **
 ** 3:  The database must be writable (not on read-only media)
 **
 ** 4:  There must be an active transaction.
 **
 ** Condition 2 warrants further discussion.  If any cursor is opened
 ** on a table with wrFlag==0, that prevents all other cursors from
 ** writing to that table.  This is a kind of "read-lock".  When a cursor
 ** is opened with wrFlag==0 it is guaranteed that the table will not
 ** change as long as the cursor is open.  This allows the cursor to
 ** do a sequential scan of the table without having to worry about
 ** entries being inserted or deleted during the scan.  Cursors should
 ** be opened with wrFlag==0 only if this read-lock property is needed.
 ** That is to say, cursors should be opened with wrFlag==0 only if they
 ** intend to use the sqlite3BtreeNext() system call.  All other cursors
 ** should be opened with wrFlag==1 even if they never really intend
 ** to write.
 **
 ** No checking is done to make sure that page iTable really is the
 ** root page of a b-tree.  If it is not, then the cursor acquired
 ** will not work correctly.
 **
 ** The comparison function must be logically the same for every cursor
 ** on a particular table.  Changing the comparison function will result
 ** in incorrect operations.  If the comparison function is NULL, a
 ** default comparison function is used.  The comparison function is
 ** always ignored for INTKEY tables.
 */

void init_cursor(BtCursor *cur, Vdbe *vdbe, Btree *bt)
{
    cur->thd = pthread_getspecific(query_info_key);
    assert(cur->thd);
    cur->clnt = cur->thd->clnt;
    cur->vdbe = vdbe;
    if (gbl_old_column_names && cur->clnt->thd &&
        cur->clnt->thd->query_preparer_running) {
        assert(query_preparer_plugin);
        /* sqlitex */
        cur->query_preparer_data = vdbe ? vdbe->db : NULL;
    } else {
        cur->sqlite = vdbe ? vdbe->db : NULL;
    }
    cur->bt = bt;
    assert(cur->clnt);
}

/*
** Return a pointer to a fake BtCursor object that will always answer
** false to the sqlite3BtreeCursorHasMoved() routine above.  The fake
** cursor returned must not be used with any other Btree interface.
*/
BtCursor *sqlite3BtreeFakeValidCursor(void){
  /* TODO: Do something here. */
  return 0;
}

int sqlite3BtreeCursor(
    Vdbe *vdbe,               /* Vdbe running the show */
    Btree *pBt,               /* BTree containing table to open */
    int iTable,               /* Index of root page */
    int wrFlag,               /* 1 for writing.  0 for read-only. */
    int forOpen,              /* 1 for open mode.  0 for create mode. */
    struct KeyInfo *pKeyInfo, /* First argument to compare function */
    BtCursor *cur             /* Space to write cursor structure */
){
    int rc = SQLITE_OK;
    static int cursorid = 0;

    bzero(cur, sizeof(*cur));
    init_cursor(cur, vdbe, pBt);

    struct sqlclntstate *clnt = cur->clnt;
    struct sql_thread *thd = cur->thd;

    if (thd->bt == NULL)
        thd->bt = pBt;
    cur->writeTransaction = clnt->writeTransaction;

    /* iTable: rootpage is not a rootpage anymore: it's the index of the entry
     * (index or table) in pBt  (-2 bias to allow for special tables)
     */

    cur->cursorid = cursorid++; /* for debug trace */
    cur->open_flags = wrFlag;
    /*printf("Open Cursor rootpage=%d flags=%x\n", iTable, wrFlag);*/

    cur->rootpage = iTable;
    cur->pKeyInfo = pKeyInfo;

    if (pBt->is_temporary) { /* temp table */
        assert(iTable >= 1); /* can never be zero or negative */
        int pgno = iTable;
        if (forOpen) {
            /*
            ** NOTE: When being called to open a temporary (table) cursor in
            **       response to OP_OpenRead, OP_OpenWrite, or OP_ReopenIdx
            **       opcodes by the VDBE, the temporary table may not have
            **       been created yet.  Attempt to do that now.
            */
            if (hash_find(pBt->temp_tables, &pgno) == NULL) {
                assert(tmptbl_clone == NULL);
                rc = sqlite3BtreeCreateTable(pBt, &pgno, BTREE_INTKEY);
            }
        }
        if (rc == SQLITE_OK) {
            assert(pgno == iTable);
            rc = sqlite3BtreeCursor_temptable(pBt, pgno, wrFlag & BTREE_CUR_WR,
                                              temp_table_cmp, pKeyInfo, cur,
                                              thd);
        }
        cur->find_cost = CDB2_TEMP_FIND_COST;
        cur->move_cost = CDB2_TEMP_MOVE_COST;
        cur->write_cost = CDB2_TEMP_WRITE_COST;
    }
    /* sqlite_master table */
    else if (iTable == RTPAGE_SQLITE_MASTER && fdb_master_is_local(cur)) {
        rc = sqlite3BtreeCursor_master(
            pBt, iTable, wrFlag & BTREE_CUR_WR, sqlite3VdbeCompareRecordPacked,
            pKeyInfo, cur, thd, clnt->keyDdl, clnt->dataDdl, clnt->nDataDdl);
        cur->find_cost = cur->move_cost = cur->write_cost = CDB2_SQLITE_STAT_COST;
    }
    /* remote cursor? */
    else if (pBt->is_remote) {
        rc = sqlite3BtreeCursor_remote(pBt, iTable, wrFlag & BTREE_CUR_WR,
                                       sqlite3VdbeCompareRecordPacked, pKeyInfo,
                                       cur, thd);
    }
    /* if this is analyze opening an index */
    else if (has_compressed_index(iTable, cur, thd)) {
        rc = sqlite3BtreeCursor_analyze(pBt, iTable, wrFlag & BTREE_CUR_WR,
                                        sqlite3VdbeCompareRecordPacked,
                                        pKeyInfo, cur, thd);
        cur->find_cost = CDB2_TEMP_FIND_COST;
        cur->move_cost = CDB2_TEMP_MOVE_COST;
        cur->write_cost = CDB2_TEMP_WRITE_COST;
    }
    /* real table */
    else {
        rc = sqlite3BtreeCursor_cursor(pBt, iTable, wrFlag & BTREE_CUR_WR,
                                       sqlite3VdbeCompareRecordPacked, pKeyInfo,
                                       cur, thd);
        /* treat sqlite_stat1 as free */
        if (is_sqlite_stat(cur->db->tablename)) {
            cur->find_cost = cur->move_cost = cur->write_cost = CDB2_SQLITE_STAT_COST;
        } else {
            cur->blob_cost = CDB2_BLOB_FETCH_COST;
            cur->find_cost = CDB2_FIND_COST;
            cur->move_cost = CDB2_MOVE_COST;
            cur->write_cost = CDB2_WRITE_COST;
        }
    }

    cur->on_list = 0;
    cur->permissions = 0;

    if (rc != SQLITE_OK && rc != SQLITE_EMPTY && rc != SQLITE_DEADLOCK) {
        /* we were leaking cursors and locks here during incoherent;
         * now there is no more bdbcur at this point if this happens */
        /*free(cur);*/
        cur = NULL;
    }

    if (cur && cur->db) {
        cur->db->sqlcur_cur++;
    }

    if (thd && cur) {
        Pthread_mutex_lock(&thd->lk);
        listc_abl(&pBt->cursors, cur);
        cur->on_list = 1;
        Pthread_mutex_unlock(&thd->lk);
    }

    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                "Cursor(pBt %d, iTable %d, wrFlag %d, cursor %d)      = %s\n",
                pBt->btreeid, iTable, wrFlag & BTREE_CUR_WR,
                cur ? cur->cursorid : -1, sqlite3ErrStr(rc));

    if (cur && (clnt->dbtran.mode == TRANLEVEL_SERIAL ||
                (cur->is_recording && gbl_selectv_rangechk))) {
        cur->range = currange_new();
        if (cur->db) {
            cur->range->tbname = strdup(cur->db->tablename);
            cur->range->idxnum = cur->ixnum;
        }
    }

    if (debug_switch_cursor_deadlock())
        return SQLITE_DEADLOCK;

    return rc;
}

static int make_stat_record(struct sql_thread *thd, BtCursor *pCur,
                            const void *pData, int nData,
                            blob_buffer_t blobs[MAXBLOBS])
{
    int n = is_stat2(pCur->db->tablename) ? 3 : 2;
    struct schema *sc = find_tag_schema(pCur->db, ".ONDISK_IX_0");

    /* extract first n fields from packed record */
    if (sqlite_to_ondisk(sc, pData, nData, pCur->ondisk_buf, pCur->clnt->tzname,
                         blobs, MAXBLOBS, &thd->clnt->fail_reason,
                         pCur) < 0) {
        logmsg(LOGMSG_ERROR, "%s failed\n", __func__);
        return SQLITE_INTERNAL;
    }

    /* save length of packed record, as the n'th field */
    struct field *f = &pCur->db->schema->member[n];
    uint8_t *out = (uint8_t *)pCur->ondisk_buf + f->offset;
    int len = htonl(nData);
    set_data(out, &len, sizeof(len) + 1);

    /* save entire packed record, as the n+1 field */
    f = &pCur->db->schema->member[n + 1];
    out = (uint8_t *)pCur->ondisk_buf + f->offset;
    if (nData > f->len) {
        logmsg(LOGMSG_ERROR, "nData: %d, f->len:%d f->name:%s => BF Grp 592\n", nData, f->len,
               f->name);
        return SQLITE_INTERNAL;
    }
    set_data(out, pData, nData + 1);
    return 0;
}

/*
 ** Start a statement subtransaction.  The subtransaction can
 ** can be rolled back independently of the main transaction.
 ** You must start a transaction before starting a subtransaction.
 ** The subtransaction is ended automatically if the main transaction
 ** commits or rolls back.
 **
 ** Only one subtransaction may be active at a time.  It is an error to try
 ** to start a new subtransaction if another subtransaction is already active.
 **
 ** Statement subtransactions are used around individual SQL statements
 ** that are contained within a BEGIN...COMMIT block.  If a constraint
 ** error occurs within the statement, the effect of that one statement
 ** can be rolled back without having to rollback the entire transaction.
 */
int sqlite3BtreeBeginStmt(Btree *pBt, int iStatement)
{
    /* We defer contstraint checks til commit time, so statement-level
     * transactions are no-ops */
    int rc = 0;
    reqlog_logf(pBt->reqlogger, REQL_TRACE, "BeginStmt(bt=%d)      = %s\n",
                pBt->btreeid, sqlite3ErrStr(rc));
    return rc;
}

#define comdb2_sqlite3VdbeError(vdbe, errstr)                                  \
    sqlite3_mutex_enter(sqlite3_db_mutex(vdbe->db));                           \
    sqlite3VdbeError(vdbe, "%s", errstr);                                      \
    sqlite3_mutex_leave(sqlite3_db_mutex(vdbe->db));

static int chunk_transaction(BtCursor *pCur, struct sqlclntstate *clnt,
                             struct sql_thread *thd)
{
    BtCursor *cur = NULL;
    int rc = SQLITE_OK;
    int commit_rc = SQLITE_OK;
    int newlocks_rc = SQLITE_OK;
    int bdberr = 0;
    fdb_t *fdb = NULL;
    fdb_tran_t *trans = NULL;
    uuid_t tid;
    int need_ssl = 0;

    uint8_t **pIdxInsert = NULL, **pIdxDelete = NULL;
    unsigned long long ins_keys_saved = 0ULL;
    unsigned long long del_keys_saved = 0ULL;

    if (clnt->dbtran.crtchunksize >= clnt->dbtran.maxchunksize) {

        /* Latch partial index/index on expression related data. We do not want
           them to be cleaned up by the micro transaction we are about to commit. */
        if (gbl_partial_indexes && pCur->db && pCur->db->ix_partial) {
            ins_keys_saved = clnt->ins_keys;
            del_keys_saved = clnt->del_keys;
        }
        if (gbl_expressions_indexes && pCur->db && pCur->db->ix_expr) {
            pIdxInsert = clnt->idxInsert;
            pIdxDelete = clnt->idxDelete;
            clnt->idxInsert = clnt->idxDelete = NULL;
        }

        /* commit current transaction and reopen another one */

        /* disconnect berkeley db cursors */
        unlock_bdb_cursors(thd, NULL, &bdberr);
        if (bdberr) {
            comdb2_sqlite3VdbeError(pCur->vdbe,
                                    "Failed to disconnect berkeleydb cursors");
            rc = SQLITE_ERROR;
            goto done;
        }

        /* need to reset shadow table fast point in cursors */
        if (thd->bt) {
            LISTC_FOR_EACH(&thd->bt->cursors, cur, lnk)
            {
                cur->shadtbl = NULL;
                if (cur->bdbcur)
                    bdb_osql_cursor_reset(thedb->bdb_env, cur->bdbcur);
            }
        }

        /* commit current transaction */
        ++clnt->dbtran.nchunks;
        if (pCur->cursor_class == CURSORCLASS_REMOTE) {
            fdb = pCur->bt->fdb;
            need_ssl = fdb_cursor_need_ssl(pCur->fdbc);
            rc = pCur->fdbc->close(pCur);
            if (rc) {
                comdb2_sqlite3VdbeError(pCur->vdbe, errstat_get_str(&clnt->osql.xerr));
                logmsg(LOGMSG_ERROR, "Failed to close remote cursor\n");
                commit_rc = SQLITE_ABORT;
            }
        }

        sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_FNSH_STATE);
        rc = handle_sql_commitrollback(clnt->thd, clnt, TRANS_CLNTCOMM_CHUNK);
        if (rc) {
            comdb2_sqlite3VdbeError(pCur->vdbe,
                                    errstat_get_str(&clnt->osql.xerr));
            logmsg(LOGMSG_ERROR, "Failed to commit chunk\n");
            commit_rc = SQLITE_ABORT;
            /* we need to recreate the transaction in any case
               goto done;
             */
        }

        if (gbl_throttle_txn_chunks_msec > 0) {
            poll(NULL, 0, gbl_throttle_txn_chunks_msec);
        }

        /* restart a new transaction */
        sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                SQLENG_PRE_STRT_STATE);
        if (pCur->cursor_class == CURSORCLASS_REMOTE) {
            /* restart a remote transaction, and open a new remote cursor */
            trans = fdb_trans_begin_or_join(clnt, fdb, (char *)tid, 0);
            pCur->fdbc = fdb_cursor_open(clnt, pCur, pCur->rootpage, trans, &pCur->ixnum, need_ssl);
        }
        rc = handle_sql_begin(clnt->thd, clnt, TRANS_CLNTCOMM_CHUNK);

        if (rc && !commit_rc) {
            comdb2_sqlite3VdbeError(pCur->vdbe, "Failed to start a new chunk");
            rc = SQLITE_ERROR;
            goto done;
        }

        rc = start_new_transaction(clnt, thd);

        if (thd->bt) {
            LISTC_FOR_EACH(&thd->bt->cursors, cur, lnk)
            {
                if (cur->bdbcur)
                    bdb_osql_cursor_set(cur->bdbcur, clnt->dbtran.shadow_tran);
            }
        }

        if (rc && !commit_rc) {
            comdb2_sqlite3VdbeError(pCur->vdbe,
                                    "Failed to initialize new transaction");

            rc = SQLITE_ERROR;
            goto done;
        }

        if (!commit_rc) {
            /* NOTE: the commit has released the curtran locks, so 
             * the table can go away. We cannot keep the table locks here
             * since it deadlocks on the master (the writer lock starvation
             * prevents a new read table lock to be aquired while there is
             * a waiting write lock for that table)
             */
            rdlock_schema_lk();
            assert(pCur->vdbe == (Vdbe*)clnt->dbtran.pStmt);
            newlocks_rc = sqlite3LockStmtTables(clnt->dbtran.pStmt);
            unlock_schema_lk();

            if (newlocks_rc) {
                comdb2_sqlite3VdbeError(
                        pCur->vdbe,
                        "Failed to renew locks, table schema changed");
                rc = SQLITE_ERROR;
                goto done;
            }
        }
        rc = commit_rc;

        clnt->dbtran.crtchunksize = 1;
        if (rc != SQLITE_OK)
            goto done;

    } else {
        clnt->dbtran.crtchunksize++;
    }
done:
    /* Restore the partial index/index on expression data onto clnt for the next
       OP_Insert/OP_Delete. */
    if (ins_keys_saved != 0 || del_keys_saved != 0) {
        clnt->ins_keys = ins_keys_saved;
        clnt->del_keys = del_keys_saved;
    }
    if (pIdxInsert != NULL || pIdxDelete != NULL) {
        clnt->idxInsert = pIdxInsert;
        clnt->idxDelete = pIdxDelete;
    }
    return rc;
}

/*
 ** Insert a new record into the BTree.  The key is given by (pKey,nKey)
 ** and the data is given by (pData,nData).  The cursor is used only to
 ** define what table the record should be inserted into.  The cursor
 ** is left pointing at a random location.
 **
 ** For an INTKEY table, only the nKey value of the key is used.  pKey is
 ** ignored.  For a ZERODATA table, the pData and nData are both ignored.
 **
 **
 ** NOTE: sqlite's backend semantics are: insert either creates a new record,
 ** or replaces an existing one (with the same record id!).   We don't know
 ** until we try whether we are called for an update or an insert.  According
 ** to Hipp, sqlite won't use record id's again without first re-reading the
 ** record.
 */
int sqlite3BtreeInsert(
    BtCursor *pCur, /* Insert data into the table of this cursor */
    const BtreePayload *pPayload, /* The key and data of the new record */
    int bias, int seekResult, int flags)
{
    const void *pKey;
    sqlite3_int64 nKey;
    const void *pData;
    int nData;
    blob_buffer_t *pblobs;

    int rc = UNIMPLEMENTED;
    int bdberr;
    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = pCur->clnt;

    pKey = pPayload->pKey;
    nKey = pPayload->nKey;
    pData = pPayload->pData;
    nData = pPayload->nData;

    /* If pData is NULL, ondisk_buf and ondisk_blobs contain comdb2 row data
       and are ready to be inserted as is. */
    if (pData == NULL) {
        pblobs = pCur->ondisk_blobs;
    } else {
        pblobs = alloca(sizeof(blob_buffer_t) * MAXBLOBS);
        memset(pblobs, 0, sizeof(blob_buffer_t) * MAXBLOBS);
    }

    if (thd && pCur->db == NULL) {
        thd->nwrite++;
        thd->cost += pCur->write_cost;
        pCur->nwrite++;
    }

    assert(0 == pCur->is_sampled_idx);

    CHECK_CLNT_READONLY_BUT_SQL_IS_WRITING(clnt, pCur);

    if (unlikely(pCur->cursor_class == CURSORCLASS_STAT24)) {
        rc = make_stat_record(thd, pCur, pData, nData, pblobs);
        if (rc) {
            char errs[128];
            convert_failure_reason_str(&thd->clnt->fail_reason,
                                       pCur->db->tablename, "SQLite format",
                                       ".ONDISK_IX_0", errs, sizeof(errs));
            fprintf(stderr, "%s: sqlite -> ondisk conversion failed!\n   %s\n",
                    __func__, errs);
            goto done;
        }
    }

    /* send opcode to reload stats at commit */
    if (clnt->is_analyze && pCur->db && is_stat1(pCur->db->tablename))
        rc = osql_updstat(pCur, thd, pCur->ondisk_buf, getdatsize(pCur->db), 0);

    if (pCur->bt->is_temporary) {
        /* data: nKey is 'rrn', pData is record, nData is size of record
         * index: pKey is key, nKey is size of key (no data) */
        UnpackedRecord *rec = NULL;
        if (pKey) {
            rec = sqlite3VdbeAllocUnpackedRecord(pCur->pKeyInfo);
            if (rec == 0) {
                logmsg(LOGMSG_ERROR, "Error rec is zero, returned from "
                                "sqlite3VdbeAllocUnpackedRecord()\n");
                return 0;
            }
            sqlite3VdbeRecordUnpack(pCur->pKeyInfo, nKey, pKey, rec);
        }

        if (pCur->ixnum == -1) {
            /* data */
            rc = pCur->cursor_put(thedb->bdb_env, pCur->tmptable->tbl,
                                  (void *)&nKey, sizeof(unsigned long long),
                                  (void *)pData, nData, rec, &bdberr, pCur);
        } else {
            /* key */
            rc = pCur->cursor_put(thedb->bdb_env, pCur->tmptable->tbl,
                                  (void *)pKey, nKey, (void *)pData, nData, rec,
                                  &bdberr, pCur);
        }
        if (rec != NULL) sqlite3DbFree(pCur->pKeyInfo->db, rec);

        if (rc) {
            logmsg(LOGMSG_ERROR, 
                   "sqlite3BtreeInsert:  table insert error rc = %d bdberr %d\n",
                   rc, bdberr);
            rc = SQLITE_INTERNAL;
            /* return rc; */
            goto done;
        }
        rc = SQLITE_OK;

    } else if (unlikely(pCur->rootpage == RTPAGE_SQLITE_MASTER)) {

        /* sqlite driven ddl, the only thing we support now are views */

        /* is this an update? no KeY! */
        if (pCur->tblpos == thd->rootpage_nentries) {
            /* we have positioned ourselves on the side row, this is an update!
            assert(nKey == 0 && pKey == NULL);
             */
        } else {
            /* an actual insert */
            clnt->keyDdl = pCur->keyDdl = nKey;
        }

        if (nData != pCur->nDataDdl) {
            clnt->dataDdl = pCur->dataDdl =
                (char *)realloc(pCur->dataDdl, nData);
            if (nData > 0 && !pCur->dataDdl) {
                logmsg(LOGMSG_ERROR, "%s malloc %d\n", __func__, nData);
                rc = SQLITE_ERROR;
                goto done;
            }
        }
        clnt->nDataDdl = pCur->nDataDdl = nData;
        if (pCur->nDataDdl) {
            memcpy(pCur->dataDdl, pData, pCur->nDataDdl);
        } else {
            /* really sqlite weirdness... we are passed a NULL one field row,
               we need to actually build a 6 NULL field  serialized structure
               to be retrieved later on ! This is not a problem for sqlite, but
               comdb2, which adds one additional field,csc2
               I have changed sqlite to explicitely set csc2 to NULL, so this
               step
               is not needed here, but left comment just in case
               */
        }

        rc = SQLITE_OK;

        goto done;

    } else { /* real insert */

        int is_update = 0;

        /* check authentication */
        if (authenticate_cursor(pCur, AUTHENTICATE_WRITE) != 0) {
            rc = SQLITE_ACCESS;
            goto done;
        }

        /* sanity check, this should never happen */
        if (!is_sql_update_mode(clnt->dbtran.mode)) {
            logmsg(LOGMSG_ERROR, 
                    "%s: calling insert/update in the wrong sql mode %d\n",
                    __func__, clnt->dbtran.mode);
            rc = UNIMPLEMENTED;
            goto done;
        }

        if (clnt->dbtran.maxchunksize > 0 &&
            clnt->dbtran.mode == TRANLEVEL_SOSQL &&
            clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
            if ((rc = chunk_transaction(pCur, clnt, thd)) != SQLITE_OK)
                goto done;
        }

        /* We ignore keys on insert but save dirty keys.
         * Keys are added if keys were set dirty when a record
         * (data portion) is inserted. */
        if (pCur->ixnum != -1) {
            if (gbl_partial_indexes)
                SQLITE3BTREE_KEY_SET_INS(pCur->ixnum);
            rc = SQLITE_OK;
            if (likely(pCur->cursor_class != CURSORCLASS_STAT24) &&
                likely(pCur->bt == NULL || pCur->bt->is_remote == 0) &&
                gbl_expressions_indexes && pCur->db->ix_expr) {
                rc = sqlite_to_ondisk(pCur->db->ixschema[pCur->ixnum], pKey, nKey, pCur->ondisk_key, clnt->tzname,
                                      pblobs, MAXBLOBS, &thd->clnt->fail_reason, pCur);
                if (rc != getkeysize(pCur->db, pCur->ixnum)) {
                    char errs[128];
                    convert_failure_reason_str(
                        &thd->clnt->fail_reason, pCur->db->tablename,
                        "SQLite format", ".ONDISK_ix", errs, sizeof(errs));
                    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                                "Moveto: sqlite_to_ondisk failed [%s]\n", errs);
                    sqlite3_mutex_enter(sqlite3_db_mutex(pCur->vdbe->db));
                    sqlite3VdbeError(pCur->vdbe, errs, (char *)0);
                    sqlite3_mutex_leave(sqlite3_db_mutex(pCur->vdbe->db));

                    rc = SQLITE_ERROR;
                    goto done;
                }
                assert(clnt->idxInsert[pCur->ixnum] == NULL);
                clnt->idxInsert[pCur->ixnum] = malloc(rc);
                if (clnt->idxInsert[pCur->ixnum] == NULL) {
                    logmsg(LOGMSG_ERROR, "%s:%d malloc %d failed\n", __func__,
                            __LINE__, rc);
                    rc = SQLITE_NOMEM;
                    goto done;
                }
                memcpy(clnt->idxInsert[pCur->ixnum], pCur->ondisk_key, rc);
                rc = 0;
            } else if (pCur->bt != NULL && pCur->bt->is_remote != 0 &&
                       pCur->fdbc->tbl_has_expridx(pCur)) {
                assert(clnt->idxInsert[pCur->ixnum] == NULL);
                clnt->idxInsert[pCur->ixnum] = malloc(sizeof(int) + nKey);
                if (clnt->idxInsert[pCur->ixnum] == NULL) {
                    logmsg(LOGMSG_ERROR, "%s:%d malloc %llu failed\n", __func__,
                           __LINE__, sizeof(int) + nKey);
                    rc = SQLITE_NOMEM;
                    goto done;
                }
                *((int *)clnt->idxInsert[pCur->ixnum]) = (int)nKey;
                memcpy((unsigned char *)clnt->idxInsert[pCur->ixnum] +
                           sizeof(int),
                       pKey, nKey);
            }
            goto done;
        }

        if (pCur->session_tbl) pCur->session_tbl->dirty = 1;

        if (likely(pCur->cursor_class != CURSORCLASS_STAT24) && likely(pCur->bt == NULL || pCur->bt->is_remote == 0) &&
            pData != NULL) {
            rc = sqlite_to_ondisk(pCur->db->schema, pData, nData, pCur->ondisk_buf, clnt->tzname, pblobs, MAXBLOBS,
                                  &thd->clnt->fail_reason, pCur);
            if (rc < 0) {
                char errs[128];
                convert_failure_reason_str(&thd->clnt->fail_reason,
                                           pCur->db->tablename, "SQLite format",
                                           ".ONDISK", errs, sizeof(errs));
                reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                            "Moveto: sqlite_to_ondisk failed [%s]\n", errs);
                sqlite3_mutex_enter(sqlite3_db_mutex(pCur->vdbe->db));
                sqlite3VdbeError(pCur->vdbe, errs, (char *)0);
                sqlite3_mutex_leave(sqlite3_db_mutex(pCur->vdbe->db));

                rc = SQLITE_ERROR;
                goto done;
            }
        }

        /* If it's a known invalid genid (see sqlite3BtreeNewRowid() for
         * rationale), don't bother actually looking for it. */
        if (!is_genid_synthetic(nKey) &&
            bdb_genid_is_recno(thedb->bdb_env, nKey))
            is_update = 0;
        else if (0 == pCur->genid)
            is_update = 0;
        else if (!bias)
            /* For ON CONFLICT DO REPLACE, the conflicting record could
             * have already been deleted before we reach here, and hence
             * pCur->genid != 0. Thus, we rely on bias to force an insert
             * and not update.
             */
            is_update = 0;
        else
            is_update = 1;

        if (pCur->bt == NULL || pCur->bt->is_remote == 0) {
            if (is_sqlite_stat(pCur->db->tablename)) {
                clnt->ins_keys = -1ULL;
                clnt->del_keys = -1ULL;
            }

            int rec_flags = 0;
            if (flags != 0) {
                /* Set the UPSERT related flags. */
                if (flags & OPFLAG_IGNORE_FAILURE) {
                    /* For ON CONFLICT(opt-target-index) DO NOTHING, the lower
                     * 8 bits of the rec_flags store the OSQL_IGNORE_FAILURE
                     * (signifying DO NOTHING), and higher bits store the index
                     * number specified in the ON CONFLICT target. Use of no
                     * target implies the implicit inclusion of all unique
                     * indexes. This is represented by storing MAXINDEX+1 in
                     * the higher bits instead.
                     */
                    rec_flags = ((comdb2UpsertIdx(pCur->vdbe) << 8) |
                                 OSQL_IGNORE_FAILURE);
                } else if (flags & OPFLAG_FORCE_VERIFY) {
                    rec_flags = OSQL_FORCE_VERIFY;
                }
            }

            if (is_update) { /* Updating an existing record. */
                rc = osql_updrec(pCur, thd, pCur->ondisk_buf, getdatsize(pCur->db), pCur->vdbe->updCols, pblobs,
                                 MAXBLOBS, rec_flags);
                clnt->effects.num_updated++;
                clnt->log_effects.num_updated++;
                clnt->nrows++;
            } else {
                rc = osql_insrec(pCur, thd, pCur->ondisk_buf, getdatsize(pCur->db), pblobs, MAXBLOBS, rec_flags);
                clnt->effects.num_inserted++;
                clnt->log_effects.num_inserted++;
                clnt->nrows++;
            }
        } else {
            /* make sure we have a distributed transaction and use that to
             * update remote */
            uuid_t tid;
            fdb_tran_t *trans = fdb_trans_begin_or_join(
                clnt, pCur->bt->fdb, (char *)tid, 0 /*TODO*/);

            if (!trans) {
                logmsg(LOGMSG_ERROR, 
                       "%s:%d failed to create or join distributed transaction!\n",
                       __func__, __LINE__);
                return SQLITE_INTERNAL;
            }

            if (is_sqlite_stat(pCur->fdbc->name(pCur))) {
                clnt->ins_keys = -1ULL;
                clnt->del_keys = -1ULL;
            }
            if (is_update) {
                rc = pCur->fdbc->update(pCur, clnt, trans, pCur->genid, nKey,
                                        nData, (char *)pData);
                clnt->effects.num_updated++;
                clnt->nrows++;
            } else {
                rc = pCur->fdbc->insert(pCur, clnt, trans, nKey, nData,
                                        (char *)pData);
                clnt->effects.num_inserted++;
                clnt->nrows++;
            }
        }
        clnt->ins_keys = 0ULL;
        clnt->del_keys = 0ULL;
        if (gbl_expressions_indexes) {
            free_cached_idx(clnt->idxInsert);
            free_cached_idx(clnt->idxDelete);
        }
        if (rc == SQLITE_DDL_MISUSE)
        {
            sqlite3_mutex_enter(sqlite3_db_mutex(pCur->vdbe->db));
            sqlite3VdbeError(pCur->vdbe,
                             "Transactional DDL Error: Overlapping Tables");
            sqlite3_mutex_leave(sqlite3_db_mutex(pCur->vdbe->db));
        }
    }

done:
    free_blob_buffers(pblobs, MAXBLOBS);
    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE, "Insert(pCur %d)      = %s\n",
                pCur->cursorid, sqlite3ErrStr(rc));
    return rc;
}

/*
** Advance the cursor to the next entry in the database. 
** Return value:
**
**    SQLITE_OK        success
**    SQLITE_DONE      cursor is already pointing at the last element
**    otherwise        some kind of error occurred
**
** The main entry point is sqlite3BtreeNext().  That routine is optimized
** for the common case of merely incrementing the cell counter BtCursor.aiIdx
** to the next cell on the current page.  The (slower) btreeNext() helper
** routine is called when it is necessary to move to a different page or
** to restore the cursor.
**
** If bit 0x01 of the F argument in sqlite3BtreeNext(C,F) is 1, then the
** cursor corresponds to an SQL index and this routine could have been
** skipped if the SQL index had been a unique index.  The F argument
** is a hint to the implement.  SQLite btree implementation does not use
** this hint, but COMDB2 does.
*/
int sqlite3BtreeNext(BtCursor *pCur, int flags)
{
    int fndlen;
    void *buf;
    int *pRes = &flags;

    if (pCur->empty || move_is_nop(pCur, pRes)) {
        return SQLITE_DONE;
    }

    int rc = pCur->cursor_move(pCur, pRes, CNEXT);
    if( *pRes==1 ) rc = SQLITE_DONE;

    if (pCur->range && pCur->db && !pCur->range->islocked) {
        if (pCur->ixnum == -1) {
            pCur->range->islocked = 1;
            pCur->range->lflag = 1;
            pCur->range->rflag = 1;
            if (pCur->range->rkey) {
                free(pCur->range->rkey);
                pCur->range->rkey = NULL;
            }
            if (pCur->range->lkey) {
                free(pCur->range->lkey);
                pCur->range->lkey = NULL;
            }
            pCur->range->lkeylen = 0;
            pCur->range->rkeylen = 0;
        } else if (*pRes == 1 || rc != SQLITE_OK) {
            pCur->range->rflag = 1;
            if (pCur->range->rkey) {
                free(pCur->range->rkey);
                pCur->range->rkey = NULL;
            }
            pCur->range->rkeylen = 0;
        } else if (pCur->bdbcur && pCur->range->rflag == 0) {
            fndlen = pCur->bdbcur->datalen(pCur->bdbcur);
            buf = pCur->bdbcur->data(pCur->bdbcur);
            if (pCur->range->rkey) {
                free(pCur->range->rkey);
                pCur->range->rkey = NULL;
            }
            pCur->range->rkey = malloc(fndlen);
            pCur->range->rkeylen = fndlen;
            memcpy(pCur->range->rkey, buf, fndlen);
        }
        if (pCur->range->lflag && pCur->range->rflag)
            pCur->range->islocked = 1;
    }

    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                "Next(pCur %d, valid %s)      = %s\n", pCur->cursorid,
                !*pRes ? "yes" : "no", sqlite3ErrStr(rc));
    return rc;
}

/*
 ** Read part of the data associated with cursor pCur.  Exactly
 ** "amt" bytes will be transfered into pBuf[].  The transfer
 ** begins at "offset".
 **
 ** Return SQLITE_OK on success or an error code if anything goes
 ** wrong.  An error is returned if "offset+amt" is larger than
 ** the available payload.
 */
int sqlite3BtreeData(BtCursor *pCur, u32 offset, u32 amt, void *pBuf)
{
    int rc = SQLITE_OK;
    char *dta;

    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                "Data(pCur %d, offset %d, amount %d)      = %s\n",
                pCur->cursorid, offset, amt, sqlite3ErrStr(rc));
    /* note: this assumes higher-level code has done necessary
     * range checks (ie: won't ask for bytes past end of key */
    assert(0 == pCur->is_sampled_idx);
    if (pCur->bt->is_temporary) {
        dta = bdb_temp_table_data(pCur->tmptable->cursor);
        if (dta == NULL) {
            logmsg(LOGMSG_ERROR, "sqlite3BtreeData: AAAAAAAAAAAAAAA\n");
            return SQLITE_INTERNAL;
        }
        memcpy(pBuf, dta + offset, amt);
    } else if (pCur->ixnum == -1) {
        memcpy(pBuf, ((char *)pCur->dtabuf) + offset, amt);
    } else if (pCur->bt->is_remote) {

        assert(pCur->fdbc);

        dta = pCur->fdbc->data(pCur);
        memcpy(pBuf, dta, amt);
    } else {
        memcpy(pBuf, pCur->sqlrrn + offset, amt);
    }
    reqlog_loghex(pCur->bt->reqlogger, REQL_TRACE, pBuf, amt);
    reqlog_logl(pCur->bt->reqlogger, REQL_TRACE, "\n");
    return rc;
}

/*
 ** This routine does a complete check of the given BTree file.  aRoot[] is
 ** an array of pages numbers were each page number is the root page of
 ** a table.  nRoot is the number of entries in aRoot.
 **
 ** If everything checks out, this routine returns NULL.  If something is
 ** amiss, an error message is written into memory obtained from malloc()
 ** and a pointer to that error message is returned.  The calling function
 ** is responsible for freeing the error message when it is done.
 */
char *sqlite3BtreeIntegrityCheck(Btree *pBt, int *aRoot, int nRoot, int mxErr,
                                 int *pnErr)
{
    int rc = SQLITE_OK;
    int i;

    *pnErr = 0;

    reqlog_logf(pBt->reqlogger, REQL_TRACE, "IntegrityCheck(pBt %d pages",
                pBt->btreeid);
    for (i = 0; i < nRoot; i++) {
        reqlog_logf(pBt->reqlogger, REQL_TRACE, "%d ", aRoot[i]);
    }
    reqlog_logf(pBt->reqlogger, REQL_TRACE, ")    = %s\n", sqlite3ErrStr(rc));
    return NULL;
}

/* obtain comdb2_rowid and optionally print it as a decimal string */
int sqlite3BtreeGetGenId(
  unsigned long long rowId,   /* IN: Original rowId (from the BtCursor). */
  unsigned long long *pGenId, /* OUT, OPT: The genId, if requested. */
  char **pzGenId,             /* OUT, OPT: Modified genId as decimal string. */
  int *pnGenId                /* OUT, OPT: Size of string buffer. */
){
  unsigned long long prgenid; /* always printed & returned in big-endian */
  prgenid = flibc_htonll(rowId);
  if( pGenId ) *pGenId = prgenid;
  if( pzGenId && pnGenId ){
    char *zGenId = sqlite3_mprintf("2:%llu", prgenid);
    if( zGenId==NULL ){
      return SQLITE_NOMEM;
    }
    *pzGenId = zGenId;
    *pnGenId = (int)strlen(zGenId); /* BUGFIX: Actual len for OP_Ne, etc. */
  }
  return SQLITE_OK;
}

/*
 ** Reset the btree and underlying pager after a malloc() failure. Any
 ** transaction that was active when malloc() failed is rolled back.
 */
int sqlite3BtreeReset(Btree *pBt) { return SQLITE_OK; }

/*
 ** Return TRUE if the given btree is set to safety level 1.  In other
 ** words, return TRUE if no sync() occurs on the disk files.
 */
int sqlite3BtreeSyncDisabled(Btree *pBt) { return 0; }

/*
 ** This function returns a pointer to a blob of memory associated with
 ** a single shared-btree. The memory is used by client code for it's own
 ** purposes (for example, to store a high-level schema associated with
 ** the shared-btree). The btree layer manages reference counting issues.
 **
 ** The first time this is called on a shared-btree, nBytes bytes of memory
 ** are allocated, zeroed, and returned to the caller. For each subsequent
 ** call the nBytes parameter is ignored and a pointer to the same blob
 ** of memory returned.
 **
 ** Just before the shared-btree is closed, the function passed as the
 ** xFree argument when the memory allocation was made is invoked on the
 ** blob of allocated memory. This function should not call sqliteFree()
 ** on the memory, the btree layer does that.
 */
void *sqlite3BtreeSchema(Btree *pBt, int nBytes, void (*xFree)(void *))
{
    if (!pBt->is_remote) {
        pBt->schema = calloc(1, nBytes);
        pBt->free_schema = xFree;
    } else {
        /* I will cache the schema-s for foreign dbs. Let this be the first
         * step to share the schema for all database connections */
        pBt->schema = fdb_sqlite_get_schema(pBt, nBytes);

        /* We ignore Xfree since this is shared and not part of a sqlite engine
         * space.
         *
         * However, if query_preparer plugin is loaded/enabled, as we do not
         * cache its db (sqlitex) handle. Thus, we will have to set free_schema
         * to avoid memory leaks. */
        struct sql_thread *thd = pthread_getspecific(query_info_key);
        if (gbl_old_column_names && thd && thd->clnt && thd->clnt->thd &&
            thd->clnt->thd->query_preparer_running) {
            assert(query_preparer_plugin);
            /* sqlitex */
            pBt->free_schema = xFree;
        }
    }
    return pBt->schema;
}

/*
 ** Return true if another user of the same shared btree as the argument
 ** handle holds an exclusive lock on the sqlite_master table.
 */
int sqlite3BtreeSchemaLocked(Btree *pBt)
{
    /* This is managed in higher layers */
    return 0;
}

/*
 ** This is not an sqlite routine. Added this so I can fetch real
 ** comdb2 genids from sqlite without further kludgery. Called from
 ** sqlit3VdbeExec to get a unique id for a new record (OP_NewRowid).
 ** The alternative was keeping a mapping table between sqlite and
 ** comdb2 record ids - too expensive.
 */
i64 sqlite3BtreeNewRowid(BtCursor *pCur)
{
    if (!pCur) {
        return 0;
    }
    struct sql_thread *thd = pCur->thd;
    if (!thd) {
        abort();
    }
    if (pCur->bt->is_temporary && pCur->tmptable) {
        return pCur->cursor_rowid(pCur->tmptable->tbl, pCur);
    }
    return bdb_recno_to_genid(++thd->clnt->recno);
}

char *sqlite3BtreeGetTblName(BtCursor *pCur)
{
    return pCur->db->tablename;
}

int sqlite3MakeRecordForComdb2(BtCursor *pCur, Mem *head, int nf, int *optimized)
{
    struct sql_thread *thd = pCur->thd;
    struct mem_info info;
    struct field_conv_opts_tz convopts = {.flags = 0};
    int nblobs = 0;
    int rc = 0;
    int i, min;

    if (pCur->cursor_class != CURSORCLASS_TABLE || /* Data only */
        pCur->rootpage == RTPAGE_SQLITE_MASTER ||  /* No DDL */
        pCur->bt == NULL ||                        /* Must point to something */
        pCur->bt->is_temporary ||                  /* No temptable */
        pCur->bt->is_remote ||                     /* No fdb */
        pCur->bt->is_hashtable /* No hashtable */) {
        *optimized = 0;
        return 0;
    }

    info.s = pCur->db->schema;
    info.fail_reason = &thd->clnt->fail_reason;
    info.tzname = thd->clnt->tzname;
    info.nblobs = &nblobs;
    info.convopts = &convopts;
    info.outblob = pCur->ondisk_blobs;
    info.maxblobs = MAXBLOBS;

    memset(info.outblob, 0, sizeof(blob_buffer_t) * MAXBLOBS);
    init_convert_failure_reason(info.fail_reason);

    for (i = 0, min = (info.s->nmembers < nf) ? info.s->nmembers : nf; i != min; ++i) {
        info.fldidx = i;
        info.m = &head[i];
        info.null = (info.m->uTemp == 0);
        if ((rc = mem_to_ondisk(pCur->ondisk_buf, &info.s->member[i], &info, NULL)) < 0) {
            char errs[128];
            convert_failure_reason_str(&thd->clnt->fail_reason, pCur->db->tablename, "SQLite format", ".ONDISK", errs,
                                       sizeof(errs));
            reqlog_logf(pCur->bt->reqlogger, REQL_TRACE, "Moveto: sqlite_to_ondisk failed [%s]\n", errs);
            sqlite3_mutex_enter(sqlite3_db_mutex(pCur->vdbe->db));
            sqlite3VdbeError(pCur->vdbe, errs, (char *)0);
            sqlite3_mutex_leave(sqlite3_db_mutex(pCur->vdbe->db));
            rc = SQLITE_ERROR;
            break;
        }
    }

    if (rc == 0)
        *optimized = 1;
    return rc;
}

char *get_dbtable_name(struct dbtable *tbl)
{
    return tbl->tablename;
}

void cancel_sql_statement(int id)
{
    int found = 0;
    struct sql_thread *thd;

    Pthread_mutex_lock(&gbl_sql_lock);
    LISTC_FOR_EACH(&thedb->sql_threads, thd, lnk)
    {
        if (thd->id == id && thd->clnt) {
            found = 1;
            /* Previously the flag was defined in the clnt struct and we would do
                   ```thd->clnt->stop_this_statement = 1```

               this wasn't thread-safe: thd->clnt might be set to NULL
               after the if check but before the line above, by the sql thread.
               The race condition would crash the database.

               To fix this we move the flag up to the sql_thread struct.
               The code is protected by the gbl_sql_lock so `thd' can't disappear
               under our noses. */
            thd->stop_this_statement = 1;
            break;
        }
    }
    Pthread_mutex_unlock(&gbl_sql_lock);
    if (found)
        logmsg(LOGMSG_USER, "Query %d was told to stop\n", id);
    else
        logmsg(LOGMSG_USER, "Query %d not found (finished?)\n", id);
}

/* cancel sql statement with the given cnonce */
void cancel_sql_statement_with_cnonce(const char *cnonce)
{
    if(!cnonce) return;

    struct sql_thread *thd;
    int found = 0;
    snap_uid_t snap;
    size_t cnonce_len = strlen(cnonce);

    Pthread_mutex_lock(&gbl_sql_lock);
    LISTC_FOR_EACH(&thedb->sql_threads, thd, lnk)
    {
        if (thd->clnt && get_cnonce(thd->clnt, &snap) == 0) {
            if (snap.keylen != cnonce_len)
                continue;
            found = (memcmp(snap.key, cnonce, cnonce_len) == 0);
            if (found) {
                /* See comments in cancel_sql_statement() */
                thd->stop_this_statement = 1;
                break;
            }
        }
    }
    Pthread_mutex_unlock(&gbl_sql_lock);
    if (found)
        logmsg(LOGMSG_USER, "Query with cnonce %s was told to stop\n", cnonce);
    else
        logmsg(LOGMSG_USER, "Query with cnonce %s not found (finished?)\n", cnonce);
}

void sql_dump_running_statements(void)
{
    struct sql_thread *thd;
    struct sqlclntstate *clnt;
    BtCursor *cur;
    struct tm tm;
    char rqid[50];

    Pthread_mutex_lock(&gbl_sql_lock);
    LISTC_FOR_EACH(&thedb->sql_threads, thd, lnk)
    {
        time_t t = thd->stime;
        localtime_r((time_t *)&t, &tm);
        Pthread_mutex_lock(&thd->lk);

        if ((clnt = thd->clnt) != NULL && clnt->sql) {
            if (clnt->osql.rqid) {
                uuidstr_t us;
                snprintf(rqid, sizeof(rqid), "txn %016llx %s", clnt->osql.rqid, comdb2uuidstr(clnt->osql.uuid, us));
            } else
                rqid[0] = 0;

            logmsg(LOGMSG_USER, "id %d %02d/%02d/%02d %02d:%02d:%02d %s%s pid %d task %s ", thd->id, tm.tm_mon + 1,
                   tm.tm_mday, 1900 + tm.tm_year, tm.tm_hour, tm.tm_min, tm.tm_sec, rqid, clnt->origin,
                   clnt->conninfo.pid, clnt->argv0 ? clnt->argv0 : "???");
            if (clnt->osql.replay != OSQL_RETRY_NONE)
                logmsg(LOGMSG_USER, "[replay] ");
            logmsg(LOGMSG_USER, "%s\n", clnt->sql);

            int nparams = clnt->plugin.param_count(clnt);
            char param[255];
            for (int i = 0; i < nparams; i++) {
                char *value = param_string_value(clnt, i, param, sizeof(param));
                if (value)
                    logmsg(LOGMSG_USER, "    %s\n", value);
            }


            if (thd->bt) {
                LISTC_FOR_EACH(&thd->bt->cursors, cur, lnk)
                {
                    logmsg(LOGMSG_USER, "  cursor on");
                    if (cur->db == NULL)
                        logmsg(LOGMSG_USER, " schema table");
                    else if (cur->ixnum == -1)
                        logmsg(LOGMSG_USER, " table %s", cur->db->tablename);
                    else
                        logmsg(LOGMSG_USER, " table %s index %d",
                               cur->db->tablename, cur->ixnum);
                    logmsg(LOGMSG_USER, " nmove %d nfind %d nwrite %d\n", cur->nmove,
                           cur->nfind, cur->nwrite);
                }
            }
            if (thd->bttmp) {
                LISTC_FOR_EACH(&thd->bttmp->cursors, cur, lnk)
                {
                    logmsg(LOGMSG_USER, "  cursor on temp nmove %d nfind %d nwrite %d\n",
                           cur->nmove, cur->nfind, cur->nwrite);
                }
            }
        }
        Pthread_mutex_unlock(&thd->lk);
    }
    Pthread_mutex_unlock(&gbl_sql_lock);
}

/*
 ** Obtain a lock on the table whose root page is iTab.  The
 ** lock is a write lock if isWritelock is true or a read lock
 ** if it is false.
 */
int sqlite3BtreeLockTable(Btree *p, int iTab, u8 isWriteLock) { return 0; }

int osql_check_shadtbls(bdb_state_type *bdb_env, struct sqlclntstate *clnt,
                        char *file, int line);

int gbl_random_get_curtran_failures;

int get_curtran_flags(bdb_state_type *bdb_state, struct sqlclntstate *clnt,
                      uint32_t flags)
{
    cursor_tran_t *curtran_out = NULL;
    int retries = 0;
    uint32_t curgen;
    int bdberr = 0;
    int max_retries =
        gbl_move_deadlk_max_attempt >= 0 ? gbl_move_deadlk_max_attempt : 500;
    int lowpri = gbl_lowpri_snapisol_sessions &&
                 (clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
                  clnt->dbtran.mode == TRANLEVEL_SERIAL);
    int curtran_flags = lowpri ? BDB_CURTRAN_LOW_PRIORITY : 0;
    int rc = 0;

    /*fprintf(stderr, "get_curtran\n"); */

    if (clnt->dbtran.cursor_tran) {
        logmsg(LOGMSG_ERROR, "%s called when we have a curtran\n", __func__);
        return -1;
    }

    if (gbl_random_get_curtran_failures &&
        !(rand() % gbl_random_get_curtran_failures)) {
        logmsg(LOGMSG_ERROR, "%s forcing a random curtran failure\n", __func__);
        return -1;
    }

    if (clnt->gen_changed) {
        logmsg(LOGMSG_DEBUG, "td %p %s line %d calling get_curtran on gen_changed\n", (void *)pthread_self(), __func__,
               __LINE__);
    }

retry:
    bdberr = 0;
    curtran_out = bdb_get_cursortran(bdb_state, curtran_flags, &bdberr);
    if (bdberr == BDBERR_DEADLOCK) {
        retries++;
        if (!max_retries || retries < max_retries)
            goto retry;
        logmsg(LOGMSG_ERROR, "%s: too much contention\n", __func__);
    }
    if (!curtran_out)
        return -1;

    /* If this is an hasql serialiable or snapshot session and durable-lsns are
     * enabled, then fail this call with a 'CHANGENODE' if the generation number
     * has changed: we don't verify that transactions are 'DURABLE' before
     * applying them to the shadow tables.  If it's not-durable, the records
     * that we are compensating for in our shadows can be rolled back by a new
     * master (which means that our shadow-tables will be incorrect).  */

    extern int gbl_test_curtran_change_code;
    curgen = bdb_get_rep_gen(bdb_state);
    if ((clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
         clnt->dbtran.mode == TRANLEVEL_SERIAL) &&
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DURABLE_LSNS) &&
        clnt->init_gen &&
        ((clnt->init_gen != curgen) ||
         (gbl_test_curtran_change_code && (0 == (rand() % 1000))))) {
        bdb_put_cursortran(bdb_state, curtran_out, curtran_flags, &bdberr);
        curtran_out = NULL;
        clnt->gen_changed = 1;
        logmsg(LOGMSG_DEBUG,
               "td %p %s: failing because generation has changed: "
               "orig-gen=%u, cur-gen=%u\n",
               (void *)pthread_self(), __func__, clnt->init_gen, curgen);
        return -1;
    }

    if (!clnt->init_gen)
        clnt->init_gen = curgen;

    clnt->dbtran.cursor_tran = curtran_out;

    assert(curtran_out != NULL);

    /* check if we have table locks that were dropped during recovery */
    rc = 0;
    if (clnt->recover_ddlk) {
        rc = clnt->recover_ddlk(clnt);
    } else if (clnt->dbtran.pStmt) {
        rc = sqlite3LockStmtTablesRecover(clnt->dbtran.pStmt);
    }
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to lock back tables!, rc %d\n", __func__, rc);
        bdb_put_cursortran(bdb_state, curtran_out, curtran_flags, &bdberr);
        clnt->dbtran.cursor_tran = NULL;
    } else {
        /* NOTE: we need to make sure the versions are the same */
    }
    return rc;
}

int get_curtran(bdb_state_type *bdb_state, struct sqlclntstate *clnt)
{
    return get_curtran_flags(bdb_state, clnt, 0);
}

int put_curtran_flags(bdb_state_type *bdb_state, struct sqlclntstate *clnt,
                      uint32_t flags)
{
    int rc = 0;
    int is_recovery = (flags & CURTRAN_RECOVERY);
    int curtran_flags = 0;
    int bdberr = 0;

#ifdef DEBUG_TRAN
    fprintf(stderr, "%p, %s\n", (void *)pthread_self(), __func__);
#endif

    if (!is_recovery) {
        if (clnt->dbtran.nLockedRemTables > 0) {
            int irc = sqlite3UnlockStmtTablesRemotes(clnt);
            if (irc) {
                logmsg(LOGMSG_ERROR,
                       "%s: error releasing remote tables rc=%d\n", __func__,
                       irc);
            }
        }

        clnt->dbtran.pStmt = NULL; /* this is pointing to freed memory at this point */
    }

    clnt->is_overlapping = 0;

    if (!clnt->dbtran.cursor_tran) {
        /* this should be visible */
        logmsg(LOGMSG_ERROR, "%s called without curtran\n", __func__);
        return 0;
    }

    rc = bdb_put_cursortran(bdb_state, clnt->dbtran.cursor_tran, curtran_flags,
                            &bdberr);

    if (clnt->dbtran.views_lk_held) {
        Pthread_rwlock_unlock(&views_lk);
        clnt->dbtran.views_lk_held = 0;
    }

    if (rc) {
        logmsg(LOGMSG_DEBUG, "%s: %p rc %d bdberror %d\n", __func__, (void *)pthread_self(), rc, bdberr);
        ctrace("%s: rc %d bdberror %d\n", __func__, rc, bdberr);
        if (bdberr == BDBERR_BUG_KILLME) {
            /* should I panic? */
            logmsg(LOGMSG_FATAL, "This db is leaking locks, it will deadlock!!! \n");
            bdb_dump_active_locks(bdb_state, stdout);
            exit(1);
        }
    }

    clnt->dbtran.cursor_tran = NULL;

    return rc;
}

int put_curtran(bdb_state_type *bdb_state, struct sqlclntstate *clnt)
{
    return put_curtran_flags(bdb_state, clnt, 0);
}

/* Return a count of all cursors to the lower layer for debugging */
int count_pagelock_cursors(void *arg)
{
    struct sql_thread *thd = (struct sql_thread *)arg;
    int count = 0;

    Pthread_mutex_lock(&thd->lk);
    count = listc_size(&thd->bt->cursors);
    Pthread_mutex_unlock(&thd->lk);

    return count;
}

/* Pause all pagelock cursors before grabbing a rowlock */
int pause_pagelock_cursors(void *arg)
{
    BtCursor *cur = NULL;
    struct sql_thread *thd = (struct sql_thread *)arg;
    struct sqlclntstate *clnt = thd->clnt;
    int bdberr;

    /* Return immediately if nothing is holding a pagelock */
    if (!clnt->holding_pagelocks_flag)
        return 0;

    Pthread_mutex_lock(&thd->lk);

    LISTC_FOR_EACH(&thd->bt->cursors, cur, lnk)
    {
        if (cur->bdbcur) {
            int rc = cur->bdbcur->pause(cur->bdbcur, &bdberr);
            if (0 == rc)
                abort();
        }
    }

    Pthread_mutex_unlock(&thd->lk);

    clnt->holding_pagelocks_flag = 0;
    return 0;
}

/* set every pCur->db and pCur->sc to NULL because they might
 * not be valid anymore after a schema change
 */
static void recover_deadlock_sc_cleanup(struct sql_thread *thd)
{
    BtCursor *cur = NULL;
    Pthread_mutex_lock(&thd->lk);
    if (thd->bt) {
        LISTC_FOR_EACH(&thd->bt->cursors, cur, lnk)
        {
            if (!cur->bt->is_remote && cur->db) {
                cur->db = NULL;
                cur->sc = NULL;
            }
        }
    }
    Pthread_mutex_unlock(&thd->lk);
}

static void unlock_bdb_cursors(struct sql_thread *thd, bdb_cursor_ifn_t *bdbcur,
                               int *bdberr)
{
    BtCursor *cur = NULL;

    /* unlock cursors */
    Pthread_mutex_lock(&thd->lk);

    if (thd->bt) {
        LISTC_FOR_EACH(&thd->bt->cursors, cur, lnk)
        {
            if (cur->bdbcur) {
                if (cur->bdbcur->unlock(cur->bdbcur, bdberr))
                    ctrace("%s: cur ixnum=%d bdberr = %d [1]\n", __func__,
                           cur->ixnum, *bdberr);
            }
        }
    }

    if (bdbcur) {
        if (bdbcur->unlock(bdbcur, bdberr))
            ctrace("%s: cur ixnum=%d bdberr = %d [2]\n", __func__, cur->ixnum,
                   *bdberr);
    }

    Pthread_mutex_unlock(&thd->lk);
}

/**
 * This open a new curtran and walk the list of BtCursors,
 * repositioning any cursor that has a bdbcursor (by closing, reopening
 * the underlying berkdb cursors while preserving the stage)
 *

 sleepms is used as both a sleep time and a boolean.  if we pass non 0,
 that means we want to relinquish ALL resources including our logical
 transaction. (and sleep).  during normal deadlock processing neither
 of these operations make sense.  it makes sense to call it this way
 when the intent is getting us out of bdblib so someone who wants the
 bdb writelock can get a chance to get in.

 */
/* BIG NOTE:
   carefull about double calling this function
 */
static int recover_deadlock_flags_int(bdb_state_type *bdb_state,
                                      struct sql_thread *thd,
                                      bdb_cursor_ifn_t *bdbcur, int sleepms,
                                      const char *func, int line,
                                      uint32_t flags)
{
    int ignore_desired = flags & RECOVER_DEADLOCK_IGNORE_DESIRED;
    int ptrace = (flags & RECOVER_DEADLOCK_PTRACE);
    int force_fail = (flags & RECOVER_DEADLOCK_FORCE_FAIL);
    int fail_type = 0;
    struct sqlclntstate *clnt = thd->clnt;
    BtCursor *cur = NULL;
    int rc = 0;
    uint32_t curtran_flags;
    int bdberr;

    if (!clnt->in_sqlite_init) {
        assert_no_schema_lk();
    }

    if (clnt->recover_deadlock_rcode) {
        assert(bdb_lockref() == 0);
        return clnt->recover_deadlock_rcode;
    }

    /*
     * BIG NOTE: if there is no cursor tran, do not get one here!
     *
     * Example: we call this when starting a transaction, or
     * waiting for a transaction to provide an rc; remote tran
     * does not have a curtran to span all transaction (instead,
     * it allocates curtrans for individual operations, like
     * begin&commit regular workflow)
     *
     */
    if (!clnt->dbtran.cursor_tran) {
        return 0;
    }

    assert(bdb_lockref() > 0);
    int new_mode = debug_switch_recover_deadlock_newmode();

    if (ignore_desired || bdb_lock_desired(thedb->bdb_env)) {
        if (!sleepms)
            sleepms = 2000;

        logmsg(LOGMSG_ERROR, "THD %p:recover_deadlock, and lock desired\n", (void *)pthread_self());
    } else if (ptrace)
        logmsg(LOGMSG_INFO, "THD %p:recover_deadlock\n", (void *)pthread_self());

    /* increment global counter */
    gbl_sql_deadlock_reconstructions++;

    unlock_bdb_cursors(thd, bdbcur, &bdberr);

    curtran_flags = CURTRAN_RECOVERY;
    /* free curtran */
    rc = put_curtran_flags(thedb->bdb_env, clnt, curtran_flags);
    assert(bdb_lockref() == 0);
    if (rc) {
        if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DURABLE_LSNS)) {
            logmsg(LOGMSG_ERROR, 
                    "%s: fail to put curtran, rc=%d, return changenode\n",
                    __func__, rc);
            return SQLITE_CLIENT_CHANGENODE;
        } else {
            logmsg(LOGMSG_ERROR, "%s: fail to close curtran, rc=%d\n", __func__, rc);
            return -300;
        }
    }

    if (unlikely(gbl_sql_random_release_interval)) {
        logmsg(LOGMSG_INFO, "%s: sleeping 10s\n", __func__);
        sleep(10);
        logmsg(LOGMSG_INFO, "%s: done sleeping\n", __func__);
    }

    /* NOTE: as empirically proven, bdb lock starvation does occur in the wild
       one of the issues is that sleeping tasks are jumping back into the fray
       before bdb write lock is acquired, slowing down upgrade long enough that
       cluster needs another master.  If all replicants are sql busy, we end up
       running master on the downgraded node (since noone will be able to
       upgrade
       fast enough;
       We can fix that by sleeping here until write lock is acquired at least
       one
       We will see how this fair with bdb_verify, the other bdb write lock
       gobbler
       */

    if (debug_switch_poll_on_lock_desired()) {
        if (bdb_lock_desired(thedb->bdb_env)) {
            while (bdb_lock_desired(thedb->bdb_env)) {
                poll(NULL, 0, 10);
            }

        } else {
            /* if bdb lock is NOT desired but caller wanted to wait, respect
             * that */
            if (sleepms > 0) {
                poll(NULL, 0, sleepms);
            }
        }
    } else {
        /* we have released all of our bdb level locks at this point (in this
         * thread, there may be others.  */
        if (sleepms > 0)
            poll(NULL, 0, sleepms);
    }

    /* Fake generation-changed failure */
    if (force_fail) {
        if (!(fail_type = (rand() % 2))) {
            clnt->gen_changed = 1;
            rc = -1;
        } else {
            rc = SQLITE_SCHEMA;
        }
    } else
        rc = get_curtran_flags(thedb->bdb_env, clnt, curtran_flags);

    if (rc) {
        char *err;
        if (rc == SQLITE_SCHEMA || rc == SQLITE_COMDB2SCHEMA) {
            err = "Database was schema changed";
            rc = SQLITE_COMDB2SCHEMA;
            logmsg(LOGMSG_ERROR, "%s: failing with SQLITE_COMDB2SCHEMA\n", __func__);
        } else if (clnt->gen_changed) {
            err = "New master under snapshot";
            rc = SQLITE_CLIENT_CHANGENODE;
            logmsg(LOGMSG_ERROR,
                   "%s: fail to open a new curtran, rc=%d, return changenode\n",
                   __func__, rc);
        } else {
            err = "Failed to reaquire locks on deadlock";
            rc = ERR_RECOVER_DEADLOCK;
            logmsg(LOGMSG_ERROR, "%s: fail to open a new curtran, rc=%d\n", __func__, rc);
        }
        if (clnt->recover_ddlk_fail) {
            clnt->recover_ddlk_fail(clnt, err);
        } else if (clnt->dbtran.pStmt) {
            struct Vdbe *vdbe = (struct Vdbe *)clnt->dbtran.pStmt;
            sqlite3_mutex_enter(sqlite3_db_mutex(vdbe->db));
            sqlite3VdbeError(vdbe, err);
            sqlite3_mutex_leave(sqlite3_db_mutex(vdbe->db));
        }
        return rc;
    }

    /* no need to mess with our shadow tran, right? */

    /* now that we have a new curtran, try to reposition them */
    Pthread_mutex_lock(&thd->lk);
    if (thd->bt) {
        LISTC_FOR_EACH(&thd->bt->cursors, cur, lnk)
        {
            if (cur->bdbcur) {
                if (new_mode)
                    rc = cur->bdbcur->set_curtran(cur->bdbcur,
                                                  clnt->dbtran.cursor_tran);
                else
                    rc =
                        cur->bdbcur->lock(cur->bdbcur, clnt->dbtran.cursor_tran,
                                          BDB_SET, &bdberr);
                if (rc != 0) {
                    /* it is possible that the row is already gone
                     * when we try to reposition, since the lock is lost
                     * in this case we simply return -1; which forces
                     * the caller to report SQLITE_DEADLOCK back to sql engine
                     */
                    Pthread_mutex_unlock(&thd->lk);
                    logmsg(LOGMSG_ERROR, "bdb_cursor_lock returned %d %d\n", rc,
                            bdberr);
                    return -700;
                }
            }

            if (!cur->bt->is_remote && cur->db) {
                if (cur->tableversion != cur->db->tableversion) {
                    Pthread_mutex_unlock(&thd->lk);
                    logmsg(LOGMSG_ERROR,
                           "%s: table version for %s changed from %d to %lld\n",
                           __func__, cur->db->tablename, cur->tableversion,
                           cur->db->tableversion);
                    sqlite3_mutex_enter(sqlite3_db_mutex(cur->vdbe->db));
                    sqlite3VdbeError(cur->vdbe,
                                     "table \"%s\" was schema changed",
                                     cur->db->tablename);
                    sqlite3VdbeTransferError(cur->vdbe);
                    sqlite3MakeSureDbHasErr(cur->vdbe->db, SQLITE_OK);
                    sqlite3_mutex_leave(sqlite3_db_mutex(cur->vdbe->db));
                    return SQLITE_COMDB2SCHEMA;
                }

                if (cur->ixnum == -1)
                    cur->sc = cur->db->schema;
                else
                    cur->sc = cur->db->ixschema[cur->ixnum];
            }
        }
    }

    if (bdbcur) {
        if (new_mode) {
            assert(bdbcur != NULL);
            rc = bdbcur->set_curtran(bdbcur, clnt->dbtran.cursor_tran);
        } else
            rc = bdbcur->lock(bdbcur, clnt->dbtran.cursor_tran, BDB_SET,
                              &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s returned %d %d\n", __func__, rc, bdberr);
            Pthread_mutex_unlock(&thd->lk);
            return -800;
        }
    }

    Pthread_mutex_unlock(&thd->lk);

    return 0;
}

int comdb2_cheapstack_char_array(char *str, int maxln);

int recover_deadlock_simple(bdb_state_type *bdb_state)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    assert(thd != NULL);
    return recover_deadlock(bdb_state, thd, NULL, 0);
}

int recover_deadlock_flags(bdb_state_type *bdb_state, struct sql_thread *thd,
                           bdb_cursor_ifn_t *bdbcur, int sleepms,
                           const char *func, int line, uint32_t flags)
{
    int rc = thd->clnt->recover_deadlock_rcode =
        recover_deadlock_flags_int(bdb_state, thd, bdbcur, sleepms, func, line, flags);
    if (rc != 0) {
        put_curtran_flags(thedb->bdb_env, thd->clnt, CURTRAN_RECOVERY);
#if INSTRUMENT_RECOVER_DEADLOCK_FAILURE
        thd->clnt->recover_deadlock_func = func;
        thd->clnt->recover_deadlock_line = line;
        thd->clnt->recover_deadlock_thd = pthread_self();
        comdb2_cheapstack_char_array(thd->clnt->recover_deadlock_stack,
                RECOVER_DEADLOCK_MAX_STACK);
#endif
        recover_deadlock_sc_cleanup(thd);
        assert(bdb_lockref() == 0);
    } else {
        assert(bdb_lockref() > 0);
#if INSTRUMENT_RECOVER_DEADLOCK_FAILURE
        thd->clnt->recover_deadlock_func = NULL;
        thd->clnt->recover_deadlock_line = 0;
        thd->clnt->recover_deadlock_thd = 0;
        thd->clnt->recover_deadlock_stack[0] = '\0';
#endif
    }
    return rc;
}

static int ddguard_bdb_cursor_find(struct sql_thread *thd, BtCursor *pCur,
                                   bdb_cursor_ifn_t *cur, void *key, int keylen,
                                   int is_temp_bdbcur, int bias, int *bdberr)
{
    int nretries = 0;
    int max_retries =
        gbl_move_deadlk_max_attempt >= 0 ? gbl_move_deadlk_max_attempt : 500;
    int rc = 0;

    static int simulatedeadlock = 0;

    int fndlen;
    void *buf;
    int cmp;

    struct sqlclntstate *clnt;
    assert(bdb_lockref() > 0);
    clnt = thd->clnt;
    CurRangeArr **append_to;
    if (pCur->range) {
        if (pCur->range->idxnum == -1 && pCur->range->islocked == 0) {
            currange_free(pCur->range);
            pCur->range = currange_new();
        } else if (pCur->range->islocked || pCur->range->lkey ||
                   pCur->range->rkey || pCur->range->lflag ||
                   pCur->range->rflag) {
            if (!pCur->is_recording ||
                (clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE &&
                 gbl_selectv_rangechk)) {
                append_to =
                    (pCur->is_recording) ? &(clnt->selectv_arr) : &(clnt->arr);
                if (!*append_to) {
                    *append_to = malloc(sizeof(CurRangeArr));
                    currangearr_init(*append_to);
                }
                currangearr_append(*append_to, pCur->range);
            } else {
                currange_free(pCur->range);
            }
            pCur->range = currange_new();
        }
    }

    if (debug_switch_simulate_find_deadlock() && !simulatedeadlock)
        simulatedeadlock = 1;
    if (debug_switch_simulate_find_deadlock_retry() && simulatedeadlock == 2)
        simulatedeadlock = 1;

    /* check authentication */
    if (authenticate_cursor(pCur, AUTHENTICATE_READ) != 0)
        return SQLITE_ACCESS;

    if (pCur->range && pCur->db) {
        if (!pCur->range->tbname) {
            pCur->range->tbname = strdup(pCur->db->tablename);
        }
        pCur->range->idxnum = pCur->ixnum;
        pCur->range->lkey = malloc(keylen);
        pCur->range->lkeylen = keylen;
        memcpy(pCur->range->lkey, key, keylen);
    }
    do {
        rc = cur->find(cur, key, keylen, (bias == OP_SeekLT), bdberr);
        if (simulatedeadlock == 1) {
            *bdberr = BDBERR_DEADLOCK;
            simulatedeadlock = 2;
        }

        if (*bdberr == BDBERR_DEADLOCK) {
            if ((rc = recover_deadlock(thedb->bdb_env, thd,
                                       (is_temp_bdbcur) ? cur : NULL, 0)) !=
                0) {
                if (rc == SQLITE_CLIENT_CHANGENODE)
                    *bdberr = BDBERR_NOT_DURABLE;
                else if (!gbl_rowlocks)
                    logmsg(LOGMSG_ERROR, "%s: %p failed dd recovery\n", __func__, (void *)pthread_self());
                return -1;
            }
        }

        nretries++;
    } while ((!max_retries || nretries < max_retries) &&
             *bdberr == BDBERR_DEADLOCK);

    if (pCur->range && pCur->db) {
        if (rc == IX_FND) {
            pCur->range->rkey = malloc(keylen);
            pCur->range->rkeylen = keylen;
            memcpy(pCur->range->rkey, key, keylen);
        } else if (rc == IX_EMPTY) {
            switch (bias) {
            case OP_SeekLT:
            case OP_SeekLE:
                pCur->range->rkeylen = pCur->range->lkeylen;
                pCur->range->rkey = pCur->range->lkey;
                pCur->range->lkeylen = 0;
                pCur->range->lkey = NULL;
                pCur->range->lflag = 1;
                break;
            case OP_SeekGE:
            case OP_SeekGT:
                pCur->range->rflag = 1;
                if (pCur->range->rkey) {
                    free(pCur->range->rkey);
                    pCur->range->rkey = NULL;
                }
                pCur->range->rkeylen = 0;
                break;
            default:
                pCur->range->rflag = 1;
                if (pCur->range->rkey) {
                    free(pCur->range->rkey);
                    pCur->range->rkey = NULL;
                }
                pCur->range->rkeylen = 0;

                pCur->range->lflag = 1;
                if (pCur->range->lkey) {
                    free(pCur->range->lkey);
                    pCur->range->lkey = NULL;
                }
                pCur->range->lkeylen = 0;
                break;
            }
        } else {
            fndlen = cur->datalen(cur);
            buf = cur->data(cur);
            cmp = memcmp(key, buf, (keylen < fndlen) ? keylen : fndlen);
            switch (bias) {
            case OP_SeekLT:
            case OP_SeekLE:
                if (cmp < 0) {
                    // searched for < found
                    pCur->range->rkey = malloc(keylen);
                    pCur->range->rkeylen = keylen;
                    memcpy(pCur->range->rkey, key, keylen);
                } else {
                    // searched for >= found
                    // reach eof
                    pCur->range->rkeylen = pCur->range->lkeylen;
                    pCur->range->rkey = pCur->range->lkey;
                    pCur->range->lkey = malloc(fndlen);
                    pCur->range->lkeylen = fndlen;
                    memcpy(pCur->range->lkey, buf, fndlen);
                }
                break;
            case OP_SeekGE:
            case OP_SeekGT:
                if (cmp < 0) {
                    // searched for < found
                    pCur->range->rkey = malloc(fndlen);
                    pCur->range->rkeylen = fndlen;
                    memcpy(pCur->range->rkey, buf, fndlen);
                } else {
                    // searched for >= found
                    // reach eof
                    pCur->range->rflag = 1;
                    if (pCur->range->rkey) {
                        free(pCur->range->rkey);
                        pCur->range->rkey = NULL;
                    }
                    pCur->range->rkeylen = 0;
                }
                break;
            default:
                switch (rc) {
                case IX_NOTFND:
                    fndlen = cur->datalen(cur);
                    buf = cur->data(cur);
                    pCur->range->rkey = malloc(fndlen);
                    pCur->range->rkeylen = fndlen;
                    memcpy(pCur->range->rkey, buf, fndlen);
                    break;
                case IX_PASTEOF:
                    pCur->range->rflag = 1;
                    if (pCur->range->rkey) {
                        free(pCur->range->rkey);
                        pCur->range->rkey = NULL;
                    }
                    pCur->range->rkeylen = 0;
                    break;
                default:
                    pCur->range->rflag = 1;
                    if (pCur->range->rkey) {
                        free(pCur->range->rkey);
                        pCur->range->rkey = NULL;
                    }
                    pCur->range->rkeylen = 0;

                    pCur->range->lflag = 1;
                    if (pCur->range->lkey) {
                        free(pCur->range->lkey);
                        pCur->range->lkey = NULL;
                    }
                    pCur->range->lkeylen = 0;
                    break;
                }
                break;
            }
        }
    }
    return rc;
}

static int ddguard_bdb_cursor_find_last_dup(struct sql_thread *thd,
                                            BtCursor *pCur,
                                            bdb_cursor_ifn_t *cur, void *key,
                                            int keylen, int keymax,
                                            bias_info *info, int *bdberr)
{
    int bias = info->bias;
    int nretries = 0;
    int max_retries =
        gbl_move_deadlk_max_attempt >= 0 ? gbl_move_deadlk_max_attempt : 500;
    int rc = 0;

    int fndlen;
    void *buf;
    int cmp;

    struct sqlclntstate *clnt;
    clnt = thd->clnt;
    assert(bdb_lockref() > 0);
    CurRangeArr **append_to;
    if (pCur->range) {
        if (pCur->range->idxnum == -1 && pCur->range->islocked == 0) {
            currange_free(pCur->range);
            pCur->range = currange_new();
        } else if (pCur->range->islocked || pCur->range->lkey ||
                   pCur->range->rkey || pCur->range->lflag ||
                   pCur->range->rflag) {
            if (!pCur->is_recording ||
                (clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE &&
                 gbl_selectv_rangechk)) {
                append_to =
                    (pCur->is_recording) ? &(clnt->selectv_arr) : &(clnt->arr);
                if (!*append_to) {
                    *append_to = malloc(sizeof(CurRangeArr));
                    currangearr_init(*append_to);
                }
                currangearr_append(*append_to, pCur->range);
            } else {
                currange_free(pCur->range);
            }
            pCur->range = currange_new();
        }
    }

    /* check authentication */
    if (authenticate_cursor(pCur, AUTHENTICATE_READ) != 0)
        return SQLITE_ACCESS;

    if (pCur->range && pCur->db) {
        if (!pCur->range->tbname) {
            pCur->range->tbname = strdup(pCur->db->tablename);
        }
        pCur->range->idxnum = pCur->ixnum;
        pCur->range->rkey = malloc(keylen);
        pCur->range->rkeylen = keylen;
        memcpy(pCur->range->rkey, key, keylen);
    }

    do {
        info->dirLeft = (bias == OP_SeekLE);
        rc = cur->find_last_dup(cur, key, keylen, keymax, info, bdberr);
        if (*bdberr == BDBERR_DEADLOCK) {
            if ((rc = recover_deadlock(thedb->bdb_env, thd, NULL, 0)) != 0) {
                if (rc == SQLITE_CLIENT_CHANGENODE)
                    *bdberr = BDBERR_NOT_DURABLE;
                else if (!gbl_rowlocks)
                    logmsg(LOGMSG_ERROR, "%s: %p failed dd recovery\n", __func__, (void *)pthread_self());
                return -1;
            }
        }
        nretries++;
    } while ((!max_retries || nretries < max_retries) &&
             *bdberr == BDBERR_DEADLOCK);

    if (pCur->range && pCur->db && !pCur->range->islocked) {
        if (rc == IX_FND) {
            pCur->range->lkey = malloc(keylen);
            pCur->range->lkeylen = keylen;
            memcpy(pCur->range->lkey, key, keylen);
        } else if (rc == IX_EMPTY) {
            switch (bias) {
            case OP_SeekLT:
            case OP_SeekLE:
                pCur->range->lflag = 1;
                if (pCur->range->lkey) {
                    free(pCur->range->lkey);
                    pCur->range->lkey = NULL;
                }
                pCur->range->lkeylen = 0;
                break;
            case OP_SeekGE:
            case OP_SeekGT:
                pCur->range->lkeylen = pCur->range->rkeylen;
                pCur->range->lkey = pCur->range->rkey;
                pCur->range->rkeylen = 0;
                pCur->range->rkey = NULL;
                pCur->range->rflag = 1;
                break;
            default:
                pCur->range->rflag = 1;
                if (pCur->range->rkey) {
                    free(pCur->range->rkey);
                    pCur->range->rkey = NULL;
                }
                pCur->range->rkeylen = 0;

                pCur->range->lflag = 1;
                if (pCur->range->lkey) {
                    free(pCur->range->lkey);
                    pCur->range->lkey = NULL;
                }
                pCur->range->lkeylen = 0;
                break;
            }
        } else {
            fndlen = cur->datalen(cur);
            buf = cur->data(cur);
            cmp = memcmp(key, buf, (keylen < fndlen) ? keylen : fndlen);
            switch (bias) {
            case OP_SeekLT:
            case OP_SeekLE:
                if (cmp < 0) {
                    // searched for < found
                    // reach bof
                    pCur->range->lflag = 1;
                    if (pCur->range->lkey) {
                        free(pCur->range->lkey);
                        pCur->range->lkey = NULL;
                    }
                    pCur->range->lkeylen = 0;
                } else {
                    // searched for >= found
                    pCur->range->lkey = malloc(fndlen);
                    pCur->range->lkeylen = fndlen;
                    memcpy(pCur->range->lkey, buf, fndlen);
                }
                break;
            case OP_SeekGE:
            case OP_SeekGT:
                if (cmp < 0) {
                    // searched for < found
                    // reach bof
                    if (stype_is_null(key)) {
                        pCur->range->lflag = 1;
                        if (pCur->range->rkey) {
                            free(pCur->range->rkey);
                            pCur->range->rkey = NULL;
                        }
                        pCur->range->rkey = malloc(fndlen);
                        pCur->range->rkeylen = fndlen;
                        memcpy(pCur->range->rkey, buf, fndlen);
                    } else {
                        pCur->range->lkeylen = pCur->range->rkeylen;
                        pCur->range->lkey = pCur->range->rkey;
                        pCur->range->rkey = malloc(fndlen);
                        pCur->range->rkeylen = fndlen;
                        memcpy(pCur->range->rkey, buf, fndlen);
                    }
                } else {
                    // searched for >= found
                    pCur->range->lkey = malloc(keylen);
                    pCur->range->lkeylen = keylen;
                    memcpy(pCur->range->lkey, key, keylen);
                }
                break;
            default:
                switch (rc) {
                case IX_NOTFND:
                    fndlen = cur->datalen(cur);
                    buf = cur->data(cur);
                    pCur->range->lkey = malloc(fndlen);
                    pCur->range->lkeylen = fndlen;
                    memcpy(pCur->range->lkey, buf, fndlen);
                    break;
                case IX_PASTEOF:
                    pCur->range->lflag = 1;
                    if (pCur->range->lkey) {
                        free(pCur->range->lkey);
                        pCur->range->lkey = NULL;
                    }
                    pCur->range->lkeylen = 0;
                    break;
                default:
                    pCur->range->lflag = 1;
                    if (pCur->range->lkey) {
                        free(pCur->range->lkey);
                        pCur->range->lkey = NULL;
                    }
                    pCur->range->lkeylen = 0;

                    pCur->range->rflag = 1;
                    if (pCur->range->rkey) {
                        free(pCur->range->rkey);
                        pCur->range->rkey = NULL;
                    }
                    pCur->range->rkeylen = 0;
                    break;
                }
                break;
            }
        }
    }

    return rc;
}

static int ddguard_bdb_cursor_move(struct sql_thread *thd, BtCursor *pCur,
                                   int flags, int *bdberr, int how,
                                   struct ireq *iq_do_prefault, int freshcursor)
{
    bdb_cursor_ifn_t *cur = pCur->bdbcur;
    int nretries = 0;
    int max_retries =
        gbl_move_deadlk_max_attempt >= 0 ? gbl_move_deadlk_max_attempt : 500;
    int rc = 0;
    int deadlock_on_last = 0;

    assert(bdb_lockref() > 0);
    /* check authentication */
    if (authenticate_cursor(pCur, AUTHENTICATE_READ) != 0)
        return IX_ACCESS;

    do {
        switch (how) {
        case CFIRST:
            rc = cur->first(cur, bdberr);
            if (rc && *bdberr == BDBERR_DEADLOCK_ON_LAST)
                *bdberr = BDBERR_DEADLOCK;
            break;
        case CLAST:
            rc = cur->last(cur, bdberr);
            if (rc && *bdberr == BDBERR_DEADLOCK_ON_LAST)
                *bdberr = BDBERR_DEADLOCK;
            break;
        case CNEXT:
            if (iq_do_prefault) {
                pCur->num_nexts++;

                if (gbl_prefaulthelper_sqlreadahead)
                    if (pCur->num_nexts == gbl_sqlreadaheadthresh) {
                        pCur->num_nexts = 0;
                        readaheadpf(iq_do_prefault, pCur->db, pCur->ixnum,
                                    pCur->ondisk_key,
                                    getkeysize(pCur->db, pCur->ixnum),
                                    gbl_sqlreadahead);
                    }
            }
            rc = cur->next(cur, bdberr);
            if (*bdberr == BDBERR_DEADLOCK_ON_LAST) {
                *bdberr = BDBERR_DEADLOCK;
                deadlock_on_last = 1;
                how = CLAST;
            }

            break;
        case CPREV:
            rc = cur->prev(cur, bdberr);
            if (*bdberr == BDBERR_DEADLOCK_ON_LAST) {
                *bdberr = BDBERR_DEADLOCK;
                deadlock_on_last = 1;
                how = CFIRST;
            }
            break;
        default:
            logmsg(LOGMSG_ERROR, "%s: unknown \"how\" operation %d\n", __func__, how);
            return SQLITE_INTERNAL;
        }

        if (*bdberr == BDBERR_DEADLOCK) {
            if ((rc = recover_deadlock(thedb->bdb_env, thd,
                                       (freshcursor) ? pCur->bdbcur : NULL,
                                       0)) != 0) {
                if (rc == SQLITE_CLIENT_CHANGENODE)
                    *bdberr = BDBERR_NOT_DURABLE;
                else
                    logmsg(LOGMSG_ERROR, "%s: %p failed dd recovery\n", __func__, (void *)pthread_self());
                return -1;
            }
        }
        nretries++;
    } while ((*bdberr == BDBERR_DEADLOCK) &&
             (!max_retries || (nretries < max_retries)));

    assert(is_good_ix_find_rc(rc) || rc == 99 || *bdberr != 0);

    /* this is actually a recovery of the last repositioning, preserve eof */
    if (deadlock_on_last && *bdberr == 0) {
        rc = IX_PASTEOF;
    }

    if (*bdberr == 0) {
        int rc2 = cursor_move_postop(pCur);
        if (rc2) {
            logmsg(LOGMSG_ERROR, "cursor_move_postop returned %d\n", rc2);
            *bdberr = BDBERR_DEADLOCK;
            if (rc2 < 0) {
                rc = SQLITE_BUSY;
            } else if (rc2 == SQLITE_CLIENT_CHANGENODE) {
                rc = SQLITE_CLIENT_CHANGENODE;
                *bdberr = BDBERR_NOT_DURABLE;
            } else {
                rc = rc2;
            }
        }
    }

    return rc;
}

/* these transaction modes can perform sql writes */
static int is_sql_update_mode(int mode)
{
    switch (mode) {
    case TRANLEVEL_SOSQL:
    case TRANLEVEL_RECOM:
    case TRANLEVEL_SNAPISOL:
    case TRANLEVEL_SERIAL: return 1;
    default: return 0;
    }
}

int sqlglue_release_genid(unsigned long long genid, int *bdberr)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (thd) {
        Pthread_mutex_lock(&thd->lk);
        BtCursor *cur;
        LISTC_FOR_EACH(&thd->bt->cursors, cur, lnk)
        {
            if (cur->bdbcur /*&& cur->genid == genid */) {
                if (cur->bdbcur->unlock(cur->bdbcur, bdberr)) {
                    logmsg(LOGMSG_ERROR, "%s: failed to unlock cursor %d\n",
                           __func__, *bdberr);
                    Pthread_mutex_unlock(&thd->lk);
                    return -100;
                }
            }
        }
        Pthread_mutex_unlock(&thd->lk);
    } else {
        logmsg(LOGMSG_ERROR, 
                "%s wow, someone forgot to set query_info_key pthread key!\n",
                __func__);
        *bdberr = BDBERR_BUG_KILLME;
        return -1;
    }

    return 0;
}

int sqlite3BtreeSetRecording(BtCursor *pCur, int flag)
{
    assert(pCur);

    pCur->is_recording = flag;

    if (pCur->is_recording) {
        if (gbl_selectv_rangechk) {
            pCur->range = currange_new();
            if (pCur->db) {
                pCur->range->tbname = strdup(pCur->db->tablename);
                pCur->range->idxnum = pCur->ixnum;
            }
        }
    }

    return 0;
}

void *get_lastkey(BtCursor *pCur)
{
    return pCur->is_sampled_idx ? sampler_key(pCur->sampler) : pCur->lastkey;
}

void set_cook_fields(BtCursor *pCur, int cols)
{
    if (pCur) {
        pCur->nCookFields = cols;
#ifdef debug_raw
        fprintf(stderr, "fields to cook: %d\n", cols);
#endif
    }
}

#if 0
void confirm_cooked(struct Cursor *cur)
{
   if (cur->pCursor && cur->nCookFields != cur->pCursor->nCookFields) {
      printf("!!!!!!!!! cursors cook info not same %d %d !!!!!!!!!\n",
            cur->nCookFields, cur->pCursor->nCookFields);
   }
}
#endif

#ifdef debug_raw
void print_cooked_access(BtCursor *pCur, int col)
{
    if (pCur && pCur->sc)
        printf("cooked: %s\n", pCur->sc->member[col].name);
}
#endif

/*
 ** Return non-zero if a read (or write) transaction is active.
 */
int sqlite3BtreeIsInReadTrans(Btree *p)
{
    /* TODO: called where? need? */
    return sqlite3BtreeIsInTrans(p);
}

int sqlite3BtreeIsInBackup(Btree *p)
{
    /* Backups are invisible to sqlite. */
    return 0;
}

/*
 ** The second argument to this function, op, is always SAVEPOINT_ROLLBACK
 ** or SAVEPOINT_RELEASE. This function either releases or rolls back the
 ** savepoint identified by parameter iSavepoint, depending on the value
 ** of op.
 **
 ** Normally, iSavepoint is greater than or equal to zero. However, if op is
 ** SAVEPOINT_ROLLBACK, then iSavepoint may also be -1. In this case the
 ** contents of the entire transaction are rolled back. This is different
 ** from a normal transaction rollback, as no locks are released and the
 ** transaction remains open.
 */
int sqlite3BtreeSavepoint(Btree *p, int op, int iSavepoint)
{
    return UNIMPLEMENTED;
}

/*
 ** Determine whether or not a cursor has moved from the position it
 ** was last placed at.  Cursors can move when the row they are pointing
 ** at is deleted out from under them.
 **
 ** This routine returns an error code if something goes wrong.  The
 ** integer *pHasMoved is set to one if the cursor has moved and 0 if not.
 */
int sqlite3BtreeCursorHasMoved(BtCursor *pCur)
{
    /* TODO: berkeley-like "cursor moved" scan? huh? */
    return 0;
}

/*
#define restoreCursorPosition(p) \
  (p->eState>=CURSOR_REQUIRESEEK ? \
         btreeRestoreCursorPosition(p) : \
         SQLITE_OK)

*/

int sqlite3BtreeCursorRestore(BtCursor *pCur, int *pDifferentRow)
{
    /* TODO AZ:
    int rc;

    assert( pCur!=0 );
    assert( pCur->eState!=CURSOR_VALID );
    rc = restoreCursorPosition(pCur);
    if ( rc ){
        *pDifferentRow = 1;
        return rc;
    }
    if ( pCur->eState!=CURSOR_VALID || NEVER(pCur->skipNext!=0) ){
        *pDifferentRow = 1;
    }else{
        *pDifferentRow = 0;
    }*/
    return SQLITE_OK;
}

int gbl_direct_count = 1;

/*
 ** The first argument, pCur, is a cursor opened on some b-tree. Count the
 ** number of entries in the b-tree and write the result to *pnEntry.
 **
 ** SQLITE_OK is returned if the operation is successfully executed.
 ** Otherwise, if an error is encountered (i.e. an IO error or database
 ** corruption) an SQLite error code is returned.
 */
int sqlite3BtreeCount(BtCursor *pCur, i64 *pnEntry)
{
    struct sql_thread *thd = pCur->thd;
    int rc;
    i64 count;
    if (pCur->is_sampled_idx) {
        count = analyze_get_sampled_nrecs(pCur->db->tablename, pCur->ixnum);
        rc = SQLITE_OK;
    } else if (pCur->cursor_count) {
        rc = pCur->cursor_count(pCur, &count);
    } else if (gbl_direct_count && !pCur->clnt->intrans &&
               pCur->clnt->dbtran.mode != TRANLEVEL_SNAPISOL &&
               pCur->clnt->dbtran.mode != TRANLEVEL_SERIAL &&
               (pCur->cursor_class == CURSORCLASS_TABLE ||
                pCur->cursor_class == CURSORCLASS_INDEX)) {
        int nretries = 0;
        int max_retries = gbl_move_deadlk_max_attempt >= 0
                              ? gbl_move_deadlk_max_attempt
                              : 500;
        do {
            if (access_control_check_sql_read(pCur, thd)) {
                rc = SQLITE_ACCESS;
                break;
            }

            rc = bdb_direct_count(pCur->bdbcur, pCur->ixnum, (int64_t *)&count);
            if (rc == BDBERR_DEADLOCK &&
                recover_deadlock(thedb->bdb_env, thd, NULL, 0)) {
                break;
            }
        } while (rc == BDBERR_DEADLOCK && nretries++ < max_retries);
        if (rc == 0) {
            pCur->nfind++;
            pCur->nmove += count;
            thd->had_tablescans = 1;
            thd->cost += pCur->find_cost + (pCur->move_cost * count);
        } else if (rc == BDBERR_DEADLOCK) {
            rc = SQLITE_DEADLOCK;
        }
    } else {
        int res;
        int cook = pCur->nCookFields;
        count = 0;
        pCur->nCookFields = 0;    /* Don't do ondisk -> sqlite */
        pCur->is_btree_count = 1; /* Don't do ondisk -> sqlite */
        rc = pCur->cursor_move(pCur, &res, CFIRST);
        while (rc == 0 && res == 0) {
            if (!gbl_selectv_rangechk) {
                if (pCur->is_recording &&
                    thd->clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
                    rc = osql_record_genid(pCur, thd, pCur->genid);
                    if (rc) {
                        logmsg(LOGMSG_ERROR, 
                                "%s: failed to record genid %llx (%llu)\n",
                                __func__, pCur->genid, pCur->genid);
                    }
                }
            }
            rc = pCur->cursor_move(pCur, &res, CNEXT);
            ++count;
        }
        pCur->nCookFields = cook;
        pCur->is_btree_count = 0;
    }
    *pnEntry = count;

    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE, "Count(pCur %d)      = %s\n",
                pCur->cursorid, sqlite3ErrStr(rc));

    return rc;
}

/*
 ** Return the size of a BtCursor object in bytes.
 **
 ** This interfaces is needed so that users of cursors can preallocate
 ** sufficient storage to hold a cursor.  The BtCursor object is opaque
 ** to users so they cannot do the sizeof() themselves - they must call
 ** this routine.
 */
int sqlite3BtreeCursorSize(void) { return sizeof(BtCursor); }

/*
 ** Initialize memory that will be converted into a BtCursor object.
 **
 ** The simple approach here would be to memset() the entire object
 ** to zero.  But it turns out that the apPage[] and aiIdx[] arrays
 ** do not need to be zeroed and they are large, so we can save a lot
 ** of run-time by skipping the initialization of those elements.
 */
void sqlite3BtreeCursorZero(BtCursor *p) { bzero(p, sizeof(*p)); }

/*
 ** Clear the current cursor position.
 */
void sqlite3BtreeClearCursor(BtCursor *pCur)
{
    /* FIXME TODO XXX: need bdb call to clear a bdb cursor. ix mode cursors
     * just need to be set as invalid. */
    return;
}

/*
 ** This routine sets the state to CURSOR_FAULT and the error
 ** code to errCode for every cursor on BtShared that pBtree
 ** references.
 **
 ** Every cursor is tripped, including cursors that belong
 ** to other database connections that happen to be sharing
 ** the cache with pBtree.
 **
 ** This routine gets called when a rollback occurs.
 ** All cursors using the same cache must be tripped
 ** to prevent them from trying to use the btree after
 ** the rollback.  The rollback may have deleted tables
 ** or moved root pages, so it is not sufficient to
 ** save the state of the cursor.  The cursor must be
 ** invalidated.
 */
int sqlite3BtreeTripAllCursors(Btree *pBtree, int errCode, int writeOnly)
{
    int rc = SQLITE_OK;
    /* TODO: not sure yet if this is relevant or needed or what */
    return rc;
}

/*
 ** This routine does the first phase of a two-phase commit.  This routine
 ** causes a rollback journal to be created (if it does not already exist)
 ** and populated with enough information so that if a power loss occurs
 ** the database can be restored to its original state by playing back
 ** the journal.  Then the contents of the journal are flushed out to
 ** the disk.  After the journal is safely on oxide, the changes to the
 ** database are written into the database file and flushed to oxide.
 ** At the end of this call, the rollback journal still exists on the
 ** disk and we are still holding all locks, so the transaction has not
 ** committed.  See sqlite3BtreeCommitPhaseTwo() for the second phase of the
 ** commit process.
 **
 ** This call is a no-op if no write-transaction is currently active on pBt.
 **
 ** Otherwise, sync the database file for the btree pBt. zMaster points to
 ** the name of a master journal file that should be written into the
 ** individual journal file, or is NULL, indicating no master journal file
 ** (single database transaction).
 **
 ** When this is called, the master journal should already have been
 ** created, populated with this journal pointer and synced to disk.
 **
 ** Once this is routine has returned, the only thing required to commit
 ** the write-transaction for this database file is to delete the journal.
 */
int sqlite3BtreeCommitPhaseOne(Btree *p, const char *zMaster)
{
    /* This does nothing.  The magic happens in sqlite3BtreeCommitPhaseTwo */
    return 0;
}

/*
 ** Commit the transaction currently in progress.
 **
 ** This routine implements the second phase of a 2-phase commit.  The
 ** sqlite3BtreeCommitPhaseOne() routine does the first phase and should
 ** be invoked prior to calling this routine.  The sqlite3BtreeCommitPhaseOne()
 ** routine did all the work of writing information out to disk and flushing the
 ** contents so that they are written onto the disk platter.  All this
 ** routine has to do is delete or truncate or zero the header in the
 ** the rollback journal (which causes the transaction to commit) and
 ** drop locks.
 **
 ** This will release the write lock on the database file.  If there
 ** are no active cursors, it also releases the read lock.
 */
int sqlite3BtreeCommitPhaseTwo(Btree *p, int dummy)
{
    /* This is the real commit call. PhaseOne does nothing. */
    return sqlite3BtreeCommit(p);
}

/*
 ** A write-transaction must be opened before calling this function.
 ** It performs a single unit of work towards an incremental vacuum.
 **
 ** If the incremental vacuum is finished after this function has run,
 ** SQLITE_DONE is returned. If it is not finished, but no error occurred,
 ** SQLITE_OK is returned. Otherwise an SQLite error code.
 */
int sqlite3BtreeIncrVacuum(Btree *p) { return SQLITE_DONE; }

/*
 ** Return the pager associated with a BTree.  This routine is used for
 ** testing and debugging only.
 */
Pager *sqlite3BtreePager(Btree *p) { return NULL; }

/*
** Return an estimate for the number of rows in the table that pCur is
** pointing to.  Return a negative number if no estimate is currently 
** available.
*/
i64 sqlite3BtreeRowCountEst(BtCursor *pCur){ return -1; }

/*
 ** Return the file handle for the database file associated
 ** with the pager.  This might return NULL if the file has
 ** not yet been opened.
 */
sqlite3_file *sqlite3PagerFile(Pager *pPager) { return NULL; }

/*
** Return the VFS structure for the pager.
*/
sqlite3_vfs *sqlite3PagerVfs(Pager *pPager) { return NULL; }

/*
** Return the file handle for the journal file (if it exists).
** This will be either the rollback journal or the WAL file.
*/
sqlite3_file *sqlite3PagerJrnlFile(Pager *pPager) { return NULL; }

/*
 ** Return the full pathname of the database file.
 */
const char *sqlite3PagerFilename(Pager *pPager, int dummy) { return NULL; }

/*
 ** Return the approximate number of bytes of memory currently
 ** used by the pager and its associated cache.
 */
int sqlite3PagerMemUsed(Pager *pPager) { return 0; }

void sqlite3PagerShrink(Pager *pPager) {}

u8 sqlite3PagerIsreadonly(Pager *pPager) { return 0; }

/* TODO: does this need any modification? */
void sqlite3PagerCacheStat(Pager *pPager, int eStat, int reset, int *pnVal) {}

int sqlite3PagerExclusiveLock(Pager *pPager) { return SQLITE_OK; }

/*
** Flush all unreferenced dirty pages to disk.
*/
int sqlite3PagerFlush(Pager *pPager) { return SQLITE_OK; }

/*
 ** Return true if the given BtCursor is valid.  A valid cursor is one
 ** that is currently pointing to a row in a (non-empty) table.
 ** This is a verification routine is used only within assert() statements.
 */
int sqlite3BtreeCursorIsValid(BtCursor *pCur) { return pCur != NULL; }

int sqlite3BtreeCursorIsValidNN(BtCursor *pCur){
  assert( pCur!=0 );
  /* TODO: This is not right. */
  return 1;
}

/*
 ** Return TRUE if the pager is in a state where it is OK to change the
 ** journalmode.  Journalmode changes can only happen when the database
 ** is unmodified.
 */
int sqlite3PagerOkToChangeJournalMode(Pager *pPager) { return 0; }

/*
 ** Return the current journal mode.
 */
int sqlite3PagerGetJournalMode(Pager *pPager) { return 0; }

/*
** Return the pPager->iDataVersion value
*/
u32 sqlite3PagerDataVersion(Pager *pPager){ return 0; }

/*
 ** Set the journal-mode for this pager. Parameter eMode must be one of:
 **
 **    PAGER_JOURNALMODE_DELETE
 **    PAGER_JOURNALMODE_TRUNCATE
 **    PAGER_JOURNALMODE_PERSIST
 **    PAGER_JOURNALMODE_OFF
 **    PAGER_JOURNALMODE_MEMORY
 **    PAGER_JOURNALMODE_WAL
 **
 ** The journalmode is set to the value specified if the change is allowed.
 ** The change may be disallowed for the following reasons:
 **
 **   *  An in-memory database can only have its journal_mode set to _OFF
 **      or _MEMORY.
 **
 **   *  Temporary databases cannot have _WAL journalmode.
 **
 ** The returned indicate the current (possibly updated) journal-mode.
 */
int sqlite3PagerSetJournalMode(Pager *pPager, int eMode) { return 0; }

/**
 * sqlite callback for preparing conversion error codes
 *
 */
void sqlite3SetConversionError(void)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (thd && thd->clnt)
        thd->clnt->fail_reason.reason =
            CONVERT_FAILED_INCOMPATIBLE_VALUES;
}

int is_comdb2_index_disableskipscan(const char *name)
{
    struct dbtable *db = get_dbtable_by_name(name);
    if (db) {
        return db->disableskipscan;
    }
    return 0;
}

int has_comdb2_index_for_sqlite(
  Table *pTab
){
  if( pTab==0 ) return 0;
  if( gbl_partial_indexes && pTab->hasPartIdx ){
    return 1;
  }
  if( gbl_expressions_indexes && pTab->hasExprIdx ){
    return 1;
  }
  return 0;
}

int need_index_checks_for_upsert(
  Table *pTab,
  Upsert *pUpsert,
  int onError,
  int noConflict
){
  if( !noConflict && has_comdb2_index_for_sqlite(pTab) ){
    return 1; /* has partial or expression index */
  }
  if( pUpsert ){
    if( pUpsert->pUpsertSet ){
      return 1; /* has ON CONFLICT DO UPDATE */
    }else{
      return 0; /* has ON CONFLICT DO NOTHING */
    }
  }
  if( onError==OE_Replace ){
    return 1; /* is INSERT OR REPLACE -or- REPLACE INTO */
  }
  return 0;
}

int is_comdb2_index_unique(const char *dbname, char *idx)
{
    struct dbtable *db = get_dbtable_by_name(dbname);
    if (db) {
        int i;
        for (i = 0; i < db->nix; ++i) {
            struct schema *s = db->ixschema[i];
            if (s->sqlitetag && strcmp(s->sqlitetag, idx) == 0) {
                return !(s->flags & SCHEMA_DUP);
            }
        }
    }
    return 0;
}

int is_comdb2_index_expression(const char *dbname)
{
    struct dbtable *db = get_dbtable_by_name(dbname);
    if (db)
        return db->ix_expr;
    return 0;
}

int is_comdb2_index_blob(const char *dbname, int icol)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;
    struct schema_mem *sm = clnt ? clnt->schema_mems : NULL;

    struct dbtable *db = get_dbtable_by_name(dbname);
    struct schema *schema = NULL;

    if (sm && sm->sc)
        schema = sm->sc; /* use the given schema for new_indexes_syntax_check */
    else if (db)
        schema = db->schema;

    if (schema) {
        if (icol < 0 || icol >= schema->nmembers)
            return -1;
        switch (schema->member[icol].type) {
        case CLIENT_BLOB:
        case SERVER_BLOB:
        case CLIENT_BLOB2:
        case SERVER_BLOB2:
        case CLIENT_VUTF8:
        case SERVER_VUTF8:
            /* mark ix_blob 1 in schema */
            schema->ix_blob = 1;
            return 1;
        default:
            return 0;
        }
    }
    return 0;
}

void comdb2SetWriteFlag(int wrflag)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;

    /* lets make sure we don't downgrade write to readonly */
    if (clnt->writeTransaction < wrflag)
        clnt->writeTransaction = wrflag;
}

void _sockpool_socket_type(const char *protocol, const char *dbname, const char *service, char *host,
                           char *socket_type, int socket_type_len)
{
    /* NOTE: in fdb, we want to require a specific node.
       We make the name include that node to be able to request
       sockets going to a specific node. */
    snprintf(socket_type, socket_type_len, "%s/%s/%s/%s", protocol, dbname, service,
             host);
}

static int _sockpool_get(const char *protocol, const char *dbname, const char *service, char *host)
{
    char socket_type[512];
    int fd;

    if (unlikely(
            bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DISABLE_SERVER_SOCKPOOL)))
        return -1;

    _sockpool_socket_type(protocol, dbname, service, host, socket_type,
                          sizeof(socket_type));

    /* TODO: don't rely on hints yet */
    /* we allow locally cached sockets */
    fd =
        socket_pool_get_ext(socket_type, 0, SOCKET_POOL_GET_GLOBAL, NULL, NULL);

    if (gbl_fdb_track)
        logmsg(LOGMSG_ERROR, "%p: Asked socket for %s got %d\n", (void *)pthread_self(), socket_type, fd);

    return fd;
}

void disconnect_remote_db(const char *protocol, const char *dbname, const char *service, char *host,
                          SBUF2 **psb)
{
    char socket_type[512];
    int fd;
    SBUF2 *sb = *psb;

    if (unlikely(
            bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DISABLE_SERVER_SOCKPOOL))) {
        /* don't use pool */
        sbuf2close(sb);
        *psb = NULL;
        return;
    }

    fd = sbuf2fileno(sb);

    _sockpool_socket_type(protocol, dbname, service, host, socket_type,
                          sizeof(socket_type));

    if (gbl_fdb_track)
        logmsg(LOGMSG_ERROR, "%p: Donating socket for %s\n", (void *)pthread_self(), socket_type);

    /* this is used by fdb sql for now */
    socket_pool_donate_ext(socket_type, fd, IOTIMEOUTMS / 1000, 0,
                           0, NULL, NULL);

    /*fprintf(stderr, "%s: donated socket %d to sockpool %s\n", __func__, fd,
     * socket_type);*/

    sbuf2free(sb);
    *psb = NULL;
}

/* use portmux to open an SBUF2 to local db or proxied db
   it is trying to use sockpool
 */
SBUF2 *connect_remote_db(const char *protocol, const char *dbname, const char *service, char *host, int use_cache)
{
    SBUF2 *sb;
    int port;
    int retry;
    int sockfd;
    int nodelay = 1;

    if (use_cache) {
        /* lets try to use sockpool, if available */
        sockfd = _sockpool_get(protocol, dbname, service, host);
        if (sockfd > 0) {
            /*fprintf(stderr, "%s: retrieved sockpool socket for %s.%s.%s.%s
              fd=%d\n",
              __func__, dbname, protocol, service, host, sockfd);*/
            goto sbuf;
        }
    }
    /*fprintf(stderr, "%s: no sockpool socket for %s.%s.%s.%s\n",
      __func__, dbname, protocol, service, host);*/

    retry = 0;
retry:

    /* this could fail due to load */
    port = portmux_get(host, "comdb2", "replication", dbname);
    if (port == -1) {
        if (retry++ < 10) {
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: cannot get port number from portmux on node %s\n",
                __func__, host);
        return NULL;
    }

    sockfd = tcpconnecth_to(host, port, 0, 0);
    if (sockfd == -1) {
        logmsg(LOGMSG_ERROR, "%s: cannot connect to %s on machine %s port %d\n",
                __func__, dbname, host, port);
        return NULL;
    }

    (void)setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

sbuf:
    sb = sbuf2open(sockfd, 0);
    if (!sb) {
        logmsg(LOGMSG_ERROR, "%s: failed to open sbuf\n", __func__);
        close(sockfd);
        return NULL;
    }

    sbuf2settimeout(sb, IOTIMEOUTMS, IOTIMEOUTMS);

    return sb;
}

int sqlite3PagerLockingMode(Pager *p, int mode) { return 0; }

int sqlite3BtreeSecureDelete(Btree *btree, int arg) { return 0; }

int sqlite3UpdateMemCollAttr(BtCursor *pCur, int idx, Mem *mem)
{
    /*
       char    *payload;
       int     payloadsz;

       if (pCur->ixnum>=0 && pCur->db->ix_collattr[pCur->ixnum])
       {
          payload = pCur->bdbcur->collattr(pCur->bdbcur);
          payloadsz = pCur->bdbcur->collattrlen(pCur->bdbcur);

       }
       */
    return 0;
}

int comdb2_get_planner_effort()
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;

    return clnt->planner_effort;
}

/* Return the index number. */
int comdb2_get_index(const char *dbname, char *idx)
{
    struct dbtable *db = get_dbtable_by_name(dbname);

    if (db) {
        int i;
        for (i = 0; i < db->nix; ++i) {
            struct schema *s = db->ixschema[i];
            if (s->sqlitetag && strcmp(s->sqlitetag, idx) == 0) {
                return i;
            }
        }
    }

    assert(0);
    return -1;
}

static void sqlite3BtreeCursorHint_Flags(BtCursor *pCur, unsigned int mask)
{
    if (mask & OPFLAG_SEEKEQ)
        pCur->is_equality = 1;
    else
        pCur->is_equality = 0;
}

const char *comdb2_get_sql(void)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);

    if (thd)
        return thd->clnt->sql;

    return NULL;
}

int gbl_fdb_track_hints = 0;
static void sqlite3BtreeCursorHint_Range(BtCursor *pCur, const Expr *pExpr)
{
    char *expr = "?no vdbe engine?";

    if (pCur && pCur->bt && pCur->bt->is_remote) {
        expr = sqlite3ExprDescribeAtRuntime(pCur->vdbe, pExpr);
        if (!expr) /* failed hinting, calling sqlite engine will catch it */
            return;

        if (pCur->fdbc) {
            if (!pCur->bt->is_remote)
                abort();

            pCur->fdbc->set_hint(pCur, (void *)pExpr);
        }

        if (gbl_fdb_track_hints)
            logmsg(LOGMSG_USER, "Hint \"%s\"\n", expr);

        sqlite3_free(expr);
    }
}

/*
** Provide hints to the cursor.  The particular hint given (and the type
** and number of the varargs parameters) is determined by the eHintType
** parameter.  See the definitions of the BTREE_HINT_* macros for details.
**
** Hints are not (currently) used by the native SQLite implementation.
** This mechanism is provided for systems that substitute an alternative
** storage engine.
*/
void sqlite3BtreeCursorHint(BtCursor *pCur, int eHintType, ...)
{
    va_list ap;
    va_start(ap, eHintType);
    switch (eHintType) {
    case BTREE_HINT_FLAGS: {
        unsigned int mask = va_arg(ap, unsigned int);

        assert(mask == BTREE_SEEK_EQ || mask == BTREE_BULKLOAD ||
               mask == (BTREE_SEEK_EQ | BTREE_BULKLOAD) || mask == 0);

        sqlite3BtreeCursorHint_Flags(pCur, mask);

        break;
    }

    case BTREE_HINT_RANGE: {
        Expr *expr = va_arg(ap, Expr *);

        sqlite3BtreeCursorHint_Range(pCur, expr);

        break;
    }
    }
    va_end(ap);
}

int fdb_packedsqlite_extract_genid(char *key, int *outlen, char *outbuf)
{
    int hdroffset = 0;
    int dataoffset = 0;
    unsigned int hdrsz;
    int type = 0;
    Mem m = {{0}};

    /* extract genid */
    hdroffset = sqlite3GetVarint32((unsigned char *)key, &hdrsz);
    dataoffset = hdrsz;
    hdroffset +=
        sqlite3GetVarint32((unsigned char *)key + hdroffset, (u32 *)&type);

    /* Sanity checks */
    if (type != 6 || hdroffset != dataoffset) {
        return -1;
    }

    sqlite3VdbeSerialGet((unsigned char *)key + dataoffset, type, &m);
    *outlen = sizeof(m.u.i);
    memcpy(outbuf, &m.u.i, *outlen);

    return 0;
}

/*TODO: all fields extracting for debugging; we only need name and rootpage, so
shrink
once tested */
void fdb_packedsqlite_process_sqlitemaster_row(char *row, int rowlen,
                                               char **etype, char **name,
                                               char **tbl_name, int *rootpage,
                                               char **sql, char **csc2,
                                               unsigned long long *version,
                                               int new_rootpage)
{
    Mem m;
    unsigned int hdrsz, type;
    int hdroffset = 0, dataoffset = 0;
    int prev_dataoffset;
    int fld;
    char *str = NULL;

    hdroffset = sqlite3GetVarint32((unsigned char *)row, &hdrsz);
    dataoffset = hdrsz;
    fld = 0;
    while (hdroffset < hdrsz) {
        hdroffset +=
            sqlite3GetVarint32((unsigned char *)row + hdroffset, &type);
        prev_dataoffset = dataoffset;
        dataoffset += sqlite3VdbeSerialGet(
            (unsigned char *)row + prev_dataoffset, type, &m);

        if (fld < 7 && fld != 3 && fld != 6) {
            str = (char *)malloc(m.n + 1);
            if (!str)
                abort();
            memcpy(str, m.z, m.n);
            str[m.n] = '\0';
        }

        switch (fld) {
        case 0:
            *etype = str;
            break;
        case 1:
            *name = str;
            break;
        case 2:
            *tbl_name = str;
            break;
        case 3:
            *rootpage = m.u.i;

            /* we need to replace the source with the local rootpage */
            m.u.i = new_rootpage;
            sqlite3VdbeSerialPut((unsigned char *)row + prev_dataoffset, &m,
                                 type);

            break;

        case 4:
            *sql = str;
            break;
        case 5:
            *csc2 = str;
            break;
        case 6:
            if (type == MEM_Null) {
                *version = 0;
            } else {
                *version = m.u.i;
            }
            break;
        default:
            abort();
        }
        fld++;
    }
    if (fld < 7) {
        /* we received an non-version answer */
        *version = 0;
    }
}

int fdb_add_remote_time(BtCursor *pCur, unsigned long long start,
                        unsigned long long end)
{
    struct sql_thread *thd = pCur->thd;
    fdbtimings_t *timings = &thd->clnt->osql.fdbtimes;
    unsigned long long duration = end - start;

    timings->total_calls++;
    timings->total_time += duration;

    if (timings->max_call == 0 || timings->max_call < duration) {
        /*
        fprintf(stderr, "Adding %llu msec, old max=%llu calls=%llu\n", duration,
        timings->max_call, timings->total_calls);
        */
        timings->max_call = duration;
    } else {
        /*
        fprintf(stderr, "Adding %llu msec, calls=%llu\n", duration,
        timings->total_calls);
        */
    }

    return 0;
}

int ctracewrap(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    vctracent(fmt, args);
    va_end(args);
    return 0;
}

static int printf_logmsg_wrap(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    logmsgv(LOGMSG_USER, fmt, args);
    va_end(args);
    return 0;
}

void stat4dump(int more, char *table, int istrace)
{
#ifndef PER_THREAD_MALLOC
    void *thread_oldm;
#endif
    void *sql_oldm;
    int rc;
    sqlite3 *db = NULL;

    thread_memcreate_with_save(128 * 1024, &thread_oldm);
    sql_mem_init_with_save(NULL, &sql_oldm);

    struct sql_thread *old_thd = pthread_getspecific(query_info_key);
    struct sql_thread *thd = start_sql_thread();
    Pthread_setspecific(query_info_key, thd);

    struct sqlclntstate clnt;
    start_internal_sql_clnt(&clnt);
    clnt.sql = "select * from sqlite_stat4"; //* from sqlite_master limit 1;";
    thd->clnt = &clnt;

    get_copy_rootpages(thd);
    sql_get_query_id(thd);
    if ((rc = get_curtran(thedb->bdb_env, &clnt)) != 0) {
        goto out;
    }
    if ((rc = sqlite3_open_serial("db", &db, 0)) != SQLITE_OK) {
        goto put;
    }
    clnt.in_sqlite_init = 1;
    if ((rc = sqlite3_exec(db, clnt.sql, NULL, NULL, NULL)) != SQLITE_OK) {
        goto close;
    }
    int (*outFunc)(const char *fmt, ...) = printf_logmsg_wrap;
    if (istrace) {
        ctrace("\n");
        outFunc = ctracewrap;
    }

    for (HashElem *i = sqliteHashFirst(&db->aDb[0].pSchema->idxHash); i;
         i = sqliteHashNext(i)) {
        Index *idx = sqliteHashData(i);
        if (table != NULL && strcmp(idx->pTable->zName, table))
            continue;
        if (idx->aSample == NULL)
            continue;
        outFunc("idx:%s.%s samples:%d cols:%d aAvgEq: ", idx->pTable->zName,
                idx->zName, idx->nSample, idx->nSampleCol);
        for (int j = 0; j < idx->nSampleCol; ++j) {
            outFunc("%d ", idx->aAvgEq[j]);
        }
        outFunc("\n");
        if (istrace)
            ctrace("\n");
        for (int k = 0; k < idx->nSample; ++k) {
            outFunc("sample:%d anEq:", k);
            for (int j = 0; j < idx->nSampleCol; ++j) {
                outFunc("%d ", idx->aSample[k].anEq[j]);
            }
            outFunc("anLt:");
            for (int j = 0; j < idx->nSampleCol; ++j) {
                outFunc("%d ", idx->aSample[k].anLt[j]);
            }
            outFunc("anDLt:");
            for (int j = 0; j < idx->nSampleCol; ++j) {
                outFunc("%d ", idx->aSample[k].anDLt[j]);
            }
            if (!more) {
                outFunc("\n");
                if (istrace)
                    ctrace("\n");
                continue;
            }
            outFunc("sample => {");
            {
                void *in = idx->aSample[k].p;
                u32 hdrsz;
                u8 hdroffset = sqlite3GetVarint32(in, &hdrsz);
                u32 dataoffset = hdrsz;
                const char *comma = "";
                while (hdroffset < hdrsz) {
                    u32 type;
                    Mem m = {{0}};
                    hdroffset += sqlite3GetVarint32(in + hdroffset, &type);
                    dataoffset +=
                        sqlite3VdbeSerialGet(in + dataoffset, type, &m);
                    outFunc("%s", comma);
                    comma = ", ";
                    if (m.flags & MEM_Null) {
                        outFunc("NULL");
                    } else if (m.flags & MEM_Int) {
                        outFunc("%" PRId64, m.u.i);
                    } else if (m.flags & MEM_Real) {
                        outFunc("%f", m.u.r);
                    } else if (m.flags & MEM_Str) {
                        outFunc("\"%.*s\"", m.n, m.z);
                    } else if (m.flags & MEM_Datetime) {
                        time_t t = m.du.dt.dttz_sec;
                        char ct[64];
                        ctime_r(&t, ct);
                        ct[strlen(ct) - 1] = 0;
                        outFunc(ct);
                    } else if (m.flags & MEM_Blob) { /* a byte array */
                        outFunc("x'");
                        for (int i = 0; i != m.n; ++i)
                            outFunc("%02X", (unsigned char)m.z[i]);
                        outFunc("'");
                    } else {
                        outFunc("type:%d", m.flags);
                    }
                }
            }
            outFunc("}\n");
            if (istrace)
                ctrace("\n");
        }
    }

    if (istrace)
        ctrace("\n");

close:
    sqlite3_close_serial(&db);
put:
    put_curtran(thedb->bdb_env, &clnt);
out:
    end_internal_sql_clnt(&clnt);
    thd->clnt = NULL;
    done_sql_thread();
    Pthread_setspecific(query_info_key, old_thd);
    sql_mem_shutdown_and_restore(NULL, &sql_oldm);
    thread_memdestroy_and_restore(&thread_oldm);
}

void curtran_assert_nolocks(void)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (!thd)
        return;
    uint32_t lockid = bdb_get_lid_from_cursortran(thd->clnt->dbtran.cursor_tran);
    if (!lockid)
        return;

    bdb_locker_assert_nolocks(thedb->bdb_env, lockid);
}

__thread int track_thread_locks = 0;
int gbl_track_curtran_gettran_locks = 0;

tran_type *curtran_gettran(void)
{
    int bdberr;
    tran_type *tran = NULL;
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (!thd || !thd->clnt || !thd->clnt->dbtran.cursor_tran)
        return NULL;
    uint32_t lockid = bdb_get_lid_from_cursortran(thd->clnt->dbtran.cursor_tran);
    if ((tran = bdb_tran_begin(thedb->bdb_env, NULL, &bdberr)) != NULL) {
        bdb_get_tran_lockerid(tran, &tran->original_lid);
        bdb_set_tran_lockerid(tran, lockid);
        if (gbl_track_curtran_gettran_locks)
            track_thread_locks = 1;
        tran->is_curtran = 1;
    }
    return tran;
}

void curtran_puttran(tran_type *tran)
{
    int bdberr;
    if (!tran)
        return;
    bdb_set_tran_lockerid(tran, tran->original_lid);
    tran->is_curtran = 0;
    track_thread_locks = 0;
    bdb_tran_abort(thedb->bdb_env, tran, &bdberr);
}

void clone_temp_table(sqlite3_stmt *stmt, struct temptable *tbl)
{
    int rc;
    char *err = NULL;
    tmptbl_clone = tbl;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
        ;
    tmptbl_clone = NULL;
    if (rc != SQLITE_DONE) {
        logmsg(LOGMSG_FATAL, "%s rc:%d err:%s\n", __func__, rc,
               err ? err : "(none)");
        abort();
    }
}

int bt_hash_table(char *table, int szkb)
{
    struct dbtable *db;
    bdb_state_type *bdb_state;
    struct ireq iq;
    tran_type *metatran = NULL;
    tran_type *tran = NULL;
    int rc, bdberr = 0;
    int bthashsz;

    db = get_dbtable_by_name(table);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "%s: invalid table %s\n", __func__, table);
        return -1;
    }

    bdb_state = (bdb_state_type *)db->handle;
    init_fake_ireq(thedb, &iq);
    iq.usedb = db;

    if (get_db_bthash(db, &bthashsz))
        bthashsz = 0;
    if (bthashsz == szkb) {
        logmsg(LOGMSG_WARN, "Table %s already hash bthash with size %dkb per stripe\n",
               table, bthashsz);
        return 0;
    }

    trans_start(&iq, NULL, &metatran);
    if (put_db_bthash(db, metatran, szkb) != 0) {
        fprintf(stderr, "Failed to write bthash to meta table\n");
        return -1;
    }
    trans_commit(&iq, metatran, gbl_myhostname);

    trans_start(&iq, NULL, &tran);
    bdb_lock_table_write(bdb_state, tran);
    logmsg(LOGMSG_WARN, "Building bthash for table %s, size %dkb per stripe\n",
           db->tablename, szkb);
    bdb_handle_dbp_add_hash(bdb_state, szkb);
    trans_commit(&iq, tran, gbl_myhostname);

    // scdone log
    rc = bdb_llog_scdone(bdb_state, bthash, bdb_state->name, 
                         strlen(bdb_state->name) + 1, 1, &bdberr);
    if (rc || bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, 
                "Failed to send logical log scdone bthash rc=%d bdberr=%d\n",
                rc, bdberr);
        return -1;
    }

    bdb_handle_dbp_hash_stat_reset(bdb_state);
    return 0;
}

int del_bt_hash_table(char *table)
{
    struct dbtable *db;
    bdb_state_type *bdb_state;
    struct ireq iq;
    tran_type *metatran = NULL;
    tran_type *tran = NULL;
    int rc, bdberr = 0;
    int bthashsz;

    db = get_dbtable_by_name(table);
    if (db == NULL) {
       logmsg(LOGMSG_ERROR, "%s: invalid table %s\n", __func__, table);
        return -1;
    }

    bdb_state = (bdb_state_type *)db->handle;
    init_fake_ireq(thedb, &iq);
    iq.usedb = db;

    if (get_db_bthash(db, &bthashsz))
        bthashsz = 0;
    if (bthashsz == 0) {
       logmsg(LOGMSG_WARN, "No bthash to delete for table %s\n", table);
        return 0;
    }

    trans_start(&iq, NULL, &metatran);
    if (put_db_bthash(db, metatran, 0) != 0) {
        logmsg(LOGMSG_ERROR, "Failed to write bthash to meta table\n");
        return -1;
    }
    trans_commit(&iq, metatran, gbl_myhostname);

    trans_start(&iq, NULL, &tran);
    bdb_lock_table_write(bdb_state, tran);
    logmsg(LOGMSG_WARN, "Deleting bthash for table %s\n", db->tablename);
    bdb_handle_dbp_drop_hash(bdb_state);
    trans_commit(&iq, tran, gbl_myhostname);

    // scdone log
    rc = bdb_llog_scdone(bdb_state, bthash, bdb_state->name,
                         strlen(bdb_state->name) + 1, 1, &bdberr);
    if (rc || bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "Failed to send logical log scdone bthash rc=%d bdberr=%d\n",
                rc, bdberr);
        return -1;
    }

    bdb_handle_dbp_hash_stat_reset(bdb_state);
    return 0;
}

int stat_bt_hash_table(char *table)
{
    struct dbtable *db;
    bdb_state_type *bdb_state;
    int bthashsz = 0;

    db = get_dbtable_by_name(table);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "%s: invalid table %s\n", __func__, table);
        return -1;
    }

    bdb_state = (bdb_state_type *)db->handle;

    if (get_db_bthash(db, &bthashsz))
        bthashsz = 0;
    if (bthashsz) {
        if (!bdb_handle_dbp_hash_stat(bdb_state)) {
            logmsg(LOGMSG_ERROR, "META INDICATES HASH ENABLED, BUT HASH IS NOT ACTUALLY "
                   "THERE!! BUG !!\n");
            return -1;
        }
        logmsg(LOGMSG_USER, "bt_hash_size: %dkb per stripe\n", bthashsz);
    } else {
        if (bdb_handle_dbp_hash_stat(bdb_state))
            logmsg(LOGMSG_ERROR, "META INDICATES HASH DISABLED, BUT HASH IS THERE!! BUG !!\n");
        logmsg(LOGMSG_USER, "bt_hash: DISABLED, size %d\n", bthashsz);
    }
    return 0;
}

int stat_bt_hash_table_reset(char *table)
{
    struct dbtable *db;
    bdb_state_type *bdb_state;

    db = get_dbtable_by_name(table);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "%s: invalid table %s\n", __func__, table);
        return -1;
    }

    bdb_state = (bdb_state_type *)db->handle;

    bdb_handle_dbp_hash_stat_reset(bdb_state);

    return 0;
}
/**
 * Retrieve the schema version for table
 *
 */
unsigned long long comdb2_table_version(const char *tablename)
{
    struct dbtable *db;

    if (is_tablename_queue(tablename))
        return 0;

    db = get_dbtable_by_name(tablename);
    if (!db) {
        ctrace("table unknown \"%s\"\n", tablename);
        return -1;
    }

    return db->tableversion;
}

void sqlite3RegisterDateTimeFunctions(void) {}

/**
 * Save what columns are accessed using this cursor
 *
 */
void sqlite3BtreeCursorSetFieldUsed(BtCursor *pCur, unsigned long long mask)
{
    pCur->col_mask = mask;
}

void clearClientSideRow(struct sqlclntstate *clnt)
{
    if (!clnt) {
        struct sql_thread *thd = pthread_getspecific(query_info_key);
        if (!thd) {
            logmsg(LOGMSG_FATAL, "%s: running in non sql thread!n", __func__);
            abort();
        }
        clnt = thd->clnt;
    }
    clnt->keyDdl = 0ULL;
    clnt->nDataDdl = 0;
    if (clnt->dataDdl) {
        free(clnt->dataDdl);
        clnt->dataDdl = NULL;
    }
}

static int queryOverlapsCursors(struct sqlclntstate *clnt, BtCursor *pCur)
{
    return clnt->is_overlapping;
}

static void ondisk_blob_to_sqlite_mem(struct field *f, Mem *m,
                                      blob_buffer_t *blobs, size_t maxblobs)
{
    assert(!blobs || f->blob_index < maxblobs);
    if (blobs && blobs[f->blob_index].exists) {
        m->z = blobs[f->blob_index].data;
        m->n = blobs[f->blob_index].length;
        if (m->z == NULL)
            m->z = "";
        else
            m->flags = MEM_Dyn;

        if (f->type == SERVER_VUTF8) {
            m->flags |= MEM_Str;
            if (m->n > 0)
                --m->n; /* sqlite string lengths do not include NULL */
        } else
            m->flags |= MEM_Blob;
    } else {
        /* assume NULL */
        m->z = NULL;
        m->n = 0;
        m->flags = MEM_Null;
    }
}

static int get_data_from_ondisk(struct schema *sc, uint8_t *in,
                                blob_buffer_t *blobs, size_t maxblobs, int fnum,
                                Mem *m, uint8_t flip_orig, const char *tzname)
{
    int null;
    i64 ival = 0;
    int outdtsz = 0;
    int rc = 0;
    struct field *f = &(sc->member[fnum]);
    uint8_t *in_orig;

    in_orig = in = in + f->offset;

    if (f->flags & INDEX_DESCEND) {
        if (gbl_sort_nulls_correctly) {
            in_orig[0] = ~in_orig[0];
        }
        if (flip_orig) {
            xorbufcpy((char *)&in[1], (char *)&in[1], f->len - 1);
        } else {
            switch (f->type) {
            case SERVER_BINT:
            case SERVER_UINT:
            case SERVER_BREAL:
            case SERVER_DATETIME:
            case SERVER_DATETIMEUS:
            case SERVER_INTVYM:
            case SERVER_INTVDS:
            case SERVER_INTVDSUS:
            case SERVER_DECIMAL: {
                /* This way we don't have to flip them back. */
                uint8_t *p = alloca(f->len);
                p[0] = in[0];
                xorbufcpy((char *)&p[1], (char *)&in[1], f->len - 1);
                in = p;
                break;
            }
            default:
                /* For byte and cstring, set the MEM_Xor flag and
                 * sqlite will flip the field */
                break;
            }
        }
    }

    null = stype_is_null(in);
    if (null) { /* this field is null, we dont need to fetch */
        m->z = NULL;
        m->n = 0;
        m->flags = MEM_Null;
        goto done;
    }

    switch (f->type) {
    case SERVER_UINT:
        rc = SERVER_UINT_to_CLIENT_INT(
            in, f->len, NULL /*convopts */, NULL /*blob */, &ival, sizeof(ival),
            &null, &outdtsz, NULL /*convopts */, NULL /*blob */);
        ival = flibc_ntohll(ival);
        m->u.i = ival;
        if (rc == -1)
            goto done;
        if (null)
            m->flags = MEM_Null;
        else
            m->flags = MEM_Int;
        break;

    case SERVER_BINT:
        rc = SERVER_BINT_to_CLIENT_INT(
            in, f->len, NULL /*convopts */, NULL /*blob */, &ival, sizeof(ival),
            &null, &outdtsz, NULL /*convopts */, NULL /*blob */);
        ival = flibc_ntohll(ival);
        m->u.i = ival;
        if (rc == -1)
            goto done;
        if (null) {
            m->flags = MEM_Null;
        } else {
            m->flags = MEM_Int;
        }
        break;

    case SERVER_BREAL: {
        double dval = 0;
        rc = SERVER_BREAL_to_CLIENT_REAL(
            in, f->len, NULL /*convopts */, NULL /*blob */, &dval, sizeof(dval),
            &null, &outdtsz, NULL /*convopts */, NULL /*blob */);
        dval = flibc_ntohd(dval);
        m->u.r = dval;
        if (rc == -1)
            goto done;
        if (null)
            m->flags = MEM_Null;
        else
            m->flags = MEM_Real;
        break;
    }
    case SERVER_BCSTR:
        /* point directly at the ondisk string */
        m->z = (char *)&in[1]; /* skip header byte in front */
        if (flip_orig || !(f->flags & INDEX_DESCEND)) {
            m->n = cstrlenlim((char *)&in[1], f->len - 1);
        } else {
            m->n = cstrlenlimflipped(&in[1], f->len - 1);
        }
        m->flags = MEM_Str | MEM_Ephem;
        break;

    case SERVER_BYTEARRAY:
        /* just point to bytearray directly */
        m->z = (char *)&in[1];
        m->n = f->len - 1;
        m->flags = MEM_Blob | MEM_Ephem;
        break;

    case SERVER_DATETIME:
        if (debug_switch_support_datetimes()) {
            assert(sizeof(server_datetime_t) == f->len);

            db_time_t sec = 0;
            unsigned short msec = 0;

            bzero(&m->du.dt, sizeof(dttz_t));

            /* TMP BROKEN DATETIME */
            if (in[0] == 0) {
                memcpy(&sec, &in[1], sizeof(sec));
                memcpy(&msec, &in[1] + sizeof(db_time_t), sizeof(msec));
                sec = flibc_ntohll(sec);
                msec = ntohs(msec);
                m->du.dt.dttz_sec = sec;
                m->du.dt.dttz_frac = msec;
                m->du.dt.dttz_prec = DTTZ_PREC_MSEC;
                m->flags = MEM_Datetime;
                m->tz = (char *)tzname;
            } else {
                rc = SERVER_BINT_to_CLIENT_INT(
                    in, sizeof(db_time_t) + 1, NULL /*convopts */,
                    NULL /*blob */, &(m->du.dt.dttz_sec),
                    sizeof(m->du.dt.dttz_sec), &null, &outdtsz,
                    NULL /*convopts */, NULL /*blob */);
                if (rc == -1)
                    goto done;

                memcpy(&msec, &in[1] + sizeof(db_time_t), sizeof(msec));
                m->du.dt.dttz_sec = flibc_ntohll(m->du.dt.dttz_sec);
                msec = ntohs(msec);
                m->du.dt.dttz_frac = msec;
                m->du.dt.dttz_prec = DTTZ_PREC_MSEC;
                m->flags = MEM_Datetime;
                m->tz = (char *)tzname;
            }

        } else {
            /* previous broken case, treat as bytearay */
            m->z = (char *)&in[1];
            m->n = f->len - 1;
            if (stype_is_null(in))
                m->flags = MEM_Null;
            else
                m->flags = MEM_Blob;
        }
        break;

    case SERVER_DATETIMEUS:
        if (debug_switch_support_datetimes()) {
            assert(sizeof(server_datetimeus_t) == f->len);
            if (stype_is_null(in)) {
                m->flags = MEM_Null;
            } else {

                db_time_t sec = 0;
                unsigned int usec = 0;

                bzero(&m->du.dt, sizeof(dttz_t));

                /* TMP BROKEN DATETIME */
                if (in[0] == 0) {
                    memcpy(&sec, &in[1], sizeof(sec));
                    memcpy(&usec, &in[1] + sizeof(db_time_t), sizeof(usec));
                    sec = flibc_ntohll(sec);
                    usec = ntohl(usec);
                    m->du.dt.dttz_sec = sec;
                    m->du.dt.dttz_frac = usec;
                    m->du.dt.dttz_prec = DTTZ_PREC_USEC;
                    m->flags = MEM_Datetime;
                    m->tz = (char *)tzname;
                } else {
                    rc = SERVER_BINT_to_CLIENT_INT(
                        in, sizeof(db_time_t) + 1, NULL /*convopts */,
                        NULL /*blob */, &(m->du.dt.dttz_sec),
                        sizeof(m->du.dt.dttz_sec), &null, &outdtsz,
                        NULL /*convopts */, NULL /*blob */);
                    if (rc == -1)
                        goto done;

                    memcpy(&usec, &in[1] + sizeof(db_time_t), sizeof(usec));
                    m->du.dt.dttz_sec = flibc_ntohll(m->du.dt.dttz_sec);
                    usec = ntohl(usec);
                    m->du.dt.dttz_frac = usec;
                    m->du.dt.dttz_prec = DTTZ_PREC_USEC;
                    m->flags = MEM_Datetime;
                    m->tz = (char *)tzname;
                }
            }
        } else {
            /* previous broken case, treat as bytearay */
            m->z = (char *)&in[1];
            m->n = f->len - 1;
            m->flags = MEM_Blob;
        }
        break;

    case SERVER_INTVYM: {
        cdb2_client_intv_ym_t ym;
        rc = SERVER_INTVYM_to_CLIENT_INTVYM(in, f->len, NULL, NULL, &ym,
                                            sizeof(ym), &null, &outdtsz, NULL,
                                            NULL);
        if (rc)
            goto done;

        if (null) {
            m->flags = MEM_Null;
        } else {
            m->flags = MEM_Interval;
            m->du.tv.type = INTV_YM_TYPE;
            m->du.tv.sign = ntohl(ym.sign);
            m->du.tv.u.ym.years = ntohl(ym.years);
            m->du.tv.u.ym.months = ntohl(ym.months);
        }
        break;
    }

    case SERVER_INTVDS: {
        cdb2_client_intv_ds_t ds;

        rc = SERVER_INTVDS_to_CLIENT_INTVDS(in, f->len, NULL, NULL, &ds,
                                            sizeof(ds), &null, &outdtsz, NULL,
                                            NULL);
        if (rc)
            goto done;

        if (null)
            m->flags = MEM_Null;
        else {

            m->flags = MEM_Interval;
            m->du.tv.type = INTV_DS_TYPE;
            m->du.tv.sign = ntohl(ds.sign);
            m->du.tv.u.ds.days = ntohl(ds.days);
            m->du.tv.u.ds.hours = ntohl(ds.hours);
            m->du.tv.u.ds.mins = ntohl(ds.mins);
            m->du.tv.u.ds.sec = ntohl(ds.sec);
            m->du.tv.u.ds.frac = ntohl(ds.msec);
            m->du.tv.u.ds.prec = DTTZ_PREC_MSEC;
        }
        break;
    }

    case SERVER_INTVDSUS: {
        cdb2_client_intv_dsus_t ds;

        rc = SERVER_INTVDSUS_to_CLIENT_INTVDSUS(in, f->len, NULL, NULL, &ds,
                                                sizeof(ds), &null, &outdtsz,
                                                NULL, NULL);
        if (rc)
            goto done;

        if (null)
            m->flags = MEM_Null;
        else {
            m->flags = MEM_Interval;
            m->du.tv.type = INTV_DSUS_TYPE;
            m->du.tv.sign = ntohl(ds.sign);
            m->du.tv.u.ds.days = ntohl(ds.days);
            m->du.tv.u.ds.hours = ntohl(ds.hours);
            m->du.tv.u.ds.mins = ntohl(ds.mins);
            m->du.tv.u.ds.sec = ntohl(ds.sec);
            m->du.tv.u.ds.frac = ntohl(ds.usec);
            m->du.tv.u.ds.prec = DTTZ_PREC_USEC;
        }
        break;
    }

    case SERVER_BLOB2: {
        int len;
        /* get the length of the vutf8 string */
        memcpy(&len, &in[1], 4);
        len = ntohl(len);

        /* TODO use types.c's enum for header length */
        /* if the string is small enough to be stored in the record */
        if (len <= f->len - 5) {
            /* point directly at the ondisk string */
            /* TODO use types.c's enum for header length */
            m->z = (char *)&in[5];

            /* m->n is the blob length */
            m->n = (len > 0) ? len : 0;

            /*fprintf(stderr, "m->n = %d\n", m->n); */
            m->flags |= MEM_Blob;
        } else {
            ondisk_blob_to_sqlite_mem(f, m, blobs, maxblobs);
        }

        break;
    }

    case SERVER_VUTF8: {
        int len;
        /* get the length of the vutf8 string */
        memcpy(&len, &in[1], 4);
        len = ntohl(len);

        /* TODO use types.c's enum for header length */
        /* if the string is small enough to be stored in the record */
        if (len <= f->len - 5) {
            /* point directly at the ondisk string */
            /* TODO use types.c's enum for header length */
            m->z = (char *)&in[5];

            /* sqlite string lengths do not include NULL */
            m->n = (len > 0) ? len - 1 : 0;

            /*fprintf(stderr, "m->n = %d\n", m->n); */
            m->flags = MEM_Str | MEM_Ephem;
        } else {
            ondisk_blob_to_sqlite_mem(f, m, blobs, maxblobs);
        }
        break;
    }

    case SERVER_BLOB: {
        ondisk_blob_to_sqlite_mem(f, m, blobs, maxblobs);
        break;
    }
    case SERVER_DECIMAL:
        m->flags = MEM_Interval;
        m->du.tv.type = INTV_DECIMAL_TYPE;
        m->du.tv.sign = 0;

        decimal_ondisk_to_sqlite(in, f->len, (decQuad *)&m->du.tv.u.dec, &null);

        break;
    default:
        logmsg(LOGMSG_ERROR, "get_data_from_ondisk: unhandled type %d\n",
               f->type);
        break;
    }

done:
    if (flip_orig)
        return rc;

    if (f->flags & INDEX_DESCEND) {
        if (gbl_sort_nulls_correctly) {
            in_orig[0] = ~in_orig[0];
        }
        switch (f->type) {
        case SERVER_BCSTR:
        case SERVER_BYTEARRAY:
            m->flags |= MEM_Xor;
            break;
        }
    }

    return rc;
}

static int bind_stmt_mem(struct schema *sc, sqlite3_stmt *stmt, Mem *m)
{
    int i, rc;
    struct field *f;
    for (i = 0; i < sc->nmembers; i++) {
        f = &sc->member[i];
        if (m[i].flags & MEM_Null)
            rc = sqlite3_bind_null(stmt, i + 1);
        else {
            switch (f->type) {
            case SERVER_UINT:
            case SERVER_BINT:
                rc = sqlite3_bind_int64(stmt, i + 1, m[i].u.i);
                break;
            case SERVER_BREAL:
                rc = sqlite3_bind_double(stmt, i + 1, m[i].u.r);
                break;
            case SERVER_BCSTR:
            case SERVER_VUTF8:
                rc = sqlite3_bind_text(stmt, i + 1, m[i].z, m[i].n, NULL);
                break;
            case SERVER_BYTEARRAY:
            case SERVER_BLOB:
            case SERVER_BLOB2:
                rc = sqlite3_bind_blob(stmt, i + 1, m[i].z, m[i].n, NULL);
                break;
            case SERVER_DATETIME:
            case SERVER_DATETIMEUS:
                rc = sqlite3_bind_datetime(stmt, i + 1, &m[i].du.dt,
                                           (char *)m[i].tz);
                break;
            case SERVER_INTVYM:
            case SERVER_INTVDS:
            case SERVER_INTVDSUS:
            case SERVER_DECIMAL:
                rc = sqlite3_bind_interval(stmt, i + 1, &m[i].du.tv);
                break;
            default:
                logmsg(LOGMSG_ERROR, "Unknown type %d\n", f->type);
                rc = -1;
            }
        }
        if (rc) {
            logmsg(LOGMSG_ERROR, "Bad argument for field:%s type:%d\n", f->name,
                    f->type);
            return rc;
        }
    }
    return 0;
}

void bind_verify_indexes_query(sqlite3_stmt *stmt, void *sm)
{
    struct schema_mem *psm = (struct schema_mem *)sm;
    if (psm->sc)
        bind_stmt_mem(psm->sc, stmt, psm->min);
}

/* verify_indexes_column_value
** Make a hard copy of the result column from an internal sql query
** so that we have access to the result even after the sql thread exits.
**
** pFrom is the result from a sql thread, pTo is a hard copy of pFrom.
** The hard copy will be converted to ondisk format in mem_to_ondisk in
** function indexes_expressions_data.
*/
int verify_indexes_column_value(sqlite3_stmt *stmt, void *sm)
{
    struct schema_mem *psm = (struct schema_mem *)sm;
    Mem *pTo = psm->mout;
    Mem *pFrom = sqlite3_column_value(stmt, 0);
    if (pTo) {
        memcpy(pTo, pFrom, MEMCELLSIZE);
        pTo->db = NULL;
        pTo->szMalloc = 0;
        pTo->zMalloc = NULL;
        pTo->n = 0;
        pTo->z = NULL;
        pTo->flags &= ~MEM_Dyn;
        /* This would allow us to check the column type. */
        if (psm->min)
            psm->min->flags = pFrom->flags;
        if (pFrom->flags & (MEM_Blob | MEM_Str)) {
            if (pFrom->zMalloc && pFrom->szMalloc) {
                pTo->szMalloc = pFrom->szMalloc;
                pTo->zMalloc = malloc(pTo->szMalloc);
                if (pTo->zMalloc == NULL)
                    return SQLITE_NOMEM;
                memcpy(pTo->zMalloc, pFrom->zMalloc, pTo->szMalloc);
                pTo->z = pTo->zMalloc;
                pTo->n = pFrom->n;
            } else if (pFrom->z && pFrom->n) {
                pTo->n = pFrom->n;
                pTo->szMalloc = pFrom->n + 1;
                pTo->zMalloc = malloc(pTo->szMalloc);
                if (pTo->zMalloc == NULL)
                    return SQLITE_NOMEM;
                memcpy(pTo->zMalloc, pFrom->z, pFrom->n);
                pTo->zMalloc[pFrom->n] = 0;
                pTo->z = pTo->zMalloc;
            }
        } else if (pFrom->flags & MEM_Int) {
            pTo->u.i = pFrom->u.i;
        }
    }
    return 0;
}

static int run_verify_indexes_query(char *sql, struct schema *sc, Mem *min,
                                     Mem *mout, int *exist)
{
    struct schema_mem sm;
    sm.sc = sc;
    sm.min = min;
    sm.mout = mout;

    struct sqlclntstate clnt;
    start_internal_sql_clnt(&clnt);
    clnt.dbtran.mode = TRANLEVEL_SOSQL;
    clnt.sql = sql;
    clnt.verify_indexes = 1;
    clnt.schema_mems = &sm;

    int rc = dispatch_sql_query(&clnt);

    if (clnt.has_sqliterow)
        *exist = 1;

    end_internal_sql_clnt(&clnt);

    return rc;
}

unsigned long long verify_indexes(struct dbtable *db, uint8_t *rec,
                                  blob_buffer_t *blobs, size_t maxblobs,
                                  int is_alter)
{
    Mem *m = NULL;
    struct schema *sc;
    strbuf *sql;
    char temp_newdb_name[MAXTABLELEN];
    int i, ixnum, len, rc;

    unsigned long long dirty_keys = 0ULL;

    if (!gbl_partial_indexes)
        return -1ULL;

    if (!rec) {
        logmsg(LOGMSG_ERROR, "%s: invalid input\n", __func__);
        return -1ULL;
    }

    if (db->ix_blob && !blobs) {
        logmsg(LOGMSG_ERROR, "%s: invalid input, no blobs\n", __func__);
        return -1ULL;
    }

    sc = db->schema;
    sql = strbuf_new();

    if (sc == NULL) {
        logmsg(LOGMSG_FATAL, "No .ONDISK tag for table %s.\n", db->tablename);
        abort();
    }

    m = (Mem *)malloc(sizeof(Mem) * MAXCOLUMNS);
    if (m == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed to malloc Mem\n", __func__);
        rc = -1;
        goto done;
    }

    for (i = 0; i < sc->nmembers; i++) {
        memset(&m[i], 0, sizeof(Mem));
        rc = get_data_from_ondisk(sc, rec, blobs, maxblobs, i, &m[i], 0,
                                  "America/New_York");
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to convert to ondisk\n", __func__);
            goto done;
        }
    }

    len = strlen(db->tablename);
    len = crc32c((uint8_t *)db->tablename, len);
    snprintf(temp_newdb_name, MAXTABLELEN, "sc_alter_temp_%X", len);

    for (ixnum = 0; ixnum < db->nix; ixnum++) {
        if (db->ixschema[ixnum]->where == NULL)
            dirty_keys |= (1ULL << ixnum);
        else {
            int exist = 0;
            strbuf_clear(sql);
            strbuf_appendf(sql, "WITH \"%s\"(\"%s\"",
                           is_alter ? temp_newdb_name : db->tablename,
                           sc->member[0].name);
            for (i = 1; i < sc->nmembers; i++) {
                strbuf_appendf(sql, ", \"%s\"", sc->member[i].name);
            }
            strbuf_appendf(sql, ") AS (SELECT @%s", sc->member[0].name);
            for (i = 1; i < sc->nmembers; i++) {
                strbuf_appendf(sql, ", @%s", sc->member[i].name);
            }
            strbuf_appendf(sql, ") SELECT 1 FROM \"%s\" %s",
                           is_alter ? temp_newdb_name : db->tablename,
                           db->ixschema[ixnum]->where);
            rc = run_verify_indexes_query((char *)strbuf_buf(sql), sc, m, NULL,
                                          &exist);
            if (rc) {
                fprintf(stderr, "%s: failed to run internal query, rc %d\n",
                        __func__, rc);
                goto done;
            }
            if (exist)
                dirty_keys |= (1ULL << ixnum);
        }
    }

done:
    if (m)
        free(m);
    strbuf_free(sql);
    if (rc)
        return -1ULL;
    return dirty_keys;
}

static inline void build_indexes_expressions_query(strbuf *sql,
                                                   struct schema *sc,
                                                   char *tblname, char *expr)
{
    int i;
    strbuf_clear(sql);
    strbuf_appendf(sql, "WITH \"%s\"(\"%s\"", tblname, sc->member[0].name);
    for (i = 1; i < sc->nmembers; i++) {
        strbuf_appendf(sql, ", \"%s\"", sc->member[i].name);
    }
    strbuf_appendf(sql, ") AS (SELECT @%s", sc->member[0].name);
    for (i = 1; i < sc->nmembers; i++) {
        strbuf_appendf(sql, ", @%s", sc->member[i].name);
    }
    strbuf_appendf(sql, ") SELECT (%s) FROM \"%s\"", expr, tblname);
}

char *indexes_expressions_unescape(char *expr)
{
    int i, j;
    char *new_expr = NULL;
    if (!expr)
        return NULL;
    if (strlen(expr) > 1) {
        new_expr = calloc(strlen(expr) + 1, sizeof(char));
        if (!new_expr)
            return NULL;
        j = 0;
        for (i = 0; i < strlen(expr) - 1; i++) {
            if (expr[i] == '\\' && expr[i + 1] == '\"') {
                new_expr[j++] = '\"';
                i++;
            } else
                new_expr[j++] = expr[i];
        }
        if (i == strlen(expr) - 1)
            new_expr[j++] = expr[i];
        new_expr[j] = '\0';
    } else {
        new_expr = strdup(expr);
    }
    return new_expr;
}

int indexes_expressions_data(struct schema *sc, const char *inbuf, char *outbuf,
                             blob_buffer_t *blobs, size_t maxblobs,
                             struct field *f,
                             struct convert_failure *fail_reason,
                             const char *tzname)
{
    Mem *m = NULL;
    Mem mout = {{0}};
    int nblobs = 0;
    struct field_conv_opts_tz convopts = {.flags = 0};
    struct mem_info info;
    strbuf *sql;
    int i, rc;
    int exist = 0;

    if (!inbuf || !outbuf) {
        logmsg(LOGMSG_ERROR, "%s: invalid input\n", __func__);
        return -1;
    }

    if (!tzname || !tzname[0])
        tzname = "America/New_York";

    sql = strbuf_new();

    m = (Mem *)alloca(sizeof(Mem) * sc->nmembers);

    for (i = 0; i < sc->nmembers; i++) {
        memset(&m[i], 0, sizeof(Mem));
        rc = get_data_from_ondisk(sc, (uint8_t *)inbuf, blobs, maxblobs, i,
                                  &m[i], 0, tzname);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to convert to ondisk\n", __func__);
            goto done;
        }
    }

    build_indexes_expressions_query(sql, sc, "expridx_temp", f->name);

    rc =
        run_verify_indexes_query((char *)strbuf_buf(sql), sc, m, &mout, &exist);
    if (rc || !exist) {
        logmsg(LOGMSG_ERROR, "%s: failed to run internal query, rc %d\n",
               __func__, rc);
        rc = -1;
        goto done;
    }

    info.s = sc;
    info.null = 0;
    info.fail_reason = fail_reason;
    info.tzname = tzname;
    info.m = &mout;
    info.nblobs = &nblobs;
    info.convopts = &convopts;
    info.outblob = NULL;
    info.maxblobs = maxblobs;
    info.fldidx = -1;

    rc = mem_to_ondisk(outbuf, f, &info, NULL);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: rc %d failed to form index \"%s\", result flag "
               "%x, index type %d\n",
               __func__, rc, f->name, mout.flags, f->type);
        goto done;
    }
done:
    if (mout.zMalloc)
        free(mout.zMalloc);
    strbuf_free(sql);
    if (rc)
        return -1;
    return 0;
}

void comdb2_set_verify_remote_schemas(void)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);

    if (thd && thd->clnt) {
        if (thd->clnt->verify_remote_schemas == 0)
            thd->clnt->verify_remote_schemas = 1;
        else /* anything else disables it */
            thd->clnt->verify_remote_schemas = 2;
    }
}

int comdb2_get_verify_remote_schemas(void)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);

    if (thd && thd->clnt)
        return thd->clnt->verify_remote_schemas == 1;

    return 0;
}

uint16_t stmt_num_tbls(sqlite3_stmt *stmt)
{
    Vdbe *v = (Vdbe *)stmt;
    return v->numTables;
}

int comdb2_save_ddl_context(char *name, void *ctx, comdb2ma mem)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;
    struct clnt_ddl_context *clnt_ddl_ctx = NULL;

    if (!clnt->ddl_contexts)
        return 1;

    clnt_ddl_ctx = calloc(1, sizeof(struct clnt_ddl_context));
    if (clnt_ddl_ctx == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d out of memory\n", __func__, __LINE__);
        return -1;
    }
    clnt_ddl_ctx->name = name;
    clnt_ddl_ctx->ctx = ctx;
    clnt_ddl_ctx->mem = mem;
    hash_add(clnt->ddl_contexts, clnt_ddl_ctx);
    return 0;
}

void *comdb2_get_ddl_context(char *name)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;
    struct clnt_ddl_context *clnt_ddl_ctx = NULL;
    void *ctx = NULL;
    if (clnt->ddl_contexts) {
        clnt_ddl_ctx = hash_find_readonly(clnt->ddl_contexts, &name);
        if (clnt_ddl_ctx)
            ctx = clnt_ddl_ctx->ctx;
    }
    return ctx;
}

int comdb2_check_vtab_access(sqlite3 *db, sqlite3_module *module)
{
    HashElem *current;

    if (!gbl_uses_password) {
        return 0;
    }

    struct sql_thread *thd = pthread_getspecific(query_info_key);

    for (current = sqliteHashFirst(&db->aModule); current;
         current = sqliteHashNext(current)) {
        struct Module *mod = sqliteHashData(current);
        if (module == mod->pModule) {
            int bdberr;
            int rc;

            if ((module->access_flag == 0) ||
                (module->access_flag & CDB2_ALLOW_ALL)) {
                return SQLITE_OK;
            }

            rc = bdb_check_user_tbl_access(
                thedb->bdb_env, thd->clnt->current_user.name,
                (char *)mod->zName, ACCESS_READ, &bdberr);
            if (rc != 0) {
                char msg[1024];
                snprintf(msg, sizeof(msg),
                         "Read access denied to %s for user %s bdberr=%d",
                         mod->zName, thd->clnt->current_user.name, bdberr);
                logmsg(LOGMSG_INFO, "%s\n", msg);
                errstat_set_rc(&thd->clnt->osql.xerr, SQLITE_ACCESS);
                errstat_set_str(&thd->clnt->osql.xerr, msg);
                return SQLITE_AUTH;
            }
            return SQLITE_OK;
        }
    }
    assert(0);
    return 0;
}

int _some_callback(void *theresult, int ncols, char **vals, char **cols)
{
    if ((ncols < 1) || (!vals[0])) {
        logmsg(LOGMSG_ERROR, "%s query failed to retrieve a proper row!\n",
               __func__);
        return SQLITE_ABORT;
    }

    *(long long *)theresult = atoll(vals[0]);

    return SQLITE_OK;
}

long long run_sql_return_ll(const char *sql, struct errstat *err)
{
    struct sql_thread *thd;
    int rc;

    thd = start_sql_thread();
    rc = run_sql_thd_return_ll(sql, thd, err);
    done_sql_thread();

    return rc;
}

long long run_sql_thd_return_ll(const char *query, struct sql_thread *thd,
                                struct errstat *err)
{
    sqlite3 *sqldb;
    int rc;
    int crc;
    char *msg;
    long long ret = LLONG_MIN;

    struct sqlclntstate clnt;
    start_internal_sql_clnt(&clnt);
    strncpy0(clnt.tzname, "UTC", sizeof(clnt.tzname));
    sql_set_sqlengine_state(&clnt, __FILE__, __LINE__, SQLENG_NORMAL_PROCESS);
    clnt.dbtran.mode = TRANLEVEL_SOSQL;
    clnt.sql = (char *)query;
    clnt.debug_sqlclntstate = pthread_self();

    sql_get_query_id(thd);
    thd->clnt = &clnt;

    if ((rc = get_curtran(thedb->bdb_env, &clnt)) != 0) {
        errstat_set_rcstrf(err, -1, "%s: failed to open a new curtran, rc=%d",
                           __func__, rc);
        goto done;
    }

    if ((rc = sqlite3_open_serial("db", &sqldb, NULL)) != 0) {
        errstat_set_rcstrf(err, -1, "%s:sqlite3_open_serial rc %d", __func__,
                           rc);
        goto cleanup;
    }

    if ((rc = sqlite3_exec(sqldb, query, _some_callback, &ret, &msg)) != 0) {
        errstat_set_rcstrf(err, -1, "q:\"%.100s\" failed rc %d: \"%.50s\"",
                           query, rc, msg ? msg : "<unknown error>");
        goto cleanup;
    }

    if ((crc = sqlite3_close_serial(&sqldb)) != 0)
        errstat_set_rcstrf(err, -1, "close rc %d\n", crc);

cleanup:
    crc = put_curtran(thedb->bdb_env, &clnt);
    if (crc && !rc)
        errstat_set_rcstrf(err, -1, "%s: failed to close curtran", __func__);
done:
    end_internal_sql_clnt(&clnt);
    thd->clnt = NULL;
    return ret;
}

struct temptable get_tbl_by_rootpg(const sqlite3 *db, int i)
{
    // aDb[1]: sqlite_temp_master
    hash_t *h = db->aDb[1].pBt[0].temp_tables;
    struct temptable *t = hash_find(h, &i);
    return *t;
}

/* Verify all CHECK constraints against this record.
 * @return
 *     <1 : Internal error
 *     0  : Success
 *     >1 : (n+1)th CHECK constraint failed
 * */
int verify_check_constraints(struct dbtable *table, uint8_t *rec,
                             blob_buffer_t *blobs, size_t maxblobs,
                             int is_alter)
{
    struct schema *sc;
    Mem *m = NULL;
    Mem mout = {{0}};
    int rc = 0; /* Assume all checks succeeded. */

    /* Skip if there are no CHECK constraints. */
    if (table->n_check_constraints <= 0) {
        return 0;
    }

    if (!rec) {
        logmsg(LOGMSG_ERROR, "%s: invalid input\n", __func__);
        return -1;
    }

    sc = table->schema;
    if (sc == NULL) {
        logmsg(LOGMSG_FATAL, "No .ONDISK tag for table %s.\n",
               table->tablename);
        abort();
    }

    m = (Mem *)malloc(sizeof(Mem) * MAXCOLUMNS);
    if (m == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed to malloc Mem\n", __func__);
        rc = -1;
        goto done;
    }

    for (int i = 0; i < sc->nmembers; ++i) {
        memset(&m[i], 0, sizeof(Mem));
        rc = get_data_from_ondisk(sc, rec, blobs, maxblobs, i, &m[i], 0,
                                  "America/New_York");
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to get field from record",
                   __func__);
            rc = -1;
            goto done;
        }
    }

    for (int i = 0; i < table->n_check_constraints; i++) {

        struct sqlclntstate clnt;
        struct schema_mem sm;

        sm.sc = sc;
        sm.min = m;
        sm.mout = &mout;

        start_internal_sql_clnt(&clnt);
        clnt.dbtran.mode = TRANLEVEL_SOSQL;
        clnt.sql = table->check_constraint_query[i];
        clnt.verify_indexes = 1;
        clnt.schema_mems = &sm;

        rc = dispatch_sql_query(&clnt);
        if (rc) {
            rc = -1;
            goto done;
        }

        /* CHECK constraint has passed if we get 1 or NULL. */
        assert(clnt.has_sqliterow);

        if (sm.mout->flags & MEM_Int) {
            if (sm.mout->u.i == 0) {
                /* CHECK constraint failed */
                rc = i + 1;
            } else {
                /* Check constraint passed */
            }
        } else if (sm.mout->flags & MEM_Null) {
            /* Check constraint passed */
        } else if (sm.min->flags & MEM_Null) { // CREATE TABLE ... CHECK (NULL) -- allow all ?
            /* Check constraint passed */
        } else {
            /* CHECK constraint failed */
            rc = i + 1;
        }

        end_internal_sql_clnt(&clnt);

        if (rc) {
            /* Check failed, no need to continue. */
            break;
        }
    }

done:
    if (m)
        free(m);
    return rc;
}

/**
 * If bdb_lock_desired, run recovery (releasing locks)
 * and pause proportionally with the number of retries
 *
 */
int clnt_check_bdb_lock_desired(struct sqlclntstate *clnt)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    int rc;

    if (!thd || !bdb_lock_desired(thedb->bdb_env))
        return 0;

    logmsg(LOGMSG_WARN, "bdb_lock_desired so calling recover_deadlock\n");

    /* scale by number of times we try, cap at 10 seconds */
    int sleepms = 100 * clnt->deadlock_recovered;
    if (sleepms > 10000)
        sleepms = 10000;

    if (gbl_master_swing_osql_verbose)
        logmsg(LOGMSG_DEBUG,
               "%s:%d bdb lock desired, recover deadlock with sleepms=%d\n",
               __func__, __LINE__, sleepms);

    rc = recover_deadlock(thedb->bdb_env, thd, NULL, sleepms);
    if (rc)
        return rc;

    if ((rc = check_recover_deadlock(clnt)))
        return rc;

    if (gbl_master_swing_osql_verbose)
        logmsg(LOGMSG_DEBUG, "%s recovered deadlock\n", __func__);

    clnt->deadlock_recovered++;

    int max_dead_rec =
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SOSQL_MAX_DEADLOCK_RECOVERED);
    if (clnt->deadlock_recovered > max_dead_rec) {
        logmsg(LOGMSG_ERROR, "%s called recover_deadlock too many times %d\n",
               __func__, clnt->deadlock_recovered);
        return -1;
    }

    return 0;
}

void comdb2_dump_blocker(unsigned int lockerid)
{
    struct sql_thread *thd;
    unsigned int clnt_lockerid;

    Pthread_mutex_lock(&gbl_sql_lock);
    LISTC_FOR_EACH(&thedb->sql_threads, thd, lnk)
    {
        if (!(thd->clnt) || !(thd->clnt->dbtran.cursor_tran))
            continue;
        clnt_lockerid = bdb_curtran_get_lockerid(thd->clnt->dbtran.cursor_tran);
        if (lockerid == clnt_lockerid) {
            logmsg(LOGMSG_USER, "id: %u sql: %s\n", thd->id, thd->clnt->sql);
            break;
        }
    }
    Pthread_mutex_unlock(&gbl_sql_lock);
}

int comdb2_is_idx_uniqnulls(BtCursor *pCur)
{
    struct schema *s;

    /* Safety */
    if (pCur->sc->ixnum < 0) {
        return 0;
    }

    s = pCur->db->ixschema[pCur->sc->ixnum];
    return (s->flags & SCHEMA_UNIQNULLS) ? 1 : 0;
}

int comdb2_is_field_indexable(const char *table_name, int fld_idx) {
    struct dbtable *tbl = get_dbtable_by_name(table_name);
    if (tbl) {
        struct field *f = &tbl->schema->member[fld_idx];
        switch (f->type) {
            case SERVER_BCSTR:
            case SERVER_BYTEARRAY:
            case SERVER_BLOB:
            case SERVER_VUTF8:
            case SERVER_BLOB2:
                return 0;
            default:
                return 1;
        }
    }
    return 1;
}
