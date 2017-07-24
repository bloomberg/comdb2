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

#include "limit_fortify.h"
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
#include <socket_pool.h>

#include <cdb2api.h>

#include <plhash.h>
#include <list.h>
#include <ctrace.h>
#include <epochlib.h>

#include <list.h>

#include <sbuf2.h>

#include <bdb_api.h>
#include <bdb_cursor.h>
#include <bdb_fetch.h>

#include "comdb2.h"
#include "crc32c.h"
#include "types.h"
#include "util.h"
#include <fsnap.h>

#include "flibc.h"

#include "sql.h"
#include <sqliteInt.h>
#include <vdbeInt.h>
#include <sqlite_btree.h>
#include <os.h>

#include "debug.h"

#include "osqlsqlthr.h"
#include "osqlshadtbl.h"
#include "bdb_cursor.h"
#include "bdb_sqlstat1.h"
#include "bdb/bdb_schemachange.h"

#include <genid.h>
#include <strbuf.h>
#include "fdb_fend.h"
#include "fdb_access.h"
#include "bdb_osqlcur.h"

#include "debug_switches.h"
#include "logmsg.h"
#include "locks.h"

unsigned long long get_id(bdb_state_type *);

struct temp_cursor;
struct temp_table;
struct temptable {
    struct temp_cursor *cursor;
    struct temp_table *tbl;
    int flags;
    char *name;
    Btree *owner;
    pthread_mutex_t *lk;
};

extern int gbl_partial_indexes;
#define SQLITE3BTREE_KEY_SET_INS(IX) (clnt->ins_keys |= (1ULL << (IX)))
#define SQLITE3BTREE_KEY_SET_DEL(IX) (clnt->del_keys |= (1ULL << (IX)))
extern int gbl_expressions_indexes;
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

extern int sqldbgflag;
extern int gbl_notimeouts;
extern int gbl_move_deadlk_max_attempt;
extern int gbl_fdb_track;
extern int gbl_selectv_rangechk;
extern volatile int gbl_schema_change_in_progress;

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

/* Static rootpages numbers. */
enum { RTPAGE_SQLITE_MASTER = 1, RTPAGE_START = 2 };

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
int currange_cmp(const void *p, const void *q)
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
void currangearr_sort(CurRangeArr *arr)
{
    qsort((void *)arr->ranges, arr->size, sizeof(CurRange *), currange_cmp);
}
void currangearr_merge_neighbor(CurRangeArr *arr)
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
void currangearr_coalesce(CurRangeArr *arr)
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
            logmsg(LOGMSG_USER, "------------------------\n", cr->tbname);
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
    struct sql_thread *thd = pCur->thd;
    int bdberr;
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

int peer_dropped_connection(struct sqlclntstate *clnt)
{
    if (clnt == NULL || clnt->sb == NULL || clnt->conninfo.pid == 0 ||
        clnt->skip_peer_chk) {
        return 0;
    }
    int rc;
    struct pollfd fd = {0};
    fd.fd = sbuf2fileno(clnt->sb);
    fd.events = POLLIN;
    if ((rc = poll(&fd, 1, 0)) == 0)
        return 0;
    if (rc < 0) {
        if (errno == EINTR || errno == EAGAIN)
            return 0;
        logmsg(LOGMSG_ERROR, "%s poll rc:%d errno:%d errstr:%s\n", __func__, rc,
                errno, strerror(errno));
        return 1;
    }
    if ((fd.revents & POLLIN) && clnt->want_query_effects)
        return 0;
    // shouldn't have any events
    return 1;
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

/*
   This is called every time the db does something (find/next/etc. on a cursor).
   The query is aborted if this returns non-zero.
 */
static int sql_tick(struct sql_thread *thd, int uses_bdb_locking)
{
    struct sqlclntstate *clnt;
    int rc;
    extern int gbl_epoch_time;

    gbl_sqltick++;

    clnt = thd->sqlclntstate;

    if (clnt == NULL)
        return 0;

    /* statement cancelled? done */
    if (clnt->stop_this_statement)
        return SQLITE_BUSY;

    if (clnt->statement_timedout)
        return SQLITE_LIMIT;

    if (uses_bdb_locking && bdb_lock_desired(thedb->bdb_env)) {
        int sleepms;

        logmsg(LOGMSG_WARN, "bdb_lock_desired so calling recover_deadlock\n");

        /* scale by number of times we try, cap at 10 seconds */
        sleepms = 100 * clnt->deadlock_recovered;
        if (sleepms > 10000)
            sleepms = 10000;

        rc = recover_deadlock(thedb->bdb_env, thd, NULL, sleepms);

        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "recover_deadlock returned %d\n", rc);
            return (rc == SQLITE_CLIENT_CHANGENODE) ? rc : SQLITE_BUSY;
        }

        logmsg(LOGMSG_DEBUG, "%s recovered deadlock\n", __func__);

        clnt->deadlock_recovered++;
    }

    if (gbl_epoch_time && (gbl_epoch_time - clnt->last_check_time > 5)) {
        clnt->last_check_time = gbl_epoch_time;
        if (!gbl_notimeouts && peer_dropped_connection(clnt)) {
            logmsg(LOGMSG_INFO, "Peer dropped connection \n");
            return SQLITE_BUSY;
        }
    }

    if (clnt->limits.maxcost && (thd->cost > clnt->limits.maxcost))
        /* TODO: we need a nice way to set sqlite3_errmsg() */
        return SQLITE_LIMIT;

    return 0;
}

pthread_key_t query_info_key;
static int query_id = 1;

int comdb2_sql_tick()
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    return sql_tick(thd, 0);
}

void sql_get_query_id(struct sql_thread *thd)
{
    if (thd) {
        pthread_mutex_lock(&gbl_sql_lock);
        thd->id = query_id++;
        pthread_mutex_unlock(&gbl_sql_lock);
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
    thd->query_hash =
        hash_init(offsetof(struct query_path_component, ix) + sizeof(int));
    pthread_mutex_init(&thd->lk, NULL);

    int rc = pthread_setspecific(query_info_key, thd);
    if (rc != 0)
        perror_errnum("start_sql_thread: pthread_setspecific", rc);

    pthread_mutex_lock(&gbl_sql_lock);
    listc_abl(&thedb->sql_threads, thd);
    pthread_mutex_unlock(&gbl_sql_lock);

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
        pthread_mutex_lock(&gbl_sql_lock);
        listc_rfl(&thedb->sql_threads, thd);
        pthread_mutex_unlock(&gbl_sql_lock);
        pthread_mutex_destroy(&thd->lk);
        pthread_setspecific(query_info_key, NULL);
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
        if (thd->rootpages) {
            free(thd->rootpages);
            thd->rootpages = NULL;
        }
        free(thd);
    }
}

static int get_data_int(BtCursor *, struct schema *, uint8_t *in, int fnum,
                        Mem *, uint8_t flip_orig, const char *tzname);

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
    int null;
    Mem *m = NULL;
    u32 *type = NULL;
    int datasz = 0;
    int hdrsz = 0;
    int remainingsz = 0;
    int sz;
    unsigned char *hdrbuf, *dtabuf;
    i64 ival;
    int nummalloc = 0;
    int ncols = 0;
    int blobnum = 0;
    int nField;
    int rec_srt_off = gbl_sort_nulls_correctly ? 0 : 1;
    u32 len;

    /* Raw index optimization */
    if (pCur && pCur->nCookFields >= 0)
        nField = pCur->nCookFields;
    else
        nField = s->nmembers;

    m = (Mem *)alloca(sizeof(Mem) * (nField + 1)); // Extra 1 for genid

    type = (u32 *)alloca(sizeof(u32) * (nField + 1));

#ifdef debug_raw
    printf("convert => %s %s %d / %d\n", db->dbname, s->tag, nField,
           s->nmembers);
#endif

    *reqsize = 0;

    for (fnum = 0; fnum < nField; fnum++) {
        rc = get_data_int(pCur, s, in, fnum, &m[fnum], 1, tzname);
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
        int rc;

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
            strncpy(convopts->tzname, tzname, sizeof(convopts->tzname));
            convopts->flags |= FLD_CONV_TZONE;
        }
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
                f->type == SERVER_BINT ||
            f->type == SERVER_UINT || f->type == SERVER_BREAL) {
            double rValue;
            char *s;
            struct sql_thread *thd = pthread_getspecific(query_info_key);
            struct sqlclntstate *clnt;
            if (thd) {
                clnt = thd->sqlclntstate;
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
    set_data(key + f->offset, db->dbname, strlen(db->dbname) + 1);
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
    snprintf(namebuf, len, "%s_ix_%d", db->dbname, ixnum);
    return stat1_find(namebuf, schema, db, ixnum, trans);
}

void form_new_style_name(char *namebuf, int len, struct schema *schema,
                         const char *csctag, const char *dbname)
{
    char buf[16 * 1024];
    int fieldctr;
    int current = 0;
    unsigned int crc;
    current += snprintf(buf + current, sizeof buf - current, "%s", dbname);
    if (schema->flags & SCHEMA_DATACOPY) {
        current += snprintf(buf + current, sizeof buf - current, "DATACOPY");
    }
    if (schema->flags & SCHEMA_DUP) {
        current += snprintf(buf + current, sizeof buf - current, "DUP");
    }
    if (schema->flags & SCHEMA_RECNUM) {
        current += snprintf(buf + current, sizeof buf - current, "RECNUM");
    }
    for (fieldctr = 0; fieldctr < schema->nmembers; ++fieldctr) {
        current += snprintf(buf + current, sizeof buf - current, "%s",
                            schema->member[fieldctr].name);
        if (schema->member[fieldctr].flags & INDEX_DESCEND) {
            current += snprintf(buf + current, sizeof buf - current, "DESC");
        }
    }
    crc = crc32(0, buf, current);
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
static int sql_index_name_trans(char *namebuf, int len, struct schema *schema,
                                struct dbtable *db, int ixnum, void *trans)
{
    int rc;
    rc = using_old_style_name(namebuf, len, schema, db, ixnum, trans);
    if (rc > 0) {
        /* found old style entry; keep using it */
        return rc;
    }

    form_new_style_name(namebuf, len, schema, schema->csctag, db->dbname);
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
    struct field_conv_opts_tz outopts = {0};
    unsigned long long uival;
    long long ival;
    double dval;
    char *cval = NULL;
    unsigned char *bval = NULL;
    int rc = 0;
    int null;
    int outsz;
    char *dstr = NULL;
    int i;

    default_type = is_out ? f->out_default_type : f->in_default_type;
    this_default = is_out ? f->out_default : f->in_default;
    this_default_len = is_out ? f->out_default_len : f->in_default_len;

    switch (default_type) {
    case SERVER_UINT:
        rc = SERVER_UINT_to_CLIENT_UINT(
            this_default, this_default_len, NULL, NULL, &uival,
            sizeof(unsigned long long), &null, &outsz, NULL, NULL);
        uival = flibc_htonll(uival);
        if (rc == 0)
            dstr = sqlite3_mprintf("%llu", uival);
        break;

    case SERVER_BINT:
        rc = SERVER_BINT_to_CLIENT_INT(this_default, this_default_len, NULL,
                                       NULL, &ival, sizeof(long long), &null,
                                       &outsz, NULL, NULL);
        ival = flibc_htonll(ival);
        if (rc == 0)
            dstr = sqlite3_mprintf("%lld", ival);
        break;

    case SERVER_BREAL:
        rc = SERVER_BREAL_to_CLIENT_REAL(this_default, this_default_len, NULL,
                                         NULL, &dval, sizeof(double), &null,
                                         &outsz, NULL, NULL);
        dval = flibc_htond(dval);
        if (rc == 0)
            dstr = sqlite3_mprintf("%f", dval);
        break;

    case SERVER_BCSTR:
        cval = sqlite3_malloc(this_default_len + 1);
        rc = SERVER_BCSTR_to_CLIENT_CSTR(this_default, this_default_len, NULL,
                                         NULL, cval, this_default_len + 1,
                                         &null, &outsz, NULL, NULL);
        if (rc == 0)
            dstr = sqlite3_mprintf("'%q'", cval);
        break;

    case SERVER_BYTEARRAY:
        /* ... */
        bval = sqlite3_malloc(this_default_len - 1);
        rc = SERVER_BYTEARRAY_to_CLIENT_BYTEARRAY(
            this_default, this_default_len, NULL, NULL, bval,
            this_default_len - 1, &null, &outsz, NULL, NULL);

        dstr = sqlite3_malloc((this_default_len * 2) + 3);
        dstr[0] = 'x';
        dstr[1] = '\'';

        for (i = 0; i < this_default_len - 1; i++)
            snprintf(&dstr[i * 2 + 2], 3, "%02x", bval[i]);
        dstr[i * 2 + 2] = '\'';
        dstr[i * 2 + 3] = 0;

        break;

    case SERVER_DATETIME:
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

    case SERVER_DATETIMEUS:
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

    /* no defaults for blobs or vutf8 */

    default:
        logmsg(LOGMSG_ERROR, "Unknown type in schema: column '%s' "
                        "type %d\n",
                f->name, f->type);
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

/* This creates SQL statements that correspond to a table's schema. These
   statements are used to bootstrap sqlite. */
static int create_sqlmaster_record(struct dbtable *db, void *tran)
{
    struct schema *schema;
    strbuf *sql;
    int field;
    char namebuf[128];
    char *type;
    int ixnum;
    char *dstr = NULL;

    sql = strbuf_new();

    /* do the table */
    strbuf_clear(sql);
    schema = db->schema;

    if (schema == NULL) {
        logmsg(LOGMSG_ERROR, "No .ONDISK tag for table %s.\n", db->dbname);
        strbuf_free(sql);
        return -1;
    }

    if (is_sqlite_stat(db->dbname)) {
        if (db->sql)
            free(db->sql);
        for (int i = 0; i < db->nsqlix; i++) {
            free(db->ixsql[i]);
            db->ixsql[i] = NULL;
        }
        switch (db->dbname[11]) {
        case '1':
            db->sql = strdup("create table sqlite_stat1(tbl,idx,stat);");
            break;
        case '2':
            db->sql =
                strdup("create table sqlite_stat2(tbl,idx,sampleno,sample);");
            break;
        case '4':
            db->sql = strdup(
                "create table sqlite_stat4(tbl,idx,neq,nlt,ndlt,sample);");
            break;
        default:
            abort();
        }
        if (db->ixsql) {
            free(db->ixsql);
            db->ixsql = NULL;
        }
        db->ixsql = calloc(sizeof(char *), db->nix);
        db->nsqlix = 0;
        strbuf_free(sql);
        db->ix_expr = 0;
        db->ix_partial = 0;
        db->ix_blob = 0;
        return 0;
    }

    strbuf_append(sql, "create table ");
    strbuf_append(sql, "\"");
    strbuf_append(sql, db->dbname);
    strbuf_append(sql, "\"");
    strbuf_append(sql, "(");
    for (field = 0; field < schema->nmembers; field++) {
        strbuf_append(sql, "\"");
        strbuf_append(sql, schema->member[field].name);
        strbuf_append(sql, "\"");
        strbuf_append(sql, " ");
        type = sqltype(&schema->member[field], namebuf, sizeof(namebuf));
        if (type == NULL) {
            logmsg(LOGMSG_ERROR, "Unsupported type in schema: column '%s' [%d] "
                            "table %s\n",
                    schema->member[field].name, field, db->dbname);
            return -1;
        }
        strbuf_append(sql, type);
        /* add defaults for write sql */
        if (schema->member[field].in_default) {
            dstr = sql_field_default_trans(&schema->member[field], 0);
            strbuf_append(sql, " DEFAULT");

            if (dstr) {
                strbuf_append(sql, " ");
                strbuf_append(sql, dstr);
                sqlite3_free(dstr);
            } else {
                logmsg(LOGMSG_ERROR, 
                        "Failed to convert default value column '%s' table "
                        "%s type %d\n",
                        schema->member[field].name, db->dbname,
                        schema->member[field].type);
                strbuf_free(sql);
                return -1;
            }
        }

        if (field != schema->nmembers - 1)
            strbuf_append(sql, ", ");
    }
    strbuf_append(sql, ");");
    if (db->sql)
        free(db->sql);
    db->sql = strdup(strbuf_buf(sql));
    if (db->nix > 0) {
        for (int i = 0; i < db->nsqlix; i++) {
            free(db->ixsql[i]);
            db->ixsql[i] = NULL;
        }
        if (db->ixsql) {
            free(db->ixsql);
            db->ixsql = NULL;
        }
        db->ixsql = calloc(sizeof(char *), db->nix);
    }
    ctrace("%s\n", strbuf_buf(sql));
    db->nsqlix = 0;

    /* do the indices */
    for (ixnum = 0; ixnum < db->nix; ixnum++) {
        /* SQLite 3.7.2: index on sqlite_stat is an error */
        if (is_sqlite_stat(db->dbname))
            break;

        strbuf_clear(sql);

        snprintf(namebuf, sizeof(namebuf), ".ONDISK_ix_%d", ixnum);
        schema = find_tag_schema(db->dbname, namebuf);
        if (schema == NULL) {
            logmsg(LOGMSG_ERROR, "No %s tag for table %s\n", namebuf, db->dbname);
            strbuf_free(sql);
            return -1;
        }

        sql_index_name_trans(namebuf, sizeof(namebuf), schema, db, ixnum, tran);
        if (schema->sqlitetag)
            free(schema->sqlitetag);
        schema->sqlitetag = strdup(namebuf);
        if (schema->sqlitetag == NULL) {
            logmsg(LOGMSG_ERROR, "%s malloc (strdup) failed - wanted %u bytes\n",
                    __func__, strlen(namebuf));
            abort();
        }

#if 0
      if (schema->flags & SCHEMA_DUP) {
         strbuf_append(sql, "create index \"");
      } else {
         strbuf_append(sql, "create unique index \"");
      }
#else
        strbuf_append(sql, "create index \"");
#endif

        strbuf_append(sql, namebuf);
        strbuf_append(sql, "\" on ");
        strbuf_append(sql, "\"");
        strbuf_append(sql, db->dbname);
        strbuf_append(sql, "\"");
        strbuf_append(sql, " (");
        for (field = 0; field < schema->nmembers; field++) {
            if (field > 0)
                strbuf_append(sql, ", ");
            if (schema->member[field].isExpr) {
                if (!gbl_expressions_indexes) {
                    logmsg(LOGMSG_ERROR, "EXPRESSIONS INDEXES FOUND IN SCHEMA! PLEASE FIRST "
                            "ENABLE THE EXPRESSIONS INDEXES FEATURE.\n");
                    if (db->iq)
                        reqerrstr(db->iq, ERR_SC,
                                  "Please enable indexes on expressions.");
                    strbuf_free(sql);
                    return -1;
                }
                strbuf_append(sql, "(");
            }
            strbuf_append(sql, schema->member[field].name);
            if (schema->member[field].isExpr)
                strbuf_append(sql, ")");
            if (schema->member[field].flags & INDEX_DESCEND)
                strbuf_append(sql, " DESC");
        }

        if (schema->flags & SCHEMA_DATACOPY) {
            struct schema *ondisk = db->schema;
            int datacopy_pos = 0;
            size_t need;
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

                strbuf_append(sql, ", \"");
                strbuf_append(sql, ondisk_field->name);
                strbuf_append(sql, "\"");
                /* stop optimizer by adding dummy collation */
                if (datacopy_pos == 0) {
                    strbuf_append(sql, " collate DATACOPY");
                    need = ondisk->nmembers * sizeof(schema->datacopy[0]);
                    schema->datacopy = (int *)malloc(need);
                    if (schema->datacopy == NULL) {
                        logmsg(LOGMSG_ERROR, 
                                "Could not malloc for datacopy lookup array\n");
                        strbuf_free(sql);
                        return -1;
                    }
                }
                /* datacopy_pos is i-th ondisk */
                schema->datacopy[datacopy_pos] = ondisk_i;
                ++datacopy_pos;
            }
        }

        strbuf_append(sql, ")");
        if (schema->where) {
            if (!gbl_partial_indexes) {
                logmsg(LOGMSG_ERROR, "PARTIAL INDEXES FOUND IN SCHEMA! PLEASE FIRST "
                                "ENABLE THE PARTIAL INDEXES FEATURE.\n");
                if (db->iq)
                    reqerrstr(db->iq, ERR_SC, "Please enable partial indexes.");
                strbuf_free(sql);
                return -1;
            }
            strbuf_append(sql, " where (");
            strbuf_append(sql, schema->where + 6);
            strbuf_append(sql, ")");
        }
        strbuf_append(sql, ";");
        if (db->ixsql[ixnum])
            free(db->ixsql[ixnum]);
        if (field > 0) {
            db->ixsql[ixnum] = strdup(strbuf_buf(sql));
            ctrace("  %s\n", strbuf_buf(sql));
            db->nsqlix++;
        } else {
            db->ixsql[ixnum] = NULL;
        }
    }

    strbuf_free(sql);

    return 0;
}

/* create and write SQL statements. uses ondisk schema. writes the sql
 * statements to stdout as per Alex's request. */
int create_sqlmaster_records(void *tran)
{
    int table;
    int rc = 0;
    sql_mem_init(NULL);
    for (table = 0; table < thedb->num_dbs; table++) {
        rc = create_sqlmaster_record(thedb->dbs[table], tran);
        if (rc)
            goto done;
    }
done:
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
            snprintf(buf, len, "float");
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

char comdb2_maxkey[MAXKEYLEN];

/* Called once from comdb2. Do all static intialization here */
void sqlinit(void)
{
    memset(comdb2_maxkey, 0xff, sizeof(comdb2_maxkey));
    pthread_mutex_init(&gbl_sql_lock, NULL);
    sql_dlmalloc_init();
}

/* Calculate space needed to store a sqlite version of a record for
   a given schema */
int schema_var_size(struct schema *sc)
{
    int i;
    Mem m;
    u64 len;
    int sz;

    sz = 0;
    m.xDel = NULL;
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

static void *create_master_table(int eidx, const char *csc2_schema, int tblnum,
                                 int ixnum, int *sz)
{
    struct field field[6] = {
        /* type name tbl_name rootpage sql */
        {SERVER_BCSTR, 0, 0, 0, "type", 0, -1, 0, 0, NULL, 0, 0, NULL, 0},
        {SERVER_BCSTR, 0, 0, 0, "name", 0, -1, 0, 0, NULL, 0, 0, NULL, 0},
        {SERVER_BCSTR, 0, 0, 0, "tbl_name", 0, -1, 0, 0, NULL, 0, 0, NULL, 0},
        {SERVER_BINT, 0, 0, 0, "rootpage", 0, -1, 0, 0, NULL, 0, 0, NULL, 0},
        {SERVER_BCSTR, 0, 0, 0, "sql", 0, -1, 0, 0, NULL, 0, 0, NULL, 0},
        {SERVER_BCSTR, 0, 0, 0, "csc2", 0, -1, 0, 0, NULL, 0, 0, NULL, 0}};
    char *sql;
    char *etype;
    char *e;
    int offset = 0;
    void *rec;
    char name[128];
    char *dbname;
    int rootpage;
    int rc, outdtsz = 0;
    struct dbtable *db;
    int reqsize;
    int maxlen = 0;

    struct schema sc = {0};
    sc.tag = ".ONDISK";
    sc.nmembers = 6;
    sc.ixnum = -1;

    assert(tblnum < thedb->num_dbs);

    db = thedb->dbs[tblnum];
    dbname = db->dbname;
    rootpage = eidx + RTPAGE_START;

    if (ixnum == -1) {
        strcpy(name, dbname);
        sql = db->sql;
        etype = "table";
    } else {
        snprintf(name, sizeof(name), ".ONDISK_ix_%d", ixnum);
        struct schema *schema = find_tag_schema(dbname, name);
        if (schema->sqlitetag) {
            strcpy(name, schema->sqlitetag);
        } else {
            sql_index_name_trans(name, sizeof name, schema, db, ixnum, NULL);
        }

        sql = db->ixsql[ixnum];
        etype = "index";
    }
    ctrace("rootpage %d sql %s\n", rootpage, sql);

    /* patch schema above with values specific to this record
     * (can't think of any other way to cram variable-length
     * records into current scheme) */
    sc.member = field;
    field[0].offset = offset;
    field[0].len = 1 + strlen(etype); /* type = "table" */
    offset += field[0].len;
    field[1].offset = offset;
    field[1].len = 1 + strlen(name);
    offset += field[1].len;
    field[2].offset = offset;
    field[2].len = 1 + strlen(dbname);
    offset += field[2].len;
    field[3].offset = offset;
    field[3].len = 5; /* ondisk-type integer */
    offset += field[3].len;
    field[4].offset = offset;
    field[4].len = 1 + strlen(sql);
    offset += field[4].len;
    field[5].offset = offset;
    if (csc2_schema) {
        field[5].len = 1 + strlen(csc2_schema);
        offset += field[5].len;
    } else {
        field[5].len = 0;
        offset++; /* still need a null byte */
    }

    e = malloc(offset); /* temporary buffer for ondisk record */

    /* The next step is very ugly: create the ondisk-format record for this
     * table.  None of the ops below should fail */
    offset = 0;
    CLIENT_CSTR_to_SERVER_BCSTR(etype, strlen(etype) + 1, 0, NULL /*convopts */,
                                NULL /*blob */, e + offset, 6, &outdtsz,
                                NULL /*convopts */, NULL /*blob */);
    offset += field[0].len;
    CLIENT_CSTR_to_SERVER_BCSTR(name, strlen(name) + 1, 0, NULL /*convopts */,
                                NULL /*blob */, e + offset, strlen(name) + 1,
                                &outdtsz, NULL /*convopts */, NULL /*blob */);
    offset += field[1].len;
    CLIENT_CSTR_to_SERVER_BCSTR(dbname, strlen(dbname) + 1, 0,
                                NULL /*convopts */, NULL /*blob */, e + offset,
                                strlen(dbname) + 1, &outdtsz,
                                NULL /*convopts */, NULL /*blob */);
    offset += field[2].len;
    rootpage = htonl(rootpage);
    CLIENT_INT_to_SERVER_BINT(&rootpage, 4, 0, NULL /*convopts */,
                              NULL /*blob */, e + offset, 5, &outdtsz,
                              NULL /*convopts */, NULL /*blob */);
    offset += field[3].len;
    CLIENT_CSTR_to_SERVER_BCSTR(sql, strlen(sql) + 1, 0, NULL /*convopts */,
                                NULL /*blob */, e + offset, strlen(sql) + 1,
                                &outdtsz, NULL /*convopts */, NULL /*blob */);
    offset += strlen(sql) + 1;

    CLIENT_CSTR_to_SERVER_BCSTR(
        csc2_schema, csc2_schema ? strlen(csc2_schema) + 1 : 1,
        csc2_schema ? 0 : 1, NULL, NULL, e + offset,
        csc2_schema ? strlen(csc2_schema) + 1 : 0, &outdtsz, NULL, NULL);

    *sz = schema_var_size(&sc);
    /* finally, convert to sqlite format */
    rec = malloc(*sz); /* allocate using worst case size */
    rc = ondisk_to_sqlite(db, &sc, e, 0, 0, rec, *sz, 0, NULL, NULL, NULL,
                          &reqsize);
    if (rc == -2) {
        rec = realloc(rec, reqsize);
        rc = ondisk_to_sqlite(db, &sc, e, 0, 0, rec, *sz, 0, NULL, NULL, NULL,
                              &reqsize);
    }
    if (rc < 0) {
        /* FATAL: can't continue */
        logmsg(LOGMSG_FATAL, "%s: ondisk_to_sqlite failed!!! rc %d\n", __func__, rc);
        exit(1);
    }
    *sz = reqsize;
    free(e);
    return rec;
}

/* table 1 entries */
static int gbl_rootpage_nentries;
static int *gbl_master_entry_sizes;
static void **gbl_master_table_entries;
static struct rootpage *gbl_rootpages;

/* map iTable entries to tblnum/ixnum */
struct rootpage {
    int tblnum;
    int ixnum;
};

// get rootpage from global structure -- not used
static int get_rootpage_info(int rootpage, int *tbl, int *ixnum)
{
    if (rootpage <= RTPAGE_SQLITE_MASTER)
        return -1;
    *tbl = gbl_rootpages[rootpage - RTPAGE_START].tblnum;
    *ixnum = gbl_rootpages[rootpage - RTPAGE_START].ixnum;
    return 0;
}

/* get tblnum and ixnum from local cached rootpage structure
 */
inline void get_sqlite_tblnum_and_ixnum(struct sql_thread *thd, int iTable,
                                        int *tblnum, int *ixnum)
{
    if (!thd->rootpages) {
        logmsg(LOGMSG_ERROR, "get_sqlite_tblnum_and_ixnum() local thd missing copy "
                        "of rootpages\n");
    }
    if (iTable < RTPAGE_START ||
        iTable >= (thd->rootpage_nentries + RTPAGE_START)) {
        *tblnum = -1;
        *ixnum = -3;
        return;
    }
    struct rootpage *rp = &thd->rootpages[iTable - RTPAGE_START];
    *tblnum = rp->tblnum;
    *ixnum = rp->ixnum;
}

static int get_sqlitethd_tblnum(struct sql_thread *thd, int iTable)
{
    if (iTable < RTPAGE_START ||
        iTable >= (thd->rootpage_nentries + RTPAGE_START))
        return -1;
    return thd->rootpages[iTable - RTPAGE_START].tblnum;
}

int get_sqlite_entry_size(int n) { return gbl_master_entry_sizes[n]; }

void *get_sqlite_entry(int n) { return gbl_master_table_entries[n]; }

int send_master_table_by_name(const char *tbl, SBUF2 *sb)
{
    void *rec = NULL;
    int ix = 0;
    int len = strlen(tbl);
    int entry_ix = 0;

    rdlock_schema_lk();
    for (ix = 0; ix < thedb->num_dbs; ix++) {
        if (len == strlen(thedb->dbs[ix]->dbname) &&
            strncmp(thedb->dbs[ix]->dbname, tbl, len) == 0) {
            /* found it, and it's records are
             * gbl_master_table_entries[entry_ix..(entry_ix+thedb->dbs[ix]->nsqlix-1)]
             */
            break;
        }
        entry_ix += 1 + thedb->dbs[ix]->nsqlix;
    }
    wrlock_schema_lk();

    return 0;
}

pthread_rwlock_t sqlite_rootpages = PTHREAD_RWLOCK_INITIALIZER;

/* allocate a range of rootpage numbers
   we need to keep WR sqite_rwlock until all
   of them are consumed !
 */
int get_rootpage_numbers(int nums)
{
    static int crt_rootpage_number = RTPAGE_START;
    int tmp;

    pthread_rwlock_wrlock(&sqlite_rootpages);

    /* if this recreates everything, reset range */
    if (gbl_rootpage_nentries == 0) {
        crt_rootpage_number = RTPAGE_START;
    }
    tmp = crt_rootpage_number + nums;
    if (tmp < crt_rootpage_number) {
        /* slow codepath, to be done ; we coud force a search and get subranges
         */
        abort();
    } else {
        /* got a new number */
        tmp =
            crt_rootpage_number; /* cache start of area, just allocated
                                    [crt_rootpage_number:crt_rootpage_number+nums-1],
                                    inclusive */
        crt_rootpage_number += nums;
    }
    pthread_rwlock_unlock(&sqlite_rootpages);

    /*fprintf(stderr, "XXX allocated [%d:%d]\n", tmp, crt_rootpage_number-1);*/

    return tmp;
}

/* copy rootpage info so a sql thread as a local copy
 */
void get_copy_rootpages_nolock(struct sql_thread *thd)
{
    if (thd->rootpages)
        free(thd->rootpages);
    thd->rootpages = calloc(gbl_rootpage_nentries, sizeof(struct rootpage));
    memcpy(thd->rootpages, gbl_rootpages,
           gbl_rootpage_nentries * sizeof(struct rootpage));
    thd->rootpage_nentries = gbl_rootpage_nentries;
}

/* copy rootpage info so a sql thread as a local copy
 */
inline void get_copy_rootpages(struct sql_thread *thd)
{
    rdlock_schema_lk();
    get_copy_rootpages_nolock(thd);
    unlock_schema_lk();
}

inline void free_copy_rootpages(struct sql_thread *thd)
{
    if (thd->rootpages)
        free(thd->rootpages);
}

/* bootstrap a master table entry - called by comdb2 at startup and when schema
 * changes */
/* This code is very ugly.  Should clean up at first oportunity. */
void create_master_tables(void)
{
    int offset = 0;
    char *sql;
    char *e;
    int rootpageNum;
    char *etype; /* entry type */
    int rc;
    int sz;
    int eidx = 0;
    int tblnum, ixnum;
    int i;
    int nix;
    int local_nentries;
    static struct rootpage *new_rootpages = NULL;
    static struct rootpage *old_rootpages = NULL;

    /* free old */
    for (i = 0; i < gbl_rootpage_nentries; i++) {
        if (gbl_master_table_entries[i]) {
            free(gbl_master_table_entries[i]);
            gbl_master_table_entries[i] = NULL;
        }
    }

    gbl_rootpage_nentries = 0;
    /* make room for fake tables */
    local_nentries = 0;
    for (tblnum = 0; tblnum < thedb->num_dbs; tblnum++)
        local_nentries += 1 + thedb->dbs[tblnum]->nsqlix;

    gbl_master_entry_sizes =
        realloc(gbl_master_entry_sizes, local_nentries * sizeof(int));
    memset(gbl_master_entry_sizes, 0, local_nentries * sizeof(int));

    gbl_master_table_entries =
        realloc(gbl_master_table_entries, local_nentries * sizeof(void **));
    memset(gbl_master_table_entries, 0, local_nentries * sizeof(void **));
    if (gbl_rootpages)
        old_rootpages = gbl_rootpages;
    new_rootpages = calloc(local_nentries, sizeof(struct rootpage));

    rootpageNum = get_rootpage_numbers(local_nentries);
    gbl_rootpage_nentries = local_nentries;

    eidx =
        rootpageNum - RTPAGE_START; /* Start of tables (after sqlite_master) */

    for (tblnum = 0; tblnum < thedb->num_dbs; tblnum++) {
        new_rootpages[eidx].tblnum = tblnum;
        new_rootpages[eidx].ixnum = -1;
        gbl_master_table_entries[eidx] =
            create_master_table(eidx, thedb->dbs[tblnum]->csc2_schema, tblnum,
                                -1, &gbl_master_entry_sizes[eidx]);
        eidx++;
#if 0
      printf
         ("create_master_entry: eidx %d rootpage %d tblname %s tblnum %d ixnum %d sz %d\n",
          eidx, rootpage, thedb->dbs[tblnum]->dbname, tblnum, -1, gbl_master_entry_sizes[eidx]);
#endif

        nix = thedb->dbs[tblnum]->nix;

        for (ixnum = 0; ixnum < nix; ixnum++) {
            /* skip indexes that we aren't advertising to sqlite */
            if (thedb->dbs[tblnum]->ixsql[ixnum] != NULL) {
                new_rootpages[eidx].tblnum = tblnum;
                new_rootpages[eidx].ixnum = ixnum; /* comdb2 index number */
                if (gbl_master_table_entries[eidx]) {
                    free(gbl_master_table_entries[eidx]);
                    gbl_master_table_entries[eidx] = NULL;
                }
                gbl_master_table_entries[eidx] = create_master_table(
                    eidx, NULL, tblnum, ixnum, &gbl_master_entry_sizes[eidx]);
#if 0
            printf
               ("create_master_entry: eidx %d rootpage %d tblnum %d ixnum %d sz %d\n",
                eidx, rootpage, tblnum, ixnum, gbl_master_entry_sizes[eidx]);
            hexdump(1, gbl_master_table_entries[eidx], gbl_master_entry_sizes[eidx]);
#endif
                eidx++;
            }
        }
    }

    assert(eidx == gbl_rootpage_nentries);

    gbl_rootpages = new_rootpages;
    if (old_rootpages)
        free(old_rootpages);
#if 0
   for (i = 0; i < gbl_rootpage_nentries; i++) {
      printf("entry %d\n", i);
      hexdump(gbl_master_table_entries[i], gbl_master_entry_sizes[i]);
   }
#endif
}

/* force an update on sqlite_master to test partial indexes syntax*/
int new_indexes_syntax_check(struct ireq *iq)
{
    int rc = 0;
    sqlite3 *hndl = NULL;
    struct sqlclntstate client;
    const char *temp = "select 1 from sqlite_master limit 1";
    char *err = NULL;
    int got_curtran = 0;

    if (!gbl_partial_indexes)
        return -1;

    sql_mem_init(NULL);

    reset_clnt(&client, NULL, 1);
    client.sb = NULL;
    client.sql = (char *)temp;
    sql_set_sqlengine_state(&client, __FILE__, __LINE__, SQLENG_NORMAL_PROCESS);
    client.dbtran.mode = TRANLEVEL_SOSQL;

    struct sql_thread *sqlthd = start_sql_thread();
    sql_get_query_id(sqlthd);
    client.debug_sqlclntstate = pthread_self();
    sqlthd->sqlclntstate = &client;

    get_copy_rootpages_nolock(sqlthd);

    rc = sqlite3_open_serial("db", &hndl, NULL);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: sqlite3_open failed\n", __func__);
        goto done;
    }

    rc = get_curtran(thedb->bdb_env, &client);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: unable to get a CURSOR transaction, rc = %d!\n",
                __func__, rc);
        goto done;
    }
    got_curtran = 1;

    rc = sqlite3_exec(hndl, client.sql, NULL, NULL, &err);
done:
    if (err) {
        logmsg(LOGMSG_ERROR, "New indexes syntax error: \"%s\"\n", err);
        if (iq)
            reqerrstr(iq, ERR_SC, "%s", err);
        sqlite3_free(err);
    }
    if (got_curtran && put_curtran(thedb->bdb_env, &client))
        logmsg(LOGMSG_ERROR, "%s: failed to close curtran\n", __func__);
    if (hndl)
        sqlite3_close(hndl);
    done_sql_thread();
    sql_mem_shutdown(NULL);
    return rc;
}

/* return true if we are in a transaction mode
 * different from TRANLEVEL_SERIAL, TRANLEVEL_SNAPISOL,
 * and TRANLEVEL_RECOM
 * If we are in one of the above modes, return true
 * only if there is no dirty pages written to that table
 *
 */
static int has_no_read_dirty_data(int tblnum)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->sqlclntstate;

    if (clnt->dbtran.mode != TRANLEVEL_SERIAL &&
        clnt->dbtran.mode != TRANLEVEL_SNAPISOL &&
        clnt->dbtran.mode != TRANLEVEL_RECOM) {
        /* Other modes don't do transactional reads */
        return 1;
    }

    /* if table number is in the bitmap */
    /* and Table has not been written into */
    if (tblnum < sizeof(clnt->dirty) * 8 && btst(clnt->dirty, tblnum) == 0) {
        return 1;
    }

    return 0;
}

static int move_is_nop(BtCursor *pCur, int *pRes)
{
    if (*pRes == 1 && pCur->cursor_class == CURSORCLASS_INDEX) {
        struct schema *s = pCur->db->ixschema[pCur->sc->ixnum];
        if ((s->flags & SCHEMA_DUP) == 0) { /* unique index? */
            return has_no_read_dirty_data(pCur->tblnum);
        }
    }
    return 0;
}

/*
   GLUE BETWEEN SQLITE AND COMDB2.

   This is the sqlite btree layer and other pieces of the sqlite backend
   re-implemented and/or stubbed out.  sqlite3Btree* calls are provided here.
   The sqlite library "thinks" it is running un-modified.

   Please only expose interfaces in this file that are ACTUAL sqlite interfaces
   needed to host the library.  All other code belongs in sqlsupport.c
 */

#include <stdlib.h>
#include <stdio.h>
#include <strings.h>
#include <sqlite3.h>
#include "sqliteInt.h"

/*#include "vdbe.h"*/

#include "os.h"
#include <assert.h>
#include <stdarg.h>
#include <stddef.h>
#include <limits.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <alloca.h>

#include <plhash.h>

#include <list.h>

#include <sbuf2.h>
#include <bdb_api.h>

#include "comdb2.h"
#include "types.h"
#include "util.h"

#include "debug.h"
#include "sqlconstraints.h"
#include "sqlinterfaces.h"
#include <bdb_api.h>
#include <time.h>
#include <poll.h>

#include <sqlite3.h>

#include "debug_switches.h"

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
static int cursor_move_preprop(BtCursor *pCur, int *pRes, int how, int *done,
                               int uses_bdb_locking)
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
        /*printf("tbl %s first/last cost %f\n", pCur->db ? pCur->db->dbname :
         * "<temp>", pCur->find_cost); */
        thd->cost += pCur->find_cost;
        pCur->nfind++;
        break;

    case CPREV:
    case CNEXT:
        /*printf("tbl %s next/prev cost %f\n", pCur->db ? pCur->db->dbname :
         * "<temp>", pCur->move_cost); */
        thd->cost += pCur->move_cost;
        pCur->nmove++;
        break;
    }

    rc = sql_tick(thd, uses_bdb_locking);
    if (rc) {
        *done = 1;
        return rc;
    }

    if (thd->sqlclntstate->is_analyze &&
        (gbl_schema_change_in_progress || get_analyze_abort_requested())) {
        if (gbl_schema_change_in_progress)
            logmsg(LOGMSG_ERROR, 
                    "%s: Aborting Analyze because schema_change_in_progress\n",
                    __func__);
        if (get_analyze_abort_requested())
            logmsg(LOGMSG_ERROR, 
                    "%s: Aborting Analyze because of send analyze abort\n",
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
    int n = is_stat2(db->dbname) ? 3 : 2;
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
    int rc;
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
    rc = bdb_temp_hash_insert(cur->bt->genid_hash, &key, sizeof(struct key),
                              &genid, sizeof(genid));
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
    struct ireq iq; /* = { 0 }; */
    void *fndkeybuf;
    int fndlen;
    int bdberr = 0;
    int rrn;
    int done = 0;
    int rc = SQLITE_OK;
    int outrc = SQLITE_OK;
    uint8_t ver;
    struct sqlclntstate *clnt = thd->sqlclntstate;

    if (access_control_check_sql_read(pCur, thd)) {
        return SQLITE_ACCESS;
    }

    rc = cursor_move_preprop(pCur, pRes, how, &done, 1);
    if (done) {
        return rc;
    }

    /* If no tablescans are allowed, return an error */
    if (!thd->sqlclntstate->limits.tablescans_ok) {
        return SQLITE_LIMIT;
    }

    if (thd)
        thd->had_tablescans = 1;

    iq.dbenv = thedb;
    iq.is_fake = 1;
    iq.usedb = pCur->db;
    iq.opcode = OP_FIND;

    outrc = SQLITE_OK;
    *pRes = 0;
    if (thd)
        thd->nmove++;

    bdberr = 0;
    rc = ddguard_bdb_cursor_move(thd, pCur, 0, &bdberr, how, NULL, 0);
    if (bdberr == BDBERR_NOT_DURABLE) {
        return SQLITE_CLIENT_CHANGENODE;
    }
    if (bdberr == BDBERR_TRANTOOCOMPLEX) {
        return SQLITE_TRANTOOCOMPLEX;
    }
    if (bdberr == BDBERR_TRAN_CANCELLED) {
        return SQLITE_TRAN_CANCELLED;
    }
    if (bdberr == BDBERR_NO_LOG) {
        return SQLITE_TRAN_NOLOG;
    }
    if (bdberr == BDBERR_DEADLOCK) {
        logmsg(LOGMSG_ERROR, "%s: too much contention, retried %d times [%llx]\n",
                __func__, gbl_move_deadlk_max_attempt,
                (thd->sqlclntstate && thd->sqlclntstate->osql.rqid)
                    ? thd->sqlclntstate->osql.rqid
                    : 0);
        ctrace("%s: too much contention, retried %d times [%llx]\n", __func__,
               gbl_move_deadlk_max_attempt,
               (thd->sqlclntstate && thd->sqlclntstate->osql.rqid)
                   ? thd->sqlclntstate->osql.rqid
                   : 0);
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
                thd->sqlclntstate->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
                rc = osql_record_genid(pCur, thd, pCur->genid);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "%s: failed to record genid %llx (%llu)\n",
                            __func__, pCur->genid, pCur->genid);
                }
            }
        }

        rc = 0;
    } else if (rc == IX_ACCESS) {
        return SQLITE_ACCESS;
    } else if (rc) {
        logmsg(LOGMSG_ERROR, "%s dir %d rc %d bdberr %d\n", __func__, how, rc, bdberr);
        outrc = SQLITE_INTERNAL;
    }

#ifdef DEBUG
    if (gbl_debug_sql_opcodes) {
        fprintf(stdout, "MOVE [%s] : genid=%llx pRes=%d how=%d rc=%d\n",
                pCur->db->dbname, pCur->genid, *pRes, how, outrc);
        fflush(stdout);
    }
#endif

    return outrc;
}

static int cursor_move_index(BtCursor *pCur, int *pRes, int how)
{
    struct sql_thread *thd = pCur->thd;
    struct ireq iq;
    void *fndkeybuf;
    int fndlen;
    int rrn;
    int bdberr = 0;
    int done = 0;
    int rc = SQLITE_OK;
    int outrc = SQLITE_OK;
    struct sqlclntstate *clnt = thd->sqlclntstate;

    if (access_control_check_sql_read(pCur, thd)) {
        return SQLITE_ACCESS;
    }

    rc = cursor_move_preprop(pCur, pRes, how, &done, 1);
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

    bdberr = 0;
    if (how == CNEXT) {
        pCur->num_nexts++;

        if (gbl_prefaulthelper_sqlreadahead)
            if (pCur->num_nexts == gbl_sqlreadaheadthresh) {
                pCur->num_nexts = 0;
                readaheadpf(&iq, pCur->db, pCur->ixnum, pCur->fndkey,
                            getkeysize(pCur->db, pCur->ixnum),
                            gbl_sqlreadahead);
            }
    }

    rc = ddguard_bdb_cursor_move(thd, pCur, 0, &bdberr, how, &iq, 0);
    if (bdberr == BDBERR_NOT_DURABLE) {
        return SQLITE_CLIENT_CHANGENODE;
    }
    if (bdberr == BDBERR_TRANTOOCOMPLEX) {
        return SQLITE_TRANTOOCOMPLEX;
    }
    if (bdberr == BDBERR_TRAN_CANCELLED) {
        return SQLITE_TRAN_CANCELLED;
    }
    if (bdberr == BDBERR_NO_LOG) {
        return SQLITE_TRAN_NOLOG;
    }
    if (bdberr == BDBERR_DEADLOCK) {
        logmsg(LOGMSG_ERROR, "%s too much contention, retried %d times.\n", __func__,
                gbl_move_deadlk_max_attempt);
        ctrace("%s: too much contention, retried %d times [%llx]\n", __func__,
               gbl_move_deadlk_max_attempt,
               (thd->sqlclntstate && thd->sqlclntstate->osql.rqid)
                   ? thd->sqlclntstate->osql.rqid
                   : 0);
        return SQLITE_DEADLOCK;
    } else if (rc == IX_FND || rc == IX_NOTFND) {
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
    } else if (rc == IX_ACCESS) {
        return SQLITE_ACCESS;
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
                thd->sqlclntstate->ctrl_sqlengine == SQLENG_INTRANS_STATE) {

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

    rc = cursor_move_preprop(pCur, pRes, how, &done, 0);
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
    int bdberr = 0;
    int done = 0;
    int rc = SQLITE_OK;

    rc = cursor_move_preprop(pCur, pRes, how, &done, 0);
    if (done)
        return rc;

    switch (how) {
    case CFIRST:
        rc = bdb_temp_table_first(thedb->bdb_env, pCur->sampled_idx->cursor,
                                  &bdberr);
        break;

    case CLAST:
        rc = bdb_temp_table_last(thedb->bdb_env, pCur->sampled_idx->cursor,
                                 &bdberr);
        break;

    case CPREV:
        rc = bdb_temp_table_prev(thedb->bdb_env, pCur->sampled_idx->cursor,
                                 &bdberr);
        break;

    case CNEXT:
        rc = bdb_temp_table_next(thedb->bdb_env, pCur->sampled_idx->cursor,
                                 &bdberr);
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
        void *tmp = bdb_temp_table_key(pCur->sampled_idx->cursor);

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
    int done = 0;
    int rc = SQLITE_OK;

    /* skip preprop. if we're called from sqlite3_open_serial
     * and if peer_dropped_connection is true, we'll get NO SQL ENGINE and
     * a wasted thread apparently.
     rc = cursor_move_preprop(pCur, pRes, how, &done, 0);
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
    struct sqlclntstate *clnt = thd->sqlclntstate;
    int done = 0;
    int rc = 0;

    /* check authentication */
    if (authenticate_cursor(pCur, AUTHENTICATE_READ) != 0)
        return SQLITE_ACCESS;

    assert(pCur->fdbc != NULL);

    rc = cursor_move_preprop(pCur, pRes, how, &done, 1);
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
        if (rc == IX_FND && !is_sqlite_stat(pCur->fdbc->name(pCur))) {
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
        /* corner case, the db was backout to a lower protocol */
        /* TODO: */
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

    char *pFree = 0;
    UnpackedRecord *rec;

    rec = sqlite3VdbeAllocUnpackedRecord(pKeyInfo, NULL, 0, &pFree);
    if (rec == 0) {
        logmsg(LOGMSG_ERROR, "Error rec is zero, returned from "
                        "sqlite3VdbeAllocUnpackedRecord()\n");
        return 0;
    }
    sqlite3VdbeRecordUnpack(pKeyInfo, k2len, key2, rec);

    int cmp = sqlite3VdbeRecordCompare(k1len, key1, rec);
    if (pFree) {
        sqlite3DbFree(pKeyInfo->db, pFree);
    }
    return cmp;
}

/* Release pagelocks if the replicant is waiting on this sql thread */
static int cursor_move_postop(BtCursor *pCur)
{
    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = thd->sqlclntstate;
    extern int gbl_sql_release_locks_on_si_lockwait;
    extern int gbl_locks_check_waiters;
    int rc = 0;

    if (gbl_locks_check_waiters && gbl_sql_release_locks_on_si_lockwait &&
        (clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
         clnt->dbtran.mode == TRANLEVEL_SERIAL)) {
        extern int gbl_sql_random_release_interval;
        if (bdb_curtran_has_waiters(thedb->bdb_env, clnt->dbtran.cursor_tran)) {
            rc = release_locks("replication is waiting on si-session");
        } else if (gbl_sql_random_release_interval &&
                   !(rand() % gbl_sql_random_release_interval)) {
            rc = release_locks("random release cursor_move_postop");
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
    int rc;
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

extern char comdb2_maxkey[MAXKEYLEN];

void xdump(void *b, int len)
{
    unsigned char *c;
    int i;

    c = (unsigned char *)b;
    for (i = 0; i < len; i++)
        logmsg(LOGMSG_USER, "%02x", c[i]);
}

const char *sqlite3ErrStr(int);

static int blob_indices[MAXBLOBS] = {1, 2,  3,  4,  5,  6,  7, 8,
                                     9, 10, 11, 12, 13, 14, 15};

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

/* no-op calls about altering tables */
void sqlite3AlterFinishAddColumn(Parse *pParse, Token *pColDef)
{
    logmsg(LOGMSG_FATAL, "STUB sqlite3AlterFinishAddColumn called!\n");
    exit(1);
}

void sqlite3AlterBeginAddColumn(Parse *pParse, SrcList *pSrc)
{
    logmsg(LOGMSG_FATAL, "STUB sqlite3AlterBeginAddColumn called\n");
    exit(1);
}

void sqlite3AlterFunctions(void)
{
    logmsg(LOGMSG_FATAL, "STUB sqlite3AlterFunctions called\n");
    exit(1);
}

void sqlite3AlterRenameTable(Parse *pParse, SrcList *pSrc, Token *pName)
{
    logmsg(LOGMSG_FATAL, "STUB sqlite3AlterRenameTable called\n");
    exit(1);
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

static int free_hash_ent(void *obj, void *dum)
{
    free(obj);
    return 0;
}

/*
 ** Close an open database and invalidate all cursors.
 */
int sqlite3BtreeClose(Btree *pBt)
{
    int i;
    int rc = SQLITE_OK;
    int bdberr;
    BtCursor *pCur;
    struct sql_thread *thd = pthread_getspecific(query_info_key);

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
            /* shouldn't happen */
            printf("sqlite3BtreeClose:sqlite3BtreeCloseCursor rc %d\n", rc);
            /* Don't stop, or will leak cursors that will
             * lock pages forever... 20081002dh
             goto done;
             */
        }
    }

    if (pBt->is_temporary) {
        /* close all tables */
        for (i = 1; i < pBt->num_temp_tables; i++) {
            /* internally this will close cursors open on the table */
            if (pBt->temp_tables[i].owner == pBt) {
                rc = bdb_temp_table_close(thedb->bdb_env,
                                          pBt->temp_tables[i].tbl, &bdberr);
                free(pBt->temp_tables[i].name);
            } else {
                rc = 0;
            }
            if (rc) {
                logmsg(LOGMSG_ERROR, "sqlite3BtreeClose:bdb_temp_table_close bdberr %d\n",
                       bdberr);
                rc = SQLITE_INTERNAL;
                goto done;
            }
        }
        pBt->num_temp_tables = 0;
        free(pBt->temp_tables);
        if (thd)
            thd->bttmp = NULL;
    } else {
        if (thd)
            thd->bt = NULL;
    }
    if (pBt->free_schema && pBt->schema) {
        pBt->free_schema(pBt->schema);
        free(pBt->schema);
    }
#if 0
 is not part of Btree anymore */
   if (pBt->tran) {
      sqlite3BtreeRollback(pBt);
      pBt->tran = NULL;
   }
#endif
done:

    /*
     * if (thd->sqlclntstate->freemewithbt)
     * free(thd->sqlclntstate);
     */

    reqlog_logf(pBt->reqlogger, REQL_TRACE, "Close(pBt %d)      = %s\n",
                pBt->btreeid, sqlite3ErrStr(rc));
    if (pBt->zFilename) {
        free(pBt->zFilename);
        pBt->zFilename = NULL;
    }
    if (rc == 0)
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
                "SetSafetyLevel(pBt %d, level %d, int fullsync)     = %s\n",
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
    int eidx = 0;
    Btree *bt;
    int ixnum, tblnum;
    char *tmpname;
    int i;
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
        *ppBtree = bt;

        listc_init(&bt->cursors, offsetof(BtCursor, lnk));
    } else if (!zFilename || strcmp(zFilename, ":memory:") == 0) {
        /* temporary connection (for temp tables and such) */
        if (flags & BTREE_UNORDERED) {
            bt->is_hashtable = 1;
        }
        bt->reqlogger = thrman_get_reqlogger(thrman_self());
        bt->btreeid = id++;
        bt->is_temporary = 1;
        bt->num_temp_tables = 1; /* see above */
        *ppBtree = bt;
        thd->bttmp = bt;
        listc_init(&bt->cursors, offsetof(BtCursor, lnk));
    } else if (zFilename) {
        /* TODO: maybe we should enforce unicity ? when attaching same dbs from
         * multiple sql threads */

        /* remote database */

        bt->reqlogger = logger;
        bt->btreeid = id++;
        listc_init(&bt->cursors, offsetof(BtCursor, lnk));
        bt->is_remote = 1;
        /* NOTE: this is a lockless pointer; at the time of setting this, we got
        a lock
        in sqlite3AddAndLockTable, so it should be good. The sqlite engine will
        keep
        this structure around after fdb tables are changed. While fdb will NOT
        go away,
        its tables can dissapear or change schema.  Cached schema in Table
        object needs
        to be matched against fdb->tbl and make sure they are consistent before
        doing anything
        on the attached fdb
        */
        bt->fdb = get_fdb(zFilename);
        if (!bt->fdb) {
            logmsg(LOGMSG_ERROR, "%s: fdb not available for %s ?\n", __func__,
                    zFilename);
            free(bt);
            bt = NULL;
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

/* This really belongs in sqlsupport.c, but in needs access to
   internals of Btree, and I don't want to expose that plague-infested
   pile of excrement outside this module. */
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
    int fndlen;
    void *buf;

    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = thd->sqlclntstate;
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
            pCur->range->tbname = strdup(pCur->db->dbname);
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

/*forward*/

/*
 ** Delete the entry that the cursor is pointing to.  The cursor
 ** is left pointing at a random location.
 */
int sqlite3BtreeDelete(BtCursor *pCur, int usage)
{
    int rc = SQLITE_OK;
    int bdberr = 0;
    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = thd->sqlclntstate;

    /* only record for temp tables - writes for real tables are recorded in
     * block code */
    if (thd && pCur->db == NULL) {
        thd->nwrite++;
        thd->cost += pCur->write_cost;
        pCur->nwrite++;
    }

    if (clnt->is_readonly &&
        /* exclude writes in a temp table for a select */
        (pCur->cursor_class != CURSORCLASS_TEMPTABLE || clnt->iswrite)){
        errstat_set_strf(&clnt->osql.xerr, "SET READONLY ON for the client");
        rc = SQLITE_ACCESS;
        goto done;
    }

    /* if this is part of an analyze skip the delete - we'll do
     * the entire update part in one shot later when the analyze is done */
    if (clnt->is_analyze && is_sqlite_stat(pCur->db->dbname)) {
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

        if (queryOverlapsCursors(clnt, pCur) == 1) {
            rc = bdb_tran_deltbl_isdeleted_dedup(pCur->bdbcur, pCur->genid, 0,
                                                 &bdberr);
            if (rc == 1) {
#if 0
            fprintf(stderr, "%s: genid is already deleted, skipping %llx\n", 
                  __func__, pCur->genid);
#endif
                rc = SQLITE_OK;
                goto done;
            } else if (rc < 0) {
                logmsg(LOGMSG_ERROR, "%s:bdb_tran_deltbl_isdeleted error rc = %d bdberr=%d\n",
                       __func__, rc, bdberr);
                rc = SQLITE_INTERNAL;
                goto done;
            }
#if 0
         fprintf(stderr, "%s: deleting genid %llx\n", __func__, pCur->genid);
#endif
        }
        if (pCur->bt == NULL || pCur->bt->is_remote == 0) {
            if (is_sqlite_stat(pCur->db->dbname)) {
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
        } else {
            /* make sure we have a distributed transaction and use that to
             * update remote */
            uuid_t tid;
            fdb_tran_t *trans =
                fdb_trans_begin_or_join(clnt, pCur->bt->fdb, tid);

            if (!trans) {
                logmsg(LOGMSG_ERROR, 
                        "%s:%d failed to create or join distributed transaction!\n",
                       __func__, __LINE__);
                return SQLITE_INTERNAL;
            }
            if (gbl_expressions_indexes && pCur->ixnum != -1 &&
                pCur->fdbc->tbl_has_expridx(pCur)) {
                assert(clnt->idxDelete[pCur->ixnum] == NULL);
                clnt->idxDelete[pCur->ixnum] =
                    malloc(sizeof(int) + pCur->keybuflen);
                if (clnt->idxDelete[pCur->ixnum] == NULL) {
                    logmsg(LOGMSG_ERROR, "%s:%d malloc %d failed\n", __func__,
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
        }
        clnt->ins_keys = 0ULL;
        clnt->del_keys = 0ULL;
        if (gbl_expressions_indexes) {
            free_cached_idx(clnt->idxInsert);
            free_cached_idx(clnt->idxDelete);
        }
        if (rc == SQLITE_DDL_MISUSE)
            sqlite3VdbeError(pCur->vdbe,
                             "Transactional DDL Error: Overlapping Tables");
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
    int fndlen;
    void *buf;

    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = thd->sqlclntstate;
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
            pCur->range->tbname = strdup(pCur->db->dbname);
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
    int rc = UNIMPLEMENTED;
    *piMoved = 0;
    int bdberr;
    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                " sqlite3BtreeDropTable(pBt %d, root %d)       = %s\n",
                pBt->btreeid, iTable, sqlite3ErrStr(rc));

    struct temptable *tbl = &pBt->temp_tables[iTable];
    if (pBt->is_temporary) {
        if (tbl->owner == pBt) {
            // NEED TO LOCK HERE??
            rc = bdb_temp_table_close(thedb->bdb_env, tbl->tbl, &bdberr);
        } else {
            rc = SQLITE_OK;
        }
        bzero(tbl, sizeof(struct temptable));
    }
    return rc;
}

/*
 ** Step the cursor to the back to the previous entry in the database.  If
 ** successful then set *pRes=0.  If the cursor
 ** was already pointing to the first entry in the database before
 ** this routine was called, then set *pRes=1.
 */
int sqlite3BtreePrevious(BtCursor *pCur, int *pRes)
{
    int rc = SQLITE_OK;
    int fndlen;
    void *buf;
    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = pCur->clnt;

    if (pCur->empty) {
        *pRes = 1;
        return rc;
    }

    if (move_is_nop(pCur, pRes)) {
        return SQLITE_OK;
    }

    rc = pCur->cursor_move(pCur, pRes, CPREV);

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
        } else if (*pRes == 1 || rc != IX_OK) {
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
    int rc = (thd && thd->sqlclntstate) ? thd->sqlclntstate->intrans : 0;

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
 ** Return the flag byte at the beginning of the page that the cursor
 ** is currently pointing to.
 */
int sqlite3BtreeFlags(BtCursor *pCur)
{
    int rc;

    if (pCur->rootpage == RTPAGE_SQLITE_MASTER ||
        pCur->ixnum == -1) /* special table or data */
        rc = BTREE_INTKEY;
    else
        rc = BTREE_BLOBKEY;

    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE, "Flags(pCur %d)        = %d\n",
                pCur->cursorid,
                rc + 8); /* TODO: what's 8? (sqlite seems to add this
                          * can't find corresponding flag) */
    return rc + 8;
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
    void *blob;

    /* Pretty sure this never gets called !*/
    if (pCur->bt && !pCur->bt->is_temporary &&
        pCur->rootpage == RTPAGE_SQLITE_MASTER)
        abort();

    // rc = SQLITE_ERROR;
    // the following code is slated for removal
    if (pCur->is_sampled_idx) {
        /* this is in ondisk format- i want to convert */
        buf = bdb_temp_table_key(pCur->sampled_idx->cursor);
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
            *pAmt = get_sqlite_entry_size(pCur->tblpos);
            out = get_sqlite_entry(pCur->tblpos);
        }
    } else if (pCur->bt->is_remote) {

        assert(pCur->fdbc);

        *pAmt = pCur->fdbc->datalen(pCur);
        out = pCur->fdbc->data(pCur);

    } else {
        if (pCur->ixnum == -1) {
            out = pCur->dtabuf;
        } else {
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
                size = pCur->nDataDdl;
            }
            size = get_sqlite_entry_size(pCur->tblpos);
        }
    } else if (pCur->ixnum == -1) {
        if (pCur->bt->is_remote || pCur->db->dtastripe)
            memcpy(&size, &pCur->genid, sizeof(unsigned long long));
        else
            size = pCur->rrn;
    } else {
        size = pCur->keybuflen;
    }

done:
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
    int rc = SQLITE_OK;
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
                size = get_sqlite_entry_size(pCur->tblpos);
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

done:
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
    struct dbtable *db = thedb->dbs[0]; /* this is not used but required */
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

int initialize_shadow_trans(struct sqlclntstate *clnt, struct sql_thread *thd)
{
    int rc = SQLITE_OK;
    struct ireq iq;
    int bdberr;
    int error = 0;
    int snapshot_file = 0;
    int snapshot_offset = 0;

    if (!clnt->snapshot && clnt->sql_query && clnt->sql_query->snapshot_info) {
        snapshot_file = clnt->sql_query->snapshot_info->file;
        snapshot_offset = clnt->sql_query->snapshot_info->offset;
    }

    init_fake_ireq(thedb, &iq);
    iq.usedb = thedb->dbs[0]; /* this is not used but required */

    switch (clnt->dbtran.mode) {
    default:
        logmsg(LOGMSG_ERROR, "%s: unknown mode %d\n", __func__, clnt->dbtran.mode);
        rc = SQLITE_INTERNAL;
        goto done;

    case TRANLEVEL_SNAPISOL:
        clnt->dbtran.shadow_tran =
            trans_start_snapisol(&iq, clnt->bdb_osql_trak, clnt->snapshot,
                                 snapshot_file, snapshot_offset, &error);

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
            goto done;
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
        clnt->dbtran.shadow_tran =
            trans_start_serializable(&iq, clnt->bdb_osql_trak, clnt->snapshot,
                                     snapshot_file, snapshot_offset, &error);

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
            goto done;
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
            rc = SQLITE_INTERNAL;
            goto done;
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
            rc = SQLITE_INTERNAL;
            goto done;
        }

        rc = osql_sock_start(clnt, OSQL_SOCK_REQ, 0);
        if (clnt->client_understands_query_stats)
            osql_query_dbglog(thd, clnt->queryid);
        break;

    }

done:
    return rc;
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

int sqlite3BtreeBeginTrans(Vdbe *vdbe, Btree *pBt, int wrflag)
{
    int rc = SQLITE_OK;
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->sqlclntstate;

#ifdef DEBUG
    if (gbl_debug_sql_opcodes) {
        logmsg(LOGMSG_ERROR, "%s %d %d\n", __func__, clnt->intrans,
               clnt->ctrl_sqlengine);
    }
#endif

    /* already have a transaction, keep using it until it commits/aborts */
    if (clnt->intrans || clnt->no_transaction ||
        (clnt->ctrl_sqlengine != SQLENG_STRT_STATE &&
         clnt->ctrl_sqlengine != SQLENG_NORMAL_PROCESS)) {
        rc = SQLITE_OK;
        goto done;
    }

    if (wrflag && clnt->origin) {
        if (gbl_check_sql_source && !allow_write_from_remote(clnt->origin)) {
            sqlite3VdbeError(vdbe, "write from node %d not allowed",
                             clnt->conninfo.node);
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

    clnt->ins_keys = 0ULL;
    clnt->del_keys = 0ULL;

    if (gbl_expressions_indexes) {
        clnt->idxInsert = calloc(MAXINDEX, sizeof(uint8_t *));
        clnt->idxDelete = calloc(MAXINDEX, sizeof(uint8_t *));
        if (!clnt->idxInsert || !clnt->idxDelete) {
            logmsg(LOGMSG_ERROR, "%s:%d malloc failed\n", __func__, __LINE__);
            rc = SQLITE_NOMEM;
            goto done;
        }
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

    clnt->ddl_tables = hash_init_str(0);
    clnt->dml_tables = hash_init_str(0);

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
    if (wrflag) {
        // cache here the nature of the query; only works because each sql
        // is a standalone sqlite transaction
        clnt->iswrite = wrflag;
    }
    if (clnt->ctrl_sqlengine == SQLENG_STRT_STATE)
        sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_INTRANS_STATE);

    clnt->intrans = 1;
    bzero(clnt->dirty, sizeof(clnt->dirty));

#ifdef DEBUG
    if (gbl_debug_sql_opcodes) {
        logmsg(LOGMSG_ERROR, "%p starts transaction tid=%d mode=%d intrans=%d\n",
                clnt, pthread_self(), clnt->dbtran.mode, clnt->intrans);
    }
#endif
    if (rc = initialize_shadow_trans(clnt, thd))
        goto done;

    uuidstr_t us;
    char rqidinfo[40];
    snprintf(rqidinfo, sizeof(rqidinfo), "rqid=%016llx %s appsock %u",
             clnt->osql.rqid, comdb2uuidstr(clnt->osql.uuid, us),
             clnt->appsock_id);
    thrman_setid(thrman_self(), rqidinfo);

done:
    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                "BeginTrans(pBt %d, wrflag %d)      = %s (rc=%d)\n",
                pBt->btreeid, wrflag, sqlite3ErrStr(rc), rc);
    return rc;
}

/*
 ** Commit the transaction currently in progress.
 **
 ** This will release the write lock on the database file.  If there
 ** are no active cursors, it also releases the read lock.
 */
int sqlite3BtreeCommit(Btree *pBt)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->sqlclntstate;
    int rc = SQLITE_OK;
    int irc = 0;
    int bdberr = 0;

#ifdef DEBUG
    if (gbl_debug_sql_opcodes) {
        uuidstr_t us;
        fprintf(
            stderr,
            "sqlite3BtreeCommit intrans=%d sqleng=%d openeng=%d rqid=%llx %s\n",
            clnt->intrans, clnt->ctrl_sqlengine, clnt->no_transaction,
            clnt->osql.rqid, comdb2uuidstr(clnt->osql.uuid, us));
    }
#endif

    if (clnt->arr)
        currangearr_coalesce(clnt->arr);
    if (clnt->selectv_arr)
        currangearr_coalesce(clnt->selectv_arr);

    if (!clnt->intrans || clnt->no_transaction ||
        (!clnt->no_transaction && clnt->ctrl_sqlengine != SQLENG_FNSH_STATE &&
         clnt->ctrl_sqlengine != SQLENG_NORMAL_PROCESS)) {
        rc = SQLITE_OK;
        goto done;
    }

    clnt->recno = 0;

    /* reset the state of the sqlengine */
    if (clnt->ctrl_sqlengine == SQLENG_FNSH_STATE)
        sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                SQLENG_NORMAL_PROCESS);

    clnt->intrans = 0;

#ifdef DEBUG
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
                clnt->dbtran.shadow_tran = NULL;
            } else {
                irc = trans_abort_shadow((void **)&clnt->dbtran.shadow_tran,
                                         &bdberr);
            }
            if (irc) {
                logmsg(LOGMSG_ERROR, "%s: commit failed rc=%d bdberr=%d\n", __func__,
                        irc, bdberr);
            }
        }
        break;

    case TRANLEVEL_SNAPISOL:
        if (clnt->dbtran.shadow_tran) {
            rc = snapisol_commit(clnt, thd, clnt->tzname);

            rc = trans_commit_shadow(clnt->dbtran.shadow_tran, &bdberr);
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

            rc = trans_commit_shadow(clnt->dbtran.shadow_tran, &bdberr);
            clnt->dbtran.shadow_tran = NULL;
        }
        break;

    case TRANLEVEL_SOSQL:
        if (gbl_selectv_rangechk)
            rc = selectv_range_commit(clnt);
        if (rc || clnt->early_retry) {
            int irc = 0;
            irc = osql_sock_abort(clnt, OSQL_SOCK_REQ);
            if (irc) {
                logmsg(LOGMSG_ERROR, 
                        "%s: failed to abort sorese transactin irc=%d\n",
                       __func__, irc);
            }
            if (clnt->early_retry) {
                clnt->osql.xerr.errval = ERR_BLOCK_FAILED + ERR_VERIFY;
                clnt->early_retry = 0;
                rc = SQLITE_ABORT;
            }
        } else {
            rc = osql_sock_commit(clnt, OSQL_SOCK_REQ);
            osqlstate_t *osql = &thd->sqlclntstate->osql;
            if (osql->xerr.errval == COMDB2_SCHEMACHANGE_OK) {
                osql->xerr.errval = 0;
            }
        }
        break;

    }

    clnt->ins_keys = 0ULL;
    clnt->del_keys = 0ULL;

    if (gbl_expressions_indexes) {
        free(clnt->idxInsert);
        free(clnt->idxDelete);
        clnt->idxInsert = clnt->idxDelete = NULL;
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

    if (clnt->ddl_tables) {
        hash_free(clnt->ddl_tables);
    }
    if (clnt->dml_tables) {
        hash_free(clnt->dml_tables);
    }
    clnt->ddl_tables = NULL;
    clnt->dml_tables = NULL;

done:
    reqlog_logf(pBt->reqlogger, REQL_TRACE, "Commit(pBt %d)      = %s\n",
                pBt->btreeid, sqlite3ErrStr(rc));
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
    struct sqlclntstate *clnt = thd->sqlclntstate;
    int rc = SQLITE_OK;
    int irc = 0;
    int bdberr = 0;

    /*
     * fprintf(stderr, "sqlite3BtreeRollback %d %d\n", clnt->intrans,
     * clnt->ctrl_sqlengine );
     */

    if (!clnt || !clnt->intrans || clnt->no_transaction ||
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
    bzero(clnt->dirty, sizeof(clnt->dirty));
#if 0
   fprintf(stderr, "%p rollbacks transaction %d %d\n", clnt, pthread_self(),
         clnt->dbtran.mode);
#endif

    switch (clnt->dbtran.mode) {
    default:
        logmsg(LOGMSG_ERROR, "%s: unknown mode %d\n", __func__, clnt->dbtran.mode);
        rc = SQLITE_INTERNAL;
        goto done;

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

    clnt->ins_keys = 0ULL;
    clnt->del_keys = 0ULL;

    if (gbl_expressions_indexes) {
        free(clnt->idxInsert);
        free(clnt->idxDelete);
        clnt->idxInsert = clnt->idxDelete = NULL;
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

    if (clnt->ddl_tables) {
        hash_free(clnt->ddl_tables);
    }
    if (clnt->dml_tables) {
        hash_free(clnt->dml_tables);
    }
    clnt->ddl_tables = NULL;
    clnt->dml_tables = NULL;

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
    int rc = (thd && thd->sqlclntstate) ? thd->sqlclntstate->intrans : 0;

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

static char *get_temp_dbname(Btree *pBt)
{
    char *s;
    unsigned long long genid;
    size_t s_sz = strlen(thedb->basedir) + 80;
    genid = get_id(thedb->bdb_env);
    s = malloc(s_sz);
    if (!s) {
        logmsg(LOGMSG_ERROR, "get_temp_dbname: out of memory\n");
        return NULL;
    }
    snprintf(s, s_sz, "%s/%s.tmpdbs/_temp_%lld.db", thedb->basedir,
             thedb->envname, genid);
    return s;
}

/*
** Don't create new btrees, use this one.
** Lua threads share temp tables.
** Temp tables were not designed to be shareable.
*/
static __thread struct temptable *tmptbl_kludge = NULL;

/*
** Use this lock for synchoronizing access to shared
** temp table.
*/
static __thread pthread_mutex_t *tmptbl_lk = NULL;
void comdb2_set_tmptbl_lk(pthread_mutex_t *lk) { tmptbl_lk = lk; }

/*
** Don't lock around access to sqlite_temp_master.
** Those are temp tables, but each thread makes its
** own copy and don't need to synchronize access to it.
*/
static __thread int tmptbl_use_lk = 0;
void comdb2_use_tmptbl_lk(int use)
{
    tmptbl_use_lk = use; // don't use lk for sqlite_temp_master
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
    int rc = SQLITE_OK;
    int bdberr;
    void *newp;
    struct sql_thread *thd;

    if ((thd = pthread_getspecific(query_info_key)) == NULL) {
        return SQLITE_INTERNAL;
    }
    thd->had_temptables = 1;
    if (!pBt->is_temporary) { /* must go through comdb2 to do this */
        rc = UNIMPLEMENTED;
        logmsg(LOGMSG_ERROR, "%s rc: %d\n", __func__, rc);
        goto done;
    }

    if (!thd->sqlclntstate->limits.temptables_ok)
        return SQLITE_LIMIT;

    /* creating a temporary table */
    int num_temp_tables = pBt->num_temp_tables;
    newp = realloc(pBt->temp_tables,
                   (num_temp_tables + 1) * sizeof(struct temptable));
    if (unlikely(newp == NULL)) {
        logmsg(LOGMSG_ERROR, "%s: realloc(%u) failed\n", __func__,
                (num_temp_tables + 1) * sizeof(struct temptable));
        rc = SQLITE_INTERNAL;
        goto done;
    }
    pBt->temp_tables = newp;
    if (num_temp_tables == 1) { // first one
        bzero(&pBt->temp_tables[0], sizeof(struct temptable));
    }
    bzero(&pBt->temp_tables[num_temp_tables], sizeof(struct temptable));
    if (pBt->is_hashtable) {
        pBt->temp_tables[num_temp_tables].tbl =
            bdb_temp_hashtable_create(thedb->bdb_env, &bdberr);
        pBt->temp_tables[num_temp_tables].owner = pBt;
        pBt->temp_tables[num_temp_tables].name = get_temp_dbname(pBt);
        pBt->temp_tables[num_temp_tables].lk = NULL;
    } else if (tmptbl_use_lk && tmptbl_kludge) { // clone
        pBt->temp_tables[num_temp_tables].tbl = tmptbl_kludge->tbl;
        pBt->temp_tables[num_temp_tables].owner = NULL;
        pBt->temp_tables[num_temp_tables].name = tmptbl_kludge->name;
        pBt->temp_tables[num_temp_tables].lk = tmptbl_lk;
    } else {
        pBt->temp_tables[num_temp_tables].tbl =
            bdb_temp_table_create(thedb->bdb_env, &bdberr);
        pBt->temp_tables[num_temp_tables].owner = pBt;
        pBt->temp_tables[num_temp_tables].name = get_temp_dbname(pBt);
        pBt->temp_tables[num_temp_tables].lk = tmptbl_use_lk ? tmptbl_lk : NULL;
    }
    if (pBt->temp_tables[num_temp_tables].tbl == NULL) {
        rc = SQLITE_INTERNAL;
        goto done;
    }
    pBt->temp_tables[num_temp_tables].flags = flags;
    *piTable = num_temp_tables;
    ++pBt->num_temp_tables;
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
    int rc = SQLITE_OK;

    if (idx < 0 || idx > 15) {
        logmsg(LOGMSG_ERROR, "sqlite3BtreeGetMeta: unknown index idx = %d\n", idx);
        rc = SQLITE_INTERNAL;
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
            case OP_Found:
            case OP_NotFound:
                pCur->next_is_eof = 1;
                break;
            case OP_SeekGT:
            case OP_SeekGE:
            case OP_Seek:
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
        /* corner case, the db was backout to a lower protocol */
        /* TODO: */
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
    int flags;
    struct ireq iq;
    int fndlen;
    int bdberr;
    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = pCur->clnt;
    int verify = 0;

    if (debug_switch_pause_moveto()) {
        logmsg(LOGMSG_USER, "Waiting 15 sec\n");
        poll(NULL, 0, 15000);
    }

    /* verification error if not found */
    extern int gbl_early_verify;
    if (gbl_early_verify && (bias == OP_NotExists || bias == OP_NotFound) &&
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
    if (likely(bdb_attr_get(thedb->bdb_attr, BDB_ATTR_ONE_PASS_DELETE)) &&
        (pCur->ixnum == -1) && (pCur->open_flags & OPFLAG_FORDELETE) &&
        !gbl_partial_indexes) {
        rc = SQLITE_OK;
        goto done;
    }

    if (thd)
        thd->nfind++;

    thd->cost += pCur->find_cost;

    /* assert that this isn't called for sampled (previously misnamed
     * compressed) */
    assert(0 == pCur->is_sampled_idx);

    rc = sql_tick(thd, pCur->bt->is_temporary == 0);

    if (rc)
        return rc;

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
                Mem mem = {0};
                sqlite3VdbeRecordPack(pIdxKey, &mem);
                rc = bdb_temp_table_find(thedb->bdb_env, pCur->tmptable->cursor,
                                         mem.z, mem.n, NULL, &bdberr);
                sqlite3VdbeMemRelease(&mem);
            } else {
                rc = pCur->cursor_find(thedb->bdb_env, pCur->tmptable->cursor,
                                       NULL, 0, pIdxKey, &bdberr, pCur);
            }
        } else {
            void *k = malloc(sizeof(intKey));
            if (k == NULL) {
                return SQLITE_NOMEM;
            }
            *(i64 *)k = intKey;
            rc = pCur->cursor_find(thedb->bdb_env, pCur->tmptable->cursor, k,
                                   sizeof(intKey), NULL, &bdberr, pCur);
            if (rc != IX_FND) {
                free(k);
            }
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

    } else if (pCur->cursor_class == CURSORCLASS_REMOTE) { /* remote cursor */

        /* filter the supported operations */
        if (bias != OP_SeekLT && bias != OP_SeekLE && bias != OP_SeekGE &&
            bias != OP_SeekGT && bias != OP_NotExists && bias != OP_Found &&
            bias != OP_NotFound && bias != OP_IdxDelete) {
            logmsg(LOGMSG_ERROR, "%s: unsupported remote cursor operation op=%d\n",
                    __func__, bias);
            rc = SQLITE_INTERNAL;
        }
        /* hack for partial indexes */
        else if (bias == OP_IdxDelete && pCur->ixnum != -1) {
            /* hack for partial indexes and indexes on expressions */
            if (gbl_expressions_indexes && pCur->fdbc->tbl_has_expridx(pCur)) {
                Mem mem = {0};
                sqlite3VdbeRecordPack(pIdxKey, &mem);
                assert(clnt->idxDelete[pCur->ixnum] == NULL);
                clnt->idxDelete[pCur->ixnum] = malloc(sizeof(int) + mem.n);
                if (clnt->idxDelete[pCur->ixnum] == NULL) {
                    logmsg(LOGMSG_ERROR, "%s:%d malloc %d failed\n", __func__,
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
        unsigned long long genid;
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
            printf("# find %s rrn %lld ", pCur->db->dbname, nKey);
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
                    thd->sqlclntstate->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
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
        struct convert_failure *fail_reason = &thd->sqlclntstate->fail_reason;

        struct bias_info info = {.bias = bias,
                                 .truncated = 0,
                                 .cmp = bias_cmp,
                                 .cur = pCur,
                                 .unpacked = pIdxKey};
        ondisk_len = rc =
            sqlite_unpacked_to_ondisk(pCur, pIdxKey, fail_reason, &info);
        if (rc < 0) {
            char errs[128];
            convert_failure_reason_str(&thd->sqlclntstate->fail_reason,
                                       pCur->db->dbname, "SQLite format",
                                       ".ONDISK", errs, sizeof(errs));
            reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                        "Moveto: sqlite_unpacked_to_ondisk failed [%s]\n",
                        errs);
            sqlite3VdbeError(pCur->vdbe, errs, (char *)0);
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
                    thd->sqlclntstate->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
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
                        (thd->sqlclntstate && thd->sqlclntstate->osql.rqid)
                            ? thd->sqlclntstate->osql.rqid
                            : 0,
                        (thd->sqlclntstate && thd->sqlclntstate->osql.rqid)
                            ? comdb2uuidstr(thd->sqlclntstate->osql.uuid, us)
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
        pCur->vdbe->readOnly == 0 && pCur->ixnum == -1)
        clnt->early_retry = 1;

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
    int rc;
    int reqsize;
    void *out = NULL;
    void *tmp;
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

/* add the costs of the sorter to the thd costs */
void addVbdeToThdCost(int type)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (thd == NULL)
        return;

    double cost = 0;
    if (type == VDBESORTER_WRITE)
        thd->cost += 0.2;
    else if (type == VDBESORTER_MOVE || type == VDBESORTER_FIND)
        thd->cost += 0.1;
}

/* append the costs of the sorter to the thd query stats */
void addVbdeSorterCost(const VdbeSorter *pSorter)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (thd == NULL)
        return;

    struct query_path_component fnd, *qc;

    fnd.u.db = 0;
    fnd.ix = 0;

    if (NULL == (qc = hash_find(thd->query_hash, &fnd))) {
        qc = calloc(sizeof(struct query_path_component), 1);
        qc->u.db = 0;
        qc->ix = 0;
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
    int bdberr;
    /* Not sure about this. Can pCur->thd be different from thread-specific */
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    /* If we are using sqldb of other thread, then the other thread will close
     * the db. */
    if (thd == NULL)
        return 0;
    struct sqlclntstate *clnt = thd->sqlclntstate;
    CurRangeArr **append_to;

#if 0
   FOR NOW, THIS IS CLEARED AT SQLCLNTSTATE LEVEL 
   pCur->keyDdl = 0ULL;
   if (pCur->dataDdl)
   {
      free(pCur->dataDdl);
      pCur->dataDdl = NULL;
      pCur->nDataDdl = 0;
   }
#endif

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

    if (thd && pCur->cursor_class != CURSORCLASS_SQLITEMASTER) {
        struct query_path_component fnd, *qc = NULL;

        if (pCur->bt && pCur->bt->is_remote) {
            if (!pCur->fdbc)
                goto skip; /* failed during cursor creation */
            fnd.u.fdb = pCur->fdbc->table_entry(pCur);
        } else {
            fnd.u.db = pCur->db;
        }
        fnd.ix = pCur->ixnum;

        if (thd->query_hash &&
            NULL == (qc = hash_find(thd->query_hash, &fnd))) {
            qc = calloc(sizeof(struct query_path_component), 1);
            if (pCur->bt && pCur->bt->is_remote) {
                qc->remote = 1;
                qc->u.fdb = fnd.u.fdb;
            } else {
                qc->u.db = pCur->db;
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
                if (pCur->ondisk_buf)
                    free(pCur->ondisk_buf);
            }
            if (pCur->ondisk_key)
                free(pCur->ondisk_key);
            if (pCur->writeTransaction) {
                if (pCur->fndkey)
                    free(pCur->fndkey);
            }
        }
        if (pCur->writeTransaction) {
            if (pCur->dtabuf)
                free(pCur->dtabuf);
        }
        if (pCur->keybuf)
            free(pCur->keybuf);

        if (pCur->is_sampled_idx) {
            rc = bdb_temp_table_close_cursor(
                thedb->bdb_env, pCur->sampled_idx->cursor, &bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "bdb_temp_table_close_cursor rc %d\n", bdberr);
                rc = SQLITE_INTERNAL;
                goto done;
            }
            free(pCur->sampled_idx->name);
            free(pCur->sampled_idx);
        } else if (pCur->bt && pCur->bt->is_temporary) {
            rc = pCur->cursor_close(thedb->bdb_env, pCur, &bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "bdb_temp_table_close_cursor rc %d\n", bdberr);
                rc = SQLITE_INTERNAL;
                goto done;
            }
            free(pCur->tmptable->name);
            free(pCur->tmptable);
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
        pthread_mutex_lock(&thd->lk);
        if (pCur->on_list)
            listc_rfl(&pCur->bt->cursors, pCur);
        pthread_mutex_unlock(&thd->lk);
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
    int tblnum, ixnum;
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->sqlclntstate;

    if (pnChange)
        *pnChange = 0;

    if (pBt->is_temporary) {
        rc = bdb_temp_table_truncate(thedb->bdb_env,
                                     pBt->temp_tables[iTable].tbl, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, 
                    "sqlite3BtreeClearTable: bdb_temp_table_clear error rc = %d\n",
                    rc);
            rc = SQLITE_INTERNAL;
            goto done;
        }
    } else {
        get_sqlite_tblnum_and_ixnum(thd, iTable, &tblnum, &ixnum);
        if (ixnum != -1) {
            rc = SQLITE_OK;
            goto done;
        }
        struct dbtable *db = thedb->dbs[tblnum];
        /* If we are in analyze, lie.  Otherwise we end up with an empty, and
         * then worse,
         * half-filled stat table during the analyze. */
        if (clnt->is_analyze && is_sqlite_stat(db->dbname)) {
            rc = SQLITE_OK;
            goto done;
        }

        if (clnt->dbtran.mode == TRANLEVEL_SOSQL ||
            clnt->dbtran.mode == TRANLEVEL_RECOM ||
            clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
            clnt->dbtran.mode == TRANLEVEL_SERIAL) {
            if (db->n_constraints == 0)
                rc = osql_cleartable(thd, db->dbname);
            else
                rc = -1;
        } else {
            rc = reinit_db(db);
        }
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
int fetch_blob_into_sqlite_mem(BtCursor *pCur, struct schema *sc, int fnum,
                               Mem *m)
{
    struct ireq iq;
    blob_status_t blobs;
    int blobnum;
    struct field *f;
    void *dta;
    int rc;
    int bdberr;
    int nretries = 0;
    struct sql_thread *thd = pCur->thd;

    if (!pCur->have_blob_descriptor) {
        gather_blob_data_byname(pCur->db->dbname, ".ONDISK",
                                &pCur->blob_descriptor);
        pCur->have_blob_descriptor = 1;
    }

    f = &sc->member[fnum];
    blobnum = f->blob_index + 1;

    pCur->nblobs++;
    if (thd) {
        thd->nblobs++;
        thd->cost += pCur->blob_cost;
    }

again:
    memcpy(&blobs, &pCur->blob_descriptor, sizeof(blobs));

    if (is_genid_synthetic(pCur->genid)) {
        rc = osql_fetch_shadblobs_by_genid(pCur, &blobnum, &blobs, &bdberr);
    } else {
        bdb_fetch_args_t args = {0};
        rc = bdb_fetch_blobs_by_rrn_and_genid_cursor(
            pCur->db->handle, pCur->rrn, pCur->genid, 1, &blobnum,
            blobs.bloblens, blobs.bloboffs, (void **)blobs.blobptrs,
            pCur->bdbcur, &args, &bdberr);
    }

    if (rc) {
        if (bdberr == BDBERR_DEADLOCK) {
            nretries++;
            if (rc = recover_deadlock(thedb->bdb_env, thd, NULL, 0)) {
                if (!gbl_rowlocks)
                    logmsg(LOGMSG_ERROR, "%s: %d failed dd recovery\n", __func__,
                            pthread_self());

                return (rc == SQLITE_CLIENT_CHANGENODE) ? rc : SQLITE_DEADLOCK;
            }
            if (nretries >= gbl_maxretries) {
                logmsg(LOGMSG_ERROR, "too much contention fetching "
                       "tbl %s blob %s tried %d times\n",
                       pCur->db->dbname, f->name, nretries);
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

    if (pCur->dtabuf) {
        dta = pCur->dtabuf;
    } else {
        dta = pCur->bdbcur->datacopy(pCur->bdbcur);
    }

    assert(dta);

    if (check_one_blob_consistency(&iq, iq.usedb->dbname, ".ONDISK", &blobs,
                                   dta, f->blob_index, 0)) {
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
        m->z = blobs.blobptrs[0];
        m->n = blobs.bloblens[0];
        m->flags = MEM_Dyn;
        m->xDel = free;

        if (f->type == SERVER_VUTF8) {
            m->flags |= MEM_Str;
            if (m->n > 0)
                --m->n; /* sqlite string lengths do not include NULL */
        } else
            m->flags |= MEM_Blob;
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

int is_remote(BtCursor *pCur)
{
    return pCur->cursor_class == CURSORCLASS_REMOTE;
}

int is_raw(BtCursor *pCur)
{
    if (pCur) {
        if (pCur->cursor_class == CURSORCLASS_TABLE) {
            return 1;
        } else if (pCur->cursor_class == CURSORCLASS_INDEX) {
            return 1;
        } else if (pCur->cursor_class == CURSORCLASS_REMOTE) {
            return 1;
        } else if (pCur->is_sampled_idx) {
            return 1;
        }
    }
    return 0;
}

static int get_data_int(BtCursor *pCur, struct schema *sc, uint8_t *in,
                        int fnum, Mem *m, uint8_t flip_orig, const char *tzname)
{
    int null;
    i64 ival;
    double dval;
    int outdtsz = 0;
    int rc = 0;
    struct field *f = &(sc->member[fnum]);
    uint8_t *in_orig = in = in + f->offset;

    if (f->flags & INDEX_DESCEND) {
        if (gbl_sort_nulls_correctly) {
            in_orig[0] = ~in_orig[0];
        }
        if (flip_orig) {
            xorbufcpy(&in[1], &in[1], f->len - 1);
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
                xorbufcpy(&p[1], &in[1], f->len - 1);
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

    case SERVER_BREAL:
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

    case SERVER_BCSTR:
        /* point directly at the ondisk string */
        m->z = &in[1]; /* skip header byte in front */
        if (flip_orig || !(f->flags & INDEX_DESCEND)) {
            m->n = cstrlenlim(&in[1], f->len - 1);
        } else {
            m->n = cstrlenlimflipped(&in[1], f->len - 1);
        }
        m->flags = MEM_Str | MEM_Ephem;
        break;

    case SERVER_BYTEARRAY:
        /* just point to bytearray directly */
        m->z = &in[1];
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
            m->z = &in[1];
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
            m->z = &in[1];
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
            m->z = &in[5];

            /* m->n is the blob length */
            m->n = (len > 0) ? len : 0;

            /*fprintf(stderr, "m->n = %d\n", m->n); */
            m->flags = MEM_Blob;
        } else
            rc = fetch_blob_into_sqlite_mem(pCur, sc, fnum, m);

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
            m->z = &in[5];

            /* sqlite string lengths do not include NULL */
            m->n = (len > 0) ? len - 1 : 0;

            /*fprintf(stderr, "m->n = %d\n", m->n); */
            m->flags = MEM_Str | MEM_Ephem;
        } else
            rc = fetch_blob_into_sqlite_mem(pCur, sc, fnum, m);
        break;
    }

    case SERVER_BLOB: {
        int len;

        memcpy(&len, &in[1], 4);
        len = ntohl(len);
        if (len == 0) { /* this blob is zerolen, we should'nt need to fetch*/
            m->z = NULL;
            m->flags = MEM_Blob;
            m->n = 0;
        } else
            rc = fetch_blob_into_sqlite_mem(pCur, sc, fnum, m);
        break;
    }
    case SERVER_DECIMAL:
        m->flags = MEM_Interval;
        m->du.tv.type = INTV_DECIMAL_TYPE;
        m->du.tv.sign = 0;

        /* if this is an index, try to extract the quantum */
        if (pCur->ixnum >= 0 && pCur->db->ix_collattr[pCur->ixnum] > 0) {
            char quantum;
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
                hexdump(new_in, f->len);
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
                hexdump(new_in, f->len);
                logmsg(LOGMSG_USER, "\n");
            }

            in = new_in;
        } else if (pCur->ixnum >= 0 && pCur->db->ix_datacopy[pCur->ixnum]) {
            struct field *fidx = &(pCur->db->schema->member[f->idx]);
            assert(f->len == fidx->len);
            in = pCur->bdbcur->datacopy(pCur->bdbcur) + fidx->offset;
        }

        decimal_ondisk_to_sqlite(in, f->len, (decQuad *)&m->du.tv.u.dec, &null);

        break;
    default:
        logmsg(LOGMSG_ERROR, "unhandled type %d\n", f->type);
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

int get_data(BtCursor *pCur, void *invoid, int fnum, Mem *m)
{
    if (unlikely(pCur->cursor_class == CURSORCLASS_REMOTE)) {
        /* convert the remote buffer to M array */
        abort(); /* this is suppsed to be a cooked access */
    } else {
        return get_data_int(pCur, pCur->sc, invoid, fnum, m, 0,
                            pCur->clnt->tzname);
    }
}

int get_datacopy(BtCursor *pCur, int fnum, Mem *m)
{
    uint8_t *in;

    in = pCur->bdbcur->datacopy(pCur->bdbcur);
    if (!is_genid_synthetic(pCur->genid)) {
        uint8_t ver = pCur->bdbcur->ver(pCur->bdbcur);
        vtag_to_ondisk_vermap(pCur->db, in, NULL, ver);
    }

    return get_data_int(pCur, pCur->db->schema, in, fnum, m, 0,
                        pCur->clnt->tzname);
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
    int bdberr = 0;
    int tblnum;
    int ixnum;
    int key_size;
    int sz;
    struct sqlclntstate *clnt = thd->sqlclntstate;
    struct dbtable *db;

    assert(iTable >= RTPAGE_START);
    assert(iTable < (thd->rootpage_nentries + RTPAGE_START));

    get_sqlite_tblnum_and_ixnum(thd, iTable, &tblnum, &ixnum);

    assert(tblnum < thedb->num_dbs);

    cur->ixnum = ixnum;
    if (tblnum < thedb->num_dbs)
        db = thedb->dbs[tblnum];

    cur->cursor_class = CURSORCLASS_TEMPTABLE;
    cur->cursor_move = cursor_move_compressed;

    cur->sampled_idx = calloc(1, sizeof(struct temptable));
    if (!cur->sampled_idx) {
        logmsg(LOGMSG_ERROR, "%s: calloc sizeof(struct temp_table) failed\n",
                __func__);
        return SQLITE_INTERNAL;
    }

    cur->sampled_idx->tbl =
        analyze_get_sampled_temptable(clnt, db->dbname, ixnum);
    assert(cur->sampled_idx->tbl != NULL);

    cur->sampled_idx->cursor = bdb_temp_table_cursor(
        thedb->bdb_env, cur->sampled_idx->tbl, pArg, &bdberr);
    if (cur->sampled_idx->cursor == NULL) {
        logmsg(LOGMSG_ERROR, "%s:bdb_temp_table_cursor failed\n", __func__);
        return SQLITE_INTERNAL;
    }

    bdb_temp_table_set_cmp_func(cur->sampled_idx->tbl, (tmptbl_cmp)xCmp);

    cur->db = db;
    cur->sc = cur->db->ixschema[ixnum];
    cur->rootpage = iTable;
    cur->bt = pBt;
    cur->ixnum = ixnum;
    cur->is_sampled_idx = 1;
    cur->nCookFields = -1;

    key_size = getkeysize(cur->db, cur->ixnum);
    cur->ondisk_key = malloc(key_size + sizeof(int));
    if (!cur->ondisk_key) {
        logmsg(LOGMSG_ERROR, "%s:malloc ondisk_key sz %d failed\n", __func__,
                key_size + sizeof(int));
        free(cur->sampled_idx);
        return SQLITE_INTERNAL;
    }
    cur->ondisk_keybuf_alloc = key_size;
    sz = schema_var_size(cur->sc);
    cur->keybuf = malloc(sz);
    if (!cur->keybuf) {
        logmsg(LOGMSG_ERROR, "%s: keybuf malloc %d failed\n", __func__, sz);
        free(cur->sampled_idx);
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
    pthread_mutex_lock(btcursor->tmptable->lk);
    int rc = tmptbl_cursor_move(btcursor, pRes, how);
    pthread_mutex_unlock(btcursor->tmptable->lk);
    return rc;
}

static int lk_tmptbl_cursor_del(bdb_state_type *bdb_state,
                                struct temp_cursor *cur, int *bdberr,
                                BtCursor *btcursor)
{
    pthread_mutex_lock(btcursor->tmptable->lk);
    int rc = tmptbl_cursor_del(bdb_state, cur, bdberr, btcursor);
    pthread_mutex_unlock(btcursor->tmptable->lk);
    return rc;
}

static int lk_tmptbl_cursor_put(bdb_state_type *bdb_state,
                                struct temp_table *tbl, void *key, int keylen,
                                void *data, int dtalen, void *unpacked,
                                int *bdberr, BtCursor *btcursor)
{
    pthread_mutex_lock(btcursor->tmptable->lk);
    int rc = tmptbl_cursor_put(bdb_state, tbl, key, keylen, data, dtalen,
                               unpacked, bdberr, btcursor);
    pthread_mutex_unlock(btcursor->tmptable->lk);
    return rc;
}

static int lk_tmptbl_cursor_close(bdb_state_type *bdb_state, BtCursor *btcursor,
                                  int *bdberr)
{
    pthread_mutex_lock(btcursor->tmptable->lk);
    int rc = tmptbl_cursor_close(bdb_state, btcursor, bdberr);
    pthread_mutex_unlock(btcursor->tmptable->lk);
    return rc;
}

static int lk_tmptbl_cursor_find(bdb_state_type *bdb_state,
                                 struct temp_cursor *cur, const void *key,
                                 int keylen, void *unpacked, int *bdberr,
                                 BtCursor *btcursor)
{
    pthread_mutex_lock(btcursor->tmptable->lk);
    int rc = tmptbl_cursor_find(bdb_state, cur, key, keylen, unpacked, bdberr,
                                btcursor);
    pthread_mutex_unlock(btcursor->tmptable->lk);
    return rc;
}

static unsigned long long lk_tmptbl_cursor_rowid(struct temp_table *tbl,
                                                 BtCursor *btcursor)
{
    pthread_mutex_lock(btcursor->tmptable->lk);
    unsigned long long rowid = tmptbl_cursor_rowid(tbl, btcursor);
    pthread_mutex_unlock(btcursor->tmptable->lk);
    return rowid;
}

static int lk_tmptbl_cursor_count(BtCursor *btcursor, i64 *count)
{
    pthread_mutex_lock(btcursor->tmptable->lk);
    int rc = tmptbl_cursor_count(btcursor, count);
    pthread_mutex_unlock(btcursor->tmptable->lk);
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
    cur->cursor_class = CURSORCLASS_TEMPTABLE;

    if (iTable < 0 || iTable > pBt->num_temp_tables) {
       logmsg(LOGMSG_ERROR, "sqlite3BtreeCursor: unknown iTable %d\n", iTable);
        return SQLITE_INTERNAL;
    }

    cur->tmptable = calloc(1, sizeof(struct temptable));
    if (!cur->tmptable) {
        logmsg(LOGMSG_ERROR, "%s: calloc sizeof(struct temp_table) failed\n",
                __func__);
        return SQLITE_INTERNAL;
    }

    struct temptable *src = &pBt->temp_tables[iTable];
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
        pthread_mutex_lock(cur->tmptable->lk);
    cur->tmptable->cursor = bdb_temp_table_cursor(
        thedb->bdb_env, cur->tmptable->tbl, pArg, &bdberr);
    bdb_temp_table_set_cmp_func(cur->tmptable->tbl, (tmptbl_cmp)xCmp);
    if (cur->tmptable->lk)
        pthread_mutex_unlock(cur->tmptable->lk);

    if (cur->tmptable->cursor == NULL) {
        logmsg(LOGMSG_ERROR, "bdb_temp_table_cursor failed\n");
        free(cur->tmptable);
        return SQLITE_INTERNAL;
    }

    /* if comments above are to be believed
     * this will be set the same on each cursor
     * that is opened for a table */
    cur->rootpage = iTable;
    cur->bt = pBt;
    if (pBt->temp_tables[iTable].flags & BTREE_INTKEY)
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
    u32 type;
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

    /* buffer just contains rrn */
    m.flags = MEM_Int;
    m.u.i = (i64)INT_MAX;
    type = sqlite3VdbeSerialType(&m, SQLITE_DEFAULT_FILE_FORMAT, &len);
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
    int ixnum, tblnum;
    int rc;
    struct sqlclntstate *clnt = thd->sqlclntstate;
    struct dbtable *db;

    if (!clnt->is_analyze) {
        return 0;
    }

    assert(iTable >= RTPAGE_START);
    assert(iTable < (thd->rootpage_nentries + RTPAGE_START));

    get_sqlite_tblnum_and_ixnum(thd, iTable, &tblnum, &ixnum);

    assert(tblnum < thedb->num_dbs);

    if (tblnum < thedb->num_dbs)
        db = thedb->dbs[tblnum];

    rc = analyze_is_sampled(clnt, db->dbname, ixnum);
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

int sqlite3LockStmtTables_int(sqlite3_stmt *pStmt, int after_recovery)
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

    if (nTables == 0)
        return 0;

    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->sqlclntstate;
    int tblnum;

    if (NULL == clnt->dbtran.cursor_tran) {
        return 0;
    }

    /* sort and dedup */
    qsort(tbls, nTables, sizeof(Table *), rootpcompare);

    prev = -1;
    nRemoteTables = 0;

    for (int i = 0; i < nTables; i++) {
        Table *tab = tbls[i];
        iTable = tab->tnum;

        assert(iTable < thd->rootpage_nentries + RTPAGE_START);

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

        tblnum = get_sqlitethd_tblnum(thd, iTable);

        if (tblnum < 0) {
            nRemoteTables++;
            continue;
        }

        struct dbtable *db = NULL;
        db = thedb->dbs[tblnum];

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

            version = table_version_select(db, NULL);
            short_version = fdb_table_version(version);
            if (gbl_fdb_track) {
                logmsg(LOGMSG_ERROR, "%s: table \"%s\" has version %llu (%u), "
                                "checking against %u\n",
                        __func__, db->dbname, version, short_version,
                        clnt->fdb_state.version);
            }

            if (short_version != clnt->fdb_state.version) {
                clnt->fdb_state.err.errval = SQLITE_SCHEMA;
                /* NOTE: first word of the error string is the actual version,
                   expected on the other side; please do not change */
                errstat_set_strf(&clnt->fdb_state.err,
                                 "%llu Stale version local %u != received %u",
                                 version, short_version,
                                 clnt->fdb_state.version);

                /* local table was schema changed in the middle, we need to pass
                 * back an error */
                return SQLITE_SCHEMA;
            }
        }

        bdb_lock_table_read_fromlid(
            db->handle, bdb_get_lid_from_cursortran(clnt->dbtran.cursor_tran));

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
                sqlite3VdbeError(p, "table \"%s\" was schema changed",
                                 db->dbname);
                sqlite3VdbeTransferError(p);

                return SQLITE_ABORT;
            }
        }

        if (after_recovery) {
            unsigned long long table_version = comdb2_table_version(db->dbname);

            /* NOTE: returning here error is very low level, branching many code
               paths.
               Leave version checking for phase 2, for now we make sure that
               longer term sql
               processing is still protected after the recovery */
            if (0) {
                sqlite3VdbeError(
                    p, "table \"%s\" was schema changed during recovery",
                    db->dbname);
                sqlite3VdbeTransferError(p);

                return SQLITE_SCHEMA;
            }
        } else {
            /* we increment only on the initial table locking */
            db->nsql++; /* per table nsql stats */
        }

        int dbtblnum = 0, ixnum;
        get_sqlite_tblnum_and_ixnum(thd, iTable, &dbtblnum, &ixnum);
        reqlog_add_table(thd->bt->reqlogger, thedb->dbs[dbtblnum]->dbname);
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

            tblnum = get_sqlitethd_tblnum(thd, iTable);

            if (tblnum >= 0) {
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

                sqlite3VdbeError(
                    p,
                    "Failed to lock remote table cache for \"%s\" rootp %d\n",
                    tab->zName, iTable);
                sqlite3VdbeTransferError(p);

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
    int tblnum;
    int prev;

    nRemoteTables = 0;
    prev = -1;
    struct sql_thread *thd = pthread_getspecific(query_info_key);

    for (int i = 0; i < nTables; i++) {
        Table *tab = tbls[i];

        iTable = tab->tnum;

        assert(iTable < thd->rootpage_nentries + RTPAGE_START);

        if (iTable < RTPAGE_START)
            continue;

        if (prev >= 0 && prev == iTable) {
            /* don't lock same table twice */
            continue;
        }
        prev = iTable;

        tblnum = get_sqlitethd_tblnum(thd, iTable);

        if (tblnum < 0) {
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
    struct sqlclntstate *clnt = thd->sqlclntstate;
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

    /* set a transaction id if none is set yet */
    if ((iTable >= RTPAGE_START) && !fdb_is_sqlite_stat(fdb, cur->rootpage)) {
        /* I would like to open here a transaction if this is
           an actual update */
        if (clnt->iswrite /* TODO: maybe only create one if we write to remote && fdb_write_is_remote()*/) {
            trans = fdb_trans_begin_or_join(clnt, fdb, tid);
        } else {
            trans = fdb_trans_join(clnt, fdb, tid);
        }
    } else {
        *(unsigned long long *)tid = 0ULL;
        trans = NULL;
    }

    if (trans)
        pthread_mutex_lock(&clnt->dtran_mtx);

    cur->fdbc = fdb_cursor_open(clnt, cur, cur->rootpage, trans, &cur->ixnum);
    if (!cur->fdbc) {
        if (trans)
            pthread_mutex_unlock(&clnt->dtran_mtx);
        return SQLITE_ERROR;
    }

    if (gbl_fdb_track) {
        if (cur->fdbc->isuuid(cur)) {
            uuidstr_t cus, tus;
            logmsg(LOGMSG_USER, "%s Created cursor cid=%s with tid=%s rootp=%d "
                            "db:tbl=\"%s:%s\"\n",
                    __func__, comdb2uuidstr(cur->fdbc->id(cur), cus),
                    comdb2uuidstr(tid, tus), iTable, pBt->zFilename,
                    cur->fdbc->name(cur));
        } else {
            logmsg(LOGMSG_USER, "%s Created cursor cid=%llx with tid=%llx rootp=%d "
                            "db:tbl=\"%s:%s\"\n",
                    __func__, *(unsigned long long *)cur->fdbc->id(cur), tid,
                    iTable, pBt->zFilename, cur->fdbc->name(cur));
        }
    }

    if (trans)
        pthread_mutex_unlock(&clnt->dtran_mtx);

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
    struct sqlclntstate *clnt = thd->sqlclntstate;
    size_t key_size;
    int ixnum, tblnum;
    int rowlocks = use_rowlocks(clnt);
    int rc = SQLITE_OK;
    int bdberr = 0;
    u32 type;
    u32 len;
    int sz = 0;
    void *addcur = NULL;
    struct schema *sc;
    void *shadow_tran = NULL;

    assert(iTable >= RTPAGE_START);
    assert(iTable < thd->rootpage_nentries + RTPAGE_START);

    get_sqlite_tblnum_and_ixnum(thd, iTable, &tblnum, &ixnum);

    assert((tblnum >= 0) && (tblnum < thedb->num_dbs));

    cur->ixnum = ixnum;
    cur->db = thedb->dbs[tblnum];

    /* initialize the shadow, if any  */
    cur->shadtbl = osql_get_shadow_bydb(thd->sqlclntstate, cur->db);

    if (ixnum == -1) {
        if (is_stat2(cur->db->dbname) || is_stat4(cur->db->dbname)) {
            cur->cursor_class = CURSORCLASS_STAT24;
        } else
            cur->cursor_class = CURSORCLASS_TABLE;
        cur->cursor_move = cursor_move_table;
        cur->sc = cur->db->schema;
    } else {
        cur->cursor_class = CURSORCLASS_INDEX;
        cur->cursor_move = cursor_move_index;
        cur->sc = cur->db->ixschema[ixnum];
        cur->nCookFields = -1;
    }

    reqlog_usetable(pBt->reqlogger, cur->db->dbname);

    /* check one time if we have blobs when we open the cursor,
     * so we dont need to run this code for every row if we dont even
     * have them */
    rc = gather_blob_data_byname(cur->db->dbname, ".ONDISK", &cur->blobs);
    if (rc) {
       logmsg(LOGMSG_ERROR, "sqlite3BtreeCursor: gather_blob_data error rc=%d\n", rc);
        return SQLITE_INTERNAL;
    }
    cur->numblobs = cur->blobs.numcblobs;
    if (cur->blobs.numcblobs)
        free_blob_status_data(&cur->blobs);

    cur->tblnum = tblnum;
    if (cur->db == NULL) {
        /* this shouldn't happen */
       logmsg(LOGMSG_ERROR, "sqlite3BtreeCursor: no cur->db\n");
        return SQLITE_INTERNAL;
    }

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
        logmsg(LOGMSG_ERROR, "%s:malloc ondisk_key sz %d failed\n", __func__,
                key_size + sizeof(int));
        free(cur->ondisk_buf);
        return SQLITE_INTERNAL;
    }
    cur->ondisk_keybuf_alloc = key_size;
    if (cur->writeTransaction) {
        cur->fndkey = malloc(key_size + sizeof(int));
        if (!cur->fndkey) {
            logmsg(LOGMSG_ERROR, "%s:malloc fndkey sz %d failed\n", __func__,
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
    if (ixnum == -1) {
        Mem m;
        /* buffer just contains rrn */
        m.flags = MEM_Int;
        m.u.i = (i64)INT_MAX;
        type = sqlite3VdbeSerialType(&m, SQLITE_DEFAULT_FILE_FORMAT, &len);
        sz += len;                   /* need this much for the data */
        sz += sqlite3VarintLen(len); /* need this much for the header */
    } else {
        sc = cur->db->ixschema[ixnum];
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

    cur->bdbcur = bdb_cursor_open(
        cur->db->handle, clnt->dbtran.cursor_tran, shadow_tran, cur->ixnum,
        (shadow_tran && (clnt->dbtran.mode != TRANLEVEL_SOSQL)) ? BDB_OPEN_BOTH
                                                                : BDB_OPEN_REAL,
        (clnt->dbtran.mode == TRANLEVEL_SOSQL)
            ? NULL
            : osql_get_shadtbl_addtbl_newcursor(cur),
        clnt->pageordertablescan, rowlocks,
        rowlocks ? &clnt->holding_pagelocks_flag : NULL,
        rowlocks ? pause_pagelock_cursors : NULL, rowlocks ? (void *)thd : NULL,
        rowlocks ? count_pagelock_cursors : NULL, rowlocks ? (void *)thd : NULL,
        clnt->bdb_osql_trak, &bdberr);
#ifdef OFFLOAD_TEST
    fprintf(stderr, "OPEN %p\n", cur->bdbcur);
#endif
    if (cur->bdbcur == NULL) {
        logmsg(LOGMSG_ERROR, "%s: bdb_cursor_open rc %d\n", __func__, bdberr);
        if (bdberr == BDBERR_DEADLOCK)
            rc = SQLITE_DEADLOCK;
        else
            rc = SQLITE_INTERNAL;
        return rc;
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
    cur->clnt = cur->thd->sqlclntstate;
    cur->vdbe = vdbe;
    cur->sqlite = vdbe ? vdbe->db : NULL;
    cur->bt = bt;
    assert(cur->thd);
    assert(cur->clnt);
}

int sqlite3BtreeCursor(
    Vdbe *vdbe,               /* Vdbe running the show */
    Btree *pBt,               /* BTree containing table to open */
    int iTable,               /* Index of root page */
    struct KeyInfo *pKeyInfo, /* First argument to compare function */
    BtCursor *cur,            /* Space to write cursor structure */
    int flags)
{
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
    cur->open_flags = flags;
    /*printf("Open Cursor rootpage=%d flags=%x\n", iTable, flags);*/

    cur->rootpage = iTable;
    cur->pKeyInfo = pKeyInfo;

    if (pBt->is_temporary) { /* temp table */
        rc = sqlite3BtreeCursor_temptable(pBt, iTable, flags & BTREE_CUR_WR,
                                          temp_table_cmp, pKeyInfo, cur, thd);

        cur->find_cost = cur->move_cost = 0.1;
        cur->write_cost = 0.2;
    }
    /* sqlite_master table */
    else if (iTable == RTPAGE_SQLITE_MASTER && fdb_master_is_local(cur)) {
        rc = sqlite3BtreeCursor_master(
            pBt, iTable, flags & BTREE_CUR_WR, sqlite3VdbeCompareRecordPacked,
            pKeyInfo, cur, thd, clnt->keyDdl, clnt->dataDdl, clnt->nDataDdl);
        cur->find_cost = cur->move_cost = cur->write_cost = 0.0;
    }
    /* remote cursor? */
    else if (pBt->is_remote) {
        rc = sqlite3BtreeCursor_remote(pBt, iTable, flags & BTREE_CUR_WR,
                                       sqlite3VdbeCompareRecordPacked, pKeyInfo,
                                       cur, thd);
    }
    /* if this is analyze opening an index */
    else if (has_compressed_index(iTable, cur, thd)) {
        rc = sqlite3BtreeCursor_analyze(pBt, iTable, flags & BTREE_CUR_WR,
                                        sqlite3VdbeCompareRecordPacked,
                                        pKeyInfo, cur, thd);
        cur->find_cost = cur->move_cost = 0.1;
        cur->write_cost = 0.2;
    }
    /* real table */
    else {
        rc = sqlite3BtreeCursor_cursor(pBt, iTable, flags & BTREE_CUR_WR,
                                       sqlite3VdbeCompareRecordPacked, pKeyInfo,
                                       cur, thd);
        /* treat sqlite_stat1 as free */
        if (is_sqlite_stat(cur->db->dbname)) {
            cur->find_cost = 0.0;
            cur->move_cost = 0.0;
            cur->write_cost = 0.0;
        } else {
            cur->blob_cost = 10.0;
            cur->find_cost = 10.0;
            cur->move_cost = 1.0;
            cur->write_cost = 100;
        }
    }

    cur->on_list = 0;

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
        pthread_mutex_lock(&thd->lk);
        listc_abl(&pBt->cursors, cur);
        cur->on_list = 1;
        pthread_mutex_unlock(&thd->lk);
    }

    reqlog_logf(pBt->reqlogger, REQL_TRACE,
                "Cursor(pBt %d, iTable %d, wrFlag %d, cursor %d)      = %s\n",
                pBt->btreeid, iTable, flags & BTREE_CUR_WR,
                cur ? cur->cursorid : -1, sqlite3ErrStr(rc));

    if (cur && (clnt->dbtran.mode == TRANLEVEL_SERIAL ||
                (cur->is_recording && gbl_selectv_rangechk))) {
        cur->range = currange_new();
        if (cur->db) {
            cur->range->tbname = strdup(cur->db->dbname);
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
    int n = is_stat2(pCur->db->dbname) ? 3 : 2;
    struct schema *sc = find_tag_schema(pCur->db->dbname, ".ONDISK_IX_0");

    /* extract first n fields from packed record */
    if (sqlite_to_ondisk(sc, pData, nData, pCur->ondisk_buf, pCur->clnt->tzname,
                         blobs, MAXBLOBS, &thd->sqlclntstate->fail_reason,
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
    int bias, int seekResult)
{
    const void *pKey = pPayload->pKey;
    sqlite3_int64 nKey = pPayload->nKey;
    const void *pData = pPayload->pData;
    int nData = pPayload->nData;
    int nZero = pPayload->nZero;
    int rc = UNIMPLEMENTED;
    int bdberr;
    int rrn;
    unsigned long long genid;
    blob_buffer_t blobs[MAXBLOBS];
    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = pCur->clnt;

    if (thd && pCur->db == NULL) {
        thd->nwrite++;
        thd->cost += pCur->write_cost;
        pCur->nwrite++;
    }

    bzero(blobs, sizeof(blobs));

    assert(0 == pCur->is_sampled_idx);

    if (clnt->is_readonly &&
        /* exclude writes in a temp table for a select */
        (pCur->cursor_class != CURSORCLASS_TEMPTABLE || clnt->iswrite)){
        errstat_set_strf(&clnt->osql.xerr, "SET READONLY ON for the client");
        rc = SQLITE_ACCESS;
        goto done;
    }

    if (unlikely(pCur->cursor_class == CURSORCLASS_STAT24)) {
        rc = make_stat_record(thd, pCur, pData, nData, blobs);
        if (rc) {
            char errs[128];
            convert_failure_reason_str(&thd->sqlclntstate->fail_reason,
                                       pCur->db->dbname, "SQLite format",
                                       ".ONDISK_IX_0", errs, sizeof(errs));
            fprintf(stderr, "%s: sqlite -> ondisk conversion failed!\n   %s\n",
                    __func__, errs);
            goto done;
        }
    }

    /* send opcode to reload stats at commit */
    if (clnt->is_analyze && is_stat1(pCur->db->dbname))
        rc = osql_updstat(pCur, thd, pCur->ondisk_buf, getdatsize(pCur->db), 0);

    if (pCur->bt->is_temporary) {
        /* data: nKey is 'rrn', pData is record, nData is size of record
         * index: pKey is key, nKey is size of key (no data) */
        char *pFree = 0;
        UnpackedRecord *rec = NULL;
        if (pKey) {
            rec =
                sqlite3VdbeAllocUnpackedRecord(pCur->pKeyInfo, NULL, 0, &pFree);
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
        if (pFree) {
            sqlite3DbFree(pCur->pKeyInfo->db, pFree);
        }

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
             */
            assert(nKey == 0 && pKey == NULL);
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
                rc = sqlite_to_ondisk(pCur->db->ixschema[pCur->ixnum], pKey,
                                      nKey, pCur->ondisk_key, clnt->tzname,
                                      blobs, MAXBLOBS,
                                      &thd->sqlclntstate->fail_reason, pCur);
                if (rc != getkeysize(pCur->db, pCur->ixnum)) {
                    char errs[128];
                    convert_failure_reason_str(
                        &thd->sqlclntstate->fail_reason, pCur->db->dbname,
                        "SQLite format", ".ONDISK_ix", errs, sizeof(errs));
                    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                                "Moveto: sqlite_to_ondisk failed [%s]\n", errs);
                    sqlite3VdbeError(pCur->vdbe, errs, (char *)0);

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
                    logmsg(LOGMSG_ERROR, "%s:%d malloc %d failed\n", __func__,
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

        if (pCur->tblnum < sizeof(clnt->dirty) * 8) { /* tbl num < total bits */
            bset(clnt->dirty, pCur->tblnum);
        }

        if (likely(pCur->cursor_class != CURSORCLASS_STAT24) &&
            likely(pCur->bt == NULL || pCur->bt->is_remote == 0)) {
            rc = sqlite_to_ondisk(
                pCur->db->schema, pData, nData, pCur->ondisk_buf, clnt->tzname,
                blobs, MAXBLOBS, &thd->sqlclntstate->fail_reason, pCur);
            if (rc < 0) {
                char errs[128];
                convert_failure_reason_str(&thd->sqlclntstate->fail_reason,
                                           pCur->db->dbname, "SQLite format",
                                           ".ONDISK", errs, sizeof(errs));
                reqlog_logf(pCur->bt->reqlogger, REQL_TRACE,
                            "Moveto: sqlite_to_ondisk failed [%s]\n", errs);
                sqlite3VdbeError(pCur->vdbe, errs, (char *)0);

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
        else
            is_update = 1;

        if (pCur->bt == NULL || pCur->bt->is_remote == 0) {
            if (is_sqlite_stat(pCur->db->dbname)) {
                clnt->ins_keys = -1ULL;
                clnt->del_keys = -1ULL;
            }
            if (is_update) { /* Updating an existing record. */
                rc = osql_updrec(pCur, thd, pCur->ondisk_buf,
                                 getdatsize(pCur->db), pCur->vdbe->updCols,
                                 blobs, MAXBLOBS);
                clnt->effects.num_updated++;
                clnt->log_effects.num_updated++;
            } else {
                rc = osql_insrec(pCur, thd, pCur->ondisk_buf,
                                 getdatsize(pCur->db), blobs, MAXBLOBS);
                clnt->effects.num_inserted++;
                clnt->log_effects.num_inserted++;
            }
        } else {
            /* make sure we have a distributed transaction and use that to
             * update remote */
            uuid_t tid;
            fdb_tran_t *trans =
                fdb_trans_begin_or_join(clnt, pCur->bt->fdb, tid);

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
            } else {
                rc = pCur->fdbc->insert(pCur, clnt, trans, nKey, nData,
                                        (char *)pData);
                clnt->effects.num_inserted++;
            }
        }
        clnt->ins_keys = 0ULL;
        clnt->del_keys = 0ULL;
        if (gbl_expressions_indexes) {
            free_cached_idx(clnt->idxInsert);
            free_cached_idx(clnt->idxDelete);
        }
        if (rc == SQLITE_DDL_MISUSE)
            sqlite3VdbeError(pCur->vdbe,
                             "Transactional DDL Error: Overlapping Tables");
    }

done:
    free_blob_buffers(blobs, MAXBLOBS);
    reqlog_logf(pCur->bt->reqlogger, REQL_TRACE, "Insert(pCur %d)      = %s\n",
                pCur->cursorid, sqlite3ErrStr(rc));
    return rc;
}

/*
** Advance the cursor to the next entry in the database.  If
** successful then set *pRes=0.  If the cursor
** was already pointing to the last entry in the database before
** this routine was called, then set *pRes=1.
**
** The main entry point is sqlite3BtreeNext().  That routine is optimized
** for the common case of merely incrementing the cell counter BtCursor.aiIdx
** to the next cell on the current page.  The (slower) btreeNext() helper
** routine is called when it is necessary to move to a different page or
** to restore the cursor.
**
** The calling function will set *pRes to 0 or 1.  The initial *pRes value
** will be 1 if the cursor being stepped corresponds to an SQL index and
** if this routine could have been skipped if that SQL index had been
** a unique index.  Otherwise the caller will have set *pRes to zero.
** Zero is the common case. The btree implementation is free to use the
** initial *pRes value as a hint to improve performance, but the current
** SQLite btree implementation does not. (Note that the comdb2 btree
** implementation does use this hint, however.)
*/
int sqlite3BtreeNext(BtCursor *pCur, int *pRes)
{
    int rc = SQLITE_OK;
    int fndlen;
    void *buf;

    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = pCur->clnt;

    if (pCur->empty) {
        *pRes = 1;
        return rc;
    }

    if (move_is_nop(pCur, pRes)) {
        return SQLITE_OK;
    }

    rc = pCur->cursor_move(pCur, pRes, CNEXT);

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
        } else if (*pRes == 1 || rc != IX_OK) {
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

int sqlite3BtreeRecordID(BtCursor *pCur, void *memp)
{
    char *mem = (char *)memp;
    memcpy(mem, &pCur->rrn, sizeof(int));
    memcpy(mem + sizeof(int), &pCur->genid, sizeof(unsigned long long));
    return SQLITE_OK;
}

int sqlite3BtreeRecordIDString(BtCursor *pCur, unsigned long long rowid,
                               char **memp, size_t maxsz)
{
    unsigned long long
        prgenid; /* it's always printed & returned in big-endian */
    int rc;

    if (maxsz == 0) {
        maxsz = 64;
        *memp = sqlite3Malloc(maxsz);
    }

    /* assert that all my assumptions are true */
    assert(sizeof(pCur->rrn) <= sizeof(int));
    assert(sizeof(pCur->genid <= sizeof(unsigned long long)));

    if (!pCur->bt->is_temporary && pCur->cursor_class == CURSORCLASS_TABLE) {
        rc = enque_pfault_olddata_oldkeys(pCur->db, rowid, 0, -1, 0, 1, 1, 1);
    }
    prgenid = flibc_htonll(rowid);
    snprintf(*memp, maxsz, "2:%llu", prgenid);
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
        /* I will cache the schema-s for foreign dbs.
           Let this be the first step to share the schema
           for all database connections */
        pBt->schema = fdb_sqlite_get_schema(pBt, nBytes);
        /* we ignore Xfree since this is shared and not part of a sqlite engine
           space */
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
    return bdb_recno_to_genid(++thd->sqlclntstate->recno);
}

char *sqlite3BtreeGetTblName(BtCursor *pCur) { return pCur->db->dbname; }

void cancel_sql_statement(int id)
{
    int found = 0;
    struct sql_thread *thd;

    pthread_mutex_lock(&gbl_sql_lock);
    LISTC_FOR_EACH(&thedb->sql_threads, thd, lnk)
    {
        if (thd->id == id && thd->sqlclntstate) {
            found = 1;
            thd->sqlclntstate->stop_this_statement = 1;
        }
    }
    pthread_mutex_unlock(&gbl_sql_lock);
    if (found)
        logmsg(LOGMSG_USER, "Query %d was told to stop\n", id);
    else
        logmsg(LOGMSG_USER, "Query %d not found (finished?)\n", id);
}

/* cancel sql statement with the given hex representation of cnonce */
void cancel_sql_statement_with_cnonce(const char *cnonce)
{
    if(!cnonce) return;

    struct sql_thread *thd;
    int found;

    pthread_mutex_lock(&gbl_sql_lock);
    LISTC_FOR_EACH(&thedb->sql_threads, thd, lnk)
    {
        found = 1;
        if (thd->sqlclntstate && thd->sqlclntstate->sql_query && 
            thd->sqlclntstate->sql_query->has_cnonce) {
            const char *sptr = cnonce;
            int cnt = 0;
            void luabb_fromhex(uint8_t *out, const uint8_t *in, size_t len);
            while(*sptr) {
                uint8_t num;
                luabb_fromhex(&num, sptr, 2);
                sptr+=2;

                if (cnt > thd->sqlclntstate->sql_query->cnonce.len || 
                        thd->sqlclntstate->sql_query->cnonce.data[cnt] != num) {
                    found = 0;
                    break;
                }
                cnt++;
            }
            if (found && cnt != thd->sqlclntstate->sql_query->cnonce.len)
                found = 0;

            if (found) {
                thd->sqlclntstate->stop_this_statement = 1;
                break;
            }
        }
    }
    pthread_mutex_unlock(&gbl_sql_lock);
    if (found)
        logmsg(LOGMSG_USER, "Query with cnonce %s was told to stop\n", cnonce);
    else
        logmsg(LOGMSG_USER, "Query with cnonce %s not found (finished?)\n", cnonce);
}

/* log binary cnonce in hex format 
 * ex. 1234 will become x'31323334' 
 */
static void log_cnonce(const char * cnonce, int len)
{
    logmsg(LOGMSG_USER, " [");
    for(int i = 0; i < len; i++) 
        logmsg(LOGMSG_USER, "%2x", cnonce[i]);
    logmsg(LOGMSG_USER, "] ");
}

void sql_dump_running_statements(void)
{
    struct sql_thread *thd;
    BtCursor *cur;
    struct tm tm;
    char rqid[50];

    pthread_mutex_lock(&gbl_sql_lock);
    LISTC_FOR_EACH(&thedb->sql_threads, thd, lnk)
    {
        time_t t = thd->stime;
        localtime_r((time_t *)&t, &tm);
        pthread_mutex_lock(&thd->lk);

        if (thd->sqlclntstate && thd->sqlclntstate->sql) {
            if (thd->sqlclntstate->osql.rqid) {
                uuidstr_t us;
                snprintf(rqid, sizeof(rqid), "txn %016llx %s",
                         thd->sqlclntstate->osql.rqid,
                         comdb2uuidstr(thd->sqlclntstate->osql.uuid, us));
            } else
                rqid[0] = 0;

            logmsg(LOGMSG_USER, "id %d %02d/%02d/%02d %02d:%02d:%02d %s%s\n", thd->id,
                   tm.tm_mon + 1, tm.tm_mday, 1900 + tm.tm_year, tm.tm_hour,
                   tm.tm_min, tm.tm_sec, rqid, thd->sqlclntstate->origin);
            log_cnonce(thd->sqlclntstate->sql_query->cnonce.data,
                thd->sqlclntstate->sql_query->cnonce.len);
            logmsg(LOGMSG_USER, "%s\n", thd->sqlclntstate->sql);

            if (thd->bt) {
                LISTC_FOR_EACH(&thd->bt->cursors, cur, lnk)
                {
                    logmsg(LOGMSG_USER, "  cursor on");
                    if (cur->db == NULL)
                        logmsg(LOGMSG_USER, " schema table");
                    else if (cur->ixnum == -1)
                        logmsg(LOGMSG_USER, " table %s", cur->db->dbname);
                    else
                        logmsg(LOGMSG_USER, " table %s index %d", cur->db->dbname,
                               cur->ixnum);
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
        pthread_mutex_unlock(&thd->lk);
    }
    pthread_mutex_unlock(&gbl_sql_lock);
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

int get_curtran(bdb_state_type *bdb_state, struct sqlclntstate *clnt)
{
    cursor_tran_t *curtran_out = NULL;
    int rcode = 0;
    int retries = 0;
    uint32_t curgen;
    int bdberr = 0;
    int max_retries =
        gbl_move_deadlk_max_attempt >= 0 ? gbl_move_deadlk_max_attempt : 500;
    int lowpri = gbl_lowpri_snapisol_sessions &&
                 (clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
                  clnt->dbtran.mode == TRANLEVEL_SERIAL);
    int rc = 0;

    /*fprintf(stderr, "get_curtran\n"); */

    if (clnt->dbtran.cursor_tran) {
        logmsg(LOGMSG_ERROR, "%s called when we have a curtran\n", __func__);
        return -1;
    }

    if (gbl_random_get_curtran_failures && !(rand() % 1000)) {
        logmsg(LOGMSG_ERROR, "%s forcing a random curtran failure\n", __func__);
        return -1;
    }

    if (clnt->gen_changed) {
        logmsg(LOGMSG_ERROR, "td %u %s line %d calling get_curtran on gen_changed\n",
                pthread_self(), __func__, __LINE__);
        cheap_stack_trace();
    }

retry:
    bdberr = 0;
    curtran_out = bdb_get_cursortran(bdb_state, lowpri, &bdberr);
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
        bdb_put_cursortran(bdb_state, curtran_out, &bdberr);
        curtran_out = NULL;
        clnt->gen_changed = 1;
        logmsg(LOGMSG_ERROR, "td %u %s: failing because generation has changed: "
                        "orig-gen=%u, cur-gen=%u\n",
                (uint32_t)pthread_self(), __func__, clnt->init_gen, curgen);
        cheap_stack_trace();
        return -1;
    }

    if (!clnt->init_gen)
        clnt->init_gen = curgen;

    clnt->dbtran.cursor_tran = curtran_out;

    assert(curtran_out != NULL);

    /* check if we have table locks that were dropped during recovery */
    if (clnt->dbtran.pStmt) {
        rc = sqlite3LockStmtTablesRecover(clnt->dbtran.pStmt);
        if (!rc) {
            /* NOTE: we need to make sure the versions are the same */

        } else {
            if (rc == SQLITE_SCHEMA) {
                /* btrees have changed under my feet */
                return rc;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to lock back tables!\n", __func__);
            /* KEEP GOING, THIS MIGHT NOT BE THAT BAD */
        }
    }

    return rcode;
}

int put_curtran_int(bdb_state_type *bdb_state, struct sqlclntstate *clnt,
                    int is_recovery)
{
    int rc = 0;
    int bdberr = 0;

#ifdef DEBUG
    fprintf(stderr, "%llx, %s\n", pthread_self(), __func__);
#endif

    if (!clnt->dbtran.cursor_tran) {
        logmsg(LOGMSG_ERROR, "%s called without curtran\n", __func__);
        cheap_stack_trace();
        return -1;
    }

    rc = bdb_put_cursortran(bdb_state, clnt->dbtran.cursor_tran, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: %d rc %d bdberror %d\n", __func__, pthread_self(),
                rc, bdberr);
        ctrace("%s: rc %d bdberror %d\n", __func__, rc, bdberr);
        if (bdberr == BDBERR_BUG_KILLME) {
            /* should I panic? */
            logmsg(LOGMSG_FATAL, "This db is leaking locks, it will deadlock!!! \n");
            bdb_dump_active_locks(bdb_state, stdout);
            exit(1);
        }
    }

    if (!is_recovery) {
        if (clnt->dbtran.nLockedRemTables > 0) {
            int irc = sqlite3UnlockStmtTablesRemotes(clnt);
            if (irc) {
                logmsg(LOGMSG_ERROR, "%s: error releasing remote tables rc=%d\n",
                        __func__, irc);
            }
        }

        clnt->dbtran.pStmt =
            NULL; /* this is pointing to freed memory at this point */
    }

    clnt->dbtran.cursor_tran = NULL;
    clnt->is_overlapping = 0;

    return rc;
}

int put_curtran_recovery(bdb_state_type *bdb_state, struct sqlclntstate *clnt)
{
    return put_curtran_int(bdb_state, clnt, 1);
}

int put_curtran(bdb_state_type *bdb_state, struct sqlclntstate *clnt)
{
    return put_curtran_int(bdb_state, clnt, 0);
}

/* Return a count of all cursors to the lower layer for debugging */
int count_pagelock_cursors(void *arg)
{
    struct sql_thread *thd = (struct sql_thread *)arg;
    struct sqlclntstate *clnt = thd->sqlclntstate;
    int rc, count = 0;

    pthread_mutex_lock(&thd->lk);
    count = listc_size(&thd->bt->cursors);
    pthread_mutex_unlock(&thd->lk);

    return count;
}

/* Pause all pagelock cursors before grabbing a rowlock */
int pause_pagelock_cursors(void *arg)
{
    BtCursor *cur = NULL;
    struct sql_thread *thd = (struct sql_thread *)arg;
    struct sqlclntstate *clnt = thd->sqlclntstate;
    int bdberr, rc;

    /* Return immediately if nothing is holding a pagelock */
    if (!clnt->holding_pagelocks_flag)
        return 0;

    pthread_mutex_lock(&thd->lk);

    LISTC_FOR_EACH(&thd->bt->cursors, cur, lnk)
    {
        if (cur->bdbcur) {
            rc = cur->bdbcur->pause(cur->bdbcur, &bdberr);
            assert(0 == rc);
        }
    }

    pthread_mutex_unlock(&thd->lk);

    clnt->holding_pagelocks_flag = 0;
    return 0;
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
static int recover_deadlock_int(bdb_state_type *bdb_state,
                                struct sql_thread *thd,
                                bdb_cursor_ifn_t *bdbcur, int sleepms,
                                int ptrace)
{
    struct sqlclntstate *clnt = thd->sqlclntstate;
    BtCursor *cur = NULL;
    int error = 0;
    int rc = 0;
    int bdberr;
#if 0
   char buf[160];
#endif
    int new_mode = debug_switch_recover_deadlock_newmode();

    if (bdb_lock_desired(thedb->bdb_env)) {
        if (!sleepms)
            sleepms = 2000;

        logmsg(LOGMSG_ERROR, "THD %d:recover_deadlock, and lock desired\n",
                pthread_self());
    } else if (ptrace)
        logmsg(LOGMSG_ERROR, "THD %d:recover_deadlock\n", pthread_self());

    /* increment global counter */
    gbl_sql_deadlock_reconstructions++;

    /* unlock cursors */
    pthread_mutex_lock(&thd->lk);

    if (thd->bt) {
        LISTC_FOR_EACH(&thd->bt->cursors, cur, lnk)
        {
            if (cur->bdbcur) {
                if (cur->bdbcur->unlock(cur->bdbcur, &bdberr))
                    ctrace("%s: cur ixnum=%d bdberr = %d [1]\n", __func__,
                           cur->ixnum, bdberr);
#ifdef OFFLOAD_TEST
                fprintf(stderr, "UNLOCK %p\n", cur->bdbcur);
#endif
            }
        }
    }

    if (bdbcur) {
        if (bdbcur->unlock(bdbcur, &bdberr))
            ctrace("%s: cur ixnum=%d bdberr = %d [2]\n", __func__, cur->ixnum,
                   bdberr);
#ifdef OFFLOAD_TEST
        fprintf(stderr, "UNLOCK %p\n", bdbcur);
#endif
    }

    pthread_mutex_unlock(&thd->lk);

#if 0
   sprintf(buf, "recover_deadlock about to put curtran tid %d\n",
         pthread_self());
   bdb_bdblock_print(thedb->bdb_env, buf);
#endif

    /* free curtran */
    rc = put_curtran_recovery(thedb->bdb_env, clnt);
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
#if 0
   sprintf(buf, "recover_deadlock put curtran tid %d\n", pthread_self());
   bdb_bdblock_print(thedb->bdb_env, buf);
#endif

    /*
     * fprintf(stderr, "sleeping\n");
     * sleep(5);
     * fprintf(stderr, "done sleeping\n");
     */

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

    /* get a new curtran */
    rc = get_curtran(thedb->bdb_env, clnt);
    if (rc) {
        if (rc == SQLITE_SCHEMA)
            return SQLITE_SCHEMA;

        if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DURABLE_LSNS)) {
            logmsg(LOGMSG_ERROR, 
                    "%s: fail to open a new curtran, rc=%d, return changenode\n",
                    __func__, rc);
            return SQLITE_CLIENT_CHANGENODE;
        } else {
            logmsg(LOGMSG_ERROR, "%s: fail to open a new curtran, rc=%d\n", __func__,
                    rc);
            return -500;
        }
    }

    /* no need to mess with our shadow tran, right? */

    /* now that we have a new curtran, try to reposition them */
    pthread_mutex_lock(&thd->lk);
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
                    pthread_mutex_unlock(&thd->lk);
                    logmsg(LOGMSG_ERROR, "bdb_cursor_lock returned %d %d\n", rc,
                            bdberr);
                    return -700;
                }
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
            pthread_mutex_unlock(&thd->lk);
            return -800;
        }
    }

    pthread_mutex_unlock(&thd->lk);

    return 0;
}

int recover_deadlock(bdb_state_type *bdb_state, struct sql_thread *thd,
                     bdb_cursor_ifn_t *bdbcur, int sleepms)
{
    return recover_deadlock_int(bdb_state, thd, bdbcur, sleepms, 1);
}

int recover_deadlock_silent(bdb_state_type *bdb_state, struct sql_thread *thd,
                            bdb_cursor_ifn_t *bdbcur, int sleepms)
{
    return recover_deadlock_int(bdb_state, thd, bdbcur, sleepms, 0);
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
    clnt = thd->sqlclntstate;
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
            pCur->range->tbname = strdup(pCur->db->dbname);
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
            if (rc = recover_deadlock(thedb->bdb_env, thd,
                                      (is_temp_bdbcur) ? cur : NULL, 0)) {
                if (rc == SQLITE_CLIENT_CHANGENODE)
                    *bdberr = BDBERR_NOT_DURABLE;
                else if (!gbl_rowlocks)
                    logmsg(LOGMSG_ERROR, "%s: %d failed dd recovery\n", __func__,
                            pthread_self());
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
    clnt = thd->sqlclntstate;
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
            pCur->range->tbname = strdup(pCur->db->dbname);
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
            if (rc = recover_deadlock(thedb->bdb_env, thd, NULL, 0)) {
                if (rc == SQLITE_CLIENT_CHANGENODE)
                    *bdberr = BDBERR_NOT_DURABLE;
                else if (!gbl_rowlocks)
                    logmsg(LOGMSG_ERROR, "%s: %d failed dd recovery\n", __func__,
                            pthread_self());
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

    struct sqlclntstate *clnt = thd->sqlclntstate;

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
            if (rc = recover_deadlock(thedb->bdb_env, thd,
                                      (freshcursor) ? pCur->bdbcur : NULL, 0)) {
                if (rc == SQLITE_CLIENT_CHANGENODE)
                    *bdberr = BDBERR_NOT_DURABLE;
                else
                    logmsg(LOGMSG_ERROR, "%s: %d failed dd recovery\n", __func__,
                            pthread_self());
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
            rc = SQLITE_CLIENT_CHANGENODE;
            *bdberr = BDBERR_NOT_DURABLE;
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
        pthread_mutex_lock(&thd->lk);
        BtCursor *cur;
        LISTC_FOR_EACH(&thd->bt->cursors, cur, lnk)
        {
            if (cur->bdbcur /*&& cur->genid == genid */) {
                if (cur->bdbcur->unlock(cur->bdbcur, bdberr)) {
                    logmsg(LOGMSG_ERROR, "%s: failed to unlock cursor %d\n",
                            __func__, *bdberr);
                    pthread_mutex_unlock(&thd->lk);
                    return -100;
                }
            }
        }
        pthread_mutex_unlock(&thd->lk);
    } else {
        logmsg(LOGMSG_ERROR, 
                "%s wow, someone forgot to set query_info_key pthread key!\n",
                __func__);
        *bdberr = BDBERR_BUG_KILLME;
        return -1;
    }

    return 0;
}

int convert_client_ftype(int type)
{
    int ret = 0;
    switch (type) {
    case CDB2_INTEGER:
        ret = CLIENT_INT;
        break;
    case CDB2_REAL:
        ret = CLIENT_REAL;
        break;
    case CDB2_CSTRING:
        ret = CLIENT_CSTR;
        break;
    case CDB2_BLOB:
        ret = CLIENT_BLOB;
        break;
    case CDB2_DATETIME:
        ret = CLIENT_DATETIME;
        break;
    case CDB2_DATETIMEUS:
        ret = CLIENT_DATETIMEUS;
        break;
    case CDB2_INTERVALYM:
        ret = CLIENT_INTVYM;
        break;
    case CDB2_INTERVALDS:
        ret = CLIENT_INTVDS;
        break;
    case CDB2_INTERVALDSUS:
        ret = CLIENT_INTVDSUS;
        break;
    default:
        ret = -1;
        break;
    }
    return ret;
}

/*
** Convert protobuf Bindvalue to our struct field
*/
struct field *convert_client_field(CDB2SQLQUERY__Bindvalue *bindvalue,
                                   struct field *c_fld)
{
    c_fld->type = convert_client_ftype(bindvalue->type);
    c_fld->datalen = bindvalue->value.len;
    c_fld->idx = -1;
    c_fld->name = bindvalue->varname;
    c_fld->offset = 0;
    return c_fld;
}

/*
** For sql queries with replaceable parameters.
** Take data from buffer and blobs, bind to
** sqlite parameters.
*/
int bind_parameters(sqlite3_stmt *stmt, struct schema *params,
                    struct sqlclntstate *clnt, char **err)
{
    /* old parameters */
    CDB2SQLQUERY *sqlquery = clnt->sql_query;
    char *buf = (char *)clnt->tagbuf;

    /* initial stack variables */
    int fld;
    struct field c_fld;
    struct field *f;
    char *str;
    int datalen;
    char parmname[32];
    int namelen;
    int isnull = 0;
    int rc;
    int blobno = 0;
    int little_endian = 0;
    int outsz;

    /* values from buffer */
    long long ival;
    unsigned long long uival;
    void *byteval;
    double dval;

    /* datetime stuff */
    /* client side */
    cdb2_client_intv_ym_t *ciym;
    cdb2_client_intv_ds_t *cids;

    /* server side */
    dttz_t dt;
    intv_t it;

    int nfields;
    int do_intv_flip = 0;

    *err = NULL;

    nfields = sqlite3_bind_parameter_count(stmt);
    if ((params && nfields != params->nmembers) ||
        (sqlquery && nfields != sqlquery->n_bindvars)) {
        if (params) {
            *err = sqlite3_mprintf(
                "Query specified %d parameters, but %d values provided",
                nfields, params->nmembers);
        } else {
            *err = sqlite3_mprintf(
                "Query specified %d parameters, but %d values provided",
                nfields, sqlquery->n_bindvars);
        }
        return -1;
    }

    for (fld = 0; fld < nfields; fld++) {
        int pos = 0;
        isnull = 0;
        if (params) {
            f = &params->member[fld];
        } else {
            f = convert_client_field(sqlquery->bindvars[fld], &c_fld);
            if (sqlquery->bindvars[fld]->has_index) {
                pos = sqlquery->bindvars[fld]->index;
            }
            buf = sqlquery->bindvars[fld]->value.data;
            if (buf == NULL) {
                if (f->type == CLIENT_BLOB && c_fld.datalen == 0 &&
                    sqlquery->bindvars[fld]->has_isnull &&
                    sqlquery->bindvars[fld]->isnull == 0) {
                    buf = "";
                    isnull = 0;
                } else {
                    isnull = 1;
                }
            } else if (sqlquery->little_endian) {
                if ((f->type == CLIENT_INT) || (f->type == CLIENT_UINT) ||
                    (f->type == CLIENT_REAL)) {
#ifndef _LINUX_SOURCE
                    uint8_t val1[8];
                    memcpy(&val1, buf, c_fld.datalen);
                    const void *new_buf = buf_little_get(
                        buf, c_fld.datalen, val1, val1 + c_fld.datalen);
#else
                    const void *new_buf =
                        buf_get(buf, c_fld.datalen, buf, buf + c_fld.datalen);
#endif
                }
                little_endian = 1;
#ifndef _LINUX_SOURCE
                if (!do_intv_flip)
                    do_intv_flip = 1;
#endif
            } else if (do_intv_flip != 1) {
#ifdef _LINUX_SOURCE
                if (!sqlquery->little_endian) {
                    do_intv_flip = 1;
                }
#endif
            }
        }

        if (pos == 0) {
            /* Bind parameters with the matching @xx identifiers in sql query.*/
            namelen = snprintf(parmname, sizeof(parmname), "@%s", f->name);
            if (namelen > 31) {
                *err = sqlite3_mprintf("Invalid field name %s\n", f->name);
                return -1;
            }

            if (f->idx != -1) {
                pos = f->idx;
            } else {
                pos = sqlite3_bind_parameter_index(stmt, parmname);
                /* will be used in caching.*/
                f->idx = pos;
            }
            if (pos == 0) {
                *err = sqlite3_mprintf(
                    "No \"%s\" parameter specified in query.", f->name);
                return -1;
            }
        }
        if (clnt->nullbits) isnull = btst(clnt->nullbits, fld) ? 1 : 0;
        if (gbl_dump_sql_dispatched)
            logmsg(LOGMSG_USER, "binding field %d name %s position %d type %d %s pos %d "
                   "null %d\n",
                   fld, f->name, pos, f->type, strtype(f->type), pos, isnull);
        if (isnull)
            rc = sqlite3_bind_null(stmt, pos);
        else {
            switch (f->type) {
            case CLIENT_INT:
                if ((rc = get_int_field(f, buf, (uint64_t *)&ival)) == 0)
                    rc = sqlite3_bind_int64(stmt, pos, ival);
                break;
            case CLIENT_UINT:
                if ((rc = get_uint_field(f, buf, (uint64_t *)&uival)) == 0)
                    rc = sqlite3_bind_int64(stmt, pos, uival);
                break;
            case CLIENT_REAL:
                if ((rc = get_real_field(f, buf, &dval)) == 0)
                    rc = sqlite3_bind_double(stmt, pos, dval);
                break;
            case CLIENT_CSTR:
            case CLIENT_PSTR:
            case CLIENT_PSTR2:
                if ((rc = get_str_field(f, buf, &str, &datalen)) == 0)
                    rc = sqlite3_bind_text(stmt, pos, str, datalen, NULL);
                break;
            case CLIENT_BYTEARRAY:
                if ((rc = get_byte_field(f, buf, &byteval, &datalen)) == 0)
                    rc = sqlite3_bind_blob(stmt, pos, byteval, datalen, NULL);
                break;
            case CLIENT_BLOB:
                if (params) {
                    if ((rc = get_blob_field(blobno, clnt, &byteval,
                                             &datalen)) == 0) {
                        rc = sqlite3_bind_blob(stmt, pos, byteval, datalen,
                                               NULL);
                        blobno++;
                    }
                } else {
                    rc = sqlite3_bind_blob(stmt, pos, buf, f->datalen, NULL);
                    blobno++;
                }
                break;
            case CLIENT_VUTF8:
                if (params) {
                    if ((rc = get_blob_field(blobno, clnt, &byteval,
                                             &datalen)) == 0) {
                        rc = sqlite3_bind_text(stmt, pos, byteval, datalen,
                                               NULL);
                        blobno++;
                    }
                } else {
                    rc = sqlite3_bind_text(stmt, pos, buf, f->datalen, NULL);
                    blobno++;
                }
                break;
            case CLIENT_DATETIME:
                if ((rc = get_datetime_field(f, buf, clnt->tzname, &dt,
                                             little_endian)) == 0)
                    rc = sqlite3_bind_datetime(stmt, pos, &dt, clnt->tzname);
                break;

            case CLIENT_DATETIMEUS:
                if ((rc = get_datetimeus_field(f, buf, clnt->tzname, &dt,
                                               little_endian)) == 0)
                    rc = sqlite3_bind_datetime(stmt, pos, &dt, clnt->tzname);
                break;

            case CLIENT_INTVYM: {
                intv_t tv;
                cdb2_client_intv_ym_t ci;
                int outnull;
                int outdtz;
                server_intv_ym_t si;

                ci = *(cdb2_client_intv_ym_t *)(buf + f->offset);
                if (do_intv_flip) {
                    char *nbuf = (buf + f->offset);
#ifdef _LINUX_SOURCE
                    client_intv_ym_get(&ci, nbuf, nbuf + sizeof(ci));
#else
                    client_intv_ym_little_get(&ci, nbuf, nbuf + sizeof(ci));
#endif
                }
                rc = CLIENT_INTVYM_to_SERVER_INTVYM(
                    &ci, sizeof(cdb2_client_intv_ym_t), 0, NULL, NULL, &si,
                    sizeof(server_intv_ym_t), &outdtz, NULL, NULL);
                if (rc) {
                    *err = sqlite3_mprintf("Can't convert client intervalym to "
                                           "sql interval rc %d\n",
                                           rc);
                    return -1;
                }
                tv.type = INTV_YM_TYPE;
                tv.sign = ci.sign;
                tv.u.ym.years = ci.years;
                tv.u.ym.months = ci.months;
                /* TODO: pass by pointer */
                rc = sqlite3_bind_interval(stmt, pos, tv);
                if (rc) {
                    *err = sqlite3_mprintf("sqlite3_bind_datetime rc %d\n", rc);
                    return -1;
                }

                break;
            }

            case CLIENT_INTVDS: {
                intv_t tv;
                cdb2_client_intv_ds_t ci;
                int outnull;
                int outdtz;
                server_intv_ds_t si;

                ci = *(cdb2_client_intv_ds_t *)(buf + f->offset);
                if (do_intv_flip) {
                    char *nbuf = (buf + f->offset);
#ifdef _LINUX_SOURCE
                    client_intv_ds_get(&ci, nbuf, nbuf + sizeof(ci));
#else

                    client_intv_ds_little_get(&ci, nbuf, nbuf + sizeof(ci));
#endif
                }
                rc = CLIENT_INTVDS_to_SERVER_INTVDS(
                    &ci, sizeof(cdb2_client_intv_ds_t), 0, NULL, NULL, &si,
                    sizeof(server_intv_ds_t), &outdtz, NULL, NULL);
                if (rc) {
                    *err = sqlite3_mprintf("Can't convert client intervalym to "
                                           "sql interval rc %d\n",
                                           rc);
                    return -1;
                }
                tv.type = INTV_DS_TYPE;
                tv.sign = ci.sign;

                tv.u.ds.days = ci.days;
                tv.u.ds.hours = ci.hours;
                tv.u.ds.mins = ci.mins;
                tv.u.ds.sec = ci.sec;
                tv.u.ds.frac = ci.msec;
                tv.u.ds.prec = DTTZ_PREC_MSEC;
                /* TODO: pass by pointer */
                rc = sqlite3_bind_interval(stmt, pos, tv);
                if (rc) {
                    *err = sqlite3_mprintf("sqlite3_bind_datetime rc %d\n", rc);
                    return -1;
                }
                break;
            }

            case CLIENT_INTVDSUS: {
                intv_t tv;
                cdb2_client_intv_dsus_t ci;
                int outnull;
                int outdtz;
                server_intv_dsus_t si;

                ci = *(cdb2_client_intv_dsus_t *)(buf + f->offset);
                if (do_intv_flip) {
                    char *nbuf = (buf + f->offset);
#ifdef _LINUX_SOURCE
                    client_intv_dsus_get(&ci, nbuf, nbuf + sizeof(ci));
#else

                    client_intv_dsus_little_get(&ci, nbuf, nbuf + sizeof(ci));
#endif
                }
                rc = CLIENT_INTVDSUS_to_SERVER_INTVDSUS(
                    &ci, sizeof(cdb2_client_intv_dsus_t), 0, NULL, NULL, &si,
                    sizeof(server_intv_dsus_t), &outdtz, NULL, NULL);
                if (rc) {
                    *err = sqlite3_mprintf("Can't convert client intervalym to "
                                           "sql interval rc %d\n",
                                           rc);
                    return -1;
                }
                tv.type = INTV_DSUS_TYPE;
                tv.sign = ci.sign;

                tv.u.ds.days = ci.days;
                tv.u.ds.hours = ci.hours;
                tv.u.ds.mins = ci.mins;
                tv.u.ds.sec = ci.sec;
                tv.u.ds.frac = ci.usec;
                tv.u.ds.prec = DTTZ_PREC_USEC;
                /* TODO: pass by pointer */
                rc = sqlite3_bind_interval(stmt, pos, tv);
                if (rc) {
                    *err = sqlite3_mprintf("sqlite3_bind_datetime rc %d\n", rc);
                    return -1;
                }
                break;
            }

            case COMDB2_NULL_TYPE:
                rc = sqlite3_bind_null(stmt, pos);
                break;

            default:
                logmsg(LOGMSG_ERROR, "Unknown type %d\n", f->type);
                rc = SQLITE_ERROR;
            }
        }
        if (gbl_dump_sql_dispatched)
            logmsg(LOGMSG_USER, 
                   "fld %d %s position %d type %d %s len %d null %d bind rc %d\n",
                   fld, f->name, f->type, pos, strtype(f->type), f->datalen,
                   isnull, rc);
        if (rc) {
            *err = sqlite3_mprintf("Bad argument for field:%s type:%d\n",
                                   f->name, f->type);
            return rc;
        }
        if (sqlquery && sqlquery->little_endian && buf &&
            ((f->type == CLIENT_INT) || (f->type == CLIENT_UINT) ||
             (f->type == CLIENT_REAL))) {
#ifndef _LINUX_SOURCE
            uint8_t val1[8];
            memcpy(&val1, buf, c_fld.datalen);
            const void *new_buf =
                buf_little_get(buf, c_fld.datalen, val1, val1 + c_fld.datalen);
#else
            const void *new_buf =
                buf_get(buf, c_fld.datalen, buf, buf + c_fld.datalen);
#endif
        }
    }
    return rc;
}

int sqlite3BtreeSetRecording(BtCursor *pCur, int flag)
{
    assert(pCur);
    struct sql_thread *thd = pCur->thd;
    struct sqlclntstate *clnt = pCur->clnt;

    pCur->is_recording = flag;

    if (pCur->is_recording) {
        if (gbl_selectv_rangechk) {
            pCur->range = currange_new();
            if (pCur->db) {
                pCur->range->tbname = strdup(pCur->db->dbname);
                pCur->range->idxnum = pCur->ixnum;
            }
        }
    }

    return 0;
}

void *get_lastkey(BtCursor *pCur)
{
    if (pCur->is_sampled_idx) {
        return bdb_temp_table_key(pCur->sampled_idx->cursor);
    } else {
        return pCur->lastkey;
    }
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
    int rc;
    /* TODO AZ:

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
        count = analyze_get_sampled_nrecs(pCur->db->dbname, pCur->ixnum);
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
        while (res == 0 && rc == 0) {
            if (!gbl_selectv_rangechk) {
                if (pCur->is_recording &&
                    thd->sqlclntstate->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
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

/* rowid caching is optional - we can always return that nothing is
   cached.  do that for now, this can be an optimization for later */
/*
 ** Set the cached rowid value of every cursor in the same database file
 ** as pCur and having the same root page number as pCur.  The value is
 ** set to iRowid.
 **
 ** Only positive rowid values are considered valid for this cache.
 ** The cache is initialized to zero, indicating an invalid cache.
 ** A btree will work fine with zero or negative rowids.  We just cannot
 ** cache zero or negative rowids, which means tables that use zero or
 ** negative rowids might run a little slower.  But in practice, zero
 ** or negative rowids are very uncommon so this should not be a problem.
 */
void sqlite3BtreeSetCachedRowid(BtCursor *pCur, sqlite3_int64 iRowid)
{
    /* don't need to do anything here */
}

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
    if (thd && thd->sqlclntstate)
        thd->sqlclntstate->fail_reason.reason =
            CONVERT_FAILED_INCOMPATIBLE_VALUES;
}

int is_comdb2_index_disableskipscan(const char *dbname, char *idx)
{
    struct dbtable *db = get_dbtable_by_name(dbname);
    if (db) {
        int i;
        for (i = 0; i < db->nix; ++i) {
            struct schema *s = db->ixschema[i];
            if (s->sqlitetag && strcmp(s->sqlitetag, idx) == 0) {
                return (s->disableskipscan);
            }
        }
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
    struct dbtable *db = get_dbtable_by_name(dbname);
    if (db) {
        struct field *f;
        if (icol < 0 || icol >= db->schema->nmembers)
            return -1;
        switch (db->schema->member[icol].type) {
        case CLIENT_BLOB:
        case SERVER_BLOB:
        case CLIENT_BLOB2:
        case SERVER_BLOB2:
        case CLIENT_VUTF8:
        case SERVER_VUTF8:
            db->ix_blob = 1;
            return 1;
        default:
            return 0;
        }
        return db->ix_expr;
    }
    return 0;
}

void comdb2SetWriteFlag(int wrflag)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->sqlclntstate;

    /* lets make sure we don't downgrade write to readonly */
    if (clnt->writeTransaction < wrflag)
        clnt->writeTransaction = wrflag;
}

void _sockpool_socket_type(const char *dbname, const char *service, char *host,
                           char *socket_type, int socket_type_len)
{
    /* NOTE: in fdb, we want to require a specific node.
       We make the name include that node to be able to request
       sockets going to a specific node. */
    snprintf(socket_type, socket_type_len, "comdb2/%s/%s/%s", dbname, service,
             host);
}

static int _sockpool_get(const char *dbname, const char *service, char *host)
{
    char socket_type[512];
    int fd;

    if (unlikely(
            bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DISABLE_SERVER_SOCKPOOL)))
        return -1;

    _sockpool_socket_type(dbname, service, host, socket_type,
                          sizeof(socket_type));

    /* TODO: don't rely on hints yet */
    /* we allow locally cached sockets */
    fd =
        socket_pool_get_ext(socket_type, 0, SOCKET_POOL_GET_GLOBAL, NULL, NULL);

    if (gbl_fdb_track)
        logmsg(LOGMSG_ERROR, "%p: Asked socket for %s got %d\n", pthread_self(),
                socket_type, fd);

    return fd;
}

void disconnect_remote_db(const char *dbname, const char *service, char *host,
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

    _sockpool_socket_type(dbname, service, host, socket_type,
                          sizeof(socket_type));

    if (gbl_fdb_track)
        logmsg(LOGMSG_ERROR, "%p: Donating socket for %s\n", pthread_self(),
                socket_type);

    /* this is used by fdb sql for now */
    socket_pool_donate_ext(socket_type, fd, IOTIMEOUTMS / 1000, 0,
                           SOCKET_POOL_GET_GLOBAL, NULL, NULL);

    sbuf2free(sb);
    *psb = NULL;
}

/* use portmux to open an SBUF2 to local db or proxied db
   it is trying to use sockpool
 */
SBUF2 *connect_remote_db(const char *dbname, const char *service, char *host)
{
    SBUF2 *sb;
    struct in_addr addr;
    int port;
    int retry;
    int sockfd;
    int flag;
    int rc;

    /* lets try to use sockpool, if available */
    sockfd = _sockpool_get(dbname, service, host);
    if (sockfd > 0) {
        goto sbuf;
    }

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

    /* disable Nagle */
    flag = 1;
    rc = setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag,
                    sizeof(int));
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: couldnt turn off nagel on new fd %d: %d %s\n",
                __func__, sockfd, errno, strerror(errno));
        close(sockfd);
        return NULL;
    }

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
    struct sqlclntstate *clnt = thd->sqlclntstate;

    return clnt->planner_effort;
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
        return thd->sqlclntstate->sql;

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

        sqlite3DbFree(pCur->sqlite, expr);
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

        assert(mask == BTREE_SEEK_EQ || mask == BTREE_BULKLOAD || mask == 0);

        sqlite3BtreeCursorHint_Flags(pCur, mask);

        break;
    }

    case BTREE_HINT_RANGE: {
        Expr *expr = va_arg(ap, Expr *);
        Mem *mem = va_arg(ap, struct Mem *);

        sqlite3BtreeCursorHint_Range(pCur, expr);

        break;
    }
    }
    va_end(ap);
}

int comdb2_sqlitecursor_is_hinted(int id)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->sqlclntstate;
    int i;

    if (!clnt->hinted_cursors || !clnt->hinted_cursors_used)
        return 0;

    for (i = 0; i < clnt->hinted_cursors_used; i++) {
        if (clnt->hinted_cursors[i] == id) {
            return 1;
        }
    }
    return 0;
}

int comdb2_sqlitecursor_set_hinted(int id)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->sqlclntstate;

    if (!clnt->hinted_cursors) {
        clnt->hinted_cursors = (int *)calloc(16, sizeof(int));
        if (!clnt->hinted_cursors) {
            logmsg(LOGMSG_ERROR, "%s: malloc fail\n", __func__);
            return -1;
        }
        clnt->hinted_cursors_alloc = 16;
        clnt->hinted_cursors_used = 0;
    }

    if (clnt->hinted_cursors_alloc < clnt->hinted_cursors_used + 1) {
        clnt->hinted_cursors = (int *)realloc(
            clnt->hinted_cursors, clnt->hinted_cursors_alloc * 2 * sizeof(int));
        if (!clnt->hinted_cursors) {
            logmsg(LOGMSG_ERROR, "%s: malloc fail\n", __func__);
            return -1;
        }
        clnt->hinted_cursors_alloc *= 2;
    }

    clnt->hinted_cursors[clnt->hinted_cursors_used] = id;
    clnt->hinted_cursors_used++;

    return 0;
}

void clnt_reset_cursor_hints(struct sqlclntstate *clnt)
{
    if (clnt->hinted_cursors) {
        if (clnt->hinted_cursors_alloc > 256) {
            clnt->hinted_cursors =
                (int *)realloc(clnt->hinted_cursors, 256 * sizeof(int));
            clnt->hinted_cursors_alloc = 256;
        }
        bzero(clnt->hinted_cursors, clnt->hinted_cursors_alloc);
        clnt->hinted_cursors_used = 0;
    } else {
        clnt->hinted_cursors_alloc = 0;
        clnt->hinted_cursors_used = 0;
    }
}
int fdb_packedsqlite_extract_genid(char *key, int *outlen, char *outbuf)
{
    int hdroffset = 0;
    int dataoffset = 0;
    unsigned int hdrsz;
    int type = 0;
    Mem m;

    /* extract genid */
    hdroffset = sqlite3GetVarint32(key, &hdrsz);
    dataoffset = hdrsz;
    hdroffset += sqlite3GetVarint32(key + hdroffset, &type);
    assert(type == 6);
    assert(hdroffset == dataoffset);
    sqlite3VdbeSerialGet(key + dataoffset, type, &m);
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
    int rc;
    int fld;
    char *str;

    hdroffset = sqlite3GetVarint32(row, &hdrsz);
    dataoffset = hdrsz;
    fld = 0;
    while (hdroffset < hdrsz) {
        hdroffset += sqlite3GetVarint32(row + hdroffset, &type);
        prev_dataoffset = dataoffset;
        dataoffset += sqlite3VdbeSerialGet(row + prev_dataoffset, type, &m);

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
            sqlite3VdbeSerialPut(row + prev_dataoffset, &m, type);

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
}

int fdb_add_remote_time(BtCursor *pCur, unsigned long long start,
                        unsigned long long end)
{
    struct sql_thread *thd = pCur->thd;
    fdbtimings_t *timings = &thd->sqlclntstate->osql.fdbtimes;
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

#include <thread_malloc.h>
void stat4dump(int more, char *table, int istrace)
{
    int rc;
    sqlite3 *db = NULL;

    thread_memcreate_notrace(128 * 1024);
    sql_mem_init(NULL);

    struct sqlclntstate clnt;
    reset_clnt(&clnt, NULL, 1);
    clnt.sql = "select 1"; //* from sqlite_master limit 1;";

    struct sql_thread *thd = start_sql_thread();
    get_copy_rootpages(thd);
    thd->sqlclntstate = &clnt;
    sql_get_query_id(thd);
    if ((rc = get_curtran(thedb->bdb_env, &clnt)) != 0) {
        goto out;
    }
    if ((rc = sqlite3_open_serial("db", &db, 0)) != SQLITE_OK) {
        goto put;
    }
    clnt.no_transaction = 1;
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
                    Mem m;
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
    sqlite3_close(db);
put:
    put_curtran(thedb->bdb_env, &clnt);
out:
    thd->sqlclntstate = NULL;
    done_sql_thread();
    sql_mem_shutdown(NULL);
    thread_memdestroy();
}

void clone_temp_table(sqlite3 *dest, const sqlite3 *src, const char *sql,
                      int rootpg)
{
    int rc;
    char *err = NULL;
    // aDb[0]: sqlite_master
    // aDb[1]: sqlite_temp_master
    Btree *s = &src->aDb[1].pBt[0];
    comdb2_use_tmptbl_lk(1);
    tmptbl_kludge = &s->temp_tables[rootpg];
    dest->force_sqlite_impl = 1;
    if ((rc = sqlite3_exec(dest, sql, NULL, NULL, &err)) != 0) {
        logmsg(LOGMSG_ERROR, "%s rc:%d err:%s sql:%s\n", __func__, rc, err, sql);
        abort();
    }
    dest->force_sqlite_impl = 0;
    comdb2_use_tmptbl_lk(0);
    tmptbl_kludge = NULL;
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
    trans_commit(&iq, metatran, gbl_mynode);

    trans_start(&iq, NULL, &tran);
    bdb_lock_table_write(bdb_state, tran);
    logmsg(LOGMSG_WARN, "Building bthash for table %s, size %dkb per stripe\n", db->dbname,
           szkb);
    bdb_handle_dbp_add_hash(bdb_state, szkb);
    trans_commit(&iq, tran, gbl_mynode);

    // scdone log
    rc = bdb_llog_scdone(bdb_state, bthash, 1, &bdberr);
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
    trans_commit(&iq, metatran, gbl_mynode);

    trans_start(&iq, NULL, &tran);
    bdb_lock_table_write(bdb_state, tran);
    logmsg(LOGMSG_WARN, "Deleting bthash for table %s\n", db->dbname);
    bdb_handle_dbp_drop_hash(bdb_state);
    trans_commit(&iq, tran, gbl_mynode);

    // scdone log
    rc = bdb_llog_scdone(bdb_state, bthash, 1, &bdberr);
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
        logmsg(LOGMSG_USER, "bt_hash: DISABLED\n", bthashsz);
    }
    return 0;
}

int stat_bt_hash_table_reset(char *table)
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
    unsigned long long ret;

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
        clnt = thd->sqlclntstate;
    }
    osqlstate_t *osql = &clnt->osql;
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
    assert(f->blob_index < maxblobs);
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
    i64 ival;
    double dval;
    int outdtsz = 0;
    int rc = 0;
    Vdbe *vdbe = NULL;
    struct field *f = &(sc->member[fnum]);
    uint8_t *in_orig;

    in_orig = in = in + f->offset;

    if (f->flags & INDEX_DESCEND) {
        if (gbl_sort_nulls_correctly) {
            in_orig[0] = ~in_orig[0];
        }
        if (flip_orig) {
            xorbufcpy(&in[1], &in[1], f->len - 1);
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
                xorbufcpy(&p[1], &in[1], f->len - 1);
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

    case SERVER_BREAL:
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

    case SERVER_BCSTR:
        /* point directly at the ondisk string */
        m->z = &in[1]; /* skip header byte in front */
        if (flip_orig || !(f->flags & INDEX_DESCEND)) {
            m->n = cstrlenlim(&in[1], f->len - 1);
        } else {
            m->n = cstrlenlimflipped(&in[1], f->len - 1);
        }
        m->flags = MEM_Str | MEM_Ephem;
        break;

    case SERVER_BYTEARRAY:
        /* just point to bytearray directly */
        m->z = &in[1];
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
            m->z = &in[1];
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
            m->z = &in[1];
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
            m->z = &in[5];

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
            m->z = &in[5];

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
        logmsg(LOGMSG_ERROR, "unhandled type %d\n", f->type);
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

struct schema_mem {
    struct schema *sc;
    Mem *min;
    Mem *mout;
};

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
                rc = sqlite3_bind_interval(stmt, i + 1, m[i].du.tv);
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
    bind_stmt_mem(psm->sc, stmt, psm->min);
}

void verify_indexes_column_value(sqlite3_stmt *stmt, void *sm)
{
    struct schema_mem *psm = (struct schema_mem *)sm;
    if (psm->mout)
        sqlite3VdbeMemCopy(psm->mout,
                           (const Mem *)sqlite3_column_value(stmt, 0));
}

static int run_verify_indexes_query(char *sql, struct schema *sc, Mem *min,
                                     Mem *mout, int *exist)
{
    struct sqlclntstate clnt;
    struct schema_mem sm;
    int rc;

    sm.sc = sc;
    sm.min = min;
    sm.mout = mout;

    reset_clnt(&clnt, NULL, 1);
    pthread_mutex_init(&clnt.wait_mutex, NULL);
    pthread_cond_init(&clnt.wait_cond, NULL);
    pthread_mutex_init(&clnt.write_lock, NULL);
    pthread_mutex_init(&clnt.dtran_mtx, NULL);
    clnt.dbtran.mode = TRANLEVEL_SOSQL;
    clnt.high_availability = 0;
    clnt.sql = sql;
    clnt.verify_indexes = 1;
    clnt.schema_mems = &sm;

    rc = dispatch_sql_query(&clnt);

    if (clnt.has_sqliterow)
        *exist = 1;

    clnt_reset_cursor_hints(&clnt);
    osql_clean_sqlclntstate(&clnt);

    if (clnt.dbglog) {
        sbuf2close(clnt.dbglog);
        clnt.dbglog = NULL;
    }

    /* XXX free logical tran?  */

    clnt.dbtran.mode = TRANLEVEL_INVALID;
    if (clnt.query_stats)
        free(clnt.query_stats);

    pthread_mutex_destroy(&clnt.wait_mutex);
    pthread_cond_destroy(&clnt.wait_cond);
    pthread_mutex_destroy(&clnt.write_lock);
    pthread_mutex_destroy(&clnt.dtran_mtx);

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
        logmsg(LOGMSG_FATAL, "No .ONDISK tag for table %s.\n", db->dbname);
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

    len = strlen(db->dbname);
    len = crc32c(db->dbname, len);
    snprintf(temp_newdb_name, MAXTABLELEN, "sc_alter_temp_%X", len);

    for (ixnum = 0; ixnum < db->nix; ixnum++) {
        if (db->ixschema[ixnum]->where == NULL)
            dirty_keys |= (1ULL << ixnum);
        else {
            int exist = 0;
            strbuf_clear(sql);
            strbuf_append(sql, "WITH ");
            strbuf_append(sql, "\"");
            strbuf_append(sql, is_alter ? temp_newdb_name : db->dbname);
            strbuf_append(sql, "\"");
            strbuf_append(sql, "(");
            strbuf_append(sql, sc->member[0].name);
            for (i = 1; i < sc->nmembers; i++) {
                strbuf_append(sql, ", ");
                strbuf_append(sql, sc->member[i].name);
            }
            strbuf_append(sql, ") AS (SELECT @");
            strbuf_append(sql, sc->member[0].name);
            for (i = 1; i < sc->nmembers; i++) {
                strbuf_append(sql, ", @");
                strbuf_append(sql, sc->member[i].name);
            }
            strbuf_append(sql, ") SELECT 1 FROM ");
            strbuf_append(sql, "\"");
            strbuf_append(sql, is_alter ? temp_newdb_name : db->dbname);
            strbuf_append(sql, "\" ");
            strbuf_append(sql, db->ixschema[ixnum]->where);
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
    strbuf_append(sql, "WITH ");
    strbuf_append(sql, "\"");
    strbuf_append(sql, tblname);
    strbuf_append(sql, "\"");
    strbuf_append(sql, "(");
    strbuf_append(sql, sc->member[0].name);
    for (i = 1; i < sc->nmembers; i++) {
        strbuf_append(sql, ", ");
        strbuf_append(sql, sc->member[i].name);
    }
    strbuf_append(sql, ") AS (SELECT @");
    strbuf_append(sql, sc->member[0].name);
    for (i = 1; i < sc->nmembers; i++) {
        strbuf_append(sql, ", @");
        strbuf_append(sql, sc->member[i].name);
    }
    strbuf_append(sql, ") SELECT (");
    strbuf_append(sql, expr);
    strbuf_append(sql, ") FROM ");
    strbuf_append(sql, "\"");
    strbuf_append(sql, tblname);
    strbuf_append(sql, "\"");
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
    Mem mout;
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
    memset(&mout, 0, sizeof(Mem));

    m = (Mem *)malloc(sizeof(Mem) * MAXCOLUMNS);
    if (m == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed to malloc Mem\n", __func__);
        rc = -1;
        goto done;
    }

    for (i = 0; i < sc->nmembers; i++) {
        memset(&m[i], 0, sizeof(Mem));
        rc = get_data_from_ondisk(sc, (char *)inbuf, blobs, maxblobs, i, &m[i],
                                  0, tzname);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to convert to ondisk\n", __func__);
            goto done;
        }
    }

    build_indexes_expressions_query(sql, sc, "expridx_temp", f->name);

    rc = run_verify_indexes_query((char *)strbuf_buf(sql), sc, m, &mout, &exist);
    if (rc || !exist) {
        logmsg(LOGMSG_ERROR, "%s: failed to run internal query, rc %d\n", __func__,
                rc);
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
        logmsg(LOGMSG_ERROR, "%s: rc %d failed to form index \"%s\", result flag "
                        "%x, index type %d\n",
                __func__, rc, f->name, mout.flags, f->type);
        goto done;
    }
    sqlite3VdbeMemRelease(&mout);
done:
    if (m)
        free(m);
    strbuf_free(sql);
    if (rc)
        return -1;
    return 0;
}

uint16_t stmt_num_tbls(sqlite3_stmt *stmt)
{
    Vdbe *v = (Vdbe *)stmt;
    return v->numTables;
}
