/*
   Copyright 2026 Bloomberg Finance L.P.

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

/* Both halves of client info on the replication stream: the master puts the
 * client behind a transaction in the log, and a replicant applying it publishes
 * what it is working on for comdb2_replication (driven from rep_record.c). */

#include <pthread.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include <build/db.h>

#include "bdb_int.h"

#include "llog_auto.h"
#include "llog_ext.h"
#include "llog_handlers.h"

#include "logmsg.h"
#include "sys_wrap.h"
#include "comdb2_atomic.h"
#include "list.h"

/* Keep off until the whole cluster is upgraded, and turn off before downgrading
 * any node: an older binary aborts on the unknown rectype. */
int gbl_log_clientinfo = 0;

/* Log the client into the block processor's txn, once per transaction. */
int bdb_llog_clientinfo_tran(bdb_state_type *bdb_state, tran_type *tran, const char *taskname, const char *host,
                             int pid, int *bdberr)
{
    DBT dtask = {0}, dhost = {0};
    DB_LSN lsn;
    int rc;

    *bdberr = BDBERR_NOERROR;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    /* Sized with the NUL so the replicant can use them as cstrings directly. */
    dtask.data = (void *)(taskname ? taskname : "");
    dtask.size = strlen((char *)dtask.data) + 1;
    dhost.data = (void *)(host ? host : "");
    dhost.size = strlen((char *)dhost.data) + 1;

    rc = llog_clientinfo_log(bdb_state->dbenv, tran->tid, &lsn, 0, pid, &dtask, &dhost);
    if (rc) {
        *bdberr = BDBERR_MISC;
        return -1;
    }
    return 0;
}

/* What a replication thread publishes while it applies. Strings are inline and
 * bounded so the systable can copy them without a length. */
struct bdb_clientinfo {
    char taskname[BDB_CLIENTINFO_STRSZ];
    char host[BDB_CLIENTINFO_STRSZ];
    int pid;
};

/* Truncation is fine here -- this is a diagnostic label, not an identifier. */
static void clientinfo_copy_str(char *dst, const DBT *src)
{
    size_t n = src->size;

    if (n == 0) {
        dst[0] = '\0';
        return;
    }
    if (n > BDB_CLIENTINFO_STRSZ)
        n = BDB_CLIENTINFO_STRSZ;
    memcpy(dst, src->data, n);
    dst[BDB_CLIENTINFO_STRSZ - 1] = '\0';
    dst[n - 1] = '\0';
}

/* Returns a malloc'd opaque handle, or NULL. berkdb holds it as a void * so it
 * never has to know this layout; free with bdb_clientinfo_free(). */
void *bdb_clientinfo_from_logrec(DB_ENV *dbenv, void *logrec)
{
    llog_clientinfo_args *argp = NULL;
    struct bdb_clientinfo *ci;

    if (llog_clientinfo_read(dbenv, logrec, &argp) != 0)
        return NULL;

    if ((ci = calloc(1, sizeof(*ci))) != NULL) {
        clientinfo_copy_str(ci->taskname, &argp->taskname);
        clientinfo_copy_str(ci->host, &argp->host);
        ci->pid = argp->pid;
    }

    free(argp);
    return ci;
}

void bdb_clientinfo_free(void *ci)
{
    free(ci);
}

/*
 * One record per replication thread that has applied, owned by the thread and
 * linked into a registry. The thread updates its own record without a lock;
 * the mutex covers only link, unlink and the systable walk, so a record can
 * never be freed while it is being read. 'active' gates the row.
 */
struct rep_thread_rec {
    LINKC_T(struct rep_thread_rec) lnk;
    int active;
    uint64_t tid;
    struct bdb_clientinfo ci;
    int have_clientinfo;
    uint8_t fingerprint[BDB_FINGERPRINTSZ];
    int have_fingerprint;
    uint32_t lsn_file;
    uint32_t lsn_offset;
};

static LISTC_T(struct rep_thread_rec) rep_thread_recs;
static pthread_mutex_t rep_thread_recs_lk = PTHREAD_MUTEX_INITIALIZER;
static pthread_key_t rep_thread_rec_key;
static pthread_once_t rep_thread_rec_once = PTHREAD_ONCE_INIT;

/* Thread exit. The pools retire idle workers all the time, so a record must
 * not outlive its thread. */
static void rep_thread_rec_release(void *p)
{
    struct rep_thread_rec *r = p;

    Pthread_mutex_lock(&rep_thread_recs_lk);
    listc_rfl(&rep_thread_recs, r);
    Pthread_mutex_unlock(&rep_thread_recs_lk);
    free(r);
}

static void rep_thread_rec_init(void)
{
    listc_init(&rep_thread_recs, offsetof(struct rep_thread_rec, lnk));
    Pthread_key_create(&rep_thread_rec_key, rep_thread_rec_release);
}

/* Whatever this thread already has; never registers, so a thread that is not
 * applying cannot register on a stray fingerprint or end. */
static struct rep_thread_rec *rep_thread_rec_current(void)
{
    pthread_once(&rep_thread_rec_once, rep_thread_rec_init);
    return pthread_getspecific(rep_thread_rec_key);
}

static struct rep_thread_rec *rep_thread_rec_register(void)
{
    struct rep_thread_rec *r;

    if ((r = rep_thread_rec_current()) != NULL)
        return r;
    if ((r = calloc(1, sizeof(*r))) == NULL)
        return NULL;
    r->tid = (uint64_t)(intptr_t)pthread_self();
    Pthread_setspecific(rep_thread_rec_key, r);

    Pthread_mutex_lock(&rep_thread_recs_lk);
    listc_abl(&rep_thread_recs, r);
    Pthread_mutex_unlock(&rep_thread_recs_lk);
    return r;
}

/* Publish the row. Unconditional, so a thread shows up whether or not the txn
 * named a client; the client and fingerprint are attached as they turn up. */
void bdb_replication_thread_begin(uint32_t lsn_file, uint32_t lsn_offset)
{
    struct rep_thread_rec *r = rep_thread_rec_register();

    if (r == NULL)
        return;

    r->have_clientinfo = 0;
    r->have_fingerprint = 0;
    r->lsn_file = lsn_file;
    r->lsn_offset = lsn_offset;
    XCHANGE32(r->active, 1); /* last: publishes the fields above */
}

/* Who this txn belongs to, once its record turns up. NULL is a no-op. */
void bdb_replication_thread_client(void *ci)
{
    struct rep_thread_rec *r = rep_thread_rec_current();

    if (r == NULL || ci == NULL)
        return;
    r->ci = *(struct bdb_clientinfo *)ci;
    r->have_clientinfo = 1;
}

/* Which statement this thread is on now; the client stays put underneath. */
void bdb_replication_thread_fingerprint(const uint8_t *fingerprint)
{
    struct rep_thread_rec *r = rep_thread_rec_current();

    if (r == NULL)
        return;
    memcpy(r->fingerprint, fingerprint, sizeof(r->fingerprint));
    r->have_fingerprint = 1;
}

void bdb_replication_thread_end(void)
{
    struct rep_thread_rec *r = rep_thread_rec_current();

    if (r == NULL)
        return;
    XCHANGE32(r->active, 0);
}

/* Walks every active record under the registry lock, which only keeps records
 * from being freed underneath it. Unordered against the applying thread, so a
 * row can catch one mid-update -- benign, the table is diagnostic. */
void bdb_replication_foreach(bdb_replication_enum_fn fn, void *arg)
{
    struct rep_thread_rec *r;

    pthread_once(&rep_thread_rec_once, rep_thread_rec_init);
    Pthread_mutex_lock(&rep_thread_recs_lk);
    LISTC_FOR_EACH(&rep_thread_recs, r, lnk)
    {
        if (!ATOMIC_LOAD32(r->active))
            continue;
        fn(arg, r->tid, r->have_clientinfo ? r->ci.taskname : NULL, r->have_clientinfo ? r->ci.host : NULL,
           r->have_clientinfo ? r->ci.pid : 0, r->have_fingerprint ? r->fingerprint : NULL, r->lsn_file, r->lsn_offset);
    }
    Pthread_mutex_unlock(&rep_thread_recs_lk);
}

/* No physical state to change, so every op is a no-op beyond prev_lsn. The
 * record is read in rep_record.c, before parallel rep reorders the records. */
int handle_clientinfo(DB_ENV *dbenv, u_int32_t rectype, llog_clientinfo_args *ciop, DB_LSN *lsn, db_recops op)
{
    switch (op) {
    /* for an UNDO record, berkeley expects us to set prev_lsn */
    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        *lsn = ciop->prev_lsn;
        break;

    case DB_TXN_APPLY:
    case DB_TXN_SNAPISOL:
        break;

    case DB_TXN_PRINT:
        printf("[%lu][%lu]clientinfo: rec: %lu txnid %lx prevlsn[%lu][%lu]\n", (u_long)lsn->file, (u_long)lsn->offset,
               (u_long)rectype, (u_long)ciop->txnid->txnid, (u_long)ciop->prev_lsn.file, (u_long)ciop->prev_lsn.offset);
        /* Sizes count the trailing NUL; keep it out of the output. */
        printf("\ttask: %.*s host: %.*s pid: %d\n", ciop->taskname.size ? (int)ciop->taskname.size - 1 : 0,
               (char *)ciop->taskname.data, ciop->host.size ? (int)ciop->host.size - 1 : 0, (char *)ciop->host.data,
               ciop->pid);
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_clientinfo\n", (int)op);
        break;
    }
    return 0;
}
