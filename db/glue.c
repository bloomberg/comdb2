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

/* glue front end to db engine */

/* any transactional code needs to return RETRY rcode to upper levels.
   This is because the transaction needs to abort, and start over again.
   non-transactional can retry within glue code.
*/
#include "limit_fortify.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <alloca.h>
#include <stdarg.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <inttypes.h>

#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <poll.h>
#include <unistd.h>
#include <gettimeofday_ms.h>

#include <ctrace.h>
#include <epochlib.h>
#include <plbitlib.h>
#include <str0.h>
#include <pthread.h>
#include <netinet/in.h>
#include <db.h>
#include <portmuxapi.h>
#include <bb_oscompat.h>

#include <list.h>
#include <memory_sync.h>

#include "comdb2.h"
#include "translistener.h"
#include "prefault.h"
#include "util.h"

#include "sql.h"
#include <sbuf2.h>
#include <bdb_api.h>
#include <bdb_cursor.h>
#include <bdb_fetch.h>
#include <bdb_queue.h>

#include <net.h>
#include <net_types.h>

#include "remote.h"

#include <cdb2api.h>

#include "comdb2_shm.h"

#include <dlmalloc.h>

#include "sqloffload.h"
#include "osqlcomm.h"

#include <flibc.h>
#include <cdb2_constants.h>
#include <autoanalyze.h>
#include "comdb2util.h"
#include <schemachange/sc_global.h>

#include "rtcpu.h"

#include <alloca.h>
#include <intern_strings.h>
#include "debug_switches.h"
#include <machine.h>
#include <trigger.h>

#include "views.h"
#include <sc_callbacks.h>

#include "views.h"
#include "logmsg.h"

/* ixrc != -1 is incorrect. Could be IX_PASTEOF or IX_EMPTY.
 * Don't want to vtag those results
 *
 * Ha! Dont need IX_EMPTY but do need IX_NOTFND and IX_PASTEOF.
 * Just use is_good_ix_find_rc() */
#define VTAG(rc, db)                                                           \
    if (is_good_ix_find_rc((rc)))                                              \
    vtag_to_ondisk((db), fnddta, fndlen, args.ver, *genid)
#define VTAG_GENID(rc, db)                                                     \
    if (is_good_ix_find_rc((rc)))                                              \
    vtag_to_ondisk((db), fnddta, fndlen, args.ver, genid)
#define VTAG_PTR(rc, db)                                                       \
    if (is_good_ix_find_rc((rc)))                                              \
    vtag_to_ondisk((db), fnddta, fndlen, args->ver, *genid)
#define VTAG_PTR_GENID(rc, db)                                                 \
    if (is_good_ix_find_rc((rc)))                                              \
    vtag_to_ondisk((db), fnddta, fndlen, args->ver, genid)

extern int verbose_deadlocks;

struct net_new_queue_msg {
    bbuint32_t reserved;
    bbuint32_t avgitemsz;
    char name[MAXTABLELEN + 1];
};

struct net_add_consumer_msg {
    bbuint32_t reserved;
    bbuint32_t consumern;
    char name[MAXTABLELEN + 1];
    char method[128];
};

struct new_procedure_op_msg {
    bbuint32_t reserved;
    bbuint32_t op;
    bbuint32_t namelen;
    bbuint32_t jarfilelen;
    bbuint32_t paramlen;
    char text[1];
};

struct net_morestripe_msg {
    int32_t reserved0;
    int32_t reserved1;
    int32_t newdtastripe;
    int32_t newblobstripe;
};

extern struct dbenv *thedb;
extern int gbl_lost_master_time;
extern int gbl_check_access_controls;

static void fix_blobstripe_genids(void);

static int meta_put(struct dbtable *db, void *input_tran, struct metahdr *hdr,
                    void *data, int dtalen);
static int meta_get(struct dbtable *db, struct metahdr *key, void *dta, int dtalen);
static int meta_get_tran(tran_type *tran, struct dbtable *db, struct metahdr *key1,
                         void *dta, int dtalen);
static int meta_get_var(struct dbtable *db, struct metahdr *key, void **dta,
                        int *fndlen);
static int meta_get_var_tran(tran_type *tran, struct dbtable *db,
                             struct metahdr *key1, void **dta, int *fndlen);
static int put_meta_int(const char *table, void *tran, int rrn, int key,
                        int value);
static int get_meta_int(const char *table, int rrn, int key);
static int get_meta_int_tran(tran_type *tran, const char *table, int rrn,
                             int key);
static int ix_find_check_blob_race(struct ireq *iq, char *inbuf, int numblobs,
                                   int *blobnums, void **blobptrs);

/* How many times we became, or ceased to be, master node. */
int gbl_master_changes = 0;

void *get_bdb_handle(struct dbtable *db, int auxdb)
{
    void *bdb_handle;

    switch (auxdb) {
    case AUXDB_NONE:
        bdb_handle = db->handle;
        break;
    case AUXDB_META:
        if (!db->meta && db->dbenv->meta)
            bdb_handle = db->dbenv->meta;
        else
            bdb_handle = db->meta;
        break;
    default:
        logmsg(LOGMSG_ERROR, "get_bdb_handle: bad auxdb=%d\n", auxdb);
        bdb_handle = NULL;
    }

    return bdb_handle;
}

static void *get_bdb_handle_ireq(struct ireq *iq, int auxdb)
{
    void *bdb_handle = NULL;

    if (iq->usedb) {
        if (auxdb == AUXDB_NONE)
            reqlog_usetable(iq->reqlogger, iq->usedb->dbname);
        return get_bdb_handle(iq->usedb, auxdb);
    }

    switch (auxdb) {
    default:
        logmsg(LOGMSG_ERROR, "get_bdb_handle_ireq: bad auxdb=%d\n", auxdb);
        bdb_handle = NULL;
    }

    return bdb_handle;
}

static void *bdb_handle_from_ireq(const struct ireq *iq)
{
    struct dbtable *db = iq->usedb;
    if (db)
        return db->handle;
    else if (iq->use_handle)
        return iq->use_handle;
    else {
        logmsg(LOGMSG_FATAL, "bdb_handle_from_ireq: ireq has no bdb handle\n");
        abort();
        return NULL;
    }
}

static struct dbenv *dbenv_from_ireq(const struct ireq *iq)
{
    struct dbtable *db = iq->usedb;
    if (db)
        return db->dbenv;
    else
        return iq->dbenv;
}

void init_fake_ireq_auxdb(struct dbenv *dbenv, struct ireq *iq, int auxdb)
{
    memset(iq, 0, sizeof(struct ireq));
    iq->transflags = 0;
    iq->is_fake = 1;
    iq->dbenv = dbenv;
    iq->use_handle = get_bdb_handle_ireq(iq, auxdb);
}

void init_fake_ireq(struct dbenv *dbenv, struct ireq *iq)
{
    /* region 1 */
    const size_t len1 = offsetof(struct ireq, region2);
    bzero(iq, len1);

    /* region 2 */
    iq->corigin[0] = '\0';
    iq->debug_buf[0] = '\0';
    iq->tzname[0] = '\0';
    iq->sqlhistory[0] = '\0';

    /* region 3 */
    const size_t len3 = sizeof(*iq) - offsetof(struct ireq, region3);
    bzero(&iq->region3, len3);

    /* Make it fake */
    iq->dbenv = dbenv;
    iq->is_fake = 1;
    iq->helper_thread = -1;
}

int set_tran_lowpri(struct ireq *iq, tran_type *tran)
{
    void *bdb_handle = bdb_handle_from_ireq(iq);
    return bdb_set_tran_lowpri(bdb_handle, tran);
}

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
/*        TRANSACTIONAL STUFF        */
/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
static int trans_start_int_int(struct ireq *iq, tran_type *parent_trans,
                               tran_type **out_trans, int logical, int sc,
                               int retries)
{
    int bdberr;
    void *bdb_handle = bdb_handle_from_ireq(iq);
    struct dbenv *dbenv = dbenv_from_ireq(iq);
    int rc = 0;
    tran_type *physical_tran = NULL;
    iq->gluewhere = "bdb_tran_begin";

    if (!logical) {
        /*
        if (retries)
           fprintf(stderr, "bdb_tran_begin_set_retries(%d)\n", retries);
        */

        *out_trans = bdb_tran_begin_set_retries(bdb_handle, parent_trans,
                                                retries, &bdberr);
    } else {
        *out_trans = bdb_tran_begin_logical(bdb_handle, 0, &bdberr);
        if (iq->tranddl && sc && *out_trans) {
            bdb_ltran_get_schema_lock(*out_trans);
            int get_physical_transaction(bdb_state_type * bdb_state,
                                         tran_type * logical_tran,
                                         tran_type * *outtran);
            rc = get_physical_transaction(bdb_handle, *out_trans,
                                          &physical_tran);
            if (rc == BDBERR_READONLY) {
                trans_abort_logical(iq, *out_trans, NULL, 0, NULL, 0);
                *out_trans = NULL;
                bdberr = rc;
            }
            if (rc) {
                logmsg(LOGMSG_FATAL, "%s :failed to get physical_tran\n",
                       __func__);
                abort();
            }
        }
    }

    iq->gluewhere = "bdb_tran_begin done";
    if (*out_trans == 0) {
        /* dbenv->master can change between calling
         * bdb_tran_begin and checking it here - in fact, we may get
         * upgraded to master in between!  ERR_NOMASTER will make the
         * proxy retry next second, so that is the simplest fix here.
         * Once we're inside a transaction we hold the bdb read lock
         * until we've committed or aborted so no need to worry about this
         * later on. */
        if (bdberr == BDBERR_READONLY /*&& dbenv->master!=gbl_mynode*/) {
            /* return NOMASTER so client retries. */
            return ERR_NOMASTER;
        }
        logmsg(LOGMSG_ERROR, "*ERROR* trans_start:failed err %d\n", bdberr);
        return ERR_INTERNAL;
    }
    return 0;
}

int trans_start_int(struct ireq *iq, void *parent_trans, tran_type **out_trans,
                    int logical, int retries)
{
    return trans_start_int_int(iq, parent_trans, out_trans, logical, 0,
                               retries);
}

int trans_start_logical_sc(struct ireq *iq, tran_type **out_trans)
{
    iq->use_handle = thedb->bdb_env;
    return trans_start_int_int(iq, NULL, out_trans, 1, 1, 0);
}

int trans_start_logical(struct ireq *iq, tran_type **out_trans)
{
    return trans_start_int(iq, NULL, out_trans, 1, 0);
}

int rowlocks_check_commit_physical(bdb_state_type *bdb_state, tran_type *tran,
                                   int blockop_count)
{
    return bdb_rowlocks_check_commit_physical(bdb_state, tran, blockop_count);
}

int is_rowlocks_transaction(tran_type *tran)
{
    return bdb_is_rowlocks_transaction(tran);
}

int trans_start(struct ireq *iq, tran_type *parent_trans, tran_type **out_trans)
{
    if (gbl_rowlocks)
        return trans_start_logical(iq, out_trans);
    else
        return trans_start_int(iq, parent_trans, out_trans, 0, 0);
}

int trans_start_sc(struct ireq *iq, tran_type *parent_trans,
                   tran_type **out_trans)
{
    return trans_start_int(iq, parent_trans, out_trans, 0, 0);
}

int trans_start_set_retries(struct ireq *iq, tran_type *parent_trans,
                            tran_type **out_trans, int retries)
{
    int rc = 0;

    if (gbl_rowlocks)
        rc = trans_start_logical(iq, out_trans);
    else
        rc = trans_start_int(iq, parent_trans, out_trans, 0, retries);

    if (verbose_deadlocks && retries != 0)
        logmsg(LOGMSG_USER, "%s ptran %p tran %p with retries %d\n", __func__,
                parent_trans, *out_trans, retries);

    return rc;
}

tran_type *trans_start_socksql(struct ireq *iq, int trak)
{
    void *bdb_handle = bdb_handle_from_ireq(iq);
    tran_type *out_trans = NULL;
    int bdberr = 0;

    iq->gluewhere = "bdb_tran_begin_socksql";
    if (gbl_extended_sql_debug_trace) {
        logmsg(LOGMSG_USER, "%s called\n", __func__);
    }
    out_trans = bdb_tran_begin_socksql(bdb_handle, trak, &bdberr);
    iq->gluewhere = "bdb_tran_begin_socksql done";

    if (out_trans == NULL) {
        logmsg(LOGMSG_ERROR, "*ERROR* %s:failed err %d\n", __func__, bdberr);
        return NULL;
    }
    return out_trans;
}

tran_type *trans_start_readcommitted(struct ireq *iq, int trak)
{
    void *bdb_handle = bdb_handle_from_ireq(iq);
    tran_type *out_trans = NULL;
    int bdberr = 0;

    iq->gluewhere = "bdb_tran_begin_readcommitted";
    out_trans = bdb_tran_begin_readcommitted(bdb_handle, trak, &bdberr);
    iq->gluewhere = "bdb_tran_begin_readcommitted done";

    if (out_trans == NULL) {
        logmsg(LOGMSG_ERROR, "*ERROR* %s:failed err %d\n", __func__, bdberr);
        return NULL;
    }
    return out_trans;
}

tran_type *trans_start_snapisol(struct ireq *iq, int trak, int epoch, int file,
                                int offset, int *error)
{
    void *bdb_handle = bdb_handle_from_ireq(iq);
    tran_type *out_trans = NULL;

    *error = 0;

    iq->gluewhere = "bdb_tran_begin_snapisol";

    if (gbl_extended_sql_debug_trace) {
        logmsg(LOGMSG_USER, "%s called with epoch=%d file=%d offset=%d\n", __func__,
                epoch, file, offset);
    }
    out_trans =
        bdb_tran_begin_snapisol(bdb_handle, trak, error, epoch, file, offset);
    iq->gluewhere = "bdb_tran_begin_snapisol done";

    if (out_trans == NULL) {
        logmsg(LOGMSG_ERROR, "*ERROR* %s:failed err %d\n", __func__, *error);
        return NULL;
    }

    return out_trans;
}

tran_type *trans_start_serializable(struct ireq *iq, int trak, int epoch, int file,
        int offset, int *error)
{
    void *bdb_handle = bdb_handle_from_ireq(iq);
    tran_type *out_trans = NULL;
    int bdberr = 0;

    iq->gluewhere = "bdb_tran_begin";

    if (gbl_extended_sql_debug_trace) {
        logmsg(LOGMSG_USER, "%s called with epoch=%d file=%d offset=%d\n", __func__,
                epoch, file, offset);
    }
    out_trans = bdb_tran_begin_serializable(bdb_handle, trak, &bdberr, epoch, 
            file, offset);
    iq->gluewhere = "bdb_tran_begin done";

    if (out_trans == NULL) {
        logmsg(LOGMSG_ERROR, "*ERROR* %s:failed err %d\n", __func__, bdberr);
        *error = bdberr;
        return NULL;
    }
    return out_trans;
}

/**
 * Shadow transaction have no berkdb txn and executes (most of the time)
 * on replicants;
 * There is nothing to wait after.
 * I also prefer to pass the bdberr on the higher levels
 *
 */
int trans_commit_shadow(void *trans, int *bdberr)
{
    int rc = 0;
    ;

    *bdberr = 0;
    rc = bdb_tran_commit(thedb->bdb_env, trans, bdberr);

    return rc;
}

/**
 * Shadow transaction have no berkdb txn and executes (most of the time)
 * on replicants;
 * There is nothing to wait after.
 * I also prefer to pass the bdberr on the higher levels
 *
 */
int trans_abort_shadow(void **trans, int *bdberr)
{
    int rc = 0;

    if (*trans == NULL)
        return rc;

    *bdberr = 0;

    rc = bdb_tran_abort(thedb->bdb_env, *trans, bdberr);

    *trans = NULL;

    return rc;
}

static int trans_commit_seqnum_int(void *bdb_handle, struct dbenv *dbenv,
                                   struct ireq *iq, void *trans,
                                   db_seqnum_type *seqnum, int logical,
                                   void *blkseq, int blklen, void *blkkey,
                                   int blkkeylen)
{
    int bdberr;
    iq->gluewhere = "bdb_tran_commit_with_seqnum_size";
    if (!logical)
        bdb_tran_commit_with_seqnum_size(
            bdb_handle, trans, (seqnum_type *)seqnum, &iq->txnsize, &bdberr);
    else {
        bdb_tran_commit_logical_with_seqnum_size(
            bdb_handle, trans, blkseq, blklen, blkkey, blkkeylen,
            (seqnum_type *)seqnum, &iq->txnsize, &bdberr);
    }
    iq->gluewhere = "bdb_tran_commit_with_seqnum_size done";
    if (bdberr != 0) {
        if (bdberr == BDBERR_DEADLOCK)
            return RC_INTERNAL_RETRY;

        if (bdberr == BDBERR_READONLY) {
            /* I was downgraded in the middle..
               return NOMASTER so client retries. */
            return ERR_NOMASTER;
        } else if (logical && bdberr == BDBERR_ADD_DUPE) {
            /*
               bdb_tran_commit_logical_with_seqnum_size takes care of aborting
               the transaction.  I hate this.  We've operated under the
               assumption that commits never fail.  Unfortunately
               with logical transactions we don't have nesting, so we can't
               have a parent that commits and a child that aborts.  It's
               ugly, but we need to live with it.  Ideas?  I am all ears.
            */
            return IX_DUP;
        }
        logmsg(LOGMSG_ERROR, "*ERROR* trans_commit:failed err %d\n", bdberr);
        return ERR_INTERNAL;
    }
    return 0;
}

int trans_commit_seqnum(struct ireq *iq, void *trans, db_seqnum_type *seqnum)
{
    void *bdb_handle = bdb_handle_from_ireq(iq);
    struct dbenv *dbenv = dbenv_from_ireq(iq);
    return trans_commit_seqnum_int(bdb_handle, dbenv, iq, trans, seqnum, 0,
                                   NULL, 0, NULL, 0);
}

static const char *sync_to_str(int sync)
{
    switch (sync) {
    case REP_SYNC_FULL:
        return "SYNC_FULL";
        break;
    case REP_SYNC_SOURCE:
        return "SYNC_SOURCE";
        break;
    case REP_SYNC_NONE:
        return "SYNC_NONE";
        break;
    case REP_SYNC_ROOM:
        return "SYNC_ROOM";
        break;
    case REP_SYNC_N:
        return "SYNC_N";
        break;
    default:
        return "INVALID";
        break;
    }
}

static int trans_wait_for_seqnum_int(void *bdb_handle, struct dbenv *dbenv,
                                     struct ireq *iq, char *source_node,
                                     int timeoutms, int adaptive,
                                     db_seqnum_type *ss)
{
    int rc = 0;
    int sync;
    int start_ms, end_ms;

    if (iq->sc_pending) {
        sync = REP_SYNC_FULL;
        adaptive = 0;
        timeoutms = -1;
    } else {
        sync = dbenv->rep_sync;
    }

    /*wait for synchronization, if necessary */
    start_ms = time_epochms();
    switch (sync) {
    default:

        /*async mode, don't wait at all */
        break;

    case REP_SYNC_SOURCE:
        /*source machine sync, wait for source machine */
        if (source_node == gbl_mynode)
            break;
        iq->gluewhere = "bdb_wait_for_seqnum_from_node";

        if (timeoutms == -1)
            rc = bdb_wait_for_seqnum_from_node(bdb_handle, (seqnum_type *)ss,
                                               source_node);
        else
            rc = bdb_wait_for_seqnum_from_node_timeout(
                bdb_handle, (seqnum_type *)ss, source_node, timeoutms);

        iq->gluewhere = "bdb_wait_for_seqnum_from_node done";
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "*WARNING* bdb_wait_seqnum:error syncing node %s rc %d\n",
                   source_node, rc);
        }
        break;

    case REP_SYNC_FULL:
        iq->gluewhere = "bdb_wait_for_seqnum_from_all";
        if (adaptive)
            rc = bdb_wait_for_seqnum_from_all_adaptive_newcoh(
                bdb_handle, (seqnum_type *)ss, iq->txnsize, &iq->timeoutms);
        else if (timeoutms == -1)
            rc = bdb_wait_for_seqnum_from_all(bdb_handle, (seqnum_type *)ss);
        else
            rc = bdb_wait_for_seqnum_from_all_timeout(
                bdb_handle, (seqnum_type *)ss, timeoutms);
        iq->gluewhere = "bdb_wait_for_seqnum_from_all done";
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "*WARNING* bdb_wait_seqnum:error syncing all nodes rc %d\n",
                   rc);
        }
        if (iq->sc_pending) {
            /* TODO: I dont know what to do here. Schema change is already
            ** commited but one or more replicants didn't get the messages
            ** to reload table.
            */
            logmsg(LOGMSG_INFO, "Schema change scdone sync all nodes, rc %d\n",
                   rc);
            rc = 0;
        }
        break;

    case REP_SYNC_ROOM:
        iq->gluewhere = "bdb_wait_for_seqnum_from_room";
        rc = bdb_wait_for_seqnum_from_room(bdb_handle, (seqnum_type *)ss);
        iq->gluewhere = "bdb_wait_for_seqnum_from_room done";
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "*WARNING* bdb_wait_seqnum:error syncing all nodes rc %d\n",
                   rc);
        }
        break;

    case REP_SYNC_N:
        rc = bdb_wait_for_seqnum_from_n(bdb_handle, (seqnum_type *)ss,
                                        thedb->wait_for_N_nodes);
        break;
    }

    if (bdb_attr_get(dbenv->bdb_attr, BDB_ATTR_COHERENCY_LEASE)) {
        uint64_t now = gettimeofday_ms(), next_commit = next_commit_timestamp();
        if (next_commit > now)
            poll(0, 0, next_commit - now);
    }

    end_ms = time_epochms();
    iq->reptimems = end_ms - start_ms;

    return rc;
}

int trans_wait_for_seqnum(struct ireq *iq, char *source_host,
                          db_seqnum_type *ss)
{
    void *bdb_handle = bdb_handle_from_ireq(iq);
    struct dbenv *dbenv = dbenv_from_ireq(iq);
    return trans_wait_for_seqnum_int(bdb_handle, dbenv, iq, source_host, -1,
                                     0 /*adaptive*/, ss);
}

int trans_wait_for_last_seqnum(struct ireq *iq, char *source_host)
{
    db_seqnum_type seqnum;
    void *bdb_handle = bdb_handle_from_ireq(iq);
    struct dbenv *dbenv = dbenv_from_ireq(iq);

    bdb_get_myseqnum(bdb_handle, (void *)&seqnum);

    return trans_wait_for_seqnum_int(bdb_handle, dbenv, iq, source_host, -1,
                                     0 /*adaptive*/, &seqnum);
}

int trans_commit_logical_tran(void *trans, int *bdberr)
{
    uint64_t size;
    char seq[SIZEOF_SEQNUM];
    return bdb_tran_commit_logical_with_seqnum_size(
        thedb->bdb_env, trans, NULL, 0, NULL, 0, (seqnum_type *)seq, &size,
        bdberr);
}

static int trans_commit_int(struct ireq *iq, void *trans, char *source_host,
                            int timeoutms, int adaptive, int logical,
                            void *blkseq, int blklen, void *blkkey,
                            int blkkeylen)
{
    int rc, rc2;
    db_seqnum_type ss;
    void *bdb_handle = bdb_handle_from_ireq(iq);
    struct dbenv *dbenv = dbenv_from_ireq(iq);

    memset(&ss, -1, sizeof(ss));

    rc = trans_commit_seqnum_int(bdb_handle, dbenv, iq, trans, &ss, logical,
                                 blkseq, blklen, blkkey, blkkeylen);
    if (rc != 0)
        return rc;

    rc = trans_wait_for_seqnum_int(bdb_handle, dbenv, iq, source_host,
                                   timeoutms, adaptive, &ss);
    return rc;
}

int trans_commit_logical(struct ireq *iq, void *trans, char *source_host,
                         int timeoutms, int adaptive, void *blkseq, int blklen,
                         void *blkkey, int blkkeylen)
{
    return trans_commit_int(iq, trans, source_host, timeoutms, adaptive, 1,
                            blkseq, blklen, blkkey, blkkeylen);
}

/* XXX i made this be the same as trans_commit_adaptive */
int trans_commit(struct ireq *iq, void *trans, char *source_host)
{
    return trans_commit_int(iq, trans, source_host, -1, 1, 0, NULL, 0, NULL, 0);
}

int trans_commit_timeout(struct ireq *iq, void *trans, char *source_host,
                         int timeoutms)
{
    return trans_commit_int(iq, trans, source_host, timeoutms, 0, 0, NULL, 0,
                            NULL, 0);
}

int trans_commit_adaptive(struct ireq *iq, void *trans, char *source_host)
{
    return trans_commit_int(iq, trans, source_host, -1, 1, 0, NULL, 0, NULL, 0);
}

int trans_abort_logical(struct ireq *iq, void *trans, void *blkseq, int blklen,
                        void *seqkey, int seqkeylen)
{
    int bdberr, rc = 0;
    void *bdb_handle = bdb_handle_from_ireq(iq);
    struct dbenv *dbenv = dbenv_from_ireq(iq);
    db_seqnum_type ss;

    iq->gluewhere = "bdb_tran_abort";
    bdb_tran_abort_logical(bdb_handle, trans, &bdberr, blkseq, blklen, seqkey,
                           seqkeylen, (seqnum_type *)&ss);
    iq->gluewhere = "bdb_tran_abort done";

    if (bdberr != 0) {
        if (bdberr == BDBERR_ADD_DUPE)
            rc = IX_DUP;
        else if (bdberr != BDBERR_DEADLOCK)
            logmsg(LOGMSG_ERROR, "*ERROR* trans_abort_logical:failed err %d\n", bdberr);
        else
            rc = ERR_INTERNAL;
    }
    /* Not sure why we have this inconsistency where we wait for logical commits
       but
       not logical aborts, but it's wrong. */
    trans_wait_for_seqnum_int(bdb_handle, dbenv, iq, gbl_mynode,
                              -1 /* timeoutms */, 1 /* adaptive */, &ss);
    return rc;
}

int trans_abort_int(struct ireq *iq, void *trans, int *priority)
{
    int bdberr;
    void *bdb_handle = bdb_handle_from_ireq(iq);
    iq->gluewhere = "bdb_tran_abort";
    bdb_tran_abort_priority(bdb_handle, trans, &bdberr, priority);
    iq->gluewhere = "bdb_tran_abort done";
    if (bdberr != 0) {
        logmsg(LOGMSG_ERROR, "*ERROR* trans_abort:failed err %d\n", bdberr);
        return ERR_INTERNAL;
    }
    return 0;
}

int trans_abort_priority(struct ireq *iq, void *trans, int *priority)
{
    return trans_abort_int(iq, trans, priority);
}

int trans_abort(struct ireq *iq, void *trans)
{
    return trans_abort_int(iq, trans, NULL);
}

int get_context(struct ireq *iq, unsigned long long *context)
{
    struct dbtable *db = iq->usedb;
    void *bdb_handle;
    bdb_handle = get_bdb_handle(db, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    iq->gluewhere = "bdb_get_cmp_context";
    *context = bdb_get_cmp_context(bdb_handle);
    iq->gluewhere = "bdb_get_cmp_context done";
    return 0;
}

/* returns 0 if record is ok in  context,
 * 1 if it is newer than context (so don't return it)
 * else an rcode */
int cmp_context(struct ireq *iq, unsigned long long genid,
                unsigned long long context)
{
    struct dbtable *db = iq->usedb;
    void *bdb_handle;
    int rc;
    bdb_handle = get_bdb_handle(db, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    iq->gluewhere = "bdb_get_cmp_context";
    rc = bdb_check_genid_is_older(bdb_handle, genid, context);
    iq->gluewhere = "bdb_get_cmp_context done";

    /*fprintf(stderr, "cmp_context: %llx %llx %d\n", genid, context, rc);*/

    return rc;
}

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
/*        TRANSACTIONAL INDEX ROUTINES        */
/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

int ix_addk_auxdb(int auxdb, struct ireq *iq, void *trans, void *key, int ixnum,
                  unsigned long long genid, int rrn, void *dta, int dtalen)
{
    struct dbtable *db = iq->usedb;
    int rc, bdberr;
    void *bdb_handle;

    if (!auxdb && (iq->usedb->ix_disabled[ixnum] & INDEX_WRITE_DISABLED)) {
        if (iq->debug)
            reqprintf(iq, "ix_addk_auxdb: ix %d write disabled", ixnum);
        return 0;
    }

    bdb_handle = get_bdb_handle(db, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

#if 0
    fprintf(stderr, "ix_addkey(%x)\n", ixnum);
    hexdump(key, getkeysize(db, ixnum));
#endif

    iq->gluewhere = "bdb_prim_addkey";
    rc = bdb_prim_addkey_genid(bdb_handle, trans, key, ixnum, rrn, genid, dta,
                               dtalen, 0 /* XXX TODO, are there null values? */,
                               &bdberr);
    iq->gluewhere = "bdb_prim_addkey done";
    if (rc == 0)
        return 0;

    /*translate engine rcodes */
    switch (bdberr) {
    case BDBERR_ADD_DUPE:
        return IX_DUP;
    case BDBERR_DEADLOCK:
        return RC_INTERNAL_RETRY;
    case BDBERR_READONLY:
        return ERR_NOMASTER;
    /*fall through to default*/
    default:
        logmsg(LOGMSG_ERROR, "*ERROR* bdb_prim_addkey return unhandled rc %d\n", bdberr);
        return ERR_INTERNAL;
    }
}

/*index routines */
int ix_addk(struct ireq *iq, void *trans, void *key, int ixnum,
            unsigned long long genid, int rrn, void *dta, int dtalen)
{
    return ix_addk_auxdb(AUXDB_NONE, iq, trans, key, ixnum, genid, rrn, dta,
                         dtalen);
}

int ix_upd_key(struct ireq *iq, void *trans, void *key, int keylen, int ixnum,
               unsigned long long oldgenid, unsigned long long genid, void *dta,
               int dtalen)
{
    struct dbtable *db = iq->usedb;
    int rc, bdberr;
    void *bdb_handle;

    if (iq->usedb->ix_disabled[ixnum] & INDEX_WRITE_DISABLED) {
        if (iq->debug)
            reqprintf(iq, "upd_key: ix %d write disabled", ixnum);
        return 0;
    }

    bdb_handle = get_bdb_handle(db, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

    iq->gluewhere = "bdb_prim_updkey";
    rc = bdb_prim_updkey_genid(bdb_handle, trans, key, keylen, ixnum, genid,
                               oldgenid, dta, dtalen, &bdberr);
    iq->gluewhere = "bdb_prim_updkey done";

    if (rc == 0)
        return 0;

    /*translate engine rcodes */
    switch (bdberr) {
    case BDBERR_DEADLOCK:
        return RC_INTERNAL_RETRY;
    case BDBERR_DELNOTFOUND:
        return IX_NOTFND;
    case BDBERR_READONLY:
        return ERR_NOMASTER;
    /*fall through to default*/
    default:
        logmsg(LOGMSG_ERROR, "*ERROR* bdb_prim_updkey return unhandled rc %d\n", bdberr);
        return ERR_INTERNAL;
    }
}

int ix_delk_auxdb(int auxdb, struct ireq *iq, void *trans, void *key, int ixnum,
                  int rrn, unsigned long long genid)
{
    struct dbtable *db = iq->usedb;
    int rc, bdberr;
    void *bdb_handle;
    if (!auxdb && (iq->usedb->ix_disabled[ixnum] & INDEX_WRITE_DISABLED)) {
        if (iq->debug)
            reqprintf(iq, "ix_delk_auxdb: ix %d write disabled", ixnum);
        return 0;
    }
    bdb_handle = get_bdb_handle(db, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    iq->gluewhere = "bdb_prim_delkey";
    rc = bdb_prim_delkey_genid(bdb_handle, trans, key, ixnum, rrn, genid,
                               &bdberr);
    iq->gluewhere = "bdb_prim_delkey done";
    if (rc == 0)
        return 0;
    /*translate engine rcodes */
    switch (bdberr) {
    case BDBERR_DEADLOCK:
        return RC_INTERNAL_RETRY;
    case BDBERR_DELNOTFOUND:
        return IX_NOTFND;
    case BDBERR_READONLY:
        return ERR_NOMASTER;

    /*fall through to default*/
    default:
        logmsg(LOGMSG_ERROR, "*ERROR* bdb_prim_delkey return unhandled rc %d\n", bdberr);
        while (1) {
            logmsg(LOGMSG_ERROR, "Thread %d got delete key error - send lockstat.\n",
                    pthread_self());
            sleep(5);
        }
        return ERR_INTERNAL;
    }
}

int ix_delk(struct ireq *iq, void *trans, void *key, int ixnum, int rrn,
            unsigned long long genid)
{
    return ix_delk_auxdb(AUXDB_NONE, iq, trans, key, ixnum, rrn, genid);
}

int dat_upv(struct ireq *iq, void *trans, int vptr, void *vdta, int vlen,
            unsigned long long vgenid, void *newdta, int newlen, int rrn,
            unsigned long long *genid, int verifydta, int modnum)
{
    return dat_upv_auxdb(AUXDB_NONE, iq, trans, vptr, vdta, vlen, vgenid,
                         newdta, newlen, rrn, genid, verifydta, modnum, 0);
}

int dat_upv_sc(struct ireq *iq, void *trans, int vptr, void *vdta, int vlen,
               unsigned long long vgenid, void *newdta, int newlen, int rrn,
               unsigned long long *genid, int verifydta, int modnum)
{
    return dat_upv_auxdb(AUXDB_NONE, iq, trans, vptr, vdta, vlen, vgenid,
                         newdta, newlen, rrn, genid, verifydta, modnum, 1);
}

int dat_upv_auxdb(int auxdb, struct ireq *iq, void *trans, int vptr, void *vdta,
                  int vlen, unsigned long long vgenid, void *newdta, int newlen,
                  int rrn, unsigned long long *genid, int verifydta, int modnum,
                  int use_new_genid)
{
    struct dbtable *db = iq->usedb;
    int rc, bdberr;
    void *bdb_handle;
    bdb_handle = get_bdb_handle(db, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    if (vptr != 0)
        return -2; /*only support offset 0 for now */
    iq->gluewhere = "bdb_prim_updvrfy_genid";
    rc = bdb_prim_updvrfy_genid(bdb_handle, trans, vdta, vlen, newdta, newlen,
                                rrn, vgenid, genid, verifydta, modnum,
                                use_new_genid, &bdberr);
    iq->gluewhere = "bdb_prim_updvrfy_genid done";
    if (rc == 0)
        return 0;
    switch (bdberr) {
    case BDBERR_RRN_NOTFOUND: /* rrn deleted considered verify error, since
                                 client provides rrn directly */
        return ERR_VERIFY;
    case BDBERR_DTA_MISMATCH:
        return ERR_VERIFY;
    case BDBERR_DEADLOCK:
        return RC_INTERNAL_RETRY;
    default:
        logmsg(LOGMSG_ERROR, "*ERROR* bdb_prim_updvrfy return unhandled rc %d\n", bdberr);
        return ERR_INTERNAL;
    }
}

int blob_upv_auxdb(int auxdb, struct ireq *iq, void *trans, int vptr,
                   unsigned long long oldgenid, void *newdta, int newlen,
                   int blobno, int rrn, unsigned long long newgenid)
{
    struct dbtable *db = iq->usedb;
    int rc, bdberr;
    void *bdb_handle;
    bdb_handle = get_bdb_handle(db, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    if (vptr != 0)
        return -2; /*only support offset 0 for now */
    iq->gluewhere = "bdb_prim_add_upd_genid";
    rc = bdb_prim_add_upd_genid(bdb_handle, trans, blobno + 1, newdta, newlen,
                                rrn, oldgenid, newgenid, 0, &bdberr);
    iq->gluewhere = "bdb_prim_add_upd_genid done";
    if (rc == 0)
        return 0;
    switch (bdberr) {
    case BDBERR_RRN_NOTFOUND: /* rrn deleted considered verify error, since
                                 client provides rrn directly */
        return ERR_VERIFY;
    case BDBERR_DTA_MISMATCH:
        return ERR_VERIFY;
    case BDBERR_DEADLOCK:
        return RC_INTERNAL_RETRY;
    default:
        logmsg(LOGMSG_ERROR, "*ERROR* bdb_prim_updvrfy return unhandled rc %d\n", bdberr);
        return ERR_INTERNAL;
    }
}

int blob_upv(struct ireq *iq, void *trans, int vptr,
             unsigned long long oldgenid, void *newdta, int newlen, int blobno,
             int rrn, unsigned long long newgenid)
{
    return blob_upv_auxdb(AUXDB_NONE, iq, trans, vptr, oldgenid, newdta, newlen,
                          blobno, rrn, newgenid);
}

int blob_upd_genid(struct ireq *iq, void *trans, int blobno, int rrn,
                   unsigned long long oldgenid, unsigned long long newgenid)
{
    struct dbtable *db = iq->usedb;
    int rc, bdberr;
    void *bdb_handle;
    bdb_handle = get_bdb_handle(db, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    iq->gluewhere = "bdb_upd_genid";
    rc = bdb_upd_genid(bdb_handle, trans, blobno + 1, rrn, oldgenid, newgenid,
                       gbl_inplace_blob_optimization, &bdberr);
    iq->gluewhere = "bdb_upd_genid done";
    if (rc == 0)
        return 0;
    switch (bdberr) {
    case BDBERR_DEADLOCK:
        return RC_INTERNAL_RETRY;
    default:
        logmsg(LOGMSG_ERROR, "*ERROR* bdb_upd_genid return unhandled rc %d\n", bdberr);
        return ERR_INTERNAL;
    }
}

/* gets the datafile/stripe that an add from this thread would write to
 * if round robin mode is on it returns -1
 */
int dat_get_active_stripe(struct ireq *iq)
{
    struct dbtable *db = iq->usedb;
    void *bdb_handle;
    int stripe;

    bdb_handle = get_bdb_handle(db, AUXDB_NONE);

    iq->gluewhere = "bdb_get_active_dtafile";
    stripe = bdb_get_active_stripe(bdb_handle);
    iq->gluewhere = "bdb_get_active_dtafile done";

    return stripe;
}

int dat_add_auxdb(int auxdb, struct ireq *iq, void *trans, void *data,
                  int datalen, unsigned long long *genid, int *out_rrn)
{
    struct dbtable *db = iq->usedb;
    int bdberr, rrn;
    void *bdb_handle;
    int modnum = 0;
    bdb_handle = get_bdb_handle(db, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    iq->gluewhere = "bdb_prim_allocdta_genid";
    if (iq->blkstate)
        modnum = iq->blkstate->modnum;
    rrn = bdb_prim_allocdta_genid(bdb_handle, trans, data, datalen, genid,
                                  modnum, &bdberr);
    iq->gluewhere = "bdb_prim_allocdta_genid done";

    if (bdberr == 0) {
        *out_rrn = rrn;
        return 0;
    }
    *out_rrn = -1;
    if (bdberr == BDBERR_DEADLOCK)
        return RC_INTERNAL_RETRY;
    if (bdberr == BDBERR_TRANTOOCOMPLEX)
        return RC_TRAN_TOO_COMPLEX;
    if (bdberr == BDBERR_READONLY)
        return ERR_NOMASTER;
    logmsg(LOGMSG_ERROR, "*ERROR* bdb_prim_allocdta return unhandled rc %d\n", bdberr);
    return ERR_INTERNAL;
}

int dat_add(struct ireq *iq, void *trans, void *data, int datalen,
            unsigned long long *genid, int *out_rrn)
{
    return dat_add_auxdb(AUXDB_NONE, iq, trans, data, datalen, genid, out_rrn);
}

int dat_set(struct ireq *iq, void *trans, void *data, size_t length, int rrn,
            unsigned long long genid)
{
    struct dbtable *db = iq->usedb;
    int bdberr;
    void *bdb_handle;
    bdb_handle = get_bdb_handle(db, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    iq->gluewhere = "bdb_prim_adddta_n_genid";
    bdb_prim_adddta_n_genid(bdb_handle, trans, 0 /*blobno*/, data, length, rrn,
                            genid, &bdberr);
    iq->gluewhere = "bdb_prim_adddta_n_genid done";

    if (bdberr == 0)
        return 0;
    if (bdberr == BDBERR_DEADLOCK)
        return RC_INTERNAL_RETRY;
    if (bdberr == BDBERR_TRANTOOCOMPLEX)
        return RC_TRAN_TOO_COMPLEX;
    if (bdberr == BDBERR_READONLY)
        return ERR_NOMASTER;
    logmsg(LOGMSG_ERROR, "*ERROR* bdb_prim_adddta_n_genid return unhandled rc %d\n", bdberr);
    return ERR_INTERNAL;
}

int blob_add(struct ireq *iq, void *trans, int blobno, void *data,
             size_t length, int rrn, unsigned long long genid)
{
    return blob_add_auxdb(AUXDB_NONE, iq, trans, blobno, data, length, rrn,
                          genid);
}

int blob_add_auxdb(int auxdb, struct ireq *iq, void *trans, int blobno,
                   void *data, size_t length, int rrn, unsigned long long genid)
{
    struct dbtable *db = iq->usedb;
    int bdberr;
    void *bdb_handle;
    bdb_handle = get_bdb_handle(db, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    iq->gluewhere = "bdb_prim_adddta_n_genid";
    bdb_prim_adddta_n_genid(bdb_handle, trans, blobno + 1, data, length, rrn,
                            genid, &bdberr);
    iq->gluewhere = "bdb_prim_adddta_n_genid done";

    if (bdberr == 0)
        return 0;
    if (bdberr == BDBERR_DEADLOCK)
        return RC_INTERNAL_RETRY;
    if (bdberr == BDBERR_TRANTOOCOMPLEX)
        return RC_TRAN_TOO_COMPLEX;
    if (bdberr == BDBERR_READONLY)
        return ERR_NOMASTER;
    logmsg(LOGMSG_ERROR, "*ERROR* bdb_prim_adddta_n_genid return unhandled rc %d\n", bdberr);
    return ERR_INTERNAL;
}

int dat_del_auxdb(int auxdb, struct ireq *iq, void *trans, int rrn,
                  unsigned long long genid, int delblobs)
{
    struct dbtable *db = iq->usedb;
    int rc, bdberr;
    void *bdb_handle;
    bdb_handle = get_bdb_handle(db, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    iq->gluewhere = "bdb_prim_deallocdta";
    if (delblobs)
        rc = bdb_prim_deallocdta_genid(bdb_handle, trans, rrn, genid, &bdberr);
    else
        rc = bdb_prim_deallocdta_n_genid(bdb_handle, trans, rrn, genid, 0,
                                         &bdberr);
    iq->gluewhere = "bdb_prim_deallocdta done";
    if (rc == 0)
        return 0;
    switch (bdberr) {
    case BDBERR_DEADLOCK:
        return RC_INTERNAL_RETRY;
    case BDBERR_READONLY:
        return ERR_NOMASTER;
    case BDBERR_DELNOTFOUND:
        return ERR_VERIFY;
    /*fall through to default*/
    default:
        /*report these errors to console */
        logmsg(LOGMSG_ERROR, ".dat_del:failed dealloc rrn %d err %d\n", rrn, bdberr);
        return ERR_DTA_FAILED;
    }
}

int dat_del(struct ireq *iq, void *trans, int rrn, unsigned long long genid)
{
    return dat_del_auxdb(AUXDB_NONE, iq, trans, rrn, genid, 1);
}

int dat_del_noblobs(struct ireq *iq, void *trans, int rrn,
                    unsigned long long genid)
{
    return dat_del_auxdb(AUXDB_NONE, iq, trans, rrn, genid, 0);
}

int blob_del(struct ireq *iq, void *trans, int rrn, unsigned long long genid,
             int blobno)
{
    return blob_del_auxdb(AUXDB_NONE, iq, trans, rrn, genid, blobno);
}

int blob_del_auxdb(int auxdb, struct ireq *iq, void *trans, int rrn,
                   unsigned long long genid, int blobno)
{
    struct dbtable *db = iq->usedb;
    int rc, bdberr;
    void *bdb_handle;
    bdb_handle = get_bdb_handle(db, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    iq->gluewhere = "bdb_prim_deallocdta_n_genid";
    rc = bdb_prim_deallocdta_n_genid(bdb_handle, trans, rrn, genid, blobno + 1,
                                     &bdberr);
    iq->gluewhere = "bdb_prim_deallocdta_n_genid done";
    if (rc == 0)
        return 0;
    switch (bdberr) {
    case BDBERR_DEADLOCK:
        return RC_INTERNAL_RETRY;
    case BDBERR_DELNOTFOUND:
        return IX_NOTFND;
    case BDBERR_READONLY:
        return ERR_NOMASTER;
    /*fall through to default*/
    default:
        /*report these errors to console */
        logmsg(LOGMSG_ERROR, "blob_del_auxdb:failed dealloc rrn %d err %d\n", rrn, bdberr);
        return ERR_DTA_FAILED;
    }
}

int dat_upgrade(struct ireq *iq, void *trans, void *newdta, int newlen,
                unsigned long long genid)
{
    struct dbtable *db = iq->usedb;
    int rc, bdberr;
    void *bdb_handle;

    bdb_handle = get_bdb_handle(db, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

    iq->gluewhere = "bdb_prim_upgrade";
    rc = bdb_prim_upgrade(bdb_handle, trans, newdta, newlen, genid, &bdberr);
    iq->gluewhere = "bdb_prim_upgrade done";

    if (rc == 0)
        return 0;

    switch (bdberr) {
    case BDBERR_BADARGS:
        // we get this error only if in-place upgrades are prohibited.
        return ERR_BADREQ;
    case BDBERR_DEADLOCK:
        return RC_INTERNAL_RETRY;
    case BDBERR_DTA_MISMATCH:
    case BDBERR_RRN_NOTFOUND:
        // txns may sneak in and change the genid. consider it done.
        return 0;
    default:
        logmsg(LOGMSG_ERROR, "*ERROR* bdb_prim_upgrade return unhandled rc %d\n", bdberr);
        return ERR_INTERNAL;
    }
}

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
/*        NON-TRANSACTIONAL ROUTINES       */
/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

static int map_unhandled_bdb_rcode(const char *func, int bdberr, int dirtyread)
{
    if (bdberr == BDBERR_INCOHERENT) {
        if (!dirtyread)
            logmsg(LOGMSG_ERROR, "*NOTICE* %s punted due to incoherent\n", func);
        return ERR_INCOHERENT;
    } else {
        if (!dirtyread)
            logmsg(LOGMSG_ERROR, "*ERROR* %s return unhandled rc %d\n", func,
                    bdberr);
        return ERR_CORRUPT;
    }
}

static int map_unhandled_bdb_wr_rcode(const char *func, int bdberr)
{
    if (bdberr == BDBERR_INCOHERENT) {
        logmsg(LOGMSG_WARN, "*NOTICE* %s punted due to incoherent\n", func);
        return ERR_INCOHERENT;
    } else {
        logmsg(LOGMSG_ERROR, "*ERROR* %s return unhandled rc %d\n", func, bdberr);
        return ERR_INTERNAL;
    }
}

int ix_find_last_dup_rnum(struct ireq *iq, int ixnum, void *key, int keylen,
                          void *fndkey, int *fndrrn, unsigned long long *genid,
                          void *fnddta, int *fndlen, int *recnum, int maxlen)
{
    struct dbtable *db = iq->usedb;
    int ixrc, bdberr, retries = 0;
    bdb_fetch_args_t args = {0};

    if ((iq->usedb->ix_disabled[ixnum] & INDEX_READ_DISABLED)) {
        if (iq->debug)
            reqprintf(iq, "ix_find_last_dup_rnum: ix %d read disabled", ixnum);
        return ERR_INDEX_DISABLED;
    }

retry:
    if (fnddta) {
        iq->gluewhere = "bdb_fetch_lastdupe_recnum";
        ixrc = bdb_fetch_lastdupe_recnum_genid(
            db->handle, key, ixnum, keylen, fnddta, maxlen, fndlen, fndkey,
            fndrrn, recnum, genid, &args, &bdberr);
        iq->gluewhere = "bdb_fetch_lastdupe_recnum done";
        VTAG(ixrc, db);
    } else {
        iq->gluewhere = "bdb_fetch_nodta_lastdupe_recnum";
        ixrc = bdb_fetch_nodta_lastdupe_recnum_genid(
            db->handle, key, ixnum, keylen, fndkey, fndrrn, recnum, genid,
            &args, &bdberr);
        iq->gluewhere = "bdb_fetch_nodta_lastdupe_recnum done";
    }
    if (ixrc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* bdb_fetch_lastdupe_recnum too much contention %d "
                   "count %d\n",
                   bdberr, retries);
            return ERR_INTERNAL;
        }
        return map_unhandled_bdb_rcode("bdb_fetch_lastdupe_recnum", bdberr, 0);
    }
    return ixrc;
}

int ix_find_rnum(struct ireq *iq, int ixnum, void *key, int keylen,
                 void *fndkey, int *fndrrn, unsigned long long *genid,
                 void *fnddta, int *fndlen, int *recnum, int maxlen)
{
    struct dbtable *db = iq->usedb;
    char *req;
    int ixrc, bdberr, retries = 0;
    bdb_fetch_args_t args = {0};

    if ((iq->usedb->ix_disabled[ixnum] & INDEX_READ_DISABLED)) {
        if (iq->debug)
            reqprintf(iq, "ix_find_rnum: ix %d read disabled", ixnum);
        return ERR_INDEX_DISABLED;
    }

retry:
    if (fnddta) {
        iq->gluewhere = req = "bdb_fetch_recnum";
        ixrc = bdb_fetch_recnum_genid(db->handle, key, ixnum, keylen, fnddta,
                                      maxlen, fndlen, fndkey, fndrrn, recnum,
                                      genid, &args, &bdberr);
        iq->gluewhere = "bdb_fetch_recnum done";
        VTAG(ixrc, db);
    } else {
        iq->gluewhere = req = "bdb_fetch_nodta_recnum";
        ixrc =
            bdb_fetch_nodta_recnum_genid(db->handle, key, ixnum, keylen, fndkey,
                                         fndrrn, recnum, genid, &args, &bdberr);
        iq->gluewhere = "bdb_fetch_nodta_recnum done";
    }
    if (ixrc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* %s too much contention %d count %d\n", req, bdberr,
                   retries);
            return ERR_INTERNAL;
        }
        return map_unhandled_bdb_rcode(req, bdberr, 0);
    }
    return ixrc;
}

int ix_next_rnum(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
                 int lastrrn, unsigned long long lastgenid, void *fndkey,
                 int *fndrrn, unsigned long long *genid, void *fnddta,
                 int *fndlen, int *recnum, int maxlen)
{
    struct dbtable *db = iq->usedb;
    char *req;
    int ixrc, bdberr, retries = 0;
    bdb_fetch_args_t args = {0};
    if ((iq->usedb->ix_disabled[ixnum] & INDEX_READ_DISABLED)) {
        if (iq->debug)
            reqprintf(iq, "ix_next_rnum: ix %d read disabled", ixnum);
        return ERR_INDEX_DISABLED;
    }
retry:
    if (fnddta) {
        iq->gluewhere = req = "bdb_fetch_next_recnum";
        ixrc = bdb_fetch_next_recnum_genid(
            db->handle, key, ixnum, keylen, last, lastrrn, lastgenid, fnddta,
            maxlen, fndlen, fndkey, fndrrn, recnum, genid, &args, &bdberr);
        iq->gluewhere = "bdb_fetch_next_recnum done";
        VTAG(ixrc, db);
    } else {
        iq->gluewhere = req = "bdb_fetch_next_nodta_recnum";
        ixrc = bdb_fetch_next_nodta_recnum_genid(
            db->handle, key, ixnum, keylen, last, lastrrn, lastgenid, fndkey,
            fndrrn, recnum, genid, &args, &bdberr);
        iq->gluewhere = "bdb_fetch_next_nodta_recnum done";
    }
    if (ixrc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* %s too much contention %d count %d\n", req, bdberr,
                   retries);
            return ERR_INTERNAL;
        }
        return map_unhandled_bdb_rcode(req, bdberr, 0);
    }
    return ixrc;
}

static int ix_find_int_ll(int auxdb, struct ireq *iq, int ixnum, void *key,
                          int keylen, void *fndkey, int *fndrrn,
                          unsigned long long *genid, void *fnddta, int *fndlen,
                          int maxlen, int numblobs, int *blobnums,
                          size_t *blobsizes, size_t *bloboffs, void **blobptrs,
                          int *retries, int dirty, int prefault,
                          int ignore_ixdta, void *trans,
                          bdb_cursor_ser_t *cur_ser, bdb_fetch_args_t *args)
{
    struct dbtable *db = iq->usedb;
    char *req;
    int ixrc, bdberr, lcl_retries;
    void *bdb_handle;
    bdb_fetch_args_t default_args;

    if (args == NULL) {
        bzero(&default_args, sizeof(default_args));
        args = &default_args;
    }

    if (!auxdb && ixnum != -1 &&
        (iq->usedb->ix_disabled[ixnum] & INDEX_READ_DISABLED)) {
        if (iq->debug)
            reqprintf(iq, "%s: ix %d read disabled", __func__, ixnum);
        return ERR_INDEX_DISABLED;
    }
    bdb_handle = get_bdb_handle(db, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    if (!retries) {
        retries = &lcl_retries;
        lcl_retries = 0;
    }
    if (trans && dirty) {
        logmsg(LOGMSG_ERROR, "Transactional dirty reads don't make sense\n");
        return ERR_INTERNAL;
    }

retry:
    if (fnddta && numblobs > 0) {
        /* convert blob numbers to data file numbers */
        int blobns[MAXBLOBS], blobn;
        if (numblobs > MAXBLOBS) {
            return ERR_INTERNAL;
        }
        for (blobn = 0; blobn < numblobs; blobn++)
            blobns[blobn] = blobnums[blobn] + 1;

        iq->gluewhere = req = "bdb_fetch_blobs_genid";

        if (cur_ser)
            ixrc = ERR_INTERNAL; /* not implemented */
        else if (dirty)
            ixrc = bdb_fetch_blobs_genid_dirty(
                bdb_handle, key, ixnum, keylen, fnddta, maxlen, fndlen, fndkey,
                fndrrn, genid, numblobs, blobns, blobsizes, bloboffs, blobptrs,
                ignore_ixdta, args, &bdberr);
        else if (trans)
            ixrc = ERR_INTERNAL; /* not implemented */
        else
            ixrc = bdb_fetch_blobs_genid(bdb_handle, key, ixnum, keylen, fnddta,
                                         maxlen, fndlen, fndkey, fndrrn, genid,
                                         numblobs, blobns, blobsizes, bloboffs,
                                         blobptrs, ignore_ixdta, args, &bdberr);
        VTAG_PTR(ixrc, db);

        iq->gluewhere = "bdb_fetch_blobs_genid done";

        /* validate blobs; lower level bdb is unable to determine
           if a blob has to be present or not; do it here,
           and fake a deadlock if a race is detected */
        if (ixrc == 0 &&
            ix_find_check_blob_race(iq, fnddta, numblobs, blobnums, blobptrs)) {
            ixrc = -1;
            bdberr = BDBERR_DEADLOCK;
        }
    } else if (fnddta) {
        iq->gluewhere = req = "bdb_fetch_genid";

        if (cur_ser)
            ixrc = bdb_fetch_genid_nl_ser(
                bdb_handle, key, ixnum, keylen, fnddta, maxlen, fndlen, fndkey,
                fndrrn, genid, ignore_ixdta, cur_ser, args, &bdberr);
        else if (prefault)
            ixrc = bdb_fetch_genid_prefault(
                bdb_handle, key, ixnum, keylen, fnddta, maxlen, fndlen, fndkey,
                fndrrn, genid, ignore_ixdta, args, &bdberr);
        else if (dirty)
            ixrc = bdb_fetch_genid_dirty(bdb_handle, key, ixnum, keylen, fnddta,
                                         maxlen, fndlen, fndkey, fndrrn, genid,
                                         ignore_ixdta, args, &bdberr);
        else if (trans)
            ixrc = bdb_fetch_genid_trans(bdb_handle, key, ixnum, keylen, fnddta,
                                         maxlen, fndlen, fndkey, fndrrn, genid,
                                         ignore_ixdta, trans, args, &bdberr);
        else
            ixrc = bdb_fetch_genid(bdb_handle, key, ixnum, keylen, fnddta,
                                   maxlen, fndlen, fndkey, fndrrn, genid,
                                   ignore_ixdta, args, &bdberr);
        VTAG_PTR(ixrc, db);

        iq->gluewhere = "bdb_fetch_genid done";
    } else {
        iq->gluewhere = req = "bdb_fetch_nodta_genid";

        if (cur_ser)
            ixrc = bdb_fetch_nodta_genid_nl_ser(bdb_handle, key, ixnum, keylen,
                                                fndkey, fndrrn, genid, cur_ser,
                                                args, &bdberr);
        else if (prefault)
            ixrc = bdb_fetch_nodta_genid_prefault(bdb_handle, key, ixnum,
                                                  keylen, fndkey, fndrrn, genid,
                                                  args, &bdberr);
        else if (dirty)
            ixrc = bdb_fetch_nodta_genid_dirty(bdb_handle, key, ixnum, keylen,
                                               fndkey, fndrrn, genid, args,
                                               &bdberr);
        else if (trans)
            ixrc = bdb_fetch_genid_trans(bdb_handle, key, ixnum, keylen, NULL,
                                         0, NULL, fndkey, fndrrn, genid, 0,
                                         trans, args, &bdberr);
        else
            ixrc = bdb_fetch_nodta_genid(bdb_handle, key, ixnum, keylen, fndkey,
                                         fndrrn, genid, args, &bdberr);

        iq->gluewhere = "bdb_fetch_nodta_genid done";
    }
    if (ixrc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            if (trans) {
                return RC_INTERNAL_RETRY;
            }

            /* no retrys on deadlock for dirty reads */
            if (!dirty) {
                iq->retries++;
                if (++(*retries) < gbl_maxretries) {
                    n_retries++;
                    goto retry;
                }
                logmsg(LOGMSG_ERROR, "*ERROR* %s too much contention %d count %d\n", req,
                       bdberr, *retries);
            }

            return ERR_INTERNAL;
        }

        return map_unhandled_bdb_rcode(req, bdberr, dirty);
    }
    return ixrc;
}

static inline int ix_find_int(int auxdb, struct ireq *iq, int ixnum, void *key,
                              int keylen, void *fndkey, int *fndrrn,
                              unsigned long long *genid, void *fnddta,
                              int *fndlen, int maxlen, int numblobs,
                              int *blobnums, size_t *blobsizes,
                              size_t *bloboffs, void **blobptrs, int *retries,
                              int dirty, int prefault, int ignore_ixdta,
                              void *trans, bdb_cursor_ser_t *cur_ser)
{
    return ix_find_int_ll(auxdb, iq, ixnum, key, keylen, fndkey, fndrrn, genid,
                          fnddta, fndlen, maxlen, numblobs, blobnums, blobsizes,
                          bloboffs, blobptrs, retries, dirty, prefault,
                          ignore_ixdta, trans, cur_ser, NULL);
}

int ix_find_auxdb(int auxdb, struct ireq *iq, int ixnum, void *key, int keylen,
                  void *fndkey, int *fndrrn, unsigned long long *genid,
                  void *fnddta, int *fndlen, int maxlen)
{
    return ix_find_int(auxdb, iq, ixnum, key, keylen, fndkey, fndrrn, genid,
                       fnddta, fndlen, maxlen, 0, NULL, NULL, NULL, NULL, NULL,
                       0, 0, 0, NULL, NULL);
}

int ix_find_blobs(struct ireq *iq, int ixnum, void *key, int keylen,
                  void *fndkey, int *fndrrn, unsigned long long *genid,
                  void *fnddta, int *fndlen, int maxlen, int numblobs,
                  int *blobnums, size_t *blobsizes, size_t *bloboffs,
                  void **blobptrs, int *retries)
{
    return ix_find_int(AUXDB_NONE, iq, ixnum, key, keylen, fndkey, fndrrn,
                       genid, fnddta, fndlen, maxlen, numblobs, blobnums,
                       blobsizes, bloboffs, blobptrs, retries, 0, 0, 0, NULL,
                       NULL);
}

int get_next_genids(struct ireq *iq, int ixnum, void *key, int keylen,
                    unsigned long long *genids, int numgenids,
                    int *num_genids_gotten)
{
    int rc;
    void *bdb_handle;
    struct dbtable *db = iq->usedb;
    int bdb_err;

    bdb_handle = get_bdb_handle(db, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

    rc = bdb_fetch_next_genids(bdb_handle, ixnum, keylen, key, genids,
                               numgenids, num_genids_gotten, &bdb_err);

    return rc;
}

int ix_find_flags(struct ireq *iq, void *trans, int ixnum, void *key,
                  int keylen, void *fndkey, int *fndrrn,
                  unsigned long long *genid, void *fnddta, int *fndlen,
                  int maxlen, int flags)
{
    bdb_fetch_args_t args = {0};
    if (flags & IX_FIND_IGNORE_INCOHERENT) {
        args.ignore_incoherent = 1;
    }

    return ix_find_int_ll(AUXDB_NONE, iq, ixnum, key, keylen, fndkey, fndrrn,
                          genid, fnddta, fndlen, maxlen, 0, NULL, NULL, NULL,
                          NULL, NULL, 0, 0, 0, trans, NULL, &args);
}

int ix_find(struct ireq *iq, int ixnum, void *key, int keylen, void *fndkey,
            int *fndrrn, unsigned long long *genid, void *fnddta, int *fndlen,
            int maxlen)
{
    return ix_find_int(AUXDB_NONE, iq, ixnum, key, keylen, fndkey, fndrrn,
                       genid, fnddta, fndlen, maxlen, 0, NULL, NULL, NULL, NULL,
                       NULL, 0, 0, 0, NULL, NULL);
}

int ix_find_nl_ser_flags(struct ireq *iq, int ixnum, void *key, int keylen,
                         void *fndkey, int *fndrrn, unsigned long long *genid,
                         void *fnddta, int *fndlen, int maxlen,
                         bdb_cursor_ser_t *cur_ser, int flags)
{
    bdb_fetch_args_t args = {0};
    if (flags & IX_FIND_IGNORE_INCOHERENT) {
        args.ignore_incoherent = 1;
    }
    return ix_find_int_ll(AUXDB_NONE, iq, ixnum, key, keylen, fndkey, fndrrn,
                          genid, fnddta, fndlen, maxlen, 0, NULL, NULL, NULL,
                          NULL, NULL, 0, 0, 0, NULL, cur_ser, &args);
}

int ix_find_nl_ser(struct ireq *iq, int ixnum, void *key, int keylen,
                   void *fndkey, int *fndrrn, unsigned long long *genid,
                   void *fnddta, int *fndlen, int maxlen,
                   bdb_cursor_ser_t *cur_ser)
{
    return ix_find_int(AUXDB_NONE, iq, ixnum, key, keylen, fndkey, fndrrn,
                       genid, fnddta, fndlen, maxlen, 0, NULL, NULL, NULL, NULL,
                       NULL, 0, 0, 0, NULL, cur_ser);
}

int ix_find_trans(struct ireq *iq, void *trans, int ixnum, void *key,
                  int keylen, void *fndkey, int *fndrrn,
                  unsigned long long *genid, void *fnddta, int *fndlen,
                  int maxlen)
{
    return ix_find_int(AUXDB_NONE, iq, ixnum, key, keylen, fndkey, fndrrn,
                       genid, fnddta, fndlen, maxlen, 0, NULL, NULL, NULL, NULL,
                       NULL, 0, 0, 0, trans, NULL);
}

int ix_find_nodatacopy(struct ireq *iq, int ixnum, void *key, int keylen,
                       void *fndkey, int *fndrrn, unsigned long long *genid,
                       void *fnddta, int *fndlen, int maxlen)
{

    return ix_find_int(AUXDB_NONE, iq, ixnum, key, keylen, fndkey, fndrrn,
                       genid, fnddta, fndlen, maxlen, 0, NULL, NULL, NULL, NULL,
                       NULL, 0, 1, 0, NULL, NULL);
}

int ix_find_prefault(struct ireq *iq, int ixnum, void *key, int keylen,
                     void *fndkey, int *fndrrn, unsigned long long *genid,
                     void *fnddta, int *fndlen, int maxlen)
{
    return ix_find_int(AUXDB_NONE, iq, ixnum, key, keylen, fndkey, fndrrn,
                       genid, fnddta, fndlen, maxlen, 0, NULL, NULL, NULL, NULL,
                       NULL, 1, 1, 0, NULL, NULL);
}

int ix_find_dirty(struct ireq *iq, int ixnum, void *key, int keylen,
                  void *fndkey, int *fndrrn, unsigned long long *genid,
                  void *fnddta, int *fndlen, int maxlen)
{
    return ix_find_int(AUXDB_NONE, iq, ixnum, key, keylen, fndkey, fndrrn,
                       genid, fnddta, fndlen, maxlen, 0, NULL, NULL, NULL, NULL,
                       NULL, 1, 0, 0, NULL, NULL);
}

int ix_find_auxdb_by_rrn_and_genid_get_curgenid(
    int auxdb, struct ireq *iq, int rrn, unsigned long long genid,
    unsigned long long *outgenid, void *fnddta, int *fndlen, int maxlen)
{
    int rc;
    int retries = 0;
    void *bdb_handle;
    int bdberr;
    char *req;
    bdb_fetch_args_t args = {0};

    bdb_handle = get_bdb_handle(iq->usedb, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
retry:
    iq->gluewhere = req = "bdb_fetch_by_rrn_and_genid_get_curgenid";
    rc = bdb_fetch_by_rrn_and_genid_get_curgenid(bdb_handle, rrn, genid,
                                                 outgenid, fnddta, maxlen,
                                                 fndlen, &args, &bdberr);
    iq->gluewhere = "bdb_fetch_by_rrn_and_genid done_get_curgenid";
    if (rc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* %s too much contention %d count %d\n", req, bdberr,
                   retries);
            return ERR_INTERNAL;
        } else if (bdberr == BDBERR_FETCH_DTA)
            return IX_NOTFND; /* not found or rrn/genid mismatch */
        return map_unhandled_bdb_rcode(req, bdberr, 0);
    } else {
        VTAG_GENID(rc, iq->usedb);
    }
    return rc;
}

int ix_find_auxdb_by_rrn_and_genid(int auxdb, struct ireq *iq, int rrn,
                                   unsigned long long genid, void *fnddta,
                                   int *fndlen, int maxlen)
{
    int rc;
    int retries = 0;
    void *bdb_handle;
    int bdberr;
    char *req;
    bdb_fetch_args_t args = {0};

    bdb_handle = get_bdb_handle(iq->usedb, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
retry:
    iq->gluewhere = req = "bdb_fetch_by_rrn_and_genid";
    rc = bdb_fetch_by_rrn_and_genid(bdb_handle, rrn, genid, fnddta, maxlen,
                                    fndlen, &args, &bdberr);
    iq->gluewhere = "bdb_fetch_by_rrn_and_genid done";

    if (rc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* %s too much contention %d count %d\n", req, bdberr,
                   retries);
            return ERR_INTERNAL;
        } else if (bdberr == BDBERR_FETCH_DTA)
            return IX_NOTFND; /* not found or rrn/genid mismatch */
        return map_unhandled_bdb_rcode(req, bdberr, 0);
    } else {
        VTAG_GENID(rc, iq->usedb);
    }
    return rc;
}

/* we dont want to retry on deadlock here. */
int ix_find_auxdb_by_rrn_and_genid_dirty(int auxdb, struct ireq *iq, int rrn,
                                         unsigned long long genid, void *fnddta,
                                         int *fndlen, int maxlen)
{
    int rc;
    void *bdb_handle;
    int bdberr;
    char *req;
    bdb_fetch_args_t args = {0};

    bdb_handle = get_bdb_handle(iq->usedb, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

    iq->gluewhere = req = "bdb_fetch_by_rrn_and_genid";

    rc = bdb_fetch_by_rrn_and_genid_dirty(bdb_handle, rrn, genid, fnddta,
                                          maxlen, fndlen, &args, &bdberr);

    iq->gluewhere = "bdb_fetch_by_rrn_and_genid done";

    if (rc == -1) {
        if (bdberr == BDBERR_FETCH_DTA)
            return IX_NOTFND; /* not found or rrn/genid mismatch */
        else
            return ERR_CORRUPT;
    } else {
        VTAG_GENID(rc, iq->usedb);
    }

    return rc;
}

int ix_find_auxdb_by_rrn_and_genid_prefault(int auxdb, struct ireq *iq, int rrn,
                                            unsigned long long genid,
                                            void *fnddta, int *fndlen,
                                            int maxlen)
{
    int rc;
    void *bdb_handle;
    int bdberr;
    char *req;
    bdb_fetch_args_t args = {0};

    bdb_handle = get_bdb_handle(iq->usedb, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

    iq->gluewhere = req = "bdb_fetch_by_rrn_and_genid_prefault";

    rc = bdb_fetch_by_rrn_and_genid_prefault(bdb_handle, rrn, genid, fnddta,
                                             maxlen, fndlen, &args, &bdberr);

    iq->gluewhere = "bdb_fetch_by_rrn_and_genid_prefault done";

    if (rc == -1) {
        if (bdberr == BDBERR_FETCH_DTA)
            return IX_NOTFND; /* not found or rrn/genid mismatch */
        else
            return ERR_CORRUPT;
    } else {
        VTAG_GENID(rc, iq->usedb);
    }

    return rc;
}

int ix_find_auxdb_by_rrn_and_genid_tran(int auxdb, struct ireq *iq, int rrn,
                                        unsigned long long genid, void *fnddta,
                                        int *fndlen, int maxlen, void *trans,
                                        int *ver)
{
    int rc;
    int retries = 0;
    void *bdb_handle;
    int bdberr;
    char *req;
    bdb_fetch_args_t args = {0};

    bdb_handle = get_bdb_handle(iq->usedb, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
retry:
    iq->gluewhere = req = "bdb_fetch_by_rrn_and_genid";
    rc = bdb_fetch_by_rrn_and_genid_tran(bdb_handle, trans, rrn, genid, fnddta,
                                         maxlen, fndlen, &args, &bdberr);
    iq->gluewhere = "bdb_fetch_by_rrn_and_genid done";
    if (rc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            if (trans)
                return RC_INTERNAL_RETRY;
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* %s too much contention %d count %d\n", req, bdberr,
                   retries);
            return ERR_INTERNAL;
        } else if (bdberr == BDBERR_FETCH_DTA)
            return IX_NOTFND; /* not found or rrn/genid mismatch */
        return map_unhandled_bdb_rcode(req, bdberr, 0);
    } else {
        VTAG_GENID(rc, iq->usedb);
        if (ver != NULL)
            *ver = args.ver;
    }
    return rc;
}

int ix_find_blobs_by_rrn_and_genid(struct ireq *iq, int rrn,
                                   unsigned long long genid, int numblobs,
                                   int *blobnums, size_t *blobsizes,
                                   size_t *bloboffs, void **blobptrs)
{
    return ix_find_auxdb_blobs_by_rrn_and_genid(AUXDB_NONE, iq, rrn, genid,
                                                numblobs, blobnums, blobsizes,
                                                bloboffs, blobptrs);
}

int ix_find_auxdb_blobs_by_rrn_and_genid(int auxdb, struct ireq *iq, int rrn,
                                         unsigned long long genid, int numblobs,
                                         int *blobnums, size_t *blobsizes,
                                         size_t *bloboffs, void **blobptrs)
{
    int rc;
    int retries = 0;
    void *bdb_handle;
    int bdberr;
    char *req;
    int blobns[MAXBLOBS], blobn;
    bdb_fetch_args_t args = {0};

    bdb_handle = get_bdb_handle(iq->usedb, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

    /* convert blob numbers to data file numbers */
    if (numblobs > MAXBLOBS) {
        return ERR_INTERNAL;
    }
    for (blobn = 0; blobn < numblobs; blobn++)
        blobns[blobn] = blobnums[blobn] + 1;
retry:
    iq->gluewhere = req = "bdb_fetch_blobs_by_rrn_and_genid";
    rc = bdb_fetch_blobs_by_rrn_and_genid(bdb_handle, rrn, genid, numblobs,
                                          blobns, blobsizes, bloboffs, blobptrs,
                                          &args, &bdberr);
    iq->gluewhere = "bdb_fetch_blobs_by_rrn_and_genid done";
    if (rc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* %s too much contention %d count %d\n", req, bdberr,
                   retries);
            return ERR_INTERNAL;
        } else if (bdberr == BDBERR_DTA_MISMATCH) {
            return ERR_VERIFY;
        } else if (bdberr == BDBERR_FETCH_DTA)
            return IX_NOTFND; /* not found or rrn/genid mismatch */
        else if (bdberr == BDBERR_INCOHERENT)
            return ERR_INCOHERENT;

        return map_unhandled_bdb_rcode(req, bdberr, 0);
    }
    return rc;
}

int ix_find_auxdb_blobs_by_rrn_and_genid_tran(int auxdb, struct ireq *iq,
                                              void *trans, int rrn,
                                              unsigned long long genid,
                                              int numblobs, int *blobnums,
                                              size_t *blobsizes,
                                              size_t *bloboffs, void **blobptrs)
{
    int rc;
    void *bdb_handle;
    int bdberr;
    char *req;
    int blobns[MAXBLOBS], blobn;
    bdb_fetch_args_t args = {0};

    bdb_handle = get_bdb_handle(iq->usedb, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

    /* convert blob numbers to data file numbers */
    if (numblobs > MAXBLOBS) {
        return ERR_INTERNAL;
    }
    for (blobn = 0; blobn < numblobs; blobn++)
        blobns[blobn] = blobnums[blobn] + 1;

    iq->gluewhere = req = "bdb_fetch_blobs_by_rrn_and_genid_tran";
    rc = bdb_fetch_blobs_by_rrn_and_genid_tran(
        bdb_handle, trans, rrn, genid, numblobs, blobns, blobsizes, bloboffs,
        blobptrs, &args, &bdberr);
    iq->gluewhere = "bdb_fetch_blobs_by_rrn_and_genid_tran done";

    if (rc == -1) {
        switch (bdberr) {
        case BDBERR_DEADLOCK:
            return RC_INTERNAL_RETRY;

        case BDBERR_DTA_MISMATCH:
            return ERR_VERIFY;

        case BDBERR_FETCH_DTA:
            return IX_NOTFND; /* not found or rrn/genid mismatch */

        default:
            return map_unhandled_bdb_wr_rcode(req, bdberr);
        }
    }
    return rc;
}

int ix_find_blobs_by_rrn_and_genid_tran(struct ireq *iq, void *trans, int rrn,
                                        unsigned long long genid, int numblobs,
                                        int *blobnums, size_t *blobsizes,
                                        size_t *bloboffs, void **blobptrs)
{
    return ix_find_auxdb_blobs_by_rrn_and_genid_tran(
        AUXDB_NONE, iq, trans, rrn, genid, numblobs, blobnums, blobsizes,
        bloboffs, blobptrs);
}

int ix_find_by_rrn_and_genid_get_curgenid(struct ireq *iq, int rrn,
                                          unsigned long long genid,
                                          unsigned long long *outgenid,
                                          void *fnddta, int *fndlen, int maxlen)
{
    return ix_find_auxdb_by_rrn_and_genid_get_curgenid(
        AUXDB_NONE, iq, rrn, genid, outgenid, fnddta, fndlen, maxlen);
}

int ix_find_by_rrn_and_genid(struct ireq *iq, int rrn, unsigned long long genid,
                             void *fnddta, int *fndlen, int maxlen)
{
    return ix_find_auxdb_by_rrn_and_genid(AUXDB_NONE, iq, rrn, genid, fnddta,
                                          fndlen, maxlen);
}

int ix_find_by_rrn_and_genid_prefault(struct ireq *iq, int rrn,
                                      unsigned long long genid, void *fnddta,
                                      int *fndlen, int maxlen)
{
    return ix_find_auxdb_by_rrn_and_genid_prefault(AUXDB_NONE, iq, rrn, genid,
                                                   fnddta, fndlen, maxlen);
}

int ix_find_by_rrn_and_genid_dirty(struct ireq *iq, int rrn,
                                   unsigned long long genid, void *fnddta,
                                   int *fndlen, int maxlen)
{
    return ix_find_auxdb_by_rrn_and_genid_dirty(AUXDB_NONE, iq, rrn, genid,
                                                fnddta, fndlen, maxlen);
}

int ix_find_by_rrn_and_genid_tran(struct ireq *iq, int rrn,
                                  unsigned long long genid, void *fnddta,
                                  int *fndlen, int maxlen, void *trans)
{
    int rc = 0;

    rc = ix_find_auxdb_by_rrn_and_genid_tran(AUXDB_NONE, iq, rrn, genid, fnddta,
                                             fndlen, maxlen, trans, NULL);

    if (rc == IX_EMPTY)
        rc = IX_NOTFND;

    return rc;
}

int ix_find_ver_by_rrn_and_genid_tran(struct ireq *iq, int rrn,
                                      unsigned long long genid, void *fnddta,
                                      int *fndlen, int maxlen, void *trans,
                                      int *version)
{
    int rc = 0;

    rc = ix_find_auxdb_by_rrn_and_genid_tran(AUXDB_NONE, iq, rrn, genid, fnddta,
                                             fndlen, maxlen, trans, version);

    if (rc == IX_EMPTY)
        rc = IX_NOTFND;

    return rc;
}

int ix_find_by_primkey_tran(struct ireq *iq, void *key, int keylen,
                            void *fndkey, int *fndrrn,
                            unsigned long long *genid, void *fnddta,
                            int *fndlen, int maxlen, void *trans)
{
    return ix_find_auxdb_by_primkey_tran(AUXDB_NONE, iq, key, keylen, fndkey,
                                         fndrrn, genid, fnddta, fndlen, maxlen,
                                         trans);
}

int ix_find_auxdb_by_primkey_tran(int auxdb, struct ireq *iq, void *key,
                                  int keylen, void *fndkey, int *fndrrn,
                                  unsigned long long *genid, void *fnddta,
                                  int *fndlen, int maxlen, void *trans)
{
    int rc;
    int retries = 0;
    void *bdb_handle;
    int bdberr;
    char *req;
    bdb_fetch_args_t args = {0};

    bdb_handle = get_bdb_handle(iq->usedb, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
retry:
    iq->gluewhere = req = "bdb_fetch_by_primkey_tran";
    rc = bdb_fetch_by_primkey_tran(bdb_handle, trans, key, fnddta, maxlen,
                                   fndlen, fndrrn, genid, &args, &bdberr);
    iq->gluewhere = "bdb_fetch_by_primkey_tran done";

    if (rc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            if (trans)
                return RC_INTERNAL_RETRY;
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* %s too much contention %d count %d\n", req, bdberr,
                   retries);
            return ERR_INTERNAL;
        } else if (bdberr == BDBERR_FETCH_DTA || bdberr == BDBERR_FETCH_IX)
            return IX_NOTFND; /* not found or rrn/genid mismatch */
        return map_unhandled_bdb_rcode(req, bdberr, 0);
    } else {
        VTAG(rc, iq->usedb);
    }
    return rc;
}

int ix_find_by_key_tran(struct ireq *iq, void *key, int keylen, int index,
                        void *fndkey, int *fndrrn, unsigned long long *genid,
                        void *fnddta, int *fndlen, int maxlen, void *trans)
{
    return ix_find_auxdb_by_key_tran(AUXDB_NONE, iq, key, keylen, index, fndkey,
                                     fndrrn, genid, fnddta, fndlen, maxlen,
                                     trans);
}

int ix_find_auxdb_by_key_tran(int auxdb, struct ireq *iq, void *key, int keylen,
                              int index, void *fndkey, int *fndrrn,
                              unsigned long long *genid, void *fnddta,
                              int *fndlen, int maxlen, void *trans)
{
    int rc;
    int retries = 0;
    void *bdb_handle;
    int bdberr;
    char *req;
    bdb_fetch_args_t args = {0};
    if (!auxdb && (iq->usedb->ix_disabled[index] & INDEX_READ_DISABLED)) {
        if (iq->debug)
            reqprintf(iq, "ix_find_auxdb_by_key_tran: ix %d read disabled",
                      index);
        return ERR_INDEX_DISABLED;
    }

    bdb_handle = get_bdb_handle(iq->usedb, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
retry:
    iq->gluewhere = req = "bdb_fetch_by_key_tran";
    rc = bdb_fetch_by_key_tran(bdb_handle, trans, key, keylen, index, fndkey,
                               fnddta, maxlen, fndlen, fndrrn, genid, &args,
                               &bdberr);
    iq->gluewhere = "bdb_fetch_by_key_tran done";

    if (rc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            if (trans)
                return RC_INTERNAL_RETRY;
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* %s too much contention %d count %d\n", req, bdberr,
                   retries);
            return ERR_INTERNAL;
        } else if (bdberr == BDBERR_FETCH_DTA || bdberr == BDBERR_FETCH_IX)
            return IX_NOTFND; /* not found or rrn/genid mismatch */
        return map_unhandled_bdb_rcode(req, bdberr, 0);
    } else {
        VTAG(rc, iq->usedb);
    }
    return rc;
}

static int ix_next_int_ll(int auxdb, int lookahead, struct ireq *iq, int ixnum,
                          void *key, int keylen, void *last, int lastrrn,
                          unsigned long long lastgenid, void *fndkey,
                          int *fndrrn, unsigned long long *genid, void *fnddta,
                          int *fndlen, int maxlen, int numblobs, int *blobnums,
                          size_t *blobsizes, size_t *bloboffs, void **blobptrs,
                          int *retries, unsigned long long context, void *trans,
                          bdb_cursor_ser_t *cur_ser, bdb_fetch_args_t *args)
{
    struct dbtable *db = iq->usedb;
    char *req;
    int ixrc, bdberr, lcl_retries;
    void *bdb_handle;
    void *curlast = last;
    int numskips = 0;
    bdb_fetch_args_t default_args;

    if (args == NULL) {
        bzero(&default_args, sizeof(default_args));
        args = &default_args;
    }

    if (!auxdb && (iq->usedb->ix_disabled[ixnum] & INDEX_READ_DISABLED)) {
        if (iq->debug)
            reqprintf(iq, "ix_next_blobs_auxdb: ix %d read disabled", ixnum);
        return ERR_INDEX_DISABLED;
    }
    bdb_handle = get_bdb_handle(db, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    if (!retries) {
        retries = &lcl_retries;
        lcl_retries = 0;
    }
retry:
    if (fnddta) {
        if (lookahead) {
            if (cur_ser)
                bdb_cursor_ser_invalidate(cur_ser);

            if (numblobs > 0) {
                /* convert blob numbers to data file numbers */
                int blobns[MAXBLOBS], blobn;
                if (numblobs > MAXBLOBS)
                    return ERR_INTERNAL;
                for (blobn = 0; blobn < numblobs; blobn++)
                    blobns[blobn] = blobnums[blobn] + 1;

                iq->gluewhere = req = "bdb_fetch_next_blobs_genid";
                ixrc = bdb_fetch_next_blobs_genid(
                    bdb_handle, key, ixnum, keylen, curlast, lastrrn, lastgenid,
                    fnddta, maxlen, fndlen, fndkey, fndrrn, genid, numblobs,
                    blobns, blobsizes, bloboffs, blobptrs, args, &bdberr);
                iq->gluewhere = "bdb_fetch_next_blobs_genid done";
            } else if (trans) {
                iq->gluewhere = req = "bdb_fetch_next_genid";
                ixrc = bdb_fetch_next_genid_tran(
                    bdb_handle, key, ixnum, keylen, curlast, lastrrn, lastgenid,
                    fnddta, maxlen, fndlen, fndkey, fndrrn, genid, trans, args,
                    &bdberr);
                iq->gluewhere = "bdb_fetch_next_genid done";
            } else {
                iq->gluewhere = req = "bdb_fetch_next_genid";
                ixrc = bdb_fetch_next_genid(bdb_handle, key, ixnum, keylen,
                                            curlast, lastrrn, lastgenid, fnddta,
                                            maxlen, fndlen, fndkey, fndrrn,
                                            genid, args, &bdberr);
                iq->gluewhere = "bdb_fetch_next_genid done";
            }
        } else if (cur_ser) {
            iq->gluewhere = req = "bdb_fetch_next_genid_nl_ser";
            ixrc = bdb_fetch_next_genid_nl_ser(
                bdb_handle, key, ixnum, keylen, curlast, lastrrn, lastgenid,
                fnddta, maxlen, fndlen, fndkey, fndrrn, genid, cur_ser, args,
                &bdberr);
            iq->gluewhere = "bdb_fetch_next_genid_nl_ser done";
        } else {
            iq->gluewhere = req = "bdb_fetch_next_genid_nl";
            ixrc = bdb_fetch_next_genid_nl(
                bdb_handle, key, ixnum, keylen, curlast, lastrrn, lastgenid,
                fnddta, maxlen, fndlen, fndkey, fndrrn, genid, args, &bdberr);
            iq->gluewhere = "bdb_fetch_next_genid_nl done";
        }
        VTAG_PTR(ixrc, db);
    } else if (cur_ser) {
        iq->gluewhere = req = "bdb_fetch_next_nodta_genid_nl_ser";
        ixrc = bdb_fetch_next_nodta_genid_nl_ser(
            bdb_handle, key, ixnum, keylen, curlast, lastrrn, lastgenid, fndkey,
            fndrrn, genid, cur_ser, args, &bdberr);
        iq->gluewhere = "bdb_fetch_next_nodta_genid_nl_ser done";
    } else {
        iq->gluewhere = req = "bdb_fetch_next_nodta_genid";
        ixrc = bdb_fetch_next_nodta_genid(bdb_handle, key, ixnum, keylen,
                                          curlast, lastrrn, lastgenid, fndkey,
                                          fndrrn, genid, args, &bdberr);
        iq->gluewhere = "bdb_fetch_next_nodta_genid done";
    }
    if (ixrc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            if (trans)
                return RC_INTERNAL_RETRY;

            iq->retries++;
            if (++(*retries) < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* %s too much contention %d count %d\n", req, bdberr,
                   *retries);
            return ERR_INTERNAL;
        }
        return map_unhandled_bdb_rcode(req, bdberr, 0);
    }

    /* loop detector */
    if (context == (unsigned long long)-1) {
        logmsg(LOGMSG_FATAL, "COMDB2 BAD CONTEXT %llu, LOOPING ALERT\n", context);
        if (debug_switch_abort_on_invalid_context())
            abort();
    }

    /* find again if we want stable cursors and this record was added after
     * the search began. */
    if (ixrc >= 0 && ixrc <= 2 && context && genid && *genid != context &&
        !bdb_check_genid_is_older(bdb_handle, *genid, context)) {
        if (blobptrs) {
            int ii;
            for (ii = 0; ii < numblobs; ii++) {
                if (blobptrs[ii]) {
                    free(blobptrs[ii]);
                    blobptrs[ii] = NULL;
                }
            }
        }
        curlast = fndkey; /* don't find the same record over and over! */
        lastrrn = *fndrrn;
        lastgenid = *genid;
        *retries = 0;
        if (++numskips == gbl_maxcontextskips) {
            logmsg(LOGMSG_ERROR, "*ERROR* %s skipped %d (too many) records\n", req, numskips);
            return ERR_INTERNAL;
        } else {

            goto retry;
        }
    }
    return ixrc;
}

static int ix_next_int(int auxdb, int lookahead, struct ireq *iq, int ixnum,
                       void *key, int keylen, void *last, int lastrrn,
                       unsigned long long lastgenid, void *fndkey, int *fndrrn,
                       unsigned long long *genid, void *fnddta, int *fndlen,
                       int maxlen, int numblobs, int *blobnums,
                       size_t *blobsizes, size_t *bloboffs, void **blobptrs,
                       int *retries, unsigned long long context, void *trans,
                       bdb_cursor_ser_t *cur_ser)
{
    return ix_next_int_ll(
        auxdb, lookahead, iq, ixnum, key, keylen, last, lastrrn, lastgenid,
        fndkey, fndrrn, genid, fnddta, fndlen, maxlen, numblobs, blobnums,
        blobsizes, bloboffs, blobptrs, retries, context, trans, cur_ser, NULL);
}

int ix_next_auxdb(int auxdb, int lookahead, struct ireq *iq, int ixnum,
                  void *key, int keylen, void *last, int lastrrn,
                  unsigned long long lastgenid, void *fndkey, int *fndrrn,
                  unsigned long long *genid, void *fnddta, int *fndlen,
                  int maxlen, unsigned long long context)
{
    return ix_next_int(auxdb, lookahead, iq, ixnum, key, keylen, last, lastrrn,
                       lastgenid, fndkey, fndrrn, genid, fnddta, fndlen, maxlen,
                       0, NULL, NULL, NULL, NULL, NULL, context, NULL, NULL);
}

int ix_next_blobs(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
                  int lastrrn, unsigned long long lastgenid, void *fndkey,
                  int *fndrrn, unsigned long long *genid, void *fnddta,
                  int *fndlen, int maxlen, int numblobs, int *blobnums,
                  size_t *blobsizes, size_t *bloboffs, void **blobptrs,
                  int *retries, unsigned long long context)
{
    return ix_next_int(AUXDB_NONE, 1, iq, ixnum, key, keylen, last, lastrrn,
                       lastgenid, fndkey, fndrrn, genid, fnddta, fndlen, maxlen,
                       numblobs, blobnums, blobsizes, bloboffs, blobptrs,
                       retries, context, NULL, NULL);
}

int ix_next_blobs_auxdb(int auxdb, int lookahead, struct ireq *iq, int ixnum,
                        void *key, int keylen, void *last, int lastrrn,
                        unsigned long long lastgenid, void *fndkey, int *fndrrn,
                        unsigned long long *genid, void *fnddta, int *fndlen,
                        int maxlen, int numblobs, int *blobnums,
                        size_t *blobsizes, size_t *bloboffs, void **blobptrs,
                        int *retries, unsigned long long context)
{
    return ix_next_int(auxdb, lookahead, iq, ixnum, key, keylen, last, lastrrn,
                       lastgenid, fndkey, fndrrn, genid, fnddta, fndlen, maxlen,
                       numblobs, blobnums, blobsizes, bloboffs, blobptrs,
                       retries, context, NULL, NULL);
}

int ix_next(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
            int lastrrn, unsigned long long lastgenid, void *fndkey,
            int *fndrrn, unsigned long long *genid, void *fnddta, int *fndlen,
            int maxlen, unsigned long long context)
{
    return ix_next_int(AUXDB_NONE, 1, iq, ixnum, key, keylen, last, lastrrn,
                       lastgenid, fndkey, fndrrn, genid, fnddta, fndlen, maxlen,
                       0, NULL, NULL, NULL, NULL, NULL, context, NULL, NULL);
}

int ix_next_nl_ser_flags(struct ireq *iq, int ixnum, void *key, int keylen,
                         void *last, int lastrrn, unsigned long long lastgenid,
                         void *fndkey, int *fndrrn, unsigned long long *genid,
                         void *fnddta, int *fndlen, int maxlen,
                         unsigned long long context, bdb_cursor_ser_t *cur_ser,
                         int flags)
{
    bdb_fetch_args_t args = {0};
    if (flags & IX_FIND_IGNORE_INCOHERENT) {
        args.ignore_incoherent = 1;
    }
    return ix_next_int_ll(AUXDB_NONE, 0, iq, ixnum, key, keylen, last, lastrrn,
                          lastgenid, fndkey, fndrrn, genid, fnddta, fndlen,
                          maxlen, 0, NULL, NULL, NULL, NULL, NULL, context,
                          NULL, cur_ser, &args);
}

int ix_next_nl_ser(struct ireq *iq, int ixnum, void *key, int keylen,
                   void *last, int lastrrn, unsigned long long lastgenid,
                   void *fndkey, int *fndrrn, unsigned long long *genid,
                   void *fnddta, int *fndlen, int maxlen,
                   unsigned long long context, bdb_cursor_ser_t *cur_ser)
{
    return ix_next_int(AUXDB_NONE, 0, iq, ixnum, key, keylen, last, lastrrn,
                       lastgenid, fndkey, fndrrn, genid, fnddta, fndlen, maxlen,
                       0, NULL, NULL, NULL, NULL, NULL, context, NULL, cur_ser);
}

int ix_next_nl(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
               int lastrrn, unsigned long long lastgenid, void *fndkey,
               int *fndrrn, unsigned long long *genid, void *fnddta,
               int *fndlen, int maxlen, unsigned long long context)
{
    return ix_next_int(AUXDB_NONE, 0, iq, ixnum, key, keylen, last, lastrrn,
                       lastgenid, fndkey, fndrrn, genid, fnddta, fndlen, maxlen,
                       0, NULL, NULL, NULL, NULL, NULL, context, NULL, NULL);
}

int ix_next_trans(struct ireq *iq, void *trans, int ixnum, void *key,
                  int keylen, void *last, int lastrrn,
                  unsigned long long lastgenid, void *fndkey, int *fndrrn,
                  unsigned long long *genid, void *fnddta, int *fndlen,
                  int maxlen, unsigned long long context)
{
    return ix_next_int(AUXDB_NONE, 1, iq, ixnum, key, keylen, last, lastrrn,
                       lastgenid, fndkey, fndrrn, genid, fnddta, fndlen, maxlen,
                       0, NULL, NULL, NULL, NULL, NULL, context, trans, NULL);
}

static int ix_prev_int(int auxdb, int lookahead, struct ireq *iq, int ixnum,
                       void *key, int keylen, void *last, int lastrrn,
                       unsigned long long lastgenid, void *fndkey, int *fndrrn,
                       unsigned long long *genid, void *fnddta, int *fndlen,
                       int maxlen, int numblobs, int *blobnums,
                       size_t *blobsizes, size_t *bloboffs, void **blobptrs,
                       int *retries, unsigned long long context,
                       bdb_cursor_ser_t *cur_ser)
{
    struct dbtable *db = iq->usedb;
    char *req;
    int ixrc, bdberr, lcl_retries;
    void *bdb_handle;
    void *curlast = last;
    int numskips = 0;
    bdb_fetch_args_t args = {0};
    if (!auxdb && (iq->usedb->ix_disabled[ixnum] & INDEX_READ_DISABLED)) {
        if (iq->debug)
            reqprintf(iq, "ix_prev_blobs_auxdb: ix %d read disabled", ixnum);
        return ERR_INDEX_DISABLED;
    }
    bdb_handle = get_bdb_handle(db, auxdb);
    iq->gluewhere = "ix_prev_blobs_auxdb";
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    if (!retries) {
        retries = &lcl_retries;
        lcl_retries = 0;
    }
retry:
    if (fnddta) {
        if (lookahead) {
            if (cur_ser)
                bdb_cursor_ser_invalidate(cur_ser);

            if (numblobs > 0) {
                /* convert blob numbers to data file numbers */
                int blobns[MAXBLOBS], blobn;
                if (numblobs > MAXBLOBS) {
                    return ERR_INTERNAL;
                }
                for (blobn = 0; blobn < numblobs; blobn++)
                    blobns[blobn] = blobnums[blobn] + 1;

                iq->gluewhere = req = "bdb_fetch_prev_blobs_genid";
                ixrc = bdb_fetch_prev_blobs_genid(
                    bdb_handle, key, ixnum, keylen, curlast, lastrrn, lastgenid,
                    fnddta, maxlen, fndlen, fndkey, fndrrn, genid, numblobs,
                    blobns, blobsizes, bloboffs, blobptrs, &args, &bdberr);
                iq->gluewhere = "bdb_fetch_prev_blobs_genid done";
            } else {
                iq->gluewhere = req = "bdb_fetch_prev_genid";
                ixrc = bdb_fetch_prev_genid(bdb_handle, key, ixnum, keylen,
                                            curlast, lastrrn, lastgenid, fnddta,
                                            maxlen, fndlen, fndkey, fndrrn,
                                            genid, &args, &bdberr);
                iq->gluewhere = "bdb_fetch_prev_genid done";
            }
        } else if (cur_ser) {
            iq->gluewhere = req = "bdb_fetch_prev_genid_nl_ser";
            ixrc = bdb_fetch_prev_genid_nl_ser(
                bdb_handle, key, ixnum, keylen, curlast, lastrrn, lastgenid,
                fnddta, maxlen, fndlen, fndkey, fndrrn, genid, cur_ser, &args,
                &bdberr);
            iq->gluewhere = "bdb_fetch_prev_genid_nl_ser done";
        } else {
            iq->gluewhere = req = "bdb_fetch_prev_genid_nl";
            ixrc = bdb_fetch_prev_genid_nl(
                bdb_handle, key, ixnum, keylen, curlast, lastrrn, lastgenid,
                fnddta, maxlen, fndlen, fndkey, fndrrn, genid, &args, &bdberr);
            iq->gluewhere = "bdb_fetch_prev_genid_nl done";
        }
        VTAG(ixrc, db);
    } else if (cur_ser) {
        iq->gluewhere = req = "bdb_fetch_prev_nodta_genid_nl_ser";
        ixrc = bdb_fetch_prev_nodta_genid_nl_ser(
            bdb_handle, key, ixnum, keylen, curlast, lastrrn, lastgenid, fndkey,
            fndrrn, genid, cur_ser, &args, &bdberr);
        iq->gluewhere = "bdb_fetch_prev_nodta_genid_nl_ser done";
    } else {
        iq->gluewhere = req = "bdb_fetch_prev_nodta_genid";
        ixrc = bdb_fetch_prev_nodta_genid(bdb_handle, key, ixnum, keylen,
                                          curlast, lastrrn, lastgenid, fndkey,
                                          fndrrn, genid, &args, &bdberr);
        iq->gluewhere = "bdb_fetch_prev_nodta_genid done";
    }
    if (ixrc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            iq->retries++;
            if (++(*retries) < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* %s too much contention %d count %d\n", req, bdberr,
                   *retries);
            return ERR_INTERNAL;
        }
        return map_unhandled_bdb_rcode(req, bdberr, 0);
    }

    /* loop detector */
    if (context == (unsigned long long)-1) {
        logmsg(LOGMSG_FATAL, "COMDB2 BAD CONTEXT %llu, LOOPING ALERT\n", context);
        if (debug_switch_abort_on_invalid_context())
            abort();
    }

    /* find again if we want stable cursors and this record was added after
     * the search began. */
    if (ixrc >= 0 && ixrc <= 2 && context && genid && *genid != context &&
        !bdb_check_genid_is_older(bdb_handle, *genid, context)) {
        if (blobptrs) {
            int ii;
            for (ii = 0; ii < numblobs; ii++) {
                if (blobptrs[ii])
                    free(blobptrs[ii]);
            }
        }
        curlast = fndkey; /* don't find the same record over and over! */
        lastgenid = *genid;
        lastrrn = *fndrrn;
        *retries = 0;
        if (++numskips == gbl_maxcontextskips)
            logmsg(LOGMSG_ERROR, "*ERROR* %s skipped %d (too many) records\n", req, numskips);
        else
            goto retry;
    }
    return ixrc;
}

int ix_prev_auxdb(int auxdb, int lookahead, struct ireq *iq, int ixnum,
                  void *key, int keylen, void *last, int lastrrn,
                  unsigned long long lastgenid, void *fndkey, int *fndrrn,
                  unsigned long long *genid, void *fnddta, int *fndlen,
                  int maxlen, unsigned long long context)
{
    return ix_prev_blobs_auxdb(auxdb, lookahead, iq, ixnum, key, keylen, last,
                               lastrrn, lastgenid, fndkey, fndrrn, genid,
                               fnddta, fndlen, maxlen, 0, NULL, NULL, NULL,
                               NULL, NULL, context);
}

int ix_prev_blobs(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
                  int lastrrn, unsigned long long lastgenid, void *fndkey,
                  int *fndrrn, unsigned long long *genid, void *fnddta,
                  int *fndlen, int maxlen, int numblobs, int *blobnums,
                  size_t *blobsizes, size_t *bloboffs, void **blobptrs,
                  int *retries, unsigned long long context)
{
    return ix_prev_blobs_auxdb(AUXDB_NONE, 1, iq, ixnum, key, keylen, last,
                               lastrrn, lastgenid, fndkey, fndrrn, genid,
                               fnddta, fndlen, maxlen, numblobs, blobnums,
                               blobsizes, bloboffs, blobptrs, retries, context);
}

int ix_prev_blobs_auxdb(int auxdb, int lookahead, struct ireq *iq, int ixnum,
                        void *key, int keylen, void *last, int lastrrn,
                        unsigned long long lastgenid, void *fndkey, int *fndrrn,
                        unsigned long long *genid, void *fnddta, int *fndlen,
                        int maxlen, int numblobs, int *blobnums,
                        size_t *blobsizes, size_t *bloboffs, void **blobptrs,
                        int *retries, unsigned long long context)
{
    return ix_prev_int(auxdb, lookahead, iq, ixnum, key, keylen, last, lastrrn,
                       lastgenid, fndkey, fndrrn, genid, fnddta, fndlen, maxlen,
                       numblobs, blobnums, blobsizes, bloboffs, blobptrs,
                       retries, context, NULL);
}

int ix_prev(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
            int lastrrn, unsigned long long lastgenid, void *fndkey,
            int *fndrrn, unsigned long long *genid, void *fnddta, int *fndlen,
            int maxlen, unsigned long long context)
{
    return ix_prev_auxdb(AUXDB_NONE, 1, iq, ixnum, key, keylen, last, lastrrn,
                         lastgenid, fndkey, fndrrn, genid, fnddta, fndlen,
                         maxlen, context);
}

int ix_prev_nl_ser(struct ireq *iq, int ixnum, void *key, int keylen,
                   void *last, int lastrrn, unsigned long long lastgenid,
                   void *fndkey, int *fndrrn, unsigned long long *genid,
                   void *fnddta, int *fndlen, int maxlen,
                   unsigned long long context, bdb_cursor_ser_t *cur_ser)
{
    return ix_prev_int(AUXDB_NONE, 0, iq, ixnum, key, keylen, last, lastrrn,
                       lastgenid, fndkey, fndrrn, genid, fnddta, fndlen, maxlen,
                       0, NULL, NULL, NULL, NULL, NULL, context, cur_ser);
}

int ix_prev_nl(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
               int lastrrn, unsigned long long lastgenid, void *fndkey,
               int *fndrrn, unsigned long long *genid, void *fnddta,
               int *fndlen, int maxlen, unsigned long long context)
{
    return ix_prev_auxdb(AUXDB_NONE, 0, iq, ixnum, key, keylen, last, lastrrn,
                         lastgenid, fndkey, fndrrn, genid, fnddta, fndlen,
                         maxlen, context);
}

int ix_prev_rnum(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
                 int lastrrn, unsigned long long lastgenid, void *fndkey,
                 int *fndrrn, unsigned long long *genid, void *fnddta,
                 int *fndlen, int *recnum, int maxlen)
{
    struct dbtable *db = iq->usedb;
    char *req;
    int ixrc, bdberr, retries = 0;
    bdb_fetch_args_t args = {0};
    if ((iq->usedb->ix_disabled[ixnum] & INDEX_READ_DISABLED)) {
        if (iq->debug)
            reqprintf(iq, "ix_prev_rnum: ix %d read disabled", ixnum);
        return ERR_INDEX_DISABLED;
    }
retry:
    if (fnddta) {
        iq->gluewhere = req = "bdb_fetch_prev_recnum";
        ixrc = bdb_fetch_prev_recnum_genid(
            db->handle, key, ixnum, keylen, last, lastrrn, lastgenid, fnddta,
            maxlen, fndlen, fndkey, fndrrn, recnum, genid, &args, &bdberr);
        VTAG(ixrc, db);
        iq->gluewhere = "bdb_fetch_prev_recnum done";
    } else {
        iq->gluewhere = req = "bdb_fetch_prev_nodta_recnum";
        ixrc = bdb_fetch_prev_nodta_recnum_genid(
            db->handle, key, ixnum, keylen, last, lastrrn, lastgenid, fndkey,
            fndrrn, recnum, genid, &args, &bdberr);
        iq->gluewhere = "bdb_fetch_prev_nodta_recnum done";
    }
    if (ixrc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* %s too much contention %d count %d\n", req, bdberr,
                   retries);
            return ERR_INTERNAL;
        }
        return map_unhandled_bdb_rcode(req, bdberr, 0);
    }
    return ixrc;
}

int dtas_next(struct ireq *iq, const unsigned long long *genid_vector,
              unsigned long long *genid, int *stripe, int stay_in_stripe,
              void *dta, void *trans, int dtalen, int *reqdtalen, int *ver)
{
    struct dbtable *db = iq->usedb;
    int bdberr, retries = 0, rc;
    bdb_fetch_args_t args = {0};
retry:
    iq->gluewhere = "bdb_fetch_next_dtastripe_record";
    rc = bdb_fetch_next_dtastripe_record(db->handle, genid_vector, genid,
                                         stripe, stay_in_stripe, dta, dtalen,
                                         reqdtalen, trans, &args, &bdberr);
    iq->gluewhere = "bdb_fetch_next_dtastripe_record done";
    if (rc == 0) {
        vtag_to_ondisk_vermap(iq->usedb, dta, reqdtalen, args.ver);
        if (ver != NULL)
            *ver = args.ver;
        return rc;
    }
    if (rc == 1) {
        return rc;
    }
    if (bdberr == BDBERR_DEADLOCK) {
        iq->retries++;
        /* don't retry if we're in the middle of a transaction */
        if (trans)
            return RC_INTERNAL_RETRY;
        if (++retries < gbl_maxretries) {
            n_retries++;
            goto retry;
        }
        logmsg(LOGMSG_ERROR, "*ERROR* bdb_fetch_next_dtastripe_record too much contention %d "
               "count %d\n",
               bdberr, retries);
        return ERR_INTERNAL;
    }
    logmsg(LOGMSG_ERROR, "*ERROR* bdb_fetch_next_dtastripe_record unhandled rcode %d\n",
           bdberr);
    return -1;
}

/* Get the next record in the database in one of the stripes.  Returns 0 on
 * success, 1 if there are no more records, -1 on failure. */
int dat_highrrn(struct ireq *iq, int *out_highrrn)
{
    *out_highrrn = 2;
    return -1;
}

int dat_numrrns(struct ireq *iq, int *out_numrrns)
{
    *out_numrrns = 0;
    return -1;
}

/* callback to report new master */
static int new_master_callback(void *bdb_handle, char *host)
{
    ++gbl_master_changes;
    struct dbenv *dbenv;
    char *oldmaster;
    int trigger_timepart = 0;
    dbenv = bdb_get_usr_ptr(bdb_handle);
    oldmaster = dbenv->master;
    dbenv->master = host;

    /*this is only used when handle not established yet. */
    if (host == gbl_mynode) {
        if (oldmaster != host) {
            logmsg(LOGMSG_WARN, "I AM NEW MASTER NODE %s\n", host);
            gbl_master_changes++;

            /* if there is an active schema changes, resume it */
            if (gbl_ready && resume_schema_change()) {
                logmsg(LOGMSG_ERROR, "failed trying to resume schema change, if "
                        "one was in progress it will have to be restarted\n");
            }
            load_auto_analyze_counters();
            trigger_clear_hash();
            trigger_timepart = 1;
        }
        ctrace("I AM NEW MASTER NODE %s\n", host);
        /*bdb_set_timeout(bdb_handle, 30000000, &bdberr);*/

        /* trigger old file recollect */
        gbl_master_changed_oldfiles = 1;
    } else {
        if (oldmaster != host)
            logmsg(LOGMSG_WARN, "NEW MASTER NODE %s\n", host);
        /*bdb_set_timeout(bdb_handle, 0, &bdberr);*/
    }

    gbl_lost_master_time = 0; /* reset this */

    /* fudge around my lockless access to gbl_master_changes */
    MEMORY_SYNC;

    osql_checkboard_check_master_changed(dbenv->master);

    /* inform watcher that we have a new master !*/
    if(trigger_timepart)
        gbl_trigger_timepart = 1;

    return 0;
}

static int threaddump_callback(void)
{
    thrman_dump();
    return 0;
}

/* callback to accept application socket */
static int appsock_callback(void *bdb_handle, SBUF2 *sb)
{
    struct dbenv *dbenv;

    dbenv = bdb_get_usr_ptr(bdb_handle);
    appsock_handler_start(dbenv, sb);

    return 0;
}

/* callback to do serializable transaction range check */
int serial_check_callback(char *tbname, int idxnum, void *key, int keylen,
                          void *ranges)
{
    CurRangeArr *arr = ranges;
    struct serial_tbname_hash *th;
    struct serial_index_hash *ih;
    int i;

    if (arr->size == 0) {
        return 0;
    }
    if ((th = hash_find(arr->hash, &(tbname))) == NULL) {
        return 0;
    }
    if (th->islocked) {
        return 1;
    }
    if (!key) {
        return 0;
    }
    if ((ih = hash_find(th->idx_hash, &(idxnum))) == NULL) {
        return 0;
    }
    for (i = ih->begin; i <= ih->end; i++) {
        CurRange *r = arr->ranges[i];
        if ((r->lflag ||
             memcmp(r->lkey, key,
                    (r->lkeylen < keylen ? r->lkeylen : keylen)) <=
                 0) && // lbound <= key
            (r->rflag ||
             memcmp(key, r->rkey,
                    (r->rkeylen < keylen ? r->rkeylen : keylen)) <=
                 0)) { // key <= rbound
            return 1;
        }
    }
    return 0;
}

int getroom_callback(void *dummy, const char *host) { return machine_dc(host); }

/* callback to report whether node is up or down through rtcpu */
static int nodeup_callback(void *bdb_handle, const char *host)
{
    return is_node_up(host);
}

static char *tcmtest_routecpu_down_node = 0;

void tcmtest_routecpu_set_down_node(char *n) { tcmtest_routecpu_down_node = n; }

int is_node_up(const char *host)
{
    int rc;

    if (gbl_rtcpu_debug && CLASS_TEST == get_mach_class(machine())) {
        /* For debugging rtcpu problems use an "alternative" rtcpu system.
         * Basically look for a file in /bbsrc/db/comdb2/rtcpu - if a file
         * for the node exists, then it is considered rt'd off. */
        char path[64];
        struct stat st;
        int nodeup = 1;
        snprintf0(path, sizeof(path), "/bbsrc/db/comdb2/rtcpu/%s", host);
        errno = 0;
        stat(path, &st);
        if (errno == 0)
            nodeup = 0;
        else if (errno != ENOENT)
            logmsg(LOGMSG_ERROR, "nodeup_callback: %s: %s\n", path, strerror(errno));
        return nodeup;
    }

    if ((tcmtest_routecpu_down_node > 0) &&
        (host == tcmtest_routecpu_down_node)) {
        /* keep chatty if we're forcing a node down to debug something */
        logmsg(LOGMSG_WARN, "%s returning bad-rcode for tcm-test node %s\n",
                __func__, host);
        return 0;
    }

    rc = machine_is_up(host);
    return (rc == 1);
    /*rc could be -1 which means bad node, just return down in that case */
}

/* callback to set dynamically configurable election settings */
static int electsettings_callback(void *bdb_handle, int *elect_time_microsecs)
{
    if (elect_time_microsecs)
        *elect_time_microsecs = get_elect_time_microsecs();
    return 0;
}

/*status of sync subsystem */
void backend_sync_stat(struct dbenv *dbenv)
{
    if (dbenv->log_sync)
        logmsg(LOGMSG_USER, "FULL TRANSACTION LOG SYNC ENABLED\n");

    switch (dbenv->rep_sync) {
    default:
        logmsg(LOGMSG_USER, "ASYNCHRONOUS MODE\n");
        break;
    case REP_SYNC_SOURCE:
        logmsg(LOGMSG_USER, "SOURCE MACHINE CACHE COHERENCY\n");
        break;
    case REP_SYNC_FULL:
        logmsg(LOGMSG_USER, "FULL CLUSTER CACHE COHERENCY\n");
        break;
    }
    if (!dbenv->log_delete)
        logmsg(LOGMSG_USER, "LOG DELETE DISABLED\n");
    else if (dbenv->log_delete_filenum < 0)
        logmsg(LOGMSG_USER, "LOG DELETE ENABLED\n");
    else
        logmsg(LOGMSG_USER, "LOG DELETE ENABLED only up to and including log.%010d\n",
               dbenv->log_delete_filenum);
    if (dbenv->log_delete_age > 0) {
        struct tm tm;
        time_t secs;
        char buf[64];
        secs = dbenv->log_delete_age;
        localtime_r(&secs, &tm);
        snprintf(buf, sizeof(buf), "%02d/%02d %02d:%02d:%02d", tm.tm_mon + 1,
                 tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_mday);
        logmsg(LOGMSG_USER, "LOG DELETE POLICY: delete log files predating %s\n", buf);
    } else {
        logmsg(LOGMSG_USER, "LOG DELETE POLICY: delete all eligible log files\n");
    }
}

/*just update the sync config */
void backend_update_sync(struct dbenv *dbenv)
{
    if (dbenv->bdb_attr == 0)
        return; /*skip */
    bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_SYNCTRANSACTIONS, dbenv->log_sync);
    bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_LOGDELETEAGE,
                 dbenv->log_delete
                     ? (dbenv->log_delete_age > 0 ? dbenv->log_delete_age
                                                  : LOGDELETEAGE_NOW)
                     : LOGDELETEAGE_NEVER);
    bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_LOGDELETELOWFILENUM,
                 dbenv->log_delete_filenum);
    backend_sync_stat(dbenv);
}

void net_quiesce_threads(void *hndl, void *uptr, char *fromnode, int usertype,
                         void *dta, int dtalen, uint8_t is_tcp)
{
    stop_threads(thedb);
    net_ack_message(hndl, 0);
}

void net_resume_threads(void *hndl, void *uptr, char *fromnode, int usertype,
                        void *dta, int dtalen, uint8_t is_tcp)
{
    resume_threads(thedb);
    net_ack_message(hndl, 0);
}

/* yuk. */
static int decode_schema_net_msg(void *hndl, void *dtap, int dtalen,
                                 char **table, char **csc2, char **fname,
                                 char **aname)
{
    int tlen, flen;
    char *dta = (char *)dtap;
    char **fvar = NULL;
    int offset;
    int origlen;

    *table = NULL;
    *csc2 = NULL;
    *fname = NULL;
    *aname = NULL;

    if (dtalen < 8) {
        net_ack_message(hndl, 1);
        return -1;
    }

    memcpy(&tlen, dta, sizeof(int));
    memcpy(&flen, dta + sizeof(int), sizeof(int));

    if (dtalen < 2 * sizeof(int) + tlen + flen) {
        net_ack_message(hndl, 1);
        return -1;
    }

    /* length of original data before I added the advisory file name */
    origlen = 2 * sizeof(int) + tlen + flen;

    if (flen > 0) {
        offset = 2 * sizeof(int) + tlen;
        if (flen >= 6 && memcmp(dta + offset, "<CSC2>", 6) == 0) {
            flen -= 6;
            offset += 6;
            fvar = csc2;
        } else {
            fvar = fname;
        }
        *fvar = malloc(flen + 1);
        if (!*fvar) {
            logmsg(LOGMSG_ERROR, "decode_schema_net_msg: out of memory\n");
            net_ack_message(hndl, 1);
            return -1;
        }
        memcpy(*fvar, dta + offset, flen);
        (*fvar)[flen] = '\0';
    }

    *table = malloc(tlen + 1);
    if (!*table) {
        if (*fvar)
            free(*fvar);
        logmsg(LOGMSG_ERROR, "decode_schema_net_msg: out of memory\n");
        net_ack_message(hndl, 1);
        return -1;
    }
    memcpy(*table, dta + 2 * sizeof(int), tlen);
    (*table)[tlen] = '\0';

    if (dtalen > origlen) {
        /* the extra data is the advised file name. */
        int anamelen = dtalen - origlen;
        *aname = malloc(anamelen + 1);
        if (!*aname) {
            logmsg(LOGMSG_ERROR, "decode_schema_net_msg: out of memory\n");
            if (*fvar)
                free(*fvar);
            free(*table);
            net_ack_message(hndl, 1);
            return -1;
        }
        memcpy(*aname, dta + origlen, anamelen);
        (*aname)[anamelen] = '\0';
    }

    return 0;
}

void net_reload_schemas(void *hndl, void *uptr, char *fromnode, int usertype,
                        void *dtap, int dtalen)
{
    char *table;
    char *csc2;
    char *fname;
    char *aname;
    int rc;
    int rc2;

    rc = decode_schema_net_msg(hndl, dtap, dtalen, &table, &csc2, &fname,
                               &aname);
    if (rc != 0)
        return;

    if (fname || aname) {
        logmsg(LOGMSG_ERROR, "%s: fname and aname no longer supported\n", __func__);

        net_ack_message(hndl, 1);
        if (table)
            free(table);
        if (csc2)
            free(csc2);
        if (fname)
            free(fname);
        if (aname)
            free(aname);
        return;
    }

    rc = reload_schema(table, csc2, NULL);

    rc2 = create_sqlmaster_records(NULL);
    create_sqlite_master(); /* create sql statements */

    net_ack_message(hndl, rc || rc2);

    if (table)
        free(table);
    if (csc2)
        free(csc2);
}

void net_close_db(void *hndl, void *uptr, char *fromnode, int usertype,
                  void *dtap, int dtalen)
{
    int len, free_handle;
    char table[MAXTABLELEN];
    char *dta = (char *)dtap;
    struct dbtable *db;
    int bdberr;

    memset(table, 0, sizeof(table));
    if (dtalen < 2 * sizeof(int)) {
        net_ack_message(hndl, 1);
        return;
    }
    memcpy(&len, dta, sizeof(int));
    if (dtalen < 2 * sizeof(int) + len) {
        net_ack_message(hndl, 1);
        return;
    }
    memcpy(table, dta + sizeof(int), len);
    memcpy(&free_handle, dta + sizeof(int) + len, sizeof(int));
    logmsg(LOGMSG_DEBUG, "table %s free_handle %d\n", table, free_handle);

    db = get_dbtable_by_name(table);
    logmsg(LOGMSG_DEBUG, "net_close_db get_dbtable_by_name 0x%08x\n", db);
    if (db == NULL) {
        net_ack_message(hndl, 1);
        return;
    }

    bdb_close_only(db->handle, &bdberr);
    logmsg(LOGMSG_DEBUG, "net_close_db bdb_close_only %d\n", bdberr);
    if (free_handle) {
        bdb_free(db->handle, &bdberr);
        db->handle = NULL;
        logmsg(LOGMSG_DEBUG, "net_close_db bdb_free %d\n", bdberr);
    }
    if (net_ack_message(hndl, 0)) {
        logmsg(LOGMSG_DEBUG, 
               "net_close_db: Error sending back the acknoledgement\n");
    }
}

static void net_close_all_dbs(void *hndl, void *uptr, char *fromnode,
                              int usertype, void *dtap, int dtalen,
                              uint8_t is_tcp)
{
    int rc;
    rc = close_all_dbs();
    net_ack_message(hndl, rc == 0 ? 0 : 1);
}

struct start_sc {
    uint64_t seed;
    int64_t time;
    char host[1];
};

static void net_start_sc(void *hndl, void *uptr, char *fromnode, int usertype,
                         void *dtap, int dtalen, uint8_t is_tcp)
{
    int rc;
    struct start_sc *sc;

    sc = (struct start_sc *)dtap;
    sc->seed = flibc_ntohll(sc->seed);
    sc->time = flibc_ntohll(sc->time);

    rc = sc_set_running(1, sc->seed, sc->host, sc->time);
    net_ack_message(hndl, rc == 0 ? 0 : 1);
}

static void net_stop_sc(void *hndl, void *uptr, char *fromnode, int usertype,
                        void *dtap, int dtalen, uint8_t is_tcp)
{
    int rc;
    uint64_t *seed;
    if (dtalen != sizeof(uint64_t)) {
        net_ack_message(hndl, 1);
        return;
    }
    seed = dtap;
    rc = sc_set_running(0, 0, NULL, 0);
    net_ack_message(hndl, rc == 0 ? 0 : 1);
}

static void net_check_sc_ok(void *hndl, void *uptr, char *fromnode,
                            int usertype, void *dtap, int dtalen,
                            uint8_t is_tcp)
{
    int rc;
    rc = check_sc_ok(NULL);
    net_ack_message(hndl, rc == 0 ? 0 : 1);
}

static void net_lua_reload(void *hndl, void *uptr, char *fromnode, int usertype,
                           void *dtap, int dtalen)
{
    gbl_analyze_gen++;
    net_ack_message(hndl, 0);
}

static void net_statistics_changed(void *hndl, void *uptr, char *fromnode,
                                   int usertype, void *dtap, int dtalen)
{
    gbl_analyze_gen++;
    net_ack_message(hndl, 0);
}

static void net_flush_all(void *hndl, void *uptr, char *fromnode, int usertype,
                          void *dtap, int dtalen, uint8_t is_tcp)
{
    logmsg(LOGMSG_DEBUG, "Received NET_FLUSH_ALL\n");
    if (!thedb || thedb->stopped || gbl_exit || !gbl_ready) {
        logmsg(LOGMSG_WARN, "I am not ready, ignoring NET_FLUSH_ALL\n");
        return;
    }
    flush_db();
    net_ack_message(hndl, 0);
}

static void net_morestripe_and_open_all_dbs(void *hndl, void *uptr,
                                            char *fromnode, int usertype,
                                            void *dtap, int dtalen,
                                            uint8_t is_tcp)
{
    int rc;
    struct net_morestripe_msg *msg;
    msg = dtap;

    if (dtalen < sizeof(struct net_morestripe_msg)) {
        logmsg(LOGMSG_ERROR, "net_morestripe_and_open_all_dbs: bad msglen %d\n",
                dtalen);
        net_ack_message(hndl, 1);
        return;
    }

    apply_new_stripe_settings(msg->newdtastripe, msg->newblobstripe);

    rc = open_all_dbs();
    if (rc != 0) {
        net_ack_message(hndl, 1);
        return;
    }

    fix_blobstripe_genids();
    net_ack_message(hndl, 0);
}

void net_new_queue(void *hndl, void *uptr, char *fromnode, int usertype,
                   void *dtap, int dtalen, uint8_t is_tcp)
{
    struct net_new_queue_msg *msg = dtap;
    int rc;

    if (dtalen != sizeof(struct net_new_queue_msg)) {
        net_ack_message(hndl, 1);
        return;
    }

    /* just to make sure */
    msg->name[sizeof(msg->name) - 1] = '\0';
    rc = add_queue_to_environment(msg->name, msg->avgitemsz, 0);
    net_ack_message(hndl, rc);
}

void net_javasp_op(void *hndl, void *uptr, char *fromnode, int usertype,
                   void *dtap, int dtalen, uint8_t is_tcp)
{
    struct new_procedure_op_msg *msg = dtap;
    char *name;
    char *jarfile;
    char *param;
    char *ptr;
    int rc;

    if (dtalen < offsetof(struct new_procedure_op_msg, text)) {
        net_ack_message(hndl, 1);
        return;
    }

    if (dtalen !=
        offsetof(struct new_procedure_op_msg, text) + msg->namelen +
            msg->jarfilelen + msg->paramlen) {
        net_ack_message(hndl, 1);
        return;
    }

    ptr = msg->text;

    name = calloc(1, msg->namelen + 1);
    memcpy(name, ptr, msg->namelen);
    ptr += msg->namelen;

    ptr += msg->jarfilelen;

    param = calloc(1, msg->paramlen + 1);
    memcpy(param, ptr, msg->paramlen);
    ptr += msg->paramlen;

    rc = javasp_do_procedure_op(msg->op, name, param, NULL);

    free(name);
    free(param);

    net_ack_message(hndl, rc);
}

void net_prefault_ops(void *hndl, void *uptr, char *fromnode, int usertype,
                      void *dtap, int dtalen, uint8_t is_tcp)
{
}

int process_broadcast_prefault(struct dbenv *dbenv, unsigned char *dta,
                               int dtalen, int is_tcp);

void net_prefault2_ops(void *hndl, void *uptr, char *fromnode, int usertype,
                       void *dta, int dtalen, uint8_t is_tcp)
{
    process_broadcast_prefault(thedb, dta, dtalen, is_tcp);
}

void net_add_consumer(void *hndl, void *uptr, char *fromnode, int usertype,
                      void *dtap, int dtalen, uint8_t is_tcp)
{
    struct net_add_consumer_msg *msg = dtap;
    int rc;
    struct dbtable *db;

    if (dtalen != sizeof(struct net_add_consumer_msg)) {
        net_ack_message(hndl, 1);
        return;
    }

    /* paranoia */
    msg->name[sizeof(msg->name) - 1] = '\0';
    msg->method[sizeof(msg->method) - 1] = '\0';

    db = getqueuebyname(msg->name);
    if (!db) {
        net_ack_message(hndl, 1);
        return;
    }

    rc = dbqueue_add_consumer(db, msg->consumern, msg->method, 0);
    fix_consumers_with_bdblib(thedb);
    net_ack_message(hndl, rc);
}

static void net_forgetmenot(void *hndl, void *uptr, char *fromnode,
                            int usertype, void *dtap, int dtalen,
                            uint8_t is_tcp)
{

    /* if this arrives too early, it will crash the master */
    if (thedb->stopped || gbl_exit || !gbl_ready) {
        logmsg(LOGMSG_ERROR, "%s: received trap during lunch time\n", __func__);
        return;
    }

    /*
       Not needed  apparently
       bdb_remind_incoherent(thedb->bdb_env, fromnode);

       doesn't need an ack */
}

static void net_trigger_register(void *hndl, void *uptr, char *fromnode,
                                 int usertype, void *dtap, int dtalen,
                                 uint8_t _)
{
    int rc = trigger_register(dtap);
    if (hndl)
        net_ack_message(hndl, rc);
}

static void net_trigger_unregister(void *hndl, void *uptr, char *fromnode,
                                   int usertype, void *dtap, int dtalen,
                                   uint8_t _)
{
    int rc = trigger_unregister(dtap);
    if (hndl)
        rc = net_ack_message(hndl, rc);
}

static void net_trigger_start(void *hndl, void *uptr, char *fromnode,
                              int usertype, void *dtap, int dtalen, uint8_t _)
{
    trigger_start(dtap);
}

static void net_authentication_check(void *hndl, void *uptr, char *fromhost,
                             int usertype, void *dtap, int dtalen,
                             uint8_t is_tcp)
{
    gbl_check_access_controls = 1;
}


int send_to_all_nodes(void *dta, int len, int type, int waittime)
{
    const char *machs[REPMAX];
    int nnodes;
    int node;
    int rc;
    int failed = 0;
    int delay, waitforack;
    if (waittime > 0) {
        delay = waittime;
        waitforack = 1;
    } else {
        delay = 0;
        waitforack = 0;
    }

    nnodes = net_get_all_nodes(thedb->handle_sibling, machs);
    for (node = 0; node < nnodes; node++) {
        rc = net_send_message(thedb->handle_sibling, machs[node], type, dta,
                              len, waitforack, delay);
        if (rc)
            failed++;
    }
    return failed;
}

#define MSGWAITTIME 300000 /* tunable? */
#define SCWAITTIME 1000

/* Send an async message to the master node reminding it that I appear to be
 * incoherent and would it kindly let me know if this isn't th case anymore. */
int send_forgetmenot(void)
{
    char *master = thedb->master;
    if (master > 0)
        return net_send_message(thedb->handle_sibling, master, NET_FORGETMENOT,
                                NULL, 0, 0, 0);
    else
        return -1;
}

int broadcast_quiesce_threads(void)
{
    int dummy_msg = 0;

    return send_to_all_nodes(&dummy_msg, sizeof(int), NET_QUIESCE_THREADS,
                             MSGWAITTIME);
}

int broadcast_resume_threads(void)
{
    int dummy_msg = 0;

    return send_to_all_nodes(&dummy_msg, sizeof(int), NET_RESUME_THREADS,
                             MSGWAITTIME);
}

int broadcast_close_all_dbs(void)
{
    return send_to_all_nodes(NULL, 0, NET_CLOSE_ALL_DBS, MSGWAITTIME);
}

int broadcast_sc_end(uint64_t seed)
{
    return send_to_all_nodes(&seed, sizeof(seed), NET_STOP_SC, SCWAITTIME);
}

const char *get_hostname_with_crc32(bdb_state_type *bdb_state,
                                    unsigned int hash);
int broadcast_sc_start(uint64_t seed, uint32_t host, time_t t)
{
    struct start_sc *sc;
    int len;
    const char *from = get_hostname_with_crc32(thedb->bdb_env, host);
    if (from == NULL) {
        from = "unknown";
    }

    len = offsetof(struct start_sc, host) + strlen(from) + 1;

    sc = alloca(len);
    sc->seed = flibc_htonll(seed);
    sc->time = flibc_htonll(t);
    strcpy(sc->host, intern(from));

    return send_to_all_nodes(sc, len, NET_START_SC, SCWAITTIME);
}

int broadcast_sc_ok(void)
{
    return send_to_all_nodes(NULL, 0, NET_CHECK_SC_OK, SCWAITTIME);
}

int broadcast_morestripe_and_open_all_dbs(int newdtastripe, int newblobstripe)
{
    struct net_morestripe_msg msg;
    bzero(&msg, sizeof(msg));
    msg.newdtastripe = newdtastripe;
    msg.newblobstripe = newblobstripe;
    return send_to_all_nodes(&msg, sizeof(msg), NET_MORESTRIPE_OPEN_DBS,
                             MSGWAITTIME);
}

int broadcast_procedure_op(int op, const char *name, const char *param)
{
    struct new_procedure_op_msg *msg;
    int namelen, paramlen;
    char *ptr;
    int len;

    namelen = strlen(name);
    paramlen = strlen(param);

    len = offsetof(struct new_procedure_op_msg, text) + namelen + paramlen;
    msg = malloc(len);
    msg->reserved = 0;
    msg->op = op;
    msg->namelen = namelen;
    msg->jarfilelen = 0;
    msg->paramlen = paramlen;

    ptr = msg->text;
    memcpy(ptr, name, namelen);
    ptr += namelen;
    memcpy(ptr, param, paramlen);
    ptr += paramlen;

    return send_to_all_nodes(msg, len, NET_JAVASP_OP, MSGWAITTIME);
}

int broadcast_add_new_queue(char *table, int avgitemsz)
{
    /* TODO:
       * boundary check limits, lengths (also in other broadcast routines)
       * what if this fails??? - backout is very hard
     */
    struct net_new_queue_msg msg;
    msg.reserved = 0;
    strncpy0(msg.name, table, sizeof(msg.name));
    msg.avgitemsz = avgitemsz;
    return send_to_all_nodes(&msg, sizeof(msg), NET_NEW_QUEUE, MSGWAITTIME);
}

int broadcast_flush_all(void)
{
    int i = 0;
    return send_to_all_nodes(&i, sizeof(int), NET_FLUSH_ALL, MSGWAITTIME);
}

int broadcast_add_consumer(const char *queuename, int consumern,
                           const char *method)
{
    struct net_add_consumer_msg msg;
    msg.reserved = 0;
    msg.consumern = consumern;
    strncpy0(msg.name, queuename, sizeof(msg.name));
    strncpy0(msg.method, method, sizeof(msg.method));
    return send_to_all_nodes(&msg, sizeof(msg), NET_ADD_CONSUMER, MSGWAITTIME);
}

/* Return 1 if we should allow connections from/to the given node, return 0
 * if we should not.  The policy is not to allow clusters to span production
 * and development. */
int net_allow_node(struct netinfo_struct *netinfo_ptr, const char *host)
{
    if (allow_cluster_from_remote(host))
        return 1;
    else
        return 0;
}

int open_auxdbs(struct dbtable *db, int force_create)
{
    int numdtafiles;
    int numix;
    short ixlen[1];
    signed char ixdups[1];
    signed char ixrecnum[1];
    signed char ixdta[1];
    char name[100];
    char litename[100];
    int bdberr;

    /* if we have a singlemeta then no need to open another meta. */
    if (thedb->meta)
        return 0;

    /* meta information dbs.  we need to make sure that lite meta tables
     * are named differently to heavy meta tables otherwise we can't tell
     * them apart at startup.. */
    if (gbl_nonames) {
        snprintf(name, sizeof(name), "comdb2_meta");
        snprintf(litename, sizeof(litename), "comdb2_metalite");
    } else {
        snprintf(name, sizeof(name), "%s.meta", db->dbname);
        snprintf(litename, sizeof(litename), "%s.metalite", db->dbname);
    }

    ctrace("bdb_open_more: opening <%s>\n", name);
    numdtafiles = 1;
    numix = 1;
    /* key = rrn + attribute
       data = blob of data associated with attribute */
    ixlen[0] = 8;
    ixdups[0] = 0;
    ixrecnum[0] = 0;
    ixdta[0] = 0;

    if (force_create) {
        if (gbl_meta_lite)
            db->meta =
                bdb_create_more_lite(litename, db->dbenv->basedir, 0, ixlen[0],
                                     0, db->dbenv->bdb_env, &bdberr);
        else
            db->meta = bdb_create(name, db->dbenv->basedir, 0, numix, ixlen,
                                  ixdups, ixrecnum, ixdta, NULL, NULL,
                                  numdtafiles, db->dbenv->bdb_env, 0, &bdberr);
    } else {
        /* see if we have a lite meta table - if so use that.  otherwise
         * fallback on a heavy meta table. */
        db->meta = bdb_open_more_lite(litename, db->dbenv->basedir, 0, ixlen[0],
                                      0, db->dbenv->bdb_env, &bdberr);
        if (!db->meta) {
            if (gbl_meta_lite)
                ctrace("bdb_open_more(meta) cannot open lite meta %d\n",
                       bdberr);
            db->meta = bdb_open_more(name, db->dbenv->basedir, 0, numix, ixlen,
                                     ixdups, ixrecnum, ixdta, NULL, NULL,
                                     numdtafiles, db->dbenv->bdb_env, &bdberr);
        }
    }
    if (db->meta == NULL) {
        logmsg(LOGMSG_ERROR, "bdb_open_more(meta) bdberr %d\n", bdberr);
    }
    if (db->meta)
        return 0;
    else
        return -1;
}

void comdb2_net_start_thread(void *opaque)
{
    backend_thread_event((struct dbenv *)opaque, 1);
}

void comdb2_net_stop_thread(void *opaque)
{
    backend_thread_event((struct dbenv *)opaque, 0);
}

int open_bdb_env(struct dbenv *dbenv)
{
    int ii, bdberr;
    void *rcv;

    /* Some sanity checks that ideally would be compile time */
    if (SIZEOF_SEQNUM != sizeof(db_seqnum_type)) {
        logmsg(LOGMSG_FATAL, "open_bdb_env: sizeof(seqnum_type) != "
                        "sizeof(db_seqnum_type)!\n");
        exit(1);
    }

    dbenv->bdb_callback = bdb_callback_create();
    if (dbenv->bdb_callback == 0) {
        logmsg(LOGMSG_FATAL, "open_bdb_env:failed bdb_callback_create\n");
        return -1;
    }

    /* set attributes */
    bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_REPCHECKSUM, gbl_repchecksum);
    bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_CACHESIZE, dbenv->cacheszkb);
    bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_CREATEDBS, gbl_create_mode);
    bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_FULLRECOVERY, gbl_fullrecovery);

    bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_REPALWAYSWAIT,
                 dbenv->rep_always_wait);

    bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_DTASTRIPE, gbl_dtastripe);
    bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_BLOBSTRIPE, gbl_blobstripe);

    if (debug_switch_use_phase_3())
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_USEPHASE3, 1);
    else
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_USEPHASE3, 0);

    backend_update_sync(dbenv);

    /* set callbacks */
    bdb_callback_set(dbenv->bdb_callback, BDB_CALLBACK_WHOISMASTER,
                     new_master_callback);
    bdb_callback_set(dbenv->bdb_callback, BDB_CALLBACK_NODEUP, nodeup_callback);
    bdb_callback_set(dbenv->bdb_callback, BDB_CALLBACK_GETROOM,
                     getroom_callback);
    bdb_callback_set(dbenv->bdb_callback, BDB_CALLBACK_APPSOCK,
                     appsock_callback);
    bdb_callback_set(dbenv->bdb_callback, BDB_CALLBACK_PRINT,
                     (BDB_CALLBACK_FP)vctrace);
    bdb_callback_set(dbenv->bdb_callback, BDB_CALLBACK_ELECTSETTINGS,
                     electsettings_callback);
    bdb_callback_set(dbenv->bdb_callback, BDB_CALLBACK_THREADDUMP,
                     threaddump_callback);
    bdb_callback_set(dbenv->bdb_callback, BDB_CALLBACK_SCDONE, scdone_callback);
    bdb_callback_set(dbenv->bdb_callback, BDB_CALLBACK_SCABORT,
                     schema_change_abort_callback);
    bdb_callback_set(dbenv->bdb_callback, BDB_CALLBACK_NODE_IS_DOWN,
                     (BDB_CALLBACK_FP)osql_checkboard_check_down_nodes);
    bdb_callback_set(dbenv->bdb_callback, BDB_CALLBACK_SERIALCHECK,
                     serial_check_callback);
/*
    bdb_callback_set(dbenv->bdb_callback, BDB_CALLBACK_UNDOSERIAL,
            osql_checkboard_foreach_serial);
*/

#if 0
    bdb_callback_set(dbenv->bdb_callback, BDB_CALLBACK_CATCHUP, 
            catchup_callback);
#endif

    if (dbenv->sibling_hostname[0] == NULL)
        dbenv->sibling_hostname[0] = gbl_mynode;

    if (dbenv->nsiblings > 0) {
        /*zero element is always me. */
        dbenv->handle_sibling = (void *)create_netinfo(
            dbenv->sibling_hostname[0], dbenv->sibling_port[0][NET_REPLICATION],
            dbenv->listen_fds[NET_REPLICATION], "comdb2", "replication",
            dbenv->envname, 0, !gbl_disable_etc_services_lookup);
        if (dbenv->handle_sibling == NULL) {
            logmsg(LOGMSG_ERROR, 
                    "open_bdb_env:failed create_netinfo host %s port %d\n",
                    dbenv->sibling_hostname[0],
                    dbenv->sibling_port[0][NET_REPLICATION]);
            return -1;
        }

        dbenv->handle_sibling_offload = create_netinfo_offload(
            dbenv->sibling_hostname[0], dbenv->sibling_port[0][NET_SQL],
            dbenv->listen_fds[NET_SQL], "comdb2", "offloadsql", dbenv->envname);
        if (dbenv->handle_sibling_offload == NULL) {
            logmsg(LOGMSG_ERROR, 
                   "open_bdb_env:failed create_netinfo host %s port %d\n",
                   dbenv->sibling_hostname[0], dbenv->sibling_port[0][NET_SQL]);
            return -1;
        }

        dbenv->handle_sibling_signal = (void *)create_netinfo(
            dbenv->sibling_hostname[0], dbenv->sibling_port[0][NET_SIGNAL],
            dbenv->listen_fds[NET_SIGNAL], "comdb2", "signal", dbenv->envname,
            1, 0);
        if (dbenv->handle_sibling_signal == 0) {
            logmsg(LOGMSG_ERROR, 
                "open_bdb_env:failed create_netinfo signal host %s port %d\n",
                dbenv->sibling_hostname[0], dbenv->sibling_port[0][NET_SIGNAL]);
            return -1;
        }

        net_set_pool_size(dbenv->handle_sibling, gbl_maxreclen + 300);
        net_set_pool_size(dbenv->handle_sibling_signal, gbl_maxreclen + 300);
        net_set_pool_size(dbenv->handle_sibling_offload, gbl_maxreclen + 300);

        net_register_child_net(dbenv->handle_sibling,
                               dbenv->handle_sibling_signal, NET_SIGNAL,
                               gbl_accept_on_child_nets);
        net_register_child_net(dbenv->handle_sibling,
                               dbenv->handle_sibling_offload, NET_SQL,
                               gbl_accept_on_child_nets);

        if (!gbl_accept_on_child_nets)
            net_set_portmux_register_interval(dbenv->handle_sibling_signal, 0);

        /* get the max rec len, or a sane default */
        gbl_maxreclen = get_max_reclen(dbenv);
        if (gbl_maxreclen == 0)
            gbl_maxreclen = 512;

#if 0
        net_set_callback_data(dbenv->handle_sibling, dbenv);
        net_register_start_thread_callback(dbenv->handle_sibling, comdb2_net_start_thread);
        net_register_stop_thread_callback(dbenv->handle_sibling, comdb2_net_stop_thread);
#endif
        for (ii = 1; ii < dbenv->nsiblings; ii++) {
            rcv = (void *)add_to_netinfo(
                dbenv->handle_sibling, intern(dbenv->sibling_hostname[ii]),
                dbenv->sibling_port[ii][NET_REPLICATION]);
            if (rcv == 0) {
                logmsg(LOGMSG_ERROR, 
                        "open_bdb_env:failed add_to_netinfo host %s port %d\n",
                        dbenv->sibling_hostname[ii],
                        dbenv->sibling_port[ii][NET_REPLICATION]);
                return -1;
            }

            rcv = (void *)add_to_netinfo(dbenv->handle_sibling_signal,
                                         intern(dbenv->sibling_hostname[ii]),
                                         dbenv->sibling_port[ii][NET_SIGNAL]);
            if (rcv == 0) {
                logmsg(LOGMSG_ERROR, 
                        "open_bdb_env:failed add_to_netinfo host %s port %d\n",
                        dbenv->sibling_hostname[ii],
                        dbenv->sibling_port[ii][NET_SIGNAL]);
                return -1;
            }
        }

        /* callbacks for schema changes */
        if (net_register_handler(dbenv->handle_sibling, NET_QUIESCE_THREADS,
                                 net_quiesce_threads))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_RESUME_THREADS,
                                 net_resume_threads))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_NEW_QUEUE,
                                 net_new_queue))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_ADD_CONSUMER,
                                 net_add_consumer))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_JAVASP_OP,
                                 net_javasp_op))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_PREFAULT_OPS,
                                 net_prefault_ops))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_PREFAULT2_OPS,
                                 net_prefault2_ops))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_CLOSE_ALL_DBS,
                                 net_close_all_dbs))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_MORESTRIPE_OPEN_DBS,
                                 net_morestripe_and_open_all_dbs))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_CHECK_SC_OK,
                                 net_check_sc_ok))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_START_SC,
                                 net_start_sc))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_STOP_SC,
                                 net_stop_sc))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_FLUSH_ALL,
                                 net_flush_all))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_FORGETMENOT,
                                 net_forgetmenot))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_TRIGGER_REGISTER,
                                 net_trigger_register))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_TRIGGER_UNREGISTER,
                                 net_trigger_unregister))
            return -1;
        if (net_register_handler(dbenv->handle_sibling, NET_TRIGGER_START,
                                 net_trigger_start))
            return -1;
        /* Authentication Check */
        if (net_register_handler(dbenv->handle_sibling, NET_AUTHENTICATION_CHECK,
                                 net_authentication_check))
            return -1;
        if (net_register_allow(dbenv->handle_sibling, net_allow_node))
            return -1;
        if (net_register_allow(dbenv->handle_sibling_signal, net_allow_node))
            return -1;
    }

    /* open environment */
    dbenv->bdb_env = bdb_open_env(
        dbenv->envname, dbenv->basedir, dbenv->bdb_attr, dbenv->bdb_callback,
        dbenv /* db */, dbenv->handle_sibling, dbenv->handle_sibling_signal,
        gbl_recovery_options, &bdberr);

    if (dbenv->bdb_env == NULL) {
        logmsg(LOGMSG_ERROR, "open_bdb_env failed bdb_open_env bdberr %d\n", bdberr);
        return -1;
    }

    /* Net has to be active before bdb_env, so we could have gotten some
     * interesting messages
     * that we couldn't process before.  For now, the only one of interest is
     * incoherent notifications
     * but there'll be others.  Process them here.  In The Old Days this
     * wouldn't happen because these
     * messages would be queue and come in via qtrap() which starts being
     * processed long after bdb_end
     * is ready. */
    return 0; /* success */
}

static int init_odh_lrl(struct dbtable *d, int *compr, int *compr_blobs,
                        int *datacopy_odh)
{
    /* new dbs always get datacopy_odh */
    *datacopy_odh = 1;
    if (put_db_datacopy_odh(d, NULL, *datacopy_odh) != 0)
        return -1;

    /* rest of the features depend on odh */
    if (gbl_init_with_odh == 0) {
        gbl_init_with_compr = 0;
        gbl_init_with_compr_blobs = 0;
        gbl_init_with_ipu = 0;
        gbl_init_with_instant_sc = 0;
    }
    if (put_db_odh(d, NULL, gbl_init_with_odh) != 0)
        return -1;
    if (put_db_compress(d, NULL, gbl_init_with_compr) != 0)
        return -1;
    if (put_db_compress_blobs(d, NULL, gbl_init_with_compr_blobs) != 0)
        return -1;
    if (put_db_inplace_updates(d, NULL, gbl_init_with_ipu) != 0)
        return -1;
    if (put_db_instant_schema_change(d, NULL, gbl_init_with_instant_sc) != 0)
        return -1;
    d->odh = gbl_init_with_odh;
    *compr = gbl_init_with_compr;
    *compr_blobs = gbl_init_with_compr_blobs;
    d->inplace_updates = gbl_init_with_ipu;
    d->instant_schema_change = gbl_init_with_instant_sc;
    return 0;
}

static int init_odh_llmeta(struct dbtable *d, int *compr, int *compr_blobs,
                           int *datacopy_odh)
{
    if (get_db_odh(d, &d->odh) != 0 || d->odh == 0) {
        // couldn't find odh in llmeta or odh off
        *compr = 0;
        *compr_blobs = 0;
        d->odh = 0;
        d->inplace_updates = 0;
        d->instant_schema_change = 0;
        *datacopy_odh = 0;
        return 0;
    }

    get_db_compress(d, compr);
    get_db_compress_blobs(d, compr_blobs);
    get_db_instant_schema_change(d, &d->instant_schema_change);
    get_db_inplace_updates(d, &d->inplace_updates);
    get_db_datacopy_odh(d, datacopy_odh);

    return 0;
}

// set to val the in-memory parameter for all the indices of this table
void set_skipscan_for_table_indices(struct dbtable *tbl, int val)
{
    for (int ii = 0; ii < tbl->nix; ii++) {
        struct schema *s = tbl->ixschema[ii];
        s->disableskipscan = val;
#ifdef DEBUGSKIPSCAN
        printf("%s: setting disableskipscan for %s.%s %d\n", __func__,
               tbl->dbname, s->sqlitetag, val);
#endif
    }
}


static void get_disable_skipscan(struct dbtable *tbl)
{
    if (tbl->dbtype != DBTYPE_UNTAGGED_TABLE &&
        tbl->dbtype != DBTYPE_TAGGED_TABLE)
        return;

    char *str = NULL;
    int rc = bdb_get_table_parameter(tbl->dbname, "disableskipscan", &str);
    if (rc != 0) {
        set_skipscan_for_table_indices(tbl, 0);
        return;
    }

    int disable = (strncmp(str, "true", 4) == 0);
    free(str);

    // set the in-memory parameter for the indices
    set_skipscan_for_table_indices(tbl, disable);
}


void get_disable_skipscan_all() 
{
#ifdef DEBUGSKIPSCAN
    logmsg(LOGMSG_WARN, "get_disable_skipscan_all() called\n");
#endif
    for (int ii = 0; ii < thedb->num_dbs; ii++) {
        struct dbtable *d = thedb->dbs[ii];
        get_disable_skipscan(d);
    }
}
 


/* open the db files, etc */
int backend_open(struct dbenv *dbenv)
{
    int bdberr, ii;
    struct dbtable *db;
    int rc;
    struct deferred_berkdb_option *opt;

    /* open tables */
    for (ii = 0; ii < dbenv->num_dbs; ii++) {
        db = dbenv->dbs[ii];

        if (db->dbnum)
            logmsg(LOGMSG_INFO, "open table '%s' (dbnum %d)\n", db->dbname, db->dbnum);
        else
            logmsg(LOGMSG_INFO, "open table '%s'\n", db->dbname);

        db->handle = bdb_open_more(
            db->dbname, dbenv->basedir, db->lrl, db->nix, db->ix_keylen,
            db->ix_dupes, db->ix_recnums, db->ix_datacopy, db->ix_collattr,
            db->ix_nullsallowed, db->numblobs + 1, /* main record + n blobs */
            dbenv->bdb_env, &bdberr);

        if (db->handle == NULL) {
            logmsg(LOGMSG_ERROR, "bdb_open:failed to open table %s/%s, rcode %d\n",
                   dbenv->basedir, db->dbname, bdberr);
            return -1;
        }
    }
    /* open queues */
    for (ii = 0; ii < dbenv->num_qdbs; ii++) {
        int pagesize;
        db = dbenv->qdbs[ii];
        logmsg(LOGMSG_INFO, "open queue '%s'\n", db->dbname);

        /* Work out best page size for the expected average item size. */
        if (db->queue_pagesize_override) {
            pagesize = db->queue_pagesize_override;
        } else {
            if (db->dbtype == DBTYPE_QUEUE)
                pagesize = bdb_queue_best_pagesize(db->avgitemsz);
            else
                pagesize = bdb_queuedb_best_pagesize(db->avgitemsz);
            logmsg(LOGMSG_INFO, "pagesize %d recommended for item size %d\n", pagesize,
                   db->avgitemsz);
        }

        db->handle = bdb_open_more_queue(
            db->dbname, dbenv->basedir, db->avgitemsz, pagesize, dbenv->bdb_env,
            db->dbtype == DBTYPE_QUEUEDB ? 1 : 0, &bdberr);
        if (db->handle == NULL) {
            logmsg(LOGMSG_ERROR, "bdb_open_more_queue:failed to open queue %s/%s, rcode %d\n",
                   dbenv->basedir, db->dbname, bdberr);
            return -1;
        }
    }
    if (fix_consumers_with_bdblib(dbenv) != 0)
        return -1;

    /* try to open the new, per database, meta table.  if this fails then
     * fall back to per table meta tables.  When a suitable comdb2.tsk is
     * stable and out there we can make it default behaviour to have a single
     * meta table, but I don't want this to happen on init mode yet because
     * we'll end up with databases that have to be installed ASAP with data
     * preserved to prod, but no supporting comdb2.tsk in prod. */
    if (!gbl_create_mode || gbl_init_single_meta) {
        char metadbname[256];

        if (gbl_nonames)
            snprintf(metadbname, sizeof(metadbname), "comdb2_metadata");
        else
            snprintf(metadbname, sizeof(metadbname), "%s.metadata",
                     dbenv->envname);

        dbenv->meta = bdb_open_more_lite(metadbname, dbenv->basedir, 0,
                                         sizeof(struct metahdr2), 0,
                                         dbenv->bdb_env, &bdberr);
    }

    if (!dbenv->meta) {
        for (ii = 0; ii < dbenv->num_dbs; ii++) {
            rc = open_auxdbs(dbenv->dbs[ii], 0);
            /* We still have production comdb2s that don't have meta, so we
             * can't
             * make this a fatal error. -- Sam J */
            if (rc) {
                logmsg(LOGMSG_ERROR, "meta database not available\n");
            }
        }
    }

    /* now that meta is open, get the blobstripe conversion genids for each
     * table so that we can find pre-blobstripe blobs */
    fix_blobstripe_genids();

    for (ii = 0; ii < dbenv->num_dbs; ii++) {
        /* read ondisk header and compression information */
        struct dbtable *d = dbenv->dbs[ii];
        int compress, compress_blobs, datacopy_odh;
        int bthashsz;
        if (gbl_create_mode) {
            if (init_odh_lrl(d, &compress, &compress_blobs, &datacopy_odh) !=
                0) {
                logmsg(LOGMSG_ERROR, "save odh to llmeta failed\n");
                return -1;
            }
            if (gbl_init_with_bthash &&
                put_db_bthash(d, NULL, gbl_init_with_bthash) != 0) {
                logmsg(LOGMSG_ERROR, "save bthash size to llmeta failed\n");
                return -1;
            }
            bthashsz = gbl_init_with_bthash;
        } else {
            if (init_odh_llmeta(d, &compress, &compress_blobs, &datacopy_odh) !=
                0) {
                logmsg(LOGMSG_ERROR, "fetch odh from llmeta failed\n");
                return -1;
            }

            if (get_db_bthash(d, &bthashsz) != 0) {
                bthashsz = 0;
            }

            get_disable_skipscan(d);
        }

        if (bthashsz) {
            logmsg(LOGMSG_INFO, "Building bthash for table %s, size %dkb per stripe\n",
                   d->dbname, bthashsz);
            bdb_handle_dbp_add_hash(d->handle, bthashsz);
        }

        /* now tell bdb what the flags are - CRUCIAL that this is done
         * before any records are read/written from/to these tables. */
        set_bdb_option_flags(d, d->odh, d->inplace_updates,
                             d->instant_schema_change, d->version, compress,
                             compress_blobs, datacopy_odh);

        ctrace("Table %s  "
               "ver %d  "
               "odh %s  "
               "isc %s  "
               "odh_datacopy %s  "
               "ipu %s",
               d->dbname, d->version, d->odh ? "yes" : "no",
               d->instant_schema_change ? "yes" : "no",
               datacopy_odh ? "yes" : "no", d->inplace_updates ? "yes" : "no");
    }

    if (gbl_create_mode) {
        if (gbl_init_with_rowlocks &&
            (rc = bdb_set_rowlocks_state(
                 NULL, (gbl_init_with_rowlocks == 1)
                           ? LLMETA_ROWLOCKS_ENABLED
                           : LLMETA_ROWLOCKS_ENABLED_MASTER_ONLY,
                 &bdberr)) != 0) {
            logmsg(LOGMSG_ERROR, "Set rowlocks llmeta failed, rc=%d bdberr=%d\n", rc, bdberr);
            return -1;
        }
        if (gbl_init_with_genid48 &&
            (rc = bdb_set_genid_format(LLMETA_GENID_48BIT, &bdberr)) != 0) {
            logmsg(LOGMSG_ERROR, "Set genid format llmeta failed, rc=%d bdberr=%d\n", rc,
                   bdberr);
            return -1;
        }
    } else {
        int rlstate;
        if ((rc = bdb_get_rowlocks_state(&rlstate, &bdberr)) != 0) {
            logmsg(LOGMSG_ERROR, "Get rowlocks llmeta failed, rc=%d bdberr=%d\n", rc, bdberr);
            return -1;
        }
        switch (rlstate) {
        case LLMETA_ROWLOCKS_ENABLED:
        case LLMETA_ROWLOCKS_ENABLED_MASTER_ONLY:
            gbl_rowlocks = 1;
            gbl_sql_tranlevel_preserved = gbl_sql_tranlevel_default;
            gbl_sql_tranlevel_default = SQL_TDEF_SNAPISOL;
            logmsg(LOGMSG_INFO, "Rowlocks is *ENABLED*\n");
            break;
        case LLMETA_ROWLOCKS_DISABLED:
            gbl_rowlocks = 0;
            gbl_sql_tranlevel_default = gbl_sql_tranlevel_preserved;
            logmsg(LOGMSG_INFO, "Rowlocks is *DISABLED*\n");
            break;
        default:
            break;
        }
        if (rlstate == LLMETA_ROWLOCKS_DISABLED_TEMP_SC)
            logmsg(LOGMSG_INFO, "Rowlocks will be re-enabled after sc completes\n");
    }

    if (gbl_iothreads)
        start_prefault_io_threads(db->dbenv, gbl_iothreads, gbl_ioqueue);

    if (gbl_prefaulthelperthreads)
        create_prefault_helper_threads(db->dbenv, gbl_prefaulthelperthreads);

    /* TODO: set any berkdb options that were deferred because berkdb wasn't
     * initialized when we parsed the lrl file*/
    bdb_berkdb_iomap_set(thedb->bdb_env, gbl_berkdb_iomap);

    return 0; /*success */
}



static void fix_blobstripe_genids(void)
{
    int ii, rc;
    struct dbtable *db;
    struct dbenv *dbenv = thedb;
    if (gbl_blobstripe) {
        for (ii = 0; ii < dbenv->num_dbs; ii++) {
            db = dbenv->dbs[ii];
            rc = get_blobstripe_genid(db, &db->blobstripe_genid);
            if (rc == 0) {
                bdb_set_blobstripe_genid(db->handle, db->blobstripe_genid);
                ctrace("blobstripe genid 0x%llx for table %s\n",
                       db->blobstripe_genid, db->dbname);
            } else {
                ctrace("no blobstripe genid for table %s\n", db->dbname);
            }
        }
    }
}

/* after a consumer change, make sure bdblib knows what's going on. */
int fix_consumers_with_bdblib(struct dbenv *dbenv)
{
    int ii;
    for (ii = 0; ii < dbenv->num_qdbs; ii++) {
        struct dbtable *db = dbenv->qdbs[ii];
        int consumern;

        /* register all consumers */
        for (consumern = 0; consumern < MAXCONSUMERS; consumern++) {
            int active = db->consumers[consumern] ? 1 : 0;
            int rc, bdberr;
            rc = bdb_queue_consumer(db->handle, consumern, active, &bdberr);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, 
                       "bdb_queue_consumer error for queue %s/%s/%d, rcode %d\n",
                       dbenv->basedir, db->dbname, consumern, bdberr);
                return -1;
            }
        }
    }
    return 0;
}

/*close engine. */
int backend_close(struct dbenv *dbenv)
{
    if (dbenv->timepart_views) {
        views_signal(dbenv->timepart_views);
    }

    /* offloading sql goes here */
    if (dbenv->nsiblings > 0) {
        osql_net_exiting();
    }

    return bdb_close_env(dbenv->bdb_env);
}

void backend_cleanup(struct dbenv *dbenv)
{
    if (dbenv->nsiblings > 0) {
        osql_cleanup();
    }
}

void backend_get_cachestats(struct dbenv *dbenv, int *cachekb, int *hits,
                            int *misses)
{
    if (dbenv->bdb_env == NULL)
        return;

    uint64_t h, m;

    bdb_get_cache_stats(dbenv->bdb_env, &h, &m, NULL, NULL, NULL, NULL);

    *cachekb = dbenv->cacheszkb;
    /* lossy */
    *hits = (int)h;
    *misses = (int)m;
}

void backend_get_iostats(int *n_reads, int *l_reads, int *n_writes,
                         int *l_writes)
{
    bdb_get_iostats(n_reads, l_reads, n_writes, l_writes);
}

void backend_stat(struct dbenv *dbenv)
{
    double f;
    uint64_t hits, misses, reads, writes, thits, tmisses;
    char *who = dbenv->master;
    int delay, delaymax;
    if (dbenv->bdb_env == NULL)
        return;
    delay = bdb_attr_get(dbenv->bdb_attr, BDB_ATTR_COMMITDELAY);
    delaymax = bdb_attr_get(dbenv->bdb_attr, BDB_ATTR_COMMITDELAYMAX);
    logmsg(LOGMSG_USER, "txn commit delay        %d ms (max %d ms)\n", delay, delaymax);
    if (dbenv->nsiblings == 0)
        logmsg(LOGMSG_USER, "LOCAL MODE.\n");
    else if (who == gbl_mynode)
        logmsg(LOGMSG_USER, "I *AM* MASTER.  MASTER IS %s\n", who);
    else
        logmsg(LOGMSG_USER, "I AM NOT MASTER.  MASTER IS %s\n", who);
    backend_sync_stat(dbenv);
    bdb_get_cache_stats(dbenv->bdb_env, &hits, &misses, &reads, &writes, &thits,
                        &tmisses);
    if (!bdb_am_i_coherent(dbenv->bdb_env))
        logmsg(LOGMSG_USER, "!!! I AM NOT COHERENT !!!\n");
    f = dbenv->cacheszkb / 1024.0;
    logmsg(LOGMSG_USER, "cachesize %.3f mb\n", f);
    logmsg(LOGMSG_USER, "hits        %llu\n", hits);
    logmsg(LOGMSG_USER, "misses      %llu\n", misses);
    logmsg(LOGMSG_USER, "page reads  %llu\n", reads);
    logmsg(LOGMSG_USER, "page writes %llu\n", writes);
    if ((hits + misses) == 0)
        f = 100.0;
    else
        f = (double)hits / (double)(hits + misses) * 100.0;
    logmsg(LOGMSG_USER, "hit rate     %.1f%%\n", f);
    if ((thits + tmisses) == 0)
        f = 100.0;
    else
        f = (double)thits / (double)(thits + tmisses) * 100.0;
    logmsg(LOGMSG_USER, "tmp hit rate %.1f%%\n", f);
}

void backend_cmd(struct dbenv *dbenv, char *line, int lline, int st)
{
    bdb_process_user_command(dbenv->bdb_env, line, lline, st);
}

void backend_thread_event(struct dbenv *dbenv, int event)
{
    bdb_thread_event(dbenv->bdb_env, event);
}

/* Form all keys, add a record.  buf is assumed to be
   in ondisk format */
int load_record(struct dbtable *db, void *buf)
{
#if 0
    int i;
    char key[MAXKEYLEN];
    char tag[MAXTAGLEN];
    int rc;
    unsigned long long genid;
    struct ireq iq = {0};
    void *trans;
    int rrn;
    int bdberr;
    int need_to_retry = 0;
    int nretries = 0;

    init_fake_ireq(db->dbenv, &iq);
    iq.usedb = db;

retry:
    nretries++;
    if (nretries == gbl_maxretries)
        return -1;

    rc = trans_start(&iq, NULL, &trans);
    if (rc) {
        fprintf(stderr, "add_record:bdb_tran_begin rc %d bdberr %d\n", rc, bdberr);
        return -1;
    }
    
    /* add data */
    rc = dat_add(&iq, trans, buf, db->lrl, &genid, &rrn);
    if (rc) {
        if (rc == RC_INTERNAL_RETRY)
            need_to_retry = 1;
        else
            fprintf(stderr, "add_record:dat_add rc %d\n", rc);
        goto backout;
    }

    for (i = 0; i < db->nix; i++) {
        snprintf(tag, MAXTAGLEN, ".ONDISK_ix_%d", i);
        rc = stag_to_stag_buf(db->dbname, ".ONDISK", buf, tag, key, NULL);
        if (rc) {
            if (rc == RC_INTERNAL_RETRY)
                need_to_retry = 1;
            else
                fprintf(stderr, "add_record:stag_to_stag_buf ix %d rc %d\n", i, rc);
            goto backout;
        }

        rc = ix_addk(&iq, trans, key, i, genid, rrn, buf, db->lrl);
        if (rc) {
            if (rc == RC_INTERNAL_RETRY)
                need_to_retry = 1;
            else
                fprintf(stderr, "add_record:ix_addk rrn %d genid %016llx rc %d\n", rrn, genid, rc);
            goto backout;
        }
    }

    /* commit */
    rc = trans_commit(&iq, trans, gbl_mynode);
    if (rc)
        fprintf(stderr, "add_record:trans_commit rc %d\n", rc);
    return 0;

backout:
    rc = trans_abort(&iq, trans);
    if (rc)
        fprintf(stderr, "add_record:trans_abort rc %d\n", rc);
    if (need_to_retry)
        goto retry;
    return -1;
#endif
    return -1;
}

int ix_find_rnum_by_recnum(struct ireq *iq, int recnum_in, int ixnum,
                           void *fndkey, int *fndrrn, unsigned long long *genid,
                           void *fnddta, int *fndlen, int *recnum, int maxlen)
{
    struct dbtable *db = iq->usedb;
    char *req;
    int ixrc, bdberr, retries = 0;
    bdb_fetch_args_t args = {0};

retry:
    if (fnddta) {
        iq->gluewhere = req = "bdb_fetch_recnum_by_recnum_genid";
        ixrc = bdb_fetch_recnum_by_recnum_genid(
            db->handle, ixnum, recnum_in, fnddta, maxlen, fndlen, fndkey,
            fndrrn, recnum, genid, &args, &bdberr);
        VTAG(ixrc, db);
        iq->gluewhere = "bdb_fetch_recnum_by_recnum done_genid";
    } else {
        iq->gluewhere = req = "bdb_fetch_nodta_by_recnum_genid";
        ixrc = bdb_fetch_nodta_by_recnum_genid(db->handle, ixnum, recnum_in,
                                               fndkey, fndrrn, genid, &args,
                                               &bdberr);
        *recnum = recnum_in;
        iq->gluewhere = "bdb_fetch_nodta_by_recnum_genid done";
    }
    if (ixrc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* %s too much contention %d count %d\n", req, bdberr,
                   retries);
            return ERR_INTERNAL;
        }
        return map_unhandled_bdb_rcode(req, bdberr, 0);
    }
    return ixrc;
}

static uint8_t *metahdr_type_put(const struct metahdr *p_metahdr,
                                 uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || METAHDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_put(&(p_metahdr->rrn), sizeof(p_metahdr->rrn), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_metahdr->attr), sizeof(p_metahdr->attr), p_buf, p_buf_end);

    return p_buf;
}

int put_csc2_stuff(struct dbtable *db, void *trans, void *stuff, size_t lenstuff)
{

    struct metahdr hdr;

    hdr.rrn = META_STUFF_RRN;
    hdr.attr = 0;
    return meta_put(db, trans, &hdr, stuff, lenstuff);
}

int put_csc2_file(const char *table, void *tran, int version, const char *text)
{
    int bdberr;
    if (bdb_new_csc2(tran, table, version, (char *)text, &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "put_csc2_file had an error for csc2 version %d table %s, rc: "
               "%d\n",
               version, table, bdberr);
        return -1;
    }
    return 0;
}

/* gets the csc2 schema for a table and version number.
 * schema is returned in a pointer set to *text (must be freed by the caller)
 * schema length is returnedin *len
 * returns !0 on failure or 0 on success */
int get_csc2_file_tran(const char *table, int version, char **text, int *len,
                       tran_type *tran)
{
    int bdberr;
    if (bdb_get_csc2(tran, table, version, text, &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "get_csc2_file had an error for csc2 version %d table %s, rc: "
               "%d\n",
               version, table, bdberr);
        return -1;
    }
    if (len)
        *len = strlen(*text);
    return 0;
}

int get_csc2_file(const char *table, int version, char **text, int *len)
{
    return get_csc2_file_tran(table, version, text, len, NULL);
}

int get_csc2_version_tran(const char *table, tran_type *tran)
{
    int csc2_vers, rc, lcl_bdberr;
    rc = bdb_get_csc2_highest(tran, table, &csc2_vers, &lcl_bdberr);
    if (rc || lcl_bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "get_csc2_version_tran failed to get version\n");
        return -1;
    }
    return csc2_vers;
}

int get_csc2_version(const char *table)
{
    return get_csc2_version_tran(table, NULL);
}

int put_blobstripe_genid(struct dbtable *db, void *tran, unsigned long long genid)
{
    struct metahdr hdr;
    int rc;
    hdr.rrn = META_BLOBSTRIPE_GENID_RRN;
    hdr.attr = 0;
    rc = meta_put(db, tran, &hdr, (void *)&genid, sizeof(genid));
    return rc;
}

int get_blobstripe_genid(struct dbtable *db, unsigned long long *genid)
{
    struct metahdr hdr;
    int rc;
    hdr.rrn = META_BLOBSTRIPE_GENID_RRN;
    hdr.attr = 0;
    rc = meta_get(db, &hdr, (void *)genid, sizeof(*genid));
    return rc;
}

#define get_put_db(x, y)                                                       \
    int put_db_##x(struct dbtable *db, tran_type *tran, int value)                  \
    {                                                                          \
        struct metahdr hdr = {.rrn = y, .attr = 0};                            \
        int tmp = htonl(value);                                                \
        return meta_put(db, tran, &hdr, &tmp, sizeof(int));                    \
    }                                                                          \
    int get_db_##x##_tran(struct dbtable *db, int *value, tran_type *tran)          \
    {                                                                          \
        struct metahdr hdr = {.rrn = y, .attr = 0};                            \
        int tmp;                                                               \
        int rc = meta_get_tran(tran, db, &hdr, &tmp, sizeof(int));             \
        if (rc == 0)                                                           \
            *value = ntohl(tmp);                                               \
        else                                                                   \
            *value = 0;                                                        \
        return rc;                                                             \
    }                                                                          \
    int get_db_##x(struct dbtable *db, int *value)                                  \
    {                                                                          \
        return get_db_##x##_tran(db, value, NULL);                             \
    }

get_put_db(odh, META_ONDISK_HEADER_RRN) get_put_db(inplace_updates,
                                                   META_INPLACE_UPDATES)
    get_put_db(compress, META_COMPRESS_RRN)
        get_put_db(compress_blobs, META_COMPRESS_BLOBS_RRN)
            get_put_db(instant_schema_change, META_INSTANT_SCHEMA_CHANGE)
                get_put_db(datacopy_odh, META_DATACOPY_ODH)
                    get_put_db(bthash, META_BTHASH)

                        static int put_meta_int(const char *table, void *tran,
                                                int rrn, int key, int value)
{
    struct metahdr hdr;
    struct dbtable *db;

    hdr.rrn = rrn;
    hdr.attr = key;
    db = get_dbtable_by_name(table);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "put_meta_int for bad db %s\n", table);
        return -1;
    }
    return meta_put(db, tran, &hdr, &value, sizeof(int));
}

static int get_meta_int_tran(tran_type *tran, const char *table, int rrn,
                             int key)
{
    struct metahdr hdr;
    struct dbtable *db;
    int rc;
    int value;

    hdr.rrn = rrn;
    hdr.attr = key;
    db = get_dbtable_by_name(table);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "get_meta_int for bad db %s\n", table);
        return -1;
    }
    rc = meta_get_tran(tran, db, &hdr, &value, sizeof(int));
    if (rc == IX_NOTFND || rc == IX_PASTEOF || rc == IX_EMPTY ||
        rc == ERR_NO_AUXDB)
        return 0;
    else if (rc == IX_FND)
        return value;
    else {
        /* duplicate? bdb err? */
        logmsg(LOGMSG_ERROR, "get_meta_int: unexpected rcode %d\n", rc);
        return -1;
    }
}

static int get_meta_int(const char *table, int rrn, int key)
{
    return get_meta_int_tran(NULL /*tran*/, table, rrn, key);
}

static const uint8_t *metahdr_type_get(struct metahdr *p_metahdr,
                                       const uint8_t *p_buf,
                                       const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || METAHDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_get(&(p_metahdr->rrn), sizeof(p_metahdr->rrn), p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_metahdr->attr), sizeof(p_metahdr->attr), p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *metahdr2_type_put(const struct metahdr2 *p_metahdr2,
                                  uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || METAHDR2_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_put(&(p_metahdr2->keystr), sizeof(p_metahdr2->keystr),
                           p_buf, p_buf_end);
    p_buf = metahdr_type_put(&(p_metahdr2->hdr1), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *metahdr2_type_get(struct metahdr2 *p_metahdr2,
                                        const uint8_t *p_buf,
                                        const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || METAHDR2_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_get(&(p_metahdr2->keystr), sizeof(p_metahdr2->keystr),
                           p_buf, p_buf_end);
    p_buf = metahdr_type_get(&(p_metahdr2->hdr1), p_buf, p_buf_end);

    return p_buf;
}

static int meta_put(struct dbtable *db, void *input_tran, struct metahdr *hdr1,
                    void *data, int dtalen)
{
    struct ireq iq = {0};
    tran_type *trans;
    int rc;
    int rrn;
    int fnd;
    struct metahdr fndhdr;
    int fndlen;
    unsigned long long genid = 0, newgenid;
    int retries = 0;
    int addrc;
    char buf;
    void *bdb_handle;
    void *hdr;
    struct metahdr2 hdr2;
    uint8_t p_metahdr[METAHDR_LEN], p_metahdr2[METAHDR2_LEN];
    uint8_t *p_hdr_buf = p_metahdr, *p_hdr_buf_end = (p_hdr_buf + METAHDR_LEN);
    uint8_t *p_hdr2_buf = p_metahdr2,
            *p_hdr2_buf_end = (p_hdr2_buf + METAHDR2_LEN);

    int keysize;

    if (db->dbenv->meta) {
        bzero(&hdr2, sizeof(struct metahdr2));
        memcpy(&hdr2.hdr1, hdr1, sizeof(struct metahdr));
        snprintf(hdr2.keystr, sizeof(hdr2.keystr), "/%s", db->dbname);
        keysize = sizeof(struct metahdr2);
        metahdr2_type_put(&hdr2, p_hdr2_buf, p_hdr2_buf_end);
        hdr = &p_metahdr2;
    } else {
        keysize = sizeof(struct metahdr);
        metahdr_type_put(hdr1, p_hdr_buf, p_hdr_buf_end);
        hdr = p_metahdr;
    }

    bdb_handle = get_bdb_handle(db, AUXDB_META);
    if (!bdb_handle) {
        /*printf("meta_put:meta database not available - old comdb2?\n");*/
        return ERR_NO_AUXDB;
    }

    iq.is_fake = 1;
    iq.usedb = db;
    iq.debug = debug_this_request(gbl_debug_until);
    if (gbl_who > 0) {
        iq.debug = 1;
    }

retry:
    retries++;
    if (retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "meta_put: giving up after %d retries\n", retries);
        return -1;
    }
    if (input_tran)
        trans = input_tran;
    else {
        rc = trans_start_int(&iq, NULL, &trans, 0, 0);
        if (rc != 0) {
            if (iq.debug)
                logmsg(LOGMSG_USER, "meta_put:trans_start failed\n");
            return rc;
        }
    }
    if (is_auxdb_lite(AUXDB_META, &iq)) {
        /* delete old entry */
        rc = lite_del_auxdb(AUXDB_META, &iq, trans, hdr);
        if (iq.debug)
            logmsg(LOGMSG_USER, "meta_put:lite_del_auxdb RC %d\n", rc);
        if (rc != 0 && rc != IX_NOTFND)
            goto backout;
        /* add new entry */
        rc = lite_add_auxdb(AUXDB_META, &iq, trans, data, dtalen, hdr);
        if (iq.debug)
            logmsg(LOGMSG_USER, "meta_put:lite_add_auxdb RC %d\n", rc);
        if (rc)
            goto backout;
    } else {
        fnd = ix_find_auxdb(AUXDB_META, &iq, 0, hdr, keysize, &fndhdr, &rrn,
                            &genid, &buf, &fndlen, 0);
        if (fnd == IX_FND) {
            if (iq.debug)
                logmsg(LOGMSG_USER, "meta_put:update genid 0x%llx rrn %d\n", genid, rrn);
            /* delete old key */
            rc = ix_delk_auxdb(AUXDB_META, &iq, trans, hdr, 0, rrn, genid);
            if (iq.debug)
                logmsg(LOGMSG_USER, "meta_put:ix_delk_auxdb RC %d\n", rc);
            if (rc)
                goto backout;
            /* update data*/
            rc = dat_upv_auxdb(AUXDB_META, &iq, trans, 0, NULL, 0, genid, data,
                               dtalen, rrn, &newgenid, 0, 0, 0);
            if (iq.debug)
                logmsg(LOGMSG_USER, "meta_put:dat_upv_auxdb RC %d\n", rc);
            if (rc)
                goto backout;
            /* need to add th new key with the new genid.. */
            genid = newgenid;
        } else {
            rc = dat_add_auxdb(AUXDB_META, &iq, trans, data, dtalen, &genid,
                               &rrn);
            if (iq.debug)
                logmsg(LOGMSG_USER, "meta_put:dat_add_auxdb RC %d\n", rc);
            if (rc)
                goto backout;
        }
        rc = ix_addk_auxdb(AUXDB_META, &iq, trans, hdr, 0, genid, rrn, NULL, 0);
        if (iq.debug)
            logmsg(LOGMSG_USER, "meta_put:ix_addk_auxdb RC %d\n", rc);
        if (rc)
            goto backout;
    }
    if (!input_tran) {
        rc = trans_commit(&iq, trans, gbl_mynode);
        if (iq.debug)
            logmsg(LOGMSG_USER, "meta_put:trans_commit RC %d\n", rc);
        if (rc == RC_INTERNAL_RETRY)
            goto retry;
        else if (rc)
            return rc;
    }
    return 0;
backout:
    addrc = rc; /* save rcode for later */
    if (!input_tran) {
        rc = trans_abort(&iq, trans);
        if (iq.debug)
            logmsg(LOGMSG_USER, "meta_put:trans_abort RC %d\n", rc);
        if (addrc == RC_INTERNAL_RETRY)
            goto retry;
    }
    return addrc;
}

/* can only use a trasaction if the meta table is a lite db */
static int meta_get_tran(tran_type *tran, struct dbtable *db, struct metahdr *key1,
                         void *dta, int dtalen)
{
    struct ireq iq = {0};
    struct metahdr fndhdr;
    int rrn;
    int fndlen;
    unsigned long long int genid;
    void *key;
    struct metahdr2 key2;
    uint8_t p_metahdr[METAHDR_LEN], p_metahdr2[METAHDR2_LEN];
    uint8_t *p_hdr_buf = p_metahdr, *p_hdr_buf_end = (p_hdr_buf + METAHDR_LEN);
    uint8_t *p_hdr2_buf = p_metahdr2,
            *p_hdr2_buf_end = (p_hdr2_buf + METAHDR2_LEN);
    int rc;

    if (db->dbenv->meta) {
        bzero(&key2, sizeof(struct metahdr2));
        memcpy(&key2.hdr1, key1, sizeof(struct metahdr));
        snprintf(key2.keystr, sizeof(key2.keystr), "/%s", db->dbname);
        metahdr2_type_put(&key2, p_hdr2_buf, p_hdr2_buf_end);
        key = &p_metahdr2;
    } else {
        metahdr_type_put(key1, p_hdr_buf, p_hdr_buf_end);
        key = p_metahdr;
    }

    iq.is_fake = 1;
    iq.usedb = db;
    iq.debug = debug_this_request(gbl_debug_until);
    if (gbl_who > 0) {
        iq.debug = 1;
    }

    if (is_auxdb_lite(AUXDB_META, &iq)) {
        rc = lite_find_exact_auxdb_tran(AUXDB_META, &iq, tran, key, dta,
                                        &fndlen, dtalen);
    } else {
        if (tran) {
            logmsg(LOGMSG_ERROR, "meta_get: using a transaction with a non-lite "
                            "meta table is not implemented\n");
            return 1;
        }
        rc = ix_find_auxdb(AUXDB_META, &iq, 0, key, sizeof(struct metahdr),
                           &fndhdr, &rrn, &genid, dta, &fndlen, dtalen);
    }

    if (db->dbenv->meta) {
        memcpy(key1, &key2.hdr1, sizeof(struct metahdr));
    }

    return rc;
}

int meta_get(struct dbtable *db, struct metahdr *key1, void *dta, int dtalen)
{
    return meta_get_tran(NULL /*tran*/, db, key1, dta, dtalen);
}

/* get variable length data, placing a pointer to it in *dta. */
/* can only use a trasaction if the meta table is a lite db */
static int meta_get_var_tran(tran_type *tran, struct dbtable *db,
                             struct metahdr *key1, void **dta, int *fndlen)
{
    struct ireq iq = {0};
    struct metahdr fndhdr;
    int rrn;
    unsigned long long int genid;
    void *key;
    struct metahdr2 key2;
    uint8_t p_metahdr[METAHDR_LEN], p_metahdr2[METAHDR2_LEN];
    uint8_t *p_hdr_buf = p_metahdr, *p_hdr_buf_end = (p_hdr_buf + METAHDR_LEN);
    uint8_t *p_hdr2_buf = p_metahdr2,
            *p_hdr2_buf_end = (p_hdr_buf + METAHDR2_LEN);
    int rc;

    if (db->dbenv->meta) {
        bzero(&key2, sizeof(struct metahdr2));
        memcpy(&key2.hdr1, key1, sizeof(struct metahdr));
        snprintf(key2.keystr, sizeof(key2.keystr), "/%s", db->dbname);
        metahdr2_type_put(&key2, p_hdr2_buf, p_hdr2_buf_end);
        key = &p_metahdr2;
    } else {
        metahdr_type_put(key1, p_hdr_buf, p_hdr_buf_end);
        key = p_metahdr;
    }

    iq.is_fake = 1;
    iq.usedb = db;
    iq.debug = debug_this_request(gbl_debug_until);
    if (gbl_who > 0) {
        iq.debug = 1;
    }

    if (is_auxdb_lite(AUXDB_META, &iq)) {
        rc = lite_find_exact_var_auxdb_tran(AUXDB_META, &iq, tran, key, dta,
                                            fndlen);
    } else {
        /* silly loop to find record with the right length */
        int dtalen = 0;
        *fndlen = DB_MAX_SERVER_KEY_SIZE;
        *dta = NULL;

        if (tran) {
            logmsg(LOGMSG_ERROR, "meta_get_var: using a transaction with a non-lite "
                            "meta table is not implemented\n");
            return 1;
        }
        do {
            if (*dta)
                free(*dta);
            dtalen = *fndlen;
            *dta = malloc(dtalen);
            rc = ix_find_auxdb(AUXDB_META, &iq, 0, key, sizeof(struct metahdr),
                               &fndhdr, &rrn, &genid, *dta, fndlen, dtalen);
            /* we are looking for an exact match, so bomb on any deviation */
            if (rc != 0) {
                free(*dta);
                *dta = NULL;
                *fndlen = 0;
                break;
            }
        } while (dtalen < *fndlen);
    }

    if (db->dbenv->meta) {
        memcpy(key1, &key2.hdr1, sizeof(struct metahdr));
    }

    return rc;
}

int meta_get_var(struct dbtable *db, struct metahdr *key1, void **dta, int *fndlen)
{
    return meta_get_var_tran(NULL /*tran*/, db, key1, dta, fndlen);
}

void purgerrns(struct dbtable *db) { return; }

void flush_db(void)
{
    int rc;
    bdb_flush(thedb->bdb_env, &rc);
}

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
/*          LITE DATABASES           */
/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

int is_auxdb_lite(int auxdb, struct ireq *iq)
{
    void *bdb_handle;
    bdb_handle = get_bdb_handle_ireq(iq, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    return bdb_get_type(bdb_handle) == BDBTYPE_LITE ? 1 : 0;
}

int lite_find_exact_auxdb_tran(int auxdb, struct ireq *iq, tran_type *tran,
                               void *key, void *fnddta, int *fndlen, int maxlen)
{
    int bdberr, rc, retries = 0;
    void *bdb_handle;
    bdb_handle = get_bdb_handle_ireq(iq, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

retry:
    iq->gluewhere = "bdb_lite_exact_fetch";
    rc = bdb_lite_exact_fetch_tran(bdb_handle, tran, key, fnddta, maxlen,
                                   fndlen, &bdberr);
    iq->gluewhere = "bdb_lite_exact_fetch done";
    if (rc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* bdb_lite_exact_fetch too much contention %d count "
                   "%d\n",
                   bdberr, retries);
            return ERR_INTERNAL;
        } else if (bdberr == BDBERR_FETCH_DTA) {
            return IX_NOTFND;
        }
        return map_unhandled_bdb_rcode("bdb_lite_exact_fetch", bdberr, 0);
    }
    return rc;
}

int lite_find_exact_auxdb(int auxdb, struct ireq *iq, void *key, void *fnddta,
                          int *fndlen, int maxlen)
{
    return lite_find_exact_auxdb_tran(auxdb, iq, NULL /*tran*/, key, fnddta,
                                      fndlen, maxlen);
}

int lite_find_exact_var_auxdb_tran(int auxdb, struct ireq *iq, tran_type *tran,
                                   void *key, void **fnddta, int *fndlen)
{
    int bdberr, rc, retries = 0;
    void *bdb_handle;
    bdb_handle = get_bdb_handle_ireq(iq, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

retry:
    iq->gluewhere = "bdb_lite_exact_fetch";
    rc = bdb_lite_exact_var_fetch_tran(bdb_handle, tran, key, fnddta, fndlen,
                                       &bdberr);
    iq->gluewhere = "bdb_lite_exact_fetch done";
    if (rc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* bdb_lite_exact_fetch too much contention %d count "
                   "%d\n",
                   bdberr, retries);
            return ERR_INTERNAL;
        } else if (bdberr == BDBERR_FETCH_DTA) {
            return IX_NOTFND;
        }
        return map_unhandled_bdb_rcode("bdb_lite_exact_fetch", bdberr, 0);
    }
    return rc;
}

int lite_find_exact_var_auxdb(int auxdb, struct ireq *iq, void *key,
                              void **fnddta, int *fndlen)
{
    return lite_find_exact_var_auxdb_tran(auxdb, iq, NULL /*tran*/, key, fnddta,
                                          fndlen);
}

int lite_get_keys_auxdb(int auxdb, struct ireq *iq, void *firstkey,
                        void *fndkeys, int maxfnd, int *numfnd)
{
    int bdberr, rc, retries = 0;
    void *bdb_handle;
    bdb_handle = get_bdb_handle_ireq(iq, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

retry:
    iq->gluewhere = "bdb_lite_fetch_keys_fwd";
    rc = bdb_lite_fetch_keys_fwd(bdb_handle, firstkey, fndkeys, maxfnd, numfnd,
                                 &bdberr);
    iq->gluewhere = "bdb_lite_fetch_keys_fwd done";
    if (rc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* bdb_lite_fetch_keys_fwd too much contention %d "
                   "count %d\n",
                   bdberr, retries);
            return ERR_INTERNAL;
        }
        return map_unhandled_bdb_rcode("bdb_lite_fetch_keys_fwd", bdberr, 0);
    }
    return rc;
}

int lite_add_auxdb(int auxdb, struct ireq *iq, void *trans, void *data,
                   int datalen, void *key)
{
    int bdberr;
    void *bdb_handle;
    bdb_handle = get_bdb_handle_ireq(iq, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    iq->gluewhere = "bdb_lite_add";
    bdb_lite_add(bdb_handle, trans, data, datalen, key, &bdberr);
    iq->gluewhere = "bdb_lite_add done";

    if (bdberr == 0)
        return 0;
    if (bdberr == BDBERR_DEADLOCK)
        return RC_INTERNAL_RETRY;
    if (bdberr == BDBERR_TRANTOOCOMPLEX)
        return RC_TRAN_TOO_COMPLEX;
    if (bdberr == BDBERR_READONLY)
        return ERR_NOMASTER;
    if (bdberr == BDBERR_ADD_DUPE)
        return IX_DUP;
    return map_unhandled_bdb_wr_rcode("bdb_lite_add", bdberr);
}

/* given a list of full keys, delete lots of records in a single transaction */
int lite_del_auxdb(int auxdb, struct ireq *iq, void *trans, void *key)
{
    int bdberr;
    void *bdb_handle;
    bdb_handle = get_bdb_handle_ireq(iq, auxdb);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

    iq->gluewhere = "bdb_lite_exact_del";
    bdb_lite_exact_del(bdb_handle, trans, key, &bdberr);
    iq->gluewhere = "bdb_lite_exact_del done";

    if (bdberr == 0)
        return 0;
    if (bdberr == BDBERR_DEADLOCK)
        return RC_INTERNAL_RETRY;
    if (bdberr == BDBERR_TRANTOOCOMPLEX)
        return RC_TRAN_TOO_COMPLEX;
    if (bdberr == BDBERR_READONLY)
        return ERR_NOMASTER;
    if (bdberr == BDBERR_DEL_DTA)
        return IX_NOTFND;
    return map_unhandled_bdb_wr_rcode("lite_del_multiple_auxdb", bdberr);
}

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
/*         QUEUE DATABASES           */
/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

int dbq_add(struct ireq *iq, void *trans, const void *dta, size_t dtalen)
{
    int bdberr;
    void *bdb_handle;
    unsigned long long genid;
    bdb_handle = get_bdb_handle_ireq(iq, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    iq->gluewhere = "bdb_queue_add";
    bdb_queue_add(bdb_handle, trans, dta, dtalen, &bdberr, &genid);
    iq->gluewhere = "bdb_queue_add done";

    if (bdberr == 0) {
        /* remember that this queue was updated so the consumer can
         * be woken after we commit. */
        if (iq->num_queues_hit <= MAX_QUEUE_HITS_PER_TRANS) {
            unsigned ii;
            for (ii = 0; ii < iq->num_queues_hit; ii++) {
                /* we already updated this queue */
                if (iq->queues_hit[ii] == iq->usedb)
                    goto recorded_hit;
            }
            if (iq->num_queues_hit < MAX_QUEUE_HITS_PER_TRANS)
                iq->queues_hit[iq->num_queues_hit] = iq->usedb;
            iq->num_queues_hit++;
        }
    recorded_hit:
        /* Add this genid to the replication list; queue consumers will block
         * on this until it has replicated. */
        iq->repl_list = add_genid_to_repl_list(genid, iq->repl_list);
        return 0;
    }
    if (bdberr == BDBERR_DEADLOCK)
        return RC_INTERNAL_RETRY;
    if (bdberr == BDBERR_READONLY)
        return ERR_NOMASTER;
    if (bdberr == BDBERR_ADD_DUPE)
        return IX_DUP;
    return map_unhandled_bdb_wr_rcode("bdb_queue_add", bdberr);
}

int dbq_consume(struct ireq *iq, void *trans, int consumer, const void *fnd)
{
    int bdberr;
    void *bdb_handle;
    bdb_handle = get_bdb_handle_ireq(iq, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    iq->gluewhere = "bdb_queue_consume";
    bdb_queue_consume(bdb_handle, trans, consumer, fnd, &bdberr);
    iq->gluewhere = "bdb_queue_consume done";

    if (bdberr == 0)
        return 0;
    if (bdberr == BDBERR_DEADLOCK)
        return RC_INTERNAL_RETRY;
    if (bdberr == BDBERR_READONLY)
        return ERR_NOMASTER;
    if (bdberr == BDBERR_DELNOTFOUND)
        return IX_NOTFND;
    return map_unhandled_bdb_wr_rcode("bdb_queue_consume", bdberr);
}

int dbq_consume_genid(struct ireq *iq, void *trans, int consumer,
                      const genid_t genid)
{
    struct bdb_queue_found qfnd = {0};
    qfnd.genid = genid;
    // TODO XXX FIXME: take care of locking in case gbl_block_qconsume_lock
    return dbq_consume(iq, trans, consumer, &qfnd);
}

int dbq_check_goose(struct ireq *iq, void *trans)
{
    int bdberr;
    void *bdb_handle;
    int retries = 0;
    int rc;
    bdb_handle = get_bdb_handle_ireq(iq, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

retry:
    iq->gluewhere = "bdb_queue_check_goose";
    rc = bdb_queue_check_goose(bdb_handle, trans, &bdberr);
    iq->gluewhere = "bdb_queue_check_goose done";
    if (rc == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            if (trans)
                return RC_INTERNAL_RETRY;
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* bdb_queue_check_goose too much contention %d count "
                   "%d\n",
                   bdberr, retries);
            return ERR_INTERNAL;
        }
        return map_unhandled_bdb_rcode("bdb_queue_check_goose", bdberr, 0);
    }
    return rc;
}

int dbq_add_goose(struct ireq *iq, void *trans)
{
    int bdberr;
    void *bdb_handle;
    bdb_handle = get_bdb_handle_ireq(iq, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    iq->gluewhere = "bdb_queue_add_goose";
    bdb_queue_add_goose(bdb_handle, trans, &bdberr);
    iq->gluewhere = "bdb_queue_add_goose done";

    if (bdberr == 0)
        return 0;
    if (bdberr == BDBERR_DEADLOCK)
        return RC_INTERNAL_RETRY;
    if (bdberr == BDBERR_READONLY)
        return ERR_NOMASTER;
    return map_unhandled_bdb_wr_rcode("bdb_queue_add_goose", bdberr);
}

/* caller is responsible for rolling back if a non-goose record was consumed
 * by mistake. */
int dbq_consume_goose(struct ireq *iq, void *trans)
{
    int bdberr;
    void *bdb_handle;
    bdb_handle = get_bdb_handle_ireq(iq, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    iq->gluewhere = "bdb_queue_consume_goose";
    bdb_queue_consume_goose(bdb_handle, trans, &bdberr);
    iq->gluewhere = "bdb_queue_consume_goose done";

    if (bdberr == 0)
        return 0;
    if (bdberr == BDBERR_DEADLOCK)
        return RC_INTERNAL_RETRY;
    if (bdberr == BDBERR_DELNOTFOUND)
        return IX_NOTFND;
    if (bdberr == BDBERR_READONLY)
        return ERR_NOMASTER;
    return map_unhandled_bdb_wr_rcode("bdb_queue_consume_goose", bdberr);
}

int dbq_get(struct ireq *iq, int consumer, const struct dbq_cursor *prevcursor,
            void **fnddta, size_t *fnddtalen, size_t *fnddtaoff,
            struct dbq_cursor *fndcursor, unsigned int *epoch)
{
    int bdberr;
    void *bdb_handle;
    int retries = 0;
    int rc;
    bdb_handle = get_bdb_handle_ireq(iq, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

retry:
    iq->gluewhere = "bdb_queue_get";
    rc = bdb_queue_get(bdb_handle, consumer,
                       (const struct bdb_queue_cursor *)prevcursor, fnddta,
                       fnddtalen, fnddtaoff,
                       (struct bdb_queue_cursor *)fndcursor, epoch, &bdberr);
    iq->gluewhere = "bdb_queue_get done";
    if (rc != 0) {
        if (bdberr == BDBERR_DEADLOCK) {
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                poll(0, 0, (rand() % 500 + 10));
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* bdb_queue_get too much contention %d count %d\n",
                   bdberr, retries);
            return ERR_INTERNAL;
        } else if (bdberr == BDBERR_FETCH_DTA) {
            return IX_NOTFND;
        }

        else if (bdberr == BDBERR_LOCK_DESIRED) {
            return IX_NOTFND;
        }

        return map_unhandled_bdb_rcode("bdb_queue_get", bdberr, 0);
    }
    return rc;
}

unsigned long long dbq_item_genid(const void *dta)
{
    return bdb_queue_item_genid(dta);
}

void dbq_get_item_info(const void *fnd, size_t *dtaoff, size_t *dtalen)
{
    bdb_queue_get_found_info(fnd, dtaoff, dtalen);
}

int dbq_dump(struct dbtable *db, FILE *out)
{
    int bdberr;
    void *bdb_handle;
    struct ireq iq;
    int rc;

    init_fake_ireq(db->dbenv, &iq);
    iq.usedb = db;

    bdb_handle = get_bdb_handle_ireq(&iq, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

    iq.gluewhere = "bdb_queue_dump";
    rc = bdb_queue_dump(bdb_handle, out, &bdberr);
    iq.gluewhere = "bdb_queue_dump done";

    if (rc != 0)
        return ERR_INTERNAL;
    return 0;
}

int dbq_walk(struct ireq *iq, int flags, dbq_walk_callback_t callback,
             void *userptr)
{
    int bdberr;
    void *bdb_handle;
    int retries = 0;
    int rc;
    bbuint32_t lastitem = 0;
    bdb_handle = get_bdb_handle_ireq(iq, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

    flags &= ~BDB_QUEUE_WALK_RESTART;

retry:
    iq->gluewhere = "bdb_queue_walk";
    rc = bdb_queue_walk(bdb_handle, flags, &lastitem,
                        (bdb_queue_walk_callback_t)callback, userptr, &bdberr);
    iq->gluewhere = "bdb_queue_walk done";
    if (rc != 0) {
        if (bdberr == BDBERR_DEADLOCK) {
            iq->retries++;
            if (++retries < gbl_maxretries) {
                n_retries++;
                flags |= BDB_QUEUE_WALK_RESTART;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "*ERROR* bdb_queue_walk too much contention %d count %d\n",
                   bdberr, retries);
            return ERR_INTERNAL;
        } else if (bdberr == BDBERR_FETCH_DTA) {
            return IX_NOTFND;
        }
        return map_unhandled_bdb_rcode("bdb_queue_get", bdberr, 0);
    }
    return rc;
}

int reinit_db(struct dbtable *db)
{
    int rc, bdberr;
    void *bdb_handle;
    int retries = 0;
    tran_type *trans;
    struct ireq iq = {0};

    bdb_handle = get_bdb_handle(db, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;
    iq.is_fake = 1;
    iq.usedb = db;

/*stop_threads(db->dbenv);*/

retry:
    rc = trans_start(&iq, NULL, &trans);
    if (rc) {
        logmsg(LOGMSG_ERROR, "tran_start failed, rc=%d\n", rc); /* shouldn't happen */
        rc = ERR_INTERNAL;
        goto done;
    }
    rc = bdb_reinit(bdb_handle, trans, &bdberr);
    if (bdberr == RC_INTERNAL_RETRY) {
        if (retries > 9999999) {
            logmsg(LOGMSG_ERROR, "*ERROR*) bdb_reinit too much contention %d count %d\n",
                   bdberr, retries);
            rc = ERR_INTERNAL;
            goto done;
        }
        retries++;
        rc = trans_abort(&iq, trans);
        if (rc) {
            logmsg(LOGMSG_ERROR, "tran_abort failed, rc=%d\n", rc); /* shouldn't happen */
            rc = ERR_INTERNAL;
            goto done;
        }
        goto retry;
    } else if (rc) {
        int rc2;
        logmsg(LOGMSG_ERROR, "reinit_db %s error %d\n", db->dbname, bdberr);
        rc2 = trans_abort(&iq, trans);
        if (rc2) {
            logmsg(LOGMSG_ERROR, "tran_abort failed, rc=%d\n", rc2); /* shouldn't happen */
            rc = ERR_INTERNAL;
            goto done;
        }
        goto done;
    }
    /* wait for 5 minutes for this to complete... */
    rc = trans_commit_timeout(&iq, trans, gbl_mynode, 5 * 60 * 1000);
    if (rc) {
        logmsg(LOGMSG_ERROR, "tran_commit failed, rc=%d\n", rc); /* shouldn't happen */
        rc = ERR_INTERNAL;
        goto done;
    }

done:
    /*resume_threads(db->dbenv);*/
    return rc;
}

int truncate_db(struct dbtable *db)
{
    int rc, bdberr;
    void *bdb_handle;

    bdb_handle = get_bdb_handle(db, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

    stop_threads(db->dbenv);

    rc = bdb_truncate(bdb_handle, &bdberr);

    resume_threads(db->dbenv);
    return rc;
}

int count_db(struct dbtable *db)
{
    int bdberr;
    void *bdb_handle;
    int numrrns;
    int retries = 0;

    bdb_handle = get_bdb_handle(db, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

retry:
    numrrns = bdb_count(bdb_handle, &bdberr);
    if (numrrns == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            printf("*ERROR* bdb_count too much contention %d count %d\n",
                   bdberr, retries);
            return -1;
        }
        printf("*ERROR* bdb_count return unhandled rc %d\n", bdberr);
        return -1;
    }

    return numrrns;
}

void diagnostics_dump_dta(struct dbtable *db, int dtanum)
{
    void *bdb_handle;
    int rc;
    FILE *fh;

    bdb_handle = get_bdb_handle(db, AUXDB_NONE);
    if (!bdb_handle)
        return;

    char *filename;
    filename =
        comdb2_location("debug", "%s.dump_dta%d.txt", db->dbname, dtanum);
    fh = fopen(filename, "w");
    if (!fh) {
        logmsg(LOGMSG_ERROR, "diagnostics_dump_dta: cannot open %s: %s\n", filename,
                strerror(errno));
        free(filename);
        return;
    }

    rc = bdb_dump_dta_file_n(bdb_handle, dtanum, fh);

    fclose(fh);
    logmsg(LOGMSG_INFO, "dumped to %s rcode %d\n", filename, rc);
    free(filename);
}

void start_backend_request(struct dbenv *env)
{
    bdb_start_request(env->bdb_env);
}

void start_exclusive_backend_request(struct dbenv *env)
{
    bdb_start_exclusive_request(env->bdb_env);
}

void end_backend_request(struct dbenv *env) { bdb_end_request(env->bdb_env); }

uint64_t calc_table_size_analyze(struct dbtable *db)
{
    int ii;
    uint64_t returnsize;
    returnsize = db->totalsize = 0;

    if (db->dbtype == DBTYPE_UNTAGGED_TABLE ||
        db->dbtype == DBTYPE_TAGGED_TABLE) {
        for (ii = 0; ii < db->nix; ii++) {
            db->ixsizes[ii] = bdb_index_size(db->handle, ii);
            db->totalsize += db->ixsizes[ii];
        }

        returnsize = db->totalsize;

        db->dtasize = bdb_data_size(db->handle, 0);
        db->totalsize += db->dtasize;

        for (ii = 0; ii < db->numblobs; ii++) {
            db->blobsizes[ii] = bdb_data_size(db->handle, ii + 1);
            db->totalsize += db->blobsizes[ii];
        }
    } else if (db->dbtype == DBTYPE_QUEUE || db->dbtype == DBTYPE_QUEUE) {
        returnsize = db->totalsize =
            bdb_queue_size(db->handle, &db->numextents);
    } else {
        logmsg(LOGMSG_ERROR, "%s: db->dbtype=%d (what the heck is this?)\n",
                __func__, db->dbtype);
    }

    return returnsize;
}

uint64_t calc_table_size(struct dbtable *db)
{
    int ii;
    db->totalsize = 0;

    if (db->dbtype == DBTYPE_UNTAGGED_TABLE ||
        db->dbtype == DBTYPE_TAGGED_TABLE) {
        for (ii = 0; ii < db->nix; ii++) {
            db->ixsizes[ii] = bdb_index_size(db->handle, ii);
            db->totalsize += db->ixsizes[ii];
        }

        db->dtasize = bdb_data_size(db->handle, 0);
        db->totalsize += db->dtasize;

        for (ii = 0; ii < db->numblobs; ii++) {
            db->blobsizes[ii] = bdb_data_size(db->handle, ii + 1);
            db->totalsize += db->blobsizes[ii];
        }
    } else if (db->dbtype == DBTYPE_QUEUE || db->dbtype == DBTYPE_QUEUEDB) {
        db->totalsize = bdb_queue_size(db->handle, &db->numextents);
    } else {
        logmsg(LOGMSG_ERROR, "%s: db->dbtype=%d (what the heck is this?)\n",
                __func__, db->dbtype);
    }

    return db->totalsize;
}

void compr_print_stats()
{
    int ii;
    int odh, compr, blob_compr;

    const char *bdb_compr_alg_2a(int alg);

    logmsg(LOGMSG_USER, "COMPRESSION FLAGS\n");
    logmsg(LOGMSG_USER, "These apply to new records only!\n");

    for (ii = 0; ii < thedb->num_dbs; ii++) {
        struct dbtable *db = thedb->dbs[ii];
        bdb_get_compr_flags(db->handle, &odh, &compr, &blob_compr);

        logmsg(LOGMSG_USER, "[%-16s] ", db->dbname);
        logmsg(LOGMSG_USER, "ODH: %3s Compress: %-8s Blob compress: %-8s  in-place updates: "
               "%-3s  instant schema change: %-3s",
               odh ? "yes" : "no", bdb_algo2compr(compr),
               bdb_algo2compr(blob_compr), db->inplace_updates ? "yes" : "no",
               db->instant_schema_change ? "yes" : "no");

        logmsg(LOGMSG_USER, "\n");
    }
}

void print_tableparams()
{
    int ii;
    for (ii = 0; ii < thedb->num_dbs; ii++) {
        struct dbtable *db = thedb->dbs[ii];
        logmsg(LOGMSG_USER, "[%-16s] ", db->dbname);

        int bdberr = 0;
        int coveragevalue = 0;
        long long thresholdvalue = 0;
        int rc = 0;

        rc = bdb_get_analyzecoverage_table(NULL, db->dbname, &coveragevalue,
                                           &bdberr);
        if (rc != 0)
            logmsg(LOGMSG_ERROR, "bdb_get_analyzecoverage_table rc = %d, bdberr=%d\n", rc,
                   bdberr);

        rc = bdb_get_analyzethreshold_table(NULL, db->dbname, &thresholdvalue,
                                            &bdberr);
        if (rc != 0)
            logmsg(LOGMSG_ERROR, "bdb_get_analyzethreshold_table rc = %d, bdberr=%d\n", rc,
                   bdberr);

        char *disableskipscanval = NULL;
        bdb_get_table_parameter(db->dbname, "disableskipscan",
                                &disableskipscanval);

        if (coveragevalue >= 0)
            logmsg(LOGMSG_USER, " analyze coverage: %3d", coveragevalue);
        else
            logmsg(LOGMSG_USER, " analyze coverage: %3s", "N/A");

        if (thresholdvalue >= 0)
            logmsg(LOGMSG_USER, " analyze threshold: %10lld", thresholdvalue);
        else
            logmsg(LOGMSG_USER, " analyze threshold: %10s", "N/A");

        if (disableskipscanval) {
            logmsg(LOGMSG_USER, " disableskipscan: %10s", disableskipscanval);
            free(disableskipscanval);
        } else
            logmsg(LOGMSG_USER, " disableskipscan: %10s", "N/A");

        char *tableparams = NULL;
        int tbplen = 0;
        bdb_get_table_csonparameters(NULL, db->dbname, &tableparams, &tbplen);
        if (tableparams) {
            logmsg(LOGMSG_USER, " tableparams: %10s", tableparams);
            free(tableparams);
        } else
            logmsg(LOGMSG_USER, " tableparams: %10s", "N/A");
        logmsg(LOGMSG_USER, "\n");
    }
}

int set_meta_odh_flags_tran(struct dbtable *db, tran_type *tran, int odh,
                            int compress, int compress_blobs, int ipupdates)
{
    int rc;
    int overall = 0;
    rc = put_db_odh(db, tran, odh);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Unable to set meta ODH option\n");
        overall |= rc;
    }
    rc = put_db_compress(db, tran, compress);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Unable to set meta compress option\n");
        overall |= rc;
    }
    rc = put_db_compress_blobs(db, tran, compress_blobs);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Unable to set meta compress_blobs option\n");
        overall |= rc;
    }
    rc = put_db_inplace_updates(db, tran, ipupdates);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Unable to set meta inplace_updates option\n");
        overall |= rc;
    }
    return overall;
}

int set_meta_odh_flags(struct dbtable *db, int odh, int compress, int compress_blobs,
                       int ipupdates)
{
    return set_meta_odh_flags_tran(db, NULL, odh, compress, compress_blobs,
                                   ipupdates);
}

int ix_fetch_last_key_tran(struct ireq *iq, void *tran, int write, int ixnum,
                           int keylen, void *fndkey, int *fndlen)
{
    int rc;
    int bdberr;
    struct dbtable *db;
    db = iq->usedb;
    iq->gluewhere = "bdb_fetch_last_key_tran";
    rc = bdb_fetch_last_key_tran(db->handle, tran, write, ixnum, keylen, fndkey,
                                 fndlen, &bdberr);
    iq->gluewhere = "bdb_fetch_last_key_tran done";
    if (rc == -1) {
        if (bdberr == BDBERR_DEADLOCK)
            return RC_INTERNAL_RETRY;
        if (bdberr == BDBERR_READONLY)
            return ERR_NOMASTER;
        logmsg(LOGMSG_ERROR, "*ERROR* bdb_prim_allocdta return unhandled rc %d\n", bdberr);
        return ERR_INTERNAL;
    }
    return rc;
}

extern int gbl_use_fastseed_for_comdb2_seqno;
long long get_unique_longlong(struct dbenv *env)
{
    long long id;

    if (gbl_use_fastseed_for_comdb2_seqno) {
        uint64_t uid;

        uid = comdb2fastseed();
        uid = flibc_htonll(uid);
        memcpy(&id, &uid, sizeof(uid));
    } else {
        struct ireq iq;
        init_fake_ireq(env, &iq);
        iq.usedb = env->dbs[0];
        get_context(&iq, (unsigned long long *)&id);
    }

    return id;
}

void debug_traverse_data(char *tbl)
{
    struct dbtable *db;
    db = get_dbtable_by_name(tbl);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "Unknown table %s\n", tbl);
        return;
    }
    bdb_dumpit(db->handle);
}

void debug_bulktraverse_data(char *tbl)
{
    struct dbtable *db;
    db = get_dbtable_by_name(tbl);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "Unknown table %s\n", tbl);
        return;
    }
    bdb_bulkdumpit(db->handle);
}

#ifdef BERKDB_46

int bdb_compact_table(bdb_state_type *bdb_state, int *bdberr, int timeout,
                      int freefs);

int compact_db(struct dbtable *db, int timeout, int freefs)
{
    int bdberr;
    void *bdb_handle;
    int numrrns;
    int retries = 0;

    bdb_handle = get_bdb_handle(db, AUXDB_NONE);
    if (!bdb_handle)
        return ERR_NO_AUXDB;

/* there are also retries for the mini transactions */
retry:
    numrrns = bdb_compact_table(bdb_handle, &bdberr, timeout, freefs);
    if (numrrns == -1) {
        if (bdberr == BDBERR_DEADLOCK) {
            if (++retries < gbl_maxretries) {
                n_retries++;
                goto retry;
            }
            logmsg(LOGMSG_ERROR, 
                   "*ERROR* bdb_compact_table too much contention %d count %d\n",
                   bdberr, retries);
            return -1;
        }
        logmsg(LOGMSG_ERROR, 
               "*ERROR* bdb_compact_table return unhandled rc %d\n", bdberr);
        return -1;
    }

    return numrrns;
}
#endif

int find_record_older_than(struct ireq *iq, void *tran, int timestamp,
                           void *rec, int *reclen, int maxlen,
                           unsigned long long *genid)
{
    int stripe;
    int rc;
    int bdberr = 0;
    int genid_timestamp;
    uint8_t ver;

    for (stripe = 0; stripe < gbl_dtastripe; stripe++) {
        rc = bdb_find_oldest_genid(iq->usedb->handle, tran, stripe, rec, reclen,
                                   maxlen, genid, &ver, &bdberr);
        if (rc && bdberr == BDBERR_DEADLOCK)
            return RC_INTERNAL_RETRY;
        else if (rc == 1) /* nothing in this stripe */
            continue;
        genid_timestamp = bdb_genid_timestamp(*genid);
        if (genid_timestamp < timestamp) {
            vtag_to_ondisk(iq->usedb, rec, reclen, ver, *genid);
            return 0;
        }
    }

    return ERR_NO_RECORDS_FOUND;
}

/**
 * Check the blob for consistency; bdb layer returns success
 * if a blob is not found;  if the blob is not marked null
 * this is a race and we should fail the check
 */
static int ix_find_check_blob_race(struct ireq *iq, char *inbuf, int numblobs,
                                   int *blobnums, void **blobptrs)
{
    char *table = iq->usedb->dbname;
    struct schema *sch;
    struct field *fld;
    int i;
    int tmp;
    int vutf8len;
    int blobidx = 0;      /* walk blobnums[numblobs] list for the tag */
    int diskblobidx = -1; /* walk ondisk blobs */

    sch = find_tag_schema(table, ".ONDISK");
    if (sch == NULL)
        return -1;

    if (numblobs <= 0)
        return 0;

    for (i = 0; i < sch->nmembers; i++) {
        fld = &sch->member[i];

        /* is it a blob?*/
        if (fld->type != SERVER_BLOB && fld->type != SERVER_VUTF8)
            continue;

        /* ok, we got a disk blob */
        diskblobidx++; /*started with -1*/

        /* is it a blob I have asked for? */
        if (blobnums[blobidx] != diskblobidx)
            continue;

        /* ok, the found disk blob matches my next required blob*/

        /* check that the pointer is not NULL (even for 0 length blobs, there
           is a pointer non NULL !*/
        if (!stype_is_null(inbuf + fld->offset)) {
            /* Detect blob-race for a normal blob. */
            if (SERVER_BLOB == fld->type && blobptrs[blobidx] == NULL) {
                logmsg(LOGMSG_ERROR, "Detected blob race\n");
                return -1;
            }

            /* Detect blob-race for vutf8 blobs. */
            if (SERVER_VUTF8 == fld->type && blobptrs[blobidx] == NULL) {
                memcpy(&tmp, inbuf + fld->offset + 1, sizeof(int));
                vutf8len = ntohl(tmp);

                /* Datalen + headerlen should fit within the record. */
                if (vutf8len + 5 > fld->len) {
                    logmsg(LOGMSG_ERROR, "Detected vutf8-blob race\n");
                    return -1;
                }
            }
        }

        blobidx++;

        /* got all the tag blobs? */
        if (blobidx > numblobs)
            break;
    }

    return 0;
}

/**
 * Try to retrieve a genid in the simplest form
 * We assume no data corruption (i.e. we don't try to validate
 * the row, that would be to slow and beyond this code purpose
 * Returns 0 if not found, 1 if found, -1 if error
 *
 */
int ix_check_genid(struct ireq *iq, void *trans, unsigned long long genid,
                   int *bdberr)
{
    int rc = 0;
    int reqdtalen = 0;

    *bdberr = 0;
    rc = ix_find_by_rrn_and_genid_tran(iq, 2 /*rrn*/, genid, NULL, &reqdtalen,
                                       0, trans);
    if (rc == IX_FND)
        return 1;
    if (rc == IX_NOTFND)
        return 0;
    *bdberr = rc;
    return -1;
}

unsigned long long get_commit_context(const void *plsn, uint32_t generation)
{
    return bdb_gen_commit_genid(thedb->bdb_env, plsn, generation);
}

void set_commit_lsn_gen(const DB_LSN *lsn, uint32_t generation)
{
    bdb_set_commit_lsn_gen(thedb->bdb_env, lsn, generation);
}

int set_commit_context(unsigned long long context, uint32_t *generation,
                       void *plsn, void *args, unsigned int rectype)
{
    if (!gbl_ready)
        return -1;

    bdb_set_commit_genid(thedb->bdb_env, context, generation, plsn, args,
                         rectype);
    return 0;
}

/* start listening on our ports */
int setup_net_listen_all(struct dbenv *dbenv)
{
    if (gbl_create_mode || (gbl_fullrecovery && gbl_exit))
        return 0;

    /* register/fetch our ports first */
    int port = dbenv->sibling_port[0][NET_REPLICATION];
    if (port <= 0) {
        if (!gbl_disable_etc_services_lookup)
            port = net_get_port_by_service(dbenv->envname);
        if (port <= 0)
            port = portmux_register("comdb2", "replication", dbenv->envname);
        else
            goto use;
        if (port <= 0) {
            logmsg(LOGMSG_ERROR, 
                   "couldn't get port for replication/primary net from portmux\n");
            return -1;
        }
        dbenv->sibling_port[0][NET_REPLICATION] = port;
    } else {
            // Ignore if this fails; we're hard coded to use this port.
            // Just go ahead and use it.
use:        portmux_use("comdb2", "replication", dbenv->envname, port);
    }
    if (gbl_accept_on_child_nets) {
        if (dbenv->sibling_port[0][NET_SQL] == 0) {
            dbenv->sibling_port[0][NET_SQL] =
                portmux_register("comdb2", "offloadsql", dbenv->envname);
            if (dbenv->sibling_port[0][NET_REPLICATION] == -1) {
                logmsg(LOGMSG_ERROR, 
                       "couldn't get port for offloadsql net from portmux\n");
                return -1;
            }
        }
        if (dbenv->sibling_port[0][NET_SIGNAL] == 0) {
            dbenv->sibling_port[0][NET_SIGNAL] =
                portmux_register("comdb2", "signal", dbenv->envname);
            if (dbenv->sibling_port[0][NET_SIGNAL] == -1) {
                logmsg(LOGMSG_ERROR, 
                        "couldn't get port for signal net from portmux\n");
                return -1;
            }
        }
    }

    logmsg(LOGMSG_INFO, "listen on %d for replication\n",
           dbenv->sibling_port[0][NET_REPLICATION]);
    dbenv->listen_fds[NET_REPLICATION] =
        net_listen(dbenv->sibling_port[0][NET_REPLICATION]);
    if (dbenv->listen_fds[NET_REPLICATION] == -1)
        return -1;

    if (gbl_accept_on_child_nets) {
        logmsg(LOGMSG_INFO, "listen on %d for sql\n", dbenv->sibling_port[0][NET_SQL]);
        dbenv->listen_fds[NET_SQL] =
            net_listen(dbenv->sibling_port[0][NET_SQL]);
        if (dbenv->listen_fds[NET_SQL] == -1)
            return -1;
        logmsg(LOGMSG_INFO, "listen on %d for signal\n", dbenv->sibling_port[0][NET_SIGNAL]);
        dbenv->listen_fds[NET_SIGNAL] =
            net_listen(dbenv->sibling_port[0][NET_SIGNAL]);
        if (dbenv->listen_fds[NET_SIGNAL] == -1)
            return -1;
    }

    return 0;
}

/**
 * Schema change that touches a table in any way updates its version
 * Called every time a schema change is successfully done
 */
int table_version_upsert(struct dbtable *db, void *trans, int *bdberr)
{
    int rc = bdb_table_version_upsert(db->handle, trans, bdberr);
    if(rc) return rc;

    //select needs to be done with the same transaction to avoid 
    //undetectable deadlock for writing and reading from same thread
    unsigned long long version;
    rc = bdb_table_version_select(db->dbname, trans, &version, bdberr);
    if (rc || *bdberr) {
        logmsg(LOGMSG_ERROR, "%s error version=%llu rc=%d bdberr=%d\n", __func__,
                version, rc, *bdberr);
        return -1;
    }

    db->tableversion = version;
    return 0;
}

/**
 * Retrieves table version or 0 if no entry
 *
 */
unsigned long long table_version_select(struct dbtable *db, tran_type *tran)
{
    int bdberr;
    unsigned long long version;
    int rc;

    rc = bdb_table_version_select(db->dbname, tran, &version, &bdberr);
    if (rc || bdberr) {
        logmsg(LOGMSG_ERROR, "%s error version=%llu rc=%d bdberr=%d\n", __func__,
                version, rc, bdberr);
        return -1;
    }

    return version;
}

/**
 * Set schema for a specific table, used for pinning table to certain versions
 * upon re-creation (for example)
 *
 */
int table_version_set(tran_type *tran, const char *tablename,
                      unsigned long long version)
{
    struct dbtable *db;
    unsigned long long ret;
    int rc;
    int bdberr = 0;

    if (is_tablename_queue(tablename, strlen(tablename))) return 0;

    db = get_dbtable_by_name(tablename);
    if (!db) {
        ctrace("table unknown \"%s\"\n", tablename);
        return -1;
    }

    rc = bdb_table_version_update(db->handle, tran, version, &bdberr);
    if (!rc && bdberr) rc = -1;

    db->tableversion = version;

    return rc;
}

void *get_bdb_env(void) { return thedb->bdb_env; }
