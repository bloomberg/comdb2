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

/**
 *
 *  Interface with sqlglue.c
 *
 *  Each sqlthread executing an osql request has associated an osqlstate_t
 *  structure.  The structure is registered in a checkboard during the
 *  osql request lifetime.
 *
 *  Sqlthreads will call insrec/updrec/delrec functions.
 *   - For socksql mode, each call results in a net transfer
 *     to the master.
 *   - For recom, snapisol and serial mode, each call will update the local
 *shadow tables (which
 *     will be transferred to the master once the transaction is committed).
 *
 */

#include <poll.h>
#include <util.h>
#include <unistd.h>
#include "sql.h"
#include "osqlsqlthr.h"
#include "osqlshadtbl.h"
#include "osqlcomm.h"
#include "osqlcheckboard.h"
#include <bdb_api.h>
#include "comdb2.h"
#include "genid.h"
#include "comdb2uuid.h"
#include "nodemap.h"
#include <bpfunc.h>

#include "debug_switches.h"
#include <net_types.h>
#include <trigger.h>
#include <logmsg.h>
#include "views.h"
#include <dbinc/queue.h>
#include "osqlsqlnet.h"
#include "schemachange.h"
#include "db_access.h"

extern int gbl_partial_indexes;
extern int gbl_expressions_indexes;
extern int gbl_reorder_socksql_no_deadlock;

int gbl_allow_bplog_restarts = 600;
int gbl_master_retry_poll_ms = 100;
int gbl_noleader_retry_duration_ms = 50 * 1000; /* wait up to 50 seconds for a new leader */
int gbl_noleader_retry_poll_ms = 10;

static int osql_send_usedb_logic(struct BtCursor *pCur, struct sql_thread *thd,
                                 int nettype);
static int osql_send_delidx_logic(struct BtCursor *pCur,
                                  struct sqlclntstate *clnt, int nettype);
static int osql_send_insidx_logic(struct BtCursor *pCur,
                                  struct sqlclntstate *clnt, int nettype);
static int osql_send_qblobs_logic(struct BtCursor *pCur, osqlstate_t *osql,
                                  int *updCols, blob_buffer_t *blobs,
                                  int nettype);
static int osql_send_recordgenid_logic(struct BtCursor *pCur,
                                       struct sql_thread *thd,
                                       unsigned long long genid, int nettype);
int osql_send_updstat_logic(struct BtCursor *pCur, struct sql_thread *thd,
                            char *pData, int nData, int nStat, int nettype);
static int osql_send_commit_logic(struct sqlclntstate *clnt, int is_retry,
                                  int nettype);
static int osql_send_abort_logic(struct sqlclntstate *clnt, int nettype);
static int check_osql_capacity(struct sql_thread *thd);

static int osql_begin(struct sqlclntstate *clnt, int type, int keep_rqid);
static int osql_end(struct sqlclntstate *clnt);
static int osql_wait(struct sqlclntstate *clnt);

static int osql_sock_restart(struct sqlclntstate *clnt, int maxretries,
                             int keep_session);

#ifdef DEBUG_REORDER
#define DEBUG_PRINT_NUMOPS()                                                   \
    do {                                                                       \
        uuidstr_t us;                                                          \
        DEBUGMSG("uuid=%s, replicant_numops=%d\n",                             \
                 comdb2uuidstr(osql->uuid, us), osql->replicant_numops);       \
    } while (0)
#else
#define DEBUG_PRINT_NUMOPS()
#endif

/*
   artificially limit the size of the transaction;
   apparently this avoids some bugs and makes the db happy
   - less deadlocks
   - no timeouts
   - no replication bugout on 4.2
*/
int g_osql_max_trans = 50000;

/**
 * Set maximum osql transaction size
 *
 */
inline void set_osql_maxtransfer(int limit)
{
    g_osql_max_trans = limit;
}

/**
 * Get maximum osql transaction size
 *
 */
inline int get_osql_maxtransfer(void)
{
    return g_osql_max_trans;
}

/**
 * Set the maximum time throttling offload-sql requests
 *
 */
inline void set_osql_maxthrottle_sec(int limit)
{
    gbl_osql_max_throttle_sec = limit;
}

inline int get_osql_maxthrottle_sec(void)
{
    return gbl_osql_max_throttle_sec;
}

int gbl_osql_random_restart = 0;

static inline int osql_should_restart(struct sqlclntstate *clnt, int rc,
                                      int keep_rqid)
{
    if (rc == OSQL_SEND_ERROR_WRONGMASTER &&
        (clnt->dbtran.mode == TRANLEVEL_SOSQL ||
         clnt->dbtran.mode == TRANLEVEL_RECOM)) {
        return 1;
    }

    if (gbl_osql_random_restart && (rand() % 100) == 0) {
        uuidstr_t us;
        snap_uid_t snap = {{0}};
        get_cnonce(clnt, &snap);
        logmsg(LOGMSG_USER,
               "Forcing random-restart of uuid=%s cnonce=%*s after nops=%d keep_rqid=%d\n",
               comdb2uuidstr(clnt->osql.uuid, us), snap.keylen, snap.key,
               clnt->osql.replicant_numops, keep_rqid);
        return 1;
    }

    return 0;
}

#define RESTART_SOCKSQL_KEEP_RQID(keep_rqid)                                   \
    do {                                                                       \
        restarted = 0;                                                         \
        if (osql_should_restart(clnt, rc, keep_rqid)) {                        \
            rc = osql_sock_restart(clnt, gbl_allow_bplog_restarts, keep_rqid); \
            if (rc) {                                                          \
                logmsg(LOGMSG_ERROR,                                           \
                       "%s: failed to restart socksql session rc=%d\n",        \
                       __func__, rc);                                          \
            } else {                                                           \
                restarted = 1;                                                 \
            }                                                                  \
        }                                                                      \
        if (rc) {                                                              \
            logmsg(LOGMSG_ERROR,                                               \
                   "%s: error writting record to master in offload mode "      \
                   "rc=%d!\n",                                                 \
                   __func__, rc);                                              \
            if (rc != SQLITE_TOOBIG && rc != ERR_SC)                           \
                rc = SQLITE_INTERNAL;                                          \
        } else {                                                               \
            rc = SQLITE_OK;                                                    \
        }                                                                      \
    } while (0)

#define RESTART_SOCKSQL RESTART_SOCKSQL_KEEP_RQID(0)

#define START_SOCKSQL                                                          \
    do {                                                                       \
        if (!clnt->osql.sock_started) {                                        \
            rc = osql_sock_start(clnt, OSQL_SOCK_REQ, 0);                      \
            if (rc) {                                                          \
                logmsg(LOGMSG_ERROR,                                           \
                       "%s: failed to start socksql transaction rc=%d\n",      \
                       __func__, rc);                                          \
                if (rc != SQLITE_ABORT)                                        \
                    rc = SQLITE_CLIENT_CHANGENODE;                             \
                return rc;                                                     \
            }                                                                  \
            uuidstr_t us;                                                      \
            sql_debug_logf(clnt, __func__, __LINE__,                           \
                           "osql_sock_start returns rqid %llu uuid %s\n",      \
                           clnt->osql.rqid,                                    \
                           comdb2uuidstr(clnt->osql.uuid, us), rc);            \
        }                                                                      \
    } while (0)

/* see below */
enum {
    OSQL_START_KEEP_RQID = 1,
    OSQL_START_NO_REORDER = 2,
};
static int osql_sock_start_int(struct sqlclntstate *clnt, int type,
                               int start_flags)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    osqlstate_t *osql = &clnt->osql;
    int flags = 0;
    int rc = 0;
    const int poll_ms = gbl_noleader_retry_poll_ms;
    int retries = 0;
    const int max_retries = gbl_noleader_retry_duration_ms / poll_ms;
    int keep_rqid = start_flags & OSQL_START_KEEP_RQID;

    /* new id */
    if (!keep_rqid) {
        if (gbl_noenv_messages) {
            osql->rqid = OSQL_RQID_USE_UUID;
            comdb2uuid(osql->uuid);
        } else {
            osql->rqid = comdb2fastseed(0);
            comdb2uuid_clear(osql->uuid);
            assert(osql->rqid);
        }
    }

    osql->is_reorder_on = start_flags & OSQL_START_NO_REORDER
                              ? 0
                              : gbl_reorder_socksql_no_deadlock;

    /* lets reset error, this could be a retry */
    osql->xerr.errval = 0;
    osql->xerr.errstr[0] = '\0';

retry:
    rc = clnt_check_bdb_lock_desired(clnt);
    if (rc) {
        logmsg(LOGMSG_ERROR, "recover_deadlock returned %d\n", rc);
        rc = osql_end(clnt);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s failed to end osql %d\n", __func__, rc);
        }
        return SQLITE_BUSY;
    }

    rc = osql_begin(clnt, type, keep_rqid);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s failed to start osql rc %d\n", __func__, rc);
        return SQLITE_BUSY;
    }
    /* protect against no master */
    if (osql->target.host == NULL || osql->target.host == db_eid_invalid) {
        if (retries < max_retries) {
            retries++;
            if (retries % (1000 / poll_ms) == 0) { /* reduce spew - once a second */
                if (gbl_noenv_messages) {
                    uuidstr_t us;
                    comdb2uuidstr(osql->uuid, us);
                    logmsg(LOGMSG_WARN, "Retrying to find the master retries=%d uuid:%s\n", retries, us);
                } else {
                    logmsg(LOGMSG_WARN, "Retrying to find the master retries=%d rqid:%llx\n", retries, osql->rqid);
                }
            }
            poll(NULL, 0, poll_ms);
            goto retry;
        } else {
            if (gbl_noenv_messages) {
                uuidstr_t us;
                comdb2uuidstr(osql->uuid, us);
                logmsg(LOGMSG_ERROR, "%s: no master for uuid:%s\n", __func__, us);
            } else {
                logmsg(LOGMSG_ERROR, "%s: no master for rqid:%llx\n", __func__, osql->rqid);
            }
            errstat_set_rc(&osql->xerr, ERR_NOMASTER);
            errstat_set_str(&osql->xerr, "No master available");
            return SQLITE_ABORT;
        }
    }

    /* socksql: check if this is a verify retry, and if we got enough of those
       to trigger a self-deadlock check on the master */

    if ((type == OSQL_SOCK_REQ || type == OSQL_SOCK_REQ_COST) &&
        clnt->verify_retries > gbl_osql_verify_ext_chk)
        flags |= OSQL_FLAGS_CHECK_SELFLOCK;
    else
        flags = 0;

    if (osql->is_reorder_on)
        flags |= OSQL_FLAGS_REORDER_ON;

    /* send request to blockprocessor */
    rc = osql_comm_send_socksqlreq(&osql->target, clnt->sql,
                                   strlen(clnt->sql) + 1, osql->rqid,
                                   osql->uuid, clnt->tzname, type, flags);

    if (rc != 0 && retries < max_retries) {
        retries++;
        if (retries % (1000 / poll_ms) == 0) { /* reduce spew */
            if (gbl_noenv_messages) {
                uuidstr_t us;
                comdb2uuidstr(osql->uuid, us);
                logmsg(LOGMSG_WARN, "Retrying to find the master (2) retries=%d uuid:%s\n", retries, us);
            } else {
                logmsg(LOGMSG_WARN, "Retrying to find the master (2) retries=%d rqid:%llx\n", retries, osql->rqid);
            }
        }
        poll(NULL, 0, poll_ms);
        goto retry;
    }

    if (rc) {
        sql_debug_logf(
            clnt, __func__, __LINE__,
            "Tried %d times and failed rc %d returning SQLITE_ABORT\n", retries,
            rc);
        errstat_set_rc(&osql->xerr, ERR_NOMASTER);
        errstat_set_str(&osql->xerr, "No leader available");
        rc = SQLITE_ABORT;
    }

    if (rc == 0) {
        if (clnt->client_understands_query_stats)
            osql_query_dbglog(thd, clnt->queryid);
        osql->sock_started = 1;
    } else if (!keep_rqid) {
        int irc = osql_end(clnt);
        if (irc) {
            logmsg(LOGMSG_ERROR, "%s failed to end osql rc %d irc %d\n",
                   __func__, rc, irc);
        }
    }

    return rc;
}

/**
 * This is called on the replicant node and starts a sosql session,
 * which creates a blockprocessor peer on the master node
 * Returns ok if the packet is sent successful to the master
 * If keep_rqid, this is a retry and we want to keep the same rqid
 */
int osql_sock_start(struct sqlclntstate *clnt, int type, int keep_id)
{
    int flags = keep_id ? OSQL_START_KEEP_RQID : 0;
    return osql_sock_start_int(clnt, type, flags);
}

/* TODO: disable this on the master; if dbq deletes are there */
int osql_sock_start_no_reorder(struct sqlclntstate *clnt, int type, int keep_id)
{
    int flags = OSQL_START_NO_REORDER;
    flags |= keep_id ? OSQL_START_KEEP_RQID : 0;
    return osql_sock_start_int(clnt, type, flags);
}

int osql_sock_start_deferred(struct sqlclntstate *clnt)
{
    int rc;
    if (clnt->dbtran.mode == TRANLEVEL_SOSQL)
        START_SOCKSQL;
    return 0;
}

static int osql_begin(struct sqlclntstate *clnt, int type, int keep_rqid)
{
    /* note: custom interface can still delegate to osql over net */
    if (clnt->begin) {
        if (!clnt->begin(clnt, type, keep_rqid))
            return 0;
    }

    /* default */
    return osql_begin_net(clnt, type, keep_rqid);
}

static int osql_end(struct sqlclntstate *clnt)
{
    /* note: custom interface can still delegate to osql over net */
    if (clnt->end)
        if (!clnt->end(clnt))
            return 0;

    /* default */
    return osql_end_net(clnt);
}

static int osql_wait(struct sqlclntstate *clnt)
{
    int timeout;
    osqlstate_t *osql = &clnt->osql;
    errstat_t dummy = {0};

    /* If an error is set (e.g., selectv error from range check), latch it. */
    errstat_t *err = (osql->xerr.errval == 0) ? &osql->xerr : &dummy;

    if (osql->running_ddl)
        timeout = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SOSQL_DDL_MAX_COMMIT_WAIT_SEC);
    else
        timeout = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SOSQL_MAX_COMMIT_WAIT_SEC);

    if (clnt->wait)
        if (!clnt->wait(clnt, timeout, err))
            return 0;

    return osql_chkboard_wait_commitrc(osql->rqid, osql->uuid, timeout, err);
}

/**
 * Process the actual sending of the delrec
 */
static int osql_send_del_logic(struct BtCursor *pCur, struct sql_thread *thd)
{
    struct sqlclntstate *clnt = thd->clnt;
    osqlstate_t *osql = &clnt->osql;

    int rc = osql_send_usedb_logic(pCur, thd, NET_OSQL_SOCK_RPL);
    if (rc != SQLITE_OK)
        return rc;

    if (osql->is_reorder_on) {
        rc = osql_send_delrec(
            &osql->target, osql->rqid, osql->uuid, pCur->genid,
            (gbl_partial_indexes && pCur->db->ix_partial) ? clnt->del_keys
                                                          : -1ULL,
            NET_OSQL_SOCK_RPL);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql row rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
    }

    if (gbl_expressions_indexes && pCur->db->ix_expr) {
        rc = osql_send_delidx_logic(pCur, thd->clnt, NET_OSQL_SOCK_RPL);
        if (rc != SQLITE_OK) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql row rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
    }

    if (!osql->is_reorder_on) {
        rc = osql_send_delrec(
            &osql->target, osql->rqid, osql->uuid, pCur->genid,
            (gbl_partial_indexes && pCur->db->ix_partial) ? clnt->del_keys
                                                          : -1ULL,
            NET_OSQL_SOCK_RPL);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql row rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
    }
    osql->replicant_numops++;
    DEBUG_PRINT_NUMOPS();

    return SQLITE_OK;
}

/**
 * Process a sqlite delete row request
 * BtCursor points to the record to be deleted.
 * Returns SQLITE_OK if successful.
 *
 */
int osql_delrec(struct BtCursor *pCur, struct sql_thread *thd)
{
    struct sqlclntstate *clnt = thd->clnt;
    int restarted;
    int rc = 0;

    if ((rc = access_control_check_sql_write(pCur, thd)))
        return rc;

    /* limit transaction*/
    if ((rc = check_osql_capacity(thd)))
        return rc;

    if (clnt->dbtran.mode == TRANLEVEL_SOSQL) {
        START_SOCKSQL;
        do {
            rc = osql_send_del_logic(pCur, thd);
            RESTART_SOCKSQL;
        } while (restarted);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql delrec rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
    }

    if (gbl_expressions_indexes && pCur->db->ix_expr) {
        rc = osql_save_index(pCur, thd, 0 /* isupd */, 1 /*isdel*/);
        if (rc != SQLITE_OK)
            return rc;
    }

    return osql_save_delrec(pCur, thd);
}

/**
 * Process an insert or update to sqlite_stat1.
 * This is handled specially to allow us to save the previous data to llmeta.
 * Returns SQLITE_OK if successul.
 *
 */

inline int osql_updstat(struct BtCursor *pCur, struct sql_thread *thd,
                        char *pData, int nData, int nStat)
{
    return osql_send_updstat_logic(pCur, thd, pData, nData, nStat,
                                   NET_OSQL_SOCK_RPL);
}

/**
 * Process the sending part for insrec
 */
static int osql_send_ins_logic(struct BtCursor *pCur, struct sql_thread *thd,
                               char *pData, int nData, blob_buffer_t *blobs,
                               int maxblobs, int flags)
{
    osqlstate_t *osql = &thd->clnt->osql;

    int rc = osql_send_usedb_logic(pCur, thd, NET_OSQL_SOCK_RPL);
    if (rc != SQLITE_OK)
        return rc;

    if (osql->is_reorder_on) {
        rc = osql_send_insrec(
            &osql->target, osql->rqid, osql->uuid, pCur->genid,
            (gbl_partial_indexes && pCur->db->ix_partial) ? thd->clnt->ins_keys
                                                          : -1ULL,
            pData, nData, NET_OSQL_SOCK_RPL, flags);

        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql row rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
    }

    if (gbl_expressions_indexes && pCur->db->ix_expr) {
        rc = osql_send_insidx_logic(pCur, thd->clnt, NET_OSQL_SOCK_RPL);
        if (rc != SQLITE_OK) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql row rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
    }

    rc = osql_send_qblobs_logic(pCur, osql, NULL, blobs, NET_OSQL_SOCK_RPL);
    if (rc != SQLITE_OK) {
        logmsg(LOGMSG_ERROR, "%s:%d %s - failed to send socksql row rc=%d\n",
               __FILE__, __LINE__, __func__, rc);
        return rc;
    }

    if (!osql->is_reorder_on) {
        rc = osql_send_insrec(
            &osql->target, osql->rqid, osql->uuid, pCur->genid,
            (gbl_partial_indexes && pCur->db->ix_partial) ? thd->clnt->ins_keys
                                                          : -1ULL,
            pData, nData, NET_OSQL_SOCK_RPL, flags);

        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql row rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
    }
    osql->replicant_numops++;
    DEBUG_PRINT_NUMOPS();
    return SQLITE_OK;
}

/**
 * Process a sqlite insert row request
 * Row is provided by (pData, nData, blobs, maxblobs)
 * Returns SQLITE_OK if successful.
 *
 */
int osql_insrec(struct BtCursor *pCur, struct sql_thread *thd, char *pData,
                int nData, blob_buffer_t *blobs, int maxblobs, int flags)
{
    struct sqlclntstate *clnt = thd->clnt;
    int restarted;
    int rc = 0;

    if ((rc = access_control_check_sql_write(pCur, thd)))
        return rc;

    /* limit transaction*/
    if ((rc = check_osql_capacity(thd)))
        return rc;

    if (clnt->dbtran.mode == TRANLEVEL_SOSQL) {
        START_SOCKSQL;
        do {
            rc = osql_send_ins_logic(pCur, thd, pData, nData, blobs, maxblobs,
                                     flags);
            RESTART_SOCKSQL;
        } while (restarted);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql insrec rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
    }

    if (gbl_expressions_indexes && pCur->db->ix_expr) {
        rc = osql_save_index(pCur, thd, 0 /* isupd */, 0 /*isdel*/);
        if (rc != SQLITE_OK)
            return rc;
    }

    rc = osql_save_qblobs(pCur, thd, blobs, maxblobs, 0);
    if (rc != SQLITE_OK)
        return rc;

    rc = osql_save_insrec(pCur, thd, pData, nData, flags);
    return rc;
}

/**
 * process the sending of updrec
 */
static int osql_send_upd_logic(struct BtCursor *pCur, struct sql_thread *thd,
                               char *pData, int nData, int *updCols,
                               blob_buffer_t *blobs, int maxblobs, int flags)
{
    osqlstate_t *osql = &thd->clnt->osql;

    int rc = osql_send_usedb_logic(pCur, thd, NET_OSQL_SOCK_RPL);
    if (rc != SQLITE_OK)
        return rc;

    if (osql->is_reorder_on) {
        rc = osql_send_updrec(
            &osql->target, osql->rqid, osql->uuid, pCur->genid,
            (gbl_partial_indexes && pCur->db->ix_partial) ? thd->clnt->ins_keys
                                                          : -1ULL,
            (gbl_partial_indexes && pCur->db->ix_partial) ? thd->clnt->del_keys
                                                          : -1ULL,
            pData, nData, NET_OSQL_SOCK_RPL);

        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql row rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
    }

    if (gbl_expressions_indexes && pCur->db->ix_expr) {
        rc = osql_send_delidx_logic(pCur, thd->clnt, NET_OSQL_SOCK_RPL);
        if (rc != SQLITE_OK) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql row rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }

        rc = osql_send_insidx_logic(pCur, thd->clnt, NET_OSQL_SOCK_RPL);
        if (rc != SQLITE_OK) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql row rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
    }

    rc = osql_send_qblobs_logic(pCur, osql, updCols, blobs, NET_OSQL_SOCK_RPL);
    if (rc != SQLITE_OK) {
        logmsg(LOGMSG_ERROR, "%s:%d %s - failed to send socksql row rc=%d\n",
               __FILE__, __LINE__, __func__, rc);
        return rc;
    }

    if (updCols) {
        rc = osql_send_updcols(&osql->target, osql->rqid, osql->uuid,
                               pCur->genid, NET_OSQL_SOCK_RPL, &updCols[1],
                               updCols[0]);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql row rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
        osql->replicant_numops++;
        DEBUG_PRINT_NUMOPS();
    }

    if (!osql->is_reorder_on) {
        rc = osql_send_updrec(
            &osql->target, osql->rqid, osql->uuid, pCur->genid,
            (gbl_partial_indexes && pCur->db->ix_partial) ? thd->clnt->ins_keys
                                                          : -1ULL,
            (gbl_partial_indexes && pCur->db->ix_partial) ? thd->clnt->del_keys
                                                          : -1ULL,
            pData, nData, NET_OSQL_SOCK_RPL);

        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql row rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
    }

    osql->replicant_numops++;
    DEBUG_PRINT_NUMOPS();
    return SQLITE_OK;
}

/**
 * Process a sqlite update row request
 * New row is provided by (pData, nData, blobs, maxblobs)
 * BtCursor points to the old row.
 * Returns SQLITE_OK if successful.
 *
 */
int osql_updrec(struct BtCursor *pCur, struct sql_thread *thd, char *pData,
                int nData, int *updCols, blob_buffer_t *blobs, int maxblobs,
                int flags)
{
    struct sqlclntstate *clnt = thd->clnt;
    int restarted;
    int rc = 0;

    if ((rc = access_control_check_sql_write(pCur, thd)))
        return rc;

    /* limit transaction*/
    if ((rc = check_osql_capacity(thd)))
        return rc;

    if (clnt->dbtran.mode == TRANLEVEL_SOSQL) {
        START_SOCKSQL;
        do {
            rc = osql_send_upd_logic(pCur, thd, pData, nData, updCols, blobs,
                                     maxblobs, flags);

            RESTART_SOCKSQL;
        } while (restarted);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql updrec rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
    }

    if (gbl_expressions_indexes && pCur->db->ix_expr) {
        rc = osql_save_index(pCur, thd, 1 /* isupd */, 1 /*isdel*/);
        if (rc != SQLITE_OK)
            return rc;

        rc = osql_save_index(pCur, thd, 1 /* isupd */, 0 /*isdel*/);
        if (rc != SQLITE_OK)
            return rc;
    }

    rc = osql_save_qblobs(pCur, thd, blobs, maxblobs, 1);
    if (rc != SQLITE_OK)
        return rc;

    rc = osql_save_updcols(pCur, thd, updCols);
    if (rc != SQLITE_OK)
        return rc;

    return osql_save_updrec(pCur, thd, pData, nData, flags);
}

/**
 * Process a sqlite clear table request
 * Currently not implemented due to non-transactional vs slowness unresolved
 * issues.
 *
 */
int osql_cleartable(struct sql_thread *thd, char *dbname)
{

    logmsg(LOGMSG_ERROR, "cleartable not implemented!\n");
    return -1;
}

int nettypetouuidnettype(int nettype)
{
    switch (nettype) {
    case NET_OSQL_SOCK_RPL:
        return NET_OSQL_SOCK_RPL_UUID;
    case NET_OSQL_SERIAL_RPL:
        return NET_OSQL_SERIAL_RPL_UUID;
    default:
        logmsg(LOGMSG_ERROR, "Unsupported nettype %d\n", nettype);
        return nettype;
    }
}

int osql_serial_send_readset(struct sqlclntstate *clnt, int nettype)
{
    osqlstate_t *osql = &clnt->osql;
    int rc = 0;

    if (nettype == NET_OSQL_SERIAL_RPL) {
        if (clnt->arr->size == 0)
            return 0;
    } else {
        if (clnt->selectv_arr->size == 0)
            return 0;
    }

    if (osql->rqid == OSQL_RQID_USE_UUID)
        nettype = nettypetouuidnettype(nettype);

    CurRangeArr *arr_ptr;
    if (nettype == NET_OSQL_SERIAL_RPL || nettype == NET_OSQL_SERIAL_RPL_UUID)
        arr_ptr = clnt->arr;
    else
        arr_ptr = clnt->selectv_arr;

    rc = osql_send_serial(&osql->target, osql->rqid, osql->uuid, arr_ptr,
                          arr_ptr->file, arr_ptr->offset, nettype);
    osql->replicant_numops++;
    DEBUG_PRINT_NUMOPS();
    return rc;
}

int gbl_master_swing_sock_restart_sleep = 0;
/**
 * Restart a broken socksql connection by opening a new blockproc on the
 * provided master and sending the cache rows to resume the current.
 * If keep_session is set, the same rqid is used for the replay
 */
static int osql_sock_restart(struct sqlclntstate *clnt, int maxretries,
                             int keep_session)
{
    osqlstate_t *osql = &clnt->osql;
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    uuidstr_t us;
    int rc = 0;
    int retries = 0;
    int bdberr = 0;
    int sentops = 0;

    if (!thd) {
        logmsg(LOGMSG_ERROR, "%s:%d Bug, not sql thread !\n", __func__, __LINE__);
        cheap_stack_trace();
    }

    do {
        /* we need to check if we need bdb write lock here to prevent a master
           upgrade blockade */
        rc = clnt_check_bdb_lock_desired(clnt);
        if (rc) {
            logmsg(LOGMSG_ERROR, "recover_deadlock returned %d\n", rc);
            rc = osql_end(clnt);
            if (rc)
                logmsg(LOGMSG_ERROR, "%s: failed to end clnt rc=%d\n", __func__,
                       rc);
            return ERR_RECOVER_DEADLOCK;
        }

        retries++;
        /* if we're shaking really badly, back off */
        if (retries > 1)
            usleep(retries * 10000); // sleep for a multiple of 10ms

        sentops = 0;

        osql->replicant_numops = 0;
        if (osql->tablename) {
            free(osql->tablename);
            osql->tablename = NULL;
            osql->tablenamelen = 0;
        }

        if (!keep_session) {
            if (gbl_master_swing_osql_verbose)
                logmsg(LOGMSG_USER,
                       "0x%p Starting new session rqid=%llx, uuid=%s\n",
                       (void*)pthread_self(), clnt->osql.rqid,
                       comdb2uuidstr(clnt->osql.uuid, us));
            rc = osql_end(clnt);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s failed to end osql rc %d\n", __func__,
                       rc);
                return rc;
            }
        } else if (gbl_master_swing_osql_verbose) {
            snap_uid_t snap = {{0}};
            get_cnonce(clnt, &snap);
            logmsg(LOGMSG_USER, "0x%p Restarting rqid=%llx uuid=%s against %s\n",
                   (void*)pthread_self(), clnt->osql.rqid,
                   comdb2uuidstr(clnt->osql.uuid, us), thedb->master);
        }

        rc = osql_sock_start(clnt,
                             (clnt->dbtran.mode == TRANLEVEL_SOSQL)
                                 ? OSQL_SOCK_REQ
                                 : OSQL_RECOM_REQ,
                             keep_session);
        if (rc) {
            sql_debug_logf(clnt, __func__, __LINE__,
                           "osql_sock_start returns %d\n", rc);
            logmsg(LOGMSG_ERROR, "osql_sock_start error rc=%d, retries=%d\n",
                   rc, retries);
            if (rc == SQLITE_ABORT)
                break;
            continue;
        }

        /* process messages from cache */
        rc = osql_shadtbl_process(clnt, &sentops, &bdberr, 1);
        if (rc == SQLITE_TOOBIG) {
            logmsg(LOGMSG_ERROR, "%s: transaction too big %d\n", __func__,
                   sentops);
            return rc;
        }
        if (rc == ERR_SC) {
            logmsg(LOGMSG_ERROR, "%s: schema change error\n", __func__);
            return rc;
        }

        if (keep_session && gbl_master_swing_sock_restart_sleep) {
            sleep(gbl_master_swing_sock_restart_sleep);
        }

        /* selectv skip optimization, not an error */
        if (unlikely(rc == -2 || rc == -3))
            rc = 0;

        if (rc)
            logmsg(LOGMSG_ERROR,
                   "Error in restablishing sosql session, rc=%d, retries=%d\n",
                   rc, retries);
    } while (rc && retries < maxretries);

    if (rc) {
        sql_debug_logf(clnt, __func__, __LINE__,
                       "failed %d times to restart socksql session\n", retries);
        logmsg(LOGMSG_ERROR,
               "%s:%d %s failed %d times to restart socksql session\n",
               __FILE__, __LINE__, __func__, retries);
        return -1;
    }

    return 0;
}

int gbl_random_blkseq_replays;
int gbl_osql_send_startgen = 1;

static inline int sock_restart_retryable_rcode(int restart_rc)
{
    switch (restart_rc) {
    case SQLITE_TOOBIG:
    case ERR_SC:
    case ERR_RECOVER_DEADLOCK:
    case SQLITE_COMDB2SCHEMA:
    case SQLITE_CLIENT_CHANGENODE:
        return 0;
    default:
        return 1;
    }
}

/**
 * Terminates a sosql session
 * Block processor is informed that all the rows are sent
 * It waits for the block processor to report back the return code
 * Returns the result of block processor commit
 *
 */
extern int gbl_is_physical_replicant;
int osql_sock_commit(struct sqlclntstate *clnt, int type, enum trans_clntcomm sideeffects)
{
    osqlstate_t *osql = &clnt->osql;
    int rc = 0, rc2;
    int rcout = 0;
    int retries = 0;
    int bdberr = 0;

    if (gbl_is_physical_replicant) {
        logmsg(LOGMSG_ERROR, "%s attempted write against physical replicant\n", __func__);
        osql_sock_abort(clnt, type);
        return SQLITE_READONLY;
    }

    /* temp hook for sql transactions */
    /* is it distributed? */
    if (clnt->dbtran.mode == TRANLEVEL_SOSQL && clnt->dbtran.dtran)
    {
        rc = fdb_trans_commit(clnt, sideeffects);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s distributed failure rc=%d\n", __func__, rc);

            rc2 = osql_sock_abort(clnt, type);

            if (rc2) {
                logmsg(LOGMSG_ERROR, "%s osql_sock_abort failed with rc=%d\n",
                        __func__, rc2);
            }

            return SQLITE_ABORT;
        }
    }

    osql->timings.commit_start = osql_log_time();

    if (clnt->dbtran.mode == TRANLEVEL_SOSQL && !osql->sock_started) {
        goto done;
    }

    assert(osql->sock_started);

    /* send results of sql processing to block master */
    /* if (thd->clnt->query_stats)*/

retry:

    /* Release our locks.  Only table locks should be held at this point. */
    if (clnt->dbtran.cursor_tran) {
        rc = bdb_free_curtran_locks(thedb->bdb_env, clnt->dbtran.cursor_tran,
                                    &bdberr);
        if (rc) {
            rcout = rc;
            sql_debug_logf(clnt, __func__, __LINE__,
                           "got %d and setting rcout to %d\n", rc, rcout);
            goto err;
        }
    }

    rc = osql_send_commit_logic(clnt, retries, req2netrpl(type));
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d: failed to send commit to master rc was %d\n", __FILE__,
                __LINE__, rc);
        rcout = SQLITE_CLIENT_CHANGENODE;
        goto err;
    }

    /* if there was a sqlite error, don't wait for block processor */
    if (osql->xerr.errval == 0) {

        /* trap */
        if (!osql->rqid) {
            logmsg(LOGMSG_ERROR, "%s: !rqid %p %p???\n", __func__, clnt, (void *)pthread_self());
            /*cheap_stack_trace();*/
            abort();
        }
        rc = osql_wait(clnt);
        if (rc) {
            rcout = SQLITE_CLIENT_CHANGENODE;
            logmsg(LOGMSG_ERROR, "%s line %d setting rcout to (%d) from %d\n", 
                    __func__, __LINE__, rcout, rc);
        } else {

            if (gbl_random_blkseq_replays && ((rand() % 50) == 0)) {
                logmsg(LOGMSG_ERROR, "%s line %d forcing random blkseq retry\n",
                       __func__, __LINE__);
                osql->xerr.errval = ERR_NOMASTER;
            }
            /* we got a return from block processor \
               propagate the result/error
             */
            if (osql->xerr.errval) {
                /* lets check to see if a master swing happened and we need to
                 * retry */
                sql_debug_logf(clnt, __func__, __LINE__,
                               "returns xerr.errval=%d\n", osql->xerr.errval);
                if (osql->xerr.errval == ERR_NOMASTER ||
                    osql->xerr.errval == ERR_NOT_DURABLE ||
                    osql->xerr.errval == 999) {
                    if (bdb_attr_get(thedb->bdb_attr,
                                     BDB_ATTR_SC_RESUME_AUTOCOMMIT) &&
                        !in_client_trans(clnt) && osql->running_ddl) {
                        clnt->osql.xerr.errval = ERR_SC;
                        errstat_cat_str(&(clnt->osql.xerr),
                                        "Master node downgrading - new "
                                        "master will resume schemachange");
                        logmsg(LOGMSG_INFO,
                               "%s: Master node downgrading - new master "
                               "will resume schemachange\n",
                               __func__);
                        rcout = SQLITE_ABORT;
                        goto err;
                    }
                    if (retries++ < gbl_allow_bplog_restarts) {
                        sql_debug_logf(
                            clnt, __func__, __LINE__,
                            "lost "
                            "connection to master, retrying %d in %d "
                            "msec\n",
                            retries, gbl_master_retry_poll_ms);

                        poll(NULL, 0, gbl_master_retry_poll_ms);

                        rc = osql_sock_restart(
                            clnt, 1,
                            1 /*no new rqid*/); /* retry at higher level */
                        if (sock_restart_retryable_rcode(rc)) {
                            if (gbl_master_swing_sock_restart_sleep) {
                                sleep(gbl_master_swing_sock_restart_sleep);
                            }
                            goto retry;
                        }
                    }
                }
                /* transaction failed on the master, abort here as well */
                if (rc != SQLITE_TOOBIG) {
                    if (osql->xerr.errval == -109 /* SQLHERR_MASTER_TIMEOUT */) {
                        sql_debug_logf(
                            clnt, __func__, __LINE__,
                            "got %d and "
                            "setting rcout to MASTER_TIMEOUT, errval is "
                            "%d setting rcout to -109\n",
                            rc, osql->xerr.errval);
                        rcout = -109;
                    } else if (osql->xerr.errval == ERR_NOT_DURABLE ||
                               rc == SQLITE_CLIENT_CHANGENODE) {
                        /* Ask the client to change nodes */
                        sql_debug_logf(clnt, __func__, __LINE__,
                                       "got %d and setting rcout to "
                                       "CHANGENODE, errval is %d\n",
                                       rc, osql->xerr.errval);
                        rcout = SQLITE_CLIENT_CHANGENODE;
                    } else if (rc == SQLITE_COMDB2SCHEMA) {
                        /* Schema has changed */
                        sql_debug_logf(clnt, __func__, __LINE__,
                                       "got %d and setting rcout to "
                                       "SQLITE_COMDB2SCHEMA, errval is %d\n",
                                       rc, osql->xerr.errval);
                        rcout = SQLITE_COMDB2SCHEMA;
                    } else {
                        sql_debug_logf(clnt, __func__, __LINE__,
                                       "got %d and setting rcout to "
                                       "SQLITE_ABORT, errval is %d\n",
                                       rc, osql->xerr.errval);
                        // SQLITE_ABORT comes out as a "4" in the client,
                        // which is translated to 4 'null key constraint'.
                        rcout = SQLITE_ABORT;
                    }
                } else {
                    sql_debug_logf(clnt, __func__, __LINE__,
                                   "got %d and setting rcout to SQLITE_TOOBIG, "
                                   "errval is %d\n",
                                   rc, osql->xerr.errval);
                    rcout = SQLITE_TOOBIG;
                }
            } else {
                sql_debug_logf(clnt, __func__, __LINE__, "got %d from %s\n", rc,
                               osql->target.host); /*TODO */
            }
        }
        if (clnt->client_understands_query_stats && clnt->dbglog)
            append_debug_logs_from_master(clnt->dbglog,
                                          clnt->master_dbglog_cookie);
    } else {
        rcout = SQLITE_ERROR;
        logmsg(LOGMSG_ERROR, "%s line %d set rcout to %d xerr %d\n", __func__,
               __LINE__, rcout, osql->xerr.errval);
    }

err:
    /* unregister this osql thread from checkboard */
    rc = osql_end(clnt);
    if (rc && !rcout) {
        logmsg(LOGMSG_ERROR, "%s line %d setting rout to SQLITE_INTERNAL (%d) rc is %d\n", 
                __func__, __LINE__, SQLITE_INTERNAL, rc);
        rcout = SQLITE_INTERNAL;
    }

done:
    osql->timings.commit_end = osql_log_time();

    /* mark socksql as non-retriable if seletv are present
       also don't retry distributed transactions
     */
    if (clnt->osql.xerr.errval == (ERR_BLOCK_FAILED + ERR_VERIFY) &&
            clnt->dbtran.mode == TRANLEVEL_SOSQL && !clnt->dbtran.dtran) {
        int bdberr = 0;
        int iirc = 0;
        iirc = osql_shadtbl_has_selectv(clnt, &bdberr);
        if (iirc != 0) /* if error or has selectv rows */
        {
            if (iirc < 0) {
                logmsg(LOGMSG_ERROR, 
                        "%s: osql_shadtbl_has_selectv failed rc=%d bdberr=%d\n",
                        __func__, rc, bdberr);
            }
            osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_LAST);
        }
    }

    /* DDLs are also non-retriable */
    if (osql->xerr.errval == (ERR_BLOCK_FAILED + ERR_VERIFY) &&
            osql->running_ddl) {
        logmsg(LOGMSG_DEBUG, "%s: marking DDL transaction as non-retriable\n",
                __func__);
        osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_LAST);
    }

    osql_shadtbl_close(clnt);

    if (clnt->dbtran.mode == TRANLEVEL_SOSQL) {
        /* we also need to free the tran object */
        rc = trans_abort_shadow((void **)&clnt->dbtran.shadow_tran, &bdberr);
        if (rc)
            logmsg(LOGMSG_ERROR, 
                    "%s:%d failed to abort shadow tran for socksql rc=%d\n",
                    __FILE__, __LINE__, rc);
    }

    osql->sock_started = 0;

    return rcout;
}

/**
 * Terminates a sosql session
 * It notifies the block processor to abort the request
 *
 */
int osql_sock_abort(struct sqlclntstate *clnt, int type)
{
    int rcout = 0;
    int rc = 0;
    int bdberr = 0;

    /* temp hook for sql transactions */
    /* is it distributed? */
    if (clnt->dbtran.mode == TRANLEVEL_SOSQL && clnt->dbtran.dtran) {
        rc = fdb_trans_rollback(clnt);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s distributed failure rc=%d\n", __func__,
                   rc);
        }
    }

    /* am I talking already with the master? rqid != 0 */
    if (clnt->osql.rqid != 0 && clnt->osql.sock_started) {
        /* send results of sql processing to block master */
        rc = osql_send_abort_logic(clnt, req2netrpl(type));
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to send abort rc=%d\n", __func__, rc);
            /* we still need to unregister the clnt */
            rcout = SQLITE_INTERNAL;
        }

        rc = osql_wait(clnt);
        if (rc)
            logmsg(LOGMSG_WARN, "%s: osql_wait rc %d\n", __func__, rc);

        /* unregister this osql thread from checkboard */
        rc = osql_end(clnt);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to end clnt rc=%d\n", __func__,
                   rc);
            rcout = SQLITE_INTERNAL;
        }

        clnt->osql.sock_started = 0;
    }

    clnt->osql.sentops = 0;  /* reset statement size counter*/
    clnt->osql.tran_ops = 0; /* reset transaction size counter*/
    clnt->osql.replicant_numops = 0; /* reset replicant numops counter*/

    osql_shadtbl_close(clnt);

    /* we also need to free the tran object, if we fail to dispatch after a
       begin
       for example.
     */
    rc = trans_abort_shadow((void **)&clnt->dbtran.shadow_tran, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, 
                "%s:%d failed to abort shadow tran for socksql rc=%d\n",
                __FILE__, __LINE__, rc);
    }

    if (clnt->osql.tablename) {
        free(clnt->osql.tablename);
        clnt->osql.tablename = NULL;
        clnt->osql.tablenamelen = 0;
    }

    return rcout;
}

/********************** INTERNALS
 * ***********************************************/
int gbl_reject_mixed_ddl_dml = 1;

static int osql_send_usedb_logic_int(char *tablename, struct sqlclntstate *clnt,
                                     int nettype)
{
    osqlstate_t *osql = &clnt->osql;
    int rc = 0;
    int restarted;

    if (gbl_reject_mixed_ddl_dml && osql->running_ddl) {
        return SQLITE_DDL_MISUSE;
    }

    if (clnt->ddl_tables && hash_find_readonly(clnt->ddl_tables, tablename)) {
        return SQLITE_DDL_MISUSE;
    }
    if (clnt->dml_tables && !hash_find_readonly(clnt->dml_tables, tablename))
        hash_add(clnt->dml_tables, strdup(tablename));

    int tablenamelen = strlen(tablename) + 1; /*including trailing 0*/
    if (osql->tablename) {
        if (osql->tablenamelen == tablenamelen &&
            !strncmp(tablename, osql->tablename, osql->tablenamelen))
            /* we've already sent this, skip */
            return SQLITE_OK;

        /* free the cache */
        free(osql->tablename);
        osql->tablename = NULL;
        osql->tablenamelen = 0;
    }

    do {
        rc = osql_send_usedb(&osql->target, osql->rqid, osql->uuid, tablename,
                             nettype, comdb2_table_version(tablename));
        RESTART_SOCKSQL;
    } while (restarted);

    if (rc == SQLITE_OK) {
        /* cache the sent tablename */
        osql->tablename = strdup(tablename);
        if (osql->tablename)
            osql->tablenamelen = tablenamelen;
    }
    osql->replicant_numops++;
    DEBUG_PRINT_NUMOPS();

    return rc;
}

static int osql_send_usedb_logic(struct BtCursor *pCur, struct sql_thread *thd,
                                 int nettype)
{
    return osql_send_usedb_logic_int(pCur->db->tablename, thd->clnt,
                                     nettype);
}

inline int osql_send_updstat_logic(struct BtCursor *pCur,
                                   struct sql_thread *thd, char *pData,
                                   int nData, int nStat, int nettype)
{
    struct sqlclntstate *clnt = thd->clnt;
    osqlstate_t *osql = &clnt->osql;
    int rc = 0;
    int restarted;

    START_SOCKSQL;
    do {
        rc = osql_send_updstat(&osql->target, osql->rqid, osql->uuid,
                               pCur->genid, pData, nData, nStat, nettype);
        RESTART_SOCKSQL;
    } while (restarted);
    osql->replicant_numops++;
    DEBUG_PRINT_NUMOPS();

    return rc;
}

/**
 * Process a sqlite index insert request
 * Index is provided by thd->clnt->idxInsert
 * Returns SQLITE_OK if successful.
 *
 */
static int osql_send_insidx_logic(struct BtCursor *pCur,
                                  struct sqlclntstate *clnt, int nettype)
{
    osqlstate_t *osql = &clnt->osql;
    int rc = 0;
    int i;

    for (i = 0; i < pCur->db->nix; i++) {
        /* only send add keys when told */
        if (gbl_partial_indexes && pCur->db->ix_partial &&
            !(clnt->ins_keys & (1ULL << i)))
            continue;
        rc = osql_send_index(&osql->target, osql->rqid, osql->uuid, pCur->genid,
                             0, i, (char *)clnt->idxInsert[i],
                             getkeysize(pCur->db, i), nettype);
        if (rc)
            break;
        osql->replicant_numops++;
        DEBUG_PRINT_NUMOPS();
    }
    return rc;
}

/**
 * Process a sqlite index delete request
 * Index is provided by thd->clnt->idxDelete
 * Returns SQLITE_OK if successful.
 *
 */
static int osql_send_delidx_logic(struct BtCursor *pCur,
                                  struct sqlclntstate *clnt, int nettype)
{
    osqlstate_t *osql = &clnt->osql;
    int rc = 0;
    int i;

    for (i = 0; i < pCur->db->nix; i++) {
        /* only send delete keys when told */
        if (gbl_partial_indexes && pCur->db->ix_partial &&
            !(clnt->del_keys & (1ULL << i)))
            continue;

        rc = osql_send_index(&osql->target, osql->rqid, osql->uuid, pCur->genid,
                             1, i, (char *)clnt->idxDelete[i],
                             getkeysize(pCur->db, i), nettype);
        if (rc)
            break;
        osql->replicant_numops++;
        DEBUG_PRINT_NUMOPS();
    }
    return rc;
}

static int osql_send_qblobs_logic(struct BtCursor *pCur, osqlstate_t *osql,
                                  int *updCols, blob_buffer_t *blobs,
                                  int nettype)
{
    int rc = 0;

    for (int i = 0; i < pCur->db->schema->numblobs; i++) {
        /* NOTE: the blobs are NOT clustered: create table t1(id int not null,
         * b1 blob, b2 blob); insert into t1 (id, b2) values(0, x'11') so we
         * need to run through all the defined blobs.  we only need to send the
         * non-null blobs, and the master will fix up those we missed.
         */

        if (!blobs[i].exists)
            continue;

        /* Send length of -2 if this isn't being used in this update. */
        if (updCols && gbl_osql_blob_optimization && blobs[i].length > 0) {
            int idx =
                get_schema_blob_field_idx(pCur->db, ".ONDISK", i);
            /* AZ: is pCur->db->schema not set to ondisk so we can instead call
             * get_schema_blob_field_idx_sc(pCur->db,i); ?? */
            int ncols = updCols[0];
            if (idx >= 0 && idx < ncols && -1 == updCols[idx + 1]) {
                /* Put a token on the network if this isn't going to be used */
                rc = osql_send_qblob(&osql->target, osql->rqid, osql->uuid, i,
                                     pCur->genid, nettype, NULL, -2);
                if (rc)
                    break; /* break out from while loop so we can return rc */
                osql->replicant_numops++;
                DEBUG_PRINT_NUMOPS();

                continue;
            }
        }

        (void)odhfy_blob_buffer(pCur->db, blobs + i, i);

        rc = osql_send_qblob(&osql->target, osql->rqid, osql->uuid,
                             blobs[i].odhind, pCur->genid, nettype,
                             blobs[i].data, blobs[i].length);
        if (rc)
            break;
        osql->replicant_numops++;
        DEBUG_PRINT_NUMOPS();
    }

    return rc;
}

static int osql_send_commit_logic(struct sqlclntstate *clnt, int is_retry,
                                  int nettype)
{
    osqlstate_t *osql = &clnt->osql;
    int rc = 0;
    int restarted;
    snap_uid_t snap_info, *snap_info_p = NULL;
    snap_uid_t zero_snap_info = {0};

    /* reset the tablename */
    if (osql->tablename) {

        /* free the cache */
        free(osql->tablename);
        osql->tablename = NULL;
        osql->tablenamelen = 0;
    }
    osql->tran_ops = 0; /* reset transaction size counter*/

    extern int gbl_always_send_cnonce;
    if (osql->rqid == OSQL_RQID_USE_UUID && !clnt->dbtran.trans_has_sp &&
        (gbl_always_send_cnonce || has_high_availability(clnt))) {
        if (clnt->dbtran.maxchunksize > 0) {
            snap_info_p = &zero_snap_info;
        } else {
            // Pass to master the state of verify retry.
            // If verify retry is ON and error is retryable, don't write to
            // blkseq on master because replicant will retry.

            snap_info.replicant_is_able_to_retry = replicant_is_able_to_retry(clnt);
            snap_info.effects = clnt->effects;

            if (get_cnonce(clnt, &snap_info) == 0) {
                comdb2uuidcpy(snap_info.uuid, osql->uuid);
            } else {
                // Add dummy snap_info to let master know that the replicant wants
                // query effects. (comdb2api does not send cnonce)
                snap_info.keylen = 0;
            }
            snap_info_p = &snap_info;
        }
    }

    do {
        rc = 0;

        if (gbl_osql_send_startgen && clnt->start_gen > 0) {
            osql->replicant_numops++;
            rc = osql_send_startgen(&osql->target, osql->rqid, osql->uuid,
                                    clnt->start_gen, nettype);
        }

        if (rc == 0) {
            osql->replicant_numops++;
            if (osql->rqid == OSQL_RQID_USE_UUID) {
                rc = osql_send_commit_by_uuid(
                    &osql->target, osql->uuid, osql->replicant_numops,
                    &osql->xerr, nettype, clnt->query_stats, snap_info_p);
            } else {
                rc = osql_send_commit(&osql->target, osql->rqid, osql->uuid,
                                      osql->replicant_numops, &osql->xerr,
                                      nettype, clnt->query_stats, NULL);
            }
        }
        RESTART_SOCKSQL_KEEP_RQID(is_retry);

    } while (restarted);

    osql->replicant_numops = 0; // reset for next time

    return rc;
}

static int osql_send_abort_logic(struct sqlclntstate *clnt, int nettype)
{
    osqlstate_t *osql = &clnt->osql;
    struct errstat xerr = {0};
    int rc = 0;
    uuidstr_t us;

    if (errstat_get_rc(&osql->xerr) != SQLITE_TOOBIG)
        errstat_set_rc(&xerr, SQLITE_ABORT);
    else
        errstat_set_rc(&xerr, SQLITE_TOOBIG);
    snprintf(&xerr.errstr[0], sizeof(xerr.errstr) - 1,
             "sql session %llu %s rollback", osql->rqid,
             comdb2uuidstr(osql->uuid, us));

    osql->replicant_numops++;

    if (osql->rqid == OSQL_RQID_USE_UUID)
        rc = osql_send_commit_by_uuid(&osql->target, osql->uuid,
                                      osql->replicant_numops, &xerr, nettype,
                                      clnt->query_stats, NULL);
    else
        rc = osql_send_commit(&osql->target, osql->rqid, osql->uuid,
                              osql->replicant_numops, &xerr, nettype,
                              clnt->query_stats, NULL);
    /* no need to restart an abort, master will drop the transaction anyway
    RESTART_SOCKSQL; */
    osql->replicant_numops = 0;

    return rc;
}

static int check_osql_capacity_int(struct sqlclntstate *clnt)
{
    osqlstate_t *osql = &clnt->osql;
    /* Print the first 1024 characters of the SQL query. */
    const static int maxwidth = 1024;
    int nremain;

    osql->sentops++;
    osql->tran_ops++;

    if (clnt->osql_max_trans && osql->tran_ops > clnt->osql_max_trans) {
        /* This trace is used by ALMN 1779 to alert database owners.. please do
         * not change without reference to that almn. */
        nremain = strlen(clnt->sql) - maxwidth;
        if (nremain <= 0)
            logmsg(LOGMSG_ERROR,
                   "check_osql_capacity: transaction size %d too big "
                   "(limit is %d) [\"%s\"]\n",
                   osql->tran_ops, clnt->osql_max_trans, clnt->sql ? clnt->sql : "not_set");
        else
            logmsg(LOGMSG_ERROR,
                   "check_osql_capacity: transaction size %d too big "
                   "(limit is %d) [\"%*s <%d more character(s)>\"]\n",
                   osql->tran_ops, clnt->osql_max_trans, maxwidth, clnt->sql ? clnt->sql : "not_set", nremain);

        errstat_set_rc(&osql->xerr, SQLITE_TOOBIG);
        errstat_set_str(&osql->xerr, "transaction too big\n");

        return SQLITE_TOOBIG;
    }

    return SQLITE_OK;
}

static int check_osql_capacity(struct sql_thread *thd)
{
    return check_osql_capacity_int(thd->clnt);
}

int osql_query_dbglog(struct sql_thread *thd, int queryid)
{

    struct sqlclntstate *clnt = thd->clnt;
    osqlstate_t *osql = &clnt->osql;
    int rc;
    int restarted;
    unsigned long long new_cookie;

    /* Why a new cookie?  The net send could be local (if no
       cluster, or the query somehow ends up running on
       the master) and we don't want to clobber the existing dbglog. */
    if (thd->clnt->master_dbglog_cookie == 0)
        thd->clnt->master_dbglog_cookie = get_id(thedb->bdb_env);
    new_cookie = thd->clnt->master_dbglog_cookie;
    do {
        rc = osql_send_dbglog(&osql->target, osql->rqid, osql->uuid, new_cookie,
                              queryid, NET_OSQL_SOCK_RPL);
        /* not sure if we want to restart this */
        RESTART_SOCKSQL;
    } while (restarted);
    osql->replicant_numops++;
    DEBUG_PRINT_NUMOPS();
    return rc;
}

/**
 * Record a genid with the current transaction so we can
 * verify its existence upon commit
 *
 */
int osql_record_genid(struct BtCursor *pCur, struct sql_thread *thd,
                      unsigned long long genid)
{
    struct sqlclntstate *clnt = thd->clnt;
    osqlstate_t *osql = &thd->clnt->osql;
    /* skip synthetic genids */
    if (is_genid_synthetic(genid)) {
        return 0;
    }

    if (thd->clnt->dbtran.mode == TRANLEVEL_SOSQL) {
        int rc;
        START_SOCKSQL;
        rc = osql_send_recordgenid_logic(pCur, thd, genid, NET_OSQL_SOCK_RPL);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql row rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
        osql->replicant_numops++;
        DEBUG_PRINT_NUMOPS();
    }
    return osql_save_recordgenid(pCur, thd, genid);
}

static int osql_send_recordgenid_logic(struct BtCursor *pCur,
                                       struct sql_thread *thd,
                                       unsigned long long genid, int nettype)
{
    struct sqlclntstate *clnt = thd->clnt;
    osqlstate_t *osql = &clnt->osql;
    int restarted;
    int rc = 0;

    do {
        rc = osql_send_usedb_logic(pCur, thd, nettype);
        if (rc == SQLITE_OK) {

            if (osql->rqid == OSQL_RQID_USE_UUID)
                nettype = NET_OSQL_SOCK_RPL_UUID;

            rc = osql_send_recordgenid(&osql->target, osql->rqid, osql->uuid,
                                       genid, nettype);
            if (gbl_master_swing_sock_restart_sleep) {
                usleep(gbl_master_swing_sock_restart_sleep * 1000);
            }
        }
        RESTART_SOCKSQL;
    } while (restarted);

    return rc;
}

int osql_dbq_consume(struct sqlclntstate *clnt, const char *spname,
                     genid_t genid)
{
    Q4SP(qname, spname);
    osqlstate_t *osql = &clnt->osql;
    int rc = osql_send_usedb_logic_int(qname, clnt, NET_OSQL_SOCK_RPL);
    if (rc != SQLITE_OK)
        return rc;
    return osql_send_dbq_consume(&osql->target, osql->rqid, osql->uuid, genid,
                                 NET_OSQL_SOCK_RPL);
}

int osql_dbq_consume_logic(struct sqlclntstate *clnt, const char *spname,
                           genid_t genid)
{
    int rc;
    if ((rc = osql_save_dbq_consume(clnt, spname, genid)) != 0) {
        return rc;
    }
    if (clnt->dbtran.mode == TRANLEVEL_SOSQL) {
        START_SOCKSQL;
        osqlstate_t *osql = &clnt->osql;
        rc = osql_dbq_consume(clnt, spname, genid);
        osql->replicant_numops++;
        DEBUG_PRINT_NUMOPS();
    }
    return rc;
}


/**
*
* Process a schema change request
* Returns SQLITE_OK if successful.
*
*/
int osql_schemachange_logic(struct schema_change_type *sc,
                            struct sql_thread *thd, int usedb)
{
    struct sqlclntstate *clnt = thd->clnt;
    osqlstate_t *osql = &clnt->osql;
    int restarted;
    int rc = 0;
    int count = 0;

    osql->running_ddl = 1;

    if (gbl_reject_mixed_ddl_dml && clnt->dml_tables) {
        hash_info(clnt->dml_tables, NULL, NULL, NULL, NULL, &count, NULL, NULL);
        if (count > 0) {
            return SQLITE_DDL_MISUSE;
        }
    }

    if (clnt->dml_tables &&
        hash_find_readonly(clnt->dml_tables, sc->tablename)) {
        return SQLITE_DDL_MISUSE;
    }
    if (clnt->ddl_tables) {
        if (hash_find_readonly(clnt->ddl_tables, sc->tablename)) {
            return SQLITE_DDL_MISUSE;
        } else
            hash_add(clnt->ddl_tables, strdup(sc->tablename));
    }

    if (!bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_RESUME_AUTOCOMMIT) ||
        in_client_trans(clnt)) {
        sc->rqid = osql->rqid;
        comdb2uuidcpy(sc->uuid, osql->uuid);
    }

    sc->usedbtablevers = comdb2_table_version(sc->tablename);

    if (thd->clnt->dbtran.mode == TRANLEVEL_SOSQL) {
        if (usedb && getdbidxbyname_ll(sc->tablename) < 0) {
            unsigned long long version;
            char *first_shardname =
                timepart_shard_name(sc->tablename, 0, 1, &version);
            if (first_shardname) {
                sc->usedbtablevers = version;
                free(first_shardname);
            } else /* user view */
                usedb = 0;
        }

        START_SOCKSQL;
        do {
            rc = osql_send_schemachange(&osql->target, osql->rqid,
                                        thd->clnt->osql.uuid, sc,
                                        NET_OSQL_SOCK_RPL);
            RESTART_SOCKSQL;
        } while (restarted);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql schemachange rc=%d\n",
                   __FILE__, __LINE__, __func__, rc);
            return rc;
        }
        osql->replicant_numops++;
        DEBUG_PRINT_NUMOPS();
    }

    rc = osql_save_schemachange(thd, sc, usedb);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s:%d %s - failed to cache socksql schemachange rc=%d\n",
               __FILE__, __LINE__, __func__, rc);
        return rc;
    }

    return rc;
}

/**
 *
 * Process a bpfunc request
 * Returns SQLITE_OK if successful.
 *
 */
int osql_bpfunc_logic(struct sql_thread *thd, BpfuncArg *arg)
{
    struct sqlclntstate *clnt = thd->clnt;
    osqlstate_t *osql = &clnt->osql;
    int restarted;
    int rc;

    if (thd->clnt->dbtran.mode == TRANLEVEL_SOSQL) {
        START_SOCKSQL;
        do {
            rc = osql_send_bpfunc(&osql->target, osql->rqid,
                                  thd->clnt->osql.uuid, arg, NET_OSQL_SOCK_RPL);
            RESTART_SOCKSQL;
        } while (restarted);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql bpfunc rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
        osql->replicant_numops++;
        DEBUG_PRINT_NUMOPS();
    }

    rc = osql_save_bpfunc(thd, arg);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s:%d %s - failed to cache socksql bpfunc rc=%d\n", __FILE__,
               __LINE__, __func__, rc);
        return rc;
    }
    return rc;
}

int osql_send_del_qdb_logic(struct sqlclntstate *clnt, char *tablename, genid_t id)
{
    osqlstate_t *osql = &clnt->osql;
    int rc = osql_send_usedb_logic_int(tablename, clnt, NET_OSQL_SOCK_RPL);
    if (rc) {
        return rc;
    }
    return osql_send_delrec(&osql->target, osql->rqid, osql->uuid, id, -1,
                            NET_OSQL_SOCK_RPL);
}

int osql_delrec_qdb(struct sqlclntstate *clnt, char *qname, genid_t id)
{
    osqlstate_t *osql = &clnt->osql;
    if (osql->is_reorder_on) {
        logmsg(LOGMSG_ERROR, "%s - reorder is unsupportd\n", __func__);
        return -1;
    }
    int rc = check_osql_capacity_int(clnt);
    if (rc) {
        return rc;
    }
    if (clnt->dbtran.mode == TRANLEVEL_SOSQL) {
        START_SOCKSQL;
        int restarted;
        do {
            rc = osql_send_del_qdb_logic(clnt, qname, id);
            RESTART_SOCKSQL;
        } while (restarted);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d %s - failed to send socksql delrec_qdb rc=%d\n", __FILE__,
                   __LINE__, __func__, rc);
            return rc;
        }
    }
    return osql_save_delrec_qdb(clnt, qname, id);
}
