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

/* comdb index front end */

#include <unistd.h> /* For usleep() */
#include <ctype.h>
#include <epochlib.h>
#include "comdb2.h"
#include "translistener.h"
#include "sltpck.h"
#include <poll.h>

#include "socket_interfaces.h"
#include "sqloffload.h"
#include "osqlcomm.h"
#include "osqlblockproc.h"
#include "osqlblkseq.h"
#include "logmsg.h"
#include "reqlog.h"
#include "plhash.h"
#include "comdb2_plugin.h"
#include "comdb2_opcode.h"
#include "sc_util.h"

static void pack_tail(struct ireq *iq);
extern int glblroute_get_buffer_capacity(int *bf);
extern int sorese_send_commitrc(struct ireq *iq, int rc);

void (*comdb2_ipc_sndbak_len_sinfo)(struct ireq *, int) = 0;

/* HASH of all registered opcode handlers (one handler per opcode) */
hash_t *gbl_opcode_hash;

/* this is dumb, but it doesn't need to be clever for now */
int a2req(const char *s)
{
    if (isdigit(s[0]))
        return atoi(s);
    else {
        int ii;
        for (ii = 0; ii < MAXTYPCNT; ii++)
            if (strcasecmp(s, req2a(ii)) == 0)
                return ii;
    }
    return -1;
}

const char *req2a(int opcode)
{
    comdb2_opcode_t *op = hash_find_readonly(gbl_opcode_hash, &opcode);
    if (op)
        return op->name;
    else
        return "????";
}

void req_stats(struct dbtable *db)
{
    int ii, jj;
    int hdr = 0;
    for (ii = 0; ii <= MAXTYPCNT; ii++) {
        if (db->typcnt[ii]) {
            if (hdr == 0) {
                logmsg(LOGMSG_USER, "REQUEST STATS FOR DB %d '%s'\n", db->dbnum, db->tablename);
                hdr = 1;
            }
            logmsg(LOGMSG_USER, "%-20s %u\n", req2a(ii), db->typcnt[ii]);
        }
    }
    for (jj = 0; jj < BLOCK_MAXOPCODE; jj++) {
        if (db->blocktypcnt[jj]) {
            if (hdr == 0) {
                logmsg(LOGMSG_USER, "REQUEST STATS FOR DB %d '%s'\n", db->dbnum, db->tablename);
                hdr = 1;
            }
            logmsg(LOGMSG_USER, "    %-20s %u\n", breq2a(jj), db->blocktypcnt[jj]);
        }
    }
    for (jj = 0; jj < MAX_OSQL_TYPES; jj++) {
        if (db->blockosqltypcnt[jj]) {
            if (hdr == 0) {
                logmsg(LOGMSG_USER, "REQUEST STATS FOR DB %d '%s'\n", db->dbnum, db->tablename);
                hdr = 1;
            }
            logmsg(LOGMSG_USER, "    %-20s %u\n", osql_reqtype_str(jj), db->blockosqltypcnt[jj]);
        }
    }
}

extern pthread_mutex_t delay_lock;
extern __thread snap_uid_t *osql_snap_info; /* contains cnonce */
extern int gbl_print_deadlock_cycles;

static void adjust_maxwthreadpenalty(int *totpen_p,
                                     double lcl_penaltyincpercent_d,
                                     int retries)
{
    Pthread_mutex_lock(&delay_lock);

    int penaltyinc;
    penaltyinc = (double)(gbl_maxwthreads - gbl_maxwthreadpenalty) *
                 (lcl_penaltyincpercent_d / retries);

    if (penaltyinc <= 0) {
        /* at least one less writer */
        penaltyinc = 1;
    }

    if (penaltyinc + gbl_maxwthreadpenalty > gbl_maxwthreads)
        penaltyinc = gbl_maxwthreads - gbl_maxwthreadpenalty;

    gbl_maxwthreadpenalty += penaltyinc;
    *totpen_p += penaltyinc;

    Pthread_mutex_unlock(&delay_lock);
}

static int handle_op_local(struct ireq *iq, int (*init)(struct ireq *),
                           int (*run)(struct ireq *))
{
    int rc;
    int64_t startus, stopus;
    int deadlocksleepus;

    static int avg_toblock_us;

    if (gbl_readonly || gbl_readonly_sc) {
        /* ERR_REJECTED will force a proxy retry. This is essential to make live
         * schema change work reliably. */
        if (get_schema_change_in_progress(__func__, __LINE__))
            rc = ERR_REJECTED;
        else
            rc = ERR_READONLY;
        return rc;
    }
    if (iq->frommach && !allow_write_from_remote(iq->frommach)) {
        return ERR_READONLY;
    }

    iq->where = "toblock";
    int retries = 0;
    int totpen = 0;
    double lcl_penaltyincpercent_d = (double)gbl_penaltyincpercent * .01;

    if (init) {
        rc = init(iq);
        if (rc)
            goto done;
    }

retry:
    startus = comdb2_time_epochus();
    rc = run(iq);
    stopus = comdb2_time_epochus();

    extern int gbl_test_blkseq_replay_code;
    if (gbl_test_blkseq_replay_code &&
        (rc != RC_INTERNAL_RETRY && rc != ERR_NOT_DURABLE) &&
        (rand() % 10) == 0) {
        logmsg(LOGMSG_USER, "Test blkseq replay: returning "
                            "ERR_NOT_DURABLE to test replay:\n");
        logmsg(LOGMSG_USER, "rc=%d, errval=%d errstr='%s' rcout=%d\n", rc,
               iq->errstat.errval, iq->errstat.errstr,
               iq->sorese ? iq->sorese->rcout : 0);
        rc = ERR_NOT_DURABLE;
    }

    if (rc == 0) {
        /* Calculate a moving average runtime of toblock calls, and cap it
           at 25 milliseconds. The value is used to determine how much time we
           sleep before retrying on a deadlock. It does not have to be 100%
           accurate so we do not hold a lock here. We could make the number of
           calls and the cap tunables but for now leave them hardcoded. */
        avg_toblock_us += ((stopus - startus - avg_toblock_us) >> 4);
        if (avg_toblock_us > 25000)
            avg_toblock_us = 25000;
    } else if (rc == RC_INTERNAL_RETRY) {
        iq->retries++;
        if (++retries < gbl_maxretries) {
            if (!bdb_attr_get(thedb->bdb_attr,
                              BDB_ATTR_DISABLE_WRITER_PENALTY_DEADLOCK)) {
                adjust_maxwthreadpenalty(&totpen, lcl_penaltyincpercent_d,
                                         iq->retries);
            }
            iq->usedb = iq->origdb;

            n_retries++;
            deadlocksleepus = (rand() % gbl_maxwthreads * avg_toblock_us);
            /* usleep(0) will likely give up the CPU. Avoid it. */
            if (deadlocksleepus != 0)
                usleep(deadlocksleepus);
            goto retry;
        }

        logmsg(LOGMSG_WARN, "toblock too much contention count=%d\n", retries);
        thd_dump();
    }

done:
    /* we need this in rare case when the request is retried
       500 times; this is happening due to other bugs usually
       this ensures no requests replays will be left stuck
       papers around other short returns in toblock jic
       */
    if (rc)
        osql_blkseq_unregister(iq);

    if (totpen) {
        Pthread_mutex_lock(&delay_lock);
        gbl_maxwthreadpenalty -= totpen;
        Pthread_mutex_unlock(&delay_lock);
    }

    /* return codes we think the proxy understands.  all other cases
       return proxy retry */
    if (rc != 0 && rc != ERR_BLOCK_FAILED && rc != ERR_READONLY && rc != ERR_SQL_PREP && rc != ERR_NO_AUXDB &&
        rc != ERR_INCOHERENT && rc != ERR_SC_COMMIT && rc != ERR_CONSTR && rc != ERR_TRAN_FAILED &&
        rc != ERR_CONVERT_DTA && rc != ERR_NULL_CONSTRAINT && rc != ERR_CONVERT_IX && rc != ERR_BADREQ &&
        rc != ERR_RMTDB_NESTED && rc != ERR_NESTED && rc != ERR_NOMASTER && rc != ERR_READONLY && rc != ERR_VERIFY &&
        rc != RC_TRAN_CLIENT_RETRY && rc != RC_INTERNAL_FORWARD && rc != RC_INTERNAL_RETRY &&
        rc != ERR_TRAN_TOO_BIG && /* THIS IS SENT BY BLOCKSQL WHEN TOOBIG */
        rc != 999 && rc != ERR_ACCESS && rc != ERR_UNCOMMITABLE_TXN && rc != ERR_DIST_ABORT &&
        (rc != ERR_NOT_DURABLE || !iq->sorese)) {
        /* XXX CLIENT_RETRY DOESNT ACTUALLY CAUSE A RETRY USUALLY, just
           a bad rc to the client! */
        /*rc = RC_TRAN_CLIENT_RETRY;*/

        rc = ERR_NOMASTER;
    }

    iq->where = "toblock complete";
    return rc;
}

static int handle_op_block(struct ireq *iq)
{
    return handle_op_local(iq, NULL, toblock);
}

int handle_op_sorese(struct ireq *iq)
{
    return handle_op_local(iq, to_sorese_init, to_sorese);
}

/* Builtin opcode handlers */
static comdb2_opcode_t block_op_handler = {OP_BLOCK, "blockop",
                                           handle_op_block};
static comdb2_opcode_t fwd_block_op_handler = {OP_FWD_BLOCK, "fwdblockop",
                                               handle_op_block};
static comdb2_opcode_t sorese_op_handler = {OP_SORESE, "sorese",
                                            handle_op_sorese};
static comdb2_opcode_t fwd_block_le_op_handler = {OP_FWD_BLOCK_LE, "fwdblockople",
                                               handle_op_block};

int init_opcode_handlers()
{
    /* Initialize the opcode handler hash. */
    gbl_opcode_hash = hash_init_i4(offsetof(comdb2_opcode_t, opcode));
    logmsg(LOGMSG_DEBUG, "opcode handler hash initialized\n");

    /* Also register the builtin opcode handlers. */
    hash_add(gbl_opcode_hash, &block_op_handler);
    hash_add(gbl_opcode_hash, &fwd_block_op_handler);
    hash_add(gbl_opcode_hash, &sorese_op_handler);
    hash_add(gbl_opcode_hash, &fwd_block_le_op_handler);

    return 0;
}

static inline int opcode_supported(int opcode)
{
    static int last_rej = 0;
    static unsigned long long rejcnt = 0;
    int now;

    if (!gbl_rowlocks && !gbl_disable_tagged_api)
        return 1;

    switch (opcode) {
    case OP_FNDRRN:
    case OP_FORMKEY:
    case OP_FNDRRNX:
    case OP_JSTFND:
    case OP_FIND:
    case OP_JSTPREV:
    case OP_JSTNX:
    case OP_PREV:
    case OP_NEXT:
    case OP_FNDKLESS:
    case OP_JFNDKLESS:
    case OP_FIND2:
    case OP_JFND2:
    case OP_FNDNXTKLESS:
    case OP_FNDPRVKLESS:
    case OP_JFNDNXTKLESS:
    case OP_JFNDPRVKLESS:
    case OP_NEXT2:
    case OP_PREV2:
    case OP_JNXT2:
    case OP_JPRV2:
    case OP_BLOBASK:
    case OP_RNGEXT2:
    case OP_RNGEXTP2:
    case OP_RNGEXTTAG:
    case OP_RNGEXTTAGP:
    case OP_RNGEXTTAGTZ:
    case OP_RNGEXTTAGPTZ:
    case OP_NUMRRN:
    case OP_HIGHRRN:
    case OP_STORED:
    case OP_COUNTTABLE:
    case OP_RMTFIND:
    case OP_RMTFINDLASTDUP:
    case OP_RMTFINDNEXT:
    case OP_RMTFINDPREV:
    case OP_RMTFINDRRN:
        rejcnt++;
        if (((now = comdb2_time_epoch()) - last_rej) > 0) {
            logmsg(LOGMSG_ERROR, "Rejecting tagged api request (%llu)\n",
                   rejcnt);
            last_rej = now;
        }
        return 0;
        break;

    case OP_DBINFO:
    case OP_DBINFO2:
    case OP_FWD_LBLOCK:
    case OP_LONGBLOCK:
    case OP_FWD_BLOCK:
    case OP_BLOCK:
    case OP_MSG_TRAP:
    case OP_DESCRIBE:
    case OP_DESCRIBEKEYS:
    case OP_GETKEYNAMES:
    case OP_CLEARTABLE:
    case OP_FASTINIT:
    case OP_MAKE_NODE_INCOHERENT:
    case OP_CLIENT_STATS:
    default:
        return 1;
        break;
    }
}

int handle_ireq(struct ireq *iq)
{
    int rc;

    bdb_reset_thread_stats();

    if (iq->opcode >= 0 && iq->opcode <= MAXTYPCNT) {
        /*this should be under lock, but its just a counter?*/
        iq->usedb->typcnt[iq->opcode]++;
    }

    /* clear errstr */
    reqerrstrclr(iq);

    /* new request and record the basic opcode */
    reqlog_new_request(iq);
    reqlog_logl(iq->reqlogger, REQL_INFO, req2a(iq->opcode));
    reqlog_pushprefixf(iq->reqlogger, "%s:REQ %s ", getorigin(iq),
                       req2a(iq->opcode));

    iq->rawnodestats =
        get_raw_node_stats(NULL, NULL, iq->frommach, sbuf2fileno(iq->sb), 0 /* tag does not support ssl */);
    if (iq->rawnodestats && iq->opcode >= 0 && iq->opcode < MAXTYPCNT)
        iq->rawnodestats->opcode_counts[iq->opcode]++;
    if (gbl_print_deadlock_cycles && IQ_HAS_SNAPINFO(iq))
        osql_snap_info = IQ_SNAPINFO(iq);

    comdb2_opcode_t *opcode = hash_find_readonly(gbl_opcode_hash, &iq->opcode);
    if (!opcode || !opcode_supported(iq->opcode)) {
        logmsg(LOGMSG_ERROR, "bad request %d from %s\n", iq->opcode,
               getorigin(iq));
        /* Starting write, no more reads */
        iq->p_buf_in_end = iq->p_buf_in = NULL;
        rc = ERR_BADREQ;
    } else {
        if (gbl_rowlocks && (opcode->opcode != OP_BLOCK) &&
            (opcode->opcode != OP_FWD_BLOCK) &&
            (opcode->opcode != OP_DBINFO2) &&
            (opcode->opcode != OP_DBINFO)) {
            rc = ERR_BADREQ;
            iq->where = "opcode execution skipped";
        } else {
            rc = opcode->opcode_handler(iq);

            /* Record the tablename (aka table) for this op */
            if (iq->usedb && iq->usedb->tablename) {
                reqlog_logl(iq->reqlogger, REQL_INFO, iq->usedb->tablename);
            }
        }
    }

    if (rc == RC_INTERNAL_FORWARD) {
        rc = 0;
    } else {
        /* SNDBAK RESPONSE */
        if (iq->debug) {
            reqprintf(iq, "iq->reply_len=%td RC %d\n",
                      (ptrdiff_t)(iq->p_buf_out - iq->p_buf_out_start), rc);
        }

        /* pack data at tail of reply */
        pack_tail(iq);

        if (iq->sorese) {
            /* we don't have a socket or a buffer for that matter,
             * instead, we need to send back the result of transaction from rc
             */

            /*
               hack alert
               override the extended code (which we don't care about, with
               the primary error code
               */
            if (rc && (!iq->sorese->rcout || rc == ERR_NOT_DURABLE))
                iq->sorese->rcout = rc;

            int sorese_rc = rc;
            if (rc == 0 && iq->sorese->rcout == 0 &&
                iq->errstat.errval == COMDB2_SCHEMACHANGE_OK) {
                // pretend error happend to get errstat shipped to replicant
                sorese_rc = 1;
            } else {
                iq->errstat.errval = iq->sorese->rcout;
            }

            if (iq->debug) {
                uuidstr_t us;
                reqprintf(iq,
                          "sorese returning rqid=%llu uuid=%s node=%s type=%d "
                          "nops=%d rcout=%d retried=%d RC=%d errval=%d\n",
                          iq->sorese->rqid, comdb2uuidstr(iq->sorese->uuid, us),
                          iq->sorese->target.host, iq->sorese->type,
                          iq->sorese->nops, iq->sorese->rcout,
                          iq->sorese->verify_retries, rc, iq->errstat.errval);
            }

            if (iq->sorese->rqid == 0)
                abort();
            osql_comm_signal_sqlthr_rc(
                &iq->sorese->target, iq->sorese->rqid, iq->sorese->uuid,
                iq->sorese->nops, &iq->errstat, IQ_SNAPINFO(iq), sorese_rc);

            iq->timings.req_sentrc = osql_log_time();

        } else if (iq->is_dumpresponse) {
            signal_buflock(iq->request_data);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR,
                       "\n Unexpected error %d in block operation", rc);
            }
        } else if (iq->is_fromsocket) {
            net_delay(iq->frommach);
            /* process socket end request */
            if (iq->is_socketrequest) {
                if (iq->sb == NULL) {
                    rc = offload_comm_send_blockreply(
                        iq->frommach, iq->fwd_tag_rqid, iq->p_buf_out_start,
                        iq->p_buf_out - iq->p_buf_out_start, rc);
                    free_bigbuf_nosignal(iq->p_buf_out_start);
                } else {
                    /* The tag request is handled locally.
                       We know for sure `request_data' is a `buf_lock_t'. */
                    struct buf_lock_t *p_slock =
                        (struct buf_lock_t *)iq->request_data;
                    {
                        Pthread_mutex_lock(&p_slock->req_lock);
                        if (p_slock->reply_state == REPLY_STATE_DISCARD) {
                            Pthread_mutex_unlock(&p_slock->req_lock);
                            cleanup_lock_buffer(p_slock);
                            free_bigbuf_nosignal(iq->p_buf_out_start);
                        } else {
                            sndbak_open_socket(
                                iq->sb, iq->p_buf_out_start,
                                iq->p_buf_out - iq->p_buf_out_start, rc);
                            free_bigbuf(iq->p_buf_out_start, iq->request_data);
                            Pthread_mutex_unlock(&p_slock->req_lock);
                        }
                    }
                }
                iq->request_data = iq->p_buf_out_start = NULL;
            } else {
                sndbak_socket(iq->sb, iq->p_buf_out_start,
                              iq->p_buf_out - iq->p_buf_out_start, rc);
                free(iq->p_buf_out_start);
            }
            iq->p_buf_out_end = iq->p_buf_out_start = iq->p_buf_out = NULL;
            iq->p_buf_in_end = iq->p_buf_in = NULL;
        } else if (comdb2_ipc_sndbak_len_sinfo) {
            comdb2_ipc_sndbak_len_sinfo(iq, rc);
        }
    }

    /* Unblock anybody waiting for stuff that was added in this transaction. */
    clear_trans_from_repl_list(iq->repl_list);

    /* records were added to queues, and we committed successfully.  wake
     * up queue consumers. */
    if (rc == 0 && iq->num_queues_hit > 0) {
        if (iq->num_queues_hit > MAX_QUEUE_HITS_PER_TRANS) {
            /* good heavens.  wake up all consumers */
            dbqueuedb_wake_all_consumers_all_queues(iq->dbenv, 0);
        } else {
            unsigned ii;
            for (ii = 0; ii < iq->num_queues_hit; ii++)
                dbqueuedb_wake_all_consumers(iq->queues_hit[ii], 0);
        }
    }

    if (iq->sorese) {
        /* Finish off logging. */
        osql_sess_reqlogquery(iq->sorese, iq->reqlogger);
        /* Free the sorese transaction buffer */
        free(iq->p_buf_out_start);
    }
    reqlog_end_request(iq->reqlogger, rc, __func__, __LINE__);
    release_node_stats(NULL, NULL, iq->frommach);
    if (gbl_print_deadlock_cycles)
        osql_snap_info = NULL;

    /* Make sure we do not leak locks */

    bdb_checklock(thedb->bdb_env);

    return rc;
}

static void pack_tail(struct ireq *iq)
{
    struct slt_cur_t *cur = NULL;

    iq->p_buf_in_end = iq->p_buf_in = NULL; /* starting write, no more reads */

    /* grab a cursor */
    cur =
        slt_init(iq->p_buf_out, iq->p_buf_out_end - iq->p_buf_out + RESERVED_SZ,
                 SLTPCK_BACKWARD);

    /* pack in order of enum (may want to prioritize if size in
     * buffer was not sufficient ...) */
    if (iq->errstrused && iq->dbenv->errstaton) {
        (void)slt_pck(cur, TAIL_ERRSTAT, &iq->errstat, sizeof(iq->errstat));
    }

    /* pack other data here */

    /* stamp data */
    iq->p_buf_out += slt_stamp(cur);
}
