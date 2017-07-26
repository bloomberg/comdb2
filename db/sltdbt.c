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

#include "limit_fortify.h"
#include <ctype.h>
#include <epochlib.h>
#include "comdb2.h"
#include "translistener.h"
#include "sltpck.h"
#include <poll.h>
#include <lockmacro.h>

#include "socket_interfaces.h"
#include "sqloffload.h"
#include "osqlcomm.h"
#include "osqlblockproc.h"
#include "osqlblkseq.h"
#include "logmsg.h"

static void pack_tail(struct ireq *iq);
extern int glblroute_get_buffer_capacity(int *bf);
extern int sorese_send_commitrc(struct ireq *iq, int rc);

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
    switch (opcode) {
    case OP_DBINFO:
        return "dbinfo";
    case OP_FWD_BLOCK:
        return "fwdblockop";
    case OP_BLOCK:
        return "blockop";
    case OP_LONGBLOCK:
        return "longblockop";
    case OP_FWD_LBLOCK:
        return "fwdlblockop";
    case OP_FIND:
        return "find";
    case OP_NEXT:
        return "next";
    case OP_JSTNX:
        return "jstnx";
    case OP_JSTFND:
        return "jstfnd";
    case OP_PREV:
        return "prev";
    case OP_JSTPREV:
        return "jstprev";
    case OP_NUMRRN:
        return "numrrn";
    case OP_HIGHRRN:
        return "highrrn";
    case OP_MSG_TRAP:
        return "msgtrap";
    case OP_STORED:
        return "rngext"; /* WE ONLY do rngext here */
    case OP_FIND2:
        return "find2"; /*new find*/
    case OP_NEXT2:
        return "next2"; /*new next*/
    case OP_PREV2:
        return "prev2"; /*new prev*/
    case OP_JFND2:
        return "jfnd2"; /*new just find*/
    case OP_JNXT2:
        return "jnxt2"; /*new just next*/
    case OP_JPRV2:
        return "jprv2"; /*new just prev*/
    case OP_RNGEXT2:
        return "rngext2"; /*new just prev*/
    case OP_RNGEXTP2:
        return "rngextp2"; /*new just prev*/
    case OP_FNDKLESS:
        return "findkl";
    case OP_FNDRRN:
        return "fndrrn";
    case OP_FNDRRNX:
        return "fndrrnx";
    case OP_JFNDKLESS:
        return "jfindkl";
    case OP_FORMKEY:
        return "formkey";
    case OP_FNDNXTKLESS:
        return "nextkl";
    case OP_FNDPRVKLESS:
        return "prevkl";
    case OP_JFNDNXTKLESS:
        return "jnextkl";
    case OP_JFNDPRVKLESS:
        return "jprevkl";
    case OP_RNGEXTTAG:
        return "rngexttag";
    case OP_RNGEXTTAGP:
        return "rngexttagp";
    case OP_RNGEXTTAGTZ:
        return "rngexttagtz";
    case OP_RNGEXTTAGPTZ:
        return "rngexttagptz";
    case OP_DBINFO2:
        return "dbinfo2";
    case OP_DESCRIBE:
        return "describe";
    case OP_DESCRIBEKEYS:
        return "describeky";
    case OP_GETKEYNAMES:
        return "getkeynames";
    case OP_REBUILD:
        return "rebuild";
    case OP_DEBUG:
        return "debug";
    case OP_BLOBASK:
        return "blobask";
    case OP_CLEARTABLE:
        return "cleartable";
    case OP_COUNTTABLE:
        return "counttable";
    case OP_RMTFIND:
        return "rmtfind";
    case OP_RMTFINDLASTDUP:
        return "rmtfindlastdup";
    case OP_RMTFINDNEXT:
        return "rmtfindnext";
    case OP_RMTFINDPREV:
        return "rmtfindprev";
    case OP_RMTFINDRRN:
        return "rmtfindrrn";
    case OP_SQL:
        return "sql";
    case OP_CHECK_TRANS:
        return "checktrans";
    case OP_TRAN_COMMIT:
        return "tran_commit";
    case OP_TRAN_ABORT:
        return "tran_abort";
    case OP_TRAN_FINALIZE:
        return "tran_finalize";
    case OP_PROX_CONFIG:
        return "prox_config";
    case OP_CLIENT_STATS:
        return "client_stats";
/* depricated stuff follows */

#if 0
        case OP_NEWRNGEX:       return "rngxt3";
#endif

#if 0
        case OP_OLD_RNGEXT:     return "oldrngex";
#endif

#if 0
        case OP_STAT:   return "stat";
#endif

    default:
        return "??";
    }
}

void req_stats(struct dbtable *db)
{
    int ii, jj;
    int hdr = 0;
    for (ii = 0; ii <= MAXTYPCNT; ii++) {
        int flag = 0;
        if (db->typcnt[ii]) {
            if (hdr == 0) {
                logmsg(LOGMSG_USER, "REQUEST STATS FOR DB %d '%s'\n", db->dbnum, db->dbname);
                hdr = 1;
            }
            logmsg(LOGMSG_USER, "%-20s %u\n", req2a(ii), db->typcnt[ii]);
        }
    }
    for (jj = 0; jj < BLOCK_MAXOPCODE; jj++) {
        if (db->blocktypcnt[jj]) {
            if (hdr == 0) {
                logmsg(LOGMSG_USER, "REQUEST STATS FOR DB %d '%s'\n", db->dbnum, db->dbname);
                hdr = 1;
            }
            logmsg(LOGMSG_USER, "    %-16s %u\n", breq2a(jj), db->blocktypcnt[jj]);
        }
    }
    for (jj = 0; jj < MAX_OSQL_TYPES; jj++) {
        if (db->blockosqltypcnt[jj]) {
            if (hdr == 0) {
                logmsg(LOGMSG_USER, "REQUEST STATS FOR DB %d '%s'\n", db->dbnum, db->dbname);
                hdr = 1;
            }
            logmsg(LOGMSG_USER, "    %-16s %u\n", osql_breq2a(jj), db->blockosqltypcnt[jj]);
        }
    }
}

extern pthread_mutex_t delay_lock;
int offload_net_send(int tonode, int usertype, void *data, int datalen,
                     int nodelay);

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
        if (((now = time_epoch()) - last_rej) > 0) {
            logmsg(LOGMSG_WARN, "Rejecting tagged api request (%llu)\n", rejcnt);
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
    int diffms, rc, mstr, len, retries;
    int irc;

    int penalty = 0;
    int totpen = 0;
    int penaltyinc;
    double gbl_penaltyincpercent_d;

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

    iq->rawnodestats = get_raw_node_stats(iq->frommach);
    if (iq->rawnodestats && iq->opcode >= 0 && iq->opcode < MAXTYPCNT)
        iq->rawnodestats->opcode_counts[iq->opcode]++;

    if (opcode_supported(iq->opcode)) {
        switch (iq->opcode) {
        default:
            logmsg(LOGMSG_ERROR, "bad request %d from %s\n", iq->opcode, getorigin(iq));
            /* starting write, no more reads */
            iq->p_buf_in_end = iq->p_buf_in = NULL;
            rc = ERR_BADREQ;
            break;


        case OP_FWD_BLOCK: /*forwarded block op*/
        case OP_BLOCK:
            if (gbl_readonly) {
                /* ERR_REJECTED will force a proxy retry.  This is essential to
                 * make live schema change work reliably. */
                if (gbl_schema_change_in_progress)
                    rc = ERR_REJECTED;
                else
                    rc = ERR_READONLY;
                break;
            }
            if (iq->frommach && !allow_write_from_remote(iq->frommach)) {
                rc = ERR_READONLY;
                break;
            }

            iq->where = "toblock";
            retries = 0;

            totpen = 0;

            gbl_penaltyincpercent_d = (double)gbl_penaltyincpercent * .01;

        retry:
            rc = toblock(iq);

            extern int gbl_test_blkseq_replay_code;
            if (gbl_test_blkseq_replay_code &&
                (rc != RC_INTERNAL_RETRY && rc != ERR_NOT_DURABLE) &&
                (rand() % 10) == 0) {
                logmsg(LOGMSG_USER, "Test blkseq replay: returning "
                                    "ERR_NOT_DURABLE to test replay:\n");
                logmsg(LOGMSG_USER, "rc=%d, errval=%d errstr='%s' rcout=%d\n",
                       rc, iq->errstat.errval, iq->errstat.errstr,
                       iq->sorese.rcout);
                rc = ERR_NOT_DURABLE;
            }

            if (rc == RC_INTERNAL_RETRY) {
                iq->retries++;
                if (++retries < gbl_maxretries) {
                    if (!bdb_attr_get(
                            thedb->bdb_attr,
                            BDB_ATTR_DISABLE_WRITER_PENALTY_DEADLOCK)) {
                        irc = pthread_mutex_lock(&delay_lock);
                        if (irc != 0) {
                            logmsg(LOGMSG_FATAL, 
                                    "pthread_mutex_lock(&delay_lock) %d\n",
                                    irc);
                            exit(1);
                        }

                        penaltyinc =
                            (double)(gbl_maxwthreads - gbl_maxwthreadpenalty) *
                            (gbl_penaltyincpercent_d / iq->retries);

                        if (penaltyinc <= 0) {
                            /* at least one less writer */
                            penaltyinc = 1;
                        }

                        if (penaltyinc + gbl_maxwthreadpenalty > gbl_maxthreads)
                            penaltyinc = gbl_maxthreads - gbl_maxwthreadpenalty;

                        gbl_maxwthreadpenalty += penaltyinc;
                        totpen += penaltyinc;

                        irc = pthread_mutex_unlock(&delay_lock);
                        if (irc != 0) {
                            logmsg(LOGMSG_FATAL, 
                                    "pthread_mutex_unlock(&delay_lock) %d\n",
                                    irc);
                            exit(1);
                        }
                    }

                    n_retries++;
                    poll(0, 0, (rand() % 25 + 1));
                    goto retry;
                }

                logmsg(LOGMSG_WARN, "*ERROR* toblock too much contention count %d\n",
                       retries);
                thd_dump();
            }

            /* we need this in rare case when the request is retried
               500 times; this is happening due to other bugs usually
               this ensures no requests replays will be left stuck
               papers around other short returns in toblock jic
               */
            osql_blkseq_unregister(iq);

            irc = pthread_mutex_lock(&delay_lock);
            if (irc != 0) {
                logmsg(LOGMSG_FATAL, "pthread_mutex_lock(&delay_lock) %d\n", irc);
                exit(1);
            }

            gbl_maxwthreadpenalty -= totpen;

            irc = pthread_mutex_unlock(&delay_lock);
            if (irc != 0) {
                logmsg(LOGMSG_FATAL, "pthread_mutex_unlock(&delay_lock) %d\n", irc);
                exit(1);
            }

            /* return codes we think the proxy understands.  all other cases
               return proxy retry */
            if (rc != 0 && rc != ERR_BLOCK_FAILED && rc != ERR_READONLY &&
                rc != ERR_SQL_PREP && rc != ERR_NO_AUXDB &&
                rc != ERR_INCOHERENT && rc != ERR_SC_COMMIT &&
                rc != ERR_CONSTR && rc != ERR_TRAN_FAILED &&
                rc != ERR_CONVERT_DTA && rc != ERR_NULL_CONSTRAINT &&
                rc != ERR_CONVERT_IX && rc != ERR_BADREQ &&
                rc != ERR_RMTDB_NESTED && rc != ERR_NESTED &&
                rc != ERR_NOMASTER && rc != ERR_READONLY && rc != ERR_VERIFY &&
                rc != RC_TRAN_CLIENT_RETRY && rc != RC_INTERNAL_FORWARD &&
                rc != RC_INTERNAL_RETRY &&
                rc != ERR_TRAN_TOO_BIG && /* THIS IS SENT BY BLOCKSQL WHEN TOOBIG */
                rc != 999 && rc != ERR_ACCESS && rc != ERR_UNCOMMITABLE_TXN &&
                (rc != ERR_NOT_DURABLE || !iq->sorese.type)) {
                /* XXX CLIENT_RETRY DOESNT ACTUALLY CAUSE A RETRY USUALLY, just
                   a bad rc to the client! */
                /*rc = RC_TRAN_CLIENT_RETRY;*/

                rc = ERR_NOMASTER;
            }

            iq->where = "toblock complete";
            break;
        }

        /* Record the dbname (aka table) for this op */
        if (iq->usedb && iq->usedb->dbname)
            reqlog_logl(iq->reqlogger, REQL_INFO, iq->usedb->dbname);
    } else {
        rc = ERR_BADREQ;
        iq->where = "opcode not supported";
    }

    if (rc == RC_INTERNAL_FORWARD) {
        rc = 0;
    } else {
        /*SNDBAK RESPONSE*/
        if (iq->debug) {
            reqprintf(iq, "iq->reply_len=%d RC %d\n",
                      iq->p_buf_out - iq->p_buf_out_start, rc);
        }

        /* pack data at tail of reply */
        pack_tail(iq);
        if (iq->sorese.type) {
            /* we don't have a socket or a buffer for that matter,
             * instead, we need to send back the result of transaction from rc
             */

            /*
               hack alert
               override the extended code (which we don't care about, with
               the primary error code
               */
            if (rc && (!iq->sorese.rcout || rc == ERR_NOT_DURABLE))
                iq->sorese.rcout = rc;

            int sorese_rc = rc;
            if (rc == 0 && iq->sorese.rcout == 0 &&
                iq->errstat.errval == COMDB2_SCHEMACHANGE_OK) {
                // pretend error happend to get errstat shipped to replicant
                sorese_rc = 1;
            } else {
                iq->errstat.errval = iq->sorese.rcout;
            }

            if (iq->debug) {
                uuidstr_t us;
                reqprintf(iq, "sorese returning rqid=%llu %s node=%s type=%d "
                              "nops=%d rcout=%d retried=%d RC=%d errval=%d\n",
                          iq->sorese.rqid, comdb2uuidstr(iq->sorese.uuid, us),
                          iq->sorese.host, iq->sorese.type, iq->sorese.nops,
                          iq->sorese.rcout, iq->sorese.osql_retry, rc,
                          iq->errstat.errval);
            }

            if (iq->sorese.rqid == 0)
                abort();
            osql_comm_signal_sqlthr_rc(&iq->sorese, &iq->errstat, sorese_rc);

            iq->timings.req_sentrc = osql_log_time();

#if 0
            /*
            I don't wanna do this here, reloq_end_request() needs sql 
            details that are in the buffer; I am not gonna remalloc and copy
            just to preserve code symmetry */
            /* free the buffer, that was created by sorese_rcvreq() */
            if(iq->p_buf_out_start) 
            {
                free(iq->p_buf_out_start);
                iq->p_buf_out_end = iq->p_buf_out_start = iq->p_buf_out = NULL;
                iq->p_buf_in_end = iq->p_buf_in = NULL;
            }
#endif
        } else if (iq->is_dumpresponse) {
            signal_buflock(iq->request_data);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "\n Unexpected error %d in block operation",
                        rc);
            }
        } else if (iq->is_fromsocket) {
            net_delay(iq->frommach);
            /* process socket end request */
            if (iq->is_socketrequest) {
                if (iq->sb == NULL) {
                    rc = offload_comm_send_blockreply(
                        iq->frommach, iq->rqid, iq->p_buf_out_start,
                        iq->p_buf_out - iq->p_buf_out_start, rc);
                } else {
                    sndbak_open_socket(iq->sb, iq->p_buf_out_start,
                                       iq->p_buf_out - iq->p_buf_out_start, rc);
                    free_bigbuf(iq->p_buf_out_start, iq->request_data);
                }
                iq->request_data = iq->p_buf_out_start = NULL;
            } else {
                sndbak_socket(iq->sb, iq->p_buf_out_start,
                              iq->p_buf_out - iq->p_buf_out_start, rc);
                free(iq->p_buf_out_start);
            }
            iq->p_buf_out_end = iq->p_buf_out_start = iq->p_buf_out = NULL;
            iq->p_buf_in_end = iq->p_buf_in = NULL;
        }
    }

    /* Unblock anybody waiting for stuff that was added in this transaction. */
    clear_trans_from_repl_list(iq->repl_list);

    /* records were added to queues, and we committed successfully.  wake
     * up queue consumers. */
    if (rc == 0 && iq->num_queues_hit > 0) {
        if (iq->num_queues_hit > MAX_QUEUE_HITS_PER_TRANS) {
            /* good heavens.  wake up all consumers */
            dbqueue_wake_all_consumers_all_queues(iq->dbenv, 0);
        } else {
            unsigned ii;
            for (ii = 0; ii < iq->num_queues_hit; ii++)
                dbqueue_wake_all_consumers(iq->queues_hit[ii], 0);
        }
    }

    /* Finish off logging. */
    if (iq->blocksql_tran) {
        osql_bplog_reqlog_queries(iq);
    }
    reqlog_end_request(iq->reqlogger, rc, __func__, __LINE__);

    if (iq->sorese.type) {
        if (iq->p_buf_out_start) {
            free(iq->p_buf_out_start);
            iq->p_buf_out_end = iq->p_buf_out_start = iq->p_buf_out = NULL;
            iq->p_buf_in_end = iq->p_buf_in = NULL;
        }
    }

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
