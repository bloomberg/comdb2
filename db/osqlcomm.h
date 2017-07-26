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

#ifndef _OSQL_COMM_H_
#define _OSQL_COMM_H_

#include "comdb2.h"
#include "sbuf2.h"
#include "osqlsession.h"
#include "sqloffload.h"
#include "block_internal.h"
#include "comdb2uuid.h"
#include "schemachange.h"
#include "bpfunc.pb-c.h"

#define OSQL_SEND_ERROR_WRONGMASTER (-1234)
/**
 * Initializes this node for osql communication
 * Creates the offload net.
 * Returns 0 if success.
 *
 */
int osql_comm_init(struct dbenv *dbenv);

/**
 * Destroy osql endpoint.
 * No communication is possible after this.
 *
 */
void osql_comm_destroy(void);

/**
 * Disable temporarily replicant "node"
 * "node" will receive no more offloading requests
 * until a blackout window will expire
 * It is used mainly with blocksql
 *
 */
int osql_comm_blkout_node(char *host);

/* Offload upgrade record request. */
int offload_comm_send_upgrade_record(const char *tbl, unsigned long long genid);

/* Offload upgrade record request. */
int offload_comm_send_upgrade_records(struct dbtable *db, unsigned long long genid);

/* Offload record upgrade statistics */
void upgrade_records_stats(void);

/* Offload the internal block request.
   And wait on a fake for reply inline. */
int offload_comm_send_sync_blockreq(char *node, void *buf, int buflen);

/* Offload the block request.
   And wait for reply inline. */
int offload_comm_send_blockreq(char *host, void *rqid, void *buf, int buflen);

/* Reply to offload block request. */
int offload_comm_send_blockreply(char *host, unsigned long long rqid, void *buf,
                                 int buflen, int rc);

/* Check snap uid request */
int check_snap_uid_req(char *host, snap_uid_t *snap_info);

/* Check snap uid reply */
int check_snap_uid_reply(snap_uid_t *snap_info);

/**
 * If "rpl" is a done packet, set xerr to error if any and return 1
 * If "rpl" is a recognizable packet, returns the length of the data type is
 * recognized,
 * or -1 otherwise
 *
 */
int osql_comm_is_done(char *rpl, int rpllen, int hasuuid, struct errstat **xerr,
                      struct ireq *);

/**
 * Send a "POKE" message to "tonode" inquering about session "rqid"
 *
 */
int osql_comm_send_poke(char *tonode, unsigned long long rqid, uuid_t uuid,
                        int type);

/**
 * Send USEDB op
 * It handles remote/local connectivity
 *
 */
int osql_send_usedb(char *tohost, unsigned long long rqid, uuid_t uuid,
                    char *tablename, int type, SBUF2 *logsb);

/**
 * Send INDEX op
 * It handles remote/local connectivity
 *
 */
int osql_send_index(char *tohost, unsigned long long rqid, uuid_t uuid,
        unsigned long long genid, int isDelete, int ixnum,
        char *pData, int nData, int type, SBUF2 *logsb);

/**
 * Send QBLOB op
 * It handles remote/local connectivity
 *
 */
int osql_send_qblob(char *tohost, unsigned long long rqid, uuid_t uuid,
                    int blobid, unsigned long long seq, int type, char *data,
                    int datalen, SBUF2 *logsb);

/**
 * Send UPDCOLS op
 * It handles remote/local connectivity
 *
 */
int osql_send_updcols(char *tohost, unsigned long long rqid, uuid_t uuid,
                      unsigned long long seq, int type, int *colList, int ncols,
                      SBUF2 *logsb);

/**
 * Send UPDREC op
 * It handles remote/local connectivity
 *
 */
int osql_send_updrec(char *tonode, unsigned long long rqid, uuid_t uuid,
                     unsigned long long genid, unsigned long long ins_keys,
                     unsigned long long del_keys, char *pData, int nData,
                     int type, SBUF2 *logsb);

/**
 * Send INSREC op
 * It handles remote/local connectivity
 *
 */
int osql_send_insrec(char *tohost, unsigned long long rqid, uuid_t uuid,
                     unsigned long long genid, unsigned long long dirty_keys,
                     char *pData, int nData, int type, SBUF2 *logsb);

/**
 * Send DELREC op
 * It handles remote/local connectivity
 *
 */
int osql_send_delrec(char *tohost, unsigned long long rqid, uuid_t uuid,
                     unsigned long long genid, unsigned long long dirty_keys,
                     int type, SBUF2 *logsb);

/**
 * Send SCHEMACHANGE op
 * It handles remote/local connectivity
 *
 */
int osql_send_schemachange(char *tonode, unsigned long long rqid, uuid_t uuid,
                           struct schema_change_type *sc, int type,
                           SBUF2 *logsb);

/**
 * Send BPFUNC op
 * It handles remote/local connectivity
 *
 */
int osql_send_bpfunc(char *tonode, unsigned long long rqid, uuid_t uuid,
                     BpfuncArg *msg, int type, SBUF2 *logsb);

/**
 * Send SERIAL op
 *
 */
int osql_send_serial(char *tohost, unsigned long long rqid, uuid_t uuid,
                     CurRangeArr *arr, unsigned int file, unsigned int offset,
                     int type, SBUF2 *logsb);

/**
 * Send DONE or DONE_XERR op
 * It handles remote/local connectivity
 *
 */
int osql_send_commit(char *tohost, unsigned long long rqid, uuid_t uuid,
                     int nops, struct errstat *xerr, int type, SBUF2 *logsb,
                     struct client_query_stats *query_stats,
                     snap_uid_t *snap_info);

int osql_send_commit_by_uuid(char *tohost, uuid_t uuid, int nops,
                             struct errstat *xerr, int type, SBUF2 *logsb,
                             struct client_query_stats *query_stats,
                             snap_uid_t *snap_info);

/**
 * Send decomission for osql net
 *
 */
int osql_process_message_decom(char *host);

int osql_send_dbq_consume(char *tohost, unsigned long long rqid, uuid_t,
                          genid_t, int type, SBUF2 *);

/**
 * Constructs a reusable osql request
 *
 */
void *osql_create_request(const char *sql, int sqlen, int type,
                          unsigned long long rqid, uuid_t uuid, char *tzname,
                          int *prqlen, char *tag, void *tagbuf, int tagbuflen,
                          void *nullbits, int numnullbits, blob_buffer_t *blobs,
                          int numblobs, int queryid, int flags);

/**
 * Handles each packet and calls record.c functions
 * to apply to received row updates
 *
 */
int osql_process_packet(struct ireq *iq, unsigned long long rqid, uuid_t uuid,
                        void *trans, char *msg, int msglen, int *flags,
                        int **updCols, blob_buffer_t blobs[MAXBLOBS], int step,
                        struct block_err *err, int *receivedrows, SBUF2 *logsb);

/**
 * Sends a user command to offload net (used by "osqlnet")
 *
 */
void osql_net_cmd(char *line, int lline, int st, int op1);

/**
 * Sets the osql net-poll value.
 *
 */
void osql_set_net_poll(int pval);

/**
 * Sends a sosql request to the master
 * Sql is the first update part of this transaction
 *
 */
int osql_comm_send_socksqlreq(char *tohost, const char *sql, int sqlen,
                              unsigned long long rqid, uuid_t uuid,
                              char *tzname, int type, int flags);

/**
 * Sends the result of block processor transaction commit
 * to the sql thread so that it can return the result to the
 * client
 *
 */
int osql_comm_signal_sqlthr_rc(sorese_info_t *sorese, struct errstat *xerr,
                               int rc);

/**
 * Report on the traffic noticed
 *
 */
int osql_comm_quick_stat(void);

/**
 * Change the rqid and to allow reusing the request
 *
 */
void osql_remap_request(osql_req_t *req, unsigned long long rqid);

/**
 * Copy the big-endian errstat_t pointed to by p_buf into p_errstat_type
 * Exposed for osql_sess_set_complete
 *
 */
const uint8_t *osqlcomm_errstat_type_get(errstat_t *p_errstat_type,
                                         const uint8_t *p_buf,
                                         const uint8_t *p_buf_end);

/**
 * Copy the little-endian errstat_t pointed to by errstat_type into 
 * p_errstat_type.  Exposed for fstblk.
 *
 */
uint8_t *osqlcomm_errstat_type_put(const errstat_t *p_errstat_type,
                                          uint8_t *p_buf,
                                          const uint8_t *p_buf_end);


/**
 * Copy and pack the host-ordered client_query_stats type into big-endian
 * format.  This routine only packs up to the path_stats component:  use
 * client_query_path_commponent_put to pack each of the path_stats
 *
 */
uint8_t *client_query_stats_put(const struct client_query_stats *p_stats,
                                uint8_t *p_buf, const uint8_t *p_buf_end);

/**
 * Test the net latency by sending a stream of packets to "tonode"
 * and waiting for them to be returned in the same order back
 * Displays per packet latencies
 *
 */
int osql_comm_echo(char *tohost, int stream, unsigned long long *sent,
                   unsigned long long *replied, unsigned long long *received);

/**
 * Signal net layer that the db is exiting
 *
 */
void osql_net_exiting(void);

/**
 * Request that a remote sql engine start recording it's query stats to a
 * dbglog file.  This will later be slurped up & returned via an
 * FSQL_GRAB_DBGLOG request.
 *
 */
int osql_send_dbglog(char *tohost, unsigned long long rqid, uuid_t uuid,
                     unsigned long long dbglog_cookie, int queryid, int type);

/**
 * Copy and pack the host-ordered dbglog_header- used to write endianized
 * dbglogfiles.
 *
 */
const uint8_t *dbglog_hdr_put(const struct dbglog_hdr *p_dbglog_hdr,
                              uint8_t *p_buf, const uint8_t *p_buf_end);

/**
 * Interprets each packet and log info
 * about it
 *
 */
int osql_log_packet(struct ireq *iq, unsigned long long rqid, uuid_t uuid,
                    void *trans, char *msg, int msglen, int *flags,
                    int **updCols, blob_buffer_t blobs[MAXBLOBS], int step,
                    struct block_err *err, int *receivedrows, SBUF2 *logsb);

/* Append a tail to an osql request */
int osql_add_to_request(osql_req_t **req, int type, void *buf, int len);

/**
 * Send RECGENID
 * It handles remote/local connectivity
 *
 */
int osql_send_recordgenid(char *tohost, unsigned long long rqid, uuid_t uuid,
                          unsigned long long genid, int type, SBUF2 *logsb);

/**
 * Enable a netinfo-test for the osqlcomm netinfo_ptr
 *
 */
int osql_enable_net_test(int testnum);

/**
 * Disable the netinfo-test for the osqlcomm netinfo_ptr
 *
 */
int osql_disable_net_test(void);

/**
 * Check if we need the bdb lock to stop long term sql sessions
 *
 */
int osql_comm_check_bdb_lock(void);

int osql_send_updstat(char *tohost, unsigned long long rqid, uuid_t uuid,
                      unsigned long long seq, char *pData, int nData, int nStat,
                      int type, SBUF2 *logsb);

netinfo_type *osql_get_netinfo(void);

/**
 * Dumps diffs since last call to logger (called in statthd)
 *
 **/
int osql_comm_diffstat(struct reqlogger *statlogger, int *have_scon_header);

enum osqlpfrq_type {
    OSQLPFRQ_OLDDATA = 1, /* given a table, genid : fault the dta record */
    OSQLPFRQ_OLDKEY = 5,  /* given a table, key   : fault the ix record  */
    OSQLPFRQ_NEWKEY = 6,  /* given a table, key   : fault the ix record  */

    OSQLPFRQ_OLDDATA_OLDKEYS = 3, /* given a table, genid :
                                         1) fault the dta record.
                                         2) then form all keys, and
                                            enque OSQLPRFQ_KEY for each
                                  */
    OSQLPFRQ_NEWDATA_NEWKEYS = 2, /* given a table, record:
                                         1) fault the dta record.
                                         2) then form all keys, and
                                           enque OSQLPRFQ_KEY for each
                                  */
    OSQLPFRQ_OLDDATA_OLDKEYS_NEWKEYS =
        4, /* given a table,genid :
                  1) fault the dta record.
                  2) then form all keys from found record
                     and enque OSQLPRFQ_KEY for each
                  3) form new record based on found
                     record + input record and
                  4) form all keys from new record and
                     enque OSQLPRFQ_KEY for each
           */

    OSQLPFRQ_EXITTHD = 7,
    OSQLPFRQ_OSQLREQ = 99
};

int osql_page_prefault(char *rpl, int rplen, struct dbtable **last_db,
                       int **iq_step_ix, unsigned long long rqid, uuid_t uuid,
                       unsigned long long seq);

int osql_close_connection(char *host);
#endif
