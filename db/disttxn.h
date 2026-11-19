/*
   Copyright 2023 Bloomberg Finance L.P.

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

#ifndef _DISTTXN_H_
#define _DISTTXN_H_
#include <comdb2.h>
#include <net_int.h>

#define DISTRIBUTED_TRANSACTIONS_TABLE "comdb2_distributed_transactions"

extern int gbl_2pc_ddl;
extern int gbl_disttxn_ddl_timeout_ms;
extern char *gbl_debug_remote_2pc_fault;
extern int gbl_coordinator_block_until_durable;
extern int gbl_debug_sleep_ddl_before_lock_ms;
extern int gbl_debug_exit_participant_before_ddl_prepare;
extern int gbl_debug_exit_participant_before_voted;
extern int gbl_debug_fail_2pc_redo_finalize;

/* gbl_debug_fail_ddl_prepare: the debug_remote_2pc_fault failure mode */
enum ddl_2pc_fault {
    DDL_2PC_FAULT_NONE = 0,
    DDL_2PC_FAULT_FAIL_PREPARE = 1,
    DDL_2PC_FAULT_FAIL_COMMIT = 2,
    DDL_2PC_FAULT_FAIL_COMMIT_WAIT = 3,
    DDL_2PC_FAULT_FAIL_COORDINATOR_COMMIT = 4,
    DDL_2PC_FAULT_FAIL_FINALIZE = 5
};
extern int gbl_debug_fail_ddl_prepare;
int debug_remote_2pc_fault_update(void *context, void *value);

/* coordinator_wait / participant_wait rcodes */
enum {
    KEEP_RCODE = 0,
    HAS_COMMITTED = 1,
    HAS_ABORTED = 2,
    IS_NOT_DURABLE = 3,
    NO_DIST_TABLE = 4,
    LOCK_DESIRED = 5,
    ALL_PREPARED = 6,
    TIMED_OUT = 7
};

/* This coordinator records participant information; is_ddl for a schema change */
int collect_participants(const char *dist_txnid, participant_list_t *participants, int is_ddl);

/* This coordinator asks participants to prepare */
int dispatch_participants(const char *dist_txnid);

/* This coordinator records that a participant has prepared successfully */
int participant_prepared(const char *dist_txnid, const char *dbname, const char *tier, const char *master);

/* This coordinator processes a participant's 'i-have-propagated' message */
int participant_propagated(const char *dist_txnid, const char *dbname, const char *tier);

/* This coordinator processes a participant's 'failed-to-prepare' message */
int participant_failed(const char *dist_txnid, const char *dbname, const char *tier, int rcode, int outrc,
                       const char *errmsg);

/* This coordinator will block until participants have responded */
int coordinator_wait(const char *dist_txnid, int can_retry, int *rcode, int *outrc, char *errmsg, int errmsglen,
                     int force_failure);

/* ddl coordinator waits for votes; ALL_PREPARED if it can commit */
int coordinator_wait_ddl(const char *dist_txnid, int *rcode, int *outrc, char *errmsg, int errmsglen);

/* This schema change coordinator adds its commit decision to trans */
int coordinator_ddl_write_decision(tran_type *trans, const char *dist_txnid);

/* tell participants the ddl coordinator committed (0), aborted (-1) or doesn't know (LOCK_DESIRED) */
int coordinator_ddl_committed(const char *dist_txnid, int rc);

struct sc_list_2pc;
void disttxn_ddl_resume_aborted(struct sc_list_2pc *dist);

/* This coordinator will block until participants have propogated this txn */
int coordinator_wait_propagate(const char *dist_txnid);

/* This coordinator will not wait for participant commits to propagate */
void coordinator_resolve(const char *dist_txnid);

/* This coordinator was unable to prepare */
void coordinator_failed(const char *dist_txnid);

/* This participant registers a dist-txn from osql stream */
int osql_register_disttxn(const char *dist_txnid, unsigned long long rqid, uuid_t uuid, char **coordinator_dbname,
                          char **coordinator_tier, char **coordinator_master);

/* This participant has received the coordinator's sanction for this osql transaction */
int osql_sanction_disttxn(const char *dist_txnid, unsigned long long *rqid, uuid_t *uuid,
                          const char *coordinator_dbname, const char *coordinator_tier, const char *coordinator_master);

/* This participant has been asked to cancel this osql transaction */
int osql_cancel_disttxn(const char *dist_txnid, unsigned long long *rqid, uuid_t *uuid);

/* This participant sends the coordinator a 'failed-prepare' message */
int participant_has_failed(const char *dist_txnid, const char *dbname, const char *master, int rcode, int outrc,
                           const char *errmsg);

/* This participant sends the coordinator an 'i-have-propagated' message */
void participant_has_propagated(const char *dist_txnid, const char *dbname, const char *master);

/* This coordinator has received a heartbeat message from a participant */
int participant_heartbeat(const char *dist_txnid, const char *participant_name, const char *participant_tier);

/* This participant tells coordinator it has prepared and waits for response */
int participant_wait(const char *dist_txnid, const char *coordinator_name, const char *coordinator_tier,
                     const char *coordinator_master);

/* ddl participant waits up to 2 * disttxn_ddl_timeout_ms or lock-desired */
int participant_wait_ddl(const char *dist_txnid, const char *coordinator_name, const char *coordinator_tier,
                         const char *coordinator_master);

/* resumed participant asks the coordinator: HAS_COMMITTED, HAS_ABORTED or LOCK_DESIRED */
int participant_wait_resumed_ddl(const char *dist_txnid, const char *coordinator_name, const char *coordinator_tier);

/* resumed participant withdraws a vote it may have cast; 0 if the coordinator's master processed it */
int participant_withdraw_ddl_vote(const char *dist_txnid, const char *coordinator_name, const char *coordinator_tier);

/* a prepared schema change's commit failed: stop heartbeating it */
void participant_ddl_commit_failed(const char *dist_txnid);

/* tables of prepared, undecided 2pc schema changes; statements naming them wait */
struct schema_change_type;
void *disttxn_ddl_pending_begin(struct schema_change_type *scs, const char *dist_txnid, const char *coordinator);
void disttxn_ddl_pending_end(void *pending);
int disttxn_ddl_table_pending(const char *table);
/* 1 if it waited, 0 if not pending, -1 if stop(arg) (checked about every second) gave up */
int disttxn_ddl_table_wait(const char *table, int (*stop)(void *), void *arg);
int disttxn_ddl_any_pending(void);
/* 0, ETIMEDOUT at deadline_ms (epoch ms; 0 waits forever), or for coordinator dist_txnid (else NULL) EDEADLK if a
 * pending ddl may be waiting for it */
int disttxn_ddl_wait_none_pending(int64_t deadline_ms, const char *dist_txnid);

/* This participant processes a coordinator's abort */
int coordinator_aborted(const char *dist_txnid);

/* Return 1 if dist_txnid is prepared and not yet committed or aborted */
int disttxn_is_prepared(const char *dist_txnid);

/* this process saw a prepare record for dist_txnid, so it may have voted yes */
void disttxn_ddl_note_prepared(const char *dist_txnid);
int disttxn_ddl_was_prepared(const char *dist_txnid);
void disttxn_ddl_forget_prepared(const char *dist_txnid);

/* This participant processes a cordinator's commit */
int coordinator_committed(const char *dist_txnid);

/* Allow this dbname/tier to act as coordinator for this participant */
void allow_coordinator(const char *dbname, const char *tier);

/* Forbid this dbname/tier to act as coordinator for this participant */
void forbid_coordinator(const char *dbname, const char *tier);

/* Process allow-coordinator directive */
char *process_allow_coordinator(char *line, int lline);

/* Process forbid-coordinator directive */
char *process_forbid_coordinator(char *line, int lline);

/* Show all allowed coordinators */
void show_allowed_coordinators(void);

/* Return 1 if we can act as a participant */
int coordinator_is_allowed(const char *dbname, const char *tier);

/* Clear disttxn handles and hashes on new master */
void disttxn_cleanup(void);

/* Prevent deadlock by disabling heartbeats while waiting for a writer thread */
void disable_heartbeats_before_dispatch(const char *dist_txnid);

/* Re-enable participant heartbeats from writer thread */
void reenable_participant_heartbeats(const char *dist_txnid);

/* Initialize disttxn data structures & register recovery callback */
void disttxn_init();

/* Initialize recover-prepared callback */
void disttxn_init_recover_prepared();
#endif
