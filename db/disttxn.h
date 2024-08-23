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

/* coordinator_wait / participant_wait rcodes */
enum { KEEP_RCODE = 0, HAS_COMMITTED = 1, HAS_ABORTED = 2, IS_NOT_DURABLE = 3, NO_DIST_TABLE = 4, LOCK_DESIRED = 5 };

/* This coordinator records participant information */
int collect_participants(const char *dist_txnid, participant_list_t *participants);

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

/* This participant processes a coordinator's abort */
int coordinator_aborted(const char *dist_txnid);

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
