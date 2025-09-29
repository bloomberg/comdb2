#include <disttxn.h>
#include <sqlquery.pb-c.h>
#include <pthread.h>
#include <epochlib.h>
#include <plhash_glue.h>
#include <stdlib.h>
#include <string.h>
#include <locks.h>
#include <sys_wrap.h>
#include <assert.h>
#include <bdb_api.h>
#include <bdb_int.h>
#include <cdb2api.h>
#include <cdb2api_int.h>
#include <unistd.h>
#include <alloca.h>
#include <segstr.h>
#include <sql.h>

/**
 * coordinator:
 * 1 add participants to data-structures
 * 2 tell participants to execute
 * 3 collect results
 * 4 write record to distributed_transactions table and commit
 * 5 notify participants of results
 * 6 distributed wait-for-seqnum (but can't mark anything incoherent)
 *
 * participant:
 * 1 prepare schedule and notify coordinator
 * 2 wait for coordinator response
 * 3 either commit or abort
 */

#define DISTRIBUTED_TRANSACTIONS_SCHEMA                                                                                \
    "{ tag ondisk { cstring dist_txnid[129] datetime timestamp dbstore=\"CURRENT_TIMESTAMP\" null=yes } keys{ "        \
    "\"ix0\" = dist_txnid dup \"ix1\" = timestamp }}"
#define COORDINATOR_LOCAL "_coordinator_local"

/* States for the transaction tracked on the coordinator */
enum transaction_states {
    DISTTXN_COLLECTED = 0,
    DISTTXN_PREPARING = 1,
    DISTTXN_ABORTED = 2,
    DISTTXN_PREPARED = 3,
    DISTTXN_COMMITTED = 4,
    DISTTXN_PROPAGATED = 5,
    DISTTXN_PROPAGATE_TIMEOUT = 6,
    DISTTXN_ABORT_RESOLVED = 7,
    DISTTXN_LOCK_DESIRED = 8,
    DISTTXN_FAILED_DISPATCH = 9
};

/* States for each participant tracked on the coordinator */
enum participant_states {
    PARTICIPANT_INIT = 0,
    PARTICIPANT_FAILED_PREPARE = 1,
    PARTICIPANT_FAILED_SEND_PREPARE = 2,
    PARTICIPANT_PREPARING = 3,
    PARTICIPANT_PREPARED = 4,
    PARTICIPANT_PROPAGATED = 5,
    PARTICIPANT_FAILED_PROPAGATE = 6,
    PARTICIPANT_FAILED = 7,
};

/* States for each participant as tracked by participant */
enum { I_AM_PREPARED = 1, I_AM_ABORTED = 2, I_AM_COMMITTED = 3 };

/* Coordinator tracking structure for a distributed transaction */
typedef struct distributed_transaction {

    char *dist_txnid;

    int state;

    time_t start_time;
    time_t resolved_time;

    participant_list_t participants;
    struct participant *coordinator_local;

    int prepared_count;
    int propagated_count;
    int propagated_timeout;
    int failed_count;

    int coordinator_waiting;
    pthread_mutex_t lk;
    pthread_cond_t cd;
    pthread_cond_t send_cd;
    LINKC_T(struct distributed_transaction) linkv;

    int sending;
    int failrc;
    int outrc;
    char *errstr;
    char *failpart_name;
    char *failpart_tier;

} transaction_t;

/* Simple structure which blocks participant */
typedef struct block_participant {
    char *dist_txnid;
    int state;
    int start_wait;
    pthread_mutex_t lk;
    pthread_cond_t cd;
} participant_t;

/* Simple structure which maps dist-txnid to local rqid+uuid */
typedef struct sanctioned {
    char *dist_txnid;
    unsigned long long rqid;
    uuid_t uuid;
    char *coordinator_dbname;
    char *coordinator_tier;
    char *coordinator_master;
    int sanctioned;
    dist_hbeats_type hbeats;
} sanctioned_t;

/* Handle-cache might go away */
typedef struct hndlnode {
    int donate_time;
    cdb2_hndl_tp *hndl;
    LINKC_T(struct hndlnode) linkv;
} hndlnode_t;

typedef struct hndlcache {
    char *dbname;
    char *machine;
    LISTC_T(struct hndlnode) handles;
} hndlcache_t;

typedef struct allowed_coordinators {
    char *dbname;
    char *tier;
} allowed_coordinators_t;

struct disttxn_collect {
    char **disttxns;
    int count;
};

/* Coordinator table containing active transactions */
static hash_t *active_transactions_hash = NULL;
static pthread_mutex_t active_transactions_lk = PTHREAD_MUTEX_INITIALIZER;

/* Participant hash table containing prepared transactions */
static hash_t *participant_hash = NULL;
static pthread_mutex_t part_lk = PTHREAD_MUTEX_INITIALIZER;

/* Osql sanctioned hash maps dist-txn to rqid+uuid */
static hash_t *sanctioned_hash = NULL;
static pthread_mutex_t sanc_lk = PTHREAD_MUTEX_INITIALIZER;

/* Hndl hash retains cdb2api handles */
static hash_t *handle_hash = NULL;
static pthread_mutex_t hndl_lk = PTHREAD_MUTEX_INITIALIZER;

/* Allowed coordinators hash */
static hash_t *allowed_coordinators_hash = NULL;
static pthread_mutex_t allowed_coordinators_lk = PTHREAD_MUTEX_INITIALIZER;

static char allowed_coordinators_str[2048] = {0};

/* Tunables */
int gbl_2pc = 0;
int gbl_disttxn_linger_time = 10;
int gbl_disttxn_handle_linger_time = 60;
int gbl_disttxn_handle_cache = 1;

/* Only coordinator 'prepare' is synchronous */
int gbl_disttxn_async_messages = 1;
int gbl_coordinator_sync_on_commit = 1;
int gbl_coordinator_block_until_durable = 1;

/* Debug tunables */
int gbl_debug_exit_participant_after_prepare = 0;
int gbl_debug_exit_coordinator_before_commit = 0;
int gbl_debug_sleep_coordinator_before_commit = 0;
int gbl_debug_exit_coordinator_after_commit = 0;
int gbl_debug_coordinator_dispatch_failure = 0;
int gbl_debug_disttxn_trace = 0;

/* Externs */
extern char gbl_dbname[MAX_DBNAME_LENGTH];
extern char *gbl_machine_class;
extern char *gbl_myhostname;

/* Constants */
#define HEARTBEAT_TIMEOUT_MS 5000

/* Create the distributed transactions table */
static void create_distributed_transactions_table(void)
{
    int rc;
    struct sqlclntstate clnt;
    start_internal_sql_clnt(&clnt);
    clnt.dbtran.mode = TRANLEVEL_SOSQL;
    clnt.admin = 1;
    clnt.skip_eventlog = 1;
    char *create_sql = "create table " DISTRIBUTED_TRANSACTIONS_TABLE DISTRIBUTED_TRANSACTIONS_SCHEMA;
    rc = run_internal_sql_clnt(&clnt, create_sql);
    end_internal_sql_clnt(&clnt);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "Error creating distributed-transactions table: %d\n", rc);
    }
}

void disttxn_verify_table(void)
{
    static int verified = 0;

    if (!gbl_2pc || verified) {
        return;
    }
    char *conf = getenv("CDB2_CONFIG");
    if (conf) {
        cdb2_set_comdb2db_config(conf);
    }

    if (get_dbtable_by_name(DISTRIBUTED_TRANSACTIONS_TABLE) == NULL) {
        create_distributed_transactions_table();
    } else {
        verified = 1;
    }
}

static void disttxn_recover_prepared(void)
{
    thedb->bdb_env->dbenv->txn_recover_all_prepared(thedb->bdb_env->dbenv);
}

static int retrieve_handle(cdb2_hndl_tp **hndl, const char *dbname, const char *type, int flags)
{
    if (gbl_disttxn_handle_cache) {
        hndlcache_t fnd = {.dbname = (char *)dbname, .machine = (char *)type}, *h;
        hndlnode_t *n = NULL;
        Pthread_mutex_lock(&hndl_lk);
        if ((h = hash_find(handle_hash, &fnd)) != NULL) {
            n = listc_rtl(&h->handles);
        }
        Pthread_mutex_unlock(&hndl_lk);
        if (n) {
            (*hndl) = n->hndl;
            free(n);
            return 0;
        }
    }
    int rc = cdb2_open(hndl, dbname, type, flags);
    if (!rc && gbl_debug_disttxn_trace) {
        cdb2_set_debug_trace(*hndl);
    }
    return rc;
}

static int donate_handle(cdb2_hndl_tp *hndl, const char *dbname, const char *machine)
{
    if (!gbl_disttxn_handle_cache) {
        return cdb2_close(hndl);
    }
    hndlcache_t fnd = {.dbname = (char *)dbname, .machine = (char *)machine}, *h;
    Pthread_mutex_lock(&hndl_lk);
    if ((h = hash_find(handle_hash, &fnd)) == NULL) {
        h = calloc(sizeof(*h), 1);
        h->dbname = strdup(dbname);
        h->machine = strdup(machine);
        listc_init(&h->handles, offsetof(hndlnode_t, linkv));
        hash_add(handle_hash, h);
    }
    hndlnode_t *n = calloc(sizeof(*n), 1);
    n->donate_time = comdb2_time_epoch();
    n->hndl = hndl;
    listc_abl(&h->handles, n);
    Pthread_mutex_unlock(&hndl_lk);
    return 0;
}

/* Create and send a disttxn message */
static int send_2pc_message(const char *dist_txnid, const char *dbname, const char *pname, const char *ptier,
                            const char *master, int op, int rcode, int outrc, const char *errmsg, int async)
{
    cdb2_hndl_tp *hndl = NULL;
    int flags = CDB2_DIRECT_CPU, rc;
    int start = comdb2_time_epochms(), end;

    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s dbname=%s pname=%s ptier=%s master=%s op=%d\n", __func__,
               dist_txnid, dbname, pname, ptier, master, op);
    }

    if ((rc = retrieve_handle(&hndl, dbname, master, flags)) != 0) {
        logmsg(LOGMSG_INFO, "%s error opening handle to %s %s, rc=%d\n", __func__, dbname, master, rc);
        return -1;
    }

    if ((rc = cdb2_send_2pc(hndl, (char *)dbname, (char *)pname, (char *)ptier, gbl_myhostname, op, (char *)dist_txnid,
                            rcode, outrc, (char *)errmsg, async)) != 0) {
        logmsg(LOGMSG_INFO, "%s dist-txn %s error sending to %s mach %s, rc=%d\n", __func__, dist_txnid, dbname, master,
               rc);
        cdb2_close(hndl);
        return -1;
    }
    donate_handle(hndl, dbname, master);
    end = comdb2_time_epochms();
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s took %d ms to send to %s\n", __func__, dist_txnid, end - start,
               dbname);
    }
    return 0;
}

/* This coordinator is telling this participant to commit */
static void send_participant_commit(const char *dist_txnid, const char *dbname, const char *master)
{
    send_2pc_message(dist_txnid, dbname, NULL, NULL, master, CDB2_DIST__COMMIT, 0, 0, NULL, gbl_disttxn_async_messages);
}

/* This coordinator is telling this participant to abort */
static void send_participant_abort(const char *dist_txnid, const char *dbname, const char *master)
{
    send_2pc_message(dist_txnid, dbname, NULL, NULL, master, CDB2_DIST__ABORT, 0, 0, NULL, gbl_disttxn_async_messages);
}

/* This participant is telling the coordinator that it has prepared */
static void send_coordinator_prepared(const char *dist_txnid, const char *dbname, const char *master)
{
    char *pname = gbl_dbname, *ptier = gbl_machine_class ? gbl_machine_class : gbl_myhostname;
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s dbname=%s tier=%s, nyname=%s mytier=%s\n", __func__, dist_txnid,
               dbname, master, pname, ptier);
    }
    send_2pc_message(dist_txnid, dbname, pname, ptier, master, CDB2_DIST__PREPARED, 0, 0, NULL,
                     gbl_disttxn_async_messages);
}

/* Find coordinator master & tell it that we have prepared */
static void send_coordinator_prepared_find_master(const char *dist_txnid, const char *coordinator_dbname,
                                                  const char *coordinator_tier)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s dbname=%s tier=%s\n", __func__, dist_txnid, coordinator_dbname,
               coordinator_tier);
    }
    cdb2_hndl_tp *hndl = NULL;
    int flags = CDB2_MASTER, rc;

    if ((rc = cdb2_open(&hndl, coordinator_dbname, coordinator_tier, flags)) != 0) {
        logmsg(LOGMSG_INFO, "%s dist-txn %s error opening handle to %s:%s, rc=%d\n", __func__, dist_txnid,
               coordinator_dbname, coordinator_tier, rc);
        return;
    }
    if (gbl_debug_disttxn_trace) {
        cdb2_set_debug_trace(hndl);
    }
    char *pname = gbl_dbname, *ptier = gbl_machine_class ? gbl_machine_class : gbl_myhostname;
    if ((rc = cdb2_send_2pc(hndl, (char *)coordinator_dbname, pname, ptier, gbl_myhostname, CDB2_DIST__PREPARED,
                            (char *)dist_txnid, 0, 0, NULL, gbl_disttxn_async_messages)) != 0) {
        logmsg(LOGMSG_INFO, "%s dist-txn %s error sending to %s tier %s, rc=%d\n", __func__, dist_txnid,
               coordinator_dbname, coordinator_tier, rc);
    }
    cdb2_close(hndl);
}

/* Return true if comdb2_distributed_transactions has this record */
static int transaction_has_committed(const char *dist_txnid)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    char key[MAXKEYLEN] = {0}, fndkey[MAXKEYLEN];
    int keylen, rrn;
    tran_type *trans = NULL;
    struct ireq iq = {0};
    struct schema *s;
    struct field *f;
    unsigned long long genid;

    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s slow-path\n", __func__, dist_txnid);
    }

    init_fake_ireq(thedb, &iq);

    BDB_READLOCK("transaction_has_committed");

    /* Don't answer if we haven't replicated to a quorum for this generation */
    if (!bdb_committed_durable(thedb->bdb_env)) {
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "%s returning NOT-DURABLE for disttxn %s\n", __func__, dist_txnid);
        }
        BDB_RELLOCK();
        return IS_NOT_DURABLE;
    }

    iq.usedb = get_dbtable_by_name(DISTRIBUTED_TRANSACTIONS_TABLE);

    if (!iq.usedb) {
        logmsg(LOGMSG_ERROR, "%s: couldn't find dist-transaction table\n", __func__);
        BDB_RELLOCK();
        return NO_DIST_TABLE;
    }

    bzero(fndkey, sizeof(fndkey));
    keylen = 0;

    s = iq.usedb->ixschema[0];
    f = &s->member[0];
    set_data(key + f->offset, dist_txnid, strlen(dist_txnid) + 1);
    keylen += f->len;

    int rc = ix_find_flags(&iq, trans, 0, key, keylen, fndkey, &rrn, &genid, NULL, 0, 0, IX_FIND_IGNORE_INCOHERENT);

    BDB_RELLOCK();

    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s slow-path find rcode=%d\n", __func__, dist_txnid, rc);
    }

    return (rc == IX_FND) ? HAS_COMMITTED : HAS_ABORTED;
}

static const char *dtran_state_str(int state)
{
    switch (state) {
    case DISTTXN_COLLECTED:
        return "DISTTXN_COLLECTED";
    case DISTTXN_PREPARING:
        return "DISTTXN_PREPARING";
    case DISTTXN_ABORTED:
        return "DISTTXN_ABORTED";
    case DISTTXN_PREPARED:
        return "DISTTXN_PREPARED";
    case DISTTXN_COMMITTED:
        return "DISTTXN_COMMITTED";
    case DISTTXN_PROPAGATED:
        return "DISTTXN_PROPAGATED";
    case DISTTXN_PROPAGATE_TIMEOUT:
        return "DISTTXN_PROPAGATE_TIMEOUT";
    case DISTTXN_ABORT_RESOLVED:
        return "DISTTXN_ABORT_RESOLVED";
    case DISTTXN_LOCK_DESIRED:
        return "DISTTXN_LOCK_DESIRED";
    }
    return "UNKNOWN";
}

static const char *has_committed_rcode(int rcode)
{
    switch (rcode) {
    case KEEP_RCODE:
        return "KEEP_RCODE";
    case HAS_COMMITTED:
        return "HAS_COMMITTED";
    case HAS_ABORTED:
        return "HAS_ABORTED";
    case IS_NOT_DURABLE:
        return "IS_NOT_DURABLE";
    case NO_DIST_TABLE:
        return "NO_DIST_TABLE";
    case LOCK_DESIRED:
        return "LOCK_DESIRED";
    }
    return "UNKNOWN";
}

static void recover_prepared_transaction(const char *dist_txnid, const char *coordinator_dbname,
                                         const char *coordinator_tier)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s %s %s %s\n", __func__, dist_txnid, coordinator_dbname, coordinator_tier);
    }
    if (!strcmp(coordinator_tier, COORDINATOR_LOCAL)) {
        int committed = transaction_has_committed(dist_txnid);
        if (committed == HAS_COMMITTED) {
            if (gbl_debug_disttxn_trace) {
                logmsg(LOGMSG_USER, "DISTTXN %s txn %s coordinator-local txn has committed\n", __func__, dist_txnid);
            }
            thedb->bdb_env->dbenv->txn_commit_recovered(thedb->bdb_env->dbenv, dist_txnid);
        } else if (committed == HAS_ABORTED) {
            if (gbl_debug_disttxn_trace) {
                logmsg(LOGMSG_USER, "DISTTXN %s txn %s coordinator-local txn has aborted\n", __func__, dist_txnid);
            }
            thedb->bdb_env->dbenv->txn_abort_recovered(thedb->bdb_env->dbenv, dist_txnid);
        } else if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s txn %s coordinator-local txn-has-committed returns %s\n", __func__,
                   dist_txnid, has_committed_rcode(committed));
        }
        return;
    }
    send_coordinator_prepared_find_master(dist_txnid, coordinator_dbname, coordinator_tier);
}

/* This participant is telling the coordinator that it failed to prepare */
static void send_coordinator_failed_prepare(const char *dist_txnid, const char *dbname, const char *master, int rcode,
                                            int outrc, const char *errmsg)
{
    char *pname = gbl_dbname, *ptier = gbl_machine_class ? gbl_machine_class : gbl_myhostname;
    send_2pc_message(dist_txnid, dbname, pname, ptier, master, CDB2_DIST__FAILED_PREPARE, rcode, outrc, errmsg,
                     gbl_disttxn_async_messages);
}

/* This participant is telling the coordinator that it has propagated the txn */
static void send_coordinator_propagated(const char *dist_txnid, const char *dbname, const char *master)
{
    char *pname = gbl_dbname, *ptier = gbl_machine_class ? gbl_machine_class : gbl_myhostname;
    send_2pc_message(dist_txnid, dbname, pname, ptier, master, CDB2_DIST__PROPAGATED, 0, 0, NULL,
                     gbl_disttxn_async_messages);
}

/* This coordinator asks this participant to begin preparing its osql schedule */
static int send_prepare_message(transaction_t *dtran, struct participant *part, int op)
{
    cdb2_hndl_tp *hndl = NULL;
    int flags = CDB2_MASTER, rc;

    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s pname %s tier %s op %d\n", __func__, dtran->dist_txnid,
               part->participant_name, part->participant_tier, op);
    }

    if ((rc = cdb2_open(&hndl, part->participant_name, part->participant_tier, flags)) != 0) {
        logmsg(LOGMSG_INFO, "%s disttxn %s failed to dispatch to %s/%s\n", __func__, dtran->dist_txnid,
               part->participant_name, part->participant_tier);
        part->status = PARTICIPANT_FAILED_PREPARE;
        return -1;
    }

    if (gbl_debug_disttxn_trace) {
        cdb2_set_debug_trace(hndl);
    }

    /* Prepare is sent synchronously- fail immediately if any are unsuccessful */
    char *tier = gbl_machine_class ? gbl_machine_class : gbl_myhostname;
    if ((rc = cdb2_send_2pc(hndl, part->participant_name, gbl_dbname, tier, gbl_myhostname, op, dtran->dist_txnid, 0, 0,
                            NULL, 0)) != 0) {
        part->status = PARTICIPANT_FAILED_SEND_PREPARE;
    }

    if (gbl_debug_disttxn_trace) {
        const char *host = cdb2_host(hndl);
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s pname %s connected to host %s\n", __func__, dtran->dist_txnid,
               part->participant_name, host);
    }

    if (gbl_debug_coordinator_dispatch_failure && !rc && !(rand() % 10000)) {
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s pname %s fake dispatch failure\n", __func__,
                   dtran->dist_txnid, part->participant_name);
        }
        rc = -1;
    }

    if (!rc) {
        part->participant_master = strdup(cdb2_host(hndl));
        part->status = PARTICIPANT_PREPARING;
    } else {
        part->status = PARTICIPANT_FAILED_PREPARE;
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s rcode from cdb2_send_2pc to %s/%s is %d %s\n", __func__,
                   part->participant_name, cdb2_host(hndl), rc, cdb2_errstr(hndl));
        }
    }
    cdb2_close(hndl);
    return rc;
}

/* Tell all participants to commit */
static void commit_participants_lk(transaction_t *dtran)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dtran->dist_txnid);
    }
    struct participant *part = NULL, *tmp = NULL;
    if (dtran->state == DISTTXN_PREPARED) {
        dtran->state = DISTTXN_COMMITTED;
        LISTC_FOR_EACH_SAFE(&dtran->participants, part, tmp, linkv)
        {
            send_participant_commit(dtran->dist_txnid, part->participant_name, part->participant_master);
        }
    }
}

static int need_to_cancel_osql(struct participant *part)
{
    return (part->status == PARTICIPANT_INIT || part->status == PARTICIPANT_FAILED_PREPARE ||
            part->status == PARTICIPANT_FAILED_SEND_PREPARE);
}

static int need_to_cancel_prepare(struct participant *part)
{
    return (part->status == PARTICIPANT_PREPARING || part->status == PARTICIPANT_PREPARED);
}

static void abort_transaction_lk(transaction_t *dtran, int abort_only_prepared)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dtran->dist_txnid);
    }
    struct participant *part = NULL, *tmp = NULL;
    int sending = dtran->sending;
    dtran->state = DISTTXN_ABORTED;
    if (!sending) {
        dtran->sending = 1;
        Pthread_mutex_unlock(&dtran->lk);
    }

    LISTC_FOR_EACH_SAFE(&dtran->participants, part, tmp, linkv)
    {
        if (abort_only_prepared && part->status != PARTICIPANT_PREPARED) {
            continue;
        }
        if (need_to_cancel_prepare(part)) {
            assert(part->participant_master);
            send_participant_abort(dtran->dist_txnid, part->participant_name, part->participant_master);
        }
    }
    /* Signal coordinator_local */
    if (!sending) {
        Pthread_mutex_lock(&dtran->lk);
        dtran->sending = 0;
    }
    dtran->resolved_time = comdb2_time_epochms();
    Pthread_cond_signal(&dtran->cd);
    Pthread_cond_signal(&dtran->send_cd);
}

static void prepare_participants_lk(transaction_t *dtran)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dtran->dist_txnid);
    }
    struct participant *part = NULL, *tmp = NULL;
    int rc = 0;
    dtran->sending = 1;
    LISTC_FOR_EACH_SAFE(&dtran->participants, part, tmp, linkv)
    {
        part->last_heartbeat = comdb2_time_epochms();
    }

    Pthread_mutex_unlock(&dtran->lk);
    LISTC_FOR_EACH_SAFE(&dtran->participants, part, tmp, linkv)
    {
        Pthread_mutex_lock(&part->lk);
        if ((rc = send_prepare_message(dtran, part, CDB2_DIST__PREPARE)) != 0) {
            logmsg(LOGMSG_INFO, "%s fail send-prepare disttxn=%s db=%s tier=%s rc=%d\n", __func__, dtran->dist_txnid,
                   part->participant_name, part->participant_tier, rc);
            Pthread_mutex_unlock(&part->lk);
            break;
        }
        Pthread_mutex_unlock(&part->lk);
    }
    if (rc) {
        abort_transaction_lk(dtran, 0);
    }
    Pthread_mutex_lock(&dtran->lk);
    if (rc) {
        dtran->failrc = ERR_DIST_ABORT;
        dtran->outrc = ERR_BLOCK_FAILED;
        dtran->errstr = strdup("Failed to dispatch");
        dtran->state = DISTTXN_FAILED_DISPATCH;
    }
    Pthread_cond_signal(&dtran->send_cd);
    dtran->sending = 0;
}

int dispatch_participants(const char *dist_txnid)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    transaction_t *dtran;
    Pthread_mutex_lock(&active_transactions_lk);
    if ((dtran = hash_find(active_transactions_hash, &dist_txnid)) != NULL) {
        Pthread_mutex_lock(&dtran->lk);
    }
    Pthread_mutex_unlock(&active_transactions_lk);
    if (dtran) {
        dtran->start_time = comdb2_time_epochms();
        if (dtran->state == DISTTXN_COLLECTED) {
            dtran->state = DISTTXN_PREPARING;
            prepare_participants_lk(dtran);
        }
        Pthread_mutex_unlock(&dtran->lk);
    } else {
        logmsg(LOGMSG_INFO, "%s missing dist_txnid %s?\n", __func__, dist_txnid);
        return -1;
    }
    return 0;
}

int collect_participants(const char *dist_txnid, participant_list_t *participants)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    int have_table = get_dbtable_by_name(DISTRIBUTED_TRANSACTIONS_TABLE) ? 1 : 0;

    /* Punt immediately if not possible */
    if (!gbl_2pc || !have_table) {
        logmsg(LOGMSG_ERROR, "%s cannot begin disttxn %s, gbl_2pc=%d, have_table=%d\n", __func__, dist_txnid, gbl_2pc,
               have_table);
        return -1;
    }

    /* Don't double add */
    transaction_t *dtran;
    Pthread_mutex_lock(&active_transactions_lk);
    dtran = hash_find(active_transactions_hash, &dist_txnid);
    if (dtran) {
        Pthread_mutex_unlock(&active_transactions_lk);
        logmsg(LOGMSG_INFO, "%s duplicate disttxn for %s\n", __func__, dist_txnid);
        return 0;
    }

    /* Prepare dist-txn struct */
    dtran = calloc(sizeof(*dtran), 1);
    listc_init(&dtran->participants, offsetof(struct participant, linkv));
    dtran->dist_txnid = strdup(dist_txnid);
    Pthread_mutex_init(&dtran->lk, NULL);
    Pthread_cond_init(&dtran->cd, NULL);
    Pthread_cond_init(&dtran->send_cd, NULL);
    dtran->state = DISTTXN_COLLECTED;

    /* Grab participant list */
    struct participant *part;
    while ((part = listc_rtl(participants)) != NULL) {
        part->status = PARTICIPANT_INIT;
        Pthread_mutex_init(&part->lk, NULL);
        listc_atl(&dtran->participants, part);
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "%s added %s/%s\n", __func__, part->participant_name, part->participant_tier);
        }
    }

    /* Coordinator local */
    dtran->coordinator_local = calloc(sizeof(struct participant), 1);
    dtran->coordinator_local->last_heartbeat = comdb2_time_epochms();
    dtran->coordinator_local->participant_name = strdup(gbl_dbname);
    dtran->coordinator_local->participant_tier = strdup(COORDINATOR_LOCAL);
    dtran->coordinator_local->status = PARTICIPANT_PREPARING;
    dtran->coordinator_waiting = 0;

    /* Add to txn hash */
    hash_add(active_transactions_hash, dtran);
    Pthread_mutex_unlock(&active_transactions_lk);

    return 0;
}

void destroy_dtran(transaction_t *dtran)
{
    Pthread_mutex_destroy(&dtran->lk);
    Pthread_cond_destroy(&dtran->cd);
    free(dtran->dist_txnid);
    if (dtran->errstr)
        free(dtran->errstr);
    if (dtran->failpart_name)
        free(dtran->failpart_name);
    if (dtran->failpart_tier)
        free(dtran->failpart_tier);
    struct participant *part;
    while ((part = listc_rtl(&dtran->participants)) != NULL) {
        free(part->participant_name);
        free(part->participant_tier);
        free(part->participant_master);
    }
    free(dtran);
}

static int collect_disttxn(void *obj, void *arg)
{
    struct disttxn_collect *collect = (struct disttxn_collect *)arg;
    transaction_t *dtran = (transaction_t *)obj;
    collect->disttxns[collect->count++] = strdup(dtran->dist_txnid);
    return 0;
}

static void cleanup_dtran(const char *dist_txnid)
{
    Pthread_mutex_lock(&active_transactions_lk);
    transaction_t *dtran = hash_find(active_transactions_hash, &dist_txnid);
    if (dtran) {
        Pthread_mutex_lock(&dtran->lk);
        hash_del(active_transactions_hash, dtran);
    }
    Pthread_mutex_unlock(&active_transactions_lk);
    if (dtran) {
        int count = 0;
        while (dtran->sending) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;
            pthread_cond_timedwait(&dtran->send_cd, &dtran->lk, &ts);
            count++;
            if (count > 2) {
                logmsg(LOGMSG_INFO, "%s waiting for %s send to complete\n", __func__, dist_txnid);
            }
        }
        Pthread_mutex_unlock(&dtran->lk);
        destroy_dtran(dtran);
    }
}

static void cleanup_all_dtrans(void)
{
    struct disttxn_collect collect = {0};
    int alloc;
    Pthread_mutex_lock(&active_transactions_lk);
    if (!active_transactions_hash) {
        Pthread_mutex_unlock(&active_transactions_lk);
        return;
    }
    hash_info(active_transactions_hash, NULL, NULL, NULL, NULL, &alloc, NULL, NULL);
    collect.disttxns = malloc(sizeof(char *) * alloc);
    hash_for(active_transactions_hash, collect_disttxn, &collect);
    Pthread_mutex_unlock(&active_transactions_lk);
    for (int i = 0; i < alloc; i++) {
        cleanup_dtran(collect.disttxns[i]);
    }

    for (int i = 0; i < collect.count; i++) {
        free(collect.disttxns[i]);
    }
    free(collect.disttxns);
}

void destroy_sanc(sanctioned_t *sanc)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s %s destroying sanc at %p\n", __func__, sanc->dist_txnid, sanc);
    }
    free(sanc->dist_txnid);
    free(sanc->coordinator_dbname);
    free(sanc->coordinator_tier);
    free(sanc->coordinator_master);
    free(sanc);
}

static struct participant *find_participant_lk(transaction_t *dtran, const char *dbname, const char *tier)
{
    struct participant *part = NULL, *tmp = NULL;
    LISTC_FOR_EACH_SAFE(&dtran->participants, part, tmp, linkv)
    {
        if (!strcmp(part->participant_name, dbname) && !strcmp(part->participant_tier, tier))
            return part;
    }
    logmsg(LOGMSG_ERROR, "%s couldn't find participant %s %s for dist-txnid %s\n", __func__, dbname, tier,
           dtran->dist_txnid);
    return NULL;
}

int disttxn_ondisk_record(struct ireq *iq, char *dist_txnid, unsigned char *rec)
{
    struct schema *s;
    int rc = 0, fix;

    s = iq->usedb->schema;
    if (!s) {
        logmsg(LOGMSG_ERROR, "%s: cannot find schema?\n", __func__);
        return -1;
    }

    /* Borrowing Adi sqlstat1 scheme */
    for (fix = 0; fix < s->nmembers && rc == 0; fix++) {
        struct field *f = &s->member[fix];
        char *cur = NULL;
        int outdtsz = 0, null = 0, type = 0, len = 0, epoch;

        if (0 == strcmp(f->name, "dist_txnid")) {
            cur = dist_txnid;
            type = CLIENT_CSTR;
            len = strlen(cur) + 1;
        } else if (0 == strcmp(f->name, "timestamp")) {
            epoch = htonl(comdb2_time_epoch());
            type = CLIENT_INT;
            cur = (char *)&epoch;
            len = sizeof(int);
        } else {
            logmsg(LOGMSG_ERROR, "%s: unknown field '%s' in " DISTRIBUTED_TRANSACTIONS_TABLE "\n", __func__, f->name);
            return -1;
        }

        rc = CLIENT_to_SERVER(cur, len, type, null, NULL, NULL, rec + f->offset, f->len, f->type, 0, &outdtsz,
                              &f->convopts, NULL);

        if (-1 == rc) {
            logmsg(LOGMSG_ERROR, "%s: CLIENT_to_SERVER returns %d\n", __func__, rc);
            return -1;
        }
    }
    return rc;
}

static void flush_log(void)
{
    thedb->bdb_env->dbenv->log_flush(thedb->bdb_env->dbenv, NULL);
}

/* Commit == write the dist-txnid to comdb2_distributed_transactions */
static int commit_distributed_transaction(transaction_t *dtran)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dtran->dist_txnid);
    }
    struct ireq iq = {0};
    char *tagname = ".ONDISK";
    uint8_t *p_tagname_buf = (uint8_t *)tagname;
    uint8_t *p_tagname_buf_end = p_tagname_buf + 7;
    uint8_t *p_buf_data, *p_buf_data_end;
    unsigned long long genid = 0LL;
    tran_type *trans;
    int opcodefail = 0, ixfailnum = 0, rrn = 0, dlcount = 0, rc;

    /* Held by the coordinator transaction */
    assert(bdb_lockref() > 0);

    init_fake_ireq(thedb, &iq);
    iq.usedb = get_dbtable_by_name(DISTRIBUTED_TRANSACTIONS_TABLE);

    if (!iq.usedb) {
        logmsg(LOGMSG_ERROR, "%s: couldn't find dist-txn table\n", __func__);
        return -1;
    }

    p_buf_data = alloca(iq.usedb->schema->recsize);
    p_buf_data_end = p_buf_data + iq.usedb->schema->recsize;

retry:
    rc = disttxn_ondisk_record(&iq, dtran->dist_txnid, p_buf_data);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: couldn't create ondisk record for dist-txn table, rc=%d\n", __func__, rc);
        return rc;
    }

    rc = trans_start(&iq, NULL, &trans);
    if (rc != 0) {
        logmsg(LOGMSG_INFO, "%s error starting transaction, %d\n", __func__, rc);
        return -1;
    }

    if (gbl_debug_disttxn_trace) {
        bdb_trans_track(thedb->bdb_env, trans);
    }

    int addflags = (RECFLAGS_NO_TRIGGERS | RECFLAGS_NO_BLOBS | RECFLAGS_INLINE_CONSTRAINTS);

    rc = add_record(&iq, trans, p_tagname_buf, p_tagname_buf_end, p_buf_data, p_buf_data_end, NULL, NULL, 0,
                    &opcodefail, &ixfailnum, &rrn, &genid, -1ULL, BLOCK2_ADDKL, 0, addflags, 0);

    if (rc != 0) {
        trans_abort(&iq, trans);
        if (rc == RC_INTERNAL_RETRY && ++dlcount < gbl_maxretries) {
            goto retry;
        }
        if (rc == RC_INTERNAL_RETRY) {
            logmsg(LOGMSG_ERROR, "%s dist-txn %s exceeded maximum retries\n", __func__, dtran->dist_txnid);
        } else {
            logmsg(LOGMSG_ERROR, "%s error adding dist-txn %s, rc=%d\n", __func__, dtran->dist_txnid, rc);
        }
        return -1;
    }

    if (gbl_coordinator_sync_on_commit) {
        trans_commit_adaptive(&iq, trans, gbl_myhostname);
        flush_log();
    } else {
        trans_commit_nowait(&iq, trans, gbl_myhostname);
    }

    /* Don't return until the commit has propagated to a quorum.  */
    if (gbl_coordinator_block_until_durable) {
        DB_LSN commit_lsn = {.file = iq.commit_file, .offset = iq.commit_offset};
        rc = bdb_block_durable(thedb->bdb_env, &commit_lsn);
    }

    return rc ? LOCK_DESIRED : 0;
}

/* Signal coordinator when this has completely propagated */
static int disttxn_check_propagated_lk(transaction_t *dtran)
{
    int rtn = 0;
    if (dtran->state == DISTTXN_COMMITTED &&
        (dtran->propagated_count + dtran->propagated_timeout) == listc_size(&dtran->participants)) {
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s has propagated\n", __func__, dtran->dist_txnid);
        }
        dtran->state = DISTTXN_PROPAGATED;
        rtn = 1;
    } else if (gbl_debug_disttxn_trace) {
        struct participant *part = NULL, *tmp = NULL;
        char hosts[1024] = {0};
        LISTC_FOR_EACH_SAFE(&dtran->participants, part, tmp, linkv)
        {
            if (part->status == PARTICIPANT_PREPARED) {
                strcat(hosts, part->participant_name);
                strcat(hosts, " ");
            }
        }
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s has not yet propagated, waiting for %s\n", __func__,
               dtran->dist_txnid, hosts);
    }
    return rtn;
}

/* Signal coordinator if this is committable */
static int disttxn_check_commitable_lk(transaction_t *dtran)
{
    int rtn = 0;
    if (dtran->state == DISTTXN_PREPARING && dtran->prepared_count == listc_size(&dtran->participants) &&
        dtran->coordinator_local->status == PARTICIPANT_PREPARED) {
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s commitable\n", __func__, dtran->dist_txnid);
        }
        /* Signal coordinator_local */
        rtn = 1;
        dtran->state = DISTTXN_PREPARED;
        Pthread_cond_signal(&dtran->cd);
    } else if (gbl_debug_disttxn_trace) {
        char hosts[1024] = {0};
        struct participant *part = NULL, *tmp = NULL;
        LISTC_FOR_EACH_SAFE(&dtran->participants, part, tmp, linkv)
        {
            char *pmaster = part->participant_master ? part->participant_master : "(failed-prepare)";
            if (part->status != PARTICIPANT_PREPARED) {
                strcat(hosts, part->participant_name);
                strcat(hosts, "/");
                strcat(hosts, pmaster);
                strcat(hosts, " ");
            }
        }
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s not-yet commitable waiting for %s\n", __func__, dtran->dist_txnid,
               hosts);
    }
    return rtn;
}

static void participant_propagated_lk(transaction_t *dtran, struct participant *part)
{
    if (part->status == PARTICIPANT_PREPARED) {
        part->status = PARTICIPANT_PROPAGATED;
        dtran->propagated_count++;
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s %s/%s/%s count %d\n", __func__, dtran->dist_txnid,
                   part->participant_name, part->participant_tier, part->participant_master, dtran->propagated_count);
        }
        Pthread_cond_signal(&dtran->cd);
    } else if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_INFO, "DISTTXN %s disttxn %s participant propagate for %s ignored because status = %d\n",
               __func__, dtran->dist_txnid, part->participant_name, part->status);
    }
}

static void participant_prepared_lk(transaction_t *dtran, struct participant *part)
{
    Pthread_mutex_lock(&part->lk);
    if (part->status == PARTICIPANT_PREPARING) {
        part->status = PARTICIPANT_PREPARED;
        dtran->prepared_count++;
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s %s:%s from %s count %d\n", __func__, dtran->dist_txnid,
                   part->participant_name, part->participant_tier, part->participant_master, dtran->prepared_count);
        }
    }
    Pthread_mutex_unlock(&part->lk);
}

static void participant_failed_lk(transaction_t *dtran, struct participant *part)
{
    Pthread_mutex_lock(&part->lk);
    if (part->status == PARTICIPANT_PREPARING) {
        part->status = PARTICIPANT_FAILED;
        dtran->failed_count++;
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s %s:%s from %s count %d\n", __func__, dtran->dist_txnid,
                   part->participant_name, part->participant_tier, part->participant_master, dtran->failed_count);
        }
    }
    Pthread_mutex_unlock(&part->lk);
}

/* The coordinator was unable to execute it's osql stream */
void coordinator_failed(const char *dist_txnid)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    transaction_t *dtran;

    Pthread_mutex_lock(&active_transactions_lk);
    dtran = hash_find(active_transactions_hash, &dist_txnid);
    if (dtran)
        Pthread_mutex_lock(&dtran->lk);
    Pthread_mutex_unlock(&active_transactions_lk);

    /* Aborted before coordinator finished preparing .. */
    if (!dtran) {
        logmsg(LOGMSG_INFO, "%s unable to find disttxn %s, assume abort\n", __func__, dist_txnid);
        return;
    }

    dtran->coordinator_local->status = PARTICIPANT_FAILED;

    assert(dtran->state != DISTTXN_PREPARED && dtran->state != DISTTXN_COMMITTED && dtran->state != DISTTXN_PROPAGATED);

    if (dtran->state == DISTTXN_PREPARING) {
        abort_transaction_lk(dtran, 0);
    }
    dtran->state = DISTTXN_ABORT_RESOLVED;

    Pthread_mutex_unlock(&dtran->lk);
    cleanup_dtran(dist_txnid);
}

static void check_participant_propagate_timeout_lk(transaction_t *dtran)
{
    struct participant *part = NULL, *tmp = NULL;
    int nowms = comdb2_time_epochms();
    /* XXX only count heartbeats from preparing transactions */
    LISTC_FOR_EACH_SAFE(&dtran->participants, part, tmp, linkv)
    {
        if (part->status == PARTICIPANT_PREPARED && ((nowms - part->last_heartbeat) > HEARTBEAT_TIMEOUT_MS)) {
            part->status = PARTICIPANT_FAILED_PROPAGATE;
            dtran->propagated_timeout++;
            logmsg(LOGMSG_USER, "DISTTXN %s dist_txn %s no heartbeat timeout\n", __func__, dtran->dist_txnid);
        }
    }
}

void coordinator_resolve(const char *dist_txnid)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    transaction_t *dtran;

    Pthread_mutex_lock(&active_transactions_lk);
    dtran = hash_find(active_transactions_hash, &dist_txnid);
    if (dtran)
        Pthread_mutex_lock(&dtran->lk);
    Pthread_mutex_unlock(&active_transactions_lk);

    /* txn disappeared? */
    if (!dtran) {
        logmsg(LOGMSG_ERROR, "%s unable to find disttxn %s, aborting\n", __func__, dist_txnid);
#if DEBUG_DISTTXN
        abort();
#endif
        return;
    } else {
        dtran->state = DISTTXN_PROPAGATE_TIMEOUT;
        dtran->resolved_time = comdb2_time_epochms();
    }
    Pthread_mutex_unlock(&dtran->lk);
    cleanup_dtran(dist_txnid);
}

/* We send 'participant_propagated' after participant-master defers commits.  So any node
 * it intended to mark incoherent will be incoherent. */
int coordinator_wait_propagate(const char *dist_txnid)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    transaction_t *dtran;
    int state;

    Pthread_mutex_lock(&active_transactions_lk);
    dtran = hash_find(active_transactions_hash, &dist_txnid);
    if (dtran)
        Pthread_mutex_lock(&dtran->lk);
    Pthread_mutex_unlock(&active_transactions_lk);

    /* Aborted before coordinator finished preparing .. */
    if (!dtran) {
        logmsg(LOGMSG_ERROR, "%s unable to find disttxn %s, aborting\n", __func__, dist_txnid);
#if DEBUG_DISTTXN
        abort();
#endif
        return 0;
    }

    check_participant_propagate_timeout_lk(dtran);
    disttxn_check_propagated_lk(dtran);
    dtran->coordinator_waiting = 1;

    int startwait = comdb2_time_epochms(), elapsed = 0;

    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s coordinator propagate-wait for %s\n", __func__, dist_txnid);
    }

    while (dtran->state == DISTTXN_COMMITTED &&
           (dtran->propagated_count + dtran->propagated_timeout < listc_size(&dtran->participants))) {
        if ((elapsed / 1000 > 5)) {
            logmsg(LOGMSG_DEBUG, "%s: dist-txnid %s waiting for participants to propagate for %d seconds\n", __func__,
                   dist_txnid, elapsed / 1000);
        }
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        pthread_cond_timedwait(&dtran->cd, &dtran->lk, &ts);
        elapsed = (comdb2_time_epochms() - startwait);
        check_participant_propagate_timeout_lk(dtran);
        disttxn_check_propagated_lk(dtran);
    }
    if (dtran->state != DISTTXN_PROPAGATED) {
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s propagate timeout state=%d\n", __func__, dist_txnid,
                   dtran->state);
        }
        dtran->state = DISTTXN_PROPAGATE_TIMEOUT;
    }
    state = dtran->state;
    dtran->coordinator_waiting = 0;
    dtran->resolved_time = comdb2_time_epochms();
    Pthread_mutex_unlock(&dtran->lk);
    cleanup_dtran(dist_txnid);

    return !(state == DISTTXN_PROPAGATED);
}

int retryable_rcode(int rcode, int outrc)
{
    return (outrc == ERR_NOTSERIAL || (outrc == ERR_BLOCK_FAILED && rcode == ERR_VERIFY));
}

static int check_participant_timeout_lk(transaction_t *dtran)
{
    struct participant *part = NULL, *tmp = NULL;
    int nowms = comdb2_time_epochms();
    /* XXX only count heartbeats from preparing transactions */
    LISTC_FOR_EACH_SAFE(&dtran->participants, part, tmp, linkv)
    {
        if (part->status == PARTICIPANT_PREPARING && ((nowms - part->last_heartbeat) > HEARTBEAT_TIMEOUT_MS)) {
            logmsg(LOGMSG_INFO, "%s disttxn %s no heartbeat from %s %s timing out\n", __func__, dtran->dist_txnid,
                   part->participant_name, part->participant_tier);
            return 1;
        }
    }
    return 0;
}

static int coordinator_should_wait_lk(transaction_t *dtran, int can_retry, int *lock_desired)
{
    *lock_desired = 0;
    if (bdb_lock_desired(thedb->bdb_env)) {
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s disttxn %s lock-desired, not waiting\n", __func__, dtran->dist_txnid);
        }
        *lock_desired = 1;
        return 0;
    }
    if (check_participant_timeout_lk(dtran)) {
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s disttxn %s participant timeout\n", __func__, dtran->dist_txnid);
        }
        return 0;
    }

    if (dtran->state == DISTTXN_COLLECTED || dtran->state == DISTTXN_PREPARING) {
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s returning true because state=%s\n", __func__,
                   dtran->state == DISTTXN_COLLECTED ? "collected" : "preparing");
        }
        return 1;
    }
    if (dtran->state == DISTTXN_ABORTED && can_retry &&
        (!dtran->failrc || retryable_rcode(dtran->failrc, dtran->outrc)) &&
        (dtran->prepared_count + dtran->failed_count) < listc_size(&dtran->participants)) {
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s returning true because prepared=%d failed=%d sz=%d\n",
                   __func__, dtran->dist_txnid, dtran->prepared_count, dtran->failed_count,
                   listc_size(&dtran->participants));
        }
        return 1;
    }

    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER,
               "DISTTXN %s dist_txnid %s returning false, state=%d can_retry=%d failrc=%d outrc=%d prepared-count=%d "
               "failed-count=%d\n",
               __func__, dtran->dist_txnid, dtran->state, can_retry, dtran->failrc, dtran->outrc, dtran->prepared_count,
               dtran->failed_count);
    }
    return 0;
}

/* This blocks the coordinator's bp until we know whether this can commit, or until a timeout */
static int coordinator_wait_int(const char *dist_txnid, int can_retry, int *rcode, int *outrc, char *errmsg,
                                int errmsglen, int force_failure)
{
    int returnzero = 0;
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    transaction_t *dtran;
    int state;

    Pthread_mutex_lock(&active_transactions_lk);
    dtran = hash_find(active_transactions_hash, &dist_txnid);
    if (dtran)
        Pthread_mutex_lock(&dtran->lk);
    Pthread_mutex_unlock(&active_transactions_lk);

    /* Aborted before coordinator finished preparing .. */
    if (!dtran) {
        logmsg(LOGMSG_ERROR, "%s unable to find disttxn %s, aborting\n", __func__, dist_txnid);
#if DEBUG_DISTTXN
        abort();
#endif
        return 0;
    }

    dtran->coordinator_local->status = PARTICIPANT_PREPARED;

    /* This can toggle the state to COMMITTING */
    disttxn_check_commitable_lk(dtran);

    dtran->coordinator_waiting = 1;
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s coordinator waiting for %s\n", __func__, dist_txnid);
    }

    /* Measured from beginning of txn */
    int elapsed = (comdb2_time_epochms() - dtran->start_time);
    int start_timer = comdb2_time_epochms();
    int lock_desired = 0;
    while (coordinator_should_wait_lk(dtran, can_retry, &lock_desired)) {
        if ((elapsed / 1000 > 3)) {
            logmsg(LOGMSG_ERROR, "%s: dist-txnid %s waiting for participants for %d seconds\n", __func__, dist_txnid,
                   elapsed / 1000);
        }
        struct timespec ts;

        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        pthread_cond_timedwait(&dtran->cd, &dtran->lk, &ts);
        elapsed = (comdb2_time_epochms() - dtran->start_time);
    }
    int stop_timer = comdb2_time_epochms();
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s wait took %d ms for %s\n", __func__, stop_timer - start_timer, dist_txnid);
    }
    /* Aborted or timeout -> RESOLVED */
    if (dtran->state == DISTTXN_ABORTED || dtran->state == DISTTXN_FAILED_DISPATCH) {
        *rcode = dtran->failrc;
        *outrc = dtran->outrc;
        snprintf(errmsg, errmsglen, "%s:%s %s", dtran->failpart_name, dtran->failpart_tier, dtran->errstr);
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s setting rc=%d outrc=%d errmsg=%s\n", __func__, *rcode, *outrc, errmsg);
        }
        dtran->state = DISTTXN_ABORT_RESOLVED;
    }

    /* We are being asked to downgrade while waiting for participant's response.
     * because we haven't written the commit record yet we can just fail. */
    if ((dtran->state == DISTTXN_PREPARED || dtran->state == DISTTXN_PREPARING) && lock_desired) {
        *rcode = ERR_DIST_ABORT;
        *outrc = ERR_BLOCK_FAILED;
        snprintf(errmsg, errmsglen, "aborting on downgrade");
        abort_transaction_lk(dtran, 0);
        dtran->state = DISTTXN_ABORT_RESOLVED;
    }

    /* Force-failure: this is a replayable rcode and all other participants succeeded
     * returning 0 asks toblock to use it's original rcode */
    if (dtran->state == DISTTXN_PREPARED && force_failure) {
        *rcode = ERR_DIST_ABORT;
        *outrc = ERR_BLOCK_FAILED;
        abort_transaction_lk(dtran, 0);
        /* We are about to abort so no need to modify state */
        // dtran->state = DISTTXN_ABORT_RESOLVED;
        returnzero = 1;
    }

    /* Timeout: no heartbeat from participant */
    if (dtran->state == DISTTXN_PREPARING) {
        *rcode = ERR_DIST_ABORT;
        *outrc = ERR_BLOCK_FAILED;
        snprintf(errmsg, errmsglen, "coordinator timeout");
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "%s DISTTXN coordinator timing out %s\n", __func__, dtran->dist_txnid);
            logmsg(LOGMSG_USER, "DISTTXN %s setting rc=%d outrc=%d errmsg=%s\n", __func__, *rcode, *outrc, errmsg);
        }
        abort_transaction_lk(dtran, 0);
        dtran->state = DISTTXN_ABORT_RESOLVED;
    }
    state = dtran->state;
    Pthread_mutex_unlock(&dtran->lk);

    /* Everything prepared successfully, write disttxn record */
    if (state == DISTTXN_PREPARED && !force_failure) {
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "%s DISTTXN coordinator committing %s\n", __func__, dtran->dist_txnid);
        }
        if (gbl_debug_exit_coordinator_before_commit) {
            flush_log();
            logmsg(LOGMSG_FATAL, "%s exiting coordinator before commit on debug tunable\n", __func__);
            sleep(3);
            exit(1);
        }
        if (gbl_debug_sleep_coordinator_before_commit) {
            logmsg(LOGMSG_USER, "%s DISTTXN %s sleeping before writing commit record on debug tunable\n", __func__,
                   dtran->dist_txnid);
            sleep(5);
        }
        start_timer = comdb2_time_epochms();
        int rc = commit_distributed_transaction(dtran);
        stop_timer = comdb2_time_epochms();
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s commit took %d ms\n", __func__, stop_timer - start_timer);
        }

        if (gbl_debug_exit_coordinator_after_commit) {
            flush_log();
            logmsg(LOGMSG_FATAL, "%s exiting coordinator after commit on debug tunable\n", __func__);
            sleep(3);
            exit(1);
        }

        Pthread_mutex_lock(&dtran->lk);

        /* Wrote commit record but haven't replicated to quorum */
        if (rc == LOCK_DESIRED) {
            dtran->state = DISTTXN_LOCK_DESIRED;
            if (gbl_debug_disttxn_trace) {
                logmsg(LOGMSG_USER, "DISTTXN %s %s failed to commit durably\n", __func__, dtran->dist_txnid);
            }
        } else if (rc) {
            *rcode = ERR_DIST_ABORT;
            *outrc = ERR_BLOCK_FAILED;
            snprintf(errmsg, errmsglen, "failed writing to disttxn table");
            start_timer = comdb2_time_epochms();
            abort_transaction_lk(dtran, 0);
            stop_timer = comdb2_time_epochms();
            if (gbl_debug_disttxn_trace) {
                logmsg(LOGMSG_USER, "DISTTXN %s abort txn took %d ms\n", __func__, stop_timer - start_timer);
            }
        } else {
            start_timer = comdb2_time_epochms();
            commit_participants_lk(dtran);
            stop_timer = comdb2_time_epochms();
            if (gbl_debug_disttxn_trace) {
                logmsg(LOGMSG_USER, "DISTTXN %s commit participants took %d ms\n", __func__, stop_timer - start_timer);
            }
        }
        dtran->resolved_time = comdb2_time_epochms();
        state = dtran->state;
        Pthread_mutex_unlock(&dtran->lk);
    } else {
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "%s DISTTXN coordinator aborting %s state=%d force-failure=%d\n", __func__,
                   dtran->dist_txnid, state, force_failure);
        }
    }
    dtran->coordinator_waiting = 0;
    if (state == DISTTXN_ABORT_RESOLVED) {
        cleanup_dtran(dist_txnid);
    }

    if (state == DISTTXN_LOCK_DESIRED) {
        return LOCK_DESIRED;
    }

    if (returnzero) {
        return KEEP_RCODE;
    }

    return (state == DISTTXN_COMMITTED) ? HAS_COMMITTED : HAS_ABORTED;
}

int coordinator_wait(const char *dist_txnid, int can_retry, int *rcode, int *outrc, char *errmsg, int errmsglen,
                     int force_failure)
{
    int startms = comdb2_time_epochms();
    int rc = coordinator_wait_int(dist_txnid, can_retry, rcode, outrc, errmsg, errmsglen, force_failure);
    int endms = comdb2_time_epochms();
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s txnid %s took %d ms\n", __func__, dist_txnid, endms - startms);
    }
    return rc;
}

static int collect_handles_timeout(void *obj, void *arg, int timeout)
{
    hndlnode_t *hndl = NULL, *tmp = NULL;
    hndlcache_t *hcache = (hndlcache_t *)obj;
    LISTC_T(struct hndlnode) *oldhandles = arg;
    int now = comdb2_time_epoch();

    LISTC_FOR_EACH_SAFE(&hcache->handles, hndl, tmp, linkv)
    {
        if ((now - hndl->donate_time) > timeout) {
            listc_rfl(&hcache->handles, hndl);
            listc_abl(oldhandles, hndl);
        }
    }
    return 0;
}

static int collect_handles(void *obj, void *arg)
{
    return collect_handles_timeout(obj, arg, gbl_disttxn_handle_linger_time);
}

static int collect_all_handles(void *obj, void *arg)
{
    return collect_handles_timeout(obj, arg, -1);
}

/* We now use wait-die for deadlock prevention.  This is still useful for detecting
 * crashed participants and aborting a distributed traqnsaction in the case of
 * super-long coordinator osql schedules */
static void disttxn_timeout(void)
{
    struct disttxn_collect collect = {0};
    int alloc;
    Pthread_mutex_lock(&active_transactions_lk);
    hash_info(active_transactions_hash, NULL, NULL, NULL, NULL, &alloc, NULL, NULL);
    collect.disttxns = malloc(sizeof(char *) * alloc);
    hash_for(active_transactions_hash, collect_disttxn, &collect);
    Pthread_mutex_unlock(&active_transactions_lk);
    for (int i = 0; i < collect.count; i++) {
        transaction_t *dtran;
        Pthread_mutex_lock(&active_transactions_lk);
        dtran = hash_find(active_transactions_hash, &collect.disttxns[i]);
        if (dtran) {
            Pthread_mutex_lock(&dtran->lk);
        }
        Pthread_mutex_unlock(&active_transactions_lk);

        if (dtran && dtran->state == DISTTXN_PREPARING && !dtran->coordinator_waiting &&
            (check_participant_timeout_lk(dtran))) {
            abort_transaction_lk(dtran, 0);
            dtran->failrc = ERR_DIST_ABORT;
            dtran->outrc = ERR_BLOCK_FAILED;
            dtran->errstr = strdup("Transaction timeout");
            logmsg(LOGMSG_INFO, "DISTTXN %s disttxn %s aborted by timeout\n", __func__, dtran->dist_txnid);
        }
        if (dtran) {
            Pthread_mutex_unlock(&dtran->lk);
        }
    }
    for (int i = 0; i < collect.count; i++) {
        free(collect.disttxns[i]);
    }
    free(collect.disttxns);
}

static void disttxn_purge_handles(int purgeall)
{
    Pthread_mutex_lock(&hndl_lk);
    if (!handle_hash) {
        Pthread_mutex_unlock(&hndl_lk);
        return;
    }

    /* Collect old handles */
    LISTC_T(struct hndlnode) oldhandles;
    listc_init(&oldhandles, offsetof(hndlnode_t, linkv));
    hash_for(handle_hash, purgeall ? collect_all_handles : collect_handles, &oldhandles);
    Pthread_mutex_unlock(&hndl_lk);

    /* Close and delete */
    hndlnode_t *hndl;
    while ((hndl = listc_rtl(&oldhandles))) {
        cdb2_close(hndl->hndl);
        free(hndl);
    }
}

static int is_committed(transaction_t *dtran)
{
    switch (dtran->state) {
    case DISTTXN_COMMITTED:
    case DISTTXN_PROPAGATED:
    case DISTTXN_PROPAGATE_TIMEOUT:
        return 1;
    }
    return 0;
}

static int is_aborted(transaction_t *dtran)
{
    switch (dtran->state) {
    case DISTTXN_ABORTED:
    case DISTTXN_ABORT_RESOLVED:
    case DISTTXN_FAILED_DISPATCH:
        return 1;
    }
    return 0;
}

static void signal_coordinator_wakeup_lk(transaction_t *dtran)
{
    if ((dtran->prepared_count + dtran->failed_count) == listc_size(&dtran->participants)) {
        Pthread_cond_signal(&dtran->cd);
    }
}

/* Core coordinator state machine */
static int participant_result(const char *dist_txnid, const char *dbname, const char *tier, const char *master,
                              int prepare_success, int partrc, int outrc, const char *errstr)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s from %s/%s/%s dist_txnid %s success=%d rc=%d outrc=%d errstr=%s\n", __func__,
               dbname, tier, master, dist_txnid, prepare_success, partrc, outrc, errstr);
    }
    transaction_t *dtran;

    Pthread_mutex_lock(&active_transactions_lk);
    dtran = hash_find(active_transactions_hash, &dist_txnid);
    if (dtran)
        Pthread_mutex_lock(&dtran->lk);
    Pthread_mutex_unlock(&active_transactions_lk);

    if (dtran) {
        /* Register success for ongoing transaction */
        if (prepare_success && dtran->state == DISTTXN_PREPARING) {
            struct participant *part = find_participant_lk(dtran, dbname, tier);
            if (!part) {
                Pthread_mutex_unlock(&dtran->lk);
                return -1;
            }
            participant_prepared_lk(dtran, part);
            part->last_heartbeat = comdb2_time_epochms();
            disttxn_check_commitable_lk(dtran);
            signal_coordinator_wakeup_lk(dtran);
            Pthread_mutex_unlock(&dtran->lk);
            return 0;
        }

        /* Notify participant of abort  */
        if (prepare_success && is_aborted(dtran)) {
            struct participant *part = find_participant_lk(dtran, dbname, tier);
            if (!part) {
                Pthread_mutex_unlock(&dtran->lk);
                return -1;
            }
            participant_failed_lk(dtran, part);
            signal_coordinator_wakeup_lk(dtran);
            Pthread_mutex_unlock(&dtran->lk);
            send_participant_abort(dist_txnid, dbname, master);
            return 0;
        }

        /* Tell this participant to commit */
        if (prepare_success && is_committed(dtran)) {
            Pthread_mutex_unlock(&dtran->lk);
            send_participant_commit(dist_txnid, dbname, master);
            return 0;
        }

        /* Tell all participants to abort */
        if (!prepare_success && dtran->state == DISTTXN_PREPARING) {
            struct participant *part = find_participant_lk(dtran, dbname, tier);
            if (!part) {
                Pthread_mutex_unlock(&dtran->lk);
                return -1;
            }
            participant_failed_lk(dtran, part);
            dtran->failrc = partrc;
            dtran->outrc = outrc;
            dtran->errstr = strdup(errstr);
            dtran->failpart_name = strdup(dbname);
            dtran->failpart_tier = strdup(tier);
            if (gbl_debug_disttxn_trace) {
                logmsg(LOGMSG_USER, "DISTTXN %s disttxn %s aborted by %s tier %s\n", __func__, dist_txnid, dbname,
                       tier);
            }
            /* If this can be retried, only cancel participants we have gotten results from.
             * This is because a different participant may have a non-retryable rcode */
            abort_transaction_lk(dtran, retryable_rcode(partrc, outrc));
            signal_coordinator_wakeup_lk(dtran);
            Pthread_mutex_unlock(&dtran->lk);
            return 0;
        }

        /* Don't need to abort already failed txn */
        if (!prepare_success) {
            struct participant *part = find_participant_lk(dtran, dbname, tier);
            if (!part) {
                Pthread_mutex_unlock(&dtran->lk);
                return -1;
            }
            participant_failed_lk(dtran, part);

            /* Switch from retryable to non-retryable rcode */
            if (retryable_rcode(dtran->failrc, dtran->outrc) && !retryable_rcode(partrc, outrc)) {

                /* Free old data */
                free(dtran->errstr);
                free(dtran->failpart_name);
                free(dtran->failpart_tier);

                /* Store new data */
                dtran->errstr = strdup(errstr);
                dtran->failpart_name = strdup(dbname);
                dtran->failpart_tier = strdup(tier);
                dtran->failrc = partrc;
                dtran->outrc = outrc;
            }

            /* Signal coordinator */
            signal_coordinator_wakeup_lk(dtran);
            assert(is_aborted(dtran));
            Pthread_mutex_unlock(&dtran->lk);
            return 0;
        }

        /* Waiting for coordinator to write commit record or new master */
        int state = dtran->state;
        Pthread_mutex_unlock(&dtran->lk);

        assert(state == DISTTXN_PREPARED || state == DISTTXN_LOCK_DESIRED);
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "%s DISTTXN %s not answering %s/%s, state=%s\n", __func__, dist_txnid, dbname, tier,
                   dtran_state_str(state));
        }
        return 0;
    }

    /* Slow path: either dist-txn has aged out or something crashed */
    if (prepare_success) {
        int committed = transaction_has_committed(dist_txnid);
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s disttxn %s transaction_has_committed from %s %s returns %s\n", __func__,
                   dist_txnid, dbname, tier, has_committed_rcode(committed));
        }
        switch (committed) {
        case HAS_COMMITTED:
            send_participant_commit(dist_txnid, dbname, master);
            break;
        case HAS_ABORTED:
            send_participant_abort(dist_txnid, dbname, master);
        }
    }

    /* Audit: TODO, remove this.. a participant should only give one answer, not 2 */
    if (!prepare_success) {
        if (transaction_has_committed(dist_txnid) == HAS_COMMITTED) {
            logmsg(LOGMSG_ERROR, "%s committed an aborted dist-txnid %s\n", __func__, dist_txnid);
#if DEBUG_DISTTXN
            abort();
#endif
        }
    }

    return 0;
}

/* Participant master tells me (coordinator master) it has prepared */
int participant_prepared(const char *dist_txnid, const char *dbname, const char *tier, const char *master)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    return participant_result(dist_txnid, dbname, tier, master, 1, 0, 0, NULL);
}

/* Participant master tells me (coordinator master) it has failed */
int participant_failed(const char *dist_txnid, const char *dbname, const char *tier, int rc, int outrc,
                       const char *errstr)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s rc %d outrc %d retryable %d\n", __func__, dist_txnid, rc, outrc,
               retryable_rcode(rc, outrc));
    }
    return participant_result(dist_txnid, dbname, tier, NULL, 0, rc, outrc, errstr);
}

/* Participant master tells me (coordinator master) it has propagated */
int participant_propagated(const char *dist_txnid, const char *dbname, const char *tier)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    transaction_t *dtran;

    Pthread_mutex_lock(&active_transactions_lk);
    dtran = hash_find(active_transactions_hash, &dist_txnid);
    if (dtran)
        Pthread_mutex_lock(&dtran->lk);
    Pthread_mutex_unlock(&active_transactions_lk);

    if (dtran) {
        struct participant *part = find_participant_lk(dtran, dbname, tier);
        if (!part) {
            Pthread_mutex_unlock(&dtran->lk);
            return -1;
        }
        participant_propagated_lk(dtran, part);
        Pthread_mutex_unlock(&dtran->lk);
    }
    return 0;
}

static participant_t *add_participant_lk(const char *dist_txnid, int state)
{
    participant_t *p = calloc(sizeof(participant_t), 1);
    p->dist_txnid = strdup(dist_txnid);
    p->state = state;
    Pthread_mutex_init(&p->lk, NULL);
    Pthread_cond_init(&p->cd, NULL);
    hash_add(participant_hash, p);
    return p;
}

static void rem_participant_lk(participant_t *p)
{
    hash_del(participant_hash, p);
    free(p->dist_txnid);
    free(p);
}

static void disable_sanc_heartbeats(const char *dist_txnid, int free_tran)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    Pthread_mutex_lock(&sanc_lk);
    sanctioned_t *sanc = hash_find(sanctioned_hash, &dist_txnid);
    assert(sanc);
    assert(sanc->hbeats.sanc == sanc);
    if (free_tran) {
        hash_del(sanctioned_hash, sanc);
        disable_dist_heartbeats_and_free(&sanc->hbeats);
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s removed %p from hash\n", __func__, dist_txnid, sanc);
        }
    } else {
        disable_dist_heartbeats(&sanc->hbeats);
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s retained %p in hash\n", __func__, dist_txnid, sanc);
        }
    }
    Pthread_mutex_unlock(&sanc_lk);
}

/* This can get into a different type of distributed deadlock if there are no
 * writer threads available on the master to dispatch this transaction.
 *
 * Simplified description: imagine that masters have only a single writer thread.
 * dist-txn #1 is prepared on master A, dist-txn #2 can't be dispatched (writer holds dist-txn 1)
 * dist-txn #2 is prepared on master B, dist-txn #1 can't be dispatched (writer holds dist-txn 2)
 *
 * We actually need heartbeats after the coordinator sends osql_sanction, because participants should
 * send heartbeats while they are collecting large transactions- coordinator will cancel these otherwise.
 *
 * This code cancels heartbeats after collecting a transaction, so that if the deadlock above has occurred,
 * the coordinator will abort the transaction after not receiving a heartbeat.  We re-enable heartbeats
 * after this has been dispatched to a writer-thread.
 */
void disable_heartbeats_before_dispatch(const char *dist_txnid)
{
    disable_sanc_heartbeats(dist_txnid, 0);
}

void reenable_participant_heartbeats(const char *dist_txnid)
{
    sanctioned_t *sanc;
    Pthread_mutex_lock(&sanc_lk);
    sanc = hash_find(sanctioned_hash, &dist_txnid);
    assert(sanc);
    enable_dist_heartbeats(&sanc->hbeats);
    Pthread_mutex_unlock(&sanc_lk);
}

/* Participant block-processor has failed */
int participant_has_failed(const char *dist_txnid, const char *coordinator_name, const char *coordinator_master,
                           int rcode, int outrc, const char *errmsg)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s rcode=%d outrc=%d errmsg=%s\n", __func__, dist_txnid, rcode,
               outrc, errmsg);
    }
    Pthread_mutex_lock(&part_lk);
    participant_t *p = hash_find(participant_hash, &dist_txnid);
    if (p) {
        p->state = I_AM_ABORTED;
        rem_participant_lk(p);
    }
    Pthread_mutex_unlock(&part_lk);
    disable_sanc_heartbeats(dist_txnid, 1);
    send_coordinator_failed_prepare(dist_txnid, coordinator_name, coordinator_master, rcode, outrc, errmsg);
    return 0;
}

void participant_has_propagated(const char *dist_txnid, const char *coordinator_name, const char *coordinator_master)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    send_coordinator_propagated(dist_txnid, coordinator_name, coordinator_master);
    disable_sanc_heartbeats(dist_txnid, 1);
}

/* Participant block-processor blocks on coordinator */
static int participant_wait_int(const char *dist_txnid, const char *coordinator_name, const char *coordinator_tier,
                                const char *coordinator_master)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    int rtn = 0, first = 1, lock_desired = 0;

    /* Because this isn't sent under lock, a coordinator commit can arrive before we get the lock */
    send_coordinator_prepared(dist_txnid, coordinator_name, coordinator_master);

    if (gbl_debug_exit_participant_after_prepare) {
        logmsg(LOGMSG_FATAL, "%s exiting participant on debug tunable\n", __func__);
        /* Sleep to make sure all replicants have gotten the prepare */
        sleep(3);
        exit(1);
    }

    Pthread_mutex_lock(&part_lk);

    participant_t *p = hash_find(participant_hash, &dist_txnid);
    if (!p) {
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s line %d adding participant %s\n", __func__, __LINE__, dist_txnid);
        }
        p = add_participant_lk(dist_txnid, I_AM_PREPARED);
        p->start_wait = comdb2_time_epochms();
    }

    Pthread_mutex_lock(&p->lk);
    Pthread_mutex_unlock(&part_lk);

    while (p->state == I_AM_PREPARED && !(lock_desired = bdb_lock_desired(thedb->bdb_env))) {
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s line %d sending prepared %s first=%d\n", __func__, __LINE__, dist_txnid,
                   first);
        }
        Pthread_mutex_unlock(&p->lk);
        if (first == 1) {
            first = 0;
        } else {
            send_coordinator_prepared_find_master(dist_txnid, coordinator_name, coordinator_tier);
            if (gbl_debug_disttxn_trace) {
                logmsg(LOGMSG_USER, "DISTTXN %s line %d finish sending prepared %s first=%d\n", __func__, __LINE__,
                       dist_txnid, first);
            }
        }
        Pthread_mutex_lock(&p->lk);
        if (p->state == I_AM_PREPARED) {
            int elapsed = comdb2_time_epochms() - p->start_wait;
            if ((elapsed / 1000 > 3)) {
                logmsg(LOGMSG_ERROR, "%s: dist-txnid %s blocked on coordinator for %d seconds\n", __func__, dist_txnid,
                       elapsed / 1000);
            }
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;
            pthread_cond_timedwait(&p->cd, &p->lk, &ts);
        }
    }
    rtn = p->state;
    Pthread_mutex_unlock(&p->lk);

    Pthread_mutex_lock(&part_lk);
    rem_participant_lk(p);
    Pthread_mutex_unlock(&part_lk);

    if (rtn != I_AM_COMMITTED) {
        disable_sanc_heartbeats(dist_txnid, 1);
    }

    if (rtn == I_AM_PREPARED && lock_desired) {
        return LOCK_DESIRED;
    }

    return (rtn == I_AM_COMMITTED) ? HAS_COMMITTED : HAS_ABORTED;
}

int participant_heartbeat(const char *dist_txnid, const char *participant_name, const char *participant_tier)
{
    int rcode = -1;
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s from %s %s\n", __func__, dist_txnid, participant_name,
               participant_tier);
    }
    transaction_t *dtran;
    Pthread_mutex_lock(&active_transactions_lk);
    dtran = hash_find(active_transactions_hash, &dist_txnid);
    if (dtran)
        Pthread_mutex_lock(&dtran->lk);
    Pthread_mutex_unlock(&active_transactions_lk);

    if (dtran) {
        struct participant *part = find_participant_lk(dtran, participant_name, participant_tier);
        if (!part) {
            Pthread_mutex_unlock(&dtran->lk);
            return -1;
        }
        part->last_heartbeat = comdb2_time_epochms();
        Pthread_mutex_unlock(&dtran->lk);
        rcode = 0;
    } else {
        logmsg(LOGMSG_ERROR, "%s heartbeat from %s/%s unable to find dist-txn %s\n", __func__, participant_name,
               participant_tier, dist_txnid);
    }

    return rcode;
}

static int participant_send_heartbeat(const char *dist_txnid, const char *coordinator_name,
                                      const char *coordinator_master)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    char *pname = gbl_dbname, *ptier = gbl_machine_class ? gbl_machine_class : gbl_myhostname;
    int rc = send_2pc_message(dist_txnid, coordinator_name, pname, ptier, coordinator_master, CDB2_DIST__HEARTBEAT, 0,
                              0, NULL, gbl_disttxn_async_messages);
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s rcode %d\n", __func__, dist_txnid, rc);
    }
    return rc;
}

int participant_wait(const char *dist_txnid, const char *coordinator_name, const char *coordinator_tier,
                     const char *coordinator_master)
{
    int startms = comdb2_time_epochms();
    int rc = participant_wait_int(dist_txnid, coordinator_name, coordinator_tier, coordinator_master);
    int endms = comdb2_time_epochms();
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s txnid %s took %d ms\n", __func__, dist_txnid, endms - startms);
    }
    return rc;
}

/* Coordinator master tells me (participant master) to abort */
int coordinator_aborted(const char *dist_txnid)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    Pthread_mutex_lock(&part_lk);
    participant_t *p = hash_find(participant_hash, &dist_txnid);
    if (!p) {

        /* Abort recovered transactions immediately */
        Pthread_mutex_unlock(&part_lk);
        int rc = thedb->bdb_env->dbenv->txn_abort_recovered(thedb->bdb_env->dbenv, dist_txnid);
        if (rc == 0) {
            if (gbl_debug_disttxn_trace) {
                logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s aborted recovered txn\n", __func__, dist_txnid);
            }
            return 0;
        }

        /* Add this as aborted */
        Pthread_mutex_lock(&part_lk);
        p = hash_find(participant_hash, &dist_txnid);
        if (!p) {
            if (gbl_debug_disttxn_trace) {
                logmsg(LOGMSG_USER, "DISTTXN %s line %d adding aborted participant %s\n", __func__, __LINE__,
                       dist_txnid);
            }
            p = add_participant_lk(dist_txnid, I_AM_ABORTED);
            Pthread_mutex_unlock(&part_lk);
            return 0;
        }
    }

    /* Signal participant */
    Pthread_mutex_lock(&p->lk);
    Pthread_mutex_unlock(&part_lk);
    if (p->state == I_AM_COMMITTED) {
        logmsg(LOGMSG_ERROR, "%s committed txn %s told to abort?\n", __func__, dist_txnid);
#if DEBUG_DISTTXN
        abort();
#endif
    } else {
        p->state = I_AM_ABORTED;
    }
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s line %d signaling existing participant %s\n", __func__, __LINE__, dist_txnid);
    }

    Pthread_cond_signal(&p->cd);
    Pthread_mutex_unlock(&p->lk);

    return 0;
}

/* Coordinator master tells me (participant master) to commit */
int coordinator_committed(const char *dist_txnid)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    Pthread_mutex_lock(&part_lk);
    participant_t *p = hash_find(participant_hash, &dist_txnid);
    if (!p) {
        p = add_participant_lk(dist_txnid, I_AM_COMMITTED);
        if (gbl_debug_disttxn_trace) {
            logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s commit occurred prior to wait\n", __func__, dist_txnid);
        }
        Pthread_mutex_unlock(&part_lk);
        /* Possibly a recovered prepare */
        int rc = thedb->bdb_env->dbenv->txn_commit_recovered(thedb->bdb_env->dbenv, dist_txnid);
        if (rc == 0) {
            logmsg(LOGMSG_INFO, "%s coordinator-commit for recovered transaction %s\n", __func__, dist_txnid);
            Pthread_mutex_lock(&part_lk);
            rem_participant_lk(p);
            Pthread_mutex_unlock(&part_lk);
        }
        return rc;
    }
    Pthread_mutex_lock(&p->lk);
    Pthread_mutex_unlock(&part_lk);

    assert(p->state != I_AM_ABORTED);
    p->state = I_AM_COMMITTED;

    Pthread_cond_signal(&p->cd);
    Pthread_mutex_unlock(&p->lk);

    return 0;
}

/* osql session registers this disttxn- it may have already been sanctioned by coordinator */
int osql_register_disttxn(const char *dist_txnid, unsigned long long rqid, uuid_t uuid, char **coordinator_dbname,
                          char **coordinator_tier, char **coordinator_master)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    int rtn = 0;
    sanctioned_t *sanc;
    Pthread_mutex_lock(&sanc_lk);
    sanc = hash_find(sanctioned_hash, &dist_txnid);
    if (sanc == NULL) {
        sanc = (sanctioned_t *)calloc(sizeof(*sanc), 1);
        sanc->dist_txnid = strdup(dist_txnid);
        sanc->rqid = rqid;
        comdb2uuidcpy(sanc->uuid, uuid);
        sanc->hbeats.sanc = sanc;
        hash_add(sanctioned_hash, sanc);
    } else {
        rtn = sanc->sanctioned;
        sanc->rqid = rqid;
        comdb2uuidcpy(sanc->uuid, uuid);
        if (sanc->sanctioned == 1) {
            (*coordinator_dbname) = strdup(sanc->coordinator_dbname);
            (*coordinator_tier) = strdup(sanc->coordinator_tier);
            (*coordinator_master) = strdup(sanc->coordinator_master);
        }
    }
    assert(sanc->hbeats.sanc == sanc);
    Pthread_mutex_unlock(&sanc_lk);
    return rtn;
}

/* Called from osql_discard- coordinator master tells me (participant master) to discard */
int osql_cancel_disttxn(const char *dist_txnid, unsigned long long *rqid, uuid_t *uuid)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    int rtn = 0;
    sanctioned_t *sanc;
    Pthread_mutex_lock(&sanc_lk);
    sanc = hash_find(sanctioned_hash, &dist_txnid);
    if (sanc != NULL) {
        (*rqid) = sanc->rqid;
        comdb2uuidcpy((*uuid), sanc->uuid);
        sanc->sanctioned = -1;
        rtn = 1;
    } else {
        sanc = (sanctioned_t *)calloc(sizeof(*sanc), 1);
        sanc->dist_txnid = strdup(dist_txnid);
        hash_add(sanctioned_hash, sanc);
        sanc->sanctioned = -1;
        rtn = 0;
    }
    Pthread_mutex_unlock(&sanc_lk);
    return rtn;
}

int dist_heartbeats(dist_hbeats_type *dt)
{
    return participant_send_heartbeat(dt->sanc->dist_txnid, dt->sanc->coordinator_dbname, dt->sanc->coordinator_master);
}

extern int enable_dist_heartbeats(dist_hbeats_type *dt);

/* Called from osql_prepare- coordinator master tells me (participant master) to prepare */
int osql_sanction_disttxn(const char *dist_txnid, unsigned long long *rqid, uuid_t *uuid,
                          const char *coordinator_dbname, const char *coordinator_tier, const char *coordinator_master)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dist_txnid);
    }
    int rtn = 0;
    sanctioned_t *sanc;
    Pthread_mutex_lock(&sanc_lk);
    sanc = hash_find(sanctioned_hash, &dist_txnid);
    if (sanc != NULL) {
        (*rqid) = sanc->rqid;
        comdb2uuidcpy((*uuid), sanc->uuid);
        sanc->sanctioned = 1;
        if (sanc->coordinator_master) {
            assert(!strcmp(coordinator_master, sanc->coordinator_master));
            assert(!strcmp(coordinator_dbname, sanc->coordinator_dbname));
            assert(!strcmp(coordinator_tier, sanc->coordinator_tier));
        } else {
            sanc->coordinator_master = strdup(coordinator_master);
            sanc->coordinator_dbname = strdup(coordinator_dbname);
            sanc->coordinator_tier = strdup(coordinator_tier);
        }
        rtn = 1;
    } else {
        sanc = (sanctioned_t *)calloc(sizeof(*sanc), 1);
        sanc->dist_txnid = strdup(dist_txnid);
        sanc->coordinator_dbname = strdup(coordinator_dbname);
        sanc->coordinator_tier = strdup(coordinator_tier);
        sanc->coordinator_master = strdup(coordinator_master);
        sanc->sanctioned = 1;
        sanc->hbeats.sanc = sanc;
        hash_add(sanctioned_hash, sanc);
        rtn = 0;
    }
    assert(sanc->hbeats.sanc == sanc);
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_DEBUG, "%s enabling heartbeats for %s %p sanc %p rtn=%d\n", __func__, sanc->dist_txnid,
               &sanc->hbeats, sanc, rtn);
    }
    enable_dist_heartbeats(&sanc->hbeats);
    Pthread_mutex_unlock(&sanc_lk);
    return rtn;
}

static int append_coordinator_str(void *obj, void *arg)
{
    allowed_coordinators_t *coordinator = (allowed_coordinators_t *)obj;
    int *offset = (int *)arg;
    char *st = (*offset) == 0 ? "%s/%s" : " %s/%s";
    (*offset) += snprintf(&allowed_coordinators_str[*offset], sizeof(allowed_coordinators_str) - (*offset), st,
                          coordinator->dbname, coordinator->tier);
    return 0;
}

static int show_coordinator(void *obj, void *arg)
{
    allowed_coordinators_t *coordinator = (allowed_coordinators_t *)obj;
    logmsg(LOGMSG_USER, "%s/%s\n", coordinator->dbname, coordinator->tier);
    return 0;
}

void show_allowed_coordinators(void)
{
    Pthread_mutex_lock(&allowed_coordinators_lk);
    hash_for(allowed_coordinators_hash, show_coordinator, NULL);
    Pthread_mutex_unlock(&allowed_coordinators_lk);
}

static void create_allowed_coordinators_str()
{
    int offset = 0;
    Pthread_mutex_lock(&allowed_coordinators_lk);
    hash_for(allowed_coordinators_hash, append_coordinator_str, &offset);
    Pthread_mutex_unlock(&allowed_coordinators_lk);
}

void allow_coordinator(const char *dbname, const char *tier)
{
    int already_allowed = 0;
    allowed_coordinators_t a = {.dbname = (char *)dbname, .tier = (char *)tier}, *c;
    Pthread_mutex_lock(&allowed_coordinators_lk);
    if ((c = hash_find(allowed_coordinators_hash, &a)) == NULL) {
        c = calloc(sizeof(*c), 1);
        c->dbname = strdup(dbname);
        c->tier = strdup(tier);
        hash_add(allowed_coordinators_hash, c);
    } else {
        already_allowed = 1;
    }
    Pthread_mutex_unlock(&allowed_coordinators_lk);
    if (already_allowed)
        logmsg(LOGMSG_USER, "%s %s/%s is already an allowed coordinator\n", __func__, dbname, tier);
}

void forbid_coordinator(const char *dbname, const char *tier)
{
    int already_forbidden = 0;
    allowed_coordinators_t a = {.dbname = (char *)dbname, .tier = (char *)tier}, *c;
    Pthread_mutex_lock(&allowed_coordinators_lk);
    if ((c = hash_find(allowed_coordinators_hash, &a)) != NULL) {
        hash_del(allowed_coordinators_hash, c);
        free(c->dbname);
        free(c->tier);
        free(c);
    } else {
        already_forbidden = 1;
    }
    Pthread_mutex_unlock(&allowed_coordinators_lk);
    if (already_forbidden)
        logmsg(LOGMSG_USER, "%s %s/%s was not an allowed coordinator\n", __func__, dbname, tier);
}

int coordinator_is_allowed(const char *dbname, const char *tier)
{
    allowed_coordinators_t a = {.dbname = (char *)dbname, .tier = (char *)tier}, *c;
    Pthread_mutex_lock(&allowed_coordinators_lk);
    c = hash_find(allowed_coordinators_hash, &a);
    Pthread_mutex_unlock(&allowed_coordinators_lk);
    return (c != NULL);
}

void dist_heartbeat_free_tran(dist_hbeats_type *dt)
{
    if (gbl_debug_disttxn_trace) {
        logmsg(LOGMSG_USER, "DISTTXN %s dist_txnid %s\n", __func__, dt->sanc->dist_txnid);
    }
    destroy_sanc(dt->sanc);
}

enum { PRIME = 8388013 };
static inline unsigned int hash_two_strptr(const char *s1, const char *s2)
{
    unsigned hash = 0;
    const char *key1 = s1;
    for (; *key1; key1++)
        hash = ((hash % PRIME) << 8) + (*key1);
    const char *key2 = s2;
    for (; *key2; key2++)
        hash = ((hash % PRIME) << 8) + (*key2);
    return hash;
}

static inline unsigned int hndl_hash(const void *key, int len)
{
    hndlcache_t *h = (hndlcache_t *)key;
    return hash_two_strptr(h->dbname, h->machine);
}

static int hndl_cmp(const void *key1, const void *key2, int len)
{
    hndlcache_t *h1 = (hndlcache_t *)key1;
    hndlcache_t *h2 = (hndlcache_t *)key2;
    int cmp;
    if ((cmp = strcmp(h1->dbname, h2->dbname)))
        return cmp;
    return strcmp(h1->machine, h2->machine);
}

static inline unsigned int coord_hash(const void *key, int len)
{
    allowed_coordinators_t *c = (allowed_coordinators_t *)key;
    return hash_two_strptr(c->dbname, c->tier);
}

static int coord_cmp(const char *key1, const char *key2, int len)
{
    allowed_coordinators_t *c1 = (allowed_coordinators_t *)key1;
    allowed_coordinators_t *c2 = (allowed_coordinators_t *)key2;
    int cmp;
    if ((cmp = strcmp(c1->dbname, c2->dbname)))
        return cmp;
    return strcmp(c1->tier, c2->tier);
}

void disttxn_cleanup(void)
{
    cleanup_all_dtrans();
    disttxn_purge_handles(1);
}

void disttxn_timer(void)
{
    disttxn_verify_table();
    disttxn_timeout();
    disttxn_purge_handles(0);
    disttxn_recover_prepared();
}

static void process_coordinator_directive(char *line, int lline, int allow)
{
    char *tok;
    int ltok, st = 0;

    tok = segtok(line, lline, &st, &ltok);
    while (ltok > 0) {
        char *name = alloca(ltok + 1);
        name[ltok] = '\0';
        tokcpy(tok, ltok, name);
        int namelen = strcspn(name, "/");
        if (namelen >= ltok) {
            logmsg(LOGMSG_USER, "%s: invalid format %s\n", __func__, name);
            tok = segtok(line, lline, &st, &ltok);
            continue;
        }
        name[namelen] = '\0';
        char *tier = &name[namelen + 1];
        if (allow) {
            logmsg(LOGMSG_USER, "Adding %s/%s as valid coordinator\n", name, tier);
            allow_coordinator(name, tier);
        } else {
            logmsg(LOGMSG_USER, "Removing %s/%s as valid coordinator\n", name, tier);
            forbid_coordinator(name, tier);
        }
        tok = segtok(line, lline, &st, &ltok);
    }
    create_allowed_coordinators_str();
}

char *process_forbid_coordinator(char *line, int lline)
{
    process_coordinator_directive(line, lline, 0);
    return allowed_coordinators_str;
}

char *process_allow_coordinator(char *line, int lline)
{
    process_coordinator_directive(line, lline, 1);
    return allowed_coordinators_str;
}

void disttxn_init_recover_prepared(void)
{
    thedb->bdb_env->dbenv->set_recover_prepared_callback(thedb->bdb_env->dbenv, recover_prepared_transaction);
}

/* Initialize coordinator and participant data structures */
void disttxn_init(void)
{
    Pthread_mutex_lock(&part_lk);
    participant_hash = hash_init_strptr(0);
    Pthread_mutex_unlock(&part_lk);

    Pthread_mutex_lock(&active_transactions_lk);
    active_transactions_hash = hash_init_strptr(0);
    Pthread_mutex_unlock(&active_transactions_lk);

    Pthread_mutex_lock(&sanc_lk);
    sanctioned_hash = hash_init_strptr(0);
    Pthread_mutex_unlock(&sanc_lk);

    Pthread_mutex_lock(&hndl_lk);
    handle_hash = hash_init_user((hashfunc_t *)hndl_hash, (cmpfunc_t *)hndl_cmp, 0, 0);
    Pthread_mutex_unlock(&hndl_lk);

    Pthread_mutex_lock(&allowed_coordinators_lk);
    allowed_coordinators_hash = hash_init_user((hashfunc_t *)coord_hash, (cmpfunc_t *)coord_cmp, 0, 0);
    Pthread_mutex_unlock(&allowed_coordinators_lk);
}
