#include <cdb2api.h>
#include <pthread.h>
#include <locks_wrap.h>
#include <physwrite.h>
#include <osqlcomm.h>
#include <osqlsession.h>
#include <osqlcheckboard.h>
#include <thread_malloc.h>
#include <bdb_api.h>
#include <tohex.h>
#include <errstat.h>

int gbl_physwrite_shared_handle = 0;
int gbl_physwrite_wait_commit = 1;
int gbl_physwrite_commit_timeout = 0;
int gbl_physwrite_long_write_threshold = 10;

enum {
    SHARED = 1,
    DISTINCT = 2
};

typedef struct session 
{
    int session_type;
    cdb2_hndl_tp *hndl;
    char *last_master;
} session_t;

static cdb2_hndl_tp *shared_hndl = NULL;

static char *dbname;
static char *dbtype;
static char *dbhost;
static int dbhostlen;

__thread session_t *sess;

static session_t *retrieve_session(void)
{
    if (sess == NULL) {
        sess = thread_malloc(sizeof(*sess));
        sess->session_type = (gbl_physwrite_shared_handle ? SHARED : DISTINCT);
        sess->last_master = NULL;
        sess->hndl = NULL;
    }
    return sess;
}

pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;

static void physwrite_enter(session_t *s)
{
    if (s->session_type == SHARED)
        Pthread_mutex_lock(&lk);
}

static void physwrite_exit(session_t *s)
{
    if (s->session_type == SHARED)
        Pthread_mutex_unlock(&lk);
}

static cdb2_hndl_tp *retrieve_handle(session_t *s)
{
    cdb2_hndl_tp **h = (s->session_type == SHARED ?  &shared_hndl : &s->hndl);
    int rc;
    if (!*h && (rc = cdb2_open(h, dbname, dbtype, CDB2_CONNECT_MASTER)) != 0) {
        logmsg(LOGMSG_ERROR, "Physwrite unable to retrieve handle for %s:%s, "
               "rc %d\n", dbname, dbtype, rc);
    }
    return *h;
}

static int signal_results(cdb2_hndl_tp *h, int type, uuid_t uuid,
        unsigned long long rqid, int *timeout)
{
    int64_t *rcode, *errval, *file, *offset;
    struct errstat errstat = {0};
    char *errstr;

    rcode = (int64_t *)cdb2_column_value(h, 0);
    errval = (int64_t *)cdb2_column_value(h, 1);
    errstr = cdb2_column_value(h, 2);
    file = cdb2_column_value(h, 3);
    offset = cdb2_column_value(h, 4);

    errstat.errval = *errval;
    if (errstr)
        strcpy(errstat.errstr, errstr);

#if 0
    int64_t *inserts, *updates, *deletes, *cupdates, *cdeletes;
    inserts = (int64_t *)cdb2_column_value(h, 5);
    updates = (int64_t *)cdb2_column_value(h, 6);
    deletes = (int64_t *)cdb2_column_value(h, 7);
    cupdates = (int64_t *)cdb2_column_value(h, 8);
    cdeletes = (int64_t *)cdb2_column_value(h, 9);
#endif

    if (gbl_physwrite_wait_commit && bdb_wait_for_lsn(thedb->bdb_env, *file,
                *offset, gbl_physwrite_commit_timeout))
        *timeout = 1;

    return osql_chkboard_sqlsession_rc(rqid, uuid, 0, NULL, &errstat);
}

static int dosend(session_t *s, int usertype, void *data, int datalen,
        uint32_t flags)
{
    int rc, type, timeout = 0, len;
    unsigned long long rqid;
    //char sql[128];
    char *sql = NULL, *blob = NULL;
    uuidstr_t us;
    uuid_t uuid;

    cdb2_hndl_tp *h = retrieve_handle(s);
    if (s->last_master && strcmp(s->last_master, cdb2_master(h))) {
        free(s->last_master);
        s->last_master = NULL;
        return OSQL_SEND_ERROR_WRONGMASTER;
    }

    type = osql_extract_type(usertype, data, datalen, &uuid, &rqid);
    logmsg(LOGMSG_INFO, "%s [%llu:%s] type %d usertype %d starting\n",
            __func__, rqid, comdb2uuidstr(uuid, us), type, usertype);

    blob = (char *)malloc((2 * datalen) + 1);
    util_tohex(blob, data, datalen);
    len = 100 + strlen(dbhost) + (2 * datalen);
    sql = (char *)malloc(len);

    snprintf(sql, len, "exec procedure "
            "sys.cmd.exec_socksql('%s', %d, x'%s', %d)", dbhost, usertype,
            blob, flags);

    logmsg(LOGMSG_INFO, "%s exec_socksql with usertype %d len %d\n",
            __func__, usertype, datalen);
    rc = cdb2_run_statement(h, sql);
    free(blob);
    free(sql);

    if (osql_comm_is_done(type, NULL, 0, 0, NULL, NULL) && rc == CDB2_OK) {
        if ((rc = cdb2_next_record(h)) == CDB2_OK)
            signal_results(h, type, uuid, rqid, &timeout);
    }

    if (rc != CDB2_OK && rc != CDB2_OK_DONE) {
        if (s->last_master)
            free(s->last_master);
        s->last_master = NULL;
        logmsg(LOGMSG_ERROR, "%s [%llu:%s] returning WRONGMASTER\n",
                __func__, rqid, comdb2uuidstr(uuid, us));
        return OSQL_SEND_ERROR_WRONGMASTER;
    }

    /* Remember last master */
    if (rc == 0 && s->last_master == NULL)
        s->last_master = strdup(cdb2_master(h));

    logmsg(LOGMSG_INFO, "%s [%llu:%s] type %d usertype %d OK\n",
            __func__, rqid, comdb2uuidstr(uuid, us), type, usertype);

    return (rc == CDB2_OK || rc == CDB2_OK_DONE) ? 0 : 1;
}

void physwrite_init(char *name, char *type, char *host)
{
    dbname = strdup(name);
    dbtype = strdup(type);
    dbhost = strdup(host);
    dbhostlen = strlen(dbhost);
}

int physwrite_route_packet(int usertype, void *data, int datalen, uint32_t flags)
{
    int rc;
    session_t *s = retrieve_session();
    physwrite_enter(s);
    rc = dosend(s, usertype, data, datalen, flags);
    physwrite_exit(s);
    return rc;
}

int physwrite_route_packet_tails(int usertype, void *data, int datalen,
        int ntails, void *tail, int tailen)
{
    void *dup;
    int rc;
    logmsg(LOGMSG_USER, "%s called with usertype %u\n", __func__, usertype);
    if (datalen + tailen > gbl_blob_sz_thresh_bytes)
        dup = comdb2_bmalloc(blobmem, datalen + tailen);
    else
        dup = malloc(datalen + tailen);
    memmove(dup, data, datalen);
    memmove(dup + datalen, tail, tailen);
    rc = physwrite_route_packet(usertype, dup, datalen + tailen, 1);
    free(dup);
    return rc;
}

int net_route_packet_flags(int usertype, void *data, int datalen, uint8_t flags);
__thread physwrite_results_t *physwrite_results;

int physwrite_exec(char *host, int usertype, void *data, int datalen,
        int *rcode, int *errval, char **errstr, int *inserts, int *updates, int *deletes,
        int *cupdates, int *cdeletes, int *commit_file, int *commit_offset,
        uint32_t flags)
{
    int cnt=0, tlen=0, rc;
    void *tails[1] = {0};
    physwrite_results_t results = {0};
    Pthread_mutex_init(&results.lk, NULL);
    Pthread_cond_init(&results.cd, NULL);
    physwrite_results = &results;

    if (flags) 
        net_osql_rpl_tail(NULL, NULL, 0, usertype, data, datalen,
                NULL, 0, PHYSWRITE);
    else
        net_route_packet_flags(usertype, data, datalen, PHYSWRITE);

    if (results.dispatched) {
        logmsg(LOGMSG_USER, "%s dispatched request on master\n", __func__);
        fflush(stdout); fflush(stderr);
        Pthread_mutex_lock(&results.lk);
        while(!results.done) {
            struct timespec now;
            if (cnt++ > gbl_physwrite_long_write_threshold) {
                logmsg(LOGMSG_WARN, "%s long write outstanding for %d seconds.\n",
                        __func__, cnt);
            }
            clock_gettime(CLOCK_REALTIME, &now);
            now.tv_sec += 1;
            pthread_cond_timedwait(&results.cd, &results.lk, &now);
        } 
        Pthread_mutex_unlock(&results.lk);
        *rcode = results.rcode;
        *errval = results.errval;
        *errstr = results.errstr;
        logmsg(LOGMSG_USER, "%s request returned %d, errval %d, errstr='%s'\n",
                __func__, *rcode, *errval, *errstr ? *errstr : "");
        fflush(stdout); fflush(stderr);
#if 0
        *inserts = results.inserts;
        *updates = results.updates;
        *deletes = results.deletes;
        *cupdates = results.cupdates;
        *cdeletes = results.cdeletes;
#endif
        *commit_file = results.commit_file;
        *commit_offset = results.commit_offset;
    }
    physwrite_results = NULL;
    Pthread_mutex_destroy(&results.lk);
    Pthread_cond_destroy(&results.cd);
    return results.dispatched;
}
