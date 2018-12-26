#include <physwrite.h>
#include <cdb2api.h>
#include <pthread.h>
#include <osqlcomm.h>
#include <osqlsession.h>
#include <thread_malloc.h>

int gbl_physwrite_shared_handle = 0;
int gbl_physwrite_wait_commit = 1;
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

static int dosend(session_t *s, int usertype, void *data, int datalen)
{
    int rc;
    cdb2_hndl_tp *h = retrieve_handle(s);
    if (s->last_master && strcmp(s->last_master, cdb2_master(h))) {
        free(s->last_master);
        s->last_master = NULL;
        return OSQL_SEND_ERROR_WRONGMASTER;
    }
    cdb2_bind_param(h, "host", CDB2_CSTRING, dbhost, strlen(dbhost));
    cdb2_bind_param(h, "usertype", CDB2_INTEGER, &usertype, sizeof(usertype));
    cdb2_bind_param(h, "data", CDB2_BLOB, data, datalen);
    rc = cdb2_run_statement(h,
            "exec procedure sys.cmd.exec_socksql(@host, @usertype, @data)");
    if (rc == 0 && s->last_master == NULL)
        s->last_master = strdup(cdb2_master(h));
    return rc;
}

void physwrite_init(char *name, char *type, char *host)
{
    dbname = strdup(name);
    dbtype = strdup(type);
    dbhost = strdup(host);
}

int physwrite_route_packet(int usertype, void *data, int datalen)
{
    int rc;
    session_t *s = retrieve_session();
    physwrite_enter(s);
    rc = dosend(s, usertype, data, datalen);
    physwrite_exit(s);
    return rc;
}

int physwrite_route_packet_tails(int usertype, void *data, int datalen,
        int ntails, void *tail, int tailen)
{
    void *dup;
    int rc;
    if (datalen + tailen > gbl_blob_sz_thresh_bytes)
        dup = comdb2_bmalloc(blobmem, datalen + tailen);
    else
        dup = malloc(datalen + tailen);
    memmove(dup, data, datalen);
    memmove(dup + datalen, tail, tailen);
    rc = physwrite_route_packet(usertype, dup, datalen + tailen);
    free(dup);
    return rc;
}

__thread physwrite_results_t *physwrite_results;

int net_route_packet_flags(int usertype, void *data, int datalen, uint8_t flags);

int physwrite_exec(char *host, int usertype, void *data, int datalen,
        int *rcode, int *errval, char **errstr, int *inserts, int *updates, int *deletes,
        int *cupdates, int *cdeletes, int *commit_file, int *commit_offset)
{
    int rc, cnt=0;
    physwrite_results_t results = {0};
    Pthread_mutex_init(&results.lk, NULL);
    Pthread_cond_init(&results.cd, NULL);
    physwrite_results = &results;
    rc = net_route_packet_flags(usertype, data, datalen, PHYSWRITE);
    if (results.dispatched) {
        Pthread_mutex_lock(&results.lk);
        while(!results.done) {
            struct timespec now;
            if (cnt++ > gbl_physwrite_long_write_threshold) {
                logmsg(LOGMSG_WARN, "%s long write outstanding for %d seconds.\n",
                        __func__, cnt);
            }
            rc = clock_gettime(CLOCK_REALTIME, &now);
            now.tv_sec += 1;
            pthread_cond_timedwait(&results.cd, &results.lk, &now);
        } 
        Pthread_mutex_unlock(&results.lk);
        *rcode = results.rcode;
        *errval = results.errval;
        *errstr = results.errstr;
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
