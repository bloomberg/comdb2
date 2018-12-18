#include <physwrites.h>
#include <cdb2api.h>
#include <pthread.h>
#include <osqlcomm.h>
#include <thread_malloc.h>

int gbl_phywrite_shared_handle = 0;
int gbl_physwrite_wait_commit = 1;
pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;

enum {
    SHARED = 1,
    DISTINCT = 2
};

typedef session 
{
    int session_type;
    cdb2_hndl_tp *hndl;
    char *last_master;
} session_t;

static cdb2_hndl_tp *shared_hndl = NULL;
__thread session_t *sess;

static char *dbname;
static char *dbtype;
static char *dbhost;

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
        logmsg(logmsg_error, "Physwrite unable to retrieve handle for %s:%s, "
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
    cdb2_bind_param(h, "host", CDB2_CSTRING, strlen(dbhost));
    cdb2_bind_param(h, "usertype", CDB2_INTEGER, sizeof(usertype));
    cdb2_bind_param(h, "data", CDB2_BLOB, datalen);
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

int physwrite_route_packet_tails(int usertype, void data, int datalen,
        int ntails, void *tail, int tailen)
{
    void *dup;
    int rc;
    if (datalen + tailen > gbl_blob_sz_thresh_bytes)
        dup = comdb2_bmalloc(blobmem, datalen + tailen);
    else
        dup = malloc(datalen + taillen);

    memmove(dup, data, datalen);
    memmove(dup + datalen, tail, tailen);
    rc = physwrite_route_packet(usertype, dup, datalen + tailen);
    free(dup);
    return rc;
}

int physwrite_exec(char *host, int usertype, void *data, int datalen,
        int *errval, char **errstr, int *inserts, int *updates,
        int *deletes, int *cupdates, int *cdeletes, DB_LSN *commit_lsn)
{
}
