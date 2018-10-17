#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include "comdb2.h"
#include "comdb2systblInt.h"
#include "ezsystables.h"
#include "cdb2api.h"

#include "comdb2uuid.h"
#include "sql.h"
#include <sqloffload.h>
#include "osqlcheckboard.h"
#include "osqlsession.h"
#include "osqlrepository.h"

static char *osqltype = "OSQL";
static char *bplogtype = "BPLOG";

typedef struct systable_osqlsession {
    char *type;
    char *origin;
    char *argv0;
    char *stack;
    char *cnonce;
    char *id;
    int64_t nops;
    int64_t start_time;
    int64_t commit_time;
    int64_t nretries;
} systable_osqlsession_t;

typedef struct getosqlsessions {
    int count;
    int alloc;
    systable_osqlsession_t *records;
} getosqlsessions_t;

static int collect_osql_session(void *obj, void *arg)
{
    osql_sqlthr_t *rq = obj;
    struct sqlclntstate *clnt = rq->clnt;
    getosqlsessions_t *osqls = arg;
    osqls->count++;
    if (osqls->count >= osqls->alloc) {
        if (osqls->alloc == 0)
            osqls->alloc = 128;
        else
            osqls->alloc = osqls->alloc * 2;
        osqls->records = realloc(osqls->records,
                                 osqls->alloc * sizeof(systable_osqlsession_t));
    }

    uuidstr_t us;
    snap_uid_t snap;

    systable_osqlsession_t *o = &osqls->records[osqls->count - 1];
    memset(o, 0, sizeof(*o));

    o->type = osqltype;
    o->origin = clnt->origin ? strdup(clnt->origin) : NULL;
    o->argv0 = clnt->argv0 ? strdup(clnt->argv0) : NULL;
    o->stack = clnt->stack ? strdup(clnt->stack) : NULL;
    if (get_cnonce(clnt, &snap) == 0) {
        o->cnonce = malloc(snap.keylen + 1);
        memcpy(o->cnonce, snap.key, snap.keylen);
        o->cnonce[snap.keylen] = '\0';
    }
    if (rq->rqid == 1) {
        comdb2uuidstr(rq->uuid, us);
        o->id = strdup(us);
    } else {
        o->id = malloc(20);
        snprintf(o->id, 20, "%llx", rq->rqid);
    }
    o->nops = clnt->osql.sentops;
    o->start_time = rq->register_time;
    o->commit_time = clnt->osql.timings.commit_start;
    o->nretries = clnt->verify_retries;
    return 0;
}

static int collect_bplog_session(void *obj, void *arg)
{
    osql_sess_t *sess = obj;
    struct ireq *iq = sess->iqcopy;

    getosqlsessions_t *osqls = arg;
    osqls->count++;
    if (osqls->count >= osqls->alloc) {
        if (osqls->alloc == 0)
            osqls->alloc = 128;
        else
            osqls->alloc = osqls->alloc * 2;
        osqls->records = realloc(osqls->records,
                                 osqls->alloc * sizeof(systable_osqlsession_t));
    }

    uuidstr_t us;

    systable_osqlsession_t *o = &osqls->records[osqls->count - 1];
    memset(o, 0, sizeof(*o));

    o->type = bplogtype;
    o->origin = sess->offhost ? strdup(sess->offhost) : NULL;
    o->stack = iq && iq->where ? strdup(iq->where) : NULL;
    if (iq && iq->have_snap_info) {
        o->cnonce = malloc(iq->snap_info.keylen + 1);
        memcpy(o->cnonce, iq->snap_info.key, iq->snap_info.keylen);
        o->cnonce[iq->snap_info.keylen] = '\0';
    }
    if (sess->rqid == 1) {
        comdb2uuidstr(sess->uuid, us);
        o->id = strdup(us);
    } else {
        o->id = malloc(20);
        snprintf(o->id, 20, "%llx", sess->rqid);
    }
    o->nops = sess->seq;
    o->start_time = sess->initstart * 1000;
    o->commit_time = sess->end * 1000;
    o->nretries = sess->retries;
    return 0;
}

static int get_osqls(void **data, int *records)
{
    getosqlsessions_t osqls = {0};
    osql_checkboard_for_each(&osqls, collect_osql_session);
    osql_repository_for_each(&osqls, collect_bplog_session);
    *data = osqls.records;
    *records = osqls.count;
    return 0;
}

static void free_osqls(void *p, int n)
{
    systable_osqlsession_t *t = (systable_osqlsession_t *)p;
    for (int i = 0; i < n; i++) {
        if (t[i].origin)
            free(t[i].origin);
        if (t[i].argv0)
            free(t[i].argv0);
        if (t[i].stack)
            free(t[i].stack);
        if (t[i].cnonce)
            free(t[i].cnonce);
        if (t[i].id)
            free(t[i].id);
    }
    free(p);
}

int systblActiveOsqlsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_active_osqls", get_osqls, free_osqls, sizeof(systable_osqlsession_t),
        CDB2_CSTRING, "type", -1, offsetof(systable_osqlsession_t, type),
        CDB2_CSTRING, "origin", -1, offsetof(systable_osqlsession_t, origin),
        CDB2_CSTRING, "argv0", -1, offsetof(systable_osqlsession_t, argv0),
        CDB2_CSTRING, "stack", -1, offsetof(systable_osqlsession_t, stack),
        CDB2_CSTRING, "cnonce", -1, offsetof(systable_osqlsession_t, cnonce),
        CDB2_CSTRING, "request_id", -1, offsetof(systable_osqlsession_t, id),
        CDB2_INTEGER, "nops", -1, offsetof(systable_osqlsession_t, nops),
        CDB2_INTEGER, "start_time", -1, offsetof(systable_osqlsession_t, start_time),
        CDB2_INTEGER, "commit_time", -1, offsetof(systable_osqlsession_t, commit_time),
        CDB2_INTEGER, "nretries", -1, offsetof(systable_osqlsession_t, nretries),
        SYSTABLE_END_OF_FIELDS);
}
