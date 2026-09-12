/*
   Copyright 2020 Bloomberg Finance L.P.

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
#include "tohex.h"

static char *osqltype = "OSQL";
static char *bplogtype = "BPLOG";

/* NULL for the all-zero sentinel, so an unknown fingerprint reads as SQL NULL. */
static char *fingerprint_hex(const unsigned char *fp)
{
    char hex[FINGERPRINTSZ * 2 + 1];
    int i;

    for (i = 0; i < FINGERPRINTSZ; i++) {
        if (fp[i] != 0)
            break;
    }
    if (i == FINGERPRINTSZ)
        return NULL;

    util_tohex(hex, (const char *)fp, FINGERPRINTSZ);
    return strdup(hex);
}

typedef struct systable_osqlsession {
    char *type;
    char *origin;
    char *argv0;
    char *where;
    char *cnonce;
    char *id;
    int64_t nops;
    int64_t start_time;
    int64_t commit_time;
    int64_t nretries;
    /* pid and fingerprint are NULL unless the client sent them; client_id is
     * always known and joins comdb2_locks.client_id. */
    int64_t pid;
    int pid_isnull;
    char *fingerprint;
    int64_t client_id;
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
    o->where = clnt->stack ? strdup(clnt->stack) : NULL;
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
    o->nops = clnt->osql.replicant_numops;
    o->start_time = rq->register_time;
    o->commit_time = clnt->osql.timings.commit_start;
    o->nretries = clnt->verify_retries;
    /* This side has the clnt, so these need nothing off the wire. */
    if (clnt->last_pid)
        o->pid = clnt->last_pid;
    else
        o->pid_isnull = 1;
    o->fingerprint = fingerprint_hex(clnt->work.aFingerprint);
    o->client_id = osql_sess_client_id(rq->rqid, rq->uuid);
    return 0;
}

static int collect_bplog_session(void *obj, void *arg)
{
    osql_sess_t *sess = obj;
    struct ireq *iq = sess->iq;

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
    o->origin = sess->target.host? strdup(sess->target.host) : NULL;
    o->where = iq && iq->where ? strdup(iq->where) : NULL;
    if (iq && IQ_HAS_SNAPINFO_KEY(iq)) {
        o->cnonce = malloc(IQ_SNAPINFO(iq)->keylen + 1);
        memcpy(o->cnonce, IQ_SNAPINFO(iq)->key, IQ_SNAPINFO(iq)->keylen);
        o->cnonce[IQ_SNAPINFO(iq)->keylen] = '\0';
    }
    if (sess->rqid == 1) {
        comdb2uuidstr(sess->uuid, us);
        o->id = strdup(us);
    } else {
        o->id = malloc(20);
        snprintf(o->id, 20, "%llx", sess->rqid);
    }
    o->nops = sess->nops;
    o->start_time = U2M(sess->sess_startus);
    o->commit_time = U2M(sess->sess_endus);
    o->nretries = iq?iq->retries:0;
    /* Only here if the client sent them; the host we already had, above. */
    o->argv0 = sess->clnt_taskname ? strdup(sess->clnt_taskname) : NULL;
    if (sess->clnt_pid)
        o->pid = sess->clnt_pid;
    else
        o->pid_isnull = 1;
    o->fingerprint = sess->have_fingerprint ? fingerprint_hex(sess->fingerprint) : NULL;
    o->client_id = sess->client_id;
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
        if (t[i].where)
            free(t[i].where);
        if (t[i].cnonce)
            free(t[i].cnonce);
        if (t[i].id)
            free(t[i].id);
        if (t[i].fingerprint)
            free(t[i].fingerprint);
    }
    free(p);
}

sqlite3_module systblActiveOsqlsModule = {
    .access_flag = CDB2_ALLOW_USER | CDB2_STRICT,
};

int systblActiveOsqlsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_active_osqls", &systblActiveOsqlsModule,
        get_osqls, free_osqls, sizeof(systable_osqlsession_t),
        CDB2_CSTRING, "type", -1, offsetof(systable_osqlsession_t, type),
        CDB2_CSTRING, "origin", -1, offsetof(systable_osqlsession_t, origin),
        CDB2_CSTRING, "argv0", -1, offsetof(systable_osqlsession_t, argv0),
        CDB2_CSTRING, "where", -1, offsetof(systable_osqlsession_t, where),
        CDB2_CSTRING, "cnonce", -1, offsetof(systable_osqlsession_t, cnonce),
        CDB2_CSTRING, "request_id", -1, offsetof(systable_osqlsession_t, id),
        CDB2_INTEGER, "nops", -1, offsetof(systable_osqlsession_t, nops),
        CDB2_INTEGER, "start_time", -1, offsetof(systable_osqlsession_t, start_time),
        CDB2_INTEGER, "commit_time", -1, offsetof(systable_osqlsession_t, commit_time),
        CDB2_INTEGER, "nretries", -1, offsetof(systable_osqlsession_t, nretries),
        CDB2_INTEGER, "pid", offsetof(systable_osqlsession_t, pid_isnull), offsetof(systable_osqlsession_t, pid),
        CDB2_CSTRING, "fingerprint", -1, offsetof(systable_osqlsession_t, fingerprint),
        CDB2_INTEGER, "client_id", -1, offsetof(systable_osqlsession_t, client_id),
        SYSTABLE_END_OF_FIELDS);
}
