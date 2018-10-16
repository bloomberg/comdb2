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

typedef struct systable_osqlsession {
    char *id;
    int64_t start_time;
    int64_t commit_time;
} systable_osqlsession_t;

typedef struct getosqlsessions {
    int count;
    int alloc;
    systable_osqlsession_t *records;
} getosqlsessions_t;

static int collect_osql_session(void *obj, void *arg)
{
    osql_sqlthr_t *rq = obj;
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
    if (rq->rqid == 1) {
        comdb2uuidstr(rq->uuid, us);
        o->id = strdup(us);
    } else {
        o->id = malloc(20);
        sprintf(o->id, "%x", rq->rqid);
    }
    o->start_time = rq->register_time;
    o->commit_time = rq->clnt->osql.timings.commit_start;
    return 0;
}

static int get_osqls(void **data, int *records)
{
    getosqlsessions_t osqls = {0};
    osql_checkboard_for_each(&osqls, collect_osql_session);
    *data = osqls.records;
    *records = osqls.count;
    return 0;
}

static void free_osqls(void *p, int n)
{
    systable_osqlsession_t *t = (systable_osqlsession_t *)p;
    for (int i = 0; i < n; i++) {
        if (t[i].id)
            free(t[i].id);
    }
    free(p);
}

int systblActiveOsqlsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_active_osqls", get_osqls, free_osqls,
        sizeof(systable_osqlsession_t), CDB2_CSTRING, "request_id", -1,
        offsetof(systable_osqlsession_t, id), CDB2_INTEGER, "start_time", -1,
        offsetof(systable_osqlsession_t, start_time), CDB2_CSTRING,
        "commit_time", -1, offsetof(systable_osqlsession_t, commit_time),
        SYSTABLE_END_OF_FIELDS);
}
