/*
    Copyright 2022, Bloomberg Finance L.P.

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

#include <db.h>
#include <bdb_int.h>

#include "dbinc/shqueue.h"
#include "dbinc/mutex.h"
#include "dbinc/rep.h"
#include "db_int.h"
#include "dbinc/log.h"
#include "dbinc/db_swap.h"

#include <cdb2api.h>
#include <unistd.h>

extern bdb_state_type *gbl_bdb_state;

static pthread_t catchup_tid;
struct catchup {
    DB_LSN start_lsn;
    u_int32_t gen;
    char *master;
    DB_ENV *dbenv;
};


// TODO: ensure only of these is in flight at a time.  It should have no harm to have more -- they'll operate with
//       different generation numbers, but only one can be doing useful work.
static void* catchup_thread(void *p) {
    struct catchup *c = (struct catchup *) p;
    cdb2_hndl_tp *db;
    DB_LSN last_lsn;

    // TODO: destination - build from list of siblings
    int rc = cdb2_open(&db, gbl_bdb_state->name, c->master, CDB2_DIRECT_CPU);
    if (rc) {
        logmsg(LOGMSG_ERROR, "fail: %s %d rc %d %s\n", __func__, __LINE__, rc, cdb2_errstr(db));
        goto done;
    }
    rc = cdb2_bind_param(db, "lognum", CDB2_INTEGER, &c->start_lsn.file, sizeof(c->start_lsn.file));
    if (rc) {
        logmsg(LOGMSG_ERROR, "fail: %s %d rc %d %s\n", __func__, __LINE__, rc, cdb2_errstr(db));
        goto done;
    }
    rc = cdb2_run_statement(db, "select lognum, maxlognum, logfile from comdb2_logfiles where lognum >= @lognum");
    if (rc) {
        logmsg(LOGMSG_ERROR, "fail: %s %d rc %d %s\n", __func__, __LINE__, rc, cdb2_errstr(db));
        goto done;
    }

    int first = 1;
    DB_ENV *dbenv = gbl_bdb_state->dbenv;
    while ((rc = cdb2_next_record(db)) == CDB2_OK) {
        int lognum = (int)*(int64_t*) cdb2_column_value(db, 0);
        void *buf = cdb2_column_value(db, 2);
        int size = cdb2_column_size(db, 2);

        if (first) {
            DB_LSN tmplsn = { .file = lognum, .offset = 0 };
            DB_LOGC *logc;
            // If this isn't the first log file, we need a REP_NEWFILE for the last LSN in the previous file.  We must have at least the first LSN in the current file
            // so get that, and walk back one.
            if (lognum != 1) {
                rc = dbenv->log_cursor(dbenv, &logc, 0);
                if (rc) {
                    fprintf(stderr, "log_cursor get rc %d\n", rc);
                    abort();
                }
                DBT tmpdbt = { .flags = DB_DBT_REALLOC };
                rc = logc->get(logc, &tmplsn, &tmpdbt, DB_SET);
                if (rc) {
                    fprintf(stderr, "logc get rc %d\n", rc);
                    abort();
                }
                printf("logc get got %u:%u\n", tmplsn.file, tmplsn.offset);
                rc = logc->get(logc, &tmplsn, &tmpdbt, DB_PREV);
                if (rc) {
                    fprintf(stderr, "logc prev rc %d\n", rc);
                    abort();
                }
                free(tmpdbt.data);
                rc = logc->close(logc, 0);
                if (rc) {
                    fprintf(stderr, "close rc %d\n", rc);
                    abort();
                }
            }
            last_lsn = tmplsn;

            first = 0;
        }

        rc = __apply_log_file(c->dbenv, c->gen, c->master, lognum, (uint8_t*) buf, size, &last_lsn);
        logmsg(LOGMSG_WARN, "apply log file %u gen %d rc %d last lsn %u:%u\n", lognum, c->gen, rc, last_lsn.file, last_lsn.offset);
        if (rc)
            goto done;
    }
    if (rc != CDB2_OK_DONE) {
        logmsg(LOGMSG_ERROR, "fail: %s %d rc %d %s\n", __func__, __LINE__, rc, cdb2_errstr(db));
        goto done;
    }
    // we did some work, but the real tail of the log may not be flushed on the node we read it from
    // so now fall through to REP_ALL_REQ with the last LSN we did process
    c->start_lsn = last_lsn;
    printf("ack gen %d\n", c->gen);
    do_ack(gbl_bdb_state, last_lsn, c->gen);

done:
    logmsg(LOGMSG_INFO, "sending to %s REP_ALL_REQ for %u:%u\n", c->master, c->start_lsn.file, c->start_lsn.offset);
    __rep_send_message(gbl_bdb_state->dbenv, c->master, REP_ALL_REQ, &c->start_lsn, NULL, DB_REP_NODROP, NULL);
    return NULL;
}

int is_commit(u_int32_t rectype);

void bdb_try_catchup(DB_ENV *dbenv, DB_LSN *lsnp, char *master) {
    char *master_out;
    u_int32_t gen, egen;
    bdb_get_rep_master(gbl_bdb_state, &master_out, &gen, &egen);

    struct catchup *c;
    c = malloc(sizeof(struct catchup));
    // Our caller is __rep_verify_match.  Latch the current gen - we'll need to pass that down
    // to berkeley.  Also latch the current log file, and the last log - we'll start there.  If anything
    // ever fails, we fall back to sending a REP_ALL_REQ.
    c->gen = gen;
    c->start_lsn = *lsnp;
    c->master = master;
    c->dbenv = dbenv;

    int rc = pthread_create(&catchup_tid, NULL, catchup_thread, c);
    if (rc) {
        free(c);
        __rep_send_message(dbenv, c->master, REP_ALL_REQ, lsnp, NULL, DB_REP_NODROP, NULL);
    }
}
