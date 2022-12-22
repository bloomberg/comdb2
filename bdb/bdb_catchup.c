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
    int gen;
    char *master;
};

static int apply_log_file(struct catchup *c, uint32_t lognum, uint8_t *log, uint32_t size, DB_LSN *last_lsn);

// TODO: insure only of these is in flight at a time.  It should have no harm to have more -- they'll operated with
//       different generation numbers, but only one can be doing useful work.
static void* catchup_thread(void *p) {
    struct catchup *c = (struct catchup *) p;
    cdb2_hndl_tp *db;
    DB_LSN last_lsn;

    // race?  what race?
    sleep(3);

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

    while ((rc = cdb2_next_record(db)) == CDB2_OK) {
        int lognum = (int)*(int64_t*) cdb2_column_value(db, 0);
        void *buf = cdb2_column_value(db, 2);
        int size = cdb2_column_size(db, 2);

        rc = apply_log_file(c, lognum, (uint8_t*) buf, size, &last_lsn);
        logmsg(LOGMSG_INFO, "apply log file %u rc %d last lsn %u:%u\n", lognum, rc, last_lsn.file, last_lsn.offset);
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

done:
    logmsg(LOGMSG_INFO, "sending to %s REP_ALL_REQ for %u:%u\n", c->master, c->start_lsn.file, c->start_lsn.offset);
    __rep_send_message(gbl_bdb_state->dbenv, c->master, REP_ALL_REQ, &c->start_lsn, NULL, DB_REP_NODROP, NULL);
    return NULL;
}

static int apply_log_file(struct catchup *c, uint32_t lognum, uint8_t *log, uint32_t size, DB_LSN *last_lsn) {
    uint32_t hdrsize;
    DB_ENV *dbenv = gbl_bdb_state->dbenv;
    HDR *hdrp;
    HDR hdr;
    int is_hmac;
    uint32_t off = 0;

    if (CRYPTO_ON(dbenv)) {
        hdrsize = HDR_CRYPTO_SZ;
        is_hmac = 1;
    } else {
		hdrsize = HDR_NORMAL_SZ;
		is_hmac = 0;
	}
    REP_CONTROL rp;

    DBT control = { .data = &rp };
    DBT rec = {0};
    do {
        rp.lsn.file = lognum;
        rp.lsn.offset = off;
        rp.gen = c->gen;
        rp.flags = 0;
        rp.gen = c->gen;
        rp.rep_version = DB_REPVERSION;
        rp.log_version = DB_LOGVERSION;
        rp.rectype = REP_LOG;
        __rep_control_swap(&rp);

        hdrp = (HDR *) ((uint8_t*) log + off);
        memcpy(&hdr, hdrp, hdrsize);

        // swap our copy so we can figure out the size
        if (LOG_SWAPPED())
            __log_hdrswap(&hdr, is_hmac);

        control.size = sizeof(rp);
        rec.data = hdrp;
        rec.size = hdr.len;

        uint32_t commit_gen;
        DB_LSN ret_lsn;
        int rc = dbenv->rep_process_message(gbl_bdb_state->dbenv, &control, &rec, &c->master, &ret_lsn, &commit_gen, 0);
        if (rc) {
            printf("lsn %u:%u %d\n", lognum, off, rc);
            return rc;
        }
        last_lsn->file = lognum;
        last_lsn->offset = off;

        off += hdr.len;
    } while(off < size);

    return 0;
}

void bdb_try_catchup(DB_ENV *dbenv, DB_LSN *lsnp, char *master) {
    struct catchup *c;
    c = malloc(sizeof(struct catchup));
    // Our caller is __rep_verify_match.  Latch the current gen - we'll need to pass that down
    // to berkeley.  Also latch the current log file, and the last log - we'll start there.  If anything
    // ever fails, we fall back to sending a REP_ALL_REQ.
    DB_REP *db_rep;
	REP *rep;

	db_rep = dbenv->rep_handle;
	rep = db_rep->region;

    c->start_lsn = *lsnp;
    c->gen = rep->gen;
    c->master = master;

    int rc = pthread_create(&catchup_tid, NULL, catchup_thread, c);
    if (rc) {
        free(c);
        __rep_send_message(dbenv, c->master, REP_ALL_REQ, lsnp, NULL, DB_REP_NODROP, NULL);
    }
}