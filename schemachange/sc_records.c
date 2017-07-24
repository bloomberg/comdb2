/*
   Copyright 2015 Bloomberg Finance L.P.

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

#include <unistd.h>
#include <stdbool.h>
#include <poll.h>

#include <bdb_fetch.h>

#include "schemachange.h"
#include "sc_records.h"
#include "sc_global.h"
#include "sc_schema.h"
#include "comdb2_atomic.h"
#include "logmsg.h"

extern int gbl_partial_indexes;

// Increase max threads to do SC -- called when no contention is detected
// A simple atomic add sufices here since this function is called from one
// place at any given time, currently from lkcounter_check() once per sec
static inline void increase_max_threads(int *maxthreads, int sc_threads)
{
    if (*maxthreads >= sc_threads) return;
    ATOMIC_ADD((*maxthreads), 1);
}

// Decrease max threads to do SC -- called when add_record gets an abort
// Used to backoff SC by having fewer threads running, decreasing contention
// We use atomic add here, since it may be called from multiple threads at once
// We also make certain that maxthreads does not go less than 1
static inline void decrease_max_threads(int *maxthreads)
{
    if (*maxthreads <= 1) return;
    /* ADDING -1 */
    if (ATOMIC_ADD((*maxthreads), -1) < 1) XCHANGE((*maxthreads), 1);
}

// increment number of rebuild threads in use
// if we are at capacity, then return 1 for failure
// if we were successful we return 0
static inline int use_rebuild_thr(int *thrcount, int *maxthreads)
{
    if (*thrcount >= *maxthreads) return 1;
    ATOMIC_ADD((*thrcount), 1);
    return 0;
}

// decrement number of rebuild threads in use
static inline void release_rebuild_thr(int *thrcount)
{
    assert(*thrcount >= 1);
    /* ADDING -1 */
    ATOMIC_ADD((*thrcount), -1);
}

static inline int tbl_had_writes(struct convert_record_data *data)
{
    unsigned oldcount = data->write_count;
    data->write_count = data->from->write_count[RECORD_WRITE_INS] +
                        data->from->write_count[RECORD_WRITE_UPD] +
                        data->from->write_count[RECORD_WRITE_DEL];
    return (data->write_count - oldcount) != 0;
}

/* prints global stats if not printed in the last sc_report_freq,
 * returns 1 if successful
 */
static inline int print_global_sc_stat(struct convert_record_data *data,
                                       int now, int sc_report_freq)
{
    static int total_lasttime = 0; /* used for global stats */
    int copy_total_lasttime = total_lasttime;

    /* Do work without locking */
    if (now < copy_total_lasttime + sc_report_freq) return 0;

    /* If time is up to print, atomically set total_lastime
     * if this thread successful in setting, it can continue
     * to print. If it failed, another thread is doing that work.
     */

    bool res = CAS(total_lasttime, copy_total_lasttime, now);
    if (!res) return 0;

    /* number of adds after schema cursor (by definition, all adds)
     * number of updates before cursor
     * number of deletes before cursor
     * number of genids added since sc began (adds + updates)
     */
    if (data->live)
        sc_printf(data->s, ">> adds %u upds %d dels %u extra genids "
                           "%u\n",
                  gbl_sc_adds, gbl_sc_updates, gbl_sc_deletes,
                  gbl_sc_adds + gbl_sc_updates);

    /* totals across all threads */
    if (data->scanmode != SCAN_PARALLEL) return 1;

    long long total_nrecs_diff = gbl_sc_nrecs - gbl_sc_prev_nrecs;
    gbl_sc_prev_nrecs = gbl_sc_nrecs;
    sc_printf(data->s, "progress TOTAL %lld +%lld actual "
                       "progress total %lld rate %lld r/s\n",
              gbl_sc_nrecs, total_nrecs_diff,
              gbl_sc_nrecs - (gbl_sc_adds + gbl_sc_updates),
              total_nrecs_diff / sc_report_freq);
    return 1;
}

static inline void lkcounter_check(struct convert_record_data *data, int now)
{
    int copy_lasttime = data->cmembers->lkcountercheck_lasttime;
    int lkcounter_freq = bdb_attr_get(data->from->dbenv->bdb_attr,
                                      BDB_ATTR_SC_CHECK_LOCKWAITS_SEC);
    /* Do work without locking */
    if (now < copy_lasttime + lkcounter_freq) return;

    /* If time is up to do work, atomically set total_lastime
     * if this thread successful in setting, it can continue
     * to adjust num threads. If it failed, another thread is doing that work.
     */
    bool res = CAS(data->cmembers->lkcountercheck_lasttime, copy_lasttime, now);
    if (!res) return;

    /* check lock waits -- there is no way to differentiate lock waits because
     * of
     * writes, with the exception that if there were writes in the last n
     * seconds
     * we may have been slowing them down.
     */

    int64_t ndeadlocks = 0, nlockwaits = 0;
    bdb_get_lock_counters(thedb->bdb_env, &ndeadlocks, &nlockwaits);

    int64_t diff_deadlocks = ndeadlocks - data->cmembers->ndeadlocks;
    int64_t diff_lockwaits = nlockwaits - data->cmembers->nlockwaits;

    data->cmembers->ndeadlocks = ndeadlocks;
    data->cmembers->nlockwaits = nlockwaits;
    logmsg(
        LOGMSG_INFO,
        "%s: diff_deadlocks=%lld, diff_lockwaits=%lld, maxthr=%d, currthr=%d\n",
        __func__, diff_deadlocks, diff_lockwaits, data->cmembers->maxthreads,
        data->cmembers->thrcount);
    increase_max_threads(
        &data->cmembers->maxthreads,
        bdb_attr_get(data->from->dbenv->bdb_attr, BDB_ATTR_SC_USE_NUM_THREADS));
}

void live_sc_enter_exclusive_all(bdb_state_type *bdb_state, tran_type *trans)
{
    unsigned stripe;
    for (stripe = 0; stripe < gbl_dtastripe; ++stripe) {
        bdb_lock_stripe_write(bdb_state, stripe, trans);
    }
    return;
}

/* If the schema is resuming it sets sc_genids to be the last genid for each
 * stripe.
 * If the schema change is not resuming it sets them all to zero
 * If success it returns 0, if failure it returns <0 */
int init_sc_genids(struct dbtable *db, struct schema_change_type *s)
{
    void *rec;
    int orglen, bdberr, stripe;
    unsigned long long *sc_genids;

    if (db->sc_genids == NULL) {
        db->sc_genids = malloc(sizeof(unsigned long long) * MAXDTASTRIPE);
        if (db->sc_genids == NULL) {
            logmsg(LOGMSG_ERROR,
                   "init_sc_genids: failed to allocate sc_genids\n");
            return -1;
        }
    }

    sc_genids = db->sc_genids;

    /* if we aren't resuming simply zero the genids */
    if (!s->resume) {
        /* if we may have to resume this schema change, clear the progress in
         * llmeta */
        if (bdb_clear_high_genid(NULL /*input_trans*/, db->dbname,
                                 db->dtastripe, &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "init_sc_genids: failed to clear high "
                                 "genids\n");
            return -1;
        }

        bzero(sc_genids, sizeof(unsigned long long) * MAXDTASTRIPE);
        return 0;
    }

    /* prepare for the largest possible data */
    orglen = MAXLRL;
    rec = malloc(orglen);

    /* get max genid for each stripe */
    for (stripe = 0; stripe < db->dtastripe; ++stripe) {
        int rc;
        uint8_t ver;
        int dtalen = orglen;

        /* get this stripe's newest genid and store it in sc_genids,
         * if we have been rebuilding the data files we can grab the genids
         * straight from there, otherwise we look in the llmeta table */
        if (is_dta_being_rebuilt(db->plan))
            rc = bdb_find_newest_genid(db->handle, NULL, stripe, rec, &dtalen,
                                       dtalen, &sc_genids[stripe], &ver,
                                       &bdberr);
        else
            rc = bdb_get_high_genid(db->dbname, stripe, &sc_genids[stripe],
                                    &bdberr);
        if (rc < 0 || bdberr != BDBERR_NOERROR) {
            sc_errf(s, "init_sc_genids: failed to find newest genid for "
                       "stripe: %d\n",
                    stripe);
            free(rec);
            return -1;
        }
        sc_printf(s, "resuming stripe %2d from 0x%016llx\n", stripe,
                  sc_genids[stripe]);
    }

    free(rec);
    return 0;
}

// this is only good for converting old schema to new schema full record
// because we only have one map from old schema to new schema
//(ie no index mapping--that can speedup insertion into indices too)
static inline int convert_server_record_cachedmap(
    const char *table, int tagmap[], const void *inbufp, char *outbuf,
    struct schema_change_type *s, struct schema *from, struct schema *to,
    blob_buffer_t *blobs, int maxblobs)
{
    char err[1024];
    struct convert_failure reason;
    int rc =
        stag_to_stag_buf_cachedmap(tagmap, from, to, (char *)inbufp, outbuf,
                                   0 /*flags*/, &reason, blobs, maxblobs);

    if (rc) {
        convert_failure_reason_str(&reason, table, from->tag, to->tag, err,
                                   sizeof(err));
        if (s->iq) reqerrstr(s->iq, ERR_SC, "cannot convert data %s", err);
        sc_errf(s, "convert_server_record_cachedmap: cannot convert data %s\n",
                err);
        return rc;
    }
    return 0;
}

static int convert_server_record_blobs(const void *inbufp, const char *from_tag,
                                       struct dbrecord *db,
                                       struct schema_change_type *s,
                                       blob_buffer_t *blobs, int maxblobs)
{
    char *inbuf = (char *)inbufp;
    struct convert_failure reason;
    char err[1024];

    if (from_tag == NULL) from_tag = ".ONDISK";

    int rc = stag_to_stag_buf_blobs(db->table, from_tag, inbuf, db->tag,
                                    db->recbuf, &reason, blobs, maxblobs, 1);
    if (rc) {
        convert_failure_reason_str(&reason, db->table, from_tag, db->tag, err,
                                   sizeof(err));
        if (s->iq) reqerrstr(s->iq, ERR_SC, "cannot convert data %s", err);
        sc_errf(s, "convert_server_record_blobs: cannot convert data %s\n",
                err);
        return 1;
    }
    return 0;
}

/* free/cleanup all resources associated with convert_record_data */
void convert_record_data_cleanup(struct convert_record_data *data)
{

    if (data->trans) {
        trans_abort(&data->iq, data->trans);
        data->trans = NULL;
    }

    if (data->dmp) {
        bdb_dtadump_done(data->from->handle, data->dmp);
        data->dmp = NULL;
    }

    free_blob_status_data(&data->blb);
    free_blob_buffers(data->freeblb,
                      sizeof(data->freeblb) / sizeof(data->freeblb[0]));

    if (data->dta_buf) {
        free(data->dta_buf);
        data->dta_buf = NULL;
    }

    if (data->rec) {
        free_db_record(data->rec);
        data->rec = NULL;
    }
}

static int convert_server_record(const void *inbufp, const char *from_tag,
                                 struct dbrecord *db,
                                 struct schema_change_type *s)
{
    return convert_server_record_blobs(inbufp, from_tag, db, s, NULL /*blobs*/,
                                       0 /*maxblobs*/);
}

static void delay_sc_if_needed(struct convert_record_data *data,
                               db_seqnum_type *ss)
{
    const int mult = 100;
    static int inco_delay = 0; /* all stripes will see this */
    int rc;

    /* wait for replication on what we just committed */
    if ((data->nrecs % data->num_records_per_trans) == 0) {
        if ((rc = trans_wait_for_seqnum(&data->iq, gbl_mynode, ss)) != 0) {
            sc_errf(data->s, "delay_sc_if_needed: error waiting for "
                             "replication rcode %d\n",
                    rc);
        } else if (gbl_sc_inco_chk) { /* committed successfully */
            int num;
            if ((num = bdb_get_num_notcoherent(thedb->bdb_env)) != 0) {
                if (num > inco_delay) { /* only goes up, or resets to 0 */
                    inco_delay = num;
                    sc_printf(data->s, "%d incoherent nodes - "
                                       "throttle sc %dms\n",
                              num, inco_delay * mult);
                }
            } else if (inco_delay != 0) {
                inco_delay = 0;
                sc_printf(data->s, "0 incoherent nodes - "
                                   "pedal to the metal\n");
            }
        } else { /* no incoherent chk */
            inco_delay = 0;
        }
    }

    if (inco_delay) poll(NULL, 0, inco_delay * mult);

    /* if we're in commitdelay mode, magnify the delay by 5 here */
    int delay = bdb_attr_get(data->from->dbenv->bdb_attr, BDB_ATTR_COMMITDELAY);
    if (delay != 0)
        poll(NULL, 0, delay * 5);
    else if (BDB_ATTR_GET(thedb->bdb_attr, SC_FORCE_DELAY))
        usleep(gbl_sc_usleep);

    /* if sanc list is not ok, snooze for 100 ms */
    if (!net_sanctioned_list_ok(data->from->dbenv->handle_sibling))
        poll(NULL, 0, 100);
}

static int report_sc_progress(struct convert_record_data *data, int now)
{
    int copy_sc_report_freq = gbl_sc_report_freq;

    if (copy_sc_report_freq > 0 &&
        now >= data->lasttime + copy_sc_report_freq) {
        /* report progress to interested parties */
        long long diff_nrecs = data->nrecs - data->prev_nrecs;
        data->lasttime = now;
        data->prev_nrecs = data->nrecs;

        /* print thread specific stats */
        sc_printf(data->s, "progress stripe %d changed genids %u progress %lld"
                           " recs +%lld (%lld r/s)\n",
                  data->stripe, data->n_genids_changed, data->nrecs, diff_nrecs,
                  diff_nrecs / copy_sc_report_freq);

        /* now do global sc data */
        int res = print_global_sc_stat(data, now, copy_sc_report_freq);
        /* check headroom only if this thread printed the global stats */
        if (res && check_sc_headroom(data->s, data->from, data->to)) {
            if (data->s->force) {
                sc_printf(data->s, "Proceeding despite low disk headroom\n");
            } else {
                return -1;
            }
        }
    }
    return 0;
}

/* converts a single record and prepares for the next one
 * should be called from a while loop
 * param data: pointer to all the state information
 * ret code:   1 means there are more records to convert
 *             0 means all work successfully done
 *             <0 means there was a failure (-2 skips some cleanup steps)
 */
static int convert_record(struct convert_record_data *data)
{
    int dtalen = 0, rc, rrn, opfailcode = 0, ixfailnum = 0;
    unsigned long long genid, ngenid, check_genid;
    void *dta = NULL;

    if (gbl_sc_thd_failed) {
        if (!data->s->retry_bad_genids == 1)
            sc_errf(data->s, "Stoping work on stripe %d because the thread for "
                             "stripe %d failed\n",
                    data->stripe, gbl_sc_thd_failed - 1);
        return -1;
    }
    if (gbl_sc_abort) {
        sc_errf(data->s, "Schema change aborted\n");
        return -1;
    }
    if (tbl_had_writes(data)) {
        usleep(gbl_sc_usleep);
        return 1;
    }

    if (data->trans == NULL) {
        /* Schema-change writes are always page-lock, not rowlock */
        rc = trans_start_sc(&data->iq, NULL, &data->trans);
        if (rc) {
            sc_errf(data->s, "error %d starting transaction\n", rc);
            return -2;
        }
        set_tran_lowpri(&data->iq, data->trans);
    }

    data->iq.debug = debug_this_request(gbl_debug_until);
    pthread_mutex_lock(&gbl_sc_lock);
    if (gbl_who > 0) {
        gbl_who--;
        data->iq.debug = 1;
    }
    pthread_mutex_unlock(&gbl_sc_lock);
    if (data->iq.debug) {
        reqlog_new_request(&data->iq);
        reqpushprefixf(&data->iq, "0x%llx: CONVERT_REC ", pthread_self());
    }

    /* Get record to convert.  We support four scan modes:-
     * - SCAN_STRIPES - DEPRECATED AND REMOVED:
     *   read one record at a time from one of the
     *   stripe files, in order.  This is primarily to support
     *   live schema change.
     * - SCAN_PARALLEL - start one thread for each stripe, the thread
     *   reads all the records in its stripe in order
     * - SCAN_DUMP - bulk dump the data file(s).  Fastest possible
     *   scan mode.
     * - SCAN_INDEX - use regular ix_ routines to scan the primary
     *   key.  Dog slow because it causes the data file scan to be
     *   in essentially random order so you get lots and lots of
     *   cache misses.  However this is a good way to uncorrupt
     *   databases that were hit by the "oops, dtastripe didn't delete
     *   records" bug in the early days.
     */
    data->iq.usedb = data->from;
    data->iq.timeoutms = gbl_sc_timeoutms;

    if (data->scanmode == SCAN_PARALLEL) {
        rc = dtas_next(&data->iq, data->sc_genids, &genid, &data->stripe, 1,
                       data->dta_buf, data->trans, data->from->lrl, &dtalen,
                       NULL);
        if (rc == 0) {
            dta = data->dta_buf;
            check_genid = bdb_normalise_genid(data->to->handle, genid);

            /* Whatever be the case, leave the lock*/
            if (check_genid != genid && !data->s->retry_bad_genids) {
                logmsg(LOGMSG_ERROR,
                       "Have old-style genids in table, disabling plan\n");
                data->s->retry_bad_genids = 1;
                return -1;
            }
        } else if (rc == 1) {
            /* we have finished all the records in our stripe
             * set pointer to -1 so all insert/update/deletes will be
             * the the left of SC pointer. This works because we now hold
             * a lock to the last page of the stripe.
             */

            // AZ: determine what locks we hold at this time
            // bdb_dump_active_locks(data->to->handle, stdout);
            data->sc_genids[data->stripe] = -1ULL;

            int usellmeta = 0;
            if (!data->to->plan) {
                usellmeta = 1; /* new dta does not have old genids */
            } else if (data->to->plan->dta_plan) {
                usellmeta = 0; /* the genid is in new dta */
            } else {
                usellmeta = 1; /* dta is not being built */
            }
            rc = 0;
            if (usellmeta && !is_dta_being_rebuilt(data->to->plan)) {
                int bdberr;
                rc = bdb_set_high_genid_stripe(NULL, data->to->dbname,
                                               data->stripe, -1ULL, &bdberr);
                if (rc != 0) rc = -1; // convert_record expects -1
            }
            sc_printf(data->s,
                      "finished stripe %d, setting genid %llx, rc %d\n",
                      data->stripe, data->sc_genids[data->stripe], rc);
            return rc;
        } else if (rc == RC_INTERNAL_RETRY) {
            trans_abort(&data->iq, data->trans);
            data->trans = NULL;

            data->totnretries++;
            if (data->cmembers->is_decrease_thrds)
                decrease_max_threads(&data->cmembers->maxthreads);
            else
                poll(0, 0, (rand() % 500 + 10));
            return 1;
        } else if (rc != 0) {
            sc_errf(data->s, "error %d reading database records\n", rc);
            return -2;
        }
        rrn = 2;
    } else if (data->scanmode == SCAN_DUMP) {
        int bdberr;
        uint8_t ver;
        if (data->dmp == NULL) {
            data->dmp = bdb_dtadump_start(data->from->handle, &bdberr, 0, 0);
            if (data->dmp == NULL) {
                sc_errf(data->s, "bdb_dtadump_start rc %d\n", bdberr);
                return -1;
            }
        }
        rc = bdb_dtadump_next(data->from->handle, data->dmp, &dta, &dtalen,
                              &rrn, &genid, &ver, &bdberr);
        vtag_to_ondisk(data->iq.usedb, dta, &dtalen, ver, genid);
        if (rc == 1) {
            /* no more records - success! */
            return 0;
        } else if (rc != 0) {
            sc_errf(data->s, "bdb error %d reading database records\n", bdberr);
            return -2;
        }

        check_genid = bdb_normalise_genid(data->to->handle, genid);
        if (check_genid != genid && !data->s->retry_bad_genids) {
            logmsg(LOGMSG_ERROR,
                   "Have old-style genids in table, disabling plan\n");
            data->s->retry_bad_genids = 1;
            return -1;
        }
    } else if (data->scanmode == SCAN_INDEX) {
        if (data->nrecs == 0) {
            bzero(data->lastkey, MAXKEYLEN);
            rc = ix_find(&data->iq, 0 /*ixnum*/, data->lastkey, 0 /*keylen*/,
                         data->curkey, &rrn, &genid, data->dta_buf, &dtalen,
                         data->from->lrl);
        } else {
            char *tmp = data->curkey;
            data->curkey = data->lastkey;
            data->lastkey = tmp;
            rc = ix_next(&data->iq, 0 /*ixnum*/, data->lastkey, 0 /*keylen*/,
                         data->lastkey, data->lastrrn, data->lastgenid,
                         data->curkey, &rrn, &genid, data->dta_buf, &dtalen,
                         data->from->lrl, 0 /*context - 0 means don't care*/);
        }
        if (rc == IX_FND || rc == IX_FNDMORE) {
            /* record found */
            data->lastrrn = rrn;
            data->lastgenid = genid;
            dta = data->dta_buf;

            check_genid = bdb_normalise_genid(data->to->handle, genid);
            if (check_genid != genid && !data->s->retry_bad_genids) {
                logmsg(LOGMSG_ERROR,
                       "Have old-style genids in table, disabling plan\n");
                data->s->retry_bad_genids = 1;
                return -1;
            }
        } else if (rc == IX_NOTFND || rc == IX_PASTEOF || rc == IX_EMPTY) {
            /* no more records - success! */
            return 0;
        } else {
            sc_errf(data->s, "ix_find/ix_next error rcode %d\n", rc);
            return -2;
        }
    } else {
        sc_errf(data->s, "internal error - bad scan mode!\n");
        return -2;
    }

    /* Report wrongly sized records */
    if (dtalen != data->from->lrl) {
        sc_errf(data->s, "invalid record size for rrn %d genid 0x%llx (%d bytes"
                         " but expected %d)\n",
                rrn, genid, dtalen, data->from->lrl);
        return -2;
    }

    /* Read associated blobs.  We usually don't need to do this in
     * planned schema change (since blobs aren't convertible the btrees
     * don't change.. unless we're changing compression options. */
    if (data->from->numblobs != 0 &&
        ((gbl_partial_indexes && data->to->ix_partial) || data->to->ix_expr ||
         !gbl_use_plan || !data->to->plan || !data->to->plan->plan_blobs ||
         data->s->force_rebuild || data->s->use_old_blobs_on_rebuild)) {
        int bdberr;
        free_blob_status_data(&data->blb);
        bdb_fetch_args_t args = {0};
        bzero(data->wrblb, sizeof(data->wrblb));

        int blobrc;
        memcpy(&data->blb, &data->blbcopy, sizeof(data->blb));
        blobrc = bdb_fetch_blobs_by_rrn_and_genid_tran(
            data->from->handle, data->trans, rrn, genid, data->from->numblobs,
            data->blobix, data->blb.bloblens, data->blb.bloboffs,
            (void **)data->blb.blobptrs, &args, &bdberr);
        if (blobrc != 0 && bdberr == BDBERR_DEADLOCK) {
            trans_abort(&data->iq, data->trans);
            data->trans = NULL;
            data->totnretries++;
            if (data->cmembers->is_decrease_thrds)
                decrease_max_threads(&data->cmembers->maxthreads);
            else
                poll(0, 0, (rand() % 500 + 10));

            return 1;
        }
        if (blobrc != 0) {
            sc_errf(data->s, "convert_record: "
                             "bdb_fetch_blobs_by_rrn_and_genid bdberr %d\n",
                    bdberr);
            return -2;
        }
        rc = check_and_repair_blob_consistency(
            &data->iq, data->iq.usedb->dbname, ".ONDISK", &data->blb, dta);

        if (data->s->force_rebuild || data->s->use_old_blobs_on_rebuild) {
            for (int ii = 0; ii < data->from->numblobs; ii++) {
                if (data->blb.blobptrs[ii] != NULL) {
                    data->wrblb[ii].exists = 1;
                    data->wrblb[ii].data = malloc(data->blb.bloblens[ii]);
                    memcpy(data->wrblb[ii].data,
                           ((char *)data->blb.blobptrs[ii]) +
                               data->blb.bloboffs[ii],
                           data->blb.bloblens[ii]);
                    data->wrblb[ii].length = data->blb.bloblens[ii];
                    data->wrblb[ii].collected = data->wrblb[ii].length;
                }
            }
        }

        if (rc != 0) {
            sc_errf(data->s,
                    "unexpected blob inconsistency rc %d, rrn %d, genid "
                    "0x%llx\n",
                    rc, rrn, genid);
            free_blob_status_data(&data->blb);
            return -2;
        }
    }

    int usellmeta = 0;
    if (!data->to->plan) {
        usellmeta = 1; /* new dta does not have old genids */
    } else if (data->to->plan->dta_plan) {
        usellmeta = 0; /* the genid is in new dta */
    } else {
        usellmeta = 1; /* dta is not being built */
    }

    int dta_needs_conversion = 1;
    if (usellmeta && data->s->rebuild_index) dta_needs_conversion = 0;

    if (dta_needs_conversion) {
        if (!data->s->force_rebuild &&
            !data->s->use_old_blobs_on_rebuild) /* We have correct blob data in
                                                   this. */
            bzero(data->wrblb, sizeof(data->wrblb));

        /* convert current.  this converts blob fields, but we need to make sure
         * we add the right blobs separately. */
        rc = convert_server_record_cachedmap(
            data->to->dbname, data->tagmap, dta, data->rec->recbuf, data->s,
            data->from->schema, data->to->schema, data->wrblb,
            sizeof(data->wrblb) / sizeof(data->wrblb[0]));
        if (rc) {
            sc_errf(data->s, "Convert failed rrn %d genid 0x%llx rc %d\n", rrn,
                    genid, rc);
            return -2;
        }

        /* TODO do the blobs returned by convert_server_record_blobs() need to
         * be converted to client blobs? */

        /* we are responsible for freeing any blob data that
         * convert_server_record_blobs() returns to us with free_blob_buffers().
         * if the plan calls for a full blob rebuild, data retrieved by
         * bdb_fetch_blobs_by_rrn_and_genid() may be added into wrblb in the
         * loop below, this blob data MUST be freed with free_blob_status_data()
         * so we need to make a copy of what we have right now so we can free it
         * seperately */
        free_blob_buffers(data->freeblb,
                          sizeof(data->freeblb) / sizeof(data->freeblb[0]));
        memcpy(data->freeblb, data->wrblb, sizeof(data->freeblb));
    }

    /* map old blobs to new blobs */
    if (!data->s->force_rebuild && !data->s->use_old_blobs_on_rebuild &&
        ((gbl_partial_indexes && data->to->ix_partial) || data->to->ix_expr ||
         !gbl_use_plan || !data->to->plan || !data->to->plan->plan_blobs)) {
        for (int ii = 0; ii < data->to->numblobs; ii++) {
            int fromblobix = data->toblobs2fromblobs[ii];
            if (fromblobix >= 0 && data->blb.blobptrs[fromblobix] != NULL) {
                if (data->wrblb[ii].data) {
                    /* this shouldn't happen because only bcstr to vutf8
                     * conversions should return any blob data from
                     * convert_server_record_blobs() and if we're createing a
                     * new vutf8 blob it should not have a fromblobix */
                    sc_errf(data->s,
                            "convert_record: attempted to "
                            "overwrite blob data retrieved from "
                            "convert_server_record_blobs() with data from "
                            "bdb_fetch_blobs_by_rrn_and_genid().  This would "
                            "leak memory and shouldn't ever happen. to blob %d "
                            "from blob %d\n",
                            ii, fromblobix);
                    return -2;
                }

                data->wrblb[ii].exists = 1;
                data->wrblb[ii].data =
                    ((char *)data->blb.blobptrs[fromblobix]) +
                    data->blb.bloboffs[fromblobix];
                data->wrblb[ii].length = data->blb.bloblens[fromblobix];
                data->wrblb[ii].collected = data->wrblb[ii].length;
            }
        }
    }

    if (data->s->use_new_genids) {
        assert(!gbl_use_plan);
        ngenid = get_genid(thedb->bdb_env, get_dtafile_from_genid(check_genid));
    } else {
        ngenid = check_genid;
    }
    if (ngenid != genid) data->n_genids_changed++;

    /* Write record to destination table */
    data->iq.usedb = data->to;

    int addflags = RECFLAGS_NO_TRIGGERS | RECFLAGS_NO_CONSTRAINTS |
                   RECFLAGS_NEW_SCHEMA | RECFLAGS_ADD_FROM_SC |
                   RECFLAGS_KEEP_GENID;

    if (data->to->plan && gbl_use_plan) addflags |= RECFLAGS_NO_BLOBS;

    int rebuild = (data->to->plan && data->to->plan->dta_plan) ||
                  schema_change == SC_CONSTRAINT_CHANGE;

    char *tagname = ".NEW..ONDISK";
    uint8_t *p_tagname_buf = (uint8_t *)tagname;
    uint8_t *p_tagname_buf_end = p_tagname_buf + 12;
    uint8_t *p_buf_data = data->rec->recbuf;
    uint8_t *p_buf_data_end = p_buf_data + data->rec->bufsize;

    if (!dta_needs_conversion) {
        p_buf_data = dta;
        p_buf_data_end = p_buf_data + dtalen;
    }

    unsigned long long dirty_keys = -1ULL;
    if (gbl_partial_indexes && data->to->ix_partial) {
        dirty_keys =
            verify_indexes(data->to, p_buf_data, data->wrblb, MAXBLOBS, 1);
        if (dirty_keys == -1ULL) {
            rc = ERR_VERIFY_PI;
            goto err;
        }
    }

    assert(data->trans != NULL);
    rc = verify_record_constraint(&data->iq, data->to, data->trans, p_buf_data,
                                  dirty_keys, data->wrblb, MAXBLOBS,
                                  ".NEW..ONDISK", rebuild, 0);
    if (rc) goto err;

    if (gbl_partial_indexes && data->to->ix_partial) {
        rc = verify_partial_rev_constraint(data->from, data->to, data->trans,
                                           p_buf_data, dirty_keys,
                                           ".NEW..ONDISK");
        if (rc) goto err;
    }

    if (schema_change != SC_CONSTRAINT_CHANGE) {
        int nrrn = rrn;
        rc = add_record(
            &data->iq, data->trans, p_tagname_buf, p_tagname_buf_end,
            p_buf_data, p_buf_data_end, NULL, data->wrblb, MAXBLOBS,
            &opfailcode, &ixfailnum, &nrrn, &ngenid,
            (gbl_partial_indexes && data->to->ix_partial) ? dirty_keys : -1ULL,
            BLOCK2_ADDKL, /* opcode */
            0,            /* blkpos */
            addflags);

        if (rc) goto err;
    }

    /* if we have been rebuilding the data files we're gonna
       call bdb_get_high_genid to resume, not look at llmeta */
    if (usellmeta && !is_dta_being_rebuilt(data->to->plan) &&
        (data->nrecs %
         BDB_ATTR_GET(thedb->bdb_attr, INDEXREBUILD_SAVE_EVERY_N)) == 0) {
        int bdberr;
        rc = bdb_set_high_genid(data->trans, data->to->dbname, genid, &bdberr);
        if (rc != 0) {
            if (bdberr == BDBERR_DEADLOCK)
                rc = RC_INTERNAL_RETRY;
            else
                rc = ERR_INTERNAL;
        }
    }

err: /*if (is_schema_change_doomed())*/
    if (gbl_sc_abort) {
        /*
        rc = ERR_CONSTR;
        break;
        */
        return -1;
    }

    /* if we should retry the operation */
    if (rc == RC_INTERNAL_RETRY) {
        trans_abort(&data->iq, data->trans);
        data->trans = NULL;
        data->num_retry_errors++;
        data->totnretries++;
        if (data->cmembers->is_decrease_thrds)
            decrease_max_threads(&data->cmembers->maxthreads);
        else
            poll(0, 0, (rand() % 500 + 10));
        return 1;
    } else if (rc == IX_DUP) {
        if (data->scanmode == SCAN_PARALLEL && data->s->rebuild_index) {
            /* if we are resuming an index rebuild schemachange,
             * and the stored llmeta genid is stale, some of the records
             * will fail insertion, and that is ok */

            sc_errf(data->s, "Skipping duplicate entry in index %d "
                             "rrn %d genid 0x%llx\n",
                    ixfailnum, rrn, genid);
            data->sc_genids[data->stripe] = genid;
            trans_abort(&data->iq, data->trans);
            data->trans = NULL;
            return 1;
        }

        if (data->s->iq)
            reqerrstr(
                data->s->iq, ERR_SC,
                "Could not add duplicate entry in index %d rrn %d genid 0x%llx",
                ixfailnum, rrn, genid);
        sc_errf(data->s, "Could not add duplicate entry in index %d "
                         "rrn %d genid 0x%llx\n",
                ixfailnum, rrn, genid);
        return -2;
    } else if (rc == ERR_CONSTR) {
        if (data->s->iq)
            reqerrstr(data->s->iq, ERR_SC,
                      "Error verifying constraints changed rrn %d genid 0x%llx",
                      rrn, genid);
        sc_errf(data->s, "Error verifying constraints changed!"
                         " rrn %d genid 0x%llx\n",
                rrn, genid);
        return -2;
    } else if (rc == ERR_VERIFY_PI) {
        if (data->s->iq)
            reqerrstr(data->s->iq, ERR_SC,
                      "Error verifying partial indexes rrn %d genid 0x%llx",
                      rrn, genid);
        sc_errf(data->s, "Error verifying partial indexes!"
                         " rrn %d genid 0x%llx\n",
                rrn, genid);
        return -2;
    } else if (rc != 0) {
        if (data->s->iq)
            reqerrstr(data->s->iq, ERR_SC,
                      "Error adding record rc %d rrn %d genid 0x%llx", rc, rrn,
                      genid);
        sc_errf(data->s, "Error adding record rcode %d opfailcode %d "
                         "ixfailnum %d rrn %d genid 0x%llx\n",
                rc, opfailcode, ixfailnum, rrn, genid);
        return -2;
    }

    /* Advance our progress markers */
    data->nrecs++;
    if (data->scanmode == SCAN_PARALLEL) {
        data->sc_genids[data->stripe] = genid;
    }

    // now do the commit
    db_seqnum_type ss;
    if (data->live) {
        rc = trans_commit_seqnum(&data->iq, data->trans, &ss);
    } else {
        rc = trans_commit(&data->iq, data->trans, gbl_mynode);
    }

    data->trans = NULL;

    if (rc) {
        sc_errf(data->s, "convert_record: trans_commit failed with "
                         "rcode %d",
                rc);
        /* If commit fail we are failing the whole operation */
        return -2;
    }

    if (data->live) delay_sc_if_needed(data, &ss);

    gbl_sc_nrecs++;

    int now = time_epoch();
    if ((rc = report_sc_progress(data, now))) return rc;

    // do the following check every second or so
    if (data->cmembers->is_decrease_thrds) lkcounter_check(data, now);

    return 1;
}

/* Thread local flag to disable page compaction when rebuild.
   Initialized in mp_fget.c */
extern pthread_key_t no_pgcompact;

/* prepares for and then calls convert_record until success or failure
 * param data: state data
 * return code: not used
 *              success or failure returned in data->outrc: 0 for success,
 *                  -1 for failure
 */
void *convert_records_thd(struct convert_record_data *data)
{
    struct thr_handle *thr_self = thrman_self();
    enum thrtype oldtype = THRTYPE_UNKNOWN;
    int rc = 1;

    if (data->isThread) thread_started("convert records");

    if (thr_self) {
        oldtype = thrman_get_type(thr_self);
        thrman_change_type(thr_self, THRTYPE_SCHEMACHANGE);
    } else {
        thr_self = thrman_register(THRTYPE_SCHEMACHANGE);
    }

    if (data->isThread) {
        backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);
    }

    data->iq.reqlogger = thrman_get_reqlogger(thr_self);
    data->outrc = -1;
    data->curkey = data->key1;
    data->lastkey = data->key2;
    data->rec = allocate_db_record(data->to->dbname, ".NEW..ONDISK");
    data->dta_buf = malloc(data->from->lrl);
    if (!data->dta_buf) {
        sc_errf(data->s, "convert_records_thd: ran out of memory trying to "
                         "malloc dta_buf: %d\n",
                data->from->lrl);
        data->outrc = -1;
        goto cleanup;
    }

    /* from this point onwards we must get to the cleanup code before
     * returning.  assume failure unless we explicitly succeed.  */
    data->lasttime = time_epoch();

    data->num_records_per_trans = gbl_num_record_converts;
    data->num_retry_errors = 0;

    if (gbl_pg_compact_thresh > 0) {
        /* Disable page compaction only if page compaction is enabled. */
        (void)pthread_setspecific(no_pgcompact, (void *)1);
    }

    /* convert each record */
    while (rc > 0) {
        if (data->cmembers->is_decrease_thrds &&
            use_rebuild_thr(&data->cmembers->thrcount,
                            &data->cmembers->maxthreads)) {
            /* num thread at max, sleep few microsec then try again */
            usleep(bdb_attr_get(data->from->dbenv->bdb_attr,
                                BDB_ATTR_SC_NO_REBUILD_THR_SLEEP));
            continue;
        }

        /* convert_record returns 1 to continue, 0 on completion, < 0 if failed
         */
        rc = convert_record(data);
        if (data->cmembers->is_decrease_thrds)
            release_rebuild_thr(&data->cmembers->thrcount);

        if (stopsc) { // set from downgrade
            data->outrc = rc;
            goto cleanup_no_msg;
        }
    }

    if (rc == -2) {
        data->outrc = -1;
        goto cleanup;
    } else {
        data->outrc = rc;
    }

    if (data->trans) {
        /* can only get here for non-live schema change, shouldn't ever get here
         * now since bulk transactions have been disabled in all schema changes
         */
        rc = trans_commit(&data->iq, data->trans, gbl_mynode);
        data->trans = NULL;
        if (rc) {
            sc_errf(data->s, "convert_records_thd: trans_commit failed due "
                             "to RC_TRAN_TOO_COMPLEX\n");
            data->outrc = -1;
            goto cleanup;
        }
    }

    if (data->outrc == 0 && data->n_genids_changed > 0) {
        sc_errf(data->s,
                "WARNING %u genids were changed by this schema change\n",
                data->n_genids_changed);
    }

cleanup:
    if (data->outrc == 0) {
        sc_printf(data->s,
                  "successfully converted %lld records with %d retries "
                  "stripe %d\n",
                  data->nrecs, data->totnretries, data->stripe);
    } else {
        if (gbl_sc_abort) {
            sc_errf(data->s,
                    "conversion aborted after %lld records, while working on"
                    " stripe %d with %d retries\n",
                    data->nrecs, data->stripe, data->totnretries);
        } else if (!data->s->retry_bad_genids) {
            sc_errf(data->s,
                    "conversion failed after %lld records, while working on"
                    " stripe %d with %d retries\n",
                    data->nrecs, data->stripe, data->totnretries);
        }

        gbl_sc_thd_failed = data->stripe + 1;
    }

cleanup_no_msg:
    convert_record_data_cleanup(data);

    if (data->isThread) backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);

    /* restore our  thread type to what it was before */
    if (oldtype != THRTYPE_UNKNOWN) thrman_change_type(thr_self, oldtype);

    return NULL;
}

int convert_all_records(struct dbtable *from, struct dbtable *to,
                        unsigned long long *sc_genids,
                        struct schema_change_type *s)
{
    struct convert_record_data data = {0};
    int ii;
    gbl_sc_thd_failed = 0;

    data.curkey = data.key1;
    data.lastkey = data.key2;
    data.from = from;
    data.to = to;
    data.live = s->live;
    data.scanmode = s->scanmode;
    data.sc_genids = sc_genids;
    data.s = s;

    if (data.live && data.scanmode != SCAN_PARALLEL) {
        sc_errf(data.s, "live schema change can only be done in parallel "
                        "scan mode\n");
        return -1;
    }

    /* Calculate blob data file numbers to feed direct into bdb.  This used
     * to be a hard coded array.  And it was wrong.  By employing for loop
     * technology, we can't possibly get this wrong again! */
    for (int ii = 0; ii < MAXBLOBS; ii++)
        data.blobix[ii] = ii + 1;

    /* set up internal rebuild request */
    init_fake_ireq(thedb, &data.iq);
    data.iq.usedb = data.from;
    data.iq.opcode = OP_REBUILD;
    data.iq.debug = 0; /*gbl_who;*/

    /* For first cut, read all blobs.  Later we can optimise by only reading
     * the blobs that the new schema needs.
     * Now, it's later.  Don't read any blobs at all if we are using a plan. */
    if ((gbl_partial_indexes && data.to->ix_partial) || data.to->ix_expr ||
        !gbl_use_plan || !data.to->plan || !data.to->plan->plan_blobs) {
        if (gather_blob_data(&data.iq, ".ONDISK", &data.blb, ".ONDISK")) {
            sc_errf(data.s, "convert_all_records:gather_blob_data failed\n");
            return -1;
        }
        data.blb.numcblobs = data.from->numblobs;
        for (ii = 0; ii < data.blb.numcblobs; ii++) {
            data.blb.cblob_disk_ixs[ii] = ii;
            data.blb.cblob_tag_ixs[ii] =
                get_schema_blob_field_idx(data.from->dbname, ".ONDISK", ii);
        }
        for (ii = 0; ii < data.to->numblobs; ii++) {
            int map;
            map = tbl_blob_no_to_tbl_blob_no(data.to->dbname, ".NEW..ONDISK",
                                             ii, data.from->dbname, ".ONDISK");
            if (map < 0 && map != -3) {
                sc_errf(data.s,
                        "convert_all_records: error mapping blob %d "
                        "from %s:%s to %s:%s blob_no_to_blob_no rcode %d\n",
                        ii, data.from->dbname, ".ONDISK", data.to->dbname,
                        ".NEW..ONDISK", map);
                return -1;
            }
            data.toblobs2fromblobs[ii] = map;
        }
        memcpy(&data.blbcopy, &data.blb, sizeof(data.blb));
    } else {
        bzero(&data.blb, sizeof(data.blb));
        bzero(&data.blbcopy, sizeof(data.blbcopy));
    }

    data.cmembers = calloc(1, sizeof(struct common_members));
    int sc_threads =
        bdb_attr_get(data.from->dbenv->bdb_attr, BDB_ATTR_SC_USE_NUM_THREADS);
    if (sc_threads <= 0 || sc_threads > gbl_dtastripe) {
        bdb_attr_set(data.from->dbenv->bdb_attr, BDB_ATTR_SC_USE_NUM_THREADS,
                     gbl_dtastripe);
        sc_threads = gbl_dtastripe;
    }
    data.cmembers->maxthreads = sc_threads;
    data.cmembers->is_decrease_thrds = bdb_attr_get(
        data.from->dbenv->bdb_attr, BDB_ATTR_SC_DECREASE_THRDS_ON_DEADLOCK);

    // tagmap only needed if we are doing work on the data file
    data.tagmap = get_tag_mapping(
        data.from->schema /*tbl .ONDISK tag schema*/,
        data.to->schema /*tbl .NEW..ONDISK schema */); // free tagmap only once
    int outrc = 0;

    /* if were not in parallel, dont start any threads */
    if (data.scanmode != SCAN_PARALLEL) {
        convert_records_thd(&data);
        outrc = data.outrc;
    } else {
        struct convert_record_data threadData[gbl_dtastripe];
        int threadSkipped[gbl_dtastripe];
        pthread_attr_t attr;
        int rc = 0;

        data.isThread = 1;

        pthread_attr_init(&attr);
        pthread_attr_setstacksize(&attr, DEFAULT_THD_STACKSZ);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

        /* start one thread for each stripe */
        for (ii = 0; ii < gbl_dtastripe; ++ii) {
            /* create a copy of the data, modifying the necessary
             * thread specific values
             */
            threadData[ii] = data;
            threadData[ii].stripe = ii;

            if (sc_genids[ii] == -1ULL) {
                sc_printf(threadData[ii].s, "stripe %d was done\n",
                          threadData[ii].stripe);
                threadSkipped[ii] = 1;
                continue;
            } else
                threadSkipped[ii] = 0;

            sc_printf(threadData[ii].s, "starting thread for stripe: %d\n",
                      threadData[ii].stripe);

            /* start thread */
            /* convert_records_thd( &threadData[ ii ]); |+ serialized calls +|*/
            rc = pthread_create(&threadData[ii].tid, &attr,
                                (void *(*)(void *))convert_records_thd,
                                &threadData[ii]);

            /* if thread creation failed */
            if (rc) {
                sc_errf(threadData[ii].s, "starting thread failed for"
                                          " stripe: %d with return code: %d\n",
                        threadData[ii].stripe, rc);

                outrc = -1;
                break;
            }
        }

        /* wait for all threads to complete */
        for (ii = 0; ii < gbl_dtastripe; ++ii) {
            void *ret;

            if (threadSkipped[ii]) continue;

            /* if the threadid is NULL, skip this one */
            if (!threadData[ii].tid) {
                sc_errf(threadData[ii].s, "skip joining thread failed for "
                                          "stripe: %d because tid is null\n",
                        threadData[ii].stripe);
                outrc = -1;
                continue;
            }

            rc = pthread_join(threadData[ii].tid, &ret);

            /* if join failed */
            if (rc) {
                sc_errf(threadData[ii].s, "joining thread failed for"
                                          " stripe: %d with return code: %d\n",
                        threadData[ii].stripe, rc);
                outrc = -1;
                continue;
            }

            /* if thread's conversions failed return error code */
            if (threadData[ii].outrc != 0) outrc = threadData[ii].outrc;
        }

        /* destroy attr */
        pthread_attr_destroy(&attr);
    }

    convert_record_data_cleanup(&data);

    if (data.cmembers) {
        free(data.cmembers);
        data.cmembers = NULL;
    }

    if (data.tagmap) {
        free(data.tagmap);
        data.tagmap = NULL;
    }
    return outrc;
}

/*
** Similar to convert_record(), with the following exceptions.
** 1. continue working on the rest stripes if some stripes failed
** 2. no retry effort if upd_record() deadlock'd
** 3. no blob/index r/w
*/
static int upgrade_records(struct convert_record_data *data)
{
    static const int mult = 100;
    static int inco_delay = 0;

    int rc;
    int nretries;
    int commitdelay;
    int now;
    int copy_sc_report_freq = gbl_sc_report_freq;
    int opfailcode = 0;
    int ixfailnum = 0;
    int dtalen = 0;
    int bdberr;
    unsigned long long genid = 0;
    int recver;
    uint8_t *p_buf_data, *p_buf_data_end;
    db_seqnum_type ss;

    // if sc has beed aborted, return
    if (gbl_sc_abort) {
        sc_errf(data->s, "Schema change aborted\n");
        return -1;
    }

    if (data->trans == NULL) {
        /* Schema-change writes are always page-lock, not rowlock */
        rc = trans_start_sc(&data->iq, NULL, &data->trans);
        if (rc) {
            sc_errf(data->s, "error %d starting transaction\n", rc);
            return -2;
        }
    }

    // make sc transaction low priority
    set_tran_lowpri(&data->iq, data->trans);

    // set debug info
    if (gbl_who > 0 || data->iq.debug > 0) {
        pthread_mutex_lock(&gbl_sc_lock);
        data->iq.debug = gbl_who;
        if (data->iq.debug > 0) {
            gbl_who = data->iq.debug - 1;
            data->iq.debug = 1;
        }
        pthread_mutex_unlock(&gbl_sc_lock);
    }

    data->iq.timeoutms = gbl_sc_timeoutms;

    // get next converted record. retry up to gbl_maxretries times
    for (nretries = 0, rc = RC_INTERNAL_RETRY;
         rc == RC_INTERNAL_RETRY && nretries++ != gbl_maxretries;) {

        if (data->nrecs > 0 || data->sc_genids[data->stripe] == 0) {
            rc = dtas_next(&data->iq, data->sc_genids, &genid, &data->stripe,
                           data->scanmode == SCAN_PARALLEL, data->dta_buf,
                           data->trans, data->from->lrl, &dtalen, &recver);
        } else {
            genid = data->sc_genids[data->stripe];
            rc = ix_find_ver_by_rrn_and_genid_tran(
                &data->iq, 2, genid, data->dta_buf, &dtalen, data->from->lrl,
                data->trans, &recver);
        }

        switch (rc) {
        case 0: // okay
            break;

        case 1: // finish reading all records
            sc_printf(data->s, "finished stripe %d\n", data->stripe);
            return 0;

        case RC_INTERNAL_RETRY: // retry
            trans_abort(&data->iq, data->trans);
            data->trans = NULL;
            break;

        default: // fatal error
            sc_errf(data->s, "error %d reading database records\n", rc);
            return -2;
        }
    }

    // exit after too many retries
    if (rc == RC_INTERNAL_RETRY) {
        sc_errf(data->s, "%s: *ERROR* dtas_next "
                         "too much contention count %d genid 0x%llx\n",
                __func__, nretries, genid);
        return -2;
    }

    /* Report wrongly sized records */
    if (dtalen != data->from->lrl) {
        sc_errf(data->s, "invalid record size for genid 0x%llx (%d bytes"
                         " but expected %d)\n",
                genid, dtalen, data->from->lrl);
        return -2;
    }

    if (recver != data->to->version) {
        // rewrite the record if not ondisk version
        p_buf_data = (uint8_t *)data->dta_buf;
        p_buf_data_end = p_buf_data + data->from->lrl;
        rc = upgrade_record(&data->iq, data->trans, genid, p_buf_data,
                            p_buf_data_end, &opfailcode, &ixfailnum,
                            BLOCK2_UPTBL, 0);
    }

    // handle rc
    switch (rc) {
    default: /* bang! */
        sc_errf(data->s, "Error upgrading record "
                         "rc %d opfailcode %d genid 0x%llx\n",
                rc, opfailcode, genid);
        return -2;

    case RC_INTERNAL_RETRY: /* deadlock */
                            /*
                             ** if deadlk occurs, abort txn and skip this record.
                             ** leaving a single record unconverted does little harm.
                             ** also the record has higher chances to be updated
                             ** by the other txns which are holding resources this txn requires.
                             */
        ++data->nrecskip;
        trans_abort(&data->iq, data->trans);
        data->trans = NULL;
        break;

    case 0: /* all good */
        ++data->nrecs;
        rc = trans_commit_seqnum(&data->iq, data->trans, &ss);
        data->trans = NULL;

        if (rc) {
            sc_errf(data->s, "%s: trans_commit failed with "
                             "rcode %d",
                    __func__, rc);
            /* If commit fail we are failing the whole operation */
            return -2;
        }

        // txn contains enough records, wait for replicants
        if ((data->nrecs % data->num_records_per_trans) == 0) {
            if ((rc = trans_wait_for_seqnum(&data->iq, gbl_mynode, &ss)) != 0) {
                sc_errf(data->s, "%s: error waiting for "
                                 "replication rcode %d\n",
                        __func__, rc);
            } else if (gbl_sc_inco_chk) {
                int num;
                if ((num = bdb_get_num_notcoherent(thedb->bdb_env)) != 0) {
                    if (num > inco_delay) { /* only goes up, or resets to 0 */
                        inco_delay = num;
                        sc_printf(data->s, "%d incoherent nodes - "
                                           "throttle sc %dms\n",
                                  num, inco_delay * mult);
                    }
                } else if (inco_delay != 0) {
                    inco_delay = 0;
                    sc_printf(data->s, "0 incoherent nodes - "
                                       "pedal to the metal\n");
                }
            } else {
                inco_delay = 0;
            }
        }

        if (inco_delay) poll(0, 0, inco_delay * mult);

        /* if we're in commitdelay mode, magnify the delay by 5 here */
        if ((commitdelay = bdb_attr_get(data->from->dbenv->bdb_attr,
                                        BDB_ATTR_COMMITDELAY)) != 0)
            poll(NULL, 0, commitdelay * 5);

        /* if sanc list is not ok, snooze for 100 ms */
        if (!net_sanctioned_list_ok(data->from->dbenv->handle_sibling))
            poll(NULL, 0, 100);

        /* snooze for a bit if writes have been coming in */
        if (gbl_sc_last_writer_time >= time_epoch() - 5) usleep(gbl_sc_usleep);
        break;
    } // end of rc check

    ++gbl_sc_nrecs;
    data->sc_genids[data->stripe] = genid;
    now = time_epoch();

    if (copy_sc_report_freq > 0 &&
        now >= data->lasttime + copy_sc_report_freq) {
        /* report progress to interested parties */
        long long diff_nrecs = data->nrecs - data->prev_nrecs;
        data->lasttime = now;
        data->prev_nrecs = data->nrecs;

        /* print thread specific stats */
        sc_printf(
            data->s,
            "TABLE UPGRADE progress stripe %d changed genids %u progress %lld"
            " recs +%lld conversions/sec %lld\n",
            data->stripe, data->n_genids_changed, data->nrecs, diff_nrecs,
            diff_nrecs / copy_sc_report_freq);

        /* now do global sc data */
        int res = print_global_sc_stat(data, now, copy_sc_report_freq);
        /* check headroom only if this thread printed the global stats */
        if (res && check_sc_headroom(data->s, data->from, data->to)) {
            if (data->s->force) {
                sc_printf(data->s, "Proceeding despite low disk headroom\n");
            } else {
                return -1;
            }
        }
    }

    if (data->s->fulluprecs)
        return 1;
    else if (recver == data->to->version)
        return 0;
    else if (data->nrecs >= data->s->partialuprecs)
        return 0;
    else
        return 1;
}

static void *upgrade_records_thd(void *vdata)
{
    int rc;

    struct convert_record_data *data = (struct convert_record_data *)vdata;
    struct thr_handle *thr_self = thrman_self();
    enum thrtype oldtype = THRTYPE_UNKNOWN;

    // transfer thread type
    if (data->isThread) thread_started("upgrade records");

    if (thr_self) {
        oldtype = thrman_get_type(thr_self);
        thrman_change_type(thr_self, THRTYPE_SCHEMACHANGE);
    } else {
        thr_self = thrman_register(THRTYPE_SCHEMACHANGE);
    }

    if (data->isThread)
        backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);

    // initialize convert_record_data
    data->outrc = -1;
    data->lasttime = time_epoch();
    data->num_records_per_trans = gbl_num_record_converts;
    data->num_records_per_trans = gbl_num_record_converts;

    data->dta_buf = malloc(data->from->lrl);
    if (!data->dta_buf) {
        sc_errf(data->s, "%s: ran out of memory trying to "
                         "malloc dta_buf: %d\n",
                __func__, data->from->lrl);
        data->outrc = -1;
        goto cleanup;
    }

    while ((rc = upgrade_records(data)) > 0) {
        if (stopsc) {
            if (data->isThread)
                backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
            return NULL;
        }
    }

    if (rc == -2) {
        data->outrc = -1;
    } else {
        data->outrc = rc;
    }

cleanup:
    if (data->outrc == 0) {
        sc_printf(data->s,
                  "successfully upgraded %lld records. skipped %lld records.\n",
                  data->nrecs, data->nrecskip);
    } else {
        if (gbl_sc_abort) {
            sc_errf(data->s, "conversion aborted after %lld records upgraded "
                             "and %lld records skipped, "
                             "while working on stripe %d\n",
                    data->nrecs, data->nrecskip, data->stripe);
        }
        gbl_sc_thd_failed = data->stripe + 1;
    }

    convert_record_data_cleanup(data);

    if (data->isThread) backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);

    if (oldtype != THRTYPE_UNKNOWN) thrman_change_type(thr_self, oldtype);
    return NULL;
}

int upgrade_all_records(struct dbtable *db, unsigned long long *sc_genids,
                        struct schema_change_type *s)
{
    int idx;
    int rc = 0;
    int outrc = 0;

    struct convert_record_data data = {0};

    gbl_sc_thd_failed = 0;

    data.from = db;
    data.to = db;
    data.sc_genids = sc_genids;
    data.s = s;
    data.scanmode = s->scanmode;

    // set up internal block request
    init_fake_ireq(thedb, &data.iq);
    data.iq.usedb = db;
    data.iq.opcode = OP_UPGRADE;
    data.iq.debug = 0;

    if (data.scanmode != SCAN_PARALLEL) {
        if (s->start_genid != 0) {
            data.stripe = get_dtafile_from_genid(s->start_genid);
            data.sc_genids[data.stripe] = s->start_genid;
        }
        upgrade_records_thd(&data);
        outrc = data.outrc;
    } else {
        struct convert_record_data thread_data[gbl_dtastripe];
        pthread_attr_t attr;
        void *ret;

        data.isThread = 1;

        // init pthread attributes
        pthread_attr_init(&attr);
        pthread_attr_setstacksize(&attr, DEFAULT_THD_STACKSZ);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

        for (idx = 0; idx != gbl_dtastripe; ++idx) {
            thread_data[idx] = data;
            thread_data[idx].stripe = idx;
            sc_printf(thread_data[idx].s, "starting thread for stripe: %d\n",
                      thread_data[idx].stripe);

            rc = pthread_create(&thread_data[idx].tid, &attr,
                                upgrade_records_thd, &thread_data[idx]);

            if (rc) {
                sc_errf(thread_data[idx].s,
                        "starting thread failed for"
                        " stripe: %d with return code: %d\n",
                        thread_data[idx].stripe, rc);

                outrc = -1;
                break;
            }
        }

        if (outrc == -1) {
            for (; idx >= 0; --idx)
                pthread_cancel(thread_data[idx].tid);
        } else {
            for (idx = 0; idx != gbl_dtastripe; ++idx) {
                rc = pthread_join(thread_data[idx].tid, &ret);
                /* if join failed */
                if (rc) {
                    sc_errf(thread_data[idx].s,
                            "joining thread failed for"
                            " stripe: %d with return code: %d\n",
                            thread_data[idx].stripe, rc);
                    outrc = -1;
                    continue;
                }

                /* if thread's conversions failed return error code */
                if (thread_data[idx].outrc != 0) outrc = thread_data[idx].outrc;
            }
        }

        pthread_attr_destroy(&attr);
    }

    convert_record_data_cleanup(&data);

    return outrc;
}
