/*
   Copyright 2015, 2017, Bloomberg Finance L.P.

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

#include "osqlcomm.h"
#include "cron.h"

#define OSQL_BP_MAXLEN (32 * 1024)

/* { REPLICANT SIDE UPGRADE RECORD LOGIC */
int gbl_num_record_upgrades = 0; /* default off */
// Dorin's cron scheduler
cron_sched_t *uprec_sched;

// structure
static struct uprec_tag {
    pthread_mutex_t *lk;         /* one big mutex, rule them all */
    const struct dbtable *owner; /* who can put elements in the array */
    const struct dbtable *touch; /* which db master will be touching */
    struct buf_lock_t slock;
    size_t thre; /* slow start threshold */
    size_t intv; /* interval */
    uint8_t buffer[OSQL_BP_MAXLEN];
    uint8_t buf_end;
    unsigned long long genid;
    /* nreqs should == nbads + ngoods + ntimeouts */
    size_t nreqs;     /* number of upgrade requests sent to master */
    size_t nbads;     /* number of bad responses recv'd from master */
    size_t ngoods;    /* number of good responses recv'd from master */
    size_t ntimeouts; /* number of timeouts */
} * uprec;

/* Offload the internal block request.
   And wait on a fake for reply inline. */
static int offload_comm_send_sync_blockreq(char *node, void *buf, int buflen);

static const uint8_t *construct_uptbl_buffer(const struct dbtable *db,
                                             unsigned long long genid,
                                             unsigned int recs_ahead,
                                             uint8_t *p_buf_start,
                                             const uint8_t *p_buf_end)
{
    uint8_t *p_buf;
    uint8_t *p_buf_req_start;
    const uint8_t *p_buf_req_end;
    uint8_t *p_buf_op_hdr_start;
    const uint8_t *p_buf_op_hdr_end;

    struct req_hdr req_hdr;
    struct block_req req = {0};
    struct packedreq_hdr op_hdr;
    struct packedreq_usekl usekl;
    struct packedreq_uptbl uptbl;

    p_buf = p_buf_start;

    // req_hdr
    req_hdr.opcode = OP_BLOCK;
    if (!(p_buf = req_hdr_put(&req_hdr, p_buf, p_buf_end)))
        return NULL;

    // save room for block req
    if (BLOCK_REQ_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf_req_start = p_buf;
    p_buf += BLOCK_REQ_LEN;
    p_buf_req_end = p_buf;

    /** use **/

    // save room for uprec header
    if (PACKEDREQ_HDR_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf_op_hdr_start = p_buf;
    p_buf += PACKEDREQ_HDR_LEN;
    p_buf_op_hdr_end = p_buf;

    usekl.dbnum = db->dbnum;
    usekl.taglen = strlen(db->tablename) + 1 /*NUL byte*/;
    if (!(p_buf = packedreq_usekl_put(&usekl, p_buf, p_buf_end)))
        return NULL;
    if (!(p_buf =
              buf_no_net_put(db->tablename, usekl.taglen, p_buf, p_buf_end)))
        return NULL;

    op_hdr.opcode = BLOCK2_USE;
    op_hdr.nxt = one_based_word_offset_from_ptr(p_buf_start, p_buf);

    if (!(p_buf = buf_zero_put(
              ptr_from_one_based_word_offset(p_buf_start, op_hdr.nxt) - p_buf,
              p_buf, p_buf_end)))
        return NULL;

    if (packedreq_hdr_put(&op_hdr, p_buf_op_hdr_start, p_buf_op_hdr_end) !=
        p_buf_op_hdr_end)
        return NULL;

    /** uptbl **/

    // save room for uprec header
    if (PACKEDREQ_HDR_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf_op_hdr_start = p_buf;
    p_buf += PACKEDREQ_HDR_LEN;
    p_buf_op_hdr_end = p_buf;

    uptbl.nrecs = recs_ahead;
    uptbl.genid = uprec->genid;
    if (!(p_buf = packedreq_uptbl_put(&uptbl, p_buf, p_buf_end)))
        return NULL;

    if (!(p_buf = buf_put(&uprec->genid, sizeof(unsigned long long), p_buf,
                          p_buf_end)))
        return NULL;

    op_hdr.opcode = BLOCK2_UPTBL;
    op_hdr.nxt = one_based_word_offset_from_ptr(p_buf_start, p_buf);

    if (!(p_buf = buf_zero_put(
              ptr_from_one_based_word_offset(p_buf_start, op_hdr.nxt) - p_buf,
              p_buf, p_buf_end)))
        return NULL;

    if (packedreq_hdr_put(&op_hdr, p_buf_op_hdr_start, p_buf_op_hdr_end) !=
        p_buf_op_hdr_end)
        return NULL;

    /* build req */
    req.num_reqs = 2;
    req.flags = BLKF_ERRSTAT; /* we want error stat */
    req.offset = op_hdr.nxt;  /* overall offset = next offset of last op */

    /* pack req in the space we saved at the start */
    if (block_req_put(&req, p_buf_req_start, p_buf_req_end) != p_buf_req_end)
        return NULL;
    p_buf_end = p_buf;

    return p_buf_end;
}

static void *uprec_cron_kickoff(struct cron_event *_, struct errstat *err)
{
    logmsg(LOGMSG_INFO, "Starting upgrade record cron job\n");
    return NULL;
}

static void *uprec_cron_event(struct cron_event *_, struct errstat *err)
{
    int rc, nwakeups;
    struct buf_lock_t *p_slock;
    struct timespec ts;
    const uint8_t *buf_end;

    {
        Pthread_mutex_lock(uprec->lk);
        /* construct buffer */
        buf_end = construct_uptbl_buffer(uprec->touch, uprec->genid,
                                         gbl_num_record_upgrades, uprec->buffer,
                                         &uprec->buf_end);
        if (buf_end == NULL)
            goto done;

        /* send and then wait */
        p_slock = &uprec->slock;
        rc = offload_comm_send_blockreq(
            thedb->master == gbl_myhostname ? 0 : thedb->master, p_slock,
            uprec->buffer, (buf_end - uprec->buffer));
        if (rc != 0)
            goto done;

        ++uprec->nreqs;
        nwakeups = 0;
        while (p_slock->reply_state != REPLY_STATE_DONE) {
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;
            rc = pthread_cond_timedwait(&p_slock->wait_cond, &p_slock->req_lock,
                                        &ts);
            ++nwakeups;

            if (nwakeups == uprec->thre) { // timedout #1
                logmsg(LOGMSG_WARN,
                       "no response from master within %d seconds\n", nwakeups);
                break;
            }
        }

        if (p_slock->reply_state != REPLY_STATE_DONE) {
            // timedout from #1
            // intv = 0.75 * intv + 0.25 * T(this time)
            uprec->intv += (uprec->intv << 1) + nwakeups;
            uprec->intv >>= 2;
            ++uprec->ntimeouts;
        } else if (p_slock->rc) {
            // something unexpected happened. double the interval.
            if (uprec->intv >= uprec->thre)
                ++uprec->intv;
            else {
                uprec->intv <<= 1;
                if (uprec->intv >= uprec->thre)
                    uprec->intv = uprec->thre;
            }
            ++uprec->nbads;
        } else {
            // all good. reset interval
            uprec->intv = 1;
            ++uprec->ngoods;
        }

    done:
        // allow the array to take new requests
        uprec->genid = 0;
        uprec->owner = NULL;

        Pthread_mutex_unlock(uprec->lk);
    }

    return NULL;
}

// Upgrade a single record.
int offload_comm_send_upgrade_record(const char *tbl, unsigned long long genid)
{
    int rc;
    uint8_t *buffer;
    const uint8_t *buffer_end;

    buffer = alloca(OSQL_BP_MAXLEN);

    buffer_end =
        construct_uptbl_buffer(get_dbtable_by_name(tbl), genid, 1, buffer,
                               (const uint8_t *)(buffer + OSQL_BP_MAXLEN));

    if (buffer_end == NULL)
        rc = EINVAL;
    else
        // send block request and free buffer
        rc = offload_comm_send_sync_blockreq(
            thedb->master == gbl_myhostname ? 0 : thedb->master, buffer,
            buffer_end - buffer);

    return rc;
}

// initialize sender queues once
static pthread_once_t uprec_sender_array_once = PTHREAD_ONCE_INIT;
static void uprec_sender_array_init(void)
{
    size_t mallocsz;
    struct errstat xerr;

    mallocsz = sizeof(struct uprec_tag) +
               (gbl_dtastripe - 1) * sizeof(unsigned long long);

    // malloc a big chunk
    uprec = malloc(mallocsz);
    if (uprec == NULL) {
        logmsg(LOGMSG_FATAL, "%s: out of memory.\n", __func__);
        abort();
    }

    // initialize the big structure
    uprec->owner = NULL;
    uprec->thre = 900; // 15 minutes
    uprec->intv = 1;
    uprec->nreqs = 0;
    uprec->nbads = 0;
    uprec->ngoods = 0;
    uprec->ntimeouts = 0;

    // initialize slock
    Pthread_mutex_init(&(uprec->slock.req_lock), NULL);
    Pthread_cond_init(&(uprec->slock.wait_cond), NULL);

    uprec->lk = &uprec->slock.req_lock;
    uprec->slock.reply_state = REPLY_STATE_NA;
    uprec->slock.sb = 0;

    // kick off upgradetable cron
    uprec_sched =
        cron_add_event(NULL, "uprec_cron", INT_MIN, uprec_cron_kickoff, NULL,
                       NULL, NULL, NULL, NULL, &xerr, NULL);

    if (uprec_sched == NULL) {
        logmsg(LOGMSG_FATAL, "%s: failed to create uprec cron scheduler.\n",
               __func__);
        abort();
    }

    logmsg(LOGMSG_INFO, "upgraderecord sender array initialized\n");
}

int offload_comm_send_upgrade_records(const dbtable *db,
                                      unsigned long long genid)
{
    int rc = 0;
    struct errstat xerr;

    if (genid == 0)
        return EINVAL;

    /* if i am master of a cluster, return. */
    if (thedb->master == gbl_myhostname &&
        net_count_nodes(osql_get_netinfo()) > 1)
        return 0;

    (void)pthread_once(&uprec_sender_array_once, uprec_sender_array_init);

    if (uprec->owner == NULL) {
        Pthread_mutex_lock(uprec->lk);
        if (uprec->owner == NULL)
            uprec->owner = db;
        Pthread_mutex_unlock(uprec->lk);
    }

    if (db == uprec->owner) {
        rc = pthread_mutex_trylock(uprec->lk);
        if (rc == 0) {
            if (db == uprec->owner) {
                // can't pass db and genid to cron scheduler because
                // scheduler will free all arguments after job is done.
                // instead make a global copy here
                uprec->genid = genid;
                uprec->touch = uprec->owner;
                uprec_sched = cron_add_event(
                    uprec_sched, NULL, comdb2_time_epoch() + uprec->intv,
                    uprec_cron_event, NULL, NULL, NULL, NULL, NULL, &xerr,
                    NULL);

                if (uprec_sched == NULL)
                    logmsg(LOGMSG_ERROR,
                           "%s: failed to schedule uprec cron job.\n",
                           __func__);

                // zap owner
                uprec->owner = (void *)~(uintptr_t)0;
            }
        }
        Pthread_mutex_unlock(uprec->lk);
    }

    return rc;
}

void upgrade_records_stats(void)
{
    if (uprec == NULL)
        return;

    logmsg(LOGMSG_USER, "# %-24s %d recs/req\n", "upgrade ahead records",
           gbl_num_record_upgrades);
    logmsg(LOGMSG_USER, "# %-24s %zu\n", "total requests", uprec->nreqs);
    logmsg(LOGMSG_USER, "# %-24s %zu\n", "bad responses", uprec->nbads);
    logmsg(LOGMSG_USER, "# %-24s %zu\n", "good responses", uprec->ngoods);
    logmsg(LOGMSG_USER, "# %-24s %zu\n", "timeouts", uprec->ntimeouts);
    logmsg(LOGMSG_USER, "%-26s %zu s\n", "cron event interval", uprec->intv);
}

static int offload_comm_send_sync_blockreq(char *node, void *buf, int buflen)
{
    int rc;
    int nwakeups;
    struct timespec ts;
    struct buf_lock_t *p_slock;

    // create a fake buf_lock
    p_slock = malloc(sizeof(struct buf_lock_t));

    if (p_slock == NULL)
        return ENOMEM;

    p_slock->reply_state = REPLY_STATE_NA;
    p_slock->sb = NULL;

    // initialize lock and cond
    Pthread_mutex_init(&(p_slock->req_lock), 0);
    Pthread_cond_init(&(p_slock->wait_cond), NULL);

    {
        Pthread_mutex_lock(&(p_slock->req_lock));
        rc = offload_comm_send_blockreq(node, p_slock, buf, buflen);
        if (rc == 0) {
            nwakeups = 0;
            while (p_slock->reply_state != REPLY_STATE_DONE) {
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_sec += 1;
                rc = pthread_cond_timedwait(&(p_slock->wait_cond),
                                            &(p_slock->req_lock), &ts);
                ++nwakeups;

                if (nwakeups == 1000)
                    break;
            }
        }
        Pthread_mutex_unlock(&(p_slock->req_lock));
    }

    // clean up
    Pthread_cond_destroy(&(p_slock->wait_cond));
    Pthread_mutex_destroy(&(p_slock->req_lock));
    free(p_slock);
    return rc;
}

/* END OF REPLICANT SIDE UPGRADE RECORD LOGIC } */
