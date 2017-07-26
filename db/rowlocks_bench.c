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

#include "limit_fortify.h"
#include <comdb2.h>
#include <epochlib.h>

/* Commit bench opcodes */
enum {
    COMMIT_BENCH = 0,
    ROWLOCKS_BENCH = 1,
    ROWLOCKS_LOCK1_BENCH = 2,
    ROWLOCKS_LOCK2_BENCH = 3
};

/* Not pretty */
int ll_rowlocks_bench(bdb_state_type *bdb_state, tran_type *tran, int op,
                      int arg1, int arg2, void *payload, int paylen);
int ll_commit_bench(bdb_state_type *bdb_state, tran_type *tran, int op,
                    int arg1, int arg2, void *payload, int paylen);
int trans_start_int(struct ireq *iq, tran_type *parent_trans,
                    tran_type **out_trans, int logical, int retries);
int bdb_tran_set_request_ack(void *trans);
unsigned long long rep_get_send_callcount(void);
unsigned long long rep_get_send_bytecount(void);
void rep_reset_send_callcount(void);
void rep_reset_send_bytecount(void);
unsigned long long net_get_num_flushes(void);
void net_reset_num_flushes(void);
unsigned long long net_get_send_interval_flushes(void);
void net_reset_send_interval_flushes(void);
unsigned long long net_get_explicit_flushes(void);
void net_reset_explicit_flushes(void);

static void commit_bench_int(bdb_state_type *bdb_state, int op, int tcount,
                             int count)
{
    int i, j, rc, start, end = 0, now, elapsed;
    tran_type *trans = NULL;
    unsigned long long repcalls, repbytes, flushes, explicit_flushes,
        interval_flushes;
    struct ireq iq;

    assert(op == 0);
    assert(count >= 0 && tcount >= 0);

    init_fake_ireq(thedb, &iq);
    iq.use_handle = thedb->bdb_env;
    rep_reset_send_callcount();
    rep_reset_send_bytecount();
    net_reset_num_flushes();
    net_reset_explicit_flushes();
    net_reset_send_interval_flushes();

    start = time_epoch();

    for (i = 0; i < tcount; i++) {
        /* trans_start changes to logical if gbl_rowlocks is set */
        if ((rc = trans_start_int(&iq, NULL, &trans, 0, 0)) != 0) {
            fprintf(stderr, "%s: error creating trans rc=%d\n", __func__, rc);
            return;
        }

        for (j = 0; j < count; j++) {
            if ((rc = ll_commit_bench(bdb_state, trans, op, 0, 0, NULL, 0)) !=
                0) {
                fprintf(stderr, "%s: ll_commit_bench returns %d\n", __func__,
                        rc);
                trans_abort(&iq, trans);
                return;
            }
        }

        if ((rc = trans_commit_adaptive(&iq, trans, gbl_mynode)) != 0) {
            fprintf(stderr, "%s: trans_commit returns %d\n", __func__, rc);
            return;
        }
    }

    end = time_epoch();
    elapsed = (end - start);

    repcalls = rep_get_send_callcount();
    repbytes = rep_get_send_bytecount();
    flushes = net_get_num_flushes();
    explicit_flushes = net_get_explicit_flushes();
    interval_flushes = net_get_send_interval_flushes();

    if (elapsed) {
        printf("Committed %d physical records in %d seconds, ", tcount * count,
               elapsed);
        printf("(%d records per second)\n", (tcount * count / elapsed));
    } else {
        printf("Committed %d records (0 seconds elapsed)\n", count);
    }

    printf("%llu rep-calls (%d per record), %llu rep-bytes (%d per record)\n",
           repcalls, (int)(repcalls / (tcount * count)), repbytes,
           (int)(repbytes / (tcount * count)));
    printf("%llu flushes (%d records/flush), %llu explict-flushes (%d "
           "records/flush), %llu interval-flushes (%d records/flush)\n",
           flushes, flushes ? (int)((tcount * count) / flushes) : 0,
           explicit_flushes,
           explicit_flushes ? (int)((tcount * count) / explicit_flushes) : 0,
           interval_flushes,
           interval_flushes ? (int)((tcount * count) / interval_flushes) : 0);

    return;
}

void rowlocks_clear_stats(void)
{
    rep_reset_send_callcount();
    rep_reset_send_bytecount();
    net_reset_num_flushes();
    net_reset_explicit_flushes();
    net_reset_send_interval_flushes();
}

void rowlocks_print_stats(FILE *f)
{
    unsigned long long repcalls, repbytes, flushes, explicit_flushes,
        interval_flushes;
    repcalls = rep_get_send_callcount();
    repbytes = rep_get_send_bytecount();
    flushes = net_get_num_flushes();
    explicit_flushes = net_get_explicit_flushes();
    interval_flushes = net_get_send_interval_flushes();

    fprintf(f, "%llu rep-calls\n", repcalls);
    fprintf(f, "%llu rep-bytes\n", repbytes);
    fprintf(f, "%llu flushes\n", flushes);
    fprintf(f, "%llu explicit-flushes\n", explicit_flushes);
    fprintf(f, "%llu interval-flushes\n", interval_flushes);
}

static void rowlocks_bench_int(bdb_state_type *bdb_state, int op, int count,
                               int phys_txns_per_logical)
{
    int i, j, rc, start, end = 0, now, elapsed, physcnt;
    unsigned long long repcalls, repbytes, flushes, explicit_flushes,
        interval_flushes;
    tran_type *trans = NULL;
    struct ireq iq;

    assert(op > 0);
    assert(count >= 1 && phys_txns_per_logical >= 1);

    init_fake_ireq(thedb, &iq);
    iq.use_handle = thedb->bdb_env;

    rep_reset_send_callcount();
    rep_reset_send_bytecount();
    net_reset_num_flushes();
    net_reset_explicit_flushes();
    net_reset_send_interval_flushes();

    start = time_epoch();

    for (i = 0; i < count; i++) {
        if ((rc = trans_start_logical(&iq, &trans)) != 0) {
            fprintf(stderr, "%s: error creating trans, rc=%d\n", __func__, rc);
            return;
        }

        for (j = 0; j < phys_txns_per_logical; j++) {
            if ((rc = ll_rowlocks_bench(thedb->bdb_env, trans, op, i, j, NULL,
                                        0)) != 0) {
                fprintf(stderr, "%s: ll_rowlocks_bench returns %d\n", __func__,
                        rc);
                trans_abort_logical(&iq, trans, NULL, 0, NULL, 0);
                return;
            }

            if ((rc = rowlocks_check_commit_physical(thedb->bdb_env, trans,
                                                     j)) != 0) {
                fprintf(stderr, "%s: %d: I don't think this should happen.  "
                                "Aborting.\n",
                        __func__, __LINE__);
                abort();
            }
        }

        /* This will wait for all nodes to respond */
        if ((rc = trans_commit_logical(&iq, trans, gbl_mynode, 0, 1, NULL, 0,
                                       iq.seq, iq.seqlen)) != 0) {
            fprintf(stderr, "%s: trans_commit_logical returns %d\n", 
                    __func__, rc);
            return;
        }

        trans = NULL;
    }

    end = time_epoch();
    elapsed = (end - start);

    repcalls = rep_get_send_callcount();
    repbytes = rep_get_send_bytecount();
    flushes = net_get_num_flushes();
    explicit_flushes = net_get_explicit_flushes();
    interval_flushes = net_get_send_interval_flushes();

    fprintf(stderr, "op=%d count=%d phys-txns/logical-txn=%d\n", op, count,
            phys_txns_per_logical);

    physcnt = (phys_txns_per_logical * count);

    if (elapsed) {
        printf("Committed %d physical records in %d seconds, ", physcnt,
               elapsed);
        printf("(%d records per second)\n", (physcnt / elapsed));
    } else {
        printf("Committed %d records (0 seconds elapsed)\n", physcnt);
    }

    printf("%llu rep-calls (%d per record), %llu rep-bytes (%d per record)\n",
           repcalls, (int)(repcalls / physcnt), repbytes,
           (int)(repbytes / physcnt));
    printf("%llu flushes (%d records/flush), %llu explict-flushes (%d "
           "records/flush), %llu interval-flushes (%d records/flush)\n",
           flushes, flushes ? (int)(physcnt / flushes) : 0, explicit_flushes,
           explicit_flushes ? (int)(physcnt / explicit_flushes) : 0,
           interval_flushes,
           interval_flushes ? (int)(physcnt / interval_flushes) : 0);

    return;
}

void rowlocks_bench(void *state, int lcount, int count)
{
    bdb_state_type *bdb_state = state;
    rowlocks_bench_int(bdb_state, ROWLOCKS_BENCH, lcount, count);
}

void rowlocks_lock1_bench(void *state, int lcount, int count)
{
    bdb_state_type *bdb_state = state;
    rowlocks_bench_int(bdb_state, ROWLOCKS_LOCK1_BENCH, lcount, count);
}

void rowlocks_lock2_bench(void *state, int lcount, int count)
{
    bdb_state_type *bdb_state = state;
    rowlocks_bench_int(bdb_state, ROWLOCKS_LOCK2_BENCH, lcount, count);
}

void commit_bench(void *state, int tcount, int count)
{
    bdb_state_type *bdb_state = state;
    commit_bench_int(bdb_state, COMMIT_BENCH, tcount, count);
}
