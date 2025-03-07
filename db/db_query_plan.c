/*
   Copyright 2022 Bloomberg Finance L.P.

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

#include "sql.h"
#include "tohex.h"
#include "string_ref.h"

#include <math.h>
#include <ctrace.h>
#include <inttypes.h>
#include <sqlexplain.h>
#include <vdbeInt.h>

int gbl_query_plan_max_plans = 20;
extern double gbl_query_plan_percentage;
extern int gbl_sample_queries;
extern hash_t *gbl_fingerprint_hash;
extern pthread_mutex_t gbl_fingerprint_hash_mu;

// return NULL if no plan
struct string_ref *form_query_plan(sqlite3_stmt *stmt)
{
    struct string_ref *query_plan_ref;
    Op *op;
    struct cursor_info c;
    Vdbe *v = (Vdbe *)stmt;
    char *operation;

    if (!v)
        return NULL;

    struct strbuf *query_plan_buf = strbuf_new();
    for (int pc = 0; pc < v->nOp; pc++) {
        op = &v->aOp[pc];
        if (op->opcode == OP_OpenRead)
            operation = "read";
        else if (op->opcode == OP_ReopenIdx)
            operation = "(re)read";
        else if (op->opcode == OP_OpenRead_Record)
            operation = "read";
        else if (op->opcode == OP_OpenWrite)
            operation = "write";
        else
            continue;

        if (strbuf_len(query_plan_buf) > 0)
            strbuf_append(query_plan_buf, ", ");

        strbuf_appendf(query_plan_buf, "open %s cursor on ", operation);
        describe_cursor(v, pc, &c);
        print_cursor_description(query_plan_buf, &c, 0);
    }

    query_plan_ref = strbuf_len(query_plan_buf) > 0 ? create_string_ref((char *)strbuf_buf(query_plan_buf)) : NULL;
    strbuf_free(query_plan_buf);
    return query_plan_ref;
}

// assumed to have fingerprint lock
// assume t->query_plan_hash is not NULL
void add_query_plan(int64_t cost, int64_t nrows, struct fingerprint_track *t, struct string_ref *zSql_ref,
                    struct string_ref *query_plan_ref, unsigned char *plan_fingerprint, char *params)
{
    if (nrows < 0) {
        return;
    } else if (nrows == 0) { // can't calculate cost per row if 0 rows, make 1 row
        nrows = 1;
    }

    double current_cost_per_row = (double)cost / nrows;

    struct query_plan_item *q = hash_find(t->query_plan_hash, plan_fingerprint);
    char fp[FINGERPRINTSZ * 2 + 1]; /* 16 ==> 33 */
    *fp = '\0';
    if (q == NULL) {
        /* make sure we haven't generated an unreasonable number of these */
        int nents = hash_get_num_entries(t->query_plan_hash);
        if (nents >= gbl_query_plan_max_plans) {
            if (t->alert_once_query_plan_max) {
                util_tohex(fp, (char *)t->fingerprint, FINGERPRINTSZ);
                logmsg(LOGMSG_WARN,
                       "Stopped tracking query plans for query %s with fingerprint %s, hit max #plans %d.\n",
                       t->zNormSql, fp, gbl_query_plan_max_plans);
                t->alert_once_query_plan_max = 0;
            }
            return;
        } else {
            q = calloc(1, sizeof(struct query_plan_item));
            memcpy(q->plan_fingerprint, plan_fingerprint, FINGERPRINTSZ);
            q->plan_ref = query_plan_ref ? get_ref(query_plan_ref) : NULL;
            q->total_cost_per_row = current_cost_per_row;
            q->nexecutions = 1;
            q->alert_once_cost = 1;
            q->avg_cost_per_row = q->total_cost_per_row / q->nexecutions;
            hash_add(t->query_plan_hash, q);
        }
    } else {
        double prev_avg = q->avg_cost_per_row;
        q->total_cost_per_row += current_cost_per_row;
        q->nexecutions++;
        q->avg_cost_per_row = q->total_cost_per_row / q->nexecutions;
        if (fabs(q->avg_cost_per_row - prev_avg) > 0.01) {
            q->alert_once_cost = 1; // reset since avg cost changed significantly
        }
    }

    // add to queries sample if there exists a plan
    if (gbl_sample_queries && q->plan_ref)
        add_query_to_samples_queries(t->fingerprint, q->plan_fingerprint, zSql_ref, q->plan_ref, params);

    // compare query plans
    if (!query_plan_ref)
        return;

    double current_avg = q->avg_cost_per_row;
    void *ent;
    unsigned int bkt;
    double max_diff = 0;
    double current_diff;
    double significance = 1 + gbl_query_plan_percentage / 100;
    for (q = (struct query_plan_item *)hash_first(t->query_plan_hash, &ent, &bkt); q;
         q = (struct query_plan_item *)hash_next(t->query_plan_hash, &ent, &bkt)) {
        if (q->plan_ref &&
            q->avg_cost_per_row * significance < current_avg) { // should be at least equal if same query plan
            current_diff = current_avg - q->avg_cost_per_row;
            if (t->alert_once_query_plan && current_diff > max_diff)
                max_diff = current_diff;

            if (q->alert_once_cost) { // Only log query plan cost differences once per query plan in trace, but reset if the avg cost changes
                if (!*fp)
                    util_tohex(fp, (char *)t->fingerprint, FINGERPRINTSZ);

                ctrace("For query %s with fingerprint %s:\n"
                       "Currently using query plan %s, which has an average cost per row of %f.\n"
                       "But query plan %s has a lower average cost per row of %f.\n",
                       t->zNormSql, fp, string_ref_cstr(query_plan_ref), current_avg, string_ref_cstr(q->plan_ref),
                       q->avg_cost_per_row);

                q->alert_once_cost = 0;
            }
        }
    }

    if (max_diff > 0 && t->alert_once_query_plan) { // only print once per fingerprint (even if avg cost changes), and print the max diff plan only
        if (!*fp)
            util_tohex(fp, (char *)t->fingerprint, FINGERPRINTSZ);

        logmsg(LOGMSG_WARN,
               "Possible better plan available for fingerprint %s, avg cost per row difference: %f, current # rows: %" PRId64 "\n",
               fp, max_diff, nrows);
        t->alert_once_query_plan = 0;
    }
}

// assumed to have fingerprint lock
int free_query_plan_hash(hash_t *query_plan_hash)
{
    int plans_count;
    void *ent;
    unsigned int bkt;
    struct query_plan_item *q;

    // update plans count
    hash_info(query_plan_hash, NULL, NULL, NULL, NULL, &plans_count, NULL, NULL);

    // free query plan hash
    for (q = (struct query_plan_item *)hash_first(query_plan_hash, &ent, &bkt); q;
         q = (struct query_plan_item *)hash_next(query_plan_hash, &ent, &bkt)) {
        if (q->plan_ref)
            put_ref(&q->plan_ref);
        free(q);
    }
    hash_clear(query_plan_hash);
    hash_free(query_plan_hash);

    return plans_count;
}

int clear_query_plans()
{
    int plans_count = 0;
    Pthread_mutex_lock(&gbl_fingerprint_hash_mu);
    if (!gbl_fingerprint_hash) {
        Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
        return plans_count;
    }

    void *ent;
    unsigned int bkt;
    struct fingerprint_track *f;
    for (f = (struct fingerprint_track *)hash_first(gbl_fingerprint_hash, &ent, &bkt); f;
         f = (struct fingerprint_track *)hash_next(gbl_fingerprint_hash, &ent, &bkt)) {
        if (f->query_plan_hash) {
            plans_count += free_query_plan_hash(f->query_plan_hash);
            f->query_plan_hash = NULL;
            f->alert_once_query_plan = 1;
            f->alert_once_query_plan_max = 1;
        }
    }

    Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
    return plans_count;
}
