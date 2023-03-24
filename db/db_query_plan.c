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

#include <math.h>
#include <ctrace.h>

int gbl_query_plan_max_plans = 20;
extern double gbl_query_plan_percentage;
extern hash_t *gbl_fingerprint_hash;
extern pthread_mutex_t gbl_fingerprint_hash_mu;

static char *form_query_plan(const struct client_query_stats *query_stats)
{
    struct strbuf *query_plan_buf;
    const struct client_query_path_component *c;
    char *query_plan;

    if (query_stats->n_components == 0) {
        return NULL;
    }

    query_plan_buf = strbuf_new();
    for (int i = 0; i < query_stats->n_components; i++) {
        if (i > 0) {
            strbuf_append(query_plan_buf, ", ");
        }
        c = &query_stats->path_stats[i];
        strbuf_appendf(query_plan_buf, "table %s index %d", c->table, c->ix);
    }

    query_plan = strdup((char *)strbuf_buf(query_plan_buf));
    strbuf_free(query_plan_buf);
    return query_plan;
}

// assumed to have fingerprint lock
// assume t->query_plan_hash is not NULL
static void add_query_plan_int(struct fingerprint_track *t, const char *query_plan, int64_t cost, int64_t nrows)
{
    double current_cost_per_row = (double)cost / nrows;
    struct query_plan_item *q = hash_find(t->query_plan_hash, &query_plan);
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
            q->plan = strdup(query_plan);
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

    // compare query plans
    double current_avg = q->avg_cost_per_row;
    void *ent;
    unsigned int bkt;
    double max_diff = 0;
    double current_diff;
    double significance = 1 + gbl_query_plan_percentage / 100;
    for (q = (struct query_plan_item *)hash_first(t->query_plan_hash, &ent, &bkt); q;
         q = (struct query_plan_item *)hash_next(t->query_plan_hash, &ent, &bkt)) {
        if (q->avg_cost_per_row * significance < current_avg) { // should be at least equal if same query plan
            current_diff = current_avg - q->avg_cost_per_row;
            if (t->alert_once_query_plan && current_diff > max_diff)
                max_diff = current_diff;

            if (q->alert_once_cost) { // Only log query plan cost differences once per query plan in trace, but reset if the avg cost changes
                if (!*fp)
                    util_tohex(fp, (char *)t->fingerprint, FINGERPRINTSZ);

                ctrace("For query %s with fingerprint %s:\n"
                       "Currently using query plan %s, which has an average cost per row of %f.\n"
                       "But query plan %s has a lower average cost per row of %f.\n",
                       t->zNormSql, fp, query_plan, current_avg, q->plan, q->avg_cost_per_row);

                q->alert_once_cost = 0;
            }
        }
    }

    if (max_diff > 0 && t->alert_once_query_plan) { // only print once per fingerprint (even if avg cost changes), and print the max diff plan only
        if (!*fp)
            util_tohex(fp, (char *)t->fingerprint, FINGERPRINTSZ);

        logmsg(LOGMSG_WARN,
               "Possible better plan available for fingerprint %s, avg cost per row difference: %f, current # rows: %ld\n",
               fp, max_diff, nrows);
        t->alert_once_query_plan = 0;
    }
}

// assumed to have fingerprint lock
// assume t->query_plan_hash is not NULL
void add_query_plan(const struct client_query_stats *query_stats, int64_t cost, int64_t nrows,
                    struct fingerprint_track *t)
{
    char *query_plan = form_query_plan(query_stats);
    if (!query_plan || nrows <= 0) { // can't calculate cost per row if 0 rows
        return;
    }

    add_query_plan_int(t, query_plan, cost, nrows);
    free(query_plan);
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
        free(q->plan);
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
