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

int gbl_query_plan_max_plans = 20;
extern int gbl_debug_print_query_plans;
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
static void add_query_plan_int(struct fingerprint_track *t, const char *query_plan, double current_cost_per_row)
{
    struct query_plan_item *q = hash_find(t->query_plan_hash, &query_plan);
    char fp[FINGERPRINTSZ * 2 + 1]; /* 16 ==> 33 */
    if (q == NULL) {
        /* make sure we haven't generated an unreasonable number of these */
        int nents = hash_get_num_entries(t->query_plan_hash);
        if (nents >= gbl_query_plan_max_plans) {
            if (t->alert_once_query_plan) {
                util_tohex(fp, (char *)t->fingerprint, FINGERPRINTSZ);
                logmsg(LOGMSG_WARN,
                       "Stopped tracking query plans for query %s with fingerprint %s, hit max #plans %d.\n",
                       t->zNormSql, fp, gbl_query_plan_max_plans);
                t->alert_once_query_plan = 0;
            }
            return;
        } else {
            q = calloc(1, sizeof(struct query_plan_item));
            q->plan = strdup(query_plan);
            q->total_cost_per_row = current_cost_per_row;
            q->nexecutions = 1;
            hash_add(t->query_plan_hash, q);
        }
    } else {
        q->total_cost_per_row += current_cost_per_row;
        q->nexecutions++;
    }

    // compare query plans
    double average_cost_per_row = q->total_cost_per_row / q->nexecutions;
    void *ent;
    unsigned int bkt;
    double alt_avg;
    double significance = 1 + gbl_query_plan_percentage / 100;
    for (q = (struct query_plan_item *)hash_first(t->query_plan_hash, &ent, &bkt); q;
         q = (struct query_plan_item *)hash_next(t->query_plan_hash, &ent, &bkt)) {
        alt_avg = q->total_cost_per_row / q->nexecutions;
        if (alt_avg * significance < average_cost_per_row) { // should be at least equal if same query plan
            util_tohex(fp, (char *)t->fingerprint, FINGERPRINTSZ);
            logmsg(LOGMSG_WARN,
                   "For query %s with fingerprint %s:\n"
                   "Currently using query plan %s, which has an average cost per row of %f.\n"
                   "But query plan %s has a lower average cost per row of %f.\n",
                   t->zNormSql, fp, query_plan, average_cost_per_row, q->plan, alt_avg);
        }
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

    double current_cost_per_row = (double)cost / nrows;
    add_query_plan_int(t, query_plan, current_cost_per_row);

    if (gbl_debug_print_query_plans) {
        void *ent, *ent2;
        unsigned int bkt, bkt2;
        struct query_plan_item *q;
        struct fingerprint_track *f;
        logmsg(LOGMSG_WARN, "START\n");
        for (f = (struct fingerprint_track *)hash_first(gbl_fingerprint_hash, &ent, &bkt); f;
             f = (struct fingerprint_track *)hash_next(gbl_fingerprint_hash, &ent, &bkt)) {
            if (!f->query_plan_hash) {
                continue;
            }
            logmsg(LOGMSG_WARN, "QUERY: %s\n", f->zNormSql);
            for (q = (struct query_plan_item *)hash_first(f->query_plan_hash, &ent2, &bkt2); q;
                 q = (struct query_plan_item *)hash_next(f->query_plan_hash, &ent2, &bkt2)) {
                logmsg(LOGMSG_WARN, "plan: %s, total cost per row: %f, num executions: %d, average: %f\n", q->plan,
                       q->total_cost_per_row, q->nexecutions, q->total_cost_per_row / q->nexecutions);
            }
        }
        logmsg(LOGMSG_WARN, "END\n\n");
    }

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
        }
    }

    Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
    return plans_count;
}
