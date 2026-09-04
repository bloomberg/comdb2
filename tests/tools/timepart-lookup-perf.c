/*
 * Point-lookup latency driver.
 *
 * Runs a configurable number of single-row lookups of the form
 *
 *     SELECT security_identifier, classification_value FROM <table>
 *      WHERE security_identifier = ?
 *
 * against one connection, and reports the latency distribution as a single
 * METRIC line.
 *
 * The identifiers are read from a file rather than picked here on purpose: an
 * A/B run drives several arms and every arm has to issue the same keys in the
 * same order, so the only thing varying between arms is the database side.
 */
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <cdb2api.h>

static char *dbname;
static char *tier = "default";
static char *table = "classifications";
static char *idfile;
static char *label = "arm";
static int niters;  /* 0 => one pass over the identifier list */
static int nwarmup; /* not measured, just pulls pages/plans into cache */

static char **ids;
static int nids;

static void usage(const char *prog)
{
    fprintf(stderr,
            "usage: %s <dbname> [tier] -f <idfile> [-t table] [-n iters] [-w warmup] [-l label]\n"
            "  -f  file of identifiers, one per line (required)\n"
            "  -t  table (or time-partition) to query, default 'classifications'\n"
            "  -n  lookups to time, default = number of identifiers in -f\n"
            "  -w  unmeasured warmup lookups to run first, default 0\n"
            "  -l  label echoed back on the METRIC line\n",
            prog);
    exit(1);
}

/* One identifier per line.  Blank lines are skipped so the caller can pipe
 * cdb2sql output in without having to scrub it. */
static void load_ids(void)
{
    FILE *f = fopen(idfile, "r");
    if (!f) {
        fprintf(stderr, "cannot open %s: %s\n", idfile, strerror(errno));
        exit(1);
    }

    int cap = 1024;
    ids = malloc(cap * sizeof(*ids));
    if (!ids) {
        fprintf(stderr, "out of memory\n");
        exit(1);
    }

    char *line = NULL;
    size_t linecap = 0;
    ssize_t len;
    while ((len = getline(&line, &linecap, f)) != -1) {
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
            line[--len] = '\0';
        if (len == 0)
            continue;
        if (nids == cap) {
            cap *= 2;
            char **grown = realloc(ids, cap * sizeof(*ids));
            if (!grown) {
                fprintf(stderr, "out of memory\n");
                exit(1);
            }
            ids = grown;
        }
        ids[nids] = strdup(line);
        if (!ids[nids]) {
            fprintf(stderr, "out of memory\n");
            exit(1);
        }
        nids++;
    }
    free(line);
    fclose(f);

    if (nids == 0) {
        fprintf(stderr, "%s has no identifiers\n", idfile);
        exit(1);
    }
}

static cdb2_hndl_tp *connect_db(void)
{
    cdb2_hndl_tp *db;
    int rc = cdb2_open(&db, dbname, tier, 0);
    if (rc != 0) {
        fprintf(stderr, "cdb2_open db:%s tier:%s rc:%d %s\n", dbname, tier, rc, cdb2_errstr(db));
        exit(1);
    }
    return db;
}

/* Returns rows fetched, or -1 on error.  The whole result has to be drained
 * before the handle can be reused, so the row loop runs even when the caller
 * only cares about the timing. */
static int one_lookup(cdb2_hndl_tp *db, const char *sql, const char *id)
{
    int rc = cdb2_bind_index(db, 1, CDB2_CSTRING, id, strlen(id));
    if (rc != 0) {
        fprintf(stderr, "cdb2_bind_index rc:%d %s\n", rc, cdb2_errstr(db));
        return -1;
    }

    rc = cdb2_run_statement(db, sql);
    if (rc != 0) {
        fprintf(stderr, "cdb2_run_statement id:%s rc:%d %s\n", id, rc, cdb2_errstr(db));
        cdb2_clearbindings(db);
        return -1;
    }

    int rows = 0;
    while ((rc = cdb2_next_record(db)) == CDB2_OK)
        rows++;

    cdb2_clearbindings(db);

    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "cdb2_next_record id:%s rc:%d %s\n", id, rc, cdb2_errstr(db));
        return -1;
    }
    return rows;
}

static int cmp_int64(const void *a, const void *b)
{
    int64_t x = *(const int64_t *)a, y = *(const int64_t *)b;
    return (x > y) - (x < y);
}

/* pct is 0..100; the array is already sorted ascending and n > 0. */
static int64_t pctile(const int64_t *sorted, int n, int pct)
{
    int idx = (int)(((int64_t)n * pct) / 100);
    if (idx >= n)
        idx = n - 1;
    return sorted[idx];
}

static int64_t now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

int main(int argc, char **argv)
{
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    if (argc < 2)
        usage(argv[0]);
    dbname = argv[1];

    /* An optional bare tier can follow the dbname, matching the other tools
     * here; everything after that is flags. */
    int argstart = 2;
    if (argc > 2 && argv[2][0] != '-') {
        tier = argv[2];
        argstart = 3;
    }

    optind = argstart;
    int c;
    while ((c = getopt(argc, argv, "f:t:n:w:l:")) != -1) {
        switch (c) {
        case 'f': idfile = optarg; break;
        case 't': table = optarg; break;
        case 'n': niters = atoi(optarg); break;
        case 'w': nwarmup = atoi(optarg); break;
        case 'l': label = optarg; break;
        default: usage(argv[0]);
        }
    }

    if (!idfile)
        usage(argv[0]);

    load_ids();
    if (niters <= 0)
        niters = nids;

    char sql[512];
    snprintf(sql, sizeof(sql), "SELECT security_identifier, classification_value FROM \"%s\" WHERE security_identifier = ?",
             table);

    int64_t *lat = malloc(niters * sizeof(*lat));
    if (!lat) {
        fprintf(stderr, "out of memory\n");
        exit(1);
    }

    cdb2_hndl_tp *db = connect_db();

    for (int i = 0; i < nwarmup; i++)
        one_lookup(db, sql, ids[i % nids]);

    int64_t rows = 0;
    int errors = 0, measured = 0;
    int64_t wall_start = now_us();
    for (int i = 0; i < niters; i++) {
        const char *id = ids[i % nids];
        int64_t t0 = now_us();
        int n = one_lookup(db, sql, id);
        int64_t t1 = now_us();
        if (n < 0) {
            errors++;
            continue;
        }
        rows += n;
        lat[measured++] = t1 - t0;
    }
    int64_t wall_us = now_us() - wall_start;

    cdb2_close(db);

    if (measured == 0) {
        printf("METRIC label=%s n=%d rows=0 errors=%d (every lookup failed)\n", label, niters, errors);
        free(lat);
        return 1;
    }

    int64_t sum = 0;
    for (int i = 0; i < measured; i++)
        sum += lat[i];
    qsort(lat, measured, sizeof(*lat), cmp_int64);

    printf("METRIC label=%s n=%d rows=%" PRId64 " errors=%d total_ms=%.1f avg_us=%.1f min_us=%" PRId64
           " p50_us=%" PRId64 " p95_us=%" PRId64 " p99_us=%" PRId64 " max_us=%" PRId64 " qps=%.1f\n",
           label, measured, rows, errors, wall_us / 1000.0, (double)sum / measured, lat[0], pctile(lat, measured, 50),
           pctile(lat, measured, 95), pctile(lat, measured, 99), lat[measured - 1],
           wall_us ? measured * 1000000.0 / wall_us : 0.0);

    free(lat);
    for (int i = 0; i < nids; i++)
        free(ids[i]);
    free(ids);
    return errors ? 1 : 0;
}
