#include <inttypes.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>

#include <cdb2api.h>

#define N 64

static int n;
static cdb2_hndl_tp *hndl[N][2];
static const char *nodes[N];

static int run(cdb2_hndl_tp *h, const char *sql)
{
    fprintf(stderr, "sql:%s ->", sql);
    int rc;
    if ((rc = cdb2_run_statement(h, sql)) != CDB2_OK) {
        fprintf(stderr, " [err:%s]\n", cdb2_errstr(h));
        return -1;
    }
    while ((rc = cdb2_next_record(h)) == CDB2_OK) {
        for (int i = 0; i < cdb2_numcolumns(h); ++i) {
            switch (cdb2_column_type(h, i)) {
            case CDB2_INTEGER: fprintf(stderr, " %"PRId64, *(int64_t *)cdb2_column_value(h, i)); break;
            case CDB2_CSTRING: fprintf(stderr, " %s", (char *)cdb2_column_value(h, i)); break;
            default: abort();
            }
        }
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, " [err:%s]\n", cdb2_errstr(h));
        return -1;
    }
    fprintf(stderr, " [success]\n");
    return 0;
}

static int run0(const char *sql)
{
    int fail = 0;
    for (int i = 0; i < n; ++i) {
        if (run(hndl[i][0], sql) != 0)
            ++fail;
    }
    return fail;
}

static int run1(const char *sql)
{
    int fail = 0;
    for (int i = 0; i < n; ++i) {
        if (run(hndl[i][1], sql) != 0)
            ++fail;
    }
    return fail;
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);

    char *cluster = getenv("CLUSTER");
    if (cluster) {
        cluster = strdup(cluster);
        char *last;
        char *node = strtok_r(cluster, " ", &last);
        while (node) {
            nodes[n++] = node;
            node = strtok_r(NULL, " ", &last);
        }
    } else {
        n = 1;
        nodes[0] = "localhost";
    }

    fprintf(stderr, "cluster nodes:");
    for (int i = 0; i < n; ++i) {
        fprintf(stderr, " %s", nodes[i]);
    }
    fprintf(stderr, "\n");

    const char *cdb2_config = getenv("CDB2_CONFIG");
    if (cdb2_config) {
        fprintf(stderr, "cdb2_config:%s\n", cdb2_config);
        cdb2_set_comdb2db_config(cdb2_config);
    }

    const char *dbname = getenv("DBNAME");
    fprintf(stderr, "dbname:%s\n", dbname);

    for (int i = 0; i < n; ++i) {
        int rc0 = cdb2_open(&hndl[i][0], dbname, nodes[i], CDB2_DIRECT_CPU);
        int rc1 = cdb2_open(&hndl[i][1], dbname, nodes[i], CDB2_DIRECT_CPU);
        if (rc0 || rc1) {
            fprintf(stderr, "cdb2_open dbname:%s node:%s err:%s\n", dbname, nodes[i], cdb2_errstr(hndl[i][0]));
            fprintf(stderr, "cdb2_open dbname:%s node:%s err:%s\n", dbname, nodes[i], cdb2_errstr(hndl[i][1]));
            return EXIT_FAILURE;
        }
        fprintf(stderr, "cdb2_open dbname:%s node:%s success\n", dbname, nodes[i]);
    }


    /* Max outstanding connections: 1 (lrl.options) */
    if (run0("select comdb2_host(), 0") != 0) {
        fprintf(stderr, "failed test at line:%d\n", __LINE__);
        return EXIT_FAILURE;
    }
    /* close 0 and execute 1 */
    if (run1("select comdb2_host(), 1") != 0) {
        fprintf(stderr, "failed test at line:%d\n", __LINE__);
        return EXIT_FAILURE;
    }


    /* 0 is no longer eligible to be closed */
    if (run0("begin") != 0) {
        fprintf(stderr, "failed test at line:%d\n", __LINE__);
        return EXIT_FAILURE;
    }
    /* 1 should fail to run */
    if (run1("select comdb2_host(), 1") != n) {
        fprintf(stderr, "failed test at line:%d\n", __LINE__);
        return EXIT_FAILURE;
    }


    /* 0 can still run */
    if (run0("rollback") != 0) {
        fprintf(stderr, "failed test at line:%d\n", __LINE__);
        return EXIT_FAILURE;
    }
    /* Now, 1 can run as well */
    if (run1("select comdb2_host(), 1") != 0) {
        fprintf(stderr, "failed test at line:%d\n", __LINE__);
        return EXIT_FAILURE;
    }

    fprintf(stderr, "PASS\n");
    return EXIT_SUCCESS;
}
