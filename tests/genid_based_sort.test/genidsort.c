#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <time.h>

#include <cdb2api.h>

int main(int argc, char **argv)
{
    cdb2_hndl_tp *hndl = NULL;
    int rc, ninserts, nselects;
    char *conf = getenv("CDB2_CONFIG");
    void *data;
    unsigned long tm, delta11, delta12, delta13, delta21, delta22, delta23;
    const char *tier;

    if (argc < 2) {
        fprintf(stderr,
                "%s database [tier][ninserts][nselects]\n",
                argv[0]);
        exit(1);
    }

    if (argc > 2)
        tier = argv[2];
    else
        tier = "default";

    if (argc > 3)
        ninserts = atoi(argv[3]);
    else
        ninserts = 1024;

    if (argc > 3)
        nselects = atoi(argv[4]);
    else
        nselects = 10;

    if (conf != NULL)
        cdb2_set_comdb2db_config(conf);

    /* Open handle */
    rc = cdb2_open(&hndl, argv[1], tier, 0);
    if (rc != 0) {
        fprintf(stderr, "%d: Error opening handle: %d: %s.\n",
                __LINE__, rc, cdb2_errstr(hndl));
        exit(1);
    }

    /* Create table */
    rc = cdb2_run_statement(hndl, "create table t { tag ondisk { int i blob b } }");
    if (rc != 0) {
        fprintf(stderr, "%d: Error running query: %d: %s.\n",
                __LINE__, rc, cdb2_errstr(hndl));
        exit(1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "%d: Error next record: %d: %s.\n",
                __LINE__, rc, cdb2_errstr(hndl));
        exit(1);
    }

    int tot = ninserts;
    data = malloc(512 * 1024 + tot);

    while (ninserts-- > 0) {
        int key = rand();
        /* Bind params */
        cdb2_bind_param(hndl, "i", CDB2_INTEGER, &key, sizeof(key));

        /* Make it *not* a multiple of page size. */
        cdb2_bind_param(hndl, "b", CDB2_BLOB, data, 512 * 1024 + ninserts);

        /* insert */
        rc = cdb2_run_statement(hndl, "insert into t values(@i, @b)");
        if (rc != 0) {
            fprintf(stderr, "%d: Error running query: %d: %s.\n",
                    __LINE__, rc, cdb2_errstr(hndl));
            exit(1);
        }

        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "%d: Error next record: %d: %s.\n",
                    __LINE__, rc, cdb2_errstr(hndl));
            exit(1);
        }

        cdb2_clearbindings(hndl);
    }

    /* Warm up */
    rc = cdb2_run_statement(hndl, "select b from t order by i");
    if (rc != 0) {
        fprintf(stderr, "%d: Error running query: %d: %s.\n",
                __LINE__, rc, cdb2_errstr(hndl));
        exit(1);
    }

    int cnt;
    for (cnt = 0; cdb2_next_record(hndl) == CDB2_OK; ++cnt);
    if (cnt != tot) {
        fprintf(stderr, "%d: Expecting %d rows, got %d.\n",
                __LINE__, tot, cnt);
        exit(1);
    }

    /* Time sqlite sort */
    tm = (unsigned long)time(NULL);
    int i;
    for (i = 0; i != nselects; ++i) {
        rc = cdb2_run_statement(hndl, "select b from t order by i");
        if (rc != 0) {
            fprintf(stderr, "%d: Error running query: %d: %s.\n",
                    __LINE__, rc, cdb2_errstr(hndl));
            exit(1);
        }
        while (cdb2_next_record(hndl) == CDB2_OK);
    }
    delta11 = ((unsigned long)time(NULL)) - tm;

    tm = (unsigned long)time(NULL);
    for (i = 0; i != nselects; ++i) {
        rc = cdb2_run_statement(hndl, "select b from t order by i limit 10");
        if (rc != 0) {
            fprintf(stderr, "%d: Error running query: %d: %s.\n",
                    __LINE__, rc, cdb2_errstr(hndl));
            exit(1);
        }
        while (cdb2_next_record(hndl) == CDB2_OK);
    }
    delta12 = ((unsigned long)time(NULL)) - tm;

    tm = (unsigned long)time(NULL);
    for (i = 0; i != nselects; ++i) {
        rc = cdb2_run_statement(hndl, "select b from t group by i");
        if (rc != 0) {
            fprintf(stderr, "%d: Error running query: %d: %s.\n",
                    __LINE__, rc, cdb2_errstr(hndl));
            exit(1);
        }
        while (cdb2_next_record(hndl) == CDB2_OK);
    }
    delta13 = ((unsigned long)time(NULL)) - tm;

    rc = cdb2_run_statement(hndl,
            "exec procedure sys.cmd.send('setsqlattr genidsort_sz_thresh 4096')");
    for (; cdb2_next_record(hndl) == CDB2_OK; );

    /* Time genid sort */
    tm = (unsigned long)time(NULL);
    for (i = 0; i != nselects; ++i) {
        rc = cdb2_run_statement(hndl, "select b from t order by i");
        if (rc != 0) {
            fprintf(stderr, "%d: Error running query: %d: %s.\n",
                    __LINE__, rc, cdb2_errstr(hndl));
            exit(1);
        }
        while (cdb2_next_record(hndl) == CDB2_OK);
    }
    delta21 = ((unsigned long)time(NULL)) - tm;

    tm = (unsigned long)time(NULL);
    for (i = 0; i != nselects; ++i) {
        rc = cdb2_run_statement(hndl, "select b from t order by i limit 10");
        if (rc != 0) {
            fprintf(stderr, "%d: Error running query: %d: %s.\n",
                    __LINE__, rc, cdb2_errstr(hndl));
            exit(1);
        }
        while (cdb2_next_record(hndl) == CDB2_OK);
    }
    delta22 = ((unsigned long)time(NULL)) - tm;

    tm = (unsigned long)time(NULL);
    for (i = 0; i != nselects; ++i) {
        rc = cdb2_run_statement(hndl, "select b from t group by i");
        if (rc != 0) {
            fprintf(stderr, "%d: Error running query: %d: %s.\n",
                    __LINE__, rc, cdb2_errstr(hndl));
            exit(1);
        }
        while (cdb2_next_record(hndl) == CDB2_OK);
    }
    delta23 = ((unsigned long)time(NULL)) - tm;

    free(data);
    cdb2_close(hndl);

    fprintf(stderr, "sqlite order-by runtime %lu seconds.\n", delta11);
    fprintf(stderr, "genid  order-by runtime %lu seconds.\n", delta21);
    fprintf(stderr, "sqlite order-by with limit runtime %lu seconds.\n", delta12);
    fprintf(stderr, "genid  order-by with limit runtime %lu seconds.\n", delta22);
    fprintf(stderr, "sqlite group-by runtime %lu seconds.\n", delta13);
    fprintf(stderr, "genid  group-by runtime %lu seconds.\n", delta23);

    if (delta11 <= delta21 || delta12 <= delta22 || delta13 <= delta23) {
        fprintf(stderr,
                "genid sort is supposed to be faster than sqlite sort\n");
        return 1;
    }

    return 0;
}
