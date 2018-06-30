#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <time.h>

#include <cdb2api.h>

int main(int argc, char **argv)
{
    cdb2_hndl_tp *hndl = NULL;
    int rc, i, len;
    char *conf = getenv("CDB2_CONFIG");
    unsigned long tm, delta1, delta2, delta3;
    const char *tier;

    if (argc < 2)
        exit(1);

    if (argc > 2)
        tier = argv[2];
    else
        tier = "default";

    if (argc > 3)
        len = atoi(argv[3]);
    else
        len = 2000;

    if (conf != NULL)
        cdb2_set_comdb2db_config(conf);

    /* The race is on! first let's test no ssl cache. */
    for (i = 0, tm = (unsigned long)time(NULL); i < len; i += 4) {
        if (i > 0 && i % 100 == 0)
            fprintf(stderr, "progress: %d\n", i);
        /* 1st */
        rc = cdb2_open(&hndl, argv[1], tier, CDB2_RANDOM);
        if (rc != 0) {
            fprintf(stderr, "Error opening a handle: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }

        rc = cdb2_run_statement(hndl, "select 1");
        if (rc != 0) {
            fprintf(stderr, "Error running query: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "Error next record: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        cdb2_close(hndl);

        /* 2nd */
        rc = cdb2_open(&hndl, argv[1], tier, CDB2_RANDOM);
        if (rc != 0) {
            fprintf(stderr, "Error opening a handle: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }

        rc = cdb2_run_statement(hndl, "select 1");
        if (rc != 0) {
            fprintf(stderr, "Error running query: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "Error next record: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        cdb2_close(hndl);

        /* 3rd */
        rc = cdb2_open(&hndl, argv[1], tier, CDB2_RANDOM);
        if (rc != 0) {
            fprintf(stderr, "Error opening a handle: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }

        rc = cdb2_run_statement(hndl, "select 1");
        if (rc != 0) {
            fprintf(stderr, "Error running query: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "Error next record: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        cdb2_close(hndl);

        /* 4th */
        rc = cdb2_open(&hndl, argv[1], tier, CDB2_RANDOM);
        if (rc != 0) {
            fprintf(stderr, "Error opening a handle: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }

        rc = cdb2_run_statement(hndl, "select 1");
        if (rc != 0) {
            fprintf(stderr, "Error running query: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "Error next record: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        cdb2_close(hndl);
    }
    delta1 = ((unsigned long)time(NULL)) - tm;
    fprintf(stderr, "Without SSL session cache: %lu.\n", delta1);

    /* Now test ssl cache */
    setenv("SSL_SESSION_CACHE", "1", 1);
    for (i = 0, tm = (unsigned long)time(NULL); i < len; i += 4) {
        if (i > 0 && i % 100 == 0)
            fprintf(stderr, "progress: %d\n", i);

        /* 1st */
        rc = cdb2_open(&hndl, argv[1], tier, CDB2_RANDOM);
        if (rc != 0) {
            fprintf(stderr, "Error opening a handle: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }

        rc = cdb2_run_statement(hndl, "select 1");
        if (rc != 0) {
            fprintf(stderr, "Error running query: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "Error next record: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        cdb2_close(hndl);

        /* 2nd */
        rc = cdb2_open(&hndl, argv[1], tier, CDB2_RANDOM);
        if (rc != 0) {
            fprintf(stderr, "Error opening a handle: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }

        rc = cdb2_run_statement(hndl, "select 1");
        if (rc != 0) {
            fprintf(stderr, "Error running query: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "Error next record: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        cdb2_close(hndl);

        /* 3rd */
        rc = cdb2_open(&hndl, argv[1], tier, CDB2_RANDOM);
        if (rc != 0) {
            fprintf(stderr, "Error opening a handle: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }

        rc = cdb2_run_statement(hndl, "select 1");
        if (rc != 0) {
            fprintf(stderr, "Error running query: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "Error next record: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        cdb2_close(hndl);

        /* 4th */
        rc = cdb2_open(&hndl, argv[1], tier, CDB2_RANDOM);
        if (rc != 0) {
            fprintf(stderr, "Error opening a handle: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }

        rc = cdb2_run_statement(hndl, "select 1");
        if (rc != 0) {
            fprintf(stderr, "Error running query: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "Error next record: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        cdb2_close(hndl);
    }

    delta2 = ((unsigned long)time(NULL)) - tm;
    fprintf(stderr, "With SSL session cache:    %lu.\n", delta2);
    if (delta2 == 0)
        fprintf(stderr, "delta1/delta2 == inf\n");
    else
        fprintf(stderr, "delta1/delta2 == %2.2f%%\n", delta1 * 100.0 / delta2);


    /* Now test "SET SSL_SESSION_CACHE ON" */
    setenv("SSL_SESSION_CACHE", "0", 1);
    for (i = 0, tm = (unsigned long)time(NULL); i < len; i += 4) {
        if (i > 0 && i % 100 == 0)
            fprintf(stderr, "progress: %d\n", i);

        /* 1st */
        rc = cdb2_open(&hndl, argv[1], tier, CDB2_RANDOM);
        if (rc != 0) {
            fprintf(stderr, "Error opening a handle: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }

        rc = cdb2_run_statement(hndl, "SET SSL_SESSION_CACHE ON");
        rc = cdb2_run_statement(hndl, "select 1");
        if (rc != 0) {
            fprintf(stderr, "Error running query: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "Error next record: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        cdb2_close(hndl);

        /* 2nd */
        rc = cdb2_open(&hndl, argv[1], tier, CDB2_RANDOM);
        if (rc != 0) {
            fprintf(stderr, "Error opening a handle: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }

        rc = cdb2_run_statement(hndl, "SET SSL_SESSION_CACHE ON");
        rc = cdb2_run_statement(hndl, "select 1");
        if (rc != 0) {
            fprintf(stderr, "Error running query: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "Error next record: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        cdb2_close(hndl);

        /* 3rd */
        rc = cdb2_open(&hndl, argv[1], tier, CDB2_RANDOM);
        if (rc != 0) {
            fprintf(stderr, "Error opening a handle: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }

        rc = cdb2_run_statement(hndl, "SET SSL_SESSION_CACHE ON");
        rc = cdb2_run_statement(hndl, "select 1");
        if (rc != 0) {
            fprintf(stderr, "Error running query: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "Error next record: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        cdb2_close(hndl);

        /* 4th */
        rc = cdb2_open(&hndl, argv[1], tier, CDB2_RANDOM);
        if (rc != 0) {
            fprintf(stderr, "Error opening a handle: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }

        rc = cdb2_run_statement(hndl, "SET SSL_SESSION_CACHE ON");
        rc = cdb2_run_statement(hndl, "select 1");
        if (rc != 0) {
            fprintf(stderr, "Error running query: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "Error next record: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        cdb2_close(hndl);
    }

    delta3 = ((unsigned long)time(NULL)) - tm;
    fprintf(stderr, "SET SSL_SESSION_CACHE ON:  %lu.\n", delta3);
    if (delta3 == 0)
        fprintf(stderr, "delta1/delta3 == inf\n");
    else
        fprintf(stderr, "delta1/delta3 == %2.2f%%\n", delta1 * 100.0 / delta3);

    return (delta2 > delta1) && (delta3 > delta1);
}
