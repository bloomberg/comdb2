#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <assert.h>
#include <stdint.h>
#include <unistd.h>

#include <cdb2api.h>

static char *argv0 = NULL;

void usage(FILE *f)
{
    fprintf(f, "Usage: %s <cmd-line>\n", argv0);
    fprintf(f, "     -d <dbname>         -   set dbname.\n");
    fprintf(
        f, "     -o                  -   for occ selectv test (skip rcode).\n");
    fprintf(f, "     -r                  -   read committed mode.\n");
    fprintf(f, "     -m <retries>        -   set max-retries.\n");
    fprintf(f, "     -D                  -   enable debug trace.\n");
    fprintf(f, "     -t <num>            -   set num of threads.\n");
    fprintf(f, "     -h                  -   this menu.\n");
    exit(1);
}

typedef struct config {
    cdb2_hndl_tp *hndl;
    char *dbname;
    int threads;
    int recom;
    int occ;
} config_t;

static config_t *default_config(void)
{
    config_t *c = malloc(sizeof(*c));
    bzero(c, sizeof(*c));
    c->threads = 1;
    c->recom = 0;
    c->occ = 0;
    return c;
}

unsigned int myrand(void)
{
    static int first = 1;
    static unsigned int seed;
    static unsigned int adds;

    if (first) {
        seed = time(NULL);
        adds = (unsigned int)pthread_self();
        first = 0;
    }

    seed = (seed << 7) ^ ((seed >> 25) + adds);
    adds = (adds << 7) ^ ((adds >> 25) + 0xbabeface);

    return seed;
}

typedef struct update_thread {
    config_t *c;
    int thd;
} update_thread_t;

void *schedule_thd(void *arg)
{
    update_thread_t *u = (update_thread_t *)arg;
    config_t *c = u->c;
    cdb2_hndl_tp *sqlh;
    char sql[1024];
    int ret, type;
    int64_t *ll, instid;
    char *ch;
    int64_t host = u->thd + 1;

    snprintf(sql, sizeof(sql), "selectv_thd%ld.log", host);
    FILE *f = fopen(sql, "w");
    if (f == NULL) {
        fprintf(stderr, "%s:%d Error opening log file %s\n", __func__, __LINE__,
                sql);
        exit(1);
    }

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);
    if ((ret = cdb2_open(&sqlh, c->dbname, "default", 0)) != 0) {
        fprintf(f, "%s:%d Error getting sql handle, ret=%d\n", __func__,
                __LINE__, ret);
        fprintf(stderr, "%s:%d Error getting sql handle, ret=%d\n", __func__,
                __LINE__, ret);
        exit(1);
    }

    cdb2_set_debug_trace(sqlh);

    if (c->recom) {
        snprintf(sql, sizeof(sql), "set transaction read committed");
        if ((ret = cdb2_run_statement(sqlh, sql)) != 0) {
            fprintf(f, "%s:%d Error setting transaction level ret=%d.\n",
                    __func__, __LINE__, ret);
            fprintf(stderr, "%s:%d Error setting transaction level ret=%d.\n",
                    __func__, __LINE__, ret);
            exit(1);
        }
        do {
            ret = cdb2_next_record(sqlh);
        } while (ret == CDB2_OK);
    }

    while (1) {
        int n = 0;
        int64_t ids[15];

        snprintf(sql, sizeof(sql), "begin");
        if ((ret = cdb2_run_statement(sqlh, sql)) != 0) {
            fprintf(f, "%s:%d error in begin, ret=%d.\n", __func__, __LINE__,
                    ret);
            do {
                ret = cdb2_next_record(sqlh);
            } while (ret == CDB2_OK);
            cdb2_run_statement(sqlh, "rollback");
            do {
                ret = cdb2_next_record(sqlh);
            } while (ret == CDB2_OK);
            continue;
        }
        fprintf(f, "sql: %s, ret = %d.\n", sql, ret);
        do {
            ret = cdb2_next_record(sqlh);
        } while (ret == CDB2_OK);

        snprintf(sql, sizeof(sql),
                 "selectv instid from jobs where state = 0 order by instid "
                 "limit 15");
        if ((ret = cdb2_run_statement(sqlh, sql)) != 0) {
            fprintf(f, "%s:%d error selecting record.\n", __func__, __LINE__);
            do {
                ret = cdb2_next_record(sqlh);
            } while (ret == CDB2_OK);
            cdb2_run_statement(sqlh, "rollback");
            do {
                ret = cdb2_next_record(sqlh);
            } while (ret == CDB2_OK);
            continue;
        }
        fprintf(f, "sql: %s, ret = %d.\n", sql, ret);

        ret = cdb2_next_record(sqlh);
        if (ret == CDB2_OK) {
            do {
                if ((type = cdb2_column_type(sqlh, 0)) == CDB2_CSTRING) {
                    ch = cdb2_column_value(sqlh, 0);
                    instid = atoll(ch);
                } else if (type == CDB2_INTEGER) {
                    ll = (int64_t *)cdb2_column_value(sqlh, 0);
                    instid = *ll;
                } else {
                    fprintf(f,
                            "%s:%d Unexpected type from cdb2_next_record, %d\n",
                            __func__, __LINE__, type);
                    fprintf(stderr,
                            "%s:%d Unexpected type from cdb2_next_record, %d\n",
                            __func__, __LINE__, type);
                    exit(1);
                }
                ids[n++] = instid;
                ret = cdb2_next_record(sqlh);
            } while (ret == CDB2_OK);

            if (ret != CDB2_OK_DONE) {
                cdb2_run_statement(sqlh, "rollback");
                do {
                    ret = cdb2_next_record(sqlh);
                } while (ret == CDB2_OK);
                continue;
            }

            int i = 0;
            for (i = 0; i < n; i++) {
                snprintf(sql, sizeof(sql),
                         "update jobs set state = state + 1, updatehost = %ld "
                         "where instid = %ld",
                         host, ids[i]);
                if ((ret = cdb2_run_statement(sqlh, sql)) != 0) {
                    fprintf(f, "%s:%d error selectv record.\n", __func__,
                            __LINE__);
                    do {
                        ret = cdb2_next_record(sqlh);
                    } while (ret == CDB2_OK);
                    cdb2_run_statement(sqlh, "rollback");
                    do {
                        ret = cdb2_next_record(sqlh);
                    } while (ret == CDB2_OK);
                    continue;
                }
                fprintf(f, "sql: %s, ret = %d.\n", sql, ret);
                do {
                    ret = cdb2_next_record(sqlh);
                } while (ret == CDB2_OK);

                if (ret != CDB2_OK_DONE) {
                    cdb2_run_statement(sqlh, "rollback");
                    do {
                        ret = cdb2_next_record(sqlh);
                    } while (ret == CDB2_OK);
                    continue;
                }
            }
        } else {
            cdb2_run_statement(sqlh, "rollback");
            do {
                ret = cdb2_next_record(sqlh);
            } while (ret == CDB2_OK);
            continue;
        }

        snprintf(sql, sizeof(sql), "commit");
        ret = cdb2_run_statement(sqlh, sql);
        fprintf(f, "sql: %s, ret = %d.\n", sql, ret);
        if (!ret) {
            do {
                ret = cdb2_next_record(sqlh);
            } while (ret == CDB2_OK);

            fprintf(f, "WON %d JOBS\n", n);

retry_add:
            snprintf(sql, sizeof(sql), "begin");
            if ((ret = cdb2_run_statement(sqlh, sql)) != 0) {
                fprintf(f, "%s:%d error in begin, ret=%d.\n", __func__,
                        __LINE__, ret);
                do {
                    ret = cdb2_next_record(sqlh);
                } while (ret == CDB2_OK);
                cdb2_run_statement(sqlh, "rollback");
                do {
                    ret = cdb2_next_record(sqlh);
                } while (ret == CDB2_OK);
                continue;
                goto retry_add;
            }
            fprintf(f, "sql: %s, ret = %d.\n", sql, ret);
            do {
                ret = cdb2_next_record(sqlh);
            } while (ret == CDB2_OK);

            int i = 0;
            for (i = 0; i < n; i++) {
                snprintf(sql, sizeof(sql),
                         "insert into schedule(instid, state, updatehost) "
                         "values(%ld, %d, %ld)",
                         ids[i], 1, host);
                if ((ret = cdb2_run_statement(sqlh, sql)) != 0) {
                    fprintf(f, "%s:%d error insert record.\n", __func__,
                            __LINE__);
                    do {
                        ret = cdb2_next_record(sqlh);
                    } while (ret == CDB2_OK);
                    cdb2_run_statement(sqlh, "rollback");
                    do {
                        ret = cdb2_next_record(sqlh);
                    } while (ret == CDB2_OK);
                    goto retry_add;
                }
                fprintf(f, "sql: %s, ret = %d.\n", sql, ret);
                do {
                    ret = cdb2_next_record(sqlh);
                } while (ret == CDB2_OK);

                if (ret != CDB2_OK_DONE) {
                    cdb2_run_statement(sqlh, "rollback");
                    do {
                        ret = cdb2_next_record(sqlh);
                    } while (ret == CDB2_OK);
                    goto retry_add;
                }
            }
            snprintf(sql, sizeof(sql), "commit");
            ret = cdb2_run_statement(sqlh, sql);
            fprintf(f, "sql: %s, ret = %d.\n", sql, ret);
            if (ret == 210 /*NOT_DURABLE*/ || ret == -1 ||
                    ret == -109 || ret == -5) {
                fprintf(f, "FAILED TO INSERT: RET %d, ERR %s\n", ret,
                        cdb2_errstr(sqlh));
            } else if (ret) {
                fprintf(f, "BUG: FAILED TO INSERT: RET %d, ERR %s\n", ret,
                        cdb2_errstr(sqlh));
                fprintf(stderr, "BUG in thread %ld: FAILED TO INSERT: RET %d, ERR %s\n",
                        host, ret, cdb2_errstr(sqlh));
                exit(1);
            }
        } else if (ret == CDB2ERR_CONSTRAINTS) {
            fprintf(f, "LOST TO ANOTHER THREAD\n");
        } else if (c->occ) {
            fprintf(f, "LOST TO ANOTHER THREAD, rc %d\n", ret);
        } else if (ret == 210 /*NOT_DURABLE*/ || ret == -1 ||
                ret == CDB2ERR_VERIFY_ERROR || ret == -109 /* MASTER LOST TXN */) {
            fprintf(f, "FAILED TO UPDATE: RET %d, ERR %s\n", ret,
                    cdb2_errstr(sqlh));
        } else {
			fprintf(f, "BUG: FAILED TO UPDATE: RET %d, ERR %s\n", ret,
                    cdb2_errstr(sqlh));
            fprintf(stderr, "BUG in thread %ld: FAILED TO UPDATE: RET %d, ERR %s\n", 
                    host, ret, cdb2_errstr(sqlh));
            exit(1);
        }
        do {
            ret = cdb2_next_record(sqlh);
        } while (ret == CDB2_OK);
    }

    cdb2_close(sqlh);
    free(arg);
    fclose(f);
    return NULL;
}

int schedule(config_t *c)
{
    pthread_attr_t attr;
    pthread_t *thds;
    int i, ret;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    thds = calloc(c->threads, sizeof(pthread_t));

    for (i = 0; i < c->threads; i++) {
        update_thread_t *upd = (update_thread_t *)malloc(sizeof(*upd));
        upd->c = c;
        upd->thd = i;

        if ((ret = pthread_create(&thds[i], &attr, schedule_thd, upd)) != 0) {
            fprintf(stderr, "Error creating pthread, ret=%d\n", ret);
            exit(1);
        }
    }

    for (i = 0; i < c->threads; i++) {
        pthread_join(thds[i], NULL);
    }

    pthread_attr_destroy(&attr);
    return 0;
}

int main(int argc, char *argv[])
{
    cdb2_hndl_tp *sqlh;
    config_t *c;
    int err = 0, opt, maxretries = 32, debug = 0;

    argv0 = argv[0];
    c = default_config();

    /* char *optarg=argument, int optind = argv index  */
    while ((opt = getopt(argc, argv, "d:hort:m:D")) != EOF) {
        switch (opt) {
        case 'd':
            c->dbname = optarg;
            break;

        case 'o':
            c->occ = 1;
            break;

        case 'r':
            c->recom = 1;
            break;

        case 't':
            c->threads = atoi(optarg);
            break;

        case 'D':
            debug = 1;
            break;

        case 'h':
            usage(stdout);
            break;

        case 'm':
            maxretries = atoi(optarg);
            break;

        default:
            fprintf(stderr, "Unknown flag, '%c'.\n", optopt);
            err++;
            break;
        }
    }


    /* Make sure dbname is set. */
    if (NULL == c->dbname) {
        fprintf(stderr, "dbname is unset.\n");
        err++;
    }
    /* Punt if there were errors. */
    if (err) {
        exit(1);
    }

    printf(
        "Running test with config: dbname %s, occ %d, recom %d, threads %d\n",
        c->dbname, c->occ, c->recom, c->threads);

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    cdb2_set_max_retries(maxretries);

    /* Allocate an sql handle. */
    if (0 == err && cdb2_open(&sqlh, c->dbname, "default", 0)) {
        fprintf(stderr, "error opening sql handle for '%s'.\n", c->dbname);
        err++;
    }

    if (debug)
        cdb2_set_debug_trace(sqlh);

    /* Punt if there were errors. */
    if (err) {
        usage(stderr);
        exit(1);
    }

    schedule(c);
    return 0;
}
