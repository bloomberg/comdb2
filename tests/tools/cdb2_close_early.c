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
#include <gettimeofday_ms.h>

static char *argv0 = NULL;

void usage(FILE *f)
{
    fprintf(f, "Usage: %s <cmd-line>\n", argv0);
    fprintf(f, "     -d <dbname>         -   set dbname.\n");
    fprintf(f, "     -e                  -   cdb2_close early.\n");
    exit(1);
}

typedef struct config {
    cdb2_hndl_tp *hndl;
    char *dbname;
    int close;
} config_t;

static config_t *default_config(void)
{
    config_t *c = malloc(sizeof(*c));
    bzero(c, sizeof(*c));
    c->close = 0;
    return c;
}

int main(int argc, char *argv[])
{
    cdb2_hndl_tp *sqlh;
    config_t *c;
    int ret, type;
    int err = 0, opt;
    char sql[1024];
    uint64_t start, end, tot;

    argv0 = argv[0];
    c = default_config();

    /* char *optarg=argument, int optind = argv index  */
    while ((opt = getopt(argc, argv, "d:eh:")) != EOF) {
        switch (opt) {
        case 'd':
            c->dbname = optarg;
            break;

        case 'e':
            c->close = 1;
            break;

        case 'h':
            usage(stdout);
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

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);
    /* Allocate an sql handle. */
    if (0 == err && cdb2_open(&sqlh, c->dbname, "default", 0)) {
        fprintf(stderr, "error opening sql handle for '%s'.\n", c->dbname);
        err++;
    }
    cdb2_set_debug_trace(sqlh);

    /* Punt if there were errors. */
    if (err) {
        usage(stderr);
        exit(1);
    }

    snprintf(sql, sizeof(sql), "select * from t1");
    if ((ret = cdb2_run_statement(sqlh, sql)) != 0) {
        fprintf(stderr, "%s:%d error selecting record.\n", __func__, __LINE__);
        exit(1);
    }

    ret = cdb2_next_record(sqlh);
    if (ret == CDB2_OK) {
        do {
            if ((type = cdb2_column_type(sqlh, 0)) == CDB2_INTEGER) {
                printf("integer: %lld\n", *((int64_t *)cdb2_column_value(sqlh, 0)));
                if (c->close)
                    goto done;

            } else {
                fprintf(stderr,
                        "%s:%d Unexpected type from cdb2_next_record, %d\n",
                        __func__, __LINE__, type);
                exit(1);
            }
            ret = cdb2_next_record(sqlh);
        } while (ret == CDB2_OK);
    }

done:
    start = gettimeofday_ms();
    cdb2_close(sqlh);
    end = gettimeofday_ms();
    tot = end - start;
    printf("cdb2_close took %d ms\n", tot);
    if (tot >= 4)
        exit(1);
    return 0;
}
