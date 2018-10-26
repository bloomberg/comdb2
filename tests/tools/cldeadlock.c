#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <errno.h>
#include <string.h>
#include <cdb2api.h>
#include <time.h>
#include <inttypes.h>

static char *argv0=NULL;

void usage(FILE *f)
{
    fprintf(f, "Usage: %s <cmd-line>\n", argv0);
    fprintf(f, " -d <dbname>\n");
    fprintf(f, " -c <config>\n");
    fprintf(f, " -t <type-string>\n");
    fprintf(f, " -r <update-range>\n");
    fprintf(f, " -p <print-interval>\n");
    fprintf(f, " -o <orphan-count>\n");
    fprintf(f, " -h <help-menu>\n");
    exit(1);
}

int64_t record_count(const char *dbname, const char *type)
{
    cdb2_hndl_tp *hndl;
    int64_t count;
    int rc;

    if ((rc = cdb2_open(&hndl, dbname, type, 0)) != 0) {
        fprintf(stderr, "Failed to allocate count handle for %s\n", dbname);
        exit(1);
    }

    if ((rc = cdb2_run_statement(hndl, "select count(*) from t1")) != 0) {
        fprintf(stderr, "Failed to run select count statement, rc=%d\n", rc);
        exit(1);
    }

    if ((rc = cdb2_next_record(hndl)) != CDB2_OK) {
        fprintf(stderr, "cdb2_next_record returns %d\n", rc);
        exit(1);
    }

    count = *(int64_t *)cdb2_column_value(hndl, 0);
    cdb2_close(hndl);
    return count;
}

int select_and_update_orphan(const char *dbname, const char *type,
        int64_t count, int range, int pint)
{
    int sel_rc, upd_rc;
    int64_t target = (rand() % count) / 10, iter = 0;
    int64_t *sel_val, upd_val, cnt=0;
    char upd_sql[80];
    cdb2_hndl_tp *sel_hndl;
    cdb2_hndl_tp *upd_hndl;

    printf("Orphan target is %" PRId64 "\n", target);
    if ((sel_rc = cdb2_open(&sel_hndl, dbname, type, 0)) != 0) {
        fprintf(stderr, "Failed to allocate sel-handle for %s\n", dbname);
        exit(1);
    }

    if ((upd_rc = cdb2_open(&upd_hndl, dbname, type, 0)) != 0) {
        fprintf(stderr, "Failed to allocate upd-handle for %s\n", dbname);
        exit(1);
    }

    if ((sel_rc = cdb2_run_statement(sel_hndl,
                    "select a from t1 order by a")) != 0) {
        fprintf(stderr, "Failed to run select statement, rc=%d\n", sel_rc);
        exit(1);
    }

    while((++iter < target) && (sel_rc = cdb2_next_record(sel_hndl)) == CDB2_OK) {
        if ((sel_val = (int64_t *)cdb2_column_value(sel_hndl, 0)) != NULL) {
            cnt++;
            if (range > 0)
                upd_val = *sel_val + (rand() % range);
            else
                upd_val = *sel_val;

            snprintf(upd_sql, sizeof(upd_sql),
                    "update t1 set a = %" PRId64 " where a = %" PRId64 "\n",
                    upd_val, *sel_val);

            if ((upd_rc = cdb2_run_statement(upd_hndl, upd_sql)) != 0 &&
                    upd_rc != CDB2ERR_VERIFY_ERROR) {
                fprintf(stderr, "Failed to run update statement, rc=%d\n", upd_rc);
                exit(1);
            }

            /* Ignore results */
            while((upd_rc = cdb2_next_record(upd_hndl)) == CDB2_OK)
                ;

            if (upd_rc != CDB2_OK_DONE) {
                fprintf(stderr, "Failed next from update cursor, rcode is %d\n",
                        upd_rc);
                exit(1);
            }
            if (pint > 0 && !(cnt % pint))
                printf("Updated %" PRId64 " records, target is %" PRId64 "\n",
                        cnt, target);
        }
    }
    printf("Orphaning connections after %" PRId64 " records\n", iter);

    // Intentionally orphan handles
    return 0;
}

int select_and_update(const char *dbname, const char *type, int64_t count,
        int orphans, int range, int pint)
{
    int i;
    for (i = 0; i < orphans; i++)
        select_and_update_orphan(dbname, type, count, range, pint);
    return 0;
}


int main(int argc,char *argv[])
{
    int c, err = 0, range = 100000, rc, pint = 10000, orphans = 10;
    char *dbname = NULL, *type = NULL, *pidfile = NULL;
    FILE *pfile;
    int64_t count;
    argv0=argv[0];

    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);
    srand(time(NULL) ^ getpid());

    while ((c = getopt(argc,argv,"hd:r:p:t:c:f:"))!=EOF) {
        switch(c) {
            case 'd':
                dbname = optarg;
                break;
            case 'c':
                cdb2_set_comdb2db_config(optarg);
                break;
            case 't':
                type = optarg;
                break;
            case 'r':
                range = atoi(optarg);
                break;
            case 'p':
                pint = atoi(optarg);
                break;
            case 'f':
                pidfile = optarg;
                break;
            case 'o':
                orphans = atoi(optarg);
                break;
            case 'h':
                usage(stdout);
                break;
        }
    }

    if (!dbname) {
        fprintf(stderr, "dbname is not set\n");
        err++;
    }

    if (pidfile) {
        if ((pfile = fopen(pidfile, "w")) == NULL) {
            fprintf(stderr, "Error opening '%s', %s\n", pidfile,
                    strerror(errno));
            err++;
        } else {
            fprintf(pfile, "%d\n", getpid());
            fflush(pfile);
            fclose(pfile);
        }
    }

    if (err) {
        usage(stderr);
    }


    count = record_count(dbname, type);
    rc = select_and_update(dbname, type, count, orphans, range, pint);
    return rc;
}
