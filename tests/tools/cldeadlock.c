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
cdb2_hndl_tp **upd_orphans;
cdb2_hndl_tp **sel_orphans;

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

static inline int failexit(const char *func, int line, int rc)
{
    printf("Exiting from %s line %d with rc %d\n", func, line, rc);
    exit(1);
}

static inline int retry_rcode(int rc)
{
    if (rc == -1 || rc == -5 || rc == 2 || rc == 4 || rc == 402)
        return 1;
    return 0;
}

int64_t record_count(const char *dbname, const char *type)
{
    cdb2_hndl_tp *hndl;
    int64_t count;
    int rc;

    if ((rc = cdb2_open(&hndl, dbname, type, 0)) != 0) {
        fprintf(stderr, "Failed to allocate count handle for %s\n", dbname);
        failexit(__func__, __LINE__, rc);
    }

again:
    if ((rc = cdb2_run_statement(hndl, "select count(*) from t1")) != 0) {
        fprintf(stderr, "Failed to run select count statement, rc=%d, %s\n", rc,
                cdb2_errstr(hndl));
        /* Connect error */
        if (retry_rcode(rc) == 1) {
            sleep(1);
            goto again;
        }
        failexit(__func__, __LINE__, rc);
    }

    if ((rc = cdb2_next_record(hndl)) != CDB2_OK) {
        fprintf(stderr, "cdb2_next_record returns %d, %s\n", rc,
                cdb2_errstr(hndl));
        failexit(__func__, __LINE__, rc);
    }

    count = *(int64_t *)cdb2_column_value(hndl, 0);
    cdb2_close(hndl);
    return count;
}

int select_and_update_orphan(const char *dbname, const char *type,
        int idx, int snapshot, int64_t count, int range, int pint)
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
        failexit(__func__, __LINE__, sel_rc);
    }
    sel_orphans[idx] = sel_hndl;

    if ((upd_rc = cdb2_open(&upd_hndl, dbname, type, 0)) != 0) {
        fprintf(stderr, "Failed to allocate upd-handle for %s\n", dbname);
        failexit(__func__, __LINE__, upd_rc);
    }
    upd_orphans[idx] = upd_hndl;

    if (snapshot) {
        if ((sel_rc = cdb2_run_statement(sel_hndl,
                        "set transaction snapshot"))!=0) {
            fprintf(stderr, "Failed to set select snapshot isolation, %s\n",
                    cdb2_errstr(sel_hndl));
            failexit(__func__, __LINE__, sel_rc);
        }

        if ((upd_rc = cdb2_run_statement(upd_hndl,
                        "set transaction snapshot"))!=0) {
            fprintf(stderr, "Failed to set update snapshot isolation, %s\n",
                    cdb2_errstr(upd_hndl));
            failexit(__func__, __LINE__, sel_rc);
        }
    }

sel_again:
    if ((sel_rc = cdb2_run_statement(sel_hndl,
                    "select a from t1 order by a")) != 0) {
        fprintf(stderr, "Failed to run select statement, rc=%d, %s\n", sel_rc,
                cdb2_errstr(sel_hndl));
        if (retry_rcode(sel_rc)) {
            sleep(1);
            goto sel_again;
        }
        failexit(__func__, __LINE__, sel_rc);
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

upd_again:
            if ((upd_rc = cdb2_run_statement(upd_hndl, upd_sql)) != 0 &&
                    upd_rc != CDB2ERR_VERIFY_ERROR) {
                fprintf(stderr, "Failed to run update statement, rc=%d, %s\n",
                        upd_rc, cdb2_errstr(upd_hndl));
                if (retry_rcode(upd_rc)) {
                    sleep(1);
                    goto upd_again;
                }
                failexit(__func__, __LINE__, upd_rc);
            }

            /* Ignore results */
            while((upd_rc = cdb2_next_record(upd_hndl)) == CDB2_OK)
                ;

            if (upd_rc != CDB2_OK_DONE) {
                fprintf(stderr, "Failed next from update cursor, rcode is %d, %s\n",
                        upd_rc, cdb2_errstr(upd_hndl));
                if (!retry_rcode(upd_rc)) {
                    failexit(__func__, __LINE__, upd_rc);
                }
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

int select_and_update(const char *dbname, const char *type, int snapshot,
        int64_t count, int orphans, int range, int pint)
{
    int i, rc;
    upd_orphans = calloc(orphans, sizeof(cdb2_hndl_tp *));
    sel_orphans = calloc(orphans, sizeof(cdb2_hndl_tp *));
    for (i = 0; i < orphans; i++)
        select_and_update_orphan(dbname, type, i, snapshot, count, range, pint);
    printf("Closing orphans\n");
    for (i = 0; i < orphans; i++) {
        cdb2_close(upd_orphans[i]);
        while((rc = cdb2_next_record(sel_orphans[i])) == CDB2_OK)
            ;
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "closing orphan next_record sel_rc is %d, %s\n", rc,
                    cdb2_errstr(sel_orphans[i]));
        }
        cdb2_close(sel_orphans[i]);
    }
    return 0;
}


int main(int argc,char *argv[])
{
    int c, err = 0, range = 100000, pint = 10000, orphans = 10, snapshot = 0;
    char *dbname = NULL, *type = NULL, *pidfile = NULL;
    FILE *pfile;
    int64_t count;
    argv0=argv[0];

    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);
    srand(time(NULL) ^ getpid());

    while ((c = getopt(argc,argv,"hd:r:p:t:c:f:s"))!=EOF) {
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
            case 's':
                snapshot = 1;
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
    select_and_update(dbname, type, snapshot, count, orphans, range, pint);
    printf("Complete\n");
    return 0;
}
