// stalewriter <db> <host> <idbase> <seconds>: on one node, insert unique ids, read each back, and delete
// and re-read even ids. Prints "I|D id rc errstr cnonce" per write and "STALEREAD ..." on a bad read.
#include <cdb2api.h>
#include <cdb2api_test.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>

static const char *ts(void)
{
    static char b[32];
    struct timeval tv;
    gettimeofday(&tv, NULL);
    struct tm t;
    localtime_r(&tv.tv_sec, &t);
    snprintf(b, sizeof(b), "%02d:%02d:%02d.%03ld", t.tm_hour, t.tm_min, t.tm_sec, (long)tv.tv_usec / 1000);
    return b;
}

static cdb2_hndl_tp *h;

static int run(const char *sql, long *val)
{
    int rc = cdb2_run_statement(h, sql);
    if (rc) return rc;
    while ((rc = cdb2_next_record(h)) == CDB2_OK)
        if (val) *val = *(long long *)cdb2_column_value(h, 0);
    return rc == CDB2_OK_DONE ? 0 : rc;
}

int main(int argc, char **argv)
{
    if (argc < 5) { fprintf(stderr, "usage: %s db host idbase seconds\n", argv[0]); return 1; }
    char *conf = getenv("CDB2_CONFIG");
    if (conf) cdb2_set_comdb2db_config(conf);
    const char *db = argv[1], *host = argv[2];
    long id = atol(argv[3]);
    time_t end = time(NULL) + atoi(argv[4]);
    setvbuf(stdout, NULL, _IOLBF, 0);
    char sql[128];
    while (time(NULL) < end) {
        if (!h && cdb2_open(&h, db, host, CDB2_DIRECT_CPU) != 0) {
            cdb2_close(h); h = NULL; usleep(10000); continue;
        }
        long cnt = -1;
        snprintf(sql, sizeof(sql), "insert into t values(%ld)", id);
        int rc = run(sql, NULL);
        printf("I %ld %d %s %s %s\n", id, rc, rc ? cdb2_errstr(h) : "-", cdb2_cnonce(h), ts());
        if (rc == 0) {
            snprintf(sql, sizeof(sql), "select count(*) from t where i = %ld", id);
            int rrc = run(sql, &cnt);
            if (rrc == 0 && cnt != 1) printf("STALEREAD after insert %ld count %ld %s\n", id, cnt, cdb2_cnonce(h));
            if (rrc == 0 && (id % 2) == 0) {
                snprintf(sql, sizeof(sql), "delete from t where i = %ld", id);
                rc = run(sql, NULL);
                cdb2_effects_tp eff = {0};
                if (rc == 0) cdb2_get_effects(h, &eff);
                printf("D %ld %d %s %s %s\n", id, rc, rc ? cdb2_errstr(h) : "-", cdb2_cnonce(h), ts());
                /* The row was just read back, so an rc-0 delete must remove it */
                if (rc == 0 && eff.num_deleted != 1)
                    printf("STALEREAD delete %ld deleted %d %s\n", id, eff.num_deleted, cdb2_cnonce(h));
                if (rc == 0) {
                    snprintf(sql, sizeof(sql), "select count(*) from t where i = %ld", id);
                    if (run(sql, &cnt) == 0 && cnt != 0)
                        printf("STALEREAD after delete %ld count %ld %s\n", id, cnt, cdb2_cnonce(h));
                }
            }
            if (rrc) rc = rrc;
        }
        id++;
        if (rc && rc != CDB2ERR_DUPLICATE) { cdb2_close(h); h = NULL; usleep(1000); }
    }
    cdb2_close(h);
    return 0;
}
