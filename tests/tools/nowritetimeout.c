#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <cdb2api.h>

#define STALL_MS 3000

static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
static long long last_read_ms;
static int fetching, done;
static int nrecords, nrows;
static char host[256];

static long long now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000LL + ts.tv_nsec / 1000000;
}

static void tlog(const char *fmt, ...)
{
    long long ms = now_ms();
    time_t t = ms / 1000;
    struct tm tm;
    char buf[32];
    va_list ap;

    localtime_r(&t, &tm);
    strftime(buf, sizeof(buf), "%H:%M:%S", &tm);
    printf("[%s.%03lld] ", buf, ms % 1000);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    putchar('\n');
}

static void *after_read_record(cdb2_hndl_tp *hndl, void *user_arg, int argc, void **argv)
{
    int rc = (int)(intptr_t)argv[0];
    pthread_mutex_lock(&lk);
    long long gap = now_ms() - last_read_ms;
    last_read_ms = now_ms();
    ++nrecords;
    pthread_mutex_unlock(&lk);
    if (rc != 0 || gap >= 1000)
        tlog("read record #%d rc=%d gap=%lldms errno=%d (%s)", nrecords, rc, gap, errno, strerror(errno));
    return NULL;
}

/* Collect socket queues and server stacks while the stall is still in progress. */
static void *watchdog(void *arg)
{
    const char *hook = getenv("NOWRITETIMEOUT_STALL_HOOK");
    int fired = 0;

    while (1) {
        usleep(200 * 1000);
        pthread_mutex_lock(&lk);
        int stop = done;
        long long gap = fetching ? now_ms() - last_read_ms : 0;
        pthread_mutex_unlock(&lk);
        if (stop)
            break;
        if (fired || gap < STALL_MS)
            continue;
        fired = 1;
        tlog("STALL: no record for %lldms after %d records / %d rows, host %s", gap, nrecords, nrows, host);
        if (hook != NULL) {
            char cmd[1024];
            snprintf(cmd, sizeof(cmd), "%s %s %d", hook, host, (int)getpid());
            int rc = system(cmd);
            tlog("STALL hook rc=%d", rc);
        }
    }
    return NULL;
}

static int fail(cdb2_hndl_tp *hndl, const char *what, int rc)
{
    int err = errno;
    pthread_mutex_lock(&lk);
    long long gap = now_ms() - last_read_ms;
    pthread_mutex_unlock(&lk);
    tlog("FAILED %s rc=%d errno=%d (%s) records=%d rows=%d since-last-record=%lldms host=%s", what, rc, err,
         strerror(err), nrecords, nrows, gap, host);
    tlog("%s", cdb2_errstr(hndl));
    return rc ? rc : -1;
}

int main(int argc, char **argv)
{
    cdb2_hndl_tp *hndl = NULL;
    char *conf = getenv("CDB2_CONFIG");
    char *tier = "local";
    pthread_t wd;
    void *blob;
    int i, rc;

    setvbuf(stdout, NULL, _IOLBF, 0);

    if (conf != NULL) {
        cdb2_set_comdb2db_config(conf);
        tier = "default";
    }

    tlog("CREATING TABLE...");
    if ((rc = cdb2_open(&hndl, argv[1], tier, 0)) != 0)
        return fail(hndl, "open", rc);
    cdb2_run_statement(hndl, "DROP TABLE IF EXISTS t");
    if ((rc = cdb2_run_statement(hndl, "CREATE TABLE t (b blob)")) != 0)
        return fail(hndl, "create", rc);

    tlog("INSERTING DATA...");
    blob = malloc(1 << 20);
    cdb2_bind_param(hndl, "b", CDB2_BLOB, blob, 1 << 20);
    for (i = 0; i != 8; ++i)
        if ((rc = cdb2_run_statement(hndl, "INSERT INTO t VALUES(@b)")) != 0)
            return fail(hndl, "insert", rc);

    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    if (rc != CDB2_OK_DONE)
        return fail(hndl, "insert next", rc);

    tlog("DONATING FD TO SOCK POOL...");
    cdb2_close(hndl);

    tlog("REOPENING FROM SOCK POOL...");
    hndl = NULL;
    if ((rc = cdb2_open(&hndl, argv[1], tier, 0)) != 0)
        return fail(hndl, "reopen", rc);
    cdb2_register_event(hndl, CDB2_AFTER_READ_RECORD, 0, after_read_record, NULL, 1, CDB2_RETURN_VALUE);
    pthread_create(&wd, NULL, watchdog, NULL);

    tlog("RUNNING QUERY TO FILL UP TCP SND BUFFER ON SERVER...");
    last_read_ms = now_ms();
    rc = cdb2_run_statement(hndl, "SELECT * FROM t a, t b, t c");
    snprintf(host, sizeof(host), "%s", cdb2_host(hndl) ? cdb2_host(hndl) : "unknown");
    if (rc != 0)
        return fail(hndl, "select", rc);
    tlog("QUERY RUNNING ON %s, %d records read", host, nrecords);

    tlog("SLEEPING 20 SECONDS...");
    sleep(20);

    tlog("I'M AWAKE. FETCHING ROWS...");
    pthread_mutex_lock(&lk);
    last_read_ms = now_ms();
    fetching = 1;
    pthread_mutex_unlock(&lk);
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
        ++nrows;
    if (rc != CDB2_OK_DONE)
        return fail(hndl, "fetch", rc);

    pthread_mutex_lock(&lk);
    done = 1;
    pthread_mutex_unlock(&lk);
    pthread_join(wd, NULL);
    tlog("DONE: %d rows, %d records", nrows, nrecords);
    return 0;
}
