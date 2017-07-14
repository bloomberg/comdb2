#include <unistd.h>
#include <ctype.h>
#include <string.h>
#include <strings.h>
#include <pthread.h>
#include <testutil.h>
#include <stdint.h>
#include <sys/time.h>
#include <stdarg.h>
#include <errno.h>

#define FMTSZ 512

void tdprintf(FILE *f, cdb2_hndl_tp *db, const char *func, int line,
              const char *format, ...)
{
    va_list ap;
    char fmt[FMTSZ];
    char buf[128];

    fmt[0] = '\0';

    if (strncasecmp(format, "XXX ", 3) == 0) {
        strcat(fmt, "XXX ");
    }

    snprintf(buf, sizeof(buf), "td %u ", (uint32_t)pthread_self());
    strcat(fmt, buf);

    snprintf(buf, sizeof(buf), "handle %p ", db);
    strcat(fmt, buf);

    snprintf(buf, sizeof(buf), "%s:%d ", func, line);
    strcat(fmt, buf);

    snprintf(buf, sizeof(buf), "cnonce '%s' ", db ? cdb2_cnonce(db) : "(null)");
    strcat(fmt, buf);

    int file = -1, offset = -1;
    if (db) cdb2_snapshot_file(db, &file, &offset);
    snprintf(buf, sizeof(buf), "snapshot_lsn [%d][%d] ", file, offset);
    strcat(fmt, buf);

    strcat(fmt, format);
    va_start(ap, format);
    vfprintf(f, fmt, ap);
    va_end(ap);
}

char *master(const char *dbname, const char *cltype)
{
    int rc;
    char *s_master = NULL;
    cdb2_hndl_tp *db;

    rc = cdb2_open(&db, dbname, cltype, CDB2_RANDOM);
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__, "thd: open rc %d %s\n", rc,
                 cdb2_errstr(db));
        cdb2_close(db);
        return NULL;
    }

    rc = cdb2_run_statement(db, "exec procedure sys.cmd.send('bdb cluster')");
    if (rc) {
        fprintf(stderr, "exec procedure sys.cmd.send: %d %s\n", rc,
                cdb2_errstr(db));
        cdb2_close(db);
        return NULL;
    }

    rc = cdb2_next_record(db);
    while (rc == CDB2_OK) {
        char *f, *s, *m;
        s = cdb2_column_value(db, 0);
        if (strstr(s, "MASTER")) {
            while (*s && isspace(*s))
                s++;
            char *endptr;
            f = m = strdup(s);
            m = strtok_r(m, ":", &endptr);
            if (m) {
                s_master = strdup(m);
            }
            free(f);
        }
        rc = cdb2_next_record(db);
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "next master rc %d %s\n", rc, cdb2_errstr(db));
        cdb2_close(db);
        return NULL;
    }

    cdb2_close(db);
    return s_master;
}

char *read_node(cdb2_hndl_tp *db)
{
    char *host;
    int rc;

    rc = cdb2_run_statement(db, "select comdb2_host()");
    if (rc) {
        tdprintf(stderr, db, __func__, __LINE__,
                 "run: don't know what node I'm on\n");
        return NULL;
    }
    rc = cdb2_next_record(db);
    if (rc != CDB2_OK) {
        tdprintf(stderr, db, __func__, __LINE__,
                 "next: don't know what node I'm on\n");
        return NULL;
    }
    host = cdb2_column_value(db, 0);
    if (host) host = strdup(host);
    while (rc == CDB2_OK) {
        rc = cdb2_next_record(db);
    }
    if (rc != CDB2_OK_DONE) {
        tdprintf(stderr, db, __func__, __LINE__, "next read node rc %d %s\n",
                 rc, cdb2_errstr(db));
        return NULL;
    }

    return host;
}

void myexit(const char *func, int line, int status)
{
    printf("calling exit from thread %u function %s line %d with status %d\n",
           (uint32_t)pthread_self(), func, line, status);
    fflush(stdout);
    fflush(stderr);
    exit(status);
}

uint64_t timems(void)
{
    int rc;
    struct timeval tv;
    rc = gettimeofday(&tv, NULL);
    if (rc == -1) {
        fprintf(stderr, "Can't get time rc %d %s\n", errno, strerror(errno));
        return 1;
    }
    return (tv.tv_sec * 1000000 + tv.tv_usec) / 1000;
}

uint64_t timeus(void)
{
    int rc;
    struct timeval tv;
    rc = gettimeofday(&tv, NULL);
    if (rc == -1) {
        fprintf(stderr, "Can't get time rc %d %s\n", errno, strerror(errno));
        return 1;
    }
    return (tv.tv_sec * 1000000000 + tv.tv_usec);
}
