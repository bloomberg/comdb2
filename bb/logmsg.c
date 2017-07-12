#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include <pthread.h>
#include <unistd.h>
#include <syslog.h>

#include "logmsg.h"
#include "util.h"
#include "segstr.h"

static loglvl level = LOGMSG_WARN;
static int do_syslog = 0;
static int do_time = 1;
static int ended_with_newline = 1;

/* from io_override.c */

static pthread_key_t iokey;
int io_override_init(void) { return pthread_key_create(&iokey, NULL); }

int io_override_set_std(FILE *f)
{
    pthread_setspecific(iokey, f);
    return 0;
}

FILE *io_override_get_std(void) { return pthread_getspecific(iokey); }

void logmsg_set_level(loglvl lvl) {
    level = lvl;
}

void logmsg_set_time(int onoff) {
    do_time = onoff;
}

void logmsg_set_syslog(int onoff) {
    do_syslog = onoff;
}

static int level_to_syslog(loglvl lvl) {
    switch (lvl) {
        case LOGMSG_DEBUG:
            return LOG_DEBUG;
        case LOGMSG_INFO:
            return LOG_INFO;
        case LOGMSG_WARN:
            return LOG_WARNING;
        case LOGMSG_ERROR:
            return LOG_ERR;
        case LOGMSG_FATAL:
        default:
            return LOG_CRIT;
    }
}

static char *logmsg_level_str(int lvl)
{
    switch (lvl) {
    case LOGMSG_DEBUG: return "DEBUG";
    case LOGMSG_INFO: return "INFO";
    case LOGMSG_WARN: return "WARNING";
    case LOGMSG_ERROR: return "ERROR";
    case LOGMSG_FATAL: return "FATAL";
    case LOGMSG_USER: return "USER";
    default: return "???";
    }
}

static int logmsgv_lk(loglvl lvl, const char *fmt, va_list args)
{
    if (!fmt) return 0;

    char *msg;
    char timestamp[200];
    va_list argscpy;
    FILE *f;
    int ret = 0;

    FILE *override = io_override_get_std();
    if (!override) {
        if (lvl < level)
            return 0;

        f = stderr;
    }
    else
        f = override;

    char buf[1];

    va_copy(argscpy, args);
    int len = vsnprintf(buf, 1, fmt, argscpy);
    va_end(argscpy);

    if (len == 0 || len == 1)
        msg = strdup(fmt);
    else
        msg = malloc(len+2);

    va_copy(argscpy, args);
    vsnprintf(msg, len+1, fmt, argscpy); 
    va_end(argscpy);

    if (do_syslog && override == NULL) {
        int syslog_level = level_to_syslog(lvl);
        syslog(syslog_level, "%s", msg);
    }
    if (do_time && !override) {
        time_t t = time(NULL);
        struct tm tm;
        localtime_r(&t, &tm);
        snprintf(timestamp, sizeof(timestamp), "%04d/%02d/%02d %02d:%02d:%02d ", 
                tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
    }
    char *savmsg = msg;
    while (do_time && ! override && ended_with_newline && *msg != 0) {
        char *s;
        ret += fprintf(f, "%s", timestamp);
        s = strchr(msg, '\n');
        if (s) {
            *s = 0;
            ended_with_newline = 1;
            /* Add a prefix for ERROR/FATAL messages. */
            if (lvl == LOGMSG_ERROR || lvl == LOGMSG_FATAL) {
                ret += fprintf(f, "[%s] ", logmsg_level_str(lvl));
            }
            ret += fprintf(f, "%s\n", msg);
            msg = s+1;
        }
        else {
            ended_with_newline = 0;
            break;
        }
    }
    if (*msg != 0) {
        /* Add a prefix for ERROR/FATAL messages. */
        if (lvl == LOGMSG_ERROR || lvl == LOGMSG_FATAL) {
            ret += fprintf(f, "[%s] ", logmsg_level_str(lvl));
        }
        ret += fprintf(f, "%s", msg);
        if (msg[strlen(msg)-1] == '\n')
            ended_with_newline = 1;
        else
            ended_with_newline = 0;
    }
    free(savmsg);
    return ret;
}

static pthread_mutex_t logmsg_lk = PTHREAD_MUTEX_INITIALIZER;
int logmsgv(loglvl lvl, const char *fmt, va_list args) {
    int ret;
    pthread_mutex_lock(&logmsg_lk);
    ret = logmsgv_lk(lvl, fmt, args);
    pthread_mutex_unlock(&logmsg_lk);
    return ret;
}

int logmsg(loglvl lvl, const char *fmt, ...) {
    int ret;
    va_list args;
    va_start(args, fmt);
    ret = logmsgv(lvl, fmt, args);
    va_end(args);
    return ret;
}


int logmsgvf(loglvl lvl, FILE *f, const char *fmt, va_list args) {
    int ret;
    if (f == stdout || f == stderr)
        ret = logmsgv(lvl, fmt, args);
    else
        ret = vfprintf(f, fmt, args);
    return ret;
}

int logmsgf(loglvl lvl, FILE *f, const char *fmt, ...) {
    int ret;
    va_list args;
    va_start(args, fmt);
    ret = logmsgvf(lvl, f, fmt, args);
    va_end(args);
    return ret;
}

int logmsgperror(const char *s) {
    int ret;
    char *err = strerror(errno);
    ret = logmsg(LOGMSG_ERROR, "%s: %s\n", s, err);
    return ret;
}

void sqlite3DebugPrintf(const char *zFormat, ...)
{
    va_list args;

#ifdef NOINTERLEAVE
    pthread_mutex_lock(&lk);
#endif

    va_start(args, zFormat);
    logmsgv(LOGMSG_USER, zFormat, args);
    va_end(args);

#ifdef NOINTERLEAVE
    pthread_mutex_unlock(&lk);
#endif
}

int logmsg_process_message(char *line, int llen) {
    char *tok;
    int st = 0;
    int ltok = 0;

    tok = segtok(line, llen, &st, &ltok);
    if (tok == NULL)
        return 1;
    tok = segtok(line, llen, &st, &ltok);
    if (tok == NULL) {
        return 1;
    } else if (tokcmp(tok, ltok, "level") == 0) {
        tok = segtok(line, llen, &st, &ltok);
        if (tok == NULL) { 
            logmsg(LOGMSG_ERROR, "expected logmsg level default_level\n");
            return 1;
        } else if (tokcmp(tok, ltok, "debug") == 0) {
            logmsg_set_level(LOGMSG_DEBUG);
        } else if (tokcmp(tok, ltok, "info") == 0) {
            logmsg_set_level(LOGMSG_INFO);
        } else if (tokcmp(tok, ltok, "warn") == 0) {
            logmsg_set_level(LOGMSG_WARN);
        } else if (tokcmp(tok, ltok, "error") == 0) {
            logmsg_set_level(LOGMSG_ERROR);
        } else if (tokcmp(tok, ltok, "fatal") == 0) {
            logmsg_set_level(LOGMSG_FATAL);
        } else {
            logmsg(LOGMSG_ERROR, "unknown logging level requested\n");
            return 1;
        }
        logmsg(LOGMSG_USER, "set default log level to %s\n", logmsg_level_str(level));
    } else if (tokcmp(tok, ltok, "timestamp") == 0) {
        logmsg_set_time(1);
        logmsg(LOGMSG_USER, "timestamps on\n");
    } else if (tokcmp(tok, ltok, "notimestamp") == 0) {
        logmsg_set_time(0);
        logmsg(LOGMSG_USER, "timestamps off\n");
    } else if (tokcmp(tok, ltok, "syslog") == 0) {
        logmsg_set_syslog(1);
        logmsg(LOGMSG_USER, "syslog on\n");
    } else if (tokcmp(tok, ltok, "nosyslog") == 0) {
        logmsg_set_syslog(0);
        logmsg(LOGMSG_USER, "syslog off\n");
    } else if (tokcmp(tok, ltok, "stat") == 0) {
        logmsg(LOGMSG_USER, "log level:  %s\n", logmsg_level_str(level));
        logmsg(LOGMSG_USER, "syslog:     %s\n", do_syslog ? "yes" : "no");
        logmsg(LOGMSG_USER, "timestamps: %s\n", do_time ? "yes" : "no");
    }

    return 0;
}
