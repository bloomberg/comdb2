#include <alloca.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include <pthread.h>
#include <unistd.h>
#include <syslog.h>

#include "logmsg.h"
#include <locks_wrap.h>
#include "util.h"
#include "segstr.h"

#define LOGMSG_STACK_BUFFER_SIZE 1024

static loglvl level = LOGMSG_WARN;
static int do_syslog = 0;
static int do_time = 1;
static int do_thread = 0;
static int do_prefix_level = 1;
static int ended_with_newline = 1;

#ifndef BUILDING_TOOLS
#include <ctrace.h>
#endif

/* from io_override.c */

static __thread FILE *ptr = NULL;
int io_override_set_std(FILE *f)
{
    ptr = f;
    return 0;
}

FILE *io_override_get_std(void) { 
    return ptr;
}

inline int logmsg_level_ok(loglvl lvl) {
    return (lvl >= level);
}

void logmsg_set_level(loglvl lvl) {
    level = lvl;
}

void logmsg_set_thd(int onoff)
{
    do_thread = onoff;
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

static int sprintf_auto_resize(char **headp, int *offp, int len, const char *fmt, const char *content)
{
    char *head = *headp;
    int off = *offp;
    char dummy[1];
    int size = snprintf(dummy, 1, fmt, content);
    int need = off + size + 1;

    if (need > len) {    /* Resize the buffer to fit more content. */
        if (off < len) { /* The buffer on the stack isn't wide enough. Switch to malloc. */
            head = malloc(need);
            memcpy(head, *headp, off);
        } else {
            head = realloc(head, need);
        }
        if (head == NULL)
            return 0;
    }

    off += snprintf(head + off, size + 1, fmt, content);

    *headp = head;
    *offp = off;
    return size;
}

int logmsgv(loglvl lvl, const char *fmt, va_list args)
{
    if (!fmt) return 0;

    char *msg, *savmsg;
    char buffer[LOGMSG_STACK_BUFFER_SIZE];
    char *head = buffer;
    int off = 0;

    char timestamp[200];
    va_list argscpy;
    FILE *f;
    int ret = 0;

    if (!logmsg_level_ok(lvl))
        return 0;

    FILE *override = io_override_get_std();
    if (!override)
        f = stderr;
    else
        f = override;

    char buf[1];

    va_copy(argscpy, args);
    int len = vsnprintf(buf, 1, fmt, argscpy);
    va_end(argscpy);

    if (len < LOGMSG_STACK_BUFFER_SIZE) {
        msg = alloca(len + 1);
        savmsg = NULL;
    } else {
        msg = malloc(len + 1);
        savmsg = msg;
    }

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
        if (do_thread) {
            snprintf(timestamp, sizeof(timestamp), "%04d/%02d/%02d %02d:%02d:%02d 0x%p ", tm.tm_year + 1900,
                     tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, (void *)pthread_self());
        } else {
            snprintf(timestamp, sizeof(timestamp),
                     "%04d/%02d/%02d %02d:%02d:%02d ", tm.tm_year + 1900,
                     tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                     tm.tm_sec);
        }
    }
    while (do_time && ! override && ended_with_newline && *msg != 0) {
        char *s;
        ret += sprintf_auto_resize(&head, &off, sizeof(buffer), "%s", timestamp);
        s = strchr(msg, '\n');
        if (s) {
            *s = 0;
            ended_with_newline = 1;
            /* Add a prefix for ERROR/FATAL messages. */
            if (do_prefix_level &&
                (lvl == LOGMSG_ERROR || lvl == LOGMSG_FATAL)) {
                ret += sprintf_auto_resize(&head, &off, sizeof(buffer), "[%s] ", logmsg_level_str(lvl));
            }
            ret += sprintf_auto_resize(&head, &off, sizeof(buffer), "%s\n", msg);
            msg = s+1;
        }
        else {
            ended_with_newline = 0;
            break;
        }
    }
    if (*msg != 0) {
        /* Add a prefix for ERROR/FATAL messages. */
        if (do_prefix_level && (lvl == LOGMSG_ERROR || lvl == LOGMSG_FATAL)) {
            ret += sprintf_auto_resize(&head, &off, sizeof(buffer), "[%s] ", logmsg_level_str(lvl));
        }
        ret += sprintf_auto_resize(&head, &off, sizeof(buffer), "%s", msg);
        if (msg[strlen(msg)-1] == '\n')
            ended_with_newline = 1;
        else
            ended_with_newline = 0;
    }
    fprintf(f, "%s", head);
    /* If `head' is different from `buffer', it's malloc'd and thus needs freed. */
    if (head != buffer)
        free(head);
    free(savmsg);
    return ret;
}

int logmsg(loglvl lvl, const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
#   ifndef BUILDING_TOOLS
    int ret = gbl_logmsg_ctrace ? ctracev(fmt, args) : logmsgv(lvl, fmt, args);
#   else
    int ret = logmsgv(lvl, fmt, args);
#   endif
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
    va_list args;
    va_start(args, fmt);
#   ifndef BUILDING_TOOLS
    int ret = gbl_logmsg_ctrace ? ctracev(fmt, args) : logmsgvf(lvl, f, fmt, args);
#   else
    int ret = logmsgvf(lvl, f, fmt, args);
#   endif
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
        if (!ltok)
            logmsg(LOGMSG_USER, "Current log level is %s\n",
                   logmsg_level_str(level));
        else
            logmsg_level_update(0, tok);
    } else if (tokcmp(tok, ltok, "thread") == 0) {
        logmsg_set_thd(1);
        logmsg(LOGMSG_USER, "threadids on\n");
    } else if (tokcmp(tok, ltok, "nothread") == 0) {
        logmsg_set_thd(1);
        logmsg(LOGMSG_USER, "threadids off\n");
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

void *logmsg_level_value(void *unused)
{
    return logmsg_level_str(level);
}

int logmsg_level_update(void *unused, void *value)
{
    char *line;
    char *tok;
    int st = 0;
    int llen;
    int ltok;

    line = (char *)value;
    llen = strlen(line);

    tok = segtok(line, llen, &st, &ltok);
    if (tok == NULL) {
        logmsg(LOGMSG_USER, "expected logmsg level default_level\n");
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
        logmsg(LOGMSG_USER, "Unknown logging level requested\n");
        return 1;
    }
    logmsg(LOGMSG_USER, "Set default log level to %s\n",
           logmsg_level_str(level));
    return 0;
}

void *logmsg_syslog_value(void *unused)
{
    return &do_syslog;
}

int logmsg_syslog_update(void *unused, void *value)
{
    logmsg_set_syslog(*(int *)value);
    return 0;
}

void *logmsg_timestamp_value(void *unused)
{
    return &do_time;
}

int logmsg_timestamp_update(void *unused, void *value)
{
    logmsg_set_time(*(int *)value);
    return 0;
}

int logmsg_prefix_level_update(void *unused, void *value)
{
    do_prefix_level = *(int *)value;
    return 0;
}

void *logmsg_prefix_level_value(void *unused)
{
    return &do_prefix_level;
}
