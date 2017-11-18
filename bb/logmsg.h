#ifndef INCLUDED_BB_LOGMSG_H
#define INCLUDED_BB_LOGMSG_H

#include <stdio.h>
#include <stdarg.h>

typedef enum {
    LOGMSG_DEBUG = 1,
    LOGMSG_INFO = 2,
    LOGMSG_WARN = 3,
    LOGMSG_ERROR = 4,
    LOGMSG_FATAL = 5,
    LOGMSG_USER = 6,
} loglvl;

#ifdef _SUN_SOURCE
#define FORMAT_PRINTF(x,y)
#else
#define FORMAT_PRINTF(x,y) __attribute__((format(printf,x,y)))
#endif
int logmsg(loglvl lvl, const char *fmt, ...) FORMAT_PRINTF(2, 3);
int logmsgv(loglvl lvl, const char *fmt, va_list args);
int logmsgf(loglvl lvl, FILE *f, const char *fmt, ...) FORMAT_PRINTF(3, 4);
int logmsgvf(loglvl lvl, FILE *f, const char *fmt, va_list args);
int logmsgperror(const char *s);

void logmsg_set_name(char *name);
void logmsg_set_syslog(int onoff);
void logmsg_set_file(FILE *file);
void logmsg_set_level(loglvl lvl);
void logmsg_set_time(int onoff);

int logmsg_process_message(char *line, int llen);

int logmsg_level_update(void *unused, void *value);
int logmsg_syslog_update(void *unused, void *value);
int logmsg_timestamp_update(void *unused, void *value);
void *logmsg_level_value(void *unused);
void *logmsg_syslog_value(void *unused);
void *logmsg_timestamp_value(void *unused);

#endif
