#ifndef INCLUDED_BB_LOGMSG_H
#define INCLUDED_BB_LOGMSG_H

#include <stdio.h>
#include <stdarg.h>

typedef enum {
    LOGMSG_DEBUG   = 1,
    LOGMSG_INFO    = 2,
    LOGMSG_WARN    = 4,
    LOGMSG_ERROR   = 5,
    LOGMSG_FATAL   = 6,

    LOGMSG_USER    = 7
} loglvl;

int logmsg(loglvl lvl, const char *fmt, ...);
int logmsgv(loglvl lvl, const char *fmt, va_list args);
int logmsgf(loglvl lvl, FILE *f, const char *fmt, ...);
int logmsgvf(loglvl lvl, FILE *f, const char *fmt, va_list args);
int logmsgperror(const char *s);

void logmsg_set_name(char *name);
void logmsg_set_syslog(int onoff);
void logmsg_set_file(FILE *file);
void logmsg_set_level(loglvl lvl);
void logmsg_set_time(int onoff);

int logmsg_process_message(char *line, int llen);
#endif
