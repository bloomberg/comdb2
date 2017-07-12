#ifndef INCLUDED_EVENTLOG_H
#define INCLUDED_EVENTLOG_H

#include "reqlog_int.h"

void eventlog_init(const char *dbname);
void eventlog_status(void);
void eventlog_add(const struct reqlogger *logger);
void eventlog_stop(void);
void eventlog_process_message(char *line, int llen, int *toff);


#endif
