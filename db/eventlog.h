#ifndef INCLUDED_EVENTLOG_H
#define INCLUDED_EVENTLOG_H

#include "reqlog_int.h"

void eventlog_init();
void eventlog_add(struct reqlogger *logger);
void eventlog_stop(void);
void eventlog_process_message(char *line, int llen, int *toff);


#endif
