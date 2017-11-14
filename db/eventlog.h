#ifndef INCLUDED_EVENTLOG_H
#define INCLUDED_EVENTLOG_H

#include "reqlog_int.h"
#include "cson_amalgamation_core.h"

cson_array *get_bind_array(struct reqlogger *logger, int nfields);
void add_to_bind_array(cson_array *arr, char *name, int type, void *val,
                       int dlen, int isnull);

void eventlog_init();
void eventlog_status(void);
void eventlog_add(const struct reqlogger *logger);
void eventlog_stop(void);
void eventlog_process_message(char *line, int llen, int *toff);


#endif
