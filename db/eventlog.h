#ifndef INCLUDED_EVENTLOG_H
#define INCLUDED_EVENTLOG_H

struct reqlogger;
struct cson_array;

struct cson_array *get_bind_array(struct reqlogger *logger, int nfields);
void add_to_bind_array(struct cson_array *, char *name, int type, void *val, int dlen);

void eventlog_init();
void eventlog_status(void);
void eventlog_add(const struct reqlogger *logger);
void eventlog_stop(void);
void eventlog_process_message(char *line, int llen, int *toff);


#endif
