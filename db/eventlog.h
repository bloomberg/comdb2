#ifndef INCLUDED_EVENTLOG_H
#define INCLUDED_EVENTLOG_H

struct reqlogger;
struct cson_array;

struct cson_array *get_bind_array(struct reqlogger *logger, int nfields);

void eventlog_bind_null(struct cson_array *, const char *);
void eventlog_bind_int64(struct cson_array *, const char *, int64_t, int);
void eventlog_bind_text(struct cson_array *, const char *, const char *, int);
void eventlog_bind_double(struct cson_array *, const char *, double, int);
void eventlog_bind_blob(struct cson_array *, const char *, const void *, int);
void eventlog_bind_varchar(struct cson_array *, const char *, const void *, int);
void eventlog_bind_datetime(struct cson_array *, const char *, dttz_t *, const char *);
void eventlog_bind_interval(struct cson_array *, const char *, intv_t *);

void eventlog_init();
void eventlog_status(void);
void eventlog_add(const struct reqlogger *logger);
void eventlog_stop(void);
void eventlog_process_message(char *line, int llen, int *toff);

#endif
