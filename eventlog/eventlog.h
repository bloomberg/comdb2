#ifndef INCLUDED_EVENTLOG_H
#define INCLUDED_EVENTLOG_H

#include <comdb2.h>

struct reqlogger;
struct cson_array;

struct cson_array *get_bind_array(struct reqlogger *logger, int sample_queries);

void eventlog_bind_null(struct cson_array *, const char *);
void eventlog_bind_int64(struct cson_array *, const char *, int64_t, int);
void eventlog_bind_text(struct cson_array *, const char *, const char *, int);
void eventlog_bind_double(struct cson_array *, const char *, double, int);
void eventlog_bind_blob(struct cson_array *, const char *, const void *, int);
void eventlog_bind_varchar(struct cson_array *, const char *, const void *, int);
void eventlog_bind_datetime(struct cson_array *, const char *, dttz_t *, const char *);
void eventlog_bind_interval(struct cson_array *, const char *, intv_t *);
void eventlog_bind_array(struct cson_array *, const char *, void *array_ptr, int array_count, int type);

typedef enum eventlog_net_direction {
    EVENTLOG_NET_IN,
    EVENTLOG_NET_OUT
} eventlog_net_direction;

void eventlog_init();
void eventlog_status(void);
void eventlog_add(const struct reqlogger *logger);
void eventlog_stop(void);
void eventlog_process_message(char *line, int llen, int *toff);
void eventlog_debug(char *fmt, ...);

int eventlog_debug_enabled(void);

#define EVENTLOG_DEBUG(_code) do { \
    if (eventlog_debug_enabled()) {  \
        do {                       \
            _code                  \
        } while(0);                \
    }                              \
} while(0)

void eventlog_net_event(const char *context, const char *api, const char *msgtype, eventlog_net_direction direction, const void *buffer, size_t bufsize);
void eventlog_net_event_sql_response(const char *context, const void *pb);

#endif
