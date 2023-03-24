/*
   Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#ifndef INCLUDED_REQLOG_H
#define INCLUDED_REQLOG_H
#include <stdarg.h>
#include <stdint.h>

/* reqlog.c - new logging stuff */
struct reqlogger;
struct ireq;
struct sqlclntstate;
struct client_query_stats;
struct string_ref;

enum {
    REQL_INFO = 1,    /* info on the request being processed */
    REQL_TRACE = 2,   /* basic trace */
    REQL_RESULTS = 4, /* query results */
    REQL_QUERY = 8    /* display only the query */
};

enum { REQL_BAD_CSTR_FLAG = 1 };

typedef enum { EV_UNSET, EV_TXN, EV_SQL, EV_SP } evtype_t;

int reqlog_init(const char *dbname);
struct reqlogger *reqlog_alloc(void);
void reqlog_reset(struct reqlogger *logger);
void reqlog_process_message(char *line, int st, int lline);
void reqlog_stat(void);
void reqlog_help(void);
void reqlog_free(struct reqlogger *reqlogger);
void reqlog_reset_logger(struct reqlogger *logger);
int reqlog_pushprefixv(struct reqlogger *logger, const char *format,
                       va_list args);
int reqlog_pushprefixf(struct reqlogger *logger, const char *format, ...);
int reqlog_popallprefixes(struct reqlogger *logger);
int reqlog_popprefix(struct reqlogger *logger);
int reqlog_logv(struct reqlogger *logger, unsigned event_flag, const char *fmt,
                va_list args);
int reqlog_logf(struct reqlogger *logger, unsigned event_flag, const char *fmt,
                ...);
int reqlog_logl(struct reqlogger *logger, unsigned event_flag, const char *s);
int reqlog_loghex(struct reqlogger *logger, unsigned event_flag, const void *d,
                  size_t len);
void reqlog_set_cost(struct reqlogger *logger, double cost);
void reqlog_set_rows(struct reqlogger *logger, int rows);
void reqlog_usetable(struct reqlogger *logger, const char *tablename);
void reqlog_setflag(struct reqlogger *logger, unsigned flag);
int reqlog_logl(struct reqlogger *logger, unsigned event_flag, const char *s);
void reqlog_new_request(struct ireq *iq);
void reqlog_new_sql_request(struct reqlogger *logger, struct string_ref *sr);
void reqlog_set_sql(struct reqlogger *logger, struct string_ref *sr);
void reqlog_set_startprcs(struct reqlogger *logger, uint64_t start);
uint64_t reqlog_current_us(struct reqlogger *logger);
void reqlog_end_request(struct reqlogger *logger, int rc, const char *callfunc, int line);
void reqlog_diffstat_init(struct reqlogger *logger);
/* this is meant to be called by only 1 thread, will need locking if
 * more than one threads were to be involved */
void reqlog_diffstat_dump(struct reqlogger *logger);
int reqlog_diffstat_thresh();
void reqlog_set_diffstat_thresh(int val);
int reqlog_truncate();
void reqlog_set_truncate(int val);
void reqlog_set_vreplays(struct reqlogger *logger, int replays);
void reqlog_set_queue_time(struct reqlogger *logger, uint64_t timeus);
uint64_t reqlog_get_queue_time(const struct reqlogger *logger);
void reqlog_reset_fingerprint(struct reqlogger *logger, size_t n);
void reqlog_set_fingerprint(struct reqlogger *logger, const char *fp, size_t n);
void reqlog_set_rqid(struct reqlogger *logger, void *id, int idlen);
void reqlog_set_event(struct reqlogger *logger, evtype_t evtype);
evtype_t reqlog_get_event(struct reqlogger *logger);
void reqlog_add_table(struct reqlogger *logger, const char *table);
void reqlog_set_error(struct reqlogger *logger, const char *error,
                      int error_code);
void reqlog_set_origin(struct reqlogger *logger, const char *fmt, ...);
const char *reqlog_get_origin(const struct reqlogger *logger);
int reqlog_get_retries(const struct reqlogger *logger);
int reqlog_get_error_code(const struct reqlogger *logger);
void reqlog_set_path(struct reqlogger *logger, struct client_query_stats *path);
void reqlog_set_context(struct reqlogger *logger, int ncontext, char **context);
void reqlog_set_clnt(struct reqlogger *, struct sqlclntstate *);
void reqlog_set_clnt(struct reqlogger *, struct sqlclntstate *);
void reqlog_set_nwrites(struct reqlogger *logger, int nwrites, int cascaded_nwrites);

void reqlog_long_running_clnt(struct sqlclntstate *);
void reqlog_long_running_sql_statements(void);
void reqlog_log_all_longreqs(void);

#endif /* !INCLUDED_COMDB2_H */
