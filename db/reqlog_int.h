#ifndef INCLUDED_REQLOG_INT_H
#define INCLUDED_REQLOG_INT_H

#include "list.h"
#include "cdb2_constants.h"
#include "cson.h"
#include "sql.h"
#include "reqlog.h"

/* This used to be private to reqlog.  Moving to a shared header since
   eventlog also needs access to reqlog internals.  I am not sure
   which system is going to win here, but it makes sense to separate
   them for now. */

enum logevent_type {
    EVENT_PUSH_PREFIX,
    EVENT_POP_PREFIX,
    EVENT_POP_PREFIX_ALL,
    EVENT_PRINT
};

enum { MAXSTMT = 31, NUMSTMTS = 16 };

struct logevent {
    struct logevent *next;
    enum logevent_type type;
};

struct push_prefix_event {
    struct logevent hdr;
    int length; /* -ve if not known, in which case text must be \0
                   terminated */
    const char *text;
};

struct pop_prefix_event {
    struct logevent hdr;
};

struct print_event {
    struct logevent hdr;
    unsigned event_flag;
    int length; /* -ve if not known, in which case text must be \0
                   terminated */
    char *text;
};

enum { MAX_PREFIXES = 16 };

struct prefix_type {
    char prefix[256];
    int pos;
    int stack[MAX_PREFIXES];
    int stack_pos;
};

struct tablelist {
    struct tablelist *next;
    int count;
    char name[1];
};


struct reqlogger {
    char origin[128];

    /* Everything from here onwards is transient and can be reset
     * with a bzero. */
    int start_transient;

    /* flags that can be set as we progress */
    unsigned reqflags;

    int in_request;
    const char *request_type;
    unsigned event_mask;
    unsigned dump_mask;
    unsigned mask; /* bitwise or of the above two masks */

    struct prefix_type prefix;
    char dumpline[1024];
    int dumplinepos;

    /* list of tables touched by this request */
    int tracking_tables;
    struct tablelist *tables;

    /* our opcode - OP_SQL for sql */
    int opcode;

    struct ireq *iq;

    /* singly linked list of all the stuff we've logged */
    struct logevent *events;
    struct logevent *last_event;

    /* the sql statement */
    struct string_ref *sql_ref;

    /* the bound parameters */
    cson_value *bound_param_cson;

    uint32_t nwrites;   /* number of writes for this txn */
    uint32_t cascaded_nwrites; /* number of cascaded writes for this txn */
    uint32_t nsqlreqs;  /* Atomically counted number of sqlreqs so far */
    int      sqlrows;
    double   sqlcost;

    uint64_t startus;     /* logger start timestamp */
    uint64_t startprcsus; /* processing start timestamp */
    uint64_t durationus;
    uint64_t queuetimeus;
    int rc;
    int vreplays;
    char fingerprint[FINGERPRINTSZ];
    int have_fingerprint;
    char id[41];
    int have_id;
    evtype_t event_type;

    int ntables;
    int alloctables;
    char **sqltables;
    char *error;
    char error_code;

    struct client_query_stats *path;
    int ncontext;
    char **context;
    struct sqlclntstate *clnt;
};

/* a rage of values to look for */
struct range {
    int from;
    int to;
};

struct dblrange {
    double from;
    double to;
};

/* a list of integers to look for (or exclude) */
enum { LIST_MAX = 32 };
struct list {
    unsigned num;
    unsigned inv; /* flag - if set allow values not in list */
    int list[LIST_MAX];
};

struct output {
    LINKC_T(struct output) linkv;

    int refcount;
    int fd;

    int use_time_prefix;
    int lasttime;
    char timeprefix[17]; /* "dd/mm hh:mm:ss: " */

    /* serialise output so that we don't get */
    pthread_mutex_t mutex;

    char filename[1];
};

/* A set of conditions that must be met to log a request, what we want to
 * log and where we should log it. */
struct logrule {

    /* A name for this rule */
    char name[32];

    int active;

    /* Part 1: conditions.  These are all and conditions (all must be met) */

    int count; /* how many to log.  delete rule after this many */

    struct range duration;
    struct range retries;
    struct range vreplays;
    struct dblrange sql_cost;
    struct range sql_rows;

    struct list rc_list;
    struct list opcode_list;

    char tablename[MAXTABLELEN + 1];

    char stmt[MAXSTMT + 1];

    /* Part 2: what to log */

    unsigned event_mask;

    /* Part 3: where to log it */

    struct output *out;

    /* Keep the rules in a linked list */
    LINKC_T(struct logrule) linkv;
};

/* per client request stats */

/* we normalise our request rate report to this period - we have a bucket for
 * each second. */
enum { NUM_BUCKETS = 10 };
struct nodestats {
    unsigned checksum;
    int node;
    char *host;
    struct in_addr addr;
    char *task;
    char *stack;
    int is_ssl;

    int ref;
    pthread_mutex_t mtx;
    LINKC_T(struct nodestats) linkv;

    /* raw counters, totals (updated locklessly by multiple threads) */
    struct rawnodestats rawtotals;

    /* previous totals for last time quanta */
    struct rawnodestats prevtotals;

    /* keep a diff of reqs/second for the last few seconds so we can
     * caculate a smooth reqs/second.  this may not get updated regularly
     * so we record epochms times. */
    unsigned cur_bucket;
    struct rawnodestats raw_buckets[NUM_BUCKETS];
    int bucket_spanms[NUM_BUCKETS];

    char mem[1];
};
typedef struct nodestats nodestats_t;

extern int gbl_time_fdb;

#endif
