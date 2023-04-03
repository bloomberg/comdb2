#ifndef TRIGGER_H
#define TRIGGER_H

#include "genid.h"

struct dbtable;
struct dbenv;
struct consumer;

struct consumer_base {
    int type;
};

struct consumer_stat {
    int has_stuff;
    size_t first_item_length;
    time_t epoch;
    int depth;
};

struct comdb2_queue_consumer {
    int type;
    int (*add_consumer)(struct dbtable *db, int consumern, const char *method, int noremove);
    void (*admin)(struct dbenv *dbenv, int type);
    int (*check_consumer)(const char *method);
    enum consumer_t (*consumer_type)(struct consumer *c);
    void (*coalesce)(struct dbenv *dbenv);
    int (*restart_consumers)(struct dbtable *db);
    int (*stop_consumers)(struct dbtable *db);
    int (*wake_all_consumers)(struct dbtable *db, int force);
    int (*wake_all_consumers_all_queues)(struct dbenv *dbenv, int force);
    int (*handles_method)(const char *method);
    int (*get_name)(struct dbtable *db, char **spname);
    int (*get_stats)(struct dbtable *db, int flags, void *tran, struct consumer_stat *stat);
};
typedef struct comdb2_queue_consumer comdb2_queue_consumer_t;


enum consumer_t {
    CONSUMER_TYPE_API = 0,
    CONSUMER_TYPE_FSTSND = 1,
    CONSUMER_TYPE_JAVASP = 2,
    CONSUMER_TYPE_LUA,
    CONSUMER_TYPE_DYNLUA,

    CONSUMER_TYPE_LAST
};

enum {
    CDB2_TRIG_REQ_SUCCESS = 1,
    CDB2_TRIG_ASSIGNED_OTHER,
    CDB2_TRIG_NOT_MASTER,
};

/* trigger registration info */
typedef struct trigger_reg {
    int node;
    int elect_cookie;
    genid_t trigger_cookie;
    char qdb_locked;
    int spname_len;
    char spname[0]; // spname_len + 1
    // hostname[]
} trigger_reg_t;

struct consumer;
enum consumer_t dbqueue_consumer_type(struct consumer *c);
int trigger_register(trigger_reg_t *);
int trigger_unregister(trigger_reg_t *);
void trigger_start(const char *);
int trigger_register_req(trigger_reg_t *);
int trigger_unregister_req(trigger_reg_t *);
int trigger_unregister_node(const char *node);
int trigger_registered(const char *);
void trigger_clear_hash(void);
int trigger_stat(void);
void trigger_reg_to_cpu(trigger_reg_t *);

#define trigger_reg_to_net trigger_reg_to_cpu

#define trigger_hostname(t) ((t)->spname + (t)->spname_len + 1)

#define trigger_reg_sz(sp_name)                                                \
    sizeof(trigger_reg_t) + strlen(sp_name) + 1 + strlen(gbl_myhostname) + 1

#define trigger_reg_init(dest, sp_name, have_lock)                             \
    do {                                                                       \
        dest = alloca(trigger_reg_sz(sp_name));                                \
        dest->node = 0;                                                        \
        dest->elect_cookie = ATOMIC_LOAD32(gbl_master_changes);                \
        dest->trigger_cookie = get_id(thedb->bdb_env);                         \
        dest->qdb_locked = have_lock;                                          \
        dest->spname_len = strlen(sp_name);                                    \
        memcpy(dest->spname, sp_name, dest->spname_len + 1);                   \
        int hostname_len = strlen(gbl_myhostname);                             \
        memcpy(trigger_hostname(dest), gbl_myhostname, hostname_len + 1);      \
    } while (0)

#define Q4SP(var, spname)                                                      \
    char var[sizeof("__q") + strlen(spname)];                                  \
    sprintf(var, "__q%s", spname);

#define SP4Q(q) ((q) + (sizeof("__q") - 1))

struct lua_State;
void force_unregister(struct lua_State *, trigger_reg_t *);

#endif
