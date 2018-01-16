#ifndef TRIGGER_H
#define TRIGGER_H

#include "genid.h"

enum consumer_t {
    CONSUMER_TYPE_API = 0,
    CONSUMER_TYPE_FSTSND = 1,
    CONSUMER_TYPE_JAVASP = 2,
    CONSUMER_TYPE_LUA,
    CONSUMER_TYPE_DYNLUA,
};

enum {
    CDB2_TRIG_REQ_SUCCESS = 1,
    CDB2_TRIG_ASSIGNED_OTHER,
    CDB2_TRIG_NOT_MASTER,
};

/* trigger registration info */
typedef struct trigger_reg {
    int elect_cookie;
    genid_t trigger_cookie;
    int spname_len;
    char spname[0]; // spname_len + 1
    // hostname[]
} trigger_reg_t;

struct consumer;
enum consumer_t consumer_type(struct consumer *c);
int trigger_register(trigger_reg_t *);
int trigger_unregister(trigger_reg_t *);
void trigger_start(const char *);
int trigger_register_req(trigger_reg_t *);
int trigger_unregister_req(trigger_reg_t *);
int trigger_unregister_node(const char *node);
int trigger_registered(const char *);
void trigger_clear_hash(void);
void trigger_stat(void);
void trigger_reg_to_cpu(trigger_reg_t *);

#define trigger_reg_to_net trigger_reg_to_cpu

#define trigger_hostname(t) ((t)->spname + (t)->spname_len + 1)

#define trigger_reg_sz(sp_name)                                                \
    sizeof(trigger_reg_t) + strlen(sp_name) + 1 + strlen(gbl_mynode) + 1

#define trigger_reg_init(dest, sp_name)                                        \
    do {                                                                       \
        dest = alloca(trigger_reg_sz(sp_name));                                \
        dest->elect_cookie = gbl_master_changes;                               \
        dest->trigger_cookie = get_id(thedb->bdb_env);                         \
        dest->spname_len = strlen(sp_name);                                    \
        strcpy(dest->spname, sp_name);                                         \
        strcpy(trigger_hostname(dest), gbl_mynode);                            \
        trigger_reg_to_net(dest);                                              \
    } while (0)

#define trigger_reg_clone(dest, sz, src)                                       \
    do {                                                                       \
        sz = trigger_reg_sz((src)->spname);                                    \
        dest = alloca(sz);                                                     \
        memcpy(dest, src, sz);                                                 \
    } while (0)

#define Q4SP(var, spname)                                                      \
    char var[sizeof("__q") + strlen(spname)];                                  \
    sprintf(var, "__q%s", spname);
#endif
