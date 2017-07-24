#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <sys/types.h>
#include <unistd.h>
#include <poll.h>
#include <pthread.h>

#include <flibc.h>
#include <translistener.h>
#include <sql.h>
#include <sqloffload.h>
#include <sp.h>
#include <locks.h>
#include <bdb_queue.h>
#include <thread_malloc.h>
#include <net_types.h>
#include <dbqueue.h>
#include <trigger.h>

#include "intern_strings.h"

/*
** Master node will maintain client subscription info.
**
** Request consists of triggername+node+genid.
**
** Genid lets us distinguish between multiple requests,
** for the same trigger from the same node.
**
** Every new master clears subscription info.
*/

static pthread_mutex_t trighash_lk = PTHREAD_MUTEX_INITIALIZER;
typedef struct {
    char *host;
    genid_t trigger_cookie;
    char qname[0];
} trigger_info_t;
static hash_t *trigger_hash;

#define GET_BDB_STATE_CAST(x, cast)                                            \
    void *x = thedb->bdb_env;                                                  \
    if (x == NULL) {                                                           \
        printf("%s thedb->bdb_env:%p\n", __func__, thedb->bdb_env);            \
        return (cast)CDB2_TRIG_NOT_MASTER;                                     \
    }

#define GET_BDB_STATE(x) GET_BDB_STATE_CAST(x, int)

static inline int trigger_register_int(trigger_reg_t *t)
{
    GET_BDB_STATE(bdb_state);
    if (!bdb_amimaster(bdb_state))
        return CDB2_TRIG_NOT_MASTER;
    if (trigger_hash == NULL) {
        printf("setting up trigger registry\n");
        trigger_hash = hash_init_str(offsetof(trigger_info_t, qname));
    }
    trigger_reg_to_cpu(t);
    trigger_info_t *info;
    if ((info = hash_find(trigger_hash, &t->qname)) == NULL) {
        info = malloc(sizeof(trigger_info_t) + strlen(t->qname) + 1);
        info->host = intern(t->qname + t->qlen);
        strcpy(info->qname, t->qname);
        info->trigger_cookie = t->trigger_cookie;
        hash_add(trigger_hash, info);
        printf("%s %s ASSIGNED host:%s trigger_cookie:0x%llx\n", __func__,
               info->qname, info->host, flibc_htonll(info->trigger_cookie));
        return 1;
    } else if (strcmp(info->host, t->qname + t->qlen) &&
               info->trigger_cookie == t->trigger_cookie) {
        printf("%s %s ALREADY ASSIGNED host:%s trigger_cookie:0x%llx\n",
               __func__, info->qname, info->host, flibc_htonll(info->trigger_cookie));
        return 1;
    }
    return CDB2_TRIG_ASSIGNED_OTHER;
}

int trigger_register(trigger_reg_t *t)
{
    GET_BDB_STATE(bdb_state);
    pthread_mutex_lock(&trighash_lk);
    BDB_READLOCK("register trigger");
    int rc = trigger_register_int(t);
    BDB_RELLOCK();
    pthread_mutex_unlock(&trighash_lk);
    return rc;
}

static void trigger_hash_del(trigger_info_t *info)
{
    printf("%s qname:%s node:%s cookie:0x%llx\n", __func__, info->qname,
           info->host, flibc_ntohll(info->trigger_cookie));
    hash_del(trigger_hash, info);
    free(info);
}

static int trigger_unregister_int(trigger_reg_t *t)
{
    GET_BDB_STATE(bdb_state);
    if (trigger_hash == NULL)
        return 0;
    trigger_info_t *info;
    trigger_reg_to_cpu(t);
    if ((info = hash_find(trigger_hash, &t->qname)) != NULL &&
        strcmp(info->host, t->qname + t->qlen) == 0 &&
        info->trigger_cookie == t->trigger_cookie) {
        trigger_hash_del(info);
        return CDB2_TRIG_REQ_SUCCESS;
    }
    printf("%s failed q:%s node:%s trigger_cookie:0x%llx\n", __func__, t->qname,
           t->qname + t->qlen, flibc_htonll(t->trigger_cookie));
    if (info) {
        printf("%s %s registered to node:%s cookie:0x%llx\n", __func__,
               info->qname, info->host, flibc_htonll(info->trigger_cookie));
    } else {
        printf("%s %s was not assigned to any node\n", __func__, t->qname);
    }
    return CDB2_TRIG_ASSIGNED_OTHER;
}

static int trigger_unregister_lk(trigger_reg_t *t)
{
    pthread_mutex_lock(&trighash_lk);
    int rc = trigger_unregister_int(t);
    pthread_mutex_unlock(&trighash_lk);
    return rc;
}

int trigger_unregister(trigger_reg_t *t)
{
    GET_BDB_STATE(bdb_state);
    BDB_READLOCK("unregister trigger");
    int rc = trigger_unregister_lk(t);
    BDB_RELLOCK();
    return rc;
}

static void *trigger_start_int(void *name_)
{
    GET_BDB_STATE_CAST(bdb_state, void *);
    char name[strlen(name_) + 1];
    strcpy(name, name_);
    free(name_);
    trigger_reg_t *reg;
    trigger_reg_init(reg, name);
    printf("%s waiting for %s elect_cookie:%d trigger_cookie:0x%llx\n",
           __func__, reg->qname, ntohl(reg->elect_cookie),
           reg->trigger_cookie);
    int rc, retry = 10;
    while (--retry > 0) {
        bdb_thread_event(bdb_state, BDBTHR_EVENT_START_RDONLY);
        rc = trigger_register_req(reg);
        bdb_thread_event(bdb_state, BDBTHR_EVENT_DONE_RDONLY);
        switch (rc) {
        case CDB2_TRIG_REQ_SUCCESS:
            break;
        case CDB2_TRIG_NOT_MASTER:
        case NET_SEND_FAIL_TIMEOUT:
        case NET_SEND_FAIL_INVALIDNODE:
        case NET_SEND_FAIL_INTERNAL:
            printf("%s trigger_register_req retry rc:%d\n", __func__, rc);
            sleep(1);
            break;
        case CDB2_TRIG_ASSIGNED_OTHER:
        default:
            printf("%s trigger_register_req fail rc:%d\n", __func__, rc);
            return NULL;
        }
        if (rc == CDB2_TRIG_REQ_SUCCESS)
            break;
    }
    if (rc != CDB2_TRIG_REQ_SUCCESS) {
        printf("%s trigger_register_req fail rc:%d\n", __func__, rc);
        return NULL;
    }
    printf("%s assigned - now running %s for %s\n", __func__, reg->qname, name);
    exec_trigger(reg);
    printf("%s done running %s for %s\n", __func__, reg->qname, name);
    return NULL;
}

// called when master selects us as the candidate to run trigger 'name'
void trigger_start(const char *name)
{
    if (!gbl_ready) return;
    printf("%s master selected me to run: %s\n", __func__, name);
    pthread_t t;
    pthread_create(&t, &gbl_pthread_attr_detached, trigger_start_int, strdup(name));
}

// FIXME TODO XXX: KEEP TWO HASHES (1) by qname (2) by node num
static int trigger_unregister_node_int(const char *host)
{
    GET_BDB_STATE(bdb_state);
    if (trigger_hash == NULL)
        return 0;

    unsigned int bkt;
    void *ent;
    trigger_info_t *prev = NULL;
    trigger_info_t *info = hash_first(trigger_hash, &ent, &bkt);
    while (info) {
        if (prev && prev->host == host)
            trigger_hash_del(prev);
        prev = info;
        info = hash_next(trigger_hash, &ent, &bkt);
    }
    // last entry in hash
    if (prev && prev->host == host)
        trigger_hash_del(prev);
    return 0;
}

int trigger_unregister_node(const char *host)
{
    pthread_mutex_lock(&trighash_lk);
    int rc = trigger_unregister_node_int(host);
    pthread_mutex_unlock(&trighash_lk);
    return rc;
}

void trigger_clear_hash()
{
    logmsg(LOGMSG_INFO, "%s CLEARING ENTIRE HASH\n", __func__);
    pthread_mutex_lock(&trighash_lk);
    hash_t *old = trigger_hash;
    trigger_hash = NULL;
    pthread_mutex_unlock(&trighash_lk);

    if (old == NULL)
        return;

    unsigned int bkt;
    void *ent;
    trigger_info_t *prev = NULL;
    trigger_info_t *info = hash_first(old, &ent, &bkt);
    while (info) {
        free(prev);
        prev = info;
        info = hash_next(old, &ent, &bkt);
    }
    free(prev);
    hash_free(old);
}

void trigger_reg_to_cpu(trigger_reg_t *t)
{
    t->qlen = ntohl(t->qlen);
    t->elect_cookie = ntohl(t->elect_cookie);
    t->trigger_cookie = flibc_ntohll(t->trigger_cookie);
}

void trigger_stat()
{
#if 0
	for (int i = 0; i < thedb->num_qdbs; ++i) {
		struct dbtable *qdb = thedb->qdbs[i];
		printf("name: %s type:%d\n", qdb->dbname, qdb->dbtype);
		for (int j = 0; j < MAXCONSUMERS; ++j) {
			struct consumer *consumer = qdb->consumers[i];
			if (!consumer)
				continue;
			printf("  consumer:%d type:%d\n", consumer->consumern, consumer->type);
		}
	}
#endif
    trigger_info_t *info;
    unsigned int bkt;
    void *ent;
    pthread_mutex_lock(&trighash_lk);
    if (trigger_hash) {
        info = hash_first(trigger_hash, &ent, &bkt);
        while (info) {
            printf("%s %s IS ASSIGNED TO node:%d cookie:0x%p\n", __func__,
                   info->qname, info->host, info->trigger_cookie);
            info = hash_next(trigger_hash, &ent, &bkt);
        }
    }
    pthread_mutex_unlock(&trighash_lk);
}

static int trigger_registered_int(const char *name)
{
    if (!trigger_hash)
        return 0;
    void *ent;
    unsigned int bkt;
    trigger_info_t *info = hash_first(trigger_hash, &ent, &bkt);
    while (info) {
        if (strcmp(name, info->qname) == 0)
            return 1;
        info = hash_next(trigger_hash, &ent, &bkt);
    }
    return 0;
}

int trigger_registered(const char *name)
{
    pthread_mutex_lock(&trighash_lk);
    int rc = trigger_registered_int(name);
    pthread_mutex_unlock(&trighash_lk);
    return rc;
}

int trigger_register_req(trigger_reg_t *reg)
{
    GET_BDB_STATE(bdb_state);
    reg->elect_cookie = htonl(gbl_master_changes);
    size_t sz;
    trigger_reg_t *t;
    trigger_reg_clone(t, sz, reg);
    if (bdb_amimaster(bdb_state)) {
        return trigger_register(t);
    }
    const char *master = bdb_whoismaster(bdb_state);
    if (thedb->handle_sibling == NULL || master == NULL) {
        return NET_SEND_FAIL_INTERNAL; // fake internal retry
    }
    return net_send_message(thedb->handle_sibling, master, NET_TRIGGER_REGISTER,
                            t, sz, 1, 500);
}

int trigger_unregister_req(trigger_reg_t *reg)
{
    GET_BDB_STATE(bdb_state);
    size_t sz;
    trigger_reg_t *t;
    trigger_reg_clone(t, sz, reg);
    if (bdb_amimaster(bdb_state)) {
        return trigger_unregister(t);
    }
    const char *master = bdb_whoismaster(bdb_state);
    if (thedb->handle_sibling == NULL || master == NULL) {
        return NET_SEND_FAIL_INTERNAL; // fake internal retry
    }
    return net_send_message(thedb->handle_sibling, master,
                            NET_TRIGGER_UNREGISTER, t, sz, 1, 500);
}

