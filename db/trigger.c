#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <sys/types.h>
#include <unistd.h>
#include <poll.h>
#include <pthread.h>
#include <netdb.h>
#include <time.h>

#include <flibc.h>
#include <translistener.h>
#include <sql.h>
#include <sqloffload.h>
#include <sp.h>
#include <locks.h>
#include <bdb_queue.h>
#include <thread_malloc.h>
#include <net_types.h>
#include <trigger.h>
#include <compat.h>
#include <intern_strings.h>
#include <ctrace.h>
#include <comdb2_atomic.h>

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

static pthread_mutex_t trig_thd_cnt_lk = PTHREAD_MUTEX_INITIALIZER;
int gbl_max_trigger_threads = 1000;
static int num_trigger_threads = 0;

int gbl_queuedb_timeout_sec = 10;
static pthread_mutex_t trighash_lk = PTHREAD_MUTEX_INITIALIZER;
typedef struct {
    char *host;
    genid_t trigger_cookie;
    time_t hbeat;
    char spname[0]; /* must be last in struct*/
} trigger_info_t;
static hash_t *trigger_hash;

#define GET_BDB_STATE_CAST(x, cast)                                            \
    void *x = thedb->bdb_env;                                                  \
    if (x == NULL) {                                                           \
        return (cast)CDB2_TRIG_NOT_MASTER;                                     \
    }

#define GET_BDB_STATE(x) GET_BDB_STATE_CAST(x, int)

static trigger_receiver trigger_recv_hostname;
static trigger_receiver *trigger_recv_impl = trigger_recv_hostname;

static trigger_sender trigger_send_hostname;
static trigger_sender *trigger_send_impl = trigger_send_hostname;

void set_trigger_receiver(trigger_receiver *r)
{
    trigger_recv_impl = r;
}

void set_trigger_sender(trigger_sender *s)
{
    trigger_send_impl = s;
}

static trigger_reg_t *trigger_recv_hostname(trigger_reg_t *t, uint8_t *buf)
{
    trigger_reg_to_cpu(t);
    return t;
}

static trigger_reg_t *trigger_send_hostname(uint8_t *b, trigger_reg_t *t, size_t *s)
{
    trigger_reg_t *a = (trigger_reg_t *)b;
    *s = trigger_reg_sz((t)->spname);
    memcpy(a, t, *s);
    trigger_reg_to_net(a);
    return a;
}

static trigger_reg_t *trigger_recv(trigger_reg_t *t, uint8_t *b)
{
    return trigger_recv_impl(t, b);
}

static trigger_reg_t *trigger_send(uint8_t *b, trigger_reg_t *t, size_t *s)
{
    return trigger_send_impl(b, t, s);
}

static void trigger_hash_del(trigger_info_t *info)
{
    hash_del(trigger_hash, info);
    free(info);
}

#define TRIGGER_REG_MAX (sizeof(trigger_reg_t) + MAX_SPNAME + NI_MAXHOST)
static inline int trigger_register_int(trigger_reg_t *t)
{
    GET_BDB_STATE(bdb_state);
    if (!bdb_amimaster(bdb_state)) {
        return CDB2_TRIG_NOT_MASTER;
    }
    if (trigger_hash == NULL) {
        trigger_hash = hash_init_str(offsetof(trigger_info_t, spname));
    }
    uint8_t buf[TRIGGER_REG_MAX];
    t = trigger_recv(t, buf);
    trigger_info_t *info;
    time_t now = time(NULL);
    if ((info = hash_find(trigger_hash, t->spname)) == NULL) {
add:    info = malloc(sizeof(trigger_info_t) + t->spname_len + 1);
        info->host = intern(trigger_hostname(t));
        info->trigger_cookie = t->trigger_cookie;
        info->hbeat = now;
        strcpy(info->spname, t->spname);
        hash_add(trigger_hash, info);
        ctrace("TRIGGER:%s %016" PRIx64 " ASSIGNED\n", info->spname, info->trigger_cookie);
        return CDB2_TRIG_REQ_SUCCESS;
    } else if (strcmp(info->host, trigger_hostname(t)) == 0 &&
               info->trigger_cookie == t->trigger_cookie) {
        info->hbeat = now;
        return CDB2_TRIG_REQ_SUCCESS;
    }
    double diff = difftime(now, info->hbeat);
    if (diff >= gbl_queuedb_timeout_sec) {
        logmsg(LOGMSG_USER,
               "Heartbeat timeout:%.0fs sp:%s host:%s trigger_cookie:%016" PRIx64
               "\n", diff, info->spname, info->host, info->trigger_cookie);
        ctrace("TRIGGER:%s %016" PRIx64 " UNASSIGNED TIMEOUT\n", info->spname, info->trigger_cookie);
        trigger_hash_del(info);
        goto add;
    }
    return CDB2_TRIG_ASSIGNED_OTHER;
}

int trigger_register(trigger_reg_t *t)
{
    GET_BDB_STATE(bdb_state);
    BDB_READLOCK("register trigger");
    Pthread_mutex_lock(&trighash_lk);
    int rc = trigger_register_int(t);
    Pthread_mutex_unlock(&trighash_lk);
    BDB_RELLOCK();
    return rc;
}

static int trigger_unregister_int(trigger_reg_t *t)
{
    if (trigger_hash == NULL) {
        return CDB2_TRIG_REQ_SUCCESS;
    }
    GET_BDB_STATE(bdb_state);
    uint8_t buf[TRIGGER_REG_MAX];
    t = trigger_recv(t, buf);
    trigger_info_t *info;
    if ((info = hash_find(trigger_hash, t->spname)) != NULL &&
        strcmp(info->host, trigger_hostname(t)) == 0 &&
        info->trigger_cookie == t->trigger_cookie
    ){
        ctrace("TRIGGER:%s %016" PRIx64 " UNASSIGNED\n", info->spname, info->trigger_cookie);
        trigger_hash_del(info);
        return CDB2_TRIG_REQ_SUCCESS;
    }
    return CDB2_TRIG_ASSIGNED_OTHER;
}

static int trigger_unregister_lk(trigger_reg_t *t)
{
    Pthread_mutex_lock(&trighash_lk);
    int rc = trigger_unregister_int(t);
    Pthread_mutex_unlock(&trighash_lk);
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

static void *trigger_start_int(void *name)
{
    comdb2_name_thread(__func__);
    exec_trigger(name);
    free(name);
    Pthread_mutex_lock(&trig_thd_cnt_lk);
    num_trigger_threads--;
    Pthread_mutex_unlock(&trig_thd_cnt_lk);
    return NULL;
}

// called when master selects us as the candidate to run trigger 'name'
void trigger_start(const char *name)
{
    if (!gbl_ready) return;
    pthread_t t;
    Pthread_mutex_lock(&trig_thd_cnt_lk);
    if (num_trigger_threads >= gbl_max_trigger_threads) {
        Pthread_mutex_unlock(&trig_thd_cnt_lk);
        logmsg(LOGMSG_ERROR, "%s: Exhausted max trigger threads. Max:%d \n", __func__, gbl_max_trigger_threads);
        return;
    }
    num_trigger_threads++;
    Pthread_mutex_unlock(&trig_thd_cnt_lk);

    if (pthread_create(&t, &gbl_pthread_attr_detached, trigger_start_int, strdup(name))) {
        Pthread_mutex_lock(&trig_thd_cnt_lk);
        num_trigger_threads--;
        Pthread_mutex_unlock(&trig_thd_cnt_lk);
    }
}

// FIXME TODO XXX: KEEP TWO HASHES (1) by spname (2) by node num
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
    Pthread_mutex_lock(&trighash_lk);
    int rc = trigger_unregister_node_int(host);
    Pthread_mutex_unlock(&trighash_lk);
    return rc;
}

void trigger_clear_hash()
{
    Pthread_mutex_lock(&trighash_lk);
    ATOMIC_ADD32(gbl_master_changes, 1);
    hash_t *old = trigger_hash;
    trigger_hash = NULL;
    Pthread_mutex_unlock(&trighash_lk);

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
    t->spname_len = ntohl(t->spname_len);
    t->elect_cookie = ntohl(t->elect_cookie);
    t->trigger_cookie = flibc_ntohll(t->trigger_cookie);
}

int trigger_stat()
{
    GET_BDB_STATE(bdb_state);
    if (!bdb_amimaster(bdb_state)) {
        logmsg(LOGMSG_USER, "%s: cannot run on replicant\n", __func__);
        return -1;
    }
    Pthread_mutex_lock(&trighash_lk);
    time_t now = time(NULL);
    for (int i = 0; i < thedb->num_qdbs; ++i) {
        struct dbtable *qdb = thedb->qdbs[i];
        consumer_lock_read(qdb);
        /* protect us from incomplete triggers (e.g., an old-style queue without a consumer */
        if (qdb->consumers[0] == NULL) {
            consumer_unlock(qdb);
            continue;
        }
        int ctype = dbqueue_consumer_type(qdb->consumers[0]);
        if (ctype != CONSUMER_TYPE_LUA && ctype != CONSUMER_TYPE_DYNLUA) {
            consumer_unlock(qdb);
            continue;
        }
        const char *type = ctype == CONSUMER_TYPE_LUA ? "trigger" : "consumer";
        char *spname = SP4Q(qdb->tablename);
        trigger_info_t *info =
            trigger_hash ? hash_find(trigger_hash, spname) : NULL;
        if (info) {
            logmsg(LOGMSG_USER,
                   "%s: %8s:%s ASSIGNED to node:%s cookie:%016" PRIx64
                   " last heartbeat: %.0fs\n",
                   __func__, type, info->spname, info->host,
                   info->trigger_cookie, difftime(now, info->hbeat));
        } else {
            logmsg(LOGMSG_USER, "%s: %8s:%s UNASSIGNED\n", __func__, type,
                   spname);
        }
        consumer_unlock(qdb);
    }
    Pthread_mutex_unlock(&trighash_lk);
    return 0;
}

static int trigger_registered_int(const char *spname)
{
    trigger_info_t *info;
    if (!trigger_hash) {
        return 0;
    }
    if ((info = hash_find(trigger_hash, spname)) != NULL) {
        time_t diff = time(NULL) - info->hbeat;
        if (diff < gbl_queuedb_timeout_sec) {
            return 1;
        }
    }
    return 0;
}

int trigger_registered(const char *name)
{
    Pthread_mutex_lock(&trighash_lk);
    int rc = trigger_registered_int(name);
    Pthread_mutex_unlock(&trighash_lk);
    return rc;
}

int trigger_register_req(trigger_reg_t *reg)
{
    GET_BDB_STATE(bdb_state);
    size_t sz;
    uint8_t buf[TRIGGER_REG_MAX];
    reg->elect_cookie = ATOMIC_LOAD32(gbl_master_changes);
    trigger_reg_t *t = trigger_send(buf, reg, &sz);
    if (bdb_amimaster(bdb_state)) {
        return trigger_register(t);
    }
    const char *master = bdb_whoismaster(bdb_state);
    if (thedb->handle_sibling == NULL || master == NULL) {
        return NET_SEND_FAIL_INTERNAL; // fake internal retry
    }
    return net_send_message(thedb->handle_sibling, master, NET_TRIGGER_REGISTER, t, sz, 1, 1000);
}

int trigger_unregister_req(trigger_reg_t *reg)
{
    GET_BDB_STATE(bdb_state);
    size_t sz;
    uint8_t buf[TRIGGER_REG_MAX];
    trigger_reg_t *t = trigger_send(buf, reg, &sz);
    if (bdb_amimaster(bdb_state)) {
        return trigger_unregister(t);
    }
    const char *master = bdb_whoismaster(bdb_state);
    if (thedb->handle_sibling == NULL || master == NULL) {
        return NET_SEND_FAIL_INTERNAL; // fake internal retry
    }
    return net_send_message(thedb->handle_sibling, master, NET_TRIGGER_UNREGISTER, t, sz, 1, 100);
}
