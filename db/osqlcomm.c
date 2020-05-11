/*
   Copyright 2015, 2017, Bloomberg Finance L.P.

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

#include <strings.h>
#include <errno.h>
#include <poll.h>
#include <util.h>
#include "osqlcomm.h"
#include "osqlsession.h"
#include "sqloffload.h"
#include "osqlcheckboard.h"
#include "osqlrepository.h"
#include "osqlblockproc.h"
#include <compile_time_assert.h>
#include <netinet/in.h>
#include <endian_core.h>
#include <alloca.h>
#include "comdb2.h"
#include "net.h"
#include "sqlstat1.h"
#include <ctrace.h>
#include "comdb2uuid.h"
#include "socket_interfaces.h"
#include "debug_switches.h"
#include <schemachange.h>
#include <genid.h>
#include <flibc.h>
#include <net_types.h>
#include <errstat.h>
#include "cron.h"
#include <bpfunc.h>
#include <strbuf.h>
#include "logmsg.h"
#include "reqlog.h"
#include "views.h"
#include "str0.h"
#include "sc_struct.h"
#include <compat.h>
#include <unistd.h>

#define MAX_CLUSTER 16

#define UNK_ERR_SEND_RETRY 10
/**
 * NOTE: the assumption here is that there are no more comm users
 * when g_osql_ready is reset.  This is done by closing all possible
 * sqlthread and appsock threads before disabling g_osql_ready.
 *
 */
extern __thread int send_prefault_udp;
extern int gbl_prefault_udp;
extern int g_osql_ready;
extern int gbl_goslow;
extern int gbl_partial_indexes;

int gbl_master_sends_query_effects = 1;
int gbl_toblock_random_deadlock_trans;

int db_is_stopped();

static int osql_net_type_to_net_uuid_type(int type);
static void osql_extract_snap_info(osql_sess_t *sess, void *rpl, int rpllen,
                                   int is_uuid);
typedef struct osql_blknds {
    char *nds[MAX_CLUSTER];        /* list of nodes to blackout in offloading */
    time_t times[MAX_CLUSTER];     /* time of blacking out */
    int n;                         /* 0..n-1 nodes valid in blknds */
    time_t delta;                  /* blackout window in seconds */
    time_t heartbeat[MAX_CLUSTER]; /* the last heartbeat received; used to
                                      select active nodes only */
    time_t delta_hbeat; /* heartbeat timestamp cannot be older than delta_hbeat
                           seconds */
} osql_blknds_t;

typedef struct osql_comm {
    void *
        handle_sibling; /* pointer to netinfo structure supporting offloading */
} osql_comm_t;

typedef struct osql_poke {
    unsigned long long rqid; /* look for this session id */
    int tstamp;              /* when this was sent */
    short from;              /* who sent this (i.e. the master) */
    short to;                /* intended offloading node */
} osql_poke_t;

typedef struct osql_poke_uuid {
    uuid_t uuid; /* look for this session id */
    int tstamp;  /* when this was sent */
} osql_poke_uuid_t;

enum { OSQLCOMM_POKE_TYPE_LEN = 8 + 4 + 2 + 2 };

enum { OSQLCOMM_POKE_UUID_TYPE_LEN = 16 + 4 };

BB_COMPILE_TIME_ASSERT(osqlcomm_poke_type_len,
                       sizeof(osql_poke_t) == OSQLCOMM_POKE_TYPE_LEN);

BB_COMPILE_TIME_ASSERT(osqlcomm_poke_uuid_type_len,
                       sizeof(osql_poke_uuid_t) == OSQLCOMM_POKE_UUID_TYPE_LEN);

static uint8_t *osqlcomm_poke_type_put(const osql_poke_t *p_poke_type,
                                       uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_POKE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_poke_type->rqid), sizeof(p_poke_type->rqid), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_poke_type->tstamp), sizeof(p_poke_type->tstamp), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_poke_type->from), sizeof(p_poke_type->from), p_buf,
                    p_buf_end);
    p_buf =
        buf_put(&(p_poke_type->to), sizeof(p_poke_type->to), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *osqlcomm_poke_type_get(osql_poke_t *p_poke_type,
                                             const uint8_t *p_buf,
                                             const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_POKE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_poke_type->rqid), sizeof(p_poke_type->rqid), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_poke_type->tstamp), sizeof(p_poke_type->tstamp), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_poke_type->from), sizeof(p_poke_type->from), p_buf,
                    p_buf_end);
    p_buf =
        buf_get(&(p_poke_type->to), sizeof(p_poke_type->to), p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *osqlcomm_poke_uuid_type_put(const osql_poke_uuid_t *p_poke_type,
                                            uint8_t *p_buf,
                                            const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_POKE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_put(p_poke_type->uuid, sizeof(p_poke_type->uuid), p_buf,
                           p_buf_end);
    p_buf = buf_put(&(p_poke_type->tstamp), sizeof(p_poke_type->tstamp), p_buf,
                    p_buf_end);

    return p_buf;
}

static const uint8_t *osqlcomm_poke_uuid_type_get(osql_poke_uuid_t *p_poke_type,
                                                  const uint8_t *p_buf,
                                                  const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_POKE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_get(p_poke_type->uuid, sizeof(p_poke_type->uuid), p_buf,
                           p_buf_end);
    p_buf = buf_get(&(p_poke_type->tstamp), sizeof(p_poke_type->tstamp), p_buf,
                    p_buf_end);

    return p_buf;
}

typedef struct osql_echo {
    int nonce;
    int idx;
    unsigned long long snt;
    unsigned long long rcv;
} osql_echo_t;

enum { OSQLCOMM_ECHO_TYPE_LEN = 4 + 4 + 8 + 8 };

BB_COMPILE_TIME_ASSERT(osqlcomm_echo_type_len,
                       sizeof(osql_echo_t) == OSQLCOMM_ECHO_TYPE_LEN);

static uint8_t *osqlcomm_echo_type_put(const osql_echo_t *p_echo_type,
                                       uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_ECHO_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_echo_type->nonce), sizeof(p_echo_type->nonce), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_echo_type->idx), sizeof(p_echo_type->idx), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_echo_type->snt), sizeof(p_echo_type->snt), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_echo_type->rcv), sizeof(p_echo_type->rcv), p_buf,
                    p_buf_end);

    return p_buf;
}

static const uint8_t *osqlcomm_echo_type_get(osql_echo_t *p_echo_type,
                                             const uint8_t *p_buf,
                                             const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_ECHO_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_echo_type->nonce), sizeof(p_echo_type->nonce), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_echo_type->idx), sizeof(p_echo_type->idx), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_echo_type->snt), sizeof(p_echo_type->snt), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_echo_type->rcv), sizeof(p_echo_type->rcv), p_buf,
                    p_buf_end);

    return p_buf;
}

/* messages */
struct osql_req {
    enum OSQL_REQ_TYPE type;
    int rqlen;
    int sqlqlen;
    int padding;
    unsigned long long rqid; /* fastseed */
    char tzname[DB_MAX_TZNAMEDB];
    unsigned char unused;
    unsigned char flags;
    char pad[1];
    char sqlq[1];
};
enum { OSQLCOMM_REQ_TYPE_LEN = 8 + 4 + 4 + 8 + DB_MAX_TZNAMEDB + 3 + 1 };
BB_COMPILE_TIME_ASSERT(osqlcomm_req_type_len,
                       sizeof(struct osql_req) == OSQLCOMM_REQ_TYPE_LEN);

static uint8_t *osqlcomm_req_type_put(const struct osql_req *p_osql_req,
                                      uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_REQ_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

#if 0
    p_buf = buf_no_net_put(&(p_osql_req->pad), sizeof(p_osql_req->pad), p_buf,
            p_buf_end);
#endif

    p_buf =
        buf_put(&p_osql_req->type, sizeof(p_osql_req->type), p_buf, p_buf_end);
    p_buf = buf_put(&p_osql_req->rqlen, sizeof(p_osql_req->rqlen), p_buf,
                    p_buf_end);
    p_buf = buf_put(&p_osql_req->sqlqlen, sizeof(p_osql_req->sqlqlen), p_buf,
                    p_buf_end);
    p_buf = buf_put(&p_osql_req->padding, sizeof(p_osql_req->padding), p_buf,
                    p_buf_end);
    p_buf =
        buf_put(&p_osql_req->rqid, sizeof(p_osql_req->rqid), p_buf, p_buf_end);
    p_buf = buf_put(&p_osql_req->tzname, sizeof(p_osql_req->tzname), p_buf,
                    p_buf_end);
    p_buf = buf_put(&p_osql_req->unused, sizeof(p_osql_req->unused), p_buf,
                    p_buf_end);
    p_buf = buf_put(&p_osql_req->flags, sizeof(p_osql_req->flags), p_buf,
                    p_buf_end);
    p_buf =
        buf_put(&p_osql_req->pad, sizeof(p_osql_req->pad), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *osqlcomm_req_type_get(struct osql_req *p_osql_req,
                                            const uint8_t *p_buf,
                                            const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_REQ_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_osql_req->type), sizeof(p_osql_req->type), p_buf,
                    p_buf_end);
    p_buf = buf_get(&p_osql_req->rqlen, sizeof(p_osql_req->rqlen), p_buf,
                    p_buf_end);
    p_buf = buf_get(&p_osql_req->sqlqlen, sizeof(p_osql_req->sqlqlen), p_buf,
                    p_buf_end);
    p_buf = buf_get(&p_osql_req->padding, sizeof(p_osql_req->padding), p_buf,
                    p_buf_end);
    p_buf =
        buf_get(&p_osql_req->rqid, sizeof(p_osql_req->rqid), p_buf, p_buf_end);
    p_buf = buf_get(&p_osql_req->tzname, sizeof(p_osql_req->tzname), p_buf,
                    p_buf_end);
    p_buf = buf_get(&p_osql_req->unused, sizeof(p_osql_req->unused), p_buf,
                    p_buf_end);
    p_buf = buf_get(&p_osql_req->flags, sizeof(p_osql_req->flags), p_buf,
                    p_buf_end);
    p_buf =
        buf_get(&p_osql_req->pad, sizeof(p_osql_req->pad), p_buf, p_buf_end);

    return (const uint8_t *)p_buf;
}

struct osql_uuid_req {
    enum OSQL_REQ_TYPE type;
    int rqlen;
    int sqlqlen;
    int flags;
    uuid_t uuid;
    char tzname[DB_MAX_TZNAMEDB];
    unsigned char unused;
    char pad[2];
    char sqlq[1];
};
enum { OSQLCOMM_REQ_UUID_TYPE_LEN = 8 + 4 + 4 + 16 + DB_MAX_TZNAMEDB + 3 + 1 };
BB_COMPILE_TIME_ASSERT(osqlcomm_req_uuid_type_len,
                       sizeof(struct osql_req) == OSQLCOMM_REQ_TYPE_LEN);

static uint8_t *
osqlcomm_req_uuid_type_put(const struct osql_uuid_req *p_osql_req,
                           uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_REQ_UUID_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_put(&p_osql_req->type, sizeof(p_osql_req->type), p_buf, p_buf_end);
    p_buf = buf_put(&p_osql_req->rqlen, sizeof(p_osql_req->rqlen), p_buf,
                    p_buf_end);
    p_buf = buf_put(&p_osql_req->sqlqlen, sizeof(p_osql_req->sqlqlen), p_buf,
                    p_buf_end);
    p_buf = buf_put(&p_osql_req->flags, sizeof(p_osql_req->flags), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_put(&p_osql_req->uuid, sizeof(p_osql_req->uuid), p_buf,
                           p_buf_end);
    p_buf = buf_put(&p_osql_req->tzname, sizeof(p_osql_req->tzname), p_buf,
                    p_buf_end);
    p_buf = buf_put(&p_osql_req->unused, sizeof(p_osql_req->unused), p_buf,
                    p_buf_end);
    p_buf =
        buf_put(&p_osql_req->pad, sizeof(p_osql_req->pad), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_req_uuid_type_get(struct osql_uuid_req *p_osql_req,
                           const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_REQ_UUID_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_osql_req->type), sizeof(p_osql_req->type), p_buf,
                    p_buf_end);
    p_buf = buf_get(&p_osql_req->rqlen, sizeof(p_osql_req->rqlen), p_buf,
                    p_buf_end);
    p_buf = buf_get(&p_osql_req->sqlqlen, sizeof(p_osql_req->sqlqlen), p_buf,
                    p_buf_end);
    p_buf = buf_get(&p_osql_req->flags, sizeof(p_osql_req->flags), p_buf,
                    p_buf_end);
    p_buf =
        buf_get(&p_osql_req->uuid, sizeof(p_osql_req->uuid), p_buf, p_buf_end);
    p_buf = buf_get(&p_osql_req->tzname, sizeof(p_osql_req->tzname), p_buf,
                    p_buf_end);
    p_buf = buf_get(&p_osql_req->unused, sizeof(p_osql_req->unused), p_buf,
                    p_buf_end);
    p_buf =
        buf_get(&p_osql_req->pad, sizeof(p_osql_req->pad), p_buf, p_buf_end);

    return (const uint8_t *)p_buf;
}
typedef struct osql_rpl {
    /* keep this header aligned head and tail! */
    enum OSQL_RPL_TYPE type;
    int padding;
    unsigned long long sid; /* concurrent access */
} osql_rpl_t;

enum { OSQLCOMM_RPL_TYPE_LEN = 4 + 4 + 8 };

BB_COMPILE_TIME_ASSERT(osqlcomm_rpl_type_len,
                       sizeof(osql_rpl_t) == OSQLCOMM_RPL_TYPE_LEN);

static uint8_t *osqlcomm_rpl_type_put(const osql_rpl_t *p_osql_rpl,
                                      uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_osql_rpl->type), sizeof(p_osql_rpl->type), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_put(&(p_osql_rpl->padding), sizeof(p_osql_rpl->padding),
                           p_buf, p_buf_end);
    if (p_osql_rpl->sid == 0)
        abort();
    p_buf =
        buf_put(&(p_osql_rpl->sid), sizeof(p_osql_rpl->sid), p_buf, p_buf_end);

    return p_buf;
}

/*static const uint8_t *osqlcomm_rpl_type_get(osql_rpl_t *p_osql_rpl, const
 * uint8_t */
const uint8_t *osqlcomm_rpl_type_get(osql_rpl_t *p_osql_rpl,
                                     const uint8_t *p_buf,
                                     const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_osql_rpl->type), sizeof(p_osql_rpl->type), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_get(&(p_osql_rpl->padding), sizeof(p_osql_rpl->padding),
                           p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_osql_rpl->sid), sizeof(p_osql_rpl->sid), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_rpl_uuid {
    enum OSQL_RPL_TYPE type;
    int padding;
    uuid_t uuid;
} osql_uuid_rpl_t;

enum { OSQLCOMM_UUID_RPL_TYPE_LEN = 4 + 4 + 16 };

BB_COMPILE_TIME_ASSERT(osqlcomm_rpl_uuid_type_len,
                       sizeof(osql_uuid_rpl_t) == OSQLCOMM_UUID_RPL_TYPE_LEN);

uuid_t zero_uuid = {0};

static uint8_t *osqlcomm_uuid_rpl_type_put(const osql_uuid_rpl_t *p_osql_rpl,
                                           uint8_t *p_buf,
                                           const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_UUID_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    if (!memcmp(&p_osql_rpl->uuid, &zero_uuid, sizeof(uuid_t)))
        abort();

    p_buf = buf_put(&(p_osql_rpl->type), sizeof(p_osql_rpl->type), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_put(&(p_osql_rpl->padding), sizeof(p_osql_rpl->padding),
                           p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_osql_rpl->uuid), sizeof(p_osql_rpl->uuid), p_buf,
                           p_buf_end);

    return p_buf;
}

/*static const uint8_t *osqlcomm_rpl_type_get(osql_rpl_t *p_osql_rpl, const
 * uint8_t */
const uint8_t *osqlcomm_uuid_rpl_type_get(osql_uuid_rpl_t *p_osql_rpl,
                                          const uint8_t *p_buf,
                                          const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_UUID_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_osql_rpl->type), sizeof(p_osql_rpl->type), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_get(&(p_osql_rpl->padding), sizeof(p_osql_rpl->padding),
                           p_buf, p_buf_end);
    p_buf = buf_get(&(p_osql_rpl->uuid), sizeof(p_osql_rpl->uuid), p_buf,
                    p_buf_end);

    return p_buf;
}
typedef struct osql_del {
    unsigned long long genid;
    unsigned long long dk; /* flag to indicate which keys to modify */
} osql_del_t;

enum { OSQLCOMM_DEL_TYPE_LEN = 8 + 8 };

BB_COMPILE_TIME_ASSERT(osqlcomm_del_type_len,
                       sizeof(osql_del_t) == OSQLCOMM_DEL_TYPE_LEN);

static uint8_t *osqlcomm_del_type_put(const osql_del_t *p_osql_del,
                                      uint8_t *p_buf, const uint8_t *p_buf_end,
                                      int send_dk)
{
    if (p_buf_end < p_buf ||
        (send_dk ? OSQLCOMM_DEL_TYPE_LEN
                 : OSQLCOMM_DEL_TYPE_LEN - sizeof(unsigned long long)) >
            (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_put(&(p_osql_del->genid), sizeof(p_osql_del->genid),
                           p_buf, p_buf_end);
    if (send_dk)
        p_buf = buf_no_net_put(&(p_osql_del->dk), sizeof(p_osql_del->dk), p_buf,
                               p_buf_end);

    return p_buf;
}

static const uint8_t *osqlcomm_del_type_get(osql_del_t *p_osql_del,
                                            const uint8_t *p_buf,
                                            const uint8_t *p_buf_end,
                                            int recv_dk)
{
    if (p_buf_end < p_buf ||
        (recv_dk ? OSQLCOMM_DEL_TYPE_LEN
                 : OSQLCOMM_DEL_TYPE_LEN - sizeof(unsigned long long)) >
            (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_get(&(p_osql_del->genid), sizeof(p_osql_del->genid),
                           p_buf, p_buf_end);
    if (recv_dk)
        p_buf = buf_no_net_get(&(p_osql_del->dk), sizeof(p_osql_del->dk), p_buf,
                               p_buf_end);

    return p_buf;
}

typedef struct {
    osql_uuid_rpl_t hd;
    genid_t genid;
} osql_dbq_consume_uuid_t;

typedef struct {
    osql_rpl_t hd;
    genid_t genid;
} osql_dbq_consume_t;

typedef struct osql_del_rpl {
    osql_rpl_t hd;
    osql_del_t dt;
} osql_del_rpl_t;

enum {
    OSQLCOMM_OSQL_DEL_RPL_TYPE_LEN =
        OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_DEL_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_del_rpl_type_len,
                       sizeof(osql_del_rpl_t) ==
                           OSQLCOMM_OSQL_DEL_RPL_TYPE_LEN);

static uint8_t *osqlcomm_del_rpl_type_put(osql_del_rpl_t *p_del_rpl,
                                          uint8_t *p_buf, uint8_t *p_buf_end,
                                          int send_dk)
{
    if (p_buf_end < p_buf ||
        (send_dk ? OSQLCOMM_OSQL_DEL_RPL_TYPE_LEN
                 : OSQLCOMM_OSQL_DEL_RPL_TYPE_LEN -
                       sizeof(unsigned long long)) > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(&(p_del_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_del_type_put(&(p_del_rpl->dt), p_buf, p_buf_end, send_dk);

    return p_buf;
}

/* startgen */
typedef struct osql_startgen {
    unsigned int start_gen;
    int padding;
} osql_startgen_t;

enum { OSQLCOMM_STARTGEN_TYPE_LEN = 8 };

BB_COMPILE_TIME_ASSERT(osqlcomm_startgen_type_len,
                       sizeof(osql_startgen_t) == OSQLCOMM_STARTGEN_TYPE_LEN);

static uint8_t *
osqlcomm_startgen_type_put(const osql_startgen_t *p_osql_startgen,
                           uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_STARTGEN_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf = buf_put(&(p_osql_startgen->start_gen),
                    sizeof(p_osql_startgen->start_gen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_osql_startgen->padding),
                    sizeof(p_osql_startgen->padding), p_buf, p_buf_end);
    return p_buf;
}

const uint8_t *osqlcomm_startgen_type_get(osql_startgen_t *p_osql_startgen,
                                          const uint8_t *p_buf,
                                          const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_STARTGEN_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf = buf_get(&(p_osql_startgen->start_gen),
                    sizeof(p_osql_startgen->start_gen), p_buf, p_buf_end);
    return p_buf;
}

typedef struct osql_startgen_rpl {
    osql_rpl_t hd;
    osql_startgen_t dt;
} osql_startgen_rpl_t;

enum {
    OSQLCOMM_STARTGEN_RPL_LEN =
        OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_STARTGEN_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_startgen_rpl_len,
                       sizeof(osql_startgen_rpl_t) ==
                           OSQLCOMM_STARTGEN_RPL_LEN);

static uint8_t *
osqlcomm_startgen_rpl_type_put(const osql_startgen_rpl_t *p_startgen_rpl,
                               uint8_t *p_buf, uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_STARTGEN_RPL_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf = osqlcomm_rpl_type_put(&(p_startgen_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_startgen_type_put(&(p_startgen_rpl->dt), p_buf, p_buf_end);
    return p_buf;
}

static const uint8_t *
osqlcomm_startgen_rpl_type_get(osql_startgen_rpl_t *p_startgen_rpl,
                               const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_STARTGEN_RPL_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf = osqlcomm_rpl_type_get(&(p_startgen_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_startgen_type_get(&(p_startgen_rpl->dt), p_buf, p_buf_end);
    return p_buf;
}

typedef struct osql_startgen_uuid_rpl {
    osql_uuid_rpl_t hd;
    osql_startgen_t dt;
} osql_startgen_uuid_rpl_t;

enum {
    OSQLCOMM_STARTGEN_UUID_RPL_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_STARTGEN_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_startgen_uuid_type_len,
                       sizeof(osql_startgen_uuid_rpl_t) ==
                           OSQLCOMM_STARTGEN_UUID_RPL_LEN);

static uint8_t *osqlcomm_startgen_uuid_rpl_type_put(
    const osql_startgen_uuid_rpl_t *p_startgen_rpl, uint8_t *p_buf,
    uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_STARTGEN_UUID_RPL_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf = osqlcomm_uuid_rpl_type_put(&(p_startgen_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_startgen_type_put(&(p_startgen_rpl->dt), p_buf, p_buf_end);
    return p_buf;
}

static const uint8_t *
osqlcomm_startgen_uuid_rpl_type_get(osql_startgen_uuid_rpl_t *p_startgen_rpl,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_STARTGEN_UUID_RPL_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf = osqlcomm_uuid_rpl_type_get(&(p_startgen_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_startgen_type_get(&(p_startgen_rpl->dt), p_buf, p_buf_end);
    return p_buf;
}

/********* OSQL BPLOG FUNC   *****************/

typedef struct osql_bpfunc {
    int32_t data_len;
    uint8_t data[];
} osql_bpfunc_t;

enum { OSQLCOMM_BPFUNC_TYPE_LEN = 4 };

BB_COMPILE_TIME_ASSERT(osqlcomm_bpfunc_type_len,
                       sizeof(osql_bpfunc_t) == OSQLCOMM_BPFUNC_TYPE_LEN);

static uint8_t *osqlcomm_bpfunc_type_put(const osql_bpfunc_t *p_osql_bpfunc,
                                         uint8_t *p_buf,
                                         const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_BPFUNC_TYPE_LEN + p_osql_bpfunc->data_len >
            (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&p_osql_bpfunc->data_len, sizeof(p_osql_bpfunc->data_len),
                    p_buf, p_buf_end);
    p_buf = buf_no_net_put(&p_osql_bpfunc->data, p_osql_bpfunc->data_len, p_buf,
                           p_buf_end);

    return p_buf;
}

static const uint8_t *osqlcomm_bpfunc_type_get(osql_bpfunc_t **p_osql_bpfunc,
                                               const uint8_t *p_buf,
                                               const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_BPFUNC_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;
    int32_t data_len = 0;

    p_buf = buf_get(&data_len, sizeof(data_len), p_buf, p_buf_end);

    *p_osql_bpfunc = malloc(sizeof(osql_bpfunc_t) + data_len);

    if (!*p_osql_bpfunc)
        return p_buf;

    (*p_osql_bpfunc)->data_len = data_len;

    if (data_len > 0) {
        p_buf = buf_no_net_get(&((*p_osql_bpfunc)->data), data_len, p_buf,
                               p_buf_end);
    }

    return p_buf;
}

static uint8_t *osqlcomm_bpfunc_rpl_type_put(osql_rpl_t *hd, osql_bpfunc_t *dt,
                                             uint8_t *p_buf, uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_BPFUNC_TYPE_LEN + dt->data_len >
            (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(hd, p_buf, p_buf_end);
    p_buf = osqlcomm_bpfunc_type_put(dt, p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *osqlcomm_bpfunc_uuid_rpl_type_put(osql_uuid_rpl_t *hd,
                                                  osql_bpfunc_t *dt,
                                                  uint8_t *p_buf,
                                                  uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_BPFUNC_TYPE_LEN + dt->data_len >
            (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_put(hd, p_buf, p_buf_end);
    p_buf = osqlcomm_bpfunc_type_put(dt, p_buf, p_buf_end);

    return p_buf;
}

/********* OSQL_SCHEMACHANGE *****************/

static uint8_t *osqlcomm_schemachange_type_get(struct schema_change_type *sc,
                                               const uint8_t *p_buf,
                                               const uint8_t *p_buf_end)
{
    uint8_t *tmp_buf =
        buf_get_schemachange(sc, (void *)p_buf, (void *)p_buf_end);

    if (sc->tablename_len == -1 || sc->fname_len == -1 || sc->aname_len == -1)
        return NULL;

    return tmp_buf;
}

static uint8_t *
osqlcomm_schemachange_rpl_type_put(osql_rpl_t *hd,
                                   struct schema_change_type *sc,
                                   uint8_t *p_buf, uint8_t *p_buf_end)
{
    size_t sc_len = schemachange_packed_size(sc);

    if (p_buf_end < p_buf ||
        OSQLCOMM_RPL_TYPE_LEN + sc_len > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(hd, p_buf, p_buf_end);
    p_buf = buf_put_schemachange(sc, p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *
osqlcomm_schemachange_uuid_rpl_type_put(osql_uuid_rpl_t *hd,
                                        struct schema_change_type *sc,
                                        uint8_t *p_buf, uint8_t *p_buf_end)
{
    size_t sc_len = schemachange_packed_size(sc);

    if (p_buf_end < p_buf ||
        OSQLCOMM_UUID_RPL_TYPE_LEN + sc_len > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_put(hd, p_buf, p_buf_end);
    p_buf = buf_put_schemachange(sc, p_buf, p_buf_end);

    return p_buf;
}
/***************************************************************************/

typedef struct osql_serial {
    // ranges info
    int buf_size;
    int arr_size;
    unsigned int file;
    unsigned int offset;
} osql_serial_t;

typedef struct osql_del_uuid_rpl {
    osql_uuid_rpl_t hd;
    osql_del_t dt;
} osql_del_uuid_rpl_t;

enum {
    OSQLCOMM_OSQL_DEL_UUID_RPL_TYPE_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_DEL_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_del_uuid_rpl_type_len,
                       sizeof(osql_del_uuid_rpl_t) ==
                           OSQLCOMM_OSQL_DEL_UUID_RPL_TYPE_LEN);

static uint8_t *osqlcomm_del_uuid_rpl_type_put(osql_del_uuid_rpl_t *p_del_rpl,
                                               uint8_t *p_buf,
                                               uint8_t *p_buf_end, int send_dk)
{
    if (p_buf_end < p_buf ||
        (send_dk ? OSQLCOMM_OSQL_DEL_UUID_RPL_TYPE_LEN
                 : OSQLCOMM_OSQL_DEL_UUID_RPL_TYPE_LEN -
                       sizeof(unsigned long long)) > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_put(&(p_del_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_del_type_put(&(p_del_rpl->dt), p_buf, p_buf_end, send_dk);

    return p_buf;
}

enum { OSQLCOMM_SERIAL_TYPE_LEN = 4 + 4 + 4 + 4 };

BB_COMPILE_TIME_ASSERT(osqlcomm_serial_type_len,
                       sizeof(osql_serial_t) == OSQLCOMM_SERIAL_TYPE_LEN);

static uint8_t *osqlcomm_serial_type_put(const osql_serial_t *p_osql_serial,
                                         uint8_t *p_buf,
                                         const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_SERIAL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_osql_serial->buf_size), sizeof(p_osql_serial->buf_size),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_osql_serial->arr_size), sizeof(p_osql_serial->arr_size),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_osql_serial->file), sizeof(p_osql_serial->file), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_osql_serial->offset), sizeof(p_osql_serial->offset),
                    p_buf, p_buf_end);

    return p_buf;
}
static const uint8_t *osqlcomm_serial_type_get(osql_serial_t *p_osql_serial,
                                               const uint8_t *p_buf,
                                               const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_SERIAL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_osql_serial->buf_size), sizeof(p_osql_serial->buf_size),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_osql_serial->arr_size), sizeof(p_osql_serial->arr_size),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_osql_serial->file), sizeof(p_osql_serial->file), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_osql_serial->offset), sizeof(p_osql_serial->offset),
                    p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_serial_rpl {
    osql_rpl_t hd;
    osql_serial_t dt;
} osql_serial_rpl_t;

enum {
    OSQLCOMM_SERIAL_RPL_LEN = OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_SERIAL_TYPE_LEN
};

typedef struct osql_serial_uuid_rpl {
    osql_uuid_rpl_t hd;
    osql_serial_t dt;
} osql_serial_uuid_rpl_t;

enum {
    OSQLCOMM_SERIAL_UUID_RPL_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_SERIAL_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_serial_uuid_rpl_len,
                       sizeof(osql_serial_uuid_rpl_t) ==
                           OSQLCOMM_SERIAL_UUID_RPL_LEN);

static uint8_t *
osqlcomm_serial_rpl_put(const osql_serial_rpl_t *p_osql_serial_rpl,
                        uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_SERIAL_RPL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(&(p_osql_serial_rpl->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_serial_type_put(&(p_osql_serial_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_serial_rpl_get(osql_serial_rpl_t *p_osql_serial_rpl,
                        const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_SERIAL_RPL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_get(&(p_osql_serial_rpl->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_serial_type_get(&(p_osql_serial_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *
osqlcomm_serial_uuid_rpl_put(const osql_serial_uuid_rpl_t *p_osql_serial_rpl,
                             uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_SERIAL_UUID_RPL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        osqlcomm_uuid_rpl_type_put(&(p_osql_serial_rpl->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_serial_type_put(&(p_osql_serial_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_serial_uuid_rpl_get(osql_serial_uuid_rpl_t *p_osql_serial_rpl,
                             const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_SERIAL_UUID_RPL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        osqlcomm_uuid_rpl_type_get(&(p_osql_serial_rpl->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_serial_type_get(&(p_osql_serial_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *serial_readset_put(const CurRangeArr *arr, int buf_size,
                                   uint8_t *p_buf, const uint8_t *p_buf_end)
{
    int i;
    int tmp;
    CurRange *cr;
    if (p_buf_end < p_buf || buf_size > (p_buf_end - p_buf))
        return NULL;
    for (i = 0; i < arr->size; i++) {
        cr = arr->ranges[i];

        tmp = strlen(cr->tbname) + 1;
        p_buf = buf_put(&tmp, sizeof(tmp), p_buf, p_buf_end);
        p_buf = buf_put(cr->tbname, tmp, p_buf, p_buf_end);

        p_buf =
            buf_put(&(cr->islocked), sizeof(cr->islocked), p_buf, p_buf_end);

        if (!cr->islocked) {
            p_buf =
                buf_put(&(cr->idxnum), sizeof(cr->idxnum), p_buf, p_buf_end);
            p_buf = buf_put(&(cr->lflag), sizeof(cr->lflag), p_buf, p_buf_end);
            if (!cr->lflag) {
                p_buf = buf_put(&(cr->lkeylen), sizeof(cr->lkeylen), p_buf,
                                p_buf_end);
                p_buf = buf_put(cr->lkey, cr->lkeylen, p_buf, p_buf_end);
            }

            p_buf = buf_put(&(cr->rflag), sizeof(cr->rflag), p_buf, p_buf_end);
            if (!cr->rflag) {
                p_buf = buf_put(&(cr->rkeylen), sizeof(cr->rkeylen), p_buf,
                                p_buf_end);
                p_buf = buf_put(cr->rkey, cr->rkeylen, p_buf, p_buf_end);
            }
        }
    }
    return p_buf;
}

static const uint8_t *serial_readset_get(CurRangeArr *arr, int buf_size,
                                         int arr_size, const uint8_t *p_buf,
                                         const uint8_t *p_buf_end)
{
    int i;
    int tmp = 0;
    CurRange *cr;
    if (p_buf_end < p_buf || buf_size > (p_buf_end - p_buf))
        return NULL;
    for (i = 0; i < arr_size; i++) {
        cr = currange_new();

        p_buf = buf_get(&tmp, sizeof(tmp), p_buf, p_buf_end);
        cr->tbname = malloc(sizeof(char) * tmp);
        p_buf = buf_get(cr->tbname, tmp, p_buf, p_buf_end);

        p_buf =
            buf_get(&(cr->islocked), sizeof(cr->islocked), p_buf, p_buf_end);

        if (!cr->islocked) {
            p_buf =
                buf_get(&(cr->idxnum), sizeof(cr->idxnum), p_buf, p_buf_end);
            p_buf = buf_get(&(cr->lflag), sizeof(cr->lflag), p_buf, p_buf_end);
            if (!cr->lflag) {
                p_buf = buf_get(&(cr->lkeylen), sizeof(cr->lkeylen), p_buf,
                                p_buf_end);
                cr->lkey = malloc(cr->lkeylen);
                p_buf = buf_get(cr->lkey, cr->lkeylen, p_buf, p_buf_end);
            }

            p_buf = buf_get(&(cr->rflag), sizeof(cr->rflag), p_buf, p_buf_end);
            if (!cr->rflag) {
                p_buf = buf_get(&(cr->rkeylen), sizeof(cr->rkeylen), p_buf,
                                p_buf_end);
                cr->rkey = malloc(cr->rkeylen);
                p_buf = buf_get(cr->rkey, cr->rkeylen, p_buf, p_buf_end);
            }
        } else {
            cr->lflag = 1;
            cr->rflag = 1;
        }

        currangearr_append(arr, cr);
    }
    return p_buf;
}

enum { OSQLCOMM_QUERY_EFFECTS_LEN = sizeof(struct query_effects) };

uint8_t *osqlcomm_query_effects_put(const struct query_effects *effects,
                                    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_QUERY_EFFECTS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(effects->num_affected), sizeof(effects->num_affected),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(effects->num_selected), sizeof(effects->num_selected),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(effects->num_updated), sizeof(effects->num_updated),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(effects->num_deleted), sizeof(effects->num_deleted),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(effects->num_inserted), sizeof(effects->num_inserted),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *osqlcomm_query_effects_get(struct query_effects *effects,
                                          const uint8_t *p_buf,
                                          const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_QUERY_EFFECTS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(effects->num_affected), sizeof(effects->num_affected),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(effects->num_selected), sizeof(effects->num_selected),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(effects->num_updated), sizeof(effects->num_updated),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(effects->num_deleted), sizeof(effects->num_deleted),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(effects->num_inserted), sizeof(effects->num_inserted),
                    p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_done {
    int rc;
    int nops;
} osql_done_t;

enum {
    OSQLCOMM_DONE_TYPE_LEN = 4 + 4,
};

BB_COMPILE_TIME_ASSERT(osqlcomm_done_type_len,
                       sizeof(osql_done_t) == OSQLCOMM_DONE_TYPE_LEN);

static uint8_t *osqlcomm_done_type_put(const osql_done_t *p_osql_done,
                                       uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_DONE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_put(&(p_osql_done->rc), sizeof(p_osql_done->rc), p_buf, p_buf_end);
    p_buf = buf_put(&(p_osql_done->nops), sizeof(p_osql_done->nops), p_buf,
                    p_buf_end);

    return p_buf;
}

static const uint8_t *osqlcomm_done_type_get(osql_done_t *p_osql_done,
                                             const uint8_t *p_buf,
                                             const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_DONE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_get(&(p_osql_done->rc), sizeof(p_osql_done->rc), p_buf, p_buf_end);
    p_buf = buf_get(&(p_osql_done->nops), sizeof(p_osql_done->nops), p_buf,
                    p_buf_end);

    return p_buf;
}

typedef struct osql_done_rpl {
    osql_rpl_t hd;
    osql_done_t dt;
} osql_done_rpl_t;

enum { OSQLCOMM_DONE_RPL_LEN = OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_DONE_TYPE_LEN };

BB_COMPILE_TIME_ASSERT(osqlcomm_done_rpl_len,
                       sizeof(osql_done_rpl_t) == OSQLCOMM_DONE_RPL_LEN);

static uint8_t *osqlcomm_done_rpl_put(const osql_done_rpl_t *p_osql_done_rpl,
                                      uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_DONE_RPL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(&(p_osql_done_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_done_type_put(&(p_osql_done_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_done_uuid_rpl {
    osql_uuid_rpl_t hd;
    osql_done_t dt;
    struct query_effects effects;
    struct query_effects fk_effects;
} osql_done_uuid_rpl_t;

enum {
    OSQLCOMM_DONE_UUID_RPL_v1_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_DONE_TYPE_LEN,
    OSQLCOMM_DONE_UUID_RPL_v2_LEN =
        OSQLCOMM_DONE_UUID_RPL_v1_LEN + (2 * sizeof(struct query_effects)),
};

#if 0
BB_COMPILE_TIME_ASSERT(osqlcomm_done_uuid_rpl_len,
                       sizeof(osql_done_uuid_rpl_t) ==
                           OSQLCOMM_DONE_UUID_RPL_LEN);
#endif

static uint8_t *
osqlcomm_done_uuid_rpl_put(const osql_done_uuid_rpl_t *p_osql_done_uuid_rpl,
                           uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_DONE_UUID_RPL_v1_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_put(&(p_osql_done_uuid_rpl->hd), p_buf,
                                       p_buf_end);
    p_buf =
        osqlcomm_done_type_put(&(p_osql_done_uuid_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_done_rpl_stats {
    osql_rpl_t hd;
    osql_done_t dt;
    // TODO: (NC) what is this??
    struct client_query_stats stats;
} osql_done_rpl_stats_t;

enum {
    OSQLCOMM_DONE_RPL_STATS_LEN =
        OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_DONE_TYPE_LEN + CLIENT_QUERY_STATS_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_done_rpl_stats_len,
                       sizeof(osql_done_rpl_stats_t) ==
                           OSQLCOMM_DONE_RPL_STATS_LEN);

/* getter for path_component */
static const uint8_t *
client_query_path_component_get(struct client_query_path_component *p_path,
                                const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        CLIENT_QUERY_PATH_COMPONENT_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_path->nfind), sizeof(p_path->nfind), p_buf, p_buf_end);
    p_buf = buf_get(&(p_path->nnext), sizeof(p_path->nnext), p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_path->nwrite), sizeof(p_path->nwrite), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_path->table), sizeof(p_path->table), p_buf,
                           p_buf_end);
    p_buf = buf_get(&(p_path->ix), sizeof(p_path->ix), p_buf, p_buf_end);

    return p_buf;
}

/* setter for path_component */
static uint8_t *client_query_path_component_put(
    const struct client_query_path_component *p_path, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        CLIENT_QUERY_PATH_COMPONENT_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_path->nfind), sizeof(p_path->nfind), p_buf, p_buf_end);
    p_buf = buf_put(&(p_path->nnext), sizeof(p_path->nnext), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_path->nwrite), sizeof(p_path->nwrite), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_path->table), sizeof(p_path->table), p_buf,
                           p_buf_end);
    p_buf = buf_put(&(p_path->ix), sizeof(p_path->ix), p_buf, p_buf_end);

    return p_buf;
}

/* getter for client_query_stats - up to the path_component */
static const uint8_t *
client_query_stats_nopath_get(struct client_query_stats *p_stats,
                              const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        CLIENT_QUERY_STATS_PATH_OFFSET > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_stats->queryid), sizeof(p_stats->queryid), p_buf,
                    p_buf_end);
    p_buf =
        buf_get(&(p_stats->nlocks), sizeof(p_stats->nlocks), p_buf, p_buf_end);
    p_buf = buf_get(&(p_stats->n_write_ios), sizeof(p_stats->n_write_ios),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_stats->n_read_ios), sizeof(p_stats->n_read_ios), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_get(&(p_stats->reserved), sizeof(p_stats->reserved),
                           p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_stats->n_rows), sizeof(p_stats->n_rows), p_buf, p_buf_end);
    p_buf = buf_get(&(p_stats->n_components), sizeof(p_stats->n_components),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_stats->cost), sizeof(p_stats->cost), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *snap_uid_put(const snap_uid_t *snap_info, uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SNAP_UID_LENGTH > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_put(snap_info->uuid, sizeof(snap_info->uuid), p_buf,
                           p_buf_end);
    p_buf = buf_put(&(snap_info->rqtype), sizeof(snap_info->rqtype), p_buf,
                    p_buf_end);
    p_buf = osqlcomm_query_effects_put(&(snap_info->effects), p_buf, p_buf_end);
    p_buf = buf_put(&(snap_info->unused), sizeof(snap_info->unused), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(snap_info->replicant_is_able_to_retry),
                    sizeof(snap_info->replicant_is_able_to_retry), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(snap_info->keylen), sizeof(snap_info->keylen), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_put(&(snap_info->key), sizeof(snap_info->key), p_buf,
                           p_buf_end);

    return p_buf;
}

static const uint8_t *snap_uid_get(snap_uid_t *snap_info, const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SNAP_UID_LENGTH > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_get(snap_info->uuid, sizeof(snap_info->uuid), p_buf,
                           p_buf_end);
    p_buf = buf_get(&(snap_info->rqtype), sizeof(snap_info->rqtype), p_buf,
                    p_buf_end);
    p_buf = osqlcomm_query_effects_get(&(snap_info->effects), p_buf, p_buf_end);
    p_buf = buf_get(&(snap_info->unused), sizeof(snap_info->unused), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(snap_info->replicant_is_able_to_retry),
                    sizeof(snap_info->replicant_is_able_to_retry), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(snap_info->keylen), sizeof(snap_info->keylen), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_get(&(snap_info->key), sizeof(snap_info->key), p_buf,
                           p_buf_end);

    return p_buf;
}

/* getter for client_query_stats */
static const uint8_t *client_query_stats_get(struct client_query_stats *p_stats,
                                             const uint8_t *p_buf,
                                             const uint8_t *p_buf_end)
{
    int ii;
    if (p_buf_end < p_buf ||
        CLIENT_QUERY_STATS_PATH_OFFSET > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_stats->queryid), sizeof(p_stats->queryid), p_buf,
                    p_buf_end);
    p_buf =
        buf_get(&(p_stats->nlocks), sizeof(p_stats->nlocks), p_buf, p_buf_end);
    p_buf = buf_get(&(p_stats->n_write_ios), sizeof(p_stats->n_write_ios),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_stats->n_read_ios), sizeof(p_stats->n_read_ios), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_get(&(p_stats->reserved), sizeof(p_stats->reserved),
                           p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_stats->n_rows), sizeof(p_stats->n_rows), p_buf, p_buf_end);
    p_buf = buf_get(&(p_stats->n_components), sizeof(p_stats->n_components),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_stats->cost), sizeof(p_stats->cost), p_buf, p_buf_end);

    if ((p_stats->n_components * CLIENT_QUERY_PATH_COMPONENT_LEN) >
        (p_buf_end - p_buf))
        return NULL;

    for (ii = 0; ii < p_stats->n_components; ii++) {
        p_buf = client_query_path_component_get(&(p_stats->path_stats[ii]),
                                                p_buf, p_buf_end);
    }

    return p_buf;
}

/* setter for dump_client_query_stats - exposed for dbglog_support */
uint8_t *client_query_stats_put(const struct client_query_stats *p_stats,
                                uint8_t *p_buf, const uint8_t *p_buf_end)
{
    int ii;
    if (p_buf_end < p_buf ||
        CLIENT_QUERY_STATS_PATH_OFFSET > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_stats->queryid), sizeof(p_stats->queryid), p_buf,
                    p_buf_end);
    p_buf =
        buf_put(&(p_stats->nlocks), sizeof(p_stats->nlocks), p_buf, p_buf_end);
    p_buf = buf_put(&(p_stats->n_write_ios), sizeof(p_stats->n_write_ios),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_stats->n_read_ios), sizeof(p_stats->n_read_ios), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_put(&(p_stats->reserved), sizeof(p_stats->reserved),
                           p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_stats->n_rows), sizeof(p_stats->n_rows), p_buf, p_buf_end);
    p_buf = buf_put(&(p_stats->n_components), sizeof(p_stats->n_components),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_stats->cost), sizeof(p_stats->cost), p_buf, p_buf_end);

    if ((p_stats->n_components * CLIENT_QUERY_PATH_COMPONENT_LEN) >
        (p_buf_end - p_buf))
        return NULL;

    for (ii = 0; ii < p_stats->n_components; ii++) {
        p_buf = client_query_path_component_put(&(p_stats->path_stats[ii]),
                                                p_buf, p_buf_end);
    }

    return p_buf;
}

typedef struct osql_done_xerr {
    osql_rpl_t hd;
    struct errstat dt;
} osql_done_xerr_t;

enum { OSQLCOMM_DONE_XERR_RPL_LEN = OSQLCOMM_RPL_TYPE_LEN + ERRSTAT_LEN };

BB_COMPILE_TIME_ASSERT(osqlcomm_done_xerr_type_len,
                       sizeof(osql_done_xerr_t) == OSQLCOMM_DONE_XERR_RPL_LEN);

uint8_t *osqlcomm_errstat_type_put(const errstat_t *p_errstat_type,
                                   uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || ERRSTAT_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_errstat_type->errval), sizeof(p_errstat_type->errval),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_errstat_type->errhdrlen),
                    sizeof(p_errstat_type->errhdrlen), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_errstat_type->reserved),
                           sizeof(p_errstat_type->reserved), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_errstat_type->errstr),
                           sizeof(p_errstat_type->errstr), p_buf, p_buf_end);

    return p_buf;
}

/* used in osqlsession */
const uint8_t *osqlcomm_errstat_type_get(errstat_t *p_errstat_type,
                                         const uint8_t *p_buf,
                                         const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || ERRSTAT_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_errstat_type->errval), sizeof(p_errstat_type->errval),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_errstat_type->errhdrlen),
                    sizeof(p_errstat_type->errhdrlen), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_errstat_type->reserved),
                           sizeof(p_errstat_type->reserved), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_errstat_type->errstr),
                           sizeof(p_errstat_type->errstr), p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *
osqlcomm_done_xerr_type_put(const osql_done_xerr_t *p_osql_done_xerr,
                            uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_DONE_XERR_RPL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(&(p_osql_done_xerr->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_errstat_type_put(&(p_osql_done_xerr->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_done_xerr_type_get(osql_done_xerr_t *p_osql_done_xerr,
                            const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_DONE_XERR_RPL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_get(&(p_osql_done_xerr->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_errstat_type_get(&(p_osql_done_xerr->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_done_uuid_xerr {
    osql_uuid_rpl_t hd;
    struct errstat dt;
} osql_done_xerr_uuid_t;

enum {
    OSQLCOMM_DONE_XERR_UUID_RPL_LEN = OSQLCOMM_UUID_RPL_TYPE_LEN + ERRSTAT_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_done_xerr_uuid_type_len,
                       sizeof(osql_done_xerr_uuid_t) ==
                           OSQLCOMM_DONE_XERR_UUID_RPL_LEN);

static uint8_t *osqlcomm_done_xerr_uuid_type_put(
    const osql_done_xerr_uuid_t *p_osql_done_xerr_uuid, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_DONE_XERR_UUID_RPL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_put(&(p_osql_done_xerr_uuid->hd), p_buf,
                                       p_buf_end);
    p_buf = osqlcomm_errstat_type_put(&(p_osql_done_xerr_uuid->dt), p_buf,
                                      p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_done_xerr_uuid_type_get(osql_done_xerr_uuid_t *p_osql_done_xerr_uuid,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_DONE_XERR_UUID_RPL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_get(&(p_osql_done_xerr_uuid->hd), p_buf,
                                       p_buf_end);
    p_buf = osqlcomm_errstat_type_get(&(p_osql_done_xerr_uuid->dt), p_buf,
                                      p_buf_end);

    return p_buf;
}
typedef struct osql_usedb {
    unsigned short tableversion;
    unsigned short tablenamelen;
    char tablename[4]; /* alignment !*/
} osql_usedb_t;

enum {
    OSQLCOMM_USEDB_TBLNAME_OFFSET = 4,
    OSQLCOMM_USEDB_TYPE_LEN = OSQLCOMM_USEDB_TBLNAME_OFFSET + 4
};

BB_COMPILE_TIME_ASSERT(osqlcomm_usedb_type_len,
                       sizeof(osql_usedb_t) == OSQLCOMM_USEDB_TYPE_LEN);

static uint8_t *osqlcomm_usedb_type_put(const osql_usedb_t *p_osql_usedb,
                                        uint8_t *p_buf,
                                        const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_USEDB_TYPE_LEN > p_buf_end - p_buf)
        return NULL;

    p_buf = buf_put(&(p_osql_usedb->tableversion),
                    sizeof(p_osql_usedb->tableversion), p_buf, p_buf_end);
    p_buf = buf_put(&(p_osql_usedb->tablenamelen),
                    sizeof(p_osql_usedb->tablenamelen), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_osql_usedb->tablename),
                           sizeof(p_osql_usedb->tablename), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *osqlcomm_usedb_type_get(osql_usedb_t *p_osql_usedb,
                                              const uint8_t *p_buf,
                                              const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_USEDB_TYPE_LEN > p_buf_end - p_buf)
        return NULL;

    p_buf = buf_get(&(p_osql_usedb->tableversion),
                    sizeof(p_osql_usedb->tableversion), p_buf, p_buf_end);
    p_buf = buf_get(&(p_osql_usedb->tablenamelen),
                    sizeof(p_osql_usedb->tablenamelen), p_buf, p_buf_end);
    /* only retrieve up to the tablename */

    return p_buf;
}

typedef struct osql_usedb_rpl {
    osql_rpl_t hd;
    osql_usedb_t dt;
} osql_usedb_rpl_t;

enum {
    OSQLCOMM_USEDB_RPL_TYPE_LEN =
        OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_USEDB_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_usedb_rpl_type_len,
                       sizeof(osql_usedb_rpl_t) == OSQLCOMM_USEDB_RPL_TYPE_LEN);

static uint8_t *
osqlcomm_usedb_rpl_type_put(const osql_usedb_rpl_t *p_osql_usedb_rpl,
                            uint8_t *p_buf, uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_USEDB_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(&(p_osql_usedb_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_usedb_type_put(&(p_osql_usedb_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_usedb_rpl_type_get(osql_usedb_rpl_t *p_osql_usedb_rpl,
                            const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_USEDB_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_get(&(p_osql_usedb_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_usedb_type_get(&(p_osql_usedb_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_usedb_rpl_uuid {
    osql_uuid_rpl_t hd;
    osql_usedb_t dt;
} osql_usedb_rpl_uuid_t;

enum {
    OSQLCOMM_USEDB_RPL_UUID_TYPE_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_USEDB_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_usedb_rpl_uuid_type_len,
                       sizeof(osql_usedb_rpl_uuid_t) ==
                           OSQLCOMM_USEDB_RPL_UUID_TYPE_LEN);

static uint8_t *osqlcomm_usedb_uuid_rpl_type_put(
    const osql_usedb_rpl_uuid_t *p_osql_usedb_uuid_rpl, uint8_t *p_buf,
    uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_USEDB_RPL_UUID_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_put(&(p_osql_usedb_uuid_rpl->hd), p_buf,
                                       p_buf_end);
    p_buf =
        osqlcomm_usedb_type_put(&(p_osql_usedb_uuid_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_usedb_rpl_uuid_type_get(osql_usedb_rpl_uuid_t *p_osql_usedb_uuid_rpl,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_USEDB_RPL_UUID_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_get(&(p_osql_usedb_uuid_rpl->hd), p_buf,
                                       p_buf_end);
    p_buf =
        osqlcomm_usedb_type_get(&(p_osql_usedb_uuid_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_index {
    unsigned long long seq;
    int ixnum;
    int nData;
} osql_index_t;

enum { OSQLCOMM_INDEX_TYPE_LEN = 8 + 4 + 4 };

BB_COMPILE_TIME_ASSERT(osqlcomm_index_type_len,
                       sizeof(osql_index_t) == OSQLCOMM_INDEX_TYPE_LEN);

static uint8_t *osqlcomm_index_type_put(const osql_index_t *p_osql_index,
                                        uint8_t *p_buf,
                                        const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_INDEX_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_put(&(p_osql_index->seq), sizeof(p_osql_index->seq),
                           p_buf, p_buf_end);
    p_buf = buf_put(&(p_osql_index->ixnum), sizeof(p_osql_index->ixnum), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_osql_index->nData), sizeof(p_osql_index->nData), p_buf,
                    p_buf_end);

    return p_buf;
}

static const uint8_t *osqlcomm_index_type_get(osql_index_t *p_osql_index,
                                              const uint8_t *p_buf,
                                              const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_INDEX_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_get(&(p_osql_index->seq), sizeof(p_osql_index->seq),
                           p_buf, p_buf_end);
    p_buf = buf_get(&(p_osql_index->ixnum), sizeof(p_osql_index->ixnum), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_osql_index->nData), sizeof(p_osql_index->nData), p_buf,
                    p_buf_end);

    return p_buf;
}

typedef struct osql_index_rpl {
    osql_rpl_t hd;
    osql_index_t dt;
} osql_index_rpl_t;

enum {
    OSQLCOMM_INDEX_RPL_TYPE_LEN =
        OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_INDEX_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_index_rpl_type_len,
                       sizeof(osql_index_rpl_t) == OSQLCOMM_INDEX_RPL_TYPE_LEN);

static uint8_t *
osqlcomm_index_rpl_type_put(const osql_index_rpl_t *p_osql_index_rpl,
                            uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_INDEX_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(&(p_osql_index_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_index_type_put(&(p_osql_index_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_index_rpl_type_get(osql_index_rpl_t *p_osql_index_rpl,
                            const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_INDEX_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_get(&(p_osql_index_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_index_type_get(&(p_osql_index_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_index_uuid_rpl {
    osql_uuid_rpl_t hd;
    osql_index_t dt;
} osql_index_uuid_rpl_t;

enum {
    OSQLCOMM_INDEX_UUID_RPL_TYPE_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_INDEX_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_index_rpl_uuid_type_len,
                       sizeof(osql_index_uuid_rpl_t) ==
                           OSQLCOMM_INDEX_UUID_RPL_TYPE_LEN);

static uint8_t *osqlcomm_index_uuid_rpl_type_put(
    const osql_index_uuid_rpl_t *p_osql_index_uuid_rpl, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_INDEX_UUID_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_put(&(p_osql_index_uuid_rpl->hd), p_buf,
                                       p_buf_end);
    p_buf =
        osqlcomm_index_type_put(&(p_osql_index_uuid_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_index_uuid_rpl_type_get(osql_index_uuid_rpl_t *p_osql_index_uuid_rpl,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_INDEX_UUID_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_get(&(p_osql_index_uuid_rpl->hd), p_buf,
                                       p_buf_end);
    p_buf =
        osqlcomm_index_type_get(&(p_osql_index_uuid_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_ins {
    unsigned long long seq;
    int flags; /* feature flags */
    int upsert_flags;
    unsigned long long dk; /* flag to indicate which keys to modify */
    int nData;
    char pData[4]; /* alignment! - pass some useful data instead of padding */
} osql_ins_t;

enum { OSQLCOMM_INS_TYPE_LEN = 8 + 4 + 4 + 8 + 4 + 4 };
enum { OSQLCOMM_INS_LEGACY_TYPE_LEN = 8 + 4 + 4 }; /* seq + nData + pData */

BB_COMPILE_TIME_ASSERT(osqlcomm_ins_type_len,
                       sizeof(osql_ins_t) == OSQLCOMM_INS_TYPE_LEN);

/* Flags that attach with OSQL_INSERT. */
enum osql_insert_flags {
    OSQL_INSERT_UPSERT = 1 << 0,
    OSQL_INSERT_SEND_DK = 1 << 1,
};

static uint8_t *osqlcomm_ins_type_put(const osql_ins_t *p_osql_ins,
                                      uint8_t *p_buf, const uint8_t *p_buf_end,
                                      int is_legacy)
{
    int expected_len;
    int flags = p_osql_ins->flags;
    if (!is_legacy) {
        expected_len = OSQLCOMM_INS_TYPE_LEN;
        if ((flags & OSQL_INSERT_UPSERT) == 0)
            expected_len -= sizeof(int);
        if ((flags & OSQL_INSERT_SEND_DK) == 0)
            expected_len -= sizeof(unsigned long long);
    } else {
        expected_len = OSQLCOMM_INS_LEGACY_TYPE_LEN;
    }

    if (p_buf_end < p_buf || expected_len > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_put(&(p_osql_ins->seq), sizeof(p_osql_ins->seq), p_buf,
                           p_buf_end);
    if (!is_legacy)
        p_buf = buf_no_net_put(&(p_osql_ins->flags), sizeof(p_osql_ins->flags),
                               p_buf, p_buf_end);
    if (!is_legacy && (flags & OSQL_INSERT_UPSERT))
        p_buf =
            buf_no_net_put(&(p_osql_ins->upsert_flags),
                           sizeof(p_osql_ins->upsert_flags), p_buf, p_buf_end);
    if (!is_legacy && (flags & OSQL_INSERT_SEND_DK))
        p_buf = buf_no_net_put(&(p_osql_ins->dk), sizeof(p_osql_ins->dk), p_buf,
                               p_buf_end);
    p_buf = buf_put(&(p_osql_ins->nData), sizeof(p_osql_ins->nData), p_buf,
                    p_buf_end);
    /* leave p_buf pointing at pData */

    return p_buf;
}

static const uint8_t *osqlcomm_ins_type_get(osql_ins_t *p_osql_ins,
                                            const uint8_t *p_buf,
                                            const uint8_t *p_buf_end,
                                            int is_legacy)
{
    if (p_buf_end < p_buf || OSQLCOMM_INS_LEGACY_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_get(&(p_osql_ins->seq), sizeof(p_osql_ins->seq), p_buf,
                           p_buf_end);
    p_osql_ins->dk = -1ULL;
    p_osql_ins->upsert_flags = 0;
    if (!is_legacy) {
        int expected_len = sizeof(p_osql_ins->nData);
        p_buf = buf_no_net_get(&(p_osql_ins->flags), sizeof(p_osql_ins->flags),
                               p_buf, p_buf_end);
        if (p_osql_ins->flags & OSQL_INSERT_UPSERT)
            expected_len += sizeof(p_osql_ins->upsert_flags);
        if (p_osql_ins->flags & OSQL_INSERT_SEND_DK)
            expected_len += sizeof(p_osql_ins->dk);
        if (expected_len > (p_buf_end - p_buf))
            return NULL;
        if (p_osql_ins->flags & OSQL_INSERT_UPSERT)
            p_buf = buf_no_net_get(&(p_osql_ins->upsert_flags),
                                   sizeof(p_osql_ins->upsert_flags), p_buf,
                                   p_buf_end);
        if (p_osql_ins->flags & OSQL_INSERT_SEND_DK)
            p_buf = buf_no_net_get(&(p_osql_ins->dk), sizeof(p_osql_ins->dk),
                                   p_buf, p_buf_end);
    }
    p_buf = buf_get(&(p_osql_ins->nData), sizeof(p_osql_ins->nData), p_buf,
                    p_buf_end);
    /* leave p_buf pointing at pData */

    return p_buf;
}

typedef struct osql_ins_rpl {
    osql_rpl_t hd;
    osql_ins_t dt;
} osql_ins_rpl_t;

enum {
    OSQLCOMM_INS_RPL_TYPE_LEN =
        OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_INS_LEGACY_TYPE_LEN
};

static uint8_t *osqlcomm_ins_rpl_type_put(const osql_ins_rpl_t *p_osql_ins_rpl,
                                          uint8_t *p_buf,
                                          const uint8_t *p_buf_end)
{
    p_buf = osqlcomm_rpl_type_put(&(p_osql_ins_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_ins_type_put(&(p_osql_ins_rpl->dt), p_buf, p_buf_end, 1);

    return p_buf;
}

typedef struct osql_ins_uuid_rpl {
    osql_uuid_rpl_t hd;
    osql_ins_t dt;
} osql_ins_uuid_rpl_t;

enum {
    OSQLCOMM_INS_UUID_RPL_TYPE_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_INS_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_ins_rpl_uuid_type_len,
                       sizeof(osql_ins_uuid_rpl_t) ==
                           OSQLCOMM_INS_UUID_RPL_TYPE_LEN);

static uint8_t *
osqlcomm_ins_uuid_rpl_type_put(const osql_ins_uuid_rpl_t *p_osql_ins_uuid_rpl,
                               uint8_t *p_buf, const uint8_t *p_buf_end)
{
    p_buf = osqlcomm_uuid_rpl_type_put(&(p_osql_ins_uuid_rpl->hd), p_buf,
                                       p_buf_end);
    p_buf =
        osqlcomm_ins_type_put(&(p_osql_ins_uuid_rpl->dt), p_buf, p_buf_end, 0);
    return p_buf;
}

typedef struct osql_updstat {
    unsigned long long seq;
    int padding1;
    int nStat; /* 1 => stat1 or 2 => stat2 */
    int nData;
    char pData[4];
} osql_updstat_t;

enum { OSQLCOMM_UPDSTAT_TYPE_LEN = 8 + 4 + 4 + 4 + 4 };

BB_COMPILE_TIME_ASSERT(osqlcomm_updstat_len,
                       sizeof(osql_updstat_t) == OSQLCOMM_UPDSTAT_TYPE_LEN);

static const uint8_t *osqlcomm_updstat_type_get(osql_updstat_t *p_osql_updstat,
                                                const uint8_t *p_buf,
                                                const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_UPDSTAT_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_get(&(p_osql_updstat->seq), sizeof(p_osql_updstat->seq),
                           p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_osql_updstat->padding1),
                           sizeof(p_osql_updstat->padding1), p_buf, p_buf_end);
    p_buf = buf_get(&(p_osql_updstat->nStat), sizeof(p_osql_updstat->nStat),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_osql_updstat->nData), sizeof(p_osql_updstat->nData),
                    p_buf, p_buf_end);

    /* leave p_buf pointing at pData */
    return p_buf;
}

static uint8_t *osqlcomm_updstat_type_put(const osql_updstat_t *p_osql_updstat,
                                          uint8_t *p_buf,
                                          const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_UPDSTAT_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_put(&(p_osql_updstat->seq), sizeof(p_osql_updstat->seq),
                           p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_osql_updstat->padding1),
                           sizeof(p_osql_updstat->padding1), p_buf, p_buf_end);
    p_buf = buf_put(&(p_osql_updstat->nStat), sizeof(p_osql_updstat->nStat),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_osql_updstat->nData), sizeof(p_osql_updstat->nData),
                    p_buf, p_buf_end);

    /* leave p_buf pointing at pData */
    return p_buf;
}

typedef struct osql_updstat_rpl {
    osql_rpl_t hd;
    osql_updstat_t dt;
} osql_updstat_rpl_t;

enum {
    OSQLCOMM_UPDSTAT_RPL_TYPE_LEN =
        OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_UPDSTAT_TYPE_LEN
};

static uint8_t *
osqlcomm_updstat_rpl_type_put(const osql_updstat_rpl_t *p_osql_updstat_rpl,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_UPDSTAT_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(&(p_osql_updstat_rpl->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_updstat_type_put(&(p_osql_updstat_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_updstat_rpl_type_get(osql_updstat_rpl_t *p_osql_updstat_rpl,
                              const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_UPDSTAT_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_get(&(p_osql_updstat_rpl->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_updstat_type_get(&(p_osql_updstat_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_updstat_uuid_rpl {
    osql_uuid_rpl_t hd;
    osql_updstat_t dt;
} osql_updstat_uuid_rpl_t;

enum {
    OSQLCOMM_UPDSTAT_UUID_RPL_TYPE_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_UPDSTAT_TYPE_LEN
};

static uint8_t *osqlcomm_updstat_uuid_rpl_type_put(
    const osql_updstat_uuid_rpl_t *p_osql_updstat_rpl, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_UPDSTAT_UUID_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        osqlcomm_uuid_rpl_type_put(&(p_osql_updstat_rpl->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_updstat_type_put(&(p_osql_updstat_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_updstat_uuid_rpl_type_get(osql_updstat_uuid_rpl_t *p_osql_updstat_rpl,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_UPDSTAT_UUID_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        osqlcomm_uuid_rpl_type_get(&(p_osql_updstat_rpl->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_updstat_type_get(&(p_osql_updstat_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_upd {
    unsigned long long genid;
    unsigned long long ins_keys;
    unsigned long long del_keys;
    int nData;
    char pData[4]; /* alignment! - pass some useful data instead of padding */
} osql_upd_t;

enum { OSQLCOMM_UPD_TYPE_LEN = 8 + 8 + 8 + 4 + 4 };

BB_COMPILE_TIME_ASSERT(osqlcomm_upd_type_len,
                       sizeof(osql_upd_t) == OSQLCOMM_UPD_TYPE_LEN);

static uint8_t *osqlcomm_upd_type_put(const osql_upd_t *p_osql_upd,
                                      uint8_t *p_buf, const uint8_t *p_buf_end,
                                      int send_dk)
{
    if (p_buf_end < p_buf ||
        (send_dk ? OSQLCOMM_UPD_TYPE_LEN
                 : OSQLCOMM_UPD_TYPE_LEN - sizeof(unsigned long long) -
                       sizeof(unsigned long long)) > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_put(&(p_osql_upd->genid), sizeof(p_osql_upd->genid),
                           p_buf, p_buf_end);
    if (send_dk) {
        p_buf = buf_no_net_put(&(p_osql_upd->ins_keys),
                               sizeof(p_osql_upd->ins_keys), p_buf, p_buf_end);
        p_buf = buf_no_net_put(&(p_osql_upd->del_keys),
                               sizeof(p_osql_upd->del_keys), p_buf, p_buf_end);
    }
    p_buf = buf_put(&(p_osql_upd->nData), sizeof(p_osql_upd->nData), p_buf,
                    p_buf_end);
    /* don't copy any of the pData */

    return p_buf;
}

static const uint8_t *osqlcomm_upd_type_get(osql_upd_t *p_osql_upd,
                                            const uint8_t *p_buf,
                                            const uint8_t *p_buf_end,
                                            int recv_dk)
{
    if (p_buf_end < p_buf ||
        (recv_dk ? OSQLCOMM_UPD_TYPE_LEN
                 : OSQLCOMM_UPD_TYPE_LEN - sizeof(unsigned long long) -
                       sizeof(unsigned long long)) > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_get(&(p_osql_upd->genid), sizeof(p_osql_upd->genid),
                           p_buf, p_buf_end);
    if (recv_dk) {
        p_buf = buf_no_net_get(&(p_osql_upd->ins_keys),
                               sizeof(p_osql_upd->ins_keys), p_buf, p_buf_end);
        p_buf = buf_no_net_get(&(p_osql_upd->del_keys),
                               sizeof(p_osql_upd->del_keys), p_buf, p_buf_end);
    }
    p_buf = buf_get(&(p_osql_upd->nData), sizeof(p_osql_upd->nData), p_buf,
                    p_buf_end);
    /* don't copy any of the pData */

    return p_buf;
}

typedef struct osql_upd_rpl {
    osql_rpl_t hd;
    osql_upd_t dt;
} osql_upd_rpl_t;

enum {
    OSQLCOMM_UPD_RPL_TYPE_LEN = OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_UPD_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_upd_rpl_type_len,
                       sizeof(osql_upd_rpl_t) == OSQLCOMM_UPD_RPL_TYPE_LEN);

static uint8_t *osqlcomm_upd_rpl_type_put(const osql_upd_rpl_t *p_osql_upd_rpl,
                                          uint8_t *p_buf,
                                          const uint8_t *p_buf_end, int send_dk)
{
    if (p_buf_end < p_buf ||
        (send_dk ? OSQLCOMM_UPD_RPL_TYPE_LEN
                 : OSQLCOMM_UPD_RPL_TYPE_LEN - sizeof(unsigned long long) -
                       sizeof(unsigned long long)) > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(&(p_osql_upd_rpl->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_upd_type_put(&(p_osql_upd_rpl->dt), p_buf, p_buf_end, send_dk);

    return p_buf;
}

static const uint8_t *osqlcomm_upd_rpl_type_get(osql_upd_rpl_t *p_osql_upd_rpl,
                                                const uint8_t *p_buf,
                                                const uint8_t *p_buf_end,
                                                int recv_dk)
{
    if (p_buf_end < p_buf ||
        (recv_dk ? OSQLCOMM_UPD_RPL_TYPE_LEN
                 : OSQLCOMM_UPD_RPL_TYPE_LEN - sizeof(unsigned long long) -
                       sizeof(unsigned long long)) > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_get(&(p_osql_upd_rpl->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_upd_type_get(&(p_osql_upd_rpl->dt), p_buf, p_buf_end, recv_dk);

    return p_buf;
}

typedef struct osql_upd_uuid_rpl {
    osql_uuid_rpl_t hd;
    osql_upd_t dt;
} osql_upd_uuid_rpl_t;

enum {
    OSQLCOMM_UPD_UUID_RPL_TYPE_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_UPD_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_upd_uuid_rpl_type_len,
                       sizeof(osql_upd_uuid_rpl_t) ==
                           OSQLCOMM_UPD_UUID_RPL_TYPE_LEN);

static uint8_t *
osqlcomm_upd_uuid_rpl_type_put(const osql_upd_uuid_rpl_t *p_osql_upd_rpl,
                               uint8_t *p_buf, const uint8_t *p_buf_end,
                               int send_dk)
{
    if (p_buf_end < p_buf ||
        (send_dk ? OSQLCOMM_UPD_UUID_RPL_TYPE_LEN
                 : OSQLCOMM_UPD_UUID_RPL_TYPE_LEN - sizeof(unsigned long long) -
                       sizeof(unsigned long long)) > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_put(&(p_osql_upd_rpl->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_upd_type_put(&(p_osql_upd_rpl->dt), p_buf, p_buf_end, send_dk);

    return p_buf;
}

static const uint8_t *
osqlcomm_upd_uuid_rpl_type_get(osql_upd_uuid_rpl_t *p_osql_upd_rpl,
                               const uint8_t *p_buf, const uint8_t *p_buf_end,
                               int recv_dk)
{
    if (p_buf_end < p_buf ||
        (recv_dk ? OSQLCOMM_UPD_UUID_RPL_TYPE_LEN
                 : OSQLCOMM_UPD_UUID_RPL_TYPE_LEN - sizeof(unsigned long long) -
                       sizeof(unsigned long long)) > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_get(&(p_osql_upd_rpl->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_upd_type_get(&(p_osql_upd_rpl->dt), p_buf, p_buf_end, recv_dk);

    return p_buf;
}

typedef struct osql_clrtbl_rpl {
    osql_rpl_t
        hd; /* there is no reply specific data, usedb contains the table name */
} osql_clrtbl_rpl_t;

typedef struct osql_qblob {
    unsigned long long seq;
    int id;
    int bloblen;
    char blob[8];
} osql_qblob_t;

enum { OSQLCOMM_QBLOB_TYPE_LEN = 8 + 4 + 4 + 8 };

BB_COMPILE_TIME_ASSERT(osqlcomm_qblob_type_len,
                       sizeof(osql_qblob_t) == OSQLCOMM_QBLOB_TYPE_LEN);

static uint8_t *osqlcomm_qblob_type_put(const osql_qblob_t *p_qblob,
                                        uint8_t *p_buf,
                                        const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_QBLOB_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_qblob->seq), sizeof(p_qblob->seq), p_buf, p_buf_end);
    p_buf = buf_put(&(p_qblob->id), sizeof(p_qblob->id), p_buf, p_buf_end);
    p_buf = buf_put(&(p_qblob->bloblen), sizeof(p_qblob->bloblen), p_buf,
                    p_buf_end);
    /* this only writes up to the blob */

    return p_buf;
}

static const uint8_t *osqlcomm_qblob_type_get(osql_qblob_t *p_qblob,
                                              const uint8_t *p_buf,
                                              const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_QBLOB_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_qblob->seq), sizeof(p_qblob->seq), p_buf, p_buf_end);
    p_buf = buf_get(&(p_qblob->id), sizeof(p_qblob->id), p_buf, p_buf_end);
    p_buf = buf_get(&(p_qblob->bloblen), sizeof(p_qblob->bloblen), p_buf,
                    p_buf_end);
    /* this only reads up to the blob */

    return p_buf;
}

typedef struct osql_qblob_rpl {
    osql_rpl_t hd;
    osql_qblob_t dt;
} osql_qblob_rpl_t;

enum {
    OSQLCOMM_QBLOB_RPL_TYPE_LEN =
        OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_QBLOB_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_qblob_rpl_type_len,
                       sizeof(osql_qblob_rpl_t) == OSQLCOMM_QBLOB_RPL_TYPE_LEN);

static uint8_t *osqlcomm_qblob_rpl_type_put(const osql_qblob_rpl_t *p_qblob,
                                            uint8_t *p_buf,
                                            const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_QBLOB_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(&(p_qblob->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_qblob_type_put(&(p_qblob->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *osqlcomm_qblob_rpl_type_get(osql_qblob_rpl_t *p_qblob,
                                                  const uint8_t *p_buf,
                                                  const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_QBLOB_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_get(&(p_qblob->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_qblob_type_get(&(p_qblob->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_qblob_uuid_rpl {
    osql_uuid_rpl_t hd;
    osql_qblob_t dt;
} osql_qblob_uuid_rpl_t;

enum {
    OSQLCOMM_QBLOB_UUID_RPL_TYPE_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_QBLOB_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_qblob_uuid_rpl_type_len,
                       sizeof(osql_qblob_uuid_rpl_t) ==
                           OSQLCOMM_QBLOB_UUID_RPL_TYPE_LEN);

static uint8_t *
osqlcomm_qblob_uuid_rpl_type_put(const osql_qblob_uuid_rpl_t *p_qblob,
                                 uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_QBLOB_UUID_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_put(&(p_qblob->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_qblob_type_put(&(p_qblob->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_qblob_uuid_rpl_type_get(osql_qblob_uuid_rpl_t *p_qblob,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_QBLOB_UUID_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_get(&(p_qblob->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_qblob_type_get(&(p_qblob->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_updcols {
    unsigned long long seq;
    int ncols;
    int padding;
    int clist[2];
} osql_updcols_t;

enum { OSQLCOMM_UPDCOLS_TYPE_LEN = 8 + 4 + 4 + 8 };

BB_COMPILE_TIME_ASSERT(osqlcomm_updcols_type_len,
                       sizeof(osql_updcols_t) == OSQLCOMM_UPDCOLS_TYPE_LEN);

static uint8_t *osqlcomm_updcols_type_put(const osql_updcols_t *p_updcols,
                                          uint8_t *p_buf,
                                          const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_UPDCOLS_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_put(&(p_updcols->seq), sizeof(p_updcols->seq), p_buf, p_buf_end);
    p_buf = buf_put(&(p_updcols->ncols), sizeof(p_updcols->ncols), p_buf,
                    p_buf_end);
    /*  don't pack the column list- it's done manually */

    return p_buf;
}

static const uint8_t *osqlcomm_updcols_type_get(osql_updcols_t *p_updcols,
                                                const uint8_t *p_buf,
                                                const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_UPDCOLS_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_get(&(p_updcols->seq), sizeof(p_updcols->seq), p_buf, p_buf_end);
    p_buf = buf_get(&(p_updcols->ncols), sizeof(p_updcols->ncols), p_buf,
                    p_buf_end);
    /*  don't pack the column list- it's done manually later */

    return p_buf;
}

typedef struct osql_updcols_rpl {
    osql_rpl_t hd;
    osql_updcols_t dt;
} osql_updcols_rpl_t;

enum {
    OSQLCOMM_UPDCOLS_RPL_TYPE_LEN =
        OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_UPDCOLS_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_updcols_rpl_type_len,
                       sizeof(osql_updcols_rpl_t) ==
                           OSQLCOMM_UPDCOLS_RPL_TYPE_LEN);

static uint8_t *
osqlcomm_updcols_rpl_type_put(const osql_updcols_rpl_t *p_updcols_rpl,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_UPDCOLS_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(&(p_updcols_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_updcols_type_put(&(p_updcols_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_updcols_rpl_type_get(osql_updcols_rpl_t *p_updcols_rpl,
                              const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_UPDCOLS_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_get(&(p_updcols_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_updcols_type_get(&(p_updcols_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_updcols_uuid_rpl {
    osql_uuid_rpl_t hd;
    osql_updcols_t dt;
} osql_updcols_uuid_rpl_t;

enum {
    OSQLCOMM_UPDCOLS_UUID_RPL_TYPE_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_UPDCOLS_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_updcols_uuid_rpl_type_len,
                       sizeof(osql_updcols_uuid_rpl_t) ==
                           OSQLCOMM_UPDCOLS_UUID_RPL_TYPE_LEN);

static uint8_t *
osqlcomm_updcols_uuid_rpl_type_put(const osql_updcols_uuid_rpl_t *p_updcols_rpl,
                                   uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_UPDCOLS_UUID_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_put(&(p_updcols_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_updcols_type_put(&(p_updcols_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_updcols_uuid_rpl_type_get(osql_updcols_uuid_rpl_t *p_updcols_rpl,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_UPDCOLS_UUID_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_get(&(p_updcols_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_updcols_type_get(&(p_updcols_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_exists {
    int status;
    int timestamp;
} osql_exists_t;

enum { OSQLCOMM_EXISTS_TYPE_LEN = 4 + 4 };

BB_COMPILE_TIME_ASSERT(osqlcomm_exists_type_len,
                       sizeof(osql_exists_t) == OSQLCOMM_EXISTS_TYPE_LEN);

static uint8_t *osqlcomm_exists_type_put(const osql_exists_t *p_exists,
                                         uint8_t *p_buf,
                                         const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_EXISTS_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_exists->status), sizeof(p_exists->status), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_exists->timestamp), sizeof(p_exists->timestamp), p_buf,
                    p_buf_end);

    return p_buf;
}

static const uint8_t *osqlcomm_exists_type_get(osql_exists_t *p_exists,
                                               const uint8_t *p_buf,
                                               const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_EXISTS_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_exists->status), sizeof(p_exists->status), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_exists->timestamp), sizeof(p_exists->timestamp), p_buf,
                    p_buf_end);

    return p_buf;
}

typedef struct osql_exists_rpl {
    osql_rpl_t hd;
    osql_exists_t dt;
} osql_exists_rpl_t;

enum {
    OSQLCOMM_EXISTS_RPL_TYPE_LEN =
        OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_EXISTS_TYPE_LEN
};

typedef struct osql_exists_uuid_rpl {
    osql_uuid_rpl_t hd;
    osql_exists_t dt;
} osql_exists_uuid_rpl_t;

enum {
    OSQLCOMM_EXISTS_UUID_RPL_TYPE_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_EXISTS_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_exists_rpl_type_len,
                       sizeof(osql_exists_rpl_t) ==
                           OSQLCOMM_EXISTS_RPL_TYPE_LEN);

static uint8_t *
osqlcomm_exists_rpl_type_put(const osql_exists_rpl_t *p_exists_rpl,
                             uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_EXISTS_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(&(p_exists_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_exists_type_put(&(p_exists_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_exists_rpl_type_get(osql_exists_rpl_t *p_exists_rpl,
                             const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_EXISTS_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_get(&(p_exists_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_exists_type_get(&(p_exists_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *
osqlcomm_exists_uuid_rpl_type_put(const osql_exists_uuid_rpl_t *p_exists_rpl,
                                  uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_EXISTS_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_put(&(p_exists_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_exists_type_put(&(p_exists_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_exists_uuid_rpl_type_get(osql_exists_uuid_rpl_t *p_exists_rpl,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_EXISTS_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_get(&(p_exists_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_exists_type_get(&(p_exists_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

/* net messages stats */
typedef struct osql_stats {
    long int snd;
    long int snd_failed;
    long int rcv;
    long int rcv_failed;
    long int rcv_rdndt;
} osql_stats_t;

typedef struct osql_dbglog {
    int opcode;
    int padding;
    unsigned long long rqid;
    unsigned long long dbglog_cookie;
    int queryid;
    int padding2;
} osql_dbglog_t;

enum { OSQLCOMM_DBGLOG_TYPE_LEN = 4 + 4 + 8 + 8 + 4 + 4 };

BB_COMPILE_TIME_ASSERT(osql_dbglog_type_len,
                       sizeof(osql_dbglog_t) == OSQLCOMM_DBGLOG_TYPE_LEN);

static uint8_t *osqlcomm_dbglog_type_put(const osql_dbglog_t *p_dbglog,
                                         uint8_t *p_buf,
                                         const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_DBGLOG_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_dbglog->opcode), sizeof(p_dbglog->opcode), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_put(&(p_dbglog->padding), sizeof(p_dbglog->padding),
                           p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_dbglog->rqid), sizeof(p_dbglog->rqid), p_buf, p_buf_end);
    p_buf = buf_put(&(p_dbglog->dbglog_cookie), sizeof(p_dbglog->dbglog_cookie),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_dbglog->queryid), sizeof(p_dbglog->queryid), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_put(&(p_dbglog->padding2), sizeof(p_dbglog->padding2),
                           p_buf, p_buf_end);
    return p_buf;
}

static const uint8_t *osqlcomm_dbglog_type_get(osql_dbglog_t *p_dbglog,
                                               const uint8_t *p_buf,
                                               const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_DBGLOG_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_dbglog->opcode), sizeof(p_dbglog->opcode), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_get(&(p_dbglog->padding), sizeof(p_dbglog->padding),
                           p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_dbglog->rqid), sizeof(p_dbglog->rqid), p_buf, p_buf_end);
    p_buf = buf_get(&(p_dbglog->dbglog_cookie), sizeof(p_dbglog->dbglog_cookie),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_dbglog->queryid), sizeof(p_dbglog->queryid), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_get(&(p_dbglog->padding2), sizeof(p_dbglog->padding2),
                           p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_recgenid {
    unsigned long long genid;
} osql_recgenid_t;

enum { OSQLCOMM_RECGENID_TYPE_LEN = 8 };

BB_COMPILE_TIME_ASSERT(osqlcomm_recgenid_type_len,
                       sizeof(osql_recgenid_t) == OSQLCOMM_RECGENID_TYPE_LEN);

static uint8_t *osqlcomm_recgenid_type_put(const osql_recgenid_t *p_recgenid,
                                           uint8_t *p_buf,
                                           const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_RECGENID_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_put(&(p_recgenid->genid), sizeof(p_recgenid->genid),
                           p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *osqlcomm_recgenid_type_get(osql_recgenid_t *p_recgenid,
                                                 const uint8_t *p_buf,
                                                 const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_RECGENID_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_get(&(p_recgenid->genid), sizeof(p_recgenid->genid),
                           p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_recgenid_rpl {
    osql_rpl_t hd;
    osql_recgenid_t dt;
} osql_recgenid_rpl_t;

enum {
    OSQLCOMM_RECGENID_RPL_TYPE_LEN =
        OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_RECGENID_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_recgenid_rpl_type_len,
                       sizeof(osql_recgenid_rpl_t) ==
                           OSQLCOMM_RECGENID_RPL_TYPE_LEN);

static uint8_t *
osqlcomm_recgenid_rpl_type_put(const osql_recgenid_rpl_t *p_recgenid,
                               uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_RECGENID_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(&(p_recgenid->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_recgenid_type_put(&(p_recgenid->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_recgenid_rpl_type_get(osql_recgenid_rpl_t *p_recgenid,
                               const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_RECGENID_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_get(&(p_recgenid->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_recgenid_type_get(&(p_recgenid->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_recgenid_uuid_rpl {
    osql_uuid_rpl_t hd;
    osql_recgenid_t dt;
} osql_recgenid_uuid_rpl_t;

enum {
    OSQLCOMM_RECGENID_UUID_RPL_TYPE_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_RECGENID_TYPE_LEN
};

static uint8_t *
osqlcomm_recgenid_uuid_rpl_type_put(const osql_recgenid_uuid_rpl_t *p_recgenid,
                                    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_RECGENID_UUID_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_put(&(p_recgenid->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_recgenid_type_put(&(p_recgenid->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_recgenid_uuid_rpl_type_get(osql_recgenid_uuid_rpl_t *p_recgenid,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_RECGENID_UUID_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_get(&(p_recgenid->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_recgenid_type_get(&(p_recgenid->dt), p_buf, p_buf_end);

    return p_buf;
}

static inline int osql_nettype_is_uuid(int type)
{
    return type >= NET_OSQL_UUID_REQUEST_MIN &&
           type < NET_OSQL_UUID_REQUEST_MAX;
}

static osql_stats_t stats[OSQL_MAX_REQ] = {{0}};

/* echo service */
#define MAX_ECHOES 256
#define MAX_LATENCY 1000
osql_echo_t msgs[MAX_ECHOES];
pthread_mutex_t msgs_mtx = PTHREAD_MUTEX_INITIALIZER;

static osql_comm_t *thecomm_obj = NULL;

static osql_comm_t *get_thecomm(void)
{
    return thecomm_obj;
}

static void net_osql_rpl(void *hndl, void *uptr, char *fromnode, int usertype,
                         void *dtap, int dtalen, uint8_t is_tcp);
static int net_osql_rpl_tail(void *hndl, void *uptr, char *fromnode,
                             int usertype, void *dtap, int dtalen, void *tail,
                             int tailen);

static void net_sosql_req(void *hndl, void *uptr, char *fromnode, int usertype,
                          void *dtap, int dtalen, uint8_t is_tcp);
static void net_recom_req(void *hndl, void *uptr, char *fromnode, int usertype,
                          void *dtap, int dtalen, uint8_t is_tcp);
static void net_snapisol_req(void *hndl, void *uptr, char *fromnode,
                             int usertype, void *dtap, int dtalen,
                             uint8_t is_tcp);
static void net_serial_req(void *hndl, void *uptr, char *fromnode, int usertype,
                           void *dtap, int dtalen, uint8_t is_tcp);

static int net_osql_nodedwn(netinfo_type *netinfo_ptr, char *node);
static void net_osql_master_check(void *hndl, void *uptr, char *fromnode,
                                  int usertype, void *dtap, int dtalen,
                                  uint8_t is_tcp);
static void net_osql_master_checked(void *hndl, void *uptr, char *fromnode,
                                    int usertype, void *dtap, int dtalen,
                                    uint8_t is_tcp);
static void net_sorese_signal(void *hndl, void *uptr, char *fromnode,
                              int usertype, void *dtap, int dtalen,
                              uint8_t is_tcp);

static void net_startthread_rtn(void *arg);
static void net_stopthread_rtn(void *arg);

static void signal_rtoff(void);

static int check_master(const char *tohost);
static int offload_net_send(const char *tohost, int usertype, void *data,
                                 int datalen, int nodelay, void *tail,
                                 int tailen);
static int sorese_rcvreq(char *fromhost, void *dtap, int dtalen, int type,
                         int nettype);
static int netrpl2req(int netrpltype);

static void net_osql_rcv_echo_ping(void *hndl, void *uptr, char *fromnode,
                                   int usertype, void *dtap, int dtalen,
                                   uint8_t is_tcp);

static void net_osql_rcv_echo_pong(void *hndl, void *uptr, char *fromnode,
                                   int usertype, void *dtap, int dtalen,
                                   uint8_t is_tcp);
static void net_block_req(void *hndl, void *uptr, char *fromhost, int usertype,
                          void *dtap, int dtalen, uint8_t is_tcp);
static void net_block_reply(void *hndl, void *uptr, char *fromhost,
                            int usertype, void *dtap, int dtalen,
                            uint8_t is_tcp);

static void net_snap_uid_req(void *hndl, void *uptr, char *fromhost,
                             int usertype, void *dtap, int dtalen,
                             uint8_t is_tcp);
static void net_snap_uid_rpl(void *hndl, void *uptr, char *fromhost,
                             int usertype, void *dtap, int dtalen,
                             uint8_t is_tcp);

#include <net/net.h>
/**
 * Initializes this node for osql communication
 * Creates the offload net.
 * Returns 0 if success.
 *
 */
int osql_comm_init(struct dbenv *dbenv)
{
    osql_comm_t *tmp = NULL;
    int ii = 0;
    void *rcv = NULL;
    int rc = 0;

    /* allocate comm */
    tmp = (osql_comm_t *)calloc(sizeof(osql_comm_t), 1);
    if (!tmp) {
        logmsg(LOGMSG_ERROR, "%s: unable to allocate %zu bytes\n", __func__,
               sizeof(osql_comm_t));
        return 0;
    }

    /* start our net infrastructure */
    tmp->handle_sibling = dbenv->handle_sibling_offload;

    net_register_allow(tmp->handle_sibling, net_allow_node);

    if (gbl_osql_max_queue)
        net_set_max_queue(tmp->handle_sibling, gbl_osql_max_queue);

    if (gbl_osql_net_poll)
        net_set_poll(tmp->handle_sibling, gbl_osql_net_poll);

    if (gbl_osql_net_portmux_register_interval)
        net_set_portmux_register_interval(
            tmp->handle_sibling, gbl_osql_net_portmux_register_interval);
    if (!gbl_accept_on_child_nets)
        net_set_portmux_register_interval(tmp->handle_sibling, 0);

    /* add peers */
    for (ii = 1; ii < dbenv->nsiblings; ii++) {

        rcv = (void *)add_to_netinfo(tmp->handle_sibling,
                                     dbenv->sibling_hostname[ii],
                                     dbenv->sibling_port[ii][NET_SQL]);
        if (rcv == 0) {
            logmsg(LOGMSG_ERROR, "%s: failed add_to_netinfo host %s port %d\n",
                    __func__, dbenv->sibling_hostname[ii],
                    dbenv->sibling_port[ii][NET_SQL]);
            free(tmp);
            return -1;
        }
    }

    /* sqloffload handler */
    net_register_handler(tmp->handle_sibling, NET_OSQL_SOCK_REQ,
                         "osql_sock_req", net_sosql_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SOCK_RPL,
                         "osql_sock_rpl", net_osql_rpl);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SIGNAL, "osql_signal",
                         net_sorese_signal);

    net_register_handler(tmp->handle_sibling, NET_OSQL_RECOM_REQ,
                         "osql_recom_req", net_recom_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_RECOM_RPL,
                         "osql_recom_rpl", net_osql_rpl);

    net_register_handler(tmp->handle_sibling, NET_OSQL_SNAPISOL_REQ,
                         "osql_snapisol_req", net_snapisol_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SNAPISOL_RPL,
                         "osql_snapisol_rpl", net_osql_rpl);

    net_register_handler(tmp->handle_sibling, NET_OSQL_SERIAL_REQ,
                         "osql_serial_req", net_serial_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SERIAL_RPL,
                         "osql_serial_rpl", net_osql_rpl);

    net_register_handler(tmp->handle_sibling, NET_OSQL_MASTER_CHECK,
                         "osql_master_check", net_osql_master_check);
    net_register_handler(tmp->handle_sibling, NET_OSQL_MASTER_CHECKED,
                         "osql_master_checked", net_osql_master_checked);
    /* register echo service handler */
    net_register_handler(tmp->handle_sibling, NET_OSQL_ECHO_PING,
                         "osql_echo_ping", net_osql_rcv_echo_ping);
    net_register_handler(tmp->handle_sibling, NET_OSQL_ECHO_PONG,
                         "osql_echo_pong", net_osql_rcv_echo_pong);

    /* register the uuid clones */
    net_register_handler(tmp->handle_sibling, NET_OSQL_SOCK_REQ_UUID,
                         "osql_sock_req_uuid", net_sosql_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SOCK_RPL_UUID,
                         "osql_sock_rpl_uuid", net_osql_rpl);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SIGNAL_UUID,
                         "osql_signal_uuid", net_sorese_signal);

    net_register_handler(tmp->handle_sibling, NET_OSQL_RECOM_REQ_UUID,
                         "osql_recom_req_uuid", net_recom_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_RECOM_RPL_UUID,
                         "osql_recom_rpl_uuid", net_osql_rpl);

    net_register_handler(tmp->handle_sibling, NET_OSQL_SNAPISOL_REQ_UUID,
                         "osql_snapisol_req_uuid", net_snapisol_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SNAPISOL_RPL_UUID,
                         "osql_snapisol_rpl_uuid", net_osql_rpl);

    net_register_handler(tmp->handle_sibling, NET_OSQL_SERIAL_REQ_UUID,
                         "osql_serial_req_uuid", net_serial_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SERIAL_RPL_UUID,
                         "osql_serial_rpl_uuid", net_osql_rpl);

    net_register_handler(tmp->handle_sibling, NET_OSQL_MASTER_CHECK_UUID,
                         "osql_master_check_uuid", net_osql_master_check);
    net_register_handler(tmp->handle_sibling, NET_OSQL_MASTER_CHECKED_UUID,
                         "osql_master_checked_uuid", net_osql_master_checked);

    /* this guy will terminate pending requests */
    net_register_hostdown(tmp->handle_sibling, net_osql_nodedwn);

    /* register the routine that will be called when a new thread
       starts that might call into bdb lib */
    net_register_start_thread_callback(tmp->handle_sibling,
                                       net_startthread_rtn);

    /* register the routine that will be called when a new thread
       starts that might call into bdb lib */
    net_register_stop_thread_callback(tmp->handle_sibling, net_stopthread_rtn);

    /* set the callback data so we get our bdb_state pointer from these
     * calls. */
    net_set_callback_data(tmp->handle_sibling, dbenv->bdb_env);

    /* set the heartbit for the offload network */
    if (gbl_heartbeat_check)
        net_set_heartbeat_check_time(tmp->handle_sibling, gbl_heartbeat_check);

    /* remote blocksql request handler. */
    net_register_handler(tmp->handle_sibling, NET_BLOCK_REQ, "block_req",
                         net_block_req);
    net_register_handler(tmp->handle_sibling, NET_BLOCK_REPLY, "block_reply",
                         net_block_reply);

    /* remote snap uid requests */
    net_register_handler(tmp->handle_sibling, NET_OSQL_SNAP_UID_REQ,
                         "osql_snap_uid_req", net_snap_uid_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SNAP_UID_RPL,
                         "osql_snap_uid_rpl", net_snap_uid_rpl);

    /* kick the guy */
    rc = net_init(tmp->handle_sibling);
    if (rc) {
        logmsg(LOGMSG_ERROR, 
            "You're on your own buddy, no peers (net_init failed w/ rc = %d)\n",
            rc);
        tmp->handle_sibling = NULL;
        free(tmp);
        return -1;
    }

    if (debug_switch_net_verbose())
        net_trace(tmp->handle_sibling, 1);

    thecomm_obj = tmp;

    bdb_register_rtoff_callback(dbenv->bdb_env, signal_rtoff);

    return 0;
}

/**
 * Destroy osql endpoint.
 * No communication is possible after this.
 *
 */
void osql_comm_destroy(void)
{
    if (g_osql_ready)
        logmsg(LOGMSG_ERROR, "Osql module was not disabled yet!\n");
    thecomm_obj = NULL;
}

typedef struct net_block_msg {
    unsigned long long rqid; /* fastseed */
    int datalen;
    int rc;
    char data[1];
} net_block_msg_t;

int offload_comm_send_blockreq(char *host, void *rqid, void *buf, int buflen)
{
    int rc = 0;
    int len = buflen + sizeof(net_block_msg_t);
    net_block_msg_t *net_msg = malloc(len);
    net_msg->rqid = (unsigned long long)rqid;
    net_msg->datalen = buflen;
    memcpy(net_msg->data, buf, buflen);
    rc = offload_net_send(host, NET_BLOCK_REQ, net_msg, len, 1, NULL, 0);
    free(net_msg);
    return rc;
}

static void net_block_req(void *hndl, void *uptr, char *fromhost, int usertype,
                          void *dtap, int dtalen, uint8_t is_tcp)
{

    net_block_msg_t *net_msg = dtap;
    handle_buf_block_offload(thedb, (uint8_t *)net_msg->data,
                             (uint8_t *)net_msg->data + net_msg->datalen, 0,
                             fromhost, net_msg->rqid);
}

int offload_comm_send_blockreply(char *host, unsigned long long rqid, void *buf,
                                 int buflen, int return_code)
{
    int rc = 0;
    int len = buflen + sizeof(net_block_msg_t);
    net_block_msg_t *net_msg = malloc(len);
    net_msg->rqid = rqid;
    net_msg->rc = return_code;
    net_msg->datalen = buflen;
    memcpy(net_msg->data, buf, buflen);
    rc = offload_net_send(host, NET_BLOCK_REPLY, net_msg, len, 1, NULL, 0);
    free(net_msg);
    return rc;
}

static void net_block_reply(void *hndl, void *uptr, char *fromhost,
                            int usertype, void *dtap, int dtalen,
                            uint8_t is_tcp)
{

    net_block_msg_t *net_msg = dtap;
    /* using p_slock pointer as the request id now, this contains info about
     * socket request.*/
    struct buf_lock_t *p_slock = (struct buf_lock_t *)net_msg->rqid;
    {
        Pthread_mutex_lock(&p_slock->req_lock);
        if (p_slock->reply_state == REPLY_STATE_DISCARD) {
            /* The tag request is handled by master. However by the time
               (1000+ seconds) the replicant receives the reply from master,
               the tag request is already discarded. */
            Pthread_mutex_unlock(&p_slock->req_lock);
            cleanup_lock_buffer(p_slock);
        } else {
            p_slock->rc = net_msg->rc;
            sndbak_open_socket(p_slock->sb, (u_char *)net_msg->data,
                               net_msg->datalen, net_msg->rc);
            /* Signal to allow the appsock thread
               to take new request from client. */
            signal_buflock(p_slock);
            Pthread_mutex_unlock(&p_slock->req_lock);
        }
    }
}

static void net_snap_uid_req(void *hndl, void *uptr, char *fromhost,
                             int usertype, void *dtap, int dtalen,
                             uint8_t is_tcp)
{
    int rc = 0;

    snap_uid_t snap_info, *snap_in = dtap;

    snap_uid_get(&snap_info, (uint8_t *)snap_in,
                 (uint8_t *)snap_in + sizeof(snap_uid_t));

    snap_uid_t snap_send, *snap_out = NULL;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;
    p_buf_start = p_buf = (uint8_t *)&snap_send;
    p_buf_end = p_buf + sizeof(snap_uid_t);

    rc = bdb_blkseq_find(thedb->bdb_env, NULL, snap_info.key, snap_info.keylen,
                         (void **)&snap_out, NULL);

    if (rc == IX_FND) {
        comdb2uuidcpy(snap_out->uuid, snap_info.uuid);
        snap_out->rqtype = OSQL_NET_SNAP_FOUND_UID;
        snap_uid_put(snap_out, p_buf, p_buf_end);
        free(snap_out);
    } else if (rc == IX_NOTFND) {
        snap_info.rqtype = OSQL_NET_SNAP_NOT_FOUND_UID;
        snap_uid_put(&snap_info, p_buf, p_buf_end);
    } else {
        snap_info.rqtype = OSQL_NET_SNAP_ERROR;
        snap_uid_put(&snap_info, p_buf, p_buf_end);
    }

    offload_net_send(fromhost, NET_OSQL_SNAP_UID_RPL, p_buf_start,
                     sizeof(snap_uid_t), 1, NULL, 0);
}

void log_snap_info_key(snap_uid_t *snap_info)
{
    if (snap_info)
        logmsg(LOGMSG_USER, "%*s", snap_info->keylen - 3, snap_info->key);
    else
        logmsg(LOGMSG_USER, "NO_CNONCE"); // ex. SC
}

static void net_snap_uid_rpl(void *hndl, void *uptr, char *fromhost,
                             int usertype, void *dtap, int dtalen,
                             uint8_t is_tcp)
{
    snap_uid_t snap_info;
    snap_uid_get(&snap_info, dtap, (uint8_t *)dtap + dtalen);
    osql_chkboard_sqlsession_rc(OSQL_RQID_USE_UUID, snap_info.uuid, 0,
                                &snap_info, NULL, &snap_info.effects);
}

int gbl_disable_cnonce_blkseq;

/**
 * If "rpl" is a done packet, set xerr to error if any and return 1
 * If "rpl" is a recognizable packet, returns the length of the data type is
 *recognized,
 * or -1 otherwise
 *
 */
int osql_comm_is_done(osql_sess_t *sess, int type, char *rpl, int rpllen,
                      int is_uuid, struct errstat **xerr,
                      struct query_effects *effects)
{
    int rc = 0;
    switch (type) {
    case OSQL_USEDB:
    case OSQL_INSREC:
    case OSQL_INSERT:
    case OSQL_INSIDX:
    case OSQL_DELIDX:
    case OSQL_QBLOB:
    case OSQL_STARTGEN:
        break;
    case OSQL_DONE_SNAP:
        osql_extract_snap_info(sess, rpl, rpllen, is_uuid);
        /* fall-through */
    case OSQL_DONE:
    case OSQL_DONE_STATS:
        if (xerr)
            *xerr = NULL;
        rc = 1;
        break;
    case OSQL_DONE_WITH_EFFECTS:
        if (effects) {
            const uint8_t *p_buf =
                (uint8_t *)rpl + sizeof(osql_done_t) +
                (is_uuid ? sizeof(osql_uuid_rpl_t) : sizeof(osql_rpl_t));

            const uint8_t *p_buf_end = (const uint8_t *)rpl + rpllen;
            if ((p_buf = osqlcomm_query_effects_get(effects, p_buf,
                                                    p_buf_end)) == NULL) {
                abort();
            }
            // TODO: (NC) also read fk_effects
        }
        if (xerr)
            *xerr = NULL;
        rc = 1;
        break;
    case OSQL_XERR:
        /* keep this un-endianized.  the code will swap what it needs to */
        if (xerr) {
            if (is_uuid)
                *xerr = &((osql_done_xerr_uuid_t *)rpl)->dt;
            else
                *xerr = &((osql_done_xerr_t *)rpl)->dt;
        }
        rc = 1;
        break;
    default:
        if (sess)
            sess->is_delayed = true;
        break;
    }
    return rc;
}

/**
 * Send a "POKE" message to "tohost" inquering about session "rqid"
 *
 */
int osql_comm_send_poke(char *tohost, unsigned long long rqid, uuid_t uuid,
                        int type)
{
    int rc = 0;

    if (rqid == OSQL_RQID_USE_UUID) {
        uint8_t buf[OSQLCOMM_POKE_UUID_TYPE_LEN];
        uint8_t *p_buf = buf, *p_buf_end = p_buf + OSQLCOMM_POKE_UUID_TYPE_LEN;
        osql_poke_uuid_t poke = {{0}};

        poke.tstamp = comdb2_time_epoch();
        comdb2uuidcpy(poke.uuid, uuid);

        if (!(p_buf = osqlcomm_poke_uuid_type_put(&poke, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returned NULL\n", __func__,
                    "osqlcomm_poke_uuid_type_put");
            return -1;
        }

        type = osql_net_type_to_net_uuid_type(type);
        rc = offload_net_send(tohost, type, &buf, sizeof(buf), 1, NULL, 0);
    } else {
        osql_poke_t poke = {0};
        uint8_t buf[OSQLCOMM_POKE_TYPE_LEN],
            *p_buf = buf, *p_buf_end = buf + OSQLCOMM_POKE_TYPE_LEN;

        poke.tstamp = comdb2_time_epoch();

        poke.from = gbl_mynodeid;
        poke.to = nodenum(tohost);
        poke.rqid = rqid;

        if (!(p_buf = osqlcomm_poke_type_put(&poke, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_poke_type_put");
            return -1;
        }
        rc = offload_net_send(tohost, type, &buf, sizeof(buf), 1, NULL, 0);
    }

    return rc;
}

static int osql_net_type_to_net_uuid_type(int type)
{
    switch (type) {
    case NET_OSQL_SOCK_REQ:
        return NET_OSQL_SOCK_REQ_UUID;
    case NET_OSQL_SOCK_RPL:
        return NET_OSQL_SOCK_RPL_UUID;
    case NET_OSQL_SIGNAL:
        return NET_OSQL_SIGNAL_UUID;
    case NET_OSQL_RECOM_REQ:
        return NET_OSQL_RECOM_REQ_UUID;
    case NET_OSQL_RECOM_RPL:
        return NET_OSQL_RECOM_RPL_UUID;
    case NET_OSQL_SNAPISOL_REQ:
        return NET_OSQL_SNAPISOL_REQ_UUID;
    case NET_OSQL_SNAPISOL_RPL:
        return NET_OSQL_SNAPISOL_RPL_UUID;
    case NET_OSQL_SERIAL_REQ:
        return NET_OSQL_SERIAL_REQ_UUID;
    case NET_OSQL_SERIAL_RPL:
        return NET_OSQL_SERIAL_RPL_UUID;
    case NET_OSQL_MASTER_CHECK:
        return NET_OSQL_MASTER_CHECK_UUID;
    case NET_OSQL_MASTER_CHECKED:
        return NET_OSQL_MASTER_CHECKED_UUID;
    default:
        return type;
    }
}

int is_tablename_queue(const char *name)
{
    /* See also, __db_open @ /berkdb/db/db_open.c for register_qdb */
    return strncmp(name, "__q", 3) == 0;
}

int osql_send_startgen(char *tohost, unsigned long long rqid, uuid_t uuid,
                       uint32_t start_gen, int type)
{
    uint8_t
        buf[(int)OSQLCOMM_STARTGEN_UUID_RPL_LEN > (int)OSQLCOMM_STARTGEN_RPL_LEN
                ? OSQLCOMM_STARTGEN_UUID_RPL_LEN
                : OSQLCOMM_STARTGEN_RPL_LEN];
    int msglen;
    int rc;

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_startgen_uuid_rpl_t startgen_uuid_rpl = {{0}};
        uint8_t *p_buf = buf;
        uint8_t *p_buf_end = (p_buf + OSQLCOMM_STARTGEN_UUID_RPL_LEN);
        msglen = OSQLCOMM_STARTGEN_UUID_RPL_LEN;
        startgen_uuid_rpl.hd.type = OSQL_STARTGEN;
        comdb2uuidcpy(startgen_uuid_rpl.hd.uuid, uuid);
        startgen_uuid_rpl.dt.start_gen = start_gen;

        if (!(p_buf = osqlcomm_startgen_uuid_rpl_type_put(&startgen_uuid_rpl,
                                                          p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                   "osqlcomm_startgen_uuid_rpl_type_put");
            return -1;
        }
        type = osql_net_type_to_net_uuid_type(NET_OSQL_SOCK_RPL);

    } else {
        osql_startgen_rpl_t startgen_rpl = {{0}};
        uint8_t *p_buf = buf;
        uint8_t *p_buf_end = (p_buf + OSQLCOMM_STARTGEN_RPL_LEN);
        msglen = OSQLCOMM_STARTGEN_RPL_LEN;
        startgen_rpl.hd.type = OSQL_STARTGEN;
        startgen_rpl.hd.sid = rqid;
        startgen_rpl.dt.start_gen = start_gen;

        if (!(p_buf = osqlcomm_startgen_rpl_type_put(&startgen_rpl, p_buf,
                                                     p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                   "osqlcomm_startgen_rpl_type_put");
            return -1;
        }
    }

    if (gbl_enable_osql_logging) {
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%llu %s] send OSQL_STARTGEN %u\n", rqid,
               comdb2uuidstr(uuid, us), start_gen);
    }

    rc = offload_net_send(tohost, type, &buf, msglen, 0, NULL, 0);

    if (rc)
        logmsg(LOGMSG_ERROR, "%s offload_net_send returns rc=%d\n", __func__,
               rc);

    return rc;
}

/**
 * Send USEDB op
 * It handles remote/local connectivity
 *
 */
int osql_send_usedb(char *tohost, unsigned long long rqid, uuid_t uuid,
                    char *tablename, int type, unsigned long long tableversion)
{
    unsigned short tablenamelen = strlen(tablename) + 1; /*including trailing 0*/
    int msglen;
    int rc = 0;
    int sent;

    uint8_t buf[(int)OSQLCOMM_USEDB_RPL_UUID_TYPE_LEN >
                        (int)OSQLCOMM_USEDB_RPL_TYPE_LEN
                    ? OSQLCOMM_USEDB_RPL_UUID_TYPE_LEN
                    : OSQLCOMM_USEDB_RPL_TYPE_LEN];

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_usedb_rpl_uuid_t usedb_uuid_rpl = {{0}};
        uint8_t *p_buf = buf;
        uint8_t *p_buf_end = (p_buf + OSQLCOMM_USEDB_RPL_UUID_TYPE_LEN);

        sent = sizeof(usedb_uuid_rpl.dt.tablename);
        msglen = OSQLCOMM_USEDB_RPL_UUID_TYPE_LEN;

        usedb_uuid_rpl.hd.type = OSQL_USEDB;
        comdb2uuidcpy(usedb_uuid_rpl.hd.uuid, uuid);
        usedb_uuid_rpl.dt.tablenamelen = tablenamelen;
        usedb_uuid_rpl.dt.tableversion = tableversion;
        /* tablename field needs to be NOT null-terminated if > than 4 chars */
        strncpy(usedb_uuid_rpl.dt.tablename, tablename,
                sizeof(usedb_uuid_rpl.dt.tablename));

        if (!(p_buf = osqlcomm_usedb_uuid_rpl_type_put(&usedb_uuid_rpl, p_buf,
                                                       p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_usedb_uuid_rpl_type_put");
            return -1;
        }
        type = osql_net_type_to_net_uuid_type(NET_OSQL_SOCK_RPL);
    } else {
        osql_usedb_rpl_t usedb_rpl = {{0}};
        uint8_t *p_buf = buf;
        uint8_t *p_buf_end = (p_buf + OSQLCOMM_USEDB_RPL_TYPE_LEN);

        /* allocate and set reply */
        sent = sizeof(usedb_rpl.dt.tablename);
        msglen = OSQLCOMM_USEDB_RPL_TYPE_LEN;

        usedb_rpl.hd.type = OSQL_USEDB;
        usedb_rpl.hd.sid = rqid;
        usedb_rpl.dt.tablenamelen = tablenamelen;
        usedb_rpl.dt.tableversion = tableversion;
        /* tablename field needs to be NOT null-terminated if > than 4 chars */
        strncpy(usedb_rpl.dt.tablename, tablename,
                sizeof(usedb_rpl.dt.tablename));

        if (!(p_buf =
                  osqlcomm_usedb_rpl_type_put(&usedb_rpl, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_usedb_rpl_type_put");
            return -1;
        }
    }

    if (gbl_enable_osql_logging) {
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%llu %s] send OSQL_USEDB %.*s\n", rqid,
               comdb2uuidstr(uuid, us), tablenamelen, tablename);
    }

    /* tablename field is not null-terminated -- send rest of tablename */
    rc = offload_net_send(tohost, type, &buf, msglen, 0,
                          (tablenamelen > sent) ? tablename + sent : NULL,
                          (tablenamelen > sent) ? tablenamelen - sent : 0);

    if (rc)
        logmsg(LOGMSG_ERROR, "%s offload_net_send returns rc=%d\n", __func__,
               rc);

    int d_ms = BDB_ATTR_GET(thedb->bdb_attr, DELAY_AFTER_SAVEOP_USEDB);
    if (d_ms) {
        logmsg(LOGMSG_DEBUG, "Sleeping for DELAY_AFTER_SAVEOP_USEDB (%dms)\n",
               d_ms);
        usleep(1000 * d_ms);
    }

    return rc;
}

/**
 * Send UPDCOLS op
 * It handles remote/local connectivity
 *
 */
int osql_send_updcols(char *tohost, unsigned long long rqid, uuid_t uuid,
                      unsigned long long seq, int type, int *colList, int ncols)
{
    int rc = 0;
    int didmalloc = 0;
    int i;
    int datalen = sizeof(int) * ncols;
    int totlen;
    uint8_t *buf;
    uint8_t *p_buf;
    uint8_t *p_buf_end;

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (ncols <= 0)
        return -2;
    totlen = datalen;
    if (rqid == OSQL_RQID_USE_UUID)
        totlen += OSQLCOMM_UPDCOLS_UUID_RPL_TYPE_LEN;
    else
        totlen += OSQLCOMM_UPDCOLS_RPL_TYPE_LEN;

    if (totlen > 4096) {
        buf = malloc(totlen);
        didmalloc = 1;
    } else {
        buf = alloca(totlen);
    }

    p_buf = buf;
    p_buf_end = (p_buf + totlen);
    if (rqid == OSQL_RQID_USE_UUID) {
        osql_updcols_uuid_rpl_t rpl = {{0}};

        rpl.hd.type = OSQL_UPDCOLS;
        comdb2uuidcpy(rpl.hd.uuid, uuid);
        rpl.dt.seq = seq;
        rpl.dt.ncols = ncols;

        if (!(p_buf =
                  osqlcomm_updcols_uuid_rpl_type_put(&rpl, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_updcols_uuid_rpl_type_put");
            if (didmalloc)
                free(buf);
            return -1;
        }

        type = osql_net_type_to_net_uuid_type(type);
    } else {
        osql_updcols_rpl_t rpl = {{0}};
        rpl.hd.type = OSQL_UPDCOLS;
        rpl.hd.sid = rqid;
        rpl.dt.seq = seq;
        rpl.dt.ncols = ncols;

        if (!(p_buf = osqlcomm_updcols_rpl_type_put(&rpl, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_updcols_rpl_type_put");
            if (didmalloc)
                free(buf);
            return -1;
        }
    }

    for (i = 0; i < ncols; i++) {
        p_buf = buf_put(&colList[i], sizeof(int), p_buf, p_buf_end);
    }

    if (gbl_enable_osql_logging) {
        logmsg(LOGMSG_DEBUG, "[%llu] send OSQL_UPDCOLS %d\n", rqid, ncols);
    }

    rc = offload_net_send(tohost, type, buf, totlen, 0, NULL, 0);

    if (didmalloc)
        free(buf);

    return rc;
}

/**
 * Send INDEX op
 * It handles remote/local connectivity
 *
 */
int osql_send_index(char *tohost, unsigned long long rqid, uuid_t uuid,
                    unsigned long long genid, int isDelete, int ixnum,
                    char *pData, int nData, int type)
{
    int msglen;
    uint8_t buf[(int)OSQLCOMM_INDEX_RPL_TYPE_LEN >
                        (int)OSQLCOMM_INDEX_UUID_RPL_TYPE_LEN
                    ? OSQLCOMM_INDEX_RPL_TYPE_LEN
                    : OSQLCOMM_INDEX_UUID_RPL_TYPE_LEN];
    int rc = 0;
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end = NULL;

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_index_uuid_rpl_t index_uuid_rpl = {{0}};

        index_uuid_rpl.hd.type = isDelete ? OSQL_DELIDX : OSQL_INSIDX;
        comdb2uuidcpy(index_uuid_rpl.hd.uuid, uuid);
        index_uuid_rpl.dt.seq = genid;
        index_uuid_rpl.dt.ixnum = ixnum;
        index_uuid_rpl.dt.nData = nData;

        p_buf_end = p_buf + OSQLCOMM_INDEX_UUID_RPL_TYPE_LEN;

        if (!(p_buf = osqlcomm_index_uuid_rpl_type_put(&index_uuid_rpl, p_buf,
                                                       p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_index_rpl_type_put");
            return -1;
        }
        msglen = sizeof(index_uuid_rpl);

        /* override message type */
        type = osql_net_type_to_net_uuid_type(NET_OSQL_SOCK_RPL);
    } else {
        osql_index_rpl_t index_rpl = {{0}};

        index_rpl.hd.type = isDelete ? OSQL_DELIDX : OSQL_INSIDX;
        index_rpl.hd.sid = rqid;
        index_rpl.dt.seq = genid;
        index_rpl.dt.ixnum = ixnum;
        index_rpl.dt.nData = nData;

        p_buf_end = p_buf + OSQLCOMM_INDEX_RPL_TYPE_LEN;

        if (!(p_buf =
                  osqlcomm_index_rpl_type_put(&index_rpl, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_index_rpl_type_put");
            return -1;
        }
        msglen = sizeof(index_rpl);
    }

    if (gbl_enable_osql_logging) {
        unsigned long long lclgenid = bdb_genid_to_host_order(genid);
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%llx %s] send %s %llx (%lld)\n", rqid,
               comdb2uuidstr(uuid, us),
               isDelete ? "OSQL_DELIDX" : "OSQL_INSIDX", lclgenid, lclgenid);
    }

    rc = offload_net_send(tohost, type, buf, msglen, 0,
                          (nData > 0) ? pData : NULL, (nData > 0) ? nData : 0);

    return rc;
}

/**
 * Send QBLOB op
 * It handles remote/local connectivity
 *
 */
int osql_send_qblob(char *tohost, unsigned long long rqid, uuid_t uuid,
                    int blobid, unsigned long long seq, int type, char *data,
                    int datalen)
{
    int sent;
    uint8_t buf[(int)OSQLCOMM_QBLOB_UUID_RPL_TYPE_LEN >
                        (int)OSQLCOMM_QBLOB_RPL_TYPE_LEN
                    ? OSQLCOMM_QBLOB_UUID_RPL_TYPE_LEN
                    : OSQLCOMM_QBLOB_RPL_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end;
    int rc = 0;
    int msgsz = 0;
    osql_qblob_rpl_t rpl = {{0}};

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_qblob_uuid_rpl_t rpl_uuid = {{0}};
        p_buf_end = p_buf + OSQLCOMM_QBLOB_UUID_RPL_TYPE_LEN;

        rpl_uuid.hd.type = OSQL_QBLOB;
        comdb2uuidcpy(rpl_uuid.hd.uuid, uuid);
        rpl_uuid.dt.id = blobid;
        rpl_uuid.dt.seq = seq;
        rpl_uuid.dt.bloblen = datalen;

        if (!(p_buf = osqlcomm_qblob_uuid_rpl_type_put(&rpl_uuid, p_buf,
                                                       p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_qblob_rpl_type_put");
            return -1;
        }

        type = osql_net_type_to_net_uuid_type(type);
        msgsz = OSQLCOMM_QBLOB_UUID_RPL_TYPE_LEN;
        sent = ((signed)sizeof(rpl_uuid.dt.blob) < datalen)
                   ? sizeof(rpl_uuid.dt.blob)
                   : datalen;
    } else {
        p_buf_end = p_buf + OSQLCOMM_QBLOB_RPL_TYPE_LEN;
        rpl.hd.type = OSQL_QBLOB;
        rpl.hd.sid = rqid;
        rpl.dt.id = blobid;
        rpl.dt.seq = seq;
        rpl.dt.bloblen = datalen;

        if (!(p_buf = osqlcomm_qblob_rpl_type_put(&rpl, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_qblob_rpl_type_put");
            return -1;
        }
        msgsz = OSQLCOMM_QBLOB_RPL_TYPE_LEN;
        sent = ((signed)sizeof(rpl.dt.blob) < datalen) ? sizeof(rpl.dt.blob)
                                                       : datalen;
    }

    /*
     * the protocol is that we always send 8 bytes of blob data, even if the
     * bloblen is less than 8.  The first 8 bytes of blob data will be sent as
     * part of the qblob structure, the rest will be the tail.
     *
     * p_buf is pointing at the blob-data offset in the qblob structure.  Zero
     * this out to handle the cases where the bloblen is less than 8 bytes.
     */

    memset(p_buf, 0, sizeof(rpl.dt.blob));

    /*
     * If there is any blob-data at all, copy up to the 8th byte into the rpl
     * buffer.
     */
    if (datalen > 0)
        p_buf = buf_no_net_put(data, sent, p_buf, p_buf_end);

    if (gbl_enable_osql_logging) {
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%llx %s] send OSQL_QBLOB %d %d\n", rqid,
               comdb2uuidstr(uuid, us), blobid, datalen);
    }

#if DEBUG_REORDER
    logmsg(
        LOGMSG_DEBUG,
        "REORDER: putting blob id=%d, seq=%lld, bloblen(datalen)=%d, sent=%d\n",
        blobid, seq, datalen, sent);
    if (datalen > 0) {
        char *tmpstr;
        void hexdumpbuf(char *key, int keylen, char **buf);
        hexdumpbuf(data, datalen, &tmpstr);
        logmsg(LOGMSG_DEBUG, "REORDER: hexdump datalen=%d blob='%s'\n", datalen,
               tmpstr);
    }
#endif

    rc = offload_net_send(tohost, type, buf, msgsz, 0,
                          (datalen > sent) ? data + sent : NULL,
                          (datalen > sent) ? datalen - sent : 0);

    return rc;
}

/**
 * Send UPDREC op
 * It handles remote/local connectivity
 *
 */
int osql_send_updrec(char *tohost, unsigned long long rqid, uuid_t uuid,
                     unsigned long long genid, unsigned long long ins_keys,
                     unsigned long long del_keys, char *pData, int nData,
                     int type)
{
    uint8_t
        buf[(int)OSQLCOMM_UPD_UUID_RPL_TYPE_LEN > (int)OSQLCOMM_UPD_RPL_TYPE_LEN
                ? OSQLCOMM_UPD_UUID_RPL_TYPE_LEN
                : OSQLCOMM_UPD_RPL_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end;
    int rc = 0;
    int sent;
    int msgsz;
    osql_upd_rpl_t upd_rpl = {{0}};
    int send_dk = 0;

    if (gbl_partial_indexes && ins_keys != -1ULL && del_keys != -1ULL)
        send_dk = 1;

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_upd_uuid_rpl_t upd_uuid_rpl = {{0}};

        if (send_dk) {
            p_buf_end = buf + OSQLCOMM_UPD_UUID_RPL_TYPE_LEN;
            msgsz = OSQLCOMM_UPD_UUID_RPL_TYPE_LEN;
        } else {
            p_buf_end = buf + OSQLCOMM_UPD_UUID_RPL_TYPE_LEN -
                        sizeof(ins_keys) - sizeof(del_keys);
            msgsz = OSQLCOMM_UPD_UUID_RPL_TYPE_LEN - sizeof(ins_keys) -
                    sizeof(del_keys);
        }
        upd_uuid_rpl.hd.type = send_dk ? OSQL_UPDATE : OSQL_UPDREC;
        comdb2uuidcpy(upd_uuid_rpl.hd.uuid, uuid);
        upd_uuid_rpl.dt.genid = genid;
        upd_uuid_rpl.dt.ins_keys = ins_keys;
        upd_uuid_rpl.dt.del_keys = del_keys;
        upd_uuid_rpl.dt.nData = nData;
        sent = sizeof(upd_uuid_rpl.dt.pData);
        if (!(p_buf = osqlcomm_upd_uuid_rpl_type_put(&upd_uuid_rpl, p_buf,
                                                     p_buf_end, send_dk))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_upd_uuid_rpl_type_put");
            return -1;
        }

        type = osql_net_type_to_net_uuid_type(type);
    } else {
        if (send_dk) {
            p_buf_end = buf + OSQLCOMM_UPD_RPL_TYPE_LEN;
            msgsz = OSQLCOMM_UPD_RPL_TYPE_LEN;
        } else {
            p_buf_end = buf + OSQLCOMM_UPD_RPL_TYPE_LEN - sizeof(ins_keys) -
                        sizeof(del_keys);
            msgsz =
                OSQLCOMM_UPD_RPL_TYPE_LEN - sizeof(ins_keys) - sizeof(del_keys);
        }
        upd_rpl.hd.type = send_dk ? OSQL_UPDATE : OSQL_UPDREC;
        upd_rpl.hd.sid = rqid;
        upd_rpl.dt.genid = genid;
        upd_rpl.dt.ins_keys = ins_keys;
        upd_rpl.dt.del_keys = del_keys;
        upd_rpl.dt.nData = nData;
        sent = sizeof(upd_rpl.dt.pData);

        if (!(p_buf = osqlcomm_upd_rpl_type_put(&upd_rpl, p_buf, p_buf_end,
                                                send_dk))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_upd_rpl_type_put");
            return -1;
        }
    }

    /*
     * p_buf is pointing at the beginning of the pData section of upd_rpl.  Zero
     * this for the case where the length is less than 8
     */
    memset(p_buf, 0, sizeof(upd_rpl.dt.pData));

    if (nData > 0) {
        p_buf = buf_no_net_put(pData, nData < sent ? nData : sent, p_buf,
                               p_buf_end);
    }

    if (gbl_enable_osql_logging) {
        unsigned long long lclgenid = bdb_genid_to_host_order(genid);
        logmsg(LOGMSG_DEBUG, "[%llu] send OSQL_UPDREC %llx (%lld)\n", rqid,
               lclgenid, lclgenid);
    }

    rc = offload_net_send(tohost, type, &buf, msgsz, 0,
                          (nData > sent) ? pData + sent : NULL,
                          (nData > sent) ? nData - sent : 0);

    return rc;
}

void osql_decom_node(char *decom_node)
{
    logmsg(LOGMSG_INFO, "osql_decom_node %s\n", decom_node);

    netinfo_type *netinfo_ptr;

    osql_comm_t *comm = get_thecomm();
    if (!comm) {
        logmsg(LOGMSG_ERROR, "osql_decom_node: no comm object?\n");
        return;
    }
    netinfo_ptr = (netinfo_type *)comm->handle_sibling;

    if (netinfo_ptr == NULL) {
        /* why is this happening?  not sure, cant reproduce yet.  stop
           the crashing though. */
        logmsg(LOGMSG_ERROR, "osql_decom_node: null netinfo\n");
        return;
    }

    net_decom_node(comm->handle_sibling, decom_node);
}

/**
 * Signal net layer that the db is exiting
 *
 */
void osql_net_exiting(void)
{
    osql_comm_t *comm = get_thecomm();
    if(!comm) return;
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    net_exiting(netinfo_ptr);
}

void osql_cleanup_netinfo(void)
{
    osql_comm_t *comm = get_thecomm();
    if(!comm) return;
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    net_cleanup_netinfo(netinfo_ptr);
}

/* Send dbglog op */
int osql_send_dbglog(char *tohost, unsigned long long rqid, uuid_t uuid,
                     unsigned long long dbglog_cookie, int queryid, int type)
{
    osql_dbglog_t req = {0};
    uint8_t buf[OSQLCOMM_DBGLOG_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end = p_buf + OSQLCOMM_DBGLOG_TYPE_LEN;
    int rc;

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    req.opcode = OSQL_DBGLOG;
    req.rqid = rqid;
    req.dbglog_cookie = dbglog_cookie;
    req.queryid = queryid;

    if (!(osqlcomm_dbglog_type_put(&req, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s:%s return NULL\n", __func__,
                "osqlcomm_dbglog_type_put");
        return -1;
    }

    rc =
        offload_net_send(tohost, type, &buf, sizeof(osql_dbglog_t), 0, NULL, 0);
    return rc;
}

/**
 * Send UPDSTAT op
 * It handles remote/local connectivity
 *
 */
int osql_send_updstat(char *tohost, unsigned long long rqid, uuid_t uuid,
                      unsigned long long seq, char *pData, int nData, int nStat,
                      int type)
{
    osql_updstat_rpl_t updstat_rpl = {{0}};
    osql_updstat_uuid_rpl_t updstat_rpl_uuid = {{0}};

    uint8_t buf[(int)OSQLCOMM_UPDSTAT_RPL_TYPE_LEN >
                        (int)OSQLCOMM_UPDSTAT_UUID_RPL_TYPE_LEN
                    ? OSQLCOMM_UPDSTAT_RPL_TYPE_LEN
                    : OSQLCOMM_UPDSTAT_UUID_RPL_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end = p_buf + sizeof(buf);
    int rc = 0;
    int sent;
    int msglen;

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (rqid == OSQL_RQID_USE_UUID) {
        updstat_rpl_uuid.hd.type = OSQL_UPDSTAT;
        comdb2uuidcpy(updstat_rpl_uuid.hd.uuid, uuid);
        updstat_rpl_uuid.dt.seq = seq;
        updstat_rpl_uuid.dt.nData = nData;
        updstat_rpl_uuid.dt.nStat = nStat;

        if (!(p_buf = osqlcomm_updstat_uuid_rpl_type_put(&updstat_rpl_uuid,
                                                         p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_updstat_uuid_rpl_type_put");
            return -1;
        }
        memset(p_buf, 0, sizeof(updstat_rpl_uuid.dt.pData));
        msglen = sizeof(updstat_rpl_uuid);
        sent = sizeof(updstat_rpl_uuid.dt.pData);
        type = osql_net_type_to_net_uuid_type(NET_OSQL_SOCK_RPL);
    } else {
        updstat_rpl.hd.type = OSQL_UPDSTAT;
        updstat_rpl.hd.sid = rqid;
        updstat_rpl.dt.seq = seq;
        updstat_rpl.dt.nData = nData;
        updstat_rpl.dt.nStat = nStat;
        if (!(p_buf = osqlcomm_updstat_rpl_type_put(&updstat_rpl, p_buf,
                                                    p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_updstat_rpl_type_put");
            return -1;
        }
        memset(p_buf, 0, sizeof(updstat_rpl.dt.pData));
        msglen = sizeof(updstat_rpl);
        sent = sizeof(updstat_rpl.dt.pData);
    }

    if (nData > 0) {
        p_buf = buf_no_net_put(pData, nData < sent ? nData : sent, p_buf,
                               p_buf_end);
    }

    if (gbl_enable_osql_logging) {
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%llu %s] send OSQL_UPDSTATREC %llx (%lld)\n",
               rqid, comdb2uuidstr(uuid, us), seq, seq);
    }

    rc = offload_net_send(tohost, type, buf, msglen, 0,
                          (nData > sent) ? pData + sent : NULL,
                          (nData > sent) ? nData - sent : 0);
    return rc;
}

/**
 * Send INSREC op
 * It handles remote/local connectivity
 *
 */
int osql_send_insrec(char *tohost, unsigned long long rqid, uuid_t uuid,
                     unsigned long long genid, unsigned long long dirty_keys,
                     char *pData, int nData, int type, int upsert_flags)
{
    int msglen;
    uint8_t
        buf[(int)OSQLCOMM_INS_RPL_TYPE_LEN > (int)OSQLCOMM_INS_UUID_RPL_TYPE_LEN
                ? OSQLCOMM_INS_RPL_TYPE_LEN
                : OSQLCOMM_INS_UUID_RPL_TYPE_LEN];
    int rc = 0;
    int sent;
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end = NULL;
    int send_dk = 0;

    if (gbl_partial_indexes && dirty_keys != -1ULL)
        send_dk = 1;

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (rqid == OSQL_RQID_USE_UUID) {
        int len = OSQLCOMM_INS_UUID_RPL_TYPE_LEN;
        int flags = 0;
        osql_ins_uuid_rpl_t ins_uuid_rpl = {{0}};

        ins_uuid_rpl.hd.type = OSQL_INSERT;
        comdb2uuidcpy(ins_uuid_rpl.hd.uuid, uuid);
        ins_uuid_rpl.dt.seq = genid;
        if (upsert_flags) {
            flags |= OSQL_INSERT_UPSERT;
            ins_uuid_rpl.dt.upsert_flags = upsert_flags;
        } else {
            len -= sizeof(ins_uuid_rpl.dt.upsert_flags);
        }
        if (send_dk) {
            flags |= OSQL_INSERT_SEND_DK;
            ins_uuid_rpl.dt.dk = dirty_keys;
        } else {
            len -= sizeof(ins_uuid_rpl.dt.dk);
        }
        ins_uuid_rpl.dt.flags = flags;
        ins_uuid_rpl.dt.nData = nData;

        p_buf_end = p_buf + len;

        if (!(p_buf = osqlcomm_ins_uuid_rpl_type_put(&ins_uuid_rpl, p_buf,
                                                     p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                   "osqlcomm_ins_uuid_rpl_type_put");
            return -1;
        }
        msglen = len;
        sent = sizeof(ins_uuid_rpl.dt.pData);

        /*
         * p_buf is pointing at the beginning of the pData section of ins_rpl.
         * Zero
         * this for the case where the length is less than 8.
         */
        memset(p_buf, 0, sizeof(ins_uuid_rpl.dt.pData));
        /* override message type */
        type = osql_net_type_to_net_uuid_type(NET_OSQL_SOCK_RPL);
    } else {
        osql_ins_rpl_t ins_rpl = {{0}};

        if (send_dk || upsert_flags) {
            logmsg(LOGMSG_ERROR,
                   "%s: partial index/upsert not supported in legacy mode\n",
                   __func__);
            return -1;
        }

        ins_rpl.hd.type = OSQL_INSREC;
        ins_rpl.hd.sid = rqid;
        ins_rpl.dt.seq = genid;
        ins_rpl.dt.nData = nData;

        p_buf_end = p_buf + OSQLCOMM_INS_RPL_TYPE_LEN;

        if (!(p_buf = osqlcomm_ins_rpl_type_put(&ins_rpl, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_ins_rpl_type_put");
            return -1;
        }
        msglen = OSQLCOMM_INS_RPL_TYPE_LEN;
        sent = sizeof(ins_rpl.dt.pData);
        memset(p_buf, 0, sizeof(ins_rpl.dt.pData));
    }

    if (nData > 0) {
        p_buf = buf_no_net_put(pData, nData < sent ? nData : sent, p_buf,
                               p_buf_end);
    }

    if (gbl_enable_osql_logging) {
        unsigned long long lclgenid = bdb_genid_to_host_order(genid);
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%llx %s] send %s %llx (%lld)\n", rqid,
               comdb2uuidstr(uuid, us),
               rqid == OSQL_RQID_USE_UUID ? "OSQL_INSERT" : "OSQL_INSREC",
               lclgenid, lclgenid);
    }

    rc = offload_net_send(tohost, type, buf, msglen, 0,
                          (nData > sent) ? pData + sent : NULL,
                          (nData > sent) ? nData - sent : 0);

    return rc;
}

int osql_send_dbq_consume(char *tohost, unsigned long long rqid, uuid_t uuid,
                          genid_t genid, int type)
{
    union {
        osql_dbq_consume_uuid_t uuid;
        osql_dbq_consume_t rqid;
    } rpl = {{{0}}};
    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;
    if (gbl_enable_osql_logging) {
        genid_t lclgenid = bdb_genid_to_host_order(genid);
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%llx %s] send OSQL_DBQ_CONSUME %llx (%lld)\n",
               rqid, comdb2uuidstr(uuid, us), (long long unsigned)lclgenid,
               (long long unsigned)lclgenid);
    }
    size_t sz;
    if (rqid == OSQL_RQID_USE_UUID) {
        rpl.uuid.hd.type = htonl(OSQL_DBQ_CONSUME);
        comdb2uuidcpy(rpl.uuid.hd.uuid, uuid);
        rpl.uuid.genid = genid;
        sz = sizeof(rpl.uuid);
        type = osql_net_type_to_net_uuid_type(type);
    } else {
        rpl.rqid.hd.type = htonl(OSQL_DBQ_CONSUME);
        rpl.rqid.hd.sid = flibc_htonll(rqid);
        rpl.rqid.genid = genid;
        sz = sizeof(rpl.rqid);
    }
    return offload_net_send(tohost, type, &rpl, sz, 0, NULL, 0);
}


/**
 * Send DELREC op
 * It handles remote/local connectivity
 *
 */
int osql_send_delrec(char *tohost, unsigned long long rqid, uuid_t uuid,
                     unsigned long long genid, unsigned long long dirty_keys,
                     int type)
{
    uint8_t buf[(int)OSQLCOMM_OSQL_DEL_RPL_TYPE_LEN >
                        (int)OSQLCOMM_OSQL_DEL_UUID_RPL_TYPE_LEN
                    ? OSQLCOMM_OSQL_DEL_RPL_TYPE_LEN
                    : OSQLCOMM_OSQL_DEL_UUID_RPL_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end;
    int rc = 0;
    int msgsz;
    int send_dk = 0;

    if (gbl_partial_indexes && dirty_keys != -1ULL)
        send_dk = 1;

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;
    if (rqid == OSQL_RQID_USE_UUID) {
        osql_del_uuid_rpl_t del_uuid_rpl = {{0}};
        if (send_dk) {
            p_buf_end = p_buf + OSQLCOMM_OSQL_DEL_UUID_RPL_TYPE_LEN;
            msgsz = OSQLCOMM_OSQL_DEL_UUID_RPL_TYPE_LEN;
        } else {
            p_buf_end = p_buf + OSQLCOMM_OSQL_DEL_UUID_RPL_TYPE_LEN -
                        sizeof(dirty_keys);
            msgsz = OSQLCOMM_OSQL_DEL_UUID_RPL_TYPE_LEN - sizeof(dirty_keys);
        }

        del_uuid_rpl.hd.type = send_dk ? OSQL_DELETE : OSQL_DELREC;
        comdb2uuidcpy(del_uuid_rpl.hd.uuid, uuid);
        del_uuid_rpl.dt.genid = genid;
        del_uuid_rpl.dt.dk = dirty_keys;

        if (!(p_buf = osqlcomm_del_uuid_rpl_type_put(&del_uuid_rpl, p_buf,
                                                     p_buf_end, send_dk))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_del_uuid_rpl_type_put");
            return -1;
        }
        type = osql_net_type_to_net_uuid_type(type);
    } else {
        osql_del_rpl_t del_rpl = {{0}};
        if (send_dk) {
            p_buf_end = p_buf + OSQLCOMM_OSQL_DEL_RPL_TYPE_LEN;
            msgsz = OSQLCOMM_OSQL_DEL_RPL_TYPE_LEN;
        } else {
            p_buf_end =
                p_buf + OSQLCOMM_OSQL_DEL_RPL_TYPE_LEN - sizeof(dirty_keys);
            msgsz = OSQLCOMM_OSQL_DEL_RPL_TYPE_LEN - sizeof(dirty_keys);
        }

        del_rpl.hd.type = send_dk ? OSQL_DELETE : OSQL_DELREC;
        del_rpl.hd.sid = rqid;
        del_rpl.dt.genid = genid;
        del_rpl.dt.dk = dirty_keys;

        if (!(p_buf = osqlcomm_del_rpl_type_put(&del_rpl, p_buf, p_buf_end,
                                                send_dk))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_del_rpl_type_put");
            return -1;
        }
    }

    if (gbl_enable_osql_logging) {
        unsigned long long lclgenid = bdb_genid_to_host_order(genid);
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%llx %s] send %s %llx (%lld)\n", rqid,
               comdb2uuidstr(uuid, us), send_dk ? "OSQL_DELETE" : "OSQL_DELREC",
               lclgenid, lclgenid);
    }

    rc = offload_net_send(tohost, type, &buf, msgsz, 0, NULL, 0);

    return rc;
}

/**
 * Send SERIAL READ SET
 *
 */
int osql_send_serial(char *tohost, unsigned long long rqid, uuid_t uuid,
                     CurRangeArr *arr, unsigned int file, unsigned int offset,
                     int type)
{
    int used_malloc = 0;
    uint8_t *buf = NULL;
    uint8_t *p_buf;
    uint8_t *p_buf_end;
    int i;
    int b_sz;
    int cr_sz = 0;
    CurRange *cr;
    int rc = 0;

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (arr) {
        for (i = 0; i < arr->size; i++) {
            cr = arr->ranges[i];
            if (cr->tbname) {
                cr_sz += sizeof(char) * (strlen(cr->tbname) + 1) + sizeof(int);
            }
            cr_sz += sizeof(cr->islocked);
            if (!cr->islocked) {
                assert(!cr->lflag || !cr->rflag);
                cr_sz += sizeof(cr->idxnum);
                cr_sz += sizeof(cr->lflag);
                if (!cr->lflag)
                    if (cr->lkey) {
                        cr_sz += sizeof(cr->lkeylen);
                        cr_sz += cr->lkeylen;
                    }
                cr_sz += sizeof(cr->rflag);
                if (!cr->rflag)
                    if (cr->rkey) {
                        cr_sz += sizeof(cr->rkeylen);
                        cr_sz += cr->rkeylen;
                    }
            }
        }
    }

    if (rqid == OSQL_RQID_USE_UUID)
        b_sz = sizeof(osql_serial_uuid_rpl_t);
    else
        b_sz = sizeof(osql_serial_rpl_t);

    b_sz += cr_sz;

    /* only use malloc if we have to */
    if (b_sz > 4096) {
        buf = malloc(b_sz);
        used_malloc = 1;
    } else {
        buf = alloca(b_sz);
    }
    /* frame output buffer */
    p_buf = buf;
    p_buf_end = (p_buf + b_sz);

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_serial_uuid_rpl_t serial_rpl = {{0}};

        serial_rpl.hd.type =
            (type == NET_OSQL_SERIAL_RPL || 
             type == NET_OSQL_SERIAL_RPL_UUID) ? OSQL_SERIAL : OSQL_SELECTV;
        comdb2uuidcpy(serial_rpl.hd.uuid, uuid);
        serial_rpl.dt.buf_size = cr_sz;
        serial_rpl.dt.arr_size = (arr) ? arr->size : 0;
        serial_rpl.dt.file = file;

        serial_rpl.dt.offset = offset;
        serial_rpl.dt.buf_size = cr_sz;
        serial_rpl.dt.arr_size = (arr) ? arr->size : 0;
        serial_rpl.dt.file = file;
        serial_rpl.dt.offset = offset;

        if (gbl_enable_osql_logging) {
            uuidstr_t us;
            logmsg(LOGMSG_DEBUG, "[%s] send OSQL_SERIAL type=%d %d %d\n",
                   comdb2uuidstr(uuid, us), type, cr_sz, arr->size);
        }

        if (!(p_buf = osqlcomm_serial_uuid_rpl_put(&serial_rpl, p_buf,
                                                   p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_serial_rpl_put");
            if (used_malloc)
                free(buf);
            return -1;
        }

        if (arr) {
            p_buf = serial_readset_put(arr, cr_sz, p_buf, p_buf_end);
        }

    } else {
        osql_serial_rpl_t serial_rpl = {{0}};

        serial_rpl.hd.type =
            (type == NET_OSQL_SERIAL_RPL ||
             type == NET_OSQL_SERIAL_RPL_UUID) ? OSQL_SERIAL : OSQL_SELECTV;
        serial_rpl.hd.sid = rqid;
        serial_rpl.dt.buf_size = cr_sz;
        serial_rpl.dt.arr_size = (arr) ? arr->size : 0;
        serial_rpl.dt.file = file;
        serial_rpl.dt.offset = offset;

        if (gbl_enable_osql_logging) {
            logmsg(LOGMSG_DEBUG, "[%llu] send OSQL_SERIAL %d %d\n", rqid, cr_sz,
                   arr->size);
        }

#if 0
       printf("Sending rqid=%llu tmp=%llu\n", rqid, osql_log_time());
#endif

        if (!(p_buf = osqlcomm_serial_rpl_put(&serial_rpl, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_serial_rpl_put");
            if (used_malloc)
                free(buf);
            return -1;
        }

        if (arr) {
            p_buf = serial_readset_put(arr, cr_sz, p_buf, p_buf_end);
        }
    }

    rc = offload_net_send(tohost, type, buf, b_sz, 1, NULL, 0);

    return rc;
}

/**
 * Send DONE or DONE_XERR op
 * It handles remote/local connectivity
 *
 */
int osql_send_commit(char *tohost, unsigned long long rqid, uuid_t uuid,
                     int nops, struct errstat *xerr, int type,
                     struct client_query_stats *query_stats,
                     snap_uid_t *snap_info)
{
    osql_done_rpl_t rpl_ok = {{0}};
    osql_done_xerr_t rpl_xerr = {{0}};
    int b_sz;
    int used_malloc = 0;
    int snap_info_length = 0;
    uint8_t *buf = NULL;
    uint8_t *p_buf;
    uint8_t *p_buf_end;
    int rc = xerr->errval;

    /* Always 'commit' to release starthrottle.  Failure if master has swung. */
    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    /* we're also sending stats - calculate the total buffer size */
    if (rc != SQLITE_OK) {
        b_sz = sizeof(rpl_xerr);
    } else {
        if (snap_info) {
            snap_info_length = sizeof(snap_uid_t);
        }

        if (!query_stats) {
            b_sz = sizeof(rpl_ok) + snap_info_length;
        } else {
            int qs_sz = offsetof(struct client_query_stats, path_stats);
            qs_sz += query_stats->n_components *
                     sizeof(struct client_query_path_component);
            b_sz = sizeof(osql_done_rpl_t) + snap_info_length + qs_sz;
        }
    }

    /* only use malloc if we have to */
    if (b_sz > 4096) {
        buf = malloc(b_sz);
        used_malloc = 1;
    } else {
        buf = alloca(b_sz);
    }

    /* frame output buffer */
    p_buf = buf;
    p_buf_end = (p_buf + b_sz);

    if (rc == SQLITE_OK) {
        if (snap_info) {
            rpl_ok.hd.type = OSQL_DONE_SNAP;
        } else {
            rpl_ok.hd.type = OSQL_DONE; /* OSQL_DONE_STATS is never set, so
                                           query_stats never read by master.? */
        }
        rpl_ok.hd.sid = rqid;
        rpl_ok.dt.rc = rc;
        /* hack to help old code interpret the results correctly
           convert SQLITE_OK to SQLITE_DONE
         */
        if (!rpl_ok.dt.rc)
            rpl_ok.dt.rc = SQLITE_DONE;
        rpl_ok.dt.nops = nops;

        if (gbl_enable_osql_logging) {
            logmsg(LOGMSG_DEBUG, "[%llu] send commit %s %d %d\n", rqid,
                   osql_reqtype_str(rpl_ok.hd.type), rc, nops);
        }

#if DEBUG_REORDER
        DEBUGMSG("[%llu] send %s rc = %d, nops = %d\n", rqid,
                 osql_reqtype_str(rpl_ok.hd.type), rc, nops);
#endif

        if (!(p_buf = osqlcomm_done_rpl_put(&rpl_ok, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                   "osqlcomm_done_rpl_put");
            if (used_malloc)
                free(buf);
            return -1;
        }

        if (snap_info) {
            p_buf = (uint8_t *)snap_uid_put(snap_info, p_buf, p_buf_end);
        }

        if (query_stats) {
            if (!(p_buf =
                      client_query_stats_put(query_stats, p_buf, p_buf_end))) {
                logmsg(LOGMSG_ERROR, "%s line %d:%s returns NULL\n", __func__,
                       __LINE__, "osqlcomm_done_rpl_put");
                if (used_malloc)
                    free(buf);
                return -1;
            }
        }
        rc = offload_net_send(tohost, type, buf, b_sz, 1, NULL, 0);

    } else {

        rpl_xerr.hd.type = OSQL_XERR;
        rpl_xerr.hd.sid = rqid;

        memcpy(&rpl_xerr.dt, xerr, sizeof(rpl_xerr.dt));

        if (!osqlcomm_done_xerr_type_put(&rpl_xerr, p_buf, p_buf_end)) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                   "osqlcomm_done_xerr_type_put");
            if (used_malloc)
                free(buf);
            return -1;
        }
        rc = offload_net_send(tohost, type, buf, sizeof(rpl_xerr), 1, NULL, 0);
    }
    if (used_malloc)
        free(buf);

    return rc;
}

int type_to_uuid_type(int type)
{
    switch (type) {
    case NET_OSQL_SOCK_RPL:
        return NET_OSQL_SOCK_RPL_UUID;
    default:
        logmsg(LOGMSG_FATAL, "type_to_uuid_type: unhandled type %d\n", type);
        abort();
    }
}

int osql_send_commit_by_uuid(char *tohost, uuid_t uuid, int nops,
                             struct errstat *xerr, int type,
                             struct client_query_stats *query_stats,
                             snap_uid_t *snap_info)
{
    osql_done_uuid_rpl_t rpl_ok = {{0}};
    osql_done_xerr_uuid_t rpl_xerr = {{0}};
    int b_sz;
    int used_malloc = 0;
    int snap_info_length = 0;
    uint8_t *buf = NULL;
    uint8_t *p_buf;
    uint8_t *p_buf_end;
    int rc = xerr->errval;

    type = osql_net_type_to_net_uuid_type(type);

    /* Always 'commit' to release starthrottle.  Failure if master has swung. */
    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    /* we're also sending stats - calculate the total buffer size */
    if (rc != SQLITE_OK) {
        b_sz = sizeof(rpl_xerr);
    } else {
        if (snap_info) {
            snap_info_length = sizeof(snap_uid_t);
        }

        if (!query_stats) {
            b_sz = sizeof(rpl_ok) + snap_info_length;
        } else {
            int qs_sz = offsetof(struct client_query_stats, path_stats);
            qs_sz += query_stats->n_components *
                     sizeof(struct client_query_path_component);
            b_sz = sizeof(osql_done_uuid_rpl_t) + snap_info_length + qs_sz;
        }
    }

    /* only use malloc if we have to */
    if (b_sz > 4096) {
        buf = malloc(b_sz);
        used_malloc = 1;
    } else {
        buf = alloca(b_sz);
    }

    /* frame output buffer */
    p_buf = buf;
    p_buf_end = (p_buf + b_sz);

    if (rc == SQLITE_OK) {
        if (snap_info) {
            rpl_ok.hd.type = OSQL_DONE_SNAP;
        } else {
            /* Send 'done' to master, no need to send OSQL_DONE_WITH_EFFECTS
             * (which also includes query effects).
             */
            rpl_ok.hd.type = OSQL_DONE; /* OSQL_DONE_STATS is never set, so
                                           query_stats never read by master.? */
        }
        comdb2uuidcpy(rpl_ok.hd.uuid, uuid);
        rpl_ok.dt.rc = rc;
        /* hack to help old code interpret the results correctly
           convert SQLITE_OK to SQLITE_DONE
         */
        if (!rpl_ok.dt.rc)
            rpl_ok.dt.rc = SQLITE_DONE;
        rpl_ok.dt.nops = nops;

        uuidstr_t us;
        if (gbl_enable_osql_logging) {
            logmsg(LOGMSG_DEBUG, "[%s] send %s %d %d\n",
                   comdb2uuidstr(uuid, us), osql_reqtype_str(rpl_ok.hd.type),
                   rc, nops);
        }

#if DEBUG_REORDER
        DEBUGMSG("uuid=%s send %s rc = %d, nops = %d\n",
                 comdb2uuidstr(uuid, us), osql_reqtype_str(rpl_ok.hd.type), rc,
                 nops);
#endif

        if (!(p_buf = osqlcomm_done_uuid_rpl_put(&rpl_ok, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                   "osqlcomm_done_uuid_rpl_put");
            if (used_malloc)
                free(buf);
            return -1;
        }

        if (snap_info) {
            p_buf = (uint8_t *)snap_uid_put(snap_info, p_buf, p_buf_end);
        }

        if (query_stats) {
            if (!(p_buf =
                      client_query_stats_put(query_stats, p_buf, p_buf_end))) {
                logmsg(LOGMSG_ERROR, "%s line %d:%s returns NULL\n", __func__,
                       __LINE__, "client_query_stats_put");
                if (used_malloc)
                    free(buf);
                return -1;
            }
        }
        rc = offload_net_send(tohost, type, buf, b_sz, 1, NULL, 0);

    } else {

        rpl_xerr.hd.type = OSQL_XERR;
        comdb2uuidcpy(rpl_xerr.hd.uuid, uuid);

        memcpy(&rpl_xerr.dt, xerr, sizeof(rpl_xerr.dt));

#if DEBUG_REORDER
        uuidstr_t us;
        DEBUGMSG("uuid=%s send %s rc = %d, nops = %d\n",
                 comdb2uuidstr(uuid, us), osql_reqtype_str(rpl_xerr.hd.type),
                 rc, nops);
#endif
        if (!osqlcomm_done_xerr_uuid_type_put(&rpl_xerr, p_buf, p_buf_end)) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                   "osqlcomm_done_xerr_type_put");
            if (used_malloc)
                free(buf);
            return -1;
        }
        rc = offload_net_send(tohost, type, buf, sizeof(rpl_xerr), 1, NULL, 0);
    }
    if (used_malloc)
        free(buf);

    return rc;
}

/**
 * Send decomission for osql net
 *
 */
int osql_process_message_decom(char *host)
{
    osql_comm_t *comm = get_thecomm();
    if (comm) {
        net_send_decom_all(comm->handle_sibling, host);
    }

    return 0;
}

/**
 * Constructs a reusable osql request
 *
 * This creates either a osql_req_t or a osql_req_uuid_t, as needed.
 *
 */
static void *osql_create_request(const char *sql, int sqlen, int type,
                                 unsigned long long rqid, uuid_t uuid,
                                 char *tzname, int *prqlen, int flags)
{
    int rqlen = 0;
    uint8_t *p_buf, *p_buf_end;
    osql_req_t req = {0};
    osql_uuid_req_t req_uuid = {0};
    void *ret;

    if (rqid == OSQL_RQID_USE_UUID) {
        rqlen = sizeof(osql_uuid_req_t) + sqlen;
    } else {
        rqlen = sizeof(osql_req_t) + sqlen;
    }

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_uuid_req_t *r_uuid_ptr;

        r_uuid_ptr = (osql_uuid_req_t *)malloc(rqlen);
        ret = r_uuid_ptr;

        if (!r_uuid_ptr) {
            logmsg(LOGMSG_ERROR, 
                    "create_sql: error malloc-ing for sql request, size %d\n",
                    rqlen);
            return NULL;
        }

        p_buf = (uint8_t *)r_uuid_ptr;
        p_buf_end = p_buf + rqlen;

        req_uuid.type = type;
        req_uuid.flags = flags;
        comdb2uuidcpy(req_uuid.uuid, uuid);
        req_uuid.rqlen = rqlen;
        req_uuid.sqlqlen = sqlen;

        if (tzname)
            strncpy0(r_uuid_ptr->tzname, tzname, sizeof(r_uuid_ptr->tzname));

        p_buf = osqlcomm_req_uuid_type_put(&req_uuid, p_buf, p_buf_end);
        r_uuid_ptr->rqlen = rqlen;
    } else {
        osql_req_t *r_ptr;

        r_ptr = (osql_req_t *)malloc(rqlen);
        ret = r_ptr;

        if (!r_ptr) {
            logmsg(LOGMSG_ERROR, 
                    "create_sql: error malloc-ing for sql request, size %d\n",
                    rqlen);
            return NULL;
        }

        p_buf = (uint8_t *)r_ptr;
        p_buf_end = p_buf + rqlen;

        req.type = type;
        req.flags = flags;
        req.padding = 0;
        req.rqid = rqid;
        req.rqlen = rqlen;
        req.sqlqlen = sqlen;

        p_buf = osqlcomm_req_type_put(&req, p_buf, p_buf_end);
        r_ptr->rqlen = rqlen;

        if (tzname)
            strncpy0(r_ptr->tzname, tzname, sizeof(r_ptr->tzname));
    }

    p_buf = buf_no_net_put(sql, sqlen, p_buf, p_buf_end);

    *prqlen = rqlen;

    return ret;
}

/**
 * Change the rqid and to allow reusing the request
 *
 */
void osql_remap_request(osql_req_t *req, unsigned long long rqid)
{
    buf_put(&rqid, sizeof(rqid), (uint8_t *)&(req->rqid),
            (uint8_t *)&(req->rqid) + sizeof(long long));
}

/**
 * Retrieve the network-ordered rqid from this osql_req_t
 */

static long long osql_request_getrqid(osql_req_t *req)
{
    long long out_rqid = 0LL;
    buf_get(&out_rqid, sizeof(out_rqid), (uint8_t *)&req->rqid,
            (uint8_t *)&req->rqid + sizeof(long long));
    return out_rqid;
}

/**
 * Get request uuid (follows rqid if request supports it)
 */
static void osql_request_getuuid(osql_req_t *req, uuid_t uuid)
{
    comdb2uuid_clear(uuid);
}

/**
 * Sends a user command to offload net (used by "osqlnet")
 *
 */
void osql_net_cmd(char *line, int lline, int st, int op1)
{
    osql_comm_t *comm = get_thecomm();
    if (comm && comm->handle_sibling) {
        net_cmd(comm->handle_sibling, line, lline, st, op1);
    } else {
        logmsg(LOGMSG_WARN, "osql not ready yet\n");
    }
}

/**
 * Sets the osql net-poll value.
 *
 */
void osql_set_net_poll(int pval)
{
    osql_comm_t *comm = get_thecomm();
    if (comm) {
        net_set_poll(comm->handle_sibling, pval);
    }
}

/**
 * Sends a sosql request to the master
 * Sql is the first update part of this transaction
 *
 */
int osql_comm_send_socksqlreq(char *tohost, const char *sql, int sqlen,
                              unsigned long long rqid, uuid_t uuid,
                              char *tzname, int type, int flags)
{

    void *req = NULL;
    int reqlen = 0;
    int rc = 0;
    int net_type;

    stats[type].snd++;

    /* TODO: need to pass tag/tagbuf/tagbuflen here? */
    req = osql_create_request(sql, sqlen, type, rqid, uuid, tzname, &reqlen,
                              flags);
    if (!req) {
        stats[type].snd_failed++;
        return rc;
    }

    net_type = req2netreq(type);
    if (rqid == OSQL_RQID_USE_UUID) {
        if (net_type == NET_OSQL_SOCK_REQ)
            net_type = NET_OSQL_SOCK_REQ_UUID;
        if (net_type == NET_OSQL_RECOM_REQ)
            net_type = NET_OSQL_RECOM_REQ_UUID;
        else if (net_type == NET_OSQL_SNAPISOL_REQ)
            net_type = NET_OSQL_SNAPISOL_REQ_UUID;
        else if (net_type == NET_OSQL_SERIAL_REQ)
            net_type = NET_OSQL_SERIAL_REQ_UUID;
        else if (net_type < NET_OSQL_UUID_REQUEST_MIN ||
                 net_type > NET_OSQL_UUID_REQUEST_MAX) {
            logmsg(LOGMSG_FATAL, "unknown type %d\n", net_type);
            abort();
        }
    }

    rc = offload_net_send(tohost, net_type, req, reqlen, 1, NULL, 0);

    if (rc)
        stats[type].snd_failed++;

    free(req);

    return rc;
}

/**
 * Sends the result of block processor transaction commit
 * to the sql thread so that it can return the result to the
 * client
 *
 */
int osql_comm_signal_sqlthr_rc(const char *host, unsigned long long rqid,
                               uuid_t uuid, int nops, struct errstat *xerr,
                               snap_uid_t *snap, int rc)
{
    uuidstr_t us;
    int msglen = 0;
    int type;
    union {
        char a[OSQLCOMM_DONE_XERR_UUID_RPL_LEN];
        char b[OSQLCOMM_DONE_UUID_RPL_v2_LEN];
        char c[OSQLCOMM_DONE_XERR_RPL_LEN];
        char d[OSQLCOMM_DONE_RPL_LEN];
    } largest_message;
    uint8_t *buf = (uint8_t *)&largest_message;

    /* test if the sql thread was the one closing the request,
     * and if so, don't send anything back, request might be gone already anyway
     */
    if (xerr->errval == SQLITE_ABORT)
        return 0;

    /* if error, lets send the error string */
    if (!host) {
        /* local */
        return osql_chkboard_sqlsession_rc(rqid, uuid, nops, snap, xerr,
                                           (snap) ? &snap->effects : NULL);
    }

    /* remote */
    if (rqid == OSQL_RQID_USE_UUID) {
        if (rc) {
            osql_done_xerr_uuid_t rpl_xerr = {{0}};
            msglen = OSQLCOMM_DONE_XERR_UUID_RPL_LEN;
            uint8_t *p_buf = buf;
            uint8_t *p_buf_end = buf + msglen;
            rpl_xerr.hd.type = OSQL_XERR;
            comdb2uuidcpy(rpl_xerr.hd.uuid, uuid);
            rpl_xerr.dt = *xerr;
            osqlcomm_done_xerr_uuid_type_put(&(rpl_xerr), p_buf, p_buf_end);
        } else {
            osql_done_uuid_rpl_t rpl_ok = {{0}};
            uint8_t *p_buf = buf;
            uint8_t *p_buf_end = buf + OSQLCOMM_DONE_UUID_RPL_v2_LEN;
            if (likely(gbl_master_sends_query_effects)) {
                rpl_ok.hd.type = OSQL_DONE_WITH_EFFECTS;
            } else {
                rpl_ok.hd.type = OSQL_DONE;
            }
            comdb2uuidcpy(rpl_ok.hd.uuid, uuid);
            rpl_ok.dt.rc = 0;
            rpl_ok.dt.nops = nops;
            if (snap) {
                rpl_ok.effects = snap->effects;
            }
            p_buf = osqlcomm_done_uuid_rpl_put(&(rpl_ok), p_buf, p_buf_end);

            msglen = OSQLCOMM_DONE_UUID_RPL_v1_LEN;

            /* Send query effects to the replicant. */
            if (likely(gbl_master_sends_query_effects)) {
                p_buf = osqlcomm_query_effects_put(&(rpl_ok.effects), p_buf,
                                                   p_buf_end);
                p_buf = osqlcomm_query_effects_put(&(rpl_ok.fk_effects), p_buf,
                                                   p_buf_end);
                msglen = OSQLCOMM_DONE_UUID_RPL_v2_LEN;
            }
        }
        type = osql_net_type_to_net_uuid_type(NET_OSQL_SIGNAL);
        logmsg(LOGMSG_DEBUG,
               "%s:%d master signaling %s uuid %s with rc=%d xerr=%d\n",
               __func__, __LINE__, host, comdb2uuidstr(uuid, us), rc,
               xerr->errval);
    } else {
        if (rc) {
            osql_done_xerr_t rpl_xerr = {{0}};
            msglen = OSQLCOMM_DONE_XERR_RPL_LEN;
            uint8_t *p_buf = buf;
            uint8_t *p_buf_end = buf + msglen;
            rpl_xerr.hd.type = OSQL_XERR;
            rpl_xerr.hd.sid = rqid;
            rpl_xerr.dt = *xerr;
            osqlcomm_done_xerr_type_put(&(rpl_xerr), p_buf, p_buf_end);
        } else {
            osql_done_rpl_t rpl_ok = {{0}};
            msglen = OSQLCOMM_DONE_RPL_LEN;
            uint8_t *p_buf = buf;
            uint8_t *p_buf_end = buf + msglen;
            rpl_ok.hd.type = OSQL_DONE;
            rpl_ok.hd.sid = rqid;
            rpl_ok.dt.rc = 0;
            rpl_ok.dt.nops = nops;
            osqlcomm_done_rpl_put(&(rpl_ok), p_buf, p_buf_end);
        }
        type = NET_OSQL_SIGNAL;
        logmsg(LOGMSG_DEBUG,
               "%s:%d master signaling %s rqid %llu with rc=%d xerr=%d\n",
               __func__, __LINE__, host, rqid, rc, xerr->errval);
    }
#if 0
  printf("Send %d rqid=%llu tmp=%llu\n",  NET_OSQL_SIGNAL, rqid, osql_log_time());
#endif
    /* lazy again, works just because node!=0 */
    int irc = offload_net_send(host, type, buf, msglen, 1, NULL, 0);
    if (irc) {
        irc = -1;
        logmsg(LOGMSG_ERROR, "%s: error sending done to %s!\n", __func__, host);
    }
    return irc;
}

/**
 * Report on the traffic noticed
 *
 */
static const char *reqtypes[OSQL_MAX_REQ] = {
    "invalid", "osql",      "sosql",      "recom",
    "serial",  "cost_osql", "cost_sosql", "snapisol"};

int osql_comm_quick_stat(void)
{
    int i = 0;

    for (i = 1; i < OSQL_MAX_REQ; i++) {
        logmsg(LOGMSG_USER, "%s snd(failed) %lu(%lu) rcv(failed, redundant) %lu(%lu,%lu)\n",
               reqtypes[i], stats[i].snd, stats[i].snd_failed, stats[i].rcv,
               stats[i].rcv_failed, stats[i].rcv_rdndt);
    }
    return 0;
}

int osql_comm_diffstat(struct reqlogger *statlogger, int *have_scon_header)
{
    static osql_stats_t last[OSQL_MAX_REQ], diff[OSQL_MAX_REQ];
    osql_stats_t now[OSQL_MAX_REQ];
    int rc = 0;

    for (int i = 0; i < OSQL_MAX_REQ; i++) {
        now[i].snd = stats[i].snd;
        now[i].snd_failed = stats[i].snd_failed;
        now[i].rcv = stats[i].rcv;
        now[i].rcv_failed = stats[i].rcv_failed;
        now[i].rcv_rdndt = stats[i].rcv_rdndt;

        diff[i].snd = now[i].snd - last[i].snd;
        diff[i].snd_failed = now[i].snd_failed - last[i].snd_failed;
        diff[i].rcv = now[i].rcv - last[i].rcv;
        diff[i].rcv_failed = now[i].rcv_failed - last[i].rcv_failed;
        diff[i].rcv_rdndt = now[i].rcv_rdndt - last[i].rcv_rdndt;

        last[i].snd = now[i].snd;
        last[i].snd_failed = now[i].snd_failed;
        last[i].rcv = now[i].rcv;
        last[i].rcv_failed = now[i].rcv_failed;
        last[i].rcv_rdndt = now[i].rcv_rdndt;

        if (diff[i].snd || diff[i].snd_failed || diff[i].rcv ||
            diff[i].rcv_failed || diff[i].rcv_rdndt) {
            if (statlogger) {
                reqlog_logf(statlogger, REQL_INFO,
                            "sqlwrops %s snd %ld snd_failed %ld rcv %ld "
                            "rcv_failed %ld rcv_redundant %ld\n",
                            reqtypes[i], diff[i].snd, diff[i].snd_failed,
                            diff[i].rcv, diff[i].rcv_failed, diff[i].rcv_rdndt);
            } else {
                if (!*have_scon_header) {
                    logmsg(LOGMSG_USER, "diff");
                    *have_scon_header = 1;
                }

                logmsg(LOGMSG_USER, " sqlwrops %s", reqtypes[i]);
                if (diff[i].snd)
                    logmsg(LOGMSG_USER, " snd %ld", diff[i].snd);
                if (diff[i].snd_failed)
                    logmsg(LOGMSG_USER, " snd_failed %ld", diff[i].snd_failed);
                if (diff[i].rcv)
                    logmsg(LOGMSG_USER, " rcv %ld", diff[i].rcv);
                if (diff[i].rcv_failed)
                    logmsg(LOGMSG_USER, " rcv %ld", diff[i].rcv_failed);
                if (diff[i].rcv_rdndt)
                    logmsg(LOGMSG_USER, " rcv_redundant %ld", diff[i].rcv_rdndt);
                rc = 1;
            }
        }
    }
    return rc;
}

/****************************** INTERNALS *************************************/

static void net_startthread_rtn(void *arg)
{
    bdb_thread_event((bdb_state_type *)arg, 1);
}

static void net_stopthread_rtn(void *arg)
{
    bdb_thread_event((bdb_state_type *)arg, 0);
}

static void net_osql_master_check(void *hndl, void *uptr, char *fromhost,
                                  int usertype, void *dtap, int dtalen,
                                  uint8_t is_tcp)
{
    uint8_t *p_buf = dtap;
    uint8_t *p_buf_end = p_buf + dtalen;
    osql_poke_t poke = {0};
    osql_poke_uuid_t pokeuuid;
    int found = 0;
    int rc = 0;

    uuid_t uuid;
    unsigned long long rqid = OSQL_RQID_USE_UUID;
    int reply_type;

    comdb2uuid_clear(uuid);

    if (db_is_stopped()) {
        /* don't do anything, we're going down */
        return;
    }

    if (osql_nettype_is_uuid(usertype)) {
        if (!osqlcomm_poke_uuid_type_get(&pokeuuid, p_buf, p_buf_end)) {
            logmsg(LOGMSG_ERROR, "%s: can't unpack %d request\n", __func__,
                    usertype);
            return;
        }
        comdb2uuidcpy(uuid, pokeuuid.uuid);
    } else {
        if (!(osqlcomm_poke_type_get(&poke, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: can't unpack %d request\n", __func__,
                    usertype);
            return;
        }
        rqid = poke.rqid;
    }

    found = osql_repository_session_exists(rqid, uuid);

    if (found) {
        uint8_t buf[OSQLCOMM_EXISTS_RPL_TYPE_LEN];
        uint8_t bufuuid[OSQLCOMM_EXISTS_UUID_RPL_TYPE_LEN];
        uint8_t *p_buf;

        if (rqid == OSQL_RQID_USE_UUID) {
            p_buf = bufuuid;
            p_buf_end = p_buf + OSQLCOMM_EXISTS_UUID_RPL_TYPE_LEN;
            osql_exists_uuid_rpl_t rpl = {{0}};

            rpl.hd.type = OSQL_EXISTS;
            comdb2uuidcpy(rpl.hd.uuid, uuid);
            rpl.dt.status = 0;
            rpl.dt.timestamp = comdb2_time_epoch();

            if (!osqlcomm_exists_uuid_rpl_type_put(&rpl, p_buf, p_buf_end))
                abort();
            reply_type = NET_OSQL_MASTER_CHECKED_UUID;

            if ((rc = offload_net_send(fromhost, reply_type, bufuuid,
                                       sizeof(bufuuid), 1, NULL, 0))) {
                logmsg(LOGMSG_ERROR, "%s: error writting record to master in "
                                "offload mode rc=%d!\n",
                        __func__, rc);
            }
        } else {
            p_buf = buf;
            p_buf_end = p_buf + OSQLCOMM_EXISTS_RPL_TYPE_LEN;

            /* send a done with an error, lost request */
            osql_exists_rpl_t rpl = {{0}};

            rpl.hd.type = OSQL_EXISTS;
            rpl.hd.sid = rqid;
            rpl.dt.status = 0;
            rpl.dt.timestamp = comdb2_time_epoch();

            if (!osqlcomm_exists_rpl_type_put(&rpl, p_buf, p_buf_end))
                abort();

            reply_type = NET_OSQL_MASTER_CHECKED;

            if ((rc = offload_net_send(fromhost, reply_type, buf, sizeof(buf),
                                       1, NULL, 0))) {
                logmsg(LOGMSG_ERROR, "%s: error writting record to master in "
                                "offload mode rc=%d!\n",
                        __func__, rc);
            }
        }

    } else {
        uuidstr_t us;
        logmsg(LOGMSG_ERROR,
               "Missing SORESE sql session %llx %s on %s from %d\n", poke.rqid,
               comdb2uuidstr(uuid, us), gbl_myhostname, poke.from);
    }
}

static void net_osql_master_checked(void *hndl, void *uptr, char *fromhost,
                                    int usertype, void *dtap, int dtalen,
                                    uint8_t is_tcp)
{
    uint8_t *p_buf = dtap;
    uint8_t *p_buf_end = p_buf + dtalen;
    int rc = 0;
    unsigned long long rqid = OSQL_RQID_USE_UUID;
    uuid_t uuid;
    uuidstr_t us;
    int status, timestamp;

    comdb2uuid_clear(uuid);

    if (db_is_stopped()) {
        /* don't do anything, we're going down */
        return;
    }

    if (osql_nettype_is_uuid(usertype)) {
        osql_exists_uuid_rpl_t rpluuid;
        if (!(osqlcomm_exists_uuid_rpl_type_get(&rpluuid, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: invalid data length\n", __func__);
            return;
        }
        comdb2uuidcpy(uuid, rpluuid.hd.uuid);
        status = rpluuid.dt.status;
        timestamp = rpluuid.dt.timestamp;
    } else {
        osql_exists_rpl_t rpl;
        if (!(osqlcomm_exists_rpl_type_get(&rpl, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: invalid data length\n", __func__);
            return;
        }
        rqid = rpl.hd.sid;
        status = rpl.dt.timestamp;
        timestamp = rpl.dt.timestamp;
    }

    /* update the status of the sorese session */
    rc = osql_checkboard_update_status(rqid, uuid, status, timestamp);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to update status for rqid %llu %s rc=%d\n", __func__,
               rqid, comdb2uuidstr(uuid, us), rc);
    }
}

/* terminate node */
static int net_osql_nodedwn(netinfo_type *netinfo_ptr, char *host)
{

    int rc = 0;

    /* this is mainly for master, but we might not be a master anymore at
       this point */
    rc = osql_repository_terminatenode(host);

    /* if only offload net drops, we might lose packets from connection but
       code will not be triggered to informed the sorese sessions that
       socket was dropped; call that here */
    osql_checkboard_check_down_nodes(host);

    return rc;
}

/* if the current node is marked down, master being,
   it will ship mastership to another node
   Here we cancell the pending requests and ask the
   client to resubmit
 */
static void signal_rtoff(void)
{

    if (g_osql_ready && thedb->master == gbl_myhostname) {
        logmsg(LOGMSG_INFO, "%s: canceling pending blocksql transactions\n",
                __func__);
        osql_repository_cancelall();
    }
}

/* this function routes the packet in the case of local communication
   include in this function only "usertype"-s that can have a tail
 */
static int net_local_route_packet_tail(int usertype, void *data, int datalen,
                                       void *tail, int taillen)
{
    switch (usertype) {
    case NET_OSQL_SOCK_REQ:
    case NET_OSQL_SOCK_REQ_COST:
    case NET_OSQL_SOCK_REQ_UUID:
    case NET_OSQL_SOCK_REQ_COST_UUID:
        net_sosql_req(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_OSQL_RECOM_REQ:
    case NET_OSQL_RECOM_REQ_UUID:
        net_recom_req(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_OSQL_SNAPISOL_REQ:
    case NET_OSQL_SNAPISOL_REQ_UUID:
        net_snapisol_req(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_OSQL_SERIAL_REQ:
    case NET_OSQL_SERIAL_REQ_UUID:
        net_serial_req(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_BLOCK_REQ:
        net_block_req(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_BLOCK_REPLY:
        net_block_reply(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_OSQL_SOCK_RPL:
    case NET_OSQL_RECOM_RPL:
    case NET_OSQL_SNAPISOL_RPL:
    case NET_OSQL_SERIAL_RPL:
    case NET_OSQL_SOCK_RPL_UUID:
    case NET_OSQL_RECOM_RPL_UUID:
    case NET_OSQL_SNAPISOL_RPL_UUID:
    case NET_OSQL_SERIAL_RPL_UUID:
        return net_osql_rpl_tail(NULL, NULL, 0, usertype, data, datalen, tail,
                                 taillen);
    default:
        logmsg(LOGMSG_ERROR, "%s: unknown packet type routed locally, %d\n",
                __func__, usertype);
        return -1;
    }
    return 0;
}

int osql_comm_check_bdb_lock(const char *func, int line)
{
    int rc = 0;
    int start;
    int end;

    /* check here if we need to wait for the lock, so we don't prevent this from
     * happening */
    start = time(NULL);
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (!thd)
        return 0;

    rc = clnt_check_bdb_lock_desired(thd->clnt);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s recover_deadlock returned %d\n", __func__, rc);
        rc = -1;
    }
    if ((end = time(NULL)) - start > 2) {
        logmsg(LOGMSG_DEBUG, "%s line %d: %s took %d seconds\n", func, line,
               __func__, end - start);
    }
    return rc;
}

/* This wrapper tries to provide a reliable net_send_tail that will prevent
 * loosing packets due to queue being full */
static int offload_net_send(const char *host, int usertype, void *data,
                            int datalen, int nodelay, void *tail, int tailen)
{
    osql_comm_t *comm = get_thecomm();
    if (!comm)
        return -1;

    netinfo_type *netinfo_ptr = comm->handle_sibling;
    int backoff = gbl_osql_bkoff_netsend;
    int total_wait = backoff;
    int unknownerror_retry = 0;
    int rc = -1;

    if (!tail && debug_switch_osql_simulate_send_error()) {
        if (rand() % 4 == 0) /*25% chance of failure*/
        {
            logmsg(LOGMSG_ERROR, "Punting %s with error -1\n", __func__);
            return -1;
        }
    }

    if (host == gbl_myhostname)
        host = NULL;

    if (!host) {
        /* local save */
        rc = net_local_route_packet_tail(usertype, data, datalen, tail, tailen);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s failed to route locally!\n", __func__);
        }
        return rc;
    }

    while (rc) {

        /* remote send */
        rc = net_send_tail(netinfo_ptr, host, usertype, data, datalen, nodelay,
                           tail, tailen);

        if (NET_SEND_FAIL_QUEUE_FULL == rc || NET_SEND_FAIL_MALLOC_FAIL == rc ||
            NET_SEND_FAIL_NOSOCK == rc) {

            if (total_wait > gbl_osql_bkoff_netsend_lmt) {
                logmsg(
                    LOGMSG_ERROR,
                    "%s:%d giving up sending to %s, rc = %d, total wait = %d\n",
                    __FILE__, __LINE__, host ? host : gbl_myhostname, rc,
                    total_wait);
                return rc;
            }

            if (osql_comm_check_bdb_lock(__func__, __LINE__) != 0) {
                logmsg(LOGMSG_ERROR,
                       "%s:%d failed to check bdb lock, giving up sending to "
                       "%s, rc = %d\n",
                       __FILE__, __LINE__, host ? host : gbl_myhostname, rc);
                return rc;
            }

            poll(NULL, 0, backoff);
            /*backoff *= 2; */
            total_wait += backoff;
        } else if (NET_SEND_FAIL_CLOSED == rc) {
            /* on closed sockets, we simply return; a callback
               will trigger on the other side signalling we've
               lost the comm party */
            logmsg(LOGMSG_ERROR, "%s:%d giving up sending to %s\n", __FILE__,
                   __LINE__, host ? host : gbl_myhostname);
            logmsg(LOGMSG_ERROR,
                   "%s:%d socket is closed, return wrong master\n", __FILE__,
                   __LINE__);
            return OSQL_SEND_ERROR_WRONGMASTER;
        } else if (rc) {
            unknownerror_retry++;
            if (unknownerror_retry >= UNK_ERR_SEND_RETRY) {
                logmsg(LOGMSG_ERROR, "%s:%d giving up sending to %s\n",
                       __FILE__, __LINE__, host ? host : gbl_myhostname);
                comdb2_linux_cheap_stack_trace();
                return -1;
            }
        }
    }

    return rc;
}

static void net_osql_rpl(void *hndl, void *uptr, char *fromnode, int usertype,
                         void *dtap, int dtalen, uint8_t is_tcp)
{
    int found = 0;
    int rc = 0;
    uuid_t uuid;
    int type = 0;
    unsigned long long rqid;
    uint8_t *p_buf = (uint8_t *)dtap;
    uint8_t *p_buf_end = (p_buf + dtalen);

    stats[netrpl2req(usertype)].rcv++;

    if (osql_nettype_is_uuid(usertype)) {
        osql_uuid_rpl_t p_osql_uuid_rpl;

        rqid = OSQL_RQID_USE_UUID;
        if (!(p_buf = (uint8_t *)osqlcomm_uuid_rpl_type_get(
                  &p_osql_uuid_rpl, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_uuid_rpl_type_get");
            rc = -1;
        } else {
            comdb2uuidcpy(uuid, p_osql_uuid_rpl.uuid);
            type = p_osql_uuid_rpl.type;
        }
    } else {
        osql_rpl_t p_osql_rpl;
        uint8_t *p_buf, *p_buf_end;

        comdb2uuid_clear(uuid);
        p_buf = (uint8_t *)dtap;
        p_buf_end = (p_buf + dtalen);

        if (!(p_buf = (uint8_t *)osqlcomm_rpl_type_get(&p_osql_rpl, p_buf,
                                                       p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_rpl_type_get");
            rc = -1;
        } else {
            rqid = p_osql_rpl.sid;
            type = p_osql_rpl.type;
        }
    }

#ifdef TEST_OSQL
    fprintf(stdout, "%s: calling sorese_rcvrpl type=%d sid=%llu\n", __func__,
            netrpl2req(usertype), ((osql_rpl_t *)dtap)->sid);
#endif
#if 0
    printf("NET RPL rqid=%llu tmp=%llu\n", ((osql_rpl_t*)dtap)->sid, osql_log_time());
#endif

    if (!rc)
        rc = osql_sess_rcvop(rqid, uuid, type, dtap, dtalen, &found);
    if (rc)
        stats[netrpl2req(usertype)].rcv_failed++;
    if (!found)
        stats[netrpl2req(usertype)].rcv_rdndt++;
}

static int check_master(const char *tohost)
{

    const char *destnode = (tohost == NULL) ? gbl_myhostname : tohost;
    char *master = thedb->master;

    if (destnode != master) {
        logmsg(LOGMSG_INFO, "%s: master swinged from %s to %s!\n", __func__,
                destnode, master);
        return -1;
    }

    return 0;
}

/* since net_send already serializes the tail,
   this is needed only when routing local packets
   we need to "serialize" the tail as well, therefore the need for duplicate */
static int net_osql_rpl_tail(void *hndl, void *uptr, char *fromhost,
                             int usertype, void *dtap, int dtalen, void *tail,
                             int tailen)
{
    void *dup;

    if (tail && tailen > 0) {
        if (dtalen + tailen > gbl_blob_sz_thresh_bytes)
            dup = comdb2_bmalloc(blobmem, dtalen + tailen);
        else
            dup = malloc(dtalen + tailen);

        if (!dup) {
            logmsg(
                LOGMSG_FATAL,
                "%s: master running out of memory! unable to alloc %d bytes\n",
                __func__, dtalen + tailen);
            abort(); /* rc = NET_SEND_FAIL_MALLOC_FAIL;*/
        }

#ifdef TEST_OSQL
        fprintf(stdout, "%s: calling sorese_rcvrpl type=%d sid=%llu\n",
                __func__, netrpl2req(usertype), ((osql_rpl_t *)dtap)->sid);
#endif

        memmove(dup, dtap, dtalen);
        memmove((char *)dup + dtalen, tail, tailen);
    } else {
        dup = dtap;
    }

    int rc = 0;
    int found = 0;
    uuid_t uuid;
    uint8_t *p_buf = (uint8_t *)dup;
    uint8_t *p_buf_end = p_buf + dtalen;
    unsigned long long rqid;
    int type = 0;
    if (osql_nettype_is_uuid(usertype)) {
        osql_uuid_rpl_t p_osql_rpl;

        if (!(p_buf = (uint8_t *)osqlcomm_uuid_rpl_type_get(&p_osql_rpl, p_buf,
                                                            p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                   "osqlcomm_rpl_type_get");
            rc = -1;
        } else {
            comdb2uuidcpy(uuid, p_osql_rpl.uuid);
            rqid = OSQL_RQID_USE_UUID;
            type = p_osql_rpl.type;
        }

    } else {
        osql_rpl_t p_osql_rpl;

        if (!(p_buf = (uint8_t *)osqlcomm_rpl_type_get(&p_osql_rpl, p_buf,
                                                       p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                   "osqlcomm_rpl_type_get");
            rc = -1;
        } else {
            rqid = p_osql_rpl.sid;
            comdb2uuid_clear(uuid);
            type = p_osql_rpl.type;
        }
    }

    if (!rc)
        rc = osql_sess_rcvop(rqid, uuid, type, dup, dtalen + tailen, &found);

    if (dup != dtap)
        free(dup);

    if (rc)
        stats[netrpl2req(usertype)].rcv_failed++;
    if (!found)
        stats[netrpl2req(usertype)].rcv_rdndt++;
    stats[netrpl2req(usertype)].rcv++;

    return rc;
}

static void net_sosql_req(void *hndl, void *uptr, char *fromhost, int usertype,
                          void *dtap, int dtalen, uint8_t is_tcp)
{

    int rc = 0;
    int type;

    stats[OSQL_SOCK_REQ].rcv++;

#ifdef TEST_BLOCKSOCK
    fprintf(stdout, "%s: calling sorese_rcvreq %d\n", __func__,
            osql_request_getrqid((osql_req_t *)dtap));
#endif

#if 0
    printf( "NET REQ rqid=%llu tmp=%llu\n", ((osql_req_t*)dtap)->rqid, osql_log_time());
#endif

    if (usertype == NET_OSQL_SOCK_REQ_COST)
        type = OSQL_SOCK_REQ_COST;
    else
        type = OSQL_SOCK_REQ;

    /* we handle this inline;
       once we are done, the queue is ready for this fromnode:rqid session so
       reader thread is free to receive rows from replicant even if blockproc is
       not yet up for us */
    if ((rc = sorese_rcvreq(fromhost, dtap, dtalen, type, usertype))) {
        static int once = 0;
        if (!once) {
            logmsg(LOGMSG_ERROR, "%s:unable to receive request rc=%d\n", __func__,
                    rc);
            once = 1;
        }
        stats[OSQL_SOCK_REQ].rcv_failed++;
    }
}

static void net_recom_req(void *hndl, void *uptr, char *fromhost, int usertype,
                          void *dtap, int dtalen, uint8_t is_tcp)
{

    int rc = 0;

    stats[OSQL_RECOM_REQ].rcv++;

#ifdef TEST_RECOM
    fprintf(stdout, "%s: calling recom_rcvreq %d\n", __func__,
            osql_request_getrqid((osql_req_t *)dtap));
#endif

    /* we handle this inline;
       once we are done, the queue is ready for this fromhost:rqid session so
       reader thread is free to receive rows from replicant even if blockproc is
       not yet up for us */
    rc = sorese_rcvreq(fromhost, dtap, dtalen, OSQL_RECOM_REQ, usertype);

    if (rc)
        stats[OSQL_RECOM_REQ].rcv_failed++;
}

static void net_snapisol_req(void *hndl, void *uptr, char *fromhost,
                             int usertype, void *dtap, int dtalen,
                             uint8_t is_tcp)
{

    int rc = 0;

    stats[OSQL_SNAPISOL_REQ].rcv++;

#ifdef TEST_SERIAL
    fprintf(stdout, "%s: calling serial_rcvreq %d\n", __func__,
            osql_request_getrqid((osql_req_t *)dtap));
#endif

    /* we handle this inline;
       once we are done, the queue is ready for this fromnode:rqid session so
       reader thread is free to receive rows from replicant even if blockproc is
       not yet up for us */
    rc = sorese_rcvreq(fromhost, dtap, dtalen, OSQL_SNAPISOL_REQ, usertype);

    if (rc)
        stats[OSQL_SNAPISOL_REQ].rcv_failed++;
}

static void net_serial_req(void *hndl, void *uptr, char *fromhost, int usertype,
                           void *dtap, int dtalen, uint8_t is_tcp)
{

    int rc = 0;

    stats[OSQL_SERIAL_REQ].rcv++;

#ifdef TEST_SERIAL
    fprintf(stdout, "%s: calling serial_rcvreq %d\n", __func__,
            osql_request_getrqid((osql_req_t *)dtap));
#endif

    /* we handle this inline;
       once we are done, the queue is ready for this fromnode:rqid session so
       reader thread is free to receive rows from replicant even if blockproc is
       not yet up for us */
    rc = sorese_rcvreq(fromhost, dtap, dtalen, OSQL_SERIAL_REQ, usertype);

    if (rc)
        stats[OSQL_SERIAL_REQ].rcv_failed++;
}

/**************************** this belongs between comm and blockproc/ put it
 * here *****/

static int conv_rc_sql2blkop(struct ireq *iq, int step, int ixnum, int rc,
                             struct block_err *err, const char *tablename,
                             int idxerr)
{
    int ret = 0;
    /* TODO: maybe we want a more explicit error code ! */
    switch (rc) {
    case 0:
    case SQLITE_DONE:
        ret = RC_OK;
        break;

    case SQLITE_CONSTRAINT:
        reqerrstr(iq, COMDB2_DEL_RC_VFY_CSTRT, "constraints violation");
        ret = ERR_CONVERT_DTA;
        break;

    case SQLITE_DEADLOCK:
        /* hopefully this will retry the blockop */

        ret = RC_INTERNAL_RETRY;
        break;

    case ERR_NO_SUCH_TABLE:
        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_TBL, "no such table \"%s\"",
                  tablename);
        ret = rc;
        break;

    case SQLITE_TOOBIG:
        reqerrstr(iq, COMDB2_BLK_RC_NB_REQS, "transaction too big");
        ret = ERR_TRAN_TOO_BIG;
        break;

    case ERR_SQL_PREP:
        reqerrstr(iq, ERR_SQL_PREPARE, "sql query syntax error");
        ret = ERR_SQL_PREPARE;
        break;

    case OSQL_FAILDISPATCH:
    case OSQL_TOOEARLY:
        ret = rc;
        break;

    case RC_TRAN_CLIENT_RETRY:
        reqerrstr(iq, COMDB2_BLK_RC_FAIL_COMMIT,
                  "block processor stopped rc = %d", rc);
        ret = rc;
        break;

    case ERR_CONVERT_DTA:
        reqerrstr(iq, COMDB2_ADD_RC_CNVT_DTA,
                  "block %d, error converting field index %d", step, idxerr);
        ret = rc;
        break;

    default:
        reqerrstr(iq, COMDB2_BLK_RC_FAIL_COMMIT,
                  "generic internal exception rc = %d", rc);
        ret = ERR_INTERNAL;
        break;
    }

    if (ret && err) {
        err->blockop_num = step;
        err->errcode = ret;
        err->ixnum = ixnum;
    }
    return ret;
}

static inline int is_write_request(int type)
{
    switch (type) {
    case OSQL_DBQ_CONSUME:
    case OSQL_DELREC:
    case OSQL_DELETE:
    case OSQL_UPDSTAT:
    case OSQL_INSREC:
    case OSQL_INSERT:
    case OSQL_UPDREC:
    case OSQL_UPDATE:
        return 1;
    default:
        return 0;
    }
}

void free_cached_idx(uint8_t **cached_idx);

int gbl_disable_tpsc_tblvers = 0;
int start_schema_change_tran_wrapper(const char *tblname,
                                     timepart_sc_arg_t *arg)
{
    struct schema_change_type *sc = arg->s;
    struct ireq *iq = sc->iq;
    int rc;

    strncpy0(sc->tablename, tblname, sizeof(sc->tablename));
    if (gbl_disable_tpsc_tblvers) {
        sc->fix_tp_badvers = 1;
    }

    rc = start_schema_change_tran(iq, sc->tran);
    if ((rc != SC_ASYNC && rc != SC_COMMIT_PENDING) ||
        sc->preempted == SC_ACTION_RESUME ||
        sc->alteronly == SC_ALTER_PENDING) {
        iq->sc = NULL;
    } else {
        iq->sc->sc_next = iq->sc_pending;
        iq->sc_pending = iq->sc;
        if (arg->nshards == arg->indx + 1) {
            /* last shard was done */
            iq->osql_flags |= OSQL_FLAGS_SCDONE;
        } else {
            struct schema_change_type *new_sc = clone_schemachange_type(sc);

            /* fields not cloned */
            new_sc->iq = sc->iq;
            new_sc->tran = sc->tran;

            /* update the new sc */
            arg->s = new_sc;
            iq->sc = new_sc;
        }
    }

    return (rc == SC_ASYNC || rc == SC_COMMIT_PENDING) ? 0 : rc;
}

/**
 * Handles each packet and start schema change
 *
 */
int osql_process_schemachange(struct ireq *iq, unsigned long long rqid,
                              uuid_t uuid, void *trans, char **pmsg, int msglen,
                              int *flags, int **updCols,
                              blob_buffer_t blobs[MAXBLOBS], int step,
                              struct block_err *err, int *receivedrows)
{
    const uint8_t *p_buf;
    const uint8_t *p_buf_end;
    int rc = 0;
    int type;
    uuidstr_t us;
    char *msg = *pmsg;

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_uuid_rpl_t rpl;
        p_buf = (const uint8_t *)msg;
        p_buf_end = (uint8_t *)p_buf + sizeof(rpl);
        p_buf = osqlcomm_uuid_rpl_type_get(&rpl, p_buf, p_buf_end);
        type = rpl.type;
        comdb2uuidstr(rpl.uuid, us);
        if (comdb2uuidcmp(rpl.uuid, uuid)) {
            uuidstr_t passedus;
            comdb2uuidstr(uuid, passedus);
            logmsg(LOGMSG_FATAL, "uuid mismatch: passed in %s, in packet %s\n",
                   passedus, us);
            abort();
        }
    } else {
        osql_rpl_t rpl;
        p_buf = (const uint8_t *)msg;
        p_buf_end = (uint8_t *)p_buf + sizeof(rpl);
        p_buf = osqlcomm_rpl_type_get(&rpl, p_buf, p_buf_end);
        type = rpl.type;
    }

    if (type != OSQL_SCHEMACHANGE)
        return 0;

    struct schema_change_type *sc = new_schemachange_type();
    p_buf_end = p_buf + msglen;
    p_buf = osqlcomm_schemachange_type_get(sc, p_buf, p_buf_end);

    if (p_buf == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d failed to read schema change object\n",
               __func__, __LINE__);
        reqerrstr(iq, ERR_SC,
                  "internal error: failed to read schema change object");
        return ERR_SC;
    }

    logmsg(LOGMSG_DEBUG, "OSQL_SCHEMACHANGE '%s' tableversion %d\n",
           sc->tablename, sc->usedbtablevers);

    if (gbl_enable_osql_logging) {
        logmsg(LOGMSG_DEBUG, "[%llu %s] OSQL_SCHEMACHANGE %s\n", rqid,
               comdb2uuidstr(uuid, us), sc->tablename);
    }

    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_ASYNC))
        sc->nothrevent = 0;
    else
        sc->nothrevent = 1;
    sc->finalize = 0;
    if (sc->original_master_node[0] != 0 &&
        strcmp(sc->original_master_node, gbl_myhostname))
        sc->resume = 1;

    iq->sc = sc;
    sc->iq = iq;
    sc->is_osql = 1;
    if (sc->db == NULL) {
        sc->db = get_dbtable_by_name(sc->tablename);
    }
    sc->tran = NULL;
    if (sc->db)
        iq->usedb = sc->db;

    if (!timepart_is_timepart(sc->tablename, 1)) {
        rc = start_schema_change_tran(iq, NULL);
        if ((rc != SC_ASYNC && rc != SC_COMMIT_PENDING) ||
            sc->preempted == SC_ACTION_RESUME ||
            sc->alteronly == SC_ALTER_PENDING) {
            iq->sc = NULL;
        } else {
            iq->sc->sc_next = iq->sc_pending;
            iq->sc_pending = iq->sc;
            iq->osql_flags |= OSQL_FLAGS_SCDONE;
        }
    } else {
        timepart_sc_arg_t arg = {0};
        arg.s = sc;
        arg.s->iq = iq;
        rc = timepart_foreach_shard(sc->tablename,
                                    start_schema_change_tran_wrapper, &arg, 0);
    }
    iq->usedb = NULL;

    if (!rc || rc == SC_ASYNC || rc == SC_COMMIT_PENDING)
        return 0;

    return ERR_SC;
}

/* get the table name part of the rpl request
 */
const char *get_tablename_from_rpl(bool is_uuid, const uint8_t *rpl,
                                   int *tableversion)
{
    osql_usedb_t dt;
    const uint8_t *p_buf =
        rpl + (is_uuid ? sizeof(osql_uuid_rpl_t) : sizeof(osql_rpl_t));
    const uint8_t *p_buf_end = p_buf + sizeof(osql_usedb_t);
    const char *tablename;

    tablename = (const char *)osqlcomm_usedb_type_get(&dt, p_buf, p_buf_end);
    if (tableversion && tablename)
        *tableversion = dt.tableversion;
    return tablename;
}

int osql_set_usedb(struct ireq *iq, const char *tablename, int tableversion,
                   int step, struct block_err *err)
{
    if (unlikely(timepart_is_timepart(tablename, 1))) {
        char *newest_shard;
        unsigned long long ver;

        newest_shard = timepart_newest_shard(tablename, &ver);
        if (newest_shard) {
            free(newest_shard);
        } else {
            logmsg(LOGMSG_ERROR, "%s: broken time partition %s\n", __func__,
                   tablename);

            return conv_rc_sql2blkop(iq, step, -1, ERR_NO_SUCH_TABLE, err,
                                     tablename, 0);
        }
    } else {
        if (is_tablename_queue(tablename)) {
            iq->usedb = getqueuebyname(tablename);
        } else {
            iq->usedb = get_dbtable_by_name(tablename);
        }
        if (iq->usedb == NULL) {
            iq->usedb = iq->origdb;
            logmsg(LOGMSG_INFO, "%s: unable to get usedb for table %.*s\n",
                   __func__, (int)strlen(tablename) + 1, tablename);
            return conv_rc_sql2blkop(iq, step, -1, ERR_NO_SUCH_TABLE, err,
                                     tablename, 0);
        }
    }

    // check usedb table version and return verify error if different
    // add/upd/del always follow a usedb opcode
    // thus they will not need to check table version
    if (iq->usedb && iq->usedb->tableversion != tableversion) {
        if (iq->debug)
            reqprintf(iq, "Stale buffer: USEDB version %d vs curr ver %llu\n",
                      tableversion, iq->usedb->tableversion);
        poll(NULL, 0, BDB_ATTR_GET(thedb->bdb_attr, SC_DELAY_VERIFY_ERROR));
        err->errcode = OP_FAILED_VERIFY;
        return ERR_VERIFY;
    }
    return 0;
}

int gbl_selectv_writelock = 0;

/**
 * Handles each packet and calls record.c functions
 * to apply to received row updates
 *
 */

#include <schemachange/sc_global.h>

int osql_process_packet(struct ireq *iq, unsigned long long rqid, uuid_t uuid,
                        void *trans, char **pmsg, int msglen, int *flags,
                        int **updCols, blob_buffer_t blobs[MAXBLOBS], int step,
                        struct block_err *err, int *receivedrows)
{
    const uint8_t *p_buf;
    const uint8_t *p_buf_end;
    int rc = 0;
    struct dbtable *db = (iq->usedb) ? iq->usedb : &thedb->static_table;
    const unsigned char tag_name_ondisk[] = ".ONDISK";
    const size_t tag_name_ondisk_len = 8 /*includes NUL*/;
    int type;
    unsigned long long id;
    char *msg = *pmsg;

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_uuid_rpl_t rpl;
        p_buf = (const uint8_t *)msg;
        p_buf_end = (uint8_t *)p_buf + sizeof(rpl);
        p_buf = osqlcomm_uuid_rpl_type_get(&rpl, p_buf, p_buf_end);
        type = rpl.type;
        id = OSQL_RQID_USE_UUID;
        if (comdb2uuidcmp(rpl.uuid, uuid)) {
            uuidstr_t us;
            uuidstr_t passedus;
            comdb2uuidstr(rpl.uuid, us);
            comdb2uuidstr(uuid, passedus);
            logmsg(LOGMSG_FATAL, "uuid mismatch: passed in %s, in packet %s\n",
                    passedus, us);
            abort();
        }
    } else {
        osql_rpl_t rpl;
        p_buf = (const uint8_t *)msg;
        p_buf_end = (uint8_t *)p_buf + sizeof(rpl);
        p_buf = osqlcomm_rpl_type_get(&rpl, p_buf, p_buf_end);
        type = rpl.type;
        id = rpl.sid;
        comdb2uuid_clear(uuid);
    }

    if (type >= 0 && type < MAX_OSQL_TYPES)
        db->blockosqltypcnt[type]++;
    else
        db->blockosqltypcnt[0]++; /* invalids */

    /* throttle blocproc on master if replication threads backing up */
    if (gbl_toblock_net_throttle && is_write_request(type))
        net_throttle_wait(thedb->handle_sibling);

#if DEBUG_REORDER
    const char *osql_reqtype_str(int type);
    DEBUGMSG("osql_process_packet(): processing %s (%d)\n",
             osql_reqtype_str(type), type);
#endif

    switch (type) {
    case OSQL_DONE:
    case OSQL_DONE_SNAP:
    case OSQL_DONE_STATS: {
        p_buf_end = p_buf + sizeof(osql_done_t);
        osql_done_t dt = {0};

        p_buf = osqlcomm_done_type_get(&dt, p_buf, p_buf_end);

        if (gbl_enable_osql_logging) {
            uuidstr_t us;
            logmsg(LOGMSG_DEBUG, "[%llu %s] OSQL_DONE%s %d %d\n", rqid,
                   comdb2uuidstr(uuid, us),
                   (type == OSQL_DONE)
                       ? ""
                       : (type == OSQL_DONE_SNAP) ? "_SNAP" : "_STATS",
                   dt.nops, dt.rc);
        }

        /* just in case */
        free_blob_buffers(blobs, MAXBLOBS);

        // TODO (NC): Check why iq->sc_pending is not getting set for views
        iq->sc = iq->sc_pending;
        while (iq->sc != NULL) {
            if (strcmp(iq->sc->original_master_node, gbl_myhostname) != 0) {
                return -1;
            }
            if (!iq->sc_locked) {
                /* Lock schema from now on before we finalize any schema changes
                 * and hold on to the lock until the transaction commits/aborts.
                 */
                wrlock_schema_lk();
                iq->sc_locked = 1;
            }
            if (iq->sc->db)
                iq->usedb = iq->sc->db;
            assert(iq->sc->nothrevent);
            rc = finalize_schema_change(iq, iq->sc_tran);
            iq->usedb = NULL;
            if (rc != SC_OK) {
                return ERR_SC;
            }
            if (iq->sc->fastinit && gbl_replicate_local)
                local_replicant_write_clear(iq, trans, iq->sc->db);
            iq->sc = iq->sc->sc_next;
        }

        /* Success: reset the table counters */
        iq->sc = iq->sc_pending;
        while (iq->sc != NULL) {
            sc_set_running(iq, iq->sc, iq->sc->tablename, 0, NULL, 0, 0,
                           __func__, __LINE__);
            iq->sc = iq->sc->sc_next;
        }

        if (iq->sc_pending) {
            create_sqlmaster_records(iq->sc_tran);
            create_sqlite_master();
        }

        // TODO Notify all bpfunc of success

        /* dt.nops carries the possible conversion error index */
        rc = conv_rc_sql2blkop(iq, step, -1, dt.rc, err, NULL, dt.nops);

        if (type == OSQL_DONE_SNAP) {
            if (!gbl_disable_cnonce_blkseq)
                assert(IQ_HAS_SNAPINFO(iq)); // was assigned in fast pass

            snap_uid_t snap_info;
            p_buf_end = (const uint8_t *)msg + msglen;

            /* Initial query effects for a transaction from replicants are
             * received by the master here. With, gbl_master_sends_query_effect
             * enabled, master zeros all the non-select counts and repopulates
             * them as it plows through the transaction's osql stream, and
             * finally sends them to the replicant as part of 'done'.
             */
            p_buf = snap_uid_get(&snap_info, p_buf, p_buf_end);

            /* The following assert could fail when/if master modifies the
             * write query effects.
             */
            if (!gbl_master_sends_query_effects && IQ_HAS_SNAPINFO(iq)) {
                assert(
                    !memcmp(&snap_info, IQ_SNAPINFO(iq), sizeof(snap_uid_t)));
            }
        }

        /* p_buf is pointing at client_query_stats if there is one */
        if (type == OSQL_DONE_STATS) { /* Never set anywhere. */
            dump_client_query_stats_packed(iq->dbglog_file, p_buf);
        }

        if (gbl_toblock_random_deadlock_trans && (rand() % 100) == 0) {
            rc = RC_INTERNAL_RETRY;
        }

        return rc ? rc : OSQL_RC_DONE; /* signal caller done processing this
                                          request */
    }
    case OSQL_USEDB: {
        osql_usedb_t dt = {0};
        p_buf_end = (uint8_t *)p_buf + sizeof(osql_usedb_t);
        const char *tablename;

        /* IDEA: don't store the usedb in the defered_table, rather right before
         * loading a new usedb, process the curret one,
         * this way tmptbl key is 8 bytes smaller
         *
        if (gbl_reorder_on) {
            process_defered_table(iq, ...);
        }
        */

        tablename =
            (const char *)osqlcomm_usedb_type_get(&dt, p_buf, p_buf_end);

        // get table lock
        rc = bdb_lock_tablename_read(thedb->bdb_env, tablename, trans);
        if (rc == BDBERR_DEADLOCK) {
            if (iq->debug)
                reqprintf(iq, "LOCK TABLE READ DEADLOCK");
            return RC_INTERNAL_RETRY;
        } else if (rc) {
            if (iq->debug)
                reqprintf(iq, "LOCK TABLE READ ERROR: %d", rc);
            return ERR_INTERNAL;
        }

        if (gbl_enable_osql_logging) {
            uuidstr_t us = {0};
            logmsg(LOGMSG_DEBUG, "[%llu %s] OSQL_USEDB %*.s\n", rqid,
                   comdb2uuidstr(uuid, us), dt.tablenamelen, tablename);
        }

        rc = osql_set_usedb(iq, tablename, dt.tableversion, step, err);
        if (rc) {
            return rc;
        }
    } break;
    case OSQL_DBQ_CONSUME: {
        genid_t *genid = (genid_t *)p_buf;
        if ((rc = dbq_consume_genid(iq, trans, 0, *genid)) != 0) {
            logmsg(LOGMSG_ERROR, "%s: dbq_consume rc:%d\n", __func__, rc);
            return rc;
        }
        break;
    }
    case OSQL_DELREC:
    case OSQL_DELETE: {
        osql_del_t dt;
        int recv_dk = (type == OSQL_DELETE);
        if (recv_dk)
            p_buf_end = p_buf + sizeof(osql_del_t);
        else
            p_buf_end = p_buf + sizeof(osql_del_t) - sizeof(unsigned long long);

        p_buf =
            (uint8_t *)osqlcomm_del_type_get(&dt, p_buf, p_buf_end, recv_dk);

        if (!recv_dk)
            dt.dk = -1ULL;

        if (gbl_enable_osql_logging) {
            unsigned long long lclgenid = bdb_genid_to_host_order(dt.genid);
            uuidstr_t us;
            logmsg(LOGMSG_DEBUG, "[%llu %s] %s %llx (2:%lld)\n", rqid,
                   comdb2uuidstr(uuid, us),
                   recv_dk ? "OSQL_DELETE" : "OSQL_DELREC", lclgenid, lclgenid);
        }

        /* has this genid been written by this transaction? */
        if (iq->vfy_genid_track) {
            unsigned long long *g = hash_find(iq->vfy_genid_hash, &dt.genid);

            /* punt immediately with uncommitable txn */
            if (g) {
                rc = ERR_UNCOMMITABLE_TXN;
                reqerrstr(iq, COMDB2_DEL_RC_INVL_KEY,
                          "uncommitable txn on del genid=%llx rc=%d",
                          bdb_genid_to_host_order(dt.genid), rc);
                err->blockop_num = step;
                err->ixnum = 0;
                err->errcode = ERR_UNCOMMITABLE_TXN;
                return rc;
            }

            /* allocate it */
            g = pool_getablk(iq->vfy_genid_pool);
            memcpy(g, &dt.genid, sizeof(unsigned long long));
            hash_add(iq->vfy_genid_hash, g);
        }

        int locflags = RECFLAGS_DONT_LOCK_TBL;

        rc = is_tablename_queue(iq->usedb->tablename)
            ? dbq_consume_genid(iq, trans, 0, dt.genid)
            : del_record(iq, trans, NULL, 0, dt.genid, dt.dk, &err->errcode, &err->ixnum, BLOCK2_DELKL, locflags);

        if (iq->idxInsert || iq->idxDelete) {
            free_cached_idx(iq->idxInsert);
            free_cached_idx(iq->idxDelete);
            free(iq->idxInsert);
            free(iq->idxDelete);
            iq->idxInsert = iq->idxDelete = NULL;
        }

        if (rc != 0) {
            if (rc != RC_INTERNAL_RETRY) {
                errstat_cat_strf(&iq->errstat,
                                 " unable to delete genid =%llx rc=%d",
                                 bdb_genid_to_host_order(dt.genid), rc);
            }

            return rc; /*this is blkproc rc */
        }

        if (likely(gbl_master_sends_query_effects) && IQ_HAS_SNAPINFO(iq)) {
            IQ_SNAPINFO(iq)->effects.num_deleted++;
        }
        (*receivedrows)++;
    } break;
    case OSQL_UPDSTAT: {
        /* this opcode was used to insert/update into btree new stat1/2/4 record
         * but since we changed the way we backup stats (used to be in llmeta)
         * this opcode is only used to reload stats now
         */
        iq->osql_flags |= OSQL_FLAGS_ANALYZE;
    } break;
    case OSQL_INSREC:
    case OSQL_INSERT: {
        osql_ins_t dt;
        unsigned char *pData = NULL;
        int rrn = 0;
        unsigned long long newgenid = 0;
        int is_legacy = (type == OSQL_INSREC);

        const uint8_t *p_buf_end;
        if (is_legacy)
            p_buf_end = p_buf + OSQLCOMM_INS_LEGACY_TYPE_LEN;
        else
            p_buf_end = p_buf + OSQLCOMM_INS_TYPE_LEN;

        pData =
            (uint8_t *)osqlcomm_ins_type_get(&dt, p_buf, p_buf_end, is_legacy);

        if (gbl_enable_osql_logging) {
            int jj = 0;
            uuidstr_t us;
            logmsg(LOGMSG_DEBUG, "[%llu %s] %s [\n", rqid,
                   comdb2uuidstr(uuid, us),
                   is_legacy ? "OSQL_INSREC" : "OSQL_INSERT");
            for (jj = 0; jj < dt.nData; jj++)
                logmsg(LOGMSG_DEBUG, "%02x", pData[jj]);

            logmsg(LOGMSG_DEBUG, "\n] -> ");
        }

        int addflags = RECFLAGS_DYNSCHEMA_NULLS_ONLY | RECFLAGS_DONT_LOCK_TBL;
        if (!iq->sorese->is_delayed && iq->usedb->n_constraints == 0 &&
            gbl_goslow == 0) {
            addflags |= RECFLAGS_NO_CONSTRAINTS;
        } else {
            iq->sorese->is_delayed = true;
        }

        rc = add_record(iq, trans, tag_name_ondisk,
                        tag_name_ondisk + tag_name_ondisk_len, /*tag*/
                        pData, pData + dt.nData,               /*dta*/
                        NULL,            /*nulls, no need as no
                                           ctag2stag is called */
                        blobs, MAXBLOBS, /*blobs*/
                        &err->errcode, &err->ixnum, &rrn, &newgenid, /*new id*/
                        dt.dk, BLOCK2_ADDKL, step, addflags,
                        dt.upsert_flags); /* do I need this?*/
        free_blob_buffers(blobs, MAXBLOBS);
        if (iq->idxInsert || iq->idxDelete) {
            free_cached_idx(iq->idxInsert);
            free_cached_idx(iq->idxDelete);
            free(iq->idxInsert);
            free(iq->idxDelete);
            iq->idxInsert = iq->idxDelete = NULL;
        }

        if (gbl_enable_osql_logging) {
            unsigned long long lclgenid = bdb_genid_to_host_order(newgenid);
            logmsg(LOGMSG_DEBUG, " %llx (%d:%lld)\n", lclgenid, rrn, lclgenid);
        }

        if (rc != 0) {
            if (err->errcode == OP_FAILED_UNIQ) {
                int upsert_idx = dt.upsert_flags >> 8;
                if ((dt.upsert_flags & OSQL_FORCE_VERIFY) != 0) {
                    err->errcode = OP_FAILED_VERIFY;
                    rc = ERR_VERIFY;
                } else if ((rc == IX_DUP) &&
                           ((dt.upsert_flags & OSQL_IGNORE_FAILURE) != 0) &&
                           ((upsert_idx == MAXINDEX + 1) ||
                            (upsert_idx == err->ixnum))) {
                    /* We're asked to ignore DUPs, no insert took place.*/
                    return 0;
                } else {
                    /* this can happen if we're skipping delayed key adds */
                    reqerrstr(iq, COMDB2_CSTRT_RC_DUP, "add key constraint "
                                                       "duplicate key '%s' on "
                                                       "table '%s' index %d",
                              get_keynm_from_db_idx(iq->usedb, err->ixnum),
                              iq->usedb->tablename, err->ixnum);
                }
            } else if (rc != RC_INTERNAL_RETRY) {
                errstat_cat_strf(&iq->errstat, " unable to add record rc = %d",
                                 rc);
            }

            if (gbl_enable_osql_logging)
                logmsg(LOGMSG_DEBUG,
                       "Added new record failed, rrn = %d, newgenid=%llx\n",
                       rrn, bdb_genid_to_host_order(newgenid));

            return rc; /*this is blkproc rc */
        } else {
            if (gbl_enable_osql_logging)
                logmsg(LOGMSG_DEBUG,
                       "Added new record rrn = %d, newgenid=%llx\n", rrn,
                       bdb_genid_to_host_order(newgenid));
        }
#if DEBUG_REORDER
        logmsg(LOGMSG_DEBUG,
               "REORDER: Added new record rrn = %d, newgenid=%llx\n", rrn,
               bdb_genid_to_host_order(newgenid));
#endif

        if (likely(gbl_master_sends_query_effects) && IQ_HAS_SNAPINFO(iq)) {
            IQ_SNAPINFO(iq)->effects.num_inserted++;
        }
        (*receivedrows)++;
    } break;
    case OSQL_STARTGEN: {
        osql_startgen_t dt = {0};
        uint32_t cur_gen;
        const uint8_t *p_buf_end;
        p_buf_end = p_buf + sizeof(osql_startgen_t);
        osqlcomm_startgen_type_get(&dt, p_buf, p_buf_end);
        cur_gen = bdb_get_rep_gen(thedb->bdb_env);
        if (cur_gen != dt.start_gen) {
            err->errcode = OP_FAILED_VERIFY;
            uuidstr_t us;
            logmsg(LOGMSG_DEBUG,
                   "[%llx %s] Startgen check failed, start_gen "
                   "%u, cur_gen %u\n",
                   id, comdb2uuidstr(uuid, us), dt.start_gen, cur_gen);
            return ERR_VERIFY;
        }
    } break;

    case OSQL_UPDREC:
    case OSQL_UPDATE: {
        osql_upd_t dt;
        const uint8_t *p_buf_end;
        unsigned char *pData;
        int rrn = 2;
        unsigned long long genid;
        int recv_dk = (type == OSQL_UPDATE);
        if (recv_dk)
            p_buf_end = p_buf + sizeof(osql_upd_t);
        else
            p_buf_end = p_buf + sizeof(osql_upd_t) -
                        sizeof(unsigned long long) - sizeof(unsigned long long);

        pData =
            (uint8_t *)osqlcomm_upd_type_get(&dt, p_buf, p_buf_end, recv_dk);
        if (!recv_dk) {
            dt.ins_keys = -1ULL;
            dt.del_keys = -1ULL;
        }
        genid = dt.genid;

        if (gbl_enable_osql_logging) {
            int jj = 0;
            uuidstr_t us;
            logmsg(LOGMSG_DEBUG,
                   "[%llu %s] OSQL_UPDREC rrn = %d, genid = %llx[\n", rqid,
                   comdb2uuidstr(uuid, us), rrn,
                   bdb_genid_to_host_order(genid));
            for (jj = 0; jj < dt.nData; jj++)
                logmsg(LOGMSG_DEBUG, "%02x", pData[jj]);
            logmsg(LOGMSG_DEBUG, "\n] -> ");
        }

        /* has this genid been written by this transaction? */
        if (iq->vfy_genid_track) {
            unsigned long long *g = hash_find(iq->vfy_genid_hash, &genid);

            /* punt immediately with uncommitable txn */
            if (g) {
                rc = ERR_UNCOMMITABLE_TXN;
                reqerrstr(iq, COMDB2_UPD_RC_INVL_KEY,
                          "uncommitable txn on upd genid=%llx rc=%d",
                          bdb_genid_to_host_order(genid), rc);
                err->blockop_num = step;
                err->ixnum = 0;
                err->errcode = ERR_UNCOMMITABLE_TXN;
                return rc;
            }

            /* allocate it */
            g = pool_getablk(iq->vfy_genid_pool);
            memcpy(g, &genid, sizeof(unsigned long long));
            hash_add(iq->vfy_genid_hash, g);
        }

#ifndef NDEBUG
        /* Sanity check the osql blob optimization. */
        if (*flags & OSQL_PROCESS_FLAGS_BLOB_OPTIMIZATION) {
            int ncols;

            assert(*updCols != NULL);
            ncols = (*updCols)[0];

            /* Make sure this is sane before sending to upd_record. */
            for (int ii = 0; ii < MAXBLOBS; ii++) {
                if (-2 == blobs[ii].length) {
                    int idx = get_schema_blob_field_idx(iq->usedb->tablename,
                                                        ".ONDISK", ii);
                    assert(idx < ncols);
                    assert(-1 == (*updCols)[idx + 1]);
                }
            }
        }
#endif

        int locflags =
            RECFLAGS_DYNSCHEMA_NULLS_ONLY | RECFLAGS_DONT_LOCK_TBL |
            RECFLAGS_DONT_SKIP_BLOBS; /* because we only receive info about
                                        blobs that should exist in the new
                                        record, override the update
                                        function's default behaviour and
                                        have it erase any blobs that havent been
                                        collected. */

        rc = upd_record(iq, trans, NULL, rrn, genid, tag_name_ondisk,
                        tag_name_ondisk + tag_name_ondisk_len, /*tag*/
                        pData, pData + dt.nData,               /* rec */
                        NULL, NULL,                            /* vrec */
                        NULL, /*nulls, no need as no
                                ctag2stag is called */
                        *updCols, blobs, MAXBLOBS, &genid, dt.ins_keys,
                        dt.del_keys, &err->errcode, &err->ixnum, BLOCK2_UPDKL,
                        step, locflags);

        free_blob_buffers(blobs, MAXBLOBS);
        if (iq->idxInsert || iq->idxDelete) {
            free_cached_idx(iq->idxInsert);
            free_cached_idx(iq->idxDelete);
            free(iq->idxInsert);
            free(iq->idxDelete);
            iq->idxInsert = iq->idxDelete = NULL;
        }

        if (*updCols) {
            free(*updCols);
            *updCols = NULL;
            /* reset blob optimization, just in case; should
               be enabled by a new updCols
             */
            *flags = (*flags) & (!OSQL_PROCESS_FLAGS_BLOB_OPTIMIZATION);
        }

        if (gbl_enable_osql_logging) {
            unsigned long long lclgenid = bdb_genid_to_host_order(genid);
            logmsg(LOGMSG_DEBUG, " %llx (%d:%lld)\n", lclgenid, rrn, lclgenid);
        }

        if (rc != 0) {
            if (rc != RC_INTERNAL_RETRY) {
                errstat_cat_strf(&iq->errstat,
                                 " unable to update record rc = %d", rc);
            }
            if (gbl_enable_osql_logging)
                logmsg(LOGMSG_DEBUG,
                       "Updated record failed, rrn = %d, genid=%llx\n", rrn,
                       bdb_genid_to_host_order(genid));
            return rc;
        } else if (gbl_enable_osql_logging)
            logmsg(LOGMSG_DEBUG, "Updated record rrn = %d, genid=%llx\n", rrn,
                   bdb_genid_to_host_order(genid));

        if (likely(gbl_master_sends_query_effects) && IQ_HAS_SNAPINFO(iq)) {
            IQ_SNAPINFO(iq)->effects.num_updated++;
        }
        (*receivedrows)++;
    } break;
    case OSQL_UPDCOLS: {
        osql_updcols_t dt = {0};
        const uint8_t *p_buf_end = p_buf + sizeof(osql_updcols_t);
        int i;

        p_buf = (uint8_t *)osqlcomm_updcols_type_get(&dt, p_buf, p_buf_end);

        if (gbl_enable_osql_logging) {
            int jj;
            uuidstr_t us;
            logmsg(LOGMSG_DEBUG, "[%llu %s] OSQL_UPDCOLS %d [\n", rqid,
                   comdb2uuidstr(uuid, us), dt.ncols);
            for (jj = 0; jj < dt.ncols; jj++)
                logmsg(LOGMSG_DEBUG, "%d ", dt.clist[jj]);
            logmsg(LOGMSG_DEBUG, "\n");
        }

        if (NULL != *updCols) {
            logmsg(LOGMSG_WARN, "%s recieved multiple update columns!  (ignoring duplicates)\n",
                __func__);
        } else {
            int sz = sizeof(int) * (dt.ncols + 1);
            *updCols = (int *)malloc(sz);

            /* reset to the end of the buffer */
            p_buf_end = p_buf + sz;
            if (!*updCols) {
                logmsg(LOGMSG_ERROR, "%s failed to allocate memory for an upd_cols "
                                "request, size %d\n",
                        __func__, sz);
                return conv_rc_sql2blkop(iq, step, -1, ERR_INTERNAL, err, NULL,
                                         0);
            }
            (*updCols)[0] = dt.ncols;
            for (i = 0; i < dt.ncols; i++) {
                p_buf = (uint8_t *)buf_get(&(*updCols)[i + 1], sizeof(int),
                                           p_buf, p_buf_end);
            }
        }
    } break;
    case OSQL_SERIAL:
    case OSQL_SELECTV: {
        uint8_t *p_buf;
        if (rqid == OSQL_RQID_USE_UUID)
            p_buf = (uint8_t *)&((osql_serial_uuid_rpl_t *)msg)->dt;
        else
            p_buf = (uint8_t *)&((osql_serial_rpl_t *)msg)->dt;
        uint8_t *p_buf_end = p_buf + sizeof(osql_serial_t);
        osql_serial_t dt = {0};
        CurRangeArr *arr = malloc(sizeof(CurRangeArr));
        currangearr_init(arr);

        p_buf = (uint8_t *)osqlcomm_serial_type_get(&dt, p_buf, p_buf_end);
        arr->file = dt.file;
        arr->offset = dt.offset;

        p_buf_end = p_buf + dt.buf_size;

        p_buf = (uint8_t *)serial_readset_get(arr, dt.buf_size, dt.arr_size,
                                              p_buf, p_buf_end);

        /* build up range hash */
        currangearr_build_hash(arr);
        if (type == OSQL_SERIAL) {
            iq->arr = arr;

        }
        else {
            iq->selectv_arr = arr;
        }

        if (gbl_enable_osql_logging) {
            uuidstr_t us;
            logmsg(LOGMSG_DEBUG, "[%llu %s] %s %d %d_%d_%d\n", rqid,
                   comdb2uuidstr(uuid, us),
                   (type == OSQL_SERIAL) ? "OSQL_SERIAL" : "OSQL_SELECTV",
                   dt.buf_size, dt.arr_size, dt.file, dt.offset);
        }
    } break;
    case OSQL_DELIDX:
    case OSQL_INSIDX: {
        osql_index_t dt = {0};
        unsigned char *pData = NULL;
        int isDelete = (type == OSQL_DELIDX);
        const uint8_t *p_buf_end;
        uint8_t *pIdx = NULL;

        p_buf_end = p_buf + sizeof(osql_index_t);

        pData = (uint8_t *)osqlcomm_index_type_get(&dt, p_buf, p_buf_end);
        if (gbl_enable_osql_logging) {
            int jj = 0;
            uuidstr_t us;
            logmsg(LOGMSG_DEBUG, "[%llu %s] %s ixnum %d [\n", rqid,
                   comdb2uuidstr(uuid, us),
                   isDelete ? "OSQL_DELIDX" : "OSQL_INSIDX", dt.ixnum);
            for (jj = 0; jj < dt.nData; jj++)
                logmsg(LOGMSG_DEBUG, "%02x", pData[jj]);

            logmsg(LOGMSG_DEBUG, "]\n");
        }
        if (!iq->idxInsert && !iq->idxDelete) {
            iq->idxInsert = calloc(MAXINDEX, sizeof(uint8_t *));
            iq->idxDelete = calloc(MAXINDEX, sizeof(uint8_t *));
            if (!iq->idxInsert || !iq->idxDelete) {
                logmsg(LOGMSG_ERROR, "%s failed to allocated indexes\n", __func__);
                return ERR_INTERNAL;
            }
        }
        if (isDelete)
            iq->idxDelete[dt.ixnum] = pIdx = malloc(dt.nData);
        else
            iq->idxInsert[dt.ixnum] = pIdx = malloc(dt.nData);
        if (pIdx == NULL) {
            logmsg(LOGMSG_ERROR, "%s failed to allocated indexes data, len %d\n",
                    __func__, dt.nData);
            return ERR_INTERNAL;
        }
        memcpy(pIdx, pData, dt.nData);
    } break;
    case OSQL_QBLOB: {
        osql_qblob_t dt = {0};
        const uint8_t *p_buf_end = p_buf + sizeof(osql_qblob_t),
                      *blob = osqlcomm_qblob_type_get(&dt, p_buf, p_buf_end);
        int odhready = (dt.id & OSQL_BLOB_ODH_BIT);

        dt.id &= ~OSQL_BLOB_ODH_BIT;

        if (gbl_enable_osql_logging) {
            int jj = 0;
            uuidstr_t us;
            logmsg(LOGMSG_DEBUG, "[%llu %s] OSQL_QBLOB %d %d [\n", rqid,
                   comdb2uuidstr(uuid, us), dt.id, dt.bloblen);
            for (jj = 0; jj < dt.bloblen; jj++)
                logmsg(LOGMSG_DEBUG, "%02x", blob[jj]);
            logmsg(LOGMSG_DEBUG, "\n]");
        }

        if (blobs[dt.id].exists) {
            logmsg(LOGMSG_ERROR, 
                    "%s received a duplicated blob id %d! (ignoring duplicates)\n",
                    __func__, dt.id);
        }
        /* Blob isn't used so we sent a short token rather than the entire blob.
           */
        else if (dt.bloblen == -2) {
            *flags |= OSQL_PROCESS_FLAGS_BLOB_OPTIMIZATION;
            blobs[dt.id].length = dt.bloblen;
            blobs[dt.id].exists = 1;
            blobs[dt.id].data = NULL;
            blobs[dt.id].collected = 1;
            blobs[dt.id].javasp_bytearray = NULL;
        } else {
            if (odhready)
                blobs[dt.id].odhind = (dt.id | OSQL_BLOB_ODH_BIT);
            blobs[dt.id].length = dt.bloblen;

            if (dt.bloblen >= 0) {
                blobs[dt.id].exists = 1;
                if (dt.bloblen > 0) {
                    blobs[dt.id].qblob = msg;
                    blobs[dt.id].data = (char *)blob;
                    blobs[dt.id].collected = dt.bloblen;
                    /* Take ownership.
                       It will be freed in free_blob_buffers(). */
                    *pmsg = NULL;
                } else {
                    blobs[dt.id].collected = 1;
                }

            } else {
                /* null blob */
                blobs[dt.id].exists = 0;
                blobs[dt.id].data = NULL;
                blobs[dt.id].length = 0;
                blobs[dt.id].collected = 1;
                blobs[dt.id].javasp_bytearray = NULL;
            }
        }
    } break;
    case OSQL_DBGLOG: {
        osql_dbglog_t dbglog = {0};
        const uint8_t *p_buf = (const uint8_t *)msg;
        const uint8_t *p_buf_end = p_buf + sizeof(osql_dbglog_t);

        osqlcomm_dbglog_type_get(&dbglog, p_buf, p_buf_end);

        if (!iq->dbglog_file)
            iq->dbglog_file = open_dbglog_file(dbglog.dbglog_cookie);

        dbglog_init_write_counters(iq);
        iq->queryid = dbglog.queryid;
    } break;
    case OSQL_RECGENID: {
        osql_recgenid_t dt = {0};
        int bdberr = 0;
        unsigned long long lclgenid;

        const uint8_t *p_buf_end = p_buf + sizeof(osql_recgenid_t);

        osqlcomm_recgenid_type_get(&dt, p_buf, p_buf_end);

        lclgenid = bdb_genid_to_host_order(dt.genid);

        if (gbl_selectv_writelock)
            rc = ix_check_genid_wl(iq, trans, dt.genid, &bdberr);
        else
            rc = ix_check_genid(iq, trans, dt.genid, &bdberr);

        if (gbl_enable_osql_logging) {
            uuidstr_t us;
            logmsg(LOGMSG_DEBUG,
                   "[%llu %s] OSQL_RECGENID %llx (%llu) -> rc = %d\n", rqid,
                   comdb2uuidstr(uuid, us), lclgenid, lclgenid, rc);
        }

        /* was error? verify error ? */
        if (rc != 1) {
            if ((bdberr == 0 && rc == 0) ||
                (bdberr == IX_PASTEOF && rc == -1)) {
                /* verify error */
                err->ixnum = -1; /* data */
                err->errcode = ERR_CONSTR;
                /* we need to make error trace generic so we can test it;
                   keep the details in ctrace file */
                ctrace("constraints error, no genid %llx (%llu)\n", lclgenid,
                       lclgenid);
                reqerrstr(iq, COMDB2_CSTRT_RC_INVL_REC,
                          "constraints error, no genid");
                return ERR_CONSTR;
            }

            if (bdberr != RC_INTERNAL_RETRY) {
                reqerrstr(iq, COMDB2_DEL_RC_INVL_KEY,
                          "unable to find genid =%llx rc=%d", lclgenid, bdberr);
            }

            return bdberr; /*this is blkproc rc */
        }
    } break;
    case OSQL_SCHEMACHANGE: {
        /* handled in osql_process_schemachange */
        return 0;
    } break;
    case OSQL_BPFUNC: {
        uint8_t *p_buf_end = (uint8_t *)msg + sizeof(osql_bpfunc_t) + msglen;
        osql_bpfunc_t *rpl = NULL;

        const uint8_t *n_p_buf =
            osqlcomm_bpfunc_type_get(&rpl, p_buf, p_buf_end);

        if (n_p_buf && rpl) {
            bpfunc_lstnode_t *lnode;
            bpfunc_t *func;
            bpfunc_info info;

            info.iq = iq;
            int rst = bpfunc_prepare(&func, rpl->data_len, rpl->data, &info);
            if (!rst)
                rc = func->exec(trans, func, &iq->errstat);

            if (rst || rc) {
                free_bpfunc(func);
            } else {
                lnode = (bpfunc_lstnode_t *)malloc(sizeof(bpfunc_lstnode_t));
                assert(lnode);
                lnode->func = func;
                listc_abl(&iq->bpfunc_lst, lnode);
                if (gbl_enable_osql_logging) {
                    uuidstr_t us;
                    logmsg(LOGMSG_DEBUG, "[%llu %s] OSQL_BPFUNC type %d\n",
                           rqid, comdb2uuidstr(uuid, us), func->arg->type);
                }
            }
        } else {
            logmsg(LOGMSG_ERROR, "Cannot read bpfunc message");
            rc = -1;
        }

        free(rpl);
        return rc;
    } break;
    default: {
        uuidstr_t us;
        logmsg(LOGMSG_ERROR, "%s [%llu %s] RECEIVED AN UNKNOWN OFF OPCODE %u, "
                        "failing the transaction\n",
                __func__, rqid, comdb2uuidstr(uuid, us), type);

        return conv_rc_sql2blkop(iq, step, -1, ERR_BADREQ, err, NULL, 0);
    }
    }

    return 0;
}

void signal_replicant_error(const char *host, unsigned long long rqid,
                            uuid_t uuid, int rc, const char *msg)
{
    struct errstat generr = {0};
    errstat_set_rcstrf(&generr, rc, msg);
    int rc2 = osql_comm_signal_sqlthr_rc(host, rqid, uuid, 0, &generr, 0, rc);
    if (rc2) {
        uuidstr_t us;
        comdb2uuidstr(uuid, us);
        logmsg(LOGMSG_ERROR,
               "%s: failed to signaled rqid=[%llx %s] host=%s of "
               "error to create bplog\n",
               __func__, rqid, us, host);
    }
}

static int sorese_rcvreq(char *fromhost, void *dtap, int dtalen, int type,
                         int nettype)
{
    osql_sess_t *sess = NULL;
    unsigned long long rqid;
    uuid_t uuid;
    char *sql;
    int sqllen;
    char *tzname;
    int flags = 0;
    int send_rc = 1;
    const char *errmsg = "";
    int rc = 0;

    /* grab the request */
    uint8_t *p_req_buf = dtap;
    const uint8_t *p_req_buf_end = p_req_buf + dtalen;
    if (osql_nettype_is_uuid(nettype)) {
        osql_uuid_req_t ureq;
        sql =
            (char *)osqlcomm_req_uuid_type_get(&ureq, p_req_buf, p_req_buf_end);
        rqid = OSQL_RQID_USE_UUID;
        comdb2uuidcpy(uuid, ureq.uuid);
        flags = ureq.flags;
        tzname = ureq.tzname;
        sqllen = ureq.sqlqlen;
    } else {
        osql_req_t req;
        sql = (char *)osqlcomm_req_type_get(&req, p_req_buf, p_req_buf_end);
        rqid = req.rqid;
        comdb2uuid_clear(uuid);
        flags = req.flags;
        tzname = req.tzname;
        sqllen = req.sqlqlen;
    }

    /* create the request */
    sess = osql_sess_create(sql, sqllen, tzname, type, rqid, uuid, fromhost,
                            flags & OSQL_FLAGS_REORDER_ON);
    if (!sess) {
        logmsg(LOGMSG_ERROR, "%s unable to create new session\n", __func__);
        errmsg = "unable to create new session";
        rc = -1;
        goto done;
    }

    /* make this visible to the world */
    rc = osql_repository_add(sess);
    if (rc) {
        /* if the session is dispatched, don't send
        back a retry return code, since the block processor
        thread will send one */
        if (rc == -2)
            send_rc = 0;
        goto done;
    }

#if DEBUG_REORDER
    logmsg(LOGMSG_DEBUG,
           "REORDER: created sess %p, with sess->is_reorder_on %d\n", sess,
           sess->is_reorder_on);
#endif

    /* for socksql, is it a retry that needs to be checked for self-deadlock? */
    if ((type == OSQL_SOCK_REQ || type == OSQL_SOCK_REQ_COST) &&
        (flags & OSQL_FLAGS_CHECK_SELFLOCK)) {
        /* just make sure we are above the threshold */
        sess->verify_retries += gbl_osql_verify_ext_chk;
    }

done:
    if (!rc) {
        /*
           successful, let the session loose
           It is possible that we are clearing sessions due to
           master being rtcpu-ed, and it will wait for the session
           clients to disappear before it will wipe out the session
         */

        rc = osql_repository_put(sess);
        if (!rc)
            return 0;
        /* if put noticed a termination flag, fall-through */
        send_rc = 1;
    }

    /* notify the sql thread there will be no response! */
    if (send_rc) {
        signal_replicant_error(fromhost, rqid, uuid, ERR_NOMASTER, errmsg);
    }
    if (sess) {
        /* session start with 1 client, this reader thread */
        osql_sess_remclient(sess);
        osql_sess_close(&sess, false);
    }

    return rc;
}

/* transaction result */
static void net_sorese_signal(void *hndl, void *uptr, char *fromhost,
                              int usertype, void *dtap, int dtalen,
                              uint8_t is_tcp)
{
    osql_done_t done = {0};
    struct errstat *xerr;
    struct query_effects effects;
    struct query_effects *p_effects = NULL;
    uint8_t *p_buf = (uint8_t *)dtap;
    uint8_t *p_buf_end = p_buf + dtalen;
    uuid_t uuid;
    unsigned long long rqid;
    int type;

    if (osql_nettype_is_uuid(usertype)) {
        osql_uuid_rpl_t uuid_hdr;
        /* unpack */
        p_buf =
            (uint8_t *)osqlcomm_uuid_rpl_type_get(&uuid_hdr, p_buf, p_buf_end);
        comdb2uuidcpy(uuid, uuid_hdr.uuid);
        rqid = OSQL_RQID_USE_UUID;
        type = uuid_hdr.type;
    } else {
        osql_rpl_t hdr;
        p_buf = (uint8_t *)osqlcomm_rpl_type_get(&hdr, p_buf, p_buf_end);
        comdb2uuid_clear(uuid);
        type = hdr.type;
        rqid = hdr.sid;
    }

    osqlcomm_done_type_get(&done, p_buf, p_buf_end);

    /* This also receives the query effects from master. */
    if (osql_comm_is_done(NULL, type, dtap, dtalen, rqid == OSQL_RQID_USE_UUID,
                          &xerr, &effects) == 1) {
        if (type == OSQL_DONE_WITH_EFFECTS) {
            p_effects = &effects;
        }

#if 0
      printf("Done rqid=%llu tmp=%llu\n", hdr->sid, osql_log_time());
#endif
        if (xerr) {
            struct errstat errstat;
            uint8_t *p_buf = (uint8_t *)xerr;
            uint8_t *p_buf_end = (p_buf + sizeof(struct errstat));
            osqlcomm_errstat_type_get(&errstat, p_buf, p_buf_end);

            osql_chkboard_sqlsession_rc(rqid, uuid, 0, NULL, &errstat, NULL);
        } else {
            osql_chkboard_sqlsession_rc(rqid, uuid, done.nops, NULL, NULL,
                                        p_effects);
        }

    } else {
        logmsg(LOGMSG_ERROR, "%s: wrong sqlthr signal %d\n", __func__, type);
        return;
    }
}

static int netrpl2req(int netrpltype)
{
    switch (netrpltype) {
    case NET_OSQL_SOCK_RPL:
    case NET_OSQL_SOCK_RPL_UUID:
        return OSQL_SOCK_REQ;

    case NET_OSQL_RECOM_RPL:
    case NET_OSQL_RECOM_RPL_UUID:
        return OSQL_RECOM_REQ;

    case NET_OSQL_SNAPISOL_RPL:
    case NET_OSQL_SNAPISOL_RPL_UUID:
        return OSQL_SNAPISOL_REQ;

    case NET_OSQL_SERIAL_RPL:
    case NET_OSQL_SERIAL_RPL_UUID:
        return OSQL_SERIAL_REQ;
    }

    logmsg(LOGMSG_ERROR, "%s: unknown request type %d\n", __func__, netrpltype);
    {
        int once = 0;
        if (!once) {
            cheap_stack_trace();
            once = 1;
        }
    }

    return 0; /* convenience to use this directly for indexing */
}

static void net_osql_rcv_echo_ping(void *hndl, void *uptr, char *fromhost,
                                   int usertype, void *dtap, int dtalen,
                                   uint8_t is_tcp)
{
    uint8_t *p_buf = dtap;
    uint8_t *p_buf_end = p_buf + dtalen;
    osql_echo_t msg;
    int rc = 0;

#if 0 
   printf("%s\n", __func__);
#endif
    if (dtalen != sizeof(osql_echo_t)) {
        logmsg(LOGMSG_ERROR,
               "Received malformed echo packet! size %d, should be %zu\n",
               dtalen, sizeof(osql_echo_t));
        return;
    }

    if (!(osqlcomm_echo_type_get(&msg, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: Error unpacking echo request\n", __func__);
        return;
    }

    msg.rcv = osql_log_time();

    if (!(osqlcomm_echo_type_put(&msg, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: Error packing echo request\n", __func__);
        return;
    }

    rc = offload_net_send(fromhost, NET_OSQL_ECHO_PONG, dtap, dtalen, 1, NULL,
                          0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Failure to pong!\n");
        return;
    }
}

static void net_osql_rcv_echo_pong(void *hndl, void *uptr, char *fromhost,
                                   int usertype, void *dtap, int dtalen,
                                   uint8_t is_tcp)
{
    uint8_t *p_buf = dtap;
    uint8_t *p_buf_end = p_buf + dtalen;
    osql_echo_t msg;

    if (!(osqlcomm_echo_type_get(&msg, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: Error unpacking echo request\n", __func__);
        return;
    }

    Pthread_mutex_lock(&msgs_mtx);
    if (msgs[msg.idx].idx != msg.idx || msgs[msg.idx].nonce != msg.nonce ||
        msgs[msg.idx].snt != msg.snt) {
        logmsg(LOGMSG_ERROR, "%s: malformed pong\n", __func__);
        return;
    }

    Pthread_mutex_unlock(&msgs_mtx);

    msgs[msg.idx].rcv = msg.rcv;
}

int osql_comm_echo(char *tohost, int stream, unsigned long long *sent,
                   unsigned long long *replied, unsigned long long *received)
{
    osql_echo_t msg;
    osql_echo_t *p_msg;
    osql_echo_t *list[MAX_ECHOES];
    uint8_t buf[OSQLCOMM_ECHO_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end = (p_buf + OSQLCOMM_ECHO_TYPE_LEN);
    unsigned long long snt;
    uint64_t nonce;
    int rc;
    int i;
    int j;
    int latency = 0;

    i = 0;
    for (j = 0; j < stream; j++) {
        /* get an echo message */
        Pthread_mutex_lock(&msgs_mtx);

        for (; i < MAX_ECHOES; i++)
            if (msgs[i].nonce == 0)
                break;
        if (i == MAX_ECHOES) {
            logmsg(LOGMSG_ERROR, "%s: too many echoes pending\n", __func__);
            Pthread_mutex_unlock(&msgs_mtx);
            return -1;
        }

        nonce = lrand48();
        snt = osql_log_time();

        bzero(&msg, sizeof(osql_echo_t));
        bzero(&msgs[i], sizeof(osql_echo_t));
        msg.nonce = msgs[i].nonce = nonce;
        msg.idx = msgs[i].idx = i;
        msg.snt = msgs[i].snt = snt;

        Pthread_mutex_unlock(&msgs_mtx);

        list[j] = &msgs[i];

        if (!(osqlcomm_echo_type_put(&msg, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: failed to pack echo message\n", __func__);
            return -1;
        }

        /*TODO: validate destination node to be valid!*/
        /* ping */
        rc = offload_net_send(tohost, NET_OSQL_ECHO_PING, (char *)buf,
                              sizeof(osql_echo_t), 1, NULL, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to send ping rc=%d\n", __func__, rc);
            return -1;
        }
        i++;
    }

    for (j = 0; j < stream; j++) {
        p_msg = list[j];

        /* waiting for pong */
        while (p_msg->rcv == 0) {
            poll(NULL, 0, 1);
            latency++;
            if (latency >= MAX_LATENCY)
                break;
        }

        if (latency >= MAX_LATENCY) {
            logmsg(LOGMSG_ERROR, "%s: Dropped message\n", __func__);
            bzero(p_msg, sizeof(osql_echo_t));
            return -1;
        }

        if (stream == 1 && (p_msg->snt != snt || p_msg->nonce != nonce)) {
            logmsg(LOGMSG_ERROR, "%s: wrong reply!!!\n", __func__);
            return -1;
        }

        *sent = p_msg->snt;
        sent++;
        *replied = p_msg->rcv;
        replied++;
        *received = osql_log_time();
        received++;
        bzero(p_msg, sizeof(osql_echo_t));
    }

    return 0;
}

/**
 * Send RECGENID
 * It handles remote/local connectivity
 *
 */
int osql_send_recordgenid(char *tohost, unsigned long long rqid, uuid_t uuid,
                          unsigned long long genid, int type)
{
    int rc = 0;
    uuidstr_t us;

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_recgenid_uuid_rpl_t recgenid_rpl = {{0}};
        uint8_t buf[OSQLCOMM_RECGENID_UUID_RPL_TYPE_LEN];
        uint8_t *p_buf = buf;
        uint8_t *p_buf_end = p_buf + OSQLCOMM_RECGENID_UUID_RPL_TYPE_LEN;

        recgenid_rpl.hd.type = OSQL_RECGENID;
        comdb2uuidcpy(recgenid_rpl.hd.uuid, uuid);
        recgenid_rpl.dt.genid = genid;

        if (!(p_buf = osqlcomm_recgenid_uuid_rpl_type_put(&recgenid_rpl, p_buf,
                                                          p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_recgenid_rpl_type_put");
            return -1;
        }

        if (gbl_enable_osql_logging) {
            logmsg(LOGMSG_DEBUG, "[%llu %s] send OSQL_RECGENID %llx (%lld)\n",
                   rqid, comdb2uuidstr(uuid, us), genid, genid);
        }

        type = osql_net_type_to_net_uuid_type(type);
        offload_net_send(tohost, type, buf, sizeof(recgenid_rpl), 0, NULL, 0);
    } else {
        osql_recgenid_rpl_t recgenid_rpl = {{0}};
        uint8_t buf[OSQLCOMM_RECGENID_RPL_TYPE_LEN];
        uint8_t *p_buf = buf;
        uint8_t *p_buf_end = p_buf + OSQLCOMM_RECGENID_RPL_TYPE_LEN;

        recgenid_rpl.hd.type = OSQL_RECGENID;
        recgenid_rpl.hd.sid = rqid;
        recgenid_rpl.dt.genid = genid;

        if (!(p_buf = osqlcomm_recgenid_rpl_type_put(&recgenid_rpl, p_buf,
                                                     p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_recgenid_rpl_type_put");
            return -1;
        }

        if (gbl_enable_osql_logging) {
            logmsg(LOGMSG_DEBUG, "[%llu %s] send OSQL_RECGENID %llx (%lld)\n",
                   rqid, comdb2uuidstr(uuid, us), genid, genid);
        }

        offload_net_send(tohost, type, buf, sizeof(recgenid_rpl), 0, NULL, 0);
    }

    return rc;
}

/**
 * Enable a netinfo test for the osqlcomm network layer
 *
 */
int osql_enable_net_test(int testnum)
{
    netinfo_type *netinfo_ptr;
    osql_comm_t *comm = get_thecomm();
    if (!comm || !comm->handle_sibling)
        return 1;
    netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    net_enable_test(netinfo_ptr, testnum);
    return 0;
}

/**
 * Disable a netinfo test for the osqlcomm network layer
 *
 */
int osql_disable_net_test(void)
{
    netinfo_type *netinfo_ptr;
    osql_comm_t *comm = get_thecomm();
    if (!comm || !comm->handle_sibling)
        return 1;
    netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    net_disable_test(netinfo_ptr);
    return 0;
}

enum { OSQL_AUTH_NODE = 1 };

typedef struct osql_corigin {
    int type; /* this is OSQL_AUTH_NODE */
    int node; /* node origin of the request */
} osql_corigin_t;

enum { OSQLCOMM_CORIGIN_TYPE_LEN = 4 + 4 };

BB_COMPILE_TIME_ASSERT(osqlcomm_corigin_type_len,
                       sizeof(osql_corigin_t) == OSQLCOMM_CORIGIN_TYPE_LEN);

static uint8_t *osqlcomm_corigin_type_put(const osql_corigin_t *p_corigin,
                                          uint8_t *p_buf,
                                          const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_CORIGIN_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_put(&(p_corigin->type), sizeof(p_corigin->type), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_corigin->node), sizeof(p_corigin->node), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *osqlcomm_corigin_type_get(osql_corigin_t *p_corigin,
                                                const uint8_t *p_buf,
                                                const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_CORIGIN_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_get(&(p_corigin->type), sizeof(p_corigin->type), p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_corigin->node), sizeof(p_corigin->node), p_buf, p_buf_end);

    return p_buf;
}

netinfo_type *osql_get_netinfo(void)
{
    osql_comm_t *comm = get_thecomm();
    if (!comm) return NULL;
    return (netinfo_type *)comm->handle_sibling;
}

/* prefault code */
#include "osqlpfthdpool.c"

/**
 * Send SCHEMACHANGE op
 * It handles remote/local connectivity
 *
 */
int osql_send_schemachange(char *tonode, unsigned long long rqid, uuid_t uuid,
                           struct schema_change_type *sc, int type)
{

    schemachange_packed_size(sc);
    size_t osql_rpl_size =
        ((rqid == OSQL_RQID_USE_UUID) ? OSQLCOMM_UUID_RPL_TYPE_LEN
                                      : OSQLCOMM_RPL_TYPE_LEN) +
        sc->packed_len;
    uint8_t *buf = alloca(osql_rpl_size);
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end = p_buf + osql_rpl_size;
    int rc = 0;
    uuidstr_t us;

    if (check_master(tonode))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (tonode)
        strcpy(sc->original_master_node, tonode);
    else
        strcpy(sc->original_master_node, gbl_myhostname);

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_uuid_rpl_t hd_uuid = {0};

        hd_uuid.type = OSQL_SCHEMACHANGE;
        comdb2uuidcpy(hd_uuid.uuid, uuid);
        if (!(p_buf = osqlcomm_schemachange_uuid_rpl_type_put(
                  &hd_uuid, sc, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                   "osqlcomm_schemachange_uuid_rpl_type_put");
            return -1;
        }

        type = osql_net_type_to_net_uuid_type(NET_OSQL_SOCK_RPL);
    } else {
        osql_rpl_t hd = {0};

        hd.type = OSQL_SCHEMACHANGE;
        hd.sid = rqid;

        if (!(p_buf = osqlcomm_schemachange_rpl_type_put(&hd, sc, p_buf,
                                                         p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                   "osqlcomm_schemachange_rpl_type_put");
            return -1;
        }
    }

    if (gbl_enable_osql_logging) {
        logmsg(LOGMSG_DEBUG, "[%llu %s] send OSQL_SCHEMACHANGE %s\n", rqid,
               comdb2uuidstr(uuid, us), sc->tablename);
    }

    rc = offload_net_send(tonode, type, buf, osql_rpl_size, 0, NULL, 0);

    return rc;
}

int osql_send_bpfunc(char *tonode, unsigned long long rqid, uuid_t uuid,
                     BpfuncArg *arg, int type)
{
    osql_bpfunc_t *dt;
    size_t data_len = bpfunc_arg__get_packed_size(arg);
    size_t osql_bpfunc_size;
    size_t osql_rpl_size;
    uint8_t *p_buf = NULL;
    uint8_t *p_buf_end;
    int rc = 0;
    uuidstr_t us;

    osql_bpfunc_size = OSQLCOMM_BPFUNC_TYPE_LEN + data_len;
    dt = malloc(osql_bpfunc_size);
    if (!dt) {
        rc = -1;
        goto freemem;
    }

    osql_rpl_size = ((rqid == OSQL_RQID_USE_UUID) ? OSQLCOMM_UUID_RPL_TYPE_LEN
                                                  : OSQLCOMM_RPL_TYPE_LEN) +
                    osql_bpfunc_size;
    p_buf = malloc(osql_rpl_size);
    if (!p_buf) {
        rc = -1;
        goto freemem;
    }

    p_buf_end = p_buf + osql_rpl_size;

    if (check_master(tonode)) {
        rc = OSQL_SEND_ERROR_WRONGMASTER;
        goto freemem;
    }

    dt->data_len = data_len;
    bpfunc_arg__pack(arg, dt->data);

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_uuid_rpl_t hd_uuid = {0};

        hd_uuid.type = OSQL_BPFUNC;
        comdb2uuidcpy(hd_uuid.uuid, uuid);

        if (!osqlcomm_bpfunc_uuid_rpl_type_put(&hd_uuid, dt, p_buf,
                                               p_buf_end)) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                   "osqlcomm_bpfunc_uuid_rpl_type_put");
            rc = -1;
            goto freemem;
        }

        type = osql_net_type_to_net_uuid_type(NET_OSQL_SOCK_RPL);
    } else {
        osql_rpl_t hd = {0};

        hd.type = OSQL_BPFUNC;
        hd.sid = rqid;

        if (!osqlcomm_bpfunc_rpl_type_put(&hd, dt, p_buf, p_buf_end)) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                   "osqlcomm_bpfunc_rpl_type_put");
            rc = -1;
            goto freemem;
        }
    }

    if (gbl_enable_osql_logging) {
        logmsg(LOGMSG_DEBUG, "[%llu %s] send OSQL_BPFUNC type %d\n", rqid,
               comdb2uuidstr(uuid, us), arg->type);
    }

    rc = offload_net_send(tonode, type, p_buf, osql_rpl_size, 0, NULL, 0);

freemem:
    if (dt)
        free(dt);
    if (p_buf)
        free(p_buf);

    return rc;
}

/* test osql stream sending a dummy uuid OSQL_DONE request */
int osql_send_test(void)
{
    struct errstat xerr = {0};
    int nettype = NET_OSQL_SOCK_RPL_UUID;
    snap_uid_t snap_info = {{0}};
    snap_info.replicant_is_able_to_retry = 0;
    snap_info.uuid[0] = 1; // just assign dummy cnonce here
    int rc;

    rc = osql_send_commit_by_uuid(thedb->master, snap_info.uuid, 1 /*numops*/,
                                  &xerr, nettype, NULL /*clnt->query_stats*/,
                                  &snap_info);
    return rc;
}

static void osql_extract_snap_info(osql_sess_t *sess, void *rpl, int rpllen,
                                   int is_uuid)
{

    // TODO (NC) : check this
    if (gbl_disable_cnonce_blkseq)
        return;

    snap_uid_t *snap_info = calloc(1, sizeof(snap_uid_t));
    if (!snap_info) {
        logmsg(LOGMSG_ERROR, "%s malloc failure, no cnonce\n", __func__);
        return;
    }

    const uint8_t *p_buf =
        (uint8_t *)rpl + sizeof(osql_done_t) +
        (is_uuid ? sizeof(osql_uuid_rpl_t) : sizeof(osql_rpl_t));

    const uint8_t *p_buf_end = (const uint8_t *)rpl + rpllen;
    if ((p_buf = snap_uid_get(snap_info, p_buf, p_buf_end)) == NULL)
        abort();

    sess->snap_info = snap_info;

    if (likely(gbl_master_sends_query_effects)) {
        /* Reset 'write' query effects as master will repopulate them
         * and report them back to the replicant.
         */
        sess->snap_info->effects.num_affected = 0;
        sess->snap_info->effects.num_updated = 0;
        sess->snap_info->effects.num_deleted = 0;
        sess->snap_info->effects.num_inserted = 0;
    }
}
