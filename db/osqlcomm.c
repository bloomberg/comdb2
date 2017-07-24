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
#include "limit_fortify.h"
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
#include "comdb2util.h"
#include "comdb2uuid.h"
#include "socket_interfaces.h"
#include "debug_switches.h"
#include <schemachange.h>
#include <genid.h>
#include <flibc.h>
#include <net_types.h>
#include <errstat.h>
#include <views_cron.h>
#include <bpfunc.h>
#include <strbuf.h>
#include <logmsg.h>

#define BLKOUT_DEFAULT_DELTA 5
#define MAX_CLUSTER 16

#define UNK_ERR_SEND_RETRY 10
/**
 * NOTE: the assumption here is that there are no more comm users
 * when g_osql_ready is reset.  This is done by closing all possible
 * sqlthread and appsock threads before disabling g_osql_ready.
 *
 */
struct thdpool *gbl_osqlpfault_thdpool = NULL;

osqlpf_step *gbl_osqlpf_step = NULL;

queue_type *gbl_osqlpf_stepq = NULL;

pthread_mutex_t osqlpf_mutex = PTHREAD_MUTEX_INITIALIZER;

extern __thread int send_prefault_udp;
extern int gbl_prefault_udp;

extern int g_osql_ready;
extern int gbl_goslow;

extern int gbl_partial_indexes;

static int osql_net_type_to_net_uuid_type(int type);

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
    osql_blknds_t blkout; /* blackout structure */
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

typedef struct hbeat {
    int dst;
    int src;
    int time;
} hbeat_t;

enum { OSQLCOMM_HBEAT_TYPE_LEN = 4 + 4 + 4 };

BB_COMPILE_TIME_ASSERT(osqlcomm_hbeat_type_len,
                       sizeof(hbeat_t) == OSQLCOMM_HBEAT_TYPE_LEN);

static uint8_t *osqlcomm_hbeat_type_put(const hbeat_t *p_hbeat_type,
                                        uint8_t *p_buf,
                                        const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_HBEAT_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_hbeat_type->dst), sizeof(p_hbeat_type->dst), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_hbeat_type->src), sizeof(p_hbeat_type->src), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_hbeat_type->time), sizeof(p_hbeat_type->time), p_buf,
                    p_buf_end);

    return p_buf;
}

static const uint8_t *osqlcomm_hbeat_type_get(hbeat_t *p_hbeat_type,
                                              const uint8_t *p_buf,
                                              const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_HBEAT_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_hbeat_type->dst), sizeof(p_hbeat_type->dst), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_hbeat_type->src), sizeof(p_hbeat_type->src), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_hbeat_type->time), sizeof(p_hbeat_type->time), p_buf,
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
    unsigned char ntails;
    unsigned char flags;
    char pad[1];
    char sqlq[1];
};
enum { OSQLCOMM_REQ_TYPE_LEN = 8 + 4 + 4 + 8 + DB_MAX_TZNAMEDB + 3 + 1 };
BB_COMPILE_TIME_ASSERT(osqlcomm_req_type_len,
                       sizeof(struct osql_req) == OSQLCOMM_REQ_TYPE_LEN);

struct osql_req_tail {
    int type;
    int len;
};
enum { OSQL_REQ_TAIL_LEN = 4 + 4 };
BB_COMPILE_TIME_ASSERT(osql_req_tail_len,
                       sizeof(struct osql_req_tail) == OSQL_REQ_TAIL_LEN);

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
    p_buf = buf_put(&p_osql_req->ntails, sizeof(p_osql_req->ntails), p_buf,
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
    p_buf = buf_get(&p_osql_req->ntails, sizeof(p_osql_req->ntails), p_buf,
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
    int padding;
    uuid_t uuid;
    char tzname[DB_MAX_TZNAMEDB];
    unsigned char ntails;
    unsigned char flags;
    char pad[1];
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
    p_buf = buf_put(&p_osql_req->padding, sizeof(p_osql_req->padding), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_put(&p_osql_req->uuid, sizeof(p_osql_req->uuid), p_buf,
                           p_buf_end);
    p_buf = buf_put(&p_osql_req->tzname, sizeof(p_osql_req->tzname), p_buf,
                    p_buf_end);
    p_buf = buf_put(&p_osql_req->ntails, sizeof(p_osql_req->ntails), p_buf,
                    p_buf_end);
    p_buf = buf_put(&p_osql_req->flags, sizeof(p_osql_req->flags), p_buf,
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
    p_buf = buf_get(&p_osql_req->padding, sizeof(p_osql_req->padding), p_buf,
                    p_buf_end);
    p_buf =
        buf_get(&p_osql_req->uuid, sizeof(p_osql_req->uuid), p_buf, p_buf_end);
    p_buf = buf_get(&p_osql_req->tzname, sizeof(p_osql_req->tzname), p_buf,
                    p_buf_end);
    p_buf = buf_get(&p_osql_req->ntails, sizeof(p_osql_req->ntails), p_buf,
                    p_buf_end);
    p_buf = buf_get(&p_osql_req->flags, sizeof(p_osql_req->flags), p_buf,
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

static uint8_t *osqlcomm_uuid_rpl_type_put(const osql_uuid_rpl_t *p_osql_rpl,
                                           uint8_t *p_buf,
                                           const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_UUID_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

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
} osql_uuid_dbq_consume_t;

typedef struct {
    osql_rpl_t hd;
    genid_t genid;
} osql_dbq_consume_t;


typedef struct {
	osql_uuid_rpl_t hd;
	genid_t genid;
} osql_dbq_consume_uuid_t;

//TODO FIXME XXX -- compile time assert to check sizeof

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
    int32_t data_len;

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

    if (sc->table_len == -1 || sc->fname_len == -1 || sc->aname_len == -1)
        return NULL;

    return tmp_buf;
}

static uint8_t *
osqlcomm_schemachange_rpl_type_put(osql_uuid_rpl_t *hd,
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

enum { OSQLCOMM_SERIAL_TYPE_LEN = 4 + 4 + 4 + 4 };

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
    int tmp;
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

typedef struct osql_done {
    int rc;
    int nops;
} osql_done_t;

enum { OSQLCOMM_DONE_TYPE_LEN = 4 + 4 };

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

static const uint8_t *osqlcomm_done_rpl_get(osql_done_rpl_t *p_osql_done_rpl,
                                            const uint8_t *p_buf,
                                            const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_DONE_RPL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_get(&(p_osql_done_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_done_type_get(&(p_osql_done_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_done_uuid_rpl {
    osql_uuid_rpl_t hd;
    osql_done_t dt;
} osql_done_uuid_rpl_t;

enum {
    OSQLCOMM_DONE_UUID_RPL_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_DONE_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_done_uuid_rpl_len,
                       sizeof(osql_done_uuid_rpl_t) ==
                           OSQLCOMM_DONE_UUID_RPL_LEN);

static uint8_t *
osqlcomm_done_uuid_rpl_put(const osql_done_uuid_rpl_t *p_osql_done_uuid_rpl,
                           uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_DONE_UUID_RPL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_put(&(p_osql_done_uuid_rpl->hd), p_buf,
                                       p_buf_end);
    p_buf =
        osqlcomm_done_type_put(&(p_osql_done_uuid_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_done_uuid_rpl_get(osql_done_uuid_rpl_t *p_osql_done_uuid_rpl,
                           const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_DONE_UUID_RPL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_get(&(p_osql_done_uuid_rpl->hd), p_buf,
                                       p_buf_end);
    p_buf =
        osqlcomm_done_type_get(&(p_osql_done_uuid_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

typedef struct osql_done_rpl_stats {
    osql_rpl_t hd;
    osql_done_t dt;
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
    p_buf = buf_put(&(snap_info->effects.num_affected),
                    sizeof(snap_info->effects.num_affected), p_buf, p_buf_end);
    p_buf = buf_put(&(snap_info->effects.num_selected),
                    sizeof(snap_info->effects.num_selected), p_buf, p_buf_end);
    p_buf = buf_put(&(snap_info->effects.num_updated),
                    sizeof(snap_info->effects.num_updated), p_buf, p_buf_end);
    p_buf = buf_put(&(snap_info->effects.num_deleted),
                    sizeof(snap_info->effects.num_deleted), p_buf, p_buf_end);
    p_buf = buf_put(&(snap_info->effects.num_inserted),
                    sizeof(snap_info->effects.num_inserted), p_buf, p_buf_end);
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
    p_buf = buf_get(&(snap_info->effects.num_affected),
                    sizeof(snap_info->effects.num_affected), p_buf, p_buf_end);
    p_buf = buf_get(&(snap_info->effects.num_selected),
                    sizeof(snap_info->effects.num_selected), p_buf, p_buf_end);
    p_buf = buf_get(&(snap_info->effects.num_updated),
                    sizeof(snap_info->effects.num_updated), p_buf, p_buf_end);
    p_buf = buf_get(&(snap_info->effects.num_deleted),
                    sizeof(snap_info->effects.num_deleted), p_buf, p_buf_end);
    p_buf = buf_get(&(snap_info->effects.num_inserted),
                    sizeof(snap_info->effects.num_inserted), p_buf, p_buf_end);
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

/* exposed for dbglog_support */
const uint8_t *dbglog_hdr_put(const struct dbglog_hdr *p_dbglog_hdr,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || DBGLOG_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_dbglog_hdr->type), sizeof(p_dbglog_hdr->type), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_dbglog_hdr->len), sizeof(p_dbglog_hdr->len), p_buf,
                    p_buf_end);

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
                                          uint8_t *p_buf,
                                          const uint8_t *p_buf_end)
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
    uint8_t *p;
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
    unsigned long long dk; /* flag to indicate which keys to modify */
    int nData;
    char pData[4]; /* alignment! - pass some useful data instead of padding */
} osql_ins_t;

enum { OSQLCOMM_INS_TYPE_LEN = 8 + 8 + 4 + 4 };

BB_COMPILE_TIME_ASSERT(osqlcomm_ins_type_len,
                       sizeof(osql_ins_t) == OSQLCOMM_INS_TYPE_LEN);

static uint8_t *osqlcomm_ins_type_put(const osql_ins_t *p_osql_ins,
                                      uint8_t *p_buf, const uint8_t *p_buf_end,
                                      int send_dk)
{
    if (p_buf_end < p_buf ||
        (send_dk ? OSQLCOMM_INS_TYPE_LEN
                 : OSQLCOMM_INS_TYPE_LEN - sizeof(unsigned long long)) >
            (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_put(&(p_osql_ins->seq), sizeof(p_osql_ins->seq), p_buf,
                           p_buf_end);
    if (send_dk)
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
                                            int recv_dk)
{
    if (p_buf_end < p_buf ||
        (recv_dk ? OSQLCOMM_INS_TYPE_LEN
                 : OSQLCOMM_INS_TYPE_LEN - sizeof(unsigned long long)) >
            (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_get(&(p_osql_ins->seq), sizeof(p_osql_ins->seq), p_buf,
                           p_buf_end);
    if (recv_dk)
        p_buf = buf_no_net_get(&(p_osql_ins->dk), sizeof(p_osql_ins->dk), p_buf,
                               p_buf_end);
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
    OSQLCOMM_INS_RPL_TYPE_LEN = OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_INS_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_ins_rpl_type_len,
                       sizeof(osql_ins_rpl_t) == OSQLCOMM_INS_RPL_TYPE_LEN);

static uint8_t *osqlcomm_ins_rpl_type_put(const osql_ins_rpl_t *p_osql_ins_rpl,
                                          uint8_t *p_buf,
                                          const uint8_t *p_buf_end, int send_dk)
{
    if (p_buf_end < p_buf ||
        (send_dk ? OSQLCOMM_INS_RPL_TYPE_LEN
                 : OSQLCOMM_INS_RPL_TYPE_LEN - sizeof(unsigned long long)) >
            (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(&(p_osql_ins_rpl->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_ins_type_put(&(p_osql_ins_rpl->dt), p_buf, p_buf_end, send_dk);

    return p_buf;
}

static const uint8_t *osqlcomm_ins_rpl_type_get(osql_ins_rpl_t *p_osql_ins_rpl,
                                                const uint8_t *p_buf,
                                                const uint8_t *p_buf_end,
                                                int recv_dk)
{
    if (p_buf_end < p_buf ||
        (recv_dk ? OSQLCOMM_INS_RPL_TYPE_LEN
                 : OSQLCOMM_INS_RPL_TYPE_LEN - sizeof(unsigned long long)) >
            (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_get(&(p_osql_ins_rpl->hd), p_buf, p_buf_end);
    p_buf =
        osqlcomm_ins_type_get(&(p_osql_ins_rpl->dt), p_buf, p_buf_end, recv_dk);

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
                               uint8_t *p_buf, const uint8_t *p_buf_end,
                               int send_dk)
{
    if (p_buf_end < p_buf ||
        (send_dk ? OSQLCOMM_INS_UUID_RPL_TYPE_LEN
                 : OSQLCOMM_INS_UUID_RPL_TYPE_LEN -
                       sizeof(unsigned long long)) > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_put(&(p_osql_ins_uuid_rpl->hd), p_buf,
                                       p_buf_end);
    p_buf = osqlcomm_ins_type_put(&(p_osql_ins_uuid_rpl->dt), p_buf, p_buf_end,
                                  send_dk);

    return p_buf;
}

static const uint8_t *
osqlcomm_ins_uuid_rpl_type_get(osql_ins_uuid_rpl_t *p_osql_ins_uuid_rpl,
                               const uint8_t *p_buf, const uint8_t *p_buf_end,
                               int recv_dk)
{
    if (p_buf_end < p_buf ||
        (recv_dk ? OSQLCOMM_INS_UUID_RPL_TYPE_LEN
                 : OSQLCOMM_INS_UUID_RPL_TYPE_LEN -
                       sizeof(unsigned long long)) > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_get(&(p_osql_ins_uuid_rpl->hd), p_buf,
                                       p_buf_end);
    p_buf = osqlcomm_ins_type_get(&(p_osql_ins_uuid_rpl->dt), p_buf, p_buf_end,
                                  recv_dk);

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

static int osql_nettype_is_uuid(int type)
{
    return type >= NET_OSQL_UUID_REQUEST_MIN &&
           type < NET_OSQL_UUID_REQUEST_MAX;
}

static osql_stats_t stats[OSQL_MAX_REQ] = {0};

/* echo service */
#define MAX_ECHOES 256
#define MAX_LATENCY 1000
osql_echo_t msgs[MAX_ECHOES];
pthread_mutex_t msgs_mtx = PTHREAD_MUTEX_INITIALIZER;

static osql_comm_t *comm = NULL;

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

static void net_osql_heartbeat(void *hndl, void *uptr, char *fromnode,
                               int usertype, void *dtap, int dtalen,
                               uint8_t is_tcp);
static int net_osql_nodedwn(netinfo_type *netinfo_ptr, char *node);
static void net_osql_poked(void *hndl, void *uptr, char *fromnode, int usertype,
                           void *dtap, int dtalen, uint8_t is_tcp);
static void net_osql_poked_uuid(void *hndl, void *uptr, char *fromnode,
                                int usertype, void *dtap, int dtalen,
                                uint8_t is_tcp);
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

static void *osql_heartbeat_thread(void *arg);
static void signal_rtoff(void);

static int check_master(char *tohost);
static int offload_net_send(char *tohost, int usertype, void *data, int datalen,
                            int nodelay);
static int offload_net_send_tail(char *tohost, int usertype, void *data,
                                 int datalen, int nodelay, void *tail,
                                 int tailen);
static int osql_check_version(int type);
static int offload_net_send_tails(char *host, int usertype, void *data,
                                  int datalen, int nodelay, int ntails,
                                  void **tails, int *tailens);
static int get_blkout(time_t now, char *nodes[REPMAX], int *nds);

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
    pthread_t stat_hbeat_tid = 0;
    int ii = 0;
    void *rcv = NULL;
    int rc = 0;
    pthread_attr_t attr;

    /* allocate comm */
    tmp = (osql_comm_t *)calloc(sizeof(osql_comm_t), 1);
    if (!tmp) {
        logmsg(LOGMSG_ERROR, "%s: unable to allocate %d bytes\n", __func__,
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
    net_register_handler(tmp->handle_sibling, NET_OSQL_BLOCK_RPL, net_osql_rpl);
    net_register_handler(tmp->handle_sibling, NET_OSQL_BLOCK_RPL_UUID,
                         net_osql_rpl);

    net_register_handler(tmp->handle_sibling, NET_OSQL_SOCK_REQ, net_sosql_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SOCK_RPL, net_osql_rpl);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SIGNAL,
                         net_sorese_signal);

    net_register_handler(tmp->handle_sibling, NET_OSQL_RECOM_REQ,
                         net_recom_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_RECOM_RPL, net_osql_rpl);

    net_register_handler(tmp->handle_sibling, NET_OSQL_SNAPISOL_REQ,
                         net_snapisol_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SNAPISOL_RPL,
                         net_osql_rpl);

    net_register_handler(tmp->handle_sibling, NET_OSQL_SERIAL_REQ,
                         net_serial_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SERIAL_RPL,
                         net_osql_rpl);

    net_register_handler(tmp->handle_sibling, NET_HBEAT_SQL,
                         net_osql_heartbeat);
    net_register_handler(tmp->handle_sibling, NET_OSQL_POKE, net_osql_poked);
    net_register_handler(tmp->handle_sibling, NET_OSQL_MASTER_CHECK,
                         net_osql_master_check);
    net_register_handler(tmp->handle_sibling, NET_OSQL_MASTER_CHECKED,
                         net_osql_master_checked);
    /* register echo service handler */
    net_register_handler(tmp->handle_sibling, NET_OSQL_ECHO_PING,
                         net_osql_rcv_echo_ping);
    net_register_handler(tmp->handle_sibling, NET_OSQL_ECHO_PONG,
                         net_osql_rcv_echo_pong);

    /* register the uuid clones */
    net_register_handler(tmp->handle_sibling, NET_OSQL_SOCK_REQ_UUID,
                         net_sosql_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SOCK_RPL_UUID,
                         net_osql_rpl);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SIGNAL_UUID,
                         net_sorese_signal);

    net_register_handler(tmp->handle_sibling, NET_OSQL_RECOM_REQ_UUID,
                         net_recom_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_RECOM_RPL_UUID,
                         net_osql_rpl);

    net_register_handler(tmp->handle_sibling, NET_OSQL_SNAPISOL_REQ_UUID,
                         net_snapisol_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SNAPISOL_RPL_UUID,
                         net_osql_rpl);

    net_register_handler(tmp->handle_sibling, NET_OSQL_SERIAL_REQ_UUID,
                         net_serial_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SERIAL_RPL_UUID,
                         net_osql_rpl);

    net_register_handler(tmp->handle_sibling, NET_HBEAT_SQL_UUID,
                         net_osql_heartbeat);
    net_register_handler(tmp->handle_sibling, NET_OSQL_POKE_UUID,
                         net_osql_poked_uuid);
    net_register_handler(tmp->handle_sibling, NET_OSQL_MASTER_CHECK_UUID,
                         net_osql_master_check);
    net_register_handler(tmp->handle_sibling, NET_OSQL_MASTER_CHECKED_UUID,
                         net_osql_master_checked);

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
    net_register_handler(tmp->handle_sibling, NET_BLOCK_REQ, net_block_req);
    net_register_handler(tmp->handle_sibling, NET_BLOCK_REPLY, net_block_reply);

    /* remote snap uid requests */
    net_register_handler(tmp->handle_sibling, NET_OSQL_SNAP_UID_REQ,
                         net_snap_uid_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SNAP_UID_RPL,
                         net_snap_uid_rpl);

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

    tmp->blkout.delta = BLKOUT_DEFAULT_DELTA;
    tmp->blkout.delta_hbeat = gbl_osql_heartbeat_alert;

    comm = tmp;

    bdb_register_rtoff_callback(dbenv->bdb_env, signal_rtoff);

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setstacksize(&attr, 100 * 1024);

    rc = pthread_create(&stat_hbeat_tid, &attr, osql_heartbeat_thread, NULL);

    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: pthread_create error %d %s\n", __func__, rc,
                strerror(errno));
        return -1;
    }

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
    /*
       LEAK THIS PLEASE: there are races in net lib that I could not
       figure it out.  Leaking ensures that we do not need to
       sync all the net threads on with this destruction effort

       destroy_netinfo(comm->handle_sibling);
       free(comm);
    */
    comm = NULL;
}

/**
 * Disable temporarily replicant "node"
 * "node" will receive no more offloading requests
 * until a blackout window will expire
 * It is used mainly with blocksql
 *
 */
int osql_comm_blkout_node(char *host)
{

    osql_blknds_t *blk = NULL;
    int i = 0;
    int len = 0;

    if (!comm)
        return -1;

    blk = &comm->blkout;

    len = blk->n;

    for (i = 0; i < len; i++) {
        if (host == blk->nds[i]) {
            blk->times[i] = time(NULL); /* refresh blackout */
#ifdef OFFLOAD_TEST
            fprintf(stderr, "BO %d %u\n", node, blk->times[i]);
#endif
            break;
        }
    }

    return 0;
}

int offload_comm_send_sync_blockreq(char *node, void *buf, int buflen)
{
    int rc;
    int nwakeups;
    struct timespec ts;
    struct buf_lock_t *p_slock;

    // create a fake buf_lock
    p_slock = malloc(sizeof(struct buf_lock_t));

    if (p_slock == NULL)
        return ENOMEM;

    p_slock->reply_done = 0;
    p_slock->sb = NULL;

    // initialize lock and cond
    rc = pthread_mutex_init(&(p_slock->req_lock), 0);
    if (rc != 0) {
        free(p_slock);
        return rc;
    }

    rc = pthread_cond_init(&(p_slock->wait_cond), NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&(p_slock->req_lock));
        free(p_slock);
        return rc;
    }

    rc = pthread_mutex_lock(&(p_slock->req_lock));
    if (rc == 0) {
        rc = offload_comm_send_blockreq(node, p_slock, buf, buflen);
        if (rc == 0) {
            nwakeups = 0;
            while (!p_slock->reply_done) {
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_sec += 1;
                rc = pthread_cond_timedwait(&(p_slock->wait_cond),
                                            &(p_slock->req_lock), &ts);
                ++nwakeups;

                if (nwakeups == 1000)
                    break;
            }
        }
        pthread_mutex_unlock(&(p_slock->req_lock));
    }

    // clean up
    pthread_cond_destroy(&(p_slock->wait_cond));
    pthread_mutex_destroy(&(p_slock->req_lock));
    free(p_slock);
    return rc;
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
    rc = offload_net_send(host, NET_BLOCK_REQ, net_msg, len, 1);
    free(net_msg);
    return rc;
}

static void net_block_req(void *hndl, void *uptr, char *fromhost, int usertype,
                          void *dtap, int dtalen, uint8_t is_tcp)
{

    int rc = 0;

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
    rc = offload_net_send(host, NET_BLOCK_REPLY, net_msg, len, 1);
    free(net_msg);
    return rc;
}

static void net_block_reply(void *hndl, void *uptr, char *fromhost,
                            int usertype, void *dtap, int dtalen,
                            uint8_t is_tcp)
{

    int rc = 0;
    net_block_msg_t *net_msg = dtap;
    /* using p_slock pointer as the request id now, this contains info about
     * socket request.*/
    struct buf_lock_t *p_slock = (struct buf_lock_t *)net_msg->rqid;
    p_slock->rc = net_msg->rc;
    sndbak_open_socket(p_slock->sb, (u_char *)net_msg->data, net_msg->datalen,
                       net_msg->rc);
    /* Signal to allow the appsock thread to take new request from client.*/
    signal_buflock(p_slock);
}

/* this is wrong in durable_lsn mode: we can't tell if this is DURABLE */
int check_snap_uid_req(char *host, snap_uid_t *snap_info)
{
    if (host == NULL) {
        struct sql_thread *thd = pthread_getspecific(query_info_key);
        if (!thd) {
            return -1;
        }
        struct sqlclntstate *clnt = thd->sqlclntstate;
        snap_uid_t *snap_out = NULL;
        int rc = bdb_blkseq_find(thedb->bdb_env, NULL, snap_info->key,
                                 snap_info->keylen, (void **)&snap_out, NULL);
        if (rc == IX_FND) {
            clnt->is_retry = 1;
            clnt->effects = snap_out->effects;
            free(snap_out);
        } else if (rc == IX_NOTFND) {
            clnt->is_retry = 0;
        } else {
            clnt->is_retry = -1;
        }
        return 0;
    } else {
        struct errstat xerr;
        snap_uid_t snap_send;

        snap_uid_put(snap_info, (uint8_t *)&snap_send,
                     (const uint8_t *)&snap_send + sizeof(snap_uid_t));
        offload_net_send(host, NET_OSQL_SNAP_UID_REQ, &snap_send,
                         sizeof(snap_uid_t), 1);
        int rc = osql_chkboard_wait_commitrc(OSQL_RQID_USE_UUID, snap_send.uuid,
                                             &xerr);
        return rc;
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
                     sizeof(snap_uid_t), 1);
}

static void net_snap_uid_rpl(void *hndl, void *uptr, char *fromhost,
                             int usertype, void *dtap, int dtalen,
                             uint8_t is_tcp)
{
    int rc = 0;
    snap_uid_t snap_info;
    snap_uid_get(&snap_info, dtap, (uint8_t *)dtap + dtalen);
    osql_chkboard_sqlsession_rc(OSQL_RQID_USE_UUID, snap_info.uuid, 0,
                                &snap_info, NULL);
}

int gbl_disable_cnonce_blkseq;

/**
 * If "rpl" is a done packet, set xerr to error if any and return 1
 * If "rpl" is a recognizable packet, returns the length of the data type is
 *recognized,
 * or -1 otherwise
 *
 */
int osql_comm_is_done(char *rpl, int rpllen, int hasuuid, struct errstat **xerr,
                      struct ireq *iq)
{
    int rc = 0;
    void *data;
    int type;
    osql_rpl_t hdr;
    osql_uuid_rpl_t uuid_hdr;
    const uint8_t *p_buf = (uint8_t *)rpl;
    const uint8_t *p_buf_end = p_buf + rpllen;

    rc = 0;
    if (hasuuid) {
        p_buf = osqlcomm_uuid_rpl_type_get(&uuid_hdr, p_buf, p_buf_end);
        type = uuid_hdr.type;
    } else {
        p_buf = osqlcomm_rpl_type_get(&hdr, p_buf, p_buf_end);
        type = hdr.type;
    }
    switch (type) {
    case OSQL_USEDB:
    case OSQL_INSREC:
    case OSQL_INSERT:
    case OSQL_INSIDX:
    case OSQL_DELIDX:
    case OSQL_QBLOB:
        break;
    case OSQL_DONE_SNAP:
        /* The iq is passed in from bplog_saveop */
        if(iq)
        {
            osql_done_t dt = {0};
            p_buf_end = p_buf + sizeof(osql_done_t);
            if((p_buf = osqlcomm_done_type_get(&dt, p_buf, p_buf_end)) == NULL)
                abort();

            p_buf_end = rpl + rpllen;

            if ((p_buf = snap_uid_get(&iq->snap_info, p_buf, p_buf_end)) == NULL)
                abort();

            iq->have_snap_info = !(gbl_disable_cnonce_blkseq);
        }

    case OSQL_DONE:
    case OSQL_DONE_STATS:

        if (xerr)
            *xerr = NULL;
        rc = 1;
        break;
    case OSQL_XERR:
        /* keep this un-endianized.  the code will swap what it needs to */
        if (xerr) {
            if (hasuuid)
                *xerr = &((osql_done_xerr_uuid_t *)rpl)->dt;
            else
                *xerr = &((osql_done_xerr_t *)rpl)->dt;
        }
        rc = 1;
        break;
    default:
        if (iq)
            osql_set_delayed(iq);
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
    void *out;

    if (rqid == OSQL_RQID_USE_UUID) {
        uint8_t buf[OSQLCOMM_POKE_UUID_TYPE_LEN];
        uint8_t *p_buf = buf, *p_buf_end = p_buf + OSQLCOMM_POKE_UUID_TYPE_LEN;
        osql_poke_uuid_t poke = {0};

        poke.tstamp = time_epoch();
        comdb2uuidcpy(poke.uuid, uuid);

        if (!(p_buf = osqlcomm_poke_uuid_type_put(&poke, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returned NULL\n", __func__,
                    "osqlcomm_poke_uuid_type_put");
            return -1;
        }

        type = osql_net_type_to_net_uuid_type(type);
        rc = offload_net_send(tohost, type, &buf, sizeof(buf), 1);
    } else {
        osql_poke_t poke = {0};
        uint8_t buf[OSQLCOMM_POKE_TYPE_LEN],
            *p_buf = buf, *p_buf_end = buf + OSQLCOMM_POKE_TYPE_LEN;

        poke.tstamp = time_epoch();

        poke.from = 0;
        poke.to = 0;
        poke.rqid = rqid;

        if (!(p_buf = osqlcomm_poke_type_put(&poke, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_poke_type_put");
            return -1;
        }
        rc = offload_net_send(tohost, type, &buf, sizeof(buf), 1);
    }

    return rc;
}

static int osql_net_type_to_net_uuid_type(int type)
{
    switch (type) {
    case NET_OSQL_BLOCK_REQ:
        return NET_OSQL_BLOCK_REQ_UUID;
    case NET_OSQL_BLOCK_REQ_PARAMS:
        return NET_OSQL_BLOCK_REQ_PARAMS_UUID;
    case NET_OSQL_BLOCK_REQ_COST:
        return NET_OSQL_BLOCK_REQ_COST_UUID;
    case NET_OSQL_BLOCK_RPL:
        return NET_OSQL_BLOCK_RPL_UUID;
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
    case NET_HBEAT_SQL:
        return NET_HBEAT_SQL_UUID;
    case NET_OSQL_POKE:
        return NET_OSQL_POKE_UUID;
    case NET_OSQL_MASTER_CHECK:
        return NET_OSQL_MASTER_CHECK_UUID;
    case NET_OSQL_MASTER_CHECKED:
        return NET_OSQL_MASTER_CHECKED_UUID;
    default:
        return type;
    }
}

static inline
int is_tablename_queue(const char * tablename, int len)
{
    return (len > 3 && tablename[0] == '_' &&
            tablename[1] == '_' && tablename[2] == 'q');
}


/**
 * Send USEDB op
 * It handles remote/local connectivity
 *
 */
int osql_send_usedb(char *tohost, unsigned long long rqid, uuid_t uuid,
                    char *tablename, int type, SBUF2 *logsb)
{
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    unsigned short tablenamelen = strlen(tablename) + 1; /*including trailing 0*/
    int msglen;
    int rc = 0;
    int sent;

    uint8_t buf[OSQLCOMM_USEDB_RPL_UUID_TYPE_LEN > OSQLCOMM_USEDB_RPL_TYPE_LEN
                    ? OSQLCOMM_USEDB_RPL_UUID_TYPE_LEN
                    : OSQLCOMM_USEDB_RPL_TYPE_LEN];

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    unsigned short tableversion = 0;
    if ( !is_tablename_queue(tablename, tablenamelen - 1) )
        tableversion = comdb2_table_version(tablename);

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_usedb_rpl_uuid_t usedb_uuid_rpl = {0};
        uint8_t *p_buf = buf;
        uint8_t *p_buf_end = (p_buf + OSQLCOMM_USEDB_RPL_UUID_TYPE_LEN);

        sent = sizeof(usedb_uuid_rpl.dt.tablename);
        msglen = OSQLCOMM_USEDB_RPL_UUID_TYPE_LEN;

        usedb_uuid_rpl.hd.type = OSQL_USEDB;
        comdb2uuidcpy(usedb_uuid_rpl.hd.uuid, uuid);
        usedb_uuid_rpl.dt.tablenamelen = tablenamelen;
        usedb_uuid_rpl.dt.tableversion = tableversion;
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
        osql_usedb_rpl_t usedb_rpl = {0};
        uint8_t *p_buf = buf;
        uint8_t *p_buf_end = (p_buf + OSQLCOMM_USEDB_RPL_TYPE_LEN);

        /* allocate and set reply */
        sent = sizeof(usedb_rpl.dt.tablename);
        msglen = OSQLCOMM_USEDB_RPL_TYPE_LEN;

        usedb_rpl.hd.type = OSQL_USEDB;
        usedb_rpl.hd.sid = rqid;
        usedb_rpl.dt.tablenamelen = tablenamelen;
        usedb_rpl.dt.tableversion = tableversion;
        strncpy(usedb_rpl.dt.tablename, tablename,
                sizeof(usedb_rpl.dt.tablename));

        if (!(p_buf =
                  osqlcomm_usedb_rpl_type_put(&usedb_rpl, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_usedb_rpl_type_put");
            return -1;
        }
    }

    if (logsb) {
        uuidstr_t us;
        sbuf2printf(logsb, "[%llu %s] send OSQL_USEDB %.*s\n", rqid,
                    comdb2uuidstr(uuid, us), tablenamelen, tablename);
        sbuf2flush(logsb);
    }

    if (tablenamelen > sent) {
        rc = offload_net_send_tail(tohost, type, &buf, msglen, 0,
                                   tablename + sent, tablenamelen - sent);
    } else {
        rc = offload_net_send(tohost, type, &buf, msglen, 0);
    }

    return rc;
}

/**
 * Send UPDCOLS op
 * It handles remote/local connectivity
 *
 */
int osql_send_updcols(char *tohost, unsigned long long rqid, uuid_t uuid,
                      unsigned long long seq, int type, int *colList, int ncols,
                      SBUF2 *logsb)
{
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    char *data = (char *)colList;
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
        osql_updcols_uuid_rpl_t rpl = {0};

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
        osql_updcols_rpl_t rpl = {0};
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

    if (logsb) {
        sbuf2printf(logsb, "[%llu] send OSQL_UPDCOLS %d\n", rqid, ncols);
        sbuf2flush(logsb);
    }

    rc = offload_net_send(tohost, type, buf, totlen, 0);

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
                    char *pData, int nData, int type, SBUF2 *logsb)
{
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    int msglen;
    uint8_t buf[OSQLCOMM_INDEX_RPL_TYPE_LEN > OSQLCOMM_INDEX_UUID_RPL_TYPE_LEN
                    ? OSQLCOMM_INDEX_RPL_TYPE_LEN
                    : OSQLCOMM_INDEX_UUID_RPL_TYPE_LEN];
    int rc = 0;
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end = NULL;

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_index_uuid_rpl_t index_uuid_rpl = {0};

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
        osql_index_rpl_t index_rpl = {0};

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

    if (logsb) {
        unsigned long long lclgenid = bdb_genid_to_host_order(genid);
        uuidstr_t us;
        sbuf2printf(logsb, "[%llx %s] send %s %llx (%lld)\n", rqid,
                    comdb2uuidstr(uuid, us),
                    isDelete ? "OSQL_DELIDX" : "OSQL_INSIDX", lclgenid,
                    lclgenid);
        sbuf2flush(logsb);
    }

    rc = (nData > 0)
             ? offload_net_send_tail(tohost, type, buf, msglen, 0, pData, nData)
             : offload_net_send(tohost, type, buf, msglen, 0);

    return rc;
}

/**
 * Send QBLOB op
 * It handles remote/local connectivity
 *
 */
int osql_send_qblob(char *tohost, unsigned long long rqid, uuid_t uuid,
                    int blobid, unsigned long long seq, int type, char *data,
                    int datalen, SBUF2 *logsb)
{
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    int sent;
    uint8_t buf[OSQLCOMM_QBLOB_UUID_RPL_TYPE_LEN > OSQLCOMM_QBLOB_RPL_TYPE_LEN
                    ? OSQLCOMM_QBLOB_UUID_RPL_TYPE_LEN
                    : OSQLCOMM_QBLOB_RPL_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end;
    int rc = 0;
    int msgsz = 0;
    osql_qblob_rpl_t rpl = {0};

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_qblob_uuid_rpl_t rpl_uuid = {0};
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

    if (logsb) {
        uuidstr_t us;
        sbuf2printf(logsb, "[%llx %s] send OSQL_QBLOB %d %d\n", rqid,
                    comdb2uuidstr(uuid, us), blobid, datalen);
        sbuf2flush(logsb);
    }

    if (datalen > sent)
        rc = offload_net_send_tail(tohost, type, buf, msgsz, 0, data + sent,
                                   datalen - sent);
    else
        rc = offload_net_send(tohost, type, buf, msgsz, 0);

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
                     int type, SBUF2 *logsb)
{
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    uint8_t buf[OSQLCOMM_UPD_UUID_RPL_TYPE_LEN > OSQLCOMM_UPD_RPL_TYPE_LEN
                    ? OSQLCOMM_UPD_UUID_RPL_TYPE_LEN
                    : OSQLCOMM_UPD_RPL_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end;
    int rc = 0;
    int sent;
    int msgsz;
    osql_upd_rpl_t upd_rpl = {0};
    int send_dk = 0;

    if (gbl_partial_indexes && ins_keys != -1ULL && del_keys != -1ULL)
        send_dk = 1;

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_upd_uuid_rpl_t upd_uuid_rpl = {0};

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

        int ntype = osql_net_type_to_net_uuid_type(type);
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

    if (logsb) {
        unsigned long long lclgenid = bdb_genid_to_host_order(genid);
        sbuf2printf(logsb, "[%llu] send OSQL_UPDREC %llx (%lld)\n", rqid,
                    lclgenid, lclgenid);
        sbuf2flush(logsb);
    }

    rc = (nData > sent) ? offload_net_send_tail(tohost, type, &buf, msgsz, 0,
                                                pData + sent, nData - sent)
                        : offload_net_send(tohost, type, &buf, msgsz, 0);

    return rc;
}

void osql_decom_node(char *decom_node)
{
    logmsg(LOGMSG_INFO, "osql_decom_node %s\n", decom_node);

    netinfo_type *netinfo_ptr;

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
    if(!comm) return;
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;

    net_exiting(netinfo_ptr);
}

int osql_close_connection(char *host)
{
    if (comm)
        return net_close_connection(comm->handle_sibling, host);
    else
        return 0;
}

/* Send dbglog op */
int osql_send_dbglog(char *tohost, unsigned long long rqid, uuid_t uuid,
                     unsigned long long dbglog_cookie, int queryid, int type)
{
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    osql_dbglog_t req;
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

    rc = offload_net_send_tail(tohost, type, &buf, sizeof(osql_dbglog_t), 0,
                               NULL, 0);
    return rc;
}

/**
 * Send UPDSTAT op
 * It handles remote/local connectivity
 *
 */
int osql_send_updstat(char *tohost, unsigned long long rqid, uuid_t uuid,
                      unsigned long long seq, char *pData, int nData, int nStat,
                      int type, SBUF2 *logsb)
{
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    osql_updstat_rpl_t updstat_rpl = {0};
    osql_updstat_uuid_rpl_t updstat_rpl_uuid = {0};

    uint8_t
        buf[OSQLCOMM_UPDSTAT_RPL_TYPE_LEN > OSQLCOMM_UPDSTAT_UUID_RPL_TYPE_LEN
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

    if (logsb) {
        sbuf2printf(logsb, "[%llu] send OSQL_UPDSTATREC %llx (%lld)\n", rqid,
                    seq, seq);
        sbuf2flush(logsb);
    }

    rc = (nData > sent) ? offload_net_send_tail(tohost, type, buf, msglen, 0,
                                                pData + sent, nData - sent)
                        : offload_net_send(tohost, type, buf, msglen, 0);

    return rc;
}

/**
 * Send INSREC op
 * It handles remote/local connectivity
 *
 */
int osql_send_insrec(char *tohost, unsigned long long rqid, uuid_t uuid,
                     unsigned long long genid, unsigned long long dirty_keys,
                     char *pData, int nData, int type, SBUF2 *logsb)
{
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    int msglen;
    uint8_t buf[OSQLCOMM_INS_RPL_TYPE_LEN > OSQLCOMM_INS_UUID_RPL_TYPE_LEN
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
        osql_ins_uuid_rpl_t ins_uuid_rpl = {0};

        ins_uuid_rpl.hd.type = send_dk ? OSQL_INSERT : OSQL_INSREC;
        comdb2uuidcpy(ins_uuid_rpl.hd.uuid, uuid);
        ins_uuid_rpl.dt.seq = genid;
        ins_uuid_rpl.dt.dk = dirty_keys;
        ins_uuid_rpl.dt.nData = nData;

        if (send_dk)
            p_buf_end = p_buf + OSQLCOMM_INS_UUID_RPL_TYPE_LEN;
        else
            p_buf_end =
                p_buf + OSQLCOMM_INS_UUID_RPL_TYPE_LEN - sizeof(dirty_keys);

        if (!(p_buf = osqlcomm_ins_uuid_rpl_type_put(&ins_uuid_rpl, p_buf,
                                                     p_buf_end, send_dk))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_ins_rpl_type_put");
            return -1;
        }
        if (send_dk)
            msglen = sizeof(ins_uuid_rpl);
        else
            msglen = sizeof(ins_uuid_rpl) - sizeof(dirty_keys);
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
        osql_ins_rpl_t ins_rpl = {0};

        ins_rpl.hd.type = send_dk ? OSQL_INSERT : OSQL_INSREC;
        ins_rpl.hd.sid = rqid;
        ins_rpl.dt.seq = genid;
        ins_rpl.dt.dk = dirty_keys;
        ins_rpl.dt.nData = nData;

        if (send_dk)
            p_buf_end = p_buf + OSQLCOMM_INS_RPL_TYPE_LEN;
        else
            p_buf_end = p_buf + OSQLCOMM_INS_RPL_TYPE_LEN - sizeof(dirty_keys);

        if (!(p_buf = osqlcomm_ins_rpl_type_put(&ins_rpl, p_buf, p_buf_end,
                                                send_dk))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_ins_rpl_type_put");
            return -1;
        }
        if (send_dk)
            msglen = sizeof(ins_rpl);
        else
            msglen = sizeof(ins_rpl) - sizeof(dirty_keys);
        sent = sizeof(ins_rpl.dt.pData);
        memset(p_buf, 0, sizeof(ins_rpl.dt.pData));
    }

    if (nData > 0) {
        p_buf = buf_no_net_put(pData, nData < sent ? nData : sent, p_buf,
                               p_buf_end);
    }

    if (logsb) {
        unsigned long long lclgenid = bdb_genid_to_host_order(genid);
        uuidstr_t us;
        sbuf2printf(logsb, "[%llx %s] send %s %llx (%lld)\n", rqid,
                    comdb2uuidstr(uuid, us),
                    send_dk ? "OSQL_INSERT" : "OSQL_INSREC", lclgenid,
                    lclgenid);
        sbuf2flush(logsb);
    }

    rc = (nData > sent) ? offload_net_send_tail(tohost, type, buf, msglen, 0,
                                                pData + sent, nData - sent)
                        : offload_net_send(tohost, type, buf, msglen, 0);

    return rc;
}

int osql_send_dbq_consume(char *tohost, unsigned long long rqid, uuid_t uuid,
                          genid_t genid, int type, SBUF2 *logsb)
{
    netinfo_type *netinfo_ptr = comm->handle_sibling;
    union {
        osql_uuid_dbq_consume_t uuid;
        osql_dbq_consume_t rqid;
    } rpl = {0};
    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;
    if (logsb) {
        genid_t lclgenid = bdb_genid_to_host_order(genid);
        uuidstr_t us;
        sbuf2printf(logsb, "[%llx %s] send OSQL_DBQ_CONSUME %llx (%lld)\n",
                    rqid, comdb2uuidstr(uuid, us), lclgenid, lclgenid);
        sbuf2flush(logsb);
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
    return offload_net_send(tohost, type, &rpl, sz, 0);
}


/**
 * Send DELREC op
 * It handles remote/local connectivity
 *
 */
int osql_send_delrec(char *tohost, unsigned long long rqid, uuid_t uuid,
                     unsigned long long genid, unsigned long long dirty_keys,
                     int type, SBUF2 *logsb)
{
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    uint8_t
        buf[OSQLCOMM_OSQL_DEL_RPL_TYPE_LEN > OSQLCOMM_OSQL_DEL_UUID_RPL_TYPE_LEN
                ? OSQLCOMM_OSQL_DEL_RPL_TYPE_LEN
                : OSQLCOMM_OSQL_DEL_UUID_RPL_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end;
    int rc = 0;
    int msgsz;
    uuidstr_t us;
    int send_dk = 0;

    if (gbl_partial_indexes && dirty_keys != -1ULL)
        send_dk = 1;

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;
    if (rqid == OSQL_RQID_USE_UUID) {
        osql_del_uuid_rpl_t del_uuid_rpl = {0};
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
        osql_del_rpl_t del_rpl = {0};
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

    if (logsb) {
        unsigned long long lclgenid = bdb_genid_to_host_order(genid);
        uuidstr_t us;
        sbuf2printf(logsb, "[%llx %s] send %s %llx (%lld)\n", rqid,
                    comdb2uuidstr(uuid, us),
                    send_dk ? "OSQL_DELETE" : "OSQL_DELREC", lclgenid,
                    lclgenid);
        sbuf2flush(logsb);
    }

    rc = offload_net_send(tohost, type, &buf, msgsz, 0);

    return rc;
}

/**
 * Send SERIAL READ SET
 *
 */
int osql_send_serial(char *tohost, unsigned long long rqid, uuid_t uuid,
                     CurRangeArr *arr, unsigned int file, unsigned int offset,
                     int type, SBUF2 *logsb)
{
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
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
        osql_serial_uuid_rpl_t serial_rpl = {0};

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

        if (logsb) {
            uuidstr_t us;
            sbuf2printf(logsb, "[%s] send OSQL_SERIAL type=%d %d %d\n",
                        comdb2uuidstr(uuid, us), type, cr_sz, arr->size);
            sbuf2flush(logsb);
        }

#if 0
       printf("Sending rqid=%llu tmp=%llu\n", rqid, osql_log_time());
#endif

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
        osql_serial_rpl_t serial_rpl = {0};

        serial_rpl.hd.type =
            (type == NET_OSQL_SERIAL_RPL ||
             type == NET_OSQL_SERIAL_RPL_UUID) ? OSQL_SERIAL : OSQL_SELECTV;
        serial_rpl.hd.sid = rqid;
        serial_rpl.dt.buf_size = cr_sz;
        serial_rpl.dt.arr_size = (arr) ? arr->size : 0;
        serial_rpl.dt.file = file;
        serial_rpl.dt.offset = offset;

        if (logsb) {
            sbuf2printf(logsb, "[%llu] send OSQL_SERIAL %d %d\n", rqid, cr_sz,
                        arr->size);
            sbuf2flush(logsb);
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

    rc = offload_net_send(tohost, type, buf, b_sz, 1);

    return rc;
}

/**
 * Send DONE or DONE_XERR op
 * It handles remote/local connectivity
 *
 */
int osql_send_commit(char *tohost, unsigned long long rqid, uuid_t uuid,
                     int nops, struct errstat *xerr, int type, SBUF2 *logsb,
                     struct client_query_stats *query_stats,
                     snap_uid_t *snap_info)
{
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    osql_done_rpl_t rpl_ok = {0};

    osql_done_xerr_t rpl_xerr = {0};
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

        if (logsb) {
            sbuf2printf(logsb, "[%llu] send OSQL_DONE %d %d\n", rqid, rc, nops);
            sbuf2flush(logsb);
        }

#if 0
      printf("Sending rqid=%llu tmp=%llu\n", rqid, osql_log_time());
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
        rc = offload_net_send(tohost, type, buf, b_sz, 1);

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
        rc = offload_net_send(tohost, type, buf, sizeof(rpl_xerr), 1);
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
        logmsg(LOGMSG_FATAL, "unhandled type %d\n", type);
        abort();
    }
}

int osql_send_commit_by_uuid(char *tohost, uuid_t uuid, int nops,
                             struct errstat *xerr, int type, SBUF2 *logsb,
                             struct client_query_stats *query_stats,
                             snap_uid_t *snap_info)
{
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    osql_done_uuid_rpl_t rpl_ok = {0};

    osql_done_xerr_uuid_t rpl_xerr = {0};
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

        if (logsb) {
            uuidstr_t us;
            sbuf2printf(logsb, "[%s] send OSQL_DONE %d %d\n",
                        comdb2uuidstr(uuid, us), rc, nops);
            sbuf2flush(logsb);
        }

#if 0
      printf("Sending rqid=%llu tmp=%llu\n", rqid, osql_log_time());
#endif

        if (!(p_buf = osqlcomm_done_uuid_rpl_put(&rpl_ok, p_buf, p_buf_end))) {
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
        rc = offload_net_send(tohost, type, buf, b_sz, 1);

    } else {

        rpl_xerr.hd.type = OSQL_XERR;
        comdb2uuidcpy(rpl_xerr.hd.uuid, uuid);

        memcpy(&rpl_xerr.dt, xerr, sizeof(rpl_xerr.dt));

        if (!osqlcomm_done_xerr_uuid_type_put(&rpl_xerr, p_buf, p_buf_end)) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_done_xerr_type_put");
            if (used_malloc)
                free(buf);
            return -1;
        }
        rc = offload_net_send(tohost, type, buf, sizeof(rpl_xerr), 1);
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

    if (comm) {
        net_send_decom_all(comm->handle_sibling, host);
    }

    return 0;
}

/**
 * Signalling sql thread to cancel the sql session
 * This is called only for blocksql, when the request fails to be dispatched
 *
 */
static void osql_blocksql_failed_dispatch(netinfo_type *netinfo_ptr,
                                          char *tohost, unsigned long long rqid,
                                          uuid_t uuid, int rc)
{

    osql_done_xerr_t rpl_xerr = {0};
    uint8_t buf[sizeof(rpl_xerr)];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end = p_buf + sizeof(rpl_xerr);

    rpl_xerr.hd.type = OSQL_XERR;
    rpl_xerr.hd.sid = rqid;

    rpl_xerr.dt.errval = rc;
    strncpy(rpl_xerr.dt.errstr, "cannot dispatch sql request",
            sizeof(rpl_xerr.dt.errstr));

    if (!(osqlcomm_done_xerr_type_put(&rpl_xerr, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                "osqlcomm_done_xerr_type_put");
        return;
    }

    offload_net_send(tohost, NET_OSQL_BLOCK_RPL, &buf, sizeof(rpl_xerr), 1);
}

/* We seem to be adding more and more things to be passed through osql requests.
   This
   packs multiple things into a request.  Note: buf must be a properly packed
   buffer (ie: network order) */
int osql_add_to_request(osql_req_t **reqp, int type, void *buf, int len)
{
    int newsize;
    void *rqbuf;
    int rqsz;
    struct osql_req_tail *tail;
    osql_req_t *req;

    req = *reqp;

    /* TODO: re-endianize */

    if (req->ntails == 255) {
        logmsg(LOGMSG_ERROR, "Too many tails\n");
        return -1;
    }

    req->ntails++;
    rqsz = req->rqlen + sizeof(struct osql_req_tail) + len;
    rqbuf = realloc(req, rqsz);
    if (rqbuf == NULL) {
        logmsg(LOGMSG_ERROR, "Failed to add tail to osql request, tail len %d\n",
                len);
        return -1;
    }
    req = rqbuf;
    tail = (struct osql_req_tail *)((char *)req + req->rqlen);
    tail->type = type;
    tail->len = len;
    memcpy(((char *)tail) + sizeof(struct osql_req_tail), buf, len);
    req->rqlen += sizeof(struct osql_req_tail) + len;

    *reqp = req;

    return 0;
}

/**
 * Constructs a reusable osql request
 *
 * This creates either a osql_req_t or a osql_req_uuid_t, as needed.
 *
 */
void *osql_create_request(const char *sql, int sqlen, int type,
                          unsigned long long rqid, uuid_t uuid, char *tzname,
                          int *prqlen, char *tag, void *tagbuf, int tagbuflen,
                          void *nullbits, int numnullbits, blob_buffer_t *blobs,
                          int numblobs, int queryid, int flags)
{
    int rqlen = 0;
    int taglen = 0;
    uint8_t *p_buf, *p_buf_end;
    int sz;
    short szs;
    int blobsizes[MAXBLOBS];
    int blobno;
    osql_req_t req = {0};
    osql_uuid_req_t req_uuid = {0};
    void *ret;

    if (rqid == OSQL_RQID_USE_UUID) {
        rqlen = sizeof(osql_uuid_req_t) + sqlen;
    } else {
        rqlen = sizeof(osql_req_t) + sqlen;
    }

    if (tag) {
        taglen = strlen(tag) + 1;
        rqlen += taglen + tagbuflen + numnullbits + 5 * sizeof(int);
        if (debug_switch_osql_force_local()) {
            rqlen += numblobs * sizeof(int);
            for (int i = 0; i < numblobs; i++) {
                rqlen += blobs[i].length;
            }
        }
    } else if (type == OSQL_BLOCK_REQ_COST)
        rqlen += sizeof(int);

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
        req_uuid.padding = 0;
        req_uuid.ntails = 0;
        comdb2uuidcpy(req_uuid.uuid, uuid);
        req_uuid.rqlen = rqlen;
        req_uuid.sqlqlen = sqlen;

        if (tzname)
            strncpy(r_uuid_ptr->tzname, tzname, sizeof(r_uuid_ptr->tzname));

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
        req.ntails = 0;
        req.rqid = rqid;
        req.rqlen = rqlen;
        req.sqlqlen = sqlen;

        if (tzname)
            strncpy(r_ptr->tzname, tzname, sizeof(r_ptr->tzname));

        p_buf = osqlcomm_req_type_put(&req, p_buf, p_buf_end);
        r_ptr->rqlen = rqlen;
    }

    p_buf = buf_no_net_put(sql, sqlen, p_buf, p_buf_end);
    if (type == OSQL_BLOCK_REQ_COST)
        p_buf = buf_put(&queryid, sizeof(queryid), p_buf, p_buf_end);
    else {
        if (tag) {
            /* this is dummy. */
            p_buf = buf_put(&queryid, sizeof(queryid), p_buf, p_buf_end);
        }
    }
    if (tag) {
        p_buf = buf_put(&taglen, sizeof(taglen), p_buf, p_buf_end);
        p_buf = buf_no_net_put(tag, taglen, p_buf, p_buf_end);
        p_buf = buf_put(&tagbuflen, sizeof(tagbuflen), p_buf, p_buf_end);
        if (tagbuflen != 0 && tagbuf != NULL) {
            p_buf = buf_no_net_put(tagbuf, tagbuflen, p_buf, p_buf_end);
        }
        p_buf = buf_put(&numnullbits, sizeof(numnullbits), p_buf, p_buf_end);
        p_buf = buf_no_net_put(nullbits, numnullbits, p_buf, p_buf_end);
        /* The blob part is already parsed and put in seperate buffers. */
        p_buf = buf_put(&numblobs, sizeof(numblobs), p_buf, p_buf_end);
        if (debug_switch_osql_force_local()) {
            for (int i = 0; i < numblobs; i++) {
                int length = blobs[i].length;
                p_buf = buf_put(&length, sizeof(length), p_buf, p_buf_end);
                p_buf = buf_no_net_put(blobs[i].data, blobs[i].length, p_buf,
                                       p_buf_end);
            }
        }
    }

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
void osql_set_net_poll(int pval) { net_set_poll(comm->handle_sibling, pval); }

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
    int retry;
    int net_type;

    stats[type].snd++;

    /* TODO: need to pass tag/tagbuf/tagbuflen here? */
    req = osql_create_request(sql, sqlen, type, rqid, uuid, tzname, &reqlen,
                              NULL, NULL, 0, NULL, 0, NULL, 0, 0, flags);
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

    rc = offload_net_send(tohost, net_type, req, reqlen, 1);

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
int osql_comm_signal_sqlthr_rc(sorese_info_t *sorese, struct errstat *xerr,
                               int rc)
{

    int irc = 0;
    int msglen = 0;
    int type;

    /* slightly kludgy - we're constructing one of 4 message types - get a
     * buffer
     * big enough for the biggest of them */
    uint8_t *buf;
    int max = 0;
    if (OSQLCOMM_DONE_XERR_UUID_RPL_LEN > max)
        max = OSQLCOMM_DONE_XERR_UUID_RPL_LEN;
    if (OSQLCOMM_DONE_UUID_RPL_LEN > max)
        max = OSQLCOMM_DONE_UUID_RPL_LEN;
    if (OSQLCOMM_DONE_XERR_RPL_LEN > max)
        max = OSQLCOMM_DONE_XERR_RPL_LEN;
    if (OSQLCOMM_DONE_RPL_LEN > max)
        max = OSQLCOMM_DONE_RPL_LEN;
    buf = alloca(max);

    /* test if the sql thread was the one closing the
       request, and if so, don't send anything back
       (request might be gone already anyway)
     */
    if (xerr->errval == SQLITE_ABORT)
        return 0;

    /* if error, lets send the error string */
    if (sorese->host) {
        /* remote */
        if (sorese->rqid == OSQL_RQID_USE_UUID) {
            osql_done_xerr_uuid_t rpl_xerr = {0};
            osql_done_uuid_rpl_t rpl_ok = {0};

            if (rc) {
                uint8_t *p_buf = buf;
                uint8_t *p_buf_end = buf + OSQLCOMM_DONE_XERR_UUID_RPL_LEN;
                rpl_xerr.hd.type = OSQL_XERR;
                comdb2uuidcpy(rpl_xerr.hd.uuid, sorese->uuid);
                rpl_xerr.dt = *xerr;

                osqlcomm_done_xerr_uuid_type_put(&(rpl_xerr), p_buf, p_buf_end);

                msglen = OSQLCOMM_DONE_XERR_UUID_RPL_LEN;

            } else {
                uint8_t *p_buf = buf;
                uint8_t *p_buf_end = buf + OSQLCOMM_DONE_UUID_RPL_LEN;

                rpl_ok.hd.type = OSQL_DONE;
                comdb2uuidcpy(rpl_ok.hd.uuid, sorese->uuid);
                rpl_ok.dt.rc = 0;
                rpl_ok.dt.nops = sorese->nops;

                osqlcomm_done_uuid_rpl_put(&(rpl_ok), p_buf, p_buf_end);

                msglen = OSQLCOMM_DONE_RPL_LEN;
            }
            type = osql_net_type_to_net_uuid_type(NET_OSQL_SIGNAL);
        } else {
            osql_done_xerr_t rpl_xerr = {0};
            osql_done_rpl_t rpl_ok = {0};

            if (rc) {
                uint8_t *p_buf = buf;
                uint8_t *p_buf_end = buf + OSQLCOMM_DONE_XERR_RPL_LEN;
                rpl_xerr.hd.type = OSQL_XERR;
                rpl_xerr.hd.sid = sorese->rqid;
                rpl_xerr.dt = *xerr;

                osqlcomm_done_xerr_type_put(&(rpl_xerr), p_buf, p_buf_end);

                msglen = OSQLCOMM_DONE_XERR_RPL_LEN;

            } else {
                uint8_t *p_buf = buf;
                uint8_t *p_buf_end = buf + OSQLCOMM_DONE_RPL_LEN;

                rpl_ok.hd.type = OSQL_DONE;
                rpl_ok.hd.sid = sorese->rqid;
                rpl_ok.dt.rc = 0;
                rpl_ok.dt.nops = sorese->nops;

                osqlcomm_done_rpl_put(&(rpl_ok), p_buf, p_buf_end);

                msglen = OSQLCOMM_DONE_RPL_LEN;
            }
            type = NET_OSQL_SIGNAL;
        }
#if 0
      printf("Send %d rqid=%llu tmp=%llu\n",  NET_OSQL_SIGNAL, sorese->rqid, osql_log_time());
#endif
        /* lazy again, works just because node!=0 */
        irc = offload_net_send(sorese->host, type, buf, msglen, 1);
        if (irc) {
            irc = -1;
            logmsg(LOGMSG_ERROR, "%s: error sending done to %s!\n", __func__,
                    sorese->host);
        }

    } else {
        /* local */

        irc = osql_chkboard_sqlsession_rc(sorese->rqid, sorese->uuid,
                                          sorese->nops, NULL, xerr);
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

/* the only hook inserting in the blkout list (i.e. reader_thread)
   times is written also by block proc threads
 */
static void net_osql_heartbeat(void *hndl, void *uptr, char *fromnode,
                               int usertype, void *dtap, int dtalen,
                               uint8_t is_tcp)
{

    osql_blknds_t *blk = NULL;
    int i = 0;

    /* if we are not the master, don't do nothing */
    if (!g_osql_ready || gbl_mynode != thedb->master)
        return;

    /* TODO: maybe look into the packet we got! */

    blk = &comm->blkout;

    /* OBSOLETE: this is brain-damaged scheme, but i want it out now.
       i need a write lock because I am adding nodes dynamically
       instead I should maybe add the nodes statically from the
       sanc list; on the other side, this makes easier moving one db
       from one cluster to another
       UPDATE: I am not offloading to nodes not present into this list
       therefore I will not blackout nodes not present here. Therefore this
       is the only writer, and it is safe to add an entry lockless as long as
       blk->n is the last to be updated.
     */
    for (i = 0; i < blk->n; i++) {
        if (blk->nds[i] == fromnode) {
            blk->heartbeat[i] = time(NULL);
            break;
        }
    }
    if (i == blk->n) {
        /* not present, simply add it */
        blk->nds[i] = fromnode;
        blk->heartbeat[i] = time(NULL);
        blk->times[i] = 0;
        blk->n++;
    }
}

/* remote poke */
static void net_osql_poked(void *hndl, void *uptr, char *fromhost, int usertype,
                           void *dtap, int dtalen, uint8_t is_tcp)
{
    uint8_t *p_buf = dtap;
    uint8_t *p_buf_end = p_buf + dtalen;
    osql_poke_t poke;
    int found = 0;
    osql_sess_t *sess = 0;
    int rc = 0;
    uuid_t uuid;

    comdb2uuid_clear(uuid);

    if (thedb->exiting || thedb->stopped) {
        /* don't do anything, we're going down */
        return;
    }

    if (!(osqlcomm_poke_type_get(&poke, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: invalid data length\n", __func__);
        return;
    }

    found = osql_chkboard_sqlsession_exists(poke.rqid, uuid, 1);

    if (!found) {

        /* send a done with an error, lost request */
        uint8_t buf[OSQLCOMM_DONE_XERR_RPL_LEN];
        uint8_t *p_buf = buf, *p_buf_end = buf + OSQLCOMM_DONE_XERR_RPL_LEN;
        netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
        osql_done_xerr_t rpl;

        rpl.hd.type = OSQL_XERR;
        rpl.hd.sid = poke.rqid;
        rpl.dt.errval = OSQL_NOOSQLTHR;
        uuidstr_t us;
        /* TODO:NOENV:uuid - should come from new msg type */
        snprintf((char *)&rpl.dt.errstr, sizeof(rpl.dt.errstr),
                 "Missing sql session %llx %s on %s\n", poke.rqid,
                 comdb2uuidstr(uuid, us), gbl_mynode);

        osqlcomm_done_xerr_type_put(&rpl, p_buf, p_buf_end);

        if ((rc = offload_net_send(fromhost, NET_OSQL_BLOCK_RPL, buf,
                                   sizeof(buf), 1))) {
            logmsg(LOGMSG_ERROR, 
                    "%s: error writting record to master in offload mode rc=%d!\n",
                    __func__, rc);
        }

    } else {
        /* TODO: we could send something back... but in tough times this will
         * not make it nevertheless */
    }
}

static void net_osql_poked_uuid(void *hndl, void *uptr, char *fromhost,
                                int usertype, void *dtap, int dtalen,
                                uint8_t is_tcp)
{
    uint8_t *p_buf = dtap;
    uint8_t *p_buf_end = p_buf + dtalen;
    osql_poke_uuid_t poke;
    int found = 0;
    osql_sess_t *sess = 0;
    int rc = 0;

    if (thedb->exiting || thedb->stopped) {
        /* don't do anything, we're going down */
        return;
    }

    if (!(osqlcomm_poke_uuid_type_get(&poke, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: invalid data length\n", __func__);
        return;
    }

    found = osql_chkboard_sqlsession_exists(OSQL_RQID_USE_UUID, poke.uuid, 1);

    if (!found) {

        /* send a done with an error, lost request */
        uint8_t buf[OSQLCOMM_DONE_XERR_UUID_RPL_LEN];
        uint8_t *p_buf = buf,
                *p_buf_end = buf + OSQLCOMM_DONE_XERR_UUID_RPL_LEN;
        netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
        osql_done_xerr_uuid_t rpl;

        rpl.hd.type = OSQL_XERR;
        comdb2uuidcpy(rpl.hd.uuid, poke.uuid);
        rpl.dt.errval = OSQL_NOOSQLTHR;
        uuidstr_t us;

        snprintf((char *)&rpl.dt.errstr, sizeof(rpl.dt.errstr),
                 "Missing sql session %s on %s\n", comdb2uuidstr(poke.uuid, us),
                 gbl_mynode);

        osqlcomm_done_xerr_uuid_type_put(&rpl, p_buf, p_buf_end);

        if ((rc = offload_net_send(fromhost, NET_OSQL_BLOCK_RPL_UUID, buf,
                                   sizeof(buf), 1))) {
            logmsg(LOGMSG_ERROR, 
                   "%s: error writting record to master in offload mode rc=%d!\n",
                   __func__, rc);
        }

    } else {
        /* TODO: we could send something back... but in tough times this will
         * not make it nevertheless */
    }
}

static void net_osql_master_check(void *hndl, void *uptr, char *fromhost,
                                  int usertype, void *dtap, int dtalen,
                                  uint8_t is_tcp)
{
    uint8_t *p_buf = dtap;
    uint8_t *p_buf_end = p_buf + dtalen;
    osql_poke_t poke;
    osql_poke_uuid_t pokeuuid;
    int found = 0;
    int rc = 0;

    uuid_t uuid;
    unsigned long long rqid = OSQL_RQID_USE_UUID;
    int reply_type;

    comdb2uuid_clear(uuid);

    if (thedb->exiting || thedb->stopped) {
        /* don't do anything, we're going down */
        return;
    }

    if (usertype > NET_OSQL_UUID_REQUEST_MIN ||
        usertype < NET_OSQL_UUID_REQUEST_MAX) {
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

    uuidstr_t us;
    found = osql_repository_session_exists(rqid, uuid);

    if (found) {
        uint8_t buf[OSQLCOMM_EXISTS_RPL_TYPE_LEN];
        uint8_t bufuuid[OSQLCOMM_EXISTS_UUID_RPL_TYPE_LEN];
        uint8_t *p_buf;
        netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;

        if (rqid == OSQL_RQID_USE_UUID) {
            p_buf = bufuuid;
            p_buf_end = p_buf + OSQLCOMM_EXISTS_UUID_RPL_TYPE_LEN;
            osql_exists_uuid_rpl_t rpl = {0};

            rpl.hd.type = OSQL_EXISTS;
            comdb2uuidcpy(rpl.hd.uuid, uuid);
            rpl.dt.status = 0;
            rpl.dt.timestamp = time_epoch();

            if (!osqlcomm_exists_uuid_rpl_type_put(&rpl, p_buf, p_buf_end))
                abort();
            reply_type = NET_OSQL_MASTER_CHECKED_UUID;

            if ((rc = offload_net_send(fromhost, reply_type, bufuuid,
                                       sizeof(bufuuid), 1))) {
                logmsg(LOGMSG_ERROR, "%s: error writting record to master in "
                                "offload mode rc=%d!\n",
                        __func__, rc);
            }
        } else {
            p_buf = buf;
            p_buf_end = p_buf + OSQLCOMM_EXISTS_RPL_TYPE_LEN;

            /* send a done with an error, lost request */
            osql_exists_rpl_t rpl = {0};

            rpl.hd.type = OSQL_EXISTS;
            rpl.hd.sid = rqid;
            rpl.dt.status = 0;
            rpl.dt.timestamp = time_epoch();

            if (!osqlcomm_exists_rpl_type_put(&rpl, p_buf, p_buf_end))
                abort();

            reply_type = NET_OSQL_MASTER_CHECKED;

            if ((rc = offload_net_send(fromhost, reply_type, buf, sizeof(buf),
                                       1))) {
                logmsg(LOGMSG_ERROR, "%s: error writting record to master in "
                                "offload mode rc=%d!\n",
                        __func__, rc);
            }
        }

    } else {
        uuidstr_t us;
        logmsg(LOGMSG_ERROR, "Missing SORESE sql session %llx %s on %u from %d\n",
                poke.rqid, comdb2uuidstr(uuid, us), gbl_mynode, poke.from);
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

    if (thedb->exiting || thedb->stopped) {
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
        logmsg(LOGMSG_ERROR, "%s: failed to update status for rqid %d %s rc=%d\n",
                __func__, rqid, comdb2uuidstr(uuid, us), rc);
    }
}

/* terminate node */
static int net_osql_nodedwn(netinfo_type *netinfo_ptr, char *host)
{

    int rc = 0;

    /* this is mainly for master, but we might not be a master anymore at
       this point
    */
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

    if (g_osql_ready && thedb->master == gbl_mynode) {
        logmsg(LOGMSG_INFO, "%s: canceling pending blocksql transactions\n",
                __func__);
        osql_repository_cancelall();
    }
}

/*
   thread responsible for sending heartbeats to the master
 */
static void *osql_heartbeat_thread(void *arg)
{

    int rc = 0;
    hbeat_t msg;

    thread_started("osql heartbeat");

    while (1) {
        uint8_t buf[OSQLCOMM_HBEAT_TYPE_LEN],
            *p_buf = buf, *p_buf_end = (buf + OSQLCOMM_HBEAT_TYPE_LEN);

        /* we get away with setting source and destination to 0 since the
         * callback code
         * doesn't actually care - it just needs heartbeats, but doesn't look at
         * the contents */
        msg.dst = 0;
        msg.src = 0;
        msg.time = time_epoch();

        osqlcomm_hbeat_type_put(&(msg), p_buf, p_buf_end);

        if (g_osql_ready)
            rc =
                net_send_message(comm->handle_sibling, thedb->master,
                                 NET_HBEAT_SQL, &buf, sizeof(buf), 0, 5 * 1000);

        poll(NULL, 0, gbl_osql_heartbeat_send * 1000);
    }
    return NULL;
}

/* this function routes the packet in the case of local communication */
static int net_local_route_packet(int usertype, void *data, int datalen)
{
    switch (usertype) {
    case NET_OSQL_BLOCK_RPL:
    case NET_OSQL_BLOCK_RPL_UUID:
        net_osql_rpl(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_OSQL_SOCK_REQ:
    case NET_OSQL_SOCK_REQ_COST:
    case NET_OSQL_SOCK_REQ_UUID:
    case NET_OSQL_SOCK_REQ_COST_UUID:
        net_sosql_req(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_OSQL_SOCK_RPL:
    case NET_OSQL_SOCK_RPL_UUID:
        net_osql_rpl(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_OSQL_RECOM_REQ:
    case NET_OSQL_RECOM_REQ_UUID:
        net_recom_req(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_OSQL_RECOM_RPL:
    case NET_OSQL_RECOM_RPL_UUID:
        net_osql_rpl(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_OSQL_SNAPISOL_REQ:
    case NET_OSQL_SNAPISOL_REQ_UUID:
        net_snapisol_req(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_OSQL_SNAPISOL_RPL:
    case NET_OSQL_SNAPISOL_RPL_UUID:
        net_osql_rpl(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_OSQL_SERIAL_REQ:
    case NET_OSQL_SERIAL_REQ_UUID:
        net_serial_req(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_OSQL_SERIAL_RPL:
    case NET_OSQL_SERIAL_RPL_UUID:
        net_osql_rpl(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_BLOCK_REQ:
        net_block_req(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    case NET_BLOCK_REPLY:
        net_block_reply(NULL, NULL, 0, usertype, data, datalen, 0);
        break;
    default:
        logmsg(LOGMSG_ERROR, "%s: unknown packet type routed locally, %d\n",
                __func__, usertype);
        return -1;
    }
    return 0;
}

/* this function routes the packet in the case of local communication
   include in this function only "usertype"-s that can have a tail
 */
static int net_local_route_packet_tails(int usertype, void *data, int datalen,
                                        int numtails, void **tails,
                                        int *taillens)
{
    switch (usertype) {
    case NET_OSQL_BLOCK_RPL:
    case NET_OSQL_SOCK_RPL:
    case NET_OSQL_RECOM_RPL:
    case NET_OSQL_SNAPISOL_RPL:
    case NET_OSQL_SERIAL_RPL:
    case NET_OSQL_BLOCK_RPL_UUID:
    case NET_OSQL_SOCK_RPL_UUID:
    case NET_OSQL_RECOM_RPL_UUID:
    case NET_OSQL_SNAPISOL_RPL_UUID:
    case NET_OSQL_SERIAL_RPL_UUID:
        return net_osql_rpl_tail(NULL, NULL, 0, usertype, data, datalen,
                                 tails[0], taillens[0]);
    default:
        logmsg(LOGMSG_ERROR, "%s: unknown packet type routed locally, %d\n",
                __func__, usertype);
        return -1;
    }
    return 0;
}

int osql_comm_check_bdb_lock(void)
{
    int rc = 0;

    /* check here if we need to wait for the lock, so we don't prevent this from
     * happening */
    if (bdb_lock_desired(thedb->bdb_env)) {
        struct sql_thread *thd = pthread_getspecific(query_info_key);
        if (!thd) return 0;

        struct sqlclntstate *clnt = thd->sqlclntstate;
        int sleepms;

        logmsg(LOGMSG_DEBUG, "%s bdb_lock_desired so calling recover_deadlock\n",
                __func__);

        /* scale by number of times we try, cap at 10 seconds */
        sleepms = 100 * clnt->deadlock_recovered;
        if (sleepms > 10000)
            sleepms = 10000;

        rc = recover_deadlock(thedb->bdb_env, thd, NULL, sleepms);

        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s recover_deadlock returned %d\n",
                    __func__, rc);
            return -1;
        }
        logmsg(LOGMSG_DEBUG, "%s recovered deadlock\n", __func__);

        clnt->deadlock_recovered++;

        if (clnt->deadlock_recovered > 100)
            return -1;
    }
    return 0;
}

/* this wrapper tries to provide a reliable net_send that will prevent loosing
   packets
   due to queue being full */
static int offload_net_send(char *host, int usertype, void *data, int datalen,
                            int nodelay)
{
    netinfo_type *netinfo_ptr = comm->handle_sibling;
    int backoff = gbl_osql_bkoff_netsend;
    int total_wait = backoff;
    int unknownerror_retry = 0;
    int rc = -1;

    if (debug_switch_osql_simulate_send_error()) {
        if (rand() % 4 == 0) /*25 chance of failure*/
        {
            logmsg(LOGMSG_ERROR, "Punting offload_net_send with error -1\n");
            return -1;
        }
    }

    if (host == gbl_mynode)
        host = NULL;

    if (host) {

        /* remote send */
        while (rc) {
            int rc2;
#if 0
         printf("NET SEND %d tmp=%llu\n", usertype, osql_log_time());
#endif
            rc = net_send(netinfo_ptr, host, usertype, data, datalen, nodelay);

            if (NET_SEND_FAIL_QUEUE_FULL == rc ||
                NET_SEND_FAIL_MALLOC_FAIL == rc || NET_SEND_FAIL_NOSOCK == rc) {

                if (total_wait > gbl_osql_bkoff_netsend_lmt) {
                    logmsg(LOGMSG_ERROR, "%s:%d giving up sending to %s\n", __FILE__,
                            __LINE__, host);
                    return -1;
                }

                if (rc2 = osql_comm_check_bdb_lock()) {
                    logmsg(LOGMSG_ERROR, "%s:%d giving up sending to %s\n", __FILE__,
                            __LINE__, host);
                    return rc;
                }

                poll(NULL, 0, backoff);
                /*backoff *= 2; */
                total_wait += backoff;
            } else if (NET_SEND_FAIL_CLOSED == rc) {
                /* on closed sockets, we simply return; a callback
                   will trigger on the other side signalling we've
                   lost the comm party */
                return rc;
            } else if (rc) {
                unknownerror_retry++;
                if (unknownerror_retry >= UNK_ERR_SEND_RETRY) {
                    logmsg(LOGMSG_ERROR, "%s:%d giving up sending to %s\n", __FILE__,
                            __LINE__, host);
                    comdb2_linux_cheap_stack_trace();
                    return -1;
                }
            }
        }
    } else {

        /* local save, this is calling sorese_rvprpl_master */
        net_local_route_packet(usertype, data, datalen);
        rc = 0;
    }

    return rc;
}

static int offload_net_send_tails(char *tohost, int usertype, void *data,
                                  int datalen, int nodelay, int ntails,
                                  void **tails, int *tailens);

/* this wrapper tries to provide a reliable net_send_tail that will prevent
   loosing packets
   due to queue being full */
static int offload_net_send_tail(char *host, int usertype, void *data,
                                 int datalen, int nodelay, void *tail,
                                 int tailen)
{
    return offload_net_send_tails(host, usertype, data, datalen, nodelay, 1,
                                  &tail, &tailen);
}

static int offload_net_send_tails(char *host, int usertype, void *data,
                                  int datalen, int nodelay, int ntails,
                                  void **tails, int *tailens)
{

    netinfo_type *netinfo_ptr = comm->handle_sibling;
    int backoff = gbl_osql_bkoff_netsend;
    int total_wait = backoff;
    int unknownerror_retry = 0;
    int rc = -1;

    while (rc) {
        if (host == gbl_mynode)
            host = NULL;

        if (host) {
            /* remote send */
            rc = net_send_tails(netinfo_ptr, host, usertype, data, datalen,
                                nodelay, ntails, tails, tailens);
        } else {
            /* local save */
            rc = net_local_route_packet_tails(usertype, data, datalen, ntails,
                                              tails, tailens);
        }

        if (NET_SEND_FAIL_QUEUE_FULL == rc || NET_SEND_FAIL_MALLOC_FAIL == rc) {

            if (total_wait > gbl_osql_bkoff_netsend_lmt) {
                logmsg(LOGMSG_ERROR, "%s giving up sending to %s\n", __func__, host);
                return -1;
            }

            if ((rc = osql_comm_check_bdb_lock()))
                return rc;

            poll(NULL, 0, backoff);
            /*backoff *= 2; */
            total_wait += backoff;
        } else if (rc) {
            unknownerror_retry++;
            if (unknownerror_retry >= UNK_ERR_SEND_RETRY)
                return -1;
        }
    }

    return rc;
}

/* we redefine this function as more osql versions are added */
static int osql_check_version(int type)
{
    return type != OSQL_BLOCK_REQ && type != OSQL_SOCK_REQ;
}

/* reader
   because it we don't delete entries
   this is lockless

   we add nodes that have a recent heartbeat
   and are not marked as blackout
 */
static int get_blkout(time_t now, char *nodes[REPMAX], int *nds)
{

    osql_blknds_t *blk = NULL;
    int i = 0, j = 0;
    int len = 0;

    if (!comm)
        return -1;

    blk = &comm->blkout;

    len = blk->n;

    for (i = 0; i < len; i++) {
        if ((now - blk->heartbeat[i] < blk->delta_hbeat) &&
            (now - blk->times[i] >= blk->delta)) {
            nodes[j++] = blk->nds[i];
        }
    }

    *nds = j;

#if 0
   printf("%s: got %d nodes: ", __func__, *nds);
   for(i=0;i<*nds;i++)
      printf("%d ", nodes[i]);
   printf("\n");
#endif

    return 0;
}

/* Replicant callback.  Note: this is a bit kludgy.
   If we get called remotely for a parametrized request
   the ntails/tails/taillens parameters are invalid and
   must be parsed from the buffer.
 */

static void net_osql_rpl(void *hndl, void *uptr, char *fromnode, int usertype,
                         void *dtap, int dtalen, uint8_t is_tcp)
{

    int found = 0;
    int rc = 0;
    uuid_t uuid;
    unsigned long long rqid;
    uint8_t *p_buf, *p_buf_end;
    p_buf = (uint8_t *)dtap;
    p_buf_end = (p_buf + dtalen);

    stats[netrpl2req(usertype)].rcv++;

    if (usertype >= NET_OSQL_UUID_REQUEST_MIN &&
        usertype < NET_OSQL_UUID_REQUEST_MAX) {
        osql_uuid_rpl_t p_osql_uuid_rpl;

        rqid = OSQL_RQID_USE_UUID;
        if (!(p_buf = (uint8_t *)osqlcomm_uuid_rpl_type_get(
                  &p_osql_uuid_rpl, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_uuid_rpl_type_get");
            rc = -1;
        } else
            comdb2uuidcpy(uuid, p_osql_uuid_rpl.uuid);
        uuidstr_t us;
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
        } else
            rqid = p_osql_rpl.sid;
    }

#ifdef TEST_OSQL
    fprintf(stdout, "%s: calling sorese_rcvrpl type=%d sid=%llu\n", __func__,
            netrpl2req, ((osql_rpl_t *)dtap)->sid);
#endif
#if 0
    printf("NET RPL rqid=%llu tmp=%llu\n", ((osql_rpl_t*)dtap)->sid, osql_log_time());
#endif

    if (rc == 0)
        rc = osql_sess_rcvop(rqid, uuid, dtap, dtalen, &found);
    if (rc)
        stats[netrpl2req(usertype)].rcv_failed++;
    if (!found)
        stats[netrpl2req(usertype)].rcv_rdndt++;
}

static int check_master(char *tohost)
{

    char *destnode = (!tohost) ? gbl_mynode : tohost;
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
    int rc = 0;
    int found = 0;
    uint8_t *p_buf, *p_buf_end;
    uuid_t uuid;

    if (dtalen + tailen > gbl_blob_sz_thresh_bytes)
        dup = comdb2_bmalloc(blobmem, dtalen + tailen);
    else
        dup = malloc(dtalen + tailen);

    stats[netrpl2req(usertype)].rcv++;

#ifdef TEST_OSQL
    fprintf(stdout, "%s: calling sorese_rcvrpl type=%d sid=%llu\n", __func__,
            netrpl2req(usertype), ((osql_rpl_t *)dtap)->sid);
#endif

    if (!dup) {
        logmsg(LOGMSG_FATAL, 
                "%s: master running out of memory! unable to alloc %d bytes\n",
                __func__, dtalen + tailen);
        abort();
        /*rc = NET_SEND_FAIL_MALLOC_FAIL;*/
    } else {

        memmove(dup, dtap, dtalen);
        memmove((char *)dup + dtalen, tail, tailen);

        p_buf = (uint8_t *)dup;
        p_buf_end = p_buf + dtalen;
        if (usertype >= NET_OSQL_UUID_REQUEST_MIN &&
            usertype < NET_OSQL_UUID_REQUEST_MAX) {
            osql_uuid_rpl_t p_osql_rpl;
            unsigned long long rqid;

            if (!(p_buf = (uint8_t *)osqlcomm_uuid_rpl_type_get(
                      &p_osql_rpl, p_buf, p_buf_end))) {
                logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                        "osqlcomm_rpl_type_get");
                rc = -1;
            } else {
                comdb2uuidcpy(uuid, p_osql_rpl.uuid);
                rqid = OSQL_RQID_USE_UUID;
                rc = osql_sess_rcvop(rqid, uuid, dup, dtalen + tailen, &found);
            }

        } else {
            osql_rpl_t p_osql_rpl;
            comdb2uuid_clear(uuid);

            if (!(p_buf = (uint8_t *)osqlcomm_rpl_type_get(&p_osql_rpl, p_buf,
                                                           p_buf_end))) {
                logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                        "osqlcomm_rpl_type_get");
                rc = -1;
            } else {
                rc = osql_sess_rcvop(p_osql_rpl.sid, uuid, dup, dtalen + tailen,
                                     &found);
            }
        }

        free(dup);
    }

    if (rc)
        stats[netrpl2req(usertype)].rcv_failed++;
    if (!found)
        stats[netrpl2req(usertype)].rcv_rdndt++;

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
                             struct block_err *err, void *data, int idxerr)
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
                  (char *)data);
        ret = rc;
        break;

    case SQLITE_TOOBIG:
        reqerrstr(iq, COMDB2_BLK_RC_NB_REQS, "transaction too big");
        ret = ERR_TRAN_TOO_BIG;
        break;

    case ERR_SQL_PREP:
        reqerrstr(iq, FSQL_PREPARE, "sql query syntax error");
        ret = FSQL_PREPARE;
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

enum { OSQL_PROCESS_FLAGS_BLOB_OPTIMIZATION = 0x00000001 };

static inline int is_write_request(int type)
{
    switch (type) {
    case OSQL_DBQ_CONSUME:
    case OSQL_DBQ_CONSUME_UUID:
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
/**
 * Handles each packet and calls record.c functions
 * to apply to received row updates
 *
 */
int osql_process_packet(struct ireq *iq, unsigned long long rqid, uuid_t uuid,
                        void *trans, char *msg, int msglen, int *flags,
                        int **updCols, blob_buffer_t blobs[MAXBLOBS], int step,
                        struct block_err *err, int *receivedrows, SBUF2 *logsb)
{
    const uint8_t *p_buf;
    const uint8_t *p_buf_end;
    int rc = 0;
    int ii;
    struct dbtable *db =
        (iq->usedb) ? iq->usedb : thedb->dbs[0]; /*add to first if no usedb*/
    const unsigned char tag_name_ondisk[] = ".ONDISK";
    const size_t tag_name_ondisk_len = 8 /*includes NUL*/;
    int type;
    uuidstr_t us;

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

#if 0
   if(logsb)
      sbuf2printf(logsb, "processing: %x\n", msg);
#endif

    /*fprintf(stderr,"rpl type is %d msg=%x\n",rpl.type, msg);*/

    if (type >= 0 && type < MAX_OSQL_TYPES)
        db->blockosqltypcnt[type]++;
    else
        db->blockosqltypcnt[0]++; /* invalids */

    /* throttle blocproc on master if replication threads backing up */
    if (gbl_toblock_net_throttle && is_write_request(type))
        net_throttle_wait(thedb->handle_sibling);

    switch (type) {
    case OSQL_DONE:
    case OSQL_DONE_SNAP:
    case OSQL_DONE_STATS: {
        p_buf_end = p_buf + sizeof(osql_done_t);
        osql_done_t dt = {0};

        p_buf = osqlcomm_done_type_get(&dt, p_buf, p_buf_end);

        if (logsb) {
            sbuf2printf(logsb, "[%llu %s] OSQL_DONE %d %d\n", rqid,
                        comdb2uuidstr(uuid, us), dt.nops, dt.rc);
            sbuf2flush(logsb);
        }

        /* just in case */
        free_blob_buffers(blobs, MAXBLOBS);

        iq->sc = iq->sc_pending;
        while (iq->sc != NULL) {
            void *ptran = bdb_get_physical_tran(trans);
            if (strcmp(iq->sc->original_master_node, gbl_mynode) != 0) {
                return -1;
            }
            if (iq->sc->db) iq->usedb = iq->sc->db;
            rc = finalize_schema_change(iq, ptran);
            if (rc != SC_OK) {
                return rc; // Change to failed schema change error;
            }
            iq->sc = iq->sc->sc_next;
        }

        // TODO Notify all bpfunc of success

        /* dt->nops carries the possible conversion error index */
        rc = conv_rc_sql2blkop(iq, step, -1, dt.rc, err, NULL, dt.nops);

        if (type == OSQL_DONE_SNAP) {
            snap_uid_t snap_info;
            assert(iq->have_snap_info == 1);

            p_buf_end = (const uint8_t *)msg + msglen;
            p_buf = snap_uid_get(&snap_info, p_buf, p_buf_end);
            iq->have_snap_info = 1;

            assert(!memcmp(&snap_info, iq->snap_info, sizeof(snap_uid_t)));
        }

        /* p_buf is pointing at client_query_stats if there is one */
        if (type == OSQL_DONE_STATS) /* Never set anywhere. */
        {
            dump_client_query_stats_packed(iq->dbglog_file, p_buf);
        }
#if 0
            I THINK THIS IS ALREADY DONE;
            MAYBE WE SHOULD CHECK THAT NOPS==RECEIVEDROWS
            if(!rc && dt->nops)
               *nops = dt->nops; /* UPDATE are counted differently; bpstat->receivedrows;*/
            else
               *nops = *receivedrows; /*pure offloading let row countng on srvr side*/
#endif

        return rc ? rc : OSQL_RC_DONE; /* signal caller done processing this
                                          request */
    }
    case OSQL_USEDB: {
        osql_usedb_t dt;
        p_buf_end = (uint8_t *)p_buf + sizeof(osql_usedb_t);
        char *tablename;

        tablename = (char *)osqlcomm_usedb_type_get(&dt, p_buf, p_buf_end);

        if (logsb) {
            sbuf2printf(logsb, "[%llu %s] OSQL_USEDB %*.s\n", rqid,
                        comdb2uuidstr(uuid, us), dt.tablenamelen, tablename);
            sbuf2flush(logsb);
        }

        if ( is_tablename_queue(tablename, strlen(tablename)) ) {
            iq->usedb = getqueuebyname(tablename);
        } else {
            iq->usedb = get_dbtable_by_name(tablename);
            iq->usedbtablevers = dt.tableversion;
        }
        if (iq->usedb == NULL) {
            iq->usedb = iq->origdb;
            logmsg(LOGMSG_ERROR, "%s: unable to get usedb for table %.*s\n",
                    __func__, dt.tablenamelen, tablename);

            return conv_rc_sql2blkop(iq, step, -1, ERR_NO_SUCH_TABLE, err,
                                     tablename, 0);
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

    case OSQL_DBQ_CONSUME_UUID: {
        osql_dbq_consume_uuid_t *dt = (osql_dbq_consume_uuid_t *)msg;
        if ((rc = dbq_consume_genid(iq, trans, 0, dt->genid)) != 0) {
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

        if (logsb) {
            unsigned long long lclgenid = bdb_genid_to_host_order(dt.genid);
            sbuf2printf(logsb, "[%llu %s] %s %llx (2:%lld)\n", rqid,
                        comdb2uuidstr(uuid, us),
                        recv_dk ? "OSQL_DELETE" : "OSQL_DELREC", lclgenid,
                        lclgenid);
            sbuf2flush(logsb);
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

        rc = del_record(iq, trans, NULL, 0, dt.genid, dt.dk, &err->errcode,
                        &err->ixnum, BLOCK2_DELKL, 0);

        if (iq->idxInsert || iq->idxDelete) {
            free_cached_idx(iq->idxInsert);
            free_cached_idx(iq->idxDelete);
            free(iq->idxInsert);
            free(iq->idxDelete);
            iq->idxInsert = iq->idxDelete = NULL;
        }

        if (rc != 0) {
            if (rc != RC_INTERNAL_RETRY) {
                reqerrstr(iq, COMDB2_DEL_RC_INVL_KEY,
                          "unable to delete genid =%llx rc=%d",
                          bdb_genid_to_host_order(dt.genid), rc);
            }

            return rc; /*this is blkproc rc */
        }

        (*receivedrows)++;
    } break;
    case OSQL_UPDSTAT: {
        /* this opcode was used to insert/update into btree new stat1/2/4 record
         * but since we changed the way we backup stats (used to be in llmeta)
         * this opcode is only used to reload stats now
         */
        bset(&iq->osql_flags, OSQL_FLAGS_ANALYZE);
    } break;
    case OSQL_INSREC:
    case OSQL_INSERT: {
        osql_ins_t dt;
        unsigned char *pData = NULL;
        int rrn = 0;
        unsigned long long genid = 0;
        int recv_dk = (type == OSQL_INSERT);

        const uint8_t *p_buf_end;
        if (recv_dk)
            p_buf_end = p_buf + sizeof(osql_ins_t);
        else
            p_buf_end = p_buf + sizeof(osql_ins_t) - sizeof(unsigned long long);

        int addflags;

        pData =
            (uint8_t *)osqlcomm_ins_type_get(&dt, p_buf, p_buf_end, recv_dk);

        if (!recv_dk)
            dt.dk = -1ULL;

        if (logsb) {
            int jj = 0;
            sbuf2printf(logsb, "[%llu %s] %s [\n", rqid,
                        comdb2uuidstr(uuid, us),
                        recv_dk ? "OSQL_INSERT" : "OSQL_INSREC");
            for (jj = 0; jj < dt.nData; jj++)
                sbuf2printf(logsb, "%02x", pData[jj]);

            sbuf2printf(logsb, "\n] -> ");
        }

        addflags = RECFLAGS_DYNSCHEMA_NULLS_ONLY;
        if (osql_get_delayed(iq) == 0 && iq->usedb->n_constraints == 0 &&
            gbl_goslow == 0) {
            addflags |= RECFLAGS_NO_CONSTRAINTS;
        } else {
            osql_set_delayed(iq);
        }

        rc = add_record(iq, trans, tag_name_ondisk,
                        tag_name_ondisk + tag_name_ondisk_len, /*tag*/
                        pData, pData + dt.nData,               /*dta*/
                        NULL,            /*nulls, no need as no
                                           ctag2stag is called */
                        blobs, MAXBLOBS, /*blobs*/
                        &err->errcode, &err->ixnum, &rrn, &genid, /*new id*/
                        dt.dk, BLOCK2_ADDKL, step,
                        addflags); /* do I need this?*/
        free_blob_buffers(blobs, MAXBLOBS);
        if (iq->idxInsert || iq->idxDelete) {
            free_cached_idx(iq->idxInsert);
            free_cached_idx(iq->idxDelete);
            free(iq->idxInsert);
            free(iq->idxDelete);
            iq->idxInsert = iq->idxDelete = NULL;
        }

        if (logsb) {
            unsigned long long lclgenid = bdb_genid_to_host_order(genid);
            sbuf2printf(logsb, " %llx (%d:%lld)\n", lclgenid, rrn, lclgenid);
            sbuf2flush(logsb);
        }

        if (rc != 0) {
            if (err->errcode == OP_FAILED_UNIQ) {
                /* this can happen if we're skipping delayed key adds */
                reqerrstr(iq, COMDB2_CSTRT_RC_DUP, "add key constraint "
                                                   "duplicate key '%s' on "
                                                   "table '%s' index %d",
                          get_keynm_from_db_idx(iq->usedb, err->ixnum),
                          iq->usedb->dbname, err->ixnum);
            } else if (rc != RC_INTERNAL_RETRY) {
                reqerrstr(iq, COMDB2_ADD_RC_INVL_KEY,
                          "unable to add record rc = %d", rc);
            }

            if (logsb)
                sbuf2printf(logsb,
                            "Added new record failed, rrn = %d, genid=%llx\n",
                            rrn, bdb_genid_to_host_order(genid));

            return rc; /*this is blkproc rc */
        } else {
            if (logsb)
                sbuf2printf(logsb, "Added new record rrn = %d, genid=%llx\n",
                            rrn, bdb_genid_to_host_order(genid));
        }

        (*receivedrows)++;
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

        if (logsb) {
            int jj = 0;
            sbuf2printf(
                logsb, "[%llu %s] OSQL_UPDREC rrn = %d, genid = %llx[\n", rqid,
                comdb2uuidstr(uuid, us), rrn, bdb_genid_to_host_order(genid));
            for (jj = 0; jj < dt.nData; jj++)
                sbuf2printf(logsb, "%02x", pData[jj]);

            sbuf2printf(logsb, "\n] -> ");
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

        /* Sanity check the osql blob optimization. */
        if (*flags & OSQL_PROCESS_FLAGS_BLOB_OPTIMIZATION) {
            int ncols;

            assert(*updCols != NULL);
            ncols = (*updCols)[0];

            /* Make sure this is sane before sending to upd_record. */
            for (ii = 0; ii < MAXBLOBS; ii++) {
                if (-2 == blobs[ii].length) {
                    int idx = get_schema_blob_field_idx(iq->usedb->dbname,
                                                        ".ONDISK", ii);
                    assert(idx < ncols);
                    assert(-1 == (*updCols)[idx + 1]);
                }
            }
        }

        rc = upd_record(
            iq, trans, NULL, rrn, genid, tag_name_ondisk,
            tag_name_ondisk + tag_name_ondisk_len, /*tag*/
            pData, pData + dt.nData,               /* rec */
            NULL, NULL,                            /* vrec */
            NULL,                                  /*nulls, no need as no
                                                     ctag2stag is called */
            *updCols, blobs, MAXBLOBS, &genid, dt.ins_keys, dt.del_keys,
            &err->errcode, &err->ixnum, BLOCK2_UPDKL, step,
            RECFLAGS_DYNSCHEMA_NULLS_ONLY |
                RECFLAGS_DONT_SKIP_BLOBS /* because we only receive info about
                                            blobs that should exist in the new
                                            record, override the update
                                            function's default behaviour and
                                            have
                                            it erase any blobs that haven't been
                                            collected. */
            );

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

        if (logsb) {
            unsigned long long lclgenid = bdb_genid_to_host_order(genid);
            sbuf2printf(logsb, " %llx (%d:%lld)\n", lclgenid, rrn, lclgenid);
            sbuf2flush(logsb);
        }

        if (rc != 0) {
            if (rc != RC_INTERNAL_RETRY) {
                reqerrstr(iq, COMDB2_UPD_RC_INVL_KEY,
                          "unable to update record rc = %d", rc);
            }
            if (logsb)
                sbuf2printf(logsb,
                            "Updated record failed, rrn = %d, genid=%llx\n",
                            rrn, bdb_genid_to_host_order(genid));
            return rc;
        } else if (logsb)
            sbuf2printf(logsb, "Updated record rrn = %d, genid=%llx\n", rrn,
                        bdb_genid_to_host_order(genid));

        (*receivedrows)++;
    } break;
    case OSQL_CLRTBL: {
        if (logsb) {
            sbuf2printf(logsb, "[%llu %s] OSQL_CLRTBL %s\n", rqid,
                        comdb2uuidstr(uuid, us), iq->usedb->dbname);
            sbuf2flush(logsb);
        }

        rc = reinit_db(iq->usedb);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s: reinit_db failed w/ rc = %d\n", __func__, rc);
            return conv_rc_sql2blkop(iq, step, -1, rc, err, NULL, 0);
        } else
            logmsg(LOGMSG_INFO, "Cleared ok\n");
    } break;
    case OSQL_UPDCOLS: {
        osql_updcols_t dt;
        const uint8_t *p_buf_end = p_buf + sizeof(osql_updcols_t);
        int i;

        p_buf = (uint8_t *)osqlcomm_updcols_type_get(&dt, p_buf, p_buf_end);

        if (logsb) {
            int jj;
            sbuf2printf(logsb, "[%llu %s] OSQL_UPDCOLS %d [\n", rqid,
                        comdb2uuidstr(uuid, us), dt.ncols);
            for (jj = 0; jj < dt.ncols; jj++)
                sbuf2printf(logsb, "%d ", dt.clist[jj]);

            sbuf2printf(logsb, "\n");
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
        char *strtype;
        currangearr_build_hash(arr);
        if (type == OSQL_SERIAL) {
            strtype = "OSQL_SERIAL";
            iq->arr = arr;
        }
        else {
            strtype = "OSQL_SELECTV";
            iq->selectv_arr = arr;
        }

        if (logsb) {
            uuidstr_t us;
            comdb2uuidstr(uuid, us);
            sbuf2printf(logsb, "[%llu %s] %s %d %d_%d_%d\n", rqid, us, strtype,
                        dt.buf_size, dt.arr_size, dt.file, dt.offset);
            sbuf2flush(logsb);
        }
    } break;
    case OSQL_DELIDX:
    case OSQL_INSIDX: {
        osql_index_t dt;
        unsigned char *pData = NULL;
        int jj = 0;
        int rrn = 2;
        unsigned long long lclgenid;
        int isDelete = (type == OSQL_DELIDX);
        const uint8_t *p_buf_end;
        uint8_t *pIdx = NULL;

        p_buf_end = p_buf + sizeof(osql_index_t);

        pData = (uint8_t *)osqlcomm_index_type_get(&dt, p_buf, p_buf_end);
        if (logsb) {
            int jj = 0;
            sbuf2printf(logsb, "[%llu %s] %s ixnum %d [\n", rqid,
                        comdb2uuidstr(uuid, us), dt.ixnum,
                        isDelete ? "OSQL_DELIDX" : "OSQL_INSIDX");
            for (jj = 0; jj < dt.nData; jj++)
                sbuf2printf(logsb, "%02x", pData[jj]);

            sbuf2printf(logsb, "]\n");
            sbuf2flush(logsb);
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
        osql_qblob_t dt;
        unsigned char *blob = NULL;
        const uint8_t *p_buf_end = p_buf + sizeof(osql_qblob_t);

        blob = (uint8_t *)osqlcomm_qblob_type_get(&dt, p_buf, p_buf_end);

        if (logsb) {
            int jj = 0;
            sbuf2printf(logsb, "[%llu %s] OSQL_QBLOB %d %d [\n", rqid,
                        comdb2uuidstr(uuid, us), dt.id, dt.bloblen);
            for (jj = 0; jj < dt.bloblen; jj++)
                sbuf2printf(logsb, "%02x", blob[jj]);

            sbuf2printf(logsb, "\n]");
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
            blobs[dt.id].length = dt.bloblen;

            if (dt.bloblen >= 0) {
                blobs[dt.id].exists = 1;
                if (dt.bloblen > 0) {
                    if (dt.bloblen > gbl_blob_sz_thresh_bytes)
                        blobs[dt.id].data = comdb2_bmalloc(blobmem, dt.bloblen);
                    else
                        blobs[dt.id].data = malloc(dt.bloblen);
                    if (!blobs[dt.id].data) {
                        logmsg(LOGMSG_ERROR, "%s failed to allocated a new blob, size %d\n",
                                __func__, blobs[dt.id].length);
                        return conv_rc_sql2blkop(iq, step, -1, ERR_INTERNAL,
                                                 err, NULL, 0);
                    }

                    /* finally copy the blob */
                    memcpy(blobs[dt.id].data, blob, dt.bloblen);

                    blobs[dt.id].collected = dt.bloblen;

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

            if (logsb) {
                sbuf2flush(logsb);
            }
        }
    } break;
    case OSQL_DBGLOG: {
        osql_dbglog_t dbglog;
        const uint8_t *p_buf_end = p_buf + sizeof(osql_dbglog_t);

        osqlcomm_dbglog_type_get(&dbglog, p_buf, p_buf_end);

        if (!iq->dbglog_file)
            iq->dbglog_file = open_dbglog_file(dbglog.dbglog_cookie);

        if (iq->queryid != dbglog.queryid)
            dbglog_init_write_counters(iq);

        iq->queryid = dbglog.queryid;
    } break;
    case OSQL_RECGENID: {
        osql_recgenid_t dt;
        int bdberr = 0;
        unsigned long long lclgenid;

        const uint8_t *p_buf_end = p_buf + sizeof(osql_recgenid_t);

        osqlcomm_recgenid_type_get(&dt, p_buf, p_buf_end);

        lclgenid = bdb_genid_to_host_order(dt.genid);

        rc = ix_check_genid(iq, trans, dt.genid, &bdberr);

        if (logsb) {
            sbuf2printf(logsb,
                        "[%llu %s] OSQL_RECGENID %llx (%llu) -> rc = %d\n",
                        rqid, comdb2uuidstr(uuid, us), lclgenid, lclgenid, rc);
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
        uint8_t *p_buf = (uint8_t *)msg + sizeof(osql_uuid_rpl_t);
        uint8_t *p_buf_end = p_buf + msglen;
        struct schema_change_type *sc = new_schemachange_type();
        p_buf = osqlcomm_schemachange_type_get(sc, p_buf, p_buf_end);

        if (p_buf == NULL) return -1;

        sc->nothrevent = 1;
        sc->finalize = 0;
        if (sc->original_master_node[0] != 0 &&
            strcmp(sc->original_master_node, gbl_mynode))
            sc->resume = 1;

        void *ptran = bdb_get_physical_tran(trans);
        bdb_ltran_get_schema_lock(trans);
        iq->sc = sc;
        if (sc->db == NULL) {
            sc->db = get_dbtable_by_name(sc->table);
        }
        sc->tran = ptran;
        if (sc->db) iq->usedb = sc->db;
        rc = start_schema_change_tran(iq, ptran);
        if (rc != SC_COMMIT_PENDING) {
            iq->sc = NULL;
        } else {
            iq->sc->sc_next = iq->sc_pending;
            iq->sc_pending = iq->sc;
            bset(&iq->osql_flags, OSQL_FLAGS_SCDONE);
        }

        return rc == SC_COMMIT_PENDING || !rc ? 0 : ERR_SC;
    } break;
    case OSQL_BPFUNC: {
        uint8_t *p_buf_end = (uint8_t *)msg + sizeof(osql_bpfunc_t) + msglen;
        osql_bpfunc_t *rpl;
        char err[MSGERR_MAXLEN]; // TODO TO BE REMOVED

        const uint8_t *n_p_buf =
            osqlcomm_bpfunc_type_get(&rpl, p_buf, p_buf_end);

        if (n_p_buf && rpl) {
            bpfunc_lstnode_t *lnode;
            bpfunc_lstnode_t *cur_bpfunc;
            bpfunc_t *func;
            bpfunc_info info;

            info.iq = iq;
            int rst = bpfunc_prepare(&func, trans, rpl->data_len, rpl->data, &info);
            if (!rst)
                rc = func->exec(trans, func, err);

            if (rst || rc) {
                free_bpfunc(func);
            } else {
                lnode = (bpfunc_lstnode_t *)malloc(sizeof(bpfunc_lstnode_t));
                assert(lnode);
                lnode->func = func;
                listc_abl(&iq->bpfunc_lst, lnode);
            }

        } else {
            logmsg(LOGMSG_ERROR, "Cannot read bpfunc message");
            rc = -1;
        }

        free(rpl);
        return rc;
    } break;
    default:

        logmsg(LOGMSG_ERROR, "%s [%llu %s] RECEIVED AN UNKNOWN OFF OPCODE %u, "
                        "failing the transaction\n",
                __func__, rqid, comdb2uuidstr(uuid, us), type);

        return conv_rc_sql2blkop(iq, step, -1, ERR_BADREQ, err, NULL, 0);
    }

    return 0;
}

#define OSQL_BP_MAXLEN (32 * 1024)

static int sorese_rcvreq(char *fromhost, void *dtap, int dtalen, int type,
                         int nettype)
{

    osql_sess_t *sess = NULL;
    osql_req_t req;
    uint8_t *p_req_buf = dtap;
    const uint8_t *p_req_buf_end = p_req_buf + dtalen;
    int rc = 0;
    uint8_t *malcd = malloc(OSQL_BP_MAXLEN);
    uint8_t *p_buf = malcd;
    const uint8_t *p_buf_end = p_buf + OSQL_BP_MAXLEN;
    sorese_info_t sorese_info = {0};
    char *sql;
    char *sqlret = NULL;
    int sqllenret = 0;
    int debug = 0;
    struct ireq *iq;
    uuid_t uuid;

    /* grab the request */
    if (nettype >= NET_OSQL_UUID_REQUEST_MIN &&
        nettype < NET_OSQL_UUID_REQUEST_MAX) {
        osql_uuid_req_t uuid_req;
        sql = (char *)osqlcomm_req_uuid_type_get(&uuid_req, p_req_buf,
                                                 p_req_buf_end);

        req.type = uuid_req.type;
        req.rqlen = uuid_req.rqlen;
        req.sqlqlen = uuid_req.sqlqlen;
        req.rqid = OSQL_RQID_USE_UUID;
        req.ntails = uuid_req.ntails;
        req.flags = uuid_req.flags;
        memcpy(req.tzname, uuid_req.tzname, sizeof(uuid_req.tzname));
        comdb2uuidcpy(uuid, uuid_req.uuid);
        comdb2uuidcpy(sorese_info.uuid, uuid_req.uuid);

        uuidstr_t us;
    } else {
        sql = (char *)osqlcomm_req_type_get(&req, p_req_buf, p_req_buf_end);
        comdb2uuid_clear(uuid);
        comdb2uuid_clear(sorese_info.uuid);
    }

    /* the req_type_get has already unpacked the rqid .. */
    sorese_info.rqid = req.rqid;
    sorese_info.host = fromhost;
    sorese_info.type = type;

#if 0   
   stats[type].rcv++;
#endif

    if (!p_buf) {
        logmsg(LOGMSG_ERROR, "%s:unable to allocate %d bytes\n", __func__,
                OSQL_BP_MAXLEN);
        rc = -1;
        goto done;
    }

    if (osql_repository_cancelled()) {
        logmsg(LOGMSG_ERROR, 
               "sorese request cancelled (schema change or exiting database)\n");
        rc = -2;
        free(malcd);
        goto done;
    }
#if 0
   printf( "Creating bplog %llu\n", osql_log_time());
#endif

    /* construct a block transaction */
    if (osql_bplog_build_sorese_req(p_buf, &p_buf_end, sql, req.sqlqlen,
                                    req.tzname, type, &sqlret, &sqllenret,
                                    req.rqid, uuid)) {
        logmsg(LOGMSG_ERROR, "%s: bug bug bug\n", __func__);
        rc = -3;
        free(malcd);
        goto done;
    }

#if 0
   printf( "Creating request %llu\n", osql_log_time());
#endif

    /* start a block processor */
    iq = create_sorese_ireq(thedb, NULL, p_buf, p_buf_end, debug, fromhost,
                            &sorese_info);
    if (iq == NULL) {
        free(malcd);
        rc = -5;
        goto done;
    }

    /* create the request */
    /* NOTE: this adds the session to the repository; we make sure that freshly
       added session have sess->iq set
       to avoid racing against signal_rtoff code */
    sess = osql_sess_create_sock(sqlret, sqllenret, req.tzname, type, req.rqid,
                                 uuid, fromhost, iq);
    if (!sess) {
        logmsg(LOGMSG_ERROR, "%s Unable to create new request\n", __func__);
        rc = -4;
        destroy_ireq(thedb, iq);
        free(malcd);
        goto done;
    }

#if 0
   printf( "Starting block processor %llu\n", osql_log_time());
#endif

    debug = debug_this_request(gbl_debug_until);

    if (gbl_who > 0) {
        gbl_who--;
        debug = 1;
    }

    /*
       Blockproc does not create a copy of the request,
       but creates a different thread to work on it
       Let THAT thread free it...
    free(p_buf);
    */

    /* for socksql, is this a retry that need to be checked for self-deadlock?
     */
    if ((type == OSQL_SOCK_REQ || type == OSQL_SOCK_REQ_COST) &&
        (btst(&req.flags, OSQL_FLAGS_CHECK_SELFLOCK))) {
        /* just make sure we are above the threshold */
        iq->sorese.verify_retries += gbl_osql_verify_ext_chk;
    }

    if (btst(&req.flags, OSQL_FLAGS_USE_BLKSEQ)) {
        iq->sorese.use_blkseq = 1;
    } else {
        iq->sorese.use_blkseq = 0;
    }

done:

    if (rc) {
        int rc2;

        /* notify the sql thread there will be no response! */
        struct errstat generr = {0};

        generr.errval = ERR_TRAN_FAILED;
        if (rc == -4) {
            strncpy(generr.errstr, "fail to create block processor log",
                    sizeof(generr.errstr));
        } else {
            strncpy(generr.errstr, "failed to create transaction",
                    sizeof(generr.errstr));
        }

        rc2 = osql_comm_signal_sqlthr_rc(&sorese_info, &generr,
                                         RC_INTERNAL_RETRY);
        if (rc2) {
            uuidstr_t us;
            comdb2uuidstr(uuid, us);
            logmsg(LOGMSG_ERROR, "%s: failed to signaled rqid=[%llx %s] host=%s of "
                            "error to create bplog\n",
                    __func__, req.rqid, us, fromhost);
        }
    } else {
        int rc2;

        /*
           successful, let the session lose
           It is possible that we are clearing sessions due to
           master being rtcpu-ed, and it will wait for the session
           clients to disappear before it will wipe out the session
         */

        rc2 = osql_sess_remclient(sess);
        if (rc2) {
            uuidstr_t us;
            comdb2uuidstr(uuid, us);
            logmsg(LOGMSG_ERROR, "%s: failed to release session rqid=[%llx %s] node=%s\n",
                    __func__, req.rqid, us, fromhost);
        }
    }

#if 0
   printf( "Done in here rc=%d %llu\n", rc, osql_log_time());
#endif

    return rc;
}

/* transaction result */
static void net_sorese_signal(void *hndl, void *uptr, char *fromhost,
                              int usertype, void *dtap, int dtalen,
                              uint8_t is_tcp)
{
    osql_done_t done;
    struct errstat *xerr;
    uint8_t *p_buf = (uint8_t *)dtap;
    uint8_t *p_buf_end = p_buf + dtalen;

    uuid_t uuid;
    unsigned long long rqid;
    int type;

    if (usertype >= NET_OSQL_UUID_REQUEST_MIN &&
        usertype < NET_OSQL_UUID_REQUEST_MAX) {
        osql_uuid_rpl_t uuid_hdr;
        /* unpack */
        osqlcomm_uuid_rpl_type_get(&uuid_hdr, p_buf, p_buf_end);
        comdb2uuidcpy(uuid, uuid_hdr.uuid);
        rqid = OSQL_RQID_USE_UUID;
        type = uuid_hdr.type;
    } else {
        osql_rpl_t hdr;
        osqlcomm_rpl_type_get(&hdr, p_buf, p_buf_end);
        comdb2uuid_clear(uuid);
        type = hdr.type;
        rqid = hdr.sid;
    }
    osqlcomm_done_type_get(&done, p_buf, p_buf_end);

    if (osql_comm_is_done(dtap, dtalen, rqid == OSQL_RQID_USE_UUID, &xerr, NULL) == 1) {

#if 0
      printf("Done rqid=%llu tmp=%llu\n", hdr->sid, osql_log_time());
#endif
        if (xerr) {
            struct errstat errstat;
            uint8_t *p_buf = (uint8_t *)xerr;
            uint8_t *p_buf_end = (p_buf + sizeof(struct errstat));
            osqlcomm_errstat_type_get(&errstat, p_buf, p_buf_end);

            osql_chkboard_sqlsession_rc(rqid, uuid, 0, NULL, &errstat);
        } else {
            osql_chkboard_sqlsession_rc(rqid, uuid, done.nops, NULL, NULL);
        }

    } else {
        logmsg(LOGMSG_ERROR, "%s: wrong sqlthr signal %d\n", __func__, type);
        return;
    }
}

static int netrpl2req(int netrpltype)
{
    switch (netrpltype) {
    case NET_OSQL_BLOCK_RPL:
    case NET_OSQL_BLOCK_RPL_UUID:
        return OSQL_BLOCK_REQ;

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
        logmsg(LOGMSG_ERROR, "Received malformed echo packet! size %d, should be %d\n",
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

    rc = offload_net_send(fromhost, NET_OSQL_ECHO_PONG, dtap, dtalen, 1);
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

    if (pthread_mutex_lock(&msgs_mtx)) {
        logmsgperror("pthread_mutex_lock");
        return;
    }
    if (msgs[msg.idx].idx != msg.idx || msgs[msg.idx].nonce != msg.nonce ||
        msgs[msg.idx].snt != msg.snt) {
        logmsg(LOGMSG_ERROR, "%s: malformed pong\n", __func__);
        return;
    }

    if (pthread_mutex_unlock(&msgs_mtx)) {
        logmsgperror("pthread_mutex_unlock");
        return;
    }

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
        if (pthread_mutex_lock(&msgs_mtx)) {
            logmsgperror("pthread_mutex_lock");
            return -1;
        }

        for (; i < MAX_ECHOES; i++)
            if (msgs[i].nonce == 0)
                break;
        if (i == MAX_ECHOES) {
            logmsg(LOGMSG_ERROR, "%s: too many echoes pending\n", __func__);
            if (pthread_mutex_unlock(&msgs_mtx))
                logmsgperror("pthread_mutex_unlock");
            return -1;
        }

        nonce = lrand48();
        snt = osql_log_time();

        bzero(&msg, sizeof(osql_echo_t));
        bzero(&msgs[i], sizeof(osql_echo_t));
        msg.nonce = msgs[i].nonce = nonce;
        msg.idx = msgs[i].idx = i;
        msg.snt = msgs[i].snt = snt;

        if (pthread_mutex_unlock(&msgs_mtx)) {
            logmsgperror("pthread_mutex_lock");
            return -1;
        }

        list[j] = &msgs[i];

        if (!(osqlcomm_echo_type_put(&msg, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: failed to pack echo message\n", __func__);
            return -1;
        }

        /*TODO: validate destination node to be valid!*/
        /* ping */
        rc = offload_net_send(tohost, NET_OSQL_ECHO_PING, (char *)buf,
                              sizeof(osql_echo_t), 1);
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
 * Interprets each packet and log info
 * about it
 *
 */
int osql_log_packet(struct ireq *iq, unsigned long long rqid, uuid_t uuid,
                    void *trans, char *msg, int msglen, int *flags,
                    int **updCols, blob_buffer_t blobs[MAXBLOBS], int step,
                    struct block_err *err, int *receivedrows, SBUF2 *logsb)
{
    uint8_t *p_buf = (uint8_t *)msg;
    uint8_t *p_buf_end =
        (uint8_t *)p_buf + sizeof(osql_rpl_t) + sizeof(osql_uuid_rpl_t);
    int rc = 0;
    struct dbtable *db =
        (iq->usedb) ? iq->usedb : thedb->dbs[0]; /*add to first if no usedb*/
    int type;
    unsigned long long id;
    uuidstr_t us;

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_uuid_rpl_t uuidrpl;
        p_buf =
            (uint8_t *)osqlcomm_uuid_rpl_type_get(&uuidrpl, p_buf, p_buf_end);
        type = uuidrpl.type;
        id = OSQL_RQID_USE_UUID;
        comdb2uuidstr(uuidrpl.uuid, us);
    } else {
        osql_rpl_t rpl;
        p_buf = (uint8_t *)osqlcomm_rpl_type_get(&rpl, p_buf, p_buf_end);
        comdb2uuidstr(uuid, us);
        type = rpl.type;
        id = rpl.sid;
        comdb2uuid_clear(us);
    }

    if (!logsb) {
        logmsg(LOGMSG_ERROR, "No log file present\n");
        return -1;
    }

    switch (type) {
    case OSQL_DONE:
    case OSQL_DONE_SNAP: {
        osql_done_t dt;
        p_buf_end = p_buf + sizeof(osql_done_t);

        osqlcomm_done_type_get(&dt, p_buf, p_buf_end);
        sbuf2printf(logsb, "[%llx %s] OSQL_DONE %d %d\n", id, us, dt.nops,
                    dt.rc);
    } break;

    case OSQL_DONE_STATS: {
        osql_done_t dt;
        p_buf_end = p_buf + sizeof(osql_done_t);

        osqlcomm_done_type_get(&dt, p_buf, p_buf_end);
        sbuf2printf(logsb, "[%llx %s] OSQL_DONE_STATS %d %d\n", id, us, dt.nops,
                    dt.rc);
    } break;

    case OSQL_USEDB: {
        osql_usedb_t dt;
        p_buf_end = p_buf + sizeof(osql_usedb_t);
        char *tablename;

        tablename = (char *)osqlcomm_usedb_type_get(&dt, p_buf, p_buf_end);

        sbuf2printf(logsb, "[%llx us] OSQL_USEDB \"%s\"\n", id, us, tablename);
    } break;
    case OSQL_DELREC:
    case OSQL_DELETE: {
        osql_del_t dt;
        unsigned long long lclgenid;
        int recv_dk = (type == OSQL_DELETE);
        uint8_t *p_buf_end;
        if (recv_dk)
            p_buf_end = p_buf + sizeof(osql_del_t);
        else
            p_buf_end = p_buf + sizeof(osql_del_t) - sizeof(unsigned long long);

        osqlcomm_del_type_get(&dt, p_buf, p_buf_end, recv_dk);
        if (!recv_dk)
            dt.dk = -1ULL;

        lclgenid = bdb_genid_to_host_order(dt.genid);
        sbuf2printf(logsb, "[%llx %s] %s %llx (%d:%lld)\n", id, us,
                    recv_dk ? "OSQL_DELETE" : "OSQL_DELREC", lclgenid, 2,
                    lclgenid);
        sbuf2flush(logsb);
    } break;
    case OSQL_INSREC:
    case OSQL_INSERT: {
        osql_ins_t dt;
        unsigned char *pData = NULL;
        int jj = 0;
        int rrn = 2;
        unsigned long long lclgenid;
        int recv_dk = (type == OSQL_INSERT);
        uint8_t *p_buf_end;
        if (recv_dk)
            p_buf_end = p_buf + sizeof(osql_ins_t);
        else
            p_buf_end = p_buf + sizeof(osql_ins_t) - sizeof(unsigned long long);

        pData =
            (uint8_t *)osqlcomm_ins_type_get(&dt, p_buf, p_buf_end, recv_dk);
        if (!recv_dk)
            dt.dk = -1ULL;
        lclgenid = bdb_genid_to_host_order(dt.seq);

        sbuf2printf(logsb, "[%llx %s] %s [\n", id, us,
                    recv_dk ? "OSQL_INSERT" : "OSQL_INSREC");
        for (jj = 0; jj < dt.nData; jj++)
            sbuf2printf(logsb, "%02x", pData[jj]);

        sbuf2printf(logsb, "\n] -> ");
        sbuf2printf(logsb, " %llx (%d:%lld)\n", lclgenid, rrn, lclgenid);
    } break;

    case OSQL_UPDREC:
    case OSQL_UPDATE: {
        osql_upd_t dt;
        unsigned char *pData;
        int rrn = 2;
        unsigned long long lclgenid;
        int recv_dk = (type == OSQL_UPDATE);
        int jj = 0;
        uint8_t *p_buf_end;
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
        lclgenid = bdb_genid_to_host_order(dt.genid);

        sbuf2printf(logsb, "[%llx %s] %s rrn = %d, genid = %llx[\n", id, us,
                    recv_dk ? "OSQL_UPDATE" : "OSQL_UPDREC", rrn, lclgenid);
        for (jj = 0; jj < dt.nData; jj++)
            sbuf2printf(logsb, "%02x", pData[jj]);

        sbuf2printf(logsb, "\n] -> ");
        sbuf2printf(logsb, " %llx (%d:%lld)\n", lclgenid, rrn, lclgenid);
    } break;

    case OSQL_CLRTBL: {
        sbuf2printf(logsb, "[%llx %s] OSQL_CLRTBL %s\n", id, us,
                    iq->usedb->dbname);
        sbuf2flush(logsb);
    } break;

    case OSQL_UPDCOLS: {
        osql_updcols_t dt;
        // uint8_t         *p_buf= (uint8_t *)&((osql_updcols_rpl_t*)msg)->dt;
        uint8_t *p_buf_end = p_buf + sizeof(osql_updcols_t);
        int jj;
        int *clist;

        clist = (int *)osqlcomm_updcols_type_get(&dt, p_buf, p_buf_end);

        sbuf2printf(logsb, "[%llx %s] OSQL_UPDCOLS %d [\n", id, us, dt.ncols);
        for (jj = 0; jj < dt.ncols; jj++)
            sbuf2printf(logsb, "%d ", ntohl(clist[jj]));

        sbuf2printf(logsb, "\n]  ");
    } break;
    case OSQL_SERIAL: {
        uint8_t *p_buf = (uint8_t *)&((osql_serial_rpl_t *)msg)->dt;
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
        currangearr_free(arr);

        if (logsb) {
            sbuf2printf(logsb, "[%llu] OSQL_SERIAL %d %d_%d_%d\n", id,
                        dt.buf_size, dt.arr_size, dt.file, dt.offset);
            sbuf2flush(logsb);
        }
    } break;
    case OSQL_SELECTV: {
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
        currangearr_free(arr);

        if (logsb) {
            sbuf2printf(logsb, "[%llu] OSQL_SELECTV %d %d_%d_%d\n", id,
                        dt.buf_size, dt.arr_size, dt.file, dt.offset);
            sbuf2flush(logsb);
        }
    } break;
    case OSQL_DELIDX:
    case OSQL_INSIDX: {
        osql_index_t dt;
        unsigned char *pData = NULL;
        int jj = 0;
        int rrn = 2;
        unsigned long long lclgenid;
        int isDelete = (type == OSQL_DELIDX);
        uint8_t *p_buf_end;

        p_buf_end = p_buf + sizeof(osql_index_t);

        pData = (uint8_t *)osqlcomm_index_type_get(&dt, p_buf, p_buf_end);
        lclgenid = bdb_genid_to_host_order(dt.seq);
        sbuf2printf(logsb, "[%llx %s] %s ixnum %d [\n", id, us, dt.ixnum,
                    isDelete ? "OSQL_DELIDX" : "OSQL_INSIDX");
        for (jj = 0; jj < dt.nData; jj++)
            sbuf2printf(logsb, "%02x", pData[jj]);

        sbuf2printf(logsb, "\n] -> ");
        sbuf2printf(logsb, " %llx (%d:%lld)\n", lclgenid, rrn, lclgenid);
    } break;
    case OSQL_QBLOB: {
        osql_qblob_t dt;
        unsigned char *blob = NULL;
        uint8_t *p_buf_end = p_buf + sizeof(osql_qblob_t);
        int jj = 0;

        blob = (uint8_t *)osqlcomm_qblob_type_get(&dt, p_buf, p_buf_end);

        sbuf2printf(logsb, "[%llx %s] OSQL_QBLOB %d %d [\n", id, us, dt.id,
                    dt.bloblen);
        for (jj = 0; jj < dt.bloblen; jj++)
            sbuf2printf(logsb, "%02x", blob[jj]);

        sbuf2printf(logsb, "]\n");
    } break;

    case OSQL_DBGLOG: {
        sbuf2printf(logsb, "OSQL_DBGLOG\n");
    } break;

    case OSQL_UPDSTAT: {
        osql_updstat_t dt;
        uint8_t *p_buf_end = p_buf + sizeof(osql_updstat_rpl_t);
        unsigned char *pData;
        unsigned long long seq;
        int rrn = 2;
        int jj = 0;

        pData = (uint8_t *)osqlcomm_updstat_type_get(&dt, p_buf, p_buf_end);
        seq = dt.seq;

        sbuf2printf(logsb, "[%llx %s] OSQL_UPDSTAT rrn = %d, seq = %llx[\n", id,
                    us, rrn, seq);
        for (jj = 0; jj < dt.nData; jj++)
            sbuf2printf(logsb, "%02x", pData[jj]);

        sbuf2printf(logsb, "\n]");
    } break;
    case OSQL_XERR: {
        struct errstat dt;
        uint8_t *p_buf_end = p_buf + sizeof(osql_done_xerr_t);

        osqlcomm_errstat_type_get(&dt, p_buf, p_buf_end);

        sbuf2printf(logsb, "[%llx %s] OSQL_XERR rc=%d %s\n", id, us,
                    errstat_get_rc(&dt), errstat_get_str(&dt));
    } break;
    case OSQL_RECGENID: {
        osql_recgenid_t dt;
        unsigned long long lclgenid;

        uint8_t *p_buf_end = p_buf + sizeof(osql_recgenid_t);

        osqlcomm_recgenid_type_get(&dt, p_buf, p_buf_end);

        lclgenid = bdb_genid_to_host_order(dt.genid);

        sbuf2printf(logsb, "[%llu %s] OSQL_RECGENID %llx (%llx)\n", id, us,
                    lclgenid, lclgenid);

    } break;

    default:

        logmsg(LOGMSG_ERROR, "%s [%llx %s] RECEIVED AN UNKNOWN OFF OPCODE %d, "
                        "failing the transaction\n",
                __func__, id, us, type);

        return conv_rc_sql2blkop(iq, step, -1, ERR_BADREQ, err, NULL, 0);
    }

    return 0;
}

/**
 * Send RECGENID
 * It handles remote/local connectivity
 *
 */
int osql_send_recordgenid(char *tohost, unsigned long long rqid, uuid_t uuid,
                          unsigned long long genid, int type, SBUF2 *logsb)
{
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    int rc = 0;
    uuidstr_t us;

    if (check_master(tohost))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (rqid == OSQL_RQID_USE_UUID) {
        osql_recgenid_uuid_rpl_t recgenid_rpl = {0};
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

        if (logsb) {
            sbuf2printf(logsb, "[%llu %s] send OSQL_RECGENID %llx (%lld)\n",
                        rqid, comdb2uuidstr(uuid, us), genid, genid);
            sbuf2flush(logsb);
        }

        offload_net_send(tohost, type, buf, sizeof(recgenid_rpl), 0);
    } else {
        osql_recgenid_rpl_t recgenid_rpl = {0};
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

        if (logsb) {
            sbuf2printf(logsb, "[%llu %s] send OSQL_RECGENID %llx (%lld)\n",
                        rqid, comdb2uuidstr(uuid, us), genid, genid);
            sbuf2flush(logsb);
        }

        offload_net_send(tohost, type, buf, sizeof(recgenid_rpl), 0);
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
    return (netinfo_type *)comm->handle_sibling;
}

int osqlpfthdpool_init(void)
{
    int i = 0;
    gbl_osqlpfault_thdpool = thdpool_create("OSQL PREFAULT pool", 0);

    if (gbl_exit_on_pthread_create_fail)
        thdpool_set_exit(gbl_osqlpfault_thdpool);

    thdpool_set_minthds(gbl_osqlpfault_thdpool, 0);
    thdpool_set_maxthds(gbl_osqlpfault_thdpool, gbl_osqlpfault_threads);
    thdpool_set_maxqueue(gbl_osqlpfault_thdpool, 1000);
    thdpool_set_linger(gbl_osqlpfault_thdpool, 10);
    thdpool_set_longwaitms(gbl_osqlpfault_thdpool, 10000);

    gbl_osqlpf_step = (osqlpf_step *)calloc(1000, sizeof(osqlpf_step));
    if (gbl_osqlpf_step == NULL)
        return 1;
    gbl_osqlpf_stepq = queue_new();
    if (gbl_osqlpf_stepq == NULL)
        return 1;
    for (i = 0; i < 1000; i++) {
        int *ii = (int *)malloc(sizeof(int));
        *ii = i;
        gbl_osqlpf_step[i].rqid = 0;
        gbl_osqlpf_step[i].step = 0;
        queue_add(gbl_osqlpf_stepq, ii);
    }
    return 0;
}

typedef struct osqlpf_rq {
    short type;
    struct dbtable *db;
    unsigned long long genid;
    int index;
    unsigned char key[MAXKEYLEN];
    void *record;
    unsigned short len; /* if its a key, the len of the key.  if its a dta rec,
                           the len of the record */
    int i;
    unsigned long long rqid;
    unsigned long long seq;
    uuid_t uuid;
} osqlpf_rq_t;

/* osql request io prefault, code stolen from prefault.c */

static int is_bad_rc(int rc)
{
    if (rc == 0)
        return 0;
    if (rc == 1)
        return 0;

    return 1;
}

static void osqlpfault_do_work_pp(struct thdpool *pool, void *work,
                                  void *thddata, int op);

/* given a table, key   : enqueue a fault for the a single ix record */
int enque_osqlpfault_oldkey(struct dbtable *db, void *key, int keylen, int ixnum,
                            int i, unsigned long long rqid,
                            unsigned long long seq)
{
    osqlpf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(osqlpf_rq_t));
    if (qdata == NULL) {
        logmsg(LOGMSG_FATAL, "failed to malloc osql prefault request\n");
        exit(1);
    }

    qdata->type = OSQLPFRQ_OLDKEY;
    qdata->len = keylen;
    qdata->index = ixnum;
    qdata->db = db;
    qdata->genid = -1;
    qdata->i = i;
    qdata->seq = seq;
    qdata->rqid = rqid;

    if ((keylen > 0) && (keylen < MAXKEYLEN))
        memcpy(qdata->key, key, keylen);

    rc = thdpool_enqueue(gbl_osqlpfault_thdpool, osqlpfault_do_work_pp, qdata,
                         0, NULL);

    if (rc != 0) {
        free(qdata);
    }
    return rc;
}

/* given a table, key   : enqueue a fault for the a single ix record */
int enque_osqlpfault_newkey(struct dbtable *db, void *key, int keylen, int ixnum,
                            int i, unsigned long long rqid,
                            unsigned long long seq)
{
    osqlpf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(osqlpf_rq_t));
    if (qdata == NULL) {
        logmsg(LOGMSG_FATAL, "failed to malloc osql prefault request\n");
        exit(1);
    }

    qdata->type = OSQLPFRQ_NEWKEY;
    qdata->len = keylen;
    qdata->index = ixnum;
    qdata->db = db;
    qdata->genid = -1;
    qdata->i = i;
    qdata->seq = seq;
    qdata->rqid = rqid;

    if ((keylen > 0) && (keylen < MAXKEYLEN))
        memcpy(qdata->key, key, keylen);

    rc = thdpool_enqueue(gbl_osqlpfault_thdpool, osqlpfault_do_work_pp, qdata,
                         0, NULL);

    if (rc != 0) {
        free(qdata);
    }
    return rc;
}

/* given a table, genid   : enqueue an op that faults in the dta record by
                            genid then forms all keys from that record and
                            enqueues n ops to fault in each key.
                            */
int enque_osqlpfault_olddata_oldkeys(struct dbtable *db, unsigned long long genid,
                                     int i, unsigned long long rqid,
                                     uuid_t uuid, unsigned long long seq)
{
    osqlpf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(osqlpf_rq_t));
    if (qdata == NULL) {
        logmsg(LOGMSG_FATAL, "failed to malloc osql prefault request\n");
        exit(1);
    }

    qdata->type = OSQLPFRQ_OLDDATA_OLDKEYS;
    qdata->index = -1;
    qdata->db = db;
    qdata->genid = genid;
    qdata->i = i;
    qdata->seq = seq;
    qdata->rqid = rqid;
    comdb2uuidcpy(qdata->uuid, uuid);

    rc = thdpool_enqueue(gbl_osqlpfault_thdpool, osqlpfault_do_work_pp, qdata,
                         0, NULL);

    if (rc != 0) {
        free(qdata);
    }
    return rc;
}

/* given a table, record : enqueue an op that faults in the dta record by
                            genid then forms all keys from that record and
                            enqueues n ops to fault in each key.
                            */
int enque_osqlpfault_newdata_newkeys(struct dbtable *db, void *record, int reclen,
                                     int i, unsigned long long rqid,
                                     uuid_t uuid, unsigned long long seq)
{
    osqlpf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(osqlpf_rq_t));
    if (qdata == NULL) {
        logmsg(LOGMSG_FATAL, "failed to malloc osql prefault request\n");
        exit(1);
    }

    qdata->type = OSQLPFRQ_NEWDATA_NEWKEYS;
    qdata->index = -1;
    qdata->db = db;
    qdata->genid = -1;
    qdata->record = malloc(reclen);
    memcpy(qdata->record, record, reclen);
    qdata->len = reclen;
    qdata->i = i;
    qdata->seq = seq;
    qdata->rqid = rqid;
    comdb2uuidcpy(qdata->uuid, uuid);

    rc = thdpool_enqueue(gbl_osqlpfault_thdpool, osqlpfault_do_work_pp, qdata,
                         0, NULL);

    if (rc != 0) {
        free(qdata->record);
        free(qdata);
    }
    return rc;
}

/* given a                      : enqueue a an op that
     table,genid,                 1) faults in dta by tbl/genid
     tag,record,reclen            2) forms all keys
                                  3) enqueues n ops to fault in each key
                                  4) forms new record by taking found record +
                                     tag/record/reclen
                                  5) forms all keys from new record.
                                  6) enqueues n ops to fault in each key.
                                  */
int enque_osqlpfault_olddata_oldkeys_newkeys(
    struct dbtable *db, unsigned long long genid, void *record, int reclen, int i,
    unsigned long long rqid, uuid_t uuid, unsigned long long seq)
{
    osqlpf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(osqlpf_rq_t));
    if (qdata == NULL) {
        logmsg(LOGMSG_FATAL, "failed to malloc osql prefault request\n");
        exit(1);
    }

    qdata->type = OSQLPFRQ_OLDDATA_OLDKEYS_NEWKEYS;
    qdata->index = -1;
    qdata->db = db;
    qdata->genid = genid;
    qdata->record = malloc(reclen);
    memcpy(qdata->record, record, reclen);
    qdata->len = reclen;
    qdata->i = i;
    qdata->seq = seq;
    qdata->rqid = rqid;
    comdb2uuidcpy(qdata->uuid, uuid);

    rc = thdpool_enqueue(gbl_osqlpfault_thdpool, osqlpfault_do_work_pp, qdata,
                         0, NULL);

    if (rc != 0) {
        free(qdata->record);
        free(qdata);
    }
    return rc;
}

static void osqlpfault_do_work(struct thdpool *pool, void *work, void *thddata)
{
    int rc = 0;
    struct dbenv *dbenv;
    struct ireq iq;
    unsigned long long step;
    osqlpf_rq_t *req = (osqlpf_rq_t *)work;
    init_fake_ireq(thedb, &iq);
    bdb_thread_event(thedb->bdb_env, 1);
    if (gbl_prefault_udp)
        send_prefault_udp = 2;

    if (!gbl_osqlpfault_threads)
        goto done;

    if (req->rqid != gbl_osqlpf_step[req->i].rqid) {
        goto done;
    }
    if (req->rqid == OSQL_RQID_USE_UUID &&
        comdb2uuidcmp(req->uuid, gbl_osqlpf_step[req->i].uuid))
        goto done;

    step = req->seq << 7;

    switch (req->type) {
    case OSQLPFRQ_OLDDATA: {
        int fndlen;
        int od_len;
        unsigned char *fnddta = malloc(32768 * sizeof(unsigned char));
        iq.usedb = req->db;

        step += 1;
        if (step <= gbl_osqlpf_step[req->i].step) {
            if (fnddta)
                free(fnddta);
            break;
        }

        od_len = getdatsize(iq.usedb);
        if (fnddta == NULL) {
            logmsg(LOGMSG_FATAL, "osqlpfault_do_work: malloc %u failed\n", od_len);
            exit(1);
        }
        rc = ix_find_by_rrn_and_genid_prefault(&iq, 2, req->genid, fnddta,
                                               &fndlen, od_len);
        if (fnddta)
            free(fnddta);
    } break;
    case OSQLPFRQ_OLDKEY: {
        int fndrrn = 0;
        char fndkey[MAXKEYLEN];
        unsigned long long genid = 0;
        if ((req->index < 0) || (req->index > 49)) {
            logmsg(LOGMSG_ERROR, "PFRQ_OLDKEY ix %d out of bounds\n", req->index);
            break;
        }

        step += ((1 + (unsigned long long)req->index) << 1);
        if (step <= gbl_osqlpf_step[req->i].step) {
            break;
        }

        iq.usedb = req->db;
        rc = ix_find_prefault(&iq, req->index, req->key, req->len, fndkey,
                              &fndrrn, &genid, NULL, NULL, 0);
    } break;
    case OSQLPFRQ_NEWKEY: {
        int fndrrn = 0;
        char fndkey[MAXKEYLEN];
        unsigned long long genid = 0;

        if ((req->index < 0) || (req->index > 49)) {
            logmsg(LOGMSG_ERROR, "PFRQ_OLDKEY ix %d out of bounds\n", req->index);
            break;
        }

        step += 1 + ((1 + (unsigned long long)req->index) << 1);
        if (step <= gbl_osqlpf_step[req->i].step) {
            break;
        }

        iq.usedb = req->db;
        rc = ix_find_prefault(&iq, req->index, req->key, req->len, fndkey,
                              &fndrrn, &genid, NULL, NULL, 0);
    } break;
    case OSQLPFRQ_OLDDATA_OLDKEYS: {
        size_t od_len;
        int od_len_int;
        int maxlen = 0, fndlen = 0, err = 0;
        int ixnum = 0;
        unsigned long long genid = 0;
        int doprimkey = 0;
        unsigned char *fnddta = malloc(32768 * sizeof(unsigned char));

        iq.usedb = req->db;

        od_len_int = getdatsize(iq.usedb);
        if (od_len_int <= 0) {
            if (fnddta)
                free(fnddta);
            break;
        }
        od_len = (size_t)od_len_int;

        step += 1;
        if (step <= gbl_osqlpf_step[req->i].step) {
            if (fnddta)
                free(fnddta);
            break;
        }

        if (fnddta == NULL) {
            logmsg(LOGMSG_FATAL, "osqlpfault_do_work: malloc %u failed\n", od_len);
            exit(1);
        }

        rc = ix_find_by_rrn_and_genid_prefault(&iq, 2, req->genid, fnddta,
                                               &fndlen, od_len);

        if ((is_bad_rc(rc)) || (od_len != fndlen)) {
            if (fnddta)
                free(fnddta);
            break;
        }

        for (ixnum = 0; ixnum < iq.usedb->nix; ixnum++) {
            char keytag[MAXTAGLEN];
            char key[MAXKEYLEN];
            int keysz = 0;
            keysz = getkeysize(iq.usedb, ixnum);
            if (keysz < 0) {
                logmsg(LOGMSG_ERROR, "osqlpfault_do_work:cannot get key size"
                                " tbl %s. idx %d\n",
                        iq.usedb->dbname, ixnum);
                break;
            }
            snprintf(keytag, sizeof(keytag), ".ONDISK_IX_%d", ixnum);
            rc = stag_to_stag_buf(iq.usedb->dbname, ".ONDISK", (char *)fnddta,
                                  keytag, key, NULL);
            if (rc == -1) {
                logmsg(LOGMSG_ERROR, 
                        "osqlpfault_do_work:cannot convert .ONDISK to IDX"
                        " %d of TBL %s\n",
                        ixnum, iq.usedb->dbname);
                break;
            }

            rc = enque_osqlpfault_oldkey(iq.usedb, key, keysz, ixnum, req->i,
                                         req->rqid, req->seq);
        }
        if (fnddta)
            free(fnddta);
    } break;
    case OSQLPFRQ_NEWDATA_NEWKEYS: {
        int ixnum = 0;

        iq.usedb = req->db;

        /* enqueue faults for new keys */
        for (ixnum = 0; ixnum < iq.usedb->nix; ixnum++) {
            char keytag[MAXTAGLEN];
            char key[MAXKEYLEN];
            int keysz = 0;
            keysz = getkeysize(iq.usedb, ixnum);
            if (keysz < 0) {
                logmsg(LOGMSG_ERROR, "osqlpfault_do_work:cannot get key size"
                                " tbl %s. idx %d\n",
                        iq.usedb->dbname, ixnum);
                continue;
            }
            snprintf(keytag, sizeof(keytag), ".ONDISK_IX_%d", ixnum);
            rc = stag_to_stag_buf(iq.usedb->dbname, ".ONDISK",
                                  (char *)req->record, keytag, key, NULL);
            if (rc == -1) {
                logmsg(LOGMSG_ERROR, 
                        "osqlpfault_do_work:cannot convert .ONDISK to IDX"
                        " %d of TBL %s\n",
                        ixnum, iq.usedb->dbname);
                continue;
            }

            rc = enque_osqlpfault_newkey(iq.usedb, key, keysz, ixnum, req->i,
                                         req->rqid, req->seq);
        }
    } break;
    case OSQLPFRQ_OLDDATA_OLDKEYS_NEWKEYS: {
        size_t od_len;
        int od_len_int;
        int fndlen = 0;
        int ixnum = 0;
        unsigned long long genid = 0;
        unsigned char *fnddta = malloc(32768 * sizeof(unsigned char));

        if (fnddta == NULL) {
            logmsg(LOGMSG_FATAL, "osqlpfault_do_work: malloc %u failed\n",
                   od_len);
            exit(1);
        }
        iq.usedb = req->db;

        od_len_int = getdatsize(iq.usedb);
        if (od_len_int <= 0) {
            free(fnddta);
            break;
        }
        od_len = (size_t)od_len_int;

        step += 1;
        if (step <= gbl_osqlpf_step[req->i].step) {
            free(fnddta);
            break;
        }


        rc = ix_find_by_rrn_and_genid_prefault(&iq, 2, req->genid, fnddta,
                                               &fndlen, od_len);

        if ((is_bad_rc(rc)) || (od_len != fndlen)) {
            free(fnddta);
            break;
        }

        /* enqueue faults for old keys */
        for (ixnum = 0; ixnum < iq.usedb->nix; ixnum++) {
            char keytag[MAXTAGLEN];
            char key[MAXKEYLEN];
            int keysz = 0;
            keysz = getkeysize(iq.usedb, ixnum);
            if (keysz < 0) {
                logmsg(LOGMSG_ERROR, "osqlpfault_do_work:cannot get key size"
                                " tbl %s. idx %d\n",
                        iq.usedb->dbname, ixnum);
                continue;
            }
            snprintf(keytag, sizeof(keytag), ".ONDISK_IX_%d", ixnum);
            rc = stag_to_stag_buf(iq.usedb->dbname, ".ONDISK", (char *)fnddta,
                                  keytag, key, NULL);
            if (rc == -1) {
                logmsg(LOGMSG_ERROR, "osqlpfault_do_work:cannot convert .ONDISK to IDX"
                        " %d of TBL %s\n",
                        ixnum, iq.usedb->dbname);
                continue;
            }

            rc = enque_osqlpfault_oldkey(iq.usedb, key, keysz, ixnum, req->i,
                                         req->rqid, req->seq);
        }

        free(fnddta);

        /* enqueue faults for new keys */
        for (ixnum = 0; ixnum < iq.usedb->nix; ixnum++) {
            char keytag[MAXTAGLEN];
            char key[MAXKEYLEN];
            int keysz = 0;
            keysz = getkeysize(iq.usedb, ixnum);
            if (keysz < 0) {
                logmsg(LOGMSG_ERROR, "osqlpfault_do_work:cannot get key size"
                                " tbl %s. idx %d\n",
                        iq.usedb->dbname, ixnum);
                continue;
            }
            snprintf(keytag, sizeof(keytag), ".ONDISK_IX_%d", ixnum);
            rc = stag_to_stag_buf(iq.usedb->dbname, ".ONDISK",
                                  (char *)req->record, keytag, key, NULL);
            if (rc == -1) {
                logmsg(LOGMSG_ERROR, "osqlpfault_do_work:cannot convert .ONDISK to IDX"
                        " %d of TBL %s\n",
                        ixnum, iq.usedb->dbname);
                continue;
            }

            rc = enque_osqlpfault_newkey(iq.usedb, key, keysz, ixnum, req->i,
                                         req->rqid, req->seq);
        }
    } break;
    }

done:
    bdb_thread_event(thedb->bdb_env, 0);
    send_prefault_udp = 0;
    if (req->record)
        free(req->record);
    free(req);
}

static void osqlpfault_do_work_pp(struct thdpool *pool, void *work,
                                  void *thddata, int op)
{
    osqlpf_rq_t *req = (osqlpf_rq_t *)work;
    switch (op) {
    case THD_RUN:
        osqlpfault_do_work(pool, work, thddata);
        break;
    case THD_FREE:
        if (req->record)
            free(req->record);
        free(req);
        break;
    }
}

int osql_page_prefault(char *rpl, int rplen, struct dbtable **last_db,
                       int **iq_step_ix, unsigned long long rqid, uuid_t uuid,
                       unsigned long long seq)
{
    static int last_step_idex = 0;
    int *ii;
    int rc;
    osql_rpl_t rpl_op;
    uint8_t *p_buf = (uint8_t *)rpl;
    uint8_t *p_buf_end = p_buf + rplen;
    osqlcomm_rpl_type_get(&rpl_op, p_buf, p_buf_end);

    if (seq == 0) {
        rc = pthread_mutex_lock(&osqlpf_mutex);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "osql_page_prefault: Failed to lock osqlpf_mutex\n");
            return 1;
        }
        ii = queue_next(gbl_osqlpf_stepq);
        rc = pthread_mutex_unlock(&osqlpf_mutex);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "osql_page_prefault: Failed to unlock osqlpf_mutex\n");
            return 1;
        }
        if (ii == NULL) {
            logmsg(LOGMSG_ERROR, "osql io prefault got a BUG!\n");
            exit(1);
        }
        last_step_idex = *ii;
        *iq_step_ix = ii;
        gbl_osqlpf_step[last_step_idex].rqid = rqid;
        comdb2uuidcpy(gbl_osqlpf_step[last_step_idex].uuid, uuid);
    }

    switch (rpl_op.type) {
    case OSQL_USEDB: {
        osql_usedb_t dt;
        p_buf = (uint8_t *)&((osql_usedb_rpl_t *)rpl)->dt;
        char *tablename;
        struct dbtable *db;

        tablename = (char *)osqlcomm_usedb_type_get(&dt, p_buf, p_buf_end);

        db = get_dbtable_by_name(tablename);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "%s: unable to get usedb for table %.*s\n",
                    __func__, dt.tablenamelen, tablename);
        } else {
            *last_db = db;
        }
    } break;
    case OSQL_DELREC:
    case OSQL_DELETE: {
        osql_del_t dt;
        p_buf = (uint8_t *)&((osql_del_rpl_t *)rpl)->dt;
        p_buf = (uint8_t *)osqlcomm_del_type_get(&dt, p_buf, p_buf_end,
                                                 rpl_op.type == OSQL_DELETE);
        enque_osqlpfault_olddata_oldkeys(*last_db, dt.genid, last_step_idex,
                                         rqid, uuid, seq);
    } break;
    case OSQL_INSREC:
    case OSQL_INSERT: {
        osql_ins_t dt;
        unsigned char *pData = NULL;
        int rrn = 0;
        unsigned long long genid = 0;
        uint8_t *p_buf = (uint8_t *)&((osql_ins_rpl_t *)rpl)->dt;
        pData = (uint8_t *)osqlcomm_ins_type_get(&dt, p_buf, p_buf_end,
                                                 rpl_op.type == OSQL_INSERT);
        enque_osqlpfault_newdata_newkeys(*last_db, pData, dt.nData,
                                         last_step_idex, rqid, uuid, seq);
    } break;
    case OSQL_UPDREC:
    case OSQL_UPDATE: {
        osql_upd_t dt;
        uint8_t *p_buf = (uint8_t *)&((osql_upd_rpl_t *)rpl)->dt;
        unsigned char *pData;
        int rrn = 2;
        unsigned long long genid;
        pData = (uint8_t *)osqlcomm_upd_type_get(&dt, p_buf, p_buf_end,
                                                 rpl_op.type == OSQL_UPDATE);
        genid = dt.genid;
        enque_osqlpfault_olddata_oldkeys_newkeys(*last_db, dt.genid, pData,
                                                 dt.nData, last_step_idex, rqid,
                                                 uuid, seq);
    } break;
    default:
        return 0;
    }
    return 0;
}

/**
 * Send SCHEMACHANGE op
 * It handles remote/local connectivity
 *
 */
int osql_send_schemachange(char *tonode, unsigned long long rqid, uuid_t uuid,
                           struct schema_change_type *sc, int type,
                           SBUF2 *logsb)
{
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    osql_uuid_rpl_t sc_rpl = {0};

    schemachange_packed_size(sc);
    size_t osql_rpl_sc_size = OSQLCOMM_UUID_RPL_TYPE_LEN + sc->packed_len;
    uint8_t *buf = alloca(osql_rpl_sc_size);
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end = p_buf + osql_rpl_sc_size;
    int rc = 0;

    if (check_master(tonode))
        return OSQL_SEND_ERROR_WRONGMASTER;

    // rc = osql_send_usedb_logic(pCur, thd, nettype); // FIX IT: not sure if
    // needed now I will look into it later

    sc_rpl.type = OSQL_SCHEMACHANGE;
    comdb2uuidcpy(sc_rpl.uuid, uuid);

    if (tonode)
        strcpy(sc->original_master_node, tonode);
    else
        strcpy(sc->original_master_node, gbl_mynode);

    if (!(p_buf = osqlcomm_schemachange_rpl_type_put(&sc_rpl, sc, p_buf,
                                                     p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                "osqlcomm_schemachange_rpl_type_put");
        return -1;
    }

    rc =
        offload_net_send(tonode, type, buf, sizeof(sc_rpl) + sc->packed_len, 0);

    return rc;
}

/* { REPLICANT SIDE UPGRADE RECORD LOGIC */
int gbl_num_record_upgrades = 0; /* default off */
// Dorin's cron scheduler
cron_sched_t *uprec_sched;

// structure
static struct uprec_tag {
    pthread_mutex_t *lk;    /* one big mutex, rule them all */
    const struct dbtable *owner; /* who can put elements in the array */
    const struct dbtable *touch; /* which db master will be touching */
    struct buf_lock_t slock;
    size_t thre; /* slow start threshold */
    size_t intv; /* interval */
    uint8_t buffer[OSQL_BP_MAXLEN];
    uint8_t buf_end;
    unsigned long long genid;
    /* nreqs should == nbads + ngoods + ntimeouts */
    size_t nreqs;     /* number of upgrade requests sent to master */
    size_t nbads;     /* number of bad responses recv'd from master */
    size_t ngoods;    /* number of good responses recv'd from master */
    size_t ntimeouts; /* number of timeouts */
} * uprec;

static const uint8_t *construct_uptbl_buffer(const struct dbtable *db,
                                             unsigned long long genid,
                                             unsigned int recs_ahead,
                                             uint8_t *p_buf_start,
                                             const uint8_t *p_buf_end)
{
    uint8_t *p_buf;
    uint8_t *p_buf_req_start;
    const uint8_t *p_buf_req_end;
    uint8_t *p_buf_op_hdr_start;
    const uint8_t *p_buf_op_hdr_end;

    struct req_hdr req_hdr;
    struct block_req req = {0};
    struct packedreq_hdr op_hdr;
    struct packedreq_usekl usekl;
    struct packedreq_uptbl uptbl;

    int ii;

    p_buf = p_buf_start;

    // req_hdr
    req_hdr.opcode = OP_BLOCK;
    if (!(p_buf = req_hdr_put(&req_hdr, p_buf, p_buf_end)))
        return NULL;

    // save room for block req
    if (BLOCK_REQ_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf_req_start = p_buf;
    p_buf += BLOCK_REQ_LEN;
    p_buf_req_end = p_buf;

    /** use **/

    // save room for uprec header
    if (PACKEDREQ_HDR_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf_op_hdr_start = p_buf;
    p_buf += PACKEDREQ_HDR_LEN;
    p_buf_op_hdr_end = p_buf;

    usekl.dbnum = db->dbnum;
    usekl.taglen = strlen(db->dbname) + 1 /*NUL byte*/;
    if (!(p_buf = packedreq_usekl_put(&usekl, p_buf, p_buf_end)))
        return NULL;
    if (!(p_buf = buf_no_net_put(db->dbname, usekl.taglen, p_buf, p_buf_end)))
        return NULL;

    op_hdr.opcode = BLOCK2_USE;
    op_hdr.nxt = one_based_word_offset_from_ptr(p_buf_start, p_buf);

    if (!(p_buf = buf_zero_put(
              ptr_from_one_based_word_offset(p_buf_start, op_hdr.nxt) - p_buf,
              p_buf, p_buf_end)))
        return NULL;

    if (packedreq_hdr_put(&op_hdr, p_buf_op_hdr_start, p_buf_op_hdr_end) !=
        p_buf_op_hdr_end)
        return NULL;

    /** uptbl **/

    // save room for uprec header
    if (PACKEDREQ_HDR_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf_op_hdr_start = p_buf;
    p_buf += PACKEDREQ_HDR_LEN;
    p_buf_op_hdr_end = p_buf;

    uptbl.nrecs = recs_ahead;
    uptbl.genid = uprec->genid;
    if (!(p_buf = packedreq_uptbl_put(&uptbl, p_buf, p_buf_end)))
        return NULL;

    if (!(p_buf = buf_put(&uprec->genid, sizeof(unsigned long long), p_buf,
                          p_buf_end)))
        return NULL;

    op_hdr.opcode = BLOCK2_UPTBL;
    op_hdr.nxt = one_based_word_offset_from_ptr(p_buf_start, p_buf);

    if (!(p_buf = buf_zero_put(
              ptr_from_one_based_word_offset(p_buf_start, op_hdr.nxt) - p_buf,
              p_buf, p_buf_end)))
        return NULL;

    if (packedreq_hdr_put(&op_hdr, p_buf_op_hdr_start, p_buf_op_hdr_end) !=
        p_buf_op_hdr_end)
        return NULL;

    /* build req */
    req.num_reqs = 2;
    req.flags = BLKF_ERRSTAT; /* we want error stat */
    req.offset = op_hdr.nxt;  /* overall offset = next offset of last op */

    /* pack req in the space we saved at the start */
    if (block_req_put(&req, p_buf_req_start, p_buf_req_end) != p_buf_req_end)
        return NULL;
    p_buf_end = p_buf;

    return p_buf_end;
}

static void *uprec_cron_kickoff(uuid_t source_id, void *arg1, void *arg2, void *arg3,
                                struct errstat *err)
{
    logmsg(LOGMSG_INFO, "Starting upgrade record cron job\n");
    return NULL;
}

static void *uprec_cron_event(uuid_t source_id, void *arg1, void *arg2, void *arg3,
                              struct errstat *err)
{
    int rc, nwakeups;
    struct buf_lock_t *p_slock;
    struct timespec ts;
    const uint8_t *buf_end;

    rc = pthread_mutex_lock(uprec->lk);
    if (rc == 0) {
        /* construct buffer */
        buf_end = construct_uptbl_buffer(uprec->touch, uprec->genid,
                                         gbl_num_record_upgrades, uprec->buffer,
                                         &uprec->buf_end);
        if (buf_end == NULL)
            goto done;

        /* send and then wait */
        p_slock = &uprec->slock;
        rc = offload_comm_send_blockreq(
            thedb->master == gbl_mynode ? 0 : thedb->master, p_slock,
            uprec->buffer, (buf_end - uprec->buffer));
        if (rc != 0)
            goto done;

        ++uprec->nreqs;
        nwakeups = 0;
        while (!p_slock->reply_done) {
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;
            rc = pthread_cond_timedwait(&p_slock->wait_cond, &p_slock->req_lock,
                                        &ts);
            ++nwakeups;

            if (nwakeups == uprec->thre) { // timedout #1
                logmsg(LOGMSG_WARN, "no response from master within %d seconds\n", 
                        nwakeups);
                break;
            }
        }

        if (!p_slock->reply_done) {
            // timedout from #1
            // intv = 0.75 * intv + 0.25 * T(this time)
            uprec->intv += (uprec->intv << 1) + nwakeups;
            uprec->intv >>= 2;
            ++uprec->ntimeouts;
        } else if (p_slock->rc) {
            // something unexpected happened. double the interval.
            if (uprec->intv >= uprec->thre)
                ++uprec->intv;
            else {
                uprec->intv <<= 1;
                if (uprec->intv >= uprec->thre)
                    uprec->intv = uprec->thre;
            }
            ++uprec->nbads;
        } else {
            // all good. reset interval
            uprec->intv = 1;
            ++uprec->ngoods;
        }

    done:
        // allow the array to take new requests
        uprec->genid = 0;
        uprec->owner = NULL;

        rc = pthread_mutex_unlock(uprec->lk);
    }

    return NULL;
}

// Upgrade a single record.
int offload_comm_send_upgrade_record(const char *tbl, unsigned long long genid)
{
    int rc;
    uint8_t *buffer;
    const uint8_t *buffer_end;

    buffer = alloca(OSQL_BP_MAXLEN);

    buffer_end =
        construct_uptbl_buffer(get_dbtable_by_name(tbl), genid, 1, buffer,
                               (const uint8_t *)(buffer + OSQL_BP_MAXLEN));

    if (buffer_end == NULL)
        rc = EINVAL;
    else
        // send block request and free buffer
        rc = offload_comm_send_sync_blockreq(
            thedb->master == gbl_mynode ? 0 : thedb->master, buffer,
            buffer_end - buffer);

    return rc;
}

// initialize sender queues once
static pthread_once_t uprec_sender_array_once = PTHREAD_ONCE_INIT;
static void uprec_sender_array_init(void)
{
    int rc, idx;
    size_t mallocsz;
    struct uprec_stripe_queue *queue;
    struct errstat xerr;

    mallocsz = sizeof(struct uprec_tag) +
               (gbl_dtastripe - 1) * sizeof(unsigned long long);

    // malloc a big chunk
    uprec = malloc(mallocsz);
    if (uprec == NULL) {
        logmsg(LOGMSG_FATAL, "%s: out of memory.\n", __func__);
        abort();
    }

    // initialize the big structure
    uprec->owner = NULL;
    uprec->thre = 900; // 15 minutes
    uprec->intv = 1;
    uprec->nreqs = 0;
    uprec->nbads = 0;
    uprec->ngoods = 0;
    uprec->ntimeouts = 0;

    // initialize slock
    if (pthread_mutex_init(&(uprec->slock.req_lock), NULL) != 0) {
        logmsg(LOGMSG_FATAL, "%s: failed to create buf_lock_t req_lock.\n",
                __func__);
        abort();
    }

    if (pthread_cond_init(&(uprec->slock.wait_cond), NULL) != 0) {
        logmsg(LOGMSG_FATAL, "%s: failed to create buf_lock_t wait_cond.\n",
                __func__);
        abort();
    }

    uprec->lk = &uprec->slock.req_lock;
    uprec->slock.reply_done = 0;
    uprec->slock.sb = 0;

    // kick off upgradetable cron
    uprec_sched =
        cron_add_event(NULL, "uprec_cron", INT_MIN, uprec_cron_kickoff, NULL,
                       NULL, NULL, NULL, &xerr);

    if (uprec_sched == NULL) {
        logmsg(LOGMSG_FATAL, "%s: failed to create uprec cron scheduler.\n",
                __func__);
        abort();
    }

    logmsg(LOGMSG_INFO, "upgraderecord sender array initialized\n");
}

int offload_comm_send_upgrade_records(struct dbtable *db, unsigned long long genid)
{
    int rc = 0, stripe, idx;
    struct errstat xerr;

    if (genid == 0)
        return EINVAL;

    /* if i am master of a cluster, return. */
    if (thedb->master == gbl_mynode && net_count_nodes(osql_get_netinfo()) > 1)
        return 0;

    (void)pthread_once(&uprec_sender_array_once, uprec_sender_array_init);

    if (uprec->owner == NULL) {
        rc = pthread_mutex_lock(uprec->lk);
        if (rc == 0) {
            if (uprec->owner == NULL)
                uprec->owner = db;
            rc = pthread_mutex_unlock(uprec->lk);
        }
    }

    if (db == uprec->owner) {
        rc = pthread_mutex_trylock(uprec->lk);
        if (rc == 0) {
            if (db == uprec->owner) {
                // can't pass db and genid to cron scheduler because
                // scheduler will free all arguments after job is done.
                // instead make a global copy here
                uprec->genid = genid;
                uprec->touch = uprec->owner;
                uprec_sched = cron_add_event(
                    uprec_sched, NULL, time_epoch() + uprec->intv,
                    uprec_cron_event, NULL, NULL, NULL, NULL, &xerr);

                if (uprec_sched == NULL)
                    logmsg(LOGMSG_ERROR, "%s: failed to schedule uprec cron job.\n",
                            __func__);

                // zap owner
                uprec->owner = (void *)~(uintptr_t)0;
            }
        }
        rc = pthread_mutex_unlock(uprec->lk);
    }

    return rc;
}

void upgrade_records_stats(void)
{
    if (uprec == NULL)
        return;

    logmsg(LOGMSG_USER, "# %-24s %d recs/req\n", "upgrade ahead records",
           gbl_num_record_upgrades);
    logmsg(LOGMSG_USER, "# %-24s %zu\n", "total requests", uprec->nreqs);
    logmsg(LOGMSG_USER, "# %-24s %zu\n", "bad responses", uprec->nbads);
    logmsg(LOGMSG_USER, "# %-24s %zu\n", "good responses", uprec->ngoods);
    logmsg(LOGMSG_USER, "# %-24s %zu\n", "timeouts", uprec->ntimeouts);
    logmsg(LOGMSG_USER, "%-26s %d s\n", "cron event interval", uprec->intv);
}
/* END OF REPLICANT SIDE UPGRADE RECORD LOGIC } */

int osql_send_bpfunc(char *tonode, unsigned long long rqid, uuid_t uuid,
                     BpfuncArg *arg, int type, SBUF2 *logsb)
{
    netinfo_type *netinfo_ptr = (netinfo_type *)comm->handle_sibling;
    osql_bpfunc_t *dt;
    size_t data_len = bpfunc_arg__get_packed_size(arg);
    size_t osql_bpfunc_size;
    size_t osql_rpl_size;
    uint8_t *p_buf;
    uint8_t *p_buf_end;
    int rc = 0;

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
                    "osqlcomm_schemachange_rpl_type_put");
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
                    "osqlcomm_schemachange_rpl_type_put");
            rc = -1;
            goto freemem;
        }
    }

    if (logsb) {
        sbuf2printf(logsb, "[%llu] send OSQL_BPLOG_FUNC \n");
        sbuf2flush(logsb);
    }

    rc = offload_net_send(tonode, type, p_buf, osql_rpl_size, 0);

freemem:
    if (dt)
        free(dt);
    if (p_buf)
        free(p_buf);

    return rc;
}
