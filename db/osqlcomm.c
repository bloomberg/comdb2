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
#include "osqlsqlnet.h"
#include "osqlsqlsocket.h"
#include "sc_global.h"
#include "logical_cron.h"
#include "sc_logic.h"

#ifdef _SUN_SOURCE
#  include <arpa/nameser_compat.h>
#endif

#ifndef BYTE_ORDER
#  error "MISSING BYTE_ORDER"
#endif

#define MAX_CLUSTER REPMAX

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
int gbl_selectv_writelock = 0;
int gbl_debug_invalid_genid;

static void osql_extract_snap_info(osql_sess_t *sess, void *rpl, int rpllen);

#ifdef XMACRO_OSQL_RPL_TYPES
#   undef XMACRO_OSQL_RPL_TYPES
#endif
#define XMACRO_OSQL_RPL_TYPES(a, b, c) case a: osql_rpl_type_to_str = c; break;
#define OSQL_RPL_TYPE_TO_STR(type)                                             \
({                                                                             \
    char *osql_rpl_type_to_str  = "UNKNOWN";                                   \
    switch (type) {                                                            \
    OSQL_RPL_TYPES                                                             \
    }                                                                          \
    osql_rpl_type_to_str;                                                      \
})

const char *osql_reqtype_str(int type)
{   
    assert(0 <= type && type < MAX_OSQL_TYPES);
    return OSQL_RPL_TYPE_TO_STR(type);
}

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

typedef struct osql_poke_uuid {
    uuid_t uuid; /* look for this session id */
    int tstamp;  /* when this was sent */
} osql_poke_uuid_t;

enum { OSQLCOMM_POKE_UUID_TYPE_LEN = 16 + 4 };

BB_COMPILE_TIME_ASSERT(osqlcomm_poke_uuid_type_len,
                       sizeof(osql_poke_uuid_t) == OSQLCOMM_POKE_UUID_TYPE_LEN);

static uint8_t *osqlcomm_poke_uuid_type_put(const osql_poke_uuid_t *p_poke_type,
                                            uint8_t *p_buf,
                                            const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_POKE_UUID_TYPE_LEN > (p_buf_end - p_buf)) return NULL;
    p_buf = buf_no_net_put(p_poke_type->uuid, sizeof(p_poke_type->uuid), p_buf, p_buf_end);
    p_buf = buf_put(&(p_poke_type->tstamp), sizeof(p_poke_type->tstamp), p_buf, p_buf_end);
    return p_buf;
}

static const uint8_t *osqlcomm_poke_uuid_type_get(osql_poke_uuid_t *p_poke_type,
                                                  const uint8_t *p_buf,
                                                  const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_POKE_UUID_TYPE_LEN > (p_buf_end - p_buf)) return NULL;
    p_buf = buf_no_net_get(p_poke_type->uuid, sizeof(p_poke_type->uuid), p_buf, p_buf_end);
    p_buf = buf_get(&(p_poke_type->tstamp), sizeof(p_poke_type->tstamp), p_buf, p_buf_end);
    return p_buf;
}

struct osql_uuid_req {
    int user_type; // Only sockbplog looks at this
    int unused;
    int sqlqlen;
    int flags;
    uuid_t uuid;
    char tzname[DB_MAX_TZNAMEDB];
    char pad[4];
    char sqlq[0];
};
enum { OSQLCOMM_REQ_UUID_TYPE_LEN = 4 + 4 + 4 + 4 + 16 + DB_MAX_TZNAMEDB + 4 };
BB_COMPILE_TIME_ASSERT(osqlcomm_req_uuid_type_len, sizeof(struct osql_uuid_req) == OSQLCOMM_REQ_UUID_TYPE_LEN);

static uint8_t *
osqlcomm_req_uuid_type_put(const struct osql_uuid_req *p_osql_req,
                           uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_REQ_UUID_TYPE_LEN > (p_buf_end - p_buf)) return NULL;
    p_buf = buf_put(&p_osql_req->user_type, sizeof(p_osql_req->user_type), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&p_osql_req->unused, sizeof(p_osql_req->unused), p_buf, p_buf_end);
    p_buf = buf_put(&p_osql_req->sqlqlen, sizeof(p_osql_req->sqlqlen), p_buf, p_buf_end);
    p_buf = buf_put(&p_osql_req->flags, sizeof(p_osql_req->flags), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&p_osql_req->uuid, sizeof(p_osql_req->uuid), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&p_osql_req->tzname, sizeof(p_osql_req->tzname), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&p_osql_req->pad, sizeof(p_osql_req->pad), p_buf, p_buf_end);
    return p_buf;
}

static const uint8_t *
osqlcomm_req_uuid_type_get(struct osql_uuid_req *p_osql_req,
                           const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_REQ_UUID_TYPE_LEN > (p_buf_end - p_buf)) return NULL;
    p_buf = buf_get(&(p_osql_req->user_type), sizeof(p_osql_req->user_type), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_osql_req->unused), sizeof(p_osql_req->unused), p_buf, p_buf_end);
    p_buf = buf_get(&p_osql_req->sqlqlen, sizeof(p_osql_req->sqlqlen), p_buf, p_buf_end);
    p_buf = buf_get(&p_osql_req->flags, sizeof(p_osql_req->flags), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&p_osql_req->uuid, sizeof(p_osql_req->uuid), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&p_osql_req->tzname, sizeof(p_osql_req->tzname), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&p_osql_req->pad, sizeof(p_osql_req->pad), p_buf, p_buf_end);
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
    if (comdb2uuid_is_zero(p_osql_rpl->uuid))
        abort();
    p_buf = buf_put(&(p_osql_rpl->type), sizeof(p_osql_rpl->type), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_osql_rpl->padding), sizeof(p_osql_rpl->padding), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_osql_rpl->uuid), sizeof(p_osql_rpl->uuid), p_buf, p_buf_end);
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

static uint8_t *osqlcomm_bpfunc_uuid_rpl_type_put(osql_uuid_rpl_t *hd,
                                                  osql_bpfunc_t *dt,
                                                  uint8_t *p_buf,
                                                  uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_BPFUNC_TYPE_LEN + dt->data_len >
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

typedef struct osql_done_uuid_rpl {
    osql_uuid_rpl_t hd;
    osql_done_t dt;
    struct query_effects effects;
    struct query_effects fk_effects;
} osql_done_uuid_rpl_t;

enum {
    OSQLCOMM_DONE_UUID_RPL_v1_LEN = OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_DONE_TYPE_LEN,
    OSQLCOMM_DONE_UUID_RPL_v2_LEN = OSQLCOMM_DONE_UUID_RPL_v1_LEN + (2 * sizeof(struct query_effects)),
};

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

/**
 * Copy and pack the host-ordered client_query_stats type into big-endian
 * format.  This routine only packs up to the path_stats component:  use
 * client_query_path_commponent_put to pack each of the path_stats
 *
 */
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

typedef struct osql_exists_uuid_rpl {
    osql_uuid_rpl_t hd;
    osql_exists_t dt;
} osql_exists_uuid_rpl_t;

enum {
    OSQLCOMM_EXISTS_UUID_RPL_TYPE_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_EXISTS_TYPE_LEN
};

static uint8_t *
osqlcomm_exists_uuid_rpl_type_put(const osql_exists_uuid_rpl_t *p_exists_rpl,
                                  uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_EXISTS_UUID_RPL_TYPE_LEN > (p_buf_end - p_buf))
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
    if (p_buf_end < p_buf || OSQLCOMM_EXISTS_UUID_RPL_TYPE_LEN > (p_buf_end - p_buf))
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
    unsigned long long unused;
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
    if (p_buf_end < p_buf || OSQLCOMM_DBGLOG_TYPE_LEN > (p_buf_end - p_buf)) return NULL;
    p_buf = buf_put(&(p_dbglog->opcode), sizeof(p_dbglog->opcode), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_dbglog->padding), sizeof(p_dbglog->padding), p_buf, p_buf_end);
    p_buf = buf_put(&(p_dbglog->unused), sizeof(p_dbglog->unused), p_buf, p_buf_end);
    p_buf = buf_put(&(p_dbglog->dbglog_cookie), sizeof(p_dbglog->dbglog_cookie), p_buf, p_buf_end);
    p_buf = buf_put(&(p_dbglog->queryid), sizeof(p_dbglog->queryid), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_dbglog->padding2), sizeof(p_dbglog->padding2), p_buf, p_buf_end);
    return p_buf;
}

static const uint8_t *osqlcomm_dbglog_type_get(osql_dbglog_t *p_dbglog,
                                               const uint8_t *p_buf,
                                               const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_DBGLOG_TYPE_LEN > (p_buf_end - p_buf)) return NULL;
    p_buf = buf_get(&(p_dbglog->opcode), sizeof(p_dbglog->opcode), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_dbglog->padding), sizeof(p_dbglog->padding), p_buf, p_buf_end);
    p_buf = buf_get(&(p_dbglog->unused), sizeof(p_dbglog->unused), p_buf, p_buf_end);
    p_buf = buf_get(&(p_dbglog->dbglog_cookie), sizeof(p_dbglog->dbglog_cookie), p_buf, p_buf_end);
    p_buf = buf_get(&(p_dbglog->queryid), sizeof(p_dbglog->queryid), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_dbglog->padding2), sizeof(p_dbglog->padding2), p_buf, p_buf_end);
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

static const char *reqtypes[OSQL_MAX_REQ] = {"invalid", "osql", "recom", "serial", "snapisol"};
static osql_stats_t stats[OSQL_MAX_REQ] = {{0}};
static osql_comm_t *thecomm_obj = NULL;

static osql_comm_t *get_thecomm(void)
{
    return thecomm_obj;
}

static int net_osql_rpl_tail(void *hndl, void *uptr, char *fromnode,
                             int usertype, void *dtap, int dtalen);

static void net_osql_rpl(void *hndl, void *uptr, char *fromnode,
                         struct interned_string *frominterned, int usertype, void *dtap, int dtalen,
                         uint8_t is_tcp);
static void net_osql_req(void *hndl, void *uptr, char *fromnode,
                         struct interned_string *frominterned, int usertype, void *dtap, int dtalen,
                         uint8_t is_tcp);
static void net_recom_req(void *hndl, void *uptr, char *fromnode,
                          struct interned_string *frominterned, int usertype,
                          void *dtap, int dtalen, uint8_t is_tcp);
static void net_snapisol_req(void *hndl, void *uptr, char *fromnode,
                             struct interned_string *frominterned,int usertype, void *dtap, int dtalen,
                             uint8_t is_tcp);
static void net_serial_req(void *hndl, void *uptr, char *fromnode,
                           struct interned_string *frominterned, int usertype,
                           void *dtap, int dtalen, uint8_t is_tcp);

static int net_osql_nodedwn(netinfo_type *netinfo_ptr, struct interned_string *node);
static void net_osql_master_check(void *hndl, void *uptr, char *fromnode,
                                  struct interned_string *frominterned,
                                  int usertype, void *dtap, int dtalen,
                                  uint8_t is_tcp);
static void net_osql_master_checked(void *hndl, void *uptr, char *fromnode,
                                    struct interned_string *frominterned,
                                    int usertype, void *dtap, int dtalen,
                                    uint8_t is_tcp);
static void net_sorese_signal(void *hndl, void *uptr, char *fromnode,
                              struct interned_string *frominterned,
                              int usertype, void *dtap, int dtalen,
                              uint8_t is_tcp);

static void net_startthread_rtn(void *arg);
static void net_stopthread_rtn(void *arg);

static void signal_rtoff(void);

static int check_master(const osql_target_t *target);
static int sorese_rcvreq(char *fromhost, void *dtap, int dtalen, OSQL_REQ_TYPE);

static void net_osql_rcv_echo_ping(void *hndl, void *uptr, char *fromnode,
                                   struct interned_string *frominterned,
                                   int usertype, void *dtap, int dtalen,
                                   uint8_t is_tcp);

static void net_osql_rcv_echo_pong(void *hndl, void *uptr, char *fromnode,
                                   struct interned_string *fromintern, int usertype,
                                   void *dtap, int dtalen,
                                   uint8_t is_tcp);
static void net_block_req(void *hndl, void *uptr, char *fromhost,
                          struct interned_string *fromintern, int usertype,
                          void *dtap, int dtalen, uint8_t is_tcp);
static void net_block_reply(void *hndl, void *uptr, char *fromhost,
                            struct interned_string *frominterned,
                            int usertype, void *dtap, int dtalen,
                            uint8_t is_tcp);

static int netrpl2req(int netrpltype)
{
    switch (netrpltype) {
    case NET_OSQL_SOCK_RPL_UUID: return OSQL_SOCK_REQ;
    case NET_OSQL_RECOM_RPL_UUID: return OSQL_RECOM_REQ;
    case NET_OSQL_SNAPISOL_RPL_UUID: return OSQL_SNAPISOL_REQ;
    case NET_OSQL_SERIAL_RPL_UUID: return OSQL_SERIAL_REQ;
    default: abort();
    }
    logmsg(LOGMSG_ERROR, "%s: unknown request type %d\n", __func__, netrpltype);
    {
        static int once = 0;
        if (!once) {
            cheap_stack_trace();
            once = 1;
        }
    }
    return 0; /* convenience to use this directly for indexing */
}


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
    net_register_handler(tmp->handle_sibling, NET_OSQL_RECOM_RPL_UUID, "osql_recom_rpl_uuid", net_osql_rpl);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SERIAL_RPL_UUID, "osql_serial_rpl_uuid", net_osql_rpl);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SNAPISOL_RPL_UUID, "osql_snapisol_rpl_uuid", net_osql_rpl);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SOCK_RPL_UUID, "osql_sock_rpl_uuid", net_osql_rpl);

    net_register_handler(tmp->handle_sibling, NET_OSQL_MASTER_CHECKED_UUID, "osql_master_checked_uuid", net_osql_master_checked);
    net_register_handler(tmp->handle_sibling, NET_OSQL_MASTER_CHECK_UUID, "osql_master_check_uuid", net_osql_master_check);
    net_register_handler(tmp->handle_sibling, NET_OSQL_RECOM_REQ_UUID, "osql_recom_req_uuid", net_recom_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SERIAL_REQ_UUID, "osql_serial_req_uuid", net_serial_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SIGNAL_UUID, "osql_signal_uuid", net_sorese_signal);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SNAPISOL_REQ_UUID, "osql_snapisol_req_uuid", net_snapisol_req);
    net_register_handler(tmp->handle_sibling, NET_OSQL_SOCK_REQ_UUID, "osql_sock_req_uuid", net_osql_req);

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

    /* remote blocksql request handler. */
    net_register_handler(tmp->handle_sibling, NET_BLOCK_REQ, "block_req", net_block_req);
    net_register_handler(tmp->handle_sibling, NET_BLOCK_REPLY, "block_reply", net_block_reply);

    rc = net_init(tmp->handle_sibling);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s init_network failed\n", __func__);
        exit(1);
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

typedef struct {
    struct buf_lock_t *lock;
    int datalen;
    int rc;
    uint8_t data[0];
} net_block_msg_t;
enum { NET_BLOCK_MSG_TYPE_LEN = 8 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(net_block_msg_type_len, sizeof(net_block_msg_t) == NET_BLOCK_MSG_TYPE_LEN);

int offload_comm_send_blockreq(char *host, struct buf_lock_t *lock, void *buf, int buflen)
{
    /* This is used for:
     * 1. Replicant forwarding a tag-write received on socket to master.
     * 2. Trickle schema version upgrade for rows.
     * This message hdr net_block_msg_t was never endianized, and so datalen
     * and rc are effectively little-endian on the wire. It also used to send
     * pointer to struct buf_lock_t as an rqid. I'm making the pointer explicit
     * in net_block_msg_t. */
#   if BYTE_ORDER == BIG_ENDIAN
    net_block_msg_t m = {.lock = lock, .datalen = flibc_intflip(buflen)};
#   elif BYTE_ORDER == LITTLE_ENDIAN
    net_block_msg_t m = {.lock = lock, .datalen = buflen};
#   endif
    return offload_net_send(host, NET_BLOCK_REQ, &m, sizeof(m), 1, buf, buflen);
}

static void net_block_req(void *hndl, void *uptr, char *fromhost,
                          struct interned_string *frominterned, int usertype,
                          void *dtap, int dtalen, uint8_t is_tcp)
{
    net_block_msg_t *m = dtap;
#   if BYTE_ORDER == BIG_ENDIAN
    m->datalen = flibc_intflip(m->datalen);
#   endif
    handle_buf_block_offload(thedb, m->data, m->data + m->datalen, 0, fromhost, m->lock);
}

int offload_comm_send_blockreply(char *host, struct buf_lock_t *lock, void *buf, int buflen, int return_code)
{
#   if BYTE_ORDER == BIG_ENDIAN
    net_block_msg_t m = {.lock = lock, .datalen = flibc_intflip(buflen), .rc = flibc_intflip(return_code)};
#   elif BYTE_ORDER == LITTLE_ENDIAN
    net_block_msg_t m = {.lock = lock, .datalen = buflen, .rc = return_code};
#   endif
    return offload_net_send(host, NET_BLOCK_REPLY, &m, sizeof(m), 1, buf, buflen);
}

static void net_block_reply(void *hndl, void *uptr, char *fromhost,
                            struct interned_string *frominterned,
                            int usertype, void *dtap, int dtalen,
                            uint8_t is_tcp)
{
    net_block_msg_t *m = dtap;
#   if BYTE_ORDER == BIG_ENDIAN
    m->datalen = flibc_intflip(m->datalen);
    m->rc = flibc_intflip(m->rc);
#   endif
    struct buf_lock_t *p_slock = m->lock;
    {
        Pthread_mutex_lock(&p_slock->req_lock);
        if (p_slock->reply_state == REPLY_STATE_DISCARD) {
            /* The tag request is handled by master. However by the time
               (1000+ seconds) the replicant receives the reply from master,
               the tag request is already discarded. */
            Pthread_mutex_unlock(&p_slock->req_lock);
            cleanup_lock_buffer(p_slock);
        } else {
            p_slock->rc = m->rc;
            sndbak_open_socket(p_slock->sb, m->data, m->datalen, m->rc);
            signal_buflock(p_slock);
            Pthread_mutex_unlock(&p_slock->req_lock);
        }
    }
}

void log_snap_info_key(snap_uid_t *snap_info)
{
    if (snap_info)
        logmsg(LOGMSG_USER, "%*s", snap_info->keylen - 3, snap_info->key);
    else
        logmsg(LOGMSG_USER, "NO_CNONCE"); // ex. SC
}

int gbl_disable_cnonce_blkseq;

/**
 * If "rpl" is a done packet, set xerr to error if any and return 1
 * If "rpl" is a recognizable packet, returns the length of the data type is
 *recognized,
 * or -1 otherwise
 *
 */
int osql_comm_is_done(osql_sess_t *sess, int type, char *rpl, int rpllen, struct errstat **xerr,
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
        osql_extract_snap_info(sess, rpl, rpllen);
        /* fall-through */
    case OSQL_DONE:
        if (xerr)
            *xerr = NULL;
        rc = 1;
        break;
    case OSQL_DONE_WITH_EFFECTS:
        if (effects) {
            const uint8_t *p_buf = (uint8_t *)rpl + sizeof(osql_done_t) + sizeof(osql_uuid_rpl_t);
            const uint8_t *p_buf_end = (const uint8_t *)rpl + rpllen;
            if ((p_buf = osqlcomm_query_effects_get(effects, p_buf, p_buf_end)) == NULL) {
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
            *xerr = &((osql_done_xerr_uuid_t *)rpl)->dt;
        }
        rc = 1;
        break;
    default:
        if (sess)
            sess->is_delayed = 1;
        break;
    }
    return rc;
}

/**
 * Send a "POKE" message to "tohost" inquering about session "rqid"
 *
 */
int osql_comm_send_poke(const char *tohost, uuid_t uuid)
{
    int rc = 0;

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
    rc = offload_net_send(tohost, NET_OSQL_MASTER_CHECK_UUID, &buf, sizeof(buf), 1, NULL, 0);

    return rc;
}

int is_tablename_queue(const char *name)
{
    /* See also, __db_open @ /berkdb/db/db_open.c for register_qdb */
    return strncmp(name, "__q", 3) == 0;
}

int osql_send_startgen(osql_target_t *target, uuid_t uuid, uint32_t start_gen)
{
    uint8_t buf[OSQLCOMM_STARTGEN_UUID_RPL_LEN];
    int msglen;
    int rc;

    if (check_master(target))
        return OSQL_SEND_ERROR_WRONGMASTER;

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

    if (gbl_enable_osql_logging) {
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%s] send OSQL_STARTGEN %u\n", comdb2uuidstr(uuid, us), start_gen);
    }

    rc = target->send(target, NET_OSQL_SOCK_RPL_UUID, &buf, msglen, 0, NULL, 0);

    if (rc)
        logmsg(LOGMSG_ERROR, "%s target->send returns rc=%d\n", __func__, rc);

    return rc;
}

/**
 * Send USEDB op
 * It handles remote/local connectivity
 *
 */
int osql_send_usedb(osql_target_t *target, uuid_t uuid, char *tablename, unsigned long long tableversion)
{
    unsigned short tablenamelen = strlen(tablename) + 1; /*including trailing 0*/
    int msglen;
    int rc = 0;
    int sent;

    uint8_t buf[OSQLCOMM_USEDB_RPL_UUID_TYPE_LEN];

    if (check_master(target))
        return OSQL_SEND_ERROR_WRONGMASTER;

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

    if (gbl_enable_osql_logging) {
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%s] send OSQL_USEDB %.*s\n",
               comdb2uuidstr(uuid, us), tablenamelen, tablename);
    }

    /* tablename field is not null-terminated -- send rest of tablename */
    rc = target->send(target, NET_OSQL_SOCK_RPL_UUID, &buf, msglen, 0,
                      (tablenamelen > sent) ? tablename + sent : NULL,
                      (tablenamelen > sent) ? tablenamelen - sent : 0);

    if (rc)
        logmsg(LOGMSG_ERROR, "%s target->send returns rc=%d\n", __func__, rc);

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
int osql_send_updcols(osql_target_t *target, uuid_t uuid, unsigned long long seq, int *colList, int ncols)
{
    int rc = 0;
    int didmalloc = 0;
    int i;
    int datalen = sizeof(int) * ncols;
    uint8_t *buf;
    uint8_t *p_buf;
    uint8_t *p_buf_end;

    if (check_master(target))
        return OSQL_SEND_ERROR_WRONGMASTER;

    if (ncols <= 0)
        return -2;
    int totlen = datalen + OSQLCOMM_UPDCOLS_UUID_RPL_TYPE_LEN;

    if (totlen > 4096) {
        buf = malloc(totlen);
        didmalloc = 1;
    } else {
        buf = alloca(totlen);
    }

    p_buf = buf;
    p_buf_end = (p_buf + totlen);
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

    for (i = 0; i < ncols; i++) {
        p_buf = buf_put(&colList[i], sizeof(int), p_buf, p_buf_end);
    }

    if (gbl_enable_osql_logging) {
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%s] send OSQL_UPDCOLS %d\n", comdb2uuidstr(uuid, us), ncols);
    }

    rc = target->send(target, NET_OSQL_SOCK_RPL_UUID, buf, totlen, 0, NULL, 0);

    if (didmalloc)
        free(buf);

    return rc;
}

/**
 * Send INDEX op
 * It handles remote/local connectivity
 *
 */
int osql_send_index(osql_target_t *target, uuid_t uuid, unsigned long long genid, int isDelete,
                    int ixnum, char *pData, int nData)
{
    int msglen;
    uint8_t buf[OSQLCOMM_INDEX_UUID_RPL_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end = NULL;

    if (check_master(target))
        return OSQL_SEND_ERROR_WRONGMASTER;

    osql_index_uuid_rpl_t index_uuid_rpl = {{0}};

    index_uuid_rpl.hd.type = isDelete ? OSQL_DELIDX : OSQL_INSIDX;
    comdb2uuidcpy(index_uuid_rpl.hd.uuid, uuid);
    index_uuid_rpl.dt.seq = genid;
    index_uuid_rpl.dt.ixnum = ixnum;
    index_uuid_rpl.dt.nData = nData;

    p_buf_end = p_buf + OSQLCOMM_INDEX_UUID_RPL_TYPE_LEN;

    if (!(p_buf = osqlcomm_index_uuid_rpl_type_put(&index_uuid_rpl, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s:osqlcomm_index_uuid_rpl_type_put returns NULL\n", __func__);
        return -1;
    }
    msglen = sizeof(index_uuid_rpl);

    if (gbl_enable_osql_logging) {
        unsigned long long lclgenid = bdb_genid_to_host_order(genid);
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%s] send %s %llx (%lld)\n", comdb2uuidstr(uuid, us),
               isDelete ? "OSQL_DELIDX" : "OSQL_INSIDX", lclgenid, lclgenid);
    }

    return target->send(target, NET_OSQL_SOCK_RPL_UUID, buf, msglen, 0,
                        (nData > 0) ? pData : NULL, (nData > 0) ? nData : 0);
}

/**
 * Send QBLOB op
 * It handles remote/local connectivity
 *
 */
int osql_send_qblob(osql_target_t *target, uuid_t uuid, int blobid, unsigned long long seq, char *data, int datalen)
{
    int sent;
    uint8_t buf[OSQLCOMM_QBLOB_UUID_RPL_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end;
    int msgsz = 0;

    if (check_master(target))
        return OSQL_SEND_ERROR_WRONGMASTER;

    osql_qblob_uuid_rpl_t rpl_uuid = {{0}};
    p_buf_end = p_buf + OSQLCOMM_QBLOB_UUID_RPL_TYPE_LEN;

    rpl_uuid.hd.type = OSQL_QBLOB;
    comdb2uuidcpy(rpl_uuid.hd.uuid, uuid);
    rpl_uuid.dt.id = blobid;
    rpl_uuid.dt.seq = seq;
    rpl_uuid.dt.bloblen = datalen;

    if (!(p_buf = osqlcomm_qblob_uuid_rpl_type_put(&rpl_uuid, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s:osqlcomm_qblob_uuid_rpl_type_put returns NULL\n", __func__);
        return -1;
    }

    msgsz = OSQLCOMM_QBLOB_UUID_RPL_TYPE_LEN;
    sent = ((signed)sizeof(rpl_uuid.dt.blob) < datalen)
               ? sizeof(rpl_uuid.dt.blob)
               : datalen;

    /*
     * the protocol is that we always send 8 bytes of blob data, even if the
     * bloblen is less than 8.  The first 8 bytes of blob data will be sent as
     * part of the qblob structure, the rest will be the tail.
     *
     * p_buf is pointing at the blob-data offset in the qblob structure.  Zero
     * this out to handle the cases where the bloblen is less than 8 bytes.
     */

    /*
     * If there is any blob-data at all, copy up to the 8th byte into the rpl
     * buffer.
     */
    if (datalen > 0)
        p_buf = buf_no_net_put(data, sent, p_buf, p_buf_end);

    if (gbl_enable_osql_logging) {
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%s] send OSQL_QBLOB %d %d\n", comdb2uuidstr(uuid, us), blobid, datalen);
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

    return target->send(target, NET_OSQL_SOCK_RPL_UUID, buf, msgsz, 0,
                        (datalen > sent) ? data + sent : NULL,
                        (datalen > sent) ? datalen - sent : 0);
}

/**
 * Send UPDREC op
 * It handles remote/local connectivity
 *
 */
int osql_send_updrec(osql_target_t *target, uuid_t uuid, unsigned long long genid,
                     unsigned long long ins_keys, unsigned long long del_keys, char *pData,
                     int nData)
{
    uint8_t buf[OSQLCOMM_UPD_UUID_RPL_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end;
    int sent;
    int msgsz;
    int send_dk = 0;

    if (gbl_partial_indexes && ins_keys != -1ULL && del_keys != -1ULL)
        send_dk = 1;

    if (check_master(target))
        return OSQL_SEND_ERROR_WRONGMASTER;

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

    /*
     * p_buf is pointing at the beginning of the pData section of upd_rpl.  Zero
     * this for the case where the length is less than 8
     */

    if (nData > 0) {
        p_buf = buf_no_net_put(pData, nData < sent ? nData : sent, p_buf,
                               p_buf_end);
    }

    if (gbl_enable_osql_logging) {
        uuidstr_t us;
        comdb2uuidstr(uuid, us);
        unsigned long long lclgenid = bdb_genid_to_host_order(genid);
        logmsg(LOGMSG_DEBUG, "[%s] send OSQL_UPDREC %llx (%lld)\n", us, lclgenid, lclgenid);
    }

    return target->send(target, NET_OSQL_SOCK_RPL_UUID, &buf, msgsz, 0,
                        (nData > sent) ? pData + sent : NULL,
                        (nData > sent) ? nData - sent : 0);
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
int osql_send_dbglog(osql_target_t *target, uuid_t uuid, unsigned long long dbglog_cookie, int queryid)
{
    osql_dbglog_t req = {0};
    uint8_t buf[OSQLCOMM_DBGLOG_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end = p_buf + OSQLCOMM_DBGLOG_TYPE_LEN;

    if (check_master(target))
        return OSQL_SEND_ERROR_WRONGMASTER;

    req.opcode = OSQL_DBGLOG;
    req.unused = 0;
    req.dbglog_cookie = dbglog_cookie;
    req.queryid = queryid;

    if (!(osqlcomm_dbglog_type_put(&req, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s:%s return NULL\n", __func__,
                "osqlcomm_dbglog_type_put");
        return -1;
    }

    return target->send(target, NET_OSQL_SOCK_RPL_UUID, &buf, sizeof(osql_dbglog_t), 0, NULL, 0);
}

/**
 * Send UPDSTAT op
 * It handles remote/local connectivity
 *
 */
int osql_send_updstat(osql_target_t *target, uuid_t uuid, unsigned long long seq, char *pData,
                      int nData, int nStat)
{
    osql_updstat_uuid_rpl_t updstat_rpl_uuid = {{0}};
    uint8_t buf[OSQLCOMM_UPDSTAT_UUID_RPL_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end = p_buf + sizeof(buf);
    int sent;
    int msglen;

    if (check_master(target))
        return OSQL_SEND_ERROR_WRONGMASTER;

    updstat_rpl_uuid.hd.type = OSQL_UPDSTAT;
    comdb2uuidcpy(updstat_rpl_uuid.hd.uuid, uuid);
    updstat_rpl_uuid.dt.seq = seq;
    updstat_rpl_uuid.dt.nData = nData;
    updstat_rpl_uuid.dt.nStat = nStat;

    if (!(p_buf = osqlcomm_updstat_uuid_rpl_type_put(&updstat_rpl_uuid, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__, "osqlcomm_updstat_uuid_rpl_type_put");
        return -1;
    }
    memset(p_buf, 0, sizeof(updstat_rpl_uuid.dt.pData));
    msglen = sizeof(updstat_rpl_uuid);
    sent = sizeof(updstat_rpl_uuid.dt.pData);

    if (nData > 0) {
        p_buf = buf_no_net_put(pData, nData < sent ? nData : sent, p_buf, p_buf_end);
    }

    if (gbl_enable_osql_logging) {
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%s] send OSQL_UPDSTATREC %llx (%lld)\n",
               comdb2uuidstr(uuid, us), seq, seq);
    }

    return target->send(target, NET_OSQL_SOCK_RPL_UUID, buf, msglen, 0,
                        (nData > sent) ? pData + sent : NULL,
                        (nData > sent) ? nData - sent : 0);
}

/**
 * Send INSREC op
 * It handles remote/local connectivity
 *
 */
int osql_send_insrec(osql_target_t *target, uuid_t uuid, unsigned long long genid,
                     unsigned long long dirty_keys, char *pData, int nData, int upsert_flags)
{
    int msglen;
    uint8_t buf[OSQLCOMM_INS_UUID_RPL_TYPE_LEN];
    int sent;
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end = NULL;
    int send_dk = 0;

    if (gbl_partial_indexes && dirty_keys != -1ULL)
        send_dk = 1;

    if (check_master(target))
        return OSQL_SEND_ERROR_WRONGMASTER;

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

    if (!(p_buf = osqlcomm_ins_uuid_rpl_type_put(&ins_uuid_rpl, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s:osqlcomm_ins_uuid_rpl_type_put  returns NULL\n", __func__);
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

    if (nData > 0) {
        p_buf = buf_no_net_put(pData, nData < sent ? nData : sent, p_buf,
                               p_buf_end);
    }

    if (gbl_enable_osql_logging) {
        unsigned long long lclgenid = bdb_genid_to_host_order(genid);
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%s] OSQL_INSERT %llx (%lld)\n", comdb2uuidstr(uuid, us),
               lclgenid, lclgenid);
    }

    return target->send(target, NET_OSQL_SOCK_RPL_UUID, buf, msglen, 0,
                        (nData > sent) ? pData + sent : NULL,
                        (nData > sent) ? nData - sent : 0);
}

int osql_send_dbq_consume(osql_target_t *target, uuid_t uuid, genid_t genid)
{
    if (check_master(target))
        return OSQL_SEND_ERROR_WRONGMASTER;
    if (gbl_enable_osql_logging) {
        genid_t lclgenid = bdb_genid_to_host_order(genid);
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%s] send OSQL_DBQ_CONSUME %llx (%lld)\n",
               comdb2uuidstr(uuid, us), (long long unsigned)lclgenid,
               (long long unsigned)lclgenid);
    }
    osql_dbq_consume_uuid_t rpl = {0};
    rpl.hd.type = htonl(OSQL_DBQ_CONSUME);
    comdb2uuidcpy(rpl.hd.uuid, uuid);
    rpl.genid = genid;
    return target->send(target, NET_OSQL_SOCK_RPL_UUID, &rpl, sizeof(rpl), 0, NULL, 0);
}


/**
 * Send DELREC op
 * It handles remote/local connectivity
 *
 */
int osql_send_delrec(osql_target_t *target, uuid_t uuid, unsigned long long genid,
                     unsigned long long dirty_keys, int type)
{
    uint8_t buf[OSQLCOMM_OSQL_DEL_UUID_RPL_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end;
    int msgsz;
    int send_dk = 0;

    if (gbl_partial_indexes && dirty_keys != -1ULL)
        send_dk = 1;

    if (check_master(target))
        return OSQL_SEND_ERROR_WRONGMASTER;
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

    if (gbl_enable_osql_logging) {
        unsigned long long lclgenid = bdb_genid_to_host_order(genid);
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%s] send %s %llx (%lld)\n",
               comdb2uuidstr(uuid, us), send_dk ? "OSQL_DELETE" : "OSQL_DELREC",
               lclgenid, lclgenid);
    }

    return target->send(target, type, &buf, msgsz, 0, NULL, 0);
}

/**
 * Send SERIAL READ SET
 *
 */
int osql_send_serial(osql_target_t *target, uuid_t uuid, CurRangeArr *arr, unsigned int file,
                     unsigned int offset, int type)
{
    int used_malloc = 0;
    uint8_t *buf = NULL;
    uint8_t *p_buf;
    uint8_t *p_buf_end;
    int i;
    int b_sz;
    int cr_sz = 0;
    CurRange *cr;

    if (check_master(target))
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

    b_sz = sizeof(osql_serial_uuid_rpl_t) + cr_sz;

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

    osql_serial_uuid_rpl_t serial_rpl = {{0}};

    serial_rpl.hd.type = type == NET_OSQL_SERIAL_RPL_UUID ? OSQL_SERIAL : OSQL_SELECTV;
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

    if (!(p_buf = osqlcomm_serial_uuid_rpl_put(&serial_rpl, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__, "osqlcomm_serial_uuid_rpl_put");
        if (used_malloc)
            free(buf);
        return -1;
    }

    if (arr) {
        p_buf = serial_readset_put(arr, cr_sz, p_buf, p_buf_end);
    }

    return target->send(target, type, buf, b_sz, 1, NULL, 0);
}

int osql_send_commit_by_uuid(struct sqlclntstate *clnt, struct errstat *xerr, snap_uid_t *snap_info)
{
    osqlstate_t *osql = &clnt->osql;
    osql_target_t *target = &osql->target;
    int nops = osql->replicant_numops;
    struct client_query_stats *query_stats = clnt->query_stats;

    osql_done_uuid_rpl_t rpl_ok = {{0}};
    osql_done_xerr_uuid_t rpl_xerr = {{0}};
    int b_sz;
    int used_malloc = 0;
    int snap_info_length = 0;
    uint8_t *buf = NULL;
    uint8_t *p_buf;
    uint8_t *p_buf_end;
    int rc = xerr->errval;

    /* Master does not read query_stats, since R5 maybe.  Do not send them
    unless we decide to fix it first */
    query_stats = NULL;

    /* Always 'commit' to release starthrottle.  Failure if master has swung. */
    if (check_master(target))
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
    bzero(buf, b_sz);

    /* frame output buffer */
    p_buf = buf;
    p_buf_end = (p_buf + b_sz);

    int nettype;
    switch (clnt->dbtran.mode) {
    case TRANLEVEL_SOSQL: nettype =  NET_OSQL_SOCK_RPL_UUID; break;
    case TRANLEVEL_RECOM: nettype = NET_OSQL_RECOM_RPL_UUID; break;
    case TRANLEVEL_SERIAL: nettype = NET_OSQL_SERIAL_RPL_UUID; break;
    case TRANLEVEL_SNAPISOL: nettype = NET_OSQL_SNAPISOL_RPL_UUID; break;
    default: abort();
    }

    if (rc == SQLITE_OK) {
        if (snap_info) {
            rpl_ok.hd.type = OSQL_DONE_SNAP;
        } else {
            rpl_ok.hd.type = OSQL_DONE;
        }
        comdb2uuidcpy(rpl_ok.hd.uuid, osql->uuid);
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
                   comdb2uuidstr(osql->uuid, us), osql_reqtype_str(rpl_ok.hd.type),
                   rc, nops);
        }

#if DEBUG_REORDER
        DEBUGMSG("uuid=%s send %s rc = %d, nops = %d\n",
                 comdb2uuidstr(osql->uuid, us), osql_reqtype_str(rpl_ok.hd.type), rc,
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
        rc = target->send(target, nettype, buf, b_sz, 1, NULL, 0);

    } else {
        rpl_xerr.hd.type = OSQL_XERR;
        comdb2uuidcpy(rpl_xerr.hd.uuid, osql->uuid);

        memcpy(&rpl_xerr.dt, xerr, sizeof(rpl_xerr.dt));

#if DEBUG_REORDER
        uuidstr_t us;
        DEBUGMSG("uuid=%s send %s rc = %d, nops = %d\n",
                 comdb2uuidstr(osql->uuid, us), osql_reqtype_str(rpl_xerr.hd.type),
                 rc, nops);
#endif
        if (!osqlcomm_done_xerr_uuid_type_put(&rpl_xerr, p_buf, p_buf_end)) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__, "osqlcomm_done_xerr_uuid_type_put");
            if (used_malloc)
                free(buf);
            return -1;
        }
        rc = target->send(target, nettype, buf, sizeof(rpl_xerr), 1, NULL, 0);
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
int osql_comm_send_socksqlreq(struct sqlclntstate *clnt, OSQL_REQ_TYPE osql_type, int user_type, int flags)
{
    osqlstate_t *osql = &clnt->osql;
    osql_target_t *target = &osql->target;
    char *sql = clnt->sql;
    int sqlen = strlen(sql) + 1;
    osql_uuid_req_t out, req = {0};
    req.user_type = user_type; /* for sockbplog, it doesn't send user_type like net */
    req.flags = flags;
    comdb2uuidcpy(req.uuid, osql->uuid);
    req.sqlqlen = sqlen;
    strncpy0(req.tzname, clnt->tzname, sizeof(req.tzname));
    int rc = -1;
    if (osqlcomm_req_uuid_type_put(&req, (void *)&out, (void *)(&out + 1)) != NULL) {
        rc = target->send(target, user_type, &out, sizeof(out), 1, sql, sqlen);
    }
    if (rc) stats[osql_type].snd_failed++;
    else stats[osql_type].snd++;
    return rc;
}

/**
 * Sends the result of block processor transaction commit
 * to the sql thread so that it can return the result to the
 * client
 *
 */
int osql_comm_signal_sqlthr_rc(osql_target_t *target, uuid_t uuid, int nops, struct errstat *xerr,
                               snap_uid_t *snap, int rc)
{
    uuidstr_t us;
    int msglen = 0;
    union {
        char a[OSQLCOMM_DONE_XERR_UUID_RPL_LEN];
        char b[OSQLCOMM_DONE_UUID_RPL_v2_LEN];
    } largest_message;
    uint8_t *buf = (uint8_t *)&largest_message;

    /* test if the sql thread was the one closing the request,
     * and if so, don't send anything back, request might be gone already anyway
     */
    if (xerr->errval == SQLITE_ABORT)
        return 0;

    /* if error, lets send the error string */
    if (target->host == gbl_myhostname) {
        /* local */
        return osql_chkboard_sqlsession_rc(uuid, nops, snap, xerr, (snap) ? &snap->effects : NULL, target->host);
    }

    /* remote */
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
    logmsg(LOGMSG_DEBUG,
           "%s:%d master signaling %s uuid %s with rc=%d xerr=%d\n",
           __func__, __LINE__, target->host, comdb2uuidstr(uuid, us), rc,
           xerr->errval);

    /* lazy again, works just because node!=0 */
    int irc = target->send(target, NET_OSQL_SIGNAL_UUID, buf, msglen, 1, NULL, 0);
    if (irc) {
        irc = -1;
        logmsg(LOGMSG_ERROR, "%s: error sending done to %s!\n", __func__,
               target->host);
    }
    return irc;
}

/**
 * Report on the traffic noticed
 *
 */
void osql_comm_quick_stat(void)
{
    for (int i = 1; i < OSQL_MAX_REQ; i++) {
        logmsg(LOGMSG_USER, "%s snd(failed) %lu(%lu) rcv(failed, redundant) %lu(%lu,%lu)\n",
               reqtypes[i], stats[i].snd, stats[i].snd_failed, stats[i].rcv,
               stats[i].rcv_failed, stats[i].rcv_rdndt);
    }
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
                                  struct interned_string *frominterned,
                                  int usertype, void *dtap, int dtalen,
                                  uint8_t is_tcp)
{
    uint8_t *p_buf = dtap;
    uint8_t *p_buf_end = p_buf + dtalen;
    osql_poke_uuid_t pokeuuid;
    int rc = 0;

    uuid_t uuid;
    int reply_type;

    comdb2uuid_clear(uuid);

    if (db_is_exiting()) {
        /* don't do anything, we're going down */
        return;
    }

    if (!osqlcomm_poke_uuid_type_get(&pokeuuid, p_buf, p_buf_end)) {
        logmsg(LOGMSG_ERROR, "%s: can't unpack %d request\n", __func__,
                usertype);
        return;
    }
    comdb2uuidcpy(uuid, pokeuuid.uuid);

    int rows_affected = -1;
    int found = osql_repository_session_exists(uuid, &rows_affected);
    if (found) {
        uint8_t bufuuid[OSQLCOMM_EXISTS_UUID_RPL_TYPE_LEN];
        uint8_t *p_buf;

        p_buf = bufuuid;
        p_buf_end = p_buf + OSQLCOMM_EXISTS_UUID_RPL_TYPE_LEN;
        osql_exists_uuid_rpl_t rpl = {{0}};

        rpl.hd.type = OSQL_EXISTS;
        comdb2uuidcpy(rpl.hd.uuid, uuid);
        rpl.dt.status = rows_affected;
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
        uuidstr_t us;
        logmsg(LOGMSG_ERROR,
               "Missing SORESE sql session %s on %s from %s\n",
               comdb2uuidstr(uuid, us), gbl_myhostname, fromhost);
    }
}

static void net_osql_master_checked(void *hndl, void *uptr, char *fromhost,
                                    struct interned_string *frominterned,
                                    int usertype, void *dtap, int dtalen,
                                    uint8_t is_tcp)
{
    uint8_t *p_buf = dtap;
    uint8_t *p_buf_end = p_buf + dtalen;
    int rc = 0;
    uuid_t uuid;
    uuidstr_t us;
    int status, timestamp;

    comdb2uuid_clear(uuid);

    if (db_is_exiting()) {
        /* don't do anything, we're going down */
        return;
    }

    osql_exists_uuid_rpl_t rpluuid;
    if (!(osqlcomm_exists_uuid_rpl_type_get(&rpluuid, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: invalid data length\n", __func__);
        return;
    }
    comdb2uuidcpy(uuid, rpluuid.hd.uuid);
    status = rpluuid.dt.status;
    timestamp = rpluuid.dt.timestamp;

    /* update the status of the sorese session */
    rc = osql_checkboard_update_status(uuid, status, timestamp);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to update status for rqid %s rc=%d\n", __func__,
               comdb2uuidstr(uuid, us), rc);
    }
}

/* terminate node */
static int net_osql_nodedwn(netinfo_type *netinfo_ptr, struct interned_string *host)
{

    int rc = 0;

    /* this is mainly for master, but we might not be a master anymore at
       this point */
    rc = osql_repository_terminatenode(host->str);

    /* if only offload net drops, we might lose packets from connection but
       code will not be triggered to informed the sorese sessions that
       socket was dropped; call that here */
    osql_checkboard_check_down_nodes(host->str);

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
static int net_local_route_packet_tail_int(int usertype, void *data, int datalen)
{
    switch (usertype) {
    case NET_OSQL_SOCK_REQ_UUID:
        net_osql_req(NULL, NULL, gbl_myhostname, gbl_myhostname_interned, usertype, data, datalen, 0);
        break;
    case NET_OSQL_RECOM_REQ_UUID:
        net_recom_req(NULL, NULL, gbl_myhostname, gbl_myhostname_interned, usertype, data, datalen, 0);
        break;
    case NET_OSQL_SNAPISOL_REQ_UUID:
        net_snapisol_req(NULL, NULL, gbl_myhostname, gbl_myhostname_interned, usertype, data, datalen, 0);
        break;
    case NET_OSQL_SERIAL_REQ_UUID:
        net_serial_req(NULL, NULL, gbl_myhostname, gbl_myhostname_interned, usertype, data, datalen, 0);
        break;
    case NET_BLOCK_REQ:
        net_block_req(NULL, NULL, 0, 0, usertype, data, datalen, 0);
        break;
    case NET_BLOCK_REPLY:
        net_block_reply(NULL, NULL, 0, 0, usertype, data, datalen, 0);
        break;
    case NET_OSQL_SOCK_RPL_UUID:
    case NET_OSQL_RECOM_RPL_UUID:
    case NET_OSQL_SNAPISOL_RPL_UUID:
    case NET_OSQL_SERIAL_RPL_UUID:
        return net_osql_rpl_tail(NULL, NULL, gbl_myhostname, usertype, data, datalen);

    default:
        logmsg(LOGMSG_ERROR, "%s: unknown packet type routed locally, %d\n", __func__, usertype);
        return -1;
    }
    return 0;
}

static int net_local_route_packet_tail(int usertype, void *data, int datalen, void *tail, int taillen)
{
    void *orig = data;
    if (taillen) {
        data = malloc(datalen + taillen);
        memcpy(data, orig, datalen);
        memcpy((uint8_t*)data + datalen, tail, taillen);
        datalen += taillen;
    }
    int rc = net_local_route_packet_tail_int(usertype, data, datalen);
    if (orig != data) free(data);
    return rc;
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

static int offload_socket_send(SBUF2 *sb, int usertype, void *data, int datalen,
                               int nodelay, void *tail, int tailen)
{
    int rc;

    rc = sbuf2fwrite((char *)data, 1, datalen, sb);
    if (rc != datalen) {
        logmsg(LOGMSG_ERROR, "%s: failed to packet rc=%d\n", __func__, rc);
        return -1;
    }

    if (tail && tailen > 0) {
        rc = sbuf2fwrite((char *)tail, 1, tailen, sb);
        if (rc != tailen) {
            logmsg(LOGMSG_ERROR, "%s: failed to write packet tail rc=%d\n",
                   __func__, rc);
            return -1;
        }
    }

    return 0;
}

static void net_osql_rpl(void *hndl, void *uptr, char *fromnode, struct interned_string *frominterned,
                         int usertype, void *dtap, int dtalen, uint8_t is_tcp)
{
    int found = 0;
    int rc = 0;
    uuid_t uuid;
    int type = 0;
    uint8_t *p_buf = (uint8_t *)dtap;
    uint8_t *p_buf_end = (p_buf + dtalen);

    stats[netrpl2req(usertype)].rcv++;

    osql_uuid_rpl_t p_osql_uuid_rpl;
    if (!(p_buf = (uint8_t *)osqlcomm_uuid_rpl_type_get(
              &p_osql_uuid_rpl, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                "osqlcomm_uuid_rpl_type_get");
        rc = -1;
    } else {
        comdb2uuidcpy(uuid, p_osql_uuid_rpl.uuid);
        type = p_osql_uuid_rpl.type;
    }

    if (!rc) {
        rc = osql_sess_rcvop(uuid, type, dtap, dtalen, &found);
    }
    if (rc)
        stats[netrpl2req(usertype)].rcv_failed++;
    if (!found)
        stats[netrpl2req(usertype)].rcv_rdndt++;
}

static int check_master(const osql_target_t *target)
{
    if (target->type == OSQL_OVER_NET) {
        char *master = thedb->master;

        if (target->host != master) {
            logmsg(LOGMSG_INFO, "%s: master swinged from %s to %s!\n", __func__,
                   target->host, master);
            return -1;
        }
    }

    return 0;
}

static int net_osql_rpl_tail(void *hndl, void *uptr, char *fromhost, int usertype, void *dtap, int dtalen)
{
    int rc;
    int found = 0;
    uuid_t uuid;
    uint8_t *p_buf = dtap;
    uint8_t *p_buf_end = p_buf + dtalen;
    osql_uuid_rpl_t p_osql_rpl;
    if (!osqlcomm_uuid_rpl_type_get(&p_osql_rpl, p_buf, p_buf_end)) {
        logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__, "osqlcomm_rpl_type_get");
        rc = -1;
    } else {
        comdb2uuidcpy(uuid, p_osql_rpl.uuid);
        int type = p_osql_rpl.type;
        rc = osql_sess_rcvop(uuid, type, dtap, dtalen, &found);
    }
    if (rc) stats[netrpl2req(usertype)].rcv_failed++;
    if (!found) stats[netrpl2req(usertype)].rcv_rdndt++;
    stats[netrpl2req(usertype)].rcv++;
    return rc;
}

static void net_osql_req(void *hndl, void *uptr, char *fromhost, struct interned_string *frominterned,
                          int usertype, void *dtap, int dtalen, uint8_t is_tcp)
{

    int rc = 0;

    stats[OSQL_SOCK_REQ].rcv++;

    /* we handle this inline;
       once we are done, the queue is ready for this fromnode:rqid session so
       reader thread is free to receive rows from replicant even if blockproc is
       not yet up for us */
    if ((rc = sorese_rcvreq(fromhost, dtap, dtalen, OSQL_SOCK_REQ))) {
        static int once = 0;
        if (!once) {
            logmsg(LOGMSG_ERROR, "%s:unable to receive request rc=%d\n", __func__,
                    rc);
            once = 1;
        }
        stats[OSQL_SOCK_REQ].rcv_failed++;
    }
}

static void net_recom_req(void *hndl, void *uptr, char *fromhost, 
                          struct interned_string *frominterned, int usertype,
                          void *dtap, int dtalen, uint8_t is_tcp)
{

    int rc = 0;

    stats[OSQL_RECOM_REQ].rcv++;

    /* we handle this inline;
       once we are done, the queue is ready for this fromhost:rqid session so
       reader thread is free to receive rows from replicant even if blockproc is
       not yet up for us */
    rc = sorese_rcvreq(fromhost, dtap, dtalen, OSQL_RECOM_REQ);

    if (rc)
        stats[OSQL_RECOM_REQ].rcv_failed++;
}

static void net_snapisol_req(void *hndl, void *uptr, char *fromhost,
                             struct interned_string *frominterned,
                             int usertype, void *dtap, int dtalen,
                             uint8_t is_tcp)
{

    int rc = 0;

    stats[OSQL_SNAPISOL_REQ].rcv++;

    /* we handle this inline;
       once we are done, the queue is ready for this fromnode:rqid session so
       reader thread is free to receive rows from replicant even if blockproc is
       not yet up for us */
    rc = sorese_rcvreq(fromhost, dtap, dtalen, OSQL_SNAPISOL_REQ);

    if (rc)
        stats[OSQL_SNAPISOL_REQ].rcv_failed++;
}

static void net_serial_req(void *hndl, void *uptr, char *fromhost,
                           struct interned_string *fromintern, int usertype,
                           void *dtap, int dtalen, uint8_t is_tcp)
{

    int rc = 0;

    stats[OSQL_SERIAL_REQ].rcv++;

    /* we handle this inline;
       once we are done, the queue is ready for this fromnode:rqid session so
       reader thread is free to receive rows from replicant even if blockproc is
       not yet up for us */
    rc = sorese_rcvreq(fromhost, dtap, dtalen, OSQL_SERIAL_REQ);

    if (rc)
        stats[OSQL_SERIAL_REQ].rcv_failed++;
}

/**************************** this belongs between comm and blockproc put it here *****/

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
static int start_schema_change_tran_wrapper(const char *tblname,
                                            timepart_sc_arg_t *arg)
{
    struct schema_change_type *sc = arg->s;
    struct ireq *iq = sc->iq;
    int rc;

    if (arg->indx == 0) {
        /* is first shard aliased? */
        struct dbtable *db = get_dbtable_by_name(tblname);
        if (!(db && db->sqlaliasname &&
              strncasecmp(db->sqlaliasname, tblname,
                          strlen(db->sqlaliasname)) == 0))
            strncpy0(sc->tablename, tblname, sizeof(sc->tablename));

    } else {
        strncpy0(sc->tablename, tblname, sizeof(sc->tablename));
    }
    if (gbl_disable_tpsc_tblvers) {
        sc->fix_tp_badvers = 1;
    }

    if (((sc->partition.type == PARTITION_ADD_TIMED ||
          sc->partition.type == PARTITION_ADD_MANUAL) && arg->indx == 0) ||
        (sc->partition.type == PARTITION_REMOVE &&
         arg->nshards == arg->indx + 1)) {
        sc->publish = partition_publish;
        sc->unpublish = partition_unpublish;
    }

    rc = start_schema_change_tran(iq, sc->tran);
    if ((rc != SC_ASYNC && rc != SC_COMMIT_PENDING) ||
        sc->preempted == SC_ACTION_RESUME ||
        sc->kind == SC_ALTERTABLE_PENDING) {
        iq->sc = NULL;
        if (rc != SC_ASYNC && rc != SC_COMMIT_PENDING && sc->nothrevent) {
            /* we need to link the sc into sc_pending so that backout
             * picks it up
             */
            sc->sc_next = iq->sc_pending;
            iq->sc_pending = sc;
            /* mark scdone so that cleanup removes llmeta */
            if (rc != SC_MASTER_DOWNGRADE)
                iq->osql_flags |= OSQL_FLAGS_SCDONE;
        }
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

static int _process_single_table_sc(struct ireq *iq)
{
    struct schema_change_type *sc = iq->sc;
    int rc;

    /* schema change for a regular table */
    rc = start_schema_change_tran(iq, NULL);
    if ((rc != SC_ASYNC && rc != SC_COMMIT_PENDING) ||
        sc->preempted == SC_ACTION_RESUME ||
        sc->kind == SC_ALTERTABLE_PENDING) {
        iq->sc = NULL;
    } else {
        iq->sc->sc_next = iq->sc_pending;
        iq->sc_pending = iq->sc;
        iq->osql_flags |= OSQL_FLAGS_SCDONE;
    }
    return rc;
}

static int start_schema_change_tran_wrapper_merge(const char *tblname,
                                                  timepart_sc_arg_t *arg)
{
    struct schema_change_type *sc = arg->s;
    struct ireq *iq = sc->iq;
    int rc;

    /* first shard drops partition also */
    if (arg->indx == 0) {
        sc->publish = partition_publish;
        sc->unpublish = partition_unpublish;
    }

    struct schema_change_type *alter_sc = clone_schemachange_type(sc);

    /* new target */
    strncpy0(alter_sc->tablename, tblname, sizeof(sc->tablename));
    /*alter_sc->usedbtablevers = sc->partition.u.mergetable.version;*/
    alter_sc->kind = SC_ALTERTABLE;
    /* use the created file as target */
    alter_sc->newdb = sc->newdb;
    alter_sc->force_rebuild = 1; /* we are moving rows here */
    /* alter only in parallel mode for live */
    alter_sc->scanmode = SCAN_PARALLEL;
    /* link the sc */
    iq->sc = alter_sc;

    rc = start_schema_change_tran(iq, NULL);
    /* link the alter */
    iq->sc->sc_next = iq->sc_pending;
    iq->sc_pending = iq->sc;
    iq->sc->newdb = NULL; /* lose ownership, otherwise double free */

    if (rc != SC_ASYNC && rc != SC_COMMIT_PENDING) {
        if (rc != SC_MASTER_DOWNGRADE)
            iq->osql_flags |= OSQL_FLAGS_SCDONE;
        return rc;
    }
    return 0;
}

static int _process_single_table_sc_merge(struct ireq *iq)
{
    struct schema_change_type *sc = iq->sc;
    int rc;

    assert(sc->partition.type == PARTITION_MERGE);

    /* if this is an create .. merge, make sure we create table first
     * if this is an alter .. merge, we still prefer to sequence alters
     * to limit the amount of parallelism in flight
     */
    sc->nothrevent = 1;
    sc->finalize = 0; /* make sure */
    if (sc->kind == SC_ALTERTABLE) {
        /* alter only switches to merge path if this is set */
        sc->partition.type = PARTITION_NONE;
    }
    rc = start_schema_change_tran(iq, NULL);
    iq->sc->sc_next = iq->sc_pending;
    iq->sc_pending = iq->sc;
    if (rc != SC_COMMIT_PENDING) {
        return ERR_SC;
    }

    /* at this point we have created the future btree, launch an alter
     * for the to-be-merged table
     */
    timepart_sc_arg_t arg = {0};
    arg.s = sc;
    arg.s->iq = iq;
    arg.indx = 1; /* no publishing */

    /* need table version */
    sc->usedbtablevers = sc->partition.u.mergetable.version;
    enum comdb2_partition_type old_part_type = sc->partition.type;
    sc->partition.type = PARTITION_MERGE;
    rc = start_schema_change_tran_wrapper_merge(
            sc->partition.u.mergetable.tablename, &arg);
    sc->partition.type = old_part_type;

    return rc;
}

static int _process_partitioned_table_merge(struct ireq *iq)
{
    struct schema_change_type *sc = iq->sc;
    int rc;
    int start_shard = 0;

    assert(sc->kind == SC_ALTERTABLE);

    /* if this was a CREATE & ALTER, first shart is an aliased
     * table with the same name as the partition
     * use that as the destination for merging
     * OTHERWISE, create a new table with the same name as 
     * the partition
     */
    char *first_shard_name = timepart_shard_name(sc->tablename, 0, 0, NULL);
    struct dbtable *first_shard = get_dbtable_by_name(first_shard_name);
    free(first_shard_name);

    /* we need to move data */
    sc->force_rebuild = 1;

    if (!first_shard->sqlaliasname) {
        /*
         * create a table with the same name as the partition
         */
        sc->nothrevent = 1; /* we need do_add_table to run first */
        sc->finalize = 0;   /* make sure */
        sc->kind = SC_ADDTABLE;

        rc = start_schema_change_tran(iq, NULL);
        iq->sc->sc_next = iq->sc_pending;
        iq->sc_pending = iq->sc;
        if (rc != SC_COMMIT_PENDING) {
            if (rc != SC_MASTER_DOWNGRADE)
                iq->osql_flags |= OSQL_FLAGS_SCDONE;
            return ERR_SC;
        }
    } else {
        /*
         * use the fast shard as the destination, after first altering it
         */
        sc->nothrevent = 1; /* we need do_alter_table to run first */
        sc->finalize = 0;
        enum comdb2_partition_type tt = sc->partition.type;
        sc->partition.type = PARTITION_NONE;

        strncpy(sc->tablename, first_shard->tablename, sizeof(sc->tablename));

        rc = start_schema_change_tran(iq, NULL);
        sc->partition.type = tt;
        iq->sc->sc_next = iq->sc_pending;
        iq->sc_pending = iq->sc;
        if (rc != SC_COMMIT_PENDING) {
            if (rc != SC_MASTER_DOWNGRADE)
                iq->osql_flags |= OSQL_FLAGS_SCDONE;
            return ERR_SC;
        }
        start_shard = 1;
        strncpy(sc->newtable, sc->tablename, sizeof(sc->newtable)); /* piggyback a rename with alter */
    }

    /* at this point we have created the future btree, launch an alter
     * for each of the shards of the partition
     */
    timepart_sc_arg_t arg = {0};
    arg.s = sc;
    arg.s->iq = iq;
    arg.indx = start_shard;
    /* note: we have already set nothrevent depending on the number of shards */
    rc = timepart_foreach_shard(
        sc->tablename, start_schema_change_tran_wrapper_merge, &arg, start_shard);

    if (first_shard->sqlaliasname) {
        sc->partition.type = PARTITION_REMOVE; /* first shard is the collapsed table */
        sc->publish = partition_publish;
        sc->unpublish = partition_unpublish;
    }
    return rc;
}

static struct schema_change_type* _create_logical_cron_systable(const char *tblname);

static int _process_single_table_sc_partitioning(struct ireq *iq) 
{
    struct schema_change_type *sc = iq->sc;
    int rc;

    if (sc->partition.type == PARTITION_REMOVE) {
        logmsg(LOGMSG_ERROR, "Partition %s does not exist\n", sc->tablename);
        sc_errf(sc, "Partition %s does not exist\n", sc->tablename);
        return ERR_SC;
    }

    assert(sc->partition.type == PARTITION_ADD_TIMED || 
           sc->partition.type == PARTITION_ADD_MANUAL);

    /* create a new time partition object */
    struct errstat err = {0};
    sc->newpartition = timepart_new_partition(
            sc->tablename, sc->partition.u.tpt.period,
            sc->partition.u.tpt.retention, sc->partition.u.tpt.start, NULL,
            TIMEPART_ROLLOUT_TRUNCATE, &sc->timepartition_name, &err);
    /* DHTEST 1 {
       sc->newpartition = NULL; err.errval = VIEW_ERR_PARAM;
       snprintf(err.errstr, sizeof(err.errstr), "Test fail"); } DHTEST */
    if (!sc->newpartition) {
        assert(err.errval != VIEW_NOERR);
        logmsg(LOGMSG_ERROR,
                   "Creating a new time partition failed rc %d \"%s\"\n",
                   err.errval, err.errstr);
        sc_errf(sc, "Creating a new time partition failed rc %d \"%s\"",
                err.errval, err.errstr);
        rc = ERR_SC;
        goto out;
    }

    /* create shards for the partition */
    rc = timepart_populate_shards(sc->newpartition, &err);
    if (rc) {
        assert(err.errval != VIEW_NOERR);

        timepart_free_view(sc->newpartition);
        logmsg(LOGMSG_ERROR, "Failed to pre-populate the shards rc %d \"%s\"\n",
               err.errval, err.errstr);
        sc_errf(sc, "Failed to pre-populate the shards rc %d \"%s\"",
                err.errval, err.errstr);
        rc = ERR_SC;
        goto out;
    }

    timepart_sc_arg_t arg = {0};
    arg.s = sc;
    arg.s->iq = iq;

    /* is this an alter? preserve existing table as first shard */
    if (sc->kind != SC_ADDTABLE) {
        /* we need to create a light rename for first shard,
         * together with the original alter
         * NOTE: we need to grab the table version first
         */
        arg.s->timepartition_version =
            arg.s->db->tableversion + 1; /* next version */

        /* launch alter for original shard */
        rc = start_schema_change_tran_wrapper(sc->tablename, &arg);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "Failed to process alter for existing table %s while "
                   "partitioning rc %d\n",
                   sc->tablename, rc);
            sc_errf(sc,
                    "Failed to process alter for existing table %s while "
                    "partitioning rc %d",
                    sc->tablename, rc);
            return ERR_SC;
        }
        /* we need to  generate retention-1 table adds, with schema provided
         * by previous alter; we need to convert an alter to a add sc
         */
        arg.s->kind = SC_ADDTABLE;
        arg.indx = 1; /* first shard is already there */
    }
    /* should we serialize ? */
    arg.s->nothrevent = sc->partition.u.tpt.retention > gbl_dohsql_sc_max_threads;
    rc = timepart_foreach_shard_lockless(
            sc->newpartition, start_schema_change_tran_wrapper, &arg);

    if (!rc&& sc->partition.type == PARTITION_ADD_MANUAL) {
        if (!get_dbtable_by_name(LOGICAL_CRON_SYSTABLE)){
            struct schema_change_type *lcsc = _create_logical_cron_systable(LOGICAL_CRON_SYSTABLE);
            if (!lcsc)
                return -1;

            iq->sc = lcsc;
            iq->sc->iq = iq;

            rc = start_schema_change_tran(iq, NULL);
            iq->sc->sc_next = iq->sc_pending;
            iq->sc_pending = iq->sc;
        }
    }
out:
    return rc;
}

static struct schema_change_type* _create_logical_cron_systable(const char *tblname)
{
    struct schema_change_type *sc;

    sc = new_schemachange_type();
    if (!sc) {
        logmsg(LOGMSG_ERROR, "Failed to create a new schema change object\n");
        return NULL;
    }

    strncpy0(sc->tablename, tblname, sizeof(sc->tablename));
    sc->tablename_len = strlen(sc->tablename) + 1;
    sc->kind = SC_ADDTABLE;
    sc->nothrevent = 1;
    sc->live = 1;
    sc->is_osql = 1;


    sc->newcsc2 = strdup(
        "schema {\n"
        "        cstring name[128]\n"
        "        int value null = yes\n"
        "}\n"
        "keys\n"
        "{\n"
        "        uniqnulls \"COMDB2_PK\" = name\n"
        "}\n"
        );
    return sc;
}

static int _process_partition_alter_and_drop(struct ireq *iq)
{
    struct schema_change_type *sc = iq->sc;
    int rc;

    if (sc->kind == SC_ADDTABLE) {
        /* trying to create a duplicate time partition */
        logmsg(LOGMSG_ERROR, "Duplicate partition %s!\n", sc->tablename);
        sc_errf(sc, "Duplicate partition %s!", sc->tablename);
        rc = SC_TABLE_ALREADY_EXIST;
        goto out;
    }

    int nshards = timepart_get_num_shards(sc->tablename);
    if (nshards <= 0) {
        /*somehow the time partition got away from us */
        logmsg(LOGMSG_ERROR, "Failed to retrieve nshards in sc for %s\n",
               sc->tablename);
        sc_errf(sc, "Failed to retrieve nshards in sc for %s",
               sc->tablename);
        return ERR_SC;
    }

    /* should we serialize ? */
    sc->nothrevent = nshards > gbl_dohsql_sc_max_threads;

    if (sc->partition.type == PARTITION_MERGE) {
        return _process_partitioned_table_merge(iq);
    }

    timepart_sc_arg_t arg = {0};
    arg.s = sc;
    arg.s->iq = iq;
    rc = timepart_foreach_shard(sc->tablename,
                                start_schema_change_tran_wrapper, &arg, -1);
out:
    return rc;
}


static const uint8_t *_get_txn_info(char *msg, uuid_t uuid,  int *type)
{
    const uint8_t *p_buf;
    const uint8_t *p_buf_end;

    osql_uuid_rpl_t rpl;
    p_buf = (const uint8_t *)msg;
    p_buf_end = (uint8_t *)p_buf + sizeof(rpl);
    p_buf = osqlcomm_uuid_rpl_type_get(&rpl, p_buf, p_buf_end);
    *type = rpl.type;
    if (comdb2uuidcmp(rpl.uuid, uuid)) {
        uuidstr_t us;
        uuidstr_t passedus;
        comdb2uuidstr(rpl.uuid, us);
        comdb2uuidstr(uuid, passedus);
        logmsg(LOGMSG_FATAL, "uuid mismatch: passed in %s, in packet %s\n",
               passedus, us);
        abort();
    }

    return p_buf;
}


/**
 * Handles each packet and start schema change
 *
 */
int osql_process_schemachange(struct ireq *iq, uuid_t uuid, void *trans, char **pmsg, int msglen,
                              int *flags, int **updCols, blob_buffer_t blobs[MAXBLOBS], int step,
                              struct block_err *err, int *receivedrows)
{
    const uint8_t *p_buf;
    const uint8_t *p_buf_end;
    int rc = 0;
    int type;
    char *msg = *pmsg;

    p_buf = _get_txn_info(msg, uuid, &type);

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
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%s] OSQL_SCHEMACHANGE %s\n", comdb2uuidstr(uuid, us), sc->tablename);
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

    int is_partition = timepart_is_partition(sc->tablename);

    if (!is_partition) {
        if (sc->partition.type == PARTITION_NONE) {
            rc = _process_single_table_sc(iq);
        } else if (sc->partition.type == PARTITION_MERGE) {
            rc = _process_single_table_sc_merge(iq);
        } else {
            rc = _process_single_table_sc_partitioning(iq);
        }
    } else {
        rc = _process_partition_alter_and_drop(iq);
    }

    iq->usedb = NULL;

    /* SC_ASYNC if nothrevent == 0
     * SC_COMMIT_PENDING if nothrevent == 1 && finalize == 0
     * SC_OK for everything else except errors
     */
    if (rc == SC_OK || rc == SC_ASYNC || rc == SC_COMMIT_PENDING)
        return 0;

    return ERR_SC;
}

/* get the table name part of the rpl request
 */
const char *get_tablename_from_rpl(const uint8_t *rpl, int *tableversion)
{
    osql_usedb_t dt;
    const uint8_t *p_buf = rpl + sizeof(osql_uuid_rpl_t);
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


/**
 * Handles each packet and calls record.c functions
 * to apply to received row updates
 *
 */
int osql_process_packet(struct ireq *iq, uuid_t uuid,
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
    char *msg = *pmsg;

    p_buf = _get_txn_info(msg, uuid, &type);

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
    case OSQL_DONE_SNAP: {
        p_buf_end = p_buf + sizeof(osql_done_t);
        osql_done_t dt = {0};

        p_buf = osqlcomm_done_type_get(&dt, p_buf, p_buf_end);

        if (gbl_enable_osql_logging) {
            uuidstr_t us;
            logmsg(LOGMSG_DEBUG, "[%s] OSQL_DONE%s %d %d\n",
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
                bdb_tran_assert_nolocks(thedb->bdb_env, trans);
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
            if (IS_FASTINIT(iq->sc) && gbl_replicate_local)
                local_replicant_write_clear(iq, trans, iq->sc->db);
            iq->sc = iq->sc->sc_next;
        }

        /* Success, need to publish results in memory */
        iq->sc = iq->sc_pending;
        int error = 0;
        while (iq->sc != NULL) {
            if (iq->sc->publish) {
                error = iq->sc->publish(trans, iq->sc);
                if (error)
                    break;
            }
            iq->sc = iq->sc->sc_next;
        }
        if (error) {
           struct schema_change_type *sc = iq->sc_pending;
            while (sc != iq->sc) {
                if (iq->sc->unpublish)  {
                    sc->unpublish(sc);
                }
                sc = sc->sc_next;
            }
        }

        /* Success: reset the table counters */
        iq->sc = iq->sc_pending;
        while (iq->sc != NULL) {
            sc_set_running(iq, iq->sc, iq->sc->tablename, 0, NULL, 0,
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
            if (!gbl_disable_cnonce_blkseq && !gbl_master_sends_query_effects)
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
        }

#if 0
        Currently this flag is not set and we do not read the bytes from the buffer;
        until we review and decide to either remove or fix the clients_query_stats
        (right now we send the wrong one), leave this in place as a reminder 
        /* p_buf is pointing at client_query_stats if there is one */
        if (type == OSQL_DONE_STATS) { 
            dump_client_query_stats_packed(iq->dbglog_file, p_buf);
        }
#endif

        if (gbl_toblock_random_deadlock_trans && (rand() % 100) == 0) {
            rc = RC_INTERNAL_RETRY;
        }

        return rc ? rc : OSQL_RC_DONE; /* signal caller done processing this
                                          request */
    }
    case OSQL_USEDB: {
        osql_usedb_t dt = {0};
        p_buf_end = (uint8_t *)p_buf + sizeof(osql_usedb_t);

        /* IDEA: don't store the usedb in the defered_table, rather right before
         * loading a new usedb, process the curret one,
         * this way tmptbl key is 8 bytes smaller
         *
        if (gbl_reorder_on) {
            process_defered_table(iq, ...);
        }
        */

        const char *tablename = (const char *)osqlcomm_usedb_type_get(&dt, p_buf, p_buf_end);
        if (iq->usedb && strcmp(iq->usedb->tablename, tablename) == 0) {
            assert(bdb_has_trans_tablename_locked(thedb->bdb_env, tablename, trans, TABLENAME_LOCKED_READ));
            return 0; /* already have tbl lock from before */
        }

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
        assert(bdb_has_trans_tablename_locked(thedb->bdb_env, tablename, trans, TABLENAME_LOCKED_READ));

        if (gbl_enable_osql_logging) {
            uuidstr_t us = {0};
            logmsg(LOGMSG_DEBUG, "[%s] OSQL_USEDB %*.s\n",
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
        osql_del_t dt = {0};
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
            logmsg(LOGMSG_DEBUG, "[%s] %s %llx (2:%lld)\n",
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
        osql_ins_t dt = {0};
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
            logmsg(LOGMSG_DEBUG, "[%s] %s [\n", comdb2uuidstr(uuid, us),
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
            iq->sorese->is_delayed = 1;
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
                if (iq->vfy_idx_track == 1 && iq->dup_key_insert == 1) {
                    rc = ERR_UNCOMMITABLE_TXN;
                    reqerrstr(iq, COMDB2_CSTRT_RC_DUP, "add key constraint "
                                                   "duplicate key '%s' on "
                                                   "table '%s' index %d",
                          get_keynm_from_db_idx(iq->usedb, err->ixnum),
                          iq->usedb->tablename, err->ixnum);
                    err->errcode = ERR_UNCOMMITABLE_TXN;
                    return rc;
                }

                int upsert_idx = dt.upsert_flags >> 8;
                if ((dt.upsert_flags & OSQL_FORCE_VERIFY) != 0) {
                    if (upsert_idx == err->ixnum) {
                        err->errcode = OP_FAILED_VERIFY;
                        rc = ERR_VERIFY;
                    }
                }

                if ((dt.upsert_flags & OSQL_IGNORE_FAILURE) != 0) {
                    if (upsert_idx == MAXINDEX + 1) {
                        /* We're asked to ignore DUPs for all unique indices, no insert took place.*/
                        err->errcode = 0;
                        return 0;
                    } else if ((dt.upsert_flags & OSQL_FORCE_VERIFY) == 1) {
                        err->errcode = 0;
                        return 0;
                    } else if (upsert_idx == err->ixnum) {
                        /* We're asked to ignore DUPs for this particular * index, no insert took place.*/
                        err->errcode = 0;
                        return 0;
                    }
                }

                if (rc != ERR_VERIFY) {
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
            logmsg(LOGMSG_DEBUG, "[%s] Startgen check failed, start_gen %u, cur_gen %u\n",
                   comdb2uuidstr(uuid, us), dt.start_gen, cur_gen);
            return ERR_VERIFY;
        }
    } break;

    case OSQL_UPDREC:
    case OSQL_UPDATE: {
        osql_upd_t dt = {0};
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

        if (gbl_debug_invalid_genid == 1) {
            genid++;
        }

        if (gbl_enable_osql_logging) {
            int jj = 0;
            uuidstr_t us;
            logmsg(LOGMSG_DEBUG, "[%s] OSQL_UPDREC rrn = %d, genid = %llx[\n",
                   comdb2uuidstr(uuid, us), rrn, bdb_genid_to_host_order(genid));
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
                    int idx = get_schema_blob_field_idx(iq->usedb, ".ONDISK", ii);
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
            logmsg(LOGMSG_DEBUG, "[%s] OSQL_UPDCOLS %d[\n", comdb2uuidstr(uuid, us), dt.ncols);
            for (jj = 0; jj < dt.ncols; jj++)
                logmsg(LOGMSG_DEBUG, "%d ", dt.clist[jj]);
            logmsg(LOGMSG_DEBUG, "]\n");
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
        p_buf = (uint8_t *)&((osql_serial_uuid_rpl_t *)msg)->dt;
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
            logmsg(LOGMSG_DEBUG, "[%s] %s %d %d_%d_%d\n", comdb2uuidstr(uuid, us),
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
            logmsg(LOGMSG_DEBUG, "[%s] %s ixnum %d [\n",
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
            logmsg(LOGMSG_DEBUG, "[%s] OSQL_QBLOB %d %d [\n",
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
                   "[%s] OSQL_RECGENID %llx (%llu) -> rc = %d\n",
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
                    logmsg(LOGMSG_DEBUG, "[%s] OSQL_BPFUNC type %d\n",
                           comdb2uuidstr(uuid, us), func->arg->type);
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
        logmsg(LOGMSG_ERROR, "%s [%s] RECEIVED AN UNKNOWN OFF OPCODE %u, failing the transaction\n",
               __func__, comdb2uuidstr(uuid, us), type);
        return conv_rc_sql2blkop(iq, step, -1, ERR_BADREQ, err, NULL, 0);
    }
    }

    return 0;
}

void signal_replicant_error(osql_target_t *target, uuid_t uuid, int rc, const char *msg)
{
    struct errstat generr = {0};
    errstat_set_rcstrf(&generr, rc, msg);
    int rc2 = osql_comm_signal_sqlthr_rc(target, uuid, 0, &generr, 0, rc);
    if (rc2) {
        uuidstr_t us;
        comdb2uuidstr(uuid, us);
        logmsg(LOGMSG_ERROR,
               "%s: failed to signaled rqid=[%s] host=%s of error to create bplog\n",
               __func__, us, target->host);
    }
}

static int sorese_rcvreq(char *fromhost, void *dtap, int dtalen, OSQL_REQ_TYPE type)
{
    osql_sess_t *sess = NULL;
    uuid_t uuid = {0};
    int sqllen;
    char *tzname = NULL;
    int flags = 0;
    int send_rc = 1;
    const char *errmsg = "";
    int rc = 0;
    int added_to_repository = 0;
    uint8_t *p_req_buf = dtap;
    const uint8_t *p_req_buf_end = p_req_buf + dtalen;
    osql_uuid_req_t ureq;
    char *sql = (char *)osqlcomm_req_uuid_type_get(&ureq, p_req_buf, p_req_buf_end);
    if (!sql) {
        logmsg(LOGMSG_ERROR, "%s failed osqlcomm_req_uuid_type_get\n", __func__);
        errmsg = "unable to create new session";
        rc = -1;
        goto done;
    }
    comdb2uuidcpy(uuid, ureq.uuid);
    flags = ureq.flags;
    tzname = strdup(ureq.tzname);
    sqllen = ureq.sqlqlen;

    /* create the request */
    sess = osql_sess_create(sql, sqllen, tzname, type, uuid, fromhost, flags & OSQL_FLAGS_REORDER_ON);
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
    added_to_repository = 1;

#if DEBUG_REORDER
    logmsg(LOGMSG_DEBUG,
           "REORDER: created sess %p, with sess->is_reorder_on %d\n", sess,
           sess->is_reorder_on);
#endif

    /* for socksql, is it a retry that needs to be checked for self-deadlock? */
    if ((type == OSQL_SOCK_REQ) && (flags & OSQL_FLAGS_CHECK_SELFLOCK)) {
        /* just make sure we are above the threshold */
        sess->verify_retries += gbl_osql_verify_ext_chk;
    }

done:
    if (tzname)
        free(tzname);

    if (added_to_repository) {
        /*
         * Add to repository was successful, let the session loose
         * It is possible that we are clearing sessions due to
         * master being rtcpu-ed, and it will wait for the session
         * clients to disappear before it will wipe out the session
         */

        rc = osql_repository_put(sess);
        if (!rc)
            return 0;
        /* if put noticed a termination flag, fall-through */
        send_rc = 1;
    } else if (sess) {
        /* Cleanup: in osql_sess_create() session is initialized with 1 client--this reader thread
         * Here we need call osql_sess_remclient() which does not get lock
         * (instead of osql_repository_put)
         */
        osql_sess_remclient(sess);
    }

    /* notify the sql thread there will be no response! */
    if (send_rc) {
        osql_target_t target = {0};
        init_bplog_net(&target);
        target.host = fromhost;
        signal_replicant_error(&target, uuid, ERR_NOMASTER, errmsg);
    }

    if (sess)
        osql_sess_close(&sess, added_to_repository);

    return rc;
}

/* transaction result */
static void net_sorese_signal(void *hndl, void *uptr, char *fromhost,
                              struct interned_string *frominterned,
                              int usertype, void *dtap, int dtalen,
                              uint8_t is_tcp)
{
    osql_done_t done = {0};
    struct errstat *xerr;
    struct query_effects effects;
    struct query_effects *p_effects = NULL;
    uint8_t *p_buf = (uint8_t *)dtap;
    uint8_t *p_buf_end = p_buf + dtalen;

    /* unpack */
    osql_uuid_rpl_t uuid_hdr = {0};
    p_buf = (uint8_t *)osqlcomm_uuid_rpl_type_get(&uuid_hdr, p_buf, p_buf_end);
    osqlcomm_done_type_get(&done, p_buf, p_buf_end);

    /* This also receives the query effects from master. */
    if (osql_comm_is_done(NULL, uuid_hdr.type, dtap, dtalen, &xerr, &effects) == 1) {
        if (uuid_hdr.type == OSQL_DONE_WITH_EFFECTS) {
            p_effects = &effects;
        }
        if (xerr) {
            struct errstat errstat;
            uint8_t *p_buf = (uint8_t *)xerr;
            uint8_t *p_buf_end = (p_buf + sizeof(struct errstat));
            osqlcomm_errstat_type_get(&errstat, p_buf, p_buf_end);
            osql_chkboard_sqlsession_rc(uuid_hdr.uuid, 0, NULL, &errstat, NULL, fromhost);
        } else {
            osql_chkboard_sqlsession_rc(uuid_hdr.uuid, done.nops, NULL, NULL, p_effects, fromhost);
        }
    } else {
        logmsg(LOGMSG_ERROR, "%s: wrong sqlthr signal %d\n", __func__, uuid_hdr.type);
        return;
    }
}

/**
 * Send RECGENID
 * It handles remote/local connectivity
 *
 */
int osql_send_recordgenid(osql_target_t *target, uuid_t uuid, unsigned long long genid)
{
    if (check_master(target))
        return OSQL_SEND_ERROR_WRONGMASTER;

    osql_recgenid_uuid_rpl_t recgenid_rpl = {{0}};
    uint8_t buf[OSQLCOMM_RECGENID_UUID_RPL_TYPE_LEN];
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end = p_buf + OSQLCOMM_RECGENID_UUID_RPL_TYPE_LEN;

    recgenid_rpl.hd.type = OSQL_RECGENID;
    comdb2uuidcpy(recgenid_rpl.hd.uuid, uuid);
    recgenid_rpl.dt.genid = genid;

    if (!(p_buf = osqlcomm_recgenid_uuid_rpl_type_put(&recgenid_rpl, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__, "osqlcomm_recgenid_rpl_type_put");
        return -1;
    }

    if (gbl_enable_osql_logging) {
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%s] send OSQL_RECGENID %llx (%lld)\n",
               comdb2uuidstr(uuid, us), genid, genid);
    }

    return target->send(target, NET_OSQL_SOCK_RPL_UUID, buf, sizeof(recgenid_rpl), 0, NULL, 0);
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

/**
 * Send SCHEMACHANGE op
 * It handles remote/local connectivity
 *
 */
int osql_send_schemachange(osql_target_t *target, uuid_t uuid, struct schema_change_type *sc)
{
    schemachange_packed_size(sc);
    size_t osql_rpl_size = OSQLCOMM_UUID_RPL_TYPE_LEN + sc->packed_len;
    uint8_t *buf = alloca(osql_rpl_size);
    uint8_t *p_buf = buf;
    uint8_t *p_buf_end = p_buf + osql_rpl_size;

    if (check_master(target))
        return OSQL_SEND_ERROR_WRONGMASTER;

    strcpy(sc->original_master_node, target->host);

    osql_uuid_rpl_t hd_uuid = {0};

    hd_uuid.type = OSQL_SCHEMACHANGE;
    comdb2uuidcpy(hd_uuid.uuid, uuid);
    if (!(p_buf = osqlcomm_schemachange_uuid_rpl_type_put( &hd_uuid, sc, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__, "osqlcomm_schemachange_uuid_rpl_type_put");
        return -1;
    }

    if (gbl_enable_osql_logging) {
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%s] send OSQL_SCHEMACHANGE %s\n", comdb2uuidstr(uuid, us), sc->tablename);
    }

    return target->send(target, NET_OSQL_SOCK_RPL_UUID, buf, osql_rpl_size, 0, NULL, 0);
}

int osql_send_bpfunc(osql_target_t *target, uuid_t uuid, BpfuncArg *arg)
{
    osql_bpfunc_t *dt;
    size_t data_len = bpfunc_arg__get_packed_size(arg);
    size_t osql_bpfunc_size;
    size_t osql_rpl_size;
    uint8_t *p_buf = NULL;
    uint8_t *p_buf_end;
    int rc = 0;

    osql_bpfunc_size = OSQLCOMM_BPFUNC_TYPE_LEN + data_len;
    dt = malloc(osql_bpfunc_size);
    if (!dt) {
        rc = -1;
        goto freemem;
    }

    osql_rpl_size = OSQLCOMM_UUID_RPL_TYPE_LEN + osql_bpfunc_size;
    p_buf = malloc(osql_rpl_size);
    if (!p_buf) {
        rc = -1;
        goto freemem;
    }

    p_buf_end = p_buf + osql_rpl_size;

    if (check_master(target)) {
        rc = OSQL_SEND_ERROR_WRONGMASTER;
        goto freemem;
    }

    dt->data_len = data_len;
    bpfunc_arg__pack(arg, dt->data);

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

    if (gbl_enable_osql_logging) {
        uuidstr_t us;
        logmsg(LOGMSG_DEBUG, "[%s] send OSQL_BPFUNC type %d\n", comdb2uuidstr(uuid, us), arg->type);
    }

    rc = target->send(target, NET_OSQL_SOCK_RPL_UUID, p_buf, osql_rpl_size, 0, NULL, 0);

freemem:
    if (dt)
        free(dt);
    if (p_buf)
        free(p_buf);

    return rc;
}

static void osql_extract_snap_info(osql_sess_t *sess, void *rpl, int rpllen)
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
        (uint8_t *)rpl + sizeof(osql_done_t) + sizeof(osql_uuid_rpl_t);

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

#define UNK_ERR_SEND_RETRY 10

int offload_net_send(const char *host, int usertype, void *data, int datalen,
                     int nodelay, void *tail, int tailen)
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

    if (host == gbl_myhostname) {
        rc = net_local_route_packet_tail(usertype, data, datalen, tail, tailen);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s failed to route locally!\n", __func__);
        }
        return rc;
    }

    while (rc) {
        rc = net_send_tail(netinfo_ptr, host, usertype, data, datalen, nodelay, tail, tailen);
        if (NET_SEND_FAIL_QUEUE_FULL == rc) {

            if (total_wait > gbl_osql_bkoff_netsend_lmt) {
                logmsg(
                    LOGMSG_ERROR,
                    "%s:%d giving up sending to %s, rc = %d, total wait = %d\n",
                    __FILE__, __LINE__, host, rc, total_wait);
                return rc;
            }

            if (osql_comm_check_bdb_lock(__func__, __LINE__) != 0) {
                logmsg(LOGMSG_ERROR,
                       "%s:%d failed to check bdb lock, giving up sending to "
                       "%s, rc = %d\n",
                       __FILE__, __LINE__, host, rc);
                return rc;
            }

            poll(NULL, 0, backoff);
            /*backoff *= 2; */
            total_wait += backoff;
        } else if (NET_SEND_FAIL_NOSOCK == rc) {
            /* on closed sockets, we simply return; a callback
               will trigger on the other side signalling we've
               lost the comm party */
            logmsg(LOGMSG_ERROR,
                   "%s:%d socket is closed, return wrong master\n", __FILE__,
                   __LINE__);
            return OSQL_SEND_ERROR_WRONGMASTER;
        } else if (rc) {
            unknownerror_retry++;
            if (unknownerror_retry >= UNK_ERR_SEND_RETRY) {
                logmsg(LOGMSG_ERROR, "%s:%d giving up sending to %s\n",
                       __FILE__, __LINE__, host);
                comdb2_linux_cheap_stack_trace();
                return -1;
            }
        }
    }

    return rc;
}

/**
 * Read a commit (DONE/XERR) from a socket, used in bplog over socket
 * Timeoutms limits total amount of waiting for a commit
 *
 */
int osql_recv_commit_rc(SBUF2 *sb, int timeoutms, int timeoutdeltams, int *nops,
                        struct errstat *err)
{
    char hdr_buf[OSQLCOMM_UUID_RPL_TYPE_LEN];
    osql_uuid_rpl_t hdr;
    int left_timeoutms = timeoutms;
    const uint8_t *p_buf;
    const uint8_t *p_buf_end;
    const char *part = "";
    int rc = 0;
    int length;

    rc = osql_read_buffer((char *)&length, sizeof(length), sb, &left_timeoutms,
                          timeoutdeltams);
    if (rc || !left_timeoutms) {
        part = "packet length";
        goto error;
    }

    /* get the header */
    rc = osql_read_buffer(hdr_buf, sizeof(hdr_buf), sb, &left_timeoutms,
                          timeoutdeltams);
    if (rc || !left_timeoutms) {
        part = "header";
        goto error;
    }
    if (strncmp(hdr_buf, "Error:", 6) == 0) {
        part = "protocol";
        goto error;
    }

    p_buf = (uint8_t *)hdr_buf;
    p_buf_end = p_buf + sizeof(hdr_buf);
    p_buf = osqlcomm_uuid_rpl_type_get(&hdr, p_buf, p_buf_end);

    switch (hdr.type) {
    case OSQL_DONE: {
        char done_buf[OSQLCOMM_DONE_TYPE_LEN];
        osql_done_t done;

        rc = osql_read_buffer(done_buf, sizeof(done_buf), sb, &left_timeoutms,
                              timeoutdeltams);
        if (rc || !left_timeoutms) {
            part = "done";
            goto error;
        }

        p_buf = (uint8_t *)done_buf;
        p_buf_end = p_buf + sizeof(done_buf);

        p_buf = osqlcomm_done_type_get(&done, p_buf, p_buf_end);
        rc = done.rc;
        *nops = done.nops;
        bzero(err, sizeof(*err));

        break;
    }
    case OSQL_DONE_WITH_EFFECTS: {
        break;
    }
    /* TODO: OSQL_DONE_STATS, OSQL_DONE_SNAP */
    case OSQL_XERR: {
        char xerr_buf[ERRSTAT_LEN];

        rc = osql_read_buffer(xerr_buf, sizeof(xerr_buf), sb, &left_timeoutms,
                              timeoutdeltams);
        if (rc || !left_timeoutms) {
            part = "xerr";
            goto error;
        }

        p_buf = (uint8_t *)xerr_buf;
        p_buf_end = p_buf + sizeof(xerr_buf);

        p_buf = osqlcomm_errstat_type_get(err, p_buf, p_buf_end);
        *nops = 0;
        rc = errstat_get_rc(err);

        break;
    }
    default:
        logmsg(LOGMSG_ERROR, "%s Unhandled return code %d\n", __func__,
               hdr.type);
        abort();
    }

    return rc;

error:
    logmsg(LOGMSG_ERROR, "%s %s reading rc from master, %s\n", __func__,
           (left_timeoutms) ? "failed" : "timeout", part);
    return ERR_NOMASTER;
}

/* check if we need to get tpt lock */
int need_views_lock(char *msg, int msglen)
{
    const uint8_t *p_buf, *p_buf_end;
    {
        osql_uuid_rpl_t rpl;
        p_buf = (const uint8_t *)msg;
        p_buf_end = (uint8_t *)p_buf + sizeof(rpl);
        p_buf = osqlcomm_uuid_rpl_type_get(&rpl, p_buf, p_buf_end);
    }

    osql_bpfunc_t *rpl = NULL;
    p_buf_end = (const uint8_t *)p_buf + sizeof(osql_bpfunc_t) + msglen;
    const uint8_t *n_p_buf = osqlcomm_bpfunc_type_get(&rpl, p_buf, p_buf_end);

    if (!n_p_buf || !rpl)
        return -1;

    return bpfunc_check(rpl->data, rpl->data_len, BPFUNC_TIMEPART_RETENTION);
}
