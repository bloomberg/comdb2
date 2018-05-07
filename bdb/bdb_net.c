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

#include <stdio.h>
#include <string.h>
#include <alloca.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <sys/time.h>
#include <errno.h>
#include <ctype.h>

#include "bdb_api.h"
#include "bdb_int.h"
#include <net.h>
#include <locks.h>

#include <util.h>
#include <gettimeofday_ms.h>

#include "nodemap.h"
#include "endian_core.h"
#include "printformats.h"
#include "crc32c.h"

#undef UDP_DEBUG
#undef UDP_TRACE

#define TOSTR_(x) #x
#define TOSTR(x) TOSTR_(x)

#define trace(x, args...)                                                      \
    printf("%-15s%-20s[" x "]\n", __FILE__ ":" TOSTR(__LINE__), __func__,      \
           ##args)

#ifdef UDP_TRACE
#define debug_trace(x, args...)                                                \
    printf("%-15s%-20s[" x "]\n", __FILE__ ":" TOSTR(__LINE__), __func__,      \
           ##args)
#else
#define debug_trace(...)
#endif

extern void fsnapf(FILE *, void *, int);
extern int db_is_stopped(void);

struct ack_info_t {
    uint32_t hdrsz;
    int32_t type;
    uint32_t len; /* payload len */
    int32_t from;
    int32_t to;
    int32_t fromlen;

    /*
    int32_t  dummy0;
    uint64_t timestamp;
    uint64_t seq;
    */

    /* To prevent double copying of payload, allocate enough
    ** memory beyond every ack_info to hold the payload.
    */
};
BB_COMPILE_TIME_ASSERT(ack_info, sizeof(ack_info) == (6 * sizeof(int32_t)));
static int udp_send(bdb_state_type *, ack_info *, const char *host);

/*
** new_ack_info() allocates storage on the stack as its intended to be sent
** inline. no need to free this storage.
*/
#define ack_info_from_host(info)                                               \
    (char *)((uint8_t *)(info) + ((info)->hdrsz - (info)->fromlen))

#define new_ack_info(ptr, payloadsz, fromhost)                                 \
    do {                                                                       \
        int __len = strlen(fromhost) + 1;                                      \
        (ptr) = alloca(sizeof(ack_info) + payloadsz + __len);                  \
        (ptr)->hdrsz = sizeof(ack_info) + __len;                               \
        (ptr)->len = payloadsz;                                                \
        (ptr)->fromlen = __len;                                                \
        (ptr)->from = (ptr)->to = 0;                                           \
        strcpy(ack_info_from_host(ptr), fromhost);                             \
    } while (0)

#define ack_info_data(info) (void *)((uint8_t *)(info) + (info)->hdrsz)

#define ack_info_size(info) ((info)->hdrsz + (info)->len)

/* Convert ack_info header into big endian */
#define ack_info_from_cpu(info)                                                \
    do {                                                                       \
        (info)->hdrsz = htonl((info)->hdrsz);                                  \
        (info)->type = htonl((info)->type);                                    \
        (info)->len = htonl((info)->len);                                      \
        (info)->from = htonl((info)->from);                                    \
        (info)->to = htonl((info)->to);                                        \
        (info)->fromlen = htonl((info)->fromlen);                              \
    } while (0)

#define ack_info_to_cpu(info)                                                  \
    do {                                                                       \
        (info)->hdrsz = ntohl((info)->hdrsz);                                  \
        (info)->type = ntohl((info)->type);                                    \
        (info)->len = ntohl((info)->len);                                      \
        (info)->from = ntohl((info)->from);                                    \
        (info)->to = ntohl((info)->to);                                        \
        (info)->fromlen = ntohl((info)->fromlen);                              \
    } while (0)

static unsigned int sent_udp = 0;
static unsigned int fail_udp = 0;

static unsigned int recd_udp = 0;
static unsigned int recl_udp = 0; /* problem with recv'd len */
static unsigned int rect_udp = 0; /* problem with recv'd to */

int gbl_ack_trace = 0;

void enable_ack_trace(void)
{
    gbl_ack_trace = 1;
}

void disable_ack_trace(void)
{
    gbl_ack_trace = 0;
}

int do_ack(bdb_state_type *bdb_state, DB_LSN permlsn, uint32_t generation)
{
    int rc;
    char *master;
    ack_info *info;
    uint8_t *p_buf;
    uint8_t *p_buf_end;
    static time_t lastpr = 0;
    time_t now = 0;
    static unsigned long long cnt = 0;
    static unsigned long long lpcnt = 0;

    cnt++;
    if (gbl_ack_trace && (now = time(NULL)) > lastpr) {
        fprintf(stderr,
                "Sending ack %d:%d, generation=%u cnt=%llu diff=%llu, udp=%d\n",
                permlsn.file, permlsn.offset, generation, cnt, cnt - lpcnt,
                gbl_udp);
        lpcnt = cnt;
        lastpr = now;
    }

    seqnum_type seqnum = {{0}, 0};
    seqnum.lsn = permlsn;
    seqnum.commit_generation = generation;
    bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &seqnum.generation);
    /* Master lease time is 0 (master will ignore) */

    if (permlsn.file == 0 || seqnum.lsn.file == 0)
        abort();

    new_ack_info(info, BDB_SEQNUM_TYPE_LEN, bdb_state->repinfo->myhost);

    p_buf = ack_info_data(info);
    p_buf_end = p_buf + BDB_SEQNUM_TYPE_LEN;
    rep_berkdb_seqnum_type_put(&seqnum, p_buf, p_buf_end);
    master = bdb_state->repinfo->master_host;

    if (unlikely(bdb_state->rep_trace)) {
        char str[80];
        lsn_to_str(str, &seqnum.lsn);
        fprintf(stderr, "sending NEWSEQ to %s <%s>\n", master, str);
    }

    if (gbl_udp) {
        info->from = 0;
        info->to = 0;
        info->type = USER_TYPE_BERKDB_NEWSEQ;
        udp_send(bdb_state, info, master);
        debug_trace("%d -> %d", ntohl(info->from), ntohl(info->to));
        rc = 0;
    } else {
        rc = net_send(bdb_state->repinfo->netinfo, master,
                      USER_TYPE_BERKDB_NEWSEQ, p_buf, sizeof(seqnum), 1);
    }
    return rc;
}

void comdb2_early_ack(DB_ENV *dbenv, DB_LSN permlsn, uint32_t generation)
{
    bdb_state_type *bdb_state = (bdb_state_type *)dbenv->app_private;
    do_ack(bdb_state, permlsn, generation);
}

char *print_addr(struct sockaddr_in *addr, char *buf)
{
    buf[0] = '\0';
    if (addr == NULL) {
        return buf;
    }
    if(addr->sin_addr.s_addr == htonl(INADDR_ANY)) {
        sprintf(buf, "[0.0.0.0 0.0.0.0:%d ]", ntohs(addr->sin_port));
        return buf;
    }
    char ip[32] = {0};
    char name[256] = {0};
    char service[256] = {0};
    char errbuf[256] = {0};
    socklen_t len;

    len = sizeof(*addr);
    int rc = getnameinfo((struct sockaddr *)addr, len, name, sizeof(name),
                         service, sizeof(service), 0);
    if (rc) {
        strerror_r(rc, errbuf, sizeof(errbuf));
        sprintf(buf, "%s:getnameinfo errbuf=%s", __func__, errbuf);
        return buf;
    }

    if (inet_ntop(addr->sin_family, &addr->sin_addr.s_addr, ip, sizeof(ip))) {
        sprintf(buf, "[%s %s:%s] ", name, ip, service);
    } else {
        strerror_r(errno, errbuf, sizeof(errbuf));
        sprintf(buf, "%s:inet_ntop:%s", __func__, errbuf);
    }
    return buf;
}

static int udp_send(bdb_state_type *bdb_state, ack_info *info, const char *to)
{
    repinfo_type *repinfo = bdb_state->repinfo;
    netinfo_type *netinfo = repinfo->netinfo;
    size_t len = ack_info_size(info);

    ack_info_from_cpu(info);
    ssize_t nsent = net_udp_send(repinfo->udp_fd, netinfo, to, len, info);

    if (nsent < 0) {
        if (nsent != -999) {
            logmsgperror("udp_send:sendto");
            ack_info_to_cpu(info);
            printf("sz:%zu, hdr:%d payload:%d type:%d from:me to:%s\n", len,
                   info->hdrsz, info->len, info->type, to);
        }
        ++fail_udp;
    } else {
        ++sent_udp;
    }

    return nsent;
}

static int send_timestamp(bdb_state_type *bdb_state, const char *to, int type)
{
    size_t size;
    ack_info *info;
    new_ack_info(info, sizeof(struct timeval), bdb_state->repinfo->myhost);
    info->from = 0;
    info->to = 0;
    info->type = type;
    gettimeofday(ack_info_data(info), NULL);
    switch (type) {
    case USER_TYPE_UDP_TIMESTAMP:
        return udp_send(bdb_state, info, to);
    case USER_TYPE_PING_TIMESTAMP:
    case USER_TYPE_TCP_TIMESTAMP:
        size = ack_info_size(info);
        ack_info_from_cpu(info);
        return net_send(bdb_state->repinfo->netinfo, to, type, info, size, 1);
    default:
        fprintf(stderr, "unknown timestamp type: %d\n", type);
        return 1;
    }
}

static int udp_send_header(bdb_state_type *bdb_state, char *to, int type)
{
    ack_info *info;
    new_ack_info(info, 0, bdb_state->repinfo->myhost);
    info->from = 0;
    info->to = 0;
    info->type = type;
    return udp_send(bdb_state, info, to);
}

void udp_ping(bdb_state_type *bdb_state, char *to)
{
    /* udp_send_header(bdb_state, to, USER_TYPE_UDP_PING); */
    if (send_timestamp(bdb_state, to, USER_TYPE_UDP_TIMESTAMP) > 0) {
        debug_trace("sent ping %s -> %s", bdb_state->repinfo->mynode, to);
    }
}

static void ping_all_int(bdb_state_type *bdb_state, int type)
{
    repinfo_type *repinfo = bdb_state->repinfo;
    const char *nodes[REPMAX];
    int i = net_get_all_nodes(repinfo->netinfo, nodes);

    while (i--) {
        send_timestamp(bdb_state, nodes[i], type);
    }
}

void udp_ping_all(bdb_state_type *bdb_state)
{
    ping_all_int(bdb_state, USER_TYPE_UDP_TIMESTAMP);
}

void udp_ping_ip(bdb_state_type *bdb_state, char *ip)
{
    char straddr[256];
    char *strport = strstr(ip, ":");
    *strport = '\0';
    ++strport;
    int port = strtol(strport, NULL, 10);

    struct sockaddr_in addr;
    struct sockaddr *paddr = (struct sockaddr *)&addr;
    int rc = inet_pton(AF_INET, ip, &addr.sin_addr);
    if (rc < 0) {
        logmsgperror("upd_ping_ip:inet_pton");
        return;
    } else if (rc == 0) {
        fprintf(stderr, "%s not a valid address\n", ip);
        return;
    }
    addr.sin_port = htons(port);
    addr.sin_family = AF_INET;

    repinfo_type *repinfo = bdb_state->repinfo;
    ack_info *info;
    new_ack_info(info, sizeof(struct timeval), bdb_state->repinfo->myhost);

    size_t len = ack_info_size(info);
    void *payload = ack_info_data(info);

    info->from = 0;
    info->to = 0;
    info->type = USER_TYPE_UDP_TIMESTAMP;
    gettimeofday(payload, NULL);
    ack_info_from_cpu(info);

    size_t nsent = sendto(repinfo->udp_fd, info, len, 0, paddr, sizeof(addr));
    if (nsent != len) {
        logmsgperror("udp_ping_ip:sendto");
        ack_info_to_cpu(info);
        printf("total len:%zu, hdr:%d type:%d len:%d from:%d to:%d %s\n", len,
               info->hdrsz, info->type, info->len, info->from, info->to,
               print_addr(&addr, straddr));
        return;
    }
    debug_trace("sent ping to %s", print_addr(&addr, straddr));
}

static void udp_bind(repinfo_type *repinfo)
{
    struct sockaddr_in *addr;
    socklen_t socklen = sizeof(*addr);

    repinfo->udp_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (repinfo->udp_fd < 0) {
        logmsgperror("udp_bind:socket");
        exit(1);
    }

    repinfo->udp_addr = addr = calloc(1, socklen);
    addr->sin_addr.s_addr = htonl(INADDR_ANY);
    addr->sin_port = htons(get_host_port(repinfo->netinfo));
    addr->sin_family = AF_INET;

    if (bind(repinfo->udp_fd, (struct sockaddr *)addr, socklen)) {
        logmsgperror("udp_bind:bind");
        exit(1);
    }
}
struct thdpool *gbl_udppfault_thdpool = NULL;

int udppfault_thdpool_init(void)
{
    gbl_udppfault_thdpool = thdpool_create("udppfaultpool", 0);

    thdpool_set_exit(gbl_udppfault_thdpool);

    thdpool_set_minthds(gbl_udppfault_thdpool, 0);
    thdpool_set_maxthds(gbl_udppfault_thdpool, 8);
    thdpool_set_maxqueue(gbl_udppfault_thdpool, 1000);
    thdpool_set_linger(gbl_udppfault_thdpool, 10);
    thdpool_set_longwaitms(gbl_udppfault_thdpool, 10000);

    return 0;
}

static int send_prefault(bdb_state_type *bdb_state, const char *tohost,
                         unsigned int fileid, unsigned int pgno)
{
    ack_info *info;
    uint8_t *p_buf;
    uint8_t *p_buf_end;

    filepage_type filepage;
    filepage.fileid = fileid;
    filepage.pgno = pgno;

    new_ack_info(info, BDB_FILEPAGE_TYPE_LEN, bdb_state->repinfo->myhost);
    p_buf = ack_info_data(info);
    p_buf_end = p_buf + BDB_FILEPAGE_TYPE_LEN;
    rep_udp_filepage_type_put(&filepage, p_buf, p_buf_end);

    info->from = 0;
    info->to = 0;
    info->type = USER_TYPE_UDP_PREFAULT;
    info->len = sizeof(filepage_type);

    udp_send(bdb_state, info, tohost);

    return 0;
}

void udp_prefault_all(bdb_state_type *bdb_state, unsigned int fileid,
                      unsigned int pgno)
{
    repinfo_type *repinfo = bdb_state->repinfo;
    const char *hosts[REPMAX];
    int i;

    if (!gbl_prefault_udp)
        return;

    if (repinfo->myhost != repinfo->master_host)
        return;

    i = net_get_all_nodes_connected(repinfo->netinfo, hosts);

    while (i--) {
        send_prefault(bdb_state, hosts[i], fileid, pgno);
    }
}

static void print_ping_rtt(ack_info *info)
{
    struct timeval now, *sent, diff;
    gettimeofday(&now, NULL);
    sent = ack_info_data(info);
    timeval_diff(sent, &now, &diff);
    const char *type;
    switch (info->type) {
    case USER_TYPE_UDP_TIMESTAMP_ACK:
        type = "UDP";
        break;
    case USER_TYPE_TCP_TIMESTAMP_ACK:
        type = "TCP";
        break;
    case USER_TYPE_PING_TIMESTAMP_ACK:
        type = "TCP->UDP";
        break;
    default:
        type = "???";
        break;
    }
    logmsg(LOGMSG_USER, "NODE:%s %s time:%.3fms\n", ack_info_from_host(info), 
           type, (double)diff.tv_sec * 1000 + (double)diff.tv_usec / 1000);
}

int enque_udppfault_filepage(bdb_state_type *bdb_state, unsigned int fileid,
                             unsigned int pgno);

const uint8_t *rep_udp_filepage_type_get(filepage_type *p_filepage_type,
                                         const uint8_t *p_buf,
                                         const uint8_t *p_buf_end);

static void *udp_reader(void *arg)
{
    bdb_state_type *bdb_state = (bdb_state_type *)arg;
    bdb_thread_event(bdb_state, BDBTHR_EVENT_START_RDONLY);

    repinfo_type *repinfo = bdb_state->repinfo;
    netinfo_type *netinfo = repinfo->netinfo;
    void *data;
    uint8_t buff[1024];
    ssize_t nrecv;
    ack_info *info = (ack_info *)buff;
    char straddr[256];
    char *from;
    int type;
    int fd = repinfo->udp_fd;
    uint8_t *p_buf, *p_buf_end;
    filepage_type fp;

    while (!db_is_stopped()) {
#ifdef UDP_DEBUG
        struct sockaddr_in addr;
        struct sockaddr_in *paddr = &addr;
        struct sockaddr *ptr;
        socklen_t socklen = sizeof(addr);
        ptr = (struct sockaddr *)&addr;
        nrecv = recvfrom(fd, &buff, sizeof(buff), 0, ptr, &socklen);
#else
        struct sockaddr_in *paddr = NULL;
        nrecv = recvfrom(fd, &buff, sizeof(buff), 0, NULL, NULL);
#endif

        if (nrecv < 0) {
            logmsgperror("udp_reader:recvfrom");
            continue;
        }

        ++recd_udp;
        ack_info_to_cpu(info);

        if (ack_info_size(info) != nrecv) {
            fprintf(stderr, "%s:invalid read of %zd (header suggests: %u)\n",
                    __func__, nrecv, ack_info_size(info));
            ++recl_udp;
            continue;
        }

        /* Old format included source/dest node numbers - no longer have that
         * luxury - read them from
         * the packet past the data payload. */
        if (info->to != 0 && info->from != 0) {
            logmsg(LOGMSG_ERROR,
                   "unexpected to/from setting: from=%d to=%d type=%d\n",
                   info->from, info->to, info->type);
            ++recl_udp;
            continue;
        }

        from = ack_info_from_host(info);
        /* sanity check? */
        if (info->hdrsz + info->fromlen > sizeof(buff) ||
            from[info->fromlen - 1] != 0) {
            fprintf(stderr, "invalid packet? %d %d\n",
                    info->hdrsz + info->fromlen > sizeof(buff),
                    from[info->fromlen] != 0);
            fsnapf(stdout, info, 64);
            continue;
        }
        from = intern(from);

/* If to == 0 it was probably through udp_ping_ip */
#if 0
        if(info->to && info->to != bdb_state->repinfo->myhost) {
            ++rect_udp;
            /* Not intended for me; discard it */
            continue;
        }
#endif

        type = info->type;
        switch (type) {
        case USER_TYPE_BERKDB_NEWSEQ:
            data = ack_info_data(info);
            berkdb_receive_rtn(NULL, bdb_state, from, type, data, info->len, 0);
            debug_trace("received lsn from: %d %s", from,
                        print_addr(paddr, straddr));
#ifdef UDP_DEBUG
            /* ack every received lsn. */
            udp_send_header(bdb_state, from, USER_TYPE_UDP_ACK);
            debug_trace("sent ack %d -> %d", info->to, from);
#endif
            break;

        case USER_TYPE_UDP_PREFAULT:
            data = ack_info_data(info);
            p_buf = data;
            p_buf_end = ((uint8_t *)data + info->len);
            p_buf = (uint8_t *)rep_udp_filepage_type_get(&fp, p_buf, p_buf_end);
            enque_udppfault_filepage(bdb_state, fp.fileid, fp.pgno);
#ifdef UDP_DEBUG
            /* ack every received lsn. */
            udp_send_header(bdb_state, from, USER_TYPE_UDP_ACK);
            debug_trace("sent ack %d -> %d", info->to, from);
#endif
            break;

        case USER_TYPE_UDP_ACK:
            debug_trace("received ack from %d %s", from,
                        print_addr(paddr, straddr));
            break;

        case USER_TYPE_UDP_PING: {
            udp_send_header(bdb_state, from, USER_TYPE_UDP_ACK);
            debug_trace("sent ack %d -> %d", info->to, from);
            break;
        }

        case USER_TYPE_UDP_TIMESTAMP: {
            /* Just send the packet back */
            ack_info *ackrsp;
            new_ack_info(ackrsp, info->len, bdb_state->repinfo->myhost);
            ackrsp->type = USER_TYPE_UDP_TIMESTAMP_ACK;
            memcpy(ack_info_data(ackrsp), ack_info_data(info), info->len);
            udp_send(bdb_state, ackrsp, from);
            debug_trace("recd timestamp from %d %s", from,
                        print_addr(paddr, straddr));
            break;
        }

        case USER_TYPE_COHERENCY_LEASE:
            data = ack_info_data(info);
            receive_coherency_lease(NULL, bdb_state, from,
                                    USER_TYPE_COHERENCY_LEASE, data, info->len,
                                    0);
            break;

        case USER_TYPE_PAGE_COMPACT:
            data = ack_info_data(info);
            berkdb_receive_msg(NULL, bdb_state, from, USER_TYPE_PAGE_COMPACT,
                               data, info->len, 0);
            break;

        case USER_TYPE_UDP_TIMESTAMP_ACK:
        case USER_TYPE_PING_TIMESTAMP_ACK:
            print_ping_rtt(info);
            break;


        default:
            printf("%s: recd unknown packet type:%d from:%s\n", __func__, type,
                   from);
            break;
        }

#ifdef UDP_DEBUG
        /* dont do this accounting unless in debug mode.
         * it slows down because it needs to get a lock */
        net_inc_recv_cnt_from(netinfo, from);
#endif
    }
    bdb_thread_event(bdb_state, BDBTHR_EVENT_DONE_RDONLY);
    return NULL;
}

void start_udp_reader(bdb_state_type *bdb_state)
{
    repinfo_type *repinfo = bdb_state->repinfo;
    netinfo_type *netinfo = repinfo->netinfo;
    if (!is_real_netinfo(netinfo)) {
        return;
    }

    udp_bind(repinfo);

    pthread_t *thread = &repinfo->udp_thread;
    extern pthread_attr_t gbl_pthread_attr;
    int rc = pthread_create(thread, &gbl_pthread_attr, udp_reader, bdb_state);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "start_udp_reader:pthread_create udp_reader: %s", strerror(errno));
        exit(1);
    }
}

void udp_summary(void)
{
    logmsg(LOGMSG_USER, 
           "udp packets sent: %u\n"
           "udp packets failed to send: %u\n"
           "udp packets received: %u\n"
           "udp packets received with len mismatch: %u\n"
           "udp packets received with destination mismatch: %u\n",
           sent_udp, fail_udp, recd_udp, recl_udp, rect_udp);
}

// Zero out all counters
void udp_reset(netinfo_type *netinfo)
{
    rect_udp = recl_udp = fail_udp = sent_udp = recd_udp = 0;
    net_reset_udp_stat(netinfo);
}

void tcp_ping_all(bdb_state_type *bdb_state)
{
    ping_all_int(bdb_state, USER_TYPE_TCP_TIMESTAMP);
}

void tcp_ping(bdb_state_type *bdb_state, char *to)
{
    if (send_timestamp(bdb_state, to, USER_TYPE_TCP_TIMESTAMP) > 0) {
        debug_trace("sent ping %d -> %d", bdb_state->repinfo->mynode, to);
    }
}

void handle_tcp_timestamp(bdb_state_type *bdb_state, ack_info *info,
                          char *tohost)
{
    int type;

    ack_info_to_cpu(info);
    size_t size = ack_info_size(info);

    info->type = type = USER_TYPE_TCP_TIMESTAMP_ACK;
    info->from = info->to = 0;
    ack_info_from_cpu(info);

    net_send(bdb_state->repinfo->netinfo, tohost, type, info, size, 1);
}

void handle_tcp_timestamp_ack(bdb_state_type *bdb_state, ack_info *info)
{
    ack_info_to_cpu(info);
    print_ping_rtt(info);
}

int send_myseqnum_to_master_udp(bdb_state_type *bdb_state)
{
    int get_myseqnum(bdb_state_type * bdb_state, uint8_t * p_net_seqnum);
    ack_info *info;
    uint8_t *p_buf;
    uint8_t *p_buf_end;
    static int lastpr = 0;
    int rc = 0, now;

    new_ack_info(info, BDB_SEQNUM_TYPE_LEN, bdb_state->repinfo->myhost);
    p_buf = ack_info_data(info);
    p_buf_end = p_buf + BDB_SEQNUM_TYPE_LEN;

    if (0 == (rc = get_myseqnum(bdb_state, p_buf))) {
        info->from = 0;
        info->to = 0;
        info->type = USER_TYPE_BERKDB_NEWSEQ;
        udp_send(bdb_state, info, bdb_state->repinfo->master_host);
    } else {
        static time_t lastpr = 0;
        time_t now;
        static uint64_t count = 0;

        count++;
        if ((now = time(NULL)) > lastpr) {
            fprintf(stderr,
                    "%s: get_myseqnum returned non-0, count=%" PRIu64 "\n",
                    __func__, count);
            lastpr = now;
        }
    }
    return rc;
}

int gbl_verbose_send_coherency_lease;

void send_coherency_leases(bdb_state_type *bdb_state, int lease_time,
                           int *inc_wait)
{
    int count, comcount, i, do_send, use_udp, master_is_coherent;
    uint8_t *p_buf, *p_buf_end, buf[COLEASE_TYPE_LEN];
    const char *hostlist[REPMAX];
    const char *comlist[REPMAX];
    colease_t colease;
    ack_info *info;
    static int last_count = 0;

    colease.issue_time = gettimeofday_ms();
    colease.lease_ms = lease_time;

    if (bdb_state->attr->leasebase_trace) {
        static time_t lastpr = 0;
        time_t now;
        if ((now = time(NULL)) > lastpr) {
            logmsg(LOGMSG_INFO, "%s: lease base time is %lu\n", __func__,
                   colease.issue_time);
            lastpr = now;
        }
    }

    use_udp = bdb_state->attr->coherency_lease_udp;

    if (!use_udp) {
        p_buf = buf;
        p_buf_end = buf + COLEASE_TYPE_LEN;

        if (!(colease_type_put(&colease, p_buf, p_buf_end)))
            abort();
    }

    count = net_get_all_nodes_connected(bdb_state->repinfo->netinfo, hostlist);
    comcount =
        net_get_all_commissioned_nodes(bdb_state->repinfo->netinfo, comlist);

    if (count != comcount) {
        static time_t lastpr = 0;
        time_t now;

        /* Assume disconnected node(s) are incoherent */
        *inc_wait = 1;

        if (gbl_verbose_send_coherency_lease &&
            (last_count != count || (now = time(NULL)) - lastpr)) {
            char *machs = (char *)malloc(1);
            int machs_len = 0;
            machs[0] = '\0';

            for (i = 0; i < count; i++) {
                machs_len += (strlen(hostlist[i]) + 2);
                machs = (char *)realloc(machs, machs_len);
                strcat(machs, hostlist[i]);
                strcat(machs, " ");
            }
            logmsg(LOGMSG_INFO,
                   "%s: only %d of %d nodes are connected: %s epoch=%ld\n",
                   __func__, count, comcount, machs, time(NULL));
            free(machs);
            lastpr = now;
        }
    } else if (last_count != comcount) {
        logmsg(LOGMSG_INFO, "%s: sending leases to all nodes, epoch=%ld\n",
               __func__, time(NULL));
    }

    last_count = count;

    /* Check our master-lease */
    if (bdb_state->attr->master_lease) {
        int verify_master_leases_int(bdb_state_type * bdb_state,
                                     const char **comlist, int comcount,
                                     const char *func, uint32_t line);
        master_is_coherent = verify_master_leases_int(
            bdb_state, comlist, comcount, __func__, __LINE__);
    } else
        master_is_coherent = 1;

    for (i = 0; i < count; i++) {
        int catchup_window = bdb_state->attr->catchup_window;
        pthread_mutex_lock(&(bdb_state->coherent_state_lock));

        if (!master_is_coherent || bdb_state->coherent_state[
                nodeix(hostlist[i])] != STATE_COHERENT) {
            *inc_wait = 1;
        }
        do_send = master_is_coherent && (bdb_state->coherent_state[
                nodeix(hostlist[i])] == STATE_COHERENT);
        pthread_mutex_unlock(&(bdb_state->coherent_state_lock));

        if (do_send) {
            if (use_udp) {
                ack_info *info;
                new_ack_info(info, COLEASE_TYPE_LEN,
                        bdb_state->repinfo->myhost);
                p_buf = ack_info_data(info);
                p_buf_end = p_buf + COLEASE_TYPE_LEN;
                if (!(colease_type_put(&colease, p_buf, p_buf_end)))
                    abort();

                info->from = 0;
                info->to = 0;
                info->type = USER_TYPE_COHERENCY_LEASE;
                info->len = COLEASE_TYPE_LEN;

                udp_send(bdb_state, info, hostlist[i]);
            } else {
                net_send_message(bdb_state->repinfo->netinfo, hostlist[i],
                                 USER_TYPE_COHERENCY_LEASE, buf,
                                 COLEASE_TYPE_LEN, 0, 0);
            }
        } else {
            static time_t lastpr = 0;
            time_t now;
            if (gbl_verbose_send_coherency_lease &&
                (now = time(NULL)) - lastpr) {
                logmsg(LOGMSG_ERROR, "%s: not sending to %s\n", __func__,
                       hostlist[i]);
                lastpr = now;
            }
        }
    }
}

void handle_ping_timestamp(bdb_state_type *bdb_state, ack_info *info, char *to)
{
    ack_info_to_cpu(info);
    info->to = 0;
    info->from = 0;
    info->type = USER_TYPE_PING_TIMESTAMP_ACK;
    udp_send(bdb_state, info, to);
}

void ping_all(bdb_state_type *bdb_state)
{
    ping_all_int(bdb_state, USER_TYPE_PING_TIMESTAMP);
}

void ping_node(bdb_state_type *bdb_state, char *to)
{
    if (send_timestamp(bdb_state, to, USER_TYPE_TCP_TIMESTAMP) > 0) {
        debug_trace("sent ping %d -> %d", bdb_state->repinfo->mynode, to);
    }
}

/* vim: set sw=4 ts=4 et: */
static int prepare_pg_compact_msg(bdb_state_type *bdb_state, ack_info *info,
                                  int32_t fileid, uint32_t size,
                                  const void *data)
{
    uint8_t *p_buf;
    uint8_t *p_buf_end;
    pgcomp_snd_t snd;

    snd.id = fileid;
    snd.size = size;

    p_buf = ack_info_data(info);
    p_buf_end = p_buf + BDB_PGCOMP_SND_TYPE_LEN + size;

    (void)pgcomp_snd_type_put(&snd, p_buf, p_buf_end, data);

    info->from = info->to = 0;
    info->type = USER_TYPE_PAGE_COMPACT;

    return 0;
}

int send_pg_compact_req(bdb_state_type *bdb_state, int32_t fileid,
                        uint32_t size, const void *data)
{
    int rc;
    char *master;
    ack_info *info;
    uint8_t *p_buf;
    repinfo_type *repinfo;

/* If the page is from a dta file, `data` is a genid and `size` is 8 bytes.
   However we may have variant-length data if the page is from ix where `data`
   is (ixdata + genid).  Given the fact that we don't touch overflow pages
   and comdb2 maximum page size is 64K, `size` can't be larger than 32K. */
#define PGCOMPMAXLEN (1U << 15)

    if (size > PGCOMPMAXLEN)
        return E2BIG;

    repinfo = bdb_state->repinfo;
    master = repinfo->master_host;

    if (repinfo->myhost == repinfo->master_host)
        rc = enqueue_pg_compact_work(bdb_state, fileid, size, data);
    else {
        new_ack_info(info, BDB_PGCOMP_SND_TYPE_LEN + size, repinfo->myhost);
        rc = prepare_pg_compact_msg(bdb_state, info, fileid, size, data);

        if (rc != 0)
            goto out;

        if (bdb_state->attr->page_compact_udp)
            rc = udp_send(bdb_state, info, master);
        else {
            p_buf = ack_info_data(info);
            rc = net_send(repinfo->netinfo, master, USER_TYPE_PAGE_COMPACT,
                          p_buf, BDB_PGCOMP_SND_TYPE_LEN + size, 1);
        }
    }
#ifdef PGCOMP_DBG
    char dbgbuf[(size << 1) + 1];
    const char *p = data;
    for (int i = 0; i != size; ++i)
        sprintf(dbgbuf + (i << 1), "%02x", *(p + i));
    dbgbuf[size << 1] = 0;
    fprintf(
        stderr,
        "(!) %s %d: sent DBT data (%d bytes) %s in fileid %d for compaction\n",
        __FILE__, __LINE__, size, dbgbuf, fileid);
#endif
out:
    return rc;
}

const char *get_hostname_with_crc32(bdb_state_type *bdb_state,
                                    unsigned int hash)
{
    repinfo_type *repinfo = bdb_state->repinfo;
    int tmp = crc32c((const uint8_t *)repinfo->myhost, strlen(repinfo->myhost));
    if (tmp == hash)
        return repinfo->myhost;

    const char *hosts[REPMAX];
    int count = net_get_all_nodes(repinfo->netinfo, hosts);

    for (int i = 0; i < count; i++) {
        if(crc32c((const uint8_t*)hosts[i], strlen(hosts[i])) == hash) 
            return hosts[i];
    }
    return NULL;
}
