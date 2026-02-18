/*
   Copyright 2021 Bloomberg Finance L.P.

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

#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <event2/buffer.h>
#include <event2/event.h>

#include <bdb_api.h>
#include <comdb2_appsock.h>
#include <comdb2_atomic.h>
#include <comdb2_plugin.h>
#include <hostname_support.h>
#include <intern_strings.h>
#include <net_appsock.h>
#include <net_int.h>
#include <rtcpu.h>
#include <sql.h>
#include <sqlwriter.h>
#include <ssl_evbuffer.h>
#include <ssl_glue.h>
#include <str0.h>
#include <timer_util.h>
#include <pb_alloc.h>
#include <comdb2uuid.h>
#include <osqlsession.h>
#include <disttxn.h>

#include <newsql.h>

void dump_response(const CDB2SQLRESPONSE *r);
void dump_request(const CDB2SQLQUERY *q);

int gbl_new_connection_grace_ms = 100;
extern int gbl_incoherent_clnt_wait;
extern int gbl_new_leader_duration;
extern SSL_CTX *gbl_ssl_ctx;
extern ssl_mode gbl_client_ssl_mode;

struct newsql_appdata_evbuffer;

struct dispatch_sql_arg {
    struct timeval start;
    int wait_time;
    int dispatched;
    struct event *ev;
    struct newsql_appdata_evbuffer *appdata;
    TAILQ_ENTRY(dispatch_sql_arg) entry;
};
static TAILQ_HEAD(, dispatch_sql_arg) dispatch_list = TAILQ_HEAD_INITIALIZER(dispatch_list);
static pthread_mutex_t dispatch_lk = PTHREAD_MUTEX_INITIALIZER;
static struct event_base *dispatch_base;

struct newsql_appdata_evbuffer {
    NEWSQL_APPDATA_COMMON /* Must be first */

    int fd;
    pthread_t thd;
    struct event_base *base;
    struct event *cleanup_ev;
    struct newsqlheader hdr;
    struct dispatch_sql_arg *dispatch;

    struct evbuffer *rd_buf;
    struct event *rd_hdr_ev;
    struct event *rd_payload_ev;

    unsigned allow_lru : 1;
    unsigned initial : 1; /* New connection or called newsql_reset */
    unsigned local : 1;

    struct sqlwriter *writer;
    struct ssl_data *ssl_data;

    void (*add_rd_event_fn)(struct newsql_appdata_evbuffer *, struct event *, struct timeval *);
    int (*rd_evbuffer_fn)(struct newsql_appdata_evbuffer *);
    void (*wr_dbinfo_fn)(struct newsql_appdata_evbuffer *);

    struct sqlclntstate clnt;
};

static void rd_hdr(int, short, void *);
static int rd_evbuffer_plaintext(struct newsql_appdata_evbuffer *);
static int rd_evbuffer_ciphertext(struct newsql_appdata_evbuffer *);
static void disable_ssl_evbuffer(struct newsql_appdata_evbuffer *);
static int newsql_write_hdr_evbuffer(struct sqlclntstate *, int, int);

static void free_newsql_appdata_evbuffer(struct newsql_appdata_evbuffer *appdata)
{
    check_thd(appdata->thd);

    struct sqlclntstate *clnt = &appdata->clnt;
    int fd = appdata->fd;

    rem_lru_evbuffer(clnt);
    rem_sql_evbuffer(clnt);
    rem_appsock_connection_evbuffer();
    if (appdata->dispatch) {
        abort(); /* should have been freed by timeout or coherency-lease */
    }
    if (appdata->cleanup_ev) {
        event_free(appdata->cleanup_ev);
    }
    if (appdata->rd_hdr_ev) {
        event_free(appdata->rd_hdr_ev);
    }
    if (appdata->rd_payload_ev) {
        event_free(appdata->rd_payload_ev);
    }
    if (appdata->rd_buf) {
        evbuffer_free(appdata->rd_buf);
    }
    if (appdata->ssl_data) {
        disable_ssl_evbuffer(appdata);
        free(appdata->ssl_data);
    }
    if (appdata->query) {
        cdb2__query__free_unpacked(appdata->query, &pb_alloc);
        appdata->query = NULL;
        clnt->externalAuthUser = NULL;
    }
    sqlwriter_free(appdata->writer);
    shutdown(fd, SHUT_RDWR);
    Close(fd);
    free_newsql_appdata(clnt);
    free(appdata);
}

static void newsql_cleanup(int dummyfd, short what, void *data)
{
    struct newsql_appdata_evbuffer *appdata = data;
    free_newsql_appdata_evbuffer(appdata);
}

static int newsql_flush_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return sql_flush(appdata->writer);
}

static void newsql_reset_evbuffer(struct newsql_appdata_evbuffer *appdata)
{
    appdata->initial = 1;
    appdata->clnt.in_local_cache = (appdata->hdr.state == NEWSQL_STATE_LOCALCACHE);
    newsql_reset(&appdata->clnt);
}

static int newsql_done_cb(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    if (sql_done(appdata->writer) == 0) {
        if (clnt->added_to_hist) {
            clnt->added_to_hist = 0;
        } else if (appdata->query) {
            cdb2__query__free_unpacked(appdata->query, &pb_alloc);
        }
        appdata->query = NULL;
        clnt->externalAuthUser = NULL;
        evtimer_once(appdata->base, rd_hdr, appdata);
    } else {
        appdata->cleanup_ev = event_new(appdata->base, -1, 0, newsql_cleanup, appdata);
        event_active(appdata->cleanup_ev, 0, 0);
    }
    return 0;
}

static int newsql_get_fileno_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return appdata->fd;
}

static int newsql_get_x509_attr_evbuffer(struct sqlclntstate *clnt, int nid, void *out, int outsz)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return ssl_data_cert(appdata->ssl_data, nid, out, outsz);
}

static int newsql_has_ssl_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return ssl_data_has_ssl(appdata->ssl_data);
}

static int newsql_has_x509_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return ssl_data_has_cert(appdata->ssl_data);
}

static int newsql_local_check_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return appdata->local;
}

static int newsql_peer_check_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return sql_peer_check(appdata->writer);
}

static int rd_evbuffer(struct newsql_appdata_evbuffer *appdata)
{
    return appdata->rd_evbuffer_fn(appdata); /* rd_evbuffer_plaintext */
}

/**
 * Called by `read_response(RESPONSE_PING_PONG)` in lua/sp.c
 * @retval  0: Received response
 * @retval -1: Timeout (1 second)
 * @retval -2: Read error
 * @retval -3: Unexpected respnose
 */
static int newsql_ping_pong_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    struct timeval start, elapsed = {0};
    gettimeofday(&start, NULL);
    int pollms = 1000;
    while (pollms > 0) {
        struct pollfd pfd;
        pfd.fd = appdata->fd;
        pfd.events = POLLIN;
        int rc = poll(&pfd, 1, pollms);
        if (rc == 0) return -1;
        if (rc != 1) return -2;
        int n = rd_evbuffer(appdata);
        if (n <= 0) return -2;
        struct newsqlheader hdr;
        if (evbuffer_get_length(appdata->rd_buf) >= sizeof(hdr)) {
            evbuffer_remove(appdata->rd_buf, &hdr, sizeof(hdr));
            if (ntohl(hdr.type) == RESPONSE_HEADER__SQL_RESPONSE_PONG) {
                return 0;
            } else {
                return -3;
            }
        }
        struct timeval now;
        gettimeofday(&now, NULL);
        timersub(&now, &start, &elapsed);
        pollms = 1000 - timeval_to_ms(elapsed);
    }
    return -1;
}

static void wr_dbinfo(int dummyfd, short what, void *arg)
{
    struct newsql_appdata_evbuffer *appdata = arg;
    appdata->wr_dbinfo_fn(appdata); /* wr_dbinfo_plaintext */
}

static void wr_dbinfo_int(struct newsql_appdata_evbuffer *appdata, int write_result)
{
    if (write_result <= 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        free_newsql_appdata_evbuffer(appdata);
        return;
    }
    struct evbuffer *wr_buf = sql_wrbuf(appdata->writer);
    if (evbuffer_get_length(wr_buf) == 0) {
        evtimer_once(appdata->base, rd_hdr, appdata);
        return;
    }
    event_base_once(appdata->base, appdata->fd, EV_WRITE, wr_dbinfo, appdata, NULL);
}

static void wr_dbinfo_ssl(struct newsql_appdata_evbuffer *appdata)
{
    int rc = wr_ssl_evbuffer(appdata->ssl_data, sql_wrbuf(appdata->writer));
    wr_dbinfo_int(appdata, rc);
}

static void wr_dbinfo_plaintext(struct newsql_appdata_evbuffer *appdata)
{
    struct evbuffer *wr_buf = sql_wrbuf(appdata->writer);
    int rc = evbuffer_write(wr_buf, appdata->fd);
    wr_dbinfo_int(appdata, rc);
}

static void process_dbinfo_int(struct get_hosts_evbuffer_arg *arg)
{
    struct newsql_appdata_evbuffer *appdata = arg->appdata;
    host_node_type **hosts = arg->hosts;
    int num_hosts = arg->num_hosts;

    CDB2DBINFORESPONSE__Nodeinfo *nodes[REPMAX];
    CDB2DBINFORESPONSE__Nodeinfo same_dc[REPMAX], diff_dc[REPMAX];
    CDB2DBINFORESPONSE__Nodeinfo no_master = CDB2__DBINFORESPONSE__NODEINFO__INIT, *master = &no_master;
    no_master.name = db_eid_invalid;
    no_master.number = -1;
    int num_same_dc = 0, num_diff_dc = 0;
    int my_dc = machine_dc(gbl_myhostname);
    int process_incoherent = 0;
    if (bdb_amimaster(thedb->bdb_env)) {
        if (gbl_incoherent_clnt_wait <= 0 || !leader_is_new()) {
            process_incoherent = 1;
        }
    }
    const char *who = bdb_whoismaster(thedb->bdb_env);
    for (int i = 0; i < num_hosts; ++i) {
        CDB2DBINFORESPONSE__Nodeinfo *node;
        int dc = machine_dc(hosts[i]->host);
        node = (dc == my_dc) ?  &same_dc[num_same_dc++] : &diff_dc[num_diff_dc++];
        cdb2__dbinforesponse__nodeinfo__init(node);
        node->has_room = 1;
        node->room = dc;
        node->has_port = 1;
        node->port = hosts[i]->port;
        node->name = hosts[i]->host;
        node->incoherent = process_incoherent ? is_incoherent(thedb->bdb_env, hosts[i]->host_interned) : 0;
        if (who && strcmp(who, node->name) == 0) {
            master = node;
        }
    }
    int j = 0;
    for (int i = 0; i < num_same_dc; ++i, ++j) {
        nodes[j] = &same_dc[i];
        nodes[j]->number = j;
    }
    for (int i = 0; i < num_diff_dc; ++i, ++j) {
        nodes[j] = &diff_dc[i];
        nodes[j]->number = j;
    }
    CDB2DBINFORESPONSE response = CDB2__DBINFORESPONSE__INIT;
    fill_ssl_info(&response);
    response.n_nodes = num_hosts;
    response.master = master;
    response.nodes = nodes;
    response.has_sync_mode = 1;
    response.sync_mode = sync_state_to_protobuf(thedb->rep_sync);

    int len = cdb2__dbinforesponse__get_packed_size(&response);
    struct newsqlheader hdr = {0};
    hdr.type = htonl(RESPONSE_HEADER__DBINFO_RESPONSE);
    hdr.length = htonl(len);
    uint8_t *out = alloca(len);
    cdb2__dbinforesponse__pack(&response, out);

    struct iovec vec[2];
    vec[0].iov_base = &hdr;
    vec[0].iov_len = sizeof(hdr);
    vec[1].iov_base = out;
    vec[1].iov_len = len;

    struct evbuffer *buf = sql_wrbuf(appdata->writer);
    evbuffer_add_iovec(buf, vec, 2);
}

static void process_dbinfo(int dummyfd, short what, void *data)
{
    struct get_hosts_evbuffer_arg *arg = data;
    struct newsql_appdata_evbuffer *appdata = arg->appdata;
    process_dbinfo_int(arg);
    wr_dbinfo(-1, 0, appdata);
    free(arg);
}

static void process_get_effects(struct newsql_appdata_evbuffer *appdata)
{
    CDB2EFFECTS effects = CDB2__EFFECTS__INIT;
    CDB2SQLRESPONSE response = CDB2__SQLRESPONSE__INIT;
    newsql_effects(&response, &effects, &appdata->clnt);
    int len = cdb2__sqlresponse__get_packed_size(&response);
    struct newsqlheader hdr = {0};
    hdr.type = htonl(RESPONSE_HEADER__SQL_EFFECTS);
    hdr.length = htonl(len);
    uint8_t out[len];
    cdb2__sqlresponse__pack(&response, out);
    struct evbuffer *wr_buf = sql_wrbuf(appdata->writer);
    evbuffer_add(wr_buf, &hdr, sizeof(hdr));
    evbuffer_add(wr_buf, out, len);
    event_base_once(appdata->base, appdata->fd, EV_WRITE, wr_dbinfo, appdata, NULL);
}

static int ssl_check(struct newsql_appdata_evbuffer *appdata, int have_ssl, int secure)
{
    if (ssl_data_has_ssl(appdata->ssl_data)) return 0;
    if (have_ssl) {
        newsql_write_hdr_evbuffer(&appdata->clnt, RESPONSE_HEADER__SQL_RESPONSE_SSL, 0);
        return 1;
    }
    if (!secure && (ssl_whitelisted(appdata->clnt.origin) || (SSL_IS_OPTIONAL(gbl_client_ssl_mode)))) {
        /* allow plaintext local connections, or server is configured to prefer (but not disallow) SSL clients. */
        newsql_write_hdr_evbuffer(&appdata->clnt, RESPONSE_HEADER__SQL_RESPONSE_SSL, 0);
        return 0;
    }
    write_response(&appdata->clnt, RESPONSE_ERROR, "database requires SSL connections", CDB2ERR_CONNECT_ERROR);
    return 2;
}

static int dispatch_client(struct newsql_appdata_evbuffer *appdata)
{
    int rc = dispatch_sql_query_no_wait(&appdata->clnt);
    if (rc == 0) {
        ATOMIC_ADD64(gbl_nnewsql, 1);
        appdata->allow_lru = 1;
    }
    return rc;
}

extern pthread_mutex_t buf_lock;
extern pool_t *p_slocks;

static void legacy_sndbak(struct ireq *iq, int rc, int len)
{
    struct buf_lock_t *p_slock = iq->request_data;
    struct legacy_response {
        int rc;
        int outlen;
        char buf[64 * 1024];
    } rsp;

    struct sqlclntstate *clnt = (struct sqlclntstate *)iq->setup_data;
    rsp.rc = rc;
    rsp.outlen = len;
    int flip = endianness_mismatch(clnt);
    if (flip)
        rsp.rc = flibc_intflip(rsp.rc);
    char *fb = (char *)rsp.buf;
    fb[0] = 0xfd;
    // seems to be lots of copying here...
    memcpy(rsp.buf, p_slock->bigbuf, len);
    write_response(clnt, RESPONSE_RAW_PAYLOAD, &rsp, 0);

    free(p_slock->bigbuf);
    Pthread_mutex_lock(&buf_lock);
    pool_relablk(p_slocks, p_slock);
    Pthread_mutex_unlock(&buf_lock);
    // we won't be adding these to history - they aren't SQL - have them
    // freed instead
    clnt->added_to_hist = 0;
    clnt->done_cb(clnt);
}

static void legacy_iq_setup(struct ireq *iq, void *setup_data)
{
    struct sqlclntstate *clnt = (struct sqlclntstate *)setup_data;
    iq->ipc_sndbak = legacy_sndbak;
    iq->setup_data = setup_data;
    iq->has_ssl = clnt->plugin.has_ssl(clnt);
    iq->identity = clnt->plugin.get_identity(clnt);
    get_client_origin(iq->corigin, sizeof(iq->corigin), clnt);
}

static int dispatch_tagged(struct sqlclntstate *clnt)
{
    int rc;
    struct newsql_appdata_evbuffer *appdata = (struct newsql_appdata_evbuffer *)clnt->appdata;

    struct buf_lock_t *p_slock = NULL;
    Pthread_mutex_lock(&buf_lock);
    p_slock = pool_getablk(p_slocks);
    Pthread_mutex_unlock(&buf_lock);

    void *buf = appdata->sqlquery->bindvars[0]->value.data;
    int sz = appdata->sqlquery->bindvars[0]->value.len;

    Pthread_mutex_init(&(p_slock->req_lock), 0);
    Pthread_cond_init(&(p_slock->wait_cond), NULL);
    p_slock->bigbuf = malloc(64 * 1024);
    memcpy(p_slock->bigbuf, buf, sz);
    p_slock->sb = NULL;
    p_slock->reply_state = REPLY_STATE_NA;

    clnt->authdata = get_authdata(clnt);
    if (appdata->sqlquery == NULL || appdata->sqlquery->n_bindvars < 3 || appdata->sqlquery->bindvars[0] == NULL ||
        appdata->sqlquery->bindvars[1] == NULL || appdata->sqlquery->bindvars[1]->value.len != sizeof(int) ||
        appdata->sqlquery->bindvars[2] == NULL || appdata->sqlquery->bindvars[2]->value.len != sizeof(int)) {
        return 1;
    }

    int luxref;
    int comdbg_flags;
    memcpy(&luxref, appdata->sqlquery->bindvars[1]->value.data, sizeof(int));
    memcpy(&comdbg_flags, appdata->sqlquery->bindvars[2]->value.data, sizeof(int));
    if (endianness_mismatch(clnt)) {
        luxref = flibc_intflip(luxref);
        comdbg_flags = flibc_intflip(comdbg_flags);
    }

    if (!bdb_am_i_coherent(thedb->bdb_env)) {
        struct req_hdr hdr;
        if (req_hdr_get(&hdr, buf, ((uint8_t *)buf) + sz, comdbg_flags) == NULL) {
            logmsg(LOGMSG_ERROR, "%s:failed to unpack req header\n", __func__);
            return 1;
        }
        if (hdr.opcode != OP_DBINFO && hdr.opcode != OP_DBINFO2) {
            logmsg(LOGMSG_ERROR, "Rejecting tagged request %d %s on incoherent node\n", hdr.opcode, req2a(hdr.opcode));
            return 1;
        }
    }

    // This will dispatch to a thread - it's not done inline
    rc = handle_buf_main2(thedb, NULL, p_slock->bigbuf, (uint8_t *)p_slock->bigbuf + 1024 * 64, 0, clnt->origin,
                          clnt->last_pid, clnt->argv0, NULL, REQ_SQLLEGACY, p_slock, luxref, 0, NULL, 0, comdbg_flags,
                          legacy_iq_setup, clnt, 0, clnt->authdata);
    return rc;
}

static void free_dispatch_sql_arg(struct dispatch_sql_arg *d)
{
    d->appdata->dispatch = NULL;
    event_free(d->ev);
    free(d);
}

static void dispatch_waiting_client(int fd, short what, void *data)
{
    struct dispatch_sql_arg *d = data;
    struct newsql_appdata_evbuffer *appdata = d->appdata;
    check_thd(appdata->thd);
    struct timeval end, diff;
    gettimeofday(&end, NULL);
    timersub(&end, &d->start, &diff);
    free_dispatch_sql_arg(d);
    if (!bdb_try_am_i_coherent(thedb->bdb_env)) {
        logmsg(LOGMSG_DEBUG, "%s: new query on incoherent node, dropping socket fd:%d\n", __func__, appdata->fd);
        free_newsql_appdata_evbuffer(appdata);
    } else if (dispatch_client(appdata) == 0) {
        logmsg(LOGMSG_USER, "%s: waited %lds.%dms for election fd:%d\n", __func__, diff.tv_sec, (int)diff.tv_usec / 1000, appdata->fd);
        sql_wait_for_leader(appdata->writer, NULL);
    } else {
        free_newsql_appdata_evbuffer(appdata);
    }
}

static void do_dispatch_waiting_clients(int fd, short what, void *data)
{
    Pthread_mutex_lock(&dispatch_lk);
    struct dispatch_sql_arg *d, *tmp;
    TAILQ_FOREACH_SAFE(d, &dispatch_list, entry, tmp) {
        d->dispatched = 1;
        TAILQ_REMOVE(&dispatch_list, d, entry);
        struct newsql_appdata_evbuffer *appdata = d->appdata;
        evtimer_once(appdata->base, dispatch_waiting_client, d);
    }
    Pthread_mutex_unlock(&dispatch_lk);
}

void dispatch_waiting_clients(void)
{
    if (!dispatch_base)  {
        // Check if setup_bases has happened yet
        dispatch_base = get_dispatch_event_base();
        if (!dispatch_base)
            return;
    }
    evtimer_once(dispatch_base, do_dispatch_waiting_clients, NULL);
}

static int do_dispatch_sql(struct newsql_appdata_evbuffer *appdata)
{
    struct dispatch_sql_arg *d = appdata->dispatch;
    if (d->dispatched) return -1; /* already dispatched */
    if (--d->wait_time <= 0) return 0; /* timed out, dispatch now */
    if (bdb_try_am_i_coherent(thedb->bdb_env)) return 0; /* am coherent, dispatch now */
    if (!bdb_whoismaster(thedb->bdb_env)) return -1; /* still no master, wait more */
    if (leader_is_new()) {
        if (d->wait_time > gbl_new_leader_duration) {
            logmsg(LOGMSG_USER, "%s: have leader, now waiting %ds (was %ds) for coherency lease fd:%d\n",
                   __func__, gbl_new_leader_duration, d->wait_time, appdata->fd);
            d->wait_time = gbl_new_leader_duration;
        }
        return -1; /* wait a bit more to catch up */
    }
    return 0; /* have established leader, dispatch now */
}

static void dispatch_sql(int fd, short what, void *data)
{
    Pthread_mutex_lock(&dispatch_lk);
    struct newsql_appdata_evbuffer *appdata = data;
    if (do_dispatch_sql(appdata) == 0) {
        TAILQ_REMOVE(&dispatch_list, appdata->dispatch, entry);
        dispatch_waiting_client(-1, EV_TIMEOUT, appdata->dispatch);
    }
    Pthread_mutex_unlock(&dispatch_lk);
}

static void timed_out_waiting_for_leader(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    struct dispatch_sql_arg *d = appdata->dispatch;
    check_thd(appdata->thd);
    Pthread_mutex_lock(&dispatch_lk);
    TAILQ_REMOVE(&dispatch_list, d, entry);
    Pthread_mutex_unlock(&dispatch_lk);

    struct timeval end, diff;
    gettimeofday(&end, NULL);
    timersub(&end, &d->start, &diff);
    logmsg(LOGMSG_USER, "%s: query timeout waiting for election waited:%lds fd:%d\n",
           __func__, diff.tv_sec, appdata->fd);
    free_dispatch_sql_arg(d);
    appdata->cleanup_ev = event_new(appdata->base, -1, 0, newsql_cleanup, appdata);
    event_active(appdata->cleanup_ev, 0, 0);
}

static void wait_for_leader(struct newsql_appdata_evbuffer *appdata, newsql_loop_result incoherent)
{
    if (incoherent == NEWSQL_INCOHERENT) {
        logmsg(LOGMSG_DEBUG, "%s: new query on incoherent node, dropping socket fd:%d\n", __func__, appdata->fd);
        free_newsql_appdata_evbuffer(appdata);
        return;
    }
    struct dispatch_sql_arg *d = calloc(1, sizeof(struct dispatch_sql_arg));
    d->appdata = appdata;
    d->ev = event_new(appdata->base, -1, EV_TIMEOUT | EV_PERSIST, dispatch_sql, appdata);
    Pthread_mutex_lock(&dispatch_lk);
    if (bdb_try_am_i_coherent(thedb->bdb_env)) {
        /* check again under lock to prevent race with concurrent leader-upgrade */
        Pthread_mutex_unlock(&dispatch_lk);
        event_free(d->ev);
        free(d);
        if (dispatch_client(appdata) != 0) {
            free_newsql_appdata_evbuffer(appdata);
        }
        return;
    }
    appdata->dispatch = d;
    if (incoherent == NEWSQL_NO_LEADER) {
        d->wait_time = gbl_incoherent_clnt_wait;
        logmsg(LOGMSG_DEBUG, "%s: new query on incoherent node, waiting %ds for election fd:%d\n",
               __func__, d->wait_time, appdata->fd);
    } else if (incoherent == NEWSQL_NEW_LEADER) {
        d->wait_time = gbl_new_leader_duration;
        logmsg(LOGMSG_DEBUG, "%s: new query on incoherent node, waiting %ds for coherency lease fd:%d\n",
               __func__, d->wait_time, appdata->fd);
    } else {
        logmsg(LOGMSG_FATAL, "%s: unknown incoherent state:%d\n", __func__, incoherent);
        abort();
    }
    sql_wait_for_leader(appdata->writer, timed_out_waiting_for_leader);
    struct timeval timeout = {.tv_sec = 1};
    gettimeofday(&d->start, NULL);
    event_add(d->ev, &timeout);
    TAILQ_INSERT_TAIL(&dispatch_list, d, entry);
    Pthread_mutex_unlock(&dispatch_lk);
}

static void process_features(struct newsql_appdata_evbuffer *appdata) {
    struct sqlclntstate *clnt = &appdata->clnt;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery = appdata->query->sqlquery;

    memset(&clnt->features, 0, sizeof(clnt->features));
    for (int i = 0; i < sqlquery->n_features; ++i) {
        switch (sqlquery->features[i]) {
        case CDB2_CLIENT_FEATURES__SSL: clnt->features.have_ssl = 1; break;
        case CDB2_CLIENT_FEATURES__SQLITE_ROW_FORMAT: clnt->features.have_sqlite_fmt = 1; break;
        case CDB2_CLIENT_FEATURES__ALLOW_INCOHERENT: clnt->features.allow_incoherent = 1; break;
        case CDB2_CLIENT_FEATURES__SKIP_INTRANS_RESULTS: clnt->features.skip_intrans_results = 1; break;
        case CDB2_CLIENT_FEATURES__FLAT_COL_VALS: clnt->features.flat_col_vals = 1; break;
        case CDB2_CLIENT_FEATURES__REQUEST_FP: clnt->features.request_fp = 1; break;
        case CDB2_CLIENT_FEATURES__REQUIRE_FASTSQL: clnt->features.require_fastsql = 1; break;
        case CDB2_CLIENT_FEATURES__CAN_REDIRECT_FDB: clnt->features.can_redirect_fdb = 1; break;
        case CDB2_CLIENT_FEATURES__ALLOW_MASTER_EXEC: clnt->features.allow_master_exec = 1; break;
        case CDB2_CLIENT_FEATURES__ALLOW_MASTER_DBINFO: clnt->features.allow_master_dbinfo = 1; break;
        case CDB2_CLIENT_FEATURES__ALLOW_QUEUING: clnt->features.queue_me = 1; break;
        }
    }
}

static void process_query(struct newsql_appdata_evbuffer *appdata)
{
    struct sqlclntstate *clnt = &appdata->clnt;
    CDB2SQLQUERY *sqlquery = appdata->sqlquery = appdata->query->sqlquery;

    if (!sqlquery) goto err;
    process_features(appdata);
    int have_ssl = clnt->features.have_ssl;
    int have_sqlite_fmt = clnt->features.have_sqlite_fmt;
    clnt->sqlite_row_format = have_sqlite_fmt;
    clnt->is_tagged = sqlquery->has_is_tagged && sqlquery->is_tagged;
    if (!clnt->is_tagged)
        ++clnt->sqltick;

    /* If the connection is forwarded from a secure pmux port,
     * both IAM and TLS must be enabled on the database. */
    if (clnt->secure) {
        int has_externalauth = gbl_uses_externalauth || gbl_uses_externalauth_connect;
        int ssl_able = SSL_IS_ABLE(gbl_client_ssl_mode);
        if (!has_externalauth || !ssl_able) {
            logmsg(LOGMSG_ERROR, "can't handle a secure connection: externalauth %d ssl %d\n", has_externalauth,
                   ssl_able);
            goto err;
        }
    }

    if (clnt->secure || SSL_IS_PREFERRED(gbl_client_ssl_mode)) {
        switch(ssl_check(appdata, have_ssl, clnt->secure)) {
        case 1: goto read;
        case 2: goto err;
        }
    }
    if (appdata->initial && newsql_first_run(clnt, sqlquery) != 0) goto err;
    appdata->initial = 0;
    newsql_loop_result incoherent = newsql_loop(clnt, sqlquery);
    if (incoherent == NEWSQL_ERROR) goto err;
    int commit_rollback;
    if (newsql_should_dispatch(clnt, &commit_rollback) != 0) {
    read:
        if (appdata->query)
            cdb2__query__free_unpacked(appdata->query, &pb_alloc);
        appdata->query = NULL;
        evtimer_once(appdata->base, rd_hdr, appdata);
        return;
    }
    sql_reset(appdata->writer);
    if (clnt->query_timeout) {
        sql_enable_timeout(appdata->writer, clnt->query_timeout);
    }
    sql_enable_heartbeat(appdata->writer);

    if (incoherent) {
        if (clnt->is_tagged) {
            goto err;
        } else {
            wait_for_leader(appdata, incoherent);
        }
    } else if (clnt->is_tagged) {
        if (dispatch_tagged(clnt) != 0) {
            goto err;
        }
    } else if (dispatch_client(appdata) != 0) {
err:    free_newsql_appdata_evbuffer(appdata);
    }
}

static void process_disttxn(struct newsql_appdata_evbuffer *appdata, CDB2DISTTXN *disttxn)
{
    struct evbuffer *buf = sql_wrbuf(appdata->writer);
    CDB2DISTTXNRESPONSE response = CDB2__DISTTXNRESPONSE__INIT;
    int rcode = 0;
    if (!bdb_amimaster(thedb->bdb_env) || bdb_lock_desired(thedb->bdb_env)) {
        rcode = -1;
        goto sendresponse;
    }

    if (!disttxn->disttxn) {
        logmsg(LOGMSG_ERROR, "%s: disttxn is NULL\n", __func__);
        rcode = -1;
        goto sendresponse;
    }

    switch (disttxn->disttxn->operation) {

    /* Coordinator master tells me (participant master) to prepare */
    case (CDB2_DIST__PREPARE):
        if (!disttxn->disttxn->name || !disttxn->disttxn->tier || !disttxn->disttxn->master) {
            logmsg(LOGMSG_ERROR, "%s: disttxn prepare missing name, tier or master\n", __func__);
            rcode = -1;
            goto sendresponse;
        }
        rcode = osql_prepare(disttxn->disttxn->txnid, disttxn->disttxn->name, disttxn->disttxn->tier,
                             disttxn->disttxn->master);
        break;

    /* Coordinator master tells me (participant master) to discard */
    case (CDB2_DIST__DISCARD):
        rcode = osql_discard(disttxn->disttxn->txnid);
        break;

    /* Participant master sends me (coordinator master) a heartbeat message */
    case (CDB2_DIST__HEARTBEAT):
        if (!disttxn->disttxn->name || !disttxn->disttxn->tier) {
            logmsg(LOGMSG_ERROR, "%s: disttxn malformed heartbeat\n", __func__);
            rcode = -1;
            goto sendresponse;
        }
        rcode = participant_heartbeat(disttxn->disttxn->txnid, disttxn->disttxn->name, disttxn->disttxn->tier);
        break;

    /* Participant master tells me (coordinator master) it has prepared */
    case (CDB2_DIST__PREPARED):
        if (!disttxn->disttxn->name || !disttxn->disttxn->tier || !disttxn->disttxn->master) {
            logmsg(LOGMSG_ERROR, "%s: disttxn prepared missing name, tier or master\n", __func__);
            rcode = -1;
            goto sendresponse;
        }
        rcode = participant_prepared(disttxn->disttxn->txnid, disttxn->disttxn->name, disttxn->disttxn->tier,
                                     disttxn->disttxn->master);
        break;

    /* Participant master tells me (coordinator master) it has failed */
    case (CDB2_DIST__FAILED_PREPARE):
        if (!disttxn->disttxn->name || !disttxn->disttxn->tier || !disttxn->disttxn->has_rcode ||
            !disttxn->disttxn->has_outrc || !disttxn->disttxn->errmsg) {
            logmsg(LOGMSG_ERROR, "%s: disttxn failed prepare missing name, tier, rcode or errmsg\n", __func__);
            rcode = -1;
            goto sendresponse;
        }
        rcode = participant_failed(disttxn->disttxn->txnid, disttxn->disttxn->name, disttxn->disttxn->tier,
                                   disttxn->disttxn->rcode, disttxn->disttxn->outrc, disttxn->disttxn->errmsg);
        break;

    /* Coordinator master tells me (participant master) to commit */
    case (CDB2_DIST__COMMIT):
        rcode = coordinator_committed(disttxn->disttxn->txnid);
        break;

    /* Coordinator master tells me (participant master) to abort */
    case (CDB2_DIST__ABORT):
        rcode = coordinator_aborted(disttxn->disttxn->txnid);
        break;

    /* Participant master tells me (coordinator master) it has propagated */
    case (CDB2_DIST__PROPAGATED):
        if (!disttxn->disttxn->name || !disttxn->disttxn->tier) {
            logmsg(LOGMSG_ERROR, "%s: disttxn propagated missing name or tier\n", __func__);
            rcode = -1;
            goto sendresponse;
        }
        rcode = participant_propagated(disttxn->disttxn->txnid, disttxn->disttxn->name, disttxn->disttxn->tier);
        break;
    }

sendresponse:
    if (disttxn->disttxn->async) {
        rd_hdr(-1, 0, appdata);
        return;
    }

    response.rcode = rcode;
    int len = cdb2__disttxnresponse__get_packed_size(&response);
    struct newsqlheader hdr = {0};
    hdr.type = htonl(RESPONSE_HEADER__DISTTXN_RESPONSE);
    hdr.length = htonl(len);
    uint8_t out[len];
    cdb2__disttxnresponse__pack(&response, out);
    evbuffer_add(buf, &hdr, sizeof(hdr));
    evbuffer_add(buf, out, len);
    event_base_once(appdata->base, appdata->fd, EV_WRITE, wr_dbinfo, appdata, NULL);
}

static void process_cdb2query(struct newsql_appdata_evbuffer *appdata, CDB2QUERY *query)
{
    if (!query) {
        free_newsql_appdata_evbuffer(appdata);
        return;
    }
    CDB2DISTTXN *disttxn = query->disttxn;
    CDB2DBINFO *dbinfo = query->dbinfo;

    if (!dbinfo && !disttxn) {
        appdata->query = query;
        process_query(appdata);
        return;
    }
    if (dbinfo) {
        if (dbinfo->has_want_effects && dbinfo->want_effects) {
            process_get_effects(appdata);
        } else {
            struct get_hosts_evbuffer_arg *arg = malloc(sizeof(struct get_hosts_evbuffer_arg));
            arg->base = appdata->base;
            arg->cb = process_dbinfo;
            arg->appdata = appdata;
            get_hosts_evbuffer(arg); /* -> process_dbinfo */
        }
    }
    if (disttxn) {
        process_disttxn(appdata, disttxn);
    }
    if (query)
        cdb2__query__free_unpacked(query, &pb_alloc);
}

static void add_rd_event_ssl(struct newsql_appdata_evbuffer *appdata, struct event *ev, struct timeval *t)
{
    if (ssl_data_pending(appdata->ssl_data)) {
        evtimer_once(event_get_base(ev), event_get_callback(ev), event_get_callback_arg(ev));
    } else {
        event_add(ev, t);
    }
}

static void add_rd_event_plaintext(struct newsql_appdata_evbuffer *appdata, struct event *ev, struct timeval *t)
{
    event_add(ev, t);
}

static struct timeval ms_to_timeval(int ms)
{
    struct timeval t = {0};
    if (ms >= 1000) {
        t.tv_sec = ms / 1000;
        ms %= 1000;
    }
    t.tv_usec = ms * 1000;
    return t;
}

static void add_rd_event(struct newsql_appdata_evbuffer *appdata, struct event *ev)
{
    struct timeval *timeout = NULL;
    struct timeval new_connection_grace;
    if (appdata->allow_lru) { /* going to wait for read; eligible for shutdown */
        add_lru_evbuffer(&appdata->clnt);
    } else if (gbl_new_connection_grace_ms) { /* new connection */
        new_connection_grace = ms_to_timeval(gbl_new_connection_grace_ms);
        timeout = &new_connection_grace;
    }
    appdata->add_rd_event_fn(appdata, ev, timeout); /* add_rd_event_plaintext */
}

static int enable_ssl_evbuffer(struct newsql_appdata_evbuffer *appdata)
{
    int rc = verify_ssl_evbuffer(appdata->ssl_data, gbl_client_ssl_mode);
    /* Enable SSL even if verification failed so we can return error */
    appdata->wr_dbinfo_fn = wr_dbinfo_ssl;
    appdata->rd_evbuffer_fn = rd_evbuffer_ciphertext;
    appdata->add_rd_event_fn = add_rd_event_ssl;
    sql_enable_ssl(appdata->writer, appdata->ssl_data);
    ssl_set_clnt_user(&appdata->clnt);
    return rc;
}

static void disable_ssl_evbuffer(struct newsql_appdata_evbuffer *appdata)
{
    appdata->wr_dbinfo_fn = wr_dbinfo_plaintext;
    appdata->rd_evbuffer_fn = rd_evbuffer_plaintext;
    appdata->add_rd_event_fn = add_rd_event_plaintext;
    sql_disable_ssl(appdata->writer);
    if (appdata->ssl_data) {
        ssl_data_free(appdata->ssl_data);
        appdata->ssl_data = NULL;
    }
}

static int rd_evbuffer_ciphertext(struct newsql_appdata_evbuffer *appdata)
{
    int eof;
    int rc = rd_ssl_evbuffer(appdata->rd_buf, appdata->ssl_data, &eof);
    if (eof) {
        disable_ssl_evbuffer(appdata);
    }
    return rc;
}

static int rd_evbuffer_plaintext(struct newsql_appdata_evbuffer *appdata)
{
    return evbuffer_read(appdata->rd_buf, appdata->fd, -1);
}

static void newsql_accept_ssl_error(void *data)
{
    struct newsql_appdata_evbuffer *appdata = data;
    write_response(&appdata->clnt, RESPONSE_ERROR, "Client certificate authentication failed", CDB2ERR_CONNECT_ERROR);
    free_newsql_appdata_evbuffer(appdata);
}

static void newsql_accept_ssl_success(void *data)
{
    struct newsql_appdata_evbuffer *appdata = data;
    if (enable_ssl_evbuffer(appdata) == 0) {
        evtimer_once(appdata->base, rd_hdr, appdata);
    } else {
        newsql_accept_ssl_error(appdata);
    }
}

static void process_ssl_request(struct newsql_appdata_evbuffer *appdata)
{
    struct sqlclntstate *clnt = &appdata->clnt;
    if (ssl_data_has_ssl(appdata->ssl_data)) {
        logmsg(LOGMSG_ERROR,
               "%s fd:%d already have ssl clnt:%s pid:%d host:%d\n", __func__,
               appdata->fd, clnt->argv0, clnt->conninfo.pid,
               clnt->conninfo.node);
        goto cleanup;
    }
    int rc;
    char ssl_response = SSL_IS_ABLE(gbl_client_ssl_mode) ? 'Y' : 'N';
    if ((rc = write(appdata->fd, &ssl_response, 1)) != 1) {
        logmsg(LOGMSG_ERROR, "%s write fd:%d ssl_response:%c rc:%d err:%s\n",
               __func__, appdata->fd, ssl_response, rc, strerror(errno));
        goto cleanup;
    }
    if (ssl_response == 'N') {
        evtimer_once(appdata->base, rd_hdr, appdata);
        return;
    }
    appdata->ssl_data = ssl_data_new(appdata->fd, clnt->origin);
    accept_ssl_evbuffer(appdata->ssl_data, appdata->base, newsql_accept_ssl_error, newsql_accept_ssl_success, appdata);
    return;

cleanup:
    free_newsql_appdata_evbuffer(appdata);
}

static const char *header_type_str(int type)
{
    switch (type) {
    case CDB2_REQUEST_TYPE__CDB2QUERY:
        return "cdb2query";
    case CDB2_REQUEST_TYPE__RESET:
        return "reset";
    case CDB2_REQUEST_TYPE__SSLCONN:
        return "sslconn";
    default:
        return "???";
    }
}

static void process_newsql_payload(struct newsql_appdata_evbuffer *appdata, CDB2QUERY *query)
{
    struct sqlclntstate *clnt = &appdata->clnt;
    rem_lru_evbuffer(clnt); /* going to work; not eligible for shutdown */
    if (clnt->evicted_appsock) { /* check after removing ourselves from lru */
        free_newsql_appdata_evbuffer(appdata);
        return;
    }
    switch (appdata->hdr.type) {
    case CDB2_REQUEST_TYPE__CDB2QUERY:
        process_cdb2query(appdata, query);
        break;
    case CDB2_REQUEST_TYPE__RESET:
        if (clnt->admin) {
            free_newsql_appdata_evbuffer(appdata);
        } else {
            newsql_reset_evbuffer(appdata);
            evtimer_once(appdata->base, rd_hdr, appdata);
        }
        break;
    case CDB2_REQUEST_TYPE__SSLCONN:
        process_ssl_request(appdata);
        break;
    default:
        logmsg(LOGMSG_ERROR, "%s bad type:%d fd:%d\n", __func__, appdata->hdr.type, appdata->fd);
        free_newsql_appdata_evbuffer(appdata);
        break;
    }
}

static void maybe_allow_lru(struct newsql_appdata_evbuffer *appdata, int fd, short what)
{
    if (appdata->allow_lru) return;
    if (fd == -1) return; /* fd == -1 -> evtimer_once, not from timeout we set */
    if (what & EV_READ) return;
    if (!(what & EV_TIMEOUT)) return;
    appdata->allow_lru = 1;
}

static void rd_payload(int fd, short what, void *arg)
{
    CDB2QUERY *query = NULL;
    struct newsql_appdata_evbuffer *appdata = arg;
    if (evbuffer_get_length(appdata->rd_buf) >= appdata->hdr.length) {
        goto payload;
    }
    if (rd_evbuffer(appdata) <= 0 && (what & EV_READ)) {
        free_newsql_appdata_evbuffer(appdata);
        return;
    }
    maybe_allow_lru(appdata, fd, what);
    if (evbuffer_get_length(appdata->rd_buf) < appdata->hdr.length) {
        add_rd_event(appdata, appdata->rd_payload_ev);
        return;
    }
payload:
    if (appdata->hdr.length) {
        int len = appdata->hdr.length;
        void *data = evbuffer_pullup(appdata->rd_buf, len);
        if (data == NULL || (query = cdb2__query__unpack(&pb_alloc, len, data)) == NULL) {
            free_newsql_appdata_evbuffer(appdata);
            return;
        }
        evbuffer_drain(appdata->rd_buf, len);
    }
    process_newsql_payload(appdata, query);
}

static void rd_hdr(int fd, short what, void *arg)
{
    struct newsqlheader hdr;
    struct newsql_appdata_evbuffer *appdata = arg;
    if (evbuffer_get_length(appdata->rd_buf) >= sizeof(struct newsqlheader)) {
        goto hdr;
    }
    if (rd_evbuffer(appdata) <= 0 && (what & EV_READ)) {
        free_newsql_appdata_evbuffer(appdata);
        return;
    }
    maybe_allow_lru(appdata, fd, what);
    if (evbuffer_get_length(appdata->rd_buf) < sizeof(struct newsqlheader)) {
        add_rd_event(appdata, appdata->rd_hdr_ev);
        return;
    }
hdr:
    evbuffer_remove(appdata->rd_buf, &hdr, sizeof(struct newsqlheader));
    appdata->hdr.type = ntohl(hdr.type);
    appdata->hdr.compression = ntohl(hdr.compression);
    appdata->hdr.state = ntohl(hdr.state);
    appdata->hdr.length = ntohl(hdr.length);
    rd_payload(-1, 0, appdata);
}

static void *newsql_destroy_stmt_evbuffer(struct sqlclntstate *clnt, void *arg)
{
    struct newsql_stmt *stmt = arg;
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    if (appdata->query == stmt->query) {
        appdata->query = NULL;
    }
    cdb2__query__free_unpacked(stmt->query, &pb_alloc);
    stmt->query = NULL;
    free(stmt);
    return NULL;
}

static int newsql_close_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    free_newsql_appdata_evbuffer(appdata);
    return 0;
}

/* read interactive cmds for debugging a stored procedure */
static int newsql_read_evbuffer(struct sqlclntstate *clnt, void *b, int l, int n)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    int have, need = l * n;
    while ((have = evbuffer_get_length(appdata->rd_buf)) < need) {
        struct pollfd pfd;
        pfd.fd = appdata->fd;
        pfd.events = POLLIN;
        int rc = poll(&pfd, 1, -1);
        if (rc != 1) {
            break;
        }
        rc = rd_evbuffer(appdata);
        if (rc <= 0) {
            break;
        }
    }
    if (need > have) {
        need = 0;
    }
    evbuffer_remove(appdata->rd_buf, b, need);
    return need / l;
}

static int newsql_pack_hb(struct sqlwriter *writer, void *arg)
{
    size_t len = sizeof(struct newsqlheader);
    struct sqlclntstate *clnt = arg;
    int state;
    uint8_t *out;
    struct iovec v[1];

    if (evbuffer_reserve_space(sql_wrbuf(writer), len, v, 1) == -1)
        return -1;
    v[0].iov_len = len;
    out = v[0].iov_base;

    if (is_pingpong(clnt))
        state = 1;
    else {
        state = (clnt->sqltick > clnt->sqltick_last_seen);
        clnt->sqltick_last_seen = clnt->sqltick;
    }
    struct newsqlheader *h = (struct newsqlheader *)out;
    memset(h, 0, sizeof(struct newsqlheader));
    h->type = htonl(RESPONSE_HEADER__SQL_RESPONSE_HEARTBEAT);
    h->state = htonl(state);

    return evbuffer_commit_space(sql_wrbuf(writer), v, 1);
}

struct newsql_pack_arg {
    struct newsql_appdata_evbuffer *appdata;
    int resp_len;
    const CDB2SQLRESPONSE *resp;
    struct newsqlheader *hdr;
};

static int newsql_pack_small(struct sqlwriter *writer, struct newsql_pack_arg *arg)
{
    const CDB2SQLRESPONSE *resp = arg->resp;
    struct newsqlheader *hdr = arg->hdr;
    struct evbuffer *wrbuf = sql_wrbuf(writer);
    int len = arg->resp_len;
    if (hdr) len += sizeof(*hdr);
    struct iovec v[1];
    if (evbuffer_reserve_space(wrbuf, len, v, 1) == -1) return -1;
    v[0].iov_len = len;
    uint8_t *out = v[0].iov_base;
    if (hdr) {
        memcpy(out, hdr, sizeof(*hdr));
        out += sizeof(*hdr);
    }
    if (resp) cdb2__sqlresponse__pack(resp, out);
    evbuffer_commit_space(wrbuf, v, 1);
    return resp ? resp->response_type == RESPONSE_TYPE__LAST_ROW : 0;
}

struct pb_evbuffer_appender {
    ProtobufCBuffer vbuf;
    struct sqlwriter *sqlwriter;
    int rc;
};

static void pb_evbuffer_append(ProtobufCBuffer *vbuf, size_t len, const uint8_t *data)
{
    struct pb_evbuffer_appender *appender = (struct pb_evbuffer_appender *)vbuf;
    appender->rc |= sql_append_packed(appender->sqlwriter, data, len);
}

#define PB_EVBUFFER_APPENDER_INIT(x)                                                                                   \
    {                                                                                                                  \
        .vbuf = {.append = pb_evbuffer_append}, .sqlwriter = x, .rc = 0                                                \
    }

static int newsql_pack(struct sqlwriter *sqlwriter, void *data)
{
    struct newsql_pack_arg *arg = data;
    if (arg->resp && arg->resp->response_type != RESPONSE_TYPE__RAW_DATA && (arg->resp_len <= SQLWRITER_MAX_BUF || arg->appdata->clnt.query_timeout)) {
        return newsql_pack_small(sqlwriter, arg);
    }
    struct pb_evbuffer_appender appender = PB_EVBUFFER_APPENDER_INIT(sqlwriter);
    if (arg->hdr) {
        pb_evbuffer_append(&appender.vbuf, sizeof(struct newsqlheader), (const uint8_t *)arg->hdr);
        if (appender.rc != 0)
            return -1;
    }
    if (arg->resp) {
        if (arg->resp->response_type == RESPONSE_TYPE__RAW_DATA) {
            pb_evbuffer_append(&appender.vbuf, sizeof(int), (uint8_t*) &arg->resp->error_code);
            if (appender.rc != 0)
                return -1;
            pb_evbuffer_append(&appender.vbuf, arg->resp->sqlite_row.len, arg->resp->sqlite_row.data);
            if (appender.rc != 0)
                return -1;
        }
        else {
            cdb2__sqlresponse__pack_to_buffer(arg->resp, &appender.vbuf);
            if (appender.rc != 0)
                return -1;
            if (arg->resp->response_type == RESPONSE_TYPE__LAST_ROW) {
                return 1;
            }
        }
    }
    return 0;
}

static int newsql_write_evbuffer(struct sqlclntstate *clnt, int type, int state,
                                 const CDB2SQLRESPONSE *resp, int flush)
{
    int response_len;

    if (resp && resp->response_type == RESPONSE_TYPE__RAW_DATA) {
        response_len = sizeof(int) + resp->sqlite_row.len;
    }
    else
        response_len = resp ? cdb2__sqlresponse__get_packed_size(resp) : 0;

    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    struct newsql_pack_arg arg = {0};
    arg.resp_len = response_len;
    struct newsqlheader hdr;
    if (type) {
        hdr.type = htonl(type);
        hdr.compression = 0;
        hdr.state = htonl(state);
        hdr.length = htonl(arg.resp_len);
        arg.hdr = &hdr;
    }
    arg.appdata = appdata;
    arg.resp = resp;
    return sql_write(appdata->writer, &arg, flush);
}

static int newsql_write_hdr_evbuffer(struct sqlclntstate *clnt, int h, int state)
{
    return newsql_write_evbuffer(clnt, h, state, 0, 1);
}

static int newsql_write_postponed_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    struct iovec v[2];

    v[0].iov_base = (char *)&appdata->postponed->hdr;
    v[0].iov_len = sizeof(struct newsqlheader);

    v[1].iov_base = (char *)appdata->postponed->row;
    v[1].iov_len = appdata->postponed->len;

    return sql_writev(appdata->writer, v, 2);
}

static int newsql_write_dbinfo_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    struct get_hosts_evbuffer_arg arg;
    arg.appdata = appdata;
    get_hosts_evbuffer_inline(&arg);
    process_dbinfo_int(&arg);
    return sql_flush(appdata->writer);
}

static int allow_admin(int local)
{
    extern int gbl_forbid_remote_admin;
    return (local || !gbl_forbid_remote_admin);
}

static void newsql_setup_clnt_evbuffer(int fd, short what, void *data)
{
    struct appsock_handler_arg *arg = data;
    int local = 0;
    if (arg->addr.sin_addr.s_addr == gbl_myaddr.s_addr) {
        local = 1;
    } else if (arg->addr.sin_addr.s_addr == htonl(INADDR_LOOPBACK)) {
        local = 1;
    }

    int admin = arg->admin;
    if (thedb->no_more_sql_connections || (gbl_server_admin_mode && !admin) || (admin && !allow_admin(local))) {
        rem_appsock_connection_evbuffer();
        evbuffer_free(arg->rd_buf);
        shutdown(arg->fd, SHUT_RDWR);
        Close(arg->fd);
        free_appsock_handler_arg(arg);
        return;
    }

    struct newsql_appdata_evbuffer *appdata = calloc(1, sizeof(*appdata));
    struct sqlclntstate *clnt = &appdata->clnt;

    reset_clnt(clnt, 1);
    char *origin = arg->origin;
    clnt->origin = origin ? origin : intern("???");
    clnt->addr = arg->addr;
    if (arg->badrte)
        logmsg(LOGMSG_ERROR, "misused rte from host %s\n", clnt->origin);
    clnt->appdata = appdata;
    clnt->done_cb = newsql_done_cb;

    newsql_setup_clnt(clnt);
    plugin_set_callbacks_newsql(evbuffer);

    clnt->admin = admin;
    clnt->force_readonly = arg->is_readonly;
    clnt->secure = arg->secure;
    appdata->thd = pthread_self();
    appdata->base = arg->base;
    appdata->initial = 1;
    appdata->local = local;
    appdata->fd = arg->fd;
    appdata->rd_buf = arg->rd_buf;
    appdata->rd_hdr_ev = event_new(appdata->base, arg->fd, EV_READ, rd_hdr, appdata);
    appdata->rd_payload_ev = event_new(appdata->base, arg->fd, EV_READ, rd_payload, appdata);
    struct sqlwriter_arg sqlwriter_arg = {
        .fd = arg->fd,
        .clnt = clnt,
        .pack = newsql_pack,
        .pack_hb = newsql_pack_hb,
        .timer_base = appdata->base,
    };
    free_appsock_handler_arg(arg);
    arg = NULL;
    appdata->writer = sqlwriter_new(&sqlwriter_arg);
    disable_ssl_evbuffer(appdata);
    add_sql_evbuffer(clnt);
    init_lru_evbuffer(clnt);
    rd_hdr(-1, 0, appdata);
}

static pthread_t gethostname_thd;
static pthread_cond_t gethostname_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t gethostname_lk = PTHREAD_MUTEX_INITIALIZER;
static int gethostname_ctr = 0;
static TAILQ_HEAD(, appsock_handler_arg) gethostname_list = TAILQ_HEAD_INITIALIZER(gethostname_list);

static void *gethostname_fn(void *arg)
{
    int max_pending = 8;
    struct timeval start, last_report;
    gettimeofday(&last_report, NULL);
    comdb2_name_thread("gethostname");
    while (1) {
        Pthread_mutex_lock(&gethostname_lk);
        if (TAILQ_EMPTY(&gethostname_list)) {
            Pthread_cond_wait(&gethostname_cond, &gethostname_lk);
        }
        struct appsock_handler_arg *arg = TAILQ_FIRST(&gethostname_list);
        TAILQ_REMOVE(&gethostname_list, arg, entry);
        int pending = --gethostname_ctr;
        Pthread_mutex_unlock(&gethostname_lk);
        gettimeofday(&start, NULL);
        arg->origin = get_cached_hostname_by_addr(&arg->addr);
        struct timeval now, diff, q;
        gettimeofday(&now, NULL);
        timersub(&now, &start, &diff);
        timersub(&now, &arg->start, &q);
        if (diff.tv_sec) {
            last_report = now;
            printf("%s  took %lds:%ldms  pending:%d\n", __func__, diff.tv_sec, diff.tv_usec / 1000L, pending);
        } else if (q.tv_sec > 2) {
            last_report = now;
            printf("%s  took %lds:%ldms  queue:%lds:%ldms  pending:%d\n", __func__,
                    diff.tv_sec, diff.tv_usec / 1000L, q.tv_sec, q.tv_usec / 1000L, pending);
        } else if (pending > max_pending) {
            last_report = now;
            max_pending = pending;
            printf("%s  pending:%d\n", __func__, pending);
        } else if (pending > 32) {
            timersub(&now, &last_report, &diff);
            if (diff.tv_sec > 10) {
                last_report = now;
                printf("%s  pending:%d\n", __func__, pending);
            }
        }
        evtimer_once(arg->base, newsql_setup_clnt_evbuffer, arg);
    }
}

static void gethostname_enqueue(int fd, short what, void *data)
{
    struct appsock_handler_arg *arg = data;
    gettimeofday(&arg->start, NULL);
    Pthread_mutex_lock(&gethostname_lk);
    TAILQ_INSERT_TAIL(&gethostname_list, arg, entry);
    ++gethostname_ctr;
    Pthread_cond_signal(&gethostname_cond); /* -> gethostname_fn */
    Pthread_mutex_unlock(&gethostname_lk);
}

static int newsql_init(void *arg)
{
    dispatch_base = get_dispatch_event_base();
    Pthread_create(&gethostname_thd, NULL, gethostname_fn, NULL);
    add_appsock_handler("newsql\n", gethostname_enqueue);
    add_appsock_handler("@newsql\n", gethostname_enqueue);
    return 0;
}

int newsql_is_newsql(struct sqlclntstate *clnt)
{
    return clnt && clnt->plugin.close == newsql_close_evbuffer;
}

comdb2_appsock_t newsql_plugin = {
    "newsql",             /* Name */
    "",                   /* Usage info */
    0,                    /* Execution count */
    0,                    /* Flags */
    NULL                  /* Handler function */
};

#include "plugin.h"
