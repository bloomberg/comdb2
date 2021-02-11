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

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <event2/buffer.h>
#include <event2/event.h>

#include <akbuf.h>
#include <bdb_api.h>
#include <hostname_support.h>
#include <intern_strings.h>
#include <net_appsock.h>
#include <net_int.h>
#include <rtcpu.h>
#include <sql.h>
#include <sqlwriter.h>
#include <str0.h>

#include <newsql.h>

static void read_newsql_hdr(int, short, void *);

struct newsql_appdata_evbuffer {
    NEWSQL_APPDATA_COMMON /* Must be first */

    int fd;
    struct sqlclntstate clnt;
    struct newsqlheader hdr;
    struct event *ping_ev;
    int ping_status;

    struct evbuffer *rd_buf;
    struct event *rd_hdr_ev;
    struct event *rd_payload_ev;
    unsigned first_run : 1;
    unsigned local : 1;

    struct sqlwriter *writer;
};

static void free_newsql_appdata_evbuffer(int fd, short what, void *arg)
{
    struct newsql_appdata_evbuffer *appdata = arg;
    printf("%s:closing fd:%d\n", __func__, appdata->fd);
    if (appdata->ping_ev) {
        event_free(appdata->ping_ev);
        appdata->ping_ev = NULL;
    }
    if (appdata->rd_hdr_ev) {
        event_free(appdata->rd_hdr_ev);
        appdata->rd_hdr_ev = NULL;
    }
    if (appdata->rd_payload_ev) {
        event_free(appdata->rd_payload_ev);
        appdata->rd_payload_ev = NULL;
    }
    if (appdata->rd_buf) {
        evbuffer_free(appdata->rd_buf);
        appdata->rd_buf = NULL;
    }
    sqlwriter_free(appdata->writer);
    shutdown(appdata->fd, SHUT_RDWR);
    close(appdata->fd);
    free_newsql_appdata(&appdata->clnt);
}

static void newsql_cleanup(int fd, short what, void *arg)
{
    if (!pthread_equal(pthread_self(), appsock_timer_thd)) {
        abort();
    }
    struct newsql_appdata_evbuffer *appdata = arg;
    sql_disable_heartbeat(appdata->writer);
    sql_disable_timeout(appdata->writer);
    event_once(appsock_rd_base, free_newsql_appdata_evbuffer, arg);
}

static int newsql_flush_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return sql_flush(appdata->writer);
}

static void newsql_read_again(int fd, short what, void *arg)
{
    struct newsql_appdata_evbuffer *appdata = arg;
    sql_disable_heartbeat(appdata->writer);
    sql_disable_timeout(appdata->writer);
    event_once(appsock_rd_base, read_newsql_hdr, appdata);
}

static int newsql_done(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    if (clnt->query_rc == CDB2ERR_IO_ERROR) { /* dispatch timed out */
        if (clnt->osql.replay == OSQL_RETRY_DO) {
            osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);
            srs_tran_destroy(clnt);
        }
        return event_once(appsock_timer_base, newsql_cleanup, appdata);
    }
    if (clnt->query_rc && clnt->query_rc != -103 && clnt->has_recording) {
        abort();
    }
    if (clnt->osql.replay == OSQL_RETRY_DO) {
        if (clnt->dbtran.trans_has_sp) {
            osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);
            srs_tran_destroy(clnt);
        } else {
            clnt->done_cb = NULL;
            srs_tran_replay_inline(clnt);
            clnt->done_cb = newsql_done;
        }
        appdata->query = NULL;
    } else if (clnt->osql.history && clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS) {
        srs_tran_destroy(clnt);
        appdata->query = NULL;
    }
    clnt->sql = NULL;
    if (appdata->query) {
        cdb2__query__free_unpacked(appdata->query, NULL);
    }
    event_callback_fn fn = sql_done(appdata->writer) ? newsql_cleanup : newsql_read_again;
    return event_once(appsock_timer_base, fn, appdata);
}

static int newsql_get_fileno_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return appdata->fd;
}

static int newsql_get_x509_attr_evbuffer(struct sqlclntstate *clnt, int nid, void *out, int outsz)
{
    abort();
    return -1;
}

static int newsql_has_ssl_evbuffer(struct sqlclntstate *clnt)
{
    return 0;
}

static int newsql_has_x509_evbuffer(struct sqlclntstate *clnt)
{
    return 0;
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

static int newsql_set_timeout_evbuffer(struct sqlclntstate *clnt, int timeout_ms)
{
    /* nop */
    return 0;
}

static void pong(int fd, short what, void *arg)
{
    struct newsql_appdata_evbuffer *appdata = arg;
    struct event_base *wrbase = sql_wrbase(appdata->writer);
    if (what & EV_TIMEOUT) {
        event_base_loopbreak(wrbase);
        return;
    }
    if (evbuffer_read(appdata->rd_buf, appdata->fd, -1) <= 0) {
        appdata->ping_status = -2;
        event_base_loopbreak(wrbase);
        return;
    }
    struct newsqlheader hdr;
    if (evbuffer_get_length(appdata->rd_buf) < sizeof(hdr)) {
        return;
    }
    evbuffer_remove(appdata->rd_buf, &hdr, sizeof(hdr));
    if (ntohl(hdr.type) == RESPONSE_HEADER__SQL_RESPONSE_PONG) {
        appdata->ping_status = 0;
    } else {
        appdata->ping_status = -3;
    }
    event_base_loopbreak(wrbase);
}

static int newsql_ping_pong_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    struct event_base *wrbase = sql_wrbase(appdata->writer);
    if (!appdata->ping_ev) {
        int flags = EV_READ | EV_PERSIST | EV_TIMEOUT;
        appdata->ping_ev = event_new(wrbase, appdata->fd, flags, pong, appdata);
    }
    appdata->ping_status = -1;
    struct timeval onesec = {.tv_sec = 1};
    event_add(appdata->ping_ev, &onesec);
    event_base_dispatch(wrbase);
    event_del(appdata->ping_ev);
    return appdata->ping_status;
}

static int newsql_maxquerytime_fn(void *arg)
{
    struct sqlclntstate *clnt = arg;
    return write_response(clnt, RESPONSE_ERROR, ERROR_LIMIT, SQLHERR_LIMIT);
}

static void write_dbinfo(int fd, short what, void *arg)
{
    if (!pthread_equal(pthread_self(), appsock_timer_thd)) {
        abort();
    }
    struct newsql_appdata_evbuffer *appdata = arg;
    struct evbuffer *wrbuf = sql_wrbuf(appdata->writer);
    if (evbuffer_write(wrbuf, appdata->fd) <= 0) {
        newsql_cleanup(-1, 0, appdata);
        return;
    }
    if (evbuffer_get_length(wrbuf)) {
        event_base_once(appsock_timer_base, appdata->fd, EV_WRITE, write_dbinfo, appdata, NULL);
    } else {
        event_once(appsock_rd_base, read_newsql_hdr, appdata);
    }
}

static void process_dbinfo(struct newsql_appdata_evbuffer *appdata)
{
    CDB2DBINFORESPONSE__Nodeinfo *master = NULL;
    CDB2DBINFORESPONSE__Nodeinfo *nodes[REPMAX];
    CDB2DBINFORESPONSE__Nodeinfo same_dc[REPMAX], diff_dc[REPMAX];
    int num_same_dc = 0, num_diff_dc = 0;
    host_node_type *hosts[REPMAX];
    int num_hosts = get_hosts_evbuffer(REPMAX, hosts);
    int my_dc = machine_dc(gbl_myhostname);
    int process_incoherent = bdb_amimaster(thedb->bdb_env);
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
        node->incoherent = process_incoherent ? is_incoherent(thedb->bdb_env, node->name) : 0;
        const char *who = bdb_whoismaster(thedb->bdb_env);
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

    // TODO: fill_sslinfo
    CDB2DBINFORESPONSE response = CDB2__DBINFORESPONSE__INIT;
    response.n_nodes = num_hosts;
    response.master = master;
    response.nodes = nodes;

    int len = cdb2__dbinforesponse__get_packed_size(&response);
    size_t sz = sizeof(struct newsqlheader) + len;
    struct iovec v[1];
    struct evbuffer *wrbuf = sql_wrbuf(appdata->writer);
    evbuffer_reserve_space(wrbuf, sz, v, 1);
    uint8_t *b = v[0].iov_base;
    struct newsqlheader *hdr = (struct newsqlheader *)b;
    hdr->type = htonl(RESPONSE_HEADER__DBINFO_RESPONSE);
    hdr->length = htonl(len);
    b += sizeof(struct newsqlheader);
    cdb2__dbinforesponse__pack(&response, b);
    v[0].iov_len = sz;
    evbuffer_commit_space(wrbuf, v, 1);
    event_base_once(appsock_timer_base, appdata->fd, EV_WRITE, write_dbinfo, appdata, NULL);
}

static void process_get_effects(struct newsql_appdata_evbuffer *appdata)
{
    CDB2EFFECTS effects = CDB2__EFFECTS__INIT;
    CDB2SQLRESPONSE response = CDB2__SQLRESPONSE__INIT;
    newsql_effects(&response, &effects, &appdata->clnt);

    int len = cdb2__sqlresponse__get_packed_size(&response);
    size_t sz = sizeof(struct newsqlheader) + len;
    struct iovec v[1];
    struct evbuffer *wrbuf = sql_wrbuf(appdata->writer);
    evbuffer_reserve_space(wrbuf, sz, v, 1);
    uint8_t *b = v[0].iov_base;
    struct newsqlheader *hdr = (struct newsqlheader *)b;
    hdr->type = htonl(RESPONSE_HEADER__SQL_EFFECTS);
    hdr->length = htonl(len);
    b += sizeof(struct newsqlheader);
    cdb2__sqlresponse__pack(&response, b);
    v[0].iov_len = sz;
    evbuffer_commit_space(wrbuf, v, 1);
    event_base_once(appsock_timer_base, appdata->fd, EV_WRITE, write_dbinfo, appdata, NULL);
}

static void process_query(struct newsql_appdata_evbuffer *appdata, CDB2QUERY *query)
{
    int commit_rollback;
    appdata->query = query;
    appdata->sqlquery = query->sqlquery;
    struct sqlclntstate *clnt = &appdata->clnt;
    if (appdata->first_run) {
        appdata->first_run = 0;
        if (newsql_first_run(clnt, query->sqlquery) != 0) {
            goto err;
        }
    }
    if (newsql_loop(clnt, query->sqlquery) != 0) {
        goto err;
    }
    if (newsql_should_dispatch(clnt, &commit_rollback) != 0) {
        cdb2__query__free_unpacked(query, NULL);
        read_newsql_hdr(appdata->fd, 0, appdata);
        return;
    }
    sql_reset(appdata->writer);
    if (clnt->query_timeout) {
        sql_enable_timeout(appdata->writer, clnt->query_timeout, newsql_maxquerytime_fn, clnt);
    }
    if (dispatch_sql_query_no_wait(clnt) != 0) {
        goto err;
    }
    sql_enable_heartbeat(appdata->writer);
    return;
err:event_once(appsock_timer_base, newsql_cleanup, appdata);
}

static void process_cdb2query(struct newsql_appdata_evbuffer *appdata, CDB2QUERY *query)
{
    CDB2DBINFO *dbinfo = query->dbinfo;
    if (!dbinfo) {
        process_query(appdata, query);
        return;
    }
    if (dbinfo->has_want_effects && dbinfo->want_effects) {
        process_get_effects(appdata);
    } else {
        process_dbinfo(appdata);
    }
    cdb2__query__free_unpacked(query, NULL);
}

static void process_newsql_payload(struct newsql_appdata_evbuffer *appdata, CDB2QUERY *query)
{
    switch (appdata->hdr.type) {
    case CDB2_REQUEST_TYPE__CDB2QUERY:
        process_cdb2query(appdata, query);
        break;
    case CDB2_REQUEST_TYPE__RESET:
        appdata->first_run = 1;
        newsql_reset(&appdata->clnt);
        read_newsql_hdr(appdata->fd, 0, appdata);
        break;
    case CDB2_REQUEST_TYPE__SSLCONN:
        /* not implemented - disable us for now */
        gbl_libevent_appsock = 0;
        event_once(appsock_timer_base, newsql_cleanup, appdata);
        break;
    default:
        logmsg(LOGMSG_ERROR, "%s bad type:%d fd:%d\n", __func__, appdata->hdr.type, appdata->fd);
        abort();
    }
}

static void read_newsql_payload(int fd, short what, void *arg)
{
    struct newsql_appdata_evbuffer *appdata = arg;
    if (what & EV_READ) {
        if (evbuffer_read(appdata->rd_buf, appdata->fd, -1) <= 0) {
            event_once(appsock_timer_base, newsql_cleanup, appdata);
            return;
        }
    }
    if (evbuffer_get_length(appdata->rd_buf) < appdata->hdr.length) {
        event_add(appdata->rd_payload_ev, NULL);
        return;
    }
    CDB2QUERY *query = NULL;
    int len = appdata->hdr.length;
    if (len) {
        void *data = evbuffer_pullup(appdata->rd_buf, len);
        if ((query = cdb2__query__unpack(NULL, len, data)) == NULL) {
            event_once(appsock_timer_base, newsql_cleanup, appdata);
            return;
        }
        evbuffer_drain(appdata->rd_buf, len);
    }
    process_newsql_payload(appdata, query);
}

static void read_newsql_hdr(int fd, short what, void *arg)
{
    if (!pthread_equal(pthread_self(), appsock_rd_thd)) {
        abort();
    }
    struct newsql_appdata_evbuffer *appdata = arg;
    if (what & EV_READ) {
        if (evbuffer_read(appdata->rd_buf, appdata->fd, -1) <= 0) {
            event_once(appsock_timer_base, newsql_cleanup, appdata);
            return;
        }
    }
    size_t len = evbuffer_get_length(appdata->rd_buf);
    if (len < sizeof(struct newsqlheader)) {
        event_add(appdata->rd_hdr_ev, NULL);
        return;
    }
    evbuffer_remove(appdata->rd_buf, &appdata->hdr, sizeof(struct newsqlheader));
    appdata->hdr.type = ntohl(appdata->hdr.type);
    appdata->hdr.compression = ntohl(appdata->hdr.compression);
    appdata->hdr.state = ntohl(appdata->hdr.state);
    appdata->hdr.length = ntohl(appdata->hdr.length);
    read_newsql_payload(appdata->fd, 0, appdata);
}

static void *newsql_destroy_stmt_evbuffer(struct sqlclntstate *clnt, void *arg)
{
    struct newsql_stmt *stmt = arg;
    cdb2__query__free_unpacked(stmt->query, NULL);
    free(stmt);
    return NULL;
}

static int newsql_close_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return shutdown(appdata->fd, SHUT_RDWR);
}

static int newsql_read_evbuffer(struct sqlclntstate *clnt, void *b, int l, int n)
{
    printf("************* TODO:%s ***************\n", __func__);
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return read(appdata->fd, b, l * n) == l * n;
}

static int newsql_pack_hb(uint8_t *out, void *arg)
{
    struct sqlclntstate *clnt = arg;
    int state;
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
    return 0;
}

struct newsql_pack_arg {
    struct newsqlheader *hdr;
    const CDB2SQLRESPONSE *resp;
};

static int newsql_pack(uint8_t *out, void *data)
{
    struct newsql_pack_arg *arg = data;
    if (arg->hdr) {
        memcpy(out, arg->hdr, sizeof(struct newsqlheader));
        out += sizeof(struct newsqlheader);
    }
    if (arg->resp) {
        cdb2__sqlresponse__pack(arg->resp, out);
        if (arg->resp->response_type == RESPONSE_TYPE__LAST_ROW) {
            return 1;
        }
    }
    return 0;
}

static int newsql_write_evbuffer(struct sqlclntstate *clnt, int type, int state,
                                 const CDB2SQLRESPONSE *resp, int flush)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    int hdr_len = type ? sizeof(struct newsqlheader) : 0;
    int response_len = resp ? cdb2__sqlresponse__get_packed_size(resp) : 0;
    int total_len = hdr_len + response_len;
    struct newsql_pack_arg arg = {0};
    struct newsqlheader hdr;
    if (type) {
        hdr.type = htonl(type);
        hdr.compression = 0;
        hdr.state = htonl(state);
        hdr.length = htonl(response_len);
        arg.hdr = &hdr;
    }
    arg.resp = resp;
    return sql_write(appdata->writer, total_len, newsql_pack, &arg, flush);

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
    process_dbinfo(clnt->appdata);
    return 0;
}

static struct newsql_appdata_evbuffer *
newsql_setup_clnt_evbuffer(struct evbuffer *rd_buf, int fd, struct sockaddr_in *addr)
{
    if (thedb->no_more_sql_connections) {
        evbuffer_free(rd_buf);
        shutdown(fd, SHUT_RDWR);
        close(fd);
        return NULL;
    }

    struct newsql_appdata_evbuffer *appdata = calloc(1, sizeof(*appdata));
    struct sqlclntstate *clnt = &appdata->clnt;

    reset_clnt(clnt, 1);
    char *origin = get_hostname_by_fileno(fd);
    clnt->origin = origin ? origin : intern("???");
    clnt->appdata = appdata;
    clnt->done_cb = newsql_done;

    newsql_setup_clnt(clnt);
    plugin_set_callbacks_newsql(evbuffer);

    update_col_info(&appdata->col_info, 32);
    if (addr->sin_addr.s_addr == gbl_myaddr.s_addr) {
        appdata->local = 1;
    } else if (addr->sin_addr.s_addr == htonl(INADDR_LOOPBACK)) {
        appdata->local = 1;
    }
    appdata->first_run = 1;
    appdata->fd = fd;
    appdata->rd_buf = rd_buf;
    appdata->rd_hdr_ev = event_new(appsock_rd_base, fd, EV_READ, read_newsql_hdr, appdata);
    appdata->rd_payload_ev = event_new(appsock_rd_base, fd, EV_READ, read_newsql_payload, appdata);
    appdata->writer = sqlwriter_new(&appdata->clnt, fd, newsql_pack_hb, sizeof(struct newsqlheader));
    return appdata;
}

static void handle_newsql_request_evbuffer(struct evbuffer *rd_buf, int fd,
                                           struct sockaddr_in *addr)
{
    struct newsql_appdata_evbuffer *appdata;
    if ((appdata = newsql_setup_clnt_evbuffer(rd_buf, fd, addr)) != 0) {
        event_once(appsock_rd_base, read_newsql_hdr, appdata);
    }
}

static void handle_newsql_admin_request_evbuffer(struct evbuffer *rd_buf,
                                                 int fd, struct sockaddr_in *addr)
{
    struct newsql_appdata_evbuffer *appdata;
    appdata = newsql_setup_clnt_evbuffer(rd_buf, fd, addr);
    if (appdata && appdata->local) {
        appdata->clnt.admin = 1;
        event_once(appsock_rd_base, read_newsql_hdr, appdata);
    } else if (appdata) {
        event_once(appsock_timer_base, newsql_cleanup, appdata);
    }
}

void setup_newsql_evbuffer_handlers(void)
{
    add_appsock_handler("newsql\n", handle_newsql_request_evbuffer);
    add_appsock_handler("@newsql\n", handle_newsql_admin_request_evbuffer);
}
