/*
   Copyright 2020 Bloomberg Finance L.P.

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

#include <inttypes.h>
#include <sys/socket.h>

#include <comdb2_appsock.h>
#include <comdb2_atomic.h>
#include <comdb2_plugin.h>
#include <intern_strings.h>
#include <net_appsock.h>
#include <pb_alloc.h>
#include <mem_protobuf.h> /* comdb2_malloc_protobuf */
#include <sql.h>
#include <ssl_glue.h>
#include <str0.h>

#include <newsql.h>

extern int gbl_sqlwrtimeoutms;

extern ssl_mode gbl_client_ssl_mode;
extern SSL_CTX *gbl_ssl_ctx;
extern char gbl_dbname[MAX_DBNAME_LENGTH];
extern int gbl_nid_dbname;
extern unsigned long long gbl_ssl_num_full_handshakes;
extern unsigned long long gbl_ssl_num_partial_handshakes;

int gbl_protobuf_prealloc_buffer_size = 8192;

struct NewsqlProtobufCAllocator {
    ProtobufCAllocator protobuf_allocator;
    uint8_t *protobuf_data;
    int protobuf_offset;
    int protobuf_size;
    int alloced_outside_buffer;
};

struct newsql_appdata_sbuf {
    NEWSQL_APPDATA_COMMON /* Must be first */
    SBUF2 *sb;
    struct NewsqlProtobufCAllocator newsql_protobuf_allocator;
};

static void newsql_protobuf_reset_offset(struct NewsqlProtobufCAllocator *npa)
{
    npa->protobuf_offset = 0;
}

static void *newsql_protobuf_alloc(void *allocator_data, size_t size)
{
    struct NewsqlProtobufCAllocator *npa = allocator_data;
    void *p = NULL;
    if (size <= npa->protobuf_size - npa->protobuf_offset) {
        p = npa->protobuf_data + npa->protobuf_offset;
        npa->protobuf_offset += size;
    } else {
        p = comdb2_malloc_protobuf(size);
        npa->alloced_outside_buffer++;
    }
    return p;
}

static void newsql_protobuf_free(void *allocator_data, void *ptr)
{
    uint8_t *p = ptr;
    struct NewsqlProtobufCAllocator *npa = allocator_data;
    if (p < npa->protobuf_data || p > (npa->protobuf_data + npa->protobuf_size)) {
        comdb2_free_protobuf(p);
        npa->alloced_outside_buffer--;
    }
}

static void newsql_protobuf_init(struct NewsqlProtobufCAllocator *npa)
{
    npa->protobuf_size = gbl_protobuf_prealloc_buffer_size;
    npa->protobuf_offset = 0;
    npa->alloced_outside_buffer = 0;
    npa->protobuf_data = comdb2_malloc_protobuf(npa->protobuf_size);
    npa->protobuf_allocator = setup_pb_allocator(newsql_protobuf_alloc, newsql_protobuf_free, npa);
}

static void newsql_protobuf_destroy(struct NewsqlProtobufCAllocator *npa)
{
    assert(npa->alloced_outside_buffer == 0);
    comdb2_free_protobuf(npa->protobuf_data);
}

static int newsql_read_sbuf(struct sqlclntstate *clnt, void *b, int l, int n)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    return sbuf2fread(b, l, n, appdata->sb);
}

struct pb_sbuf_writer {
    SBUF2 *sb;
    ProtobufCBuffer base;
    int nbytes; /* bytes written */
};

static void pb_sbuf_write(ProtobufCBuffer *base, size_t len, const uint8_t *data)
{
    struct pb_sbuf_writer *writer = container_of(base, struct pb_sbuf_writer);
    writer->nbytes += sbuf2write((char *)data, len, writer->sb);
}

#define PB_SBUF_WRITER_INIT(x) {.sb = x, .base = {.append = pb_sbuf_write}, .nbytes = 0 }

static int newsql_write_sbuf(struct sqlclntstate *clnt, int t, int s,
                             const CDB2SQLRESPONSE *r, int flush)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    struct pb_sbuf_writer writer = PB_SBUF_WRITER_INIT(appdata->sb);
    size_t len = r ? cdb2__sqlresponse__get_packed_size(r) : 0;
    struct newsqlheader hdr = {0};
    hdr.type = htonl(t);
    hdr.state = htonl(s);
    hdr.length = htonl(len);
    int rc;
    lock_client_write_lock(clnt);
    if (t && (rc = sbuf2write((char *)&hdr, sizeof(hdr), appdata->sb)) != sizeof(hdr))
        goto out;
    if (r && (rc = cdb2__sqlresponse__pack_to_buffer(r, &writer.base)) != writer.nbytes)
        goto out;
    if (flush && (rc = sbuf2flush(appdata->sb)) < 0)
        goto out;
    rc = 0;
out:unlock_client_write_lock(clnt);
    return rc;
}

static int newsql_write_hdr_sbuf(struct sqlclntstate *clnt, int h, int state)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    struct newsqlheader hdr = {0};
    hdr.type = ntohl(h);
    hdr.state = ntohl(state);
    int rc;
    lock_client_write_lock(clnt);
    if ((rc = sbuf2write((char *)&hdr, sizeof(hdr), appdata->sb)) != sizeof(hdr))
        goto out;
    if ((rc = sbuf2flush(appdata->sb)) < 0)
        goto out;
    rc = 0;
out:unlock_client_write_lock(clnt);
    return rc;
}

static int newsql_flush_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    lock_client_write_lock(clnt);
    int rc = sbuf2flush(appdata->sb);
    unlock_client_write_lock(clnt);
    return rc < 0;
}

static int newsql_write_postponed_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    char *hdr = (char *)&appdata->postponed->hdr;
    size_t hdrsz = sizeof(struct newsqlheader);
    char *row = (char *)appdata->postponed->row;
    size_t len = appdata->postponed->len;
    int rc;
    lock_client_write_lock(clnt);
    if ((rc = sbuf2write(hdr, hdrsz, appdata->sb)) != hdrsz)
        goto out;
    if ((rc = sbuf2write(row, len, appdata->sb)) != len)
        goto out;
    rc = 0;
out:unlock_client_write_lock(clnt);
    return rc;
}

static int newsql_set_timeout_sbuf(struct sqlclntstate *clnt, int wr_timeout_ms)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    return sbuf_set_timeout(clnt, appdata->sb, wr_timeout_ms);
}

static int newsql_ping_pong_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    struct newsqlheader hdr = {0};
    int rc, r, w, timeout = 0;
    sbuf2gettimeout(appdata->sb, &r, &w);
    sbuf2settimeout(appdata->sb, 1000, w);
    rc = sbuf2fread_timeout((void *)&hdr, sizeof(hdr), 1, appdata->sb, &timeout);
    sbuf2settimeout(appdata->sb, r, w);
    if (timeout) return -1;
    if (rc != 1) return -2;
    if (ntohl(hdr.type) != RESPONSE_HEADER__SQL_RESPONSE_PONG) return -3;
    return 0;
}

static int newsql_get_fileno_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    return sbuf2fileno(appdata->sb);
}

static int newsql_close_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    return shutdown(sbuf2fileno(appdata->sb), SHUT_RDWR);
}

static int newsql_local_check_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    return sbuf_is_local(appdata->sb);
}

static int newsql_peer_check_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    return peer_dropped_connection_sbuf(appdata->sb);
}

static int newsql_has_ssl_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    return sslio_has_ssl(appdata->sb);
}

static int newsql_has_x509_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    return sslio_has_x509(appdata->sb);
}

static int newsql_get_x509_attr_sbuf(struct sqlclntstate *clnt, int nid, void *out, int outsz)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    return sslio_x509_attr(appdata->sb, nid, out, outsz);
}

static int newsql_write_dbinfo_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    SBUF2 *sb = appdata->sb;
    CDB2DBINFORESPONSE *dbinfo_response = malloc(sizeof(CDB2DBINFORESPONSE));
    cdb2__dbinforesponse__init(dbinfo_response);
    fill_dbinfo(dbinfo_response, thedb->bdb_env);
    int len = cdb2__dbinforesponse__get_packed_size(dbinfo_response);
    struct pb_sbuf_writer writer = PB_SBUF_WRITER_INIT(sb);
    struct newsqlheader hdr = {0};
    hdr.type = htonl(RESPONSE_HEADER__DBINFO_RESPONSE);
    hdr.length = htonl(len);
    sbuf2write((char *)&hdr, sizeof(hdr), sb);
    cdb2__dbinforesponse__pack_to_buffer(dbinfo_response, &writer.base);
    int rc = sbuf2flush(sb) > 0 ? 0 : -1;
    cdb2__dbinforesponse__free_unpacked(dbinfo_response, &pb_alloc);
    return rc;
}

static void *newsql_destroy_stmt_sbuf(struct sqlclntstate *clnt, void *arg)
{
    struct newsql_stmt *stmt = arg;
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    if (appdata->query == stmt->query) {
        appdata->query = NULL;
    }
    cdb2__query__free_unpacked(stmt->query, &appdata->newsql_protobuf_allocator.protobuf_allocator);
    free(stmt);
    return NULL;
} 

static void newsql_setup_clnt_sbuf(struct sqlclntstate *clnt, SBUF2 *sb)
{
    struct newsql_appdata_sbuf *appdata = calloc(1, sizeof(struct newsql_appdata_sbuf));
    appdata->sb = sb;
    newsql_protobuf_init(&appdata->newsql_protobuf_allocator);

    reset_clnt(clnt, 1);
    char *origin = get_origin_mach_by_buf(sb);
    clnt->origin = origin ? origin : intern("???");
    clnt->appdata = appdata;

    newsql_setup_clnt(clnt);
    clnt_register(clnt);
    plugin_set_callbacks_newsql(sbuf);
}

int gbl_send_failed_dispatch_message = 0;

static CDB2QUERY *read_newsql_query(struct dbenv *dbenv,
                                    struct sqlclntstate *clnt, SBUF2 *sb)
{
    struct newsqlheader hdr = {0};
    CDB2QUERY *query = NULL;
    int rc;
    int pre_enabled = 0;
    int was_timeout = 0;

    /* reset here, if query has this feature,
    it will be enabled before returning */
    clnt->sqlite_row_format = 0;

retry_read:
    rc = sbuf2fread_timeout((char *)&hdr, sizeof(hdr), 1, sb, &was_timeout);
    if (rc != 1) {
        if (was_timeout && gbl_send_failed_dispatch_message) {
            handle_failed_dispatch(clnt, "Socket read timeout.");
        }
        return NULL;
    }

    hdr.type = ntohl(hdr.type);
    hdr.compression = ntohl(hdr.compression);
    hdr.length = ntohl(hdr.length);

    if (hdr.type == CDB2_REQUEST_TYPE__SSLCONN) {
        if (sslio_has_ssl(sb)) {
            logmsg(LOGMSG_WARN, "The connection is already SSL encrypted.\n");
            return NULL;
        }
        /* Flush the SSL ability byte. We need to do this because:
           1) The `require_ssl` field in dbinfo may not reflect the
              actual status of this node;
           2) Doing SSL_accept() immediately would cause too many
              unnecessary EAGAIN/EWOULDBLOCK's for non-blocking BIO. */
        char ssl_able = SSL_IS_ABLE(gbl_client_ssl_mode) ? 'Y' : 'N';
        if ((rc = sbuf2putc(sb, ssl_able)) < 0 || (rc = sbuf2flush(sb)) < 0) {
            return NULL;
        }

        if (ssl_able == 'N')
            goto retry_read;

        /* Don't close the connection if SSL verify fails so that we can
           send back an error to the client. */
        if (sslio_accept(sb, gbl_ssl_ctx, gbl_client_ssl_mode, gbl_dbname, gbl_nid_dbname, 0) != 1) {
            write_response(clnt, RESPONSE_ERROR, "Client certificate authentication failed", CDB2ERR_CONNECT_ERROR);
            /* Print the error message in the sbuf2. */
            char err[256];
            sbuf2lasterror(sb, err, sizeof(err));
            logmsg(LOGMSG_ERROR, "%s\n", err);
            return NULL;
        }

        /* keep track of number of full and partial handshakes */
        if (SSL_session_reused(sslio_get_ssl(sb)))
            ATOMIC_ADD64(gbl_ssl_num_partial_handshakes, 1);
        else
            ATOMIC_ADD64(gbl_ssl_num_full_handshakes, 1);

        /* Extract the user from the certificate. */
        ssl_set_clnt_user(clnt);
        goto retry_read;
    } else if (hdr.type == CDB2_REQUEST_TYPE__RESET) {
        struct newsql_appdata_sbuf *appdata = clnt->appdata;
        newsql_protobuf_reset_offset(&appdata->newsql_protobuf_allocator);
        newsql_reset(clnt);
        goto retry_read;
    }

    if (hdr.type > 2) {
        logmsg(LOGMSG_ERROR, "%s: Invalid message  %d\n", __func__, hdr.type);
        return NULL;
    }

    int bytes = hdr.length;
    if (bytes <= 0) {
        logmsg(LOGMSG_ERROR, "%s: Junk message  %d\n", __func__, bytes);
        return NULL;
    }

    char *p;
    if (bytes <= gbl_blob_sz_thresh_bytes)
        p = malloc(bytes);
    else
        while (1) { // big buffer. most certainly it is a huge blob.
            errno = 0; /* precondition: well-defined before call that may set */
            p = comdb2_timedmalloc(blobmem, bytes, 1000);

            if (p != NULL || errno != ETIMEDOUT)
                break;

            Pthread_mutex_lock(&clnt->wait_mutex);
            clnt->heartbeat = 1;
            if (clnt->ready_for_heartbeats == 0) {
                pre_enabled = 1;
                clnt->ready_for_heartbeats = 1;
            }
            newsql_heartbeat(clnt);
            fdb_heartbeats(clnt);
            Pthread_mutex_unlock(&clnt->wait_mutex);
        }

    if (pre_enabled) {
        Pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;
        Pthread_mutex_unlock(&clnt->wait_mutex);
        pre_enabled = 0;
    }

    if (!p) {
        logmsg(LOGMSG_ERROR, "%s: out of memory malloc %d\n", __func__, bytes);
        return NULL;
    }

    rc = sbuf2fread(p, bytes, 1, sb);
    if (rc != 1) {
        free(p);
        logmsg(LOGMSG_DEBUG, "Error in sbuf2fread rc=%d\n", rc);
        return NULL;
    }

    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    while (1) {
        errno = 0; /* precondition: well-defined before call that may set */
        query = cdb2__query__unpack(&appdata->newsql_protobuf_allocator.protobuf_allocator, bytes, (uint8_t *)p);
        // errno can be set by cdb2__query__unpack
        // we retry malloc on out of memory condition

        if (query || errno != ETIMEDOUT)
            break;

        Pthread_mutex_lock(&clnt->wait_mutex);
        if (clnt->heartbeat == 0)
            clnt->heartbeat = 1;
        if (clnt->ready_for_heartbeats == 0) {
            pre_enabled = 1;
            clnt->ready_for_heartbeats = 1;
        }
        newsql_heartbeat(clnt);
        fdb_heartbeats(clnt);
        Pthread_mutex_unlock(&clnt->wait_mutex);
    }
    free(p);

    if (pre_enabled) {
        Pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;
        Pthread_mutex_unlock(&clnt->wait_mutex);
    }

    if (!query) {
        logmsg(LOGMSG_ERROR, "%s:%d Error unpacking query error: %s\n", __func__, __LINE__, strerror(errno));
        return NULL;
    }

    // one of dbinfo or sqlquery must be non-NULL
    if (unlikely(!query->dbinfo && !query->sqlquery)) {
        cdb2__query__free_unpacked(query, &appdata->newsql_protobuf_allocator.protobuf_allocator);
        query = NULL;
        goto retry_read;
    }

    if (query->dbinfo) {
        if (query->dbinfo->has_want_effects &&
            query->dbinfo->want_effects == 1) {
            CDB2SQLRESPONSE sql_response = CDB2__SQLRESPONSE__INIT;
            CDB2EFFECTS effects = CDB2__EFFECTS__INIT;
            newsql_effects(&sql_response, &effects, clnt);
            newsql_write_sbuf(clnt, RESPONSE_HEADER__SQL_EFFECTS, 0, &sql_response, 1);
        } else {
            newsql_write_dbinfo_sbuf(clnt);
        }
        cdb2__query__free_unpacked(query, &appdata->newsql_protobuf_allocator.protobuf_allocator);
        query = NULL;
        goto retry_read;
    }

    /* Do security check before we return. We do it only after
       the query has been unpacked so that we know whether
       it is a new client (new clients have SSL feature).
       The check must be done for every query, otherwise
       attackers could bypass it by using pooled connections
       from sockpool. The overhead of the check is negligible. */
    if (SSL_IS_PREFERRED(gbl_client_ssl_mode) && !sslio_has_ssl(sb)) {
        /* The code block does 2 things:
           1. Return an error to outdated clients;
           2. Send dbinfo to new clients to trigger SSL.
              It may happen when require_ssl is first time
              enabled across the cluster. */
        int client_supports_ssl = 0;
        for (int ii = 0; ii < query->sqlquery->n_features; ++ii) {
            if (CDB2_CLIENT_FEATURES__SSL == query->sqlquery->features[ii]) {
                client_supports_ssl = 1;
                break;
            }
        }

        if (client_supports_ssl) {
            newsql_write_hdr_sbuf(clnt, RESPONSE_HEADER__SQL_RESPONSE_SSL, 0);
            cdb2__query__free_unpacked(query, &appdata->newsql_protobuf_allocator.protobuf_allocator);
            query = NULL;
            goto retry_read;
        } else if (ssl_whitelisted(clnt->origin) || (SSL_IS_OPTIONAL(gbl_client_ssl_mode))) {
            /* allow plaintext local connections, or server is configured to prefer (but not disallow) SSL clients. */
            return query;
        } else {
            write_response(clnt, RESPONSE_ERROR, "The database requires SSL connections.", CDB2ERR_CONNECT_ERROR);
        }
        cdb2__query__free_unpacked(query, &appdata->newsql_protobuf_allocator.protobuf_allocator);
        return NULL;
    }

    for (int ii = 0; ii < query->sqlquery->n_features; ++ii) {
        if (CDB2_CLIENT_FEATURES__SQLITE_ROW_FORMAT ==
            query->sqlquery->features[ii]) {
            clnt->sqlite_row_format = 1;
            break;
        }
    }

    return query;
}

static void free_newsql_appdata_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata_sbuf *appdata = clnt->appdata;
    clnt_unregister(clnt);
    free_newsql_appdata(clnt);
    newsql_protobuf_destroy(&appdata->newsql_protobuf_allocator);
    free(appdata);
}

static int handle_newsql_request(comdb2_appsock_arg_t *arg)
{
    int rc = 0;
    struct thr_handle *thr_self = arg->thr_self;
    struct dbenv *dbenv = arg->dbenv;

    /* There are points when we can't accept any more connections. */
    if (dbenv->no_more_sql_connections) {
        return APPSOCK_RETURN_OK;
    }

    /*
      If we are NOT the master, and the db is set up for async replication, we
      should return an error at this point rather than proceed with potentially
      incoherent data.
    */
    if (!arg->admin && dbenv->rep_sync == REP_SYNC_NONE &&
        dbenv->master != gbl_myhostname) {
        logmsg(LOGMSG_DEBUG,
               "%s:%d td %" PRIxPTR "new query on replicant with sync none, dropping\n",
               __func__, __LINE__, (intptr_t)pthread_self());
        return APPSOCK_RETURN_OK;
    }


    /*
      This flag cannot be set to non-zero until after all the early returns in
      this function; otherwise, we may "leak" appsock connections.
    */
    if (arg->keepsocket)
        *arg->keepsocket = 1;

    /*
      New way. Do the basic socket I/O in line in this thread (which has a very
      small stack); the handle_fastsql_requests function will dispatch to a
      pooled sql engine for performing queries.
    */
    thrman_change_type(thr_self, THRTYPE_APPSOCK_SQL);

    struct sqlclntstate clnt;
    struct sbuf2 *sb = arg->sb;
    newsql_setup_clnt_sbuf(&clnt, sb);
    clnt.admin = arg->admin;
    sbuf_set_timeout(&clnt, sb, gbl_sqlwrtimeoutms);

    struct newsql_appdata_sbuf *appdata = clnt.appdata;
    CDB2QUERY *query = read_newsql_query(dbenv, &clnt, sb);
    if (query == NULL) {
        goto done;
    }

    if (!clnt.admin && check_active_appsock_connections(&clnt)) {
        exhausted_appsock_connections(&clnt);
        goto done;
    }

#if 0
    else
        logmsg(LOGMSG_DEBUG, "New Query: %s\n", query->sqlquery->sql_query);
#endif
    if (query->sqlquery == NULL) {
        logmsg(LOGMSG_DEBUG, "Malformed SQL request.\n");
        goto done;
    }

    CDB2SQLQUERY *sql_query = query->sqlquery;

    if (newsql_first_run(&clnt, sql_query) != 0) {
        goto done;
    }

    sbuf2flush(sb);
    net_set_writefn(sb, sql_write_sbuf);
    sbuf2setclnt(sb, &clnt);

    while (query) {
        sql_query = query->sqlquery;
#ifdef EXTENDED_DEBUG
#define MAXTOPRINT 200
        int num = logmsg(LOGMSG_DEBUG, "Query is '%.*s", MAXTOPRINT, sql_query->sql_query);
        if (num >= MAXTOPRINT)
            logmsg(LOGMSG_DEBUG, "...'\n");
        else
            logmsg(LOGMSG_DEBUG, "'\n");
#endif
        appdata->query = query;
        appdata->sqlquery = sql_query;
        if (newsql_loop(&clnt, sql_query) != 0) {
            goto done;
        }
        clnt.heartbeat = 1;
        int isCommitRollback;
        if (newsql_should_dispatch(&clnt, &isCommitRollback) == 0) {
            comdb2bma_pass_priority_back(blobmem);
            rc = dispatch_sql_query(&clnt);
            if (clnt.had_errors && isCommitRollback) {
                rc = -1;
            }
        }
        clnt_change_state(&clnt, CONNECTION_IDLE);

        if (clnt.osql.replay == OSQL_RETRY_DO) {
            if (clnt.dbtran.trans_has_sp) {
                osql_set_replay(__FILE__, __LINE__, &clnt, OSQL_RETRY_NONE);
                srs_tran_destroy(&clnt);
                newsql_protobuf_reset_offset(&appdata->newsql_protobuf_allocator);
            } else {
                rc = srs_tran_replay(&clnt);
            }

            if (clnt.osql.history == NULL) {
                query = appdata->query = NULL;
            }
        } else {
            /* if this transaction is done (marked by SQLENG_NORMAL_PROCESS),
               clean transaction sql history
            */
            if (clnt.osql.history && clnt.ctrl_sqlengine == SQLENG_NORMAL_PROCESS) {
                srs_tran_destroy(&clnt);
                newsql_protobuf_reset_offset(&appdata->newsql_protobuf_allocator);
                query = appdata->query = NULL;
            }
        }

        if (!in_client_trans(&clnt)) {
            newsql_protobuf_reset_offset(&appdata->newsql_protobuf_allocator);
            if (rc) {
                goto done;
            }
        }

        if (clnt.added_to_hist) {
            clnt.added_to_hist = 0;
        } else if (appdata->query) {
            /* cleanup if we did not add to history (single stmt or select inside a tran) */
            cdb2__query__free_unpacked(appdata->query, &appdata->newsql_protobuf_allocator.protobuf_allocator);
            /* clnt.sql points into the protobuf unpacked buffer, which becomes
             * invalid after cdb2__query__free_unpacked. Reset the pointer here.
             */
            Pthread_mutex_lock(&clnt.sql_lk);
            clnt.sql = NULL;
            Pthread_mutex_unlock(&clnt.sql_lk);
        }
        appdata->query = NULL;

        query = read_newsql_query(dbenv, &clnt, sb);
    }

done:
    sbuf2setclnt(sb, NULL);
    if (query) {
        cdb2__query__free_unpacked(query, &appdata->newsql_protobuf_allocator.protobuf_allocator);
    }
    free_newsql_appdata_sbuf(&clnt);
    close_appsock(sb);
    arg->sb = NULL;
    return APPSOCK_RETURN_OK;
}

static int newsql_init(void *arg)
{
    setup_newsql_evbuffer_handlers();
    return 0;
}

comdb2_appsock_t newsql_plugin = {
    "newsql",             /* Name */
    "",                   /* Usage info */
    0,                    /* Execution count */
    APPSOCK_FLAG_IS_SQL,  /* Flags */
    handle_newsql_request /* Handler function */
};

#include "plugin.h"
