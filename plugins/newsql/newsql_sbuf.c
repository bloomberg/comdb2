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

#include <comdb2_appsock.h>
#include <comdb2_atomic.h>
#include <comdb2_plugin.h>
#include <intern_strings.h>
#include <newsql.h>
#include <pb_alloc.h>
#include <sql.h>
#include <str0.h>

#ifndef container_of
/* I'm requiring that pointer variable and struct member have the same name */
#define container_of(ptr, type) (type *)((uint8_t *)ptr - offsetof(type, ptr))
#endif

struct thr_handle;

extern int gbl_sqlwrtimeoutms;
extern int active_appsock_conns;

#if WITH_SSL
extern ssl_mode gbl_client_ssl_mode;
extern SSL_CTX *gbl_ssl_ctx;
extern char gbl_dbname[MAX_DBNAME_LENGTH];
extern int gbl_nid_dbname;
void ssl_set_clnt_user(struct sqlclntstate *clnt);
#endif

int gbl_protobuf_prealloc_buffer_size = 8192;
int64_t gbl_denied_appsock_connection_count = 0;

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
        p = malloc(size);
        npa->alloced_outside_buffer++;
    }
    return p;
}

static void newsql_protobuf_free(void *allocator_data, void *ptr)
{
    uint8_t *p = ptr;
    struct NewsqlProtobufCAllocator *npa = allocator_data;
    if (p < npa->protobuf_data || p > (npa->protobuf_data + npa->protobuf_size)) {
        free(p);
        npa->alloced_outside_buffer--;
    }
}

static void newsql_protobuf_init(struct NewsqlProtobufCAllocator *npa)
{
    npa->protobuf_size = gbl_protobuf_prealloc_buffer_size;
    npa->protobuf_offset = 0;
    npa->alloced_outside_buffer = 0;
    npa->protobuf_data = malloc(npa->protobuf_size);
    npa->protobuf_allocator = setup_pb_allocator(newsql_protobuf_alloc, newsql_protobuf_free, npa);
}

static void newsql_protobuf_destroy(struct NewsqlProtobufCAllocator *npa)
{
    assert(npa->alloced_outside_buffer == 0);
    free(npa->protobuf_data);
}

static int newsql_read_sbuf(struct sqlclntstate *clnt, void *b, int l, int n)
{
    struct newsql_appdata *appdata = clnt->appdata;
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
    struct newsql_appdata *appdata = clnt->appdata;
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
    struct newsql_appdata *appdata = clnt->appdata;
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
    struct newsql_appdata *appdata = clnt->appdata;
    lock_client_write_lock(clnt);
    int rc = sbuf2flush(appdata->sb);
    unlock_client_write_lock(clnt);
    return rc < 0;
}

static int newsql_write_postponed_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
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
    struct newsql_appdata *appdata = clnt->appdata;
    return sbuf_set_timeout(clnt, appdata->sb, wr_timeout_ms);
}

static int newsql_ping_pong_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
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
    struct newsql_appdata *appdata = clnt->appdata;
    return sbuf2fileno(appdata->sb);
}

static int newsql_close_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    return sbuf2close(appdata->sb);
}

static int newsql_local_check_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    return sbuf_is_local(appdata->sb);
}

static int newsql_peer_check_sbuf(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    return peer_dropped_connection_sbuf(appdata->sb);
}

static int newsql_has_ssl_sbuf(struct sqlclntstate *clnt)
{
#   if WITH_SSL
    struct newsql_appdata *appdata = clnt->appdata;
    return sslio_has_ssl(appdata->sb);
#   else
    return 0;
#   endif
}

static int newsql_has_x509_sbuf(struct sqlclntstate *clnt)
{
#   if WITH_SSL
    struct newsql_appdata *appdata = clnt->appdata;
    return sslio_has_x509(appdata->sb);
#   else
    return 0;
#   endif
}

static int newsql_get_x509_attr_sbuf(struct sqlclntstate *clnt, int nid, void *out, int outsz)
{
#   if WITH_SSL
    struct newsql_appdata *appdata = clnt->appdata;
    return sslio_x509_attr(appdata->sb, nid, out, outsz);
#   else
    return -1;
#   endif
}

static void send_dbinforesponse_sbuf(struct dbenv *dbenv, struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    SBUF2 *sb = appdata->sb;
    CDB2DBINFORESPONSE *dbinfo_response = malloc(sizeof(CDB2DBINFORESPONSE));
    cdb2__dbinforesponse__init(dbinfo_response);
    fill_dbinfo(dbinfo_response, dbenv->bdb_env);
    int len = cdb2__dbinforesponse__get_packed_size(dbinfo_response);
    struct pb_sbuf_writer writer = PB_SBUF_WRITER_INIT(sb);
    struct newsqlheader hdr = {0};
    hdr.type = htonl(RESPONSE_HEADER__DBINFO_RESPONSE);
    hdr.length = htonl(len);
    sbuf2write((char *)&hdr, sizeof(hdr), sb);
    cdb2__dbinforesponse__pack_to_buffer(dbinfo_response, &writer.base);
    sbuf2flush(sb);
    cdb2__dbinforesponse__free_unpacked(dbinfo_response, &pb_alloc);
}

static void setup_newsql_clnt_sbuf(struct sqlclntstate *clnt, SBUF2 *sb)
{
    reset_clnt(clnt, 1);
    struct newsql_appdata *appdata = get_newsql_appdata(clnt, APPDATA_MINCOLS);
    setup_newsql_clnt(clnt);
    appdata->sb = sb;
    appdata->close_impl = newsql_close_sbuf;
    appdata->get_fileno_impl = newsql_get_fileno_sbuf;
    appdata->flush_impl = newsql_flush_sbuf;
    appdata->local_check_impl = newsql_local_check_sbuf;
    appdata->peer_check_impl = newsql_peer_check_sbuf;
    appdata->ping_pong_impl = newsql_ping_pong_sbuf;
    appdata->read_impl = newsql_read_sbuf;
    appdata->set_timeout_impl = newsql_set_timeout_sbuf;
    appdata->write_impl = newsql_write_sbuf;
    appdata->write_hdr_impl = newsql_write_hdr_sbuf;
    appdata->write_postponed_impl = newsql_write_postponed_sbuf;
    appdata->has_ssl_impl = newsql_has_ssl_sbuf;
    appdata->has_x509_impl = newsql_has_x509_sbuf;
    appdata->get_x509_attr_impl = newsql_get_x509_attr_sbuf;
    newsql_protobuf_init(&appdata->newsql_protobuf_allocator);
}

static int do_query_on_master_check(struct dbenv *dbenv,
                                    struct sqlclntstate *clnt,
                                    CDB2SQLQUERY *sql_query)
{
    int allow_master_exec = 0;
    int allow_master_dbinfo = 0;
    for (int ii = 0; ii < sql_query->n_features; ii++) {
        if (CDB2_CLIENT_FEATURES__ALLOW_MASTER_EXEC ==
            sql_query->features[ii]) {
            allow_master_exec = 1;
        } else if (CDB2_CLIENT_FEATURES__ALLOW_MASTER_DBINFO ==
                   sql_query->features[ii]) {
            allow_master_dbinfo = 1;
        } else if (CDB2_CLIENT_FEATURES__ALLOW_QUEUING ==
                   sql_query->features[ii]) {
            clnt->queue_me = 1;
        }
    }

    int do_master_check;
    if (dbenv->rep_sync == REP_SYNC_NONE || newsql_local_check_sbuf(clnt))
        do_master_check = 0;
    else
        do_master_check = 1;

    if (do_master_check && bdb_master_should_reject(dbenv->bdb_env) &&
        allow_master_exec == 0) {
        ATOMIC_ADD32(gbl_masterrejects, 1);
        /* Send sql response with dbinfo. */
        if (allow_master_dbinfo)
            send_dbinforesponse_sbuf(dbenv, clnt);

        logmsg(LOGMSG_DEBUG, "Query on master, will be rejected\n");
        return 1;
    }
    return 0;
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
#if WITH_SSL
        /* If client requires SSL and we haven't done that,
           do SSL_accept() now. handle_newsql_request()
           will close the sb if SSL_accept() fails. */

        /* Can't SSL_accept twice - probably a client API logic error.
           Let it disconnect. */
        if (sslio_has_ssl(sb)) {
            logmsg(LOGMSG_WARN, "The connection is already SSL encrypted.\n");
            return NULL;
        }

        /* Flush the SSL ability byte. We need to do this because:
           1) The `require_ssl` field in dbinfo may not reflect the
              actual status of this node;
           2) Doing SSL_accept() immediately would cause too many
              unnecessary EAGAIN/EWOULDBLOCK's for non-blocking BIO. */
        char ssl_able = (gbl_client_ssl_mode >= SSL_ALLOW) ? 'Y' : 'N';
        if ((rc = sbuf2putc(sb, ssl_able)) < 0 || (rc = sbuf2flush(sb)) < 0)
            return NULL;

        /* Don't close the connection if SSL verify fails so that we can
           send back an error to the client. */
        if (ssl_able == 'Y' &&
            sslio_accept(sb, gbl_ssl_ctx, gbl_client_ssl_mode, gbl_dbname, gbl_nid_dbname, 0) != 1) {
            write_response(clnt, RESPONSE_ERROR, "Client certificate authentication failed.", CDB2ERR_CONNECT_ERROR);
            /* Print the error message in the sbuf2. */
            char err[256];
            sbuf2lasterror(sb, err, sizeof(err));
            logmsg(LOGMSG_ERROR, "%s\n", err);
            return NULL;
        }

        /* Extract the user from the certificate. */
        ssl_set_clnt_user(clnt);
#else
        /* Not compiled with SSL. Send back `N' to client and retry read. */
        if ((rc = sbuf2putc(sb, 'N')) < 0 || (rc = sbuf2flush(sb)) < 0)
            return NULL;
#endif
        goto retry_read;
    } else if (hdr.type == CDB2_REQUEST_TYPE__RESET) { /* Reset from sockpool.*/

        if (clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
            /* Discard the pending transaction when receiving RESET from the
               sockpool. We reach here if
               1) the handle is in a open transaction, and
               2) the last statement is a SELECT, and
               3) the client closes the handle and donates the connection
                  to the sockpool, and then,
               4) the client creates a new handle and reuses the connection
                  from the sockpool. */
            handle_sql_intrans_unrecoverable_error(clnt);
        }

        reset_clnt(clnt, 0);
        struct newsql_appdata *appdata = clnt->appdata;
        newsql_protobuf_reset_offset(&appdata->newsql_protobuf_allocator);
        clnt->tzname[0] = '\0';
        clnt->osql.count_changes = 1;
        clnt->heartbeat = 1;
        clnt->dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
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

    struct newsql_appdata *appdata = clnt->appdata;
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
            sql_response.response_type =
                RESPONSE_TYPE__COMDB2_INFO; /* Last Row. */
            sql_response.n_value = 0;
            if (clnt->verifyretry_off == 1 ||
                clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
                clnt->dbtran.mode == TRANLEVEL_SERIAL) {
                clnt->effects.num_affected = clnt->effects.num_updated +
                                             clnt->effects.num_deleted +
                                             clnt->effects.num_inserted;
                effects.num_affected = clnt->effects.num_affected;
                effects.num_selected = clnt->effects.num_selected;
                effects.num_updated = clnt->effects.num_updated;
                effects.num_deleted = clnt->effects.num_deleted;
                effects.num_inserted = clnt->effects.num_inserted;
                set_sent_data_to_client(clnt, 1, __func__, __LINE__);
                sql_response.effects = &effects;
                sql_response.error_code = 0;
            } else {
                sql_response.error_code = -1;
                sql_response.error_string = "Get effects not supported in "
                                            "transaction with verifyretry on";
            }
            newsql_write_sbuf(clnt, RESPONSE_HEADER__SQL_EFFECTS, 0, &sql_response, 1);
        } else {
            send_dbinforesponse_sbuf(dbenv, clnt);
        }
        cdb2__query__free_unpacked(query, &appdata->newsql_protobuf_allocator.protobuf_allocator);
        query = NULL;
        goto retry_read;
    }

#if WITH_SSL
    /* Do security check before we return. We do it only after
       the query has been unpacked so that we know whether
       it is a new client (new clients have SSL feature).
       The check must be done for every query, otherwise
       attackers could bypass it by using pooled connections
       from sockpool. The overhead of the check is negligible. */
    if (gbl_client_ssl_mode >= SSL_REQUIRE && !sslio_has_ssl(sb)) {
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
        } else {
            write_response(clnt, RESPONSE_ERROR, "The database requires SSL connections.", CDB2ERR_CONNECT_ERROR);
        }
        cdb2__query__free_unpacked(query, &appdata->newsql_protobuf_allocator.protobuf_allocator);
        return NULL;
    }
#endif
    return query;
}

extern int gbl_allow_incoherent_sql;
static int incoh_reject(int admin, bdb_state_type *bdb_state)
{
    /* If this isn't from an admin session and the node isn't coherent
       and we disallow running queries on an incoherent node, reject */
    return (!admin && !bdb_am_i_coherent(bdb_state) &&
            !gbl_allow_incoherent_sql);
}

static void free_newsql_appdata(struct sqlclntstate *clnt)
{
    struct newsql_appdata *appdata = clnt->appdata;
    if (appdata == NULL) {
        return;
    }
    if (appdata->postponed) {
        free(appdata->postponed->row);
        free(appdata->postponed);
        appdata->postponed = NULL;
    }
    newsql_protobuf_destroy(&appdata->newsql_protobuf_allocator);
    free(appdata);
    clnt->appdata = NULL;
}


#define APPDATA ((struct newsql_appdata *)(clnt.appdata))
static int handle_newsql_request(comdb2_appsock_arg_t *arg)
{
    CDB2QUERY *query = NULL;
    int rc = 0;
    struct sqlclntstate clnt;
    struct thr_handle *thr_self;
    struct sbuf2 *sb;
    struct dbenv *dbenv;

    thr_self = arg->thr_self;
    dbenv = arg->dbenv;
    sb = arg->sb;

    if (incoh_reject(arg->admin, dbenv->bdb_env)) {
        logmsg(LOGMSG_DEBUG,
               "%s:%d td %u new query on incoherent node, dropping socket\n",
               __func__, __LINE__, (uint32_t)pthread_self());
        return APPSOCK_RETURN_OK;
    }

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
               "%s:%d td %u new query on replicant with sync none, dropping\n",
               __func__, __LINE__, (uint32_t)pthread_self());
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

    setup_newsql_clnt_sbuf(&clnt, sb);

    char *origin = get_origin_mach_by_buf(sb);
    clnt.origin = origin ? origin : intern("???");
    clnt.tzname[0] = '\0';
    clnt.admin = arg->admin;
    sbuf_set_timeout(&clnt, sb, gbl_sqlwrtimeoutms);

    clnt_register(&clnt);

    if (incoh_reject(clnt.admin, thedb->bdb_env)) {
        logmsg(LOGMSG_ERROR,
               "%s:%d td %u new query on incoherent node, dropping socket\n",
               __func__, __LINE__, (uint32_t)pthread_self());
        goto done;
    }

    query = read_newsql_query(dbenv, &clnt, sb);
    if (query == NULL) {
        goto done;
    }

    if (!clnt.admin && check_active_appsock_connections(&clnt)) {
        static time_t pr = 0;
        time_t now;

        gbl_denied_appsock_connection_count++;
        if ((now = time(NULL)) - pr) {
            logmsg(LOGMSG_WARN,
                   "%s: Exhausted appsock connections, total %d connections "
                   "denied-connection count=%"PRId64"\n",
                   __func__, active_appsock_conns,
                   gbl_denied_appsock_connection_count);
            pr = now;
        }

        write_response(&clnt, RESPONSE_ERROR, "Exhausted appsock connections.", CDB2__ERROR_CODE__APPSOCK_LIMIT);
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

    if (!clnt.admin && do_query_on_master_check(dbenv, &clnt, sql_query))
        goto done;

    if (sql_query->client_info) {
        clnt.conninfo.pid = sql_query->client_info->pid;
        clnt.last_pid = sql_query->client_info->pid;
    }
    else {
        clnt.conninfo.pid = 0;
        clnt.last_pid = 0;
    }
    clnt.osql.count_changes = 1;
    clnt.dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
    clnt.plugin.clr_high_availability(&clnt);

    sbuf2flush(sb);
    net_set_writefn(sb, sql_writer);
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
        APPDATA->query = query;
        APPDATA->sqlquery = sql_query;
        clnt.sql = sql_query->sql_query;
        clnt.added_to_hist = 0;

        if (!in_client_trans(&clnt)) {
            bzero(&clnt.effects, sizeof(clnt.effects));
            bzero(&clnt.log_effects, sizeof(clnt.log_effects));
            clnt.had_errors = 0;
            clnt.ctrl_sqlengine = SQLENG_NORMAL_PROCESS;
        }
        if (clnt.dbtran.mode < TRANLEVEL_SOSQL) {
            clnt.dbtran.mode = TRANLEVEL_SOSQL;
        }
        clnt.osql.sent_column_data = 0;
        clnt.stop_this_statement = 0;

        if (clnt.tzname[0] == '\0' && sql_query->tzname)
            strncpy0(clnt.tzname, sql_query->tzname, sizeof(clnt.tzname));

        if (sql_query->dbname && dbenv->envname && strcasecmp(sql_query->dbname, dbenv->envname)) {
            char errstr[64 + (2 * MAX_DBNAME_LENGTH)];
            snprintf(errstr, sizeof(errstr),
                     "DB name mismatch query:%s actual:%s", sql_query->dbname,
                     dbenv->envname);
            logmsg(LOGMSG_ERROR, "%s\n", errstr);
            write_response(&clnt, RESPONSE_ERROR, errstr, CDB2__ERROR_CODE__WRONG_DB);
            goto done;
        }

        if (sql_query->client_info) {
            if (clnt.rawnodestats) {
                release_node_stats(clnt.argv0, clnt.stack, clnt.origin);
                clnt.rawnodestats = NULL;
            }
            if (clnt.conninfo.pid && clnt.conninfo.pid != sql_query->client_info->pid) {
                /* Different pid is coming without reset. */
                logmsg(LOGMSG_WARN,
                       "Multiple processes using same socket PID 1 %d PID 2 %d Host %.8x\n",
                       clnt.conninfo.pid, sql_query->client_info->pid,
                       sql_query->client_info->host_id);
            }
            clnt.conninfo.pid = sql_query->client_info->pid;
            clnt.conninfo.node = sql_query->client_info->host_id;
            if (clnt.argv0) {
                free(clnt.argv0);
                clnt.argv0 = NULL;
            }
            if (clnt.stack) {
                free(clnt.stack);
                clnt.stack = NULL;
            }
            if (sql_query->client_info->argv0) {
                clnt.argv0 = strdup(sql_query->client_info->argv0);
            }
            if (sql_query->client_info->stack) {
                clnt.stack = strdup(sql_query->client_info->stack);
            }
        }

        if (clnt.rawnodestats == NULL) {
            clnt.rawnodestats = get_raw_node_stats(
                clnt.argv0, clnt.stack, clnt.origin, sbuf2fileno(sb));
        }

        if (process_set_commands(&clnt, sql_query))
            goto done;

        if (gbl_rowlocks && clnt.dbtran.mode != TRANLEVEL_SERIAL)
            clnt.dbtran.mode = TRANLEVEL_SNAPISOL;

        /* avoid new accepting new queries/transaction on opened connections
           if we are incoherent (and not in a transaction). */
        if (incoh_reject(clnt.admin, thedb->bdb_env) &&
            (clnt.ctrl_sqlengine == SQLENG_NORMAL_PROCESS)) {
            logmsg(LOGMSG_ERROR,
                   "%s line %d td %u new query on incoherent node, dropping socket\n",
                   __func__, __LINE__, (uint32_t)pthread_self());
            goto done;
        }

        clnt.heartbeat = 1;
        ATOMIC_ADD32(gbl_nnewsql, 1);

        bool isCommitRollback = (strncasecmp(clnt.sql, "commit", 6) == 0 ||
                                 strncasecmp(clnt.sql, "rollback", 8) == 0)
                                    ? true
                                    : false;

        if (!clnt.had_errors || isCommitRollback) {
            /* tell blobmem that I want my priority back
               when the sql thread is done */
            comdb2bma_pass_priority_back(blobmem);
            rc = dispatch_sql_query(&clnt, PRIORITY_T_DEFAULT);

            if (clnt.had_errors && isCommitRollback) {
                rc = -1;
            }
        }
        clnt_change_state(&clnt, CONNECTION_IDLE);

        if (clnt.osql.replay == OSQL_RETRY_DO) {
            if (clnt.dbtran.trans_has_sp) {
                osql_set_replay(__FILE__, __LINE__, &clnt, OSQL_RETRY_NONE);
                srs_tran_destroy(&clnt);
                newsql_protobuf_reset_offset(&APPDATA->newsql_protobuf_allocator);
            } else {
                rc = srs_tran_replay(&clnt, arg->thr_self);
            }

            if (clnt.osql.history == NULL) {
                query = APPDATA->query = NULL;
            }
        } else {
            /* if this transaction is done (marked by SQLENG_NORMAL_PROCESS),
               clean transaction sql history
            */
            if (clnt.osql.history && clnt.ctrl_sqlengine == SQLENG_NORMAL_PROCESS) {
                srs_tran_destroy(&clnt);
                newsql_protobuf_reset_offset(&APPDATA->newsql_protobuf_allocator);
                query = APPDATA->query = NULL;
            }
        }

        if (!in_client_trans(&clnt)) {
            newsql_protobuf_reset_offset(&APPDATA->newsql_protobuf_allocator);
            if (rc) {
                goto done;
            }
        }

        if (clnt.added_to_hist) {
            clnt.added_to_hist = 0;
        } else if (APPDATA->query) {
            /* cleanup if we did not add to history (single stmt or select inside a tran) */
            cdb2__query__free_unpacked(APPDATA->query, &APPDATA->newsql_protobuf_allocator.protobuf_allocator);
            APPDATA->query = NULL;
            /* clnt.sql points into the protobuf unpacked buffer, which becomes
             * invalid after cdb2__query__free_unpacked. Reset the pointer here.
             */
            clnt.sql = NULL;
        }

        query = read_newsql_query(dbenv, &clnt, sb);
    }

done:
    sbuf2setclnt(sb, NULL);
    clnt_unregister(&clnt);

    if (clnt.ctrl_sqlengine == SQLENG_INTRANS_STATE) {
        handle_sql_intrans_unrecoverable_error(&clnt);
    }

    if (clnt.rawnodestats) {
        release_node_stats(clnt.argv0, clnt.stack, clnt.origin);
        clnt.rawnodestats = NULL;
    }

    if (clnt.argv0) {
        free(clnt.argv0);
        clnt.argv0 = NULL;
    }

    if (clnt.stack) {
        free(clnt.stack);
        clnt.stack = NULL;
    }

    close_sp(&clnt);
    osql_clean_sqlclntstate(&clnt);

    if (clnt.dbglog) {
        sbuf2close(clnt.dbglog);
        clnt.dbglog = NULL;
    }

    if (query) {
        cdb2__query__free_unpacked(query, &APPDATA->newsql_protobuf_allocator.protobuf_allocator);
    }

    free_newsql_appdata(&clnt);

    /* XXX free logical tran?  */
    close_appsock(sb);
    arg->sb = NULL;
    cleanup_clnt(&clnt);

    return APPSOCK_RETURN_OK;
}

comdb2_appsock_t newsql_plugin = {
    "newsql",             /* Name */
    "",                   /* Usage info */
    0,                    /* Execution count */
    APPSOCK_FLAG_IS_SQL,  /* Flags */
    handle_newsql_request /* Handler function */
};

#include "plugin.h"
