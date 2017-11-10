/*
   Copyright 2017 Bloomberg Finance L.P.

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

#include <pthread.h>
#include <stdlib.h>

typedef struct VdbeSorter VdbeSorter;

#include "comdb2_plugin.h"
#include "pb_alloc.h"
#include "sp.h"
#include "sql.h"
#include "comdb2_appsock.h"
#include "comdb2_atomic.h"

struct thr_handle;
struct sbuf2;

extern int gbl_sqlwrtimeoutms;
extern int active_appsock_conns;
extern ssl_mode gbl_client_ssl_mode;
extern SSL_CTX *gbl_ssl_ctx;

void reset_clnt(struct sqlclntstate *clnt, SBUF2 *sb, int initial);
int disable_server_sql_timeouts(void);
int tdef_to_tranlevel(int tdef);
int osql_clean_sqlclntstate(struct sqlclntstate *clnt);
int watcher_warning_function(void *arg, int timeout, int gap);
void handle_sql_intrans_unrecoverable_error(struct sqlclntstate *clnt);
int send_heartbeat(struct sqlclntstate *clnt);
void send_prepare_error(struct sqlclntstate *clnt, const char *errstr,
                        int clnt_retry);
int fdb_access_control_create(struct sqlclntstate *clnt, char *str);
int handle_failed_dispatch(struct sqlclntstate *clnt, char *errstr);
int sbuf_is_local(SBUF2 *sb);
int fsql_writer(SBUF2 *sb, const char *buf, int nbytes);

static int newsql_init(void *unused)
{
    return 0;
}

static int newsql_destroy(void)
{
    return 0;
}

/* skip spaces and tabs if present. */
static char *cdb2_skipws(char *str)
{
    if (*str && isspace(*str)) {
        while (*str && isspace(*str))
            str++;
    }
    return str;
}

/* Process sql query if it is a set command. */
static int process_set_commands(struct dbenv *dbenv, struct sqlclntstate *clnt)
{
    CDB2SQLQUERY *sql_query = NULL;
    int num_commands = 0;
    sql_query = clnt->sql_query;
    char *sqlstr = NULL;
    char *endp;
    int rc = 0;
    num_commands = sql_query->n_set_flags;
    for (int ii = 0; ii < num_commands && rc == 0; ii++) {
        sqlstr = sql_query->set_flags[ii];
        sqlstr = cdb2_skipws(sqlstr);
        if (strncasecmp(sqlstr, "set", 3) == 0) {
            if (gbl_extended_sql_debug_trace) {
                logmsg(LOGMSG_ERROR,
                       "td %u %s line %d processing set command '%s'\n",
                       pthread_self(), __func__, __LINE__, sqlstr);
            }
            sqlstr += 3;
            sqlstr = cdb2_skipws(sqlstr);
            if (strncasecmp(sqlstr, "transaction", 11) == 0) {
                sqlstr += 11;
                sqlstr = cdb2_skipws(sqlstr);
                clnt->dbtran.mode = TRANLEVEL_INVALID;
                set_high_availability(clnt, 0);
                // clnt->high_availability = 0;
                if (strncasecmp(sqlstr, "read", 4) == 0) {
                    sqlstr += 4;
                    sqlstr = cdb2_skipws(sqlstr);
                    if (strncasecmp(sqlstr, "committed", 4) == 0) {
                        clnt->dbtran.mode = TRANLEVEL_RECOM;
                    }
                } else if (strncasecmp(sqlstr, "serial", 6) == 0) {
                    clnt->dbtran.mode = TRANLEVEL_SERIAL;
                    if (clnt->hasql_on == 1) {
                        set_high_availability(clnt, 1);
                        // clnt->high_availability = 1;
                    }
                } else if (strncasecmp(sqlstr, "blocksql", 7) == 0) {
                    clnt->dbtran.mode = TRANLEVEL_SOSQL;
                } else if (strncasecmp(sqlstr, "snap", 4) == 0) {
                    sqlstr += 4;
                    clnt->dbtran.mode = TRANLEVEL_SNAPISOL;
                    clnt->verify_retries = 0;
                    if (clnt->hasql_on == 1) {
                        set_high_availability(clnt, 1);
                        // clnt->high_availability = 1;
                        logmsg(
                            LOGMSG_ERROR,
                            "Enabling snapshot isolation high availability\n");
                    }
                }
                if (clnt->dbtran.mode == TRANLEVEL_INVALID)
                    rc = ii + 1;
            } else if (strncasecmp(sqlstr, "timeout", 7) == 0) {
                sqlstr += 7;
                sqlstr = cdb2_skipws(sqlstr);
                int timeout = strtol(sqlstr, &endp, 10);
                int notimeout = disable_server_sql_timeouts();
                sbuf2settimeout(clnt->sb, 0, notimeout ? 0 : timeout);
                if (timeout == 0)
                    net_add_watch(clnt->sb, 0, 0);
                else
                    net_add_watch_warning(
                        clnt->sb,
                        bdb_attr_get(dbenv->bdb_attr,
                                     BDB_ATTR_MAX_SQL_IDLE_TIME),
                        notimeout ? 0 : (timeout / 1000), clnt,
                        watcher_warning_function);
            } else if (strncasecmp(sqlstr, "maxquerytime", 12) == 0) {
                sqlstr += 12;
                sqlstr = cdb2_skipws(sqlstr);
                int timeout = strtol(sqlstr, &endp, 10);
                if (timeout >= 0)
                    clnt->query_timeout = timeout;
            } else if (strncasecmp(sqlstr, "timezone", 8) == 0) {
                sqlstr += 8;
                sqlstr = cdb2_skipws(sqlstr);
                strncpy(clnt->tzname, sqlstr, sizeof(clnt->tzname));
            } else if (strncasecmp(sqlstr, "datetime", 8) == 0) {
                sqlstr += 8;
                sqlstr = cdb2_skipws(sqlstr);

                if (strncasecmp(sqlstr, "precision", 9) == 0) {
                    sqlstr += 9;
                    sqlstr = cdb2_skipws(sqlstr);
                    DTTZ_TEXT_TO_PREC(sqlstr, clnt->dtprec, 0, return -1);
                } else {
                    rc = ii + 1;
                }
            } else if (strncasecmp(sqlstr, "user", 4) == 0) {
                sqlstr += 4;
                sqlstr = cdb2_skipws(sqlstr);
                clnt->have_user = 1;
                strncpy(clnt->user, sqlstr, sizeof(clnt->user));
            } else if (strncasecmp(sqlstr, "password", 8) == 0) {
                sqlstr += 8;
                sqlstr = cdb2_skipws(sqlstr);
                clnt->have_password = 1;
                strncpy(clnt->password, sqlstr, sizeof(clnt->password));
                sqlite3Dequote(clnt->password);
            } else if (strncasecmp(sqlstr, "spversion", 9) == 0) {
                clnt->spversion.version_num = 0;
                free(clnt->spversion.version_str);
                clnt->spversion.version_str = NULL;
                sqlstr += 9;
                sqlstr = cdb2_skipws(sqlstr);
                char *spname = sqlstr;
                while (!isspace(*sqlstr)) {
                    ++sqlstr;
                }
                *sqlstr = 0;
                if ((sqlstr - spname) < MAX_SPNAME) {
                    strncpy(clnt->spname, spname, MAX_SPNAME);
                    clnt->spname[MAX_SPNAME] = '\0';
                } else {
                    rc = ii + 1;
                }
                ++sqlstr;

                sqlstr = cdb2_skipws(sqlstr);
                int ver = strtol(sqlstr, &endp, 10);
                if (*sqlstr == '\'' || *sqlstr == '"') { // looks like a str
                    if (strlen(sqlstr) < MAX_SPVERSION_LEN) {
                        clnt->spversion.version_str = strdup(sqlstr);
                        sqlite3Dequote(clnt->spversion.version_str);
                    } else {
                        rc = ii + 1;
                    }
                } else if (*endp == 0) { // parsed entire number successfully
                    clnt->spversion.version_num = ver;
                } else {
                    rc = ii + 1;
                }
            } else if (strncasecmp(sqlstr, "readonly", 8) == 0) {
                sqlstr += 8;
                sqlstr = cdb2_skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->is_readonly = 0;
                } else {
                    clnt->is_readonly = 1;
                }
            } else if (strncasecmp(sqlstr, "expert", 6) == 0) {
                sqlstr += 6;
                sqlstr = cdb2_skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->is_expert = 0;
                } else {
                    clnt->is_expert = 1;
                }
            } else if (strncasecmp(sqlstr, "sptrace", 7) == 0) {
                sqlstr += 7;
                sqlstr = cdb2_skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->want_stored_procedure_trace = 0;
                } else {
                    clnt->want_stored_procedure_trace = 1;
                }
            } else if (strncasecmp(sqlstr, "spdebug", 7) == 0) {
                sqlstr += 7;
                sqlstr = cdb2_skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->want_stored_procedure_debug = 0;
                } else {
                    clnt->want_stored_procedure_debug = 1;
                }
            } else if (strncasecmp(sqlstr, "HASQL", 5) == 0) {
                sqlstr += 5;
                sqlstr = cdb2_skipws(sqlstr);
                if (strncasecmp(sqlstr, "on", 2) == 0) {
                    clnt->hasql_on = 1;
                    if (clnt->dbtran.mode == TRANLEVEL_SERIAL ||
                        clnt->dbtran.mode == TRANLEVEL_SNAPISOL) {
                        set_high_availability(clnt, 1);
                        // clnt->high_availability = 1;
                        if (gbl_extended_sql_debug_trace) {
                            logmsg(
                                LOGMSG_USER,
                                "td %u %s line %d setting high_availability\n",
                                pthread_self(), __func__, __LINE__);
                        }
                    }
                } else {
                    clnt->hasql_on = 0;
                    set_high_availability(clnt, 0);
                    // clnt->high_availability = 0;
                    if (gbl_extended_sql_debug_trace) {
                        logmsg(LOGMSG_USER,
                               "td %u %s line %d clearing high_availability\n",
                               pthread_self(), __func__, __LINE__);
                    }
                }
            } else if (strncasecmp(sqlstr, "verifyretry", 11) == 0) {
                sqlstr += 11;
                sqlstr = cdb2_skipws(sqlstr);
                if (strncasecmp(sqlstr, "on", 2) == 0) {
                    clnt->verifyretry_off = 0;
                } else {
                    clnt->verifyretry_off = 1;
                }
            } else if (strncasecmp(sqlstr, "remote", 6) == 0) {
                sqlstr += 6;
                sqlstr = cdb2_skipws(sqlstr);

                int rc = fdb_access_control_create(clnt, sqlstr);
                if (rc) {
                    logmsg(
                        LOGMSG_ERROR,
                        "%s: failed to process remote access settings \"%s\"\n",
                        __func__, sqlstr);
                }
                rc = ii + 1;
            } else if (strncasecmp(sqlstr, "getcost", 7) == 0) {
                sqlstr += 7;
                sqlstr = cdb2_skipws(sqlstr);
                if (strncasecmp(sqlstr, "on", 2) == 0) {
                    clnt->get_cost = 1;
                } else {
                    clnt->get_cost = 0;
                }
            } else if (strncasecmp(sqlstr, "explain", 7) == 0) {
                sqlstr += 7;
                sqlstr = cdb2_skipws(sqlstr);
                if (strncasecmp(sqlstr, "on", 2) == 0) {
                    clnt->is_explain = 1;
                } else if (strncasecmp(sqlstr, "verbose", 7) == 0) {
                    clnt->is_explain = 2;
                } else {
                    clnt->is_explain = 0;
                }
            } else if (strncasecmp(sqlstr, "maxtransize", 11) == 0) {
                sqlstr += 11;
                int maxtransz = strtol(sqlstr, &endp, 10);
                if (endp != sqlstr && maxtransz >= 0)
                    clnt->osql_max_trans = maxtransz;
                else
                    logmsg(LOGMSG_ERROR,
                           "Error: bad value for maxtransize %s\n", sqlstr);
#ifdef DEBUG
                printf("setting clnt->osql_max_trans to %d\n",
                       clnt->osql_max_trans);
#endif
            } else if (strncasecmp(sqlstr, "plannereffort", 13) == 0) {
                sqlstr += 13;
                int effort = strtol(sqlstr, &endp, 10);
                if (0 < effort && effort <= 10)
                    clnt->planner_effort = effort;
#ifdef DEBUG
                printf("setting clnt->planner_effort to %d\n",
                       clnt->planner_effort);
#endif
            } else {
                rc = ii + 1;
            }

            if (rc) {
                char err[256];
                snprintf(err, sizeof(err) - 1, "Invalid set command '%s'",
                         sqlstr);
                send_prepare_error(clnt, err, 0);
            }
        }
    }
    return rc;
}

static void send_dbinforesponse(struct dbenv *dbenv, SBUF2 *sb)
{
    struct newsqlheader hdr;
    CDB2DBINFORESPONSE *dbinfo_response = malloc(sizeof(CDB2DBINFORESPONSE));
    cdb2__dbinforesponse__init(dbinfo_response);

    fill_dbinfo(dbinfo_response, dbenv->bdb_env);

    int len = cdb2__dbinforesponse__get_packed_size(dbinfo_response);
    void *buf = malloc(len);
    cdb2__dbinforesponse__pack(dbinfo_response, buf);

    hdr.type = ntohl(RESPONSE_HEADER__DBINFO_RESPONSE);
    hdr.compression = 0;
    hdr.dummy = 0;
    hdr.length = ntohl(len);

    sbuf2write((char *)&hdr, sizeof(hdr), sb);

    sbuf2write(buf, len, sb);
    sbuf2flush(sb);
    free(buf);
    cdb2__dbinforesponse__free_unpacked(dbinfo_response, &pb_alloc);
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
            clnt->req.flags |= SQLF_QUEUE_ME;
        }
    }

    int do_master_check;
    if (dbenv->rep_sync == REP_SYNC_NONE || sbuf_is_local(clnt->sb))
        do_master_check = 0;
    else
        do_master_check = 1;

    if (do_master_check && bdb_master_should_reject(dbenv->bdb_env) &&
        allow_master_exec == 0) {
        ATOMIC_ADD(gbl_masterrejects, 1);
        /* Send sql response with dbinfo. */
        if (allow_master_dbinfo)
            send_dbinforesponse(dbenv, clnt->sb);
        return 1;
    }
    return 0;
}

static CDB2QUERY *read_newsql_query(struct dbenv *dbenv,
                                    struct sqlclntstate *clnt, SBUF2 *sb)
{
    struct newsqlheader hdr;
    int rc;
    int pre_enabled = 0;
    int was_timeout = 0;
    char ssl_able;

retry_read:
    rc = sbuf2fread_timeout((char *)&hdr, sizeof(hdr), 1, sb, &was_timeout);
    if (rc != 1) {
        if (was_timeout) {
            handle_failed_dispatch(clnt, "Socket read timeout.");
        }
        return NULL;
    }

    hdr.type = ntohl(hdr.type);
    hdr.compression = ntohl(hdr.compression);
    hdr.length = ntohl(hdr.length);

    if (hdr.type == FSQL_SSLCONN) {
#if WITH_SSL
        /* If client requires SSL and we haven't done that,
           do SSL_accept() now. handle_newsql_requests()
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
        ssl_able = (gbl_client_ssl_mode >= SSL_ALLOW) ? 'Y' : 'N';
        if ((rc = sbuf2putc(sb, ssl_able)) < 0 || (rc = sbuf2flush(sb)) < 0)
            return NULL;

        if (ssl_able == 'Y' &&
            sslio_accept(sb, gbl_ssl_ctx, SSL_REQUIRE, NULL, 0) != 1)
            return NULL;

        if (sslio_verify(sb, gbl_client_ssl_mode, NULL, 0) != 0) {
            char *err = "Client certificate authentication failed.";
            struct fsqlresp resp;
            bzero(&resp, sizeof(resp));
            resp.response = FSQL_ERROR;
            resp.rcode = CDB2ERR_CONNECT_ERROR;
            rc = fsql_write_response(clnt, &resp, err, strlen(err) + 1, 1,
                                     __func__, __LINE__);
            return NULL;
        }
#else
        /* Not compiled with SSL. Send back `N' to client and retry read. */
        if ((rc = sbuf2putc(sb, 'N')) < 0 || (rc = sbuf2flush(sb)) < 0)
            return NULL;
#endif
        goto retry_read;
    } else if (hdr.type == FSQL_RESET) { /* Reset from sockpool.*/

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

        reset_clnt(clnt, sb, 0);
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

    CDB2QUERY *query;

    char *p;

    if (bytes <= gbl_blob_sz_thresh_bytes)
        p = malloc(bytes);
    else
        while (1) { // big buffer. most certainly it is a huge blob.
            p = comdb2_timedmalloc(blobmem, bytes, 1000);

            if (p != NULL || errno != ETIMEDOUT)
                break;

            pthread_mutex_lock(&clnt->wait_mutex);
            clnt->heartbeat = 1;
            if (clnt->ready_for_heartbeats == 0) {
                pre_enabled = 1;
                clnt->ready_for_heartbeats = 1;
            }
            send_heartbeat(clnt);
            fdb_heartbeats(clnt);
            pthread_mutex_unlock(&clnt->wait_mutex);
        }

    if (pre_enabled) {
        pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;
        pthread_mutex_unlock(&clnt->wait_mutex);
        pre_enabled = 0;
    }

    if (!p) {
        logmsg(LOGMSG_ERROR, "%s: out of memory malloc %d\n", __func__, bytes);
        return NULL;
    }

    if (bytes) {
        rc = sbuf2fread(p, bytes, 1, sb);
        if (rc != 1) {
            free(p);
            return NULL;
        }
    }

    while (1) {
        query = cdb2__query__unpack(&pb_alloc, bytes, p);

        if (query != NULL || errno != ETIMEDOUT)
            break;

        pthread_mutex_lock(&clnt->wait_mutex);
        if (clnt->heartbeat == 0)
            clnt->heartbeat = 1;
        if (clnt->ready_for_heartbeats == 0) {
            pre_enabled = 1;
            clnt->ready_for_heartbeats = 1;
        }
        send_heartbeat(clnt);
        fdb_heartbeats(clnt);
        pthread_mutex_unlock(&clnt->wait_mutex);
    }

    if (pre_enabled) {
        pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;
        pthread_mutex_unlock(&clnt->wait_mutex);
    }
    free(p);

    if (query && query->dbinfo) {
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
                sql_response.effects = &effects;
                sql_response.error_code = 0;
            } else {
                sql_response.error_code = -1;
                sql_response.error_string = "Get effects not supported in "
                                            "transaction with verifyretry on";
            }

            newsql_write_response(clnt, RESPONSE_HEADER__SQL_EFFECTS,
                                  &sql_response, 1 /*flush*/, malloc, __func__,
                                  __LINE__);
        } else {
            send_dbinforesponse(dbenv, sb);
        }
        cdb2__query__free_unpacked(query, &pb_alloc);
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
            newsql_write_response(clnt, RESPONSE_HEADER__SQL_RESPONSE_SSL, NULL,
                                  1, malloc, __func__, __LINE__);
            /* Client is going to reuse the connection. Don't drop it. */
            cdb2__query__free_unpacked(query, &pb_alloc);
            goto retry_read;
        } else {
            char *err = "The database requires SSL connections.";
            struct fsqlresp resp;
            bzero(&resp, sizeof(resp));
            resp.response = FSQL_ERROR;
            resp.rcode = CDB2ERR_CONNECT_ERROR;
            rc = fsql_write_response(clnt, &resp, err, strlen(err) + 1, 1,
                                     __func__, __LINE__);
        }
        cdb2__query__free_unpacked(query, &pb_alloc);
        return NULL;
    }
#endif
    return query;
}

extern int gbl_allow_incoherent_sql;

static int handle_newsql_request(comdb2_appsock_arg_t *arg)
{
    int rc = 0;
    struct sqlclntstate clnt;
    struct thr_handle *thr_self;
    struct sbuf2 *sb;
    struct dbenv *dbenv;
    struct dbtable *tab;
    char *cmdline;

    thr_self = arg->thr_self;
    dbenv = arg->dbenv;
    tab = arg->tab;
    sb = arg->sb;
    cmdline = arg->cmdline;

    if (tab->dbtype != DBTYPE_TAGGED_TABLE) {
        /*
          Don't change this message. The sql api recognises the first four
          characters (Erro) and can respond gracefully.
        */
        sbuf2printf(sb, "Error: newsql is only supported for tagged DBs\n");
        logmsg(LOGMSG_ERROR,
               "Error: newsql is only supported for tagged DBs\n");
        sbuf2flush(sb);
        return 0;
    }

    if (!bdb_am_i_coherent(dbenv->bdb_env) && !gbl_allow_incoherent_sql) {
        return 0;
    }

    /* There are points when we can't accept any more connections. */
    if (dbenv->no_more_sql_connections) {
        return 0;
    }

    /*
      If we are NOT the master, and the db is set up for async replication, we
      should return an error at this point rather than proceed with potentially
      incoherent data.
    */
    if (dbenv->rep_sync == REP_SYNC_NONE && dbenv->master != gbl_mynode) {
        return 0;
    }

    /*
      New way. Do the basic socket I/O in line in this thread (which has a very
      small stack); the handle_fastsql_requests function will dispatch to a
      pooled sql engine for performing queries.
    */
    thrman_change_type(thr_self, THRTYPE_APPSOCK_SQL);

    reset_clnt(&clnt, sb, 1);
    clnt.tzname[0] = '\0';
    clnt.is_newsql = 1;

    pthread_mutex_init(&clnt.wait_mutex, NULL);
    pthread_cond_init(&clnt.wait_cond, NULL);
    pthread_mutex_init(&clnt.write_lock, NULL);
    pthread_mutex_init(&clnt.dtran_mtx, NULL);

    if (active_appsock_conns >
        bdb_attr_get(dbenv->bdb_attr, BDB_ATTR_MAXAPPSOCKSLIMIT)) {
        logmsg(LOGMSG_WARN,
               "%s: Exhausted appsock connections, total %d connections \n",
               __func__, active_appsock_conns);
        char *err = "Exhausted appsock connections.";
        struct fsqlresp resp;
        bzero(&resp, sizeof(resp));
        resp.response = FSQL_ERROR;
        resp.rcode = SQLHERR_APPSOCK_LIMIT;
        rc = fsql_write_response(&clnt, &resp, err, strlen(err) + 1, 1,
                                 __func__, __LINE__);
        goto done;
    }

    if (!bdb_am_i_coherent(dbenv->bdb_env)) {
        logmsg(LOGMSG_ERROR,
               "%s:%d td %u new query on incoherent node, dropping socket\n",
               __func__, __LINE__, (uint32_t)pthread_self());
        goto done;
    }

    CDB2QUERY *query = read_newsql_query(dbenv, &clnt, sb);
    if (query == NULL)
        goto done;
    assert(query->sqlquery);
    CDB2SQLQUERY *sql_query = query->sqlquery;
    clnt.query = query;

    if (do_query_on_master_check(dbenv, &clnt, sql_query))
        goto done;

#ifdef DEBUGQUERY
    printf("\n Query '%s'\n", sql_query->sql_query);
#endif

    clnt.osql.count_changes = 1;
    clnt.dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
    set_high_availability(&clnt, 0);
    // clnt.high_availability = 0;

    int notimeout = disable_server_sql_timeouts();
    sbuf2settimeout(
        sb, bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAX_SQL_IDLE_TIME) * 1000,
        notimeout ? 0 : gbl_sqlwrtimeoutms);

    sbuf2flush(sb);
    net_set_writefn(sb, fsql_writer);

    int wrtimeoutsec;
    if (gbl_sqlwrtimeoutms == 0 || notimeout)
        wrtimeoutsec = 0;
    else
        wrtimeoutsec = gbl_sqlwrtimeoutms / 1000;

    net_add_watch_warning(
        sb, bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAX_SQL_IDLE_TIME),
        wrtimeoutsec, &clnt, watcher_warning_function);

    /* appsock threads aren't sql threads so for appsock pool threads
     * sqlthd will be NULL */
    struct sql_thread *sqlthd = pthread_getspecific(query_info_key);
    if (sqlthd) {
        bzero(&sqlthd->sqlclntstate->conn, sizeof(struct conninfo));
        sqlthd->sqlclntstate->origin[0] = 0;
    }

    while (query) {
        assert(query->sqlquery);
        sql_query = query->sqlquery;
        clnt.sql_query = sql_query;
        clnt.sql = sql_query->sql_query;
        clnt.query = query;
        clnt.added_to_hist = 0;

        if (!clnt.in_client_trans) {
            bzero(&clnt.effects, sizeof(clnt.effects));
            bzero(&clnt.log_effects, sizeof(clnt.log_effects));
            clnt.trans_has_sp = 0;
        }
        clnt.is_newsql = 1;
        if (clnt.dbtran.mode < TRANLEVEL_SOSQL) {
            clnt.dbtran.mode = TRANLEVEL_SOSQL;
        }
        clnt.osql.sent_column_data = 0;
        clnt.stop_this_statement = 0;

        if ((clnt.tzname[0] == '\0') && sql_query->tzname)
            strncpy(clnt.tzname, sql_query->tzname, sizeof(clnt.tzname));

        if (sql_query->dbname && dbenv->envname &&
            strcasecmp(sql_query->dbname, dbenv->envname)) {
            char errstr[64 + (2 * MAX_DBNAME_LENGTH)];
            snprintf(errstr, sizeof(errstr),
                     "DB name mismatch query:%s actual:%s", sql_query->dbname,
                     dbenv->envname);
            logmsg(LOGMSG_ERROR, "%s\n", errstr);
            struct fsqlresp resp;

            resp.response = FSQL_COLUMN_DATA;
            resp.flags = 0;
            resp.rcode = CDB2__ERROR_CODE__WRONG_DB;
            fsql_write_response(&clnt, &resp, (void *)errstr,
                                strlen(errstr) + 1, 1 /*flush*/, __func__,
                                __LINE__);
            goto done;
        }

        if (sql_query->client_info) {
            if (clnt.conninfo.pid &&
                clnt.conninfo.pid != sql_query->client_info->pid) {
                /* Different pid is coming without reset. */
                logmsg(LOGMSG_WARN,
                       "Multiple processes using same socket PID 1 %d "
                       "PID 2 %d Host %.8x\n",
                       clnt.conninfo.pid, sql_query->client_info->pid,
                       sql_query->client_info->host_id);
            }
            clnt.conninfo.pid = sql_query->client_info->pid;
            clnt.conninfo.node = sql_query->client_info->host_id;
        }

        if (process_set_commands(dbenv, &clnt))
            goto done;

        if (gbl_rowlocks && clnt.dbtran.mode != TRANLEVEL_SERIAL)
            clnt.dbtran.mode = TRANLEVEL_SNAPISOL;

        if (sql_query->little_endian) {
            clnt.have_endian = 1;
            clnt.endian = FSQL_ENDIAN_LITTLE_ENDIAN;
        } else {
            clnt.have_endian = 0;
        }

        /* avoid new accepting new queries/transaction on opened connections
           if we are incoherent (and not in a transaction). */
        if (!bdb_am_i_coherent(dbenv->bdb_env) &&
            (clnt.ctrl_sqlengine == SQLENG_NORMAL_PROCESS)) {
            logmsg(LOGMSG_ERROR,
                   "%s line %d td %u new query on incoherent node, "
                   "dropping socket\n",
                   __func__, __LINE__, (uint32_t)pthread_self());
            goto done;
        }

        clnt.heartbeat = 1;

        if (clnt.had_errors && strncasecmp(clnt.sql, "commit", 6) &&
            strncasecmp(clnt.sql, "rollback", 8)) {
            if (clnt.in_client_trans == 0) {
                clnt.had_errors = 0;
                /* tell blobmem that I want my priority back
                   when the sql thread is done */
                comdb2bma_pass_priority_back(blobmem);
                rc = dispatch_sql_query(&clnt);
            } else {
                /* Do Nothing */
                send_heartbeat(&clnt);
            }
        } else if (clnt.had_errors) {
            /* Do Nothing */
            if (clnt.ctrl_sqlengine == SQLENG_STRT_STATE)
                clnt.ctrl_sqlengine = SQLENG_NORMAL_PROCESS;

            clnt.had_errors = 0;
            clnt.in_client_trans = 0;
            rc = -1;
        } else {
            /* tell blobmem that I want my priority back
               when the sql thread is done */
            comdb2bma_pass_priority_back(blobmem);
            rc = dispatch_sql_query(&clnt);
        }

        if (clnt.osql.replay == OSQL_RETRY_DO) {
            if (clnt.trans_has_sp == 0) {
                srs_tran_replay(&clnt, arg->thr_self);
            } else {
                osql_set_replay(__FILE__, __LINE__, &clnt, OSQL_RETRY_NONE);
                srs_tran_destroy(&clnt);
            }
        } else {
            /* if this transaction is done (marked by SQLENG_NORMAL_PROCESS),
               clean transaction sql history
            */
            if (clnt.osql.history &&
                clnt.ctrl_sqlengine == SQLENG_NORMAL_PROCESS)
                srs_tran_destroy(&clnt);
        }

        if (rc && !clnt.in_client_trans)
            goto done;

        pthread_mutex_lock(&clnt.wait_mutex);
        if (clnt.query) {
            if (clnt.added_to_hist == 1) {
                clnt.query = NULL;
            } else {
                cdb2__query__free_unpacked(clnt.query, &pb_alloc);
                clnt.query = NULL;
            }
        }
        pthread_mutex_unlock(&clnt.wait_mutex);

        query = read_newsql_query(dbenv, &clnt, sb);
    }

done:
    if (clnt.ctrl_sqlengine == SQLENG_INTRANS_STATE) {
        handle_sql_intrans_unrecoverable_error(&clnt);
    }

    close_sp(&clnt);
    osql_clean_sqlclntstate(&clnt);

    if (clnt.dbglog) {
        sbuf2close(clnt.dbglog);
        clnt.dbglog = NULL;
    }

    if (clnt.query) {
        if (clnt.added_to_hist == 1) {
            clnt.query = NULL;
        } else {
            cdb2__query__free_unpacked(clnt.query, &pb_alloc);
            clnt.query = NULL;
        }
    }

    /* XXX free logical tran?  */
    close_appsock(sb);

    clnt.dbtran.mode = TRANLEVEL_INVALID;
    set_high_availability(&clnt, 0);
    // clnt.high_availability = 0;
    if (clnt.query_stats)
        free(clnt.query_stats);

    pthread_mutex_destroy(&clnt.wait_mutex);
    pthread_cond_destroy(&clnt.wait_cond);
    pthread_mutex_destroy(&clnt.write_lock);
    pthread_mutex_destroy(&clnt.dtran_mtx);

    return 0;
}

comdb2_appsock_t newsql_plugin = {
    "newsql",                /* Name */
    "",                      /* Usage info */
    0,                       /* Execution count */
    APPSOCK_FLAG_CACHE_CONN, /* Flags */
    handle_newsql_request    /* Handler function */
};

#include "plugin.h"
