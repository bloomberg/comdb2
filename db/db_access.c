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

#include "comdb2.h"
#include "comdb2_atomic.h"
#include "sql.h"
#include "bdb_api.h"
#include "bdb_access.h"
#include "sql_stmt_cache.h"
#include "views.h"
#include "debug_switches.h"

int gbl_check_access_controls;
int gbl_allow_anon_id_for_spmux = 0;
extern ssl_mode gbl_client_ssl_mode;

static void check_auth_enabled(struct dbenv *dbenv)
{
    int rc;
    int bdberr;

    rc = bdb_authentication_get(dbenv->bdb_env, NULL, &bdberr);
    if (rc) {
        gbl_uses_password = 0;
        logmsg(LOGMSG_WARN, "user authentication disabled (bdberr: %d)\n",
               bdberr);
        return;
    }

    if (!gbl_uses_password) {
        gbl_uses_password = 1;
        logmsg(LOGMSG_INFO, "User authentication enabled\n");
    }
}

/* Check whether access controls have been enabled. */
void check_access_controls(struct dbenv *dbenv)
{
    check_auth_enabled(dbenv);
}

int reject_anon_id(struct sqlclntstate *clnt)
{
    ATOMIC_ADD64(gbl_num_auth_denied, 1);
    logmsg(LOGMSG_ERROR, "Client %s pid:%d mach:%d use of Anonymous ID is disallowed\n",
           clnt->argv0 ? clnt->argv0 : "???", clnt->conninfo.pid, clnt->conninfo.node);
    write_response(clnt, RESPONSE_ERROR, "authentication data not found", CDB2ERR_ACCESS);
    return SQLITE_AUTH;
}

int (*externalComdb2AuthenticateUserMakeRequest)(void *, const char *) = NULL;

void get_client_origin(char *out, size_t outlen, struct sqlclntstate *clnt) {
    snprintf(out, outlen,
            "%s:origin:%s:pid:%d",
            clnt->argv0 ? clnt->argv0 : "?",
            clnt->origin ? clnt->origin: "?",
            clnt->conninfo.pid);
} 

int gbl_fdb_auth_error = 0;

/* If user password does not match this function
 * will write error response and return a non 0 rc
 */
int check_user_password(struct sqlclntstate *clnt)
{
    int password_rc = 0;
    int valid_user;
    static int remsql_warned = 0;

    if (gbl_uses_password) {
        if (!clnt->current_user.have_name) {
            clnt->current_user.have_name = 1;
            strcpy(clnt->current_user.name, DEFAULT_USER);
        }

        if (!clnt->current_user.have_password) {
            clnt->current_user.have_password = 1;
            strcpy(clnt->current_user.password, DEFAULT_PASSWORD);
        }
    }

    if ((gbl_uses_externalauth || gbl_uses_externalauth_connect) &&
            (externalComdb2AuthenticateUserMakeRequest || debug_switch_ignore_null_auth_func()) &&
            !clnt->admin && !clnt->current_user.bypass_auth) {
        clnt->authdata = get_authdata(clnt);
        if (clnt->allow_make_request)
            return 0;
        if (!clnt->authdata && clnt->secure && !gbl_allow_anon_id_for_spmux)
            return reject_anon_id(clnt);
        if (gbl_externalauth_warn && !clnt->authdata) {
            logmsg(LOGMSG_INFO, "Client %s pid:%d mach:%d is missing authentication data\n",
                   clnt->argv0 ? clnt->argv0 : "???", clnt->conninfo.pid, clnt->conninfo.node);
            return 0;
        }
        if (debug_switch_ignore_null_auth_func()) {
            return 0;
        }
        char client_info[1024];
        get_client_origin(client_info, sizeof(client_info), clnt);
        int rc = externalComdb2AuthenticateUserMakeRequest(clnt->authdata, client_info);
        if (rc) {
            ATOMIC_ADD64(gbl_num_auth_denied, 1);
            char errstr[1024];
            snprintf(errstr, sizeof(errstr),
                     "User %s isn't allowed to make request on this db",
                     clnt->externalAuthUser ? clnt->externalAuthUser : "");
            write_response(clnt, RESPONSE_ERROR,
                           errstr,
                           CDB2ERR_ACCESS);
         } else {
             clnt->allow_make_request = 1;
             ATOMIC_ADD64(gbl_num_auth_allowed, 1);
         }
         return rc;
    }

    if ((!remsql_warned || gbl_fdb_auth_error) && (!gbl_uses_password && !gbl_uses_externalauth) &&
        ((clnt->remsql_set.is_remsql != NO_REMSQL) || clnt->features.have_sqlite_fmt)) {
        char *dbname = clnt->remsql_set.srcdbname ? clnt->remsql_set.srcdbname : "fdb_push";
        char errstr[1024];
        snprintf(errstr, sizeof(errstr),
               "Remote sql being used on database with authentication disabled, please enable IAM on this database. sourcedb:%s",
               dbname);
        if (!remsql_warned) {
            logmsg(LOGMSG_WARN, "%s\n", errstr);
            remsql_warned = 1;
        }
        if (gbl_fdb_auth_error) {
            ATOMIC_ADD64(gbl_num_auth_denied, 1);
            write_response(clnt, RESPONSE_ERROR,
               errstr,
               CDB2ERR_ACCESS);
            return 1;
        }
    }

    if (!gbl_uses_password || clnt->current_user.bypass_auth) {
        return 0;
    }

    tran_type *tran = curtran_gettran();
    password_rc =
        bdb_user_password_check(tran, clnt->current_user.name,
                                clnt->current_user.password, &valid_user);
    curtran_puttran(tran);

    if (password_rc != 0) {
        ATOMIC_ADD64(gbl_num_auth_denied, 1);
        write_response(clnt, RESPONSE_ERROR_ACCESS, "access denied", 0);
        return 1;
    }
    ATOMIC_ADD64(gbl_num_auth_allowed, 1);
    return 0;
}

/* Return current authenticated user for the session */
char *get_current_user(struct sqlclntstate *clnt)
{
    if (clnt && !clnt->current_user.is_x509_user &&
        clnt->current_user.have_name) {
        return clnt->current_user.name;
    }
    return NULL;
}

void reset_user(struct sqlclntstate *clnt)
{
    if (!clnt)
        return;
    bzero(&clnt->current_user, sizeof(clnt->current_user));
}

int check_sql_access(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    int rc, bpfunc_auth_gen = gbl_bpfunc_auth_gen;

    /* Enable global access control flags, if not done already. */
    if (gbl_check_access_controls) {
        check_access_controls(thedb);
        gbl_check_access_controls = 0;
    }

    /* Free pass if our authentication gen is up-to-date. */
    if (clnt->authgen == bpfunc_auth_gen)
        return 0;

    /* Allow the user, if
       1) this is an SSL connection, and
       2) client sends a certificate, and
       3) client does not override the user
    */
    if (clnt->plugin.has_x509(clnt) && clnt->current_user.is_x509_user)
        rc = 0;
    else
        rc = check_user_password(clnt);

    if ((gbl_uses_externalauth || gbl_uses_externalauth_connect) && externalComdb2AuthenticateUserMakeRequest && !clnt->admin) {
        if (rc == 0 && gbl_uses_externalauth_connect)
            clnt->authgen = bpfunc_auth_gen;
        return rc;
    }

    if (rc == 0) {
        if (thd->have_lastuser &&
            strcmp(thd->lastuser, clnt->current_user.name) != 0) {
            stmt_cache_reset(thd->stmt_cache);
        }
        thd->have_lastuser = 1;
        strcpy(thd->lastuser, clnt->current_user.name);
        clnt->authgen = bpfunc_auth_gen;
    } else {
        clnt->authgen = 0;
    }

    return rc;
}

int (*externalComdb2AuthenticateUserRead)(void *, const char *tablename, const char *) = NULL;
int (*externalComdb2AuthenticateUserWrite)(void *, const char *tablename, const char *) = NULL;

int access_control_check_sql_write(struct BtCursor *pCur,
                                   struct sql_thread *thd)
{
    int rc = 0;
    int bdberr = 0;
    void *authdata;

    struct sqlclntstate *clnt = thd->clnt;

    if (pCur->permissions & ACCESS_WRITE) {
        return 0;
    }

    const char *table_name = NULL;

    if (pCur && pCur->db)
        table_name = pCur->db->timepartition_name ? pCur->db->timepartition_name
                                                  : pCur->db->tablename;

    /* For remote cursors (no local db), skip local access checks */
    if (!table_name)
        return 0;

    if (clnt->authz_write_tables && hash_find_readonly(clnt->authz_write_tables, table_name)) {
        return 0;
    }

    if (gbl_uses_externalauth && !clnt->admin && (thd->clnt->in_sqlite_init == 0) &&
        externalComdb2AuthenticateUserWrite && !clnt->current_user.bypass_auth) {
        if ((authdata = get_authdata(clnt)) != NULL)
            clnt->authdata = authdata;
        char client_info[1024];
        snprintf(client_info, sizeof(client_info),
                 "%s:origin:%s:pid:%d",
                 clnt->argv0 ? clnt->argv0 : "?",
                 clnt->origin ? clnt->origin: "?",
                 clnt->conninfo.pid);
        if (!clnt->authdata && clnt->secure && !gbl_allow_anon_id_for_spmux) {
            return reject_anon_id(clnt);
        }
        if (gbl_externalauth_warn && !clnt->authdata) {
            logmsg(LOGMSG_INFO, "Client %s pid:%d mach:%d is missing authentication data\n",
                   clnt->argv0 ? clnt->argv0 : "???", clnt->conninfo.pid, clnt->conninfo.node);
        } else if (externalComdb2AuthenticateUserWrite(clnt->authdata, table_name, client_info)) {
            ATOMIC_ADD64(gbl_num_auth_denied, 1);
            char msg[1024];
            snprintf(msg, sizeof(msg), "Write access denied to table %s for user %s",
                      table_name, clnt->externalAuthUser ? clnt->externalAuthUser : "");
            logmsg(LOGMSG_INFO, "%s\n", msg);
            errstat_set_rc(&thd->clnt->osql.xerr, SQLITE_ACCESS);
            errstat_set_str(&thd->clnt->osql.xerr, msg);
            return SQLITE_ABORT;
        }
    } else {
        /* Check read access if its not user schema. */
        /* Check it only if engine is open already. */
        if (gbl_uses_password && !clnt->current_user.bypass_auth && (thd->clnt->in_sqlite_init == 0)) {
            rc = bdb_check_user_tbl_access(
                pCur->db->dbenv->bdb_env, thd->clnt->current_user.name,
                pCur->db->tablename, ACCESS_WRITE, &bdberr);
            if (rc != 0) {
                ATOMIC_ADD64(gbl_num_auth_denied, 1);
                char msg[1024];
                snprintf(msg, sizeof(msg),
                         "Write access denied to %s for user %s bdberr=%d",
                         table_name, thd->clnt->current_user.name, bdberr);
                logmsg(LOGMSG_INFO, "%s\n", msg);
                errstat_set_rc(&thd->clnt->osql.xerr, SQLITE_ACCESS);
                errstat_set_str(&thd->clnt->osql.xerr, msg);

                return SQLITE_ABORT;
            }
        }
    }
    ATOMIC_ADD64(gbl_num_auth_allowed, 1);
    pCur->permissions |= ACCESS_WRITE;
    if (clnt->authz_write_tables) {
        hash_add(clnt->authz_write_tables, strdup(table_name));
    }
    return 0;
}

int access_control_check_sql_read(struct BtCursor *pCur, struct sql_thread *thd, char *rscName)
{
    int rc = 0;
    int bdberr = 0;
    void *authdata = NULL;

    struct sqlclntstate *clnt = thd->clnt;

    if (pCur && pCur->cursor_class == CURSORCLASS_TEMPTABLE)
        return 0;
    if (pCur && pCur->permissions & ACCESS_READ) {
        return 0;
    }

    const char *table_name = NULL;

    if (pCur && pCur->db)
        table_name = pCur->db->timepartition_name ? pCur->db->timepartition_name
                                                  : pCur->db->tablename;
    else if (rscName)
        table_name = rscName;

    /* For remote cursors (no local db), skip local access checks */
    if (!table_name)
        return 0;

    if (clnt->authz_read_tables && hash_find_readonly(clnt->authz_read_tables, table_name)) {
        return 0;
    }
    /* Check read access if its not user schema. */
    /* Check it only if engine is open already. */
    if (gbl_uses_externalauth && (thd->clnt->in_sqlite_init == 0) &&
        externalComdb2AuthenticateUserRead && !clnt->admin /* not admin connection */
        && !clnt->current_user.bypass_auth /* not analyze */) {
        if ((authdata = get_authdata(clnt)) != NULL)
            clnt->authdata = authdata;
        char client_info[1024];
        snprintf(client_info, sizeof(client_info),
                 "%s:origin:%s:pid:%d",
                 clnt->argv0 ? clnt->argv0 : "?",
                 clnt->origin ? clnt->origin: "?",
                 clnt->conninfo.pid);
        if (!clnt->authdata && clnt->secure && !gbl_allow_anon_id_for_spmux)
            return reject_anon_id(clnt);
        if (gbl_externalauth_warn && !clnt->authdata) {
            logmsg(LOGMSG_INFO, "Client %s pid:%d mach:%d is missing authentication data\n",
                   clnt->argv0 ? clnt->argv0 : "???", clnt->conninfo.pid, clnt->conninfo.node);
        } else if (externalComdb2AuthenticateUserRead(clnt->authdata, table_name, client_info)) {
            ATOMIC_ADD64(gbl_num_auth_denied, 1);
            char msg[1024];
            snprintf(msg, sizeof(msg), "Read access denied to table %s for user %s",
                      table_name, clnt->externalAuthUser ? clnt->externalAuthUser : "");
            logmsg(LOGMSG_INFO, "%s\n", msg);
            errstat_set_rc(&thd->clnt->osql.xerr, SQLITE_ACCESS);
            errstat_set_str(&thd->clnt->osql.xerr, msg);
            return SQLITE_ABORT;
        }
    } else {
        if (gbl_uses_password && !clnt->current_user.bypass_auth && pCur && pCur->db &&
            thd->clnt->in_sqlite_init == 0) {
            rc = bdb_check_user_tbl_access(
                pCur->db->dbenv->bdb_env, thd->clnt->current_user.name,
                pCur->db->tablename, ACCESS_READ, &bdberr);
            if (rc != 0) {
                ATOMIC_ADD64(gbl_num_auth_denied, 1);
                char msg[1024];
                snprintf(msg, sizeof(msg),
                         "Read access denied to %s for user %s bdberr=%d",
                         table_name, thd->clnt->current_user.name, bdberr);
                logmsg(LOGMSG_INFO, "%s\n", msg);
                errstat_set_rc(&thd->clnt->osql.xerr, SQLITE_ACCESS);
                errstat_set_str(&thd->clnt->osql.xerr, msg);

                return SQLITE_ABORT;
            }
        }
    }
    if (pCur)
        pCur->permissions |= ACCESS_READ;
    ATOMIC_ADD64(gbl_num_auth_allowed, 1);
    if (clnt->authz_read_tables) {
        hash_add(clnt->authz_read_tables, strdup(table_name));
    }
    return 0;
}

static int check_tag_access(struct ireq *iq) {
    if ((gbl_uses_password || gbl_uses_externalauth) && !gbl_unauth_tag_access && !iq->authdata) {
        reqerrstr(iq, ERR_ACCESS, "Tag access denied for table %s from %s\n", iq->usedb ? iq->usedb->tablename : "???",
                  iq->corigin);
        return ERR_ACCESS;
    }

    if (SSL_IS_REQUIRED(gbl_client_ssl_mode) && !iq->has_ssl) {
        reqerrstr(iq, ERR_ACCESS, "The database requires SSL connections\n");
        return ERR_ACCESS;
    }
    return 0;
}

int access_control_check_write(struct ireq *iq, tran_type *trans, int *bdberr)
{
    int rc = 0;

    if (gbl_uses_externalauth && iq->authdata && externalComdb2AuthenticateUserRead) {
        rc = externalComdb2AuthenticateUserWrite(iq->authdata, iq->usedb->tablename, iq->corigin);
        if (rc)
            rc = ERR_ACCESS;
        return rc;
    }

    rc = check_tag_access(iq);
    if (rc)
        return ERR_ACCESS;

    return 0;
}

int access_control_check_read(struct ireq *iq, tran_type *trans, int *bdberr)
{
    int rc = 0;

    if (gbl_uses_externalauth && iq->authdata && externalComdb2AuthenticateUserRead && iq->usedb) {
        rc = externalComdb2AuthenticateUserRead(iq->authdata, iq->usedb->tablename, iq->corigin);
        if (rc)
            rc = ERR_ACCESS;
        return rc;
    }

    rc = check_tag_access(iq);
    if (rc)
        return ERR_ACCESS;

    return 0;
}

int comdb2_check_vtab_access(sqlite3 *db, sqlite3_module *module)
{
    HashElem *current;

    if (!gbl_uses_password) {
        return 0;
    }

    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;

    for (current = sqliteHashFirst(&db->aModule); current;
         current = sqliteHashNext(current)) {
        struct Module *mod = sqliteHashData(current);
        if (module != mod->pModule) {
            continue;
        }

        int bdberr;
        int rc;

        if ((module->access_flag == 0) ||
            (module->access_flag & CDB2_ALLOW_ALL)) {
            return SQLITE_OK;
        }

        if (gbl_uses_externalauth && (thd->clnt->in_sqlite_init == 0) &&
            externalComdb2AuthenticateUserRead && !clnt->admin /* not admin connection */
            && !clnt->current_user.bypass_auth /* not analyze */) {
            clnt->authdata = get_authdata(clnt);
            char client_info[1024];
            snprintf(client_info, sizeof(client_info),
                     "%s:origin:%s:pid:%d",
                     clnt->argv0 ? clnt->argv0 : "?",
                     clnt->origin ? clnt->origin: "?",
                     clnt->conninfo.pid);
            if (!clnt->authdata && clnt->secure && !gbl_allow_anon_id_for_spmux)
                return reject_anon_id(clnt);
            if (gbl_externalauth_warn && !clnt->authdata) {
                logmsg(LOGMSG_INFO, "Client %s pid:%d mach:%d is missing authentication data\n",
                       clnt->argv0 ? clnt->argv0 : "???", clnt->conninfo.pid, clnt->conninfo.node);
            } else if (externalComdb2AuthenticateUserRead(clnt->authdata, mod->zName, client_info)) {
                ATOMIC_ADD64(gbl_num_auth_denied, 1);
                char msg[1024];
                snprintf(msg, sizeof(msg), "Read access denied to table %s for user %s",
                         mod->zName, clnt->externalAuthUser ? clnt->externalAuthUser : "");
                logmsg(LOGMSG_INFO, "%s\n", msg);
                errstat_set_rc(&thd->clnt->osql.xerr, SQLITE_ACCESS);
                errstat_set_str(&thd->clnt->osql.xerr, msg);
                return SQLITE_ABORT;
            }
            return SQLITE_OK;
        } else {
            rc = bdb_check_user_tbl_access(
                thedb->bdb_env, thd->clnt->current_user.name,
                (char *)mod->zName, ACCESS_READ, &bdberr);
            if (rc != 0) {
                char msg[1024];
                snprintf(msg, sizeof(msg),
                         "Read access denied to %s for user %s bdberr=%d",
                         mod->zName, thd->clnt->current_user.name, bdberr);
                logmsg(LOGMSG_INFO, "%s\n", msg);
                errstat_set_rc(&thd->clnt->osql.xerr, SQLITE_ACCESS);
                errstat_set_str(&thd->clnt->osql.xerr, msg);
                return SQLITE_AUTH;
            }
            return SQLITE_OK;
        }
    }
    assert(0);
    return 0;
}

