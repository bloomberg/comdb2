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
#include "nodemap.h"
#include "sql_stmt_cache.h"
#include "views.h"

int gbl_check_access_controls;
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

static void check_tableXnode_enabled(struct dbenv *dbenv)
{
    int rc;
    int bdberr;

    rc = bdb_accesscontrol_tableXnode_get(dbenv->bdb_env, NULL, &bdberr);
    if (rc) {
        gbl_uses_accesscontrol_tableXnode = 0;
        return;
    }

    if (bdb_access_create(dbenv->bdb_env, &bdberr)) {
        logmsg(LOGMSG_ERROR,
               "failed to enable tableXnode control (bdberr: %d\n)", bdberr);
        gbl_uses_accesscontrol_tableXnode = 0;
        return;
    }

    gbl_uses_accesscontrol_tableXnode = 1;
    logmsg(LOGMSG_INFO, "access control tableXnode enabled\n");
}

/* Check whether access controls have been enabled. */
void check_access_controls(struct dbenv *dbenv)
{
    check_auth_enabled(dbenv);
    check_tableXnode_enabled(dbenv);
}

int (*externalComdb2AuthenticateUserMakeRequest)(void *, const char *) = NULL;

/* If user password does not match this function
 * will write error response and return a non 0 rc
 */
static int check_user_password(struct sqlclntstate *clnt)
{
    int password_rc = 0;
    int valid_user;

    if ((gbl_uses_externalauth || gbl_uses_externalauth_connect) && externalComdb2AuthenticateUserMakeRequest &&
            !clnt->admin && !clnt->current_user.bypass_auth) {
        clnt->authdata = get_authdata(clnt);
        if (gbl_externalauth_warn && !clnt->authdata) {
            logmsg(LOGMSG_INFO, "Client %s pid:%d mach:%d is missing authentication data\n",
                   clnt->argv0 ? clnt->argv0 : "???", clnt->conninfo.pid, clnt->conninfo.node);
            return 0;
        }
        char client_info[1024];
        snprintf(client_info, sizeof(client_info),
                 "%s:origin:%s:pid:%d",
                 clnt->argv0 ? clnt->argv0 : "?",
                 clnt->origin ? clnt->origin: "?",
                 clnt->conninfo.pid);
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
             ATOMIC_ADD64(gbl_num_auth_allowed, 1);
         }
         return rc;
    }

    if (!gbl_uses_password || clnt->current_user.bypass_auth) {
        return 0;
    }

    if (!clnt->current_user.have_name) {
        clnt->current_user.have_name = 1;
        strcpy(clnt->current_user.name, DEFAULT_USER);
    }

    if (!clnt->current_user.have_password) {
        clnt->current_user.have_password = 1;
        strcpy(clnt->current_user.password, DEFAULT_PASSWORD);
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

int (*externalComdb2AuthenticateUserRead)(void *, const char *tablename) = NULL;
int (*externalComdb2AuthenticateUserWrite)(void *,
                                           const char *tablename) = NULL;

int access_control_check_sql_write(struct BtCursor *pCur,
                                   struct sql_thread *thd)
{
    int rc = 0;
    int bdberr = 0;

    struct sqlclntstate *clnt = thd->clnt;

    if (pCur->permissions & ACCESS_WRITE) {
        return 0;
    }

    if (gbl_uses_accesscontrol_tableXnode) {
        rc = bdb_access_tbl_write_by_mach_get(
            pCur->db->dbenv->bdb_env, NULL, pCur->db->tablename,
            nodeix(thd->clnt->origin), &bdberr);
        if (rc <= 0) {
            char msg[1024];
            snprintf(msg, sizeof(msg),
                     "Write access denied to %s from %d bdberr=%d",
                     pCur->db->tablename, nodeix(thd->clnt->origin), bdberr);
            logmsg(LOGMSG_INFO, "%s\n", msg);
            errstat_set_rc(&thd->clnt->osql.xerr, SQLITE_ACCESS);
            errstat_set_str(&thd->clnt->osql.xerr, msg);

            return SQLITE_ABORT;
        }
    }
    const char *table_name = NULL;
    if (pCur->db)
        table_name = pCur->db->timepartition_name ? pCur->db->timepartition_name
                                                  : pCur->db->tablename;

    if (gbl_uses_externalauth && !clnt->admin && (thd->clnt->in_sqlite_init == 0) &&
        externalComdb2AuthenticateUserWrite) {
        clnt->authdata = get_authdata(clnt);
        if (gbl_externalauth_warn && !clnt->authdata)
            logmsg(LOGMSG_INFO, "Client %s pid:%d mach:%d is missing authentication data\n",
                   clnt->argv0 ? clnt->argv0 : "???", clnt->conninfo.pid, clnt->conninfo.node);
        else if (externalComdb2AuthenticateUserWrite(clnt->authdata, table_name)) {
            ATOMIC_ADD64(gbl_num_auth_denied, 1);
            char msg[1024];
            snprintf(msg, sizeof(msg), "Write access denied for table %s",
                     table_name);
            logmsg(LOGMSG_INFO, "%s\n", msg);
            errstat_set_rc(&thd->clnt->osql.xerr, SQLITE_ACCESS);
            errstat_set_str(&thd->clnt->osql.xerr, msg);
            return SQLITE_ABORT;
        }
    } else {
        /* Check read access if its not user schema. */
        /* Check it only if engine is open already. */
        if (gbl_uses_password && (thd->clnt->in_sqlite_init == 0)) {
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
    return 0;
}

int access_control_check_sql_read(struct BtCursor *pCur, struct sql_thread *thd)
{
    int rc = 0;
    int bdberr = 0;

    struct sqlclntstate *clnt = thd->clnt;

    if (pCur->cursor_class == CURSORCLASS_TEMPTABLE)
        return 0;
    if (pCur->permissions & ACCESS_READ) {
        return 0;
    }

    if (gbl_uses_accesscontrol_tableXnode) {
        rc = bdb_access_tbl_read_by_mach_get(
            pCur->db->dbenv->bdb_env, NULL, pCur->db->tablename,
            nodeix(thd->clnt->origin), &bdberr);
        if (rc <= 0) {
            char msg[1024];
            snprintf(msg, sizeof(msg),
                     "Read access denied to %s from %d bdberr=%d",
                     pCur->db->tablename, nodeix(thd->clnt->origin), bdberr);
            logmsg(LOGMSG_INFO, "%s\n", msg);
            errstat_set_rc(&thd->clnt->osql.xerr, SQLITE_ACCESS);
            errstat_set_str(&thd->clnt->osql.xerr, msg);

            return SQLITE_ABORT;
        }
    }

    const char *table_name = NULL;

    if (pCur->db)
        table_name = pCur->db->timepartition_name ? pCur->db->timepartition_name
                                                  : pCur->db->tablename;

    /* Check read access if its not user schema. */
    /* Check it only if engine is open already. */
    if (gbl_uses_externalauth && (thd->clnt->in_sqlite_init == 0) &&
        externalComdb2AuthenticateUserRead && !clnt->admin /* not admin connection */
        && !clnt->current_user.bypass_auth /* not analyze */) {
        clnt->authdata = get_authdata(clnt);
        if (gbl_externalauth_warn && !clnt->authdata)
            logmsg(LOGMSG_INFO, "Client %s pid:%d mach:%d is missing authentication data\n",
                   clnt->argv0 ? clnt->argv0 : "???", clnt->conninfo.pid, clnt->conninfo.node);
        else if (externalComdb2AuthenticateUserRead(clnt->authdata, table_name)) {
            ATOMIC_ADD64(gbl_num_auth_denied, 1);
            char msg[1024];
            snprintf(msg, sizeof(msg), "Read access denied for table %s",
                     table_name);
            logmsg(LOGMSG_INFO, "%s\n", msg);
            errstat_set_rc(&thd->clnt->osql.xerr, SQLITE_ACCESS);
            errstat_set_str(&thd->clnt->osql.xerr, msg);
            return SQLITE_ABORT;
        }
    } else {
        if (gbl_uses_password && thd->clnt->in_sqlite_init == 0) {
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

    pCur->permissions |= ACCESS_READ;
    ATOMIC_ADD64(gbl_num_auth_allowed, 1);
    return 0;
}

static int check_tag_access(struct ireq *iq) {
    if ((gbl_uses_password || gbl_uses_externalauth) && !gbl_unauth_tag_access) {
        reqerrstr(iq, ERR_ACCESS,
                  "Tag access denied for table %s from %s\n",
                  iq->usedb->tablename, iq->corigin);
        return ERR_ACCESS;
    }

    if (SSL_IS_REQUIRED(gbl_client_ssl_mode)) {
        reqerrstr(iq, ERR_ACCESS, "The database requires SSL connections\n");
        return ERR_ACCESS;
    }
    return 0;
}

int access_control_check_write(struct ireq *iq, tran_type *trans, int *bdberr)
{
    int rc = 0;

    rc = check_tag_access(iq);
    if (rc)
        return rc;

    if (gbl_uses_accesscontrol_tableXnode) {
        rc = bdb_access_tbl_write_by_mach_get(iq->dbenv->bdb_env, trans,
                                              iq->usedb->tablename,
                                              nodeix(iq->frommach), bdberr);
        if (rc <= 0) {
            reqerrstr(iq, ERR_ACCESS,
                      "Write access denied to %s from %s bdberr=%d\n",
                      iq->usedb->tablename, iq->corigin, *bdberr);
            return ERR_ACCESS;
        }
    }

    return 0;
}

int access_control_check_read(struct ireq *iq, tran_type *trans, int *bdberr)
{
    int rc = 0;

    rc = check_tag_access(iq);
    if (rc)
        return rc;

    if (gbl_uses_accesscontrol_tableXnode) {
        rc = bdb_access_tbl_read_by_mach_get(iq->dbenv->bdb_env, trans,
                                             iq->usedb->tablename,
                                             nodeix(iq->frommach), bdberr);
        if (rc <= 0) {
            reqerrstr(iq, ERR_ACCESS,
                      "Read access denied to %s from %s bdberr=%d\n",
                      iq->usedb->tablename, iq->corigin, *bdberr);
            return ERR_ACCESS;
        }
    }

    return 0;
}
