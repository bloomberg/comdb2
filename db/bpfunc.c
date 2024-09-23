#include <pb_alloc.h>
#include <bpfunc.h>
#include <comdb2.h>
#include <osqlcomm.h>
#include <views.h>
#include <net_types.h>
#include <bdb_access.h>
#include <bpfunc.pb-c.h>
#include <bdb_schemachange.h>
#include <logmsg.h>
#include <sys/stat.h>
#include <unistd.h>
#include "logical_cron.h"
#include "db_access.h" /* gbl_check_access_controls */
#include "alias.h"

/* Automatically create 'default' user when authentication is enabled. */
int gbl_create_default_user;

/*                           FUNCTION DECLARATIONS */

static int prepare_methods(bpfunc_t *func, bpfunc_info *info);
/*static int prepare_create_timepart(bpfunc_t *tp);
static int prepare_create_timepart(bpfunc_t *tp);
static int prepare_drop_timepart(bpfunc_t *tp);
static int prepare_timepart_retention(bpfunc_t *tp);
static int exec_grant(void *tran, bpfunc_t *func, struct errstat *err);
static int exec_authentication(void *tran, bpfunc_t *func, struct errstat *err);
static int exec_password(void *tran, bpfunc_t *func, struct errstat *err);
static int exec_alias(void *tran, bpfunc_t *func, struct errstat *err);
static int exec_analyze_threshold(void *tran, bpfunc_t *func,
                                  struct errstat *err);
static int exec_analyze_coverage(void *tran, bpfunc_t *func,
                                 struct errstat *err);
static int exec_rowlocks_enable(void *tran, bpfunc_t *func,
                                struct errstat *err);
static int exec_genid48_enable(void *tran, bpfunc_t *func, struct errstat *err);
static int exec_set_skipscan(void *tran, bpfunc_t *func, struct errstat *err);
static int exec_delete_from_sc_history(void *tran, bpfunc_t *func, struct errstat *err);
*/
extern int bulk_import_do_import(const char *srcdb, const char *src_tablename,
                                 const char *dst_tablename);

/********************      UTILITIES     ***********************/

static int empty(void *tran, bpfunc_t *func, struct errstat *err)
{
    return 0;
}

void free_bpfunc(bpfunc_t *func)
{
    if (unlikely(!func)) return;
    free_bpfunc_arg(func->arg);
    if (func)
        free(func);
}

void free_bpfunc_arg(BpfuncArg *arg)
{
    bpfunc_arg__free_unpacked(arg, &pb_alloc);
}

static int init_bpfunc(bpfunc_t *bpf)
{
    memset(bpf, 0, sizeof(*bpf));
    return 0;
}

static int _get_bpfunc(bpfunc_t *func, int32_t data_len, const uint8_t *data)
{
    init_bpfunc(func);

    func->arg = bpfunc_arg__unpack(&pb_alloc, data_len, data);

    if (!func->arg)
        return -1;

    return 0;
}

int bpfunc_prepare(bpfunc_t **f, int32_t data_len, uint8_t *data,
                   bpfunc_info *info)
{
    bpfunc_t *func = *f = (bpfunc_t *)malloc(sizeof(bpfunc_t));

    if (func == NULL)
        goto end;

    if (_get_bpfunc(func, data_len, data))
        goto fail_arg;

    if (!prepare_methods(func, info))
        return 0;

fail_arg:
    free_bpfunc(func);
end:
    return -1;
}

/* unpack a serialized bpfunc and check if the type matches the argument */
int bpfunc_check(const uint8_t *data, int32_t data_len, int type)
{
    bpfunc_t func = {0};
    int ret;

    if (_get_bpfunc(&func, data_len, data))
        return -1;

    ret = type == func.arg->type;

    free_bpfunc_arg(func.arg);

    return ret;
}

/******************************** TIME PARTITIONS
 * ***********************************/

/************************ CREATE TIME PARTITIONS
 * ***********************************/

int exec_create_timepart(void *tran, bpfunc_t *func, struct errstat *err)
{
    char *json_cmd = NULL;
    int len = 0;
    int rc;

    build_createcmd_json(
        &json_cmd, &len, func->arg->crt_tp->partition_name,
        func->arg->crt_tp->tablename, func->arg->crt_tp->period,
        func->arg->crt_tp->retention, func->arg->crt_tp->start);
    assert(json_cmd);
    rc = views_do_partition(tran, thedb->timepart_views,
                            func->arg->crt_tp->partition_name, json_cmd, err);

    free(json_cmd);

    return rc;
}

int success_create_timepart(void *tran, bpfunc_t *func, struct errstat *err)
{
    int rc = 0;
    int bdberr = 0;

    rc = bdb_llog_views(thedb->bdb_env, func->arg->crt_tp->partition_name, 1,
                        &bdberr);
    if (rc)
        errstat_set_rcstrf(err, rc, "%s -- bdb_llog_views rc:%d bdberr:%d",
                           __func__, rc, bdberr);
    return rc;
}

static int prepare_create_timepart(bpfunc_t *tp)
{
    tp->exec = exec_create_timepart;
    tp->success = success_create_timepart;

    return 0;
}

/************************ DROP TIME PARTITIONS
 * ***********************************/

int exec_drop_timepart(void *tran, bpfunc_t *func, struct errstat *err)
{
    char *json_cmd = NULL;
    int len = 0;
    int rc;

    build_dropcmd_json(&json_cmd, &len, func->arg->drop_tp->partition_name);

    assert(json_cmd);

    rc = views_do_partition(tran, thedb->timepart_views,
                            func->arg->drop_tp->partition_name, json_cmd, err);

    free(json_cmd);

    return rc;
}

int success_drop_timepart(void *tran, bpfunc_t *func, struct errstat *err)
{
    int rc = 0;
    int bdberr = 0;

    rc = bdb_llog_views(thedb->bdb_env, func->arg->drop_tp->partition_name, 1,
                        &bdberr);
    if (rc)
        errstat_set_rcstrf(err, rc, "%s -- bdb_llog_views rc:%d bdberr:%d",
                           __func__, rc, bdberr);
    return rc;
}

static int prepare_drop_timepart(bpfunc_t *tp)
{
    tp->exec = exec_drop_timepart;
    tp->success = success_drop_timepart;

    return 0;
}

/*********************** GRANT ****************************/

static int grantAuth(void *tran, int permission, int command_type,
                     char *tablename, char *userschema, char *username)
{
    int rc = 0;
    int bdberr = 0;
    bdb_state_type *bdb_state = thedb->bdb_env;
    switch (permission) {
    case AUTH_READ:
        rc = bdb_tbl_access_read_set(bdb_state, tran, tablename, username,
                                     &bdberr);
        if (!rc) {
            rc = timepart_shards_grant_access(bdb_state, tran, tablename,
                                              username, ACCESS_READ);
        }
        break;
    case AUTH_WRITE:
        rc = bdb_tbl_access_write_set(bdb_state, tran, tablename, username,
                                      &bdberr);
        if (!rc) {
            rc = timepart_shards_grant_access(bdb_state, tran, tablename,
                                              username, ACCESS_WRITE);
        }
        break;
    case AUTH_USERSCHEMA:
        rc = bdb_tbl_access_userschema_set(bdb_state, tran, userschema,
                                           username, &bdberr);
        break;
    case AUTH_OP:
        rc = bdb_tbl_op_access_set(bdb_state, tran, command_type, tablename,
                                   username, &bdberr);
        if (!rc) {
            rc = timepart_shards_grant_access(bdb_state, tran, tablename,
                                              username, ACCESS_DDL);
        }
        break;
    default:
        rc = SQLITE_INTERNAL;
        break;
    }

    if (bdberr == BDBERR_ADD_DUPE)
        rc = 0;

    return rc;
}

static int revokeAuth(void *tran, int permission, int command_type,
                      char *tablename, char *userschema, char *username)
{
    int rc = 0;
    int bdberr = 0;
    bdb_state_type *bdb_state = thedb->bdb_env;
    switch (permission) {
    case AUTH_READ:
        rc = bdb_tbl_access_read_delete(bdb_state, tran, tablename, username,
                                        &bdberr);
        if (!rc) {
            rc = timepart_shards_revoke_access(bdb_state, tran, tablename,
                                               username, ACCESS_READ);
        }
        break;
    case AUTH_WRITE:
        rc = bdb_tbl_access_write_delete(bdb_state, tran, tablename, username,
                                         &bdberr);
        if (!rc) {
            rc = timepart_shards_revoke_access(bdb_state, tran, tablename,
                                               username, ACCESS_WRITE);
        }
        break;
    case AUTH_OP:
        rc = bdb_tbl_op_access_delete(bdb_state, tran, command_type, tablename,
                                      username, &bdberr);
        if (!rc) {
            rc = timepart_shards_revoke_access(bdb_state, tran, tablename,
                                               username, ACCESS_DDL);
        }
        break;
    case AUTH_USERSCHEMA:
        rc = bdb_tbl_access_userschema_delete(bdb_state, tran, userschema,
                                              username, &bdberr);
        break;
    }

    if (bdberr == BDBERR_DELNOTFOUND)
        rc = 0;

    return rc;
}

int gbl_lock_dba_user = 0;

static int exec_grant(void *tran, bpfunc_t *func, struct errstat *err)
{
    BpfuncGrant *grant = func->arg->grant;
    int rc = 0;

    if (gbl_lock_dba_user &&
        (strcasecmp(grant->username, DEFAULT_DBA_USER) == 0)) {
        rc = 1;
        errstat_set_rcstrf(err, rc, "dba user is locked!");
        return rc;
    }

    if (!grant->yesno)
        rc = grantAuth(tran, grant->perm, 0, grant->table, grant->userschema,
                       grant->username);
    else
        rc = revokeAuth(tran, grant->perm, 0, grant->table, grant->userschema,
                        grant->username);

    if (rc)
        errstat_set_rcstrf(err, rc, "%s access denied", __func__);

    if (rc == 0) {
        rc = net_send_authcheck_all(thedb->handle_sibling);
    }

    ++gbl_bpfunc_auth_gen;

    return rc;
}

/************************ PASSWORD *********************/

static int exec_password(void *tran, bpfunc_t *func, struct errstat *err)
{
    int rc;
    BpfuncPassword *pwd = func->arg->pwd;

    if (gbl_lock_dba_user && (strcasecmp(pwd->user, DEFAULT_DBA_USER) == 0)) {
        rc = 1;
        errstat_set_rcstrf(err, rc, "dba user is locked!");
        return rc;
    }

    rc = pwd->disable ? bdb_user_password_delete(tran, pwd->user)
                      : bdb_user_password_set(tran, pwd->user, pwd->password);

    if (rc == 0 && pwd->disable) {
        int bdberr;
        /* Delete OP access for this user. */
        rc = bdb_tbl_op_access_delete(thedb->bdb_env, tran, 0, "",
                                      pwd->user, &bdberr);

        if (rc == 0) {
            /* Also delete all the table accesses for this user. */
            rc = bdb_del_all_user_access(thedb->bdb_env, tran, pwd->user);
        }
    }

    if (rc == 0) {
        rc = net_send_authcheck_all(thedb->handle_sibling);
    }

    ++gbl_bpfunc_auth_gen;

    return rc;
}

/************************ AUTHENTICATION *********************/

static int exec_authentication(void *tran, bpfunc_t *func, struct errstat *err)
{
    BpfuncAuthentication *auth = func->arg->auth;
    int bdberr = 0;
    int valid_user;

    if (auth->enabled)
        bdb_user_password_check(tran, DEFAULT_USER, DEFAULT_PASSWORD, &valid_user);

    /* Check if there is already op password. */
    int rc = bdb_authentication_set(thedb->bdb_env, tran, auth->enabled, &bdberr);

    if (gbl_create_default_user && auth->enabled && valid_user == 0 && rc == 0)
        rc = bdb_user_password_set(tran, DEFAULT_USER, DEFAULT_PASSWORD);

    if (rc == 0)
      rc = net_send_authcheck_all(thedb->handle_sibling);

    gbl_check_access_controls = 1;
    ++gbl_bpfunc_auth_gen;

    return rc;
}

/************************ ALIAS ******************************/

static int exec_alias(void *tran, bpfunc_t *func, struct errstat *err)
{
    int rc = 0;
    char *error;
    BpfuncAlias *alias = func->arg->alias;
    if (alias->op == ALIAS_OP__CREATE) {
        rc = llmeta_set_tablename_alias(NULL, alias->name, alias->remote, &error);
    } else {
        rc = llmeta_rem_tablename_alias(alias->name, &error);
    }
    if (rc) {
        if (err) {
            errstat_set_rcstrf(err, rc, "%s", error);
            free(error);
        }
        return rc;
    }

    /* update in-mem structure */

    if (alias->op == ALIAS_OP__CREATE) 
        add_alias(alias->name, alias->remote);
    else 
        remove_alias(alias->name);
    return 0;
}


int success_alias(void *tran, bpfunc_t *func, struct errstat *err) {
     int rc = 0;
     int bdberr = 0;

     /* tell replicants to do so as well */
     rc = bdb_llog_alias(thedb->bdb_env, 1 /* wait */, &bdberr);
     if(rc)
         errstat_set_rcstrf(err, rc, "%s -- bdb_llog_views rc:%d bdberr:%d",
                            __func__, rc, bdberr);
     return rc;
}

int fail_alias(void *tran, bpfunc_t *func, struct errstat *err) {
    int rc = 0;
    char *error;
    BpfuncAlias *alias = func->arg->alias;

    if (alias->op == ALIAS_OP__CREATE) {
        rc = llmeta_rem_tablename_alias(alias->name, &error);
    } else {
        rc = llmeta_set_tablename_alias(NULL, alias->name, alias->remote, &error);
    }
    if (rc) {
        /* we should never reach here */
        abort();
    }

    /* update in-mem structure */
    if (alias->op == ALIAS_OP__CREATE) 
        remove_alias(alias->name);
    else 
        add_alias(alias->name, alias->remote);
    return rc;
}

/******************************* ANALYZE *****************************/

static int exec_analyze_threshold(void *tran, bpfunc_t *func,
                                  struct errstat *err)
{

    BpfuncAnalyzeThreshold *thr_f = func->arg->an_thr;
    int bdberr;
    int rc;

    if (thr_f->newvalue == -1)
        rc = bdb_clear_analyzethreshold_table(tran, thr_f->tablename, &bdberr);
    else
        rc = bdb_set_analyzethreshold_table(tran, thr_f->tablename,
                                            thr_f->newvalue, &bdberr);
    if (rc)
        errstat_set_rcstrf(err, rc, "%s failed bdberr %d", __func__, bdberr);
    return rc;
}

static int exec_analyze_coverage(void *tran, bpfunc_t *func,
                                 struct errstat *err)
{

    BpfuncAnalyzeCoverage *cov_f = func->arg->an_cov;
    int bdberr;
    int rc;

    if (cov_f->newvalue == -1)
        rc = bdb_clear_analyzecoverage_table(tran, cov_f->tablename, &bdberr);
    else
        rc = bdb_set_analyzecoverage_table(tran, cov_f->tablename,
                                           cov_f->newvalue, &bdberr);
    if (rc)
        errstat_set_rcstrf(err, rc, "%s failed bdberr %d", __func__, bdberr);
    return rc;
}

static int exec_timepart_retention(void *tran, bpfunc_t *func,
                                   struct errstat *err)
{
    BpfuncTimepartRetention *ret_f = func->arg->tp_ret;

    return timepart_update_retention(tran, ret_f->timepartname, ret_f->newvalue,
                                     err);
}

int success_timepart_retention(void *tran, bpfunc_t *func, struct errstat *err)
{
     int rc = 0;
     int bdberr = 0;

     rc = bdb_llog_views(thedb->bdb_env, func->arg->tp_ret->timepartname, 1,
                         &bdberr);
     if(rc)
         errstat_set_rcstrf(err, rc, "%s -- bdb_llog_views rc:%d bdberr:%d",
                            __func__, rc, bdberr);
     return rc;
}

static int prepare_timepart_retention(bpfunc_t *tp)
{
    tp->exec = exec_timepart_retention;
    tp->success = success_timepart_retention;

    return 0;
}

static int exec_set_skipscan(void *tran, bpfunc_t *func, struct errstat *err)
{
    BpfuncAnalyzeCoverage *cov_f = func->arg->an_cov;
    int bdberr;
    int rc;

    if (cov_f->newvalue == 1) //enable skipscan means clear llmeta entry
        rc = bdb_clear_table_parameter(tran, cov_f->tablename, "disableskipscan");
    else {
        const char *value = "true";
        rc = bdb_set_table_parameter(tran, cov_f->tablename, "disableskipscan", value);
    }
    if (!rc)
        bdb_llog_analyze(thedb->bdb_env, 1, &bdberr);
    if (rc)
        errstat_set_rcstrf(err, rc, "%s failed", __func__);
    return rc;
}

static int exec_genid48_enable(void *tran, bpfunc_t *func, struct errstat *err)
{
    BpfuncGenid48Enable *gn = func->arg->gn_enable;
    int format = bdb_genid_format(thedb->bdb_env);

    if (gn->enable && format == LLMETA_GENID_48BIT) {
        errstat_set_rcstrf(err, -1, "%s -- genid48 is already enabled",
                           __func__);
        return -1;
    }

    if (!gn->enable && format != LLMETA_GENID_48BIT) {
        errstat_set_rcstrf(err, -1, "%s -- genid48 is already disabled",
                           __func__);
        return -1;
    }

    /* Set flags: we'll actually set the format in the block processor */
    struct ireq *iq = (struct ireq *)func->info->iq;
    iq->osql_genid48_enable = gn->enable;
    iq->osql_flags |= OSQL_FLAGS_GENID48;
    return 0;
}

static int exec_rowlocks_enable(void *tran, bpfunc_t *func, struct errstat *err)
{
    BpfuncRowlocksEnable *rl = func->arg->rl_enable;
    int rc;

    if (rl->enable && gbl_rowlocks) {
        errstat_set_rcstrf(err, -1, "%s -- rowlocks is already enabled",
                           __func__);
        return -1;
    }

    if (!rl->enable && !gbl_rowlocks) {
        errstat_set_rcstrf(err, -1, "%s -- rowlocks is already disabled",
                           __func__);
        return -1;
    }

    rc = set_rowlocks(tran, rl->enable);
    if (!rc) {
        struct ireq *iq = (struct ireq *)func->info->iq;
        iq->osql_rowlocks_enable = rl->enable;
        iq->osql_flags |= OSQL_FLAGS_ROWLOCKS;
    } else
        errstat_set_rcstrf(err, rc, "%s -- set_rowlocks failed rc %d", __func__,
                           rc);
    return rc;
}

static int exec_delete_from_sc_history(void *tran, bpfunc_t *func,
                                       struct errstat *err)
{
    BpfuncDeleteFromScHistory *tblseed = func->arg->tblseed;
    int rc = bdb_del_schema_change_history(tran, tblseed->tablename, tblseed->seed);
    if (rc)
        errstat_set_rcstrf(err, rc, "%s failed delete", __func__);
    return rc;
}

static int exec_bulk_import(void *tran, bpfunc_t *func, struct errstat *err)
{
    const int rc = bulk_import_do_import(func->arg->bimp->srcdb, 
        func->arg->bimp->src_tablename, func->arg->bimp->dst_tablename);
    if (rc != 0) {
        errstat_set_rcstrf(err, rc, "%s Failed bulk import", __func__);
    }
    return rc;
}

static int prepare_methods(bpfunc_t *func, bpfunc_info *info)
{
    func->exec = empty;
    func->success = empty;
    func->fail = empty;
    func->info = info;

    switch (func->arg->type) {
    case BPFUNC_CREATE_TIMEPART:
        prepare_create_timepart(func);
        break;

    case BPFUNC_DROP_TIMEPART:
        prepare_drop_timepart(func);
        break;

    case BPFUNC_GRANT:
        func->exec = exec_grant;
        break;

    case BPFUNC_PASSWORD:
        func->exec = exec_password;
        break;

    case BPFUNC_AUTHENTICATION:
        func->exec = exec_authentication;
        break;

    case BPFUNC_ALIAS:
        func->exec = exec_alias;
        func->success = success_alias;
        func->fail = fail_alias;
        break;

    case BPFUNC_ANALYZE_THRESHOLD:
        func->exec = exec_analyze_threshold;
        break;

    case BPFUNC_ANALYZE_COVERAGE:
        func->exec = exec_analyze_coverage;
        break;

    case BPFUNC_TIMEPART_RETENTION:
        prepare_timepart_retention(func);
        break;

    case BPFUNC_ROWLOCKS_ENABLE:
        func->exec = exec_rowlocks_enable;
        break;

    case BPFUNC_GENID48_ENABLE:
        func->exec = exec_genid48_enable;
        break;

    case BPFUNC_SET_SKIPSCAN:
        func->exec = exec_set_skipscan;
        break;

    case BPFUNC_DELETE_FROM_SC_HISTORY:
        func->exec = exec_delete_from_sc_history;
        break;

    case BPFUNC_BULK_IMPORT:
        func->exec = exec_bulk_import;
        break;

    default:
        logmsg(LOGMSG_ERROR, "Unknown function_id in bplog function\n");
        return -1;
        break;
    }

    return 0;
}
