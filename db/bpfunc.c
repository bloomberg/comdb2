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

extern int gbl_check_access_controls;

/*                           FUNCTION DECLARATIONS */

static int prepare_create_timepart(bpfunc_t *tp);
static int prepare_drop_timepart(bpfunc_t *tp);
static int prepare_timepart_retention(bpfunc_t *tp);
static int exec_grant(void *tran, bpfunc_t *func, char *err);
static int exec_authentication(void *tran, bpfunc_t *func, char *err);
static int exec_password(void *tran, bpfunc_t *func, char *err);
static int exec_alias(void *tran, bpfunc_t *func, char *err);
static int exec_analyze_threshold(void *tran, bpfunc_t *func, char *err);
static int exec_analyze_coverage(void *tran, bpfunc_t *func, char *err);
static int exec_rowlocks_enable(void *tran, bpfunc_t *func, char *err);
static int exec_genid48_enable(void *tran, bpfunc_t *func, char *err);
static int exec_set_skipscan(void *tran, bpfunc_t *func, char *err);
/********************      UTILITIES     ***********************/

static int empty(void *tran, bpfunc_t *func, char *err) { return 0; }

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

    default:
        logmsg(LOGMSG_ERROR, "Unknown function_id in bplog function\n");
        return -1;
        break;
    }

    return 0;
}

int bpfunc_prepare(bpfunc_t **f, void *tran, int32_t data_len,
        uint8_t *data, bpfunc_info *info)
{
    bpfunc_t *func = *f = (bpfunc_t *)malloc(sizeof(bpfunc_t));

    if (func == NULL)
        goto end;

    init_bpfunc(func);

    func->arg = bpfunc_arg__unpack(&pb_alloc, data_len, data);

    if (!func->arg)
        goto fail_arg;

    if (!prepare_methods(func, info))
        return 0;

fail_arg:
    free_bpfunc(func);
end:
    return -1;
}
/******************************** TIME PARTITIONS
 * ***********************************/

/************************ CREATE TIME PARTITIONS
 * ***********************************/

int exec_create_timepart(void *tran, bpfunc_t *func, char *err)
{
    char *json_cmd = NULL;
    char cmd[256];
    int len = 0;
    int rc;
    struct errstat errst;

    build_createcmd_json(
        &json_cmd, &len, func->arg->crt_tp->partition_name,
        func->arg->crt_tp->tablename, func->arg->crt_tp->period,
        func->arg->crt_tp->retention, func->arg->crt_tp->start);
    assert(json_cmd);
    rc =
        views_do_partition(tran, thedb->timepart_views,
                           func->arg->crt_tp->partition_name, json_cmd, &errst);

    free(json_cmd);

    return rc;
}

int success_create_timepart(void *tran, bpfunc_t *func, char *err)
{
    int rc = 0;
    int bdberr = 0;

    rc = bdb_llog_views(thedb->bdb_env, func->arg->crt_tp->partition_name, 1,
                        &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s -- bdb_llog_views rc:%d bdberr:%d\n", __func__, rc,
               bdberr);
    }

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

int exec_drop_timepart(void *tran, bpfunc_t *func, char *err)
{
    char *json_cmd = NULL;
    char cmd[256];
    int len = 0;
    int rc;
    struct errstat errst;

    build_dropcmd_json(&json_cmd, &len, func->arg->drop_tp->partition_name);

    assert(json_cmd);

    rc = views_do_partition(tran, thedb->timepart_views,
                            func->arg->drop_tp->partition_name, json_cmd,
                            &errst);

    free(json_cmd);

    return rc;
}

int success_drop_timepart(void *tran, bpfunc_t *func, char *err)
{
    int rc = 0;
    int bdberr = 0;

    rc = bdb_llog_views(thedb->bdb_env, func->arg->drop_tp->partition_name, 1,
                        &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s -- bdb_llog_views rc:%d bdberr:%d\n", __func__, rc,
                bdberr);
    }

    return rc;
}

static int prepare_drop_timepart(bpfunc_t *tp)
{
    tp->exec = exec_drop_timepart;
    tp->success = success_drop_timepart;

    return 0;
}

/*********************************** GRANT
 * ***********************************************/

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
        break;
    case AUTH_WRITE:
        rc = bdb_tbl_access_write_set(bdb_state, tran, tablename, username,
                                      &bdberr);
        break;
    case AUTH_USERSCHEMA:
        rc = bdb_tbl_access_userschema_set(bdb_state, tran, userschema,
                                           username, &bdberr);
        break;
    case AUTH_OP:
        rc = bdb_tbl_op_access_set(bdb_state, tran, command_type, tablename,
                                   username, &bdberr);
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
        break;
    case AUTH_WRITE:
        rc = bdb_tbl_access_write_delete(bdb_state, tran, tablename, username,
                                         &bdberr);
        break;
    case AUTH_OP:
        rc = bdb_tbl_op_access_delete(bdb_state, tran, command_type, tablename,
                                      username, &bdberr);
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

static int exec_grant(void *tran, bpfunc_t *func, char *err)
{

    BpfuncGrant *grant = func->arg->grant;
    int rc = 0;

    if (!grant->yesno)
        rc = grantAuth(tran, grant->perm, 0, grant->table, grant->userschema,
                       grant->username);
    else
        rc = revokeAuth(tran, grant->perm, 0, grant->table, grant->userschema,
                        grant->username);

    return rc;
}

/************************ PASSWORD *********************/

static int exec_password(void *tran, bpfunc_t *func, char *err)
{
    BpfuncPassword *pwd = func->arg->pwd;
    return pwd->disable ? bdb_user_password_delete(tran, pwd->user)
                        : bdb_user_password_set(tran, pwd->user, pwd->password);
}

/************************ AUTHENTICATION *********************/

static int exec_authentication(void *tran, bpfunc_t *func, char *err)
{
    BpfuncAuthentication *auth = func->arg->auth;
    int bdberr = 0;
    /* Check if there is already op password. */
    int rc = bdb_authentication_set(thedb->bdb_env, tran, auth->enabled, &bdberr);
    if (rc == 0)
      rc = net_send_authcheck_all(thedb->handle_sibling);
    gbl_check_access_controls = 1;
    return rc;
}

/************************ ALIAS ******************************/

static int exec_alias(void *tran, bpfunc_t *func, char *err)
{
    int rc = 0;
    char *error;
    BpfuncAlias *alias = func->arg->alias;

    rc = llmeta_set_tablename_alias(NULL, alias->name, alias->remote, &error);

    if (error) {
        // todo should be send upstream ....
        free(error);
    }

    return rc;
}

/******************************* ANALYZE *****************************/

static int exec_analyze_threshold(void *tran, bpfunc_t *func, char *err)
{

    BpfuncAnalyzeThreshold *thr_f = func->arg->an_thr;
    int bdberr;
    int rc;

    if (thr_f->newvalue == -1)
        rc = bdb_clear_analyzethreshold_table(tran, thr_f->tablename, &bdberr);
    else
        rc = bdb_set_analyzethreshold_table(tran, thr_f->tablename,
                                            thr_f->newvalue, &bdberr);

    // TODO bdberr should not be ignored also a better way to pass err msg
    // upstream
    // would be nice

    return rc;
}


static int exec_analyze_coverage(void *tran, bpfunc_t *func, char *err)
{

    BpfuncAnalyzeCoverage *cov_f = func->arg->an_cov;
    int bdberr;
    int rc;

    if (cov_f->newvalue == -1)
        rc = bdb_clear_analyzecoverage_table(tran, cov_f->tablename, &bdberr);
    else
        rc = bdb_set_analyzecoverage_table(tran, cov_f->tablename,
                                           cov_f->newvalue, &bdberr);

    // TODO bdberr should not be ignored also a better way to pass err msg
    // upstream
    // would be nice
    return rc;
}

static int exec_timepart_retention(void *tran, bpfunc_t *func, char *err)
{
    BpfuncTimepartRetention *ret_f = func->arg->tp_ret;
    struct errstat xerr = {0};
    int rc;

    rc = timepart_update_retention( tran, ret_f->timepartname, ret_f->newvalue, &xerr);

    // TODO bdberr should not be ignored also a better way to pass err msg upstream
    // would be nice
    return rc;
}

int success_timepart_retention(void *tran, bpfunc_t *func, char *err)
{
     int rc = 0;
     int bdberr = 0;

     rc = bdb_llog_views(thedb->bdb_env, func->arg->tp_ret->timepartname, 1, &bdberr);
     if(rc)
     {
        logmsg(LOGMSG_ERROR, "%s -- bdb_llog_views rc:%d bdberr:%d\n",
               __func__, rc, bdberr);
     }
  
    return rc;
}

static int prepare_timepart_retention(bpfunc_t *tp)
{
    tp->exec = exec_timepart_retention;
    tp->success = success_timepart_retention;

    return 0;
}

static int exec_set_skipscan(void *tran, bpfunc_t *func, char *err)
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
    bdb_llog_analyze(thedb->bdb_env, 1, &bdberr);
    return rc;
}

static int exec_genid48_enable(void *tran, bpfunc_t *func, char *err)
{
    BpfuncGenid48Enable *gn = func->arg->gn_enable;
    int format = bdb_genid_format(thedb->bdb_env), rc;

    if (gn->enable && format == LLMETA_GENID_48BIT) {
        fprintf(stderr, "%s -- genid48 is already enabled\n", __func__);
        return -1;
    }

    if (!gn->enable && format != LLMETA_GENID_48BIT) {
        fprintf(stderr, "%s -- genid48 is already disabled\n", __func__);
        return -1;
    }

    /* Set flags: we'll actually set the format in the block processor */
    struct ireq *iq = (struct ireq *)func->info->iq;
    iq->osql_genid48_enable = gn->enable;
    bset(&iq->osql_flags, OSQL_FLAGS_GENID48);
    return 0;
}

static int exec_rowlocks_enable(void *tran, bpfunc_t *func, char *err)
{
    BpfuncRowlocksEnable *rl = func->arg->rl_enable;
    int rc;

    if (rl->enable && gbl_rowlocks) {
        fprintf(stderr, "%s -- rowlocks is already enabled\n", __func__);
        return -1;
    }

    if (!rl->enable && !gbl_rowlocks) {
        fprintf(stderr, "%s -- rowlocks is already disabled\n", __func__);
        return -1;
    }

    rc = set_rowlocks(tran, rl->enable);
    if (!rc) {
        struct ireq *iq = (struct ireq *)func->info->iq;
        iq->osql_rowlocks_enable = rl->enable;
        bset(&iq->osql_flags, OSQL_FLAGS_ROWLOCKS);
    }
    return rc;
}

