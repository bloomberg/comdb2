#include "settings.h"
#include "block_internal.h"
#include "comdb2.h"
#include "list.h"
#include "logmsg.h"
#include "plhash.h"
#include "sbuf2.h"
#include "sql.h"
#include <stdio.h>
#include <strings.h>
#include <str0.h>

/**
 * Add a new setting:
 *  1. Register the setting as accessible in REGISTER_ACC_SETTING. Add a default variable as well.
 *  2. Add new states in the set_state enum
 *  3. Make corresponding changes in int transition(set_state_mach_t *sm, char *key){}
 *  4. Define a set_<setting_name> function
 */

#define SM_ERROR_LEN 512
#define SET_ERR(...) snprintf(err, SM_ERROR_LEN, ##__VA_ARGS__);

#define SET_TOKEN_CMP(X) strncmp(key, #X, sizeof(#X) - 1) == 0
#define SET_TOKEN_CASECMP(X) strncasecmp(key, #X, sizeof(#X) - 1) == 0

// TODO: read default stuff
int gbl_setting_default_query_timeout = 0;
int gbl_setting_default_chunk_size = 0;
int gbl_setting_default_mode = TRANLEVEL_INVALID;
int gbl_setting_default_timeout = 0;
int gbl_setting_default_password;
int gbl_setting_default_spversion;
int gbl_setting_default_prepare_only;
int gbl_setting_default_readonly;
int gbl_setting_default_expert;
int gbl_setting_default_sptrace;
int gbl_setting_default_cursordebug;
int gbl_setting_default_spdebug;
int gbl_setting_default_hasql;
int gbl_setting_default_verifyretry;
int gbl_setting_default_queryeffects;
int gbl_setting_default_remote;
int gbl_setting_default_getcost;
int gbl_setting_default_explain;
int gbl_setting_default_maxtransize;
int gbl_setting_default_groupconcatmemlimit;
int gbl_setting_default_plannereffort;
int gbl_setting_default_intransresults;
int gbl_setting_default_admin;
int gbl_setting_default_querylimit;
int gbl_setting_default_rowbuffer;
int gbl_setting_default_sockbplog;
int gbl_setting_default_user;
int gbl_setting_default_password;
int gbl_setting_default_spversion;
int gbl_setting_default_prepare_only;
int gbl_setting_default_readonly;
int gbl_setting_default_expert;
int gbl_setting_default_sptrace;
int gbl_setting_default_cursordebug;
int gbl_setting_default_spdebug;
int gbl_setting_default_hasql;
int gbl_setting_default_verifyretry;
int gbl_setting_default_queryeffects;
int gbl_setting_default_remote;
int gbl_setting_default_getcost;
int gbl_setting_default_explain;
int gbl_setting_default_maxtransize;
int gbl_setting_default_groupconcatmemlimit;
int gbl_setting_default_plannereffort;
int gbl_setting_default_intransresults;
int gbl_setting_default_admin;
int gbl_setting_default_querylimit;
int gbl_setting_default_rowbuffer;
int gbl_setting_default_sockbplog;
int gbl_setting_default_timezone;
// char* and other default;

static inline char *skipws(char *str)
{
    if (str) {
        while (*str && isspace(*str))
            str++;
    }
    return str;
}

int init_client_settings()
{
    // BIG TODO: Initialized clients with default settings
    desc_settings = hash_init_strptr(offsetof(db_clnt_setting_t, desc));
    logmsg(LOGMSG_DEBUG, "Settings hash initialized\n");
    return 1;
}

enum set_state {
    SET_STATE_INIT = 0,
    SET_STATE_SET,
    SET_STATE_TRANS,
    SET_STATE_CHUNK,
    SET_STATE_MODE,
    SET_STATE_TIMEOUT,
    SET_STATE_MAXQUERYTIME,
    SET_STATE_TIMEZONE,
    SET_STATE_DATETIME,
    SET_STATE_PRECISION,
    SET_STATE_USER,
    SET_STATE_PASSWORD,
    SET_STATE_SPVERSION,
    SET_STATE_PREPARE_ONLY,
    SET_STATE_READONLY,
    SET_STATE_EXPERT,
    SET_STATE_SPTRACE,
    SET_STATE_CURSORDEBUG,
    SET_STATE_SPDEBUG,
    SET_STATE_HASQL,
    SET_STATE_VERIFYRETRY,
    SET_STATE_QUERYEFFECTS,
    SET_STATE_REMOTE,
    SET_STATE_GETCOST,
    SET_STATE_EXPLAIN,
    SET_STATE_MAXTRANSIZE,
    SET_STATE_GROUPCONCATMEMLIMIT,
    SET_STATE_PLANNEREFFORT,
    SET_STATE_INTRANSRESULTS,
    SET_STATE_ADMIN,
    SET_STATE_QUERYLIMIT,
    SET_STATE_ROWBUFFER,
    SET_STATE_SOCKBPLOG,
    SET_STATE_APPLY = 998,
    SET_STATE_DEAD = 999
};

typedef struct set_state_mach {
    struct sqlclntstate *clnt;
    enum set_state state;

    int num_self_transition;

    char *key;
    char *val;

    int rc;
} set_state_mach_t;

void set_apply(set_state_mach_t *sm, char *key, char *value)
{
    sm->key = key;
    sm->val = value;
    sm->state = SET_STATE_APPLY;
}

int apply_sett(set_state_mach_t *sm, char *err)
{
    if (sm->key == NULL) {
        SET_ERR("invalid key supplied");
        return 5;
    }
    db_clnt_setting_t *sett = NULL;
    if ((sett = hash_find(desc_settings, &sm->key)) == NULL) {
        SET_ERR("no setting found for key %s\n", sm->key);
        return 5;
    }
    if (sett->set_clnt == NULL) {
        SET_ERR("no setter specified for key %s\n", sm->key);
        return 5;
    }
    return sett->set_clnt(sett, sm->clnt, sm->val, err);
}

int transition(set_state_mach_t *sm, char *key)
{
    int rc = 0;
    if (sm->state == SET_STATE_INIT) {
        if (strncmp(key, "set", 3) == 0)
            sm->state = SET_STATE_SET;
        else {
            rc = 1;
            goto transerr;
        }
    } else if (sm->state == SET_STATE_SET) {
        if (SET_TOKEN_CMP(transaction)) {
            sm->state = SET_STATE_TRANS;
        } else if (SET_TOKEN_CMP(timeout)) {
            sm->state = SET_STATE_TIMEOUT;
        } else if (SET_TOKEN_CMP(maxquerytime)) {
            sm->state = SET_STATE_MAXQUERYTIME;
        } else if (SET_TOKEN_CMP(timezone)) {
            sm->state = SET_STATE_TIMEZONE;
        } else if (SET_TOKEN_CMP(datetime)) {
            sm->state = SET_STATE_DATETIME;
        } else if (SET_TOKEN_CMP(user)) {
            sm->state = SET_STATE_USER;
        } else if (SET_TOKEN_CMP(password)) {
            sm->state = SET_STATE_PASSWORD;
        } else if (SET_TOKEN_CMP(spversion)) {
            sm->state = SET_STATE_SPVERSION;
        } else if (SET_TOKEN_CMP(prepare_only)) {
            sm->state = SET_STATE_PREPARE_ONLY;
        } else if (SET_TOKEN_CASECMP(readonly)) {
            sm->state = SET_STATE_READONLY;
        } else if (SET_TOKEN_CASECMP(expert)) {
            sm->state = SET_STATE_EXPERT;
        } else if (SET_TOKEN_CASECMP(sptrace)) {
            sm->state = SET_STATE_SPTRACE;
        } else if (SET_TOKEN_CASECMP(cursordebug)) {
            sm->state = SET_STATE_CURSORDEBUG;
        } else if (SET_TOKEN_CASECMP(spdebug)) {
            sm->state = SET_STATE_SPDEBUG;
        } else if (SET_TOKEN_CASECMP(HASQL)) {
            sm->state = SET_STATE_HASQL;
        } else if (SET_TOKEN_CASECMP(verifyretry)) {
            sm->state = SET_STATE_VERIFYRETRY;
        } else if (SET_TOKEN_CASECMP(queryeffects)) {
            sm->state = SET_STATE_QUERYEFFECTS;
        } else if (SET_TOKEN_CASECMP(remote)) {
            sm->state = SET_STATE_REMOTE;
        } else if (SET_TOKEN_CASECMP(getcost)) {
            sm->state = SET_STATE_GETCOST;
        } else if (SET_TOKEN_CASECMP(explain)) {
            sm->state = SET_STATE_EXPLAIN;
        } else if (SET_TOKEN_CASECMP(maxtransize)) {
            sm->state = SET_STATE_MAXTRANSIZE;
        } else if (SET_TOKEN_CASECMP(groupconcatmemlimit)) {
            sm->state = SET_STATE_GROUPCONCATMEMLIMIT;
        } else if (SET_TOKEN_CASECMP(plannereffort)) {
            sm->state = SET_STATE_PLANNEREFFORT;
        } else if (SET_TOKEN_CASECMP(intransresults)) {
            sm->state = SET_STATE_INTRANSRESULTS;
        } else if (SET_TOKEN_CASECMP(admin)) {
            sm->state = SET_STATE_ADMIN;
        } else if (SET_TOKEN_CASECMP(querylimit)) {
            sm->state = SET_STATE_QUERYLIMIT;
        } else if (SET_TOKEN_CASECMP(rowbuffer)) {
            sm->state = SET_STATE_ROWBUFFER;
        } else if (SET_TOKEN_CASECMP(sockbplog)) {
            sm->state = SET_STATE_SOCKBPLOG;
        } else {
            rc = 2;
            goto transerr;
        }
    } else if (sm->state == SET_STATE_TRANS) {
        if (strncmp(key, "chunk", 5) == 0) {
            sm->state = SET_STATE_CHUNK;
        } else if (strncmp(key, "mode", 4) == 0) {
            sm->state = SET_STATE_MODE;
        } else {
            rc = 3;
            goto transerr;
        }
    } else if (sm->state == SET_STATE_CHUNK) {
        // set chunk
        // get setter function from setting->set_func(setting, sm->clnt, char*value, char * err)
        set_apply(sm, "chunk", key);
    } else if (sm->state == SET_STATE_MODE) {
        // set mode
        if (strncmp(key, "read", 4) == 0) {
            if (sm->num_self_transition == 0) {
                ++sm->num_self_transition;
            } else {
                rc = 4;
                goto transerr;
            }
        } else if (sm->num_self_transition && (strncmp(key, "committed", 9) == 0)) {
            set_apply(sm, "mode", "read committed");
        } else if (key != NULL) {
            set_apply(sm, "mode", key);
        } else {
            rc = 3;
            goto transerr;
        }
    } else if (sm->state == SET_STATE_TIMEOUT) {
        set_apply(sm, "timeout", key);
    } else if (sm->state == SET_STATE_MAXQUERYTIME) {
        set_apply(sm, "maxquerytime", key);
    } else if (sm->state == SET_STATE_TIMEZONE) {
        set_apply(sm, "timezone", key);
    } else if (sm->state == SET_STATE_DATETIME) {
        if ((sm->num_self_transition == 0) && (strncmp(key, "precision", 9) == 0)) {
            ++sm->num_self_transition;
        } else if ((sm->num_self_transition != 0)) {
            set_apply(sm, "datetime", key);
            sm->num_self_transition = 0;
        } else {
            rc = 4;
            goto transerr;
        }
        // TODO: single level operations can be turned to
        //     a simple array, by indexing these states starting
        //     from custom value = 100. i.e. start SET_STATE_USER=100 and
        //     arr[state - 100] = "user" and so on
    } else if (sm->state == SET_STATE_USER) {
        set_apply(sm, "user", key);
    } else if (sm->state == SET_STATE_PASSWORD) {
        set_apply(sm, "password", key);
    } else if (sm->state == SET_STATE_SPVERSION) {
        set_apply(sm, "spversion", key);
    } else if (sm->state == SET_STATE_PREPARE_ONLY) {
        set_apply(sm, "prepare_only", key);
    } else if (sm->state == SET_STATE_READONLY) {
        set_apply(sm, "readonly", key);
    } else if (sm->state == SET_STATE_EXPERT) {
        set_apply(sm, "expert", key);
    } else if (sm->state == SET_STATE_SPTRACE) {
        set_apply(sm, "sptrace", key);
    } else if (sm->state == SET_STATE_CURSORDEBUG) {
        set_apply(sm, "cursordebug", key);
    } else if (sm->state == SET_STATE_SPDEBUG) {
        set_apply(sm, "spdebug", key);
    } else if (sm->state == SET_STATE_HASQL) {
        set_apply(sm, "hasql", key);
    } else if (sm->state == SET_STATE_VERIFYRETRY) {
        set_apply(sm, "verifyretry", key);
    } else if (sm->state == SET_STATE_QUERYEFFECTS) {
        set_apply(sm, "queryeffects", key);
    } else if (sm->state == SET_STATE_REMOTE) {
        set_apply(sm, "remote", key);
    } else if (sm->state == SET_STATE_GETCOST) {
        set_apply(sm, "getcost", key);
    } else if (sm->state == SET_STATE_EXPLAIN) {
        set_apply(sm, "explain", key);
    } else if (sm->state == SET_STATE_MAXTRANSIZE) {
        set_apply(sm, "maxtransize", key);
    } else if (sm->state == SET_STATE_GROUPCONCATMEMLIMIT) {
        set_apply(sm, "groupconcatmemlimit", key);
    } else if (sm->state == SET_STATE_PLANNEREFFORT) {
        set_apply(sm, "plannereffort", key);
    } else if (sm->state == SET_STATE_INTRANSRESULTS) {
        set_apply(sm, "intransresults", key);
    } else if (sm->state == SET_STATE_ADMIN) {
        set_apply(sm, "admin", key);
    } else if (sm->state == SET_STATE_QUERYLIMIT) {
        set_apply(sm, "querylimit", key);
    } else if (sm->state == SET_STATE_ROWBUFFER) {
        set_apply(sm, "rowbuffer", key);
    } else if (sm->state == SET_STATE_SOCKBPLOG) {
        set_apply(sm, "sockbplog", key);
    } else {
        rc = 1;
        goto transerr;
    }

transerr:
    sm->rc = rc;
    return rc;
}

set_state_mach_t *init_state_machine(struct sqlclntstate *clnt)
{
    set_state_mach_t *sm = (set_state_mach_t *)malloc(sizeof(set_state_mach_t));
    sm->clnt = clnt;
    sm->state = SET_STATE_INIT;
    sm->rc = 0;
    sm->num_self_transition = 0;
    return sm;
}

int destroy_state_machine(set_state_mach_t *sm)
{
    if (sm)
        free(sm);
    return 0;
}

int populate_settings(struct sqlclntstate *clnt, const char *sqlstr)
{
    set_state_mach_t *sm = init_state_machine(clnt);

    char *argv[SET_CMD_WORD_LEN];
    char **ap, *temp = strdup(sqlstr);

    int rc = 0;
    for (ap = argv; ((*ap = strsep(&temp, " \t")) != NULL);) {
        if (**ap != '\0') {
            transition(sm, *ap);
            if ((sm->rc) || (sm->state == SET_STATE_APPLY)) {
                break;
            }
        }
    }

    char err[SM_ERROR_LEN] = {0};
    if ((sm->rc == 0) && sm->state != SET_STATE_APPLY) {
        rc = 10;
    }

    if ((rc == 0) && ((rc = sm->rc) == 0)) {
        if ((rc = apply_sett(sm, err)) != 0) {
            logmsg(LOGMSG_ERROR, "%s\n", err);
        };
    }
    destroy_state_machine(sm);
    return rc;
}

int set_chunk(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *value, char *err)
{
    int chunk_size = 0;
    if (!value || ((chunk_size = atoi(value)) <= 0)) {
        SET_ERR("set transaction chunk N: missing chunk size N \"%s\"", value);
        return 7;
    } else if (clnt->dbtran.mode != TRANLEVEL_SOSQL) {
        SET_ERR("transaction chunks require SOCKSQL transaction mode");
        return 7;
    }
    clnt->dbtran.maxchunksize = chunk_size;
    /* in chunked mode, we disable verify retries */
    clnt->verifyretry_off = 1;
    return 0;
}

int set_mode(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *value, char *err)
{
    clnt->dbtran.mode = TRANLEVEL_INVALID;
    clnt->high_availability_flag = 1;
    if (strncasecmp(value, "read committed", 14) == 0) {
        clnt->dbtran.mode = TRANLEVEL_RECOM;
    } else if (strncasecmp(value, "serial", 6) == 0) {
        clnt->dbtran.mode = TRANLEVEL_SERIAL;
        if (clnt->hasql_on == 1) {
            clnt->high_availability_flag = 1;
        }
    } else if (strncasecmp(value, "blocksql", 7) == 0) {
        clnt->dbtran.mode = TRANLEVEL_SOSQL;
    } else if (strncasecmp(value, "snap", 4) == 0) {
        clnt->dbtran.mode = TRANLEVEL_SNAPISOL;
        clnt->verify_retries = 0;
        if (clnt->hasql_on == 1) {
            clnt->high_availability_flag = 1;
            SET_ERR("Enabling snapshot isolation high availability\n");
        }
    }

    return 7 ? (clnt->dbtran.mode == TRANLEVEL_INVALID) : 0;
}

int set_user(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *value, char *err)
{
    if (!sqlite3IsCorrectlyQuoted(value)) {
        snprintf(err, SM_ERROR_LEN, "set user: '%s' is an incorrectly quoted string", value);
        return 7;
    }
    char *deqval = strdup(value);
    sqlite3Dequote(deqval);
    if (strlen(deqval) >= sizeof(clnt->current_user.name)) {
        snprintf(err, SM_ERROR_LEN, "set user: '%s' exceeds %zu characters", deqval,
                 sizeof(clnt->current_user.name) - 1);
        return 7;
    }
    clnt->current_user.have_name = 1;
    /* Re-authenticate the new user. */
    if (clnt->authgen && strcmp(clnt->current_user.name, deqval) != 0)
        clnt->authgen = 0;
    clnt->current_user.is_x509_user = 0;
    strcpy(clnt->current_user.name, deqval);
    return 0;
}
int set_password(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    if (!sqlite3IsCorrectlyQuoted(sqlstr)) {
        snprintf(err, SM_ERROR_LEN, "set user: '%s' is an incorrectly quoted string", sqlstr);
        return 7;
    }
    char *deqval = strdup(sqlstr);
    sqlite3Dequote(deqval);
    if (strlen(deqval) >= sizeof(clnt->current_user.password)) {
        snprintf(err, SM_ERROR_LEN,
                 "set password: password length exceeds %zu "
                 "characters",
                 sizeof(clnt->current_user.password) - 1);
        return 7;
    }
    clnt->current_user.have_password = 1;
    /* Re-authenticate the new password. */
    if (clnt->authgen && strcmp(clnt->current_user.password, deqval) != 0)
        clnt->authgen = 0;
    strcpy(clnt->current_user.password, deqval);
    return 0;
}
int set_spversion(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    clnt->spversion.version_num = 0;
    free(clnt->spversion.version_str);

    clnt->spversion.version_str = NULL;
    // TODO: do this
    //    if ((sqlstr - spname) < MAX_SPNAME) {
    //        strncpy0(clnt->spname, spname, MAX_SPNAME);
    //    } else {
    //        rc = ii + 1;
    //    }
    //    ++sqlstr;
    //
    //    sqlstr = skipws(sqlstr);
    //    int ver = strtol(sqlstr, &endp, 10);
    //    if (*sqlstr == '\'' || *sqlstr == '"') { // looks like a str
    //        if (strlen(sqlstr) < MAX_SPVERSION_LEN) {
    //            clnt->spversion.version_str = strdup(sqlstr);
    //            sqlite3Dequote(clnt->spversion.version_str);
    //        } else {
    //            rc = ii + 1;
    //        }
    //    } else if (*endp == 0) { // parsed entire number successfully
    //        clnt->spversion.version_num = ver;
    //    } else {
    //        rc = ii + 1;
    //    }
    return 0;
}
int set_prepare_only(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    clnt->prepare_only = 0 ? (strncasecmp(sqlstr, "off", 3) == 0) : 1;
    return 0;
}
int set_readonly(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    clnt->is_readonly = 0 ? (strncasecmp(sqlstr, "off", 3) == 0) : 1;
    return 0;
}
int set_expert(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    if (strncasecmp(sqlstr, "off", 3) == 0) {
        clnt->is_expert = 0;
    } else {
        clnt->is_expert = 1;
        clnt->is_fast_expert = (strncasecmp(sqlstr, "fast", 4) == 0);
    }
    return 0;
}
int set_sptrace(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    // TODO: should probably make all of these "on"/"off"s more concrete by checking if
    // sqlstr is 'on' or 'off'.
    clnt->want_stored_procedure_trace = 0 ? (strncasecmp(sqlstr, "off", 3) == 0) : 1;
    return 0;
}
int set_cursordebug(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    char *value = strdup(sqlstr);
    return bdb_osql_trak(value, &clnt->bdb_osql_trak);
}
int set_spdebug(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    clnt->want_stored_procedure_debug = 0 ? (strncasecmp(sqlstr, "off", 3) == 0) : 1;
    return 0;
}
int set_hasql(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    if (strncasecmp(sqlstr, "on", 2) == 0) {
        clnt->hasql_on = 1;
        if (clnt->dbtran.mode == TRANLEVEL_SERIAL || clnt->dbtran.mode == TRANLEVEL_SNAPISOL) {
            clnt->high_availability_flag = 1;
            sql_debug_logf(clnt, __func__, __LINE__,
                           "setting "
                           "high_availability\n");
        }
    } else {
        clnt->hasql_on = 0;
        clnt->high_availability_flag = 0;
        sql_debug_logf(clnt, __func__, __LINE__,
                       "clearing "
                       "high_availability\n");
    }
    return 0;
}
int set_verifyretry(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    if (strncasecmp(sqlstr, "on", 2) == 0) {
        clnt->verifyretry_off = 0;
    } else {
        clnt->verifyretry_off = 1;
    }
    return 0;
}
int set_queryeffects(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    // TODO: if it isn't either return error;
    if (strncasecmp(sqlstr, "statement", 9) == 0) {
        clnt->statement_query_effects = 1;
    }
    if (strncasecmp(sqlstr, "transaction", 11) == 0) {
        clnt->statement_query_effects = 0;
    }
    return 0;
}
int set_remote(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    char *value = strdup(sqlstr);
    int fdbrc = fdb_access_control_create(clnt, value);
    if (fdbrc) {
        snprintf(err, SM_ERROR_LEN, "%s: failed to process remote access settings \"%s\"\n", __func__, value);
        return fdbrc;
    }
    return 0;
}
int set_getcost(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    if (strncasecmp(sqlstr, "on", 2) == 0) {
        clnt->get_cost = 1;
    } else {
        clnt->get_cost = 0;
    }
    return 0;
}
int set_explain(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    if (strncasecmp(sqlstr, "on", 2) == 0) {
        clnt->is_explain = 1;
    } else if (strncasecmp(sqlstr, "verbose", 7) == 0) {
        clnt->is_explain = 2;
        char *value = strdup(sqlstr);
        value += 7;
        value = skipws(value);

        /*
           0x2    -> show headnote and footnote from the solver
           0x4    -> show how the best index is picked
           0x8    -> show how the cost of each index is calculated
           0x10   -> show trace for stat4
           0x100  -> show all where terms
           0x200  -> show trace for Or terms
           0x840  -> show trace for virtual tables
         */

        if (value[0] == '\0')
            clnt->where_trace_flags = ~0;
        else
            clnt->where_trace_flags = (int)strtol(value, NULL, 16);
    } else {
        clnt->is_explain = 0;
    }
    return 0;
}
int set_maxtransize(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{

    int maxtransz = strtol(sqlstr, NULL, 10);

    if (maxtransz < 0) {
        snprintf(err, SM_ERROR_LEN, "Error: bad value for maxtransize %s\n", sqlstr);
        return 7;
    }
    clnt->osql_max_trans = maxtransz;
#ifdef DEBUG
    printf("setting clnt->osql_max_trans to %d\n", clnt->osql_max_trans);
#endif
    return 0;
}
int set_groupconcatmemlimit(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    int sz = strtol(sqlstr, NULL, 10);
    if (sz < 0) {
        snprintf(err, SM_ERROR_LEN, "Error: bad value for groupconcatmemlimit %s\n", sqlstr);
        return 7;
    }

    clnt->group_concat_mem_limit = sz;
#ifdef DEBUG
    printf("setting clnt->group_concat_mem_limit to %d\n", clnt->group_concat_mem_limit);
#endif
    return 0;
}
int set_plannereffort(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    int effort = strtol(sqlstr, NULL, 10);
    if (0 < effort && effort <= 10)
        clnt->planner_effort = effort;
#ifdef DEBUG
    printf("setting clnt->planner_effort to %d\n", clnt->planner_effort);
#endif
    return 0;
}
int set_intransresults(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    //    struct newsql_appdata *appdata = clnt->appdata;
    //    appdata->send_intrans_response = 0 ? (strncasecmp(sqlstr, "off", 3) == 0) : -1;
    return 0;
}
int set_admin(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    clnt->admin = (strncasecmp(sqlstr, "off", 3) != 0);
    return 0;
}
int set_querylimit(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    //    return handle_set_querylimits(sqlstr, clnt);
    return 0;
}
int set_rowbuffer(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    clnt->rowbuffer = (strncasecmp(sqlstr, "on", 2) == 0);
    return 0;
}
int set_sockbplog(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    // init_bplog_socket(clnt);
    return 0;
}

int set_timeout(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *value, char *err)
{
    long timeout = strtol(value, NULL, 10);
    return clnt->plugin.set_timeout(clnt, timeout);
}

int set_maxquerytime(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *value, char *err)
{
    long timeout = strtol(value, NULL, 10);
    clnt->query_timeout = timeout;
    return 0;
}

int set_timezone(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *value, char *err)
{
    strncpy0(clnt->tzname, value, sizeof(clnt->tzname));
    return 0;
}

int set_datetime(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *value, char *err)
{
    DTTZ_TEXT_TO_PREC(value, clnt->dtprec, 0, return -1);
    return 0;
}

int temp_debug_register(char *name, comdb2_setting_type type, comdb2_setting_flag flag, int blah)
{
    db_clnt_setting_t *s = malloc(sizeof(db_clnt_setting_t));

    s->name = name;
    s->type = type;
    s->flag = flag;
    s->def = &blah;

    listc_abl(&settings, s);
    return 0;
}

int add_set_clnt(char *desc, set_clnt_setting *setf)
{
    db_clnt_setting_t *set = settings.bot;
    set->desc = strdup(desc);
    set->set_clnt = setf;
    hash_add(desc_settings, set);
    return 0;
}

int register_settings(struct sqlclntstate *clnt)
{
#include "db_clnt_settings.h"
    return 0;
}
