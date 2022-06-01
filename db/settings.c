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

// TODO: read default stuff
int gbl_setting_default_query_timeout = 0;
int gbl_setting_default_chunk_size = 0;
int gbl_setting_default_mode = TRANLEVEL_INVALID;
int gbl_setting_default_timeout = 0;
// char* and other default

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
    SET_STATE_PREPARE_ONLy,
    SET_STATE_READONLY,
    SET_STATE_EXPERT,
    SET_STATE_SPTRACE,
    SET_STATE_CURSORDEBUG,
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
        if (strncmp(key, "transaction", 11) == 0) {
            sm->state = SET_STATE_TRANS;
        } else if (strncmp(key, "timeout", 7) == 0) {
            sm->state = SET_STATE_TIMEOUT;
        } else if (strncmp(key, "maxquerytime", 7) == 0) {
            sm->state = SET_STATE_MAXQUERYTIME;
        } else if (strncmp(key, "timezone", 8) == 0) {
            sm->state = SET_STATE_TIMEZONE;
        } else if (strncmp(key, "datetime", 8) == 0) {
            sm->state = SET_STATE_DATETIME;
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

int set_spname(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}

int set_want_stored_procedure_trace(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr,
                                    char *err)
{
    return 0;
}
int set_bdb_osql_trak(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}
int set_verifyretry_off(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}
int set_statement_query_effects(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}
int set_get_cost(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}
int set_is_explain(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}
int set_osql_max_trans(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}
int set_group_concat_mem_limit(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}
int set_planner_effort(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}
int set_appdata(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}
int set_admin(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}
int set_is_readonly(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}
int set_is_expert(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}
int set_is_fast_expert(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}

int set_authgen(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}

int set_current_user(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
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

int set_plugin(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}

int set_tzname(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}

int set_dtprec(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
    return 0;
}

// Can probably multiplex it here based on mode or chunk
int set_dbtran(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr, char *err)
{
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