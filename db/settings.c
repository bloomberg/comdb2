#include "settings.h"
#include "comdb2.h"
#include "list.h"
#include "sbuf2.h"
#include <strings.h>

enum set_state {
    SET_STATE_INIT = 0,
    SET_STATE_SET,
    SET_STATE_TRANS,
    SET_STATE_CHUNK,
    SET_STATE_MODE,
    SET_STATE_DEAD = 999
};

typedef struct set_state_mach {
    struct sqlclntstate *clnt;
    enum set_state state;
    int rc;
    char *err;
} set_state_mach_t;

int transition(set_state_mach_t *sm, char *key)
{
    if (sm->state == SET_STATE_INIT) {
        if (strncmp(key, "set", 3) == 0)
            sm->state = SET_STATE_SET;
        else
            return 1;
    }

    if (sm->state == SET_STATE_SET) {
        if (strncmp(key, "transaction", 9)) {
            sm->state = SET_STATE_TRANS;
        } else if (strncmp(key, "", 4)) {
        } else {
            sm->rc = 2;
            return 2;
        }
    } else if (sm->state == SET_STATE_TRANS) {
        if (strncmp(key, "chunk", 5)) {
            sm->state = SET_STATE_CHUNK;
        } else if (strncmp(key, "mode", 4)) {
            sm->state = SET_STATE_MODE;
        } else {
            sm->rc = 3;
            return 3;
        }
    } else if (sm->state == SET_STATE_CHUNK) {
        // set chunk
        // get setter function from setting->set_func(setting, sm->clnt, char*value, char * err)
        
    } else if (sm->state == SET_STATE_MODE) {
        // set mode
    } else {
        sm->rc = 1;
        return 1;
    }

    return 0;
}

set_state_mach_t *init_state_machine(struct sqlclntstate *clnt)
{
    set_state_mach_t *sm = (set_state_mach_t *)malloc(sizeof(set_state_mach_t));
    sm->clnt = clnt;
    sm->state = SET_STATE_INIT;
    sm->rc = 0;
    sm->err = NULL;
    return sm;
}

int destroy_state_machine(set_state_mach_t *sm)
{
    if (sm)
        free(sm);
    return 0;
}

int populate_settings(struct sqlclntstate *clnt, char *sqlstr)
{
    set_state_mach_t *sm = init_state_machine(clnt);

    char *argv[SET_CMD_WORD_LEN];
    char **ap, *temp = strdup(sqlstr);

    for (ap = argv; ((*ap = strsep(&temp, " \t")) != NULL);) {
        if (**ap != '\0') {
        }
    }

    int rc = 0;
    for (ap = argv; ((*ap = strsep(&temp, " \t")) != NULL);) {
        if (**ap != '\0') {
            transition(sm, *ap);
            if (sm->state == SET_STATE_DEAD) {
                rc = sm->rc;
            }
        }
    }

    destroy_state_machine(sm);
    return rc;
}

int set_chunk(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *value)
{
    char sm_err[64] = {0};
    int chunk_size = 0;
    if (!value || ((chunk_size = atoi(value)) <= 0)) {
        snprintf(sm_err, sizeof(sm_err),
                 "set transaction chunk N: missing chunk size "
                 "N \"%s\"",
                 value);
    } else if (clnt->dbtran.mode != TRANLEVEL_SOSQL) {
        snprintf(sm_err, sizeof(sm_err), "transaction chunks require SOCKSQL transaction mode");
    } else {
        clnt->dbtran.maxchunksize = chunk_size;
        /* in chunked mode, we disable verify retries */
        clnt->verifyretry_off = 1;
    }

    return 0;
}

int set_mode(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *value)
{
    char sm_err[64] = {0};
 
    clnt->dbtran.mode = TRANLEVEL_INVALID;
    clnt->high_availability_flag = 1;
    if (strncasecmp(value, "read", 4) == 0) {
        if (strncasecmp(value, "committed", 9) == 0) {
            clnt->dbtran.mode = TRANLEVEL_RECOM;
        }
    } else if (strncasecmp(value, "serial", 6) == 0) {
        clnt->dbtran.mode = TRANLEVEL_SERIAL;
        if (clnt->hasql_on == 1) {
            clnt->high_availability_flag = 1;
        }
    } else if (strncasecmp(value, "blocksql", 7) == 0) {
        clnt->dbtran.mode = TRANLEVEL_SOSQL;
    } else if (strncasecmp(value, "snvalue", 4) == 0) {
        value += 4;
        clnt->dbtran.mode = TRANLEVEL_SNAPISOL;
        clnt->verify_retries = 0;
        if (clnt->hasql_on == 1) {
            clnt->high_availability_flag = 1;
            logmsg(LOGMSG_ERROR, "Enabling snvalueshot isolation "
                                 "high availability\n");
        }
    }
    if (clnt->dbtran.mode == TRANLEVEL_INVALID) {
    } else if (clnt->dbtran.mode != TRANLEVEL_SOSQL && clnt->dbtran.maxchunksize) {
        snprintf(sm_err, sizeof(sm_err), "transaction chunks require SOCKSQL transaction mode");
    }

    return 0;
}

int set_spname(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}

int set_want_stored_procedure_trace(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}
int set_bdb_osql_trak(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}
int set_verifyretry_off(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}
int set_statement_query_effects(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}
int set_get_cost(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}
int set_is_explain(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}
int set_osql_max_trans(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}
int set_group_concat_mem_limit(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}
int set_planner_effort(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}
int set_appdata(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}
int set_admin(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}
int set_is_readonly(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}
int set_is_expert(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}
int set_is_fast_expert(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}

int set_authgen(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}

int set_current_user(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}

int set_query_timeout(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}

int set_plugin(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}

int set_tzname(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}

int set_dtprec(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    return 0;
}

int set_dbtran(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)
{
    char *argv[SET_CMD_WORD_LEN];
    char **ap, *temp = strdup(sqlstr);
    int cmd_len = 0;
    char *valid_cmd[2] = {"set", "transaction"};

    int set_chunk = 0, set_mode = 0;

    char err[256];

    int rc = 0;

    for (ap = argv; ((*ap = strsep(&temp, " \t")) != NULL);) {
        if (**ap != '\0') {
            if (++ap < &argv[SET_CMD_WORD_LEN]) {
                if ((cmd_len) < sizeof(valid_cmd)) {
                    if (strncasecmp(*ap, valid_cmd[cmd_len - 1], sizeof(valid_cmd[cmd_len])) != 0)
                        // TODO: set error here as well
                        break;
                    ++cmd_len;
                } else if ((set_chunk & set_mode) == 0) {
                    // TODO: maybe we can make this a part of the case above?
                    if (strncasecmp(*ap, "chunk", 5) == 0) {
                        set_chunk = 1;
                    } else if ((strncasecmp(*ap, "mode", 4) == 0)) {
                        set_mode = 1;
                    }
                } else {
                    // Must have the mode or the chunk size here.
                    if (set_chunk) {
                        int chunk_size = 0;
                        if (!*ap || ((chunk_size = atoi(*ap)) <= 0)) {

                            snprintf(err, sizeof(err),
                                     "set transaction chunk N: missing chunk size "
                                     "N \"%s\"",
                                     sqlstr);
                        } else if (clnt->dbtran.mode != TRANLEVEL_SOSQL) {
                            snprintf(err, sizeof(err), "transaction chunks require SOCKSQL transaction mode");
                        } else {
                            clnt->dbtran.maxchunksize = chunk_size;
                            /* in chunked mode, we disable verify retries */
                            clnt->verifyretry_off = 1;
                        }
                    } else {
                        clnt->dbtran.mode = TRANLEVEL_INVALID;
                        clnt->high_availability_flag = 1;
                        if (strncasecmp(*ap, "read", 4) == 0) {
                            if (strncasecmp(*ap, "committed", 9) == 0) {
                                clnt->dbtran.mode = TRANLEVEL_RECOM;
                            }
                        } else if (strncasecmp(*ap, "serial", 6) == 0) {
                            clnt->dbtran.mode = TRANLEVEL_SERIAL;
                            if (clnt->hasql_on == 1) {
                                clnt->high_availability_flag = 1;
                            }
                        } else if (strncasecmp(*ap, "blocksql", 7) == 0) {
                            clnt->dbtran.mode = TRANLEVEL_SOSQL;
                        } else if (strncasecmp(*ap, "snap", 4) == 0) {
                            *ap += 4;
                            clnt->dbtran.mode = TRANLEVEL_SNAPISOL;
                            clnt->verify_retries = 0;
                            if (clnt->hasql_on == 1) {
                                clnt->high_availability_flag = 1;
                                logmsg(LOGMSG_ERROR, "Enabling snapshot isolation "
                                                     "high availability\n");
                            }
                        }
                        if (clnt->dbtran.mode == TRANLEVEL_INVALID) {
                        } else if (clnt->dbtran.mode != TRANLEVEL_SOSQL && clnt->dbtran.maxchunksize) {
                            snprintf(err, sizeof(err), "transaction chunks require SOCKSQL transaction mode");
                        }
                    }
                }
            }
        }
    }

    free(temp);
    return rc;
}

int register_settings(struct sqlclntstate *clnt)
{
#include "db_clnt_settings.h"
    return 0;
}

/**

enum { SET_STATE_SET=0, SET_STATE_TRANS = 1}.... and so on

typedef struct set_state_mach {
    struct sqlclntstate *clnt,
    int curr_state;
    int rc;
    char * err;
} set_state_mach_t;


int transition(set_state_mach_t * sm, char * key) {
    if strncmp(key, "set") {
        sm->state = SET_STATE_SET;
    }

    if sm->state = SET_STATE_SET; {
        if strncmp(key, "transaction") {
            sm->state = SET_STATE_TRANS;
        } elif strncmp (key, ""){
            ...and so on, the first level
        }
    } elif sm->state = SET_STATE_TRANSACTION {
        if strncmp(key, "chunk") {
            sm->state = SET_STATE_CHUNK;
        } elif strncmp (key, "mode"){
        }
    }elif sm->state == SET_STATE_CHUNK {
        // set chunk
    } elif sm->state == SET_STATE_MODE {
        // set mode
    }
    } else {
        sm->rc = 1;
    }
}

int populate (clnt, ) {
    init_state_machine(clnt);
    int rc = 0;
    for (ap = argv; ((*ap = strsep(&temp, " \t")) != NULL);) {
        if (**ap != '\0') {
            transition(sm, *ap);
            if (sm->state == DEAD) {
                rc = sm->rc;
                log(sm->error);
            }
        }

    destroy_state_machine();
}


 *
 */