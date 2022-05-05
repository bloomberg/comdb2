#include "settings.h"
#include "comdb2.h"
#include "list.h"
#include "sbuf2.h"
#include <strings.h>

/** Finds and populates clnt field with the appropriate set command.
if succesful return 0, else 1
**/
int populate_settings(struct sqlclntstate *clnt, char *set_cmd)
{
    int populating = 1;

    db_clnt_setting_t *curr;
    LISTC_FOR_EACH(&settings, curr, lnk)
    {
        // if curr->set_clnt returns not null, then
        if ((curr->set_clnt) && ((populating &= curr->set_clnt(curr, clnt, set_cmd)) == 0))
            break;
    }
    return populating;
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