#ifndef _SETTINGS_H
#define _SETTINGS_H

#include "list.h"
#include "sbuf2.h"
#include "sql.h"

/**

clnt.dbtrans = SET_COMPO
dbtrans.mode = <->

gbl_set_default_dbtrans

**/

int gbl_setting_default_query_timeout = 0;

// TODO: If values for these already exists, remove
#define SET_CMD_LEN 128
// maximum number of words in the set command
#define SET_CMD_WORD_LEN 15

typedef enum {
    SETTING_INTEGER,
    SETTING_DOUBLE,
    SETTING_BOOLEAN,
    SETTING_STRING,
    SETTING_ENUM,
    SETTING_FUNC,
    SETTING_COMPOSITE,
} comdb2_setting_type;

typedef enum {
    /** Internal, don't expose this to the user **/
    SETFLAG_INTERNAL = 1 << 1,
    /** Derived field, no explicit setters **/
    SETFLAG_DERIVED = 1 << 2
} comdb2_setting_flag;

struct db_clnt_setting_t;
typedef struct db_clnt_setting_t db_clnt_setting_t;

typedef int set_clnt_setting(db_clnt_setting_t *, struct sqlclntstate *, const char *);
typedef void *get_clnt_setting(struct sqlclntstate *, int);

struct db_clnt_setting_t {
    char *name;
    char *desc;
    comdb2_setting_type type;
    comdb2_setting_flag flag;
    size_t offset;
    // command that set this setting
    // e.g, default or "set timeout 10"
    char cmd[SET_CMD_LEN];
    // def = initial value
    // make it constant?
    void *def;
    LINKC_T(struct db_clnt_setting_t) lnk;

    set_clnt_setting *set_clnt;
    get_clnt_setting *get_clnt;
};

int register_settings(struct sqlclntstate *clnt);

LISTC_T(struct db_clnt_setting_t) settings;

#define SETTING_SET_FUNC(SETTING)                                                                                      \
    int set_##SETTING(db_clnt_setting_t *setting, struct sqlclntstate *clnt, const char *sqlstr)

SETTING_SET_FUNC(plugin);
SETTING_SET_FUNC(tzname);
SETTING_SET_FUNC(dtprec);
SETTING_SET_FUNC(dbtran);
SETTING_SET_FUNC(current_user);
SETTING_SET_FUNC(query_timeout);
SETTING_SET_FUNC(authgen);
SETTING_SET_FUNC(spname);
SETTING_SET_FUNC(want_stored_procedure_trace);
SETTING_SET_FUNC(bdb_osql_trak);
SETTING_SET_FUNC(verifyretry_off);
SETTING_SET_FUNC(current_user);
SETTING_SET_FUNC(query_timeout);
SETTING_SET_FUNC(authgen);
SETTING_SET_FUNC(spname);
SETTING_SET_FUNC(want_stored_procedure_trace);
SETTING_SET_FUNC(bdb_osql_trak);
SETTING_SET_FUNC(verifyretry_off);
SETTING_SET_FUNC(statement_query_effects);
SETTING_SET_FUNC(get_cost);
SETTING_SET_FUNC(is_explain);
SETTING_SET_FUNC(osql_max_trans);
SETTING_SET_FUNC(group_concat_mem_limit);
SETTING_SET_FUNC(planner_effort);
SETTING_SET_FUNC(appdata);
SETTING_SET_FUNC(admin);
SETTING_SET_FUNC(is_readonly);
SETTING_SET_FUNC(is_expert );
SETTING_SET_FUNC(is_fast_expert);

// TODO: can i add (int*) parse_fun(struct sqlclntstate *, char*cmd, db_clnt_setting_t*);
// if command can be parsed, to the value as x, then we
// can do, *((*clnt + s->offset) = x
// s->cmd = cmd

// or, we can do add (int*) parse_fun(struct sqlclntstate *, char*cmd, int offset);
// return -> 0 if cmd is valid, else 1
// if command can be parsed, to the value as x, then we
// can do, *((*clnt + s->offset) = x
// s->cmd = cmd

// Actually, this would work best,
// where cmd can be something like 'set timeout'
// (int*) parse_cmd(char*cmd, void *value) -> return 0 if cmd is valid else 1;
// if command can be parsed, to the value as value, then we,
// *((*clnt + s->offset) = x
// s->cmd = cmd
// Would this be problematic? - global s->cmd stays in s->cmd while clnt->field value is stored in the client?
// Bind a map to the client? and remove cmd from clnt_setting_t
// get value from (s->type)s->get_cmd(struct sqlclntstate *, int offset) if s->FLAG is not internal
// return *((*clnt + offset)

// TODO: format the composite different
#define REGISTER_SETTING(NAME, DESC, TYPE, FLAG, DEFAULT)                                                              \
    do {                                                                                                               \
        db_clnt_setting_t s = {.name = #NAME,                                                                          \
                               .desc = DESC,                                                                           \
                               .type = TYPE,                                                                           \
                               .flag = FLAG,                                                                           \
                               .offset = offsetof(struct sqlclntstate, NAME),                                          \
                               .cmd = "default",                                                                       \
                               .def = &DEFAULT,                                                                        \
                               .lnk = {}};                                                                             \
        listc_abl(&settings, &s);                                                                                      \
    } while (0)

#define REGISTER_ACC_SETTING(NAME, DESC, TYPE, FLAG, DEFAULT)                                                          \
    REGISTER_SETTING(NAME, DESC, TYPE, FLAG, DEFAULT);                                                                 \
    do {                                                                                                               \
        db_clnt_setting_t *set = listc_rbl(&settings);                                                                 \
        set->set_clnt = set_##NAME;                                                                                    \
        listc_abl(&settings, set);                                                                                     \
    } while (0)

#endif