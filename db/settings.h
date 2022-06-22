#ifndef _SETTINGS_H
#define _SETTINGS_H

#include "hash.h"
#include "list.h"
#include "sbuf2.h"
#include "sql.h"

/**

clnt.dbtrans = SET_COMPO
dbtrans.mode = <->

gbl_set_default_dbtrans

**/

// TODO: If values for these already exists, remove
#define SET_CMD_LEN 128
// maximum number of words in the set command
#define SET_CMD_WORD_LEN 15

typedef enum {
    SETTING_INTEGER = 1 << 1,
    SETTING_LONG = 1 << 2,
    SETTING_DOUBLE = 1 << 3,
    SETTING_BOOLEAN = 1 << 4,
    SETTING_STRING = 1 << 5,
    SETTING_CSTRING = 1 << 6,
    SETTING_ENUM = 1 << 7,
    SETTING_MULTIPLE = 1 << 8,
    SETTING_STRUCT = 1 << 9
} comdb2_setting_type;

typedef enum {
    /** Internal, don't expose this to the user **/
    SETFLAG_INTERNAL = 1 << 1,
    SETFLAG_WRITEONLY = 1 << 2
} comdb2_setting_flag;

struct db_clnt_setting_t;
typedef struct db_clnt_setting_t db_clnt_setting_t;

typedef int set_clnt_setting(db_clnt_setting_t *, struct sqlclntstate *, const char *, char *);
typedef void *get_clnt_setting(struct sqlclntstate *, int);

struct db_clnt_setting_t {
    LINKC_T(struct db_clnt_setting_t) lnk;

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

    set_clnt_setting *set_clnt;
    get_clnt_setting *get_clnt;
};

int init_client_settings();
int register_settings();
int populate_settings(struct sqlclntstate *, const char *);

LISTC_T(struct db_clnt_setting_t) settings;
hash_t *desc_settings;

#define SETTING_SET_FUNC(SETTING) int set_##SETTING(db_clnt_setting_t *, struct sqlclntstate *, const char *, char *err)

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
int temp_debug_register(char *, comdb2_setting_type, comdb2_setting_flag, int, int);

#define REGISTER_SETTING(NAME, TYPE, FLAG, DEFAULT)                                                                    \
    temp_debug_register(#NAME, TYPE, FLAG, DEFAULT, offsetof(struct sqlclntstate, NAME));

/*
    do {                                                                                                               \
        db_clnt_setting_t s = {.name = #NAME,                                                                          \
                               .type = TYPE,                                                                           \
                               .flag = FLAG,                                                                           \
                               .offset = offsetof(struct sqlclntstate, NAME),                                          \
                               .cmd = "default",                                                                       \
                               .def = &DEFAULT,                                                                        \
                               .lnk = {}};                                                                             \
        listc_abl(&settings, &s);                                                                                      \
    } while (0)
*/
int add_set_clnt(char *, set_clnt_setting *);
void get_value(const struct sqlclntstate *, const db_clnt_setting_t *, char *, size_t);

#define REGISTER_ACC_SETTING(NAME, DESC, TYPE, FLAG, DEFAULT)                                                          \
    REGISTER_SETTING(NAME, TYPE, FLAG, DEFAULT);                                                                       \
    add_set_clnt(#DESC, set_##DESC);

#endif
