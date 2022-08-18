#ifndef _SETTINGS_H
#define _SETTINGS_H

#include "hash.h"
#include "list.h"
#include "sbuf2.h"
#include "sql.h"

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
    char *def;

    set_clnt_setting *set_clnt;
    get_clnt_setting *get_clnt;
};

int init_client_settings();
int register_setting(char *, comdb2_setting_type, comdb2_setting_flag, char *, int);
int register_settings();
int populate_settings(struct sqlclntstate *, const char *, char * err);

LISTC_T(struct db_clnt_setting_t) settings;
hash_t *desc_settings;

#define SETTING_SET_FUNC(SETTING) int set_##SETTING(db_clnt_setting_t *, struct sqlclntstate *, const char *, char *err)

#define REGISTER_SETTING(NAME, TYPE, FLAG, DEFAULT)                                                                    \
    register_setting(#NAME, TYPE, FLAG, DEFAULT, offsetof(struct sqlclntstate, NAME));

int add_set_clnt(char *, set_clnt_setting *);
void get_value(const struct sqlclntstate *, const db_clnt_setting_t *, char *, size_t);
void apply_sett_defaults(struct sqlclntstate *);

#define REGISTER_ACC_SETTING(NAME, DESC, TYPE, FLAG, DEFAULT)                                                          \
    REGISTER_SETTING(NAME, TYPE, FLAG, DEFAULT);                                                                       \
    add_set_clnt(#DESC, set_##DESC);

#endif
