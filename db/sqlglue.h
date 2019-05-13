#ifndef SQLGLUE_H
#define SQLGLUE_H

struct Mem;
struct sqlthdstate;
struct sqlite3_context;

typedef struct {
    struct sqlthdstate *thd;
    char *name;
} lua_func_arg_t;

void get_sfuncs(char ***, int *);
void get_afuncs(char ***, int *);

int find_lua_sfunc(const char *);
int find_lua_afunc(const char *);

void lua_func(struct sqlite3_context *, int, struct sqlite3_value **);

void lua_step(struct sqlite3_context *, int, struct sqlite3_value **);
void lua_final(struct sqlite3_context *);

#endif
