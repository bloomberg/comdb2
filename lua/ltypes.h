/*
   Copyright 2015 Bloomberg Finance L.P.
  
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
   
       http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and 
   limitations under the License.
 */

#ifndef INCLUDED_LTYPES_H
#define INCLUDED_LTYPES_H

#include <strings.h>
#include <lua.h>
#include <comdb2.h>

extern int gbl_spstrictassignments;

// one switch to rule them all
#define DBTYPES \
XMACRO_DBTYPES(DBTYPES_LNIL,       "nil",        t0,         NULL)\
XMACRO_DBTYPES(DBTYPES_LBOOLEAN,   "boolean",    t1,         NULL)\
XMACRO_DBTYPES(DBTYPES_LLIGHTUDATA,"luserdata",  t2,         NULL)\
XMACRO_DBTYPES(DBTYPES_LNUMBER,    "number",     t3,         NULL)\
XMACRO_DBTYPES(DBTYPES_LSTRING,    "string",     t4,         NULL)\
XMACRO_DBTYPES(DBTYPES_LTABLE,     "table",      t5,         NULL)\
XMACRO_DBTYPES(DBTYPES_LFUNCTION,  "function",   t6,         NULL)\
XMACRO_DBTYPES(DBTYPES_LUSERDATA,  "userdata",   t7,         NULL)\
XMACRO_DBTYPES(DBTYPES_LTHREAD,    "thread",     t8,         NULL)\
XMACRO_DBTYPES(DBTYPES_MINTYPE,    "min",        min,        NULL)\
XMACRO_DBTYPES(DBTYPES_INTEGER,    "int",        integer,    l_int_tostring)\
XMACRO_DBTYPES(DBTYPES_REAL,       "real",       real,       l_real_tostring)\
XMACRO_DBTYPES(DBTYPES_DECIMAL,    "decimal",    decimal,    l_decimal_tostring)\
XMACRO_DBTYPES(DBTYPES_DATETIME,   "datetime",   datetime,   l_datetime_tostring)\
XMACRO_DBTYPES(DBTYPES_INTERVALYM, "intervalym", intervalym, l_interval_tostring)\
XMACRO_DBTYPES(DBTYPES_INTERVALDS, "intervalds", intervalds, l_interval_tostring)\
XMACRO_DBTYPES(DBTYPES_CSTRING,    "cstring",    cstring,    l_cstring_tostring)\
XMACRO_DBTYPES(DBTYPES_BLOB,       "blob",       blob,       l_blob_tostring)\
XMACRO_DBTYPES(DBTYPES_DBTABLE,    "dbtable",    dbtable,    NULL)\
XMACRO_DBTYPES(DBTYPES_THREAD,     "dbthread",   dbthread,   NULL)\
XMACRO_DBTYPES(DBTYPES_DBCONSUMER, "dbconsumer", dbconsumer, NULL)\
XMACRO_DBTYPES(DBTYPES_STMT,       "stmt",         stmt,	NULL)\
XMACRO_DBTYPES(DBTYPES_CDB2TABLES, "comdb2tables", comdb2tables,NULL)\
XMACRO_DBTYPES(DBTYPES_DBQUEUE,    "dbqueue",    dbqueue,    NULL)\
XMACRO_DBTYPES(DBTYPES_DBSTMT,     "dbstmt",     dbstmt,     NULL)\
XMACRO_DBTYPES(DBTYPES_DB,         "db",         db,         NULL)\
XMACRO_DBTYPES(DBTYPES_MAXTYPE,    "max",        maxtype,    NULL)

#define XMACRO_DBTYPES(type, name, structname, tostring) type,
enum dbtypes_enum { DBTYPES };
typedef enum dbtypes_enum dbtypes_enum;
#undef XMACRO_DBTYPES

typedef enum {
    LUA_OP_EQ = 1,
    LUA_OP_LT,
    LUA_OP_LE,
    LUA_OP_ADD,
    LUA_OP_SUB,
    LUA_OP_MUL,
    LUA_OP_DIV,
    LUA_OP_MOD
} operation_t;

typedef enum {
    RANK_NIL,
    RANK_INT,
    RANK_REAL,
    RANK_DECIMAL,
    RANK_INTERVALDS,
    RANK_INTERVALYM,
    RANK_DATETIME,
    RANK_MAX
} rank_t;

#define XMACRO_DBTYPES(type, name, structname, tostring) const char *structname;
typedef struct { DBTYPES } dbtypes_t;
#undef XMACRO_DBTYPES

typedef struct lua_State* Lua;
typedef int (*tostringfunc)(Lua);
extern const tostringfunc dbtypes_tostring[];
extern dbtypes_t dbtypes;
extern const char *dbtypes_str[];
#define dbtypes_pfx "db.types."

#define DBTYPES_MAGIC 'c' + 'o' + 'm' + 'd' + 'b' + '2'
#define null_str "NULL"
#define DBTYPES_COMMON		\
	int magic;		\
	dbtypes_enum dbtype;	\
	int is_typed;		\
	int is_null

typedef struct {
    DBTYPES_COMMON;
} lua_dbtypes_t;

typedef struct {
    DBTYPES_COMMON;
    long long val;
} lua_int_t;

typedef struct {
    DBTYPES_COMMON;
    double val;
} lua_real_t;

typedef struct {
    DBTYPES_COMMON;
    decQuad val;
} lua_dec_t;

typedef struct {
    DBTYPES_COMMON;
    datetime_t val;
} lua_datetime_t;

typedef struct {
    DBTYPES_COMMON;
    intv_t val;
} lua_intervalym_t;

typedef struct {
    DBTYPES_COMMON;
    intv_t val;
} lua_intervalds_t;

typedef struct {
    DBTYPES_COMMON;
    char* val;
} lua_cstring_t;

typedef struct {
    int length;
    void *data;
} blob_t;

typedef struct {
    DBTYPES_COMMON;
    blob_t val;
} lua_blob_t;

#define init_new_t(n, DBTYPE) \
    (n)->magic = DBTYPES_MAGIC;\
    (n)->is_null = 0;\
    (n)->is_typed = gbl_spstrictassignments;\
    (n)->dbtype = DBTYPE

#define new_lua_t_sz(L, n, lua_t, DBTYPE, sz) \
    n = lua_newuserdata(L, sz);\
    bzero(n, sz);\
    init_new_t(n, DBTYPE);\
    luaL_getmetatable(L, dbtypes_str[DBTYPE]);\
    lua_setmetatable(L, -2)

#define new_lua_t(n, lua_t, DBTYPE) new_lua_t_sz(lua, n, lua_t, DBTYPE, sizeof(lua_t))

typedef union {
    long long in;
    double rl;
    decQuad dq;
    datetime_t dt;
    intv_t iv;
    char *cs;
    blob_t bl;
} all_types_t;

#endif
