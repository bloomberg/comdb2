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

#ifndef INCLUDED_SP_INT_H
#define INCLUDED_SP_INT_H

#include <berkdb/dbinc/queue.h>

#include <lua.h>
#include <lauxlib.h>
#include <ltypes.h>

#include <types.h>
#include <sbuf2.h>
#include <comdb2.h>
#include <sp.h>


typedef struct stored_proc *SP;
typedef struct dbstmt_t dbstmt_t;
typedef struct tmptbl_info_t tmptbl_info_t;
struct stored_proc {
    Lua lua;
    int lua_version;
    comdb2ma mspace;
    char spname[MAX_SPNAME];
    struct spversion_t spversion;
    char *src;
    struct sqlclntstate *clnt;
    struct sqlclntstate *debug_clnt;
    struct sqlthdstate *thd;
    int bufsz;
    long long nrows;
    int num_instructions;
    int max_num_instructions;
    uint8_t *buf;
    char *error;
    int  rc;
    SP parent;

    pthread_mutex_t *emit_mutex; //parent only
    int ntypes; //parent only
    char **clntname; //parent only
    int *clnttype; //parent only

    LIST_HEAD(, dbstmt_t) dbstmts;
    LIST_HEAD(, tmptbl_info_t) tmptbls;

    dbstmt_t *prev_dbstmt; // for db_bind -- deprecated

    unsigned initial           : 1;
    unsigned pingpong          : 1;
    unsigned have_consumer     : 1;
    unsigned in_parent_trans   : 1;
    unsigned make_parent_trans : 1;
};

#define getsp(x) ((SP)lua_getsp(x))

void luabb_toblob(Lua, int index, blob_t *);
const char *luabb_tostring(Lua, int index);
void luabb_todatetime(Lua, int index, datetime_t *);
void luabb_tointeger(Lua, int index, long long *);
void luabb_tointervalds(Lua, int index, intv_t *);
void luabb_tointervalym(Lua, int index, intv_t *);
void luabb_toreal(Lua, int index, double *);
void luabb_todecimal(Lua, int index, decQuad *);

void luabb_pushblob(Lua, const blob_t *);
void luabb_pushblob_dl(Lua, const blob_t *); //dl -> dup-less
void luabb_pushcstring(Lua, const char *);
void luabb_pushcstring_dl(Lua, const char *); //dl -> dup-less
void luabb_pushcstringlen(Lua, const char *, int len); //don't call strlen
void luabb_pushdatetime(Lua, const datetime_t *);
void luabb_pushdecimal(Lua, const decQuad *);
void luabb_pushinteger(Lua, long long);
void luabb_pushintervalds(Lua, const intv_t *);
void luabb_pushintervalym(Lua, const intv_t *);
void luabb_pushnull(Lua, int dbtype);
void luabb_pushreal(Lua, double);

int luabb_isnull(Lua, int index);
int luabb_istyped(Lua, int index);

/* must be called before any exposed functionality is available in lua */
void init_dbtypes(Lua);

void client_datetime_to_datetime_t(const cdb2_client_datetime_t *, datetime_t *, int flip);
void client_datetimeus_to_datetime_t(const cdb2_client_datetimeus_t *, datetime_t *, int flip);
void datetime_t_to_client_datetime(const datetime_t *, cdb2_client_datetime_t *);
void datetime_t_to_client_datetimeus(const datetime_t *, cdb2_client_datetimeus_t *);
void datetime_t_to_dttz(const datetime_t *, dttz_t *);
void init_sys_funcs(Lua);
void dttz_to_datetime_t(const dttz_t *, const char *tz, datetime_t *);

int db_csvcopy(Lua lua);

char* find_syssp(const char *);

#endif
