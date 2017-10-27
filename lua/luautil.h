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

#ifndef INCLUDED_LUAUTIL_H
#define INCLUDED_LUAUTIL_H

#include <sp_int.h>
#include <logmsg.h>

const char *luabb_pushfstring(lua_State *lua, char *fmt, ...);
void luabb_dumpcstack_(Lua);
#define luabb_dumpcstack(L)                                                    \
    do {                                                                       \
        logmsg(LOGMSG_DEBUG, "\n---- %s @ %s:%d ----\n", __func__, __FILE__, __LINE__);      \
        luabb_dumpcstack_(L);                                                  \
        logmsg(LOGMSG_DEBUG, "---- luabb_dumpcstack finished ----\n\n");                     \
    } while (0)
int luabb_istype(lua_State *lua, int index, dbtypes_enum);
int luabb_error(lua_State *lua, struct stored_proc *sp, const char *fmt, ...);
int luabb_issql_type(lua_State *lua, int index, int sql_type);
void luabb_typeconvert_int(struct lua_State *, int from_pos, dbtypes_enum to, const char *to_str);
void lua_datetime_to_client(lua_datetime_t *ld, cdb2_client_datetime_t *val);
void client_datetime_to_lua(cdb2_client_datetime_t *val, lua_datetime_t *ld);
int precompile_lua(char *lua_buffer, void **compiled_data, int *size);
const char *luabb_dbtypename(Lua, int idx);
#define luabb_dbtype(l, i) luabb_type(l, i)
dbtypes_enum luabb_type(Lua, int idx);
int to_positive_index(Lua, int);
void luabb_fromhex(uint8_t *out, const uint8_t *in, size_t len);
#endif
