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

#ifndef LUAGLUE_H
#define LUAGLUE_H

struct lua_State;
struct lua_TValue;

enum dbtypes_enum;
int luabb_type_by_name(const char *);
int luabb_dbtype_by_name(const char *);
enum dbtypes_enum;
const char *luabb_dbtype_to_str(enum dbtypes_enum);
void luabb_typeconvert(struct lua_State *, int pos, int type);
char *luabb_newblob(struct lua_State *, int len, void **blob);
int parseblob(const char *str, int len, char *out);
typedef enum {
	Invalid,
	Numeric,
	String
} HashType;
HashType luabb_hashinfo(void *udata, double *, const char **, size_t *);
int luabb_eq(const struct lua_TValue *, const struct lua_TValue *, int *eq);
int luabb_dbtype_from_tvalue(struct lua_TValue *);
int luabb_isnumber(struct lua_State *, int idx);
double luabb_tonumber(struct lua_State *, int idx);
int luabb_iscstring(struct lua_State *, int idx);
const char *luabb_tocstring(struct lua_State *, int idx);
int luabb_isnull(struct lua_State *, int idx);
void luabb_tointeger(struct lua_State *, int idx, long long *val);

#endif /* LUAGLUE_H */
