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

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stddef.h>
#include <stdarg.h>
#include <assert.h>
#include <alloca.h>

#include <types.h>
#include <sqlite3.h>

#include <lundump.h>
#include <lua.h>
#include <lauxlib.h>
#include <lobject.h>
#include <lstate.h>

#include <ltypes.h>
#include <luautil.h>
#include <luaglue.h>
#include <logmsg.h>

static int lua_writer1(lua_State* lua, const void* p, size_t size, void* u)
{
    UNUSED(lua);
    int *total_size = (int*)u;
    *total_size = *total_size + size;
    return 0;
}

static int lua_writer2(lua_State* lua, const void* p, size_t size, void* u)
{
    char **data = (char**)u;
    UNUSED(lua);
    if (size) {
     memcpy(*data,p,size);
     *data = *data + size;
    }
    return 0;
}

static int l_panic(lua_State *lua) {
    const char *err;
    err = lua_tostring(lua, -1);
    logmsg(LOGMSG_ERROR, "PANIC: %s\n", err);
    return 0;
}

int precompile_lua(char *lua_buffer, void **compiled_data, int *size) {
  lua_State *lua;
  Proto *f;
  lua=lua_open();
  luaL_loadstring(lua,lua_buffer);
  lua_atpanic(lua, l_panic);
  const Closure* c=(const Closure*)lua_topointer(lua,-1);
  if (c) {
    f = c->l.p;
  } else {
      return -1;
  }
  luaU_dump(lua,f,lua_writer1,size, 0);
  *compiled_data = malloc(*size);
  void *data = NULL;
  data = *compiled_data;
  luaU_dump(lua,f,lua_writer2,&data, 0);
  return 0;
}

const char *luabb_pushfstring(lua_State *lua, char *fmt, ...) {
    char c[1];
    va_list args;
    int len;
    char *out = NULL;
    va_start(args, fmt);
    len = vsnprintf(c, 1, fmt, args);
    va_end(args);
    if (len == 0)
        out = strdup("");
    else if (len < 0)
        luaL_error(lua, "can't format string");
    else {
        out = malloc(len+2);
        va_start(args, fmt);
        vsnprintf(out, len+1, fmt, args);
        out[len+1] = 0;
        va_end(args);
    }
    lua_pushstring(lua, out);
    free(out);
    out = (char*) lua_tostring(lua, -1);
    return (const char*) out;
}

//don't call from any of the l_*_tostring functions
void luabb_dumpcstack_(Lua lua)
{
    int i;
    int top = lua_gettop(lua);

    for (i = top; i;  --i) {
        logmsg(LOGMSG_USER, "%2d: ", i);
        switch (lua_type(lua, i)) {
        case LUA_TNIL:
            logmsg(LOGMSG_USER, "nil\n");
            break;
        case LUA_TBOOLEAN:
            logmsg(LOGMSG_USER, "boolean: %s\n", lua_toboolean(lua, i) == 0 ? "false" : "true");
            break;
        case LUA_TLIGHTUSERDATA:
            logmsg(LOGMSG_USER, "lightuserdata\n");
            break;
        case LUA_TNUMBER:
            logmsg(LOGMSG_USER, "number: %f\n", lua_tonumber(lua, i));
            break;
        case LUA_TSTRING:
            logmsg(LOGMSG_USER, "string: \"%s\"\n", lua_tostring(lua, i));
            break;
        case LUA_TTABLE:
            logmsg(LOGMSG_USER, "table\n");
            break;
        case LUA_TFUNCTION:
            logmsg(LOGMSG_USER, "function\n");
            break;
        case LUA_TTHREAD:
            logmsg(LOGMSG_USER, "thread\n");
            break;
        case LUA_TUSERDATA: {
            dbtypes_enum luabb_dbtype(lua_State *, int index);
            dbtypes_enum t = luabb_dbtype(lua, i);
            if (t >= DBTYPES_MAXTYPE) {
                logmsg(LOGMSG_USER, "invalid type\n");
            } else {
                if (dbtypes_tostring[t]) {
                    logmsg(LOGMSG_USER, "%s: %s\n", dbtypes_str[t], luabb_tostring(lua, i));
                } else {
                    logmsg(LOGMSG_USER, "%s\n", dbtypes_str[t]);
                }
            }
            break;
        }
        default:
            logmsg(LOGMSG_USER, "unknown type %d\n",lua_type(lua, i));
            break;

        }
    }
}

int luabb_issql_type(lua_State *lua, int index, int sql_type)
{
    dbtypes_enum type;
    switch (sql_type) {
    case SQLITE_INTEGER:
        type = DBTYPES_INTEGER;
        break;
    case SQLITE_FLOAT:
        type = DBTYPES_REAL;
        break;
    case SQLITE_TEXT:
        type = DBTYPES_CSTRING;
        break;
    case SQLITE_DATETIME:
    case SQLITE_DATETIMEUS:
        type = DBTYPES_DATETIME;
        break;
    case SQLITE_BLOB:
        type = DBTYPES_BLOB;
        break;
    case SQLITE_INTERVAL_YM:
        type = DBTYPES_INTERVALYM;
        break;
    case SQLITE_INTERVAL_DS:
    case SQLITE_INTERVAL_DSUS:
        type = DBTYPES_INTERVALDS;
        break;
    case SQLITE_DECIMAL:
        type = DBTYPES_DECIMAL;
        break;
    default:
        return 0;
    }
    return luabb_istype(lua, index, type);
}

dbtypes_enum luabb_type(lua_State *lua, int index)
{
    if (index > lua_gettop(lua)) return DBTYPES_MAXTYPE;
    dbtypes_enum t = lua_type(lua, index);
    if (t != LUA_TUSERDATA)
        return t;
    const lua_dbtypes_t *v = lua_topointer(lua, index);
    return v->dbtype;
}

const char *luabb_dbtypename(Lua lua, int index)
{
    dbtypes_enum t = luabb_dbtype(lua, index);
    if (t < DBTYPES_MAXTYPE) return dbtypes_str[t];
    return NULL;
}


int luabb_istype(lua_State *lua, int index, dbtypes_enum type)
{
    if (index > lua_gettop(lua)) return 0;
    if (lua_type(lua, index) != LUA_TUSERDATA) return 0;
    const lua_dbtypes_t *t = lua_topointer(lua, index);
    assert(t->magic == DBTYPES_MAGIC);
    return t->dbtype == type;
}

int luabb_error(Lua lua, SP sp, const char *fmt, ...)
{
    char c[1];
    va_list args;
    int len;
    char *out = NULL;
    va_start(args, fmt);
    len = vsnprintf(c, 1, fmt, args);
    va_end(args);

    if (len == 0)
        out = sp ? strdup("") : "";
    else if (len < 0)
        luaL_error(lua, "can't format string");
    else {
        out = sp ? malloc(len + 2) : alloca(len + 2);
        va_start(args, fmt);
        vsnprintf(out, len+1, fmt, args);
        out[len+1] = 0;
        va_end(args);
    }

    extern int gbl_allow_lua_print;
    if (gbl_allow_lua_print)  {
        luaL_where(lua, 0);
        lua_pop(lua, 1);
#if 0
        const char *src = lua_tostring(lua, -1);
        if (sp && sp->spname)
            fprintf(stderr, "ERROR in SP:%s Ver:%d %s:%s\n",
              sp->spname, sp->spversion, out, src);
        else  
            fprintf(stderr, "ERROR:%s %s]\n", src, out);
#endif
    }

    if (sp) {
      if(sp->rc == 0)
          sp->rc = -1;
      lua_pushnil(lua);
      lua_pushnumber(lua, sp->rc);
      if (sp->error)
          free(sp->error);
      sp->error = out;
      return 1; /* Just returning error code. */
    } else {
      lua_pushnumber(lua, -1);
      return luaL_error(lua, out);
    }
}

int luabb_type_by_name_int(const char *name, dbtypes_enum min)
{
    if (strncmp(name, dbtypes_pfx, sizeof(dbtypes_pfx) - 1) == 0)
        name += (sizeof(dbtypes_pfx) - 1);
    int i;
    for (i = min; i < DBTYPES_MAXTYPE; ++i) {
        if (strcmp(name, dbtypes_str[i]) == 0) break;
    }
    return (i == DBTYPES_MAXTYPE) ? 0 : i;
}

int luabb_type_by_name(const char *name)
{
    return luabb_type_by_name_int(name, 0);
}

int luabb_dbtype_by_name(const char *name)
{
    return luabb_type_by_name_int(name, DBTYPES_MINTYPE);
}

const char *luabb_type_to_str(dbtypes_enum type)
{
        return dbtypes_str[type];
}

const char *luabb_dbtype_to_str(dbtypes_enum type)
{
    if (type > DBTYPES_MINTYPE && type < DBTYPES_MAXTYPE)
        return luabb_type_to_str(type);
    return NULL;
}

void luabb_typeconvert_int(Lua l, int pos, dbtypes_enum to, const char *to_str)
{
    dbtypes_enum from = luabb_type(l, pos);
    if (from == to) {
        lua_pushvalue(l, pos);
        return;
    }
    if (lua_isnil(l, pos)) {
        if (to < DBTYPES_MINTYPE)
            lua_pushnil(l);
        else
            luabb_pushnull(l, to);
        return;
    }
    if (luabb_isnull(l, pos)) {
        if (to < DBTYPES_MINTYPE)
            lua_pushnil(l);
        else
            luabb_pushnull(l, to);
        return;
    }

    all_types_t u;
    switch (to) {
    case DBTYPES_LSTRING:
        // call lua's tostring() -- will call __tostring metamethod
        lua_getglobal(l, "tostring");
        lua_pushvalue(l, pos);
        lua_call(l, 1, 1);
        break;
    case DBTYPES_LNUMBER:
        // __tonumber metamethod doesn't exist
        luabb_toreal(l, pos, &u.rl);
        lua_pushnumber(l, u.rl);
        break;
    case DBTYPES_INTEGER:
        luabb_tointeger(l, pos, &u.in);
        luabb_pushinteger(l, u.in);
        break;
    case DBTYPES_CSTRING:
        u.cs = strdup(luabb_tostring(l, pos));
        luabb_pushcstring(l, u.cs);
        free(u.cs);
        break;
    case DBTYPES_REAL:
        luabb_toreal(l, pos, &u.rl);
        luabb_pushreal(l, u.rl);
        break;
    case DBTYPES_DATETIME:
        luabb_todatetime(l, pos, &u.dt);
        luabb_pushdatetime(l, &u.dt);
        break;
    case DBTYPES_DECIMAL:
        luabb_todecimal(l, pos, &u.dq);
        luabb_pushdecimal(l, &u.dq);
        break;
    case DBTYPES_BLOB:
        luabb_toblob(l, pos, &u.bl);
        luabb_pushblob(l, &u.bl);
        break;
    case DBTYPES_INTERVALYM:
        luabb_tointervalym(l, pos, &u.iv);
        luabb_pushintervalym(l, &u.iv);
        break;
    case DBTYPES_INTERVALDS:
        luabb_tointervalds(l, pos, &u.iv);
        luabb_pushintervalds(l, &u.iv);
        break;
    default:
        luaL_error(l, "conversion failed %s -> %s",
          luabb_type_to_str(from), to_str ? to_str : luabb_type_to_str(to));
        break;
    }
}

void luabb_typeconvert(Lua l, int pos, int type)
{
    luabb_typeconvert_int(l, pos, type, NULL);
    /* Pushed the converted type on the top of the stack.
     * Just swap with the orig arg. */
    lua_replace(l, pos);
}

#define luabb_todbpointer(x) ((lua_dbtypes_t *)(rawuvalue(x) + 1))

HashType luabb_hashinfo(void *udata, double *d, const char **c, size_t *l)
{
    const lua_dbtypes_t *t = udata;
    if (t->magic != DBTYPES_MAGIC || t->is_null) return Invalid;
    switch (t->dbtype) {
    case DBTYPES_INTEGER:
        if (d != NULL)
            *d = ((lua_int_t *)t)->val;
        return Numeric;
    case DBTYPES_REAL:
        if (d != NULL)
            *d = ((lua_real_t *)t)->val;
        return Numeric;
    case DBTYPES_INTERVALDS:
        *d = interval_to_double(&((lua_intervalds_t *)t)->val);
        return Numeric;
    case DBTYPES_INTERVALYM:
        *d = interval_to_double(&((lua_intervalym_t *)t)->val);
        return Numeric;
    //case DBTYPES_DATETIME:
    //    return Numeric;
    case DBTYPES_CSTRING:
    case DBTYPES_BLOB:
        if (c != NULL && l != NULL) {
            if (t->dbtype == DBTYPES_CSTRING) {
                *c = ((lua_cstring_t *)t)->val;
                *l = strlen(*c);
            } else {
                *c = ((lua_blob_t *)t)->val.data;
                *l = ((lua_blob_t *)t)->val.length;
            }
        }
        return String;
    }
    return Invalid;
}

static int numeq(lua_dbtypes_t *b1, TValue *t2, int *eq)
{
    double d1 = 0, d2;
    lua_dbtypes_t *b2;
    luabb_hashinfo(b1, &d1, NULL, NULL);
    switch(ttype(t2)) {
    case LUA_TNUMBER:
        d2 = nvalue(t2);
        *eq = (d1 == d2);
        return 0;
    case LUA_TUSERDATA:
        b2 = luabb_todbpointer(t2);
        if (luabb_hashinfo(b2, &d2, NULL, NULL) == Numeric) {
            *eq = (d1 == d2);
            return 0;
        }
    }
    return 1;
}

static int streq(lua_cstring_t *b1, TValue *t2, int *eq)
{
    const char *s1, *s2 = NULL;
    lua_cstring_t *b2;
    s1 = b1->val;
    switch (ttype(t2)) {
    case LUA_TSTRING:
        s2 = svalue(t2);
        break;
    case LUA_TUSERDATA:
        b2 = (lua_cstring_t *)luabb_todbpointer(t2);
        s2 = b2->val;
    }
    if (s1 == NULL ||  s2 == NULL)
        return 1;
    *eq = (strcmp(s1, s2) == 0);
    return 0;
}

static int sameq(lua_dbtypes_t *t1, lua_dbtypes_t *t2, int *eq)
{
    if (t1->magic != DBTYPES_MAGIC || t2->magic != DBTYPES_MAGIC)
        return 1;
    if (t1->dbtype != t2->dbtype)
        return 1;
    blob_t b1, b2;
    switch (t1->dbtype) {
    case DBTYPES_BLOB:
        b1 = ((lua_blob_t *)t1)->val;
        b2 = ((lua_blob_t *)t2)->val;
        if (b1.length == b2.length)
            *eq = !memcmp(b1.data, b2.data, b1.length);
        else
            *eq = 0;
        return 0;
    case DBTYPES_INTERVALDS:
        *eq = !memcmp(t1, t2, sizeof(lua_intervalds_t));
        return 0;
    case DBTYPES_INTERVALYM:
        *eq = !memcmp(t1, t2, sizeof(lua_intervalym_t));
        return 0;
    case DBTYPES_DATETIME:
        *eq = !memcmp(t1, t2, sizeof(lua_datetime_t));
        return 0;
    default:
        logmsg(LOGMSG_ERROR, "%s - not implemented -> %s == %s\n", __func__,
          luabb_dbtype_to_str(t1->dbtype), luabb_dbtype_to_str(t2->dbtype));
    }
    return 1;
}

int luabb_eq(const TValue *t1_, const TValue *t2_, int *eq)
{
    TValue *t1, *t2;
    if (ttype((TValue *)t1_) == LUA_TUSERDATA) {
        t1 = (TValue *)t1_;
        t2 = (TValue *)t2_;
    } else if (ttype((TValue *)t2_) == LUA_TUSERDATA) {
        t1 = (TValue *)t2_;
        t2 = (TValue *)t1_;
    } else {
        logmsg(LOGMSG_WARN, "%s misuse: one arg must be a comdb2 type\n", __func__);
        return 1;
    }

    lua_dbtypes_t *b1, *b2;
    b1 = luabb_todbpointer(t1);
    switch (b1->dbtype) {
    case DBTYPES_INTEGER:
    case DBTYPES_REAL:
    case DBTYPES_DECIMAL:
        return numeq(b1, t2, eq);
    case DBTYPES_CSTRING:
        return streq((lua_cstring_t *)b1, t2, eq);
    default:
        if (ttype(t1) != ttype(t2))
            break;
        b2 = luabb_todbpointer(t2);
        if (b1->dbtype != b2->dbtype)
            break;
        return sameq(b1, b2, eq);
    }
    return 1;
}

int luabb_dbtype_from_tvalue(TValue *t)
{
    lua_dbtypes_t *d = luabb_todbpointer(t);
    if (d->magic == DBTYPES_MAGIC)
        return d->dbtype;
    return DBTYPES_MAXTYPE;
}

int luabb_isnumber(Lua l, int idx)
{
    const TValue *o = index2adr(l, idx);
    if (!ttisuserdata(o)) return 0;
    lua_dbtypes_t *bb = luabb_todbpointer(o);
    switch (bb->dbtype) {
    case DBTYPES_INTEGER:
    case DBTYPES_REAL:
        return 1;
    default:
        return 0;
    }
}

/* assumes luabb_isnumber is true */
double luabb_tonumber(Lua l, int idx)
{
    const TValue *o = index2adr(l, idx);
    lua_dbtypes_t *bb = luabb_todbpointer(o);
    switch (bb->dbtype) {
    case DBTYPES_INTEGER: return ((lua_int_t *)bb)->val;
    case DBTYPES_REAL: return ((lua_real_t *)bb)->val;
    default: abort();
    }
}

int luabb_iscstring(Lua l, int idx)
{
    const TValue *o = index2adr(l, idx);
    if (!ttisuserdata(o)) return 0;
    lua_dbtypes_t *bb = luabb_todbpointer(o);
    if (bb->is_null) return 0;
    return bb->dbtype == DBTYPES_CSTRING;
}

/* assumes luabb_iscstring is true */
const char *luabb_tocstring(Lua l, int idx)
{
    const TValue *o = index2adr(l, idx);
    lua_dbtypes_t *bb = luabb_todbpointer(o);
    if (bb->dbtype != DBTYPES_CSTRING) abort();
    lua_cstring_t *ptr = (lua_cstring_t *)bb;
    return ptr->val;
}

/* out should be appropriately sized */
void luabb_fromhex(uint8_t *out, const uint8_t *in, size_t len)
{
    const uint8_t *end = in + len;
    while (in != end) {
        uint8_t i0 = tolower(*(in++));
        uint8_t i1 = tolower(*(in++));
        i0 -= isdigit(i0) ? '0' : ('a' - 0xa);
        i1 -= isdigit(i1) ? '0' : ('a' - 0xa);
        *(out++) = (i0 << 4) | i1;
    }
}
