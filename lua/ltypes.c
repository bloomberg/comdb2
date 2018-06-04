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
#include <limits.h>
#include <math.h>
#include <time.h>
#include <assert.h>
#include <alloca.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <luautil.h>
#include <comdb2.h>
#include <sql.h>

#include <flibc.h>
#include <luaglue.h>
#include <sp.h>
#include "util.h"

dbtypes_t dbtypes;

#define XMACRO_DBTYPES(type, name, structname, tostring) name,
const char *dbtypes_str[] = { DBTYPES };
#undef XMACRO_DBTYPES

static int l_int_tostring(Lua);
static int l_cstring_tostring(Lua);
static int l_real_tostring(Lua);
static int l_datetime_tostring(Lua);
static int l_decimal_tostring(Lua);
static int l_blob_tostring(Lua);
static int l_interval_tostring(Lua);

#define XMACRO_DBTYPES(type, name, structname, tostring) tostring,
const tostringfunc dbtypes_tostring[] = { DBTYPES };
#undef XMACRO_DBTYPES

#define nullchk(lua, idx)                                                      \
    do {                                                                       \
        if (luabb_dbtype(lua, idx) > DBTYPES_MINTYPE) {                        \
            const lua_dbtypes_t *t = lua_topointer(lua, idx);                  \
            if (t->is_null) {                                                  \
                return luaL_error(                                             \
                    lua, "attempt to perform arithmetic on a NULL value");     \
            }                                                                  \
        }                                                                      \
    } while (0)
static int typecmperr(Lua lua, int i1, int i2)
{
    return luabb_error(lua, NULL, "attempt to compare %s with %s",
      luabb_dbtypename(lua, i1), luabb_dbtypename(lua, i2));
}

static int l_datetime_tostring_int(Lua, int);
static int l_blob_tostring_int(Lua, int);

void datetime_t_to_client_datetime(const datetime_t *in, cdb2_client_datetime_t *out)
{
    out->tm.tm_sec   = htonl(in->tm.tm_sec);
    out->tm.tm_min   = htonl(in->tm.tm_min);
    out->tm.tm_hour  = htonl(in->tm.tm_hour);
    out->tm.tm_mday  = htonl(in->tm.tm_mday);
    out->tm.tm_mon   = htonl(in->tm.tm_mon);
    out->tm.tm_year  = htonl(in->tm.tm_year);
    out->tm.tm_wday  = htonl(in->tm.tm_wday);
    out->tm.tm_yday  = htonl(in->tm.tm_yday);
    out->tm.tm_isdst = htonl(in->tm.tm_isdst);
    out->msec        = (in->prec == DTTZ_PREC_MSEC) ? 
                        htonl(in->frac) : 
                        htonl(in->frac * 1000);
    strcpy(out->tzname, in->tzname);
}

void datetime_t_to_client_datetimeus(const datetime_t *in, cdb2_client_datetimeus_t *out)
{
    out->tm.tm_sec   = htonl(in->tm.tm_sec);
    out->tm.tm_min   = htonl(in->tm.tm_min);
    out->tm.tm_hour  = htonl(in->tm.tm_hour);
    out->tm.tm_mday  = htonl(in->tm.tm_mday);
    out->tm.tm_mon   = htonl(in->tm.tm_mon);
    out->tm.tm_year  = htonl(in->tm.tm_year);
    out->tm.tm_wday  = htonl(in->tm.tm_wday);
    out->tm.tm_yday  = htonl(in->tm.tm_yday);
    out->tm.tm_isdst = htonl(in->tm.tm_isdst);
    out->usec        = (in->prec == DTTZ_PREC_MSEC) ? 
                        htonl(in->frac) * 1000 :
                        htonl(in->frac);
    strcpy(out->tzname, in->tzname);
}

void client_datetime_to_datetime_t(const cdb2_client_datetime_t *cdt,
  datetime_t *datetime, int flip)
{
    datetime->prec = DTTZ_PREC_MSEC;
    strcpy(datetime->tzname, cdt->tzname);
    if (flip) {
        datetime->tm.tm_sec   = flibc_intflip(cdt->tm.tm_sec);
        datetime->tm.tm_min   = flibc_intflip(cdt->tm.tm_min);
        datetime->tm.tm_hour  = flibc_intflip(cdt->tm.tm_hour);
        datetime->tm.tm_mday  = flibc_intflip(cdt->tm.tm_mday);
        datetime->tm.tm_mon   = flibc_intflip(cdt->tm.tm_mon);
        datetime->tm.tm_year  = flibc_intflip(cdt->tm.tm_year);
        datetime->tm.tm_wday  = flibc_intflip(cdt->tm.tm_wday);
        datetime->tm.tm_yday  = flibc_intflip(cdt->tm.tm_yday);
        datetime->tm.tm_isdst = flibc_intflip(cdt->tm.tm_isdst);
        datetime->frac        = flibc_intflip(cdt->msec);
    } else {
        datetime->tm.tm_sec   = cdt->tm.tm_sec;
        datetime->tm.tm_min   = cdt->tm.tm_min;
        datetime->tm.tm_hour  = cdt->tm.tm_hour;
        datetime->tm.tm_mday  = cdt->tm.tm_mday;
        datetime->tm.tm_mon   = cdt->tm.tm_mon;
        datetime->tm.tm_year  = cdt->tm.tm_year;
        datetime->tm.tm_wday  = cdt->tm.tm_wday;
        datetime->tm.tm_yday  = cdt->tm.tm_yday;
        datetime->tm.tm_isdst = cdt->tm.tm_isdst;
        datetime->frac        = cdt->msec;
    }
}

void client_datetimeus_to_datetime_t(const cdb2_client_datetimeus_t *cdt,
  datetime_t *datetime, int flip)
{
    datetime->prec = DTTZ_PREC_USEC;
    strcpy(datetime->tzname, cdt->tzname);
    if (flip) {
        datetime->tm.tm_sec   = flibc_intflip(cdt->tm.tm_sec);
        datetime->tm.tm_min   = flibc_intflip(cdt->tm.tm_min);
        datetime->tm.tm_hour  = flibc_intflip(cdt->tm.tm_hour);
        datetime->tm.tm_mday  = flibc_intflip(cdt->tm.tm_mday);
        datetime->tm.tm_mon   = flibc_intflip(cdt->tm.tm_mon);
        datetime->tm.tm_year  = flibc_intflip(cdt->tm.tm_year);
        datetime->tm.tm_wday  = flibc_intflip(cdt->tm.tm_wday);
        datetime->tm.tm_yday  = flibc_intflip(cdt->tm.tm_yday);
        datetime->tm.tm_isdst = flibc_intflip(cdt->tm.tm_isdst);
        datetime->frac        = flibc_intflip(cdt->usec);
    } else {
        datetime->tm.tm_sec   = cdt->tm.tm_sec;
        datetime->tm.tm_min   = cdt->tm.tm_min;
        datetime->tm.tm_hour  = cdt->tm.tm_hour;
        datetime->tm.tm_mday  = cdt->tm.tm_mday;
        datetime->tm.tm_mon   = cdt->tm.tm_mon;
        datetime->tm.tm_year  = cdt->tm.tm_year;
        datetime->tm.tm_wday  = cdt->tm.tm_wday;
        datetime->tm.tm_yday  = cdt->tm.tm_yday;
        datetime->tm.tm_isdst = cdt->tm.tm_isdst;
        datetime->frac        = cdt->usec;
    }
}

void datetime_t_to_dttz(const datetime_t *datetime, dttz_t *dt)
{
    cdb2_client_datetime_t cdt;
    cdb2_client_datetimeus_t cdtus;

    if (datetime->prec == DTTZ_PREC_MSEC) {
        datetime_t_to_client_datetime(datetime, &cdt);
        client_datetime_to_dttz(&cdt, datetime->tzname, dt, 0);
    } else {
        datetime_t_to_client_datetimeus(datetime, &cdtus);
        client_datetimeus_to_dttz(&cdtus, datetime->tzname, dt, 0);
    }
}

void dttz_to_datetime_t(const dttz_t *dt, const char *tz, datetime_t *datetime)
{
    cdb2_client_datetime_t cdt;
    cdb2_client_datetimeus_t cdtus;

    if (datetime->prec == DTTZ_PREC_MSEC) {
        dttz_to_client_datetime(dt, tz, &cdt);
        client_datetime_to_datetime_t(&cdt, datetime, 0);
    } else {
        dttz_to_client_datetimeus(dt, tz, &cdtus);
        client_datetimeus_to_datetime_t(&cdtus, datetime, 0);
    }
}

void init_lua_dbtypes(void)
{
    int i;
    const char **p = (const char **) &dbtypes;
    for (i = 0; i < DBTYPES_MAXTYPE; ++i, ++p) {
        *p = dbtypes_str[i];
    }
}

static int luabb_type_err(Lua lua, const char *to, int from)
{
    int type = lua_type(lua, from);
    const char *ourtype = luabb_dbtypename(lua, from);
    return luaL_error(lua, "conversion to: %s failed from: %s",
      to, ourtype ? ourtype : lua_typename(lua, type));
}

/* Trying to make this work ->
 * i := '10.0' -- declared as int */
static int parseint(const char *str, long long *val)
{
    char *endp;
    *val = strtoll(str, &endp, 10);
    if (*endp == 0) return 0;
    double d = strtod(str, &endp);
    if (*endp != 0) return 1;
    *val = d;
#if 0
    if (*val == d) return 0; // lossless?
    return 1;
#else
    return 0;
#endif
}

void luabb_tointeger(Lua lua, int idx, long long *val)
{
    const lua_intervalym_t *ym;
    const lua_intervalds_t *ds;
    const lua_datetime_t *d;
    cdb2_client_datetime_t c;
    dttz_t dt;
    double darg;
    const char *s;

    switch (luabb_dbtype(lua, idx)) {

    case DBTYPES_CSTRING:
        s = ((lua_cstring_t *)lua_topointer(lua, idx))->val;
        goto str;

    case LUA_TSTRING:
        s = lua_tostring(lua, idx);
    str:if (parseint(s, val) != 0)
            luaL_error(lua, "conversion to: int failed from: %s", s);
        return;

    case LUA_TNUMBER:
        darg = lua_tonumber(lua, idx);
        goto dbl;

    case DBTYPES_REAL:
        darg = ((lua_real_t *)lua_topointer(lua, idx))->val;
    dbl:if (darg < LLONG_MIN || darg > LLONG_MAX)
            luaL_error(lua, "conversion to: int failed from: %f", darg);
        *val= (long long) darg;
        return;

    case DBTYPES_INTEGER:
        *val = ((lua_int_t *)lua_topointer(lua, idx))->val;
        return;

    case DBTYPES_DATETIME:
        d = lua_topointer(lua, idx);
        /* Precision does not matter as we only need the whole seconds. */
        datetime_t_to_client_datetime(&d->val, &c);
        if (client_datetime_to_dttz(&c, c.tzname, &dt, 0) == 0) {
            *val = dt.dttz_sec;
            return;
        }
        break;

    case DBTYPES_INTERVALYM:
        ym = lua_topointer(lua, idx);
        *val = interval_to_double(&ym->val);
        return;

    case DBTYPES_INTERVALDS:
        ds = lua_topointer(lua, idx);
        *val = interval_to_double(&ds->val);
        return;
    }

    luabb_type_err(lua, dbtypes.integer, idx);
    *val = 0; //just to silence warnings -- will never reach here
}

void luabb_toreal(Lua lua, int idx, double *ret)
{
    const lua_intervalym_t *ym;
    const lua_intervalds_t *ds;
    const lua_datetime_t *d;
    cdb2_client_datetimeus_t c; /* convert to usec to avoid loss of precision */
    dttz_t dt;
    const char *s;
    char *e;

    switch (luabb_dbtype(lua, idx)) {
    case DBTYPES_INTEGER:
        *ret = ((lua_int_t *)lua_topointer(lua, idx))->val;
        return;
    case DBTYPES_LNUMBER:
        *ret = lua_tonumber(lua, idx);
        return;
    case DBTYPES_REAL:
        *ret = ((lua_real_t *)lua_topointer(lua, idx))->val;
        return;
    case DBTYPES_LSTRING:
        s = lua_tostring(lua, idx);
        goto str;
    case DBTYPES_CSTRING:
        s = ((lua_cstring_t *)lua_topointer(lua, idx))->val;
    str:errno = 0;
        *ret = strtod(s, &e);
        if (errno == 0)
            return;
        break;
    case DBTYPES_DATETIME:
        d = lua_topointer(lua, idx);
        /* Use highest precision so we don't lose precision. */
        datetime_t_to_client_datetimeus(&d->val, &c);
        if (client_datetimeus_to_dttz(&c, c.tzname, &dt, 0) == 0) {
            *ret = (double)dt.dttz_sec + (dt.dttz_frac / 1E6);
            return;
        }
        break;
    case DBTYPES_INTERVALYM:
        ym = lua_topointer(lua, idx);
        *ret = interval_to_double(&ym->val);
        return;
    case DBTYPES_INTERVALDS:
        ds = lua_topointer(lua, idx);
        *ret = interval_to_double(&ds->val);
        return;
    }
err:luabb_type_err(lua, dbtypes.real, idx);
    *ret = 0; //just to silence warnings -- will never reach here
}

void luabb_tointervalym(Lua lua, int idx, intv_t *ret)
{
    int rc = 0;
    const char *c;
    double d;
    long long i;
    int64_t tmp = 0, tmp2 = 0;
    int type = 0;
    ret->type = INTV_YM_TYPE;
    switch(luabb_dbtype(lua, idx)) {
    case DBTYPES_INTEGER:
        type = 2; // see str_to_interval()
        luabb_tointeger(lua, idx, &i);
        rc = int_to_interval(i, (uint64_t *)&tmp, (uint64_t *)&tmp2, &ret->sign);
        break;
    case LUA_TNUMBER:
    case DBTYPES_REAL:
        type = 2; // see str_to_interval()
        luabb_toreal(lua, idx, &d);
        rc = double_to_interval(d, (uint64_t *)&tmp, (uint64_t *)&tmp2, &ret->sign);
        break;
    case DBTYPES_INTERVALYM:
        *ret = ((lua_intervalym_t *)lua_topointer(lua, idx))->val;
        return;
    case LUA_TSTRING:
    case DBTYPES_CSTRING:
        type = INTV_YM_TYPE;
        c = luabb_tostring(lua, idx);
        rc = str_to_interval(c, strlen(c), &type, (uint64_t *)&tmp, (uint64_t *)&tmp2, &ret->u.ds,
          &ret->sign);
        if (type == 0 || type == 2) // 'years-months' or 'number'
             break;
        // else fall through - couldn't parse intervalym
    err:
    default:
        luabb_type_err(lua, dbtypes.intervalym, idx);
        break;
    }
    if (rc != 0)
        goto err;
    if (type == 0) {
        ret->u.ym.years = tmp;
        ret->u.ym.months = tmp2;
    } else if (type == 2) {
        ret->u.ym.years = 0;
        ret->u.ym.months = tmp;
    }
    _normalizeIntervalYM(&ret->u.ym);
}

void luabb_tointervalds(Lua lua, int idx, intv_t *ret)
{
    int rc = 0;
    const char *c;
    double d;
    long long i;
    int64_t tmp = 0, tmp2 = 0;
    int type;
    switch(luabb_dbtype(lua, idx)) {
    case DBTYPES_INTEGER:
        luabb_tointeger(lua, idx, &i);
        rc = int_to_interval(i, (uint64_t *)&tmp, (uint64_t *)&tmp2, &ret->sign);
        break;
    case LUA_TNUMBER:
    case DBTYPES_REAL:
        luabb_toreal(lua, idx, &d);
        rc = double_to_interval(d, (uint64_t *)&tmp, (uint64_t *)&tmp2, &ret->sign);
        break;
    case DBTYPES_INTERVALDS:
        *ret = ((lua_intervalds_t *)lua_topointer(lua, idx))->val;
        return;
    case LUA_TSTRING:
    case DBTYPES_CSTRING:
        type = INTV_DS_TYPE;
        c = luabb_tostring(lua, idx);
        rc = str_to_interval(c, strlen(c), &type, (uint64_t *)&tmp, (uint64_t *)&tmp2, &ret->u.ds,
          &ret->sign);
        if (type == 1) { // days hr:mn:sec.msec
            ret->type = ret->u.ds.prec == DTTZ_PREC_MSEC ? INTV_DS_TYPE : INTV_DSUS_TYPE;
            return;
        } else if (type == 2) { // number
            break;
        }
        // else fall through - couldn't parse intervalds
    err:
    default:
        luabb_type_err(lua, dbtypes.intervalym, idx);
        break;
    }
    if (rc != 0)
        goto err;
    if ((int)(tmp2 / 1E3) * 1000 == tmp2) {
        ret->type = INTV_DS_TYPE;
        _setIntervalDS(&ret->u.ds, tmp, tmp2 / 1000);
    } else {
        ret->type = INTV_DSUS_TYPE;
        _setIntervalDSUS(&ret->u.ds, tmp, tmp2);
    }
}

/*
** WARNING: luabb_tostring RETURNS AN EPHEMERAL STRING.
** DON'T CALL INTO ANY lua_* FUNCTIONS WITH IT.
** A GC-RUN CAN TRASH RETURNED MEMORY.
*/
const char *luabb_tostring(Lua L, int idx)
{
    idx = to_positive_index(L, idx);
    const char *ret = NULL;
    dbtypes_enum dbtype = luabb_dbtype(L, idx);
    if (dbtype > DBTYPES_MINTYPE && dbtype < DBTYPES_MAXTYPE) {
        if (dbtypes_tostring[dbtype]) {
            lua_pushvalue(L, idx);
            dbtypes_tostring[dbtype](L);
            ret = lua_tostring(L, -1);
            lua_pop(L, 2);
            return ret;
        }
    } else if (lua_isnumber(L, idx)) {
            lua_pushvalue(L, idx);
            ret = lua_tostring(L, -1);
            lua_pop(L, 1);
            return ret;
    } else if (lua_isstring(L, idx)) {
            lua_pushvalue(L, idx);
            ret = lua_tostring(L, -1);
            lua_pop(L, 1);
            return ret;
    }
    luabb_type_err(L, "string", idx);
    return NULL;
}

void luabb_toblob(Lua lua, int idx, blob_t *ret)
{
    const char *c;
    size_t s;
    int type = luabb_type(lua, idx);
    if (type == DBTYPES_LSTRING) {
        c = lua_tostring(lua, idx);
str:    s = strlen(c);
        ret->data = strdup(c);
        ret->length = s;
        return;
    } else if (type == DBTYPES_CSTRING) {
        c = ((lua_cstring_t*)lua_touserdata(lua, idx))->val;
        goto str;
    } else if (type == DBTYPES_BLOB) {
        const lua_blob_t *blob = lua_topointer(lua, idx);
        *ret = blob->val;
    } else {
        luabb_type_err(lua, dbtypes.blob, idx);
    }
}

static int get_int_value(lua_State *lua, int index, char *type)
{
    if (index < 0) index = lua_gettop(lua) + index + 1;
    lua_pushstring(lua, type);
    lua_gettable(lua, index);
    int i;
    if (lua_isnumber(lua, -1))  {
        i = lua_tointeger( lua, -1 );
        lua_pop(lua, 1);
        return i;
    } else if (lua_isboolean(lua, -1))  {
        i = lua_toboolean( lua, -1 );
        lua_pop(lua, 1);
        return i;
    }
    luaL_error(lua, "data is not a valid number.");
    return -1;
}

static const char * get_string_value(lua_State *lua, int index, char *type)
{
    lua_pushvalue(lua, index); 
    lua_pushstring(lua,type);
    lua_gettable(lua, -2);
    if (lua_isstring(lua, -1)) {
        const char* str = lua_tostring( lua, -1 );
        return str;
    } else {
        return NULL;
    }
}

static int l_datetime_change_timezone(lua_State *lua)
{
    lua_datetime_t *d;
    struct tm newtm;
    const char *newtimezone;
    server_datetime_t sdt;
    luaL_checkudata(lua, 1, dbtypes.datetime);
    d = (lua_datetime_t *) lua_topointer(lua, 1);
    newtimezone = lua_tostring(lua,2);
    sdt.sec = db_struct2time(d->val.tzname, &(d->val.tm));
    if( db_time2struct(newtimezone, &sdt.sec, &(newtm)) == 0) {
        strncpy(d->val.tzname, newtimezone, sizeof(d->val.tzname));
        d->val.tm = newtm;
        lua_pushinteger(lua, 0);
        return 1;
    }
    lua_pushinteger(lua, -1);
    return 1;
}

static int l_datetime_change_datetime_precision(lua_State *lua)
{
    lua_datetime_t *d;
    int newprecision;
    const char *z;
    luaL_checkudata(lua, 1, dbtypes.datetime);
    d = (lua_datetime_t *) lua_topointer(lua, 1);
    z = lua_tostring(lua, 2);
    DTTZ_TEXT_TO_PREC(z, newprecision, 0, goto err);
    d->val.frac = (d->val.frac * newprecision) / d->val.prec;
    d->val.prec = newprecision;
    if (0) {
err:
        lua_pushinteger(lua, -1);
    }
    lua_pushinteger(lua, 0);
    return 1;
}

void luabb_todatetime(lua_State *lua, int idx, datetime_t *ret)
{
    double d;
    int64_t i;
    dttz_t dt;
    time_t temp_t;
    const char *str;
    cdb2_client_datetime_t cdtms;
    cdb2_client_datetimeus_t cdtus;
    dbtypes_enum dbtype;

    SP sp = getsp(lua);
    const char *tzname = sp->clnt->tzname;
    int precision = sp->clnt->dtprec;

    switch (luabb_dbtype(lua, idx)) {
    case LUA_TSTRING:
        str = lua_tostring(lua, idx);
        if (str_to_dttz(str, strlen(str), tzname, &dt, precision) != 0)
            goto err;
        break;
    case LUA_TNUMBER:
        d = lua_tonumber(lua, idx);
        if(real_to_dttz(d, &dt, precision) != 0)
            goto err;
        break;
    case LUA_TTABLE:
        /* This is for lua date, which is of type LUA_TTABLE. */
        ret->tm.tm_year = get_int_value(lua,idx, "year") - 1900;
        ret->tm.tm_mon = get_int_value(lua,idx, "month") - 1;
        ret->tm.tm_mday = get_int_value(lua,idx, "day");
        ret->tm.tm_yday = get_int_value(lua,idx, "yday") - 1;
        ret->tm.tm_wday = get_int_value(lua,idx, "wday");
        ret->tm.tm_hour = get_int_value(lua,idx, "hour");
        ret->tm.tm_min = get_int_value(lua,idx, "min");
        ret->tm.tm_sec = get_int_value(lua,idx, "sec");
        ret->tm.tm_isdst = get_int_value(lua,idx, "isdst");
        temp_t = mktime(&ret->tm);
        ret->frac = 0;
        ret->prec = precision;
        tzname = get_string_value(lua,idx, "tz");
        if (tzname) {
            strcpy(ret->tzname, tzname);
        } else {
            strcpy(ret->tzname, "US/Eastern");
        }
        struct tm *t = localtime(&temp_t);
        ret->tm = *t; /* Correct it, if table doesn't have correct values. */
        return;
    case DBTYPES_DATETIME:
        *ret = ((lua_datetime_t *)lua_topointer(lua, idx))->val;
        return;
    case DBTYPES_CSTRING:
        str = ((lua_cstring_t*)lua_topointer(lua, idx))->val;
        if (str_to_dttz(str, strlen(str), tzname, &dt, precision) != 0)
            goto err;
        break;
    case DBTYPES_REAL:
        d = ((lua_real_t*)lua_topointer(lua, idx))->val;
        if (real_to_dttz(d, &dt, precision) != 0)
            goto err;
        break;
    case DBTYPES_INTEGER:
        i = ((lua_int_t*)lua_topointer(lua, idx))->val;
        if (int_to_dttz(i, &dt, precision) != 0)
            goto err;
        break;
    default:
        goto err;
    }
    if (dt.dttz_prec == DTTZ_PREC_USEC) {
        if (dttz_to_client_datetimeus(&dt, tzname, &cdtus) != 0)
            goto err;
        client_datetimeus_to_datetime_t(&cdtus, ret, 0);
    } else {
        if (dttz_to_client_datetime(&dt, tzname, &cdtms) != 0)
            goto err;
        client_datetime_to_datetime_t(&cdtms, ret, 0);
    }
    return;
err:luabb_type_err(lua, dbtypes.datetime, idx);
}

void luabb_todecimal(lua_State *lua, int idx, decQuad *val)
{
    const char *sval = NULL;
    char sbuf[LUAI_MAXNUMBER2STR];
    double rval;
    int ival;

    switch (luabb_dbtype(lua, idx)) {
    case LUA_TNUMBER:
    case LUA_TSTRING:
        sval = lua_tostring(lua, idx);
        break;
    case DBTYPES_CSTRING:
        sval = ((lua_cstring_t *)lua_topointer(lua, idx))->val;
        break;
    case DBTYPES_INTEGER:
        ival = ((lua_int_t *) lua_topointer(lua, idx))->val;
        sprintf(sbuf, "%d", ival);
        sval = sbuf;
        break;
    case DBTYPES_REAL:
        rval = ((lua_real_t *) lua_topointer(lua, idx))->val;
        lua_number2str(sbuf, rval);
        sval = sbuf;
        break;
    case DBTYPES_DECIMAL:
        *val = ((lua_dec_t *) lua_topointer(lua, idx))->val;
        return;
    default:
        luabb_type_err(lua, dbtypes.decimal, idx);
    }

    decContext ctx;
    dec_ctx_init(&ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);
    decQuadFromString(val, sval, &ctx);
    if (dfp_conv_check_status(&ctx,"string", "quad")) {
        luabb_type_err(lua, dbtypes.decimal, idx);
    }
}

static int l_add(Lua);
static int l_sub(Lua);
static int l_mul(Lua);
static int l_div(Lua);
static int l_mod(Lua);
void init_arithmetic(Lua lua, int mod)
{
    lua_pushcfunction(lua, l_add);
    lua_setfield(lua, -2, "__add");

    lua_pushcfunction(lua, l_sub);
    lua_setfield(lua, -2, "__sub");

    lua_pushcfunction(lua, l_mul);
    lua_setfield(lua, -2, "__mul");

    lua_pushcfunction(lua, l_div);
    lua_setfield(lua, -2, "__div");

    if (mod != LUA_OP_MOD) return;
    lua_pushcfunction(lua, l_mod);
    lua_setfield(lua, -2, "__mod");
}

static int l_eq(Lua);
static int l_lt(Lua);
static int l_le(Lua);
void init_cmp(Lua lua)
{
    lua_pushcfunction(lua, l_eq);
    lua_setfield(lua, -2, "__eq");

    lua_pushcfunction(lua, l_lt);
    lua_setfield(lua, -2, "__lt");

    lua_pushcfunction(lua, l_le);
    lua_setfield(lua, -2, "__le");
}

static int l_typed_assignment(Lua);
static int l_concat(Lua);
void init_common(Lua lua)
{
    lua_pushcfunction(lua, l_typed_assignment);
    lua_setfield(lua, -2, "__type");       
    lua_pushcfunction(lua, l_column_cast);
    lua_setfield(lua, -2, "__cast");    
    lua_pushcfunction(lua, l_concat);
    lua_setfield(lua, -2, "__concat");

}

static void l_decimal_cmp(lua_State *lua, int op) {
    decQuad dec1, dec2;

    luabb_todecimal(lua, 1, &dec1);
    luabb_todecimal(lua, 2, &dec2);

    decQuad result;
    decContext   ctx;

    dec_ctx_init(&ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);    

    decQuadCompare( &result, (decQuad*)&dec1, (decQuad*)&dec2, &ctx);
    if (((op ==LUA_OP_EQ) || (op == LUA_OP_LE)) && decQuadIsZero( &result) ) {
        lua_pushboolean(lua, 1);
    } else if (((op ==LUA_OP_LT) || (op == LUA_OP_LE)) && decQuadIsSigned( &result) ) {
        lua_pushboolean(lua, 1);
    } else {
        lua_pushboolean(lua, 0);
    }
}

static void l_real_cmp(Lua lua, int op)
{
    double val1, val2;
    luabb_toreal(lua, 1, &val1);
    luabb_toreal(lua, 2, &val2);
    switch (op) {
    case LUA_OP_EQ: lua_pushboolean(lua, val1 == val2); break;
    case LUA_OP_LT: lua_pushboolean(lua, val1 <  val2); break;
    case LUA_OP_LE: lua_pushboolean(lua, val1 <= val2); break;
    }
}

static void l_int_cmp(Lua lua, int op)
{
    long long val1, val2;
    luabb_tointeger(lua, 1, &val1);
    luabb_tointeger(lua, 2, &val2);
    switch (op) {
    case LUA_OP_EQ: lua_pushboolean(lua, val1 == val2); break;
    case LUA_OP_LT: lua_pushboolean(lua, val1 <  val2); break;
    case LUA_OP_LE: lua_pushboolean(lua, val1 <= val2); break;
    }
}

static int datetime_cmp(Lua);
static void l_datetime_cmp(Lua lua, int op)
{
    switch (op) {
    case LUA_OP_EQ: lua_pushboolean(lua, datetime_cmp(lua) == 0); break;
    case LUA_OP_LT: lua_pushboolean(lua, datetime_cmp(lua) < 0); break;
    case LUA_OP_LE: lua_pushboolean(lua, datetime_cmp(lua) <= 0); break;
    }
}

static void intv_cmp(Lua lua, intv_t *i1, intv_t *i2, int op)
{
    switch (op) {
    case LUA_OP_EQ: lua_pushboolean(lua, interval_cmp(i1, i2) == 0);
    case LUA_OP_LT: lua_pushboolean(lua, interval_cmp(i1, i2) < 0);
    case LUA_OP_LE: lua_pushboolean(lua, interval_cmp(i1, i2) <= 0);
    }
}

static void l_intervalds_cmp(Lua lua, int op)
{
    intv_t i1, i2;
    luabb_tointervalds(lua, 1, &i1);
    luabb_tointervalds(lua, 2, &i2);
    intv_cmp(lua, &i1, &i2, op);
}

static void l_intervalym_cmp(Lua lua, int op)
{
    intv_t i1, i2;
    luabb_tointervalym(lua, 1, &i1);
    luabb_tointervalym(lua, 2, &i2);
    intv_cmp(lua, &i1, &i2, op);
}

static rank_t getrank(Lua lua, int idx)
{
    switch (luabb_dbtype(lua, idx)) {
    case DBTYPES_LNIL: return RANK_NIL;
    case DBTYPES_INTEGER: return RANK_INT;
    case DBTYPES_REAL:
    case DBTYPES_LNUMBER: return RANK_REAL;
    case DBTYPES_DECIMAL: return RANK_DECIMAL;
    case DBTYPES_INTERVALDS: return RANK_INTERVALDS;
    case DBTYPES_INTERVALYM: return RANK_INTERVALYM;
    case DBTYPES_DATETIME: return RANK_DATETIME;
    default: return RANK_MAX;
    }
}

static int l_cmp(Lua lua, int op)
{
    nullchk(lua, 1);
    nullchk(lua, 2);

    rank_t rank1 = getrank(lua, 1);
    rank_t rank2 = getrank(lua, 2);

    rank_t min = rank1 < rank2 ? rank1 : rank2;
    rank_t max = rank1 > rank2 ? rank1 : rank2;

    if (min == 0) { // got one nil
        if (op == LUA_OP_EQ)
            lua_pushboolean(lua, 0); // nil compares false in equality
        else
            typecmperr(lua, 1, 2);
        return 0;
    }

    switch (max) {
    case RANK_INT: l_int_cmp(lua, op); break;
    case RANK_REAL: l_real_cmp(lua, op); break;
    case RANK_DECIMAL: l_decimal_cmp(lua, op); break;
    case RANK_INTERVALDS: l_intervalds_cmp(lua, op); break;
    case RANK_INTERVALYM: l_intervalym_cmp(lua, op); break;
    case RANK_DATETIME: l_datetime_cmp(lua, op); break;
    default:
        if (op == LUA_OP_EQ)
            lua_pushboolean(lua, 0);
        else
            typecmperr(lua, 1, 2);
    }
    return 0;
}

static int l_eq(Lua lua)
{
    l_cmp(lua, LUA_OP_EQ);
    return 1;
}

static int l_lt(Lua lua)
{
    l_cmp(lua, LUA_OP_LT);
    return 1;
}

static int l_le(Lua lua)
{
    l_cmp(lua, LUA_OP_LE);
    return 1;
}

static int l_decimal_add(lua_State *lua)
{
    lua_dec_t *in;
    decQuad arg;
    lua_dec_t *out;
    decContext    dfp_ctx;
    dec_ctx_init(&dfp_ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);
    luaL_checkudata(lua, 1, dbtypes.decimal);
    in =  (lua_dec_t*) lua_topointer(lua, 1);
    luabb_todecimal(lua, 2, &arg);
    new_lua_t(out, lua_dec_t, DBTYPES_DECIMAL);
    decQuadAdd(&(out->val), &(in->val), &arg, &dfp_ctx);
    if (dfp_conv_check_status( &dfp_ctx,"quad", "add(quad)")) {
       luaL_error(lua, "decimal conversion error");
    }
    return 1;
}

static int l_decimal_sub(lua_State *lua)
{
    lua_dec_t *in;
    decQuad arg;
    lua_dec_t *out;
    decContext    dfp_ctx;
    dec_ctx_init(&dfp_ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);
    luaL_checkudata(lua, 1, dbtypes.decimal);
    in =  (lua_dec_t*) lua_topointer(lua, 1);
    luabb_todecimal(lua, 2, &arg);
    new_lua_t(out, lua_dec_t, DBTYPES_DECIMAL);
    decQuadSubtract (&(out->val), &(in->val), &arg, &dfp_ctx);
    if (dfp_conv_check_status( &dfp_ctx,"quad", "subtract(quad)")) {
       luaL_error(lua, "decimal conversion error");
    }
    return 1;
}

static int l_decimal_mul(lua_State *lua)
{
    lua_dec_t *in;
    decQuad arg;
    lua_dec_t *out;
    decContext    dfp_ctx;
    dec_ctx_init(&dfp_ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);

    luaL_checkudata(lua, 1, dbtypes.decimal);
    in =  (lua_dec_t*) lua_topointer(lua, 1);
    luabb_todecimal(lua, 2, &arg);
    new_lua_t(out, lua_dec_t, DBTYPES_DECIMAL);
    decQuadMultiply(&(out->val), &(in->val), &arg, &dfp_ctx);
    if (dfp_conv_check_status( &dfp_ctx,"quad", "multiply(quad)")) {
       luaL_error(lua, "decimal conversion error");
    }
    return 1;
}

static int l_decimal_div(lua_State *lua)
{
    lua_dec_t *in;
    decQuad arg;
    lua_dec_t *out;
    decContext    dfp_ctx;
    dec_ctx_init(&dfp_ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);

    luaL_checkudata(lua, 1, dbtypes.decimal);
    in =  (lua_dec_t*) lua_topointer(lua, 1);
    luabb_todecimal(lua, 2, &arg);
    new_lua_t(out, lua_dec_t, DBTYPES_DECIMAL);
    decQuadDivide(&(out->val), &(in->val), &arg, &dfp_ctx);
    if (dfp_conv_check_status( &dfp_ctx,"quad", "divide(quad)")) {
       luaL_error(lua, "decimal conversion error");
    }
    return 1;
}

static int l_int_new(Lua);
static int l_int_arithmetic(Lua lua, operation_t op)
{
    long long v1, v2;
    luabb_tointeger(lua, 1, &v1);
    luabb_tointeger(lua, 2, &v2);
    l_int_new(lua);
    lua_int_t *v = (lua_int_t *)lua_topointer(lua, -1);
    switch (op) {
    case LUA_OP_ADD: v->val = v1 + v2; break;
    case LUA_OP_SUB: v->val = v1 - v2; break;
    case LUA_OP_MUL: v->val = v1 * v2; break;
    case LUA_OP_DIV: v->val = v1 / v2; break;
    case LUA_OP_MOD: v->val = v1 % v2; break;
    }
    return 1;
}

static int l_real_new(Lua);
static int l_real_arithmetic(Lua lua, operation_t op)
{
    double v1, v2;
    luabb_toreal(lua, 1, &v1);
    luabb_toreal(lua, 2, &v2);
    l_real_new(lua);
    lua_real_t *v = (lua_real_t *)lua_topointer(lua, -1);
    switch (op) {
    case LUA_OP_ADD: v->val = v1 + v2; break;
    case LUA_OP_SUB: v->val = v1 - v2; break;
    case LUA_OP_MUL: v->val = v1 * v2; break;
    case LUA_OP_DIV: v->val = v1 / v2; break;
    case LUA_OP_MOD: v->val = fmod(v1, v2); break;
    }
    return 1;
}

static int l_decimal_arithmetic(Lua lua, operation_t op)
{
    switch (op) {
    case LUA_OP_ADD: return l_decimal_add(lua);
    case LUA_OP_SUB: return l_decimal_sub(lua);
    case LUA_OP_MUL: return l_decimal_mul(lua);
    case LUA_OP_DIV: return l_decimal_div(lua);
    default: return luaL_error(lua, "invalid decimal operation");
    }
}

/*
** Return non-zero if calling routine needs to convert
** value on top of stack to intervalym/ds
*/
static int l_interval_arithmetic(Lua lua, operation_t op)
{
    rank_t r1 = getrank(lua, 1);
    rank_t r2 = getrank(lua, 2);
    rank_t min = r1 < r2 ? r1 : r2;
    if (r1 == r2)
        l_real_arithmetic(lua, op);
    else if (min == RANK_INT)
        l_int_arithmetic(lua, op);
    else if (min == RANK_REAL)
        l_real_arithmetic(lua, op);
    else if (min == RANK_DECIMAL)
        l_decimal_arithmetic(lua, op);
    return (r1 == r2 || op == LUA_OP_MUL || op == LUA_OP_DIV);
}

/*
** interval +/- interval = interval
** interval mul/div number = interval
** interval +/- number = number 
*/
int l_intervalds_new(Lua);
static int l_intervalds_arithmetic(Lua lua, operation_t op)
{
    if (l_interval_arithmetic(lua, op)) {
        l_intervalds_new(lua);
        lua_intervalds_t *ds = (lua_intervalds_t *)lua_topointer(lua, -1);
        luabb_tointervalds(lua, lua_gettop(lua) - 1, &ds->val);
    }
    return 1;
}

int l_intervalym_new(Lua);
static int l_intervalym_arithmetic(Lua lua, operation_t op)
{
    if (l_interval_arithmetic(lua, op)) {
        l_intervalym_new(lua);
        lua_intervalym_t *ym = (lua_intervalym_t *)lua_topointer(lua, -1);
        int top = lua_gettop(lua);
        luabb_tointervalym(lua, top - 1, &ym->val);
    }
    return 1;
}

static void datetime_normalize(lua_datetime_t *dt)
{
    server_datetime_t sdt;
    struct tm newtm;
    sdt.sec = db_struct2time(dt->val.tzname, &(dt->val.tm));
    db_time2struct(dt->val.tzname, &sdt.sec, &(newtm));
    dt->val.tm = newtm;
}

int l_datetime_new(Lua);
static int datetime_intvds(Lua lua, const lua_datetime_t *ldt,
  const lua_intervalds_t *ds, operation_t op)
{
    l_datetime_new(lua);
    lua_datetime_t *out = (lua_datetime_t *)lua_topointer(lua, -1);
    dttz_t dt, rs;
    datetime_t_to_dttz(&ldt->val, &dt);
    if (op == LUA_OP_ADD)
        add_dttz_intvds(&dt, &ds->val, &rs);
    else if (op == LUA_OP_SUB)
        sub_dttz_intvds(&dt, &ds->val, &rs);
    dttz_to_datetime_t(&rs, ldt->val.tzname, &out->val);
    return 1;
}

static int add_datetime_intvym(Lua lua, const lua_datetime_t *dt, const lua_intervalym_t *ym)
{
    const intv_ym_t *i = &ym->val.u.ym;
    lua_datetime_t *r;
    l_datetime_new(lua);
    r = (lua_datetime_t *)lua_topointer(lua, -1);
    *r = *dt;
    if (ym->val.sign == 1) {
        r->val.tm.tm_year = dt->val.tm.tm_year + i->years;
        r->val.tm.tm_mon = dt->val.tm.tm_mon + i->months;
    } else if (ym->val.sign == -1) {
        r->val.tm.tm_year =  dt->val.tm.tm_year - i->years;
        r->val.tm.tm_mon = dt->val.tm.tm_mon - i->months;
    } else {
        logmsg(LOGMSG_FATAL, "missing sign\n");
        abort();
    }
    datetime_normalize(r);
    return 1;
}

// datetime + datetime => error
// datetime + interval => datetime
// interval + datetime => datetime
// datetime + number/real/int => real
// number/real/int + datetime => real
static int l_datetime_add(Lua lua, int date, int other)
{
    const lua_intervalds_t *ds;
    const lua_intervalym_t *ym;
    const lua_datetime_t *dt = lua_topointer(lua, date);
    rank_t r = getrank(lua, other);
    switch (r) {
    case RANK_INT:
    case RANK_REAL:
        return l_real_arithmetic(lua, LUA_OP_ADD);
    case RANK_INTERVALDS:
        ds = lua_topointer(lua, other);
        return datetime_intvds(lua, dt, ds, LUA_OP_ADD);
    case RANK_INTERVALYM:
        ym = lua_topointer(lua, other);
        return add_datetime_intvym(lua, dt, ym);
    }
    return luaL_error(lua, "invalid datetime operation");
}

// datetime - datetime => interval
// datetime - interval => datetime
// interval - datetime => error
// datetime - number/real/int => real
// number/real/int - datetime => real
static int l_datetime_sub(Lua lua, int date, int other)
{
    lua_intervalds_t *ds;
    lua_intervalym_t ym;
    const lua_datetime_t *d1, *d2;
    dttz_t a, b;
    d1 = lua_topointer(lua, date);
    rank_t r = getrank(lua, other);
    switch (r) {
    case RANK_INT:
    case RANK_REAL:
        return l_real_arithmetic(lua, LUA_OP_SUB);
    case RANK_INTERVALDS:
        if (other == 1) break;
        ds = (lua_intervalds_t*)lua_topointer(lua, other);
        return datetime_intvds(lua, d1, ds, LUA_OP_SUB);
    case RANK_INTERVALYM:
        if (other == 1) break;
        ym = *(lua_intervalym_t*)lua_topointer(lua, other);
        ym.val.sign *= -1;
        return add_datetime_intvym(lua, d1, &ym);
    case RANK_DATETIME:
        d2 = lua_topointer(lua, 2);
        datetime_t_to_dttz(&d1->val, &a);
        datetime_t_to_dttz(&d2->val, &b);
        l_intervalds_new(lua);
        ds = (lua_intervalds_t *)lua_topointer(lua, -1);
        sub_dttz_dttz(&a, &b, &ds->val);
        return 1;
    }
    return luaL_error(lua, "invalid datetime operation");
}

static int l_datetime_arithmetic(Lua lua, operation_t op)
{
    int date, other;
    if (luabb_dbtype(lua, 1) == DBTYPES_DATETIME) {
        date = 1;
        other = 2;
    } else {
        date = 2;
        other = 1;
    }
    switch (op) {
    case LUA_OP_ADD: return l_datetime_add(lua, date, other);
    case LUA_OP_SUB: return l_datetime_sub(lua, date, other);
    case LUA_OP_MUL:
    case LUA_OP_DIV: return l_real_arithmetic(lua, op);
    default: return luaL_error(lua, "invalid datetime operation");
    }
}

static int l_arithmetic(Lua lua, operation_t op)
{
    nullchk(lua, 1);
    nullchk(lua, 2);
    rank_t rank1 = getrank(lua, 1);
    rank_t rank2 = getrank(lua, 2);
    rank_t max = rank1 > rank2 ? rank1 : rank2;
    switch (max) {
    case RANK_INT: return l_int_arithmetic(lua, op);
    case RANK_REAL: return l_real_arithmetic(lua, op);
    case RANK_DECIMAL: return l_decimal_arithmetic(lua, op);
    case RANK_INTERVALDS: return l_intervalds_arithmetic(lua, op);
    case RANK_INTERVALYM: return l_intervalym_arithmetic(lua, op);
    case RANK_DATETIME: return l_datetime_arithmetic(lua, op);
    default: return luaL_error(lua, "bad arithmetic operation");
    }
}

static int l_add(Lua lua)
{
    return l_arithmetic(lua, LUA_OP_ADD);
}

static int l_sub(Lua lua)
{
    return l_arithmetic(lua, LUA_OP_SUB);
}

static int l_mul(Lua lua)
{
    return l_arithmetic(lua, LUA_OP_MUL);
}

static int l_div(Lua lua)
{
    return l_arithmetic(lua, LUA_OP_DIV);
}

static int l_mod(Lua lua)
{
    return l_arithmetic(lua, LUA_OP_MOD);
}

int l_cstring_new(lua_State *lua) {
    lua_cstring_t *s;
    new_lua_t(s, lua_cstring_t, DBTYPES_CSTRING);
    s->val = NULL;
    return 1;
}

int l_cstring_tostring(lua_State *lua) {
    luaL_checkudata(lua, -1, dbtypes.cstring);
    const lua_cstring_t* s = lua_topointer(lua, -1);
    if (s->is_null)
      lua_pushstring(lua, null_str);
    else   
      lua_pushstring(lua, s->val);
    return 1;
}

int l_cstring_free(Lua lua)
{
    lua_cstring_t *s = lua_touserdata(lua, 1);
    free(s->val);
    return 0;
}

int l_cstring_length(lua_State *lua) {
    const lua_cstring_t *s;
    s = (const lua_cstring_t*) lua_topointer(lua, 1);
    lua_pushnumber(lua, utf8_bytelen(s->val, strlen(s->val)));
    return 1;
}

static void getstr(Lua lua, const char **s, int i)
{
    *s = NULL;
    dbtypes_enum t = luabb_dbtype(lua, i);
    if (t == DBTYPES_CSTRING)
        *s = (((const lua_cstring_t *)lua_topointer(lua, i))->val);
    else if (lua_isstring(lua, i))
        *s = lua_tostring(lua, i);
}

static void getstrs(Lua lua, const char **s1, const char **s2)
{
    getstr(lua, s1, 1);
    getstr(lua, s2, 2);
}

#undef LIBERAL_TYPES /* they are no good */

static int l_cstring_eq(Lua lua)
{
    if (lua_isnil(lua, 1) || lua_isnil(lua, 2)) {
        lua_pushboolean(lua, 0);
        return 1;
    }
    const char *s1, *s2;
    getstrs(lua, &s1, &s2);
    if (s1 && s2) {
        lua_pushboolean(lua, strcmp(s1, s2) == 0);
        return 1;
#ifdef LIBERAL_TYPES
    } else if (s1 || s2) {
        l_cmp(lua, LUA_OP_EQ);
        return 1;
#endif
    } else {
        lua_pushboolean(lua, 0);
        return 1;
    }
}

static int l_cstring_lt(lua_State *lua)
{
    const char *s1, *s2;
    getstrs(lua, &s1, &s2);
    if (s1 && s2) {
        lua_pushboolean(lua, strcmp(s1, s2) < 0);
        return 1;
#ifdef LIBERAL_TYPES
    } else if (s1 || s2) {
        l_cmp(lua, LUA_OP_LT);
        return 1;
#endif
    } else {
        return typecmperr(lua, 1, 2);
    }
}

static int l_cstring_le(lua_State *lua)
{
    const char *s1, *s2;
    getstrs(lua, &s1, &s2);
    if (s1 && s2) {
        lua_pushboolean(lua, strcmp(s1, s2) <= 0);
        return 1;
#ifdef LIBERAL_TYPES
    } else if (s1 || s2) {
        l_cmp(lua, LUA_OP_LE);
        return 1;
#endif
    } else {
        return typecmperr(lua, 1, 2);
    }
}

static const struct luaL_Reg cstring_funcs[] = {
    { "__tostring", l_cstring_tostring },
    { "__len", l_cstring_length },
    { "__gc", l_cstring_free },
    { "__eq", l_cstring_eq },
    { "__lt", l_cstring_lt },
    { "__le", l_cstring_le },
    { "len", l_cstring_length },
    { NULL, NULL }
};

static void init_cstring(Lua L)
{
    luaL_newmetatable(L, dbtypes.cstring);
    lua_pushstring(L, "__index");
    lua_pushvalue(L, -2);
    lua_settable(L, -3);

    luaL_openlib(L, NULL, cstring_funcs, 0);
    init_common(L);

    lua_pop(L, 1);
}

int l_blob_new(lua_State *lua) {
    lua_blob_t *d;
    new_lua_t(d, lua_blob_t, DBTYPES_BLOB);
    d->val.length = 0;
    d->val.data = NULL;
    return 1;
}

int l_datetime_new(Lua lua) {
    lua_datetime_t *d;
    new_lua_t(d, lua_datetime_t, DBTYPES_DATETIME);
    return 1;
}

int l_intervalym_new(Lua lua)
{
    lua_intervalym_t *d;
    new_lua_t(d, lua_intervalym_t, DBTYPES_INTERVALYM);
    d->val.type = INTV_YM_TYPE;
    d->val.sign = 1;
    return 1;
}

int l_intervalds_new(Lua lua)
{
    lua_intervalds_t *d;
    new_lua_t(d, lua_intervalds_t, DBTYPES_INTERVALDS);
    d->val.type = INTV_DS_TYPE;
    d->val.sign = 1;
    return 1;
}

static int l_datetime_tostring_int(lua_State *lua, int idx)
{
    luaL_checkudata(lua, idx, dbtypes.datetime);
    const lua_datetime_t *dt = lua_topointer(lua, idx);
    if (dt->is_null) { 
      lua_pushstring(lua, null_str);
      return 1;
    }

    int sz;
    dttz_t dz;
    char buf[64];

    datetime_t_to_dttz(&dt->val, &dz);
    dttz_to_str(&dz, buf, sizeof(buf), &sz, dt->val.tzname);
    lua_pushstring(lua, buf);
    return 1;
}

static int l_datetime_tostring(Lua L)
{
    return l_datetime_tostring_int(L, -1);
}

static int l_interval_tostring(Lua L)
{
    if (luabb_isnull(L, -1)) {
        lua_pushstring(L, null_str);
        return 1;
    }
    const lua_dbtypes_t *n = lua_topointer(L, -1);
    if (n->dbtype == DBTYPES_INTERVALYM || n->dbtype == DBTYPES_INTERVALDS) {
        /* lua-ym and lua-ds have same layout (intv_t) */
        const lua_intervalym_t *d = (const lua_intervalym_t *)n;
        char tmp[256];
        int n;
        if (intv_to_str(&d->val, tmp, sizeof(tmp), &n) == 0) {
            lua_pushlstring(L, tmp, n);
            return 1;
        }
    }
    return 0;
}

static int l_datetime_year(Lua L)
{
        lua_datetime_t *d = lua_touserdata(L, 1);
        lua_pushinteger(L, d->val.tm.tm_year + 1900);
        return 1;
}

static int l_datetime_month(Lua L)
{
        lua_datetime_t *d = lua_touserdata(L, 1);
        lua_pushinteger(L, d->val.tm.tm_mon + 1);
        return 1;
}

static int l_datetime_mday(Lua L)
{
        lua_datetime_t *d = lua_touserdata(L, 1);
        lua_pushinteger(L, d->val.tm.tm_mday);
        return 1;
}

static int l_datetime_yday(Lua L)
{
        lua_datetime_t *d = lua_touserdata(L, 1);
        lua_pushinteger(L, d->val.tm.tm_yday + 1);
        return 1;
}

static int l_datetime_wday(Lua L)
{
        lua_datetime_t *d = lua_touserdata(L, 1);
        lua_pushinteger(L, d->val.tm.tm_wday + 1);
        return 1;
}

static int l_datetime_hour(Lua L)
{
        lua_datetime_t *d = lua_touserdata(L, 1);
        lua_pushinteger(L, d->val.tm.tm_hour);
        return 1;
}

static int l_datetime_min(Lua L)
{
        lua_datetime_t *d = lua_touserdata(L, 1);
        lua_pushinteger(L, d->val.tm.tm_min);
        return 1;
}

static int l_datetime_sec(Lua L)
{
        lua_datetime_t *d = lua_touserdata(L, 1);
        lua_pushinteger(L, d->val.tm.tm_sec);
        return 1;
}

static int l_datetime_msec(Lua L)
{
        lua_datetime_t *d = lua_touserdata(L, 1);
        lua_pushinteger(L, (d->val.prec == DTTZ_PREC_MSEC) ?
                            d->val.frac :
                            d->val.frac / 1000);
        return 1;
}

static int l_datetime_usec(Lua L)
{
        lua_datetime_t *d = lua_touserdata(L, 1);
        lua_pushinteger(L, (d->val.prec == DTTZ_PREC_MSEC) ?
                            d->val.frac * 1000:
                            d->val.frac);
        return 1;
}

static int l_datetime_isdst(Lua L)
{
        lua_datetime_t *d = lua_touserdata(L, 1);
        lua_pushboolean(L, d->val.tm.tm_isdst);
        return 1;
}

static int l_datetime_timezone(Lua L)
{
        lua_datetime_t *d = lua_touserdata(L, 1);
        lua_pushstring(L, d->val.tzname);
        return 1;
}

static int l_datetime_to_table(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.datetime);
    lua_newtable(L);
    l_datetime_year(L);
    lua_setfield(L, -2, "year");
    l_datetime_month(L);
    lua_setfield(L, -2, "month");
    l_datetime_mday(L);
    lua_setfield(L, -2, "day");
    l_datetime_yday(L);
    lua_setfield(L, -2, "yday");
    l_datetime_wday(L);
    lua_setfield(L, -2, "wday");
    l_datetime_hour(L);
    lua_setfield(L, -2, "hour");
    l_datetime_min(L);
    lua_setfield(L, -2, "min");
    l_datetime_sec(L);
    lua_setfield(L, -2, "sec");
    l_datetime_isdst(L);
    lua_setfield(L, -2, "isdst");
    return 1;
}

static int datetime_cmp(Lua lua)
{
    nullchk(lua, 1);
    nullchk(lua, 2);
    dttz_t d1, d2;
    datetime_t a1, a2;
    luabb_todatetime(lua, 1, &a1);
    luabb_todatetime(lua, 2, &a2);
    datetime_t_to_dttz(&a1, &d1);
    datetime_t_to_dttz(&a2, &d2);
    return dttz_cmp(&d1, &d2);
}

static const struct luaL_Reg datetime_funcs[] = {
    { "change_timezone", l_datetime_change_timezone },
    { "change_datetime_precision", l_datetime_change_datetime_precision },
    { "to_table",  l_datetime_to_table },
    { NULL, NULL }
};

static int l_datetime_helper(Lua L)
{
    luaL_getmetatable(L, "datetime_helper");
    lua_insert(L, -2);
    lua_gettable(L, -2);
    return 1;
}

static int l_datetime_index(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.datetime);
    luaL_checkstring(L, 2);
    const char *property = lua_tostring(L, 2);
    if (strcmp(property, "year") == 0)
        return l_datetime_year(L);
    else if (strcmp(property, "month") == 0)
        return l_datetime_month(L);
    else if (strcmp(property, "day") == 0)
        return l_datetime_mday(L);
    else if (strcmp(property, "yday") == 0)
        return l_datetime_yday(L);
    else if (strcmp(property, "wday") == 0)
        return l_datetime_wday(L);
    else if (strcmp(property, "hour") == 0)
        return l_datetime_hour(L);
    else if (strcmp(property, "min") == 0)
        return l_datetime_min(L);
    else if (strcmp(property, "sec") == 0)
        return l_datetime_sec(L);
    else if (strcmp(property, "msec") == 0)
        return l_datetime_msec(L);
    else if (strcmp(property, "usec") == 0)
        return l_datetime_usec(L);
    else if (strcmp(property, "isdst") == 0)
        return l_datetime_isdst(L);
    else if (strcmp(property, "timezone") == 0)
        return l_datetime_timezone(L);
    else
        return l_datetime_helper(L);
}

#define setup_method(L, name, func) \
    lua_pushcfunction(L, func);     \
    lua_setfield(L, -2, name)

static void init_datetime(Lua L)
{
    luaL_newmetatable(L, "datetime_helper");
    lua_pushstring(L, "__index");
    lua_pushvalue(L, -2);
    lua_settable(L, 3);
    luaL_openlib(L, NULL, datetime_funcs, 0);
    lua_pop(L, 1);

    luaL_newmetatable(L, dbtypes.datetime);
    setup_method(L, "__index", l_datetime_index);
    setup_method(L, "__tostring", l_datetime_tostring);
    init_arithmetic(L, 0);
    init_cmp(L);
    init_common(L);
    lua_pop(L, 1);
}

static int l_intervalym_years(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.intervalym);
    lua_intervalym_t *i = lua_touserdata(L, 1);
    lua_pushinteger(L, i->val.u.ym.years);
    return 1;
}

static int l_intervalym_months(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.intervalym);
    lua_intervalym_t *i = lua_touserdata(L, 1);
    lua_pushinteger(L, i->val.u.ym.months);
    return 1;
}

static const struct luaL_Reg intervalym_funcs[] = {
    { "__tostring", l_interval_tostring },
    { "years", l_intervalym_years },
    { "months", l_intervalym_months },
    { NULL, NULL}
};

static void init_intervalym(Lua L)
{
    luaL_newmetatable(L, dbtypes.intervalym);
    lua_pushstring(L, "__index");
    lua_pushvalue(L, -2);
    lua_settable(L, -3);
    luaL_openlib(L, NULL, intervalym_funcs, 0);
    init_arithmetic(L, 0);
    init_cmp(L);
    init_common(L);
    lua_pop(L, 1);
}

static int l_intervalds_days(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.intervalds);
    lua_intervalds_t *i = lua_touserdata(L, 1);
    lua_pushinteger(L, i->val.u.ds.days);
    return 1;
}

static int l_intervalds_hours(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.intervalds);
    lua_intervalds_t *i = lua_touserdata(L, 1);
    lua_pushinteger(L, i->val.u.ds.hours);
    return 1;
}

static int l_intervalds_mins(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.intervalds);
    lua_intervalds_t *i = lua_touserdata(L, 1);
    lua_pushinteger(L, i->val.u.ds.mins);
    return 1;
}

static int l_intervalds_secs(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.intervalds);
    lua_intervalds_t *i = lua_touserdata(L, 1);
    lua_pushinteger(L, i->val.u.ds.sec);
    return 1;
}

static int l_intervalds_msecs(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.intervalds);
    lua_intervalds_t *i = lua_touserdata(L, 1);
    lua_pushinteger(L, (i->val.u.ds.prec == DTTZ_PREC_MSEC) ?
                       i->val.u.ds.frac :
                       i->val.u.ds.frac / 1000);
    return 1;
}

static int l_intervalds_usecs(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.intervalds);
    lua_intervalds_t *i = lua_touserdata(L, 1);
    lua_pushinteger(L, (i->val.u.ds.prec == DTTZ_PREC_MSEC) ?
                       i->val.u.ds.frac * 1000:
                       i->val.u.ds.frac);
    return 1;
}

static const struct luaL_Reg intervalds_funcs[] = {
    { "__tostring", l_interval_tostring },
    { "days", l_intervalds_days},
    { "hours", l_intervalds_hours },
    { "mins", l_intervalds_mins },
    { "secs", l_intervalds_secs },
    { "msecs", l_intervalds_msecs },
    { "usecs", l_intervalds_usecs },
    { NULL, NULL}
};

static void init_intervalds(Lua L)
{
    luaL_newmetatable(L, dbtypes.intervalds);
    lua_pushstring(L, "__index");
    lua_pushvalue(L, -2);
    lua_settable(L, -3);
    luaL_openlib(L, NULL, intervalds_funcs, 0);
    init_arithmetic(L, 0);
    init_cmp(L);
    init_common(L);
    lua_pop(L, 1);
}

static int clone_blob(Lua lua)
{
    luaL_checkudata(lua, 1, dbtypes.blob);
    lua_blob_t *blob = lua_touserdata(lua, 1);
    lua_blob_t *clone;
    new_lua_t(clone, lua_blob_t, DBTYPES_BLOB);
    *clone = *blob;
    clone->val.data = malloc(blob->val.length);
    memcpy(clone->val.data, blob->val.data, blob->val.length);
    return 1;
}

static int l_blob_index(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.blob);
    lua_blob_t *b = lua_touserdata(L, 1);

    long long i = 0;
    if (lua_isnumber(L, 2)) {
        i = lua_tointeger(L, 2);
    } else if (luabb_istype(L, 2, DBTYPES_INTEGER)) {
        luabb_tointeger(L, 2, &i);
    } else if (lua_isstring(L, 2) && strcmp(lua_tostring(L, 2), "clone") == 0) {
        lua_pushcfunction(L, clone_blob);
        return 1;
    } else {
        luaL_error(L, "bad blob index");
    }
    --i;
    if (i < 0 || i >= b->val.length) luaL_error(L, "blob index out of range");

    blob_t byte = {.length = 1, .data = ((uint8_t *)b->val.data) + i};
    luabb_pushblob(L, &byte);
    return 1;
}

static int l_blob_newindex(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.blob);
    luaL_checkudata(L, 3, dbtypes.blob);
    lua_blob_t *b1 = lua_touserdata(L, 1);
    lua_blob_t *b3 = lua_touserdata(L, 3);

    long long i = 0;
    if (lua_isnumber(L, 2))
        i = lua_tointeger(L, 2);
    else if (luabb_istype(L, 2, DBTYPES_INTEGER))
        luabb_tointeger(L, 2, &i);
    else
        luaL_error(L, "bad blob index");
    --i;
    if (i < 0 || i >= b1->val.length) luaL_error(L, "blob index out of range");

    luaL_argcheck(L, b3->val.length == 1, 3, "assigning more than one byte");
    *((uint8_t *)b1->val.data + i) = *(uint8_t *)b3->val.data;
    return 0;
}

static int l_blob_tostring_int(Lua lua, int idx)
{
    luaL_checkudata(lua, idx, dbtypes.blob);
    const lua_blob_t *blob = lua_topointer(lua, idx);

    if (blob->is_null)
        lua_pushstring(lua, null_str);
    else {
      int len = blob->val.length * 2;
      char *hexified = malloc(len + 1);
      util_tohex(hexified, blob->val.data, blob->val.length);
      lua_pushlstring(lua, hexified, len);
      free(hexified);
    }
    return 1;
}

static int l_blob_tostring(Lua lua)
{
    return l_blob_tostring_int(lua, -1);
}

static int l_blob_eq(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.blob);
    luaL_checkudata(L, 2, dbtypes.blob);

    nullchk(L, 1);
    nullchk(L, 2);

    blob_t *b1 = &((lua_blob_t *)lua_touserdata(L, 1))->val;
    blob_t *b2 = &((lua_blob_t *)lua_touserdata(L, 2))->val;

    if (b1->length != b2->length) {
        lua_pushboolean(L, 0);
    } else {
        lua_pushboolean(L, memcmp(b1->data, b2->data, b1->length) == 0 ? 1 : 0);
    }
    return 1;
}

static int l_blob_lt(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.blob);
    luaL_checkudata(L, 2, dbtypes.blob);

    nullchk(L, 1);
    nullchk(L, 2);

    blob_t *b1 = &((lua_blob_t *)lua_touserdata(L, 1))->val;
    blob_t *b2 = &((lua_blob_t *)lua_touserdata(L, 2))->val;

    size_t n = b1->length < b2->length ? b1->length : b2->length;
    int c = memcmp(b1->data, b2->data, n);
    if (c < 0) {
        lua_pushboolean(L, 1);
    } else if (c > 0) {
        lua_pushboolean(L, 0);
    } else {
        lua_pushboolean(L, b1->length < b2->length);
    }
    return 1;
}

static int l_blob_le(Lua L)
{
    luaL_checkudata(L, 1, dbtypes.blob);
    luaL_checkudata(L, 2, dbtypes.blob);

    nullchk(L, 1);
    nullchk(L, 2);

    blob_t *b1 = &((lua_blob_t *)lua_touserdata(L, 1))->val;
    blob_t *b2 = &((lua_blob_t *)lua_touserdata(L, 2))->val;

    size_t n = b1->length < b2->length ? b1->length : b2->length;
    int c = memcmp(b1->data, b2->data, n);
    if (c < 0) {
        lua_pushboolean(L, 1);
    } else if (c > 0) {
        lua_pushboolean(L, 0);
    } else {
        lua_pushboolean(L, b1->length <= b2->length);
    }
    return 1;
}

static int l_blob_free(Lua lua)
{
    const lua_blob_t *b = lua_topointer(lua, 1);
    free(b->val.data);
    return 0;
}

static int l_blob_length(Lua lua)
{
    const lua_blob_t *blob;

    luaL_checkudata(lua, 1, dbtypes.blob);
    blob = lua_topointer(lua, 1);

    lua_pushnumber(lua, blob->val.length);

    return 1;
}

static void init_blob(Lua L)
{
    luaL_newmetatable(L, dbtypes.blob);
    setup_method(L, "__index", l_blob_index);
    setup_method(L, "__newindex", l_blob_newindex);
    setup_method(L, "__tostring", l_blob_tostring);
    setup_method(L, "__eq", l_blob_eq);
    setup_method(L, "__lt", l_blob_lt);
    setup_method(L, "__le", l_blob_le);
    setup_method(L, "__cast", l_column_cast);
    setup_method(L, "__type", l_typed_assignment);
    setup_method(L, "__gc", l_blob_free);
    setup_method(L, "__len", l_blob_length);

    lua_pop(L, 1);
}

static int l_errfunc(lua_State *lua) {
    logmsg(LOGMSG_DEBUG, "%s\n", lua_tostring(lua, -1));
    lua_getglobal(lua, "debug");
    lua_getfield(lua, -1, "traceback");
    lua_pcall(lua, 0, 1, 0);
    logmsg(LOGMSG_DEBUG, "%s\n", lua_tostring(lua, -1));
    return 1;
}

/* Convenience routines */
void luabb_pushinteger(lua_State *lua, long long ival) {
    lua_int_t *i;
    l_int_new(lua);
    i = (lua_int_t*) lua_topointer(lua, -1);
    i->val = ival;
}

static int l_decimal_new(Lua);
void luabb_pushdecimal(lua_State *lua, const decQuad *val)
{
    l_decimal_new(lua);
    lua_dec_t *i = (lua_dec_t *) lua_topointer(lua, -1);
    i->val = *val;
}

void luabb_pushcstring(lua_State *lua, const char* cstrval) {
    l_cstring_new(lua);
    lua_cstring_t *s = (lua_cstring_t *) lua_topointer(lua, -1);
    s->val = strdup(cstrval);
}

void luabb_pushcstringlen(lua_State *lua, const char* cstrval, int len)
{
    l_cstring_new(lua);
    lua_cstring_t *s = (lua_cstring_t *) lua_topointer(lua, -1);
    s->val = malloc(len + 1);
    memcpy(s->val, cstrval, len);
    s->val[len] = '\0';
}

// dl -> str'dup'-less
void luabb_pushcstring_dl(lua_State *lua, const char* cstrval)
{
    l_cstring_new(lua);
    lua_cstring_t *s = (lua_cstring_t *) lua_topointer(lua, -1);
    s->val = (char *)cstrval;
}

void luabb_pushblob(lua_State *lua, const blob_t *val)
{
    lua_blob_t *i;
    l_blob_new(lua);
    i = (lua_blob_t*) lua_topointer(lua, -1);
    i->val.length = val->length;
    i->val.data = malloc(val->length);
    memcpy(i->val.data, val->data, val->length);
}

// dl -> str'dup'-less
void luabb_pushblob_dl(lua_State *lua, const blob_t *val)
{
    lua_blob_t *i;
    l_blob_new(lua);
    i = (lua_blob_t*) lua_topointer(lua, -1);
    i->val = *val;
}

void luabb_pushreal(lua_State *lua, double dval) {
    lua_real_t *i;
    l_real_new(lua);
    i = (lua_real_t*) lua_topointer(lua, -1);
    i->val = dval;
}

int luabb_isnull(lua_State *lua, int arg_index)
{
    int type = lua_type(lua, arg_index);
    if (type == LUA_TUSERDATA) {
        const lua_dbtypes_t *n = lua_topointer(lua, arg_index);
        return n->is_null;
    }
    return 0;
}

int luabb_istyped(lua_State *lua, int arg_index)
{
    int type = lua_type(lua, arg_index);
    if (type == LUA_TUSERDATA) {
        const lua_dbtypes_t *n = lua_topointer(lua, arg_index);
        return n->is_typed;
    }
    return 0;
}

void luabb_pushnull(Lua lua, int dbtype)
{
    switch (dbtype) {
    case DBTYPES_INTEGER:
        l_int_new(lua);
        break;
    case DBTYPES_REAL:
        l_real_new(lua);
        break;
    case DBTYPES_BLOB:
        l_blob_new(lua);
        break;
    case DBTYPES_DATETIME:
        l_datetime_new(lua);
        break;
    case DBTYPES_INTERVALYM:
        l_intervalym_new(lua);
        break;
    case DBTYPES_INTERVALDS:
        l_intervalds_new(lua);
        break;
    case DBTYPES_DECIMAL:
        l_decimal_new(lua);
        break;
    case DBTYPES_CSTRING:
    default:
        l_cstring_new(lua);
        break;
    }
    lua_dbtypes_t *t = lua_touserdata(lua, -1);
    t->is_null = 1;
    return; 
}

void luabb_pushdatetime(lua_State *lua, const datetime_t *val)
{
    l_datetime_new(lua);
    lua_datetime_t *i = lua_touserdata(lua, -1);
    i->val = *val;
}

void luabb_pushintervalym(lua_State *lua, const intv_t *val)
{
    l_intervalym_new(lua);
    lua_intervalym_t *i = (lua_intervalym_t*) lua_topointer(lua, -1);
    i->val = *val;
}

void luabb_pushintervalds(lua_State *lua, const intv_t *val)
{
    l_intervalds_new(lua);
    lua_intervalds_t *i = (lua_intervalds_t*) lua_topointer(lua, -1);
    i->val = *val;
}

char *luabb_newblob(Lua lua, int len, void **blob)
{
    lua_blob_t *b;
    l_blob_new(lua);
    *blob = b = (void *) lua_topointer(lua, -1);
    lua_pop(lua, 1);
    b->val.data = malloc(len);
    memset(b->val.data, 0xff, len);
    b->val.length = len;
    return (char *)b->val.data;
}

static int l_typed_assignment(lua_State *lua)
{
   const char *name1 = NULL;
   const char *name2 = NULL;
   if ((lua_type(lua, 1) == LUA_TUSERDATA)) {
       if (luabb_istyped(lua,1)) {
         lua_getmetatable(lua, 1); 
         lua_pushstring(lua, "__metatable");
         lua_gettable(lua, -2);
         name1 = lua_tostring(lua, -1);
         lua_getmetatable(lua, 2); 
         lua_pushstring(lua, "__metatable");
         lua_gettable(lua, -2);
         name2 = lua_tostring(lua, -1);         
         if (name1 && name2 && strcmp(name1, name2) == 0) { 
           lua_pop(lua,4);
           lua_remove(lua,1);
         } else {
           /* Remove everything and push nil. */  
           lua_settop(lua, 0);
           if (name1) {
             luabb_error(lua, NULL, "Invalid assignment, invalid typed assignment for type %s", name1);
           } else {
             luabb_error(lua, NULL, "Invalid assignment, invalid typed assignment");
           }
         }
       }
   } else {
         lua_remove(lua,1);
   }
   return 1;
}

int l_column_cast(Lua L)
{
    if (lua_isnil(L, -2)) {
        const char *name = lua_tostring(L, -1);
        int dbtype = luabb_dbtype_by_name(name);
        luabb_pushnull(L, dbtype);
    } else {
        int dbtype = luabb_dbtype(L, -2);
        luabb_typeconvert_int(L, -1, dbtype, NULL);
    }
    return 1;
}

static int l_concat(Lua lua)
{
    char *s1 = strdup(luabb_tostring(lua, 1));
    char *s2 = strdup(luabb_tostring(lua, 2));
    lua_pushfstring(lua, "%s%s", s1, s2);
    free(s1);
    free(s2);
    return 1;
}

static int l_int_new(lua_State *lua)
{
    lua_int_t *ival;
    new_lua_t(ival, lua_int_t, DBTYPES_INTEGER);
    return 1;
}

static int l_int_tostring(lua_State *lua)
{
    luaL_checkudata(lua, -1, dbtypes.integer);
    const lua_int_t *ival = lua_topointer(lua, -1);
    if (ival->is_null)
        lua_pushstring(lua, null_str);
    else
        luabb_pushfstring(lua, "%lld", ival->val);
    return 1;
}

static int l_int_unm(lua_State *lua)
{
    lua_int_t *ival, *out;
    luaL_checkudata(lua, 1, dbtypes.integer);
    ival = (lua_int_t*) lua_topointer(lua, 1);
    new_lua_t(out, lua_int_t, DBTYPES_INTEGER);
    *out = *ival;
    out->val = -out->val;
    return 1;
}

static const struct luaL_Reg int_funcs[] = {
    { "__tostring", l_int_tostring },
    { "__unm", l_int_unm },
    { NULL, NULL }
};

static void init_int(Lua L)
{
    luaL_newmetatable(L, dbtypes.integer);
    lua_pushstring(L, "__index");
    lua_pushvalue(L, -2);
    lua_settable(L, -3);

    luaL_openlib(L, NULL, int_funcs, 0);
    init_arithmetic(L, LUA_OP_MOD);
    init_cmp(L);
    init_common(L);

    lua_pop(L, 1);
}

static int l_real_new(Lua lua)
{
    lua_real_t *dval;
    new_lua_t(dval, lua_real_t, DBTYPES_REAL);
    return 1;
}

static int l_real_tostring(Lua lua)
{
    lua_real_t *dval;
    luaL_checkudata(lua, -1, dbtypes.real);
    dval = (lua_real_t*) lua_topointer(lua, -1);
    if (dval->is_null)
        lua_pushstring(lua, null_str);
    else {
        char buf[LUAI_MAXNUMBER2STR];
        lua_number2str(buf, dval->val);
        lua_pushstring(lua, buf);
    }
    return 1;
}

static int l_real_unm(lua_State *lua)
{
    lua_real_t * ival, *out;
    luaL_checkudata(lua, 1, dbtypes.real);
    new_lua_t(out, lua_real_t, DBTYPES_REAL);
    ival = (lua_real_t*) lua_topointer(lua, 1);
    *out = *ival;
    out->val = -out->val;

    return 1;
}

static const struct luaL_Reg real_funcs[] = {
    { "__tostring", l_real_tostring },
    { "__unm", l_real_unm },
    { NULL, NULL }
};

static void init_real(Lua L)
{
    luaL_newmetatable(L, dbtypes.real);
    lua_pushstring(L, "__index");
    lua_pushvalue(L, -2);
    lua_settable(L, -3);

    luaL_openlib(L, NULL, real_funcs, 0);
    init_arithmetic(L, LUA_OP_MOD);
    init_cmp(L);
    init_common(L);

    lua_pop(L, 1);
}

static int l_decimal_tostring(Lua lua)
{
    char out[DFP_128_MAX_STR];
    luaL_checkudata(lua, -1, dbtypes.decimal);
    const lua_dec_t *decVal = lua_topointer(lua, -1);
    if (decVal->is_null)
        lua_pushstring(lua, null_str);
    else {
        decQuadToString(&(decVal->val), (char*)out);
        lua_pushstring(lua, out);
    }
    return 1;
}

static int l_decimal_new(Lua lua)
{
    lua_dec_t *decVal;
    new_lua_t(decVal, lua_dec_t, DBTYPES_DECIMAL);
    return 1;
}

static const struct luaL_Reg decimal_funcs[] = {
    { "__tostring", l_decimal_tostring },
    { NULL, NULL }
};

static void init_decimal(Lua L)
{
    luaL_newmetatable(L, dbtypes.decimal);
    lua_pushstring(L, "__index");
    lua_pushvalue(L, -2);
    lua_settable(L, -3);

    luaL_openlib(L, NULL, decimal_funcs, 0);
    init_arithmetic(L, 0);
    init_cmp(L);
    init_common(L);

    lua_pop(L, 1);
}

static void init_null(Lua L)
{
    luabb_pushnull(L, DBTYPES_INTEGER);
    lua_setglobal(L, "NULL");
}

void init_dbtypes(Lua lua)
{
    init_int(lua);
    init_real(lua);
    init_cstring(lua);
    init_datetime(lua);
    init_intervalym(lua);
    init_intervalds(lua);
    init_blob(lua);
    init_decimal(lua);
    init_null(lua);
}

