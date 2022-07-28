/*
   Copyright 2015, 2021, Bloomberg Finance L.P.

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

#include <string.h>
#include <strings.h>
#include <limits.h>
#include <alloca.h>
#include <compile_time_assert.h>
#include <flibc.h>
#include "bdb_int.h"
#include "endian_core.h"
#include "locks.h"
#include "genid.h"
#include <fsnapf.h>
#include <cdb2_constants.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include "logmsg.h"
#include "str0.h"
#include "lrucache.h"
#include <sys/time.h>
#include "lockmacros.h"
#include <sys/poll.h>

extern int gbl_maxretries;
extern int gbl_disable_access_controls;

static bdb_state_type *llmeta_bdb_state = NULL; /* the low level meta table */

enum {
    LLMETA_IXLEN = 120 /* length of a llmeta key */
    ,
    LLMETA_TBLLEN = MAXTABLELEN /* maximum table name length */
    ,
    LLMETA_SPLEN = MAX_SPNAME /* maximum SP length. see also */
    ,
    LLMETA_STATS_IXLEN = 64 /* maximum index name length for sqlite_stat1 & 2. */
    ,
    LLMETA_ALIASLEN = 63 /* maximum alias name, must be at least MAXALIASNAME in comdb2.h! */
    ,
    LLMETA_URLLEN = 255, /* maximum target name, format [CLASS_]DBNAME.TBLNAME */
    LLMETA_COLUMNLEN = MAXCOLNAME
};

/* this enum serves as a header for the llmeta keys */
typedef enum {
    LLMETA_FVER_FILE_TYPE_TBL = 1 /* file version for a table */
    ,
    LLMETA_FVER_FILE_TYPE_IX = 2 /* file version for a index */
    ,
    LLMETA_FVER_FILE_TYPE_DTA = 3 /* file version for a data/blob */

    ,
    LLMETA_TBL_NAMES = 3 /* the names of tables */
    ,
    LLMETA_IN_SCHEMA_CHANGE = 4 /* whether a table is in a schema chg */
    ,
    LLMETA_HIGH_GENID = 5 /* highest in its stripe in schema chg */
    ,
    LLMETA_CSC2 = 6 /* csc2 schema */

    /* per table records */
    ,
    LLMETA_PAGESIZE_FILE_TYPE_IX = 7 /* pagesize for a index */
    ,
    LLMETA_PAGESIZE_FILE_TYPE_DTA = 8 /* pagesize for a data */
    ,
    LLMETA_PAGESIZE_FILE_TYPE_BLOB = 9 /* pagesize for a blob */

    /* defaults for all tables that are overriden with the above
       per table records */
    ,
    LLMETA_PAGESIZE_FILE_TYPE_ALLIX = 10 /* pagesize for a index */
    ,
    LLMETA_PAGESIZE_FILE_TYPE_ALLDTA = 11 /* pagesize for a data */
    ,
    LLMETA_PAGESIZE_FILE_TYPE_ALLBLOB = 12 /* pagesize for a blob */

    /* first LSN whose log file is safe to delete for
       the purposes of logecovery - aka Low Water Mark */
    ,
    LLMETA_LOGICAL_LSN_LWM = 13

    ,
    LLMETA_SC_SEEDS = 14

    ,
    LLMETA_TABLE_USER_READ = 15 /* key  = 15 + TABLENAME[32] + USERNAME[16]
                                   data = no data
                                   for this table, this user can read? */
    ,
    LLMETA_TABLE_USER_WRITE = 16 /* key  = 16 + TABLENAME[32] + USERNAME[16]
                                    data = no data
                                    for this table, this user can write? */
    ,
    LLMETA_USER_PASSWORD = 17 /* key  = 17 + USER[16]
                                 data = PASSWORD[16]
                                 whats the password for this user? */
    ,
    LLMETA_AUTHENTICATION = 18 /* key  = 18
                                  data = no data
                                  does db use authentication? */
    ,
    LLMETA_ACCESSCONTROL_TABLExNODE = 19

    ,
    LLMETA_SQLITE_STAT1_PREV_DONT_USE = 20 /* store previous sqlite-stat1 records- dont use this. */
    ,
    LLMETA_SQLITE_STAT2_PREV_DONT_USE = 21 /* store previous sqlite-stat2 records- dont use this. */
    ,
    LLMETA_SQLITE_STAT1_PREV = 22 /* store previous sqlite-stat1 records. */
    ,
    LLMETA_SQLITE_STAT2_PREV = 23 /* store previous sqlite-stat2 records. */
    ,
    LLMETA_SP_LUA_FILE = 24 /* Store the LUA sp data. */
    ,
    LLMETA_SP_LUA_SOURCE = 25 /* Store the LUA sp source. */
    ,
    LLMETA_BS = 26,
    LLMETA_ANALYZECOVERAGE_TABLE = 27 /* analyze coverage percent for a table */
    /* default is 20, meaninful range [0:100] */
    ,
    LLMETA_ANALYZETHRESHOLD_TABLE = 28 /* analyze threshold size for a table */
    ,
    LLMETA_CURR_ANALYZE_COUNT = 29,
    LLMETA_LAST_ANALYZE_COUNT = 30,
    LLMETA_LAST_ANALYZE_EPOCH = 31,
    LLMETA_FDB_TABLENAME_ALIAS = 32 /* table name to replace a full path
                                    DBNAME.TABLENAME */
    ,
    LLMETA_TABLE_VERSION = 33 /* reliable table version, updated by any schema change
                               */
    ,
    LLMETA_TABLE_PARAMETERS = 34 /* store various parameter values for tables
                              stored as a blob */
    ,
    LLMETA_ROWLOCKS_STATE = 35
    /* for some reason we skip 36 */
    ,
    LLMETA_TABLE_USER_OP = 37 /* The user can use DDL-like commands on the table
                                 key = 37 + TABLENAME[32] + USERNAME[16] */
    ,
    LLMETA_TRIGGER = 38,
    LLMETA_GENID_FORMAT = 39,
    LLMETA_LUA_SFUNC = 40,
    LLMETA_LUA_AFUNC = 41,
    LLMETA_VERSIONED_SP = 42,
    LLMETA_DEFAULT_VERSIONED_SP = 43,
    LLMETA_TABLE_USER_SCHEMA = 44,
    LLMETA_USER_PASSWORD_HASH = 45,
    LLMETA_FVER_FILE_TYPE_QDB = 46, /* file version for a dbqueue */
    LLMETA_TABLE_NUM_SC_DONE = 47,
    LLMETA_GLOBAL_STRIPE_INFO = 48,
    LLMETA_SC_START_LSN = 49,
    LLMETA_SCHEMACHANGE_STATUS = 50,
    LLMETA_VIEW = 51,                 /* User defined views */
    LLMETA_SCHEMACHANGE_HISTORY = 52, /* 52 + SEED[8] */
    LLMETA_SEQUENCE_VALUE = 53,
    LLMETA_LUA_SFUNC_FLAG = 54,
    LLMETA_NEWSC_REDO_GENID = 55, /* 55 + TABLENAME + GENID -> MAX-LSN */
    LLMETA_SCHEMACHANGE_STATUS_V2 = 56,
} llmetakey_t;

struct llmeta_file_type_key {
    int file_type;
};

int gbl_llmeta_deadlock_poll = 0;

enum { LLMETA_FILE_TYPE_KEY_LEN = 4 };

BB_COMPILE_TIME_ASSERT(llmeta_file_type_key,
                       sizeof(struct llmeta_file_type_key) ==
                           LLMETA_FILE_TYPE_KEY_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_file_type_key_overflow,
                       sizeof(struct llmeta_file_type_key) <= LLMETA_IXLEN);

static int kv_get(tran_type *t, void *k, size_t klen, void ***ret, int *num, int *bdberr);
static int kv_put(tran_type *tran, void *k, void *v, size_t vlen, int *bdberr);
static int kv_del(tran_type *tran, void *k, int *bdberr);
static int kv_get_kv(tran_type *t, void *k, size_t klen, void ***keys,
                     void ***values, int *num, int *bdberr);
static int kv_del_by_value(tran_type *tran, void *k, size_t klen, void *v, size_t vlen, int *bdberr);

static uint8_t *
llmeta_file_type_key_put(const struct llmeta_file_type_key *p_file_type_key,
                         uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_FILE_TYPE_KEY_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_file_type_key->file_type),
                    sizeof(p_file_type_key->file_type), p_buf, p_buf_end);

    return p_buf;
}

struct llmeta_version_number_type {
    unsigned long long version_num;
};

enum { LLMETA_VERSION_NUMBER_TYPE_SIZE = 8 };

BB_COMPILE_TIME_ASSERT(llmeta_version_number_type,
                       sizeof(struct llmeta_version_number_type) ==
                           LLMETA_VERSION_NUMBER_TYPE_SIZE);

BB_COMPILE_TIME_ASSERT(llmeta_version_number_type_overflow,
                       sizeof(struct llmeta_version_number_type) <=
                           LLMETA_IXLEN);

static const uint8_t *
llmeta_version_number_get(struct llmeta_version_number_type *p_version_num,
                          const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_VERSION_NUMBER_TYPE_SIZE > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_no_net_get(&(p_version_num->version_num),
                       sizeof(p_version_num->version_num), p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *llmeta_version_number_put(
    const struct llmeta_version_number_type *p_version_num, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_VERSION_NUMBER_TYPE_SIZE > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_no_net_put(&(p_version_num->version_num),
                       sizeof(p_version_num->version_num), p_buf, p_buf_end);

    return p_buf;
}

struct llmeta_table_name {
    char table_name[LLMETA_TBLLEN + 1];
    char padding1[3];
    int table_name_len;
    int dbnum;
};

enum {
    LLMETA_TABLE_NAME_LEN = LLMETA_TBLLEN + 1 + 3 + 4 + 4,
    LLMETA_TABLE_NAME_MIN_LEN = 2 + 4
};

BB_COMPILE_TIME_ASSERT(llmeta_table_name_len,
                       sizeof(struct llmeta_table_name) ==
                           LLMETA_TABLE_NAME_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_table_name_len_overflow,
                       sizeof(struct llmeta_table_name) <= LLMETA_IXLEN);

/* formulates a table_names data-packet for the llmeta module */
static uint8_t *
llmeta_table_name_put(const struct llmeta_table_name *p_table_name,
                      uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_TABLE_NAME_MIN_LEN > p_buf_end - p_buf)
        return NULL;

    if (p_table_name->table_name_len > sizeof(p_table_name->table_name))
        return NULL;

    p_buf = buf_no_net_put(&(p_table_name->table_name),
                           p_table_name->table_name_len, p_buf, p_buf_end);
    p_buf = buf_put(&(p_table_name->dbnum), sizeof(p_table_name->dbnum), p_buf,
                    p_buf_end);

    return p_buf;
}

static const uint8_t *
llmeta_table_name_get(struct llmeta_table_name *p_table_name,
                      const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_TABLE_NAME_MIN_LEN > p_buf_end - p_buf)
        return NULL;

    if ((p_table_name->table_name_len = strlen((const char *)p_buf) + 1) >
        sizeof(p_table_name->table_name))
        return NULL;

    p_buf = buf_no_net_get(&(p_table_name->table_name),
                           p_table_name->table_name_len, p_buf, p_buf_end);
    p_buf = buf_get(&(p_table_name->dbnum), sizeof(p_table_name->dbnum), p_buf,
                    p_buf_end);

    return p_buf;
}

struct llmeta_file_type_dbname_file_num_key {
    int file_type;
    char dbname[LLMETA_TBLLEN + 1];
    char padding1[3];
    int dbname_len;
    int file_num;
};

enum {
    LLMETA_FILE_TYPE_DBNAME_FILE_NUM_KEY_LEN = 4 + LLMETA_TBLLEN + 1 + 3 + 4 + 4
};

BB_COMPILE_TIME_ASSERT(llmeta_file_type_dbname_file_num_key,
                       sizeof(struct llmeta_file_type_dbname_file_num_key) ==
                           LLMETA_FILE_TYPE_DBNAME_FILE_NUM_KEY_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_file_type_dbname_file_num_key_overflow,
                       sizeof(struct llmeta_file_type_dbname_file_num_key) <=
                           LLMETA_IXLEN);

static uint8_t *llmeta_file_type_dbname_file_num_put(
    const struct llmeta_file_type_dbname_file_num_key
        *p_file_type_dbname_file_num_key,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf)
        return NULL;

    if (p_file_type_dbname_file_num_key->dbname_len >
        sizeof(p_file_type_dbname_file_num_key->dbname))
        return NULL;

    p_buf = buf_put(&(p_file_type_dbname_file_num_key->file_type),
                    sizeof(p_file_type_dbname_file_num_key->file_type), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_put(&(p_file_type_dbname_file_num_key->dbname),
                           p_file_type_dbname_file_num_key->dbname_len, p_buf,
                           p_buf_end);
    p_buf = buf_put(&(p_file_type_dbname_file_num_key->file_num),
                    sizeof(p_file_type_dbname_file_num_key->file_num), p_buf,
                    p_buf_end);

    return p_buf;
}

static const uint8_t *
llmeta_file_type_dbname_file_num_get(struct llmeta_file_type_dbname_file_num_key
                                         *p_file_type_dbname_file_num_key,
                                     const uint8_t *p_buf,
                                     const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_FILE_TYPE_DBNAME_FILE_NUM_KEY_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_file_type_dbname_file_num_key->file_type),
                    sizeof(p_file_type_dbname_file_num_key->file_type), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_get(&(p_file_type_dbname_file_num_key->dbname),
                           sizeof(p_file_type_dbname_file_num_key->dbname),
                           p_buf, p_buf_end);
    p_buf += 3;
    p_buf = buf_get(&(p_file_type_dbname_file_num_key->dbname_len),
                    sizeof(p_file_type_dbname_file_num_key->dbname_len), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_file_type_dbname_file_num_key->file_num),
                    sizeof(p_file_type_dbname_file_num_key->file_num), p_buf,
                    p_buf_end);

    return p_buf;
}

struct llmeta_file_type_dbname_key {
    int file_type;
    char dbname[LLMETA_TBLLEN + 1];
    char padding1[3];
    int dbname_len;
};

enum { LLMETA_FILE_TYPE_DBNAME_KEY_LEN = 4 + LLMETA_TBLLEN + 1 + 3 + 4 };

BB_COMPILE_TIME_ASSERT(llmeta_file_type_dbname_key,
                       sizeof(struct llmeta_file_type_dbname_key) ==
                           LLMETA_FILE_TYPE_DBNAME_KEY_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_file_type_dbname_key_overflow,
                       sizeof(struct llmeta_file_type_dbname_key) <=
                           LLMETA_IXLEN);

static uint8_t *llmeta_file_type_dbname_key_put(
    const struct llmeta_file_type_dbname_key *p_file_type_dbname_key,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf)
        return NULL;
    if (p_file_type_dbname_key->dbname_len >
        sizeof(p_file_type_dbname_key->dbname))
        return NULL;

    p_buf =
        buf_put(&(p_file_type_dbname_key->file_type),
                sizeof(p_file_type_dbname_key->file_type), p_buf, p_buf_end);
    p_buf =
        buf_no_net_put(&(p_file_type_dbname_key->dbname),
                       p_file_type_dbname_key->dbname_len, p_buf, p_buf_end);

    return p_buf;
}

/* for bdb_get_pagesize */
struct llmeta_page_size_type {
    int page_size;
};

enum { LLMETA_PAGE_SIZE_TYPE = 4 };

BB_COMPILE_TIME_ASSERT(llmeta_page_size_type,
                       sizeof(struct llmeta_page_size_type) ==
                           LLMETA_PAGE_SIZE_TYPE);

BB_COMPILE_TIME_ASSERT(llmeta_page_size_type_overflow,
                       sizeof(struct llmeta_page_size_type) <= LLMETA_IXLEN);

static uint8_t *
llmeta_page_size_put(const struct llmeta_page_size_type *p_page_size,
                     uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_PAGE_SIZE_TYPE > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_page_size->page_size), sizeof(p_page_size->page_size),
                    p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
llmeta_page_size_get(struct llmeta_page_size_type *p_page_size,
                     const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_PAGE_SIZE_TYPE > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_page_size->page_size), sizeof(p_page_size->page_size),
                    p_buf, p_buf_end);

    return p_buf;
}

struct llmeta_file_type_spname_lua_vers_key {
    int file_type;
    char spname[LLMETA_SPLEN + 1];
    char padding1[3];
    int lua_vers;
    int spname_len;
};

enum {
    LLMETA_FILE_TYPE_SPNAME_LUA_VERS_LEN = 4 + LLMETA_SPLEN + 1 + 3 + 4 + 4,
    LLMETA_FILE_TYPE_SPNAME_LUA_VERS_MIN_LEN = 4 + 2 + 4
};

BB_COMPILE_TIME_ASSERT(llmeta_file_type_spname_lua_vers_key,
                       sizeof(struct llmeta_file_type_spname_lua_vers_key) ==
                           LLMETA_FILE_TYPE_SPNAME_LUA_VERS_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_file_type_spname_lua_vers_key_overflow,
                       sizeof(struct llmeta_file_type_spname_lua_vers_key) <=
                           LLMETA_IXLEN);

static uint8_t *llmeta_file_type_spname_lua_vers_key_put(
    const struct llmeta_file_type_spname_lua_vers_key
        *p_file_type_spname_lua_vers_key,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_FILE_TYPE_SPNAME_LUA_VERS_MIN_LEN > (p_buf_end - p_buf))
        return NULL;
    if (p_file_type_spname_lua_vers_key->spname_len >
        sizeof(p_file_type_spname_lua_vers_key->spname))
        return NULL;

    p_buf = buf_put(&(p_file_type_spname_lua_vers_key->file_type),
                    sizeof(p_file_type_spname_lua_vers_key->file_type), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_put(&(p_file_type_spname_lua_vers_key->spname),
                           p_file_type_spname_lua_vers_key->spname_len, p_buf,
                           p_buf_end);
    p_buf = buf_put(&(p_file_type_spname_lua_vers_key->lua_vers),
                    sizeof(p_file_type_spname_lua_vers_key->lua_vers), p_buf,
                    p_buf_end);

    return p_buf;
}

static const uint8_t *llmeta_file_type_spname_lua_vers_key_get(
    struct llmeta_file_type_spname_lua_vers_key
        *p_file_type_spname_lua_vers_key,
    const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_FILE_TYPE_SPNAME_LUA_VERS_MIN_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf = buf_get(&(p_file_type_spname_lua_vers_key->file_type),
                    sizeof(p_file_type_spname_lua_vers_key->file_type), p_buf,
                    p_buf_end);
    if ((p_file_type_spname_lua_vers_key->spname_len =
             (strlen((const char *)p_buf) + 1)) >
        sizeof(p_file_type_spname_lua_vers_key->spname))
        return NULL;
    p_buf = buf_no_net_get(&(p_file_type_spname_lua_vers_key->spname),
                           p_file_type_spname_lua_vers_key->spname_len, p_buf,
                           p_buf_end);
    p_buf = buf_get(&(p_file_type_spname_lua_vers_key->lua_vers),
                    sizeof(p_file_type_spname_lua_vers_key->lua_vers), p_buf,
                    p_buf_end);

    return p_buf;
}

struct llmeta_file_type_dbname_csc2_vers_key {
    int file_type;
    char dbname[LLMETA_TBLLEN + 1];
    char padding1[3];
    int dbname_len;
    int csc2_vers;
};

enum {
    LLMETA_FILE_TYPE_DBNAME_CSC2_VERS_LEN = 4 + LLMETA_TBLLEN + 1 + 3 + 4 + 4,
    LLMETA_FILE_TYPE_DBNAME_CSC2_VERS_MIN_LEN = 4 + 2 + 4
};

BB_COMPILE_TIME_ASSERT(llmeta_file_type_dbname_csc2_vers_key,
                       sizeof(struct llmeta_file_type_dbname_csc2_vers_key) ==
                           LLMETA_FILE_TYPE_DBNAME_CSC2_VERS_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_file_type_dbname_csc2_vers_key_overflow,
                       sizeof(struct llmeta_file_type_dbname_csc2_vers_key) <=
                           LLMETA_IXLEN);

static uint8_t *llmeta_file_type_dbname_csc2_vers_key_put(
    const struct llmeta_file_type_dbname_csc2_vers_key
        *p_file_type_dbname_csc2_vers_key,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_FILE_TYPE_DBNAME_CSC2_VERS_MIN_LEN > (p_buf_end - p_buf))
        return NULL;
    if (p_file_type_dbname_csc2_vers_key->dbname_len >
        sizeof(p_file_type_dbname_csc2_vers_key->dbname))
        return NULL;

    p_buf = buf_put(&(p_file_type_dbname_csc2_vers_key->file_type),
                    sizeof(p_file_type_dbname_csc2_vers_key->file_type), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_put(&(p_file_type_dbname_csc2_vers_key->dbname),
                           p_file_type_dbname_csc2_vers_key->dbname_len, p_buf,
                           p_buf_end);
    p_buf = buf_put(&(p_file_type_dbname_csc2_vers_key->csc2_vers),
                    sizeof(p_file_type_dbname_csc2_vers_key->csc2_vers), p_buf,
                    p_buf_end);

    return p_buf;
}

static const uint8_t *llmeta_file_type_dbname_csc2_vers_key_get(
    struct llmeta_file_type_dbname_csc2_vers_key
        *p_file_type_dbname_csc2_vers_key,
    const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_FILE_TYPE_DBNAME_CSC2_VERS_MIN_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf = buf_get(&(p_file_type_dbname_csc2_vers_key->file_type),
                    sizeof(p_file_type_dbname_csc2_vers_key->file_type), p_buf,
                    p_buf_end);
    if ((p_file_type_dbname_csc2_vers_key->dbname_len =
             (strlen((const char *)p_buf) + 1)) >
        sizeof(p_file_type_dbname_csc2_vers_key->dbname))
        return NULL;
    p_buf = buf_no_net_get(&(p_file_type_dbname_csc2_vers_key->dbname),
                           p_file_type_dbname_csc2_vers_key->dbname_len, p_buf,
                           p_buf_end);
    p_buf = buf_get(&(p_file_type_dbname_csc2_vers_key->csc2_vers),
                    sizeof(p_file_type_dbname_csc2_vers_key->csc2_vers), p_buf,
                    p_buf_end);

    return p_buf;
}

struct llmeta_sane_table_version {
    int file_type;
    char tblname[LLMETA_TBLLEN + 1];
    char padding1[3];
};

enum { LLMETA_SANE_TABLE_VERSION_LEN = 4 + LLMETA_TBLLEN + 1 + 3 };

BB_COMPILE_TIME_ASSERT(llmeta_sane_table_version,
                       sizeof(struct llmeta_sane_table_version) ==
                           LLMETA_SANE_TABLE_VERSION_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_sane_table_version_overflow,
                       sizeof(struct llmeta_sane_table_version) <=
                           LLMETA_IXLEN);

static uint8_t *llmeta_sane_table_version_put(
    const struct llmeta_sane_table_version *p_llmeta_sane_table_version,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_SANE_TABLE_VERSION_LEN > (p_buf_end - p_buf))
        return NULL;

    bzero(p_buf, sizeof(*p_llmeta_sane_table_version));
    p_buf = buf_put(&(p_llmeta_sane_table_version->file_type),
                    sizeof(p_llmeta_sane_table_version->file_type), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_put(&(p_llmeta_sane_table_version->tblname),
                           sizeof(p_llmeta_sane_table_version)->tblname, p_buf,
                           p_buf_end);
    return p_buf;
}

static const uint8_t *llmeta_sane_table_version_get(
    struct llmeta_sane_table_version *p_llmeta_sane_table_version,
    const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_SANE_TABLE_VERSION_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_llmeta_sane_table_version->file_type),
                    sizeof(p_llmeta_sane_table_version->file_type), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_get(&(p_llmeta_sane_table_version->tblname),
                           sizeof(p_llmeta_sane_table_version->tblname), p_buf,
                           p_buf_end);
    return p_buf;
}

struct llmeta_rowlocks_state_key_type {
    int file_type;
};

enum { LLMETA_ROWLOCKS_STATE_KEY_LEN = 4 };

BB_COMPILE_TIME_ASSERT(llmeta_rowlocks_state_key_len,
                       sizeof(struct llmeta_rowlocks_state_key_type) ==
                           LLMETA_ROWLOCKS_STATE_KEY_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_rowlocks_state_key_len_overflow,
                       sizeof(struct llmeta_rowlocks_state_key_type) <=
                           LLMETA_IXLEN);

static uint8_t *llmeta_rowlocks_state_key_type_put(
    const struct llmeta_rowlocks_state_key_type *p_rowlocks_state,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_ROWLOCKS_STATE_KEY_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf = buf_put(&(p_rowlocks_state->file_type),
                    sizeof(p_rowlocks_state->file_type), p_buf, p_buf_end);
    return p_buf;
}

struct llmeta_rowlocks_state_data_type {
    int rowlocks_state;
};

enum { LLMETA_ROWLOCKS_STATE_DATA_LEN = 4 };

BB_COMPILE_TIME_ASSERT(llmeta_rowlocks_state_data_len,
                       sizeof(struct llmeta_rowlocks_state_data_type) ==
                           LLMETA_ROWLOCKS_STATE_DATA_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_rowlocks_state_data_len_overflow,
                       sizeof(struct llmeta_rowlocks_state_data_type) <=
                           LLMETA_IXLEN);

static uint8_t *llmeta_rowlocks_state_data_type_put(
    const struct llmeta_rowlocks_state_data_type *p_rowlocks_state,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_ROWLOCKS_STATE_DATA_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf = buf_put(&(p_rowlocks_state->rowlocks_state),
                    sizeof(p_rowlocks_state->rowlocks_state), p_buf, p_buf_end);
    return p_buf;
}

static const uint8_t *llmeta_rowlocks_state_data_type_get(
    struct llmeta_rowlocks_state_data_type *p_rowlocks_state,
    const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_ROWLOCKS_STATE_DATA_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf = buf_get(&(p_rowlocks_state->rowlocks_state),
                    sizeof(p_rowlocks_state->rowlocks_state), p_buf, p_buf_end);
    return p_buf;
}

struct llmeta_schema_change_type {
    int file_type;
    char dbname[LLMETA_TBLLEN + 1];
    char padding1[3];
    int dbname_len;
};

enum { LLMETA_SCHEMA_CHANGE_TYPE_LEN = 4 + LLMETA_TBLLEN + 1 + 3 + 4 };

BB_COMPILE_TIME_ASSERT(llmeta_schema_change_type,
                       sizeof(struct llmeta_schema_change_type) ==
                           LLMETA_SCHEMA_CHANGE_TYPE_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_schema_change_type_overflow,
                       sizeof(struct llmeta_schema_change_type) <=
                           LLMETA_IXLEN);

static uint8_t *llmeta_schema_change_type_put(
    const struct llmeta_schema_change_type *p_schema_change, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_SCHEMA_CHANGE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_schema_change->file_type),
                    sizeof(p_schema_change->file_type), p_buf, p_buf_end);

    p_buf = buf_no_net_put(&(p_schema_change->dbname),
                           p_schema_change->dbname_len, p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
llmeta_schema_change_type_get(struct llmeta_schema_change_type *p_schema_change,
                              const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_SCHEMA_CHANGE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_schema_change->file_type),
                    sizeof(p_schema_change->file_type), p_buf, p_buf_end);

    p_buf = buf_no_net_get(&(p_schema_change->dbname),
                           sizeof(p_schema_change->dbname), p_buf, p_buf_end);

    p_buf += 3;

    p_buf = buf_get(&(p_schema_change->dbname_len),
                    sizeof(p_schema_change->dbname_len), p_buf, p_buf_end);

    return p_buf;
}

struct llmeta_high_genid_key_type {
    int file_type;
    char dbname[LLMETA_TBLLEN + 1];
    int stripe;
    int dbname_len; /* apparently this is the existing on disk order, stripe
                       followed by dbname_len */
};

enum {
    LLMETA_HIGH_GENID_KEY_TYPE_LEN = 4 + LLMETA_TBLLEN + 1 + 4 + 4,
    LLMETA_HIGH_GENID_KEY_TYPE_MIN_LEN = 4 + 2 + 4
};

static uint8_t *llmeta_high_genid_key_type_put(
    const struct llmeta_high_genid_key_type *p_high_genid_key_type,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_HIGH_GENID_KEY_TYPE_MIN_LEN > (p_buf_end - p_buf))
        return NULL;

    if (p_high_genid_key_type->dbname_len >
        sizeof(p_high_genid_key_type->dbname))
        return NULL;

    p_buf = buf_put(&(p_high_genid_key_type->file_type),
                    sizeof(p_high_genid_key_type->file_type), p_buf, p_buf_end);

    p_buf = buf_no_net_put(&(p_high_genid_key_type->dbname),
                           p_high_genid_key_type->dbname_len, p_buf, p_buf_end);

    p_buf = buf_put(&(p_high_genid_key_type->stripe),
                    sizeof(p_high_genid_key_type->stripe), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *llmeta_high_genid_key_type_get(
    struct llmeta_high_genid_key_type *p_high_genid_key_type,
    const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    int i;
    if (p_buf_end < p_buf ||
        LLMETA_HIGH_GENID_KEY_TYPE_MIN_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_high_genid_key_type->file_type),
                    sizeof(p_high_genid_key_type->file_type), p_buf, p_buf_end);

    /* the structure is fubar, patching here */
    /* THE STRING IS NOT EVEN NULL TERMINATED !!! */
    for (i = 0; p_buf[i] && i < sizeof(p_high_genid_key_type->dbname); i++)
        ;
    if (i >= sizeof(p_high_genid_key_type->dbname)) {
        return NULL;
    }

    p_buf =
        buf_no_net_get(&(p_high_genid_key_type->dbname), i, p_buf, p_buf_end);

    p_buf = buf_get(&(p_high_genid_key_type->stripe),
                    sizeof(p_high_genid_key_type->stripe), p_buf, p_buf_end);

    /* MIA field
       p_buf = buf_get(&(p_high_genid_key_type->dbname_len),
       sizeof(p_high_genid_key_type->dbname_len),p_buf, p_buf_end);
     */
    p_high_genid_key_type->dbname_len = i + 1;

    return p_buf;
}

struct llmeta_db_lsn_data_type {
    DB_LSN lsn;
};

enum { LLMETA_DB_LSN_DATA_TYPE_LEN = 8 };

static uint8_t *llmeta_db_lsn_data_type_put(
    const struct llmeta_db_lsn_data_type *p_db_lsn_key_type, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_DB_LSN_DATA_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_db_lsn_key_type->lsn.file),
                    sizeof(p_db_lsn_key_type->lsn.file), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_lsn_key_type->lsn.offset),
                    sizeof(p_db_lsn_key_type->lsn.offset), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
llmeta_db_lsn_data_type_get(struct llmeta_db_lsn_data_type *p_db_lsn_key_type,
                            const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_DB_LSN_DATA_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_db_lsn_key_type->lsn.file),
                    sizeof(p_db_lsn_key_type->lsn.file), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_lsn_key_type->lsn.offset),
                    sizeof(p_db_lsn_key_type->lsn.offset), p_buf, p_buf_end);

    return p_buf;
}

/*************** LLMETA TABLE ACCESS STRUCT AND SERIALIZING METHODS *********/
struct llmeta_tbl_access {
    int file_type;
    char tablename[32];
    char username[32];
};

enum { LLMETA_TBL_ACCESS_LEN = 4 + 32 + 32 };
BB_COMPILE_TIME_ASSERT(llmeta_tbl_access_len,
                       sizeof(struct llmeta_tbl_access) ==
                           LLMETA_TBL_ACCESS_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_tbl_access_len_overflow,
                       sizeof(struct llmeta_tbl_access) <= LLMETA_IXLEN);

static const uint8_t *
llmeta_tbl_access_get(struct llmeta_tbl_access *p_tbl_access,
                      const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_TBL_ACCESS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_tbl_access->file_type), sizeof(p_tbl_access->file_type),
                    p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_tbl_access->tablename),
                           sizeof(p_tbl_access->tablename), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_tbl_access->username),
                           sizeof(p_tbl_access->username), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
llmeta_tbl_access_put(struct llmeta_tbl_access *p_tbl_access, uint8_t *p_buf,
                      const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_TBL_ACCESS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_tbl_access->file_type), sizeof(p_tbl_access->file_type),
                    p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_tbl_access->tablename),
                           sizeof(p_tbl_access->tablename), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_tbl_access->username),
                           sizeof(p_tbl_access->username), p_buf, p_buf_end);

    return p_buf;
}

/*******************************************************************************/

/*************** LLMETA PER TABLE OP ACCESS STRUCT AND SERIALIZING METHODS ****/

struct llmeta_tbl_op_access {
    int file_type;
    int command_type;
    char tablename[32];
    char username[32];
};

enum { LLMETA_TBL_OP_ACCESS_LEN = 4 + 4 + 32 + 32 };
BB_COMPILE_TIME_ASSERT(llmeta_tbl_op_access_len,
                       sizeof(struct llmeta_tbl_op_access) ==
                           LLMETA_TBL_OP_ACCESS_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_tbl_op_access_len_overflow,
                       sizeof(struct llmeta_tbl_op_access) <= LLMETA_IXLEN);

static const uint8_t *
llmeta_tbl_op_access_get(struct llmeta_tbl_op_access *p_tbl_access,
                         const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_TBL_OP_ACCESS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_tbl_access->file_type), sizeof(p_tbl_access->file_type),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_tbl_access->command_type),
                    sizeof(p_tbl_access->command_type), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_tbl_access->tablename),
                           sizeof(p_tbl_access->tablename), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_tbl_access->username),
                           sizeof(p_tbl_access->username), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
llmeta_tbl_op_access_put(struct llmeta_tbl_op_access *p_tbl_access,
                         uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_TBL_OP_ACCESS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_tbl_access->file_type), sizeof(p_tbl_access->file_type),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_tbl_access->command_type),
                    sizeof(p_tbl_access->command_type), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_tbl_access->tablename),
                           sizeof(p_tbl_access->tablename), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_tbl_access->username),
                           sizeof(p_tbl_access->username), p_buf, p_buf_end);

    return p_buf;
}

/*******************************************************************************/

enum { LLMETA_USER_LEN = 16 };

struct llmeta_user_password {
    int file_type;
    char user[LLMETA_USER_LEN];
};

enum { LLMETA_USER_PASSWORD_LEN = LLMETA_FILE_TYPE_KEY_LEN  + LLMETA_USER_LEN };

BB_COMPILE_TIME_ASSERT(llmeta_user_password_len,
                       sizeof(struct llmeta_user_password) ==
                           LLMETA_USER_PASSWORD_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_user_password_len_overflow,
                       sizeof(struct llmeta_user_password) <= LLMETA_IXLEN);

static const uint8_t *
llmeta_user_password_get(struct llmeta_user_password *p_user_password,
                         const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_USER_PASSWORD_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_user_password->file_type),
                    sizeof(p_user_password->file_type), p_buf, p_buf_end);

    p_buf = buf_no_net_get(&(p_user_password->user),
                           sizeof(p_user_password->user), p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *
llmeta_user_password_put(struct llmeta_user_password *p_user_password,
                         uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_USER_PASSWORD_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_user_password->file_type),
                    sizeof(p_user_password->file_type), p_buf, p_buf_end);

    p_buf = buf_no_net_put(&(p_user_password->user),
                           sizeof(p_user_password->user), p_buf, p_buf_end);

    return p_buf;
}

struct llmeta_authentication {
    int file_type;
};

enum { LLMETA_AUTHENTICATION_LEN = 4 };
BB_COMPILE_TIME_ASSERT(llmeta_authentication_len,
                       sizeof(struct llmeta_authentication) ==
                           LLMETA_AUTHENTICATION_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_authentication_len_overflow,
                       sizeof(struct llmeta_authentication) <= LLMETA_IXLEN);

static const uint8_t *
llmeta_authentication_get(struct llmeta_authentication *p_authentication,
                          const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_AUTHENTICATION_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_authentication->file_type),
                    sizeof(p_authentication->file_type), p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *
llmeta_authentication_put(struct llmeta_authentication *p_authentication,
                          uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_AUTHENTICATION_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_authentication->file_type),
                    sizeof(p_authentication->file_type), p_buf, p_buf_end);

    return p_buf;
}

/* llmeta sqlite_stats
 *
 * This key is used for storing previous analyze stats for databases.  At
 * commit time, analyze copies and stores the previous data in llmeta.  This
 * facilitates analyze's 'backout' functionality.  In sqlite_stat1
 * the indexes names have the following form:
 *
 * '$KEY1KEY2KEYKEY3_BUNCHOFHASHEDDATA'
 *
 * The hashed data the crc32 of the key attributes.  See sqlglue.csql_index_name
 * for the exact details. The maximum size of this is capped at 64 bytes- the
 * length of the column as defined in the sqlite_stat1 table.
 *
 */
struct llmeta_sqlstat1_key {
    int file_type;
    char table_name[LLMETA_TBLLEN + 1];
    char padding1[3];
    char index_name[LLMETA_STATS_IXLEN + 8];
};

/* enum'd size */
enum {
    LLMETA_SQLSTAT1_KEY_LEN = 4 + LLMETA_TBLLEN + 1 + 3 + LLMETA_STATS_IXLEN + 8
};

/* sanity check */
BB_COMPILE_TIME_ASSERT(llmeta_sqlstat1_key_len,
                       sizeof(struct llmeta_sqlstat1_key) <=
                           LLMETA_SQLSTAT1_KEY_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_sqlstat1_key_len_overflow,
                       sizeof(struct llmeta_sqlstat1_key) <= LLMETA_IXLEN);

/* pack sqlite_stat1 key */
static uint8_t *
llmeta_sqlstat1_key_put(const struct llmeta_sqlstat1_key *p_sqlstat1,
                        uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_SQLSTAT1_KEY_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_sqlstat1->file_type), sizeof(p_sqlstat1->file_type),
                    p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_sqlstat1->table_name),
                           sizeof(p_sqlstat1->table_name), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_sqlstat1->padding1),
                           sizeof(p_sqlstat1->padding1), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_sqlstat1->index_name),
                           sizeof(p_sqlstat1->index_name), p_buf, p_buf_end);

    return p_buf;
}

/* I have to keep the broken definitions around temporarily to cleanup the
 * stale llmeta entries.
 */

struct llmeta_sqlstat1_stale_key {
    int file_type;
    char table_name[LLMETA_TBLLEN + 1];
    char padding1[3];
    char index_name[LLMETA_TBLLEN + 8];
};

enum {
    LLMETA_SQLSTAT1_STALE_KEY = 4 + LLMETA_TBLLEN + 1 + 3 + LLMETA_TBLLEN + 8
};

BB_COMPILE_TIME_ASSERT(llmeta_sqlstat1_stale_key,
                       sizeof(struct llmeta_sqlstat1_stale_key) ==
                           LLMETA_SQLSTAT1_STALE_KEY);

BB_COMPILE_TIME_ASSERT(llmeta_sqlstat1_stale_key_overflow,
                       sizeof(struct llmeta_sqlstat1_stale_key) <=
                           LLMETA_IXLEN);

static uint8_t *llmeta_sqlstat1_stale_key_put(
    const struct llmeta_sqlstat1_stale_key *p_sqlstat1, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_SQLSTAT1_STALE_KEY > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_sqlstat1->file_type), sizeof(p_sqlstat1->file_type),
                    p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_sqlstat1->table_name),
                           sizeof(p_sqlstat1->table_name), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_sqlstat1->padding1),
                           sizeof(p_sqlstat1->padding1), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_sqlstat1->index_name),
                           sizeof(p_sqlstat1->index_name), p_buf, p_buf_end);

    return p_buf;
}

struct llmeta_tablename_alias_key {
    int file_type;
    char tablename_alias[LLMETA_ALIASLEN + 1];
};

enum { LLMETA_TABLENAME_ALIAS_KEY_LEN = 4 + LLMETA_ALIASLEN + 1 };

BB_COMPILE_TIME_ASSERT(llmeta_tablename_alias_key,
                       sizeof(struct llmeta_tablename_alias_key) ==
                           LLMETA_TABLENAME_ALIAS_KEY_LEN);

BB_COMPILE_TIME_ASSERT(llmeta_tablename_alias_key_overflow,
                       sizeof(struct llmeta_tablename_alias_key) <=
                           LLMETA_IXLEN);

static uint8_t *llmeta_tablename_alias_key_put(
    const struct llmeta_tablename_alias_key *p_tablename_alias_key,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf)
        return NULL;

    p_buf = buf_put(&(p_tablename_alias_key->file_type),
                    sizeof(p_tablename_alias_key->file_type), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_tablename_alias_key->tablename_alias),
                           sizeof(p_tablename_alias_key->tablename_alias),
                           p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *llmeta_tablename_alias_key_get(
    struct llmeta_tablename_alias_key *p_tablename_alias_key,
    const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_TABLENAME_ALIAS_KEY_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_tablename_alias_key->file_type),
                    sizeof(p_tablename_alias_key->file_type), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_tablename_alias_key->tablename_alias),
                           sizeof(p_tablename_alias_key->tablename_alias),
                           p_buf, p_buf_end);

    return p_buf;
}

struct llmeta_tablename_alias_data {
    char url[LLMETA_URLLEN + 1];
};

enum { LLMETA_TABLENAME_ALIAS_DATA_LEN = LLMETA_URLLEN + 1 };

BB_COMPILE_TIME_ASSERT(llmeta_tablename_alias_data,
                       sizeof(struct llmeta_tablename_alias_data) ==
                           LLMETA_TABLENAME_ALIAS_DATA_LEN);

static uint8_t *llmeta_tablename_alias_data_put(
    const struct llmeta_tablename_alias_data *p_tablename_alias_data,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf)
        return NULL;

    p_buf =
        buf_no_net_put(&(p_tablename_alias_data->url),
                       sizeof(p_tablename_alias_data->url), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *llmeta_tablename_alias_data_get(
    struct llmeta_tablename_alias_data *p_tablename_alias_data,
    const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        LLMETA_TABLENAME_ALIAS_DATA_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_no_net_get(&(p_tablename_alias_data->url),
                       sizeof(p_tablename_alias_data->url), p_buf, p_buf_end);

    return p_buf;
}

/* returns true if we have a llmeta table open else false */
int bdb_have_llmeta() { return llmeta_bdb_state != NULL; }

/* opens the low level meta table, if this is not called, any calls to
 * bdb_get_file_version* will return successfully but with a 0 version_number
 * any calls to bdb_new_file_version will fail */
int bdb_llmeta_open(char name[], char dir[], bdb_state_type *parent_bdb_handle,
                    int create_override, int *bdberr)
{
    bdb_state_type *bdb_state = parent_bdb_handle;
    BDB_READLOCK("bdb_llmeta_open");

    if (llmeta_bdb_state) {
        BDB_RELLOCK();
        return 0;
    }

    if (create_override)
        llmeta_bdb_state = bdb_create_more_lite(name, dir, 0, LLMETA_IXLEN, 0,
                                                parent_bdb_handle, bdberr);
    else
        llmeta_bdb_state = bdb_open_more_lite(
            name, dir, 0, LLMETA_IXLEN, 0, parent_bdb_handle, NULL, 0, bdberr);

    BDB_RELLOCK();

    if (llmeta_bdb_state)
        return 0;
    else
        return -1;
}

/* change the list of tables that are stored in the low level meta table
 * returns <0 on failure or 0 on success */
int bdb_llmeta_set_tables(
    tran_type *input_trans, /* if this is !NULL it will be used as the
                             * transaction for all actions, if it is NULL a
                             * new transaction will be
                             * created internally */
    char **tblnames,        /* the table names to add */
    const int *dbnums,      /* the table's dbnums to add (or 0 if the table
                             * does not have a dbnum */
    int numdbs,             /* number of tables passed in */
    int *bdberr)
{
    int buflen = numdbs * (LLMETA_TBLLEN + sizeof(int)), offset = 0, i,
        retries = 0, rc, prev_bdberr, tmpkey;
    char key[LLMETA_IXLEN] = {0};
    uint8_t *p_buf = NULL, *p_buf_start, *p_buf_end;
    tran_type *trans;
    const uint8_t *p_table_names_key;
    const uint8_t *p_table_names_key_end;

    /*fail if the db isn't open*/
    if (!llmeta_bdb_state) {
        logmsg(LOGMSG_ERROR, "%s: low level meta table not yet "
                        "open, you must run bdb_llmeta_open\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (!tblnames || !dbnums || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: low level meta table db not "
                        "lite\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* construct the key */
    tmpkey = LLMETA_TBL_NAMES;
    p_table_names_key = (uint8_t *)key;
    p_table_names_key_end = p_table_names_key + sizeof(int);

    if (!(p_table_names_key =
              buf_put(&tmpkey, sizeof(tmpkey), (uint8_t *)p_table_names_key,
                      p_table_names_key_end))) {
        logmsg(LOGMSG_ERROR, "%s: error converting to correct "
                        "endianess\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (buflen && (p_buf = malloc(buflen)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed to malloc %u bytes\n", __func__, buflen);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    p_buf_start = p_buf;
    p_buf_end = (p_buf_start + buflen);

    /* construct the data */
    for (i = 0; i < numdbs && offset < buflen; ++i) {
        struct llmeta_table_name llmeta_tbl;

        strncpy0(llmeta_tbl.table_name, tblnames[i],
                 sizeof(llmeta_tbl.table_name));
        llmeta_tbl.table_name_len = strlen(llmeta_tbl.table_name) + 1;
        llmeta_tbl.dbnum = dbnums[i];

        if (!(p_buf = llmeta_table_name_put(&llmeta_tbl, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: tablename: %s longer then "
                            "the max: %d\n",
                    __func__, tblnames[i], LLMETA_TBLLEN);
            *bdberr = BDBERR_BADARGS;
            free(p_buf_start);
            p_buf = p_buf_start = p_buf_end = NULL;
            return -1;
        }
    }

    offset = (p_buf - p_buf_start);

    if (i < numdbs || offset > buflen) {
        logmsg(LOGMSG_ERROR, "%s: buffer was not long enough, "
                        "this should not happen",
                __func__);
        *bdberr = BDBERR_MISC;
        goto cleanup;
    }

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        goto cleanup;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get "
                            "transaction\n",
                    __func__);
            goto cleanup;
        }
    } else
        trans = input_trans;

    /* delete old entry */
    rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_DEL_DTA)
        goto backout;

    /* add new entry */
    rc =
        bdb_lite_add(llmeta_bdb_state, trans, p_buf_start, offset, key, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR)
        goto backout;

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            goto cleanup;
    }

    *bdberr = BDBERR_NOERROR;
    free(p_buf_start);
    p_buf = NULL;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with "
                            "bdberr %d\n",
                    __func__, *bdberr);
            free(p_buf_start);
            p_buf = NULL;
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }
cleanup:
    free(p_buf_start);
    p_buf = NULL;
    return -1;
}

/* gets all the tables' names and dbnumbers  from the low level meta table.
 * returns <0 on failure or 0 on success */
int bdb_llmeta_get_tables(
    tran_type *input_trans,
    char **tblnames,   /* will be populated with the table's names */
    int *dbnums,       /* will be populated with the table's dbnums (or 0
                        * if a table doesn't have a dbnum) */
    size_t maxnumtbls, /* size of tblnames and dbnums */
    int *fndnumtbls,   /* will be populated with the number of tables that
                        * were returned */
    int *bdberr)
{
    int rc, fndlen, retries = 0, offset = 0, tmpkey;
    char key[LLMETA_IXLEN] = {0};
    uint8_t *p_outbuf, *p_outbuf_start, *p_outbuf_end;
    size_t outbuflen = maxnumtbls * (LLMETA_TBLLEN + sizeof(int));
    const uint8_t *p_table_names_key;
    const uint8_t *p_table_names_key_end;

    /*stop here if the db isn't open*/
    if (!llmeta_bdb_state) {
        *bdberr = BDBERR_DBEMPTY;
        return -1;
    }

    if (!tblnames || !dbnums || !fndnumtbls || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: low level meta table db not "
                        "lite\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /*set the key*/
    tmpkey = LLMETA_TBL_NAMES;
    p_table_names_key = (uint8_t *)key;
    p_table_names_key_end = p_table_names_key + sizeof(int);

    if (!(p_table_names_key =
              buf_put(&tmpkey, sizeof(tmpkey), (uint8_t *)p_table_names_key,
                      p_table_names_key_end))) {
        logmsg(LOGMSG_ERROR, "%s: error converting to correct "
                        "endianess\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    /*prepare the out buffer*/
    if (!(p_outbuf = malloc(outbuflen))) {
        logmsg(LOGMSG_ERROR, "%s: failed to malloc %zu bytes\n", __func__,
               outbuflen);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    p_outbuf_start = p_outbuf;

    *fndnumtbls = 0;

retry:
    /* try to fetch the version number */
    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, input_trans, key, p_outbuf,
                                   outbuflen, &fndlen, bdberr);

    /* handle return codes */
    if (rc && *bdberr != BDBERR_NOERROR) {
        if (*bdberr == BDBERR_DEADLOCK && !input_trans) {
            if (++retries < gbl_maxretries) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, 
                    "%s: *ERROR* bdb_lite_exact_fetch too much contention "
                    "%d count %d\n",
                    __func__, *bdberr, retries);
        }

        /*fail on all other errors*/
        free(p_outbuf_start);
        p_outbuf = p_outbuf_start = NULL;
        return -1;
    }

    p_outbuf_end = p_outbuf + fndlen;

    /* the data we get back should be a series of string and int pairs, one for
     * each table (table name, db number or 0 if has none) */
    while (p_outbuf && offset < fndlen) {
        struct llmeta_table_name llmeta_tbl;
        p_outbuf = (uint8_t *)llmeta_table_name_get(&llmeta_tbl, p_outbuf,
                                                    p_outbuf_end);

        if (p_outbuf) {
            *tblnames = strdup(llmeta_tbl.table_name);
            *dbnums = llmeta_tbl.dbnum;
            ++tblnames;
            ++dbnums;
            ++*fndnumtbls;
            offset = p_outbuf - p_outbuf_start;
        }
    }

    if (offset != fndlen)
        logmsg(LOGMSG_ERROR, "%s: returned data did not match "
                        "length exactly, this should not happen\n",
                __func__);

    *bdberr = BDBERR_NOERROR;
    free(p_outbuf_start);
    p_outbuf = NULL;
    return 0;
}

/* updates the version number for a specified file
 * returns <0 if something fails or 0 on success
 * generally this is not called directly, usually bdb_new_file_index or data are
 * called instead */
static int bdb_new_file_version(
    tran_type *input_trans,             /* if this is !NULL it will be used as
                                         * the transaction for all actions, if
                                         * it is NULL a new transaction will be
                                         * created internally */
    const char *db_name, int file_type, /* see FILE_VERSIONS_FILE_TYPE_* */
    int file_num,                       /* ixnum or dtanum */
    unsigned long long inversion_num, int *bdberr)
{
    int retries = 0, rc;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_version_number_type version_num;
    tran_type *trans;
    unsigned long long real_version_num;
    struct llmeta_file_type_dbname_file_num_key file_type_dbname_file_num_key;
    uint8_t *p_buf, *p_buf_end, *p_key_buf, *p_key_buf_end;

    /*fail if the db isn't open*/
    if (!llmeta_bdb_state) {
        logmsg(LOGMSG_ERROR, "%s: low level meta table not yet "
                        "open, you must run bdb_llmeta_open\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (!db_name || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* make sure we have a proper file type */
    switch (file_type) {
    case LLMETA_FVER_FILE_TYPE_TBL:
    case LLMETA_FVER_FILE_TYPE_DTA:
    case LLMETA_FVER_FILE_TYPE_IX:
    case LLMETA_FVER_FILE_TYPE_QDB:
        break;

    default:
        logmsg(LOGMSG_ERROR, "%s: unrecognized file type\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /*add the file_type (ie dta, ix) */
    file_type_dbname_file_num_key.file_type = file_type;

    /* copy the db_name and check its length so that it fit with enough room
     * left for the rest of the key */
    strncpy0(file_type_dbname_file_num_key.dbname, db_name,
             sizeof(file_type_dbname_file_num_key.dbname));
    file_type_dbname_file_num_key.dbname_len =
        strlen(file_type_dbname_file_num_key.dbname) + 1;

    if (file_type_dbname_file_num_key.dbname_len > LLMETA_TBLLEN) {
        logmsg(LOGMSG_ERROR, "%s: db_name is too long\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }
    /* add the file_num (ie ixnum dtanum) */
    file_type_dbname_file_num_key.file_num = file_num;

    p_key_buf = (uint8_t *)key;
    p_key_buf_end = (uint8_t *)(key + LLMETA_IXLEN);

    if (!(llmeta_file_type_dbname_file_num_put(&file_type_dbname_file_num_key,
                                               p_key_buf, p_key_buf_end))) {
        logmsg(LOGMSG_ERROR, 
                "%s: llmeta_file_type_dbname_file_num_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    version_num.version_num = inversion_num;

    p_buf = (uint8_t *)&real_version_num;
    p_buf_end = p_buf + sizeof(real_version_num);

    if (!(llmeta_version_number_put(&(version_num), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_version_number_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:

    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_INFO, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get transaction\n", __func__);
            return -1;
        }
    } else
        trans = input_trans;

    /* delete old entry */
    rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_DEL_DTA)
        goto backout;

    /* add new entry */
    rc = bdb_lite_add(llmeta_bdb_state, trans, &real_version_num,
                      sizeof(real_version_num), key, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR)
        goto backout;

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        int prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with "
                            "bdberr %d\n",
                    __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }
    return -1;
}

/* deletes all the file versions associated with a specific table name and file
 * type.
 * returns <0 if something fails or 0 on success */
static int
bdb_chg_file_versions_int(tran_type *trans, /* must be !NULL */
                          const char *tbl_name, const char *new_tbl_name,
                          int file_type, /* see FILE_VERSIONS_FILE_TYPE_* */
                          int *bdberr)
{
    int numfnd = 0, rc;
    char key[LLMETA_IXLEN] = {0};
    char new_key[LLMETA_IXLEN] = {0};
    char key_orig[LLMETA_IXLEN] = {0};
    char key_fnd[LLMETA_IXLEN] = {0};
    size_t key_offset = 0;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;
    struct llmeta_file_type_dbname_key skey;
    struct llmeta_file_type_dbname_key new_skey;

    if (!tbl_name || !bdberr || !trans) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* make sure we have a proper file type */
    switch (file_type) {
    case LLMETA_FVER_FILE_TYPE_TBL:
    case LLMETA_FVER_FILE_TYPE_DTA:
    case LLMETA_FVER_FILE_TYPE_IX:
        break;

    default:
        logmsg(LOGMSG_ERROR, "%s: unrecognized file type\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    new_skey.file_type = skey.file_type = file_type;

    /* copy the db_name and check its length so that it fit with enough room
     * left for the rest of the key */
    strncpy0(skey.dbname, tbl_name, sizeof(skey.dbname));
    skey.dbname_len = strlen(skey.dbname) + 1;

    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = (uint8_t *)key + LLMETA_IXLEN;

    p_buf = llmeta_file_type_dbname_key_put(&skey, p_buf, p_buf_end);
    if (!p_buf) {
        logmsg(LOGMSG_ERROR, 
                "%s: llmeta_file_type_dbname_file_num_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    key_offset = p_buf - p_buf_start;

    /* save the origional key */
    memcpy(key_orig, key, key_offset);

    if (new_tbl_name) {
        strncpy0(new_skey.dbname, new_tbl_name, sizeof(new_skey.dbname));
        new_skey.dbname_len = strlen(new_skey.dbname) + 1;

        p_buf_start = p_buf = (uint8_t *)new_key;
        p_buf_end = (uint8_t *)new_key + LLMETA_IXLEN;

        p_buf = llmeta_file_type_dbname_key_put(&new_skey, p_buf, p_buf_end);
        if (!p_buf) {
            logmsg(LOGMSG_ERROR,
                   "%s: llmeta_file_type_dbname_file_num_put returns NULL\n",
                   __func__);
            *bdberr = BDBERR_BADARGS;
            return -1;
        }

        char fnddta[sizeof(unsigned long long)];
        int fndlen = 0;
        rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, trans, key, fnddta,
                                       sizeof(fnddta), &fndlen, bdberr);
        if (!rc) {
            rc = bdb_lite_add(llmeta_bdb_state, trans, fnddta, fndlen, new_key,
                              bdberr);
            if (rc && *bdberr != BDBERR_NOERROR)
                return -1;
        }
    }

    /* delete anything matching this key exactly */
    rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_DEL_DTA)
        return -1;

    /* delete anything with the same begining as this key */
    while (1) {
        rc = bdb_lite_fetch_keys_fwd_tran(llmeta_bdb_state, trans, key, key_fnd,
                                          1 /*maxfnd*/, &numfnd, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;

        /* if we didn't find any for this table or we went past the end of it's
         * records we're done */
        if (!numfnd || memcmp(key_orig, key_fnd, key_offset)) {
            *bdberr = BDBERR_NOERROR;
            return 0;
        }

        /* delete old entry */
        rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key_fnd, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;

        /* get the next key next time */
        memcpy(key, key_fnd, sizeof(key));
    }
}

/* updates/deletes all the file versions associated with a specific table name
 * if "new_tbl_name" is not null, the entries are actually updated
 * returns <0 if something fails or 0 on success */
int bdb_chg_file_versions(
    bdb_state_type *bdb_state,
    tran_type *input_trans, /* if null, an internal tran is used */
    const char *new_tbl_name, int *bdberr)
{
    int retries = 0, rc;
    tran_type *trans;

    /*fail if the db isn't open*/
    if (!llmeta_bdb_state) {
        logmsg(LOGMSG_ERROR, "%s: low level meta table not yet open, you must run "
                        "bdb_llmeta_open\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (!bdb_state || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get transaction\n", __func__);
            return -1;
        }
    } else
        trans = input_trans;

    /* delete table version */
    rc = bdb_chg_file_versions_int(trans, bdb_state->name, new_tbl_name,
                                   LLMETA_FVER_FILE_TYPE_TBL, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_FETCH_IX)
        goto backout;

    /* delete data versions */
    rc = bdb_chg_file_versions_int(trans, bdb_state->name, new_tbl_name,
                                   LLMETA_FVER_FILE_TYPE_DTA, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_FETCH_IX)
        goto backout;

    /* delete index versions */
    rc = bdb_chg_file_versions_int(trans, bdb_state->name, new_tbl_name,
                                   LLMETA_FVER_FILE_TYPE_IX, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_FETCH_IX)
        goto backout;

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        int prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with bdberr %d\n", __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }
    return -1;
}

/* deletes all the file versions associated with a specific table name
 * returns <0 if something fails or 0 on success */
int bdb_del_file_versions(
    bdb_state_type *bdb_state,
    tran_type *input_trans, /* if this is !NULL it will be used as
                             * the transaction for all actions, if
                             * it is NULL a new transaction will be
                             * created internally */
    int *bdberr)
{
    return bdb_chg_file_versions(bdb_state, input_trans, NULL, bdberr);
}

static int bdb_del_file_version(
    bdb_state_type *bdb_state,
    tran_type *input_trans,    /* if this is !NULL it will be used as
                                * the transaction for all actions, if
                                * it is NULL a new transaction will be
                                * created internally */
    int file_type,             /* see FILE_VERSIONS_FILE_TYPE_* */
    int file_num,              /* ixnum or dtanum */
    int *bdberr)
{
    int retries = 0, rc;
    char key[LLMETA_IXLEN] = {0};
    tran_type *trans;
    struct llmeta_file_type_dbname_file_num_key file_type_dbname_file_num_key;
    uint8_t *p_key_buf, *p_key_buf_end;

    /*fail if the db isn't open*/
    if (!llmeta_bdb_state) {
        logmsg(LOGMSG_ERROR, "%s: low level meta table not yet "
                        "open, you must run bdb_llmeta_open\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (!bdb_state || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* make sure we have a proper file type */
    switch (file_type) {
    case LLMETA_FVER_FILE_TYPE_TBL:
    case LLMETA_FVER_FILE_TYPE_DTA:
    case LLMETA_FVER_FILE_TYPE_IX:
    case LLMETA_FVER_FILE_TYPE_QDB:
        break;

    default:
        logmsg(LOGMSG_ERROR, "%s: unrecognized file type\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /*add the file_type (ie dta, ix) */
    file_type_dbname_file_num_key.file_type = file_type;

    /* copy the db_name and check its length so that it fit with enough room
     * left for the rest of the key */
    strncpy0(file_type_dbname_file_num_key.dbname, bdb_state->name,
             sizeof(file_type_dbname_file_num_key.dbname));
    file_type_dbname_file_num_key.dbname_len =
        strlen(file_type_dbname_file_num_key.dbname) + 1;

    if (file_type_dbname_file_num_key.dbname_len > LLMETA_TBLLEN) {
        logmsg(LOGMSG_ERROR, "%s: db_name is too long\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }
    /* add the file_num (ie ixnum dtanum) */
    file_type_dbname_file_num_key.file_num = file_num;

    p_key_buf = (uint8_t *)key;
    p_key_buf_end = (uint8_t *)(key + LLMETA_IXLEN);

    if (!(llmeta_file_type_dbname_file_num_put(&file_type_dbname_file_num_key,
                                               p_key_buf, p_key_buf_end))) {
        logmsg(LOGMSG_ERROR, 
                "%s: llmeta_file_type_dbname_file_num_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:

    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_INFO, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get transaction\n", __func__);
            return -1;
        }
    } else
        trans = input_trans;

    /* delete old entry */
    rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_DEL_DTA)
        goto backout;

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        int prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR,
                   "%s: trans abort failed with "
                   "bdberr %d\n",
                   __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }
    return -1;
}

int bdb_del_file_version_qdb(
    bdb_state_type *bdb_state,
    tran_type *input_trans,    /* if this is !NULL it will be used as
                                * the transaction for all actions, if
                                * it is NULL a new transaction will be
                                * created internally */
    int file_num,              /* ixnum or dtanum */
    int *bdberr)
{
    return bdb_del_file_version(bdb_state, input_trans,
                                LLMETA_FVER_FILE_TYPE_QDB, file_num, bdberr);
}

static int
bdb_set_pagesize(tran_type *input_trans, /* if this is !NULL it will be used as
                                          * the transaction for all actions, if
                                          * it is NULL a new transaction will be
                                          * created internally */
                 const char *db_name,
                 int file_type, /* see FILE_VERSIONS_FILE_TYPE_* */
                 int pagesize, int *bdberr)
{
    int retries = 0, rc;
    char key[LLMETA_IXLEN] = {0};
    size_t key_offset = 0;
    tran_type *trans;
    struct llmeta_file_type_dbname_key file_type_dbname_key;
    struct llmeta_file_type_key file_type_key;
    int tmppagesz;
    uint8_t *p_buf, *p_buf_start, *p_buf_end, *p_pgsz_buf, *p_pgsz_buf_end;

    if (db_name && strncasecmp(db_name, "new.", 4) == 0)
        db_name += 4;

    /*fail if the db isn't open*/
    if (!llmeta_bdb_state) {
        logmsg(LOGMSG_ERROR, "%s: low level meta table not yet "
                        "open, you must run bdb_llmeta_open\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (!bdberr) {
        logmsg(LOGMSG_ERROR, "bdb_new_pagesize: NULL argument\n");
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (!db_name) {
        switch (file_type) {
        case LLMETA_PAGESIZE_FILE_TYPE_ALLDTA:
        case LLMETA_PAGESIZE_FILE_TYPE_ALLBLOB:
        case LLMETA_PAGESIZE_FILE_TYPE_ALLIX:
            break;
        default:
            *bdberr = BDBERR_BADARGS;
            return -1;
        }
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* make sure we have a proper file type */
    switch (file_type) {
    case LLMETA_PAGESIZE_FILE_TYPE_BLOB:
    case LLMETA_PAGESIZE_FILE_TYPE_DTA:
    case LLMETA_PAGESIZE_FILE_TYPE_IX:
    case LLMETA_PAGESIZE_FILE_TYPE_ALLDTA:
    case LLMETA_PAGESIZE_FILE_TYPE_ALLBLOB:
    case LLMETA_PAGESIZE_FILE_TYPE_ALLIX:
        break;

    default:
        logmsg(LOGMSG_ERROR, "%s: unrecognized file type\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /*add the file_type (ie dta, ix) */
    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    /* handle per table records */
    switch (file_type) {
    case LLMETA_PAGESIZE_FILE_TYPE_BLOB:
    case LLMETA_PAGESIZE_FILE_TYPE_DTA:
    case LLMETA_PAGESIZE_FILE_TYPE_IX:
        /* copy the db_name and check its length so that it fit
           with enough room left for the rest of the key */
        file_type_dbname_key.file_type = file_type;
        strncpy0(file_type_dbname_key.dbname, db_name,
                 sizeof(file_type_dbname_key.dbname));
        file_type_dbname_key.dbname_len =
            strlen(file_type_dbname_key.dbname) + 1;

        p_buf = llmeta_file_type_dbname_key_put(&(file_type_dbname_key), p_buf,
                                                p_buf_end);
        break;

    default:
        file_type_key.file_type = file_type;
        p_buf = llmeta_file_type_key_put(&(file_type_key), p_buf, p_buf_end);

        break;
    }

    key_offset = p_buf - p_buf_start;

    if (key_offset > sizeof(key)) {
        logmsg(LOGMSG_ERROR, "%s: db_name is too long\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    p_pgsz_buf = (uint8_t *)&tmppagesz;
    p_pgsz_buf_end = (p_pgsz_buf + sizeof(tmppagesz));

    p_pgsz_buf =
        buf_put(&pagesize, sizeof(pagesize), p_pgsz_buf, p_pgsz_buf_end);

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get transaction\n", __func__);
            return -1;
        }
    } else
        trans = input_trans;

    /* delete old entry */
    rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_DEL_DTA)
        goto backout;

    /* add new entry */
    rc = bdb_lite_add(llmeta_bdb_state, trans, &tmppagesz, sizeof(int), key,
                      bdberr);
    if (rc && *bdberr != BDBERR_NOERROR)
        goto backout;

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        int prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with bdberr "
                            "%d\n",
                    __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }
    return -1;
}

/* calls bdb_new_file_version for a datafile */
int bdb_new_file_version_data(bdb_state_type *bdb_state, tran_type *tran,
                              int dtanum, unsigned long long version_num,
                              int *bdberr)
{
    return bdb_new_file_version(tran, bdb_state->name,
                                LLMETA_FVER_FILE_TYPE_DTA, dtanum, version_num,
                                bdberr);
}

/* calls bdb_new_file_version for an indexfile */
int bdb_new_file_version_index(bdb_state_type *bdb_state, tran_type *tran,
                               int ixnum, unsigned long long version_num,
                               int *bdberr)
{
    return bdb_new_file_version(tran, bdb_state->name, LLMETA_FVER_FILE_TYPE_IX,
                                ixnum, version_num, bdberr);
}

int bdb_set_pagesize_data(bdb_state_type *bdb_state, tran_type *tran,
                          int pagesize, int *bdberr)
{
    return bdb_set_pagesize(tran, bdb_state->name,
                            LLMETA_PAGESIZE_FILE_TYPE_DTA, pagesize, bdberr);
}
int bdb_set_pagesize_blob(bdb_state_type *bdb_state, tran_type *tran,
                          int pagesize, int *bdberr)
{
    return bdb_set_pagesize(tran, bdb_state->name,
                            LLMETA_PAGESIZE_FILE_TYPE_BLOB, pagesize, bdberr);
}
int bdb_set_pagesize_index(bdb_state_type *bdb_state, tran_type *tran,
                           int pagesize, int *bdberr)
{
    return bdb_set_pagesize(tran, bdb_state->name, LLMETA_PAGESIZE_FILE_TYPE_IX,
                            pagesize, bdberr);
}

int bdb_set_pagesize_alldata(tran_type *tran, int pagesize, int *bdberr)
{
    return bdb_set_pagesize(tran, NULL, LLMETA_PAGESIZE_FILE_TYPE_ALLDTA,
                            pagesize, bdberr);
}
int bdb_set_pagesize_allblob(tran_type *tran, int pagesize, int *bdberr)
{
    return bdb_set_pagesize(tran, NULL, LLMETA_PAGESIZE_FILE_TYPE_ALLBLOB,
                            pagesize, bdberr);
}
int bdb_set_pagesize_allindex(tran_type *tran, int pagesize, int *bdberr)
{
    return bdb_set_pagesize(tran, NULL, LLMETA_PAGESIZE_FILE_TYPE_ALLIX,
                            pagesize, bdberr);
}

/* calls bdb_new_file_version for a table, note this does not increase the
 * versions for any of the table's files only the table itself (this is
 * basically used as a marker to see if the table is using version numbers) */
int bdb_new_file_version_table(bdb_state_type *bdb_state, tran_type *tran,
                               unsigned long long version_num, int *bdberr)
{
    return bdb_new_file_version(tran, bdb_state->name,
                                LLMETA_FVER_FILE_TYPE_TBL, 0 /*file_num*/,
                                version_num, bdberr);
}

int bdb_new_file_version_qdb(bdb_state_type *bdb_state, tran_type *tran,
                             int file_num, unsigned long long version_num,
                             int *bdberr)
{
    return bdb_new_file_version(tran, bdb_state->name,
                                LLMETA_FVER_FILE_TYPE_QDB, file_num,
                                version_num, bdberr);
}

/* update all of the db's file's version numbers, usually called when first
 * creating a table */
int bdb_new_file_version_all(bdb_state_type *bdb_state, tran_type *input_tran,
                             int *bdberr)
{
    int dtanum, ixnum, retries = 0;
    unsigned long long version_num;
    tran_type *tran;

    /*stop here if the db isn't open*/
    if (!llmeta_bdb_state) {
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (!bdb_state || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_tran) {
        tran = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!tran) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get "
                            "transaction\n",
                    __func__);
            return -1;
        }
    } else
        tran = input_tran;

    /* set table version num, this is used to test if the table is using file
     * versions */
    version_num = bdb_get_cmp_context(bdb_state);
    if (bdb_new_file_version(tran, bdb_state->name, LLMETA_FVER_FILE_TYPE_TBL,
                             0 /*file_num*/, version_num, bdberr) ||
        *bdberr != BDBERR_NOERROR)
        goto backout;

    /*update all the data file's versions*/
    for (dtanum = 0; dtanum < bdb_state->numdtafiles; dtanum++) {
        version_num = bdb_get_cmp_context(bdb_state);

        if (bdb_new_file_version_data(bdb_state, tran, dtanum, version_num,
                                      bdberr) ||
            *bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: failed to update "
                            "version number\n",
                    __func__);
            goto backout;
        }
    }

    /*update all the index file's versions*/
    for (ixnum = 0; ixnum < bdb_state->numix; ixnum++) {
        version_num = bdb_get_cmp_context(bdb_state);
        if (bdb_new_file_version_index(bdb_state, tran, ixnum, version_num,
                                       bdberr) ||
            *bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: failed to update "
                            "version number\n",
                    __func__);
            goto backout;
        }
    }

    /*commit if we created our own transaction*/
    if (!input_tran) {
        if (bdb_tran_commit(llmeta_bdb_state, tran, bdberr) &&
            *bdberr != BDBERR_NOERROR)
            return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_tran) {
        int prev_bdberr = *bdberr;

        /*kill the transaction*/
        if (bdb_tran_abort(llmeta_bdb_state, tran, bdberr) && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with "
                            "bdberr %d\n",
                    __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }
    }
    logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    return -1;
}

/* looks up the version of a file in the file version db
 * returns <0 if there was an error, 0 the lookup was successful or 1 if the key
 * wasn't found in the llmeta db */
static int bdb_get_file_version(
    tran_type *tran,                    /* transaction to use */
    const char *db_name, int file_type, /* see FILE_VERSIONS_FILE_TYPE_* */
    int file_num,                       /* ixnum or dtanum */
    unsigned long long *p_version_num,  /* output: the version number */
    int *bdberr)
{
    bdb_state_type *parent; /* the low level meta table */
    int rc, fndlen, retries = 0;
    char key[LLMETA_IXLEN] = {0};
    unsigned long long tmpversion;
    struct llmeta_version_number_type version_num;
    struct llmeta_file_type_dbname_file_num_key file_type_dbname_file_num_key;
    uint8_t *p_buf, *p_buf_end;

    /*set to 0 incase we return early*/
    *p_version_num = 0ULL;

    /*stop here if the db isn't open*/
    if (!llmeta_bdb_state) {
        *bdberr = BDBERR_DBEMPTY;
        return -1;
    }

    if (!db_name || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* make sure we have a proper file type */
    switch (file_type) {
    case LLMETA_FVER_FILE_TYPE_TBL:
    case LLMETA_FVER_FILE_TYPE_DTA:
    case LLMETA_FVER_FILE_TYPE_IX:
    case LLMETA_FVER_FILE_TYPE_QDB:
        break;

    default:
        logmsg(LOGMSG_ERROR, "%s: unrecognized file type\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /*add the file_type (ie dta, ix) */
    file_type_dbname_file_num_key.file_type = file_type;

    strncpy0(file_type_dbname_file_num_key.dbname, db_name,
             sizeof(file_type_dbname_file_num_key.dbname));
    file_type_dbname_file_num_key.dbname_len =
        strlen(file_type_dbname_file_num_key.dbname) + 1;

    /* copy the db_name and check its length so that it fit with enough room
     * left for the rest of the key */
    file_type_dbname_file_num_key.file_num = file_num;

    p_buf = (uint8_t *)key;
    p_buf_end = (p_buf + LLMETA_IXLEN);

    if (!(p_buf = llmeta_file_type_dbname_file_num_put(
              &file_type_dbname_file_num_key, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, 
                "%s: llmeta_file_type_dbname_file_num_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    /* try to fetch the version number */
    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, tran, key, &tmpversion,
                                   sizeof(tmpversion), &fndlen, bdberr);

    /* handle return codes */
    if (rc || *bdberr != BDBERR_NOERROR) {
        if (*bdberr == BDBERR_DEADLOCK && !tran) {
            if (++retries < gbl_maxretries) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, 
                    "%s: *ERROR* bdb_lite_exact_fetch too much contention "
                    "%d count %d\n",
                    __func__, *bdberr, retries);
        }

        /* it might be ok if no data was found,
         * return 1 and *p_version_num = 0ULL */
        if (*bdberr == BDBERR_FETCH_DTA) {
            return 1;
        }

        /*fail on all other errors*/
        return -1;
    }
    /*if we did not get the right amount of data*/
    else if (fndlen != sizeof(tmpversion)) {
        *bdberr = BDBERR_DTA_MISMATCH; /* TODO right error to throw? */
        return -1;
    }

    p_buf = (uint8_t *)&tmpversion;
    p_buf_end = p_buf + sizeof(tmpversion);

    if (!(llmeta_version_number_get(&(version_num), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s:llmeta_version_number_get returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    *p_version_num = version_num.version_num;

    parent = llmeta_bdb_state->parent;
    assert(parent);
    if (bdb_cmp_genids(version_num.version_num, get_gblcontext(parent)) > 0) {
        set_gblcontext(parent, version_num.version_num);
    }

    *bdberr = BDBERR_NOERROR;
    return 0;
}

int bdb_get_file_version_data(bdb_state_type *bdb_state, tran_type *tran,
                              int dtanum, unsigned long long *version_num,
                              int *bdberr)
{
    return bdb_get_file_version(tran, bdb_state->name,
                                LLMETA_FVER_FILE_TYPE_DTA, dtanum, version_num,
                                bdberr);
}

int bdb_get_file_version_index(bdb_state_type *bdb_state, tran_type *tran,
                               int ixnum, unsigned long long *version_num,
                               int *bdberr)
{
    return bdb_get_file_version(tran, (bdb_state)->name,
                                LLMETA_FVER_FILE_TYPE_IX, ixnum, version_num,
                                bdberr);
}

int bdb_get_file_version_table(bdb_state_type *bdb_state, tran_type *tran,
                               unsigned long long *version_num, int *bdberr)
{
    return bdb_get_file_version(tran, (bdb_state)->name,
                                LLMETA_FVER_FILE_TYPE_TBL, 0, version_num,
                                bdberr);
}

int bdb_get_file_version_qdb(bdb_state_type *bdb_state, tran_type *tran,
                             int file_num, unsigned long long *version_num,
                             int *bdberr)
{
    return bdb_get_file_version(tran, bdb_state->name,
                                LLMETA_FVER_FILE_TYPE_QDB, file_num,
                                version_num, bdberr);
}

int bdb_get_file_version_data_by_name(tran_type *tran, const char *name,
                                      int file_num,
                                      unsigned long long *version_num,
                                      int *bdberr)
{
    return bdb_get_file_version(tran, name, LLMETA_FVER_FILE_TYPE_DTA, file_num,
                                version_num, bdberr);
}

int bdb_get_file_version_index_by_name(tran_type *tran, const char *name,
                                       int file_num,
                                       unsigned long long *version_num,
                                       int *bdberr)
{
    return bdb_get_file_version(tran, name, LLMETA_FVER_FILE_TYPE_IX, file_num,
                                version_num, bdberr);
}

static int bdb_get_pagesize(tran_type *tran, /* transaction to use */
                            const char *db_name,
                            int file_type, /* see FILE_VERSIONS_FILE_TYPE_* */
                            int *pagesize, int *bdberr)
{
    int rc, fndlen, retries = 0;
    char key[LLMETA_IXLEN] = {0};
    size_t key_offset = 0;
    struct llmeta_file_type_dbname_key file_type_dbname_key;
    struct llmeta_file_type_key file_type_key;
    struct llmeta_page_size_type p_page_size;
    uint8_t *p_buf, *p_buf_start, *p_buf_end, *p_page_buf, *p_page_buf_end;
    int pgsize;

    if (db_name && strncasecmp(db_name, "new.", 4) == 0)
        db_name += 4;

    /*set to 0 incase we return early*/
    *pagesize = 0;

    /*stop here if the db isn't open*/
    if (!llmeta_bdb_state) {
        *bdberr = BDBERR_DBEMPTY;
        return -1;
    }

    if (!bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (!db_name) {
        switch (file_type) {
        case LLMETA_PAGESIZE_FILE_TYPE_ALLDTA:
        case LLMETA_PAGESIZE_FILE_TYPE_ALLBLOB:
        case LLMETA_PAGESIZE_FILE_TYPE_ALLIX:
            break;
        default:
            *bdberr = BDBERR_BADARGS;
            return -1;
        }
    }

    /*
    fprintf(stderr, "calling bdb_get_pagesize: %d %s %d\n", file_type, db_name,
       *pagesize);
       */

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* make sure we have a proper file type */
    switch (file_type) {
    case LLMETA_PAGESIZE_FILE_TYPE_DTA:
    case LLMETA_PAGESIZE_FILE_TYPE_BLOB:
    case LLMETA_PAGESIZE_FILE_TYPE_IX:
    case LLMETA_PAGESIZE_FILE_TYPE_ALLDTA:
    case LLMETA_PAGESIZE_FILE_TYPE_ALLBLOB:
    case LLMETA_PAGESIZE_FILE_TYPE_ALLIX:
        break;

    default:
        logmsg(LOGMSG_ERROR, "%s: unrecognized file type\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /*add the file_type (ie dta, ix) */
    /*memcpy( key, &file_type, sizeof( file_type ) );*/
    /*key_offset += sizeof( file_type );*/

    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    /* handle per table records */
    switch (file_type) {
    case LLMETA_PAGESIZE_FILE_TYPE_BLOB:
    case LLMETA_PAGESIZE_FILE_TYPE_DTA:
    case LLMETA_PAGESIZE_FILE_TYPE_IX:
        /* copy the db_name and check its length so that it fit
           with enough room left for the rest of the key */
        file_type_dbname_key.file_type = file_type;
        strncpy0(file_type_dbname_key.dbname, db_name,
                 sizeof(file_type_dbname_key.dbname));
        file_type_dbname_key.dbname_len =
            strlen(file_type_dbname_key.dbname) + 1;

        if (!(p_buf = llmeta_file_type_dbname_key_put(&file_type_dbname_key,
                                                      p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: dbname is too long\n", __func__);
            *bdberr = BDBERR_BADARGS;
            return -1;
        }
        break;
    default:
        file_type_key.file_type = file_type;

        if (!(p_buf =
                  llmeta_file_type_key_put(&file_type_key, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
                    __func__);
            *bdberr = BDBERR_BADARGS;
            return -1;
        }
        break;
    }

    key_offset = p_buf - p_buf_start;

    if (key_offset > sizeof(key)) {
        logmsg(LOGMSG_ERROR, "%s: key_offset error\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    p_page_buf = (uint8_t *)&pgsize;
    p_page_buf_end = p_page_buf + sizeof(pgsize);

retry:

    /* try to fetch the version number */
    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, tran, key, &pgsize,
                                   sizeof(int), &fndlen, bdberr);

    /* handle return codes */
    if (rc || *bdberr != BDBERR_NOERROR) {
        if (*bdberr == BDBERR_DEADLOCK && !tran) {
            if (++retries < gbl_maxretries) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, 
                    "%s:*ERROR* bdb_lite_exact_fetch too much contention "
                    "%d count %d\n",
                    __func__, *bdberr, retries);
        }

        /*fail on all other errors*/
        return -1;
    }
    /*if we did not get the right amount of data*/
    else if (fndlen != sizeof(int)) {
        *bdberr = BDBERR_DTA_MISMATCH; /* TODO right error to throw? */
        return -1;
    }

    if (!(llmeta_page_size_get(&p_page_size, p_page_buf, p_page_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s returns NULL?\n", __func__);
        *bdberr = BDBERR_DTA_MISMATCH;
        return -1;
    }
    *pagesize = p_page_size.page_size;

    *bdberr = BDBERR_NOERROR;

    return 0;
}

int bdb_get_pagesize_data(bdb_state_type *bdb_state, tran_type *tran,
                          int *pagesize, int *bdberr)
{
    return bdb_get_pagesize(tran, bdb_state->name,
                            LLMETA_PAGESIZE_FILE_TYPE_DTA, pagesize, bdberr);
}
int bdb_get_pagesize_blob(bdb_state_type *bdb_state, tran_type *tran,
                          int *pagesize, int *bdberr)
{
    return bdb_get_pagesize(tran, bdb_state->name,
                            LLMETA_PAGESIZE_FILE_TYPE_BLOB, pagesize, bdberr);
}
int bdb_get_pagesize_index(bdb_state_type *bdb_state, tran_type *tran,
                           int *pagesize, int *bdberr)
{
    return bdb_get_pagesize(tran, bdb_state->name, LLMETA_PAGESIZE_FILE_TYPE_IX,
                            pagesize, bdberr);
}

int bdb_get_pagesize_alldata(tran_type *tran, int *pagesize, int *bdberr)
{
    return bdb_get_pagesize(tran, NULL, LLMETA_PAGESIZE_FILE_TYPE_ALLDTA,
                            pagesize, bdberr);
}
int bdb_get_pagesize_allblob(tran_type *tran, int *pagesize, int *bdberr)
{
    return bdb_get_pagesize(tran, NULL, LLMETA_PAGESIZE_FILE_TYPE_ALLBLOB,
                            pagesize, bdberr);
}
int bdb_get_pagesize_allindex(tran_type *tran, int *pagesize, int *bdberr)
{
    return bdb_get_pagesize(tran, NULL, LLMETA_PAGESIZE_FILE_TYPE_ALLIX,
                            pagesize, bdberr);
}

int bdb_add_dummy_llmeta_wait(int wait_for_seqnum)
{
    tran_type *tran;
    int rc;
    int bdberr;
    int retries = gbl_maxretries;
    uint8_t key[LLMETA_IXLEN] = {0};

    if (!bdb_have_llmeta()) {
        logmsg(LOGMSG_ERROR, "%s got add_dummy request while opening backend.\n",
                __func__);
        return -1;
    }

retry:
    if (bdb_lock_desired(llmeta_bdb_state->parent)) {
        logmsg(LOGMSG_ERROR, "%s short-circuiting because bdb_lock_desired\n",
               __func__);
        return -1;
    }
    tran = bdb_tran_begin(llmeta_bdb_state, NULL, &bdberr);
    if (tran == NULL)
        goto fail;

    *(int *)key = htonl(LLMETA_BS);

    rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, &bdberr);
    if (rc && bdberr != BDBERR_NOERROR && bdberr != BDBERR_DEL_DTA) {
        logmsg(LOGMSG_ERROR, "%s bdb_lite_exact_del rc: %d bdberr: %d\n", __func__,
                rc, bdberr);
        goto fail;
    }

    rc = bdb_lite_add(llmeta_bdb_state, tran, NULL, 0, key, &bdberr);
    if (rc && bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "%s bdb_lite_add rc: %d bdberr: %d\n", __func__, rc,
                bdberr);
        goto fail;
    }

    seqnum_type ss;
    rc = bdb_tran_commit_with_seqnum_size(llmeta_bdb_state, tran, &ss, NULL,
                                          &bdberr);

    if (rc == 0 && wait_for_seqnum) {
        int timeoutms;
        rc = bdb_wait_for_seqnum_from_all_adaptive_newcoh(
                llmeta_bdb_state->parent, &ss, 0, &timeoutms);
    }
    // rc = bdb_tran_commit(llmeta_bdb_state, tran, &bdberr);
    if (rc && bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "%s bdb_tran_commit rc: %d bdberr: %d\n", __func__, rc,
                bdberr);
        tran = NULL;
        goto fail;
    }
    return 0;

fail:
    if (bdberr == BDBERR_DEADLOCK) {
        --retries;
    } else {
        retries = 0;
    }
    if (tran) {
        rc = bdb_tran_abort(llmeta_bdb_state, tran, &bdberr);
        if (rc && bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s bdb_tran_abort rc: %d bdberr: %d\n", __func__,
                    rc, bdberr);
            return -1;
        }
    }
    if (retries) {
        int dp = gbl_llmeta_deadlock_poll;
        if (dp > 1)
            poll(NULL, 0, rand() % dp);
        goto retry;
    }
    return -1;
}

int bdb_add_dummy_llmeta(void)
{
    return bdb_add_dummy_llmeta_wait(1);
}

/* store a new csc2 schema in the llmeta table
 * returns <0 if something fails or 0 on success */
int bdb_new_csc2(tran_type *input_trans, /* if this is !NULL it will be used as
                                          * the transaction for all actions, if
                                          * it is NULL a new transaction will be
                                          * created internally */
                 const char *db_name,
                 int csc2_vers, /* version 0 to 255 or -1 to make it's
                                   version one higher then the current
                                   highest */
                 char *schema,  /* text of the schema */
                 int *bdberr)
{
    int retries = 0, rc;
    char key[LLMETA_IXLEN] = {0};
    size_t key_offset = 0;
    struct llmeta_file_type_dbname_csc2_vers_key
        p_file_type_dbname_csc2_vers_key;
    tran_type *trans;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;

    /*fail if the db isn't open*/
    if (!llmeta_bdb_state) {
        logmsg(LOGMSG_ERROR, "%s: low level meta table not yet open,"
                        "you must run bdb_llmeta_open\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (!db_name || !schema || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* make sure version is in range */
    if (csc2_vers < -1 /* TODO || csc2_vers > 255 */) {
        logmsg(LOGMSG_ERROR, "%s: csc2 version out of range\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* add the key type */
    p_file_type_dbname_csc2_vers_key.file_type = LLMETA_CSC2;

    /* copy the db_name and check its length so that it fit with enough room
     * left for the rest of the key */
    strncpy0(p_file_type_dbname_csc2_vers_key.dbname, db_name,
             sizeof(p_file_type_dbname_csc2_vers_key.dbname));
    p_file_type_dbname_csc2_vers_key.dbname_len =
        strlen(p_file_type_dbname_csc2_vers_key.dbname) + 1;

    /* zero this out for now */
    p_file_type_dbname_csc2_vers_key.csc2_vers = 0;

    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    /* trial put */
    if (!(p_buf = llmeta_file_type_dbname_csc2_vers_key_put(
              &(p_file_type_dbname_csc2_vers_key), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, 
                "%s: llmeta_file_type_dbname_csc2_vers_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    key_offset = p_buf - p_buf_start;

    if (key_offset > sizeof(key)) {
        logmsg(LOGMSG_ERROR, "%s: db_name is too long\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

/* csc2_vers added to key below */
retry:

    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get transaction\n", __func__);
            return -1;
        }
    } else
        trans = input_trans;

    /* fetch the previous highest version if we've been asked to */
    if (csc2_vers == -1) {
        rc = bdb_get_csc2_highest(trans, db_name, &csc2_vers, bdberr);
        if (rc || *bdberr != BDBERR_NOERROR)
            goto backout;
        ++csc2_vers; /* move to the next avialable version */
    }

    /* reset p_buf to the beginning */
    p_buf = (uint8_t *)key;

    /* set csc2_vers */
    p_file_type_dbname_csc2_vers_key.csc2_vers = csc2_vers;

    /* TODO(NC): called again only to put csc2_vers. */
    if (!(p_buf = llmeta_file_type_dbname_csc2_vers_key_put(
              &(p_file_type_dbname_csc2_vers_key), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, 
                "%s: llmeta_file_type_dbname_csc2_vers_key_put returns NULL\n",
                __func__);
        goto backout;
    }

    /* delete old entry */
    rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_DEL_DTA)
        goto backout;

    /* add new entry */
    rc = bdb_lite_add(llmeta_bdb_state, trans, schema, strlen(schema) + 1, key,
                      bdberr);
    if (rc && *bdberr != BDBERR_NOERROR)
        goto backout;

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        int prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with bdberr "
                            "%d\n",
                    __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }
    return -1;
}

/* finds the highest schema version for this table name
 * returns <0 if something fails or 0 on success */
int bdb_get_csc2_highest(tran_type *trans, /* transaction to use, may be NULL */
                         const char *db_name,
                         int *csc2_vers, /* will be set to the highest version*/
                         int *bdberr)
{
    int rc, retries = 0, numfnd, max_csc2_vers = INT_MAX; /* TODO 255? */
    char key[LLMETA_IXLEN] = {0}, fndkey[LLMETA_IXLEN] = {0};
    size_t key_offset = 0;
    struct llmeta_file_type_dbname_csc2_vers_key
        p_file_type_dbname_csc2_vers_key;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;

    /*stop here if the db isn't open*/
    if (!llmeta_bdb_state) {
        *bdberr = BDBERR_DBEMPTY;
        return -1;
    }

    if (!db_name || !csc2_vers || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* add the key type */
    p_file_type_dbname_csc2_vers_key.file_type = LLMETA_CSC2;

    /* copy the db_name and check its length so that it fit with enough room
     * left for the rest of the key */
    strncpy0(p_file_type_dbname_csc2_vers_key.dbname, db_name,
             sizeof(p_file_type_dbname_csc2_vers_key.dbname));
    p_file_type_dbname_csc2_vers_key.dbname_len =
        strlen(p_file_type_dbname_csc2_vers_key.dbname) + 1;

    /* set the highest possible version */
    p_file_type_dbname_csc2_vers_key.csc2_vers = max_csc2_vers;

    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf_start + LLMETA_IXLEN;

    if (!(p_buf = llmeta_file_type_dbname_csc2_vers_key_put(
              &(p_file_type_dbname_csc2_vers_key), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, 
                "%s: llmeta_file_type_dbname_csc2_vers_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    key_offset = p_buf - p_buf_start;
    key_offset -= sizeof(*csc2_vers);

    if (key_offset > sizeof(key)) {
        logmsg(LOGMSG_ERROR, "%s: db_name is too long\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    /* try to find the highest version for this table */
    rc = bdb_lite_fetch_keys_bwd_tran(llmeta_bdb_state, trans, key, fndkey,
                                      1 /*maxfnd*/, &numfnd, bdberr);

    /* handle return codes */
    if (rc || *bdberr != BDBERR_NOERROR) {

        if (*bdberr == BDBERR_DEADLOCK) {
            /* need to have caller retry */
            if (trans)
                return -1;

            if (++retries < gbl_maxretries) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: *ERROR* bdb_lite_fetch_keys_bwd too much "
                            "contention %d count %d\n",
                    __func__, *bdberr, retries);
        }

        /* it's ok if no data was found, fail on all other errors*/
        if (*bdberr != BDBERR_FETCH_DTA)
            return -1;
    }

    /* if nothing is found or the header and db_name aren't the same */
    if (!numfnd || *bdberr == BDBERR_FETCH_DTA ||
        memcmp(key, fndkey, key_offset))
        *csc2_vers = 0; /* this is the first entry */
    else {
        p_buf = (uint8_t *)fndkey;
        p_buf_end = p_buf + key_offset + sizeof(*csc2_vers);
        if (!(p_buf = (uint8_t *)llmeta_file_type_dbname_csc2_vers_key_get(
                  &p_file_type_dbname_csc2_vers_key, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, 
                  "%s: llmeta_file_type_dbname_csc2_vers_key_get returns NULL\n",
                __func__);
            *bdberr = BDBERR_MISC;
            return -1;
        }
        *csc2_vers = p_file_type_dbname_csc2_vers_key.csc2_vers;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;
}

/* delete all csc2 versions from "ver" to 1 for table "dbname" */
int bdb_reset_csc2_version(tran_type *trans, const char *dbname, int ver, int start_ver)
{
    int rc;
    int bdberr;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_file_type_dbname_csc2_vers_key vers_key;
    uint8_t *p_buf_start;
    uint8_t *p_buf_end;

    p_buf_start = (uint8_t *)key;
    p_buf_end = p_buf_start + LLMETA_IXLEN;

    vers_key.file_type = LLMETA_CSC2;
    strncpy0(vers_key.dbname, dbname, sizeof(vers_key.dbname));
    vers_key.dbname_len = strlen(vers_key.dbname) + 1;

    while (ver >= start_ver) {
        vers_key.csc2_vers = ver;
        llmeta_file_type_dbname_csc2_vers_key_put(&vers_key, p_buf_start,
                                                  p_buf_end);
        rc = bdb_lite_exact_del(llmeta_bdb_state, trans, &key, &bdberr);
        if (rc && bdberr != BDBERR_NOERROR && bdberr != BDBERR_DEL_DTA) {
            logmsg(LOGMSG_ERROR, "%s() failed for ver: %d\n", __func__, ver);
            return bdberr;
        }
        --ver;
    }
    return BDBERR_NOERROR;
}

/* looks up a csc2 schema in the llmeta table
 * returns <0 if something fails or 0 on success */
int bdb_get_csc2(tran_type *tran, /* transaction to use, may be NULL */
                 const char *db_name,
                 int csc2_vers, /* version 0 to 255 or -1 to look up the
                                   highest */
                 char **schema, /* will point to a string that
                                 * contains the schema, pointer must be
                                 * freed by caller if successfull */
                 int *bdberr)
{
    int rc, retries = 0, schema_len;
    char key[LLMETA_IXLEN] = {0};
    size_t key_offset = 0;
    struct llmeta_file_type_dbname_csc2_vers_key
        p_file_type_dbname_csc2_vers_key;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;

    /*stop here if the db isn't open*/
    if (!llmeta_bdb_state) {
        *bdberr = BDBERR_DBEMPTY;
        return -1;
    }

    if (!db_name || !schema || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* make sure version is in range */
    if (csc2_vers < -1 /* TODO || csc2_vers > 255 */) {
        logmsg(LOGMSG_ERROR, "%s: csc2 version out of range\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* add the key type */
    p_file_type_dbname_csc2_vers_key.file_type = LLMETA_CSC2;

    /* copy the db_name and check its length so that it fit with enough room
     * left for the rest of the key */
    strncpy0(p_file_type_dbname_csc2_vers_key.dbname, db_name,
             sizeof(p_file_type_dbname_csc2_vers_key.dbname));
    p_file_type_dbname_csc2_vers_key.dbname_len =
        strlen(p_file_type_dbname_csc2_vers_key.dbname) + 1;
    /* zero this out for now */
    p_file_type_dbname_csc2_vers_key.csc2_vers = 0;

    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    /* trial put */
    if (!(p_buf = llmeta_file_type_dbname_csc2_vers_key_put(
              &(p_file_type_dbname_csc2_vers_key), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, 
                "%s: llmeta_file_type_dbname_csc2_vers_key_put returns NULL\n",
                __func__);
        logmsg(LOGMSG_ERROR, "%s: possible tablename length error?\n", __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    key_offset = p_buf - p_buf_start;

    if (key_offset > sizeof(key)) {
        logmsg(LOGMSG_ERROR, "%s: db_name is too long\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

/* csc2_vers added to key below */

retry:
    /* fetch the previous highest version if we've been asked to */
    if (csc2_vers == -1) {
        rc = bdb_get_csc2_highest(tran, db_name, &csc2_vers, bdberr);
        if (rc || *bdberr != BDBERR_NOERROR) {
            if (*bdberr == BDBERR_DEADLOCK) {
                if (++retries < gbl_maxretries && !tran) {
                    int dp = gbl_llmeta_deadlock_poll;
                    if (dp > 1)
                        poll(NULL, 0, rand() % dp);
                    goto retry;
                }

                logmsg(LOGMSG_ERROR, "%s:*ERROR* bdb_get_csc2_highest too much "
                                "contention %d count %d\n",
                        __func__, *bdberr, retries);
            }

            return -1;
        }
    }

    /* add version to key */
    p_file_type_dbname_csc2_vers_key.csc2_vers = csc2_vers;
    p_buf = p_buf_start;

    if (!(p_buf = llmeta_file_type_dbname_csc2_vers_key_put(
              &(p_file_type_dbname_csc2_vers_key), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, 
                "%s: llmeta_file_type_dbname_csc2_vers_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    /* try to fetch the schema change data */
    rc = bdb_lite_exact_var_fetch_tran(llmeta_bdb_state, tran, key,
                                       (void **)schema, &schema_len, bdberr);

    /* handle return codes */
    if (rc || *bdberr != BDBERR_NOERROR) {

        if (*bdberr == BDBERR_DEADLOCK) {
            if (++retries < gbl_maxretries && !tran) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, 
                    "%s:*ERROR* bdb_lite_exact_fetch too much contention "
                    "%d count %d\n",
                    __func__, *bdberr, retries);
        }

        return -1;
    }

    /* make sure the length appears normal */
    if (schema_len != strlen(*schema) + 1 /* for NULL byte */) {
        logmsg(LOGMSG_ERROR, "%s: schema length does not match length "
                        "retrieved\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;
}

static int bdb_file_version_change_filenum(bdb_state_type *bdb_state,
                                           tran_type *tran, int file_type,
                                           int from_file_num, int to_file_num,
                                           int *bdberr)
{
    unsigned long long version_num;

    /*look up the version*/
    if (bdb_get_file_version(tran, bdb_state->name, file_type, from_file_num,
                             &version_num, bdberr) ||
        *bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "%s: could not look up "
                        "temporary version\n",
                __func__);
        return -1;
    }

    /*set regular version*/
    if (bdb_new_file_version(tran, bdb_state->name, file_type, to_file_num,
                             version_num, bdberr) ||
        *bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "%s: could not set "
                        "regular version\n",
                __func__);
        return -1;
    }

    return 0;
}

/*calls bdb_file_version_change_filenum for a datafile*/
int bdb_file_version_change_dtanum(bdb_state_type *bdb_state, tran_type *tran,
                                   int fromdtanum, int todtanum, int *bdberr)
{
    return bdb_file_version_change_filenum(bdb_state, tran,
                                           LLMETA_FVER_FILE_TYPE_DTA,
                                           fromdtanum, todtanum, bdberr);
}

/*calls bdb_file_version_change_filenum for a indexfile*/
int bdb_file_version_change_ixnum(bdb_state_type *bdb_state, tran_type *tran,
                                  int fromixnum, int toixnum, int *bdberr)
{
    return bdb_file_version_change_filenum(
        bdb_state, tran, LLMETA_FVER_FILE_TYPE_IX, fromixnum, toixnum, bdberr);
}

/* updates the file's regular version number to equal its temporary version
 * number
 * returns <0 on failure, 0 on success */
static int
bdb_commit_temp_file_version(bdb_state_type *bdb_state, tran_type *tran,
                             int file_type, /* see FILE_VERSIONS_FILE_TYPE_* */
                             int file_num,  /* ixnum or dtanum */
                             int *bdberr)
{
    unsigned long long version_num;

    *bdberr = BDBERR_NOERROR;

    /* get a version of the tablename that doesn't start with new.SOMETHING. */
    const char *newtablename;
    if (bdb_state->origname) {
        newtablename = bdb_state->origname;
    } else {
        newtablename = bdb_unprepend_new_prefix(bdb_state->name, bdberr);
        if (*bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, 
                "%s: database name (%s) does"
                " not start with new.SOMETHING., it is not a temporary table\n",
                __func__, bdb_state->name);
            return -1;
        }
    }

    /*look up the temporary version*/
    if (bdb_get_file_version(tran, bdb_state->name, file_type, file_num,
                             &version_num, bdberr) ||
        *bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "%s: could not look up "
                        "temporary version\n",
                __func__);
        return -1;
    }

    /*set regular version*/
    if (bdb_new_file_version(tran, newtablename, file_type, file_num,
                             version_num, bdberr) ||
        *bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "%s: could not set regular "
                        "version\n",
                __func__);
        return -1;
    }

    if (file_type == LLMETA_FVER_FILE_TYPE_IX) {
        logmsg(LOGMSG_INFO, "%s: pointing ix %d at  %016llx\n", __func__,
               file_num, version_num);
    } else if (file_type == LLMETA_FVER_FILE_TYPE_DTA) {
        logmsg(LOGMSG_INFO, "%s: pointing data %d at  %016llx\n", __func__,
               file_num, version_num);
    }

    return 0;
}

/* calls bdb_commit_temp_file_version for a datafile */
int bdb_commit_temp_file_version_data(bdb_state_type *bdb_state,
                                      tran_type *tran, int dtanum, int *bdberr)
{
    return bdb_commit_temp_file_version(
        bdb_state, tran, LLMETA_FVER_FILE_TYPE_DTA, dtanum, bdberr);
}

/* calls bdb_commit_temp_file_version for an indexfile */
int bdb_commit_temp_file_version_index(bdb_state_type *bdb_state,
                                       tran_type *tran, int ixnum, int *bdberr)
{
    return bdb_commit_temp_file_version(
        bdb_state, tran, LLMETA_FVER_FILE_TYPE_IX, ixnum, bdberr);
}

/* calls bdb_commit_temp_file_version for all files in a db
 * WARNING: each file in the db has its version updated to its own temporary
 * version, therefore if those temporary versions are different so will the
 * files regular versions be after this call */
int bdb_commit_temp_file_version_all(bdb_state_type *bdb_state, tran_type *tran,
                                     int *bdberr)
{
    int dtanum, ixnum;

    /* update data files */
    for (dtanum = 0; dtanum < bdb_state->numdtafiles; dtanum++)
        if (bdb_commit_temp_file_version_data(bdb_state, tran, dtanum, bdberr))
            return -1;

    /* update the index files */
    for (ixnum = 0; ixnum < bdb_state->numix; ixnum++)
        if (bdb_commit_temp_file_version_index(bdb_state, tran, ixnum, bdberr))
            return -1;

    return 0;
}

/* updates whether a table is in a schema change (so that if it gets interrupted
 * and there is a new master the master knows to continue)
 * returns <0 if something fails or 0 on success */
int bdb_set_in_schema_change(
    tran_type *input_trans, /* if this is !NULL it will be used as
                             * the transaction for all actions, if
                             * it is NULL a new transaction will be
                             * created internally */
    const char *db_name,
    void *schema_change_data,      /* if this is NULL it means we are no
                                    * longer in a schema change
                                    * if !NULL it means that we are in the
                                    * schema change and that this buffer
                                    * contains the data another node would
                                    * need to continue/finish it if the
                                    * master gets changed before it
                                    * completes */
    size_t schema_change_data_len, /* the length of the schema_change_data
                                    * buffer, or 0 if it is NULL */
    int *bdberr)
{
    int retries = 0, rc;
    char key[LLMETA_IXLEN] = {0};
    tran_type *trans;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;
    struct llmeta_schema_change_type schema_change;

    /*fail if the db isn't open*/
    if (!llmeta_bdb_state) {
        logmsg(LOGMSG_ERROR, "%s: file versions db not yet "
                        "open, you must run bdb_open_file_versions\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (!db_name || (!schema_change_data && schema_change_data_len) ||
        (schema_change_data && !schema_change_data_len) || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL or inconsistant "
                        "argument\n",
                __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /*add the key type */
    schema_change.file_type = LLMETA_IN_SCHEMA_CHANGE;

    /*copy the table name and check its length so that we have a clean key*/
    strncpy0(schema_change.dbname, db_name, sizeof(schema_change.dbname));
    schema_change.dbname_len = strlen(schema_change.dbname) + 1;

    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf_start + LLMETA_IXLEN;

    if (!(p_buf = llmeta_schema_change_type_put(&schema_change, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_schema_change_type_put returns NULL\n", __func__);
        logmsg(LOGMSG_ERROR, "%s: check the length of db_name\n", __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d "
                        "retries\n",
                __func__, retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get "
                            "transaction, rc:%d\n",
                    __func__, *bdberr);
            return -1;
        }
    } else
        trans = input_trans;

    /* delete old entry */
    rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_DEL_DTA)
        goto backout;

    /* add new entry if we are in a schema change */
    if (schema_change_data) {
        rc = bdb_lite_add(llmeta_bdb_state, trans, schema_change_data,
                          schema_change_data_len, key, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            goto backout;
    }

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        int prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with"
                            " bdberr %d\n",
                    __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }
    return -1;
}

/* looks up whether a table is in the middle of a schema change
 * returns <0 if there was an error, 0 the lookup was successful or if  the key
 * wasn't found in the llmeta db */
int bdb_get_in_schema_change(
    tran_type *input_trans, const char *db_name, /* name of table to check */
    void **schema_change_data,      /* if this points to NULL it means we
                                     * are no longer in a schema change
                                     * if !NULL it means that we are in the
                                     * schema change and that this buffer
                                     * contains the data another node would
                                     * need to continue/finish it if the
                                     * master gets changed before it
                                     * completes, must be freed by caller if
                                     * call was successfull */
    size_t *schema_change_data_len, /* points to the length of the
                                     * schema_change_data buffer, or 0 if it
                                     * is NULL */
    int *bdberr)
{
    int rc, retries = 0, datalen;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_schema_change_type schema_change;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;

    /*stop here if the db isn't open*/
    if (!llmeta_bdb_state) {
        *bdberr = BDBERR_DBEMPTY;
        return -1;
    }

    if (!db_name || !schema_change_data || !schema_change_data_len || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /*add the file_type (ie dta, ix) and the file_num (ie ixnum dtanum)*/
    schema_change.file_type = LLMETA_IN_SCHEMA_CHANGE;

    /*copy the table name and check its length so that we have a clean key*/
    strncpy0(schema_change.dbname, db_name, sizeof(schema_change.dbname));
    schema_change.dbname_len = strlen(schema_change.dbname) + 1;

    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf_start + LLMETA_IXLEN;

    if (!(p_buf = llmeta_schema_change_type_put(&schema_change, p_buf,
                                                p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_schema_change_type_put returns NULL\n",
                __func__);
        logmsg(LOGMSG_ERROR, "%s: check the length of db_name\n", __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

retry:
    /* try to fetch the schema change data */
    rc = bdb_lite_exact_var_fetch_tran(llmeta_bdb_state, input_trans, key,
                                       schema_change_data, &datalen, bdberr);

    /* handle return codes */
    if (rc || *bdberr != BDBERR_NOERROR) {

        if (*bdberr == BDBERR_DEADLOCK && !input_trans) {
            if (++retries < gbl_maxretries) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, 
                    "%s: *ERROR* bdb_lite_exact_fetch too much contention "
                    "%d count %d\n",
                    __func__, *bdberr, retries);
        }

        /* it's ok if no data was found, fail on all other errors*/
        if (*bdberr != BDBERR_FETCH_DTA)
            return -1;
    }

    *schema_change_data_len = datalen;

    *bdberr = BDBERR_NOERROR;
    return 0;
}

static unsigned long long get_epochms(void)
{
    struct timeval tv;
    int rc;
    rc = gettimeofday(&tv, NULL);
    if (rc) {
        logmsg(LOGMSG_FATAL, "gettimeofday rc %d\n", rc);
        abort();
    }
    return (tv.tv_sec * 1000 + tv.tv_usec / 1000);
}

typedef struct llmeta_sequence_key {
    int file_type;
    char tablename[LLMETA_TBLLEN + 1];
    uint8_t padding[3];
    char columnname[LLMETA_COLUMNLEN + 1];
} llmeta_sequence_key;

enum { LLMETA_SEQUENCE_TYPE_KEY_LEN = 4 + LLMETA_TBLLEN + 1 + 3 + LLMETA_COLUMNLEN + 1 };
BB_COMPILE_TIME_ASSERT(llmeta_sequence_key_len, sizeof(llmeta_sequence_key) == LLMETA_SEQUENCE_TYPE_KEY_LEN);

int bdb_get_sequence(tran_type *t, const char *tablename, const char *columnname, int64_t *sequence, int *bdberr)
{
    int64_t s = 0;
    int rc = 0, fndlen;
    llmeta_sequence_key k = {0};
    k.file_type = htonl(LLMETA_SEQUENCE_VALUE);
    strncpy0(k.tablename, tablename, sizeof(k.tablename));
    strncpy0(k.columnname, columnname, sizeof(k.columnname));

    rc = bdb_lite_exact_fetch_full_tran(llmeta_bdb_state, t, &k, sizeof(k), &s, sizeof(s), &fndlen, bdberr);
    if (rc || fndlen != sizeof(int64_t)) {
        logmsg(LOGMSG_ERROR, "%s: tbl %s column %s sz=%d rc=%d bdberr=%d\n", __func__, tablename, columnname, fndlen,
               rc, *bdberr);
        return -1;
    }
    (*sequence) = flibc_ntohll(s);
    return 0;
}

int bdb_del_sequence(tran_type *t, const char *tablename, const char *columnname, int *bdberr)
{
    llmeta_sequence_key k = {0};
    k.file_type = htonl(LLMETA_SEQUENCE_VALUE);
    strncpy0(k.tablename, tablename, sizeof(k.tablename));
    strncpy0(k.columnname, columnname, sizeof(k.columnname));
    int rc = bdb_lite_delete(llmeta_bdb_state, t, &k, sizeof(k), bdberr);
    if (rc)
        logmsg(LOGMSG_ERROR, "%s: tbl %s column %s rc=%d bdberr=%d\n", __func__, tablename, columnname, rc, *bdberr);
    return rc;
}

int bdb_set_sequence(tran_type *t, const char *tablename, const char *columnname, int64_t sequence, int *bdberr)
{
    llmeta_sequence_key k = {0};
    k.file_type = htonl(LLMETA_SEQUENCE_VALUE);
    strncpy0(k.tablename, tablename, sizeof(k.tablename));
    strncpy0(k.columnname, columnname, sizeof(k.columnname));
    sequence = flibc_htonll(sequence);
    int rc = bdb_lite_delete(llmeta_bdb_state, t, &k, sizeof(k), bdberr);
    if (rc != 0 && *bdberr != BDBERR_DEL_DTA) {
        logmsg(LOGMSG_ERROR, "%s: tbl %s column %s del rc=%d bdberr=%d\n", __func__, tablename, columnname, rc,
               *bdberr);
        return rc;
    }
    rc = bdb_lite_full_add(llmeta_bdb_state, t, &sequence, sizeof(sequence), &k, sizeof(k), bdberr);
    if (rc)
        logmsg(LOGMSG_ERROR, "%s: tbl %s column %s rc=%d bdberr=%d\n", __func__, tablename, columnname, rc, *bdberr);
    return rc;
}

int bdb_increment_and_set_sequence(tran_type *t, const char *tablename, const char *columnname, int64_t *sequence,
                                   int *bdberr)
{
    int rc = bdb_get_sequence(t, tablename, columnname, sequence, bdberr);
    if (!rc) {
        if (*sequence == INT64_MAX) {
            logmsg(LOGMSG_ERROR, "%s: tbl %s column %s sequence at max %" PRId64 "\n", __func__, tablename, columnname,
                   *sequence);
            *bdberr = BDBERR_MAX_SEQUENCE;
            return -1;
        }
        (*sequence)++;
        rc = bdb_set_sequence(t, tablename, columnname, *sequence, bdberr);
    }
    return rc;
}

int bdb_check_and_set_sequence(tran_type *t, const char *tablename, const char *columnname, int64_t sequence,
                               int *bdberr)
{
    int64_t s;
    int rc = bdb_get_sequence(t, tablename, columnname, &s, bdberr);
    if (!rc && sequence > s) {
        rc = bdb_set_sequence(t, tablename, columnname, sequence, bdberr);
    }
    return rc;
}

typedef struct {
    int file_type;
    char tablename[LLMETA_TBLLEN + 1];
    char padding[3];
    uint64_t genid;
} llmeta_newsc_redo_genid_key;

enum { LLMETA_NEWSC_REDO_GENID_KEY_LEN = 4 + 32 + 1 + 3 + 8 };
BB_COMPILE_TIME_ASSERT(llmeta_newsc_redo_genid_key_len,
                       sizeof(llmeta_newsc_redo_genid_key) == LLMETA_NEWSC_REDO_GENID_KEY_LEN);

static uint8_t *llmeta_newsc_redo_genid_key_put(const llmeta_newsc_redo_genid_key *p_redo_genid, uint8_t *p_buf,
                                                const uint8_t *p_buf_end)
{
    p_buf = buf_put(&(p_redo_genid->file_type), sizeof(p_redo_genid->file_type), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_redo_genid->tablename), sizeof(p_redo_genid->tablename), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_redo_genid->padding), sizeof(p_redo_genid->padding), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_redo_genid->genid), sizeof(p_redo_genid->genid), p_buf, p_buf_end);
    return p_buf;
}

static const uint8_t *llmeta_newsc_redo_genid_key_get(llmeta_newsc_redo_genid_key *p_redo_genid, const uint8_t *p_buf,
                                                      const uint8_t *p_buf_end)
{
    p_buf = buf_get(&(p_redo_genid->file_type), sizeof(p_redo_genid->file_type), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_redo_genid->tablename), sizeof(p_redo_genid->tablename), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_redo_genid->padding), sizeof(p_redo_genid->padding), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_redo_genid->genid), sizeof(p_redo_genid->genid), p_buf, p_buf_end);
    return p_buf;
}

int bdb_llmeta_get_all_sc_redo_genids(tran_type *t, const char *tablename, llmeta_sc_redo_data **redo_out, int *num,
                                      int *bdberr)
{
    void **data = NULL;
    void **keys = NULL;
    int nkey = 0, rc = 1;
    llmeta_sc_redo_data *sc_redo = NULL;

    *num = 0;
    *redo_out = NULL;

    union {
        llmeta_newsc_redo_genid_key key;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};

    u.key.file_type = htonl(LLMETA_NEWSC_REDO_GENID);
    strncpy0(u.key.tablename, tablename, sizeof(u.key.tablename));
    int sz = offsetof(llmeta_newsc_redo_genid_key, padding);

    rc = kv_get_kv(t, &u, sz, &keys, &data, &nkey, bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed kv_get rc %d\n", __func__, rc);
        return -1;
    }
    if (nkey == 0)
        return 0;
    sc_redo = calloc(nkey, sizeof(llmeta_sc_redo_data) * nkey);
    if (sc_redo == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed malloc\n", __func__);
        *bdberr = BDBERR_MALLOC;
        return -1;
    }

    for (int i = 0; i < nkey; i++) {
        llmeta_newsc_redo_genid_key k = {0};
        struct llmeta_db_lsn_data_type d = {{0}};
        llmeta_newsc_redo_genid_key_get(&k, keys[i], (uint8_t *)(keys[i]) + sizeof(llmeta_newsc_redo_genid_key));
        sc_redo[i].genid = k.genid;
        llmeta_db_lsn_data_type_get(&d, data[i], (uint8_t *)(data[i]) + sizeof(struct llmeta_db_lsn_data_type));
        sc_redo[i].file = d.lsn.file;
        sc_redo[i].offset = d.lsn.offset;
    }

    for (int i = 0; i < nkey; i++) {
        free(keys[i]);
        free(data[i]);
    }

    free(data);
    free(keys);
    *num = nkey;
    *redo_out = sc_redo;

    return 0;
}

int bdb_newsc_del_redo_genid(tran_type *t, const char *tablename, uint64_t genid, int *bdberr)
{
    union {
        llmeta_newsc_redo_genid_key key;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};

    u.key.file_type = htonl(LLMETA_NEWSC_REDO_GENID);
    strncpy0(u.key.tablename, tablename, sizeof(u.key.tablename));
    u.key.genid = genid;
    *bdberr = BDBERR_NOERROR;
    int rc = kv_del(t, &u, bdberr);
    return rc;
}

int bdb_newsc_del_all_redo_genids(tran_type *t, const char *tablename, int *bdberr)
{
    union {
        llmeta_newsc_redo_genid_key key;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};

    u.key.file_type = htonl(LLMETA_NEWSC_REDO_GENID);
    strncpy0(u.key.tablename, tablename, sizeof(u.key.tablename));
    int rc = kv_del_by_value(t, &u, offsetof(llmeta_newsc_redo_genid_key, padding), NULL, 0, bdberr);
    return rc;
}

int bdb_newsc_set_redo_genid(tran_type *t, const char *tablename, uint64_t genid, unsigned int file,
                             unsigned int offset, int *bdberr)
{
    union {
        llmeta_newsc_redo_genid_key key;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};

    u.key.file_type = htonl(LLMETA_NEWSC_REDO_GENID);
    strncpy0(u.key.tablename, tablename, sizeof(u.key.tablename));
    u.key.genid = genid;
    struct llmeta_db_lsn_data_type newsc_lsn = {.lsn.file = file, .lsn.offset = offset};

    uint8_t *p_buf_start, *p_buf_end;
    p_buf_start = alloca(sizeof(struct llmeta_db_lsn_data_type));
    p_buf_end = p_buf_start + sizeof(struct llmeta_db_lsn_data_type);
    llmeta_db_lsn_data_type_put(&newsc_lsn, p_buf_start, p_buf_end);

    *bdberr = BDBERR_NOERROR;
    int rc = kv_put(t, &u, p_buf_start, sizeof(struct llmeta_db_lsn_data_type), bdberr);
    return rc;
}

static uint8_t *llmeta_sc_hist_data_put(const llmeta_sc_hist_data *p_sc_hist,
                                        uint8_t *p_buf,
                                        const uint8_t *p_buf_end)
{
    p_buf = buf_put(&(p_sc_hist->converted), sizeof(p_sc_hist->converted),
                    p_buf, p_buf_end);

    p_buf = buf_put(&(p_sc_hist->start), sizeof(p_sc_hist->start), p_buf,
                    p_buf_end);

    p_buf =
        buf_put(&(p_sc_hist->last), sizeof(p_sc_hist->last), p_buf, p_buf_end);

    p_buf = buf_put(&(p_sc_hist->status), sizeof(p_sc_hist->status), p_buf,
                    p_buf_end);

    p_buf = buf_no_net_put(&(p_sc_hist->errstr), LLMETA_SCERR_LEN, p_buf,
                           p_buf_end);

    /* If we want to store scdata in the future:
    p_buf = buf_put(&(p_sc_hist->sc_data_len),
                    sizeof(p_sc_hist->sc_data_len), p_buf, p_buf_end);
     */

    return p_buf;
}

// write the key info into sc_hist_row
static const uint8_t *llmeta_sc_hist_data_get(sc_hist_row *p_sc_hist,
                                              const uint8_t *p_buf,
                                              const uint8_t *p_buf_end)
{
    p_buf = buf_get(&(p_sc_hist->converted), sizeof(p_sc_hist->converted),
                    p_buf, p_buf_end);

    p_buf = buf_get(&(p_sc_hist->start), sizeof(p_sc_hist->start), p_buf,
                    p_buf_end);

    p_buf =
        buf_get(&(p_sc_hist->last), sizeof(p_sc_hist->last), p_buf, p_buf_end);

    p_buf = buf_get(&(p_sc_hist->status), sizeof(p_sc_hist->status), p_buf,
                    p_buf_end);

    p_buf = buf_no_net_get(&(p_sc_hist->errstr), sizeof(p_sc_hist->errstr),
                           p_buf, p_buf_end);

    /* If we want to store scdata in the future:
    p_buf = buf_get(&(p_sc_hist->sc_data_len),
                    sizeof(p_sc_hist->sc_data_len), p_buf, p_buf_end);
     */

    return p_buf;
}

struct llmeta_hist_key {
    int file_type;
    char tablename[LLMETA_TBLLEN + 1];
    char pad[3];
    uint64_t seed;
};
enum { LLMETA_HIST_KEY_LEN = 4 + LLMETA_TBLLEN + 4 + 8 };

BB_COMPILE_TIME_ASSERT(llmeta_hist_key_len,
                       sizeof(struct llmeta_hist_key) == LLMETA_HIST_KEY_LEN);

// put the key info
static const uint8_t *
llmeta_sc_hist_key_put(const struct llmeta_hist_key *p_hist_k, uint8_t *p_buf,
                       const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        sizeof(struct llmeta_hist_key) > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_hist_k->file_type), sizeof(p_hist_k->file_type), p_buf,
                    p_buf_end);

    p_buf = buf_no_net_put(&(p_hist_k->tablename), sizeof(p_hist_k->tablename),
                           p_buf, p_buf_end);

    p_buf += sizeof(p_hist_k->pad);
    p_buf =
        buf_put(&(p_hist_k->seed), sizeof(p_hist_k->seed), p_buf, p_buf_end);

    return p_buf;
}

// get the key info
static const uint8_t *llmeta_sc_hist_key_get(struct llmeta_hist_key *p_hist_k,
                                             const uint8_t *p_buf,
                                             const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        sizeof(struct llmeta_hist_key) > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_hist_k->file_type), sizeof(p_hist_k->file_type), p_buf,
                    p_buf_end);

    p_buf = buf_no_net_get(&(p_hist_k->tablename), sizeof(p_hist_k->tablename),
                           p_buf, p_buf_end);

    p_buf += sizeof(p_hist_k->pad);
    p_buf =
        buf_get(&(p_hist_k->seed), sizeof(p_hist_k->seed), p_buf, p_buf_end);

    return p_buf;
}

int bdb_del_schema_change_history(tran_type *t, const char *tablename,
                                  uint64_t seed)
{
    union {
        struct llmeta_hist_key key;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};

    u.key.file_type = htonl(LLMETA_SCHEMACHANGE_HISTORY);
    strncpy0(u.key.tablename, tablename, sizeof(u.key.tablename));
    u.key.seed = flibc_ntohll(seed);

    int bdberr;
    int rc = kv_del(t, &u, &bdberr);
    if (rc)
        logmsg(LOGMSG_ERROR,
               "%s: tbl %s seed %0#16" PRIx64 " rc=%d bdberr=%d\n", __func__,
               tablename, seed, rc, bdberr);
    return rc;
}

int bdb_set_schema_change_history(tran_type *t, const char *tablename,
                                  uint64_t seed, uint64_t converted, int status,
                                  uint64_t start, uint64_t last,
                                  const char *errstr, int *bdberr)
{
    union {
        struct llmeta_hist_key key;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};

    u.key.file_type = htonl(LLMETA_SCHEMACHANGE_HISTORY);
    strncpy0(u.key.tablename, tablename, sizeof(u.key.tablename));
    u.key.seed = seed;

    uint8_t *p_buf_start, *p_buf_end;
    p_buf_start = alloca(sizeof(llmeta_sc_hist_data));

    llmeta_sc_hist_data sc_hist = {
        .converted = converted, .start = start, .last = last, .status = status};
    if (errstr)
        strncpy0(sc_hist.errstr, errstr, sizeof(sc_hist.errstr));
    p_buf_end = p_buf_start + sizeof(llmeta_sc_hist_data);
    llmeta_sc_hist_data_put(&sc_hist, p_buf_start, p_buf_end);

    int rc = kv_put(t, &u, p_buf_start, sizeof(llmeta_sc_hist_data), bdberr);

    *bdberr = BDBERR_NOERROR;
    return rc;
}

int bdb_llmeta_get_sc_history(tran_type *t, sc_hist_row **hist_out, int *num,
                              int *bdberr, const char *tablename)
{
    void **data = NULL;
    void **keys = NULL;
    int nkey = 0, rc = 1;
    sc_hist_row *hist = NULL;
    void **sc_data = NULL;

    *num = 0;
    *hist_out = NULL;
    union {
        struct llmeta_hist_key key;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};

    u.key.file_type = htonl(LLMETA_SCHEMACHANGE_HISTORY);
    int sz = sizeof(int);

    if (tablename) {
        strncpy0(u.key.tablename, tablename, sizeof(u.key.tablename));
        sz += sizeof(u.key.tablename);
    }

    rc = kv_get_kv(t, &u, sz, &keys, &data, &nkey, bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed kv_get rc %d\n", __func__, rc);
        return -1;
    }
    if (nkey == 0)
        return 0;
    hist = calloc(nkey, sizeof(sc_hist_row) * nkey);
    if (hist == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed malloc\n", __func__);
        *bdberr = BDBERR_MALLOC;
        return -1;
    }

    sc_data = calloc(nkey, sizeof(void *));
    if (sc_data == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed malloc\n", __func__);
        free(hist);
        *bdberr = BDBERR_MALLOC;
        return -1;
    }

    for (int i = 0; i < nkey; i++) {
        struct llmeta_hist_key k = {0};
        llmeta_sc_hist_key_get(&k, keys[i], (uint8_t *)(keys[i]) + sizeof(struct llmeta_hist_key));
        strcpy(hist[i].tablename, k.tablename);
        hist[i].seed = k.seed;
        llmeta_sc_hist_data_get(&hist[i], data[i],
                                (uint8_t *)(data[i]) +
                                    sizeof(llmeta_sc_hist_data));
    }

    for (int i = 0; i < nkey; i++) {
        free(keys[i]);
        free(data[i]);
    }
    free(data);
    free(keys);

    *num = nkey;
    *hist_out = hist;
    return 0;
}

enum { LLMETA_SC_STATUS_DATA_LEN = 8 + 4 + 8 + LLMETA_SCERR_LEN + 4 };

static uint8_t *
llmeta_sc_status_data_put(const llmeta_sc_status_data *p_sc_status,
                          uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_SC_STATUS_DATA_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_sc_status->start), sizeof(p_sc_status->start), p_buf,
                    p_buf_end);

    p_buf = buf_put(&(p_sc_status->status), sizeof(p_sc_status->status), p_buf,
                    p_buf_end);

    p_buf = buf_put(&(p_sc_status->last), sizeof(p_sc_status->last), p_buf,
                    p_buf_end);

    p_buf = buf_no_net_put(&(p_sc_status->errstr), LLMETA_SCERR_LEN, p_buf,
                           p_buf_end);

    p_buf = buf_put(&(p_sc_status->sc_data_len),
                    sizeof(p_sc_status->sc_data_len), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
llmeta_sc_status_data_get(llmeta_sc_status_data *p_sc_status,
                          const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_SC_STATUS_DATA_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_sc_status->start), sizeof(p_sc_status->start), p_buf,
                    p_buf_end);

    p_buf = buf_get(&(p_sc_status->status), sizeof(p_sc_status->status), p_buf,
                    p_buf_end);

    p_buf = buf_get(&(p_sc_status->last), sizeof(p_sc_status->last), p_buf,
                    p_buf_end);

    p_buf = buf_no_net_get(&(p_sc_status->errstr), sizeof(p_sc_status->errstr),
                           p_buf, p_buf_end);

    p_buf = buf_get(&(p_sc_status->sc_data_len),
                    sizeof(p_sc_status->sc_data_len), p_buf, p_buf_end);

    return p_buf;
}

int bdb_set_schema_change_status(tran_type *input_trans, const char *db_name,
                                 uint64_t seed, uint64_t converted,
                                 void *schema_change_data,
                                 size_t schema_change_data_len, int status,
                                 const char *errstr, int *bdberr)
{
    int retries = 0, rc;
    char key[LLMETA_IXLEN] = {0};
    tran_type *trans;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;
    struct llmeta_schema_change_type schema_change = {0};
    uint8_t *data = NULL;
    int datalen;
    llmeta_sc_status_data sc_status_data = {0};

    if (!llmeta_bdb_state) {
        logmsg(LOGMSG_ERROR, "%s: no llmeta_bdb_state\n", __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (!db_name || (!schema_change_data && schema_change_data_len) ||
        (schema_change_data && !schema_change_data_len) || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL or inconsistant argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /*add the key type */
    schema_change.file_type = LLMETA_SCHEMACHANGE_STATUS_V2;

    /*copy the table name and check its length so that we have a clean key*/
    strncpy0(schema_change.dbname, db_name, sizeof(schema_change.dbname));
    schema_change.dbname_len = strlen(schema_change.dbname) + 1;

    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf_start + LLMETA_IXLEN;

    if (!(p_buf = llmeta_schema_change_type_put(&schema_change, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_schema_change_type_put returns NULL\n",
               __func__);
        logmsg(LOGMSG_ERROR, "%s: check the length of db_name\n", __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__,
               retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get transaction, rc:%d\n",
                   __func__, *bdberr);
            return -1;
        }
    } else
        trans = input_trans;

    /* try to fetch the existing schema change status */
    rc = bdb_lite_exact_var_fetch_tran(llmeta_bdb_state, trans, key,
                                       (void **)&data, &datalen, bdberr);

    int new_sc = 0;
    /* handle return codes */
    if (rc || *bdberr != BDBERR_NOERROR) {

        if (*bdberr == BDBERR_DEADLOCK)
            goto backout;

        /* it's ok if no data was found, fail on all other errors*/
        if (*bdberr != BDBERR_FETCH_DTA || status == BDB_SC_ABORTED)
            goto backout;

        new_sc = 1;
    } else {
        void *sc_data = NULL;
        assert(data != NULL);
        assert(datalen != 0);

        sc_data = (void *)llmeta_sc_status_data_get(&sc_status_data, data,
                                                    data + datalen);
        assert((uint8_t *)sc_data + sc_status_data.sc_data_len ==
               data + datalen);

        if (schema_change_data == NULL && schema_change_data_len == 0) {
            schema_change_data = sc_data;
            schema_change_data_len = sc_status_data.sc_data_len;
            assert(schema_change_data != NULL && schema_change_data_len != 0);
        }

        if (sc_status_data.status == BDB_SC_ABORTED ||
            sc_status_data.status == BDB_SC_COMMITTED)
            new_sc = 1;

        /* delete old entry */
        rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_DEL_DTA)
            goto backout;
    }

    /* update status */
    if (new_sc)
        sc_status_data.start = get_epochms();
    sc_status_data.status = status;
    sc_status_data.last = get_epochms();
    snprintf(sc_status_data.errstr, LLMETA_SCERR_LEN, "%s",
             errstr ? errstr : "");
    sc_status_data.sc_data_len = schema_change_data_len;

    assert(schema_change_data != NULL && schema_change_data_len != 0);

    /* prepare data payload */
    p_buf_start = p_buf =
        malloc(LLMETA_SC_STATUS_DATA_LEN + schema_change_data_len);
    if (p_buf == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed to malloc %zu\n", __func__,
               LLMETA_SC_STATUS_DATA_LEN + schema_change_data_len);
        *bdberr = BDBERR_MALLOC;
        goto backout;
    }
    p_buf_end =
        p_buf_start + LLMETA_SC_STATUS_DATA_LEN + schema_change_data_len;

    p_buf = llmeta_sc_status_data_put(&sc_status_data, p_buf, p_buf_end);
    assert(p_buf != NULL && p_buf_end > p_buf &&
           (p_buf_end - p_buf) >= schema_change_data_len);
    memcpy(p_buf, schema_change_data, schema_change_data_len);

    /* add new entry */
    rc = bdb_lite_add(llmeta_bdb_state, trans, p_buf_start,
                      p_buf_end - p_buf_start, key, bdberr);
    free(p_buf_start);
    if (rc && *bdberr != BDBERR_NOERROR)
        goto backout;

    rc = bdb_set_schema_change_history(trans, db_name, seed, converted, status,
                                       sc_status_data.start,
                                       sc_status_data.last, errstr, bdberr);

    if (rc && *bdberr != BDBERR_NOERROR)
        goto backout;

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
    }

    if (data)
        free(data);

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        int prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with bdberr %d\n",
                   __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }
    if (data)
        free(data);
    return -1;
}

int bdb_llmeta_get_all_sc_status(tran_type *tran, llmeta_sc_status_data **status_out, void ***sc_data_out, int *num,
                                 int *bdberr)
{
    int rc = 1;
    llmeta_sc_status_data *status = NULL;
    void **sc_data = NULL;

    *num = 0;
    *status_out = NULL;
    *sc_data_out = NULL;

    /* Extract old (v1) sc status data */
    llmetakey_t k_v1 = htonl(LLMETA_SCHEMACHANGE_STATUS);
    int nkey_v1 = 0;
    void **data_v1 = NULL;
    rc = kv_get(tran, &k_v1, sizeof(k_v1), &data_v1, &nkey_v1, bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed kv_get rc %d\n", __func__, rc);
        return -1;
    }

    /* Extract new (v2) sc status data */
    llmetakey_t k_v2 = htonl(LLMETA_SCHEMACHANGE_STATUS_V2);
    int nkey_v2 = 0;
    void **data_v2 = NULL;
    rc = kv_get(tran, &k_v2, sizeof(k_v2), &data_v2, &nkey_v2, bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed kv_get rc %d\n", __func__, rc);
        return -1;
    }

    int nkey = nkey_v1 + nkey_v2;

    if (nkey == 0)
        return 0;

    status = calloc(nkey, sizeof(llmeta_sc_status_data) * nkey);
    if (status == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed malloc\n", __func__);
        *bdberr = BDBERR_MALLOC;
        return -1;
    }

    sc_data = calloc(nkey, sizeof(void *));
    if (sc_data == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed malloc\n", __func__);
        free(status);
        *bdberr = BDBERR_MALLOC;
        return -1;
    }

    for (int i = 0; i < nkey_v1; i++) {
        const uint8_t *p_buf;
        p_buf = llmeta_sc_status_data_get(&status[i], data_v1[i],
                                          (uint8_t *)(data_v1[i]) +
                                              sizeof(llmeta_sc_status_data));
        sc_data[i] = malloc(status[i].sc_data_len);
        if (sc_data[i] == NULL) {
            logmsg(LOGMSG_ERROR, "%s: failed malloc\n", __func__);
            *bdberr = BDBERR_MALLOC;
            goto err;
        }

        memcpy(sc_data[i], p_buf, status[i].sc_data_len);
    }

    for (int i = 0; i < nkey_v1; i++) {
        if (data_v1[i])
            free(data_v1[i]);
    }
    free(data_v1);

    for (int i = 0; i < nkey_v2; i++) {
        const uint8_t *p_buf;
        p_buf = llmeta_sc_status_data_get(&status[nkey_v1+i], data_v2[i],
                                          (uint8_t *)(data_v2[i]) +
                                              sizeof(llmeta_sc_status_data));
        sc_data[nkey_v1 + i] = malloc(status[nkey_v1 + i].sc_data_len);
        if (sc_data[nkey_v1 + i] == NULL) {
            logmsg(LOGMSG_ERROR, "%s: failed malloc\n", __func__);
            *bdberr = BDBERR_MALLOC;
            goto err;
        }

        memcpy(sc_data[nkey_v1 + i], p_buf, status[nkey_v1 + i].sc_data_len);
    }

    for (int i = 0; i < nkey_v2; i++) {
        if (data_v2[i])
            free(data_v2[i]);
    }
    free(data_v2);

    *num = nkey;
    *status_out = status;
    *sc_data_out = sc_data;
    return 0;

err:
    for (int i = 0; i < nkey_v1; i++) {
        if (data_v1[i])
            free(data_v1[i]);
        if (sc_data[i])
            free(sc_data[i]);
    }
    free(data_v1);

    for (int i = 0; i < nkey_v2; i++) {
        if (data_v2[i])
            free(data_v2[i]);
        if (sc_data[nkey_v1 + i])
            free(sc_data[nkey_v1 + i]);
    }
    free(data_v2);

    free(status);
    free(sc_data);
    return -1;
}

/* updates the last processed genid for a stripe in the in progress schema
 * change. should only be used if schema change is not rebuilding main data
 * files because if it is you can simply query those for their highest genids
 *
 * this should only be called directly when you want to clear the high genid for
 * a stripe (by using a 0 genid) otherwise bdb_set_high_genid should be called
 * so that stripe is set properly
 *
 * returns <0 if something fails or 0 on success */
static int bdb_set_high_genid_int(
    tran_type *input_trans,          /* if this is !NULL it will be used as
                                      * the transaction for all actions, if
                                      * it is NULL a new transaction will be
                                      * created internally */
    const char *db_name, int stripe, /* stripe to set this genid for, should
                                      *  match the genid, this parameter is
                                      *  here so that you can clear a
                                      *  specific stripe with a 0 genid */
    unsigned long long genid,        /* genid to set as highest in its
                                      * stripe */
    int *bdberr)
{
    int retries = 0, rc;
    char key[LLMETA_IXLEN] = {0};
    tran_type *trans;
    struct llmeta_high_genid_key_type high_genid_key_type;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;

    /*fail if the db isn't open*/
    if (!llmeta_bdb_state) {
        logmsg(LOGMSG_ERROR, "%s: file versions db not yet "
                        "open, you must run bdb_open_file_versions\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (!db_name || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /*add the key type */
    high_genid_key_type.file_type = LLMETA_HIGH_GENID;

    /*copy the table name and check its length so that we have a clean key*/
    /* BTW- There's NO NULL BYTE!  So the stripe ends up using 3 bytes
       rather than 4 */
    strncpy0(high_genid_key_type.dbname, db_name,
             sizeof(high_genid_key_type.dbname));
    high_genid_key_type.dbname_len = strlen(high_genid_key_type.dbname);

    /* add stripe to key */
    high_genid_key_type.stripe = stripe;

    /* set pointers to start and end of buffer */
    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf_start + LLMETA_IXLEN;

    /* write endianized key */
    if (!(p_buf = llmeta_high_genid_key_type_put(&high_genid_key_type, p_buf,
                                                 p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_high_genid_key_type_put returns NULL\n",
                __func__);
        logmsg(LOGMSG_ERROR, "%s: possibly the db_name is too long\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get "
                            "transaction\n",
                    __func__);
            return -1;
        }
    } else
        trans = input_trans;

    /* delete old entry */
    rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_DEL_DTA)
        goto backout;

    /* add new entry if we were given a legitimate genid */
    if (genid) {
        unsigned long long tmpgenid = genid;
        rc = bdb_lite_add(llmeta_bdb_state, trans, &tmpgenid, sizeof(tmpgenid),
                          key, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            goto backout;
    }

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        int prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with"
                            " bdberr %d\n",
                    __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }
    return -1;
}

/* clears out the saved high genid for each stripe should be called when
 * starting a new schema change that has a table scan so that it resumes
 * properly */
int bdb_clear_high_genid(
    tran_type *input_trans,               /* if this is !NULL it will be used as
                                           * the transaction for all actions, if
                                           * it is NULL a new transaction will be
                                           * created internally for each stripe */
    const char *db_name, int num_stripes, /* number of stripes to clear */
    int *bdberr)
{
    int stripe;

    /* clear out the highest genid saved for each stripe */
    for (stripe = 0; stripe < num_stripes; stripe++) {
        if (bdb_set_high_genid_int(input_trans, db_name, stripe, 0ULL /*genid*/,
                                   bdberr) &&
            *bdberr != BDBERR_NOERROR)
            return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;
}

/* determines what stripe the genid is part of and calls
 * bdb_set_high_genid_int */
int bdb_set_high_genid(tran_type *input_trans, const char *db_name,
                       unsigned long long genid, int *bdberr)
{
    return bdb_set_high_genid_int(input_trans, db_name,
                                  get_dtafile_from_genid(genid), genid, bdberr);
}

int bdb_set_high_genid_stripe(tran_type *input_trans, const char *db_name,
                              int stripe, unsigned long long genid, int *bdberr)
{
    return bdb_set_high_genid_int(input_trans, db_name, stripe, genid, bdberr);
}

/* looks up the last procesed genid for a given stripe in the in progress schema
 * change, returned values have no meaning if bdb_get_in_schema_change does not
 * say we are in a schema change or if the schema change is rebuilding its data
 * files
 * returns <0 if there was an error, 0 the lookup was successful or if  the key
 * wasn't found in the llmeta db */
int bdb_get_high_genid(
    const char *db_name,       /* name of table to check */
    int stripe,                /* stripe to get highest genid for */
    unsigned long long *genid, /* set to genid that found (or 0 if
                                * nothing found for given db_name and
                                * stripe ) */
    int *bdberr)
{
    int rc, fndlen, retries = 0;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_high_genid_key_type high_genid_key_type;
    unsigned long long tmpgenid = 0;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;

    /*stop here if the db isn't open*/
    if (!llmeta_bdb_state) {
        *bdberr = BDBERR_DBEMPTY;
        return -1;
    }

    if (!db_name || !genid || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /*add the key type */
    high_genid_key_type.file_type = LLMETA_HIGH_GENID;

    /*copy the table name and check its length so that we have a clean key*/
    /* BTW- There's NO NULL BYTE!  So the stripe ends up using 3 bytes
       rather than 4 */
    strncpy0(high_genid_key_type.dbname, db_name,
             sizeof(high_genid_key_type.dbname));
    high_genid_key_type.dbname_len = strlen(high_genid_key_type.dbname);

    /* add stripe to key */
    high_genid_key_type.stripe = stripe;

    /* set key-pointers */
    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf_start + LLMETA_IXLEN;

    /* format buffer */
    if (!(p_buf = llmeta_high_genid_key_type_put(&high_genid_key_type, p_buf,
                                                 p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_high_genid_key_type_put returns NULL\n",
                __func__);
        logmsg(LOGMSG_ERROR, "%s: possibly the db_name is too long\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    /* try to fetch the schema change data */
    rc = bdb_lite_exact_fetch(llmeta_bdb_state, key, &tmpgenid,
                              sizeof(tmpgenid), &fndlen, bdberr);

    /* genids don't get flipped */
    *genid = tmpgenid;

    /* handle return codes */
    if (rc || *bdberr != BDBERR_NOERROR) {

        if (*bdberr == BDBERR_DEADLOCK) {
            /* TODO: this function doesn't take tran argument */
            if (++retries < gbl_maxretries) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, 
                    "%s:*ERROR* bdb_lite_exact_fetch too much contention "
                    "%d count %d\n",
                    __func__, *bdberr, retries);
        }

        /* it's ok if no data was found, fail on all other errors*/
        if (*bdberr != BDBERR_FETCH_DTA)
            return -1;

        *genid = 0; /* set to 0 if we didn't find any data */
    }
    /*if we did not get the right amount of data*/
    else if (fndlen != sizeof(*genid)) {
        *bdberr = BDBERR_DTA_MISMATCH; /* TODO right error to throw? */
        return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;
}

int bdb_delete_file_lwm(bdb_state_type *bdb_state, tran_type *tran, int *bdberr)
{
    char key[LLMETA_IXLEN] = {0};
    int rc;
    struct llmeta_file_type_key file_type_key;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;

    file_type_key.file_type = LLMETA_LOGICAL_LSN_LWM;
    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf_start + LLMETA_IXLEN;

    if (!(llmeta_file_type_key_put(&(file_type_key), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, bdberr);
    if (*bdberr == BDBERR_DEL_DTA) {
        rc = 0;
        *bdberr = BDBERR_NOERROR;
    }

    return rc;
}

int bdb_get_file_lwm(bdb_state_type *bdb_state, tran_type *tran, DB_LSN *lsn,
                     int *bdberr)
{
    char key[LLMETA_IXLEN] = {0};
    int fndlen;
    int rc;
    DB_LSN tmplsn;
    struct llmeta_file_type_key file_type_key;
    struct llmeta_db_lsn_data_type lsn_data;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;

    file_type_key.file_type = LLMETA_LOGICAL_LSN_LWM;
    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf_start + LLMETA_IXLEN;

    if (!(llmeta_file_type_key_put(&(file_type_key), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* llmeta_bdb_state */
    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, tran, key, &tmplsn,
                                   sizeof(tmplsn), &fndlen, bdberr);
    if (rc) {
        /* if we don't have an entry, return 1:0. */
        if (*bdberr == BDBERR_FETCH_DTA) {
            lsn->file = 1;
            lsn->offset = 0;
            return 0;
        }
    } else {
        p_buf = (uint8_t *)&tmplsn;
        p_buf_end = p_buf + sizeof(*lsn);

        if (!(llmeta_db_lsn_data_type_get(&lsn_data, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: llmeta_db_lsn_data_type_get returns NULL\n",
                    __func__);
            *bdberr = BDBERR_BADARGS;
            return -1;
        }
        lsn->file = lsn_data.lsn.file;
        lsn->offset = lsn_data.lsn.offset;
    }
    return rc;
}

static int make_sp_key(uint8_t *key, const char *name, int version)
{
    struct llmeta_file_type_spname_lua_vers_key luakey;
    luakey.file_type = LLMETA_SP_LUA_SOURCE;
    luakey.spname_len = strlen(name) + 1;
    luakey.lua_vers = version;
    if (luakey.spname_len > LLMETA_SPLEN)
        return -1;
    strncpy0(luakey.spname, name, sizeof(luakey.spname));
    uint8_t *key_end = key + LLMETA_IXLEN;
    if (llmeta_file_type_spname_lua_vers_key_put(&luakey, key, key_end) == NULL)
        return -1;
    return 0;
}

int bdb_get_sp_name(tran_type *trans, /* transaction to use, may be NULL */
                    const char *sp_name,
                    char *new_sp_name, /* will be set to the highest version*/
                    int *bdberr)
{
    int rc, retries = 0, numfnd;
    uint8_t key[LLMETA_IXLEN] = {0}, fndkey[LLMETA_IXLEN] = {0};

    /*stop here if the db isn't open*/
    if (!llmeta_bdb_state) {
        *bdberr = BDBERR_DBEMPTY;
        return -1;
    }

    if (!sp_name || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (make_sp_key(key, sp_name, 0) != 0) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    /* try to find the highest version for this table */
    rc = bdb_lite_fetch_keys_bwd_tran(llmeta_bdb_state, trans, key, fndkey,
                                      1 /*maxfnd*/, &numfnd, bdberr);

    /* handle return codes */
    if (rc || *bdberr != BDBERR_NOERROR) {

        if (*bdberr == BDBERR_DEADLOCK) {
            if (++retries < gbl_maxretries && !trans) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: *ERROR* bdb_lite_fetch_keys_bwd too much "
                            "contention %d count %d\n",
                    __func__, *bdberr, retries);
        }

        /* it's ok if no data was found, fail on all other errors*/
        if (*bdberr != BDBERR_FETCH_DTA)
            return -1;
    }

    /* if nothing is found or the header and sp_name aren't the same */
    if (!numfnd || *bdberr == BDBERR_FETCH_DTA) {
        new_sp_name[0] = '\0';
        return -1;
    } else {
        struct llmeta_file_type_spname_lua_vers_key
            p_file_type_spname_lua_vers_key;
        const uint8_t *p_buf = fndkey;
        const uint8_t *p_buf_end = p_buf + LLMETA_IXLEN;
        if (!(p_buf = llmeta_file_type_spname_lua_vers_key_get(
                  &p_file_type_spname_lua_vers_key, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, 
                "%s: llmeta_file_type_spname_lua_vers_key_get returns NULL\n",
                __func__);
            *bdberr = BDBERR_MISC;
            return -1;
        }
        if (p_file_type_spname_lua_vers_key.file_type != LLMETA_SP_LUA_SOURCE) {
            return -1;
        }
        strcpy(new_sp_name, p_file_type_spname_lua_vers_key.spname);
    }

    *bdberr = BDBERR_NOERROR;
    return 0;
}

/* finds the highest schema version for this table name
 * returns <0 if something fails or 0 on success */
int bdb_get_lua_highest(tran_type *trans, /* transaction to use, may be NULL */
                        const char *sp_name,
                        int *lua_vers, /* will be set to the highest version*/
                        int max_lua_vers, int *bdberr)
{
    int rc, retries = 0, numfnd;
    uint8_t key[LLMETA_IXLEN] = {0}, fndkey[LLMETA_IXLEN] = {0};

    /*stop here if the db isn't open*/
    if (!llmeta_bdb_state) {
        *bdberr = BDBERR_DBEMPTY;
        return -1;
    }

    if (!sp_name || !lua_vers || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (make_sp_key(key, sp_name, max_lua_vers) != 0) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    /* try to find the highest version for this table */
    rc = bdb_lite_fetch_keys_bwd_tran(llmeta_bdb_state, trans, key, fndkey,
                                      1 /*maxfnd*/, &numfnd, bdberr);

    /* handle return codes */
    if (rc || *bdberr != BDBERR_NOERROR) {

        if (*bdberr == BDBERR_DEADLOCK) {
            if (++retries < gbl_maxretries && !trans) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: *ERROR* bdb_lite_fetch_keys_bwd too much "
                            "contention %d count %d\n",
                    __func__, *bdberr, retries);
        }

        /* it's ok if no data was found, fail on all other errors*/
        if (*bdberr != BDBERR_FETCH_DTA)
            return -1;
    }

    size_t hdrlen = sizeof(llmetakey_t) + strlen(sp_name) + 1;
    if (numfnd == 0 || memcmp(key, fndkey, hdrlen) != 0)
        *lua_vers = 0; /* this is the first entry */
    else {
        struct llmeta_file_type_spname_lua_vers_key
            p_file_type_spname_lua_vers_key;
        uint8_t *p_buf = fndkey;
        uint8_t *p_buf_end = p_buf + LLMETA_IXLEN;
        if (!(p_buf = (uint8_t *)llmeta_file_type_spname_lua_vers_key_get(
                  &p_file_type_spname_lua_vers_key, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, 
                "%s: llmeta_file_type_spname_lua_vers_key_get returns NULL\n",
                __func__);
            *bdberr = BDBERR_MISC;
            return -1;
        }
        *lua_vers = p_file_type_spname_lua_vers_key.lua_vers;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;
}

int bdb_get_sp_lua_source(bdb_state_type *bdb_state, tran_type *tran,
                          const char *sp_name, char **lua_file, int lua_ver,
                          int *size, int *bdberr)
{
    if (lua_ver == 0) {
        if ((lua_ver = bdb_get_sp_get_default_version_tran(tran, sp_name, bdberr)) <= 0) {
            *bdberr = BDBERR_BADARGS;
            return -1;
        }
    }
    uint8_t key[LLMETA_IXLEN] = {0};
    *bdberr = BDBERR_NOERROR;
    if (make_sp_key(key, sp_name, lua_ver) != 0) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }
    return bdb_lite_exact_var_fetch_tran(llmeta_bdb_state, tran, key, (void **)lua_file, size, bdberr);
}

int bdb_get_sp_get_default_version_tran(tran_type *tran, const char *sp_name, int *bdberr)
{

    int rc;
    int size;
    uint8_t key[LLMETA_IXLEN] = {0};

    *bdberr = BDBERR_NOERROR;

    if (make_sp_key(key, sp_name, 0) != 0) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    int default_ver;
    int default_version;
    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, tran, key, &default_ver, sizeof(int), &size, bdberr);
    buf_get(&default_version, sizeof(default_version), (uint8_t *)&default_ver,
            ((uint8_t *)&default_ver) + sizeof(default_ver));
    if (rc)
        return rc;

    if (default_version < 1 || (size != sizeof(default_version)))
        return -1;

    return default_version;
}

int bdb_get_sp_get_default_version(const char *sp_name, int *bdberr)
{
    return bdb_get_sp_get_default_version_tran(NULL, sp_name, bdberr);
}

int bdb_set_sp_lua_source(bdb_state_type *bdb_state, tran_type *tran,
                          const char *sp_name, char *lua_file, int size,
                          int version, int *bdberr)
{
    int rc;
    int started_our_own_transaction = 0;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_file_type_spname_lua_vers_key file_type_key;
    uint8_t *p_buf = (uint8_t *)key, *p_buf_end = (p_buf + LLMETA_IXLEN);

    *bdberr = BDBERR_NOERROR;

    if (tran == NULL) {
        started_our_own_transaction = 1;
        tran = bdb_tran_begin(llmeta_bdb_state->parent, NULL, bdberr);
        if (tran == NULL)
            return -1;
    }

    file_type_key.file_type = LLMETA_SP_LUA_SOURCE;
    file_type_key.spname_len = strlen(sp_name) + 1;
    file_type_key.lua_vers = 0;
    if (file_type_key.spname_len > LLMETA_SPLEN)
        return -1;
    strncpy0(file_type_key.spname, sp_name, LLMETA_SPLEN);

    int lua_ver;
    if (version == 0) {
        rc = bdb_get_lua_highest(NULL, sp_name, &lua_ver, INT_MAX, bdberr);

        if (rc || lua_ver == 0) {
            if (*bdberr == BDBERR_FETCH_DTA || lua_ver == 0) {
                file_type_key.lua_vers = 1;
                if (!(llmeta_file_type_spname_lua_vers_key_put(
                        &(file_type_key), p_buf, p_buf_end))) {
                    logmsg(LOGMSG_ERROR, "%s: "
                                    "llmeta_file_type_spname_lua_vers_key_put "
                                    "returns NULL\n",
                            __func__);
                    *bdberr = BDBERR_BADARGS;
                    rc = -1;
                    goto done;
                }

                rc = bdb_lite_add(llmeta_bdb_state, tran, lua_file,
                                  strlen(lua_file) + 1, key, bdberr);

                file_type_key.lua_vers = 0;
                if (!(llmeta_file_type_spname_lua_vers_key_put(
                        &(file_type_key), p_buf, p_buf_end))) {
                    logmsg(LOGMSG_ERROR, "%s: "
                                    "llmeta_file_type_spname_lua_vers_key_put "
                                    "returns NULL\n",
                            __func__);
                    *bdberr = BDBERR_BADARGS;
                    rc = -1;
                    goto done;
                }

                int lua_ver = 1;

                int lua_version;
                buf_put(&lua_ver, sizeof(lua_ver), (uint8_t *)&lua_version,
                        ((uint8_t *)&lua_version) + sizeof(lua_version));

                rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, bdberr);
                if (rc && *bdberr != BDBERR_DEL_DTA)
                    goto done;
                rc = bdb_lite_add(llmeta_bdb_state, tran, &lua_version,
                                  sizeof(lua_version), key, bdberr);

                goto done;
            } else
                goto done;
        }
    } else {
        lua_ver = version - 1;
    }

    file_type_key.lua_vers = lua_ver + 1;

    if (!(llmeta_file_type_spname_lua_vers_key_put(&(file_type_key), p_buf,
                                                   p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_spname_lua_vers_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        rc = -1;
        goto done;
    }

    rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, bdberr);
    if (rc && *bdberr != BDBERR_DEL_DTA)
        goto done;

    rc = bdb_lite_add(llmeta_bdb_state, tran, lua_file, size, key, bdberr);

done:
    if (started_our_own_transaction) {
        if (rc == 0)
            rc = bdb_tran_commit(llmeta_bdb_state->parent, tran, bdberr);
        else {
            int arc;
            arc = bdb_tran_abort(llmeta_bdb_state->parent, tran, bdberr);
            if (arc)
                rc = arc;
        }
    }
    logmsg(LOGMSG_INFO, "Added SP %s:%d\n", sp_name, lua_ver + 1);
    return rc;
}

static int bdb_del_default_sp(tran_type *tran, char *name, int *bdberr)
{
    uint8_t key[LLMETA_IXLEN] = {0};
    if (make_sp_key(key, name, 0) != 0) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }
    int rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, bdberr);
    if (rc && *bdberr == BDBERR_DEL_DTA)
        return 0;
    return rc;
}

int bdb_set_sp_lua_default(bdb_state_type *bdb_state, tran_type *tran,
                           char *sp_name, int lua_ver, int *bdberr)
{
    int started_our_own_transaction = 0;
    uint8_t key[LLMETA_IXLEN] = {0};
    *bdberr = BDBERR_NOERROR;
    if (tran == NULL) {
        started_our_own_transaction = 1;
        tran = bdb_tran_begin(llmeta_bdb_state->parent, NULL, bdberr);
        if (tran == NULL)
            return -1;
    }
    int rc;
    if ((rc = bdb_del_default_sp(tran, sp_name, bdberr)) != 0)
        goto done;
    if ((rc = bdb_del_default_versioned_sp(tran, sp_name)) != 0)
        goto done;
    if ((rc = make_sp_key(key, sp_name, 0)) != 0) {
        *bdberr = BDBERR_BADARGS;
        goto done;
    }
    int lua_version;
    buf_put(&lua_ver, sizeof(lua_ver), (uint8_t *)&lua_version,
            ((uint8_t *)&lua_version) + sizeof(lua_version));
    rc = bdb_lite_add(llmeta_bdb_state, tran, &lua_version, sizeof(lua_version),
                      key, bdberr);
done:
    if (started_our_own_transaction) {
        if (rc == 0)
            rc = bdb_tran_commit(llmeta_bdb_state->parent, tran, bdberr);
        else {
            int arc;
            arc = bdb_tran_abort(llmeta_bdb_state->parent, tran, bdberr);
            if (arc)
                rc = arc;
        }
    }
    if (rc == 0)
        logmsg(LOGMSG_INFO, "Default SP %s:%d\n", sp_name, lua_ver);
    else
        logmsg(LOGMSG_ERROR, "%s %s:%d rc:%d\n", __func__, sp_name, lua_ver, rc);
    return rc;
}

int bdb_delete_sp_lua_source(bdb_state_type *bdb_state, tran_type *tran,
                             const char *sp_name, int lua_ver, int *bdberr)
{
    int rc;
    int started_our_own_transaction = 0;
    uint8_t key[LLMETA_IXLEN] = {0};

    *bdberr = BDBERR_NOERROR;

    if (tran == NULL) {
        started_our_own_transaction = 1;
        tran = bdb_tran_begin(llmeta_bdb_state->parent, NULL, bdberr);
        if (tran == NULL)
            return -1;
    }

    if ((rc = make_sp_key(key, sp_name, lua_ver)) != 0)
        goto done;

    rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, bdberr);
    if (rc && *bdberr == BDBERR_DEL_DTA) {
        rc = 0;
    }

done:
    if (started_our_own_transaction) {
        if (rc == 0)
            rc = bdb_tran_commit(llmeta_bdb_state->parent, tran, bdberr);
        else {
            int arc;
            arc = bdb_tran_abort(llmeta_bdb_state->parent, tran, bdberr);
            if (arc)
                rc = arc;
        }
    }
    if (rc == 0)
        logmsg(LOGMSG_INFO, "Deleted SP %s:%d\n", sp_name, lua_ver);
    else
        logmsg(LOGMSG_ERROR, "%s %s:%d rc:%d\n", __func__, sp_name, lua_ver, rc);
    return rc;
}

struct llmeta_global_stripe_info {
    int stripes;
    int blobstripe;
};

enum { LLMETA_GLOBAL_STRIPE_INFO_LEN = 8 };

BB_COMPILE_TIME_ASSERT(llmeta_global_stripe_info_len,
                       sizeof(struct llmeta_global_stripe_info) ==
                           LLMETA_GLOBAL_STRIPE_INFO_LEN);

static const uint8_t *llmeta_global_stripe_info_get(
    struct llmeta_global_stripe_info *p_global_stripe_info,
    const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_FILE_TYPE_KEY_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf = buf_get(&(p_global_stripe_info->stripes),
                    sizeof(p_global_stripe_info->stripes), p_buf, p_buf_end);
    p_buf = buf_get(&(p_global_stripe_info->blobstripe),
                    sizeof(p_global_stripe_info->blobstripe), p_buf, p_buf_end);
    return p_buf;
}

static uint8_t *llmeta_global_stripe_info_put(
    const struct llmeta_global_stripe_info *p_global_stripe_info,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LLMETA_FILE_TYPE_KEY_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf = buf_put(&(p_global_stripe_info->stripes),
                    sizeof(p_global_stripe_info->stripes), p_buf, p_buf_end);
    p_buf = buf_put(&(p_global_stripe_info->blobstripe),
                    sizeof(p_global_stripe_info->blobstripe), p_buf, p_buf_end);
    return p_buf;
}

int bdb_get_global_stripe_info(tran_type *tran, int *stripes, int *blobstripe,
                               int *bdberr)
{
    int rc;
    char buf[LLMETA_GLOBAL_STRIPE_INFO_LEN];
    struct llmeta_global_stripe_info stripe_info;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_file_type_key file_type_key;
    int fndlen;
    uint8_t *p_buf = (uint8_t *)key, *p_buf_end = (p_buf + LLMETA_IXLEN);

    *bdberr = BDBERR_NOERROR;

    file_type_key.file_type = LLMETA_GLOBAL_STRIPE_INFO;

    if (!(llmeta_file_type_key_put(&(file_type_key), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
               __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, tran, key, buf,
                                   LLMETA_GLOBAL_STRIPE_INFO_LEN, &fndlen,
                                   bdberr);

    p_buf = (uint8_t *)buf;
    p_buf_end = (uint8_t *)buf + LLMETA_GLOBAL_STRIPE_INFO_LEN;

    if (!(llmeta_global_stripe_info_get(&stripe_info, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_global_stripe_info_get returns NULL\n",
               __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (rc == 0) {
        *stripes = stripe_info.stripes;
        *blobstripe = stripe_info.blobstripe;
    } else {
        *stripes = -1;
        *blobstripe = -1;
    }

    return 0;
}

int bdb_set_global_stripe_info(tran_type *tran, int stripes, int blobstripe,
                               int *bdberr)
{
    int rc;
    int started_our_own_transaction = 0;
    uint8_t buf[LLMETA_GLOBAL_STRIPE_INFO_LEN];
    struct llmeta_global_stripe_info stripe_info;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_file_type_key file_type_key;
    uint8_t *p_buf = (uint8_t *)key, *p_buf_end = (p_buf + LLMETA_IXLEN);

    *bdberr = BDBERR_NOERROR;

    if (tran == NULL) {
        started_our_own_transaction = 1;
        tran = bdb_tran_begin(llmeta_bdb_state->parent, NULL, bdberr);
        if (tran == NULL)
            return -1;
    }

    file_type_key.file_type = LLMETA_GLOBAL_STRIPE_INFO;

    if (!(llmeta_file_type_key_put(&(file_type_key), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
               __func__);
        *bdberr = BDBERR_BADARGS;
        rc = -1;
        goto done;
    }

    stripe_info.stripes = stripes;
    stripe_info.blobstripe = blobstripe;

    p_buf = buf;
    p_buf_end = buf + LLMETA_GLOBAL_STRIPE_INFO_LEN;
    if (!(llmeta_global_stripe_info_put(&stripe_info, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_global_stripe_info_put returns NULL\n",
               __func__);
        *bdberr = BDBERR_BADARGS;
        rc = -1;
        goto done;
    }

    rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, bdberr);
    if (rc && *bdberr != BDBERR_DEL_DTA)
        goto done;

    stripes = htonl(stripes);

    rc = bdb_lite_add(llmeta_bdb_state, tran, buf,
                      LLMETA_GLOBAL_STRIPE_INFO_LEN, key, bdberr);

done:
    if (started_our_own_transaction) {
        if (rc == 0)
            rc = bdb_tran_commit(llmeta_bdb_state->parent, tran, bdberr);
        else {
            int arc;
            arc = bdb_tran_abort(llmeta_bdb_state->parent, tran, bdberr);
            if (arc)
                rc = arc;
        }
    }

    return rc;
}

int bdb_get_num_sc_done(bdb_state_type *bdb_state, tran_type *tran,
                        unsigned long long *num, int *bdberr)
{
    int rc;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_file_type_key file_type_key;
    int fndlen;
    *num = 0;
    uint8_t *p_buf = (uint8_t *)key, *p_buf_end = (p_buf + LLMETA_IXLEN);
    unsigned long long tmpnum;

    *bdberr = BDBERR_NOERROR;

    file_type_key.file_type = LLMETA_TABLE_NUM_SC_DONE;

    if (!(llmeta_file_type_key_put(&(file_type_key), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
               __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, tran, key, &tmpnum,
                                   sizeof(tmpnum), &fndlen, bdberr);

    tmpnum = flibc_ntohll(tmpnum);

    if (rc == 0)
        *num = tmpnum;
    else
        *num = 0;
    return 0;
}

int bdb_increment_num_sc_done(bdb_state_type *bdb_state, tran_type *tran,
                              int *bdberr)
{
    int rc;
    char key[LLMETA_IXLEN] = {0};
    unsigned long long num = 0;

    struct llmeta_file_type_key file_type_key;
    uint8_t *p_buf = (uint8_t *)key, *p_buf_end = (p_buf + LLMETA_IXLEN);

    *bdberr = BDBERR_NOERROR;

    if (tran == NULL) {
        /* This function needs to be called with a parent transaction.*/
        return -1;
    }

    file_type_key.file_type = LLMETA_TABLE_NUM_SC_DONE;

    if (!(llmeta_file_type_key_put(&(file_type_key), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
               __func__);
        *bdberr = BDBERR_BADARGS;
        rc = -1;
        goto done;
    }

    rc = bdb_get_num_sc_done(bdb_state, tran, &num, bdberr);
    if (rc) {
        if (*bdberr == BDBERR_FETCH_DTA) {
            num = 1;
            num = flibc_htonll(num);
            rc = bdb_lite_add(llmeta_bdb_state, tran, &num,
                              sizeof(unsigned long long), key, bdberr);
            goto done;
        } else
            goto done;
    }

    rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, bdberr);
    if (rc && *bdberr != BDBERR_DEL_DTA)
        goto done;

    num++;

    num = flibc_htonll(num);

    rc = bdb_lite_add(llmeta_bdb_state, tran, &num, sizeof(unsigned long long),
                      key, bdberr);

done:
    return rc;
}

int bdb_get_sc_seed(bdb_state_type *bdb_state, tran_type *tran,
                    const char *tablename, unsigned long long *genid,
                    unsigned int *host, int *bdberr)
{
    int rc;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_schema_change_type schema_change;
    int fndlen;
    uint8_t *p_buf = (uint8_t *)key, *p_buf_end = (p_buf + LLMETA_IXLEN);
    int data_sz = sizeof(unsigned long long) + sizeof(unsigned int);
    uint8_t *data_buf = alloca(data_sz);

    *bdberr = BDBERR_NOERROR;

    schema_change.file_type = LLMETA_SC_SEEDS;
    /*copy the table name and check its length so that we have a clean key*/
    strncpy0(schema_change.dbname, tablename, sizeof(schema_change.dbname));
    schema_change.dbname_len = strlen(schema_change.dbname) + 1;

    if (!(llmeta_schema_change_type_put(&(schema_change), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_schema_change_type_put returns NULL\n",
               __func__);
        logmsg(LOGMSG_ERROR, "%s: check the length of table: %s\n", __func__,
               tablename);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, tran, key, data_buf,
                                   data_sz, &fndlen, bdberr);
    if (rc == 0) {
        *genid = *(unsigned long long *)data_buf;
        *host = ntohl(*(unsigned int *)(data_buf + sizeof(unsigned long long)));
    }
    return rc;
}

int bdb_set_sc_seed(bdb_state_type *bdb_state, tran_type *tran,
                    const char *tablename, unsigned long long genid,
                    unsigned int host, int *bdberr)
{
    int rc;
    int started_our_own_transaction = 0;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_schema_change_type schema_change;
    uint8_t *p_buf = (uint8_t *)key, *p_buf_end = (p_buf + LLMETA_IXLEN);

    int data_sz = sizeof(unsigned long long) + sizeof(unsigned int);
    uint8_t *data_buf = alloca(data_sz);
    *(unsigned long long *)data_buf = genid;
    *(unsigned int *)(data_buf + sizeof(unsigned long long)) = htonl(host);

    *bdberr = BDBERR_NOERROR;

    if (tran == NULL) {
        started_our_own_transaction = 1;
        tran = bdb_tran_begin(llmeta_bdb_state->parent, NULL, bdberr);
        if (tran == NULL) {
            logmsg(LOGMSG_ERROR, "%s: bdb_tran_begin returns NULL\n", __func__);
            return -1;
        }
    }

    schema_change.file_type = LLMETA_SC_SEEDS;
    /*copy the table name and check its length so that we have a clean key*/
    strncpy0(schema_change.dbname, tablename, sizeof(schema_change.dbname));
    schema_change.dbname_len = strlen(schema_change.dbname) + 1;

    if (!(llmeta_schema_change_type_put(&(schema_change), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_schema_change_type_put returns NULL\n",
               __func__);
        logmsg(LOGMSG_ERROR, "%s: check the length of table: %s\n", __func__,
               tablename);
        *bdberr = BDBERR_BADARGS;
        rc = -1;
        goto done;
    }

    rc = bdb_get_sc_seed(bdb_state, tran, tablename, &genid, &host, bdberr);
    if (rc) { //not found, just add -- should refactor
        if (*bdberr == BDBERR_FETCH_DTA) {
            rc = bdb_lite_add(llmeta_bdb_state, tran, data_buf, data_sz, key,
                              bdberr);
        } 
        goto done;
    }

    rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, bdberr);
    if (rc && *bdberr != BDBERR_DEL_DTA)
        goto done;

    rc = bdb_lite_add(llmeta_bdb_state, tran, data_buf, data_sz, key, bdberr);

done:
    if (started_our_own_transaction) {
        if (rc == 0)
            rc = bdb_tran_commit(llmeta_bdb_state->parent, tran, bdberr);
        else {
            int arc;
            arc = bdb_tran_abort(llmeta_bdb_state->parent, tran, bdberr);
            if (arc)
                rc = arc;
        }
    }
    return rc;
}

int bdb_delete_sc_seed(bdb_state_type *bdb_state, tran_type *tran,
                       const char *tablename, int *bdberr)
{
    int rc;
    int started_our_own_transaction = 0;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_schema_change_type schema_change;
    uint8_t *p_buf = (uint8_t *)key, *p_buf_end = (p_buf + LLMETA_IXLEN);

    *bdberr = BDBERR_NOERROR;

    if (tran == NULL) {
        started_our_own_transaction = 1;
        tran = bdb_tran_begin(llmeta_bdb_state->parent, NULL, bdberr);
        if (tran == NULL)
            return -1;
    }

    schema_change.file_type = LLMETA_SC_SEEDS;
    /*copy the table name and check its length so that we have a clean key*/
    strncpy0(schema_change.dbname, tablename, sizeof(schema_change.dbname));
    schema_change.dbname_len = strlen(schema_change.dbname) + 1;

    if (!(llmeta_schema_change_type_put(&(schema_change), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_schema_change_type_put returns NULL\n",
               __func__);
        logmsg(LOGMSG_ERROR, "%s: check the length of table: %s\n", __func__,
               tablename);
        *bdberr = BDBERR_BADARGS;
        rc = -1;
        goto done;
    }

    rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, bdberr);
    if (*bdberr == BDBERR_DEL_DTA) {
        rc = 0;
        *bdberr = BDBERR_NOERROR;
    }

done:
    if (started_our_own_transaction) {
        if (rc == 0)
            rc = bdb_tran_commit(llmeta_bdb_state->parent, tran, bdberr);
        else {
            int arc;
            arc = bdb_tran_abort(llmeta_bdb_state->parent, tran, bdberr);
            if (arc)
                rc = arc;
        }
    }
    return rc;
}

int bdb_set_file_lwm(bdb_state_type *bdb_state, tran_type *tran, DB_LSN *lsn,
                     int *bdberr)
{
    int rc;
    char key[LLMETA_IXLEN] = {0};
    int started_our_own_transaction = 0;
    DB_LSN oldlsn;
    DB_LSN tmplsn;
    struct llmeta_file_type_key file_type_key;
    struct llmeta_db_lsn_data_type lsn_data_type;
    uint8_t *p_buf, *p_buf_end, *p_data_buf, *p_data_buf_end;

    *bdberr = BDBERR_NOERROR;

    file_type_key.file_type = LLMETA_LOGICAL_LSN_LWM;
    p_buf = (uint8_t *)key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    if (!(llmeta_file_type_key_put(&(file_type_key), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (tran == NULL) {
        started_our_own_transaction = 1;
        tran = bdb_tran_begin(llmeta_bdb_state->parent, NULL, bdberr);
        if (tran == NULL)
            return -1;
    }

    lsn_data_type.lsn.file = lsn->file;
    lsn_data_type.lsn.offset = lsn->offset;

    p_data_buf = (uint8_t *)&tmplsn;
    p_data_buf_end = p_data_buf + sizeof(tmplsn);

    if (!(llmeta_db_lsn_data_type_put(&lsn_data_type, p_data_buf,
                                      p_data_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_db_lsn_data_type_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    rc = bdb_get_file_lwm(bdb_state, tran, &oldlsn, bdberr);
    if (rc) {
        if (*bdberr == BDBERR_FETCH_DTA) {
            /* didn't find it, so just add it here */
            logmsg(LOGMSG_DEBUG, "****** [FIRST] lwm %u:%u\n", tmplsn.file, tmplsn.offset);
            rc = bdb_lite_add(llmeta_bdb_state, tran, &tmplsn, sizeof(tmplsn),
                              key, bdberr);
            goto done;
        } else
            goto done;
    }
    /* found it - delete old value */
    rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, bdberr);
    if (rc && *bdberr != BDBERR_DEL_DTA) {
        logmsg(LOGMSG_ERROR, "bdb_lite_exact_del rc %d bdberr %d\n", rc, *bdberr);
        goto done;
    }
    /* now add */
    rc = bdb_lite_add(llmeta_bdb_state, tran, &tmplsn, sizeof(tmplsn), key,
                      bdberr);
    if (rc)
        logmsg(LOGMSG_ERROR, "add lwm rc %d\n", rc);
done:
    /* if we started our own transaction, make sure we commit/abort it */
    if (started_our_own_transaction) {
        if (rc == 0)
            rc = bdb_tran_commit(llmeta_bdb_state->parent, tran, bdberr);
        else
            rc = bdb_tran_abort(llmeta_bdb_state->parent, tran, bdberr);
    }
    return rc;
}

/******************* TABLE ACCESS SET METHODS ***************************/

static int bdb_tbl_access_set(bdb_state_type *bdb_state, tran_type *input_trans,
                              const char *tblname, const char *username, int access_mode,
                              int *bdberr)
{
    uint8_t key[LLMETA_IXLEN] = {0};
    int rc;
    struct llmeta_tbl_access tbl_access_data = {0};
    uint8_t *p_buf, *p_buf_start = NULL, *p_buf_end;
    tran_type *trans;
    int retries = 0;
    int prev_bdberr;

    p_buf = key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    switch (access_mode) {
    case ACCESS_READ:
        tbl_access_data.file_type = LLMETA_TABLE_USER_READ;
        break;
    case ACCESS_WRITE:
        tbl_access_data.file_type = LLMETA_TABLE_USER_WRITE;
        break;
    case ACCESS_USERSCHEMA:
        tbl_access_data.file_type = LLMETA_TABLE_USER_SCHEMA;
        break;
    }

    strncpy0(tbl_access_data.tablename, tblname,
             sizeof(tbl_access_data.tablename));

    strncpy0(tbl_access_data.username, username,
             sizeof(tbl_access_data.username));

    /* form llmeta record with file_type endianized */
    if (!(llmeta_tbl_access_put(&tbl_access_data, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get "
                            "transaction\n",
                    __func__);
            free(p_buf_start);
            p_buf = NULL;
            return -1;
        }
    } else
        trans = input_trans;

    /* add the record to llmeta */
    rc = bdb_lite_add(llmeta_bdb_state, trans, NULL, 0, key, bdberr);

    if (rc && *bdberr != BDBERR_NOERROR)
        goto backout;

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with "
                            "bdberr %d\n",
                    __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }
    return -1;
}

int bdb_tbl_access_write_set(bdb_state_type *bdb_state, tran_type *input_trans,
                             char *tblname, char *username, int *bdberr)
{
    return bdb_tbl_access_set(bdb_state, input_trans, tblname, username,
                              ACCESS_WRITE, bdberr);
}

int bdb_tbl_access_read_set(bdb_state_type *bdb_state, tran_type *input_trans,
                            char *tblname, char *username, int *bdberr)
{
    return bdb_tbl_access_set(bdb_state, input_trans, tblname, username,
                              ACCESS_READ, bdberr);
}

int bdb_tbl_access_userschema_set(bdb_state_type *bdb_state,
                                  tran_type *input_trans, const char *schema,
                                  const char *username, int *bdberr)
{
    /* Adding username infront to make it search for partial key. */
    return bdb_tbl_access_set(bdb_state, input_trans, username, schema,
                              ACCESS_USERSCHEMA, bdberr);
}
/**********************************************************************/

/**************** TABLE OP (DDL etc) ACCESS SET METHODS ***************/

int bdb_tbl_op_access_set(bdb_state_type *bdb_state, tran_type *input_trans,
                          int command_type, const char *tblname, const char *username,
                          int *bdberr)
{
    uint8_t key[LLMETA_IXLEN] = {0};
    int rc;
    struct llmeta_tbl_op_access tbl_access_data = {0};
    uint8_t *p_buf, *p_buf_start = NULL, *p_buf_end;
    tran_type *trans;
    int retries = 0;
    int prev_bdberr;
    command_type = 0; // OVERRIDES PARAMETER BECAUSE WE DO NOT CHECK THE COMMAND YET
    p_buf = key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    tbl_access_data.file_type = LLMETA_TABLE_USER_OP;

    tbl_access_data.command_type = command_type;

    strncpy0(tbl_access_data.tablename, tblname,
             sizeof(tbl_access_data.tablename));

    strncpy0(tbl_access_data.username, username,
             sizeof(tbl_access_data.username));

    /* form llmeta record with file_type endianized */
    if (!(llmeta_tbl_op_access_put(&tbl_access_data, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get "
                            "transaction\n",
                    __func__);
            free(p_buf_start);
            p_buf = NULL;
            return -1;
        }
    } else
        trans = input_trans;

    /* add the record to llmeta */
    rc = bdb_lite_add(llmeta_bdb_state, trans, NULL, 0, key, bdberr);

    if (rc && *bdberr != BDBERR_NOERROR)
        goto backout;

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with "
                            "bdberr %d\n",
                    __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }
    return -1;
}

int bdb_tbl_op_access_get(bdb_state_type *bdb_state, tran_type *input_trans,
                          int command_type, const char *tblname, const char *username,
                          int *bdberr)
{
    uint8_t key[LLMETA_IXLEN] = {0};
    int fndlen, rc;
    struct llmeta_tbl_op_access tbl_access_data = {0};
    uint8_t *p_buf, *p_buf_end;
    command_type =
        0; // OVERRIDES PARAMETER BECAUSE WE DO NOT CHECK THE COMMAND YET
    p_buf = key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    tbl_access_data.file_type = LLMETA_TABLE_USER_OP;
    tbl_access_data.command_type = command_type;
    strncpy0(tbl_access_data.tablename, tblname,
             sizeof(tbl_access_data.tablename));

    strncpy0(tbl_access_data.username, username,
             sizeof(tbl_access_data.username));

    /* form llmeta record with file_type endianized */
    if (!(llmeta_tbl_op_access_put(&tbl_access_data, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, input_trans, key, NULL, 0,
                                   &fndlen, bdberr);

    return rc;
}

int bdb_tbl_op_access_delete(bdb_state_type *bdb_state, tran_type *input_trans,
                             int command_type, const char *tblname, const char *username,
                             int *bdberr)
{
    uint8_t key[LLMETA_IXLEN] = {0};
    struct llmeta_tbl_op_access tbl_access_data = {0};
    tran_type *trans;
    uint8_t *p_buf, *p_buf_end;
    int rc;
    int retries = 0;
    int prev_bdberr = 0;

    p_buf = key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    tbl_access_data.file_type = LLMETA_TABLE_USER_OP;
    tbl_access_data.command_type = command_type;
    strncpy0(tbl_access_data.tablename, tblname,
             sizeof(tbl_access_data.tablename));

    strncpy0(tbl_access_data.username, username,
             sizeof(tbl_access_data.username));

    /* form llmeta record with file_type endianized */
    if (!(llmeta_tbl_op_access_put(&tbl_access_data, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get "
                            "transaction\n",
                    __func__);
            return -1;
        }
    } else
        trans = input_trans;

    /* add the record to llmeta */
    rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key, bdberr);

    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_DEL_DTA)
        goto backout;

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
    }

    /* invalidate any caches */
    bdb_access_tbl_invalidate(bdb_state);

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with "
                            "bdberr %d\n",
                    __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }

    return -1;
}

/**********************************************************************/

/* call this to enable authentication on a database */
int bdb_feature_set_int(bdb_state_type *bdb_state, tran_type *input_trans, int *bdberr, int add, int file_type)
{
    uint8_t key[LLMETA_IXLEN + sizeof(uint8_t)] = {0};
    int rc;
    struct llmeta_authentication authentication_data = {0};
    uint8_t *p_buf, *p_buf_start = NULL, *p_buf_end;
    tran_type *trans;
    int retries = 0;
    int prev_bdberr;

    p_buf = key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    authentication_data.file_type = file_type;

    /* form llmeta record with file_type endianized */
    if (!(llmeta_authentication_put(&authentication_data, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get "
                            "transaction\n",
                    __func__);
            free(p_buf_start);
            p_buf = NULL;
            return -1;
        }
    } else
        trans = input_trans;

    if (add) {
        /* add the record to llmeta */
        rc = bdb_lite_add(llmeta_bdb_state, trans, NULL, 0, key, bdberr);
    } else {
        rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key, bdberr);
    }

    if (rc && *bdberr != BDBERR_NOERROR)
        goto backout;

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with "
                            "bdberr %d\n",
                    __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }

    return -1;
}

int bdb_authentication_set(bdb_state_type *bdb_state, tran_type *input_trans, int enabled,
                           int *bdberr)
{
    return bdb_feature_set_int(bdb_state, input_trans, bdberr, enabled,
                               LLMETA_AUTHENTICATION);
}
int bdb_accesscontrol_tableXnode_set(bdb_state_type *bdb_state,
                                     tran_type *input_trans, int *bdberr)
{
    return bdb_feature_set_int(bdb_state, input_trans, bdberr, 1,
                               LLMETA_ACCESSCONTROL_TABLExNODE);
}

/* does db use authentication? */
static int bdb_feature_get_int(bdb_state_type *bdb_state, tran_type *tran,
                               int *bdberr, int file_type)
{
    int rc;
    uint8_t key[LLMETA_IXLEN] = {0};
    struct llmeta_authentication authentication_data = {0};
    int fndlen;
    uint8_t *p_buf, *p_buf_end;

    *bdberr = BDBERR_NOERROR;

    p_buf = key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    authentication_data.file_type = file_type;

    /* form llmeta record with file_type endianized */
    if (!(llmeta_authentication_put(&authentication_data, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, tran, key, NULL, 0,
                                   &fndlen, bdberr);

    return rc;
}

int bdb_authentication_get(bdb_state_type *bdb_state, tran_type *tran,
                           int *bdberr)
{
    if (gbl_disable_access_controls) {
        logmsg(LOGMSG_WARN, "**Bypassing user authentication**\n");
        return 1;
    }
    return bdb_feature_get_int(bdb_state, tran, bdberr, LLMETA_AUTHENTICATION);
}

int bdb_accesscontrol_tableXnode_get(bdb_state_type *bdb_state, tran_type *tran,
                                     int *bdberr)
{
    return bdb_feature_get_int(bdb_state, tran, bdberr,
                               LLMETA_ACCESSCONTROL_TABLExNODE);
}

static int bdb_tbl_access_get(bdb_state_type *bdb_state, tran_type *input_trans,
                              char *tblname, char *username, int access_mode,
                              int *bdberr)
{
    uint8_t key[LLMETA_IXLEN] = {0};
    int fndlen, rc;
    struct llmeta_tbl_access tbl_access_data = {0};
    uint8_t *p_buf, *p_buf_end;

    p_buf = key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    switch (access_mode) {
    case ACCESS_READ:
        tbl_access_data.file_type = LLMETA_TABLE_USER_READ;
        break;
    case ACCESS_WRITE:
        tbl_access_data.file_type = LLMETA_TABLE_USER_WRITE;
        break;
    }

    strncpy0(tbl_access_data.tablename, tblname,
             sizeof(tbl_access_data.tablename));

    strncpy0(tbl_access_data.username, username,
             sizeof(tbl_access_data.username));

    /* form llmeta record with file_type endianized */
    if (!(llmeta_tbl_access_put(&tbl_access_data, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, input_trans, key, NULL, 0,
                                   &fndlen, bdberr);

    return rc;
}

int bdb_tbl_access_write_get(bdb_state_type *bdb_state, tran_type *input_trans,
                             char *tblname, char *username, int *bdberr)
{
    return bdb_tbl_access_get(bdb_state, input_trans, tblname, username,
                              ACCESS_WRITE, bdberr);
}

int bdb_tbl_access_read_get(bdb_state_type *bdb_state, tran_type *input_trans,
                            char *tblname, char *username, int *bdberr)
{
    return bdb_tbl_access_get(bdb_state, input_trans, tblname, username,
                              ACCESS_READ, bdberr);
}

int bdb_tbl_access_userschema_get(bdb_state_type *bdb_state,
                                  tran_type *input_trans, const char *username,
                                  char *userschema, int *bdberr)
{
    uint8_t key[LLMETA_IXLEN] = {0};
    uint8_t fndkey[LLMETA_IXLEN] = {0};
    int fndlen, rc;
    struct llmeta_tbl_access tbl_access_data = {0};
    uint8_t *p_buf, *p_buf_end;

    p_buf = key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    tbl_access_data.file_type = LLMETA_TABLE_USER_SCHEMA;

    strncpy0(tbl_access_data.tablename, username,
             sizeof(tbl_access_data.tablename));

    bzero(tbl_access_data.username, sizeof(tbl_access_data.username));

    /* form llmeta record with file_type endianized */
    if (!(llmeta_tbl_access_put(&tbl_access_data, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* There can be at most 1 mapping. */
    rc = bdb_lite_fetch_partial(llmeta_bdb_state, key,
                                sizeof(tbl_access_data) -
                                    sizeof(tbl_access_data.username),
                                fndkey, &fndlen, bdberr);

    if (fndlen) {
        p_buf = fndkey;
        p_buf_end = p_buf + LLMETA_IXLEN;
        /* form llmeta record with file_type endianized */
        if (!(llmeta_tbl_access_get(&tbl_access_data, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
                    __func__);
            *bdberr = BDBERR_BADARGS;
            return -1;
        }
        strncpy0(userschema, tbl_access_data.username,
                 sizeof(tbl_access_data.username));
        logmsg(LOGMSG_INFO, "User Schema for username %s is %s\n", username,
               userschema);
    } else {
        rc = -1;
    }
    return rc;
}

static int bdb_tbl_access_delete(bdb_state_type *bdb_state,
                                 tran_type *input_trans, const char *tblname,
                                 const char *username, int access_mode, int *bdberr)
{
    uint8_t key[LLMETA_IXLEN] = {0};
    struct llmeta_tbl_access tbl_access_data = {0};
    tran_type *trans;
    uint8_t *p_buf, *p_buf_end;
    int rc;
    int retries = 0;
    int prev_bdberr = 0;

    p_buf = key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    switch (access_mode) {
    case ACCESS_READ:
        tbl_access_data.file_type = LLMETA_TABLE_USER_READ;
        break;
    case ACCESS_WRITE:
        tbl_access_data.file_type = LLMETA_TABLE_USER_WRITE;
        break;
    case ACCESS_USERSCHEMA:
        tbl_access_data.file_type = LLMETA_TABLE_USER_SCHEMA;
        break;
    }

    strncpy0(tbl_access_data.tablename, tblname,
             sizeof(tbl_access_data.tablename));

    strncpy0(tbl_access_data.username, username,
             sizeof(tbl_access_data.username));

    /* form llmeta record with file_type endianized */
    if (!(llmeta_tbl_access_put(&tbl_access_data, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_file_type_key_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get "
                            "transaction\n",
                    __func__);
            return -1;
        }
    } else
        trans = input_trans;

    /* add the record to llmeta */
    rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key, bdberr);

    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_DEL_DTA)
        goto backout;

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
    }

    /* invalidate any caches */
    bdb_access_tbl_invalidate(bdb_state);

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with "
                            "bdberr %d\n",
                    __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }

    return -1;
}

int bdb_tbl_access_write_delete(bdb_state_type *bdb_state,
                                tran_type *input_trans, const char *tblname,
                                const char *username, int *bdberr)
{
    return bdb_tbl_access_delete(bdb_state, input_trans, tblname, username,
                                 ACCESS_WRITE, bdberr);
}

int bdb_tbl_access_read_delete(bdb_state_type *bdb_state,
                               tran_type *input_trans, const char *tblname,
                               const char *username, int *bdberr)
{
    return bdb_tbl_access_delete(bdb_state, input_trans, tblname, username,
                                 ACCESS_READ, bdberr);
}

int bdb_tbl_access_userschema_delete(bdb_state_type *bdb_state,
                                     tran_type *input_trans, const char *schema,
                                     const char *username, int *bdberr)
{
    return bdb_tbl_access_delete(bdb_state, input_trans, username, schema,
                                 ACCESS_USERSCHEMA, bdberr);
}

int bdb_llmeta_print_record(bdb_state_type *bdb_state, void *key, int keylen,
                            void *data, int datalen, int *bdberr)
{
    int type = 0;
    const uint8_t *p_buf_key;
    const uint8_t *p_buf_end_key;
    const uint8_t *p_buf_data;
    const uint8_t *p_buf_end_data;

    if (keylen <= 0) {
        logmsg(LOGMSG_ERROR, "Invalid entry (keylen=%d)\n", keylen);
    }

    p_buf_key = key;
    p_buf_end_key = (uint8_t *)key + keylen;
    p_buf_data = data;
    p_buf_end_data = (uint8_t *)data + datalen;

    buf_get(&type, sizeof(type), p_buf_key, p_buf_end_key);

    switch (type) {
    case LLMETA_FVER_FILE_TYPE_TBL:
    case LLMETA_FVER_FILE_TYPE_IX:
    case LLMETA_FVER_FILE_TYPE_DTA: {
        struct llmeta_file_type_dbname_file_num_key akey;
        struct llmeta_version_number_type adata = {0};

        if (keylen < sizeof(akey)) {
            logmsg(LOGMSG_USER, "%s:%d: wrong LLMETA_FVER_FILE_TYPE_TBL entry\n",
                    __FILE__, __LINE__);
            *bdberr = BDBERR_MISC;
            return -1;
        }

        p_buf_key = llmeta_file_type_dbname_file_num_get(&akey, p_buf_key,
                                                         p_buf_end_key);
        if (type == LLMETA_FVER_FILE_TYPE_DTA && akey.dbname[0] == 0) {
            /* stupid overloaded type, this one contains the list of the names
             */
            struct llmeta_table_name llmeta_tbl;

           logmsg(LOGMSG_USER, "LLMETA_TBL_NAMES:\n");

            while (p_buf_data < p_buf_end_data) {
                p_buf_data = llmeta_table_name_get(&llmeta_tbl, p_buf_data,
                                                   p_buf_end_data);

               logmsg(LOGMSG_USER, "   table=\"%s\" dbnum=%d\n", llmeta_tbl.table_name,
                       llmeta_tbl.dbnum);
            }
        } else {
            p_buf_data =
                llmeta_version_number_get(&adata, p_buf_data, p_buf_end_data);

            adata.version_num =
                flibc_htonll(adata.version_num); /* duh, not host ? */

           logmsg(LOGMSG_USER, "%s table=\"%s\" filenum=%d version=%llx (%lld)\n",
                   (type == LLMETA_FVER_FILE_TYPE_TBL)
                       ? "LLMETA_FVER_FILE_TYPE_TBL"
                       : ((type == LLMETA_FVER_FILE_TYPE_IX)
                              ? "LLMETA_FVER_FILE_TYPE_IX"
                              : "LLMETA_FVER_FILE_TYPE_DTA|LLMETA_TBL_NAMES"),
                   akey.dbname, akey.file_num, adata.version_num,
                   adata.version_num);
        }
    } break;

    case LLMETA_IN_SCHEMA_CHANGE: {
        struct llmeta_schema_change_type akey;

        if (keylen < sizeof(akey) || datalen < sizeof(unsigned long long)) {
            logmsg(LOGMSG_USER, "%s:%d: wrong LLMETA_IN_SCHEMA_CHANGE entry\n",
                    __FILE__, __LINE__);
            *bdberr = BDBERR_MISC;
            return -1;
        }

        p_buf_key =
            llmeta_schema_change_type_get(&akey, p_buf_key, p_buf_end_key);

       logmsg(LOGMSG_USER, "LLMETA_IN_SCHEMA_CHANGE: table=\"%s\" %s\n", akey.dbname,
               (datalen == 0) ? "done" : "in progress");
    } break;

    case LLMETA_SCHEMACHANGE_STATUS: {
        struct llmeta_schema_change_type schema_change = {0};
        llmeta_sc_status_data sc_status_data = {0};

        if (keylen < sizeof(schema_change) ||
            datalen < sizeof(llmeta_sc_status_data)) {
            logmsg(LOGMSG_USER,
                   "%s:%d: wrong LLMETA_SCHEMACHANGE_STATUS entry\n", __FILE__,
                   __LINE__);
            *bdberr = BDBERR_MISC;
            return -1;
        }

        p_buf_key = llmeta_schema_change_type_get(&schema_change, p_buf_key,
                                                  p_buf_end_key);

        llmeta_sc_status_data_get(&sc_status_data, p_buf_data, p_buf_end_data);

        logmsg(LOGMSG_USER,
               "LLMETA_SCHEMACHANGE_STATUS: table=\"%s\" start=%lld status=%d "
               "last=%lld errstr=\"%s\"\n",
               schema_change.dbname, sc_status_data.start,
               sc_status_data.status, sc_status_data.last,
               sc_status_data.errstr);
    } break;

    case LLMETA_NEWSC_REDO_GENID: {
        if (keylen < sizeof(llmeta_newsc_redo_genid_key) || datalen < sizeof(struct llmeta_db_lsn_data_type)) {
            logmsg(LOGMSG_USER, "%s:%d: wrong LLMETA_NEWSC_REDO_GENID entry\n", __FILE__, __LINE__);
            *bdberr = BDBERR_MISC;
            return -1;
        }
        llmeta_newsc_redo_genid_key k;
        struct llmeta_db_lsn_data_type newsc_lsn = {{0}};
        llmeta_newsc_redo_genid_key_get(&k, p_buf_key, p_buf_end_key);
        llmeta_db_lsn_data_type_get(&newsc_lsn, p_buf_data, p_buf_end_data);

        logmsg(LOGMSG_USER, "LLMETA_NEWSC_REDO_GENID: table=\"%s\" genid=%0#16" PRIx64 " LSN: [%u:%u]\n", k.tablename,
               k.genid, newsc_lsn.lsn.file, newsc_lsn.lsn.offset);
    } break;

    case LLMETA_SCHEMACHANGE_HISTORY: {
        sc_hist_row sc_hist = {0};

        if (keylen < sizeof(struct llmeta_hist_key) ||
            datalen < sizeof(llmeta_sc_hist_data)) {
            logmsg(LOGMSG_USER,
                   "%s:%d: wrong LLMETA_SCHEMACHANGE_HISTORY entry\n", __FILE__,
                   __LINE__);
            *bdberr = BDBERR_MISC;
            return -1;
        }

        struct llmeta_hist_key k;
        llmeta_sc_hist_key_get(&k, p_buf_key, p_buf_end_key);
        llmeta_sc_hist_data_get(&sc_hist, p_buf_data, p_buf_end_data);

        logmsg(LOGMSG_USER,
               "LLMETA_SCHEMACHANGE_HISTORY: table=\"%s\" seed=%0#16" PRIx64
               " start=%" PRIu64 " status=%d "
               "last=%" PRIu64 " errstr=\"%s\"\n",
               k.tablename, k.seed, sc_hist.start, sc_hist.status, sc_hist.last,
               sc_hist.errstr);
    } break;

    case LLMETA_HIGH_GENID: {
        struct llmeta_high_genid_key_type akey;
        unsigned long long genid;

        if (keylen < sizeof(akey) || datalen < sizeof(unsigned long long)) {
            logmsg(LOGMSG_USER, "%s:%d: wrong LLMETA_HIGH_GENID entry\n", __FILE__,
                    __LINE__);
            *bdberr = BDBERR_MISC;
            return -1;
        }

        p_buf_key =
            llmeta_high_genid_key_type_get(&akey, p_buf_key, p_buf_end_key);

        genid = *(unsigned long long *)data;
        genid = flibc_htonll(genid);

        logmsg(LOGMSG_USER, "LLMETA_HIGH_GENID: table=\"%s\" stripe=%d genid=%llx (%lld)\n",
               akey.dbname, akey.stripe, genid, genid);
    } break;

    case LLMETA_CSC2: {
        struct llmeta_file_type_dbname_csc2_vers_key csc2_vers_key;

        p_buf_key = llmeta_file_type_dbname_csc2_vers_key_get(
            &csc2_vers_key, p_buf_key, p_buf_end_key);

        logmsg(LOGMSG_USER, "LLMETA_CSC2: table \"%s\" csc2 version %d\n",
               csc2_vers_key.dbname, csc2_vers_key.csc2_vers);
    } break;
    case LLMETA_PAGESIZE_FILE_TYPE_IX:
        logmsg(LOGMSG_USER, "LLMETA_PAGESIZE_FILE_TYPE_IX\n");
        break;
    case LLMETA_PAGESIZE_FILE_TYPE_DTA:
        logmsg(LOGMSG_USER, "LLMETA_PAGESIZE_FILE_TYPE_DTA\n");
        break;
    case LLMETA_PAGESIZE_FILE_TYPE_BLOB:
        logmsg(LOGMSG_USER, "LLMETA_PAGESIZE_FILE_TYPE_BLOB\n");
        break;
    case LLMETA_PAGESIZE_FILE_TYPE_ALLIX:
        logmsg(LOGMSG_USER, "LLMETA_PAGESIZE_FILE_TYPE_ALLIX\n");
        break;
    case LLMETA_PAGESIZE_FILE_TYPE_ALLDTA:
        logmsg(LOGMSG_USER, "LLMETA_PAGESIZE_FILE_TYPE_ALLDTA\n");
        break;
    case LLMETA_PAGESIZE_FILE_TYPE_ALLBLOB:
        logmsg(LOGMSG_USER, "LLMETA_PAGESIZE_FILE_TYPE_ALLBLOB\n");
        break;
    case LLMETA_LOGICAL_LSN_LWM:
        logmsg(LOGMSG_USER, "LLMETA_LOGICAL_LSN_LWM\n");
        break;
    case LLMETA_SC_SEEDS: {
        struct llmeta_schema_change_type akey;

        if (keylen < sizeof(akey) || datalen < sizeof(unsigned long long)) {
            logmsg(LOGMSG_USER, "%s:%d: wrong LLMETA_IN_SCHEMA_CHANGE entry\n",
                    __FILE__, __LINE__);
            *bdberr = BDBERR_MISC;
            return -1;
        }

        p_buf_key =
            llmeta_schema_change_type_get(&akey, p_buf_key, p_buf_end_key);

       logmsg(LOGMSG_USER, "LLMETA_SC_SEEDS: table=\"%s\" seed=\"0x%llx\" %s\n",
              akey.dbname, *(unsigned long long *)p_buf_data,
               (datalen == 0) ? "done" : "in progress");

    } break;
    case LLMETA_SC_START_LSN: {
        struct llmeta_schema_change_type akey;
        struct llmeta_db_lsn_data_type adata = {{0}};

        if (keylen < sizeof(akey) || datalen < sizeof(adata)) {
            logmsg(LOGMSG_USER, "%s:%d: wrong LLMETA_SC_START_LSN entry\n",
                   __FILE__, __LINE__);
            *bdberr = BDBERR_MISC;
            return -1;
        }

        p_buf_key =
            llmeta_schema_change_type_get(&akey, p_buf_key, p_buf_end_key);

        p_buf_data =
            llmeta_db_lsn_data_type_get(&adata, p_buf_data, p_buf_end_data);

        logmsg(LOGMSG_USER, "LLMETA_SC_START_LSN: table=\"%s\" [%u:%u]\n",
               akey.dbname, adata.lsn.file, adata.lsn.offset);
    } break;
    case LLMETA_TABLE_USER_READ:
    case LLMETA_TABLE_USER_WRITE: {
        struct llmeta_tbl_access akey;

        p_buf_key = llmeta_tbl_access_get(&akey, p_buf_key, p_buf_end_key);

        logmsg(LOGMSG_USER, "%s: clnt=\"%s\" has %s for table=\"%s\"\n",
               (type == LLMETA_TABLE_USER_READ) ? "LLMETA_TABLE_USER_READ"
                                                : "LLMETA_TABLE_USER_WRITE",
               akey.username,
               (type == LLMETA_TABLE_USER_READ) ? "READ" : "WRITE",
               akey.tablename);
    } break;

    case LLMETA_USER_PASSWORD:
        logmsg(LOGMSG_USER, "LLMETA_USER_PASSWORD\n");
        break;
    case LLMETA_AUTHENTICATION:
        logmsg(LOGMSG_USER, "LLMETA_AUTHENTICATION is enabled\n");
        break;
    case LLMETA_ACCESSCONTROL_TABLExNODE:
        logmsg(LOGMSG_USER, "LLMETA_ACCESSCONTROL_TABLExNODE is enabled\n");
        break;
    case LLMETA_SQLITE_STAT1_PREV_DONT_USE:
        logmsg(LOGMSG_USER, "LLMETA_SQLITE_STAT1_PREV_DONT_USE\n");
        break;
    case LLMETA_SQLITE_STAT2_PREV_DONT_USE:
        logmsg(LOGMSG_USER, "LLMETA_SQLITE_STAT2_PREV_DONT_USE\n");
        break;
    case LLMETA_SQLITE_STAT1_PREV:
        logmsg(LOGMSG_USER, "LLMETA_SQLITE_STAT1_PREV\n");
        break;
    case LLMETA_SQLITE_STAT2_PREV:
        logmsg(LOGMSG_USER, "LLMETA_SQLITE_STAT2_PREV\n");
        break;
    case LLMETA_SP_LUA_FILE:
        logmsg(LOGMSG_USER, "LLMETA_SP_LUA_FILE\n");
        break;
    case LLMETA_SP_LUA_SOURCE:
        logmsg(LOGMSG_USER, "LLMETA_SP_LUA_SOURCE\n");
        break;
    case LLMETA_BS:
        logmsg(LOGMSG_USER, "LLMETA_BS\n");
        break;
    case LLMETA_ANALYZECOVERAGE_TABLE:
        logmsg(LOGMSG_USER, "LLMETA_ANALYZECOVERAGE_TABLE\n");
        break;
    case LLMETA_TABLE_VERSION: {
        char tblname[LLMETA_TBLLEN + 1];
        buf_no_net_get(&(tblname),
                 sizeof(tblname), p_buf_key+sizeof(int), p_buf_end_key);
        unsigned long long version = *(unsigned long long *)data;
        logmsg(LOGMSG_USER,
               "LLMETA_TABLE_VERSION table=\"%s\" version=\"%" PRIu64 "\"\n",
               tblname, flibc_ntohll(version));
        } break;
        case LLMETA_TABLE_NUM_SC_DONE: {
            unsigned long long version = *(unsigned long long *)data;
            logmsg(LOGMSG_USER,
                   "LLMETA_TABLE_NUM_SC_DONE version=\"%" PRIu64 "\"\n",
                   flibc_ntohll(version));
        } break;
        case LLMETA_GENID_FORMAT: {
            uint64_t genid_format;
            genid_format = flibc_htonll(*(unsigned long long *)data);
            logmsg(LOGMSG_USER, "LLMETA_GENID_FORMAT %s\n",
                   (genid_format == LLMETA_GENID_ORIGINAL)
                       ? "LLMETA_GENID_ORIGINAL"
                       : (genid_format == LLMETA_GENID_48BIT)
                             ? "LLMETA_GENID_48BIT"
                             : "UNKNOWN GENID FORMAT");
        } break;
        case LLMETA_TABLE_PARAMETERS: {
            char tblname[LLMETA_TBLLEN + 1];
            buf_no_net_get(&(tblname), sizeof(tblname), p_buf_key + sizeof(int),
                           p_buf_end_key);

            logmsg(LOGMSG_USER,
                   "LLMETA_TABLE_PARAMETERS table=\"%s\" value=\"%s\"\n",
                   tblname, (char *)data);
        } break;
        default:
            logmsg(LOGMSG_USER, "Todo (type=%d)\n", type);
            break;
    }
    return 0;
}

int bdb_llmeta_list_records(bdb_state_type *bdb_state, int *bdberr)
{
    int rc = 0;

    if (!bdb_have_llmeta()) {
        logmsg(LOGMSG_ERROR, "%s:%d: No llmeta!\n", __FILE__, __LINE__);
        return 0;
    }

    rc = bdb_lite_list_records(llmeta_bdb_state, bdb_llmeta_print_record,
                               bdberr);

    return rc;
}

struct llmeta_analyzecoverage_key_type {
    int file_type;
    char dbname[LLMETA_TBLLEN + 1];
    int dbname_len;
    int coveragevalue;
};

static uint8_t *llmeta_analyzecoverage_key_type_put(
    const struct llmeta_analyzecoverage_key_type *p_analyzecoverage_key,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{

    if (p_buf_end < p_buf)
        return NULL;
    if (p_analyzecoverage_key->dbname_len >
        sizeof(p_analyzecoverage_key->dbname))
        return NULL;

    p_buf = buf_put(&(p_analyzecoverage_key->file_type),
                    sizeof(p_analyzecoverage_key->file_type), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_analyzecoverage_key->dbname),
                           p_analyzecoverage_key->dbname_len, p_buf, p_buf_end);

    return p_buf;
}

/* returns -1 if coverage is not set for this table 
 * so analyze should use default coverage values
 */
int bdb_get_analyzecoverage_table(tran_type *input_trans, const char *tbl_name,
                                  int *coveragevalue, int *bdberr)
{
    int rc, fndlen, retries = 0;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_analyzecoverage_key_type analyzecoverage_key;
    int tmpval = 0;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;

    /*stop here if the db isn't open*/
    if (!llmeta_bdb_state) {
        *bdberr = BDBERR_DBEMPTY;
        return -1;
    }

    if (!tbl_name || !coveragevalue || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    analyzecoverage_key.file_type = LLMETA_ANALYZECOVERAGE_TABLE;
    strncpy0(analyzecoverage_key.dbname, tbl_name,
             sizeof(analyzecoverage_key.dbname));
    analyzecoverage_key.dbname_len = strlen(analyzecoverage_key.dbname);

    /* set pointers to start and end of buffer */
    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf_start + LLMETA_IXLEN;

    /* write endianized key */
    p_buf = llmeta_analyzecoverage_key_type_put(&analyzecoverage_key, p_buf,
                                                p_buf_end);

retry:
    /* try to fetch the schema change data */
    rc = bdb_lite_exact_fetch(llmeta_bdb_state, key, &tmpval, sizeof(tmpval),
                              &fndlen, bdberr);

    /* tmpval may need to get flipped */
    *coveragevalue = tmpval;

    /* handle return codes */
    if (rc || *bdberr != BDBERR_NOERROR) {

        if (*bdberr == BDBERR_DEADLOCK) {
            /* TODO: input_trans is not used */
            if (++retries < gbl_maxretries) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, 
                    "%s:*ERROR* bdb_lite_exact_fetch too much contention "
                    "%d count %d\n",
                    __func__, *bdberr, retries);
        }

        /* it's ok if no data was found, fail on all other errors*/
        if (*bdberr != BDBERR_FETCH_DTA)
            return -1;

        *coveragevalue = -1; /* set to 0 if we didn't find any data */
    }
    /*if we did not get the right amount of data*/
    else if (fndlen != sizeof(*coveragevalue)) {
        *bdberr = BDBERR_DTA_MISMATCH; /* TODO right error to throw? */
        return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;
}

/* will store a number for the table in question
 * when analyze is run, this coverage value will be used
 * coveragevalue needs to be between 0 and 100 to have meaning
 * if coveragevalue is negative then we are clearing the setting
 */
int bdb_set_analyzecoverage_table(tran_type *input_trans, const char *tbl_name,
                                  int coveragevalue, int *bdberr)
{
    int retries = 0;
    struct llmeta_analyzecoverage_key_type analyzecoverage_key;
    char key[LLMETA_IXLEN] = {0};

    tran_type *trans;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;

    /*fail if the db isn't open*/
    if (!llmeta_bdb_state) {
        logmsg(LOGMSG_ERROR, "%s: file versions db not yet "
                        "open, you must run bdb_open_file_versions\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (!tbl_name || coveragevalue > 100 || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL or inconsistant "
                        "argument\n",
                __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    analyzecoverage_key.file_type = LLMETA_ANALYZECOVERAGE_TABLE;
    strncpy0(analyzecoverage_key.dbname, tbl_name,
             sizeof(analyzecoverage_key.dbname));
    analyzecoverage_key.dbname_len = strlen(analyzecoverage_key.dbname);

    /* set pointers to start and end of buffer */
    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf_start + LLMETA_IXLEN;

    /* write endianized key */
    p_buf = llmeta_analyzecoverage_key_type_put(&analyzecoverage_key, p_buf,
                                                p_buf_end);

    size_t key_offset = p_buf - p_buf_start;

    if (key_offset > sizeof(key)) {
        logmsg(LOGMSG_ERROR, "%s: db_name is too long\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get "
                            "transaction\n",
                    __func__);
            return -1;
        }
    } else
        trans = input_trans;

    /* delete old entry */
    int rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_DEL_DTA)
        goto backout;

    if (coveragevalue >= 0) {
        int tmpval = coveragevalue;
        rc = bdb_lite_add(llmeta_bdb_state, trans, &tmpval, sizeof(tmpval), key,
                          bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            goto backout;
    }

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        int prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with"
                            " bdberr %d\n",
                    __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }
    return -1;
}

int bdb_clear_analyzecoverage_table(tran_type *input_trans,
                                    const char *tbl_name, int *bdberr)
{
    return bdb_set_analyzecoverage_table(input_trans, tbl_name, -1, bdberr);
}

struct llmeta_analyzethreshold_key_type {
    int file_type;
    char dbname[LLMETA_TBLLEN + 1];
    int dbname_len;
    long long threshold;
};

static uint8_t *llmeta_analyzethreshold_key_type_put(
    const struct llmeta_analyzethreshold_key_type *p_analyzethreshold_key,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{

    if (p_buf_end < p_buf)
        return NULL;
    if (p_analyzethreshold_key->dbname_len >
        sizeof(p_analyzethreshold_key->dbname))
        return NULL;

    p_buf =
        buf_put(&(p_analyzethreshold_key->file_type),
                sizeof(p_analyzethreshold_key->file_type), p_buf, p_buf_end);
    p_buf =
        buf_no_net_put(&(p_analyzethreshold_key->dbname),
                       p_analyzethreshold_key->dbname_len, p_buf, p_buf_end);

    return p_buf;
}

int bdb_get_analyzethreshold_table(tran_type *input_trans, const char *tbl_name,
                                   long long *threshold, int *bdberr)
{
    int rc, fndlen, retries = 0;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_analyzethreshold_key_type analyzethreshold_key;
    long long tmpval = 0;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;

    /*stop here if the db isn't open*/
    if (!llmeta_bdb_state) {
        *bdberr = BDBERR_DBEMPTY;
        return -1;
    }

    if (!tbl_name || !threshold || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    analyzethreshold_key.file_type = LLMETA_ANALYZETHRESHOLD_TABLE;
    strncpy0(analyzethreshold_key.dbname, tbl_name,
             sizeof(analyzethreshold_key.dbname));
    analyzethreshold_key.dbname_len = strlen(analyzethreshold_key.dbname);

    /* set pointers to start and end of buffer */
    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf_start + LLMETA_IXLEN;

    /* write endianized key */
    p_buf = llmeta_analyzethreshold_key_type_put(&analyzethreshold_key, p_buf,
                                                 p_buf_end);

retry:
    /* try to fetch the schema change data */
    rc = bdb_lite_exact_fetch(llmeta_bdb_state, key, &tmpval, sizeof(tmpval),
                              &fndlen, bdberr);

    /* tmpval may need to get flipped */
    *threshold = tmpval;

    /* handle return codes */
    if (rc || *bdberr != BDBERR_NOERROR) {

        if (*bdberr == BDBERR_DEADLOCK) {
            /* TODO: input_trans unused */
            if (++retries < gbl_maxretries) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, 
                    "%s:*ERROR* bdb_lite_exact_fetch too much contention "
                    "%d count %d\n",
                    __func__, *bdberr, retries);
        }

        /* it's ok if no data was found, fail on all other errors*/
        if (*bdberr != BDBERR_FETCH_DTA)
            return -1;

        *threshold = -1; /* set to 0 if we didn't find any data */
    }
    /*if we did not get the right amount of data*/
    else if (fndlen != sizeof(*threshold)) {
        *bdberr = BDBERR_DTA_MISMATCH; /* TODO right error to throw? */
        return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;
}

int bdb_get_rowlocks_state(int *rlstate, tran_type *tran, int *bdberr)
{
    int rc, fndlen, retries = 0;
    struct llmeta_rowlocks_state_key_type rowlocks_key;
    struct llmeta_rowlocks_state_data_type rowlocks_data;
    char key[LLMETA_IXLEN] = {0};
    char data[LLMETA_ROWLOCKS_STATE_DATA_LEN] = {0};
    uint8_t *p_buf, *p_buf_end;

    if (!llmeta_bdb_state) {
        logmsg(LOGMSG_ERROR, "%s: file versions db not yet "
                        "open, you must run bdb_open_file_versions\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (!bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL bdberr\n", __func__);
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    rowlocks_key.file_type = LLMETA_ROWLOCKS_STATE;
    p_buf = (uint8_t *)key;
    p_buf_end = p_buf + LLMETA_ROWLOCKS_STATE_KEY_LEN;
    llmeta_rowlocks_state_key_type_put(&rowlocks_key, p_buf, p_buf_end);

retry:
    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, tran, key, data,
                                   LLMETA_ROWLOCKS_STATE_DATA_LEN, &fndlen,
                                   bdberr);

    if (rc || *bdberr != BDBERR_NOERROR) {
        if (*bdberr == BDBERR_DEADLOCK) {
            if (++retries < gbl_maxretries && !tran) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, 
                    "%s:*ERROR* bdb_lite_exact_fetch too much contention "
                    "%d count %d\n",
                    __func__, *bdberr, retries);
        }

        /* it's ok if no data was found, fail on all other errors*/
        if (*bdberr != BDBERR_FETCH_DTA)
            return -1;

        *rlstate = LLMETA_ROWLOCKS_DISABLED;
    } else if (fndlen != LLMETA_ROWLOCKS_STATE_DATA_LEN) {
        *bdberr = BDBERR_DTA_MISMATCH;
        return -1;
    } else {
        p_buf = (uint8_t *)data;
        p_buf_end = p_buf + LLMETA_ROWLOCKS_STATE_DATA_LEN;
        llmeta_rowlocks_state_data_type_get(&rowlocks_data, p_buf, p_buf_end);
        *rlstate = rowlocks_data.rowlocks_state;
        assert(*rlstate >= 0 && *rlstate < LLMETA_ROWLOCKS_STATE_MAX);
    }
    return 0;
}

int bdb_set_rowlocks_state(tran_type *input_trans, int rlstate, int *bdberr)
{
    int retries = 0, rc;
    struct llmeta_rowlocks_state_key_type rowlocks_key;
    struct llmeta_rowlocks_state_data_type rowlocks_data;
    char key[LLMETA_IXLEN] = {0};
    char data[LLMETA_ROWLOCKS_STATE_DATA_LEN] = {0};

    tran_type *trans;
    uint8_t *p_buf, *p_buf_end;

    assert(rlstate >= 0 && rlstate < LLMETA_ROWLOCKS_STATE_MAX);

    if (!llmeta_bdb_state) {
        logmsg(LOGMSG_ERROR, "%s: file versions db not yet "
                        "open, you must run bdb_open_file_versions\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (!bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL bdberr\n", __func__);
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    rowlocks_key.file_type = LLMETA_ROWLOCKS_STATE;
    p_buf = (uint8_t *)key;
    p_buf_end = p_buf + LLMETA_ROWLOCKS_STATE_KEY_LEN;
    llmeta_rowlocks_state_key_type_put(&rowlocks_key, p_buf, p_buf_end);

    rowlocks_data.rowlocks_state = rlstate;
    p_buf = (uint8_t *)data;
    p_buf_end = p_buf + LLMETA_ROWLOCKS_STATE_DATA_LEN;
    llmeta_rowlocks_state_data_type_put(&rowlocks_data, p_buf, p_buf_end);

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: failed to get rowlocks_state\n", __func__);
            return -1;
        }
    } else
        trans = input_trans;

    rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key, bdberr);

    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_DEL_DTA)
        goto backout;

    rc = bdb_lite_add(llmeta_bdb_state, trans, data,
                      LLMETA_ROWLOCKS_STATE_DATA_LEN, key, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR)
        goto backout;

    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
        llmeta_bdb_state->dbenv->log_flush(llmeta_bdb_state->dbenv, NULL);
    }

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    if (!input_trans) {
        int prev_bdberr = *bdberr;
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with"
                            " bdberr %d\n",
                    __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }
    return -1;
}

/* will store a number for the table in question
 * when analyze is run, this threshold value will be used
 * if threshold is negative then we are clearing the setting
 */
int bdb_set_analyzethreshold_table(tran_type *input_trans, const char *tbl_name,
                                   long long threshold, int *bdberr)
{
    int retries = 0;
    struct llmeta_analyzethreshold_key_type analyzethreshold_key;
    char key[LLMETA_IXLEN] = {0};

    tran_type *trans;
    uint8_t *p_buf, *p_buf_start, *p_buf_end;

    /*fail if the db isn't open*/
    if (!llmeta_bdb_state) {
        logmsg(LOGMSG_ERROR, "%s: file versions db not yet "
                        "open, you must run bdb_open_file_versions\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (!tbl_name || !bdberr) {
        logmsg(LOGMSG_ERROR,
               "%s: NULL or inconsistant "
               "argument\n",
               __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    analyzethreshold_key.file_type = LLMETA_ANALYZETHRESHOLD_TABLE;
    strncpy0(analyzethreshold_key.dbname, tbl_name,
             sizeof(analyzethreshold_key.dbname));
    analyzethreshold_key.dbname_len = strlen(analyzethreshold_key.dbname);

    /* set pointers to start and end of buffer */
    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = p_buf_start + LLMETA_IXLEN;

    /* write endianized key */
    p_buf = llmeta_analyzethreshold_key_type_put(&analyzethreshold_key, p_buf,
                                                 p_buf_end);

    size_t key_offset = p_buf - p_buf_start;

    if (key_offset > sizeof(key)) {
        logmsg(LOGMSG_ERROR, "%s: db_name is too long\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        trans = bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
        if (!trans) {
            if (*bdberr == BDBERR_DEADLOCK) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR,
                   "%s: failed to get "
                   "transaction\n",
                   __func__);
            return -1;
        }
    } else
        trans = input_trans;

    /* delete old entry */
    int rc = bdb_lite_exact_del(llmeta_bdb_state, trans, key, bdberr);
    if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_DEL_DTA)
        goto backout;

    if (threshold >= 0) {
        long long tmpval = threshold;
        rc = bdb_lite_add(llmeta_bdb_state, trans, &tmpval, sizeof(tmpval), key,
                          bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            goto backout;
    }

    /*commit if we created our own transaction*/
    if (!input_trans) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR)
            return -1;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        int prev_bdberr = *bdberr;

        /*kill the transaction*/
        rc = bdb_tran_abort(llmeta_bdb_state, trans, bdberr);
        if (rc && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with"
                            " bdberr %d\n",
                    __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }
    return -1;
}

int bdb_clear_analyzethreshold_table(tran_type *input_trans,
                                     const char *tbl_name, int *bdberr)
{
    return bdb_set_analyzethreshold_table(input_trans, tbl_name, -1, bdberr);
}

// LLMeta helpers for CRUD-ing 64bit values
static int llmeta_get_uint64(llmetakey_t key, uint64_t *value)
{
    // returns 0: key found
    //   1: not found
    //  -1: error
    if (llmeta_bdb_state == NULL)
        return -1;
    int rc, bdberr;
    int fndlen;
    uint64_t tmp;
    char llkey[LLMETA_IXLEN] = {0};
    key = htonl(key);
    memcpy(llkey, &key, sizeof(key));
    if ((rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, NULL, llkey, &tmp,
                                        sizeof(tmp), &fndlen, &bdberr)) == 0) {
        *value = flibc_htonll(tmp);
    } else if (bdberr == BDBERR_FETCH_DTA) {
        rc = 1;
    }
    return rc;
}
static int llmeta_set_uint64(llmetakey_t key, uint64_t value)
{
    // find & delete old -> add new
    // returns 0: success
    //  -1: error
    if (llmeta_bdb_state == NULL)
        return -1;
    void *tran = NULL;
    int rc, bdberr;
    int retry = 0;

rep:
    if ((tran = bdb_tran_begin(llmeta_bdb_state, NULL, &bdberr)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s: bdb_tran_begin bdberr:%d retries:%d\n", __func__, bdberr, retry);
        rc = bdberr;
        goto err;
    }
    char llkey[LLMETA_IXLEN] = {0};
    key = htonl(key);
    memcpy(llkey, &key, sizeof(key));
    int fndlen;
    uint64_t tmp;
    if ((rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, tran, llkey, &tmp,
                                        sizeof(tmp), &fndlen, &bdberr)) != 0) {
        if (bdberr == BDBERR_FETCH_DTA) {
            // not found -- just add
            goto add;
        }
        logmsg(LOGMSG_ERROR, "%s: bdb_lite_exact_fetch_tran rc:%d bdberr:%d\n",
                __func__, rc, bdberr);
        goto err;
    }
    if ((rc = bdb_lite_exact_del(llmeta_bdb_state, tran, llkey, &bdberr)) !=
        0) {
        logmsg(LOGMSG_ERROR, "%s: bdb_lite_exact_del rc:%d bdberr:%d\n", __func__,
                rc, bdberr);
        goto err;
    }

add:
    value = flibc_ntohll(value);
    if ((rc = bdb_lite_add(llmeta_bdb_state, tran, &value, sizeof(value), llkey,
                           &bdberr)) != 0) {
        logmsg(LOGMSG_ERROR, "%s: bdb_lite_add failed rc:%d bdberr:%d\n", __func__,
                rc, bdberr);
        goto err;
    }
    if ((rc = bdb_tran_commit(llmeta_bdb_state, tran, &bdberr)) != 0) {
        logmsg(LOGMSG_ERROR, "%s bdb_tran_commit rc:%d bdberr:%d\n", __func__, rc,
                bdberr);
    }
    return rc;

err:
    if (tran) {
        if ((rc = bdb_tran_abort(llmeta_bdb_state, tran, &bdberr)) != 0) {
            logmsg(LOGMSG_ERROR, "%s bdb_tran_abort rc:%d bdberr:%d\n", __func__, rc,
                    bdberr);
            goto out;
        }
    }
    if (retry < gbl_maxretries &&
        (bdberr == BDBERR_NOERROR || bdberr == BDBERR_DEADLOCK)) {
        int dp = gbl_llmeta_deadlock_poll;
        if (dp > 1)
            poll(NULL, 0, rand() % dp);
        ++retry;
        goto rep;
    }

out:
    return rc;
}

int bdb_get_genid_format(uint64_t *genid_format, int *bdberr)
{
    if (llmeta_get_uint64(LLMETA_GENID_FORMAT, genid_format) != 0)
        *genid_format = LLMETA_GENID_ORIGINAL;
    return 0;
}

int bdb_set_genid_format(uint64_t genid_format, int *bdberr)
{
    return llmeta_set_uint64(LLMETA_GENID_FORMAT, genid_format);
}

int llmeta_get_curr_analyze_count(uint64_t *value)
{
    return llmeta_get_uint64(LLMETA_CURR_ANALYZE_COUNT, value);
}
int llmeta_set_curr_analyze_count(uint64_t value)
{
    return llmeta_set_uint64(LLMETA_CURR_ANALYZE_COUNT, value);
}
int llmeta_get_last_analyze_count(uint64_t *value)
{
    return llmeta_get_uint64(LLMETA_LAST_ANALYZE_COUNT, value);
}
int llmeta_set_last_analyze_count(uint64_t value)
{
    return llmeta_set_uint64(LLMETA_LAST_ANALYZE_COUNT, value);
}
int llmeta_get_last_analyze_epoch(uint64_t *value)
{
    return llmeta_get_uint64(LLMETA_LAST_ANALYZE_EPOCH, value);
}
int llmeta_set_last_analyze_epoch(uint64_t value)
{
    return llmeta_set_uint64(LLMETA_LAST_ANALYZE_EPOCH, value);
}

static int __llmeta_preop_alias(struct llmeta_tablename_alias_key *key,
                                const char *tablename_alias, char *key_buf,
                                int key_buf_len, char **errstr)
{
    if (unlikely(key_buf_len < LLMETA_TABLENAME_ALIAS_KEY_LEN))
        abort();

    *errstr = NULL;

    /*fail if the db isn't open*/
    if (!llmeta_bdb_state) {
        logmsg(LOGMSG_ERROR, "%s: low level meta table not yet "
                        "open, you must run bdb_llmeta_open\n",
                __func__);
        if (errstr)
            *errstr = strdup("low level meta table not opened yet");

        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: low level meta table db not "
                        "lite\n",
                __func__);
        if (errstr)
            *errstr = strdup("low level meta table db no lite");
        return -1;
    }

    if (!tablename_alias) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (errstr)
            *errstr = strdup("tablename_alias null");
        return -1;
    }

    if (strlen(tablename_alias) + 1 > sizeof(key->tablename_alias)) {
        logmsg(LOGMSG_ERROR, "%s: tablename alias too long, limit is %zu\n",
               __func__, sizeof(key->tablename_alias));
        if (errstr)
            *errstr = strdup("tablename alias too long");
        return -1;
    }

    key->file_type = LLMETA_FDB_TABLENAME_ALIAS;
    strncpy0(key->tablename_alias, tablename_alias,
             sizeof(key->tablename_alias));

    if (llmeta_tablename_alias_key_put(key, (uint8_t *)key_buf,
                                       (uint8_t *)(key_buf + key_buf_len)) ==
        NULL) {
        logmsg(LOGMSG_ERROR, "%s: tablename alias serializing error\n", __func__);
        if (errstr)
            *errstr = strdup("tablename alias serializing error");
        return -1;
    }

    return 0;
}

int llmeta_set_tablename_alias(void *ptran, const char *tablename_alias,
                               const char *url, char **errstr)
{
    struct llmeta_tablename_alias_key key = {0};
    struct llmeta_tablename_alias_data data = {{0}};

    char key_buf[LLMETA_IXLEN] = {0};
    char data_buf[LLMETA_TABLENAME_ALIAS_DATA_LEN] = {0};
    int retries = 0;
    int bdberr = 0;
    int rc = 0;
    tran_type *trans = NULL;

    if (__llmeta_preop_alias(&key, tablename_alias, key_buf, sizeof(key_buf),
                             errstr))
        return -1;

    /* input validation */
    if (!url) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (errstr)
            *errstr = strdup((!tablename_alias) ? "tablename_alias null"
                                                : "url null");
        return -1;
    }

    if (strlen(url) + 1 > sizeof(data.url)) {
        logmsg(LOGMSG_ERROR, "%s: tablename url too long, limit is %zu\n", __func__, sizeof(data.url));
        if (errstr)
            *errstr = strdup("tablename url too long");
        return -1;
    }

    strncpy0(data.url, url, sizeof(data.url));

    if (llmeta_tablename_alias_data_put(
            &data, (uint8_t *)data_buf,
            (uint8_t *)(data_buf + sizeof(data_buf))) == NULL) {
        logmsg(LOGMSG_ERROR, "%s: tablename url serializing error\n", __func__);
        if (errstr)
            *errstr = strdup("tablename url serializing error");
        return -1;
    }

    retries = 0;
retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        if (errstr)
            *errstr = strdup("failed to commit transaction, hit max retries");
        return -1;
    }

    trans = bdb_tran_begin(llmeta_bdb_state, ptran, &bdberr);
    if (!trans) {
        if (bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s: failed to get transaction bdberr=%d\n", __func__,
                bdberr);
        if (errstr)
            *errstr = strdup("failed to start transaction");

        return -1;
    }

    rc = bdb_lite_add(llmeta_bdb_state, trans, &data_buf, sizeof(data_buf),
                      &key_buf, &bdberr);
    if (rc || bdberr != BDBERR_NOERROR) {
        if (bdberr == BDBERR_DEADLOCK && !ptran) {
            if ((rc = bdb_tran_abort(llmeta_bdb_state, trans, &bdberr)) == 0) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }
        }

        if (bdberr == BDBERR_ADD_DUPE) {
            logmsg(LOGMSG_ERROR, "%s: tablename alias already exists!\n", __func__);
            if (errstr)
                *errstr = strdup("tablename alias already exists");
            rc = -1;
        } else {
            logmsg(LOGMSG_ERROR, 
                    "%s: unrecognized error adding row rc=%d bdberr=%d!\n",
                    __func__, rc, bdberr);
            if (errstr)
                *errstr = strdup("unhandled error");
            rc = -1;
        }
    }

    if (!rc) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, &bdberr);
        if (rc || bdberr != BDBERR_NOERROR) {
            if (bdberr == BDBERR_DEADLOCK && !ptran) {
                if ((rc = bdb_tran_abort(llmeta_bdb_state, trans, &bdberr)) == 0) {
                    int dp = gbl_llmeta_deadlock_poll;
                    if (dp > 1)
                        poll(NULL, 0, rand() % dp);
                    goto retry;
                }
            }

            logmsg(LOGMSG_ERROR, "%s: failed to commit transaction bdberr=%d\n", __func__, bdberr);
            if (errstr)
                *errstr = strdup("failed to commit transaction");

            return -1;
        }
    } else {
        rc = bdb_tran_abort(llmeta_bdb_state, trans, &bdberr);
        if (rc || bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with bdberr "
                            "%d\n",
                    __func__, bdberr);
        }

        return -1;
    }

    return 0;
}

char *llmeta_get_tablename_alias_tran(tran_type *tran, const char *tablename_alias, char **errstr)
{
    struct llmeta_tablename_alias_key key = {0};

    char key_buf[LLMETA_IXLEN] = {0};
    char *data_buf;
    int retries = 0;
    int bdberr = 0;
    int rc = 0;
    int fndlen = 0;

    if (__llmeta_preop_alias(&key, tablename_alias, key_buf, sizeof(key_buf),
                             errstr))
        return NULL;

    retries = 0;

    data_buf = (char *)calloc(1, LLMETA_TABLENAME_ALIAS_DATA_LEN);
    if (data_buf == NULL) {
        if (errstr)
            *errstr = strdup("malloc buffer");
        return NULL;
    }

retry:
    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, tran, &key_buf, data_buf, LLMETA_TABLENAME_ALIAS_DATA_LEN, &fndlen,
                                   &bdberr);
    if (rc || bdberr != BDBERR_NOERROR) {
        if (bdberr == BDBERR_DEADLOCK) {
            if (++retries < gbl_maxretries && !tran) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__,
                    retries);
            if (errstr)
                *errstr = strdup("failed to read record, hit max retries");

            free(data_buf);
            return NULL;
        } else if (bdberr == BDBERR_FETCH_DTA) {
#if 0
            No error for not found 
         /* not found, alias does not exist */
         if(errstr)
            *errstr = strdup("tablename alias does NOT exist");
#endif
            free(data_buf);
            return NULL;
        }

        logmsg(LOGMSG_ERROR, 
                "%s: unrecognized error fetching row rc=%d bdberr=%d!\n",
                __func__, rc, bdberr);
        if (errstr)
            *errstr = strdup("unhandled read error");

        free(data_buf);
        return NULL;
    }

    return data_buf;
}

char *llmeta_get_tablename_alias(const char *tablename_alias, char **errstr)
{
    return llmeta_get_tablename_alias_tran(NULL, tablename_alias, errstr);
}

int llmeta_rem_tablename_alias(const char *tablename_alias, char **errstr)
{
    struct llmeta_tablename_alias_key key = {0};

    char key_buf[LLMETA_IXLEN] = {0};
    int retries = 0;
    int bdberr = 0;
    int rc = 0;
    tran_type *trans = NULL;

    if (__llmeta_preop_alias(&key, tablename_alias, key_buf, sizeof(key_buf),
                             errstr))
        return -1;

    retries = 0;
retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s:%d giving up after %d retries\n", __func__, __LINE__, retries);
        if (errstr)
            *errstr = strdup("failed to commit transaction, hit max retries");
        return -1;
    }

    trans = bdb_tran_begin(llmeta_bdb_state, NULL, &bdberr);
    if (!trans) {
        if (bdberr == BDBERR_DEADLOCK) {
            int dp = gbl_llmeta_deadlock_poll;
            if (dp > 1)
                poll(NULL, 0, rand() % dp);
            goto retry;
        }

        logmsg(LOGMSG_ERROR, "%s:%d failed to get transaction bdberr=%d\n", __func__,
                __LINE__, bdberr);
        if (errstr)
            *errstr = strdup("failed to start transaction");

        return -1;
    }

    rc = bdb_lite_exact_del(llmeta_bdb_state, trans, &key_buf, &bdberr);
    if (rc || bdberr != BDBERR_NOERROR) {
        if (bdberr == BDBERR_DEADLOCK) {
            if ((rc = bdb_tran_abort(llmeta_bdb_state, trans, &bdberr)) == 0) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            } else {
                logmsg(LOGMSG_ERROR, "%s: trans abort failed with bdberr %d\n", __func__, bdberr);
            }
        }

        if (bdberr == BDBERR_DEL_DTA) {
            if (errstr)
                *errstr = strdup("tablename alias does NOT exists");
            rc = -1;
        } else {
            logmsg(LOGMSG_ERROR, 
                    "%s: unrecognized error adding row rc=%d bdberr=%d!\n",
                    __func__, rc, bdberr);
            if (errstr)
                *errstr = strdup("unhandled error");
            rc = -1;
        }
    }

    if (!rc) {
        rc = bdb_tran_commit(llmeta_bdb_state, trans, &bdberr);
        if (rc || bdberr != BDBERR_NOERROR) {
            if (bdberr == BDBERR_DEADLOCK) {
                if ((rc = bdb_tran_abort(llmeta_bdb_state, trans, &bdberr)) == 0) {
                    int dp = gbl_llmeta_deadlock_poll;
                    if (dp > 1)
                        poll(NULL, 0, rand() % dp);
                    goto retry;
                } else {
                    logmsg(LOGMSG_ERROR, "%s: trans abort failed with bdberr %d\n", __func__, bdberr);
                }
            }

            logmsg(LOGMSG_ERROR, "%s: failed to commit transaction bdberr=%d\n", __func__, bdberr);
            if (errstr)
                *errstr = strdup("failed to commit transaction");

            return -1;
        }
    } else {
        rc = bdb_tran_abort(llmeta_bdb_state, trans, &bdberr);
        if (rc || bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with bdberr "
                            "%d\n",
                    __func__, bdberr);
        }

        return -1;
    }

    return 0;
}

int bdb_llmeta_print_alias(bdb_state_type *bdb_state, void *key, int keylen,
                           void *data, int datalen, int *bdberr)
{
    struct llmeta_tablename_alias_key akey;
    struct llmeta_tablename_alias_data adata;
    int type = 0;

    *bdberr = BDBERR_NOERROR;

    buf_get(&type, sizeof(type), key, ((char *)key) + keylen);

    if (type != LLMETA_FDB_TABLENAME_ALIAS)
        return 0;

    bzero(&akey, sizeof(akey));
    bzero(&adata, sizeof(adata));

    if (llmeta_tablename_alias_key_get(&akey, key, ((const uint8_t *)key) +
                                                       keylen) == NULL) {
        logmsg(LOGMSG_ERROR, "%s: wrong format tablename alias key\n", __func__);
        return -1;
    }
    if (llmeta_tablename_alias_data_get(&adata, data, ((const uint8_t *)data) +
                                                          datalen) == NULL) {
        logmsg(LOGMSG_ERROR, "%s: wrong format tablename alias key\n", __func__);
        return -1;
    }

   logmsg(LOGMSG_USER, "\"%s\" -> \"%s\"\n", akey.tablename_alias, adata.url);

    return 0;
}

void llmeta_list_tablename_alias(void)
{
    int bdberr = 0;

    if (!bdb_have_llmeta()) {
        logmsg(LOGMSG_ERROR, "%s:%d: No llmeta!\n", __FILE__, __LINE__);
        return;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: low level meta table db not "
                        "lite\n",
                __func__);
        return;
    }

    bdb_lite_list_records(llmeta_bdb_state, bdb_llmeta_print_alias, &bdberr);
}

bdb_state_type *bdb_llmeta_bdb_state(void) { return llmeta_bdb_state; }

static int bdb_table_version_upsert_int(bdb_state_type *bdb_state,
                                        tran_type *tran,
                                        unsigned long long *val, int *bdberr)
{
    struct llmeta_sane_table_version schema_version;
    char *tblname = bdb_state->name;
    unsigned long long version;
    char key[LLMETA_IXLEN] = {0};
    uint8_t *p_buf, *p_buf_end;
    int len;
    int rc;

    if (unlikely(!tran || !tblname || !bdberr)) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /*fail if the db isn't open*/
    if (unlikely(!llmeta_bdb_state)) {
        logmsg(LOGMSG_ERROR, "%s: low level meta table not yet open,"
                        "you must run bdb_llmeta_open\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }
    if (unlikely(bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE)) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* input validation */
    len = strlen(bdb_state->name) + 1;
    if (unlikely(len > sizeof(schema_version.tblname))) {
        logmsg(LOGMSG_ERROR, "%s: tablename too long %zu\n", __func__,
               strlen(bdb_state->name));
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* find the existing record, if any */
    rc = bdb_table_version_select(bdb_state->name, tran, &version, bdberr);
    if (rc) {
        *bdberr = BDBERR_MISC;
        return -1;
    }

    /* add the key type */
    bzero(&schema_version, sizeof(schema_version));
    schema_version.file_type = LLMETA_TABLE_VERSION;
    strncpy0(schema_version.tblname, bdb_state->name,
             sizeof(schema_version.tblname));

    p_buf = (uint8_t *)key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    /* put onto buffer */
    if (!(p_buf = llmeta_sane_table_version_put(&schema_version, p_buf,
                                                p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_sane_table_version_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    /* delete old entry */
    rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, bdberr);
    if ((rc || *bdberr != BDBERR_NOERROR) && *bdberr != BDBERR_DEL_DTA) {
        return rc;
    }

    /* add new entry */
    if (val) {
        version = *val;
    } else {
        version++;
    }

    version = flibc_htonll(version);
    rc = bdb_lite_add(llmeta_bdb_state, tran, &version, sizeof(version), key,
                      bdberr);
    if (rc || *bdberr != BDBERR_NOERROR) {
        return rc;
    } else {
        logmsg(LOGMSG_INFO, "Saved tableversion %" PRIu64 " for table %s\n",
               flibc_htonll(version), bdb_state->name);
    }

    *bdberr = BDBERR_NOERROR;
    return 0;
}

/**
 *  Increment the TABLE VERSION ENTRY for table "bdb_state->name".
 *  If an entry doesn't exist, an entry with value 1 is created (default 0 means
 * non-existing)
 *
 */
int bdb_table_version_upsert(bdb_state_type *bdb_state, tran_type *tran,
                             int *bdberr)
{
    return bdb_table_version_upsert_int(bdb_state, tran, NULL, bdberr);
}

/**
 * Set the TABLE VERSION ENTRY for table "bdb_state->name" to "val"
 * (It creates or, if existing, updates an entry)
 *
 */
int bdb_table_version_update(bdb_state_type *bdb_state, tran_type *tran,
                             unsigned long long val, int *bdberr)
{
    return bdb_table_version_upsert_int(bdb_state, tran, &val, bdberr);
}

/**
 *  Delete the TABLE VERSION ENTRY for table "bdb_state->name"
 *
 */
int bdb_table_version_delete(bdb_state_type *bdb_state, tran_type *tran,
                             int *bdberr)
{
    struct llmeta_sane_table_version schema_version;
    char *tblname = bdb_state->name;
    unsigned long long version;
    char key[LLMETA_IXLEN] = {0};
    uint8_t *p_buf, *p_buf_end;
    int len;
    int rc;

    if (unlikely(!tran || !tblname || !bdberr)) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /*fail if the db isn't open*/
    if (unlikely(!llmeta_bdb_state)) {
        logmsg(LOGMSG_ERROR, "%s: low level meta table not yet open,"
                        "you must run bdb_llmeta_open\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }
    if (unlikely(bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE)) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* input validation */
    len = strlen(bdb_state->name) + 1;
    if (unlikely(len > sizeof(schema_version.tblname))) {
        logmsg(LOGMSG_ERROR, "%s: tablename too long %zu\n", __func__,
               strlen(bdb_state->name));
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* find the existing record, if any */
    rc = bdb_table_version_select(bdb_state->name, tran, &version, bdberr);
    if (rc) {
        *bdberr = BDBERR_MISC;
        return -1;
    }

    /* add the key type */
    bzero(&schema_version, sizeof(schema_version));
    schema_version.file_type = LLMETA_TABLE_VERSION;
    strncpy0(schema_version.tblname, bdb_state->name,
             sizeof(schema_version.tblname));

    p_buf = (uint8_t *)key;
    p_buf_end = p_buf + LLMETA_IXLEN;

    /* put onto buffer */
    if (!(p_buf = llmeta_sane_table_version_put(&schema_version, p_buf,
                                                p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_sane_table_version_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    /* delete old entry */
    rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, bdberr);
    if ((rc || *bdberr != BDBERR_NOERROR) && *bdberr != BDBERR_DEL_DTA) {
        return rc;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;
}

/**
 *  Select the TABLE VERSION ENTRY for table "bdb_state->name".
 *  If an entry doesn't exist, version 0 is returned
 *
 */
int bdb_table_version_select_verbose(const char *tblname, tran_type *tran,
                                     unsigned long long *version, int *bdberr,
                                     int verbose)
{
    struct llmeta_sane_table_version schema_version;
    char key[LLMETA_IXLEN] = {0};
    char fnddata[sizeof(*version)];
    int fnddatalen;
    int tblnamelen;
    uint8_t *p_buf, *p_buf_end;
    int retries = 0;
    int rc;

    *bdberr = 0;
    *version = -1LL;

    /*stop here if the db isn't open*/
    if (!llmeta_bdb_state) {
        *bdberr = BDBERR_DBEMPTY;
        return -1;
    }

    if (!tblname || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: NULL argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }
    tblnamelen = strlen(tblname) + 1;
    if (tblnamelen > sizeof(schema_version.tblname)) {
        logmsg(LOGMSG_ERROR, "%s: tablename too long \"%s\"\n", __func__, tblname);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_get_type(llmeta_bdb_state) != BDBTYPE_LITE) {
        logmsg(LOGMSG_ERROR, "%s: llmeta db not lite\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    bzero(&schema_version, sizeof(schema_version));
    schema_version.file_type = LLMETA_TABLE_VERSION;
    strncpy0(schema_version.tblname, tblname, sizeof(schema_version.tblname));

    p_buf = (uint8_t *)key;
    p_buf_end = (p_buf + LLMETA_IXLEN);

    if (!(p_buf = llmeta_sane_table_version_put(&schema_version, p_buf,
                                                p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_sane_table_version_put returns NULL\n",
                __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

retry:
    /* try to fetch the version number */
    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, tran, key, fnddata,
                                   sizeof(fnddata), &fnddatalen, bdberr);

    /* handle return codes */
    if (rc || *bdberr != BDBERR_NOERROR) {
        /* initial case */
        if (*bdberr == BDBERR_FETCH_DTA) {
            /* not found, not yet schema changed */
            rc = *bdberr = 0;
            *version = 0LL;
            return 0;
        }

        /* errored case */
        if (tran == NULL && *bdberr == BDBERR_DEADLOCK) {
            if (++retries < gbl_maxretries) {
                int dp = gbl_llmeta_deadlock_poll;
                if (dp > 1)
                    poll(NULL, 0, rand() % dp);
                goto retry;
            }

            logmsg(LOGMSG_ERROR,
                   "%s: *ERROR* bdb_lite_exact_fetch too much contention "
                   "%d count %d\n",
                   __func__, *bdberr, retries);
        }

        /*fail on all other errors*/
        return -1;
    }
    /*if we did not get the right amount of data*/
    else if (fnddatalen != sizeof(fnddata)) {
        *bdberr = BDBERR_DTA_MISMATCH;
        return -1;
    }

    *version = *((unsigned long long *)fnddata);
    *version = flibc_ntohll(*version);

    if (verbose)
        logmsg(LOGMSG_INFO, "Retrieved tableversion %lld for %s\n",
               *version, tblname);

    *bdberr = BDBERR_NOERROR;
    return 0;
}

int bdb_table_version_select(const char *tblname, tran_type *tran, unsigned long long *version, int *bdberr)
{
    return bdb_table_version_select_verbose(tblname, tran, version, bdberr, 1);
}

/* caller is responsible to free the memory
 * returns 0: key found
 *  1: not found
 * -1: error
 */
static int llmeta_get_blob(llmetakey_t key, tran_type *tran, const char *table,
                           char **value, int *len)
{
#ifdef DEBUG_LLMETA
    fprintf(stderr, "%s\n", __func__);
#endif
    if (llmeta_bdb_state == NULL)
        return -1;
    int rc, bdberr;
    char *tmpstr = NULL;
    char llkey[LLMETA_IXLEN] = {0};
    int retry = 0;

    key = htonl(key);
    memcpy(llkey, &key, sizeof(key));
    if (table)
        memcpy((llkey + sizeof(key)), table,
               strnlen(table, LLMETA_IXLEN - sizeof(key)));

rep:
    if ((rc = bdb_lite_exact_var_fetch_tran(llmeta_bdb_state, tran, llkey,
                                            (void **)&tmpstr, len, &bdberr)) ==
        0) {
        assert(tmpstr != NULL);
        *value = malloc(*len + 1);
        strncpy(*value, tmpstr, *len);
        (*value)[*len] = '\0';
#ifdef DEBUG_LLMETA
        fprintf(
            stderr,
            "%s: bdb_lite_exact_fetch_tran found:%s *len:%d rc:%d bdberr:%d\n",
            __func__, *value, *len, rc, bdberr);
#endif
        free(tmpstr);
    } else if (bdberr == BDBERR_FETCH_DTA) {
#ifdef DEBUG_LLMETA
        fprintf(stderr,
                "%s: bdb_lite_exact_fetch_tran not found rc:%d bdberr:%d\n",
                __func__, rc, bdberr);
#endif
        rc = 1;
    } else if (rc == -1 && bdberr == BDBERR_DEADLOCK && !tran &&
               retry < gbl_maxretries) {
        int dp = gbl_llmeta_deadlock_poll;
        if (dp > 1)
            poll(NULL, 0, rand() % dp);
        ++retry;
        goto rep;
    } else
        logmsg(LOGMSG_ERROR, "%s: bdb_lite_exact_fetch_tran error rc:%d bdberr:%d\n",
                __func__, rc, bdberr);

    return rc;
}

/* find & delete old -> add new
 * returns 0: success
 *  -1: error
 */
static int llmeta_del_set_blob(void *parent_tran, llmetakey_t key,
                               const char *table, const char *value, int len,
                               int deleteonly)
{
#ifdef DEBUG_LLMETA
    fprintf(stderr, "%s\n", __func__);
#endif
    if (llmeta_bdb_state == NULL)
        return -1;
    void *tran = NULL;
    int rc, bdberr;
    int retry = 0;

rep:

    if ((tran = bdb_tran_begin(llmeta_bdb_state, parent_tran, &bdberr)) ==
        NULL) {
        logmsg(LOGMSG_ERROR, "%s: bdb_tran_begin bdberr:%d retries:%d\n", __func__,
                bdberr, retry);
        rc = bdberr;
        goto err;
    }
    char llkey[LLMETA_IXLEN] = {0};
    key = htonl(key);
    memcpy(llkey, &key, sizeof(key));
    if (table)
        memcpy((llkey + sizeof(key)), table, strnlen(table, LLMETA_IXLEN - sizeof(key)));

    int fndlen;
    char *tmpstr = NULL;
    if ((rc = bdb_lite_exact_var_fetch_tran(llmeta_bdb_state, tran, llkey,
                                            (void **)&tmpstr, &fndlen,
                                            &bdberr)) != 0) {

        if (bdberr == BDBERR_FETCH_DTA) {
            // not found -- just add
            goto add;
        }
        logmsg(LOGMSG_ERROR, "%s: bdb_lite_exact_fetch_tran rc:%d bdberr:%d\n",
                __func__, rc, bdberr);
        goto err;
    }

#ifdef DEBUG_LLMETA
    tmpstr[fndlen - 1] = '\0';
    fprintf(
        stderr,
        "%s: bdb_lite_exact_fetch_tran found:%s fndlen:%d rc:%d bdberr:%d\n",
        __func__, tmpstr, fndlen, rc, bdberr);
#endif
    free(tmpstr);

    if ((rc = bdb_lite_exact_del(llmeta_bdb_state, tran, llkey, &bdberr)) !=
        0) {
        logmsg(LOGMSG_ERROR, "%s: bdb_lite_exact_del rc:%d bdberr:%d\n", __func__,
                rc, bdberr);
        goto err;
    }
add:
    if (!deleteonly &&
        (rc = bdb_lite_add(llmeta_bdb_state, tran, (void *)value,
                           len, // include \0
                           llkey, &bdberr)) != 0) {
        logmsg(LOGMSG_ERROR, "%s: bdb_lite_add failed rc:%d bdberr:%d\n", __func__,
                rc, bdberr);
        goto err;
    }
    if ((rc = bdb_tran_commit(llmeta_bdb_state, tran, &bdberr)) != 0) {
        logmsg(LOGMSG_ERROR, "%s bdb_tran_commit rc:%d bdberr:%d\n", __func__, rc,
                bdberr);
    }
    return rc;
err:
    if (tran && (rc = bdb_tran_abort(llmeta_bdb_state, tran, &bdberr)) != 0) {
        logmsg(LOGMSG_ERROR, "%s bdb_tran_abort rc:%d bdberr:%d\n", __func__, rc,
                bdberr);
        goto out;
    }
    if (retry < gbl_maxretries && (bdberr == BDBERR_NOERROR || bdberr == BDBERR_DEADLOCK) && !parent_tran) {
        int dp = gbl_llmeta_deadlock_poll;
        if (dp > 1)
            poll(NULL, 0, rand() % dp);
        ++retry;
        goto rep;
    }
out:
    return rc;
}

static inline int llmeta_del_blob(void *parent_tran, llmetakey_t key,
                                  const char *table)
{
    return llmeta_del_set_blob(parent_tran, key, table, NULL, 0, 1);
}

static inline int llmeta_set_blob(void *parent_tran, llmetakey_t key,
                                  const char *table, const char *value, int len)
{
    return llmeta_del_set_blob(parent_tran, key, table, value, len, 0);
}

/* return parameters for tbl into value
 * NB: caller needs to free that memory area
 */
int bdb_get_table_csonparameters(tran_type *tran, const char *table,
                                 char **value, int *len)
{
    return llmeta_get_blob(LLMETA_TABLE_PARAMETERS, tran, table, value, len);
}

int bdb_set_table_csonparameters(void *parent_tran, const char *table,
                                 const char *value, int len)
{
    return llmeta_set_blob(parent_tran, LLMETA_TABLE_PARAMETERS, table, value,
                           len);
}

int bdb_del_table_csonparameters(void *parent_tran, const char *table)
{
    return llmeta_del_blob(parent_tran, LLMETA_TABLE_PARAMETERS, table);
}

#include <cson.h>

/* return parameter for tbl into value
 * NB: caller needs to free that memory area
 */
int bdb_get_table_parameter_tran(const char *table, const char *parameter,
                                 char **value, tran_type *tran)
{
#ifdef DEBUG_LLMETA
    fprintf(stderr, "%s()\n", __func__);
#endif
    if (llmeta_bdb_state == NULL)
        return 1;

    char *blob = NULL;
    int len;
    int rc = bdb_get_table_csonparameters(tran, table, &blob, &len);
    assert(rc == 0 || (rc == 1 && blob == NULL));

    if (blob == NULL)
        return 1;

    cson_value *rootV = NULL;
    cson_object *rootObj = NULL;

    rc = cson_parse_string(&rootV, blob, len);
    // The NULL arguments hold optional information for/about
    // the parse results. These can be used to set certain
    // parsing options and get more detailed error information
    // if parsing fails.

    if (0 != rc) {
       logmsg(LOGMSG_ERROR, "bdb_get_table_parameter:Error code %d (%s)!\n", rc,
               cson_rc_string(rc));
        free(blob);
        return 1;
    }

    assert(cson_value_is_object(rootV));
    rootObj = cson_value_get_object(rootV);
    assert(rootObj != NULL);

    cson_value *param = cson_object_get(rootObj, parameter);
    if (param == NULL) {
#ifdef DEBUG_LLMETA
        printf("param %s not found\n", parameter);
#endif
        rc = 1;
        goto out;
    }

    cson_string const *str = cson_value_get_string(param);
    *value = strdup(cson_string_cstr(str));

#ifdef DEBUG_LLMETA
    fprintf(stdout, "%s\n", cson_string_cstr(str));
    fprintf(stdout, "%s\n", *value);
    { // print root object
        cson_object_iterator iter;
        rc = cson_object_iter_init(rootObj, &iter);
        if (0 != rc) {
            printf("Error code %d (%s)!\n", rc, cson_rc_string(rc));
            rc = 1;
            goto out;
        }

        cson_kvp *kvp; // key/value pair
        while ((kvp = cson_object_iter_next(&iter))) {
            cson_string const *ckey = cson_kvp_key(kvp);
            cson_value *v = cson_kvp_value(kvp);
            // Here we just print out: KEY=VALUE
            fprintf(stdout, "%s", cson_string_cstr(ckey));
            putchar('=');
            cson_output_FILE(v, stdout);
        }
        // cson_object_iterator objects own no memory and need not be cleaned
        // up.
        // Iteration results are undefined if the object being iterated over is
        // modified (keys added/removed) while iterating.
    }
#endif

out:
    cson_value_free(rootV);
    free(blob);
    return rc;
}

int bdb_get_table_parameter(const char *table, const char *parameter,
                            char **value)
{
    return bdb_get_table_parameter_tran(table, parameter, value, NULL);
}

int bdb_set_table_parameter(void *parent_tran, const char *table,
                            const char *parameter, const char *value)
{
#ifdef DEBUG_LLMETA
    fprintf(stderr, "%s()\n", __func__);
#endif
    char *blob = NULL;
    int len;
    int rc = bdb_get_table_csonparameters(parent_tran, table, &blob, &len);
    assert(rc == 0 || (rc == 1 && blob == NULL));

    cson_value *rootV = NULL;
    cson_object *rootObj = NULL;

    if (blob != NULL) {
        rc = cson_parse_string(&rootV, blob, len);
        // The NULL arguments hold optional information for/about
        // the parse results. These can be used to set certain
        // parsing options and get more detailed error information
        // if parsing fails.

        if (0 != rc) {
           logmsg(LOGMSG_ERROR, "bdb_set_table_parameter: Error code %d (%s)!\n", rc,
                   cson_rc_string(rc));
            return 1;
        }

        assert(cson_value_is_object(rootV));
        rootObj = cson_value_get_object(rootV);
        assert(rootObj != NULL);
    } else if (value == NULL) { // blob is null and we are clearing
        return 0;               // nothing to do
    } else {
        // Create a rootV object:
        rootV = cson_value_new_object();
        rootObj = cson_value_get_object(rootV);
    }

    if (value == NULL) {
        cson_value *param = cson_object_get(rootObj, parameter);
        if (param == NULL) {
#ifdef DEBUG_LLMETA
            printf("param %s not found -- nothing to do\n", parameter);
#endif
            cson_value_free(rootV);
            free(blob);
            return 1;
        }

        rc = cson_object_unset(rootObj, parameter);
        if (0 != rc) {
            cson_string const *str = cson_value_get_string(param);
            logmsg(LOGMSG_ERROR, "error unsetting %s=%s", parameter,
                   cson_string_cstr(str));
            cson_value_free(rootV);
            free(blob);
            return 1;
        }
    } else {
        // Add the param and value to table:
        cson_object_set(rootObj, parameter,
                        cson_value_new_string(value, strlen(value)));
    }

#ifdef DEBUG_LLMETA
    { // print root object
        cson_object_iterator iter;
        rc = cson_object_iter_init(rootObj, &iter);
        if (0 != rc) {
            printf("Error code %d (%s)!\n", rc, cson_rc_string(rc));
            return 1;
        }

        cson_kvp *kvp; // key/value pair
        while ((kvp = cson_object_iter_next(&iter))) {
            cson_string const *ckey = cson_kvp_key(kvp);
            cson_value *v = cson_kvp_value(kvp);
            // Here we just print out: KEY=VALUE
            fprintf(stdout, "%s", cson_string_cstr(ckey));
            putchar('=');
            cson_output_FILE(v, stdout);
        }
        // cson_object_iterator objects own no memory and need not be cleaned
        // up.
        // Iteration results are undefined if the object being iterated over is
        // modified (keys added/removed) while iterating.
    }
#endif

    cson_buffer buf;
    rc = cson_output_buffer(rootV, &buf); // write obj to buffer
    if (0 != rc) {
        logmsg(LOGMSG_ERROR, "cson_output_buffer returned rc %d", rc);
    } else if (buf.used > 2) {
        // JSON data is the first (buf.used) bytes of (buf.mem).
        bdb_set_table_csonparameters(parent_tran, table, (const char *)buf.mem,
                                     buf.used); // save to llmeta blob
    } else {
        bdb_del_table_csonparameters(parent_tran, table);
    }

    // Clean up
    cson_value_free(rootV);
    free(blob);
    return rc;
}

int bdb_clear_table_parameter(void *parent_tran, const char *table,
                              const char *parameter)
{
    return bdb_set_table_parameter(parent_tran, table, parameter, NULL);
}

/**
 * Set the sql alias for a table "bdb_state->name" to newname
 *
 */
int bdb_set_table_sqlalias(const char *tablename, tran_type *tran,
                           char *newname)
{
    int rc = 0;

    rc = bdb_set_table_parameter(tran, tablename, "sqlalias", newname);

    return rc;
}

/**
 * Get the sql alias for a table "bdb_state->name" in newname (malloced)
 *
 */
int bdb_get_table_sqlalias_tran(const char *tablename, tran_type *tran,
                                char **newname)
{
    int rc = 0;

    rc = bdb_get_table_parameter_tran(tablename, "sqlalias", newname, tran);

    return rc;
}

/**
 * Delete the sql alias for a table "bdb_state->name"
 *
 */
int bdb_del_table_sqlalias_tran(const char *tablename, tran_type *tran)
{
    return bdb_set_table_parameter(tran, tablename, "sqlalias", NULL);
}

struct queue_key {
    int file_type;
    char dbname[LLMETA_TBLLEN + 1];
};

/* data is a bit of a mess:
 * int configlen
 * char config[configlen]
 * int numdests
 * int destlen[numdests]
 * char dest[destlen[0]]
 * char dest[destlen[1]]
 * ...
 * char dest[destlen[numdests-1]]
 * */
struct queue_data {
    char *config;
    int ndests;
    char **dest;
};

static uint8_t *llmeta_queue_key_put(struct queue_key *key, uint8_t *p_buf,
                                     uint8_t *p_buf_end)
{
    if (p_buf_end - p_buf < sizeof(struct queue_key))
        return NULL;
    p_buf = buf_put(&key->file_type, sizeof(key->file_type), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&key->dbname, sizeof(key->dbname), p_buf, p_buf_end);
    return p_buf;
}

static uint8_t *llmeta_queue_key_get(struct queue_key *key, uint8_t *p_buf,
                                     uint8_t *p_buf_end)
{
    if (p_buf_end - p_buf < sizeof(struct queue_key))
        return NULL;
    p_buf = (uint8_t *)buf_get(&key->file_type, sizeof(key->file_type), p_buf,
                               p_buf_end);
    p_buf = (uint8_t *)buf_no_net_get(&key->dbname, sizeof(key->dbname), p_buf,
                                      p_buf_end);
    return p_buf;
}

static int llmeta_queue_data_size(struct queue_data *data)
{
    int sz = 0;

    sz += sizeof(int); /* configlen */
    sz += strlen(data->config) + 1;
    sz += sizeof(int);                /* numdests */
    sz += sizeof(int) * data->ndests; /* destlen */
    for (int i = 0; i < data->ndests; i++) {
        sz += sizeof(int);
        sz += strlen(data->dest[i]) + 1;
    }
    return sz;
}

static uint8_t *llmeta_queue_data_put(struct queue_data *data, uint8_t *p_buf,
                                      uint8_t *p_buf_end)
{
    int sz = 0;
    int len;

    sz = llmeta_queue_data_size(data);
    if (p_buf_end - p_buf < sz)
        return NULL;
    len = strlen(data->config) + 1;
    p_buf = buf_put(&len, sizeof(int), p_buf, p_buf_end);
    p_buf = buf_no_net_put(data->config, len, p_buf, p_buf_end);
    p_buf = buf_put(&data->ndests, sizeof(int), p_buf, p_buf_end);
    for (int i = 0; i < data->ndests; i++) {
        len = strlen(data->dest[i]) + 1;
        p_buf = buf_put(&len, sizeof(int), p_buf, p_buf_end);
        p_buf = buf_no_net_put(data->dest[i], len, p_buf, p_buf_end);
    }
    return p_buf;
}

static void queue_data_destroy(struct queue_data *qd)
{
    if (qd->config)
        free(qd->config);
    if (qd->dest) {
        for (int i = 0; i < qd->ndests; i++)
            if (qd->dest[i])
                free(qd->dest[i]);
        free(qd->dest);
    }
}

static struct queue_data *llmeta_queue_data_get(uint8_t *p_buf,
                                                uint8_t *p_buf_end)
{
    struct queue_data *qd = NULL;
    int len = 0;

    qd = calloc(1, sizeof(struct queue_data));
    if (qd == NULL)
        goto bad_alloc;
    p_buf = (uint8_t *)buf_get(&len, sizeof(int), p_buf, p_buf_end);
    qd->config = malloc(len);
    if (qd->config == NULL)
        goto bad_alloc;
    p_buf = (uint8_t *)buf_get(qd->config, len, p_buf, p_buf_end);
    p_buf = (uint8_t *)buf_get(&qd->ndests, sizeof(int), p_buf, p_buf_end);
    qd->dest = calloc(1, sizeof(char *) * qd->ndests);
    for (int i = 0; i < qd->ndests; i++) {
        p_buf = (uint8_t *)buf_get(&len, sizeof(int), p_buf, p_buf_end);
        qd->dest[i] = malloc(len);
        p_buf = (uint8_t *)buf_no_net_get(qd->dest[i], len, p_buf, p_buf_end);
    }
    if (p_buf == NULL)
        goto bad_alloc;
    return qd;

bad_alloc:
    if (qd)
        queue_data_destroy(qd);
    return NULL;
}

int bdb_llmeta_add_queue(bdb_state_type *bdb_state, tran_type *tran,
                         char *queue, char *config, int ndests, char **dests,
                         int *bdberr)
{
    char key[LLMETA_IXLEN] = {0};
    int dtalen;
    uint8_t *p_buf, *p_buf_end;
    struct queue_data qd = {0};
    struct queue_key qk = {0};
    int rc;

    p_buf = (uint8_t *)key;
    p_buf_end = p_buf + LLMETA_IXLEN;
    qk.file_type = LLMETA_TRIGGER;
    /* TODO: range check? assume sanitized at this point? */

    if (strlen(queue) >= LLMETA_TBLLEN) {
        *bdberr = BDBERR_MISC;
        logmsg(LOGMSG_ERROR, "%s: queue name length is too long\n", __func__);
        return -1;
    }
    strcpy(qk.dbname, queue);

    p_buf = llmeta_queue_key_put(&qk, p_buf, p_buf_end);
    if (p_buf == NULL) {
        *bdberr = BDBERR_MISC;
        logmsg(LOGMSG_ERROR, "%s: failed to encode queue llmeta key\n", __func__);
        return -1;
    }
    qd.config = config;
    qd.ndests = ndests;
    qd.dest = dests;
    dtalen = llmeta_queue_data_size(&qd);
    p_buf = malloc(dtalen);
    p_buf_end = p_buf + dtalen;
    if (llmeta_queue_data_put(&qd, p_buf, p_buf_end) == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed to encode queue llmeta data\n", __func__);
        free(p_buf);
        return -1;
    }
    rc = bdb_lite_add(llmeta_bdb_state, tran, p_buf, dtalen, key, bdberr);
    if (rc)
        logmsg(LOGMSG_ERROR,
               "%s: failed to add llmeta queue entry for %s: %d\n", __func__,
               queue, *bdberr);
    free(p_buf);

    return rc;
}

int bdb_llmeta_alter_queue(bdb_state_type *bdb_state, tran_type *tran,
                           char *queue, char *config, int ndests, char **dests,
                           int *bdberr)
{
    int rc;

    /* delete and add */
    rc = bdb_llmeta_drop_queue(bdb_state, tran, queue, bdberr);
    if (rc)
        goto done;
    rc = bdb_llmeta_add_queue(bdb_state, tran, queue, config, ndests, dests,
                              bdberr);
    if (rc)
        goto done;

done:
    return rc;
}

int bdb_llmeta_drop_queue(bdb_state_type *bdb_state, tran_type *tran,
                          char *queue, int *bdberr)
{
    char key[LLMETA_IXLEN] = {0};
    uint8_t *p_buf, *p_buf_end;
    struct queue_key qk = {0};
    int rc;

    *bdberr = BDBERR_NOERROR;

    bdb_lock_table_write(bdb_state, tran);

    p_buf = (uint8_t *)key;
    p_buf_end = p_buf + LLMETA_IXLEN;
    qk.file_type = LLMETA_TRIGGER;
    strcpy(qk.dbname, queue);

    p_buf = llmeta_queue_key_put(&qk, p_buf, p_buf_end);
    if (p_buf == NULL) {
        *bdberr = BDBERR_MISC;
        logmsg(LOGMSG_ERROR, "%s: failed to encode queue llmeta key\n", __func__);
        rc = -1;
        goto done;
    }

    rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, bdberr);
    if (rc)
        goto done;

done:
    return rc;
}

int bdb_llmeta_get_queues(char **queue_names, size_t max_queues,
                          int *fnd_queues, int *bdberr)
{
    int rc;
    uint8_t key[LLMETA_IXLEN] = {0};
    uint8_t nextkey[LLMETA_IXLEN];
    struct queue_key qk = {0};
    uint8_t *p_buf, *p_buf_end;
    int nqueues = 0;
    int fnd;

    qk.file_type = htonl(LLMETA_TRIGGER);

    /* Ok, I officially hate the 'lite' APIs greatly. */
    rc = bdb_lite_fetch_partial(llmeta_bdb_state, &qk.file_type, sizeof(int),
                                key, &fnd, bdberr);
    while (rc == 0 && fnd == 1) {
        p_buf = key;
        p_buf_end = p_buf + LLMETA_IXLEN;
        p_buf = llmeta_queue_key_get(&qk, p_buf, p_buf_end);
        if (p_buf == NULL) {
            logmsg(LOGMSG_ERROR, "%s: failed to decode queue key\n", __func__);
            fsnapf(stdout, key, LLMETA_IXLEN);
            return -1;
        }
        if (qk.file_type != LLMETA_TRIGGER)
            break;
        if (nqueues >= max_queues)
            break;
        queue_names[nqueues] = strdup(qk.dbname);
        ++nqueues;
        if ((rc = bdb_lite_fetch_keys_fwd(llmeta_bdb_state, key, nextkey, 1,
                                          &fnd, bdberr)) != 0)
            return rc;
        memcpy(key, nextkey, LLMETA_IXLEN);
    }
    *fnd_queues = nqueues;
    return rc;
}

int bdb_llmeta_get_queue(tran_type *trans, char *qname, char **config,
                         int *ndests, char ***dests, int *bdberr)
{
    struct queue_key qk = {0};
    struct queue_data *qd = NULL;
    uint8_t key[LLMETA_IXLEN] = {0};
    uint8_t *p_buf, *p_buf_end;
    int rc;
    void *dta = NULL;
    int foundlen;

    *bdberr = BDBERR_NOERROR;

    qk.file_type = LLMETA_TRIGGER;
    strcpy(qk.dbname, qname);

    p_buf = key;
    p_buf_end = p_buf + LLMETA_IXLEN;
    p_buf = llmeta_queue_key_put(&qk, p_buf, p_buf_end);
    if (p_buf == NULL) {
        logmsg(LOGMSG_ERROR, "%s: can't encode key for queue %s\n", __func__, qname);
        *bdberr = BDBERR_MISC;
        rc = -1;
        goto done;
    }

    rc = bdb_lite_exact_fetch_alloc_tran(llmeta_bdb_state, trans, key, &dta,
                                         &foundlen, bdberr);
    if (rc) {
        *bdberr = BDBERR_FETCH_DTA;
        goto done;
    }
    p_buf = dta;
    p_buf_end = p_buf + foundlen;
    qd = llmeta_queue_data_get(p_buf, p_buf_end);
    if (qd == NULL) {
        *bdberr = BDBERR_MISC;
        rc = -1;
        goto done;
    }
    *config = qd->config;
    *ndests = qd->ndests;
    *dests = qd->dest;

    free(qd);
    qd = NULL;
done:
    if (dta)
        free(dta);
    if (qd)
        queue_data_destroy(qd);
    return rc;
}

/*
** kv_funcs() - operate on arbitrary key-value pairs
*/

// get values for all matching keys
static int kv_get(tran_type *t, void *k, size_t klen, void ***ret, int *num, int *bdberr)
{
    int fnd;
    int n = 0;
    int inc = 10;
    int alloc = 0;
    uint8_t out[LLMETA_IXLEN];
    void **vals = NULL;
    int rc = bdb_lite_fetch_partial_tran(llmeta_bdb_state, t, k, klen, out,
                                         &fnd, bdberr);
    while (rc == 0 && fnd == 1) {
        if (memcmp(k, out, klen) != 0) {
            break;
        }
        void *dta;
        int dsz;
        rc = bdb_lite_exact_var_fetch_tran(llmeta_bdb_state, t, out, &dta, &dsz,
                                           bdberr);
        if (rc || *bdberr != BDBERR_NOERROR) {
            break;
        }
        if (n == alloc) {
            alloc += inc;
            vals = realloc(vals, sizeof(void *) * alloc);
        }
        vals[n++] = dta;
        uint8_t nxt[LLMETA_IXLEN];
        rc = bdb_lite_fetch_keys_fwd_tran(llmeta_bdb_state, t, out, nxt, 1,
                                          &fnd, bdberr);
        memcpy(out, nxt, sizeof(out));
    }
    *num = n;
    *ret = vals;
    return rc;
}

// get full keys for all matching partial keys
static int kv_get_keys(tran_type *t, void *k, size_t klen, void ***ret,
                       int *num, int *bdberr)
{
    int fnd;
    int n = 0;
    int inc = 10;
    int alloc = 0;
    uint8_t out[LLMETA_IXLEN];
    void **names = NULL;
    int rc = bdb_lite_fetch_partial_tran(llmeta_bdb_state, t, k, klen, out,
                                         &fnd, bdberr);
    while (rc == 0 && fnd == 1) {
        if (memcmp(k, out, klen) != 0) {
            break;
        }
        if (n == alloc) {
            alloc += inc;
            names = realloc(names, sizeof(char *) * alloc);
        }
        names[n] = malloc(LLMETA_IXLEN);
        memcpy(names[n], out, LLMETA_IXLEN);
        ++n;
        uint8_t nxt[LLMETA_IXLEN];
        rc = bdb_lite_fetch_keys_fwd_tran(llmeta_bdb_state, t, out, nxt, 1,
                                          &fnd, bdberr);
        memcpy(out, nxt, sizeof(out));
    }
    *num = n;
    *ret = names;
    return rc;
}

// get keys and values for all matching keys
static int kv_get_kv(tran_type *t, void *k, size_t klen, void ***keys,
                     void ***values, int *num, int *bdberr)
{
    int fnd;
    int n = 0;
    int inc = 10;
    int alloc = 0;
    uint8_t out[LLMETA_IXLEN];
    void **vals = NULL;
    void **names = NULL;
    int rc = bdb_lite_fetch_partial_tran(llmeta_bdb_state, t, k, klen, out,
                                         &fnd, bdberr);
    while (rc == 0 && fnd == 1) {
        if (memcmp(k, out, klen) != 0) {
            break;
        }
        if (n == alloc) {
            alloc += inc;
            names = realloc(names, sizeof(char *) * alloc);
            vals = realloc(vals, sizeof(void *) * alloc);
        }
        names[n] = malloc(LLMETA_IXLEN);
        memcpy(names[n], out, LLMETA_IXLEN);

        void *dta;
        int dsz;
        rc = bdb_lite_exact_var_fetch_tran(llmeta_bdb_state, t, out, &dta, &dsz,
                                           bdberr);
        if (rc || *bdberr != BDBERR_NOERROR) {
            break;
        }
        vals[n] = dta;
        ++n;
        uint8_t nxt[LLMETA_IXLEN];
        rc = bdb_lite_fetch_keys_fwd_tran(llmeta_bdb_state, t, out, nxt, 1,
                                          &fnd, bdberr);
        memcpy(out, nxt, sizeof(out));
    }
    *num = n;
    *keys = names;
    *values = vals;
    return rc;
}

static int kv_del(tran_type *tran, void *k, int *bdberr)
{
    return bdb_lite_exact_del(llmeta_bdb_state, tran, k, bdberr);
}

/*
** Find partial key
** Find matching value
** Delete matching value
*/
static int kv_del_by_value(tran_type *tran, void *k, size_t klen, void *v,
                           size_t vlen, int *bdberr)
{
    int rc, fnd;
    uint8_t fndk[LLMETA_IXLEN];
    rc = bdb_lite_fetch_partial_tran(llmeta_bdb_state, tran, k, klen, fndk,
                                     &fnd, bdberr);
    while (rc == 0 && fnd == 1) {
        if (memcmp(k, fndk, klen) != 0) {
            break;
        }
        void *fndv;
        int flen;
        rc = bdb_lite_exact_var_fetch(llmeta_bdb_state, fndk, &fndv, &flen,
                                      bdberr);
        if (rc || *bdberr != BDBERR_NOERROR) {
            return -1;
        }
        if (vlen == flen && memcmp(v, fndv, vlen) == 0) {
            free(fndv);
            return bdb_lite_exact_del(llmeta_bdb_state, tran, fndk, bdberr);
        }
        free(fndv);
        uint8_t nxt[LLMETA_IXLEN];
        rc = bdb_lite_fetch_keys_fwd(llmeta_bdb_state, fndk, nxt, 1, &fnd,
                                     bdberr);
        memcpy(fndk, nxt, sizeof(fndk));
    }
    return rc;
}

static int kv_put_int(tran_type *tran, void *k, void *v, size_t vlen,
                      int *bdberr)
{
    int rc = kv_del(tran, k, bdberr);
    if (rc != 0 && *bdberr != BDBERR_DEL_DTA) {
        // key exists and failed to delete
        return rc;
    }
    return bdb_lite_add(llmeta_bdb_state, tran, v, vlen, k, bdberr);
}

static int kv_put(tran_type *tran, void *k, void *v, size_t vlen, int *bdberr)
{
    tran_type *t = tran ? tran : bdb_tran_begin(llmeta_bdb_state, NULL, bdberr);
    int rc = kv_put_int(t, k, v, vlen, bdberr);
    if (tran == NULL) {
        if (rc == 0)
            rc = bdb_tran_commit(llmeta_bdb_state, t, bdberr);
        else
            rc = bdb_tran_abort(llmeta_bdb_state, t, bdberr);
    }
    return rc;
}

/*
**
** bdb_kv_funcs() - operate on key-value pairs where:
** Schema:
**    key: llmetakey_t + genid (to make unique; don't want berk handling dups)
**  value: SP name or other generic payload
*/
typedef struct {
    llmetakey_t llkey;
    uint8_t padding[4];
    genid_t seq;
} llmeta_kv_key;
BB_COMPILE_TIME_ASSERT(key_seq, sizeof(llmeta_kv_key) == 4 + 4 + 8);

static int bdb_kv_get(llmetakey_t llkey, char ***ret, int *num, int *bdberr)
{
    llmetakey_t k = htonl(llkey);
    return kv_get(NULL, &k, sizeof(k), (void ***)ret, num, bdberr);
}

static int bdb_kv_put(tran_type *tran, llmetakey_t llkey, void *dta, int dsz,
                      int *bdberr)
{
    uint8_t buf[LLMETA_IXLEN] = {0};
    llmeta_kv_key *key = (llmeta_kv_key *)buf;
    key->llkey = htonl(llkey);
    key->seq = get_id(llmeta_bdb_state);
    return kv_put(tran, key, dta, dsz, bdberr);
}

static int bdb_kv_del_by_value(tran_type *tran, llmetakey_t k, void *v,
                               size_t vlen, int *bdberr)
{
    k = htonl(k);
    return kv_del_by_value(tran, &k, sizeof(k), v, vlen, bdberr);
}

struct sfunc_key {
    int32_t key; // LLMETA_LUA_SFUNC
    char func_name[LLMETA_TBLLEN];
};

static int bdb_llmeta_get_lua_sfunc_flags(char *func, int **flags, int *bdberr)
{
    union {
        struct sfunc_key skey;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};
    int num = 0;
    u.skey.key = htonl(LLMETA_LUA_SFUNC_FLAG);
    strncpy0(u.skey.func_name, func, sizeof(u.skey.func_name));

    int ** iflags = NULL;
    int rc = kv_get(NULL, &u, LLMETA_IXLEN, (void ***)&iflags, &num, bdberr);
    if (!rc && (num == 1) && iflags) {
        *flags = *iflags;
    } else {
        rc = 1000 + rc;
    }

    return rc;
}
static int bdb_llmeta_add_lua_sfunc_flag(char *name, int *flags, int *bdberr)
{
    union {
        struct sfunc_key skey;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};

    u.skey.key = htonl(LLMETA_LUA_SFUNC_FLAG);
    strncpy0(u.skey.func_name, name, sizeof(u.skey.func_name));

    return kv_put(NULL, &u, flags, sizeof(flags), bdberr);
}
static int bdb_llmeta_del_lua_sfunc_flag(char *func, int *bdberr)
{
    union {
        struct sfunc_key skey;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};

    u.skey.key = htonl(LLMETA_LUA_SFUNC_FLAG);
    strncpy0(u.skey.func_name, func, sizeof(u.skey.func_name));

    return kv_del(NULL, &u, bdberr);
}

// scalar lua function names
int bdb_llmeta_get_lua_sfuncs(void *sfuncs, int *bdberr)
{
    char **funcs = NULL;
    int *flags = NULL;
    int num = 0;
    int rc = bdb_kv_get(LLMETA_LUA_SFUNC, &funcs, &num, bdberr);

    for (int i = 0; i < num; ++i) {
        struct lua_func_t *sfunc = malloc(sizeof(struct lua_func_t));
        sfunc->name = funcs[i];
        sfunc->flags = 0;
        /** Note: Can't fetch flags failure is being ignored to maintain backward compatibility. **/
        if ((bdb_llmeta_get_lua_sfunc_flags(funcs[i], &flags, bdberr) != 0) || !flags) {
            logmsg(LOGMSG_WARN, "%s: failed fetching flags for lua sfunc %d\n", __func__, rc);
        } else {
            sfunc->flags = *flags;
        }
        listc_atl(sfuncs, sfunc);
    }
    return rc;
}

int bdb_llmeta_add_lua_sfunc(char *name, int *flag, int *bdberr)
{
    return bdb_llmeta_add_lua_sfunc_flag(name, flag, bdberr) ||
           bdb_kv_put(NULL, LLMETA_LUA_SFUNC, name, strlen(name) + 1, bdberr);
}
int bdb_llmeta_del_lua_sfunc(char *name, int *bdberr)
{
    return bdb_llmeta_del_lua_sfunc_flag(name, bdberr) ||
           bdb_kv_del_by_value(NULL, LLMETA_LUA_SFUNC, name, strlen(name) + 1, bdberr);
}

// aggregate lua function names

int bdb_llmeta_get_lua_afuncs(void *afuncs, int *bdberr)
{
    int rc = 0;
    char **funcs = NULL;
    int num = 0;
    if((rc = bdb_kv_get(LLMETA_LUA_AFUNC, &funcs, &num, bdberr) != 0)) {
        return rc;
    };
    for(int i = 0; i < num; ++i) {
        struct lua_func_t * afunc = malloc(sizeof(struct lua_func_t));
        afunc->name = funcs[i];
        // afunc->flags is unset as we don't store aggregate func flags
        listc_atl(afuncs, afunc);
    }
    return rc;
}

int bdb_llmeta_add_lua_afunc(char *name, int *flag, int *bdberr)
{
    return bdb_kv_put(NULL, LLMETA_LUA_AFUNC, name, strlen(name) + 1, bdberr);
}
int bdb_llmeta_del_lua_afunc(char *name, int *bdberr)
{
    return bdb_kv_del_by_value(NULL, LLMETA_LUA_AFUNC, name, strlen(name) + 1, bdberr);
}

/*
** Client versioned stored procedures
** Schema:
**   key: sp name + version
** value: contains lua src code
*/
struct versioned_sp {
    int32_t key; // LLMETA_VERSIONED_SP
    char name[LLMETA_SPLEN];
    char version[MAX_SPVERSION_LEN];
};
int bdb_add_versioned_sp(tran_type *t, char *name, char *version, char *src)
{
    union {
        struct versioned_sp sp;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};
    u.sp.key = htonl(LLMETA_VERSIONED_SP);
    strcpy(u.sp.name, name);
    strcpy(u.sp.version, version);
    int bdberr;
    int rc = kv_put(t, &u, src, strlen(src) + 1, &bdberr);
    if (rc == 0) {
        logmsg(LOGMSG_INFO, "Added SP %s:'%s'\n", name, version);
    }
    return rc;
}
int bdb_get_versioned_sp_tran(tran_type *tran, char *name, char *version, char **src)
{
    union {
        struct versioned_sp sp;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};
    u.sp.key = htonl(LLMETA_VERSIONED_SP);
    strncpy0(u.sp.name, name, sizeof(u.sp.name));
    strncpy0(u.sp.version, version, sizeof(u.sp.version));
    char **srcs;
    int rc, bdberr, num;
    rc = kv_get(tran, &u, sizeof(u), (void ***)&srcs, &num, &bdberr);
    if (rc == 0) {
        if (num == 1) {
            *src = srcs[0];
        } else { // logic error - expect only 1 match
            for (int i = 0; i < num; ++i) {
                free(srcs[i]);
            }
            rc = -1;
        }
    }
    free(srcs);
    return rc;
}

int bdb_get_versioned_sp(char *name, char *version, char **src)
{
    return bdb_get_versioned_sp_tran(NULL, name, version, src);
}

static int bdb_del_versioned_sp_int(tran_type *t, char *name, char *version)
{
    union {
        struct versioned_sp sp;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};
    u.sp.key = htonl(LLMETA_VERSIONED_SP);
    strcpy(u.sp.name, name);
    strcpy(u.sp.version, version);
    int bdberr, rc;
    if ((rc = kv_del(t, &u, &bdberr)) == 0) {
        logmsg(LOGMSG_INFO, "Deleted SP %s:'%s'\n", name, version);
    }
    return rc;
}
int bdb_del_versioned_sp(char *name, char *version)
{
    int del_default = 0;
    char *default_ver = NULL;
    bdb_get_default_versioned_sp(name, &default_ver);
    if (default_ver && strcmp(default_ver, version) == 0) {
        del_default = 1;
    }
    free(default_ver);

    int rc, bdberr;
    tran_type *t = bdb_tran_begin(llmeta_bdb_state, NULL, &bdberr);
    rc = bdb_del_versioned_sp_int(t, name, version);
    if (rc == 0 && del_default) {
        rc = bdb_del_default_versioned_sp(t, name);
    }
    if (rc == 0)
        rc = bdb_tran_commit(llmeta_bdb_state, t, &bdberr);
    else
        bdb_tran_abort(llmeta_bdb_state, t, &bdberr);
    return rc;
}

/*
** Given spname, map to a default client versioned string
** Schema:
**   key: sp name
** value: contains default version
*/
struct default_versioned_sp {
    int32_t key; // LLMETA_DEFAULT_VERSIONED_SP
    char name[LLMETA_TBLLEN];
};
static int bdb_set_default_versioned_sp_int(tran_type *tran, char *name,
                                            char *version)
{
    union {
        struct default_versioned_sp sp;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};
    u.sp.key = htonl(LLMETA_DEFAULT_VERSIONED_SP);
    strcpy(u.sp.name, name);
    int rc, bdberr;
    if ((rc = bdb_del_default_sp(tran, name, &bdberr)) != 0)
        return rc;
    return kv_put(tran, &u, version, strlen(version) + 1, &bdberr);
}
int bdb_set_default_versioned_sp(tran_type *t, char *name, char *version)
{
    int rc;
    if ((rc = bdb_set_default_versioned_sp_int(t, name, version)) != 0)
        return rc;
    logmsg(LOGMSG_INFO, "Default SP %s:%s\n", name, version);
    return 0;
}
int bdb_get_default_versioned_sp_tran(tran_type *tran, char *name, char **version)
{
    union {
        struct default_versioned_sp sp;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};
    u.sp.key = htonl(LLMETA_DEFAULT_VERSIONED_SP);
    if (strlen(name) >= LLMETA_IXLEN)
        return -1;
    strncpy0(u.sp.name, name, sizeof(u.sp.name));
    char **versions;
    int rc, bdberr, num;
    rc = kv_get(tran, &u, sizeof(u), (void ***)&versions, &num, &bdberr);
    if (rc == 0) {
        if (num == 1) {
            *version = versions[0];
        } else { // logic error - expect only 1 match
            for (int i = 0; i < num; ++i) {
                free(versions[i]);
            }
            rc = -1;
        }
    }
    free(versions);
    return rc;
}

int bdb_get_default_versioned_sp(char *name, char **version)
{
    return bdb_get_default_versioned_sp_tran(NULL, name, version);
}

int bdb_del_default_versioned_sp(tran_type *tran, char *name)
{
    union {
        struct default_versioned_sp sp;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};
    u.sp.key = htonl(LLMETA_DEFAULT_VERSIONED_SP);
    strncpy0(u.sp.name, name, sizeof(u.sp.name));
    int bdberr;
    int rc = kv_del(tran, &u, &bdberr);
    if (rc && bdberr == BDBERR_DEL_DTA)
        return 0;
    return rc;
}
static int bdb_get_sps_int_tran(tran_type *tran, llmetakey_t k, char ***names, int *num)
{
    k = htonl(k);
    union {
        struct versioned_sp sp;
        uint8_t buf[LLMETA_IXLEN];
    } * *v;
    int n, bdberr;
    int rc = kv_get_keys(tran, &k, sizeof(k), (void ***)&v, &n, &bdberr);
    char **ret = malloc(n * sizeof(char *));
    for (int i = 0; i < n; ++i) {
        ret[i] = strdup(v[i]->sp.name);
        free(v[i]);
    }
    free(v);
    *num = n;
    *names = ret;
    return rc;
}

static int bdb_get_sps_int(llmetakey_t k, char ***names, int *num)
{
    return bdb_get_sps_int_tran(NULL, k, names, num);
}

int bdb_get_versioned_sps(char ***names, int *num)
{
    return bdb_get_sps_int(LLMETA_VERSIONED_SP, names, num);
}

int bdb_get_versioned_sps_tran(tran_type *tran, char ***names, int *num)
{
    return bdb_get_sps_int_tran(tran, LLMETA_VERSIONED_SP, names, num);
}

int bdb_get_default_versioned_sps(char ***names, int *num)
{
    return bdb_get_sps_int(LLMETA_DEFAULT_VERSIONED_SP, names, num);
}
int bdb_get_all_for_versioned_sp_tran(tran_type *tran, char *name, char ***versions, int *num)
{
    union {
        struct versioned_sp sp;
        uint8_t buf[LLMETA_IXLEN];
    } k = {{0}}, **v;
    k.sp.key = htonl(LLMETA_VERSIONED_SP);
    strcpy(k.sp.name, name);
    size_t klen = sizeof(llmetakey_t) + strlen(name) + 1;
    int n, bdberr;
    int rc = kv_get_keys(tran, &k, klen, (void ***)&v, &n, &bdberr);
    char **ret = malloc(n * sizeof(char *));
    for (int i = 0; i < n; ++i) {
        ret[i] = strdup(v[i]->sp.version);
        free(v[i]);
    }
    free(v);
    *num = n;
    *versions = ret;
    return rc;
}

int bdb_get_all_for_versioned_sp(char *name, char ***versions, int *num)
{
    return bdb_get_all_for_versioned_sp_tran(NULL, name, versions, num);
}

static int bdb_process_each_entry(bdb_state_type *bdb_state, tran_type *tran, void *key, int klen,
                                  int (*func)(bdb_state_type *bdb_state, tran_type *tran, void *arg, void *rec),
                                  void *arg, int *bdberr)
{
    int fnd;
    uint8_t out[LLMETA_IXLEN];
    uint8_t nxt[LLMETA_IXLEN];
    int rc;
    int irc = 0;

    rc = bdb_lite_fetch_partial_tran(llmeta_bdb_state, tran, key, klen, out,
                                     &fnd, bdberr);
    while (rc == 0 && fnd == 1) {
        if (memcmp(key, out, klen) != 0) {
            break;
        }

        if ((irc = (*func)(bdb_state, tran, arg, out)) != 0)
            break;

        rc = bdb_lite_fetch_keys_fwd_tran(llmeta_bdb_state, tran, out, nxt, 1,
                                          &fnd, bdberr);
        memcpy(out, nxt, sizeof(out));
    }
    return irc ? irc : rc;
}

static int table_version_callback(bdb_state_type *bdb_state, tran_type *tran, void *arg,
                                  struct llmeta_sane_table_version *rec)
{
    const char *tblname = rec->tblname;
    int bdberr;
    int rc;

#if 0  
    /* This test would skip alter leaked file; we don't need to */
    if (get_dbnum_by_name(bdb_state, tblname) < 0)
    {
        fprintf(stderr, "Found deleted file %s\n", tblname);
        rc = bdb_purge_unused_files_by_name(tblname);
    }
#else
    rc = ((int (*)(bdb_state_type *, const char *, int *))arg)(
        bdb_state, tblname, &bdberr);
#endif

    return rc;
}

int bdb_process_each_table_version_entry(bdb_state_type *bdb_state,
                                         int (*func)(bdb_state_type *bdb_state,
                                                     const char *tblname,
                                                     int *bdberr),
                                         int *bdberr)
{
    struct llmeta_file_type_key key = {0};

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    key.file_type = htonl(LLMETA_TABLE_VERSION);

    return bdb_process_each_entry(bdb_state, NULL, &key, sizeof(key.file_type),
                                  (int (*)(bdb_state_type *, tran_type *, void *, void *))table_version_callback,
                                  (void *)func, bdberr);
}

static int table_file_callback(bdb_state_type *bdb_state, tran_type *tran, unsigned long long *file_version,
                               struct llmeta_file_type_dbname_file_num_key *rec)
{
    int rc;
    unsigned long long version;
    int fndlen;
    int bdberr;

    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, tran, rec, &version, sizeof(version), &fndlen, &bdberr);

    return (rc == 0 && fndlen == sizeof(version) && version == *file_version);
}

static int bdb_process_each_table_entry(bdb_state_type *bdb_state,
                                        tran_type *tran, int type,
                                        const char *tblname,
                                        unsigned long long version, int *bdberr)
{
    struct llmeta_file_type_dbname_key key_struct = {0};
    char key[LLMETA_IXLEN] = {0};
    uint8_t *p_buf, *p_buf_start, *p_buf_end;
    size_t key_offset = 0;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    key_struct.file_type = type;
    strncpy0(key_struct.dbname, tblname, sizeof(key_struct.dbname));
    key_struct.dbname_len = strlen(key_struct.dbname) + 1 /* NULL byte */;

    if (key_struct.dbname_len > LLMETA_TBLLEN) {
        fprintf(stderr, "%s: db_name is too long\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    p_buf_start = p_buf = (uint8_t *)key;
    p_buf_end = (uint8_t *)(key + LLMETA_IXLEN);

    p_buf = llmeta_file_type_dbname_key_put(&key_struct, p_buf, p_buf_end);

    if (!p_buf) {
        logmsg(LOGMSG_ERROR,
               "%s: llmeta_file_type_dbname_key_put returns NULL\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    key_offset = p_buf - p_buf_start;

    return bdb_process_each_entry(bdb_state, tran, &key, key_offset,
                                  (int (*)(bdb_state_type *, tran_type *, void *, void *))table_file_callback, &version,
                                  bdberr);
}

int bdb_process_each_table_dta_entry(bdb_state_type *bdb_state, tran_type *tran,
                                     const char *tblname,
                                     unsigned long long version, int *bdberr)
{
    return bdb_process_each_table_entry(
        bdb_state, tran, LLMETA_FVER_FILE_TYPE_DTA, tblname, version, bdberr);
}

int bdb_process_each_table_idx_entry(bdb_state_type *bdb_state, tran_type *tran,
                                     const char *tblname,
                                     unsigned long long version, int *bdberr)
{
    return bdb_process_each_table_entry(
        bdb_state, tran, LLMETA_FVER_FILE_TYPE_IX, tblname, version, bdberr);
}

/*
** Save hashed passwords
** Key layout is same as plaintext passwd for: LLMETA_USER_PASSWORD
** Key is: LLMETA_USER_PASSWORD_HASH
** Value is: 'struct passwd_hash' below
*/

#define PASSWD_HASH_SZ 32
#define PASSWD_SALT_SZ 32

typedef union {
    struct llmeta_user_password passwd;
    uint8_t buf[LLMETA_IXLEN];
} passwd_key;
typedef struct {
    uint8_t salt[PASSWD_SALT_SZ];
    uint8_t hash[PASSWD_HASH_SZ];
} passwd_v0;
typedef struct {
    uint8_t ver;
    union {
        passwd_v0 p0;
    } u;
    int niterations;
} passwd_hash;

static int llmeta_get_user_passwd(tran_type *tran, char *user, llmetakey_t type, void ***out)
{
    int bdberr;
    int num = 0;
    passwd_key key;
    memset(&key, 0, sizeof(key));
    key.passwd.file_type = htonl(type);
    strcpy(key.passwd.user, user);
    int rc = kv_get(tran, &key, sizeof(key), out, &num, &bdberr);
    if (rc == 0 && num == 1) return 0;
    if (*out) {
        void **data = *out;
        for (int i = 0; i < num; ++i) {
            free(data[i]);
        }
        free(*out);
        *out = NULL;
    }
    return 1;
}
#define ITERATIONS_V0 1000
#define ITERATIONS_V1 16 * 1024
#define ITERATIONS_MIN 4096
int gbl_pbkdf2_iterations = ITERATIONS_MIN;

int set_pbkdf2_iterations(int val)
{
    if (val < ITERATIONS_MIN) {
        logmsg(LOGMSG_ERROR,
               "Number of iterations of PBKDF2 too low. Minimum is %d.\n",
               ITERATIONS_MIN);
        return 1;
    }
    gbl_pbkdf2_iterations = val;
    return 0;
}

/*
  Check whether the user exists.

  @return:
    1        user exists
    0        user does not exist
    -1       error
*/
int bdb_user_exists(tran_type *tran, char *user)
{
    int rc = 0;
    void **data = NULL;
    size_t ulen = strlen(user) + 1;
    if (ulen > LLMETA_USER_LEN) {
        rc = -1;
        goto out;
    }

    /* Check cleartext password */
    if (llmeta_get_user_passwd(tran, user, LLMETA_USER_PASSWORD, &data) == 0) {
        rc = 1;
        goto out;
    }

    /* Check password hash */
    if (llmeta_get_user_passwd(tran, user, LLMETA_USER_PASSWORD_HASH, &data) ==
        0) {
        rc = 1;
        goto out;
    }

out:
    if (data) {
        free(*data);
        free(data);
    }
    return rc;
}

int gbl_max_password_cache_size = 100;
static lrucache *password_cache; // saved password hashes
static pthread_mutex_t password_cache_mu = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    uint8_t key[PASSWD_HASH_SZ]; // Hash of plaintext password
    uint8_t password[PASSWD_HASH_SZ];
    int iterations;
    lrucache_link lnk;
} password_cache_entry_t;

static unsigned int password_hash(const void *p, int len)
{
    unsigned int h = 0;
    uint8_t *key = (uint8_t *)p;
    for (int i = 0; i < PASSWD_HASH_SZ; ++ i) {
        h = ((h % 8388013) << 8) + (key[i]);
    }
    return h;
}

static int password_cmp(const void *key1, const void *key2, int len)
{
    return CRYPTO_memcmp(key1, key2, PASSWD_HASH_SZ);
}

void init_password_cache()
{
    if (gbl_max_password_cache_size <= 0) {
        return;
    }
    password_cache = lrucache_init(password_hash, password_cmp, free,
                                   offsetof(password_cache_entry_t, lnk),
                                   offsetof(password_cache_entry_t, key),
                                   PASSWD_HASH_SZ, gbl_max_password_cache_size);
}

void destroy_password_cache()
{
    if (!password_cache)
      return;

    lrucache_destroy(password_cache);
    password_cache = 0;
}

int bdb_user_password_check(tran_type *tran, char *user, char *passwd, int *valid_user)
{
    int passwd_rc = 1;
    void **data = NULL;
    if (valid_user) {
        *valid_user = 0;
    }
    size_t ulen = strlen(user) + 1;
    if (ulen > LLMETA_USER_LEN) {
        goto out;
    }
    // check cleartext password
    if (llmeta_get_user_passwd(tran, user, LLMETA_USER_PASSWORD, &data) == 0) {
        passwd_rc = strcmp(data[0], passwd);
        if (valid_user)
            *valid_user = 1;
        goto out;
    }
    // check password hash
    if (llmeta_get_user_passwd(tran, user, LLMETA_USER_PASSWORD_HASH, &data) != 0) {
        goto out;
    }
    unsigned iterations;
    passwd_hash computed, *stored = data[0];
    switch (stored->ver) {
    case 0: iterations = ITERATIONS_V0; break;
    case 1: iterations = ITERATIONS_V1; break;
    case 2:
        iterations = ntohl(stored->niterations);
        break;
    default: logmsg(LOGMSG_ERROR, "bad passwd ver:%u", stored->ver); goto out;
    }

    if (valid_user) {
        *valid_user = 1;
    }

    // Since calculating password hashes can be CPU intensive, we maintain a
    // hash table of passwords and their respective hashes. In the following
    // lines, we check if the hash for the specified password is already stored
    // in the table.
    uint8_t key[PASSWD_HASH_SZ]; // Password key for lookup
    PKCS5_PBKDF2_HMAC_SHA1(passwd, strlen(passwd), stored->u.p0.salt,
                           sizeof(stored->u.p0.salt), 1, sizeof(key), key);

    password_cache_entry_t *password_entry = NULL;

    if (gbl_max_password_cache_size > 0) {
        Pthread_mutex_lock(&password_cache_mu);
        password_entry = lrucache_find(password_cache, &key);
        Pthread_mutex_unlock(&password_cache_mu);

    }

    uint8_t matching_entry_found = 0;
    if (password_entry) {
        if (password_entry->iterations == iterations) {
            memcpy(computed.u.p0.hash, password_entry->password,
                   sizeof(stored->u.p0.hash));
            matching_entry_found = 1;
        }

        Pthread_mutex_lock(&password_cache_mu);
        lrucache_release(password_cache, &key);
        Pthread_mutex_unlock(&password_cache_mu);
    }

    if (matching_entry_found == 0) {
        PKCS5_PBKDF2_HMAC_SHA1(passwd, strlen(passwd), stored->u.p0.salt,
                               sizeof(stored->u.p0.salt), iterations,
                               sizeof(computed.u.p0.hash), computed.u.p0.hash);
    }

    passwd_rc = CRYPTO_memcmp(computed.u.p0.hash, stored->u.p0.hash,
                              sizeof(stored->u.p0.hash));

    // Add matching password entry to the password cache
    if ((gbl_max_password_cache_size > 0) && (passwd_rc == 0) &&
        !password_entry) {
        password_cache_entry_t *entry;
        entry = malloc(sizeof(password_cache_entry_t));

        if (entry) {
            entry->iterations = iterations;
            memcpy(entry->key, key, sizeof(key));
            memcpy(entry->password, computed.u.p0.hash, sizeof(computed.u.p0.hash));

            Pthread_mutex_lock(&password_cache_mu);
            lrucache_add(password_cache, entry);
            Pthread_mutex_unlock(&password_cache_mu);
        }
    }

out:
    if (data) {
        free(*data);
        free(data);
    }

    return passwd_rc;
}
int bdb_user_password_set(tran_type *tran, char *user, char *passwd)
{
    int pbkdf2_niters = gbl_pbkdf2_iterations;
    passwd_key key = {{0}};
    size_t ulen = strlen(user) + 1;
    if (ulen > sizeof(key.passwd.user))
        return -1;
    memcpy(key.passwd.user, user, ulen);
    key.passwd.file_type = htonl(LLMETA_USER_PASSWORD);
    int bdberr, rc;
    rc = kv_del(tran, &key, &bdberr);
    if (rc && bdberr != BDBERR_DEL_DTA)
        return rc;
    key.passwd.file_type = htonl(LLMETA_USER_PASSWORD_HASH);
    passwd_hash data;
    data.ver = 2;
    data.niterations = htonl(pbkdf2_niters);
    RAND_bytes(data.u.p0.salt, sizeof(data.u.p0.salt));
    PKCS5_PBKDF2_HMAC_SHA1(passwd, strlen(passwd), data.u.p0.salt,
                           sizeof(data.u.p0.salt), pbkdf2_niters,
                           sizeof(data.u.p0.hash), data.u.p0.hash);
    return kv_put(tran, &key, &data, sizeof(data), &bdberr);
}
int bdb_user_password_delete(tran_type *tran, char *user)
{
    passwd_key key = {{0}};
    size_t ulen = strlen(user) + 1;
    if (ulen > sizeof(key.passwd.user))
        return -1;
    memcpy(key.passwd.user, user, ulen);
    key.passwd.file_type = htonl(LLMETA_USER_PASSWORD);
    int bdberr, rc;
    rc = kv_del(tran, &key, &bdberr);
    if (rc && bdberr != BDBERR_DEL_DTA)
        return rc;
    key.passwd.file_type = htonl(LLMETA_USER_PASSWORD_HASH);
    rc = kv_del(tran, &key, &bdberr);
    if (rc && bdberr == BDBERR_DEL_DTA)
        return 0;
    return rc;
}

int bdb_user_get_all_tran(tran_type *tran, char ***users, int *num)
{
    void **u1, **u2;
    int key, n1, n2, bdberr;
    key = htonl(LLMETA_USER_PASSWORD);
    kv_get_keys(tran, &key, sizeof(key), &u1, &n1, &bdberr);
    key = htonl(LLMETA_USER_PASSWORD_HASH);
    kv_get_keys(tran, &key, sizeof(key), &u2, &n2, &bdberr);
    int n = n1 + n2;
    u1 = realloc(u1, sizeof(void *) * n);
    memcpy(u1 + n1, u2, sizeof(void *) * n2);
    free(u2);
    for (int i = 0; i < n; ++i) {
        struct llmeta_user_password *k = u1[i];
        memmove(&k->file_type, &k->user, strlen(k->user) + 1);
    }
    *users = (char **)u1;
    *num = n;
    return 0;
}

int bdb_user_get_all(char ***users, int *num)
{
    return bdb_user_get_all_tran(NULL, users, num);
}

/*
 * For now, this will not delete the old page sizes; it will create new sizes
 * under the new
 * name
 *
 */
int bdb_rename_table_pagesizes(bdb_state_type *bdb_state, tran_type *tran,
                               const char *newname, int *bdberr)
{
#define NFUNCS 3
    int (*rfuncs[NFUNCS])(bdb_state_type *, tran_type *, int *, int *) = {
        bdb_get_pagesize_data, bdb_get_pagesize_blob, bdb_get_pagesize_index};
    int (*wfuncs[NFUNCS])(bdb_state_type *, tran_type *, int, int *) = {
        bdb_set_pagesize_data, bdb_set_pagesize_blob, bdb_set_pagesize_index};
    int rc;
    int pagesize = 0;
    char *orig_name;
    int i;

    orig_name = bdb_state->name;
    for (i = 0; i < NFUNCS; i++) {
        rc = rfuncs[i](bdb_state, tran, &pagesize, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR) {
            if (*bdberr == BDBERR_FETCH_DTA) {
                *bdberr = 0;
                return 0;
            }
            return rc;
        }

        orig_name = bdb_state->name;
        bdb_state->name = (char *)newname;
        rc = wfuncs[i](bdb_state, tran, pagesize, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR) {
            bdb_state->name = orig_name;
            return rc;
        }
        bdb_state->name = orig_name;
    }
    return 0;
}

/* rename all csc2 versions for table "tblname" to "newtblname" */
int bdb_rename_csc2_version(tran_type *trans, const char *tblname,
                            const char *newtblname, int ver, int *bdberr)
{
    int rc;
    char key[LLMETA_IXLEN] = {0};
    char new_key[LLMETA_IXLEN] = {0};
    struct llmeta_file_type_dbname_csc2_vers_key vers_key;
    struct llmeta_file_type_dbname_csc2_vers_key new_vers_key;

    logmsg(LOGMSG_DEBUG, "%s renaming from '%s' to '%s', version %d\n",
           __func__, tblname, newtblname, ver);

    vers_key.file_type = LLMETA_CSC2;
    strncpy0(vers_key.dbname, tblname, sizeof(vers_key.dbname));
    vers_key.dbname_len = strlen(vers_key.dbname) + 1;
    new_vers_key.file_type = LLMETA_CSC2;
    strncpy0(new_vers_key.dbname, newtblname, sizeof(new_vers_key.dbname));
    new_vers_key.dbname_len = strlen(new_vers_key.dbname) + 1;

    while (ver >= 0) {
        vers_key.csc2_vers = ver;
        new_vers_key.csc2_vers = ver;
        llmeta_file_type_dbname_csc2_vers_key_put(
            &vers_key, (uint8_t *)key, (uint8_t *)key + LLMETA_IXLEN);
        llmeta_file_type_dbname_csc2_vers_key_put(
            &new_vers_key, (uint8_t *)new_key,
            (uint8_t *)new_key + LLMETA_IXLEN);
        char *fnddta = NULL;
        int fndlen = 0;
        rc = bdb_lite_exact_var_fetch_tran(llmeta_bdb_state, trans, &key,
                                           (void **)&fnddta, &fndlen, bdberr);
        if (!rc) {
            rc = bdb_lite_add(llmeta_bdb_state, trans, fnddta, fndlen, &new_key,
                              bdberr);
            if (fnddta)
                free(fnddta);
            if (rc && *bdberr != BDBERR_NOERROR) {
                logmsg(LOGMSG_ERROR, "%s() add failed for ver: %d\n", __func__,
                       ver);
                return rc;
            }
            logmsg(LOGMSG_DEBUG,
                   "%s added table '%s' (old table '%s') version %d\n",
                   __func__, newtblname, tblname, ver);
        } else {
            if (ver == 0)
                return 0;
            logmsg(LOGMSG_DEBUG,
                   "%s didn't find old table '%s' version %d (so "
                   "not adding new-table '%s'?)\n",
                   __func__, tblname, ver, newtblname);
        }

        rc = bdb_lite_exact_del(llmeta_bdb_state, trans, &key, bdberr);
        if (rc && *bdberr != BDBERR_NOERROR && *bdberr != BDBERR_DEL_DTA) {
            logmsg(LOGMSG_ERROR, "%s() del failed for ver: %d\n", __func__,
                   ver);
            return rc;
        }
        --ver;
    }
    return 0;
}

/* rename the file with new version numbers */
int bdb_rename_files(bdb_state_type *bdb_state, tran_type *tran,
                     const char *newname, int *bdberr)
{
    int rc;

    /* generate new version and rename files per newname */
    rc = bdb_rename_table(bdb_state, tran, (char *)newname, bdberr);
    if (rc)
        return rc;

    /* delete file versions for old file */
    rc = bdb_del_file_versions(bdb_state, tran, bdberr);

    return rc;
}

/* rename csonparameters for table "oldname" */
int bdb_rename_table_csonparameters(void *tran, const char *oldname,
                                    const char *newname)
{
    char *cson = NULL;
    int cson_len = 0;
    int rc;

    rc = bdb_get_table_csonparameters(tran, oldname, &cson, &cson_len);
    if (rc) {
        if (rc == 1)
            rc = 0; /* not found */
        goto done;
    }
    rc = bdb_del_table_csonparameters(tran, oldname);
    if (rc) {
        goto done;
    }
    rc = bdb_set_table_csonparameters(tran, newname, cson, cson_len);
    if (rc) {
        goto done;
    }
done:
    if (cson)
        free(cson);
    return rc;
}

/* rename transactionally all llmeta information about a table */
int bdb_rename_table_metadata(bdb_state_type *bdb_state, tran_type *tran,
                              const char *newname, int version, int *bdberr)
{
    int rc;

    /* create custom page sizes, if any */
    rc = bdb_rename_table_pagesizes(bdb_state, tran, newname, bdberr);
    if (rc)
        return rc;

    /* rename cson parameters */
    rc = bdb_rename_table_csonparameters(tran, bdb_state->name, newname);
    if (rc)
        return rc;

    /* rename csc2 */
    rc = bdb_rename_csc2_version(tran, bdb_state->name, newname, version,
                                 bdberr);
    if (rc)
        return rc;

    /* rename files finally, with new versions */
    rc = bdb_rename_files(bdb_state, tran, newname, bdberr);
    if (rc)
        return rc;

    return rc;
}

int bdb_get_sc_start_lsn(tran_type *tran, const char *table, void *plsn,
                         int *bdberr)
{
    int rc;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_schema_change_type schema_change;
    int fndlen;
    uint8_t *p_buf = (uint8_t *)key, *p_buf_end = (p_buf + LLMETA_IXLEN);
    DB_LSN *lsn = (DB_LSN *)plsn;

    DB_LSN tmplsn;
    struct llmeta_db_lsn_data_type lsn_data;

    *bdberr = BDBERR_NOERROR;

    schema_change.file_type = LLMETA_SC_START_LSN;
    /*copy the table name and check its length so that we have a clean key*/
    strncpy0(schema_change.dbname, table, sizeof(schema_change.dbname));
    schema_change.dbname_len = strlen(schema_change.dbname) + 1;

    if (!(llmeta_schema_change_type_put(&(schema_change), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_schema_change_type_put returns NULL\n",
               __func__);
        logmsg(LOGMSG_ERROR, "%s: check the length of table: %s\n", __func__,
               table);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    rc = bdb_lite_exact_fetch_tran(llmeta_bdb_state, tran, key, &tmplsn,
                                   sizeof(tmplsn), &fndlen, bdberr);
    if (rc == 0) {
        p_buf = (uint8_t *)&tmplsn;
        p_buf_end = p_buf + sizeof(*lsn);
        if (!(llmeta_db_lsn_data_type_get(&lsn_data, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR,
                   "%s: llmeta_db_lsn_data_type_get returns NULL\n", __func__);
            *bdberr = BDBERR_BADARGS;
            return -1;
        }
        lsn->file = lsn_data.lsn.file;
        lsn->offset = lsn_data.lsn.offset;
    }
    return rc;
}

int bdb_set_sc_start_lsn(tran_type *tran, const char *table, void *plsn,
                         int *bdberr)
{
    int rc;
    int started_our_own_transaction = 0;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_schema_change_type schema_change;
    uint8_t *p_buf = (uint8_t *)key, *p_buf_end = (p_buf + LLMETA_IXLEN);

    DB_LSN *lsn = (DB_LSN *)plsn;
    struct llmeta_db_lsn_data_type lsn_data_type;
    DB_LSN oldlsn;
    DB_LSN tmplsn;
    uint8_t *p_data_buf, *p_data_buf_end;

    *bdberr = BDBERR_NOERROR;

    if (tran == NULL) {
        started_our_own_transaction = 1;
        tran = bdb_tran_begin(llmeta_bdb_state->parent, NULL, bdberr);
        if (tran == NULL) {
            logmsg(LOGMSG_ERROR, "%s: bdb_tran_begin returns NULL\n", __func__);
            return -1;
        }
    }

    schema_change.file_type = LLMETA_SC_START_LSN;
    /*copy the table name and check its length so that we have a clean key*/
    strncpy0(schema_change.dbname, table, sizeof(schema_change.dbname));
    schema_change.dbname_len = strlen(schema_change.dbname) + 1;

    if (!(llmeta_schema_change_type_put(&(schema_change), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_schema_change_type_put returns NULL\n",
               __func__);
        logmsg(LOGMSG_ERROR, "%s: check the length of table: %s\n", __func__,
               table);
        *bdberr = BDBERR_BADARGS;
        rc = -1;
        goto done;
    }

    lsn_data_type.lsn.file = lsn->file;
    lsn_data_type.lsn.offset = lsn->offset;

    p_data_buf = (uint8_t *)&tmplsn;
    p_data_buf_end = p_data_buf + sizeof(tmplsn);
    if (!(llmeta_db_lsn_data_type_put(&lsn_data_type, p_data_buf,
                                      p_data_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_db_lsn_data_type_put returns NULL\n",
               __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    rc = bdb_get_sc_start_lsn(tran, table, &oldlsn, bdberr);
    if (rc) { // not found, just add -- should refactor
        if (*bdberr == BDBERR_FETCH_DTA) {
            rc = bdb_lite_add(llmeta_bdb_state, tran, &tmplsn, sizeof(tmplsn),
                              key, bdberr);
            if (rc || *bdberr) {
                logmsg(LOGMSG_ERROR, "%s:%d failed with rc %d bdberr %d\n",
                       __func__, __LINE__, rc, *bdberr);
            }
        }
        goto done;
    }

    rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, bdberr);
    if (rc && *bdberr != BDBERR_DEL_DTA) {
        logmsg(LOGMSG_ERROR, "bdb_lite_exact_del rc %d bdberr %d\n", rc,
               *bdberr);
        goto done;
    }

    rc = bdb_lite_add(llmeta_bdb_state, tran, &tmplsn, sizeof(tmplsn), key,
                      bdberr);
    if (rc || *bdberr) {
        logmsg(LOGMSG_ERROR, "%s:%d failed with rc %d bdberr %d\n", __func__,
               __LINE__, rc, *bdberr);
    }

done:
    if (started_our_own_transaction) {
        if (rc == 0) {
            rc = bdb_tran_commit(llmeta_bdb_state->parent, tran, bdberr);
            if (rc || *bdberr) {
                logmsg(LOGMSG_ERROR, "%s:%d failed with rc %d bdberr %d\n",
                       __func__, __LINE__, rc, *bdberr);
            }
        } else {
            int arc;
            arc = bdb_tran_abort(llmeta_bdb_state->parent, tran, bdberr);
            if (arc)
                rc = arc;
        }
    }
    return rc;
}

int bdb_delete_sc_start_lsn(tran_type *tran, const char *table, int *bdberr)
{
    int rc;
    int started_our_own_transaction = 0;
    char key[LLMETA_IXLEN] = {0};
    struct llmeta_schema_change_type schema_change;
    uint8_t *p_buf = (uint8_t *)key, *p_buf_end = (p_buf + LLMETA_IXLEN);

    *bdberr = BDBERR_NOERROR;

    if (tran == NULL) {
        started_our_own_transaction = 1;
        tran = bdb_tran_begin(llmeta_bdb_state->parent, NULL, bdberr);
        if (tran == NULL)
            return -1;
    }

    schema_change.file_type = LLMETA_SC_START_LSN;
    /*copy the table name and check its length so that we have a clean key*/
    strncpy0(schema_change.dbname, table, sizeof(schema_change.dbname));
    schema_change.dbname_len = strlen(schema_change.dbname) + 1;

    if (!(llmeta_schema_change_type_put(&(schema_change), p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: llmeta_schema_change_type_put returns NULL\n",
               __func__);
        logmsg(LOGMSG_ERROR, "%s: check the length of table: %s\n", __func__,
               table);
        *bdberr = BDBERR_BADARGS;
        rc = -1;
        goto done;
    }

    rc = bdb_lite_exact_del(llmeta_bdb_state, tran, key, bdberr);
    if (*bdberr == BDBERR_DEL_DTA) {
        rc = 0;
        *bdberr = BDBERR_NOERROR;
    }

done:
    if (started_our_own_transaction) {
        if (rc == 0)
            rc = bdb_tran_commit(llmeta_bdb_state->parent, tran, bdberr);
        else {
            int arc;
            arc = bdb_tran_abort(llmeta_bdb_state->parent, tran, bdberr);
            if (arc)
                rc = arc;
        }
    }
    return rc;
}

/* View key */
struct llmeta_view_key {
    int file_type;
    char view_name[LLMETA_TBLLEN]; /* View name must be NULL terminated */
};

/* Fetch all view names */
int bdb_get_view_names(tran_type *t, char **names, int *num)
{
    union {
        struct llmeta_view_key key;
        uint8_t buf[LLMETA_IXLEN];
    } * *v;
    int rc, n, bdberr;
    llmetakey_t k;

    k = htonl(LLMETA_VIEW);
    rc = kv_get_keys(t, &k, sizeof(k), (void ***)&v, &n, &bdberr);
    if (rc || (n == 0)) {
        *num = 0;
        return rc;
    }

    for (int i = 0; i < n; ++i) {
        names[i] = strdup(v[i]->key.view_name);
        free(v[i]);
    }
    free(v);
    *num = n;
    return rc;
}

/* Fetch a specific view */
int bdb_get_view(tran_type *t, const char *view_name, char **view_def)
{
    union {
        struct llmeta_view_key key;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};
    int rc, bdberr, num;
    char **view_defs;

    /* Type */
    u.key.file_type = htonl(LLMETA_VIEW);
    /* View name */
    strncpy0(u.key.view_name, view_name, sizeof(u.key.view_name));

    rc = kv_get(t, &u, sizeof(u), (void ***)&view_defs, &num, &bdberr);
    if (rc == 0) {
        if (num == 1) {
            *view_def = view_defs[0];
        } else { // logical error: there can't be more that one view definition
                 // per view
            for (int i = 0; i < num; i++) {
                free(view_defs[i]);
            }
            rc = 1;
        }
    }
    free(view_defs);
    return rc;
}

/* Add the given view */
int bdb_put_view(tran_type *t, const char *view_name, char *view_def)
{
    union {
        struct llmeta_view_key key;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};
    int rc, bdberr;

    /* Type */
    u.key.file_type = htonl(LLMETA_VIEW);
    /* View name */
    strncpy0(u.key.view_name, view_name, sizeof(u.key.view_name));

    rc = kv_put(t, &u, view_def, strlen(view_def) + 1, &bdberr);
    if (rc == 0) {
        logmsg(LOGMSG_INFO, "View '%s' added\n", view_name);
    }
    return rc;
}

/* Delete the given view */
int bdb_del_view(tran_type *t, const char *view_name)
{
    union {
        struct llmeta_view_key key;
        uint8_t buf[LLMETA_IXLEN];
    } u = {{0}};
    int rc, bdberr;

    /* Type */
    u.key.file_type = htonl(LLMETA_VIEW);
    /* View name */
    strncpy0(u.key.view_name, view_name, sizeof(u.key.view_name));

    rc = kv_del(t, &u, &bdberr);
    if (rc == 0) {
        logmsg(LOGMSG_INFO, "View '%s' deleted\n", view_name);
    }
    return rc;
}

#include "schemachange.h"

/*
  DRQS-170879936:

  In version R8, some backwards incompatible changes got introduced into
  the schema change object that broke the object's original deserializer
  function (buf_get_schemachange()). As a result, reading an sc status object
  created by R7 would fail if read by R8 (via comdb2_sc_status).

  The fix was to keep the both the versions of the deserializer functions and
  invoke them appropriately.

  The current (potential hackish) method to pick the right version on the
  deserializer function is based on the content of the first 4 bytes of the
  LLMETA_SCHEMACHANGE_STATUS payload, where it is assumed that the valid
  values of s->kind (between SC_INVALID and SC_LAST, exclusive) will not
  coincide with the first 4 bytes of the rqid (fastseed) stored as the first
  member in old (7.0's) LLMETA_SCHEMACHANGE_STATUS payload.
*/
static int buf_get_schemachange_key_type(void *p_buf, void *p_buf_end)
{
    int first = 0;

    if (p_buf >= p_buf_end) return -1;

    buf_get(&first, sizeof(first), p_buf, p_buf_end);

    if (first > SC_INVALID && first < SC_LAST) {
        return LLMETA_SCHEMACHANGE_STATUS_V2;
    }
    return LLMETA_SCHEMACHANGE_STATUS;
}

void *buf_get_schemachange(struct schema_change_type *s, void *p_buf,
                           void *p_buf_end)
{
    int sc_key_type = buf_get_schemachange_key_type(p_buf, p_buf_end);

    switch (sc_key_type) {
    case LLMETA_SCHEMACHANGE_STATUS:
        return buf_get_schemachange_v1(s, (void *)p_buf, (void *)p_buf_end);
    case LLMETA_SCHEMACHANGE_STATUS_V2:
        return buf_get_schemachange_v2(s, (void *)p_buf, (void *)p_buf_end);
    default:
        break;
    }
    return NULL;
}
