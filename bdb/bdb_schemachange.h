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

#ifndef BDB_SCHEMACHANGE_H
#define BDB_SCHEMACHANGE_H

extern int gbl_maxretries;
extern volatile int gbl_lua_version;
extern volatile uint32_t gbl_analyze_gen;
extern volatile int gbl_views_gen;

/* NOTE: keep this in sync with SCDONE_CALLBACKS */
typedef enum scdone {
    invalid = -1,            // -1
    alter,                   //  0
    fastinit,                //  1
    add = fastinit,          //  1
    drop,                    //  2
    bulkimport,              //  3
    setcompr,                //  4
    luareload,               //  5
    sc_analyze,              //  6
    bthash,                  //  7
    rowlocks_on,             //  8
    rowlocks_on_master_only, //  9
    rowlocks_off,            // 10
    views,                   // 11
    llmeta_queue_add,        // 12
    llmeta_queue_alter,      // 13
    llmeta_queue_drop,       // 14
    genid48_enable,          // 15
    genid48_disable,         // 16
    lua_sfunc,               // 17
    lua_afunc,               // 18
    rename_table,            // 19
    change_stripe,           // 20
    user_view,               // 21
    add_queue_file,          // 22
    del_queue_file,          // 23
    alias_table,             // 24
    alias                    // 25
} scdone_t;

#define BDB_BUMP_DBOPEN_GEN(type, msg) \
    bdb_bump_dbopen_gen(type, (msg), \
                        __func__, __FILE__, __LINE__)

const char *bdb_get_scdone_str(scdone_t type);

void bdb_bump_dbopen_gen(scdone_t type, const char *message,
                        const char *funcName, const char *fileName, int lineNo);

int bdb_llog_scdone_tran(bdb_state_type *bdb_state, scdone_t type,
                         tran_type *tran, const char *tbl, int tbllen, int *bdberr);
int bdb_llog_scdone(bdb_state_type *, scdone_t, const char *tablename, 
                    int tablenamelen, int wait, int *bdberr);
int bdb_llog_luareload(bdb_state_type *, int wait, int *bdberr);
int bdb_llog_analyze(bdb_state_type *, int wait, int *bdberr);
int bdb_llog_views(bdb_state_type *bdb_state, char *name, int wait,
                   int *bdberr);
int bdb_llog_partition(bdb_state_type *bdb_state, tran_type *tran, char *name,
                       int *bdberr);
int bdb_llog_rowlocks(bdb_state_type *, scdone_t, int *bdberr);
int bdb_llog_genid_format(bdb_state_type *, scdone_t, int *bdberr);
int bdb_reload_rowlocks(bdb_state_type *, scdone_t, int *bdberr);
int bdb_llog_luafunc(bdb_state_type *, scdone_t, int wait, int *bdberr);
/* run on the replecants after the master is done so that they can reload/update
 * their copies of the modified database */
typedef int (*SCDONEFP)(bdb_state_type *, const char table[], void *arg,
                        scdone_t);
/* aborts a schema change if one is in progress and waits for it to finish */
typedef void (*SCABORTFP)(void);

void bdb_replace_cached_data_version(bdb_state_type *target, bdb_state_type *new);
void bdb_replace_cached_blob_version(bdb_state_type *target, int targetnum, bdb_state_type *new, int newnum);
void bdb_replace_cached_index_version(bdb_state_type *target, int targetnum, bdb_state_type *new, int newnum);
int bdb_llog_alias(bdb_state_type *bdb_state, int wait, int *bdberr); 
#endif
