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
extern volatile int gbl_dbopen_gen;
extern volatile int gbl_lua_version;
extern volatile int gbl_analyze_gen;
extern volatile int gbl_views_gen;

typedef enum scdone {
    alter,
    fastinit,
    add = fastinit, /* but, get_dbtable_by_name == NULL */
    drop,
    bulkimport,
    setcompr,
    luareload,
    sc_analyze,
    bthash,
    rowlocks_on,
    rowlocks_on_master_only,
    rowlocks_off,
    views,
    llmeta_queue_add,
    llmeta_queue_alter,
    llmeta_queue_drop,
    genid48_enable,
    genid48_disable,
    lua_sfunc,
    lua_afunc,
    llmeta_sequence_add,
    llmeta_sequence_alter,
    llmeta_sequence_drop,
} scdone_t;

int bdb_llog_scdone_tran(bdb_state_type *bdb_state, scdone_t type,
                         tran_type *tran, int *bdberr);
int bdb_llog_scdone(bdb_state_type *, scdone_t, int wait, int *bdberr);
int bdb_llog_sequences_tran(bdb_state_type *bdb_state, char *name,
                            scdone_t type, tran_type *tran, int *bdberr);
int bdb_llog_luareload(bdb_state_type *, int wait, int *bdberr);
int bdb_llog_analyze(bdb_state_type *, int wait, int *bdberr);
int bdb_llog_views(bdb_state_type *, char *name, int wait, int *bdberr);
int bdb_llog_rowlocks(bdb_state_type *, scdone_t, int *bdberr);
int bdb_llog_genid_format(bdb_state_type *, scdone_t, int *bdberr);
int bdb_reload_rowlocks(bdb_state_type *, scdone_t, int *bdberr);
int bdb_llog_luafunc(bdb_state_type *, scdone_t, int wait, int *bdberr);
/* run on the replecants after the master is done so that they can reload/update
 * their copies of the modified database */
typedef int (*SCDONEFP)(bdb_state_type *, const char table[], scdone_t);
/* aborts a schema change if one is in progress and waits for it to finish */
typedef void (*SCABORTFP)(void);

#endif
