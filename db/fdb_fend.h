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

/**
 * Foreign db front-end
 *
 * This api is used by sqlglue.c to access remote data
 * A cache of foreign db is also maintained here
 *
 */

#ifndef _FDB_FEND_H_
#define _FDB_FEND_H_

#include "list.h"
#include <sbuf2.h>

#include "comdb2.h"
#include "sql.h"
#include "sqliteInt.h"
#include "vdbeInt.h"
#include "comdb2uuid.h"
#include "net_int.h"
#include "fdb_fend_minimal.h"

/**
 * REMOTE SQL VERSIONING
 *
 * In order to provide an extensible protocol, FDB_VER numbers are used to
 * determine which code version the server runs. The intent is to make the
 * versions properly communicate with each other, and a server running newer
 * code to downgrade its features to match the older server.
 *
 * NOTE:
 * - code versionon was not backed initially in the protocol, therefore the
 * way to implement this is hacky in some places
 * - code version check is done when a cursor is open coming from a remote
 * source
 * - newer servers on the receive side will be able to downgrade and match
 * an older originator starting with FDB_VER_CODE_VERSION
 * - older servers on the receive side that pre-dates release
 * FDB_VER_CODE_VERSION are not able to recognize versioning, therefore
 * FDB_VER_CODE_VERSION is built to run fully compatible with FDB_VER_LEGACY.
 * FDB_VER_CODE_VERSION will add code to check code version and ask for
 * lower version if version is too high
 * - future FDB_VER > FDB_VER_CODE_VERSION, if any, will be incompatible
 * with FDB_VER_LEGACY (relies on code version check); such a release will
 * be possible only after FDB_VER_CODE_VERSION is runnning everywhere
 * - code version is sent in repurpused rootpage unused field from CURSOR_OPEN,
 * value being sent is -(FDB_VER+1), to skip >=0 and -1 which were used in
 * FDB_VER_LEGACY
 *
 */

#define FDB_VER_LEGACY 0
#define FDB_VER_CODE_VERSION 1
#define FDB_VER_SOURCE_ID 2
#define FDB_VER_WR_NAMES 3
#define FDB_VER_SSL 4
#define FDB_VER_PROXY 5
#define FDB_VER_AUTH 6
#define FDB_VER_CDB2API 7
#define FDB_VER_WR_CDB2API 8
#define FDB_VER_2PC_CDB2API 9

#define FDB_VER FDB_VER_2PC_CDB2API

extern int gbl_fdb_default_ver;

#define FDB_2PC_VER 1

/* cc2 ftw */
#define fdb_ver_encoded(ver) (-(ver + 1))
#define fdb_ver_decoded(ver) (-(ver + 1))

extern int gbl_fdb_push_remote;
extern int gbl_fdb_push_remote_write;
extern int gbl_fdb_push_redirect_foreign;

#define fdb_is_error(n) ((n) < FDB_NOERR)
#define fdb_node_invalid(n) (fdb_is_error((n)))

typedef struct fdb_tbl fdb_tbl_t;
typedef struct fdb_cache fdb_cache_t;
typedef struct fdb_cursor fdb_cursor_t;

typedef struct fdb_sqlstat_cache fdb_sqlstat_cache_t;
typedef struct fdb_sqlstat_table fdb_sqlstat_table_t;
typedef struct fdb_sqlstat_cursor fdb_sqlstat_cursor_t;

struct fdb_tran {
    char magic[4];
    char *tid; /* transaction id */
    char *
        host; /* what is the remote replicant this transaction is submitted to
                 */
    int rc;   /* cached result */
    int errstrlen; /* length of err string, if any */
    char *errstr;  /* error string */
    fdb_t *fdb;    /* pointer to the foreign db */
    cdb2_effects_tp last_effects; /* only set in handle_fdb_push_write  */
    int is_cdb2api;
    union {
        SBUF2 *sb;     /* connection to this fdb */
        cdb2_hndl_tp *hndl; /* cdb2api connection, iff is_cdb2api == 1 */
    } fcon;

    LINKC_T(
        struct fdb_tran) lnk; /* chain of subtransactions, part of the same
                                 distributed transaction */
    LISTC_T(struct svc_cursor)
        cursors; /* list of cursors for tran (used on backend side) */

    uuid_t tiduuid;

    int seq; /* sequencing tran begin/commit/rollback, writes, cursor open/close
                */

    /*
    ** Genid deduplication. Nothing fancy. It's just a temptable recording
    ** deleted or updated genid-s that have been sent to the remote database
    ** (similar to osql's "skip shadow", but much simpler).
    */
    bdb_state_type *bdb_state;
    struct temp_table *dedup_tbl;
    struct temp_cursor *dedup_cur;
    int nwrites; /* number of writes (ins/upd/del) issues on the fdb tran */

    /**
     * libevent heartbeats
     */
    fdb_hbeats_type hbeats;
    int rmt2pcvers;
};
typedef struct fdb_tran fdb_tran_t;

struct fdb_distributed_tran {
    int remoted; /* set to 1 if this is the remote part of a transaction */
    LISTC_T(fdb_tran_t) fdb_trans; /* list of subtransactions */
};

typedef struct fdb_cursor_if {
    fdb_cursor_t *impl;

    char *(*id)(BtCursor *pCur);
    char *(*data)(BtCursor *pCur);
    int (*datalen)(BtCursor *pCur);
    unsigned long long (*genid)(BtCursor *pCur);
    void (*get_found_data)(BtCursor *pCur, unsigned long long *genid,
                           int *datalen, char **data);
    int (*move)(BtCursor *pCur, int how);
    int (*close)(BtCursor *pCur);
    int (*find)(BtCursor *pCur, Mem *key, int nfields, int descending);
    int (*find_last)(BtCursor *pCur, Mem *key, int nfields, int descending);

    int (*set_hint)(BtCursor *pCur, void *hint);
    void *(*get_hint)(BtCursor *pCur);

    int (*set_sql)(BtCursor *pCur, const char *sql);
    char *(*name)(BtCursor *pCur);
    char *(*tblname)(BtCursor *pCur);
    int (*tbl_has_partidx)(BtCursor *pCur);
    int (*tbl_has_expridx)(BtCursor *pCur);
    char *(*dbname)(BtCursor *pCur);

    int (*insert)(BtCursor *pCur, sqlclntstate *clnt, fdb_tran_t *trans,
                  unsigned long long genid, int datalen, char *data);
    int (*delete)(BtCursor *pCur, sqlclntstate *clnt, fdb_tran_t *trans,
                  unsigned long long genid);
    int (*update)(BtCursor *pCur, sqlclntstate *clnt, fdb_tran_t *trans,
                  unsigned long long oldgenid, unsigned long long genid,
                  int datalen, char *data);
    int (*create_tran)(sqlclntstate *clnt, fdb_t *fdb, int use_ssl);

    int (*access)(BtCursor *pCur, int how);

} fdb_cursor_if_t;

/**
 * Initialize the remote cursor cache
 *
 */
int fdb_cache_init(int n);

/**
 * Retrieve a fdb object
 * Protected by the fdbs array mutex
 * If found, the object returned is read locked
 *
 */
enum fdb_get_flag { FDB_GET_NOLOCK = 0, FDB_GET_LOCK = 1 };
fdb_t *get_fdb_int(const char *dbname, enum fdb_get_flag flag, const char *f, int l);
#define get_fdb(dbname, flag) get_fdb_int(dbname, flag, __func__, __LINE__)

/**
 * Remove the read lock on a fdb object
 * Protected by the fdbs array mutex
 * Flag controls the removal;
 * - NOFREE: the fdb is read unlocked and left in the fdbs array to be reused
 * - TRYFREE: we try to write lock the fdb; if we succeed, this is the only reader
 *   so it will be unlinked from cache and freed
 * - FORCEFREE: under fdbs array mutex we block until a write lock is acquired
 *   !!!CAUTION this blocks new access to fdbs until the write lock is acquired
 */
enum fdb_put_flag { FDB_PUT_NOFREE = 0, FDB_PUT_TRYFREE = 1, FDB_PUT_FORCEFREE = 2 };
void put_fdb_int(fdb_t *fdb, enum fdb_put_flag flag, const char *f, int l);
#define put_fdb(dbname, flag) put_fdb_int(dbname, flag, __func__, __LINE__)

/**
 * Move a cursor on sqlite_master table
 *
 */
int fdb_cursor_move_master(BtCursor *pCur, int *pRes, int how);

/**
 * Retrieve the sqlite_master row size for provided entry
 *
 */
int fdb_get_sqlite_master_entry_size(fdb_t *fdb, fdb_tbl_ent_t *ent);

/**
 * Retrieve the sqlite_master row size for provided entry
 *
 */
void *fdb_get_sqlite_master_entry(fdb_t *fdb, fdb_tbl_ent_t *ent);

/**
 * Retrieve the name for a specific rootpage
 * Caller must free the returned pointer
 *
 */
char *fdb_sqlexplain_get_name(struct sqlclntstate *clnt, int rootpage);

/**
 * Retrieve the field name for the table identified by "rootpage", index
 * "ixnum", field "fieldnum"
 */
char *fdb_sqlexplain_get_field_name(struct sqlclntstate *clnt, Vdbe *v, int rootpage, int ixnum, int fieldnum);

/**
 * Create a connection to fdb
 *
 */
fdb_cursor_if_t *fdb_cursor_open(sqlclntstate *clnt, BtCursor *pCur,
                                 int rootpage, fdb_tran_t *trans, int *ixnum,
                                 int need_ssl);

/**
 * Release lock on fdb sqlstats_mtx
 *
 */
void fdb_sqlstats_put(fdb_t *fdb);

/**
 * Get dbname, tablename, and so on
 *
 */
const char *fdb_table_entry_tblname(fdb_tbl_ent_t *ent);
const char *fdb_table_entry_dbname(fdb_tbl_ent_t *ent);

/**
 * Get table entries from sqlite vdbe cache
 *
 */
fdb_tbl_ent_t *fdb_clnt_cache_get_ent(sqlclntstate *clnt, int rootpage);

/* transactional api */
fdb_tran_t *fdb_trans_begin_or_join(sqlclntstate *clnt, fdb_t *fdb,
                                    int use_ssl, int *created);
fdb_tran_t *fdb_trans_join(sqlclntstate *clnt, fdb_t *fdb);
int fdb_trans_commit(sqlclntstate *clnt, enum trans_clntcomm sideeffects);
int fdb_trans_rollback(sqlclntstate *clnt);
char *fdb_trans_id(fdb_tran_t *trans);

char *fdb_get_alias(const char **p_tablename);
void fdb_stat_alias(void);

/**
 * This function will check some critical regions
 * hanging if something is wrong
 *
 */
void fdb_sanity_check(void);

/**
 * Check if master table access if local or remote
 *
 */
int fdb_master_is_local(BtCursor *);

/**
 * Process remote messages
 *
 */
int fdb_process_message(const char *line, int lline);

/**
 * Hint: reduce a 64b version to 32b hint
 *
 * Explanation: the original protocol included a 32bit
 * allowance for table version.  I decided to make the
 * llmeta table version 64 bit, but in order to avoid any
 * rollout issues I decided to keep the protocol only 32 bit
 * I extract 32bit hint from the 64bit unique version.
 * Unless I am able to generate 2^32 schema changes in for the
 * lifetime duration of a sqlite engine (i.e. node turn),
 * this should not be a problem
 *
 */
int fdb_table_version(unsigned long long version);

/**
 * Clear sqlclntstate fdb_state object
 *
 */
void fdb_clear_sqlclntstate(sqlclntstate *clnt);

/**
 * Clear sqlite* schema for a certain remote table
 *
 * NOTE: sqlite_stat schemas are also cleared
 *
 */
void fdb_clear_sqlite_cache(sqlite3 *sqldb, const char *dbname,
                            const char *tblname);

/**
 * Lock a remote table schema cache
 *
 * A remote schema change will trigger a flush of local schema cache
 * The lock prevents the flush racing against running remote access
 *
 */
int fdb_lock_table(sqlite3_stmt *pStmt, sqlclntstate *clnt, Table *tab,
                   fdb_tbl_ent_t **p_ent);

/**
 * Unlock a remote table schema cache
 *
 * This matches fdb_lock_table, allowing again exclusive access to that table
 *
 */
int fdb_unlock_table(sqlclntstate *clnt, fdb_tbl_ent_t *ent);

/**
 * Send heartbeats to remote dbs in a distributed transaction
 *
 */
int fdb_heartbeats(fdb_hbeats_type *hbeats);

/**
 * Close sbuf2, destroy mutex and free fdb-tran
 *
 */
void fdb_heartbeat_free_tran(fdb_hbeats_type *hbeats);

/* return if ssl is needed */
int fdb_cursor_need_ssl(fdb_cursor_if_t *cur);

int fdb_set_genid_deleted(fdb_tran_t *, unsigned long long);
int fdb_is_genid_deleted(fdb_tran_t *, unsigned long long);

extern int gbl_fdb_incoherence_percentage;
extern int gbl_fdb_io_error_retries;

int process_fdb_set_cdb2api(sqlclntstate *clnt, char *sqlstr,
                            char *err, int errlen);

int process_fdb_set_cdb2api_2pc(sqlclntstate *clnt, char *sqlstr,
                                char *err, int errlen);

/**
 * Check that fdb class matches a specific class
 *
 */
int fdb_check_class_match(fdb_t *fdb, int local, enum mach_class class,
                          int class_override);

/**
 * Connect to a remote cluster based of push connector information
 * and additional configuration options
 *
 */
cdb2_hndl_tp *fdb_push_connect(sqlclntstate *clnt, int *client_redir,
                               struct errstat *err);

/**
 * Free resources for a specific fdb_tran
 *
 */
void fdb_free_tran(sqlclntstate *clnt, fdb_tran_t *tran);

/**
 * Initialize a 2pc coordinator
 *
 */
void fdb_init_disttxn(sqlclntstate *clnt);

/**
 * SET the options for a distributed 2pc transaction
 *
 */
int fdb_2pc_set(sqlclntstate *clnt, fdb_t *fdb, cdb2_hndl_tp *hndl);

/**
 *  Create a fdb push connector
 *
 */
fdb_push_connector_t* fdb_push_create(const char *dbname, enum mach_class class, int override, int local,
                                      enum ast_type type);

/**
 * Callback for cdb2api transport I/O retry
 *
 */
const char *fdb_retry_callback(void *arg);

/**
 * Populate temp tables with stats from remote db
 * The fdb sqlstats_mtx is acquired at this point
 *
 */
int fdb_sqlstat_cache_populate(struct sqlclntstate *clnt, fdb_t *fdb,
                               /* out */ struct temp_table *stat1,
                               /* out */ struct temp_table *stat4,
                               /* out */ int *nrows_stat1,
                               /* out */ int *nrows_stat4);

/**
 * Return 1 if rootpage is for a sqlite_state table,
 * or if no table exists for that rootpage
 *
 */
int fdb_is_sqlite_stat(sqlclntstate *clnt, int rootpage);

#endif

