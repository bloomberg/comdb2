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


#define FDB_VER FDB_VER_PROXY

/* cc2 ftw */
#define fdb_ver_encoded(ver) (-(ver + 1))
#define fdb_ver_decoded(ver) (-(ver + 1))

enum fdb_errors {
    FDB_NOERR = 0 /* NO ERROR */
    ,
    FDB_ERR_GENERIC = -1 /* failure, generic */
    ,
    FDB_ERR_WRITE_IO = -2 /* failure writing on a socket */
    ,
    FDB_ERR_READ_IO = -3 /* failure reading from a socket */
    ,
    FDB_ERR_MALLOC = -4 /* failed allocation */
    ,
    FDB_ERR_UNSUPPORTED = -5 /* recognizing a protocol option */
    ,
    FDB_ERR_TRAN_IO =
        -6 /* failed communication with a node involved in a transaction*/
    ,
    FDB_ERR_FDB_VERSION =
        -7 /* sent by FDB_VER_CODE_VERSION and above to downgrade protocol */
    ,
    FDB_ERR_FDB_NOTFOUND = -8 /* fdb name not found */
    ,
    FDB_ERR_FDB_TBL_NOTFOUND = -9 /* fdb found but table not existing */
    ,
    FDB_ERR_CLASS_UNKNOWN = -10 /* explicit class in fdb name, unrecognized */
    ,
    FDB_ERR_CLASS_DENIED =
        -11 /* denied access to the class, either implicit or explicit */
    ,
    FDB_ERR_REGISTER_NOTFOUND = -12 /* unable to access comdb2db */
    ,
    FDB_ERR_REGISTER_NODB = -13 /* no information about fdb name in comdb2db */
    ,
    FDB_ERR_REGISTER_NONODES = -14 /* no nodes available for the fdb */
    ,
    FDB_ERR_REGISTER_IO = -15 /* failure talking to comdb2db */
    ,
    FDB_ERR_REGISTER_NORESCPU = -16 /* no known node is rescpu */
    ,
    FDB_ERR_PTHR_LOCK = -17 /* pthread locks are mashed */
    ,
    FDB_ERR_CONNECT = -18 /* failed to connect to db */
    ,
    FDB_ERR_CONNECT_CLUSTER = -19 /* failed to connect to cluster */
    ,
    FDB_ERR_BUG = -20 /* bug in the code, should not see this */
    ,
    FDB_ERR_SOCKPOOL = -21 /* sockpool return error */
    ,
    FDB_ERR_PI_DISABLED = -22 /* foreign table has partial indexes but the
                                 feature is disabled locally */
    ,
    FDB_ERR_EXPRIDX_DISABLED =
        -23 /* foreign table has expressions indexes but the feature is
               disabled locally */
    ,
    FDB_ERR_INDEX_DESCRIBE = -24 /* failed to describe index */
    ,
    FDB_ERR_SSL = -25 /* SSL configuration error */
    ,
    FDB_ERR_ACCESS = -26 /* Access error */
    ,
    FDB_ERR_TRANSIENT_IO = -27 /* Temporary IO failure */
};

extern int gbl_fdb_push_remote;

#define fdb_is_error(n) ((n) < FDB_NOERR)
#define fdb_node_invalid(n) (fdb_is_error((n)))

typedef struct fdb_tbl_ent fdb_tbl_ent_t;
typedef struct fdb_tbl fdb_tbl_t;
typedef struct fdb fdb_t;
typedef struct fdb_cache fdb_cache_t;
typedef struct fdb_cursor fdb_cursor_t;
typedef struct fdb_access fdb_access_t;
typedef struct fdb_affinity fdb_affinity_t;

typedef struct fdb_sqlstat_cache fdb_sqlstat_cache_t;
typedef struct fdb_sqlstat_table fdb_sqlstat_table_t;
typedef struct fdb_sqlstat_cursor fdb_sqlstat_cursor_t;

struct fdb_tran {
    char *tid; /* transaction id */
    char *
        host; /* what is the remote replicant this transaction is submitted to
                 */
    int rc;   /* cached result */
    int errstrlen; /* length of err string, if any */
    char *errstr;  /* error string */
    fdb_t *fdb;    /* pointer to the foreign db */
    SBUF2 *sb;     /* connection to this fdb */

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
};
typedef struct fdb_tran fdb_tran_t;

struct fdb_distributed_tran {
    int remoted; /* set to 1 if this is the remote part of a transaction */
    LISTC_T(fdb_tran_t) fdb_trans; /* list of subtransactions */
};
typedef struct fdb_distributed_tran fdb_distributed_tran_t;

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

    int (*insert)(BtCursor *pCur, struct sqlclntstate *clnt, fdb_tran_t *trans,
                  unsigned long long genid, int datalen, char *data);
    int (*delete)(BtCursor *pCur, struct sqlclntstate *clnt, fdb_tran_t *trans,
                  unsigned long long genid);
    int (*update)(BtCursor *pCur, struct sqlclntstate *clnt, fdb_tran_t *trans,
                  unsigned long long oldgenid, unsigned long long genid,
                  int datalen, char *data);

    fdb_tbl_ent_t *(*table_entry)(BtCursor *pCur);

    int (*access)(BtCursor *pCur, int how);

} fdb_cursor_if_t;

/**
 * Initialize the remote cursor cache
 *
 */
int fdb_cache_init(int n);

/**
 * Retrieve a foreign db object
 * The callers of this function should make sure a table lock is acquired
 * Such by calling fdb_lock_table().
 *
 */
fdb_t *get_fdb(const char *dbname);

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
char *fdb_sqlexplain_get_name(int rootpage);

/**
 * Retrieve the field name for the table identified by "rootpage", index
 * "ixnum",
 * field "fieldnum"
 */
char *fdb_sqlexplain_get_field_name(Vdbe *v, int rootpage, int ixnum,
                                    int fieldnum);

/**
 * Create a connection to fdb
 *
 */
fdb_cursor_if_t *fdb_cursor_open(struct sqlclntstate *clnt, BtCursor *pCur,
                                 int rootpage, fdb_tran_t *trans, int *ixnum,
                                 int need_ssl);

/**
 * Retrieve/create space for a Btree schema change (per foreign db)
 *
 */
Schema *fdb_sqlite_get_schema(Btree *pBt, int nbytes);

/*
   This returns the sqlstats table under a mutex
 */
fdb_sqlstat_cache_t *fdb_sqlstats_get(fdb_t *fdb);
void fdb_sqlstats_put(fdb_t *fdb);

/**
 * Get dbname, tablename, and so on
 *
 */
const char *fdb_dbname_name(fdb_t *fdb);
const char *fdb_dbname_class_routing(fdb_t *fdb);
const char *fdb_table_entry_tblname(fdb_tbl_ent_t *ent);
const char *fdb_table_entry_dbname(fdb_tbl_ent_t *ent);

/**
 * Retrieve entry for table|index given a fdb and name
 *
 */
fdb_tbl_ent_t *fdb_table_entry_by_name(fdb_t *fdb, const char *name);

int fdb_is_sqlite_stat(fdb_t *fdb, int rootpage);

/* transactional api */
fdb_tran_t *fdb_trans_begin_or_join(struct sqlclntstate *clnt, fdb_t *fdb,
                                    char *ptid, int use_ssl);
fdb_tran_t *fdb_trans_join(struct sqlclntstate *clnt, fdb_t *fdb, char *ptid);
int fdb_trans_commit(struct sqlclntstate *clnt, enum trans_clntcomm sideeffects);
int fdb_trans_rollback(struct sqlclntstate *clnt);
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
void fdb_clear_sqlclntstate(struct sqlclntstate *clnt);

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
int fdb_lock_table(sqlite3_stmt *pStmt, struct sqlclntstate *clnt, Table *tab,
                   fdb_tbl_ent_t **p_ent);

/**
 * Unlock a remote table schema cache
 *
 * This matches fdb_lock_table, allowing again exclusive access to that table
 *
 */
int fdb_unlock_table(fdb_tbl_ent_t *ent);

/**
 * Send heartbeats to remote dbs in a distributed transaction
 *
 */
int fdb_heartbeats(struct sqlclntstate *clnt);

/**
 * Change association of a cursor to a table (see body note)
 *
 */
void fdb_cursor_use_table(fdb_cursor_t *cur, struct fdb *fdb,
                          const char *tblname);

/* return if ssl is needed */
int fdb_cursor_need_ssl(fdb_cursor_if_t *cur);

/**
 * Retrieve the schema of a remote table
 *
 */
int fdb_get_remote_version(const char *dbname, const char *table,
                           enum mach_class class, int local,
                           unsigned long long *version);

int fdb_table_exists(int rootpage);

int fdb_set_genid_deleted(fdb_tran_t *, unsigned long long);
int fdb_is_genid_deleted(fdb_tran_t *, unsigned long long);

extern int gbl_fdb_incoherence_percentage;
extern int gbl_fdb_io_error_retries;

#endif

