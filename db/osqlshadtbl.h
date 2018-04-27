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

#ifndef _OSQL_SHADTBL_H_
#define _OSQL_SHADTBL_H_

#include <comdb2.h>

struct BtCursor;
struct sql_thread;
struct sqlclntstate;
struct blob_buffer;
struct osqlstate;

struct tmp_table {
    void *table;
    /*int flags;*/
};

struct shad_tbl {
    struct tmp_table *add_tbl; /* all inserts/updates go here,
                                key=tbl->seq, data=newrow */

    hash_t *addidx_hash; /* remember which index to add/del */
    hash_t *delidx_hash;

    struct tmp_table *upd_tbl; /* all updates go here also,
                                  key=tbl->seq, data=original_genid */
    struct tmp_table *blb_tbl; /* all blobs go here,
                                  key=tbl->seq:blob_index; data=blob_row */

    /* indexes on expressions: replicants are responsible for creating key
     * values */
    struct tmp_table *delidx_tbl; /* all sqlite idxDeletes go here,
                                     key=tbl->seq:ixnum; data=key_value*/
    struct temp_cursor *delidx_cur;
    struct tmp_table *insidx_tbl; /* all sqlite idxInserts go here,
                                     key=tbl->seq:ixnum; data=key_value*/
    struct temp_cursor *insidx_cur;

    struct temp_cursor *add_cur; /* cursors for each table */
    struct temp_cursor *upd_cur;
    struct temp_cursor *blb_cur;

    unsigned long long seq; /* used to generate uniq row ids */
    struct dbenv *env;
    char tablename[MAXTABLELEN];
    int tableversion;
    int nix;
    int ix_expr;
    int ix_partial;
    struct dbtable *db; /* TODO: db has dbenv, chop it */
    int dbnum;
    int nblobs;
    int updcols; /* 1 if we have update columns */
    int nops;    /* count how many rows are correctly processed */
    LINKC_T(struct shad_tbl) linkv; /* have to link em */
};

struct recgenidlst;
typedef struct recgenidlst recgenidlst_t;
typedef struct shad_tbl shad_tbl_t;

void osql_set_skpcur(struct BtCursor *pCur);

void *osql_get_shadtbl_addtbl(struct BtCursor *pCur);
void *osql_get_shadtbl_addtbl_newcursor(struct BtCursor *pCur);
void *osql_open_shadtbl_addtbl(struct BtCursor *pCur);
void *osql_get_shadtbl_updtbl(struct BtCursor *pCur);
void *osql_get_shadtbl_skpcur(struct BtCursor *pCur);

int osql_save_delrec(struct BtCursor *pCur, struct sql_thread *thd);
int osql_save_insrec(struct BtCursor *pCur, struct sql_thread *thd, char *pData,
                     int nData);
int osql_save_updrec(struct BtCursor *pCur, struct sql_thread *thd, char *pData,
                     int nData);
int osql_save_qblobs(struct BtCursor *pCur, struct sql_thread *thd,
                     struct blob_buffer *blobs, int maxblobs, int is_update);
int osql_save_index(struct BtCursor *pCur, struct sql_thread *thd,
                    int is_update, int is_delete);
int osql_save_updcols(struct BtCursor *pCur, struct sql_thread *thd,
                      int *updCols);
int osql_save_dbq_consume(struct sqlclntstate *, const char *spname, genid_t);

void *osql_get_shadow_bydb(struct sqlclntstate *clnt, struct dbtable *db);
int osql_fetch_shadblobs_by_genid(struct BtCursor *pCur, int *blobnum,
                                  blob_status_t *blobs, int *bdberr);
int osql_get_shadowdata(struct BtCursor *pCur, unsigned long long genid,
                        void **buf, int *buflen, int *bdberr);

/**
 * Scan the shadow tables for the current transaction
 * and send to the master the ops
 */
int osql_process_shadtbl(struct sqlclntstate *clnt, int *nops, int *bdberr);

/**
 * Clear the rows from the shadow tables at the end of a transaction
 *
 */
int osql_shadtbl_cleartbls(struct sqlclntstate *clnt);

/**
 * Frees shadow tables used by this sql client
 *
 */
void osql_shadtbl_close(struct sqlclntstate *clnt);

/**
 * Truncate all the shadow tables but for selectv records
 *
 */
int osql_shadtbl_reset_for_selectv(struct sqlclntstate *clnt);

/**
 * Open cursors for shadow tables, if any are present
 *
 */
int osql_shadtbl_begin_query(bdb_state_type *bdb_env,
                             struct sqlclntstate *clnt);

/**
 * Close cursors for shadow tables, if any are present
 *
 */
int osql_shadtbl_done_query(bdb_state_type *bdb_env, struct sqlclntstate *clnt);

/**
 * Record a genid for this transaction
 *
 */
int osql_save_recordgenid(struct BtCursor *pCur, struct sql_thread *thd,
                          unsigned long long genid);

/**
 * Check if a genid was recorded
 */
int is_genid_recorded(struct sql_thread *thd, struct BtCursor *pCur,
                      unsigned long long genid);

/**
 * Record a schemachange for this transaction
 *
 */
int osql_save_schemachange(struct sql_thread *thd,
                           struct schema_change_type *sc, int usedb);

/**
 * Record a schemachange for this transaction
 *
 */
int osql_save_bpfunc(struct sql_thread *thd, BpfuncArg *arg);

/**
 * Process shadow tables
 *
 */
int osql_shadtbl_process(struct sqlclntstate *clnt, int *nops, int *bdberr,
                         int restarting);

/**
 *  Check of a shadow table transaction has cached selectv records
 *
 */
int osql_shadtbl_has_selectv(struct sqlclntstate *clnt, int *bdberr);

/**
 * Check if there are any shadow tables to process
 *
 */
int osql_shadtbl_empty(struct sqlclntstate *clnt);

/**
 * Check if there are any shadow tables to process
 * or if it only contains usedb record
 */
int osql_shadtbl_usedb_only(struct sqlclntstate *clnt);

#endif
