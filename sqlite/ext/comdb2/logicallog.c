/*
   Copyright 2017 Bloomberg Finance L.P.

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

#include "sqlite3ext.h"
#include "logicallog.h"
#include <assert.h>
#include <string.h>
#include "comdb2.h"
#include "build/db.h"
#include "dbinc/db_swap.h"
#include "dbinc_auto/txn_auto.h"
#include <bdb/bdb_int.h>

/* Column numbers */
#define LOGICALLOG_COLUMN_START        0
#define LOGICALLOG_COLUMN_STOP         1
#define LOGICALLOG_COLUMN_FLAGS        2
#define LOGICALLOG_COLUMN_LSN          3
#define LOGICALLOG_COLUMN_RECTYPE      4
#define LOGICALLOG_COLUMN_GENERATION   5
#define LOGICALLOG_COLUMN_LOG          6

extern pthread_mutex_t gbl_logput_lk;
extern pthread_cond_t gbl_logput_cond;
extern pthread_mutex_t gbl_durable_lsn_lk;
extern pthread_cond_t gbl_durable_lsn_cond;

/* Modeled after generate_series */
typedef struct logicallog_cursor logicallog_cursor;
struct logicallog_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
  sqlite3_int64 idx;
  DB_LSN curLsn;             /* Current LSN */
  DB_LSN minLsn;             /* Minimum LSN */
  DB_LSN maxLsn;             /* Maximum LSN */
  char *minLsnStr;
  char *maxLsnStr;
  char *curLsnStr;
  int flags;           /* 1 if we should block */
  int hitLast;
  int notDurable;
  int openCursor;
  int subop;
  DB_LOGC *logc;             /* Log Cursor */
  DBT data;
  bdb_osql_log_t *log;
  void *packed;
  void *unpacked;
};

static int logicallogConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  sqlite3_vtab *pNew;
  int rc;
  rc = sqlite3_declare_vtab(db,
     "CREATE TABLE x(minlsn hidden,maxlsn hidden, flags hidden,lsn,rectype,generation,payload)");
  if( rc==SQLITE_OK ){
    pNew = *ppVtab = sqlite3_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }
  return rc;
}

static int logicallogDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int logicallogOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  logicallog_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

static int logicallogClose(sqlite3_vtab_cursor *cur){
  logicallog_cursor *pCur = (logicallog_cursor*)cur;
  if (pCur->openCursor) {
      assert(pCur->logc);
      pCur->logc->close(pCur->logc, 0);
      pCur->openCursor = 0;
  }
  if (pCur->data.data)
      free(pCur->data.data);
  if (pCur->minLsnStr)
      sqlite3_free(pCur->minLsnStr);
  if (pCur->maxLsnStr)
      sqlite3_free(pCur->maxLsnStr);
  if (pCur->curLsnStr)
      sqlite3_free(pCur->curLsnStr);
  if (pCur->packed)
      sqlite3_free(pCur->packed);
  if (pCur->unpacked)
      sqlite3_free(pCur->unpacked);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int is_commit(u_int32_t rectype)
{
    switch(rectype) {
        case DB___txn_regop:
        case DB___txn_regop_gen:
        case DB___txn_regop_rowlocks:
            return 1;
        default:
            return 0;
    }
}

static inline retrieve_start_lsn(DBT *data, u_int32_t rectype, DB_LSN *lsn)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    DB_ENV *dbenv = bdb_state->dbenv;
	__txn_regop_args *txn_args;
	__txn_regop_gen_args *txn_gen_args;
	__txn_regop_rowlocks_args *txn_rl_args;

    switch(rectype) {
        case DB___txn_regop:
            if ((rc = __txn_regop_read(dbenv, data->data,
                            &txn_args)) != 0) {
                logmsg(LOGMSG_ERROR, "%s line %d regop read returns %d for "
                        "%d:%d\n", __func__, __LINE__, rc, lsn->file,
                        lsn->offset);
                return 1;
            }
            if (txn_args->opcode != TXN_COMMIT) {
                logmsg(LOGMSG_ERROR, "%s line %d regop opcode not commit, %d "
                        "for %d:%d\n", __func__, __LINE__, txn_args->opcode,
                        lsn->file, lsn->offset);
                __os_free(dbenv, txn_args);
                return 1;
            }
            *lsn = txn_args->prev_lsn;
            __os_free(txn_args);
            break;

        case DB___txn_regop_gen:
            if ((rc = __txn_regop_gen_read(dbenv, data->data,
                            &txn_gen_args)) != 0) {
                logmsg(LOGMSG_ERROR, "%s line %d regop_gen read returns %d for "
                        "%d:%d\n", __func__, __LINE__, rc, lsn->file,
                        lsn->offset);
                return 1;
            }
            if (txn_gen_args->opcode != TXN_COMMIT) {
                logmsg(LOGMSG_ERROR, "%s line %d regop_gen opcode not commit, "
                        "%d for %d:%d\n", __func__, __LINE__,
                        txn_gen_args->opcode, lsn->file, lsn->offset);
                __os_free(dbenv, txn_gen_args);
                return 1;
            }
            *lsn = txn_gen_args->prev_lsn;
            __os_free(txn_gen_args);
            break;

        case DB___txn_regop_rowlocks:
            if ((ret = __txn_regop_rowlocks_read(dbenv, data->data,
                            &txn_rl_args)) != 0) {
                logmsg(LOGMSG_ERROR, "%s line %d regop_rl opcode failed read, "
                        "%d for %d:%d\n",
                        __func__, __LINE__, rc, lsn->file, lsn->offset);
                __os_free(dbenv, txn_rl_args);
                return 1;
            }

            if (txn_rl_args->opcode != TXN_COMMIT ||
                    !(txn_rl_args->lflags & DB_TXN_LOGICAL_COMMIT)) {
                logmsg(LOGMSG_ERROR, "%s line %d regop_rl opcode not commit, %d"
                        "for %d:%d\n", __func__, __LINE__, txn_rl_args->opcode,
                        lsn->file, lsn->offset);
                __os_free(dbenv, txn_rl_args);
                return 1;
            }
            *lsn = txn_rl_args->prev_lsn;
            __os_free(dbenv, txn_rl_args);
            break;

        default:
            abort();
    }
    return 0;
}

static int create_logical_payload(DB_LSN regop_lsn, DBT *data, u_int32_t rectype)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    int rc, bdberr=0;
    DB_LOGC *logc;
    DB_LSN lsn;

    if (rc = retrieve_start_lsn(data, rectype, &lsn))
        return rc;

    if (rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0) != 0) {
        logmsg(LOGMSG_ERROR, "%s line %d cannot allocate log-cursor\n",
                __func__, __LINE__);
        return SQLITE_NOMEM;
    }

    if (pCur->log = parse_log_for_shadows_int(bdb_state, logc, &lsn, 1, 0,
                &bdberr)) {
        logmsg(LOGMSG_ERROR, "%d line %d parse_log_for_shadows failed for "
                "%d:%d\n", __func__, __LINE__, lsn.file, lsn.offset);
        return 1;
    }
    pCur->subop = -1;
}

static int advance_to_next(logicallog_cursor *pCur)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    if (!pCur->openCursor) {
        if ((rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &pCur->logc,
                        0)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d error getting a log cursor rc=%d\n",
                    __func__, __LINE__, rc);
            return SQLITE_INTERNAL;
        }
        pCur->openCursor = 1;
        pCur->data.flags = DB_DBT_REALLOC;

        if (pCur->minLsn.file == 0) {
            getflags = DB_FIRST;
        } else {
            pCur->curLsn = pCur->minLsn;
            getflags = DB_SET;
        }
    } else {
        getflags = DB_NEXT;
    }

again:
    do {
        if (rc = pCur->logc->get(pCur->logc, &pCur->curLsn, &pCur->data, getflags) != 0) {
            if (getflags != DB_NEXT) {
                return SQLITE_INTERNAL;
            }
            if (pCur->flags & LOGICALLOG_FLAGS_BLOCK) {
                do {
                    struct timespec ts;
                    clock_gettime(CLOCK_REALTIME, &ts);
                    ts.tv_nsec += (200 * 1000000);
                    pthread_mutex_lock(&gbl_logput_lk);
                    pthread_cond_timedwait(&gbl_logput_cond, &gbl_logput_lk, &ts);
                    pthread_mutex_unlock(&gbl_logput_lk);
                } while (rc = pCur->logc->get(pCur->logc, &pCur->curLsn, &pCur->data, DB_NEXT));
                rc = pCur->logc->get(pCur->logc, &pCur->curLsn,
                        &pCur->data, DB_NEXT) != 0;
            } else {
                pCur->hitLast = 1;
            }
            getflags = DB_NEXT;
            if (pCur->data.data)
                LOGCOPY_32(&rectype, pCur->data.data); 
            else 
                rectype = 0;
        }
    } while(!pCur->hitLast && is_commit(rectype));

    if (pCur->flags & LOGICALLOG_FLAGS_DURABLE) {
        do {
            struct timespec ts;
            bdb_state->dbenv->get_durable_lsn(bdb_state->dbenv,
                    &durable_lsn, &durable_gen);

            /* We've already returned this lsn: break when the next is durable */
            if (log_compare(&durable_lsn, &pCur->curLsn) >= 0)
                break;

            /* Return immediately if we are non-blocking */
            else if ((pCur->flags & LOGICALLOG_FLAGS_BLOCK) == 0) {
                pCur->notDurable = 1;
                break;
            }

            /* Wait on a condition variable */
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += (200 * 1000000);
            pthread_mutex_lock(&gbl_durable_lsn_lk);
            pthread_cond_timedwait(&gbl_durable_lsn_cond, &gbl_durable_lsn_lk, &ts);
            pthread_mutex_unlock(&gbl_durable_lsn_lk);

        } while(1);
    }

    if (!pCur->notDurable && !pCur->hitLast) {
        /* Can happen if we're missing the beginning of the transaction */
        switch (rc = create_logical_payload(pCur->curLsn, pCur->data, rectype)){
            /* Reconstructed logical log */
            case 0:
                assert(pCur->log != NULL);
                break;
             /* Go to next */
            case 1:
                getflags = DB_NEXT;
                goto again:
                break;
            /* Other error */
            default:
                pCur->hitLast = 1;
                break;
        }
    }

    return (pCur->notDurable || pCur->hitLast) ? -1 : 0;
}

static void *retrieve_packed_memory(logicallog_cursor *pCur)
{
    if (pCur->packed == NULL) {
        pCur->packed = sqlite3_malloc(MAXRECSZ);
    }
    return pCur->packed;
}

static void *retrieve_unpacked_memory(logicallog_cursor *pCur)
{
    if (pCur->unpacked == NULL) {
        pCur->unpacked = sqlite3_malloc(MAXRECSZ);
    }
    return pCur->unpacked;
}

static int produce_delete_data_record(logicallog_cursor *pCur, DB_LOGC *logc, 
        bdb_osql_log_rec_t *rec, DBT *logdta)
{
    int rc, dtalen, page, index;
    struct odh odh = {0};
    unsigned long long genid;
    short dtafile, dtastripe;
    void *packedbuf = NULL;
    void *unpackedbuf = NULL;
    llog_undo_del_dta_args *del_dta = NULL;
    llog_undo_del_dta_lk_args *del_dta_lk = NULL;
    bdb_state_type *bdb_state = thedb->bdb_env;

    if (rec->type == DB_llog_undo_del_dta_lk) {
        if ((rc = llog_undo_del_dta_lk_read(bdb_state->dbenv,
                        logdta.data,&del_dta_lk)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d error unpacking del_dta_lk, %d\n",
                    __func__, __LINE__, rc);
            rc = SQLITE_INTERNAL;
            goto done;
        }
        genid = del_dta_lk->genid;
        dtafile = del_dta_lk->dtafile;
        dtastripe = del_dta_lk->dtastripe;
        dtalen = del_dta_lk->dtalen;
    } else {
        if ((rc = llog_undo_del_dta_read(bdb_state->dbenv, logdta.data,
                &del_dta)) != 0) {
            logmsg(LOGMSG_ERROR, "%s line %d error unpacking del_dta, %d\n",
                    __func__, __LINE__, rc);
            rc = SQLITE_INTERNAL;
            goto done;
        }
        genid = del_dta->genid;
        dtafile = del_dta->dtafile;
        dtastripe = del_dta->dtastripe;
        dtalen = del_dta->dtalen;
    }

    assert(dtalen <= MAXRECSZ);
    if ((packedbuf = retrieve_packed_memory(pCur)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s line %d allocating memory\n", __func__,
                __LINE__);
        rc = SQLITE_INTERNAL;
        goto done;
    }

    if ((rc = bdb_reconstruct_delete(bdb_state, &rec->lsn, &page, &index, NULL,
                0, packedbuf, dtalen, NULL)) != 0) {
        logmsg(LOGMSG_ERROR, "%s line %d error %d reconstructing delete for "
                "%d:%d\n", __func__, __LINE__, rc, rec->lsn.file,
                rec->lsn.offset);
        goto done;
    }

    if ((unpackedbuf = retrieve_unpacked_memory(pCur)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s line %d allocating memory\n", __func__,
                __LINE__);
        rc = SQLITE_INTERNAL;
        goto done;
    }

    if ((rc = bdb_unpack(bdb_state, packedbuf, dtalen, unpackedbuf, MAXRECSZ,
                    &odh, NULL)) != 0) {
        logmsg(LOGMSG_ERROR, "%s line %d error unpacking buf %d\n", __func__,
                __LINE__, rc);
        rc = SQLITE_INTERNAL;
        goto done;
    }

done:
    if (del_dta)
        free(del_dta);
    if (del_dta_lk)
        free(del_dta_lk);

    return rc;
}

static int unpack_logical_record(logicallog_cursor *pCur)
{
    bdb_osql_log_rec_t *rec = listc_rbl(&pCur->log->impl->recs);
    bdb_state_type *bdb_state = thedb->bdb_env;
    u_int32_t rectype;
    DBT logdta = {0};
    DB_LOGC *logc;

    if (rec == NULL)
        abort();

    if (listc_size(&pCur->log->impl->recs) == 0) {
        /* Tear-down */
        bdb_osql_log_destroy(&pCur->log);
        pCur->log = NULL;
    }

    if ((rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0))
            != 0) { 
        logmsg(LOGMSG_ERROR, "%s line %d error getting log-cursor %d\n",
                __func__, __LINE__, rc);
        return SQLITE_INTERNAL;
    }

    logdta.flags = DB_DBT_REALLOC;
    if ((rc = logc->get(logc, &rec->lsn, logdta, DB_SET)) != 0) {
        logmsg(LOGMSG_ERROR, "%s line %d error %d retrieving lsn %d:%d\n",
                __func__, __LINE__, rc, rec->lsn.file, rec->lsn.offset);
        logc->close(logc, 0);
        return SQLITE_INTERNAL;
    }
    LOGCOPY_32(&rectype, logdta.data);
    assert(rectype == rec->type);

    switch(rec->type) {
        case DB_llog_undo_del_dta:
        case DB_llog_undo_del_dta_lk:
            rc = produce_delete_data_record(pCur, logc, rec, &logdta);
            break;
        case DB_llog_undo_del_ix:
        case DB_llog_undo_del_ix_lk:
            rc = produce_delete_index_record(pCur, logc, rec, &logdta);
            break;
        case DB_llog_undo_add_dta:
        case DB_llog_undo_add_dta_lk:
            rc = produce_add_data_record(pCur, logc, rec, &logdta);
            break;
        case DB_llog_undo_add_ix:
        case DB_llog_undo_add_ix_lk:
            rc = produce_add_index_record(pCur, logc, rec, &logdta);
            break;
        case DB_llog_undo_upd_dta:
        case DB_llog_undo_upd_dta_lk:
            rc = produce_update_data_record(pCur, logc, rec, &logdta);
            break;
        case DB_llog_undo_upd_ix:
        case DB_llog_undo_upd_ix_lk:
            rc = produce_update_index_record(pCur, logc, rec, &logdta);
            break;
        case DB_llog_ltran_comprec:
            rc = produce_compensation_record(pCur, logc, rec, &logdta);
            break;
    }
}

/*
** Advance a logicallog cursor to the next log entry
*/
static int logicallogNext(sqlite3_vtab_cursor *cur){
  logicallog_cursor *pCur = (logicallog_cursor*)cur;
  DB_LSN durable_lsn = {0};
  int durable_gen=0, rc, getflags;
  u_int32_t rectype = 0;
  bdb_state_type *bdb_state = thedb->bdb_env;

  if (pCur->notDurable || pCur->hitLast)
      return SQLITE_OK;

  if (pCur->log == NULL && advance_to_next(pCur) != 0)
      return SQLITE_INTERNAL;

  if (pCur->log && !pCur->notDurable && !pCur->hitLast) {
      if ((rc = unpack_logical_record(pCur)) != 0) {
          return SQLITE_INTERNAL;
      }
  }

  pCur->iRowid++;
  return SQLITE_OK;
}

#define skipws(p) { while (*p != '\0' && *p == ' ') p++; }
#define isnum(p) ( *p >= '0' && *p <= '9' )

static inline void logicallog_lsn_to_str(char *st, DB_LSN *lsn)
{
    sprintf(st, "{%d:%d}", lsn->file, lsn->offset);
}

static inline int parse_lsn(const char *lsnstr, DB_LSN *lsn)
{
    const char *p = lsnstr;
    int file, offset;
    while (*p != '\0' && *p == ' ') p++;
    skipws(p);

    /* Parse opening '{' */
    if (*p != '{')
        return -1;
    p++;
    skipws(p);
    if ( !isnum(p) )
        return -1;

    /* Parse file */
    file = atoi(p);
    while( isnum(p) )
        p++;
    skipws(p);
    if ( *p != ':' )
        return -1;
    p++;
    skipws(p);
    if ( !isnum(p) )
        return -1;

    /* Parse offset */
    offset = atoi(p);
    while( isnum(p) )
        p++;

    skipws(p);

    /* Parse closing '}' */
    if (*p != '}')
        return -1;
    p++;

    skipws(p);
    if (*p != '\0')
        return -1;

    lsn->file = file;
    lsn->offset = offset;
    return 0;
}

static u_int32_t get_generation_from_regop_gen_record(char *data)
{
    u_int32_t generation;
    LOGCOPY_32( &generation, &data[ 4 + 4 + 8 + 4] );
    return generation;
}

static u_int32_t get_generation_from_rowlocks_record(char *data)
{
    u_int32_t generation;
    LOGCOPY_32( &generation, &data[4 + 4 + 8 + 4 + 8 + 8 + 8 + 8 + 8 + 4] );
    return generation;
}

/*
** Return values of columns for the row at which the series_cursor
** is currently pointing.
*/
static int logicallogColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  logicallog_cursor *pCur = (logicallog_cursor*)cur;
  int rc;
  u_int32_t rectype = 0;
  u_int32_t generation = 0;

  switch( i ){
    case LOGICALLOG_COLUMN_START:
        if (!pCur->minLsnStr) {
            pCur->minLsnStr = sqlite3_malloc(32);
            logicallog_lsn_to_str(pCur->minLsnStr, &pCur->minLsn);
        }
        sqlite3_result_text(ctx, pCur->minLsnStr, -1, NULL);
        break;

    case LOGICALLOG_COLUMN_STOP:
        if (!pCur->maxLsnStr) {
            pCur->maxLsnStr = sqlite3_malloc(32);
            logicallog_lsn_to_str(pCur->maxLsnStr, &pCur->maxLsn);
        }
        sqlite3_result_text(ctx, pCur->maxLsnStr, -1, NULL);
        break;

    case LOGICALLOG_COLUMN_FLAGS:
        sqlite3_result_int64(ctx, pCur->flags);
        break;
    case LOGICALLOG_COLUMN_LSN:
        if (!pCur->curLsnStr) {
            pCur->curLsnStr = sqlite3_malloc(32);
        }
        logicallog_lsn_to_str(pCur->curLsnStr, &pCur->curLsn);
        sqlite3_result_text(ctx, pCur->curLsnStr, -1, NULL);
        break;
    case LOGICALLOG_COLUMN_RECTYPE:
        if (pCur->data.data)
            LOGCOPY_32(&rectype, pCur->data.data); 
        sqlite3_result_int64(ctx, rectype);
        break;
    case LOGICALLOG_COLUMN_GENERATION:
        if (pCur->data.data)
            LOGCOPY_32(&rectype, pCur->data.data); 

        if (rectype == DB___txn_regop_gen){
            generation = get_generation_from_regop_gen_record(pCur->data.data);
        }

        if (rectype == DB___txn_regop_rowlocks) {
            generation = get_generation_from_rowlocks_record(pCur->data.data);
        }

        if (generation > 0) {
            sqlite3_result_int64(ctx, generation);
        }
        break;
    case LOGICALLOG_COLUMN_LOG:
        sqlite3_result_blob(ctx, &pCur->data.data, pCur->data.size, NULL);
        break;
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int logicallogRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  logicallog_cursor *pCur = (logicallog_cursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int logicallogEof(sqlite3_vtab_cursor *cur){
  logicallog_cursor *pCur = (logicallog_cursor*)cur;
  int rc;

  /* If we are not positioned, position now */
  if (pCur->openCursor == 0) {
      if ((rc=logicallogNext(cur)) != SQLITE_OK)
          return rc;
  }
  if (pCur->hitLast || pCur->notDurable)
      return 1;
  if (pCur->maxLsn.file > 0 && log_compare(&pCur->curLsn, &pCur->maxLsn) > 0)
      return 1;
  return 0;
}

static int logicallogFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  logicallog_cursor *pCur = (logicallog_cursor *)pVtabCursor;
  int i = 0;

  bzero(&pCur->minLsn, sizeof(pCur->minLsn));
  if( idxNum & 1 ){
    const char *minLsn = sqlite3_value_text(argv[i++]);
    if (minLsn && parse_lsn(minLsn, &pCur->minLsn)) {
        return SQLITE_CONV_ERROR;
    }
  }
  bzero(&pCur->maxLsn, sizeof(pCur->maxLsn));
  if( idxNum & 2 ){
    const char *maxLsn = sqlite3_value_text(argv[i++]);
    if (maxLsn && parse_lsn(maxLsn, &pCur->maxLsn)) {
        return SQLITE_CONV_ERROR;
    }
  }
  pCur->flags = 0;
  if( idxNum & 4 ){
    int64_t flags = sqlite3_value_int64(argv[i++]);
    pCur->flags = flags;
  }
  pCur->iRowid = 1;
  return SQLITE_OK;
}

static int logicallogBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;                 /* Loop over constraints */
  int idxNum = 0;        /* The query plan bitmask */
  int startIdx = -1;     /* Index of the start= constraint, or -1 if none */
  int stopIdx = -1;      /* Index of the stop= constraint, or -1 if none */
  int flagsIdx = -1;     /* Index of the block= constraint, block waiting if set */
  int nArg = 0;          /* Number of arguments that seriesFilter() expects */

  const struct sqlite3_index_constraint *pConstraint;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( pConstraint->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pConstraint->iColumn ){
      case LOGICALLOG_COLUMN_START:
        startIdx = i;
        idxNum |= 1;
        break;
      case LOGICALLOG_COLUMN_STOP:
        stopIdx = i;
        idxNum |= 2;
        break;
      case LOGICALLOG_COLUMN_FLAGS:
        flagsIdx = i;
        idxNum |= 4;
        break;
    }
  }
  if( startIdx>=0 ){
    pIdxInfo->aConstraintUsage[startIdx].argvIndex = ++nArg;
    pIdxInfo->aConstraintUsage[startIdx].omit = 1;
  }
  if( stopIdx>=0 ){
    pIdxInfo->aConstraintUsage[stopIdx].argvIndex = ++nArg;
    pIdxInfo->aConstraintUsage[stopIdx].omit = 1;
  }
  if( flagsIdx>=0 ){
    pIdxInfo->aConstraintUsage[flagsIdx].argvIndex = ++nArg;
    pIdxInfo->aConstraintUsage[flagsIdx].omit = 1;
  }
  if( (idxNum & 3)==3 ){
    /* Both start= and stop= boundaries are available.  This is the 
    ** the preferred case */
    pIdxInfo->estimatedCost = (double)1;
  }else{
    /* If either boundary is missing, we have to generate a huge span
    ** of numbers.  Make this case very expensive so that the query
    ** planner will work hard to avoid it. */
    pIdxInfo->estimatedCost = (double)2000000000;
  }
  pIdxInfo->idxNum = idxNum;
  return SQLITE_OK;
}

/*
** This following structure defines all the methods for the 
** generate_series virtual table.
*/
sqlite3_module systblLogicalLogModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  logicallogConnect,            /* xConnect */
  logicallogBestIndex,          /* xBestIndex */
  logicallogDisconnect,         /* xDisconnect */
  0,                         /* xDestroy */
  logicallogOpen,               /* xOpen - open a cursor */
  logicallogClose,              /* xClose - close a cursor */
  logicallogFilter,             /* xFilter - configure scan constraints */
  logicallogNext,               /* xNext - advance a cursor */
  logicallogEof,                /* xEof - check for end of scan */
  logicallogColumn,             /* xColumn - read data */
  logicallogRowid,              /* xRowid - read data */
  0,                         /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindMethod */
  0,                         /* xRename */
};


