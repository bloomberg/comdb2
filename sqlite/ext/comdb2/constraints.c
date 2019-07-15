/*
**
** Vtables interface for Schema Tables.
**
** Though this is technically an extension, currently it must be

** run time extensions at this time.
**
** For a little while we had to use our own "fake" tables, because
** eponymous system tables did not exist. Now that they do, we
** have moved schema tables to their own extension.
**
** We have piggy backed off of SQLITE_BUILDING_FOR_COMDB2 here, though
** a new #define would also suffice.
*/
#if (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
    && !defined(SQLITE_OMIT_VIRTUALTABLE)

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
# define SQLITE_CORE 1
#endif

#include <stdlib.h>
#include <string.h>

#include "comdb2.h"
#include "sql.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"

int gen_fk_constraint_name(constraint_t *pConstraint, int parent_idx, char *buf,
                           size_t size);
int gen_check_constraint_name(check_constraint_t *pConstraint, char *out,
                              size_t out_size);

enum {
    CONS_FKEY,
    CONS_CHECK,
};

/* systbl_constraint_cursor is a subclass of sqlite3_vtab_cursor which
** serves as the underlying cursor to enumerate constraint. We keep track
** of the table we're on and the Constraint/Rule in the given table.
*/
typedef struct systbl_constraints_cursor systbl_constraints_cursor;
struct systbl_constraints_cursor {
  sqlite3_vtab_cursor base;    /* Base class - must be first */
  sqlite3_int64 iRowid;        /* The rowid (table) */
  sqlite3_int64 iConstraintid; /* Constraint */
  sqlite3_int64 iRuleid;       /* Rule */
  sqlite3_int64 iIndex;        /* How many constraints have we already visitied? */
  uint8_t       iConstraintType; /* Which constraint list are we currently on? */
};

static int systblConstraintsConnect(
  sqlite3 *db,
  void *pAux,
  int argc,
  const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pErr
){
  sqlite3_vtab *pNew;
  int rc;

/* Column numbers */
#define STCON_NAME 0
#define STCON_TYPE 1
#define STCON_TABLE 2
#define STCON_KEY 3
#define STCON_FTNAME 4
#define STCON_FKNAME 5
#define STCON_CDELETE 6
#define STCON_CUPDATE 7
#define STCON_EXPR 8

  rc = sqlite3_declare_vtab(db,
    "CREATE TABLE comdb2_constraints(name,type,tablename,keyname,"
    "foreigntablename,foreignkeyname,iscascadingdelete,iscascadingupdate,expr)");
  if( rc==SQLITE_OK ){
    pNew = *ppVtab = sqlite3_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }
  return rc;
}

/*
** Destructor for sqlite3_vtab objects.
*/
static int systblConstraintsDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for systbl_constraints_cursor objects.
*/
static int systblConstraintsOpen(
  sqlite3_vtab *p,
  sqlite3_vtab_cursor **ppCursor
){
  systbl_constraints_cursor *pCur;

  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;

  comdb2_next_allowed_table(&pCur->iRowid);

  return SQLITE_OK;
}

/*
** Destructor for systbl_constraints_cursor.
*/
static int systblConstraintsClose(sqlite3_vtab_cursor *cur){
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Advance to the next key.
*/
static int systblConstraintsNext(sqlite3_vtab_cursor *cur)
{
    systbl_constraints_cursor *pCur = (systbl_constraints_cursor *)cur;

    /* Let's move to the next constraint/rule. */
    if (pCur->iConstraintType == CONS_FKEY) {
        pCur->iRuleid++;
    } else {
        pCur->iConstraintid++;
    }
    pCur->iIndex++;

again:

    switch (pCur->iConstraintType) {
    case CONS_FKEY:
        if ((pCur->iConstraintid < thedb->dbs[pCur->iRowid]->n_constraints) &&
            (pCur->iRuleid < thedb->dbs[pCur->iRowid]
                                 ->constraints[pCur->iConstraintid]
                                 .nrules)) {
            return SQLITE_OK;
        }

        pCur->iRuleid = 0;
        pCur->iConstraintid++;

        if (pCur->iConstraintid < thedb->dbs[pCur->iRowid]->n_constraints) {
            return SQLITE_OK;
        }

        pCur->iConstraintType = CONS_CHECK;
        pCur->iConstraintid = 0;

        /* fallthrough */

    case CONS_CHECK:
        if (pCur->iConstraintid <
            thedb->dbs[pCur->iRowid]->n_check_constraints) {
            return SQLITE_OK;
        }
        break;
    }

    /* Move on to the next (authorized) table. */
    pCur->iRowid++;
    if (pCur->iRowid < thedb->num_dbs) {
        comdb2_next_allowed_table(&pCur->iRowid);
        /* Make sure we haven't moved past the last table. */
        if (pCur->iRowid < thedb->num_dbs) {
            /* Start over with FOREIGN KEYs */
            pCur->iConstraintType = CONS_FKEY;
            pCur->iRuleid = 0;
            pCur->iConstraintid = 0;
            goto again;
        }
    }

    return SQLITE_OK;
}

static int systblFKeyConstraintsColumn(sqlite3_vtab_cursor *cur,
                                       sqlite3_context *ctx, int i)
{
    systbl_constraints_cursor *pCur = (systbl_constraints_cursor *)cur;
    struct dbtable *pDb = thedb->dbs[pCur->iRowid];
    constraint_t *pConstraint = &pDb->constraints[pCur->iConstraintid];

  switch( i ){
    case STCON_NAME: {
        int rc;
        char *constraint_name;
        if (pConstraint->consname) {
            constraint_name = pConstraint->consname;
            sqlite3_result_text(ctx, constraint_name, -1, NULL);
        } else {
            constraint_name = sqlite3_malloc(MAXGENCONSLEN);
            if (constraint_name == 0)
                return SQLITE_NOMEM;

            rc = gen_fk_constraint_name(pConstraint, pCur->iRuleid,
                                        constraint_name, MAXGENCONSLEN);
            if (rc)
                return SQLITE_INTERNAL;
            sqlite3_result_text(ctx, constraint_name, -1, sqlite3_free);
        }
        break;
    }
    case STCON_TYPE: {
        sqlite3_result_text(ctx, "FOREIGN KEY", -1, NULL);
        break;
    }
    case STCON_TABLE: {
        sqlite3_result_text(ctx, pDb->tablename, -1, NULL);
        break;
    }
    case STCON_KEY: {
        sqlite3_result_text(ctx, pConstraint->lclkeyname, -1, NULL);
        break;
    }
    case STCON_FTNAME: {
        sqlite3_result_text(ctx, pConstraint->table[pCur->iRuleid], -1, NULL);
        break;
    }
    case STCON_FKNAME: {
        sqlite3_result_text(ctx, pConstraint->keynm[pCur->iRuleid], -1, NULL);
        break;
    }
    case STCON_CDELETE: {
        sqlite3_result_text(ctx, YESNO(pConstraint->flags & CT_DEL_CASCADE), -1,
                            SQLITE_STATIC);
        break;
    }
    case STCON_CUPDATE: {
        sqlite3_result_text(ctx, YESNO(pConstraint->flags & CT_UPD_CASCADE), -1,
                            SQLITE_STATIC);
        break;
    }
    case STCON_EXPR: {
        sqlite3_result_text(ctx, 0, -1, NULL);
        break;
    }
    }
    return SQLITE_OK;
}

static int systblCheckConstraintsColumn(sqlite3_vtab_cursor *cur,
                                        sqlite3_context *ctx, int i)
{
    systbl_constraints_cursor *pCur = (systbl_constraints_cursor *)cur;
    struct dbtable *pDb = thedb->dbs[pCur->iRowid];
    check_constraint_t *pConstraint =
        &pDb->check_constraints[pCur->iConstraintid];

  switch( i ){
    case STCON_NAME: {
        int rc;
        char *constraint_name;
        if (pConstraint->consname) {
            constraint_name = pConstraint->consname;
            sqlite3_result_text(ctx, constraint_name, -1, NULL);
        } else {
            constraint_name = sqlite3_malloc(MAXGENCONSLEN);
            if (constraint_name == 0)
                return SQLITE_NOMEM;

            rc = gen_check_constraint_name(pConstraint, constraint_name,
                                           MAXGENCONSLEN);
            if (rc)
                return SQLITE_INTERNAL;
            sqlite3_result_text(ctx, constraint_name, -1, sqlite3_free);
        }
        break;
    }
    case STCON_TYPE: {
        sqlite3_result_text(ctx, "CHECK", -1, NULL);
        break;
    }
    case STCON_TABLE: {
        sqlite3_result_text(ctx, 0, -1, NULL);
        break;
    }
    case STCON_KEY: {
        sqlite3_result_text(ctx, 0, -1, NULL);
        break;
    }
    case STCON_FTNAME: {
        sqlite3_result_text(ctx, 0, -1, NULL);
        break;
    }
    case STCON_FKNAME: {
        sqlite3_result_text(ctx, 0, -1, NULL);
        break;
    }
    case STCON_CDELETE: {
        sqlite3_result_text(ctx, 0, -1, NULL);
        break;
    }
    case STCON_CUPDATE: {
        sqlite3_result_text(ctx, 0, -1, NULL);
        break;
    }
    case STCON_EXPR: {
        sqlite3_result_text(ctx, pConstraint->expr, -1, NULL);
        break;
    }
    }
    return SQLITE_OK;
}

/*
** Return the table name for the current row.
*/
static int systblConstraintsColumn(sqlite3_vtab_cursor *cur,
                                   sqlite3_context *ctx, int i)
{
    systbl_constraints_cursor *pCur = (systbl_constraints_cursor *)cur;

    switch (pCur->iConstraintType) {
    case CONS_FKEY:
        return systblFKeyConstraintsColumn(cur, ctx, i);
    case CONS_CHECK:
        return systblCheckConstraintsColumn(cur, ctx, i);
    default:
        abort();
    }
    return SQLITE_OK;
}

/*
** Return the rowid for the current rule. We arrive at this number by
** iterating through all rules on all the constraint on all the tables
** previous to this, and then adding the current Ruleid for the
** Constraint that we're presently on.
*/
static int systblConstraintsRowid(sqlite3_vtab_cursor *cur,
                                  sqlite_int64 *pRowid)
{
    systbl_constraints_cursor *pCur = (systbl_constraints_cursor *)cur;
    *pRowid = pCur->iIndex;
    return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int systblConstraintsEof(sqlite3_vtab_cursor *cur){
  systbl_constraints_cursor *pCur = (systbl_constraints_cursor*)cur;

  return pCur->iRowid >= thedb->num_dbs;
}

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to seriesColumn() or seriesRowid() or
** seriesEof().
*/
static int systblConstraintsFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  systbl_constraints_cursor *pCur = (systbl_constraints_cursor*)pVtabCursor;

  pCur->iRowid = 0;
  pCur->iConstraintid = 0;
  pCur->iRuleid = 0;
  pCur->iIndex = 0;

  /* Advance to the first constraint, as it's possible that the cursor will
  ** start on a table without a constraint.
  */
  if( thedb->dbs[pCur->iRowid]->n_constraints == 0 ){
    systblConstraintsNext(pVtabCursor);
  }

  return SQLITE_OK;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int systblConstraintsBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlite3_module systblConstraintsModule = {
  0,                             /* iVersion */
  0,                             /* xCreate */
  systblConstraintsConnect,      /* xConnect */
  systblConstraintsBestIndex,    /* xBestIndex */
  systblConstraintsDisconnect,   /* xDisconnect */
  0,                             /* xDestroy */
  systblConstraintsOpen,         /* xOpen - open a cursor */
  systblConstraintsClose,        /* xClose - close a cursor */
  systblConstraintsFilter,       /* xFilter - configure scan constraints */
  systblConstraintsNext,         /* xNext - advance a cursor */
  systblConstraintsEof,          /* xEof - check for end of scan */
  systblConstraintsColumn,       /* xColumn - read data */
  systblConstraintsRowid,        /* xRowid - read data */
  0,                             /* xUpdate */
  0,                             /* xBegin */
  0,                             /* xSync */
  0,                             /* xCommit */
  0,                             /* xRollback */
  0,                             /* xFindMethod */
  0,                             /* xRename */
  0,                             /* xSavepoint */
  0,                             /* xRelease */
  0,                             /* xRollbackTo */
  0,                             /* xShadowName */
  .access_flag = CDB2_ALLOW_ALL,
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
