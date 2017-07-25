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

/* systbl_constraint_cursor is a subclass of sqlite3_vtab_cursor which
** serves as the underlying cursor to enumerate constraint. We keep track
** of the table we're on and the Constraint/Rule in the given table.
*/
typedef struct systbl_constraints_cursor systbl_constraints_cursor;
struct systbl_constraints_cursor {
  sqlite3_vtab_cursor base;    /* Base class - must be first */
  sqlite3_int64 iRowid;        /* The rowid */
  sqlite3_int64 iConstraintid; /* The rule we're on */
  sqlite3_int64 iRuleid;       /* The rule we're on */
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
#define STCON_TABLE     0
#define STCON_KEY       1
#define STCON_FTNAME    2
#define STCON_FKNAME    3
#define STCON_CDELETE   4
#define STCON_CUPDATE   5

  rc = sqlite3_declare_vtab(db,
    "CREATE TABLE comdb2_constraints(tablename,keyname,"
    "foreigntablename,foreignkeyname,iscascadingdelete,iscascadingupdate)");
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
static int systblConstraintsNext(sqlite3_vtab_cursor *cur){
  systbl_constraints_cursor *pCur = (systbl_constraints_cursor*)cur;

  pCur->iRuleid++;

  /* Test just in case cursor is in a bad state */
  if( pCur->iRowid < thedb->num_dbs ){
    if( pCur->iConstraintid >= thedb->dbs[pCur->iRowid]->n_constraints
     || pCur->iRuleid >=
               thedb->dbs[pCur->iRowid]->constraints[pCur->iConstraintid].nrules
    ){
      pCur->iRuleid = 0;
      pCur->iConstraintid++;

      while( pCur->iRowid < thedb->num_dbs
       && pCur->iConstraintid >= thedb->dbs[pCur->iRowid]->n_constraints ){
        pCur->iConstraintid = 0;
        pCur->iRowid++;
      }
    }
  }

  return SQLITE_OK;
}

/*
** Return the table name for the current row.
*/
static int systblConstraintsColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  systbl_constraints_cursor *pCur = (systbl_constraints_cursor*)cur;
  struct dbtable *pDb = thedb->dbs[pCur->iRowid];
  constraint_t *pConstraint = &pDb->constraints[pCur->iConstraintid];

  switch( i ){
    case STCON_TABLE: {
      sqlite3_result_text(ctx, pDb->dbname, -1, NULL);
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
      sqlite3_result_text(ctx, YESNO(pConstraint->flags & CT_DEL_CASCADE), 
        -1, SQLITE_STATIC);
      break;
    }
    case STCON_CUPDATE: {
      sqlite3_result_text(ctx, YESNO(pConstraint->flags & CT_UPD_CASCADE), 
        -1, SQLITE_STATIC);
      break;
    }
  }
  return SQLITE_OK; 
};

/*
** Return the rowid for the current rule. We arrive at this number by
** iterating through all rules on all the constraint on all the tables
** previous to this, and then adding the current Ruleid for the
** Constraint that we're presently on.
*/
static int systblConstraintsRowid(
  sqlite3_vtab_cursor *cur,
  sqlite_int64 *pRowid
){
  systbl_constraints_cursor *pCur = (systbl_constraints_cursor*)cur;
  int i;

  *pRowid = 0;
  for( i = 0; i < pCur->iRowid - 1; i++ ){
    for( int j = 0; j < thedb->dbs[i]->n_constraints - 1; j++ ){
      *pRowid += thedb->dbs[i]->constraints[j].nrules;
    }
  }
  for( int j = 0; j < thedb->dbs[i]->n_constraints - 1; j++ ){
    *pRowid += thedb->dbs[i]->constraints[j].nrules;
  }
  *pRowid += pCur->iConstraintid;
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
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
