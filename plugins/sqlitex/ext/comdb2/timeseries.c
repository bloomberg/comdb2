#if (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
    && !defined(SQLITE_OMIT_VIRTUALTABLE)

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
# define SQLITE_CORE 1
#endif

#include <stdlib.h>
#include <string.h>

#include "comdb2.h"
#include "averager.h"
#include "perf.h"
#include "sqliteInt.h"
#include "vdbeInt.h"

/* systbl_metrics_cursor is a subclass of sqlitex_vtab_cursor which
** serves as the underlying cursor to enumerate the rows in this
** vtable. As of now the only thing this cursor reports is the size of
** the table.
*/
struct systbl_metrics_cursor {
  sqlitex_vtab_cursor base;  /* Base class - must be first */
  int filtered;
  struct time_metric *metric;
  int npoints;
  struct point *points;
  sqlitex_int64 iRowid;      /* The rowid */

};
typedef struct systbl_metrics_cursor systbl_metrics_cursor;

static int systblTimeseriesConnect(
  sqlitex *db,
  void *pAux,
  int argc,
  const char *const*argv,
  sqlitex_vtab **ppVtab,
  char **pErr
){
  sqlitex_vtab *pNew;
  int rc;

  rc = sqlitex_declare_vtab(
    db, "CREATE TABLE comdb2_timeseries(metric, time, value)");
  if( rc==SQLITE_OK ){
    pNew = *ppVtab = sqlitex_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }
  return rc;
}

static int systblTimeseriesDisconnect(sqlitex_vtab *pVtab){
  sqlitex_free(pVtab);
  return SQLITE_OK;
}

static int systblTimeseriesOpen(sqlitex_vtab *p, sqlitex_vtab_cursor **ppCursor){
  systbl_metrics_cursor *pCur;

  pCur = sqlitex_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

static int systblTimeseriesClose(sqlitex_vtab_cursor *cur){
  systbl_metrics_cursor *pCur = (systbl_metrics_cursor*)cur;

  free(pCur->points);

  sqlitex_free(cur);
  return SQLITE_OK;
}

static int init_metric(systbl_metrics_cursor *pCur) {
    free(pCur->points);
    pCur->points = NULL;
    if (time_metric_get_points(pCur->metric, &pCur->points, &pCur->npoints)) 
        return SQLITE_NOMEM;

    return SQLITE_OK;
}

static int systblTimeseriesNext(sqlitex_vtab_cursor *cur){
  systbl_metrics_cursor *pCur = (systbl_metrics_cursor*)cur;
  int rc = SQLITE_OK;

  pCur->iRowid++;
  if (pCur->iRowid >= pCur->npoints) {
      if (pCur->filtered)
          pCur->metric = NULL;
      else {
          pCur->metric = time_metric_next(pCur->metric);
          if (pCur->metric)
              rc = init_metric(pCur);
          pCur->iRowid = 0;
      }
  }

  return SQLITE_OK;
}

enum {
    COLUMN_METRIC,
    COLUMN_TIME,
    COLUMN_VALUE
};

static int systblTimeseriesColumn(
  sqlitex_vtab_cursor *cur,
  sqlitex_context *ctx,
  int i
){
  systbl_metrics_cursor *pCur = (systbl_metrics_cursor*)cur;

  switch (i) {
      case COLUMN_METRIC:
          sqlitex_result_text(ctx, time_metric_name(pCur->metric), -1, NULL);
          break;
      case COLUMN_TIME: {
          /* TODO: sqlitex_result_datetime - but what to do with timezone? */
          sqlitex_result_int64(ctx, pCur->points[pCur->iRowid].time_added);
          break;
      }
      case COLUMN_VALUE:
          sqlitex_result_int64(ctx, pCur->points[pCur->iRowid].value);
          break;
  }

  return SQLITE_OK;
};

static int systblTimeseriesRowid(sqlitex_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_metrics_cursor *pCur = (systbl_metrics_cursor*)cur;

  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

static int systblTimeseriesEof(sqlitex_vtab_cursor *cur){
    systbl_metrics_cursor *pCur = (systbl_metrics_cursor*)cur;

    if (pCur->metric == NULL)
        return 1;

    if (pCur->filtered) {
        if (pCur->iRowid >= pCur->npoints)
            return 1;
    }

    return 0;
}

static int systblTimeseriesFilter(
  sqlitex_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlitex_value **argv
){
  systbl_metrics_cursor *pCur = (systbl_metrics_cursor*)pVtabCursor;

  if (argc && argv[0]->flags & MEM_Str) {
      pCur->filtered = 1;
      pCur->metric = time_metric_get(argv[0]->z);
  }
  else
      pCur->metric = time_metric_first();

  int rc = SQLITE_OK;
  if (pCur->metric)
      rc = init_metric(pCur);

  pCur->iRowid = 0;
  return rc;
}

static int systblTimeseriesBestIndex(
  sqlitex_vtab *tab,
  sqlitex_index_info *pIdxInfo
){
  if (pIdxInfo->nConstraint) {
      int i;
      for (i = 0; i < pIdxInfo->nConstraint && pIdxInfo->aConstraint[i].usable; i++) {
         if (pIdxInfo->aConstraint[0].iColumn == 0 && pIdxInfo->aConstraint[0].op == SQLITE_INDEX_CONSTRAINT_EQ) {
             pIdxInfo->idxNum = 1;
             pIdxInfo->aConstraintUsage[0].argvIndex = 1;
         }
      }
  }
  return SQLITE_OK;
}

const sqlitex_module systblTimeseriesModule = {
  0,                            /* iVersion */
  0,                            /* xCreate */
  systblTimeseriesConnect,       /* xConnect */
  systblTimeseriesBestIndex,     /* xBestIndex */
  systblTimeseriesDisconnect,    /* xDisconnect */
  0,                            /* xDestroy */
  systblTimeseriesOpen,          /* xOpen - open a cursor */
  systblTimeseriesClose,         /* xClose - close a cursor */
  systblTimeseriesFilter,        /* xFilter - configure scan constraints */
  systblTimeseriesNext,          /* xNext - advance a cursor */
  systblTimeseriesEof,           /* xEof - check for end of scan */
  systblTimeseriesColumn,        /* xColumn - read data */
  systblTimeseriesRowid,         /* xRowid - read data */
  0,                            /* xUpdate */
  0,                            /* xBegin */
  0,                            /* xSync */
  0,                            /* xCommit */
  0,                            /* xRollback */
  0,                            /* xFindMethod */
  0,                            /* xRename */
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
