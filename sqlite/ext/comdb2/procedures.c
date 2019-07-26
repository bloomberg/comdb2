/*
**
** Vtables interface for Schema Tables.
**
** Though this is technically an extension, currently it must be
** built as part of SQLITE_CORE, as comdb2 does not support
** run time extensions at this time.
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
#include "bdb_api.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"

/*
** Functions to load server & client versioned SPs
*/

typedef struct {
  int sVer;
  char *cVer;
} spversion;

/* systbl_tables_cursor is a subclass of sqlite3_vtab_cursor which serves
** as the underlying cursor to enumerate the rows in this vtable.
*/
typedef struct {
  sqlite3_vtab_cursor base; /* Base class - must be first */
  int iProc;                /* Current SP */
  int iVer;                 /* Current version */
  char **ppProc;            /* Array of SP Names */
  int nProcs;               /* Number of SPs above */
  spversion defaultVer;     /* Default version for current SP */
  spversion *pVer;          /* Array of versions for Current SP */
  int nVers;                /* Number of versions above */
} systbl_sps_cursor;

inline static void free_sp_versions(systbl_sps_cursor *c){
  if(!c->pVer) return;
  for (int i = 0; i < c->nVers; ++i)
    if (c->pVer[i].cVer) free(c->pVer[i].cVer); // bdb owns this memory
  /* c->defaultVer.cVer is allocated in berkdb. Free it. */
  free(c->defaultVer.cVer);
  sqlite3_free(c->pVer);
  c->pVer = NULL;
}

static void get_sp_versions(systbl_sps_cursor *c) {
  free_sp_versions(c);
  char *sp = c->ppProc[c->iProc];
  char **cvers;
  int scnt, ccnt, bdberr;
  bdb_get_lua_highest(NULL, sp, &scnt, INT_MAX, &bdberr);
  bdb_get_all_for_versioned_sp(sp, &cvers, &ccnt);
  c->pVer = sqlite3_malloc(sizeof(spversion) * (scnt + ccnt));
  int i;
  for(i = 0; i < scnt; ++i) {
    c->pVer[i].sVer = i + 1;
    c->pVer[i].cVer = NULL;
  }
  for(int j = 0; j < ccnt; ++i, ++j) {
    c->pVer[i].sVer = 0;
    c->pVer[i].cVer = cvers[j];
  }
  c->nVers = i;

  /* cvers is an array of version string pointers, allocated in llmeta.
     We do not need it. */
  free(cvers);

  c->defaultVer.sVer = 0;
  c->defaultVer.cVer = NULL;
  int rc;
  if((rc = bdb_get_sp_get_default_version(sp, &bdberr)) > 0) {
    c->defaultVer.sVer = rc;
  } else {
    bdb_get_default_versioned_sp(sp, &c->defaultVer.cVer);
  }
}

static void get_server_versioned_sps(char ***a, int *x) {
  char old_sp[MAX_SPNAME] = {0};
  char new_sp[MAX_SPNAME] = {0};
  old_sp[0] = 127;
  char **names = NULL;
  int n = 0;
  int inc = 5;
  int alloc = 0;
  while(1) {
    int bdberr;
    int rc = bdb_get_sp_name(NULL, old_sp, new_sp, &bdberr);
    if(rc || (strcmp(old_sp, new_sp) <= 0)) {
      break;
    }
    if (n == alloc) {
      alloc += inc;
      names = sqlite3_realloc(names, sizeof(char *) * alloc);
    }
    names[n] = strdup(new_sp);
    ++n;
    strcpy(old_sp, new_sp);
  }
  *x = n;
  *a = names;
}

static int strptrcmp(const void *p1, const void *p2) {
  return strcmp(*(char *const *)p1, *(char *const *)p2);
}

static int systblSPsConnect(
  sqlite3 *db,
  void *pAux,
  int argc,
  const char *const *argv,
  sqlite3_vtab **ppVtab,
  char **pErr
){
  sqlite3_vtab *pNew;
  int rc;

/* Column numbers */
#define STSP_NAME     0   /* sp name */
#define STSP_VER      1   /* version name or number */
#define STSP_CVER     2   /* is client versioned */
#define STSP_DEF      3   /* is default */
#define STSP_SRC      4   /* source */

  rc = sqlite3_declare_vtab(db, "CREATE TABLE "
                                "comdb2_procedures(\"name\",\"version\", "
                                "\"client_versioned\", \"default\",\"src\")");
  /* Lunch start here allocate rows */
  if( rc==SQLITE_OK ){
    pNew = *ppVtab = sqlite3_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }
  return rc;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int systblSPsBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}

/*
** Destructor for sqlite3_vtab objects.
*/
static int systblSPsDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Load all SPs
** Load all versions for first SP
*/
static int systblSPsOpen(
  sqlite3_vtab *p,
  sqlite3_vtab_cursor **ppCursor
){
  systbl_sps_cursor *pCur = sqlite3_malloc(sizeof(*pCur));
  if(pCur == 0) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;

  char **sname, **cname; // server name, client name
  int scnt, ccnt;        // server count, client count

  get_server_versioned_sps(&sname, &scnt);
  bdb_get_versioned_sps(&cname, &ccnt);

  // SP can have both clnt and server versioned names.
  // Merge the two lists to de-dup
  qsort(cname, ccnt, sizeof(char *), strptrcmp);
  qsort(sname, scnt, sizeof(char *), strptrcmp);
  sname = sqlite3_realloc(sname, sizeof(char *) * (ccnt + scnt));
  pCur->ppProc = sname;
  int i, j;
  // API is such that, cnames can have dups (one per version)
  char *last = ""; // de-dup cnames
  for(i = scnt, j = 0; j < ccnt; ++j) {
    if(strcmp(last, cname[j]) == 0)
      continue;
    last = cname[j];
    if(bsearch(&cname[j], sname, scnt, sizeof(char *), strptrcmp) != NULL)
      continue;
    pCur->ppProc[i++] = strdup(cname[j]);
  }
  pCur->nProcs = i;
  // memory owned by bdb:
  for(i = 0; i < ccnt; ++i)
    free(cname[i]);
  free(cname);
  return SQLITE_OK;
}

static int systblSPsClose(sqlite3_vtab_cursor *cur) {
  systbl_sps_cursor *pCur = (systbl_sps_cursor *)cur;
  for(int i = 0; i < pCur->nProcs; ++i) {
      free(pCur->ppProc[i]);
  }
  sqlite3_free(pCur->ppProc);
  free_sp_versions(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to xColumn() or xRowid() or xEof().
*/
static int systblSPsFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  systbl_sps_cursor *pCur = (systbl_sps_cursor *)pVtabCursor;

  pCur->iProc = 0;
  pCur->iVer = 0;
  free_sp_versions(pCur);
  return SQLITE_OK;
}

static int systblSPsNext(sqlite3_vtab_cursor *cur){
  systbl_sps_cursor *pCur = (systbl_sps_cursor*)cur;
  ++pCur->iVer;
  if (pCur->iVer >= pCur->nVers) {
    ++pCur->iProc;
    pCur->iVer = 0;
    free_sp_versions(pCur);
  }
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int systblSPsEof(sqlite3_vtab_cursor *cur){
  systbl_sps_cursor *pCur = (systbl_sps_cursor*)cur;
  return pCur->iProc == pCur->nProcs;
}

static int systblSPsColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int col
){
  systbl_sps_cursor *c = (systbl_sps_cursor *)cur;
  char *src = NULL;
  char *sp = NULL;
  int def = 0, size;
  spversion *v;

  /* Check to see if c->pVer is already allocated. If not, allocate. */
  if (c->iVer == 0 && c->pVer == NULL) {
    get_sp_versions(c);
  }
  v = &c->pVer[c->iVer];

  switch( col ) {
    case STSP_NAME:
      sp = c->ppProc[c->iProc];
      sqlite3_result_text( ctx, strdup(sp), -1, free );
      break;
    case STSP_VER:
      if (v == NULL)
        sqlite3_result_null( ctx );
      else if( v->cVer ) {
        sqlite3_result_text( ctx, strdup(v->cVer), -1, free );
        assert(v->sVer == 0);
      }
      else {
        char ver[32];
        sprintf( ver, "%d", v->sVer );
        sqlite3_result_text( ctx, strdup(ver), -1, free );
      }
      break;
    case STSP_CVER:
      if (v == NULL)
        sqlite3_result_null( ctx );
      else
        sqlite3_result_text( ctx, YESNO( v->cVer ), -1, NULL );
      break;
    case STSP_DEF:
      if (v == NULL)
        sqlite3_result_null( ctx );
      else if( c->defaultVer.cVer ) { // default is client-versioned
        if( v->cVer )
          def = strcmp( v->cVer, c->defaultVer.cVer ) == 0;
      } else if( c->defaultVer.sVer ) {
        def = v->sVer == c->defaultVer.sVer;
      }
      sqlite3_result_text( ctx, YESNO( def ), -1, NULL );
      break;
    case STSP_SRC:
      sp = c->ppProc[c->iProc];
      if (v == NULL)
        sqlite3_result_null( ctx );
      else if( v->sVer ) {
        int bdberr;
        bdb_get_sp_lua_source( NULL, NULL, sp, &src, v->sVer, &size, &bdberr );
      } else {
        bdb_get_versioned_sp( sp, v->cVer, &src );
        size = strlen(src);
      }
      if( src == NULL )
        sqlite3_result_null( ctx );
      else
        sqlite3_result_text( ctx, src, size, free );
      break;
  }
  return SQLITE_OK;
}

static int systblSPsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_sps_cursor *pCur = (systbl_sps_cursor*)cur;
  *pRowid = pCur->iProc;
  return SQLITE_OK;
}

const sqlite3_module systblSPsModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  systblSPsConnect,          /* xConnect */
  systblSPsBestIndex,        /* xBestIndex */
  systblSPsDisconnect,       /* xDisconnect */
  0,                         /* xDestroy */
  systblSPsOpen,             /* xOpen - open a cursor */
  systblSPsClose,            /* xClose - close a cursor */
  systblSPsFilter,           /* xFilter - configure scan constraints */
  systblSPsNext,             /* xNext - advance a cursor */
  systblSPsEof,              /* xEof - check for end of scan */
  systblSPsColumn,           /* xColumn - read data */
  systblSPsRowid,            /* xRowid - read data */
  0,                         /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindMethod */
  0,                         /* xRename */
  0,                         /* xSavepoint */
  0,                         /* xRelease */
  0,                         /* xRollbackTo */
  0,                         /* xShadowName */
  .access_flag = CDB2_ALLOW_USER,
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
