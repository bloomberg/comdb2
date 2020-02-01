/*
** 2004 May 26
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains code use to implement APIs that are part of the
** VDBE.
*/
#include "sqliteInt.h"
#include "vdbeInt.h"

#ifndef SQLITE_OMIT_DEPRECATED
/*
** Return TRUE (non-zero) of the statement supplied as an argument needs
** to be recompiled.  A statement needs to be recompiled whenever the
** execution environment changes in a way that would alter the program
** that sqlitex_prepare() generates.  For example, if new functions or
** collating sequences are registered or if an authorizer function is
** added or changed.
*/
int sqlitex_expired(sqlitex_stmt *pStmt){
  Vdbe *p = (Vdbe*)pStmt;
  return p==0 || p->expired;
}
#endif

/*
** Check on a Vdbe to make sure it has not been finalized.  Log
** an error and return true if it has been finalized (or is otherwise
** invalid).  Return false if it is ok.
*/
static int vdbeSafety(Vdbe *p){
  if( p->db==0 ){
    sqlitex_log(SQLITE_MISUSE, "API called with finalized prepared statement");
    return 1;
  }else{
    return 0;
  }
}
static int vdbeSafetyNotNull(Vdbe *p){
  if( p==0 ){
    sqlitex_log(SQLITE_MISUSE, "API called with NULL prepared statement");
    return 1;
  }else{
    return vdbeSafety(p);
  }
}

/*
** The following routine destroys a virtual machine that is created by
** the sqlitex_compile() routine. The integer returned is an SQLITE_
** success/failure code that describes the result of executing the virtual
** machine.
**
** This routine sets the error code and string returned by
** sqlitex_errcode(), sqlitex_errmsg() and sqlitex_errmsg16().
*/
int sqlitex_finalize(sqlitex_stmt *pStmt){
  int rc;
  if( pStmt==0 ){
    /* IMPLEMENTATION-OF: R-57228-12904 Invoking sqlitex_finalize() on a NULL
    ** pointer is a harmless no-op. */
    rc = SQLITE_OK;
  }else{
    Vdbe *v = (Vdbe*)pStmt;
    sqlitex *db = v->db;
    if( vdbeSafety(v) ) return SQLITE_MISUSE_BKPT;
    sqlitex_mutex_enter(db->mutex);
    rc = sqlitexVdbeFinalize(v);
    rc = sqlitexApiExit(db, rc);
    sqlitexLeaveMutexAndCloseZombie(db);
  }
  return rc;
}


/*
** COMDB2 modification to reset the tspec of sqlite machine.
*/
int sqlitex_resetclock(sqlitex_stmt *pStmt) {
  int rc;
  if( pStmt==0 ){
    rc = SQLITE_OK;
  }else{
    Vdbe *v = (Vdbe*)pStmt;
    sqlitex_mutex_enter(v->db->mutex);
    rc = sqlitexVdbeResetClock(v);
    sqlitex_mutex_leave(v->db->mutex);
  }
  return rc;
}

char *stmt_tznameXX(sqlitex_stmt *pStmt) {
  return ((Vdbe *)pStmt)->tzname;
}

void stmt_set_dtprecXX(sqlitex_stmt *pStmt, int precision) {
  /* It's the caller's (currently only Lua)
     responsiblity to validate the precision */
  ((Vdbe *)pStmt)->dtprec = precision;
}

/*
** Terminate the current execution of an SQL statement and reset it
** back to its starting state so that it can be reused. A success code from
** the prior execution is returned.
**
** This routine sets the error code and string returned by
** sqlitex_errcode(), sqlitex_errmsg() and sqlitex_errmsg16().
*/
int sqlitex_reset(sqlitex_stmt *pStmt){
  int rc;
  if( pStmt==0 ){
    rc = SQLITE_OK;
  }else{
    Vdbe *v = (Vdbe*)pStmt;
    sqlitex_mutex_enter(v->db->mutex);
    rc = sqlitexVdbeReset(v);
    sqlitexVdbeRewind(v);
    assert( (rc & (v->db->errMask))==rc );
    rc = sqlitexApiExit(v->db, rc);
    sqlitex_mutex_leave(v->db->mutex);
  }
  return rc;
}

/*
** COMDB2 MODIFICATION
** Check if there are remote dbs in the statement
** Default, sqlite in comdb2 has 2 dbs, "main" and "temp"
** Anything else is a remote db.
**
*/
int sqlitex_stmt_has_remotes(sqlitex_stmt *pStmt)
{
   Vdbe *v = (Vdbe*)pStmt;
   int   rc = 0;

   if (v)
   {
      sqlitex_mutex_enter(v->db->mutex);
#if SQLITE_MAX_ATTACHED>30
   /* v->btreeMask is an array */
      int i;
      if(v->btreeMask[0] >= 4)
      {
        rc = 1;
      }
      else
      {
        rc = !sqlitexDbMaskAllZero(v->btreeMask, 1);
      }
#else
      rc = (v->btreeMask >= 4);
#endif
      sqlitex_mutex_leave(v->db->mutex);
   }
   return rc;
}


int sqlitexDbMaskAllZero(yDbMask mask, int start)
{
   int i;

   for(i=start;i<sizeof(mask)/sizeof(mask[0]); i++)
   {
      if(mask[i]!=0)
      {
         return 0;
      }
   }
   return 1;
}



/*
** Set all the parameters in the compiled SQL statement to NULL.
*/
int sqlitex_clear_bindings(sqlitex_stmt *pStmt){
  int i;
  int rc = SQLITE_OK;
  Vdbe *p = (Vdbe*)pStmt;
#if SQLITE_THREADSAFE
  sqlitex_mutex *mutex = ((Vdbe*)pStmt)->db->mutex;
#endif
  sqlitex_mutex_enter(mutex);
  for(i=0; i<p->nVar; i++){
    sqlitexVdbeMemRelease(&p->aVar[i]);
    p->aVar[i].flags = MEM_Null;
  }
  if( p->isPrepareV2 && p->expmask ){
    p->expired = 1;
  }
  sqlitex_mutex_leave(mutex);
  return rc;
}


/**************************** sqlitex_value_  *******************************
** The following routines extract information from a Mem or sqlitex_value
** structure.
*/

/* COMDB2 MODIFICATION */
const dttz_t *sqlitex_value_datetime(sqlitex_value *pVal){
  Mem *p = (Mem*)pVal;
  if( !(p->flags & MEM_Datetime )){
    if(sqlitexVdbeMemDatetimefy(pVal)) {
        fprintf(stderr, "sqlitex_value_datetime: failed conversion\n");
    }
  }
  return &p->du.dt;
}
const intv_t *sqlitex_value_interval(sqlitex_value *pVal, int type){
  Mem *p = (Mem*)pVal;
  if( !(p->flags & MEM_Interval)){
    if (type == SQLITE_DECIMAL)
    {
       if(sqlitexVdbeMemDecimalfy(pVal)) {
          fprintf(stderr, "sqlitex_value_interval: failed decimal conversion\n");
       }
    }
    else
    {
       if(sqlitexVdbeMemIntervalfy(pVal, type)) {
          fprintf(stderr, "sqlitex_value_interval: failed conversion\n");
       }
    }
  }
  return &p->du.tv;
}
const void *sqlitex_value_blob(sqlitex_value *pVal){
  Mem *p = (Mem*)pVal;
  if( p->flags & (MEM_Blob|MEM_Str) ){
    sqlitexVdbeMemExpandBlob(p);
    p->flags |= MEM_Blob;
    return p->n ? p->z : 0;
  }else{
    return sqlitex_value_text(pVal);
  }
}
int sqlitex_value_bytes(sqlitex_value *pVal){
  return sqlitexValueBytes(pVal, SQLITE_UTF8);
}
int sqlitex_value_bytes16(sqlitex_value *pVal){
  return sqlitexValueBytes(pVal, SQLITE_UTF16NATIVE);
}
double sqlitex_value_double(sqlitex_value *pVal){
  return sqlitexVdbeRealValue((Mem*)pVal);
}
int sqlitex_value_int(sqlitex_value *pVal){
  return (int)sqlitexVdbeIntValue((Mem*)pVal);
}
sqlite_int64 sqlitex_value_int64(sqlitex_value *pVal){
  return sqlitexVdbeIntValue((Mem*)pVal);
}
const unsigned char *sqlitex_value_text(sqlitex_value *pVal){
  return (const unsigned char *)sqlitexValueText(pVal, SQLITE_UTF8);
}
#ifndef SQLITE_OMIT_UTF16
const void *sqlitex_value_text16(sqlitex_value* pVal){
  return sqlitexValueText(pVal, SQLITE_UTF16NATIVE);
}
const void *sqlitex_value_text16be(sqlitex_value *pVal){
  return sqlitexValueText(pVal, SQLITE_UTF16BE);
}
const void *sqlitex_value_text16le(sqlitex_value *pVal){
  return sqlitexValueText(pVal, SQLITE_UTF16LE);
}
#endif /* SQLITE_OMIT_UTF16 */
/* EVIDENCE-OF: R-12793-43283 Every value in SQLite has one of five
** fundamental datatypes: 64-bit signed integer 64-bit IEEE floating
** point number string BLOB NULL
*/
int sqlitex_value_type(sqlitex_value* pVal){
  static const u8 aType[] = {
     SQLITE_BLOB,     /* 0x00 */
     SQLITE_NULL,     /* 0x01 */
     SQLITE3_TEXT,     /* 0x02 */
     SQLITE_NULL,     /* 0x03 */
     SQLITE_INTEGER,  /* 0x04 */
     SQLITE_NULL,     /* 0x05 */
     SQLITE_INTEGER,  /* 0x06 */
     SQLITE_NULL,     /* 0x07 */
     SQLITE_FLOAT,    /* 0x08 */
     SQLITE_NULL,     /* 0x09 */
     SQLITE_FLOAT,    /* 0x0a */
     SQLITE_NULL,     /* 0x0b */
     SQLITE_INTEGER,  /* 0x0c */
     SQLITE_NULL,     /* 0x0d */
     SQLITE_INTEGER,  /* 0x0e */
     SQLITE_NULL,     /* 0x0f */
     SQLITE_BLOB,     /* 0x10 */
     SQLITE_NULL,     /* 0x11 */
     SQLITE3_TEXT,     /* 0x12 */
     SQLITE_NULL,     /* 0x13 */
     SQLITE_INTEGER,  /* 0x14 */
     SQLITE_NULL,     /* 0x15 */
     SQLITE_INTEGER,  /* 0x16 */
     SQLITE_NULL,     /* 0x17 */
     SQLITE_FLOAT,    /* 0x18 */
     SQLITE_NULL,     /* 0x19 */
     SQLITE_FLOAT,    /* 0x1a */
     SQLITE_NULL,     /* 0x1b */
     SQLITE_INTEGER,  /* 0x1c */
     SQLITE_NULL,     /* 0x1d */
     SQLITE_INTEGER,  /* 0x1e */
     SQLITE_NULL,     /* 0x1f */
  };

  /* COMDB2 MODIFICATION */
  if( pVal->flags & MEM_Interval){
    switch (pVal->du.tv.type) {
      case INTV_YM_TYPE:
        return SQLITE_INTERVAL_YM;
      case INTV_DS_TYPE:
        return SQLITE_INTERVAL_DS;
      case INTV_DSUS_TYPE:
        return SQLITE_INTERVAL_DSUS;
      default:
        return SQLITE_DECIMAL;
    }
  }else if( pVal->flags & MEM_Datetime ){
    return (pVal->du.dt.dttz_prec == DTTZ_PREC_USEC)?
        SQLITE_DATETIMEUS:SQLITE_DATETIME;
  }

  return aType[pVal->flags&MEM_AffMask];
}

/**************************** sqlitex_result_  *******************************
** The following routines are used by user-defined functions to specify
** the function result.
**
** The setStrOrError() function calls sqlitexVdbeMemSetStr() to store the
** result as a string or blob but if the string or blob is too large, it
** then sets the error code to SQLITE_TOOBIG
**
** The invokeValueDestructor(P,X) routine invokes destructor function X()
** on value P is not going to be used and need to be destroyed.
*/
static void setResultStrOrError(
  sqlitex_context *pCtx,  /* Function context */
  const char *z,          /* String pointer */
  int n,                  /* Bytes in string, or negative */
  u8 enc,                 /* Encoding of z.  0 for BLOBs */
  void (*xDel)(void*)     /* Destructor function */
){
  if( sqlitexVdbeMemSetStr(pCtx->pOut, z, n, enc, xDel)==SQLITE_TOOBIG ){
    sqlitex_result_error_toobig(pCtx);
  }
}
static int invokeValueDestructor(
  const void *p,             /* Value to destroy */
  void (*xDel)(void*),       /* The destructor */
  sqlitex_context *pCtx      /* Set a SQLITE_TOOBIG error if no NULL */
){
  assert( xDel!=SQLITE_DYNAMIC );
  if( xDel==0 ){
    /* noop */
  }else if( xDel==SQLITEX_TRANSIENT ){
    /* noop */
  }else{
    xDel((void*)p);
  }
  if( pCtx ) sqlitex_result_error_toobig(pCtx);
  return SQLITE_TOOBIG;
}
void sqlitex_result_blob(
  sqlitex_context *pCtx, 
  const void *z, 
  int n, 
  void (*xDel)(void *)
){
  assert( n>=0 );
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  setResultStrOrError(pCtx, z, n, 0, xDel);
}
void sqlitex_result_blob64(
  sqlitex_context *pCtx, 
  const void *z, 
  sqlitex_uint64 n,
  void (*xDel)(void *)
){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  assert( xDel!=SQLITE_DYNAMIC );
  if( n>0x7fffffff ){
    (void)invokeValueDestructor(z, xDel, pCtx);
  }else{
    setResultStrOrError(pCtx, z, (int)n, 0, xDel);
  }
}
void sqlitex_result_double(sqlitex_context *pCtx, double rVal){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  sqlitexVdbeMemSetDouble(pCtx->pOut, rVal);
}
void sqlitex_result_error(sqlitex_context *pCtx, const char *z, int n){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  pCtx->isError = SQLITE_ERROR;
  pCtx->fErrorOrAux = 1;
  sqlitexVdbeMemSetStr(pCtx->pOut, z, n, SQLITE_UTF8, SQLITEX_TRANSIENT);
}
#ifndef SQLITE_OMIT_UTF16
void sqlitex_result_error16(sqlitex_context *pCtx, const void *z, int n){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  pCtx->isError = SQLITE_ERROR;
  pCtx->fErrorOrAux = 1;
  sqlitexVdbeMemSetStr(pCtx->pOut, z, n, SQLITE_UTF16NATIVE, SQLITEX_TRANSIENT);
}
#endif
void sqlitex_result_int(sqlitex_context *pCtx, int iVal){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  sqlitexVdbeMemSetInt64(pCtx->pOut, (i64)iVal);
}
void sqlitex_result_int64(sqlitex_context *pCtx, i64 iVal){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  sqlitexVdbeMemSetInt64(pCtx->pOut, iVal);
}
void sqlitex_result_null(sqlitex_context *pCtx){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  sqlitexVdbeMemSetNull(pCtx->pOut);
}
void sqlitex_result_text(
  sqlitex_context *pCtx, 
  const char *z, 
  int n,
  void (*xDel)(void *)
){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  setResultStrOrError(pCtx, z, n, SQLITE_UTF8, xDel);
}
void sqlitex_result_text64(
  sqlitex_context *pCtx, 
  const char *z, 
  sqlitex_uint64 n,
  void (*xDel)(void *),
  unsigned char enc
){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  assert( xDel!=SQLITE_DYNAMIC );
  if( enc==SQLITE_UTF16 ) enc = SQLITE_UTF16NATIVE;
  if( n>0x7fffffff ){
    (void)invokeValueDestructor(z, xDel, pCtx);
  }else{
    setResultStrOrError(pCtx, z, (int)n, enc, xDel);
  }
}
#ifndef SQLITE_OMIT_UTF16
void sqlitex_result_text16(
  sqlitex_context *pCtx, 
  const void *z, 
  int n, 
  void (*xDel)(void *)
){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  setResultStrOrError(pCtx, z, n, SQLITE_UTF16NATIVE, xDel);
}
void sqlitex_result_text16be(
  sqlitex_context *pCtx, 
  const void *z, 
  int n, 
  void (*xDel)(void *)
){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  setResultStrOrError(pCtx, z, n, SQLITE_UTF16BE, xDel);
}
void sqlitex_result_text16le(
  sqlitex_context *pCtx, 
  const void *z, 
  int n, 
  void (*xDel)(void *)
){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  setResultStrOrError(pCtx, z, n, SQLITE_UTF16LE, xDel);
}
#endif /* SQLITE_OMIT_UTF16 */
void sqlitex_result_value(sqlitex_context *pCtx, sqlitex_value *pValue){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  sqlitexVdbeMemCopy(pCtx->pOut, pValue);
}
void sqlitex_result_zeroblob(sqlitex_context *pCtx, int n){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  sqlitexVdbeMemSetZeroBlob(pCtx->pOut, n);
}
void sqlitex_result_error_code(sqlitex_context *pCtx, int errCode){
  pCtx->isError = errCode;
  pCtx->fErrorOrAux = 1;
#ifdef SQLITE_DEBUG
  if( pCtx->pVdbe ) pCtx->pVdbe->rcApp = errCode;
#endif
  if( pCtx->pOut->flags & MEM_Null ){
    sqlitexVdbeMemSetStr(pCtx->pOut, sqlitexErrStr(errCode), -1, 
                         SQLITE_UTF8, SQLITEX_STATIC);
  }
}

/* COMDB2 MODIFICATION */
void sqlitex_result_datetime(sqlitex_context *pCtx, dttz_t *dt, const char *tz){
  sqlitexVdbeMemSetDatetime(pCtx->pOut, dt, tz);
}

/* COMDB2 MODIFICATION */
void sqlitex_result_interval(sqlitex_context *pCtx, intv_t *pValue){
  sqlitexVdbeMemSetInterval(pCtx->pOut, pValue);
}

/* COMDB2 MODIFICATION */
void sqlitex_result_decimal(sqlitex_context *pCtx, decQuad *dec){
  sqlitexVdbeMemSetDecimal(pCtx->pOut, dec);
}

/* Force an SQLITE_TOOBIG error. */
void sqlitex_result_error_toobig(sqlitex_context *pCtx){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  pCtx->isError = SQLITE_TOOBIG;
  pCtx->fErrorOrAux = 1;
  sqlitexVdbeMemSetStr(pCtx->pOut, "string or blob too big", -1, 
                       SQLITE_UTF8, SQLITEX_STATIC);
}

/* An SQLITE_NOMEM error. */
void sqlitex_result_error_nomem(sqlitex_context *pCtx){
  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  sqlitexVdbeMemSetNull(pCtx->pOut);
  pCtx->isError = SQLITE_NOMEM;
  pCtx->fErrorOrAux = 1;
  pCtx->pOut->db->mallocFailed = 1;
}

/*
** This function is called after a transaction has been committed. It 
** invokes callbacks registered with sqlitex_wal_hook() as required.
*/
static int doWalCallbacks(sqlitex *db){
  int rc = SQLITE_OK;
#ifndef SQLITE_OMIT_WAL
  int i;
  for(i=0; i<db->nDb; i++){
    Btree *pBt = db->aDb[i].pBt;
    if( pBt ){
      int nEntry;
      sqlite3BtreeEnter(pBt);
      nEntry = sqlitexPagerWalCallback(sqlite3BtreePager(pBt));
      sqlite3BtreeLeave(pBt);
      if( db->xWalCallback && nEntry>0 && rc==SQLITE_OK ){
        rc = db->xWalCallback(db->pWalArg, db, db->aDb[i].zName, nEntry);
      }
    }
  }
#endif
  return rc;
}

/*
** Execute the statement pStmt, either until a row of data is ready, the
** statement is completely executed or an error occurs.
**
** This routine implements the bulk of the logic behind the sqlite_step()
** API.  The only thing omitted is the automatic recompile if a 
** schema change has occurred.  That detail is handled by the
** outer sqlitex_step() wrapper procedure.
*/
static int sqlitexStep(Vdbe *p){
  sqlitex *db;
  int rc;

  assert(p);
  if( p->magic!=VDBE_MAGIC_RUN ){
    /* We used to require that sqlitex_reset() be called before retrying
    ** sqlitex_step() after any error or after SQLITE_DONEX.  But beginning
    ** with version 3.7.0, we changed this so that sqlitex_reset() would
    ** be called automatically instead of throwing the SQLITE_MISUSE error.
    ** This "automatic-reset" change is not technically an incompatibility, 
    ** since any application that receives an SQLITE_MISUSE is broken by
    ** definition.
    **
    ** Nevertheless, some published applications that were originally written
    ** for version 3.6.23 or earlier do in fact depend on SQLITE_MISUSE 
    ** returns, and those were broken by the automatic-reset change.  As a
    ** a work-around, the SQLITE_OMIT_AUTORESET compile-time restores the
    ** legacy behavior of returning SQLITE_MISUSE for cases where the 
    ** previous sqlitex_step() returned something other than a SQLITE_LOCKED
    ** or SQLITE_BUSY error.
    */
#ifdef SQLITE_OMIT_AUTORESET
    if( (rc = p->rc&0xff)==SQLITE_BUSY || rc==SQLITE_LOCKED ){
      sqlitex_reset((sqlitex_stmt*)p);
    }else{
      return SQLITE_MISUSE_BKPT;
    }
#else
    sqlitex_reset((sqlitex_stmt*)p);
#endif
  }

  /* Check that malloc() has not failed. If it has, return early. */
  db = p->db;
  if( db->mallocFailed ){
    p->rc = SQLITE_NOMEM;
    return SQLITE_NOMEM;
  }

  if( p->pc<=0 && p->expired ){
    p->rc = SQLITE_SCHEMA;
    rc = SQLITE_ERROR;
    goto end_of_step;
  }
  if( p->pc<0 ){
    /* If there are no other statements currently running, then
    ** reset the interrupt flag.  This prevents a call to sqlitex_interrupt
    ** from interrupting a statement that has not yet started.
    */
    if( db->nVdbeActive==0 ){
      db->u1.isInterrupted = 0;
    }

    assert( db->nVdbeWrite>0 || db->autoCommit==0 
        || (db->nDeferredCons==0 && db->nDeferredImmCons==0)
    );

#ifndef SQLITE_OMIT_TRACE
    if( db->xProfile && !db->init.busy ){
      sqlitexOsCurrentTimeInt64(db->pVfs, &p->startTime);
    }
#endif

    db->nVdbeActive++;
    if( p->readOnly==0 ) db->nVdbeWrite++;
    if( p->bIsReader ) db->nVdbeRead++;
    p->pc = 0;
  }
#ifdef SQLITE_DEBUG
  p->rcApp = SQLITE_OK;
#endif
#ifndef SQLITE_OMIT_EXPLAIN
  if( p->explain ){
    rc = sqlitexVdbeList(p);
  }else
#endif /* SQLITE_OMIT_EXPLAIN */
  {
    db->nVdbeExec++;
    rc = sqlitexVdbeExec(p);
    db->nVdbeExec--;
  }


  if (rc == SQLITE_COMDB2SCHEMA)
    return rc;

#ifndef SQLITE_OMIT_TRACE
  /* Invoke the profile callback if there is one
  */
  if( rc!=SQLITE_ROW && db->xProfile && !db->init.busy && p->zSql ){
    sqlitex_int64 iNow;
    sqlitexOsCurrentTimeInt64(db->pVfs, &iNow);
    db->xProfile(db->pProfileArg, p->zSql, (iNow - p->startTime)*1000000);
  }
#endif

  if( rc==SQLITE_DONEX ){
    assert( p->rc==SQLITE_OK );
    p->rc = doWalCallbacks(db);
    if( p->rc!=SQLITE_OK ){
      rc = SQLITE_ERROR;
    }
  }

  db->errCode = rc;
  if( SQLITE_NOMEM==sqlitexApiExit(p->db, p->rc) ){
    p->rc = SQLITE_NOMEM;
  }
end_of_step:
  /* At this point local variable rc holds the value that should be 
  ** returned if this statement was compiled using the legacy 
  ** sqlitex_prepare() interface. According to the docs, this can only
  ** be one of the values in the first assert() below. Variable p->rc 
  ** contains the value that would be returned if sqlitex_finalize() 
  ** were called on statement p.
  */
  assert( rc==SQLITE_ROW  || rc==SQLITE_DONEX   || rc==SQLITE_ERROR 
       || rc==SQLITE_BUSY || rc==SQLITE_MISUSE
       || rc==SQLITE_ABORT
       /* COMDB2 MODIFICATION */
       || rc==SQLITE_TOOBIG
  );
  assert( (p->rc!=SQLITE_ROW && p->rc!=SQLITE_DONEX) || p->rc==p->rcApp );
  if( p->isPrepareV2 && rc!=SQLITE_ROW && rc!=SQLITE_DONEX ){
    /* If this statement was prepared using sqlitex_prepare_v2(), and an
    ** error has occurred, then return the error code in p->rc to the
    ** caller. Set the error code in the database handle to the same value.
    */ 
    rc = sqlitexVdbeTransferError(p);
  }
  return (rc&db->errMask);
}

/*
** This is the top-level implementation of sqlitex_step().  Call
** sqlitexStep() to do most of the work.  If a schema error occurs,
** call sqlitexReprepare() and try again.
*/
int sqlitex_step(sqlitex_stmt *pStmt){
  int rc = SQLITE_OK;      /* Result from sqlitexStep() */
  int rc2 = SQLITE_OK;     /* Result from sqlitexReprepare() */
  Vdbe *v = (Vdbe*)pStmt;  /* the prepared statement */
  int cnt = 0;             /* Counter to prevent infinite loop of reprepares */
  sqlitex *db;             /* The database connection */

  if( vdbeSafetyNotNull(v) ){
    return SQLITE_MISUSE_BKPT;
  }
  db = v->db;
  sqlitex_mutex_enter(db->mutex);
  v->doingRerun = 0;
  while( (rc = sqlitexStep(v))==SQLITE_SCHEMA
         && cnt++ < SQLITE_MAX_SCHEMA_RETRY ){
    int savedPc = v->pc;
    rc2 = rc = sqlitexReprepare(v);
    if( rc!=SQLITE_OK) break;
    sqlitex_reset(pStmt);
    if( savedPc>=0 ) v->doingRerun = 1;
    assert( v->expired==0 );
  }

  if (rc == SQLITE_COMDB2SCHEMA)
    return rc;

  if( rc2!=SQLITE_OK ){
    /* This case occurs after failing to recompile an sql statement. 
    ** The error message from the SQL compiler has already been loaded 
    ** into the database handle. This block copies the error message 
    ** from the database handle into the statement and sets the statement
    ** program counter to 0 to ensure that when the statement is 
    ** finalized or reset the parser error message is available via
    ** sqlitex_errmsg() and sqlitex_errcode().
    */
    const char *zErr = (const char *)sqlitex_value_text(db->pErr); 
    sqlitexDbFree(db, v->zErrMsg);
    if( !db->mallocFailed ){
      v->zErrMsg = sqlitexDbStrDup(db, zErr);
      v->rc = rc2;
    } else {
      v->zErrMsg = 0;
      v->rc = rc = SQLITE_NOMEM;
    }
  }
  rc = sqlitexApiExit(db, rc);
  sqlitex_mutex_leave(db->mutex);
  return rc;
}


/*
** Extract the user data from a sqlitex_context structure and return a
** pointer to it.
*/
void *sqlitex_user_data(sqlitex_context *p){
  assert( p && p->pFunc );
  return p->pFunc->pUserData;
}

/*
** Extract the user data from a sqlitex_context structure and return a
** pointer to it.
**
** IMPLEMENTATION-OF: R-46798-50301 The sqlitex_context_db_handle() interface
** returns a copy of the pointer to the database connection (the 1st
** parameter) of the sqlitex_create_function() and
** sqlitex_create_function16() routines that originally registered the
** application defined function.
*/
sqlitex *sqlitex_context_db_handle(sqlitex_context *p){
  assert( p && p->pFunc );
  return p->pOut->db;
}

/*
** Return the current time for a statement.  If the current time
** is requested more than once within the same run of a single prepared
** statement, the exact same time is returned for each invocation regardless
** of the amount of time that elapses between invocations.  In other words,
** the time returned is always the time of the first call.
*/
sqlitex_int64 sqlitexStmtCurrentTime(sqlitex_context *p){
  int rc;
#ifndef SQLITE_ENABLE_STAT3_OR_STAT4
  sqlitex_int64 *piTime = &p->pVdbe->iCurrentTime;
  assert( p->pVdbe!=0 );
#else
  sqlitex_int64 iTime = 0;
  sqlitex_int64 *piTime = p->pVdbe!=0 ? &p->pVdbe->iCurrentTime : &iTime;
#endif
  if( *piTime==0 ){
    rc = sqlitexOsCurrentTimeInt64(p->pOut->db->pVfs, piTime);
    if( rc ) *piTime = 0;
  }
  return *piTime;
}

/*
** The following is the implementation of an SQL function that always
** fails with an error message stating that the function is used in the
** wrong context.  The sqlitex_overload_function() API might construct
** SQL function that use this routine so that the functions will exist
** for name resolution but are actually overloaded by the xFindFunction
** method of virtual tables.
*/
void sqlitexInvalidFunction(
  sqlitex_context *context,  /* The function calling context */
  int NotUsed,               /* Number of arguments to the function */
  sqlitex_value **NotUsed2   /* Value of each argument */
){
  const char *zName = context->pFunc->zName;
  char *zErr;
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  zErr = sqlitex_mprintf(
      "unable to use function %s in the requested context", zName);
  sqlitex_result_error(context, zErr, -1);
  sqlitex_free(zErr);
}

/*
** Create a new aggregate context for p and return a pointer to
** its pMem->z element.
*/
static SQLITE_NOINLINE void *createAggContext(sqlitex_context *p, int nByte){
  Mem *pMem = p->pMem;
  assert( (pMem->flags & MEM_Agg)==0 );
  if( nByte<=0 ){
    sqlitexVdbeMemSetNull(pMem);
    pMem->z = 0;
  }else{
    sqlitexVdbeMemClearAndResize(pMem, nByte);
    pMem->flags = MEM_Agg;
    pMem->u.pDef = p->pFunc;
    if( pMem->z ){
      memset(pMem->z, 0, nByte);
    }
  }
  return (void*)pMem->z;
}

/*
** Allocate or return the aggregate context for a user function.  A new
** context is allocated on the first call.  Subsequent calls return the
** same context that was returned on prior calls.
*/
void *sqlitex_aggregate_context(sqlitex_context *p, int nByte){
  assert( p && p->pFunc && p->pFunc->xStep );
  assert( sqlitex_mutex_held(p->pOut->db->mutex) );
  testcase( nByte<0 );
  if( (p->pMem->flags & MEM_Agg)==0 ){
    return createAggContext(p, nByte);
  }else{
    return (void*)p->pMem->z;
  }
}

/*
** Return the auxiliary data pointer, if any, for the iArg'th argument to
** the user-function defined by pCtx.
*/
void *sqlitex_get_auxdata(sqlitex_context *pCtx, int iArg){
  AuxData *pAuxData;

  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
#if SQLITE_ENABLE_STAT3_OR_STAT4
  if( pCtx->pVdbe==0 ) return 0;
#else
  assert( pCtx->pVdbe!=0 );
#endif
  for(pAuxData=pCtx->pVdbe->pAuxData; pAuxData; pAuxData=pAuxData->pNext){
    if( pAuxData->iOp==pCtx->iOp && pAuxData->iArg==iArg ) break;
  }

  return (pAuxData ? pAuxData->pAux : 0);
}

/*
** Set the auxiliary data pointer and delete function, for the iArg'th
** argument to the user-function defined by pCtx. Any previous value is
** deleted by calling the delete function specified when it was set.
*/
void sqlitex_set_auxdata(
  sqlitex_context *pCtx, 
  int iArg, 
  void *pAux, 
  void (*xDelete)(void*)
){
  AuxData *pAuxData;
  Vdbe *pVdbe = pCtx->pVdbe;

  assert( sqlitex_mutex_held(pCtx->pOut->db->mutex) );
  if( iArg<0 ) goto failed;
#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
  if( pVdbe==0 ) goto failed;
#else
  assert( pVdbe!=0 );
#endif

  for(pAuxData=pVdbe->pAuxData; pAuxData; pAuxData=pAuxData->pNext){
    if( pAuxData->iOp==pCtx->iOp && pAuxData->iArg==iArg ) break;
  }
  if( pAuxData==0 ){
    pAuxData = sqlitexDbMallocZero(pVdbe->db, sizeof(AuxData));
    if( !pAuxData ) goto failed;
    pAuxData->iOp = pCtx->iOp;
    pAuxData->iArg = iArg;
    pAuxData->pNext = pVdbe->pAuxData;
    pVdbe->pAuxData = pAuxData;
    if( pCtx->fErrorOrAux==0 ){
      pCtx->isError = 0;
      pCtx->fErrorOrAux = 1;
    }
  }else if( pAuxData->xDelete ){
    pAuxData->xDelete(pAuxData->pAux);
  }

  pAuxData->pAux = pAux;
  pAuxData->xDelete = xDelete;
  return;

failed:
  if( xDelete ){
    xDelete(pAux);
  }
}

#ifndef SQLITE_OMIT_DEPRECATED
/*
** Return the number of times the Step function of an aggregate has been 
** called.
**
** This function is deprecated.  Do not use it for new code.  It is
** provide only to avoid breaking legacy code.  New aggregate function
** implementations should keep their own counts within their aggregate
** context.
*/
int sqlitex_aggregate_count(sqlitex_context *p){
  assert( p && p->pMem && p->pFunc && p->pFunc->xStep );
  return p->pMem->n;
}
#endif

/*
** Return the number of columns in the result set for the statement pStmt.
*/
int sqlitex_column_count(sqlitex_stmt *pStmt){
  Vdbe *pVm = (Vdbe *)pStmt;
  return pVm ? pVm->nResColumn : 0;
}

/*
** Return the number of values available from the current row of the
** currently executing statement pStmt.
*/
int sqlitex_data_count(sqlitex_stmt *pStmt){
  Vdbe *pVm = (Vdbe *)pStmt;
  if( pVm==0 || pVm->pResultSet==0 ) return 0;
  return pVm->nResColumn;
}

/*
** Return a pointer to static memory containing an SQL NULL value.
*/
static const Mem *columnNullValue(void){
  /* Even though the Mem structure contains an element
  ** of type i64, on certain architectures (x86) with certain compiler
  ** switches (-Os), gcc may align this Mem object on a 4-byte boundary
  ** instead of an 8-byte one. This all works fine, except that when
  ** running with SQLITE_DEBUG defined the SQLite code sometimes assert()s
  ** that a Mem structure is located on an 8-byte boundary. To prevent
  ** these assert()s from failing, when building with SQLITE_DEBUG defined
  ** using gcc, we force nullMem to be 8-byte aligned using the magical
  ** __attribute__((aligned(8))) macro.  */
  static const Mem nullMem 
#if defined(SQLITE_DEBUG) && defined(__GNUC__)
    __attribute__((aligned(8))) 
#endif
    = {
        /* .u          = */ {0},
        /* COMDB2 MODIFICATION BEGINS */
        /* .du         = */ {0},
        /* .tz         = */ 0,
        /* .dtprec     = */ 0,
        /* COMDB2 MODIFICATION ENDS */
        /* .flags      = */ MEM_Null,
        /* .enc        = */ 0,
        /* .n          = */ 0,
        /* .z          = */ 0,
        /* .db         = */ 0,
        /* .zMalloc    = */ 0,
        /* .szMalloc   = */ 0,
        /* .uTemp      = */ 0,
        /* .xDel       = */ 0,
#ifdef SQLITE_DEBUG
        /* .pScopyFrom = */ 0,
        /* .pFiller    = */ 0,
#endif
      };
  return &nullMem;
}

/*
** Check to see if column iCol of the given statement is valid.  If
** it is, return a pointer to the Mem for the value of that column.
** If iCol is not valid, return a pointer to a Mem which has a value
** of NULL.
*/
static Mem *columnMem(sqlitex_stmt *pStmt, int i){
  Vdbe *pVm;
  Mem *pOut;

  pVm = (Vdbe *)pStmt;
  if( pVm && pVm->pResultSet!=0 && i<pVm->nResColumn && i>=0 ){
    sqlitex_mutex_enter(pVm->db->mutex);
    pOut = &pVm->pResultSet[i];
    /* COMDB2 MODIFICATION */
    /* we need to damn tzname in most impossible functions
     * so spread it around when you have it 20071010dh */
    pOut->tz = pVm->tzname;
    pOut->dtprec = pVm->dtprec;
    /* COMDB2 MODIFICATION */
    /* Associate a db with the Mem so we can set errors on it
     * when there's conversion failures */
    pOut->db = ((Vdbe*) pStmt)->db;
  }else{
    if( pVm && ALWAYS(pVm->db) ){
      sqlitex_mutex_enter(pVm->db->mutex);
      sqlitexError(pVm->db, SQLITE_RANGE);
    }
    pOut = (Mem*)columnNullValue();
  }
  return pOut;
}

/*
** This function is called after invoking an sqlitex_value_XXX function on a 
** column value (i.e. a value returned by evaluating an SQL expression in the
** select list of a SELECT statement) that may cause a malloc() failure. If 
** malloc() has failed, the threads mallocFailed flag is cleared and the result
** code of statement pStmt set to SQLITE_NOMEM.
**
** Specifically, this is called from within:
**
**     sqlitex_column_int()
**     sqlitex_column_int64()
**     sqlitex_column_text()
**     sqlitex_column_text16()
**     sqlitex_column_real()
**     sqlitex_column_bytes()
**     sqlitex_column_bytes16()
**     sqiite3_column_blob()
*/
static void columnMallocFailure(sqlitex_stmt *pStmt)
{
  /* If malloc() failed during an encoding conversion within an
  ** sqlitex_column_XXX API, then set the return code of the statement to
  ** SQLITE_NOMEM. The next call to _step() (if any) will return SQLITE_ERROR
  ** and _finalize() will return NOMEM.
  */
  Vdbe *p = (Vdbe *)pStmt;
  if( p ){
    p->rc = sqlitexApiExit(p->db, p->rc);
    sqlitex_mutex_leave(p->db->mutex);
  }
}

/**************************** sqlitex_column_  *******************************
** The following routines are used to access elements of the current row
** in the result set.
*/
const void *sqlitex_column_blob(sqlitex_stmt *pStmt, int i){
  const void *val;
  val = sqlitex_value_blob( columnMem(pStmt,i) );
  /* Even though there is no encoding conversion, value_blob() might
  ** need to call malloc() to expand the result of a zeroblob() 
  ** expression. 
  */
  columnMallocFailure(pStmt);
  return val;
}
int sqlitex_column_bytes(sqlitex_stmt *pStmt, int i){
  int val = sqlitex_value_bytes( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return val;
}
int sqlitex_column_bytes16(sqlitex_stmt *pStmt, int i){
  int val = sqlitex_value_bytes16( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return val;
}
double sqlitex_column_double(sqlitex_stmt *pStmt, int i){
  double val = sqlitex_value_double( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return val;
}
int sqlitex_column_int(sqlitex_stmt *pStmt, int i){
  int val = sqlitex_value_int( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return val;
}
sqlite_int64 sqlitex_column_int64(sqlitex_stmt *pStmt, int i){
  sqlite_int64 val = sqlitex_value_int64( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return val;
}
const unsigned char *sqlitex_column_text(sqlitex_stmt *pStmt, int i){
  const unsigned char *val = sqlitex_value_text( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return val;
}
sqlitex_value *sqlitex_column_value(sqlitex_stmt *pStmt, int i){
  Mem *pOut = columnMem(pStmt, i);
  if( pOut->flags&MEM_Static ){
    pOut->flags &= ~MEM_Static;
    pOut->flags |= MEM_Ephem;
  }
  columnMallocFailure(pStmt);
  return (sqlitex_value *)pOut;
}
#ifndef SQLITE_OMIT_UTF16
const void *sqlitex_column_text16(sqlitex_stmt *pStmt, int i){
  const void *val = sqlitex_value_text16( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return val;
}
#endif /* SQLITE_OMIT_UTF16 */
int sqlitex_column_type(sqlitex_stmt *pStmt, int i){
  int iType = sqlitex_value_type( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return iType;
}

/* COMDB2 MODIFICATION */
const dttz_t *sqlitex_column_datetime(sqlitex_stmt *pStmt, int i){
  return sqlitex_value_datetime( columnMem(pStmt,i) );
}

/* COMDB2 MODIFICATION */
const void *sqlitex_column_interval(sqlitex_stmt *pStmt, int i, int type){
  const void *val;
  val = sqlitex_value_interval( columnMem(pStmt,i), type );
  return val;
}

/*
** Convert the N-th element of pStmt->pColName[] into a string using
** xFunc() then return that string.  If N is out of range, return 0.
**
** There are up to 5 names for each column.  useType determines which
** name is returned.  Here are the names:
**
**    0      The column name as it should be displayed for output
**    1      The datatype name for the column
**    2      The name of the database that the column derives from
**    3      The name of the table that the column derives from
**    4      The name of the table column that the result column derives from
**
** If the result is not a simple column reference (if it is an expression
** or a constant) then useTypes 2, 3, and 4 return NULL.
*/
static const void *columnName(
  sqlitex_stmt *pStmt,
  int N,
  const void *(*xFunc)(Mem*),
  int useType
){
  const void *ret;
  Vdbe *p;
  int n;
  sqlitex *db;
#ifdef SQLITE_ENABLE_API_ARMOR
  if( pStmt==0 ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
  ret = 0;
  p = (Vdbe *)pStmt;
  db = p->db;
  assert( db!=0 );
  n = sqlitex_column_count(pStmt);
  if( N<n && N>=0 ){
    N += useType*n;
    sqlitex_mutex_enter(db->mutex);
    assert( db->mallocFailed==0 );
    ret = xFunc(&p->aColName[N]);
     /* A malloc may have failed inside of the xFunc() call. If this
    ** is the case, clear the mallocFailed flag and return NULL.
    */
    if( db->mallocFailed ){
      db->mallocFailed = 0;
      ret = 0;
    }
    sqlitex_mutex_leave(db->mutex);
  }
  return ret;
}

/*
** Return the name of the Nth column of the result set returned by SQL
** statement pStmt.
*/
const char *sqlitex_column_name(sqlitex_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlitex_value_text, COLNAME_NAME);
}
#ifndef SQLITE_OMIT_UTF16
const void *sqlitex_column_name16(sqlitex_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlitex_value_text16, COLNAME_NAME);
}
#endif

/*
** Constraint:  If you have ENABLE_COLUMN_METADATA then you must
** not define OMIT_DECLTYPE.
*/
#if defined(SQLITE_OMIT_DECLTYPE) && defined(SQLITE_ENABLE_COLUMN_METADATA)
# error "Must not define both SQLITE_OMIT_DECLTYPE \
         and SQLITE_ENABLE_COLUMN_METADATA"
#endif

#ifndef SQLITE_OMIT_DECLTYPE
/*
** Return the column declaration type (if applicable) of the 'i'th column
** of the result set of SQL statement pStmt.
*/
const char *sqlitex_column_decltype(sqlitex_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlitex_value_text, COLNAME_DECLTYPE);
}
#ifndef SQLITE_OMIT_UTF16
const void *sqlitex_column_decltype16(sqlitex_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlitex_value_text16, COLNAME_DECLTYPE);
}
#endif /* SQLITE_OMIT_UTF16 */
#endif /* SQLITE_OMIT_DECLTYPE */

#ifdef SQLITE_ENABLE_COLUMN_METADATA
/*
** Return the name of the database from which a result column derives.
** NULL is returned if the result column is an expression or constant or
** anything else which is not an unambiguous reference to a database column.
*/
const char *sqlitex_column_database_name(sqlitex_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlitex_value_text, COLNAME_DATABASE);
}
#ifndef SQLITE_OMIT_UTF16
const void *sqlitex_column_database_name16(sqlitex_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlitex_value_text16, COLNAME_DATABASE);
}
#endif /* SQLITE_OMIT_UTF16 */

/*
** Return the name of the table from which a result column derives.
** NULL is returned if the result column is an expression or constant or
** anything else which is not an unambiguous reference to a database column.
*/
const char *sqlitex_column_table_name(sqlitex_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlitex_value_text, COLNAME_TABLE);
}
#ifndef SQLITE_OMIT_UTF16
const void *sqlitex_column_table_name16(sqlitex_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlitex_value_text16, COLNAME_TABLE);
}
#endif /* SQLITE_OMIT_UTF16 */

/*
** Return the name of the table column from which a result column derives.
** NULL is returned if the result column is an expression or constant or
** anything else which is not an unambiguous reference to a database column.
*/
const char *sqlitex_column_origin_name(sqlitex_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlitex_value_text, COLNAME_COLUMN);
}
#ifndef SQLITE_OMIT_UTF16
const void *sqlitex_column_origin_name16(sqlitex_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlitex_value_text16, COLNAME_COLUMN);
}
#endif /* SQLITE_OMIT_UTF16 */
#endif /* SQLITE_ENABLE_COLUMN_METADATA */


/******************************* sqlitex_bind_  ***************************
** 
** Routines used to attach values to wildcards in a compiled SQL statement.
*/
/*
** Unbind the value bound to variable i in virtual machine p. This is the 
** the same as binding a NULL value to the column. If the "i" parameter is
** out of range, then SQLITE_RANGE is returned. Othewise SQLITE_OK.
**
** A successful evaluation of this routine acquires the mutex on p.
** the mutex is released if any kind of error occurs.
**
** The error code stored in database p->db is overwritten with the return
** value in any case.
*/
static int vdbeUnbind(Vdbe *p, int i){
  Mem *pVar;
  if( vdbeSafetyNotNull(p) ){
    return SQLITE_MISUSE_BKPT;
  }
  sqlitex_mutex_enter(p->db->mutex);
  if( p->magic!=VDBE_MAGIC_RUN || p->pc>=0 ){
    sqlitexError(p->db, SQLITE_MISUSE);
    sqlitex_mutex_leave(p->db->mutex);
    sqlitex_log(SQLITE_MISUSE, 
        "bind on a busy prepared statement: [%s]", p->zSql);
    return SQLITE_MISUSE_BKPT;
  }
  if( i<1 || i>p->nVar ){
    sqlitexError(p->db, SQLITE_RANGE);
    sqlitex_mutex_leave(p->db->mutex);
    return SQLITE_RANGE;
  }
  i--;
  pVar = &p->aVar[i];
  sqlitexVdbeMemRelease(pVar);
  pVar->flags = MEM_Null;
  sqlitexError(p->db, SQLITE_OK);

  /* If the bit corresponding to this variable in Vdbe.expmask is set, then 
  ** binding a new value to this variable invalidates the current query plan.
  **
  ** IMPLEMENTATION-OF: R-48440-37595 If the specific value bound to host
  ** parameter in the WHERE clause might influence the choice of query plan
  ** for a statement, then the statement will be automatically recompiled,
  ** as if there had been a schema change, on the first sqlitex_step() call
  ** following any change to the bindings of that parameter.
  */
  if( p->isPrepareV2 &&
     ((i<32 && p->expmask & ((u32)1 << i)) || p->expmask==0xffffffff)
  ){
    p->expired = 1;
  }
  return SQLITE_OK;
}

/*
** Bind a text or BLOB value.
*/
static int bindText(
  sqlitex_stmt *pStmt,   /* The statement to bind against */
  int i,                 /* Index of the parameter to bind */
  const void *zData,     /* Pointer to the data to be bound */
  int nData,             /* Number of bytes of data to be bound */
  void (*xDel)(void*),   /* Destructor for the data */
  u8 encoding            /* Encoding for the data */
){
  Vdbe *p = (Vdbe *)pStmt;
  Mem *pVar;
  int rc;

  rc = vdbeUnbind(p, i);
  if( rc==SQLITE_OK ){
    if( zData!=0 ){
      pVar = &p->aVar[i-1];
      rc = sqlitexVdbeMemSetStr(pVar, zData, nData, encoding, xDel);
      if( rc==SQLITE_OK && encoding!=0 ){
        rc = sqlitexVdbeChangeEncoding(pVar, ENC(p->db));
      }
      sqlitexError(p->db, rc);
      rc = sqlitexApiExit(p->db, rc);
    }
    sqlitex_mutex_leave(p->db->mutex);
  }else if( xDel!=SQLITEX_STATIC && xDel!=SQLITEX_TRANSIENT ){
    xDel((void*)zData);
  }
  return rc;
}
 
/* COMDB2 MODIFICATION */
static int bindDatetime(
  sqlitex_stmt *pStmt, 
  int i, dttz_t *dt, char *tz
){
  Vdbe *p = (Vdbe *) pStmt;
  Mem *pVar;
  int rc;

  rc = vdbeUnbind(p, i);
  pVar = &p->aVar[i-1];
  pVar->flags &= ~(MEM_Str|MEM_Static|MEM_Dyn|MEM_Ephem);
  pVar->flags = MEM_Datetime;
  pVar->du.dt = *dt;
  pVar->xDel = NULL;
  pVar->enc = 0;
  pVar->tz = tz;

  sqlitexError(((Vdbe *)pStmt)->db, rc);
  return sqlitexApiExit(((Vdbe *)pStmt)->db, rc);
}

/* COMDB2 MODIFICATION */
static int bindInterval(
  sqlitex_stmt *pStmt, 
  int i, intv_t it
){
  Vdbe *p = (Vdbe *) pStmt;
  Mem *pVar;
  int rc;

  rc = vdbeUnbind(p, i);
  pVar = &p->aVar[i-1];
  pVar->flags = MEM_Interval;
  pVar->flags &= ~(MEM_Str|MEM_Static|MEM_Dyn|MEM_Ephem);
  pVar->du.tv = it;
  pVar->xDel = NULL;
  pVar->enc = 0;
  pVar->tz = NULL;

  sqlitexError(((Vdbe *)pStmt)->db, rc);
  return sqlitexApiExit(((Vdbe *)pStmt)->db, rc);
}

/*
** Bind a blob value to an SQL statement variable.
*/
int sqlitex_bind_blob(
  sqlitex_stmt *pStmt, 
  int i, 
  const void *zData, 
  int nData, 
  void (*xDel)(void*)
){
  return bindText(pStmt, i, zData, nData, xDel, 0);
}
int sqlitex_bind_blob64(
  sqlitex_stmt *pStmt, 
  int i, 
  const void *zData, 
  sqlitex_uint64 nData, 
  void (*xDel)(void*)
){
  assert( xDel!=SQLITE_DYNAMIC );
  if( nData>0x7fffffff ){
    return invokeValueDestructor(zData, xDel, 0);
  }else{
    return bindText(pStmt, i, zData, (int)nData, xDel, 0);
  }
}
int sqlitex_bind_double(sqlitex_stmt *pStmt, int i, double rValue){
  int rc;
  Vdbe *p = (Vdbe *)pStmt;
  rc = vdbeUnbind(p, i);
  if( rc==SQLITE_OK ){
    sqlitexVdbeMemSetDouble(&p->aVar[i-1], rValue);
    sqlitex_mutex_leave(p->db->mutex);
  }
  return rc;
}
int sqlitex_bind_int(sqlitex_stmt *p, int i, int iValue){
  return sqlitex_bind_int64(p, i, (i64)iValue);
}
int sqlitex_bind_int64(sqlitex_stmt *pStmt, int i, sqlite_int64 iValue){
  int rc;
  Vdbe *p = (Vdbe *)pStmt;
  rc = vdbeUnbind(p, i);
  if( rc==SQLITE_OK ){
    sqlitexVdbeMemSetInt64(&p->aVar[i-1], iValue);
    sqlitex_mutex_leave(p->db->mutex);
  }
  return rc;
}
int sqlitex_bind_null(sqlitex_stmt *pStmt, int i){
  int rc;
  Vdbe *p = (Vdbe*)pStmt;
  rc = vdbeUnbind(p, i);
  if( rc==SQLITE_OK ){
    sqlitex_mutex_leave(p->db->mutex);
  }
  return rc;
}
int sqlitex_bind_text( 
  sqlitex_stmt *pStmt, 
  int i, 
  const char *zData, 
  int nData, 
  void (*xDel)(void*)
){
  return bindText(pStmt, i, zData, nData, xDel, SQLITE_UTF8);
}
int sqlitex_bind_text64( 
  sqlitex_stmt *pStmt, 
  int i, 
  const char *zData, 
  sqlitex_uint64 nData, 
  void (*xDel)(void*),
  unsigned char enc
){
  assert( xDel!=SQLITE_DYNAMIC );
  if( nData>0x7fffffff ){
    return invokeValueDestructor(zData, xDel, 0);
  }else{
    if( enc==SQLITE_UTF16 ) enc = SQLITE_UTF16NATIVE;
    return bindText(pStmt, i, zData, (int)nData, xDel, enc);
  }
}
#ifndef SQLITE_OMIT_UTF16
int sqlitex_bind_text16(
  sqlitex_stmt *pStmt, 
  int i, 
  const void *zData, 
  int nData, 
  void (*xDel)(void*)
){
  return bindText(pStmt, i, zData, nData, xDel, SQLITE_UTF16NATIVE);
}
#endif /* SQLITE_OMIT_UTF16 */
int sqlitex_bind_value(sqlitex_stmt *pStmt, int i, const sqlitex_value *pValue){
  int rc;
  switch( sqlitex_value_type((sqlitex_value*)pValue) ){
    case SQLITE_INTEGER: {
      rc = sqlitex_bind_int64(pStmt, i, pValue->u.i);
      break;
    }
    case SQLITE_FLOAT: {
      rc = sqlitex_bind_double(pStmt, i, pValue->u.r);
      break;
    }
    case SQLITE_BLOB: {
      if( pValue->flags & MEM_Zero ){
        rc = sqlitex_bind_zeroblob(pStmt, i, pValue->u.nZero);
      }else{
        rc = sqlitex_bind_blob(pStmt, i, pValue->z, pValue->n,SQLITEX_TRANSIENT);
      }
      break;
    }
    case SQLITE3_TEXT: {
      rc = bindText(pStmt,i,  pValue->z, pValue->n, SQLITEX_TRANSIENT,
                              pValue->enc);
      break;
    }
    default: {
      rc = sqlitex_bind_null(pStmt, i);
      break;
    }
  }
  return rc;
}
int sqlitex_bind_zeroblob(sqlitex_stmt *pStmt, int i, int n){
  int rc;
  Vdbe *p = (Vdbe *)pStmt;
  rc = vdbeUnbind(p, i);
  if( rc==SQLITE_OK ){
    sqlitexVdbeMemSetZeroBlob(&p->aVar[i-1], n);
    sqlitex_mutex_leave(p->db->mutex);
  }
  return rc;
}

/*
** Return the number of wildcards that can be potentially bound to.
** This routine is added to support DBD::SQLite.  
*/
int sqlitex_bind_parameter_count(sqlitex_stmt *pStmt){
  Vdbe *p = (Vdbe*)pStmt;
  return p ? p->nVar : 0;
}

/*
** Return the name of a wildcard parameter.  Return NULL if the index
** is out of range or if the wildcard is unnamed.
**
** The result is always UTF-8.
*/
const char *sqlitex_bind_parameter_name(sqlitex_stmt *pStmt, int i){
  Vdbe *p = (Vdbe*)pStmt;
  if( p==0 || i<1 || i>p->nzVar ){
    return 0;
  }
  return p->azVar[i-1];
}

/*
** Given a wildcard parameter name, return the index of the variable
** with that name.  If there is no variable with the given name,
** return 0.
*/
int sqlitexVdbeParameterIndex(Vdbe *p, const char *zName, int nName){
  int i;
  if( p==0 ){
    return 0;
  }
  if( zName ){
    for(i=0; i<p->nzVar; i++){
      const char *z = p->azVar[i];
      if( z && strncmp(z,zName,nName)==0 && z[nName]==0 ){
        return i+1;
      }
    }
  }
  return 0;
}
int sqlitex_bind_parameter_index(sqlitex_stmt *pStmt, const char *zName){
  return sqlitexVdbeParameterIndex((Vdbe*)pStmt, zName, sqlitexStrlen30(zName));
}

/*
** Transfer all bindings from the first statement over to the second.
*/
int sqlitexTransferBindings(sqlitex_stmt *pFromStmt, sqlitex_stmt *pToStmt){
  Vdbe *pFrom = (Vdbe*)pFromStmt;
  Vdbe *pTo = (Vdbe*)pToStmt;
  int i;
  assert( pTo->db==pFrom->db );
  assert( pTo->nVar==pFrom->nVar );
  sqlitex_mutex_enter(pTo->db->mutex);
  for(i=0; i<pFrom->nVar; i++){
    sqlitexVdbeMemMove(&pTo->aVar[i], &pFrom->aVar[i]);
  }
  sqlitex_mutex_leave(pTo->db->mutex);
  return SQLITE_OK;
}

#ifndef SQLITE_OMIT_DEPRECATED
/*
** Deprecated external interface.  Internal/core SQLite code
** should call sqlitexTransferBindings.
**
** It is misuse to call this routine with statements from different
** database connections.  But as this is a deprecated interface, we
** will not bother to check for that condition.
**
** If the two statements contain a different number of bindings, then
** an SQLITE_ERROR is returned.  Nothing else can go wrong, so otherwise
** SQLITE_OK is returned.
*/
int sqlitex_transfer_bindings(sqlitex_stmt *pFromStmt, sqlitex_stmt *pToStmt){
  Vdbe *pFrom = (Vdbe*)pFromStmt;
  Vdbe *pTo = (Vdbe*)pToStmt;
  if( pFrom->nVar!=pTo->nVar ){
    return SQLITE_ERROR;
  }
  if( pTo->isPrepareV2 && pTo->expmask ){
    pTo->expired = 1;
  }
  if( pFrom->isPrepareV2 && pFrom->expmask ){
    pFrom->expired = 1;
  }
  return sqlitexTransferBindings(pFromStmt, pToStmt);
}
#endif

/*
** Return the sqlitex* database handle to which the prepared statement given
** in the argument belongs.  This is the same database handle that was
** the first argument to the sqlitex_prepare() that was used to create
** the statement in the first place.
*/
sqlitex *sqlitex_db_handle(sqlitex_stmt *pStmt){
  return pStmt ? ((Vdbe*)pStmt)->db : 0;
}

/*
** Return true if the prepared statement is guaranteed to not modify the
** database.
*/
int sqlitex_stmt_readonly(sqlitex_stmt *pStmt){
  return pStmt ? ((Vdbe*)pStmt)->readOnly : 1;
}

/*
** Return true if the prepared statement is in need of being reset.
*/
int sqlitex_stmt_busy(sqlitex_stmt *pStmt){
  Vdbe *v = (Vdbe*)pStmt;
  return v!=0 && v->pc>=0 && v->magic==VDBE_MAGIC_RUN;
}

/*
** Return a pointer to the next prepared statement after pStmt associated
** with database connection pDb.  If pStmt is NULL, return the first
** prepared statement for the database connection.  Return NULL if there
** are no more.
*/
sqlitex_stmt *sqlitex_next_stmt(sqlitex *pDb, sqlitex_stmt *pStmt){
  sqlitex_stmt *pNext;
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(pDb) ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
  sqlitex_mutex_enter(pDb->mutex);
  if( pStmt==0 ){
    pNext = (sqlitex_stmt*)pDb->pVdbe;
  }else{
    pNext = (sqlitex_stmt*)((Vdbe*)pStmt)->pNext;
  }
  sqlitex_mutex_leave(pDb->mutex);
  return pNext;
}

/*
** Return the value of a status counter for a prepared statement
*/
int sqlitex_stmt_status(sqlitex_stmt *pStmt, int op, int resetFlag){
  Vdbe *pVdbe = (Vdbe*)pStmt;
  u32 v;
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !pStmt ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
  v = pVdbe->aCounter[op];
  if( resetFlag ) pVdbe->aCounter[op] = 0;
  return (int)v;
}

#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
/*
** Return status data for a single loop within query pStmt.
*/
int sqlitex_stmt_scanstatus(
  sqlitex_stmt *pStmt,            /* Prepared statement being queried */
  int idx,                        /* Index of loop to report on */
  int iScanStatusOp,              /* Which metric to return */
  void *pOut                      /* OUT: Write the answer here */
){
  Vdbe *p = (Vdbe*)pStmt;
  ScanStatus *pScan;
  if( idx<0 || idx>=p->nScan ) return 1;
  pScan = &p->aScan[idx];
  switch( iScanStatusOp ){
    case SQLITE_SCANSTAT_NLOOP: {
      *(sqlitex_int64*)pOut = p->anExec[pScan->addrLoop];
      break;
    }
    case SQLITE_SCANSTAT_NVISIT: {
      *(sqlitex_int64*)pOut = p->anExec[pScan->addrVisit];
      break;
    }
    case SQLITE_SCANSTAT_EST: {
      double r = 1.0;
      LogEst x = pScan->nEst;
      while( x<100 ){
        x += 10;
        r *= 0.5;
      }
      *(double*)pOut = r*sqlitexLogEstToInt(x);
      break;
    }
    case SQLITE_SCANSTAT_NAME: {
      *(const char**)pOut = pScan->zName;
      break;
    }
    case SQLITE_SCANSTAT_EXPLAIN: {
      if( pScan->addrExplain ){
        *(const char**)pOut = p->aOp[ pScan->addrExplain ].p4.z;
      }else{
        *(const char**)pOut = 0;
      }
      break;
    }
    case SQLITE_SCANSTAT_SELECTID: {
      if( pScan->addrExplain ){
        *(int*)pOut = p->aOp[ pScan->addrExplain ].p1;
      }else{
        *(int*)pOut = -1;
      }
      break;
    }
    default: {
      return 1;
    }
  }
  return 0;
}

/*
** Zero all counters associated with the sqlitex_stmt_scanstatus() data.
*/
void sqlitex_stmt_scanstatus_reset(sqlitex_stmt *pStmt){
  Vdbe *p = (Vdbe*)pStmt;
  memset(p->anExec, 0, p->nOp * sizeof(i64));
}
#endif /* SQLITE_ENABLE_STMT_SCANSTATUS */

/* COMDB2 MODIFICATION */
int sqlitex_bind_datetime(
  sqlitex_stmt *pStmt,
  int i, dttz_t *dt, char *tz
){
  return bindDatetime(pStmt, i, dt, tz);
}

/* COMDB2 MODIFICATION */
int sqlitex_bind_interval(
  sqlitex_stmt *pStmt,
  int i, intv_t it
){
  return bindInterval(pStmt, i, it);
}

/* COMDB2 MODIFICATION */
int sqlitex_hasResultSet(
  sqlitex_stmt *pStmt
){
  Vdbe *pVm = (Vdbe *)pStmt;
  if( pVm && pVm->pResultSet!=0 ){
    return 1;
  }
  return 0;
}
