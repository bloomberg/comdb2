/*
** 2002 February 23
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains the C-language implementations for many of the SQL
** functions of SQLite.  (Some function, and in particular the date and
** time functions, are implemented separately.)
*/
#include "sqliteInt.h"
#include <stdlib.h>
#include <assert.h>
#ifndef SQLITE_OMIT_FLOATING_POINT
#include <math.h>
#endif
#include "vdbeInt.h"
#if defined(SQLITE_BUILDING_FOR_COMDB2)
#include <unistd.h>
#include <uuid/uuid.h>
#include <memcompare.c>
#include <zlib.h>
#include "comdb2.h"
#include "sql.h"
#include "bdb_int.h"
#include "md5.h"
#include "tohex.h"

#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */

/*
** Return the collating function associated with a function.
*/
static CollSeq *sqlite3GetFuncCollSeq(sqlite3_context *context){
  VdbeOp *pOp;
  assert( context->pVdbe!=0 );
  pOp = &context->pVdbe->aOp[context->iOp-1];
  assert( pOp->opcode==OP_CollSeq );
  assert( pOp->p4type==P4_COLLSEQ );
  return pOp->p4.pColl;
}

/*
** Indicate that the accumulator load should be skipped on this
** iteration of the aggregate loop.
*/
static void sqlite3SkipAccumulatorLoad(sqlite3_context *context){
  assert( context->isError<=0 );
  context->isError = -1;
  context->skipFlag = 1;
}

/*
** Implementation of the non-aggregate min() and max() functions
*/
static void minmaxFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int i;
  int mask;    /* 0 for min() or 0xffffffff for max() */
  int iBest;
  CollSeq *pColl;

  assert( argc>1 );
  mask = sqlite3_user_data(context)==0 ? 0 : -1;
  pColl = sqlite3GetFuncCollSeq(context);
  assert( pColl );
  assert( mask==-1 || mask==0 );
  iBest = 0;
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ) return;
  for(i=1; i<argc; i++){
    if( sqlite3_value_type(argv[i])==SQLITE_NULL ) return;
    if( (sqlite3MemCompare(argv[iBest], argv[i], pColl)^mask)>=0 ){
      testcase( mask==0 );
      iBest = i;
    }
  }
  sqlite3_result_value(context, argv[iBest]);
}

/*
** Return the type of the argument.
*/
static void typeofFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **argv
){
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  static const char *azType[] = {
    "integer", "real", "text", "blob", "null",
    "datetime", "interval_ym", "interval_ds",
    "datetime", "interval_ds", "decimal"
  };
#else /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  static const char *azType[] = { "integer", "real", "text", "blob", "null" };
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  int i = sqlite3_value_type(argv[0]) - 1;
  UNUSED_PARAMETER(NotUsed);
  assert( i>=0 && i<ArraySize(azType) );
  assert( SQLITE_INTEGER==1 );
  assert( SQLITE_FLOAT==2 );
  assert( SQLITE_TEXT==3 );
  assert( SQLITE_BLOB==4 );
  assert( SQLITE_NULL==5 );
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  assert( SQLITE_DATETIME==6 );
  assert( SQLITE_INTERVAL_YM==7 );
  assert( SQLITE_INTERVAL_DS==8 );
  assert( SQLITE_DATETIMEUS==9 );
  assert( SQLITE_INTERVAL_DSUS==10 );
  assert( SQLITE_DECIMAL==11 );
  assert( SQLITE_NEXTSEQ==(SQLITE_MAX_U32-2) );
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  /* EVIDENCE-OF: R-01470-60482 The sqlite3_value_type(V) interface returns
  ** the datatype code for the initial datatype of the sqlite3_value object
  ** V. The returned value is one of SQLITE_INTEGER, SQLITE_FLOAT,
  ** SQLITE_TEXT, SQLITE_BLOB, or SQLITE_NULL. */
  sqlite3_result_text(context, azType[i], -1, SQLITE_STATIC);
}

/* subtype(X)
**
** Return the subtype of X
*/
static void subtypeFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  UNUSED_PARAMETER(argc);
  sqlite3_result_int(context, sqlite3_value_subtype(argv[0]));
}

/*
** Implementation of the length() function
*/
static void lengthFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  switch( sqlite3_value_type(argv[0]) ){
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    case SQLITE_INTERVAL_YM:
    case SQLITE_INTERVAL_DS:
    case SQLITE_INTERVAL_DSUS:
    case SQLITE_DATETIME:
    case SQLITE_DATETIMEUS:
    case SQLITE_DECIMAL:
    case (int)SQLITE_NEXTSEQ:
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
    case SQLITE_BLOB:
    case SQLITE_INTEGER:
    case SQLITE_FLOAT: {
      sqlite3_result_int(context, sqlite3_value_bytes(argv[0]));
      break;
    }
    case SQLITE_TEXT: {
      const unsigned char *z = sqlite3_value_text(argv[0]);
      const unsigned char *z0;
      unsigned char c;
      if( z==0 ) return;
      z0 = z;
      while( (c = *z)!=0 ){
        z++;
        if( c>=0xc0 ){
          while( (*z & 0xc0)==0x80 ){ z++; z0++; }
        }
      }
      sqlite3_result_int(context, (int)(z-z0));
      break;
    }
    default: {
      sqlite3_result_null(context);
      break;
    }
  }
}

/*
** Implementation of the octet_length() function
*/
static void bytelengthFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  switch( sqlite3_value_type(argv[0]) ){
    case SQLITE_BLOB: {
      sqlite3_result_int(context, sqlite3_value_bytes(argv[0]));
      break;
    }
    case SQLITE_INTEGER:
    case SQLITE_FLOAT: {
      i64 m = sqlite3_context_db_handle(context)->enc<=SQLITE_UTF8 ? 1 : 2;
      sqlite3_result_int64(context, sqlite3_value_bytes(argv[0])*m);
      break;
    }
    case SQLITE_TEXT: {
      if( sqlite3_value_encoding(argv[0])<=SQLITE_UTF8 ){
        sqlite3_result_int(context, sqlite3_value_bytes(argv[0]));
      }else{
        sqlite3_result_int(context, sqlite3_value_bytes16(argv[0]));
      }
      break;
    }
    default: {
      sqlite3_result_null(context);
      break;
    }
  }
}

/*
** Implementation of the abs() function.
**
** IMP: R-23979-26855 The abs(X) function returns the absolute value of
** the numeric argument X.
*/
static void absFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  switch( sqlite3_value_type(argv[0]) ){
    case SQLITE_INTEGER: {
      i64 iVal = sqlite3_value_int64(argv[0]);
      if( iVal<0 ){
        if( iVal==SMALLEST_INT64 ){
          /* IMP: R-31676-45509 If X is the integer -9223372036854775808
          ** then abs(X) throws an integer overflow error since there is no
          ** equivalent positive 64-bit two complement value. */
          sqlite3_result_error(context, "integer overflow", -1);
          return;
        }
        iVal = -iVal;
      }
      sqlite3_result_int64(context, iVal);
      break;
    }
    case SQLITE_NULL: {
      /* IMP: R-37434-19929 Abs(X) returns NULL if X is NULL. */
      sqlite3_result_null(context);
      break;
    }
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    case SQLITE_DECIMAL: {
      decContext ctx;
      intv_t *val = (intv_t*)sqlite3_value_interval( argv[0], SQLITE_DECIMAL);
      intv_t res_val;

      dec_ctx_init(&ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);

      decQuadAbs( &res_val.u.dec, &val->u.dec, &ctx);
      if( dfp_conv_check_status( &ctx, "quad", "abs(quad)") ){
          sqlite3_result_error(context, "decimal overflow", -1);
          break;
      }

      res_val.type = INTV_DECIMAL_TYPE;
      res_val.sign = 0;

      sqlite3_result_interval(context, &res_val);
      break;
    }
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
    default: {
      /* Because sqlite3_value_double() returns 0.0 if the argument is not
      ** something that can be converted into a number, we have:
      ** IMP: R-01992-00519 Abs(X) returns 0.0 if X is a string or blob
      ** that cannot be converted to a numeric value.
      */
      double rVal = sqlite3_value_double(argv[0]);
      if( rVal<0 ) rVal = -rVal;
      sqlite3_result_double(context, rVal);
      break;
    }
  }
}

/*
** Implementation of the instr() function.
**
** instr(haystack,needle) finds the first occurrence of needle
** in haystack and returns the number of previous characters plus 1,
** or 0 if needle does not occur within haystack.
**
** If both haystack and needle are BLOBs, then the result is one more than
** the number of bytes in haystack prior to the first occurrence of needle,
** or 0 if needle never occurs in haystack.
*/
static void instrFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *zHaystack;
  const unsigned char *zNeedle;
  int nHaystack;
  int nNeedle;
  int typeHaystack, typeNeedle;
  int N = 1;
  int isText;
  unsigned char firstChar;
  sqlite3_value *pC1 = 0;
  sqlite3_value *pC2 = 0;

  UNUSED_PARAMETER(argc);
  typeHaystack = sqlite3_value_type(argv[0]);
  typeNeedle = sqlite3_value_type(argv[1]);
  if( typeHaystack==SQLITE_NULL || typeNeedle==SQLITE_NULL ) return;
  nHaystack = sqlite3_value_bytes(argv[0]);
  nNeedle = sqlite3_value_bytes(argv[1]);
  if( nNeedle>0 ){
    if( typeHaystack==SQLITE_BLOB && typeNeedle==SQLITE_BLOB ){
      zHaystack = sqlite3_value_blob(argv[0]);
      zNeedle = sqlite3_value_blob(argv[1]);
      isText = 0;
    }else if( typeHaystack!=SQLITE_BLOB && typeNeedle!=SQLITE_BLOB ){
      zHaystack = sqlite3_value_text(argv[0]);
      zNeedle = sqlite3_value_text(argv[1]);
      isText = 1;
    }else{
      pC1 = sqlite3_value_dup(argv[0]);
      zHaystack = sqlite3_value_text(pC1);
      if( zHaystack==0 ) goto endInstrOOM;
      nHaystack = sqlite3_value_bytes(pC1);
      pC2 = sqlite3_value_dup(argv[1]);
      zNeedle = sqlite3_value_text(pC2);
      if( zNeedle==0 ) goto endInstrOOM;
      nNeedle = sqlite3_value_bytes(pC2);
      isText = 1;
    }
    if( zNeedle==0 || (nHaystack && zHaystack==0) ) goto endInstrOOM;
    firstChar = zNeedle[0];
    while( nNeedle<=nHaystack
       && (zHaystack[0]!=firstChar || memcmp(zHaystack, zNeedle, nNeedle)!=0)
    ){
      N++;
      do{
        nHaystack--;
        zHaystack++;
      }while( isText && (zHaystack[0]&0xc0)==0x80 );
    }
    if( nNeedle>nHaystack ) N = 0;
  }
  sqlite3_result_int(context, N);
endInstr:
  sqlite3_value_free(pC1);
  sqlite3_value_free(pC2);
  return;
endInstrOOM:
  sqlite3_result_error_nomem(context);
  goto endInstr;
}

/*
** Implementation of the printf() (a.k.a. format()) SQL function.
*/
static void printfFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  PrintfArguments x;
  StrAccum str;
  const char *zFormat;
  int n;
  sqlite3 *db = sqlite3_context_db_handle(context);

  if( argc>=1 && (zFormat = (const char*)sqlite3_value_text(argv[0]))!=0 ){
    x.nArg = argc-1;
    x.nUsed = 0;
    x.apArg = argv+1;
    sqlite3StrAccumInit(&str, db, 0, 0, db->aLimit[SQLITE_LIMIT_LENGTH]);
    str.printfFlags = SQLITE_PRINTF_SQLFUNC;
    sqlite3_str_appendf(&str, zFormat, &x);
    n = str.nChar;
    sqlite3_result_text(context, sqlite3StrAccumFinish(&str), n,
                        SQLITE_DYNAMIC);
  }
}

/*
** Implementation of the substr() function.
**
** substr(x,p1,p2)  returns p2 characters of x[] beginning with p1.
** p1 is 1-indexed.  So substr(x,1,1) returns the first character
** of x.  If x is text, then we actually count UTF-8 characters.
** If x is a blob, then we count bytes.
**
** If p1 is negative, then we begin abs(p1) from the end of x[].
**
** If p2 is negative, return the p2 characters preceding p1.
*/
static void substrFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *z;
  const unsigned char *z2;
  int len;
  int p0type;
  i64 p1, p2;

  assert( argc==3 || argc==2 );
  p0type = sqlite3_value_type(argv[0]);
  p1 = sqlite3_value_int64(argv[1]);
  if( p0type==SQLITE_BLOB ){
    len = sqlite3_value_bytes(argv[0]);
    z = sqlite3_value_blob(argv[0]);
    if( z==0 ) return;
    assert( len==sqlite3_value_bytes(argv[0]) );
  }else{
    z = sqlite3_value_text(argv[0]);
    if( z==0 ) return;
    len = 0;
    if( p1<0 ){
      for(z2=z; *z2; len++){
        SQLITE_SKIP_UTF8(z2);
      }
    }
  }
  if( argc==3 ){
    p2 = sqlite3_value_int64(argv[2]);
    if( p2==0 && sqlite3_value_type(argv[2])==SQLITE_NULL ) return;
  }else{
    p2 = sqlite3_context_db_handle(context)->aLimit[SQLITE_LIMIT_LENGTH];
  }
  if( p1==0 ){
#ifdef SQLITE_SUBSTR_COMPATIBILITY
    /* If SUBSTR_COMPATIBILITY is defined then substr(X,0,N) work the same as
    ** as substr(X,1,N) - it returns the first N characters of X.  This
    ** is essentially a back-out of the bug-fix in check-in [5fc125d362df4b8]
    ** from 2009-02-02 for compatibility of applications that exploited the
    ** old buggy behavior. */
    p1 = 1; /* <rdar://problem/6778339> */
#endif
    if( sqlite3_value_type(argv[1])==SQLITE_NULL ) return;
  }
  if( p1<0 ){
    p1 += len;
    if( p1<0 ){
      if( p2<0 ){
        p2 = 0;
      }else{
        p2 += p1;
      }
      p1 = 0;
    }
  }else if( p1>0 ){
    p1--;
  }else if( p2>0 ){
    p2--;
  }
  if( p2<0 ){
    if( p2<-p1 ){
      p2 = p1;
    }else{
      p2 = -p2;
    }
    p1 -= p2;
  }
  assert( p1>=0 && p2>=0 );
  if( p0type!=SQLITE_BLOB ){
    while( *z && p1 ){
      SQLITE_SKIP_UTF8(z);
      p1--;
    }
    for(z2=z; *z2 && p2; p2--){
      SQLITE_SKIP_UTF8(z2);
    }
    sqlite3_result_text64(context, (char*)z, z2-z, SQLITE_TRANSIENT,
                          SQLITE_UTF8);
  }else{
    if( p1>=len ){
      p1 = p2 = 0;
    }else if( p2>len-p1 ){
      p2 = len-p1;
      assert( p2>0 );
    }
    sqlite3_result_blob64(context, (char*)&z[p1], (u64)p2, SQLITE_TRANSIENT);
  }
}

#if defined(SQLITE_BUILDING_FOR_COMDB2)
extern int comdb2_sql_tick();
/*
** Implementation of the sleep() function
*/
static void sleepFunc(sqlite3_context *context, int argc, sqlite3_value *argv[]) {
  int n;
  if( argc != 1 ){
    sqlite3_result_int(context, -1);
    return;
  }
  n = sqlite3_value_int(argv[0]);
  if( n < 0 ){
    sqlite3_result_int(context, -1);
    return;
  }
  int i;
  for(i = 0; i < n; i++) {
    sleep(1);
    if( comdb2_sql_tick() ){
      sqlite3_result_error_code(context, SQLITE_ERROR);
      return;
    }
  }
  sqlite3_result_int(context, i);
}

static void usleepFunc(sqlite3_context *context, int argc, sqlite3_value *argv[]) {
  int total, remain, us;
  if( argc != 1 ){
    sqlite3_result_int(context, -1);
    return;
  }
  total = remain = sqlite3_value_int(argv[0]);
  if( total < 0 ){
    sqlite3_result_int(context, -1);
    return;
  }
  while( remain > 0 ){
    us = ( remain > 1000000 ) ? 1000000 : remain;
    remain -= us;
    usleep(us);
    if( us == 1000000 && comdb2_sql_tick() ){
      sqlite3_result_error_code(context, SQLITE_ERROR);
      return;
    }
  }
  sqlite3_result_int(context, (total - remain));
}

static void tableNamesFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db;
  int wasPrepareOnly;
  Parse sParse;
  const char *zSql;
  if( sqlite3_value_type(argv[0])!=SQLITE_TEXT ) return;
  zSql = (const char *)sqlite3_value_text(argv[0]);
  if( !zSql ) return;
  db = sqlite3_context_db_handle(context);
  wasPrepareOnly = (db->flags&SQLITE_PREPARE_ONLY)!=0;
  db->flags |= SQLITE_PrepareOnly;
  sqlite3ParseObjectInit(&sParse, db);
  sParse.prepFlags = SQLITE_PREPARE_ONLY|SQLITE_PREPARE_SRCLIST_ONLY;
  if( sqlite3RunParser(&sParse, zSql) ){
    if( sParse.zErrMsg ){
      sqlite3_result_error(context, sParse.zErrMsg, -1);
    }
    goto done;
  }
  if( sParse.nSrcListOnly>0 ){
    StrAccum str;
    int i;
    sqlite3StrAccumInit(&str, db, 0, 0, db->aLimit[SQLITE_LIMIT_LENGTH]);
    for(i=0; i<sParse.nSrcListOnly; i++){
      if( i>0 ) sqlite3_str_append(&str, " ", 1);
      sqlite3_str_appendall(&str, sParse.azSrcListOnly[i]);
    }
    sqlite3_result_text(context, sqlite3StrAccumFinish(&str), -1, SQLITE_DYNAMIC);
  }
done:
  if( !wasPrepareOnly ) db->flags &= ~SQLITE_PrepareOnly;
  if( sParse.pVdbe ){
    sqlite3VdbeFinalize(sParse.pVdbe);
    sParse.pVdbe = 0;
  }
  sqlite3DbFree(db, sParse.zErrMsg);
  sqlite3ParseObjectReset(&sParse);
}

static void normalizeSqlFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int rc;
  sqlite3 *db;
  const char *zSql;
  sqlite3_stmt *pStmt = NULL;
  if( sqlite3_value_type(argv[0])!=SQLITE_TEXT ) return;
  zSql = (const char *)sqlite3_value_text(argv[0]);
  if( !zSql ) return;
  db = sqlite3_context_db_handle(context);
  rc = sqlite3_prepare_v3(db, zSql, -1, SQLITE_PREPARE_NORMALIZE |
                          SQLITE_PREPARE_ONLY, &pStmt, NULL);
  if( rc==SQLITE_OK ){
    sqlite3_result_text(context, sqlite3_normalized_sql(pStmt),
                        -1, SQLITE_TRANSIENT);
    sqlite3_finalize(pStmt);
  }else{
    sqlite3_result_error(context, sqlite3_errmsg(db), -1);
  }
}
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */

/*
** Implementation of the round() function
*/
#ifndef SQLITE_OMIT_FLOATING_POINT
static void roundFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
  i64 n = 0;
  double r;
  char *zBuf;
  assert( argc==1 || argc==2 );
  if( argc==2 ){
    if( SQLITE_NULL==sqlite3_value_type(argv[1]) ) return;
    n = sqlite3_value_int64(argv[1]);
    if( n>30 ) n = 30;
    if( n<0 ) n = 0;
  }
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ) return;
  r = sqlite3_value_double(argv[0]);
  /* If Y==0 and X will fit in a 64-bit int,
  ** handle the rounding directly,
  ** otherwise use printf.
  */
  if( r<-4503599627370496.0 || r>+4503599627370496.0 ){
    /* The value has no fractional part so there is nothing to round */
  }else if( n==0 ){ 
    r = (double)((sqlite_int64)(r+(r<0?-0.5:+0.5)));
  }else{
    zBuf = sqlite3_mprintf("%!.*f",(int)n,r);
    if( zBuf==0 ){
      sqlite3_result_error_nomem(context);
      return;
    }
    sqlite3AtoF(zBuf, &r, sqlite3Strlen30(zBuf), SQLITE_UTF8);
    sqlite3_free(zBuf);
  }
  sqlite3_result_double(context, r);
}
#endif

/*
** Allocate nByte bytes of space using sqlite3Malloc(). If the
** allocation fails, call sqlite3_result_error_nomem() to notify
** the database handle that malloc() has failed and return NULL.
** If nByte is larger than the maximum string or blob length, then
** raise an SQLITE_TOOBIG exception and return NULL.
*/
static void *contextMalloc(sqlite3_context *context, i64 nByte){
  char *z;
  sqlite3 *db = sqlite3_context_db_handle(context);
  assert( nByte>0 );
  testcase( nByte==db->aLimit[SQLITE_LIMIT_LENGTH] );
  testcase( nByte==(i64)db->aLimit[SQLITE_LIMIT_LENGTH]+1 );
  if( nByte>db->aLimit[SQLITE_LIMIT_LENGTH] ){
    sqlite3_result_error_toobig(context);
    z = 0;
  }else{
    z = sqlite3Malloc(nByte);
    if( !z ){
      sqlite3_result_error_nomem(context);
    }
  }
  return z;
}

/*
** Implementation of the upper() and lower() SQL functions.
*/
static void upperFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
  char *z1;
  const char *z2;
  int i, n;
  UNUSED_PARAMETER(argc);
  z2 = (char*)sqlite3_value_text(argv[0]);
  n = sqlite3_value_bytes(argv[0]);
  /* Verify that the call to _bytes() does not invalidate the _text() pointer */
  assert( z2==(char*)sqlite3_value_text(argv[0]) );
  if( z2 ){
    z1 = contextMalloc(context, ((i64)n)+1);
    if( z1 ){
      for(i=0; i<n; i++){
        z1[i] = (char)sqlite3Toupper(z2[i]);
      }
      sqlite3_result_text(context, z1, n, sqlite3_free);
    }
  }
}
static void lowerFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
  char *z1;
  const char *z2;
  int i, n;
  UNUSED_PARAMETER(argc);
  z2 = (char*)sqlite3_value_text(argv[0]);
  n = sqlite3_value_bytes(argv[0]);
  /* Verify that the call to _bytes() does not invalidate the _text() pointer */
  assert( z2==(char*)sqlite3_value_text(argv[0]) );
  if( z2 ){
    z1 = contextMalloc(context, ((i64)n)+1);
    if( z1 ){
      for(i=0; i<n; i++){
        z1[i] = sqlite3Tolower(z2[i]);
      }
      sqlite3_result_text(context, z1, n, sqlite3_free);
    }
  }
}

/*
** Some functions like COALESCE() and IFNULL() and UNLIKELY() are implemented
** as VDBE code so that unused argument values do not have to be computed.
** However, we still need some kind of function implementation for this
** routines in the function table.  The noopFunc macro provides this.
** noopFunc will never be called so it doesn't matter what the implementation
** is.  We might as well use the "version()" function as a substitute.
*/
#define noopFunc versionFunc   /* Substitute function - never called */

/*
** Implementation of random().  Return a random integer. 
*/
static void randomFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  sqlite_int64 r;
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  sqlite3_randomness(sizeof(r), &r);
  if( r<0 ){
    /* We need to prevent a random number of 0x8000000000000000
    ** (or -9223372036854775808) since when you do abs() of that
    ** number of you get the same value back again.  To do this
    ** in a way that is testable, mask the sign bit off of negative
    ** values, resulting in a positive value.  Then take the
    ** 2s complement of that positive value.  The end result can
    ** therefore be no less than -9223372036854775807.
    */
    r = -(r & LARGEST_INT64);
  }
  sqlite3_result_int64(context, r);
}

/*
** Implementation of randomblob(N).  Return a random blob
** that is N bytes long.
*/
static void randomBlob(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3_int64 n;
  unsigned char *p;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  n = sqlite3_value_int64(argv[0]);
  if( n<1 ){
    n = 1;
  }
  p = contextMalloc(context, n);
  if( p ){
    sqlite3_randomness(n, p);
    sqlite3_result_blob(context, (char*)p, n, sqlite3_free);
  }
}

#if defined(SQLITE_BUILDING_FOR_COMDB2)
/* 36 characters + trailing '\0' - taken from `man uuid_unparse` */
#define GUID_STR_LENGTH 37

static void guidFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert( argc==0 );
  UNUSED_PARAMETER(argc);

  uuid_t guid;
  uuid_generate(guid);

  sqlite3_result_blob(context, (char*)guid, sizeof(uuid_t), SQLITE_TRANSIENT);
}

static void guidStrFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert( argc==0 );
  UNUSED_PARAMETER(argc);

  uuid_t guid;
  uuid_generate(guid);

  char guid_str[GUID_STR_LENGTH];
  uuid_unparse(guid, guid_str);

  sqlite3_result_text(context, guid_str, GUID_STR_LENGTH - 1, SQLITE_TRANSIENT);
}

static void guidFromStrFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert( argc==1 );
  UNUSED_PARAMETER(argc);

  if(sqlite3_value_type(argv[0]) != SQLITE_TEXT) {
      return;
  }

  /* be liberal in what we accept - uuid_parse expects a string
   * in the format of "%08x-%04x-%04x-%04x-%012x", 36 bytes plus the trailing '\0')
   * but will fail later if we don't have that
   */
  if(sqlite3_value_bytes(argv[0]) < (GUID_STR_LENGTH - 1)) {
      return;
  }

  const unsigned char * guid_str = sqlite3_value_text(argv[0]);

  uuid_t guid;
  if(uuid_parse((const char*)guid_str, guid) == 0) {
      sqlite3_result_blob(context, (char*)guid, sizeof(uuid_t), SQLITE_TRANSIENT);
  }
}

static void guidFromByteFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert( argc==1 );
  UNUSED_PARAMETER(argc);

  if(sqlite3_value_type(argv[0]) != SQLITE_BLOB) {
      return;
  }

  if(sqlite3_value_bytes(argv[0]) != sizeof(uuid_t)) {
      return;
  }

  const void * guid_blob = sqlite3_value_blob(argv[0]);

  char guid_str[GUID_STR_LENGTH];
  uuid_unparse(guid_blob, guid_str);

  sqlite3_result_text(context, guid_str, GUID_STR_LENGTH - 1, SQLITE_TRANSIENT);
}

static void comdb2DoubleToBlobFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  u8 aByte[8];
  assert( argc==1 );
  if( sqlite3_value_type(argv[0])==SQLITE_INTEGER ){
    sqlite3Put8byte(aByte, (u64)sqlite3_value_int64(argv[0]));
  }else if( sqlite3_value_type(argv[0])==SQLITE_FLOAT ){
    sqlite3Put8byte(aByte, (u64)sqlite3DoubleToInt64(
                    sqlite3_value_double(argv[0])));
  }else{
    sqlite3_result_null(context);
    return;
  }
  sqlite3_result_blob(context, (char*)aByte, sizeof(aByte), SQLITE_TRANSIENT);
}

static void comdb2BlobToDoubleFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert( argc==1 );
  if( sqlite3_value_type(argv[0])!=SQLITE_BLOB ){
    sqlite3_result_null(context);
    return;
  }
  if( sqlite3_value_bytes(argv[0])!=8 ){
    sqlite3_result_null(context);
    return;
  }
  sqlite3_result_double(context, sqlite3Int64ToDouble(
                        (i64)sqlite3Get8byte(sqlite3_value_blob(argv[0]))));
}

/*
** Implementation of the comdb2_sysinfo() SQL function.  The return
** value depends on the class of system information being requested;
** however, it is generally pieces of rarely changing (scalar) data
** related to the overall state of the server -OR- the current SQL
** query being processed.
*/
static void comdb2SysinfoFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const char *zName;
  assert( argc==1 );
  if( sqlite3_value_type(argv[0])!=SQLITE_TEXT ){
    return;
  }
  zName = (const char *)sqlite3_value_text(argv[0]);
  if( sqlite3_stricmp(zName, "pid")==0 ){
    sqlite3_result_int64(context, (sqlite3_int64)getpid());
  }else if( sqlite3_stricmp(zName, "tid")==0 ){
    sqlite3_result_int64(context, (sqlite3_int64)getarchtid());
  }else if( sqlite3_stricmp(zName, "master")==0 ){
    sqlite3_result_text(context, thedb->bdb_env->repinfo->master_host, -1,
                        SQLITE_TRANSIENT);
  }else if( sqlite3_stricmp(zName, "host")==0 ){
    char zHostName[1024];
    memset(zHostName, 0, sizeof(zHostName));
    if( gethostname(zHostName, sizeof(zHostName))==0 ){
      sqlite3_result_text(context, zHostName, -1, SQLITE_TRANSIENT);
    }else{
      sqlite3_result_error(context, "unable to obtain host name", -1);
    }
  }else if( sqlite3_stricmp(zName, "class")==0 ){
    sqlite3_result_text(context, get_my_mach_class_str(), -1, SQLITE_TRANSIENT);
  }else if( sqlite3_stricmp(zName, "version")==0 ){
    char *zVersion = sqlite3_mprintf("[%s] [%s] [%s] [%s] [%s]", gbl_db_version,
                                     gbl_db_codename, gbl_db_semver,
                                     gbl_db_git_version_sha, gbl_db_buildtype);
    sqlite3_result_text(context, zVersion, -1, SQLITE_TRANSIENT);
    sqlite3_free(zVersion);
  }
}

/*
** Implementation of the comdb2_ctxinfo() SQL function.  The return
** value depends on the class of context information being requested;
** however, it is generally pieces of (scalar) data related to the
** state of the client connection -OR- the current SQL query being
** processed.
*/
static void comdb2CtxinfoFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const char *zName;
  assert( argc==1 );
  if( sqlite3_value_type(argv[0])!=SQLITE_TEXT ){
    return;
  }

  struct sqlclntstate *clnt = get_sql_clnt();

  zName = (const char *)sqlite3_value_text(argv[0]);
  if( sqlite3_stricmp(zName, "parallel")==0 ){
    if( clnt ){
      sqlite3_result_int(context, clnt->conns!=NULL);
    }
  }else if( sqlite3_stricmp(zName, "ruleset_result")==0 ){
    if( clnt ){
      sqlite3_result_text(context, clnt->work.zRuleRes, -1, SQLITE_STATIC);
    }
  }else if( sqlite3_stricmp(zName, "user")==0 ){
    sqlite3_result_text(context, get_current_user(clnt), -1, SQLITE_STATIC);
  }
}

/*
** Implementation of the comdb2_version() SQL function.  The return
** value is the same as the stat value for version
*/
static void comdb2VersionFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  sqlite3_result_text(context, gbl_db_version, -1, SQLITE_STATIC);
}

static void comdb2SemVerFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  sqlite3_result_text(context, gbl_db_semver, -1, SQLITE_STATIC);
}

extern char * comdb2_get_prev_query_cost();
extern char * comdb2_free_prev_query_cost();

static void comdb2PrevquerycostFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  char * cost = comdb2_get_prev_query_cost();
  if(cost) {
      sqlite3_result_text(context, cost, -1, SQLITE_TRANSIENT);
      comdb2_free_prev_query_cost();
  }
}

static void comdb2HostFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  sqlite3_result_text(context, gbl_myhostname, -1, SQLITE_STATIC);
}

extern int comdb2_get_server_port();
static void comdb2PortFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  sqlite3_result_int64(context, comdb2_get_server_port());
}


static void comdb2DbnameFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  extern char gbl_dbname[];
  sqlite3_result_text(context, gbl_dbname, -1, SQLITE_STATIC);
}

extern unsigned long long comdb2_table_version(const char *tablename);
/*
** Implementation of the table_version() SQL function.  This returns
** the comdb2 table version
*/
static void tableVersionFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const char *tablename;
  sqlite_int64 version;

  assert(argc==1);
  if(sqlite3_value_type(argv[0]) != SQLITE_TEXT) {
    return;
  }

  tablename = (const char *)sqlite3_value_text(argv[0]);

  version = comdb2_table_version(tablename);
  if (version<0) {
    return;
  }

  sqlite3_result_int64(context, version);
}

extern char* comdb2_partition_info(const char *partition, const char *option);
/*
** Implementation of the table_version() SQL function.  This returns
** the comdb2 table version
*/
static void partitionInfoFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const char   *partition_name;
  const char   *option;
  char         *info;

  assert( argc==2 );
  if( sqlite3_value_type(argv[0]) != SQLITE_TEXT ){
    return;
  }
  partition_name = (const char *)sqlite3_value_text(argv[0]);
  
  if( sqlite3_value_type(argv[1]) != SQLITE_TEXT ){
    return;
  }
  option = (const char *)sqlite3_value_text(argv[1]);

  info = comdb2_partition_info(partition_name, option);

  if( !info ){

    sqlite3_result_null(context);
  }else{
    sqlite3_result_text(context, info, -1, SQLITE_TRANSIENT);

    if(info) {
      free(info);
    }
  }
}

/*
** Implementation of the comdb2_starttime() SQL function.
*/
static void comdb2StartTimeFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  extern int gbl_starttime;
  dttz_t dt = {gbl_starttime, 0};
  sqlite3_result_datetime(context, &dt, NULL);
}

static void md5Func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
  ){
    const unsigned char *s;
    MD5Context ctx = {0};
    char checksum[16];
    char out[33];
    unsigned int len;
    int type = sqlite3_value_type(argv[0]);

    switch (type) {
        case SQLITE_TEXT:
            s = sqlite3_value_text(argv[0]);
            len = strlen((char*) s);
            break;

        case SQLITE_BLOB:
            s = sqlite3_value_blob(argv[0]);
            len = sqlite3_value_bytes(argv[0]);
            break;

        default:
            return;
    }

    MD5Init(&ctx);
    MD5Update(&ctx, (unsigned char*) s, len);
    MD5Final((unsigned char*) checksum, &ctx);
    util_tohex(out, (char*) checksum, sizeof(checksum));
    sqlite3_result_text(context, strdup(out), 32, free);
}

static void comdb2UserFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);

  sqlite3_result_text(context, get_current_user(get_sql_clnt()), -1,
                      SQLITE_STATIC);
}

int gbl_use_current_lsn_for_non_snapshot = 0;

static void comdb2SnapshotLsnFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  struct sqlclntstate *clnt = get_sql_clnt();
  char lsn[MAX_LSN_STR];
  int sz = 0;
  if (clnt != NULL && clnt->dbtran.mode == TRANLEVEL_SNAPISOL) {
    sz = snprintf(lsn, sizeof(lsn), "{%d:%d}",
             clnt->modsnap_start_lsn_file,
             clnt->modsnap_start_lsn_offset);
  } else if (gbl_use_current_lsn_for_non_snapshot || (clnt != NULL && clnt->use_current_lsn_for_snapshot)) {
    DB_LSN curlsn;
    __log_txn_lsn(thedb->bdb_env->dbenv, &curlsn, NULL, NULL);
    sz = snprintf(lsn, sizeof(lsn), "{%d:%d}", curlsn.file, curlsn.offset);
  } else {
    sqlite3_result_null(context);
    return;
  }

  assert(sz < sizeof(lsn));
  (void)sz; // suppress unused variable warning
  sqlite3_result_text(context, lsn, -1, SQLITE_TRANSIENT);
}

/* The code below is based on the example presented here: https://www.zlib.net/zpipe.c */

#define CHUNK 32768
static unsigned char *compressFunc_int(sqlite3_context *context,
                                       unsigned char *uncomprData,
                                       unsigned int   uncomprLen,
                                       unsigned int  *comprLen,
                                       int(*deflateInitFunc)(z_stream*))  {
    unsigned char buffer[CHUNK];     // Buffer to stage compressed data
    unsigned char *comprData = NULL; // Final buffer (realloc-ed when needed)
    int rc;
    z_stream stream;

    *comprLen = 0;

    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;

    rc = deflateInitFunc(&stream);
    if (rc != Z_OK) {
        sqlite3_result_error(context, "zlib: failed to initialize", -1);
        return NULL;
    }

    stream.avail_in = uncomprLen;
    stream.next_in = uncomprData;

    do {
        stream.avail_out = CHUNK;
        stream.next_out = buffer;

        rc = deflate(&stream, Z_FINISH);
        if (rc == Z_STREAM_ERROR) {
            sqlite3_result_error(context, "zlib: stream error", -1);
            deflateEnd(&stream);
            return NULL;
        }

        unsigned int compr_bytes = CHUNK - stream.avail_out;
        comprData = (comprData == NULL)
            ? (unsigned char *)contextMalloc(context, compr_bytes)
            : (unsigned char *)sqlite3_realloc(comprData, *comprLen + compr_bytes);
        memcpy(comprData + *comprLen, buffer, compr_bytes);
        *comprLen += compr_bytes;
    } while (stream.avail_out == 0);

    deflateEnd(&stream);
    return comprData;
}

static unsigned char *uncompressFunc_int(sqlite3_context *context,
                                         unsigned char *comprData,
                                         unsigned int   comprLen,
                                         unsigned int  *uncomprLen,
                                         int(*inflateInitFunc)(z_stream *)) {
    unsigned char buffer[CHUNK];       // Buffer to stage uncompressed data
    unsigned char *uncomprData = NULL; // Final buffer (realloc-ed when needed)
    int rc;
    z_stream stream;

    *uncomprLen = 0;

    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;

    rc = inflateInitFunc(&stream);
    if (rc != Z_OK) {
        sqlite3_result_error(context, "zlib: failed to initialize", -1);
        return NULL;
    }

    stream.avail_in = comprLen;
    stream.next_in = comprData;

    do {
        stream.avail_out = CHUNK;
        stream.next_out = buffer;

        rc = inflate(&stream, Z_NO_FLUSH);
        if (rc < 0) {
            sqlite3_result_error(context, "zlib: internal error", -1);
            inflateEnd(&stream);
            return NULL;
        }

        unsigned int uncompr_bytes = CHUNK - stream.avail_out;
        uncomprData = (uncomprData == NULL)
            ? (unsigned char *)contextMalloc(context, uncompr_bytes)
            : (unsigned char *)sqlite3_realloc(uncomprData, *uncomprLen + uncompr_bytes);
        memcpy(uncomprData + *uncomprLen, buffer, uncompr_bytes);
        *uncomprLen += uncompr_bytes;
    } while (stream.avail_out == 0);

    inflateEnd(&stream);
    return uncomprData;
}

static int deflateInit__(z_stream *stream) {
    return deflateInit(stream, Z_DEFAULT_COMPRESSION);
}

static int inflateInit__(z_stream *stream) {
    return inflateInit(stream);
}

static int deflateInit2__(z_stream *stream) {
    return deflateInit2(stream, Z_DEFAULT_COMPRESSION,
                        Z_DEFLATED, 15 | 16 /* Add gzip header & trailer */, 8,
                        Z_DEFAULT_STRATEGY);
}

static int inflateInit2__(z_stream *stream) {
    return inflateInit2(stream, 15 | 16);
}

static void compressFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
  ){
    unsigned char *uncomprData;
    unsigned int uncomprLen;

    int type = sqlite3_value_type(argv[0]);

    switch (type) {
        case SQLITE_TEXT:
            uncomprData = (unsigned char *)sqlite3_value_text(argv[0]);
            uncomprLen = strlen((char*) uncomprData);
            break;

        case SQLITE_BLOB:
            uncomprData = (unsigned char *)sqlite3_value_blob(argv[0]);
            uncomprLen = sqlite3_value_bytes(argv[0]);
            break;

        case SQLITE_NULL:
            sqlite3_result_null(context);
            return;

        default:
            sqlite3_result_error(context, "zlib: data type not supported", -1);
            return;
    }

    unsigned char *comprData = NULL;
    unsigned int comprLen = 0;
    comprData = compressFunc_int(context, uncomprData, uncomprLen, &comprLen,
                                 deflateInit__);

    if (comprData != NULL) {
        sqlite3_result_blob(context, comprData, comprLen, NULL);
    }
    return;
}

static void uncompressFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
  ){
    unsigned char *comprData;
    unsigned int comprLen;

    int type = sqlite3_value_type(argv[0]);

    switch (type) {
        case SQLITE_BLOB:
            comprData = (unsigned char *)sqlite3_value_blob(argv[0]);
            comprLen = sqlite3_value_bytes(argv[0]);
            break;

        case SQLITE_NULL:
            sqlite3_result_null(context);
            return;

        default:
            sqlite3_result_error(context, "zlib: data type not supported", -1);
            return;
    }

    unsigned char *uncomprData = NULL;
    unsigned int uncomprLen = 0;
    uncomprData = uncompressFunc_int(context, comprData, comprLen,
                                     &uncomprLen, inflateInit__);

    if (uncomprData != NULL) {
        sqlite3_result_blob(context, uncomprData, uncomprLen, NULL);
    }
    return;
}

static void compressGzipFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
  ){
    unsigned char *uncomprData;
    unsigned int uncomprLen;

    int type = sqlite3_value_type(argv[0]);

    switch (type) {
        case SQLITE_TEXT:
            uncomprData = (unsigned char *)sqlite3_value_text(argv[0]);
            uncomprLen = strlen((char*) uncomprData);
            break;

        case SQLITE_BLOB:
            uncomprData = (unsigned char *)sqlite3_value_blob(argv[0]);
            uncomprLen = sqlite3_value_bytes(argv[0]);
            break;

        case SQLITE_NULL:
            sqlite3_result_null(context);
            return;

        default:
            sqlite3_result_error(context, "zlib: data type not supported", -1);
            return;
    }

    unsigned char *comprData = NULL;
    unsigned int comprLen = 0;
    comprData = compressFunc_int(context, uncomprData, uncomprLen, &comprLen,
                                 deflateInit2__);

    if (comprData != NULL) {
        sqlite3_result_blob(context, comprData, comprLen, sqlite3_free);
    }
    return;
}

static void uncompressGzipFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
  ){
    unsigned char *comprData;
    unsigned int comprLen;

    int type = sqlite3_value_type(argv[0]);

    switch (type) {
        case SQLITE_BLOB:
            comprData = (unsigned char *)sqlite3_value_blob(argv[0]);
            comprLen = sqlite3_value_bytes(argv[0]);
            break;

        case SQLITE_NULL:
            sqlite3_result_null(context);
            return;

        default:
            sqlite3_result_error(context, "zlib: data type not supported", -1);
            return;
    }

    unsigned char *uncomprData = NULL;
    unsigned int uncomprLen = 0;
    uncomprData = uncompressFunc_int(context, comprData, comprLen,
                                     &uncomprLen, inflateInit2__);

    if (uncomprData != NULL) {
        sqlite3_result_blob(context, uncomprData, uncomprLen, sqlite3_free);
    }
    return;
}

#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */

/*
** Implementation of the last_insert_rowid() SQL function.  The return
** value is the same as the sqlite3_last_insert_rowid() API function.
*/
static void last_insert_rowid(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  /* IMP: R-51513-12026 The last_insert_rowid() SQL function is a
  ** wrapper around the sqlite3_last_insert_rowid() C/C++ interface
  ** function. */
  sqlite3_result_int64(context, sqlite3_last_insert_rowid(db));
}

/*
** Implementation of the changes() SQL function.
**
** IMP: R-32760-32347 The changes() SQL function is a wrapper
** around the sqlite3_changes64() C/C++ function and hence follows the
** same rules for counting changes.
*/
static void changes(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  sqlite3_result_int64(context, sqlite3_changes64(db));
}

/*
** Implementation of the total_changes() SQL function.  The return value is
** the same as the sqlite3_total_changes64() API function.
*/
static void total_changes(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  /* IMP: R-11217-42568 This function is a wrapper around the
  ** sqlite3_total_changes64() C/C++ interface. */
  sqlite3_result_int64(context, sqlite3_total_changes64(db));
}

#if defined(SQLITE_BUILDING_FOR_COMDB2)
/*
** Moved to "sqliteInt.h" for use by Comdb2.
*/
#else /* defined(SQLITE_BUILDING_FOR_COMDB2) */
/*
** A structure defining how to do GLOB-style comparisons.
*/
struct compareInfo {
  u8 matchAll;          /* "*" or "%" */
  u8 matchOne;          /* "?" or "_" */
  u8 matchSet;          /* "[" or 0 */
  u8 noCase;            /* true to ignore case differences */
};
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */

/*
** For LIKE and GLOB matching on EBCDIC machines, assume that every
** character is exactly one byte in size.  Also, provide the Utf8Read()
** macro for fast reading of the next character in the common case where
** the next character is ASCII.
*/
#if defined(SQLITE_EBCDIC)
# define sqlite3Utf8Read(A)        (*((*A)++))
# define Utf8Read(A)               (*(A++))
#else
# define Utf8Read(A)               (A[0]<0x80?*(A++):sqlite3Utf8Read(&A))
#endif

static const struct compareInfo globInfo = { '*', '?', '[', 0 };
/* The correct SQL-92 behavior is for the LIKE operator to ignore
** case.  Thus  'a' LIKE 'A' would be true. */
static const struct compareInfo likeInfoNorm = { '%', '_',   0, 1 };
/* If SQLITE_CASE_SENSITIVE_LIKE is defined, then the LIKE operator
** is case sensitive causing 'a' LIKE 'A' to be false */
static const struct compareInfo likeInfoAlt = { '%', '_',   0, 0 };

/*
** Possible error returns from patternMatch()
*/
#define SQLITE_MATCH             0
#define SQLITE_NOMATCH           1
#define SQLITE_NOWILDCARDMATCH   2

/*
** Compare two UTF-8 strings for equality where the first string is
** a GLOB or LIKE expression.  Return values:
**
**    SQLITE_MATCH:            Match
**    SQLITE_NOMATCH:          No match
**    SQLITE_NOWILDCARDMATCH:  No match in spite of having * or % wildcards.
**
** Globbing rules:
**
**      '*'       Matches any sequence of zero or more characters.
**
**      '?'       Matches exactly one character.
**
**     [...]      Matches one character from the enclosed list of
**                characters.
**
**     [^...]     Matches one character not in the enclosed list.
**
** With the [...] and [^...] matching, a ']' character can be included
** in the list by making it the first character after '[' or '^'.  A
** range of characters can be specified using '-'.  Example:
** "[a-z]" matches any single lower-case letter.  To match a '-', make
** it the last character in the list.
**
** Like matching rules:
**
**      '%'       Matches any sequence of zero or more characters
**
***     '_'       Matches any one character
**
**      Ec        Where E is the "esc" character and c is any other
**                character, including '%', '_', and esc, match exactly c.
**
** The comments within this routine usually assume glob matching.
**
** This routine is usually quick, but can be N**2 in the worst case.
*/
#if defined(SQLITE_BUILDING_FOR_COMDB2)
int patternCompare(
#else /* defined(SQLITE_BUILDING_FOR_COMDB2) */
static int patternCompare(
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  const u8 *zPattern,              /* The glob pattern */
  const u8 *zString,               /* The string to compare against the glob */
  const struct compareInfo *pInfo, /* Information about how to do the compare */
  u32 matchOther                   /* The escape char (LIKE) or '[' (GLOB) */
){
  u32 c, c2;                       /* Next pattern and input string chars */
  u32 matchOne = pInfo->matchOne;  /* "?" or "_" */
  u32 matchAll = pInfo->matchAll;  /* "*" or "%" */
  u8 noCase = pInfo->noCase;       /* True if uppercase==lowercase */
  const u8 *zEscaped = 0;          /* One past the last escaped input char */
 
  while( (c = Utf8Read(zPattern))!=0 ){
    if( c==matchAll ){  /* Match "*" */
      /* Skip over multiple "*" characters in the pattern.  If there
      ** are also "?" characters, skip those as well, but consume a
      ** single character of the input string for each "?" skipped */
      while( (c=Utf8Read(zPattern)) == matchAll
             || (c == matchOne && matchOne!=0) ){
        if( c==matchOne && sqlite3Utf8Read(&zString)==0 ){
          return SQLITE_NOWILDCARDMATCH;
        }
      }
      if( c==0 ){
        return SQLITE_MATCH;   /* "*" at the end of the pattern matches */
      }else if( c==matchOther ){
        if( pInfo->matchSet==0 ){
          c = sqlite3Utf8Read(&zPattern);
          if( c==0 ) return SQLITE_NOWILDCARDMATCH;
        }else{
          /* "[...]" immediately follows the "*".  We have to do a slow
          ** recursive search in this case, but it is an unusual case. */
          assert( matchOther<0x80 );  /* '[' is a single-byte character */
          while( *zString ){
            int bMatch = patternCompare(&zPattern[-1],zString,pInfo,matchOther);
            if( bMatch!=SQLITE_NOMATCH ) return bMatch;
            SQLITE_SKIP_UTF8(zString);
          }
          return SQLITE_NOWILDCARDMATCH;
        }
      }

      /* At this point variable c contains the first character of the
      ** pattern string past the "*".  Search in the input string for the
      ** first matching character and recursively continue the match from
      ** that point.
      **
      ** For a case-insensitive search, set variable cx to be the same as
      ** c but in the other case and search the input string for either
      ** c or cx.
      */
      if( c<0x80 ){
        char zStop[3];
        int bMatch;
        if( noCase ){
          zStop[0] = sqlite3Toupper(c);
          zStop[1] = sqlite3Tolower(c);
          zStop[2] = 0;
        }else{
          zStop[0] = c;
          zStop[1] = 0;
        }
        while(1){
          zString += strcspn((const char*)zString, zStop);
          if( zString[0]==0 ) break;
          zString++;
          bMatch = patternCompare(zPattern,zString,pInfo,matchOther);
          if( bMatch!=SQLITE_NOMATCH ) return bMatch;
        }
      }else{
        int bMatch;
        while( (c2 = Utf8Read(zString))!=0 ){
          if( c2!=c ) continue;
          bMatch = patternCompare(zPattern,zString,pInfo,matchOther);
          if( bMatch!=SQLITE_NOMATCH ) return bMatch;
        }
      }
      return SQLITE_NOWILDCARDMATCH;
    }
    if( c==matchOther ){
      if( pInfo->matchSet==0 ){
        c = sqlite3Utf8Read(&zPattern);
        if( c==0 ) return SQLITE_NOMATCH;
        zEscaped = zPattern;
      }else{
        u32 prior_c = 0;
        int seen = 0;
        int invert = 0;
        c = sqlite3Utf8Read(&zString);
        if( c==0 ) return SQLITE_NOMATCH;
        c2 = sqlite3Utf8Read(&zPattern);
        if( c2=='^' ){
          invert = 1;
          c2 = sqlite3Utf8Read(&zPattern);
        }
        if( c2==']' ){
          if( c==']' ) seen = 1;
          c2 = sqlite3Utf8Read(&zPattern);
        }
        while( c2 && c2!=']' ){
          if( c2=='-' && zPattern[0]!=']' && zPattern[0]!=0 && prior_c>0 ){
            c2 = sqlite3Utf8Read(&zPattern);
            if( c>=prior_c && c<=c2 ) seen = 1;
            prior_c = 0;
          }else{
            if( c==c2 ){
              seen = 1;
            }
            prior_c = c2;
          }
          c2 = sqlite3Utf8Read(&zPattern);
        }
        if( c2==0 || (seen ^ invert)==0 ){
          return SQLITE_NOMATCH;
        }
        continue;
      }
    }
    c2 = Utf8Read(zString);
    if( c==c2 ) continue;
    if( noCase  && sqlite3Tolower(c)==sqlite3Tolower(c2) && c<0x80 && c2<0x80 ){
      continue;
    }
    if( c==matchOne && zPattern!=zEscaped && c2!=0 ) continue;
    return SQLITE_NOMATCH;
  }
  return *zString==0 ? SQLITE_MATCH : SQLITE_NOMATCH;
}

/*
** The sqlite3_strglob() interface.  Return 0 on a match (like strcmp()) and
** non-zero if there is no match.
*/
int sqlite3_strglob(const char *zGlobPattern, const char *zString){
  if( zString==0 ){
    return zGlobPattern!=0;
  }else if( zGlobPattern==0 ){
    return 1;
  }else {
    return patternCompare((u8*)zGlobPattern, (u8*)zString, &globInfo, '[');
  }
}

/*
** The sqlite3_strlike() interface.  Return 0 on a match and non-zero for
** a miss - like strcmp().
*/
int sqlite3_strlike(const char *zPattern, const char *zStr, unsigned int esc){
  if( zStr==0 ){
    return zPattern!=0;
  }else if( zPattern==0 ){
    return 1;
  }else{
    return patternCompare((u8*)zPattern, (u8*)zStr, &likeInfoNorm, esc);
  }
}

/*
** Count the number of times that the LIKE operator (or GLOB which is
** just a variation of LIKE) gets called.  This is used for testing
** only.
*/
#ifdef SQLITE_TEST
int sqlite3_like_count = 0;
#endif


/*
** Implementation of the like() SQL function.  This function implements
** the built-in LIKE operator.  The first argument to the function is the
** pattern and the second argument is the string.  So, the SQL statements:
**
**       A LIKE B
**
** is implemented as like(B,A).
**
** This same function (with a different compareInfo structure) computes
** the GLOB operator.
*/
static void likeFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *zA, *zB;
  u32 escape;
  int nPat;
  sqlite3 *db = sqlite3_context_db_handle(context);
  struct compareInfo *pInfo = sqlite3_user_data(context);
  struct compareInfo backupInfo;

#ifdef SQLITE_LIKE_DOESNT_MATCH_BLOBS
  if( sqlite3_value_type(argv[0])==SQLITE_BLOB
   || sqlite3_value_type(argv[1])==SQLITE_BLOB
  ){
#ifdef SQLITE_TEST
    sqlite3_like_count++;
#endif
    sqlite3_result_int(context, 0);
    return;
  }
#endif

  /* Limit the length of the LIKE or GLOB pattern to avoid problems
  ** of deep recursion and N*N behavior in patternCompare().
  */
  nPat = sqlite3_value_bytes(argv[0]);
  testcase( nPat==db->aLimit[SQLITE_LIMIT_LIKE_PATTERN_LENGTH] );
  testcase( nPat==db->aLimit[SQLITE_LIMIT_LIKE_PATTERN_LENGTH]+1 );
  if( nPat > db->aLimit[SQLITE_LIMIT_LIKE_PATTERN_LENGTH] ){
    sqlite3_result_error(context, "LIKE or GLOB pattern too complex", -1);
    return;
  }
  if( argc==3 ){
    /* The escape character string must consist of a single UTF-8 character.
    ** Otherwise, return an error.
    */
    const unsigned char *zEsc = sqlite3_value_text(argv[2]);
    if( zEsc==0 ) return;
    if( sqlite3Utf8CharLen((char*)zEsc, -1)!=1 ){
      sqlite3_result_error(context,
          "ESCAPE expression must be a single character", -1);
      return;
    }
    escape = sqlite3Utf8Read(&zEsc);
    if( escape==pInfo->matchAll || escape==pInfo->matchOne ){
      memcpy(&backupInfo, pInfo, sizeof(backupInfo));
      pInfo = &backupInfo;
      if( escape==pInfo->matchAll ) pInfo->matchAll = 0;
      if( escape==pInfo->matchOne ) pInfo->matchOne = 0;
    }
  }else{
    escape = pInfo->matchSet;
  }
  zB = sqlite3_value_text(argv[0]);
  zA = sqlite3_value_text(argv[1]);
  if( zA && zB ){
#ifdef SQLITE_TEST
    sqlite3_like_count++;
#endif
    sqlite3_result_int(context,
                      patternCompare(zB, zA, pInfo, escape)==SQLITE_MATCH);
  }
}

/*
** Implementation of the NULLIF(x,y) function.  The result is the first
** argument if the arguments are different.  The result is NULL if the
** arguments are equal to each other.
*/
static void nullifFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **argv
){
  CollSeq *pColl = sqlite3GetFuncCollSeq(context);
  UNUSED_PARAMETER(NotUsed);
  if( sqlite3MemCompare(argv[0], argv[1], pColl)!=0 ){
    sqlite3_result_value(context, argv[0]);
  }
}

/*
** Implementation of the sqlite_version() function.  The result is the version
** of the SQLite library that is running.
*/
static void versionFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  /* IMP: R-48699-48617 This function is an SQL wrapper around the
  ** sqlite3_libversion() C-interface. */
  sqlite3_result_text(context, sqlite3_libversion(), -1, SQLITE_STATIC);
}

/*
** Implementation of the sqlite_source_id() function. The result is a string
** that identifies the particular version of the source code used to build
** SQLite.
*/
static void sourceidFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  /* IMP: R-24470-31136 This function is an SQL wrapper around the
  ** sqlite3_sourceid() C interface. */
  sqlite3_result_text(context, sqlite3_sourceid(), -1, SQLITE_STATIC);
}

/*
** Implementation of the sqlite_log() function.  This is a wrapper around
** sqlite3_log().  The return value is NULL.  The function exists purely for
** its side-effects.
*/
static void errlogFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  UNUSED_PARAMETER(argc);
  UNUSED_PARAMETER(context);
  sqlite3_log(sqlite3_value_int(argv[0]), "%s", sqlite3_value_text(argv[1]));
}

/*
** Implementation of the sqlite_compileoption_used() function.
** The result is an integer that identifies if the compiler option
** was used to build SQLite.
*/
#ifndef SQLITE_OMIT_COMPILEOPTION_DIAGS
static void compileoptionusedFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const char *zOptName;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  /* IMP: R-39564-36305 The sqlite_compileoption_used() SQL
  ** function is a wrapper around the sqlite3_compileoption_used() C/C++
  ** function.
  */
  if( (zOptName = (const char*)sqlite3_value_text(argv[0]))!=0 ){
    sqlite3_result_int(context, sqlite3_compileoption_used(zOptName));
  }
}
#endif /* SQLITE_OMIT_COMPILEOPTION_DIAGS */

/*
** Implementation of the sqlite_compileoption_get() function.
** The result is a string that identifies the compiler options
** used to build SQLite.
*/
#ifndef SQLITE_OMIT_COMPILEOPTION_DIAGS
static void compileoptiongetFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int n;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  /* IMP: R-04922-24076 The sqlite_compileoption_get() SQL function
  ** is a wrapper around the sqlite3_compileoption_get() C/C++ function.
  */
  n = sqlite3_value_int(argv[0]);
  sqlite3_result_text(context, sqlite3_compileoption_get(n), -1, SQLITE_STATIC);
}
#endif /* SQLITE_OMIT_COMPILEOPTION_DIAGS */

/* Array for converting from half-bytes (nybbles) into ASCII hex
** digits. */
static const char hexdigits[] = {
  '0', '1', '2', '3', '4', '5', '6', '7',
  '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
};

/*
** Append to pStr text that is the SQL literal representation of the
** value contained in pValue.
*/
void sqlite3QuoteValue(StrAccum *pStr, sqlite3_value *pValue, int bEscape){
  /* As currently implemented, the string must be initially empty.
  ** we might relax this requirement in the future, but that will
  ** require enhancements to the implementation. */
  assert( pStr!=0 && pStr->nChar==0 );

  switch( sqlite3_value_type(pValue) ){
    case SQLITE_FLOAT: {
      double r1, r2;
      const char *zVal;
      r1 = sqlite3_value_double(pValue);
      sqlite3_str_appendf(pStr, "%!0.15g", r1);
      zVal = sqlite3_str_value(pStr);
      if( zVal ){
        sqlite3AtoF(zVal, &r2, pStr->nChar, SQLITE_UTF8);
        if( r1!=r2 ){
          sqlite3_str_reset(pStr);
          sqlite3_str_appendf(pStr, "%!0.20e", r1);
        }
      }
      break;
    }
    case SQLITE_INTEGER: {
      sqlite3_str_appendf(pStr, "%lld", sqlite3_value_int64(pValue));
      break;
    }
    case SQLITE_BLOB: {
      char const *zBlob = sqlite3_value_blob(pValue);
      i64 nBlob = sqlite3_value_bytes(pValue);
      assert( zBlob==sqlite3_value_blob(pValue) ); /* No encoding change */
      sqlite3StrAccumEnlarge(pStr, nBlob*2 + 4);
      if( pStr->accError==0 ){
        char *zText = pStr->zText;
        int i;
        for(i=0; i<nBlob; i++){
          zText[(i*2)+2] = hexdigits[(zBlob[i]>>4)&0x0F];
          zText[(i*2)+3] = hexdigits[(zBlob[i])&0x0F];
        }
        zText[(nBlob*2)+2] = '\'';
        zText[(nBlob*2)+3] = '\0';
        zText[0] = 'X';
        zText[1] = '\'';
        pStr->nChar = nBlob*2 + 3;
      }
      break;
    }
    case SQLITE_TEXT: {
      const unsigned char *zArg = sqlite3_value_text(pValue);
      sqlite3_str_appendf(pStr, bEscape ? "%#Q" : "%Q", zArg);
      break;
    }
    default: {
      assert( sqlite3_value_type(pValue)==SQLITE_NULL );
      sqlite3_str_append(pStr, "NULL", 4);
      break;
    }
  }
}

/*
** Return true if z[] begins with N hexadecimal digits, and write
** a decoding of those digits into *pVal.  Or return false if any
** one of the first N characters in z[] is not a hexadecimal digit.
*/
static int isNHex(const char *z, int N, u32 *pVal){
  int i;
  u32 v = 0;
  for(i=0; i<N; i++){
    if( !sqlite3Isxdigit(z[i]) ) return 0;
    v = (v<<4) + sqlite3HexToInt(z[i]);
  }
  *pVal = v;
  return 1;
}

/*
** Implementation of the UNISTR() function.
**
** This is intended to be a work-alike of the UNISTR() function in
** PostgreSQL.  Quoting from the PG documentation (PostgreSQL 17 -
** scraped on 2025-02-22):
**
**    Evaluate escaped Unicode characters in the argument. Unicode
**    characters can be specified as \XXXX (4 hexadecimal digits),
**    \+XXXXXX (6 hexadecimal digits), \uXXXX (4 hexadecimal digits),
**    or \UXXXXXXXX (8 hexadecimal digits). To specify a backslash,
**    write two backslashes. All other characters are taken literally.
*/
static void unistrFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  char *zOut;
  const char *zIn;
  int nIn;
  int i, j, n;
  u32 v;

  assert( argc==1 );
  UNUSED_PARAMETER( argc );
  zIn = (const char*)sqlite3_value_text(argv[0]);
  if( zIn==0 ) return;
  nIn = sqlite3_value_bytes(argv[0]);
  zOut = sqlite3_malloc64(nIn+1);
  if( zOut==0 ){
    sqlite3_result_error_nomem(context);
    return;
  }
  i = j = 0;
  while( i<nIn ){
    char *z = strchr(&zIn[i],'\\');
    if( z==0 ){
      n = nIn - i;
      memmove(&zOut[j], &zIn[i], n);
      j += n;
      break;
    }
    n = z - &zIn[i];
    if( n>0 ){
      memmove(&zOut[j], &zIn[i], n);
      j += n;
      i += n;
    }
    if( zIn[i+1]=='\\' ){
      i += 2;
      zOut[j++] = '\\';
    }else if( sqlite3Isxdigit(zIn[i+1]) ){
      if( !isNHex(&zIn[i+1], 4, &v) ) goto unistr_error;
      i += 5;
      j += sqlite3AppendOneUtf8Character(&zOut[j], v);
    }else if( zIn[i+1]=='+' ){
      if( !isNHex(&zIn[i+2], 6, &v) ) goto unistr_error;
      i += 8;
      j += sqlite3AppendOneUtf8Character(&zOut[j], v);
    }else if( zIn[i+1]=='u' ){
      if( !isNHex(&zIn[i+2], 4, &v) ) goto unistr_error;
      i += 6;
      j += sqlite3AppendOneUtf8Character(&zOut[j], v);
    }else if( zIn[i+1]=='U' ){
      if( !isNHex(&zIn[i+2], 8, &v) ) goto unistr_error;
      i += 10;
      j += sqlite3AppendOneUtf8Character(&zOut[j], v);
    }else{
      goto unistr_error;
    }
  }
  zOut[j] = 0;
  sqlite3_result_text64(context, zOut, j, sqlite3_free, SQLITE_UTF8);
  return;

unistr_error:
  sqlite3_free(zOut);
  sqlite3_result_error(context, "invalid Unicode escape", -1);
  return;
}


/*
** Implementation of the QUOTE() function. 
**
** The quote(X) function returns the text of an SQL literal which is the
** value of its argument suitable for inclusion into an SQL statement.
** Strings are surrounded by single-quotes with escapes on interior quotes
** as needed. BLOBs are encoded as hexadecimal literals. Strings with
** embedded NUL characters cannot be represented as string literals in SQL
** and hence the returned string literal is truncated prior to the first NUL.
**
** If sqlite3_user_data() is non-zero, then the UNISTR_QUOTE() function is
** implemented instead.  The difference is that UNISTR_QUOTE() uses the
** UNISTR() function to escape control characters.
*/
static void quoteFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
  sqlite3_str str;
  sqlite3 *db = sqlite3_context_db_handle(context);
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  sqlite3StrAccumInit(&str, db, 0, 0, db->aLimit[SQLITE_LIMIT_LENGTH]);
  sqlite3QuoteValue(&str,argv[0],SQLITE_PTR_TO_INT(sqlite3_user_data(context)));
  sqlite3_result_text(context, sqlite3StrAccumFinish(&str), str.nChar,
                      SQLITE_DYNAMIC);
  if( str.accError!=SQLITE_OK ){
    sqlite3_result_null(context);
    sqlite3_result_error_code(context, str.accError);
  }
}

/*
** The unicode() function.  Return the integer unicode code-point value
** for the first character of the input string.
*/
static void unicodeFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *z = sqlite3_value_text(argv[0]);
  (void)argc;
  if( z && z[0] ) sqlite3_result_int(context, sqlite3Utf8Read(&z));
}

/*
** The char() function takes zero or more arguments, each of which is
** an integer.  It constructs a string where each character of the string
** is the unicode character for the corresponding integer argument.
*/
static void charFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  unsigned char *z, *zOut;
  int i;
  zOut = z = sqlite3_malloc64( argc*4+1 );
  if( z==0 ){
    sqlite3_result_error_nomem(context);
    return;
  }
  for(i=0; i<argc; i++){
    sqlite3_int64 x;
    unsigned c;
    x = sqlite3_value_int64(argv[i]);
    if( x<0 || x>0x10ffff ) x = 0xfffd;
    c = (unsigned)(x & 0x1fffff);
    if( c<0x00080 ){
      *zOut++ = (u8)(c&0xFF);
    }else if( c<0x00800 ){
      *zOut++ = 0xC0 + (u8)((c>>6)&0x1F);
      *zOut++ = 0x80 + (u8)(c & 0x3F);
    }else if( c<0x10000 ){
      *zOut++ = 0xE0 + (u8)((c>>12)&0x0F);
      *zOut++ = 0x80 + (u8)((c>>6) & 0x3F);
      *zOut++ = 0x80 + (u8)(c & 0x3F);
    }else{
      *zOut++ = 0xF0 + (u8)((c>>18) & 0x07);
      *zOut++ = 0x80 + (u8)((c>>12) & 0x3F);
      *zOut++ = 0x80 + (u8)((c>>6) & 0x3F);
      *zOut++ = 0x80 + (u8)(c & 0x3F);
    }                                                    \
  }
  *zOut = 0;
  sqlite3_result_text64(context, (char*)z, zOut-z, sqlite3_free, SQLITE_UTF8);
}

/*
** The hex() function.  Interpret the argument as a blob.  Return
** a hexadecimal rendering as text.
*/
static void hexFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int i, n;
  const unsigned char *pBlob;
  char *zHex, *z;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  pBlob = sqlite3_value_blob(argv[0]);
  n = sqlite3_value_bytes(argv[0]);
  assert( pBlob==sqlite3_value_blob(argv[0]) );  /* No encoding change */
  z = zHex = contextMalloc(context, ((i64)n)*2 + 1);
  if( zHex ){
    for(i=0; i<n; i++, pBlob++){
      unsigned char c = *pBlob;
      *(z++) = hexdigits[(c>>4)&0xf];
      *(z++) = hexdigits[c&0xf];
    }
    *z = 0;
    sqlite3_result_text64(context, zHex, (u64)(z-zHex),
                          sqlite3_free, SQLITE_UTF8);
  }
}

/*
** Buffer zStr contains nStr bytes of utf-8 encoded text. Return 1 if zStr
** contains character ch, or 0 if it does not.
*/
static int strContainsChar(const u8 *zStr, int nStr, u32 ch){
  const u8 *zEnd = &zStr[nStr];
  const u8 *z = zStr;
  while( z<zEnd ){
    u32 tst = Utf8Read(z);
    if( tst==ch ) return 1;
  }
  return 0;
}

/*
** The unhex() function. This function may be invoked with either one or
** two arguments. In both cases the first argument is interpreted as text
** a text value containing a set of pairs of hexadecimal digits which are
** decoded and returned as a blob.
**
** If there is only a single argument, then it must consist only of an
** even number of hexadecimal digits. Otherwise, return NULL.
**
** Or, if there is a second argument, then any character that appears in
** the second argument is also allowed to appear between pairs of hexadecimal
** digits in the first argument. If any other character appears in the
** first argument, or if one of the allowed characters appears between
** two hexadecimal digits that make up a single byte, NULL is returned.
**
** The following expressions are all true:
**
**     unhex('ABCD')       IS x'ABCD'
**     unhex('AB CD')      IS NULL
**     unhex('AB CD', ' ') IS x'ABCD'
**     unhex('A BCD', ' ') IS NULL
*/
static void unhexFunc(
  sqlite3_context *pCtx,
  int argc,
  sqlite3_value **argv
){
  const u8 *zPass = (const u8*)"";
  int nPass = 0;
  const u8 *zHex = sqlite3_value_text(argv[0]);
  int nHex = sqlite3_value_bytes(argv[0]);
#ifdef SQLITE_DEBUG
  const u8 *zEnd = zHex ? &zHex[nHex] : 0;
#endif
  u8 *pBlob = 0;
  u8 *p = 0;

  assert( argc==1 || argc==2 );
  if( argc==2 ){
    zPass = sqlite3_value_text(argv[1]);
    nPass = sqlite3_value_bytes(argv[1]);
  }
  if( !zHex || !zPass ) return;

  p = pBlob = contextMalloc(pCtx, (nHex/2)+1);
  if( pBlob ){
    u8 c;                         /* Most significant digit of next byte */
    u8 d;                         /* Least significant digit of next byte */

    while( (c = *zHex)!=0x00 ){
      while( !sqlite3Isxdigit(c) ){
        u32 ch = Utf8Read(zHex);
        assert( zHex<=zEnd );
        if( !strContainsChar(zPass, nPass, ch) ) goto unhex_null;
        c = *zHex;
        if( c==0x00 ) goto unhex_done;
      }
      zHex++;
      assert( *zEnd==0x00 );
      assert( zHex<=zEnd );
      d = *(zHex++);
      if( !sqlite3Isxdigit(d) ) goto unhex_null;
      *(p++) = (sqlite3HexToInt(c)<<4) | sqlite3HexToInt(d);
    }
  }

 unhex_done:
  sqlite3_result_blob(pCtx, pBlob, (p - pBlob), sqlite3_free);
  return;

 unhex_null:
  sqlite3_free(pBlob);
  return;
}


/*
** The zeroblob(N) function returns a zero-filled blob of size N bytes.
*/
static void zeroblobFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  i64 n;
  int rc;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  n = sqlite3_value_int64(argv[0]);
  if( n<0 ) n = 0;
  rc = sqlite3_result_zeroblob64(context, n); /* IMP: R-00293-64994 */
  if( rc ){
    sqlite3_result_error_code(context, rc);
  }
}

/*
** The replace() function.  Three arguments are all strings: call
** them A, B, and C. The result is also a string which is derived
** from A by replacing every occurrence of B with C.  The match
** must be exact.  Collating sequences are not used.
*/
static void replaceFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *zStr;        /* The input string A */
  const unsigned char *zPattern;    /* The pattern string B */
  const unsigned char *zRep;        /* The replacement string C */
  unsigned char *zOut;              /* The output */
  int nStr;                /* Size of zStr */
  int nPattern;            /* Size of zPattern */
  int nRep;                /* Size of zRep */
  i64 nOut;                /* Maximum size of zOut */
  int loopLimit;           /* Last zStr[] that might match zPattern[] */
  int i, j;                /* Loop counters */
  unsigned cntExpand;      /* Number zOut expansions */
  sqlite3 *db = sqlite3_context_db_handle(context);

  assert( argc==3 );
  UNUSED_PARAMETER(argc);
  zStr = sqlite3_value_text(argv[0]);
  if( zStr==0 ) return;
  nStr = sqlite3_value_bytes(argv[0]);
  assert( zStr==sqlite3_value_text(argv[0]) );  /* No encoding change */
  zPattern = sqlite3_value_text(argv[1]);
  if( zPattern==0 ){
    assert( sqlite3_value_type(argv[1])==SQLITE_NULL
            || sqlite3_context_db_handle(context)->mallocFailed );
    return;
  }
  if( zPattern[0]==0 ){
    assert( sqlite3_value_type(argv[1])!=SQLITE_NULL );
    sqlite3_result_text(context, (const char*)zStr, nStr, SQLITE_TRANSIENT);
    return;
  }
  nPattern = sqlite3_value_bytes(argv[1]);
  assert( zPattern==sqlite3_value_text(argv[1]) );  /* No encoding change */
  zRep = sqlite3_value_text(argv[2]);
  if( zRep==0 ) return;
  nRep = sqlite3_value_bytes(argv[2]);
  assert( zRep==sqlite3_value_text(argv[2]) );
  nOut = nStr + 1;
  assert( nOut<SQLITE_MAX_LENGTH );
  zOut = contextMalloc(context, nOut);
  if( zOut==0 ){
    return;
  }
  loopLimit = nStr - nPattern; 
  cntExpand = 0;
  for(i=j=0; i<=loopLimit; i++){
    if( zStr[i]!=zPattern[0] || memcmp(&zStr[i], zPattern, nPattern) ){
      zOut[j++] = zStr[i];
    }else{
      if( nRep>nPattern ){
        nOut += nRep - nPattern;
        testcase( nOut-1==db->aLimit[SQLITE_LIMIT_LENGTH] );
        testcase( nOut-2==db->aLimit[SQLITE_LIMIT_LENGTH] );
        if( nOut-1>db->aLimit[SQLITE_LIMIT_LENGTH] ){
          sqlite3_result_error_toobig(context);
          sqlite3_free(zOut);
          return;
        }
        cntExpand++;
        if( (cntExpand&(cntExpand-1))==0 ){
          /* Grow the size of the output buffer only on substitutions
          ** whose index is a power of two: 1, 2, 4, 8, 16, 32, ... */
          u8 *zOld;
          zOld = zOut;
          zOut = sqlite3Realloc(zOut, (int)nOut + (nOut - nStr - 1));
          if( zOut==0 ){
            sqlite3_result_error_nomem(context);
            sqlite3_free(zOld);
            return;
          }
        }
      }
      memcpy(&zOut[j], zRep, nRep);
      j += nRep;
      i += nPattern-1;
    }
  }
  assert( j+nStr-i+1<=nOut );
  memcpy(&zOut[j], &zStr[i], nStr-i);
  j += nStr - i;
  assert( j<=nOut );
  zOut[j] = 0;
  sqlite3_result_text(context, (char*)zOut, j, sqlite3_free);
}

/*
** Implementation of the TRIM(), LTRIM(), and RTRIM() functions.
** The userdata is 0x1 for left trim, 0x2 for right trim, 0x3 for both.
*/
static void trimFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *zIn;         /* Input string */
  const unsigned char *zCharSet;    /* Set of characters to trim */
  unsigned int nIn;                 /* Number of bytes in input */
  int flags;                        /* 1: trimleft  2: trimright  3: trim */
  int i;                            /* Loop counter */
  unsigned int *aLen = 0;           /* Length of each character in zCharSet */
  unsigned char **azChar = 0;       /* Individual characters in zCharSet */
  int nChar;                        /* Number of characters in zCharSet */

  if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
    return;
  }
  zIn = sqlite3_value_text(argv[0]);
  if( zIn==0 ) return;
  nIn = (unsigned)sqlite3_value_bytes(argv[0]);
  assert( zIn==sqlite3_value_text(argv[0]) );
  if( argc==1 ){
    static const unsigned lenOne[] = { 1 };
    static unsigned char * const azOne[] = { (u8*)" " };
    nChar = 1;
    aLen = (unsigned*)lenOne;
    azChar = (unsigned char **)azOne;
    zCharSet = 0;
  }else if( (zCharSet = sqlite3_value_text(argv[1]))==0 ){
    return;
  }else{
    const unsigned char *z;
    for(z=zCharSet, nChar=0; *z; nChar++){
      SQLITE_SKIP_UTF8(z);
    }
    if( nChar>0 ){
      azChar = contextMalloc(context,
                     ((i64)nChar)*(sizeof(char*)+sizeof(unsigned)));
      if( azChar==0 ){
        return;
      }
      aLen = (unsigned*)&azChar[nChar];
      for(z=zCharSet, nChar=0; *z; nChar++){
        azChar[nChar] = (unsigned char *)z;
        SQLITE_SKIP_UTF8(z);
        aLen[nChar] = (unsigned)(z - azChar[nChar]);
      }
    }
  }
  if( nChar>0 ){
    flags = SQLITE_PTR_TO_INT(sqlite3_user_data(context));
    if( flags & 1 ){
      while( nIn>0 ){
        unsigned int len = 0;
        for(i=0; i<nChar; i++){
          len = aLen[i];
          if( len<=nIn && memcmp(zIn, azChar[i], len)==0 ) break;
        }
        if( i>=nChar ) break;
        zIn += len;
        nIn -= len;
      }
    }
    if( flags & 2 ){
      while( nIn>0 ){
        unsigned int len = 0;
        for(i=0; i<nChar; i++){
          len = aLen[i];
          if( len<=nIn && memcmp(&zIn[nIn-len],azChar[i],len)==0 ) break;
        }
        if( i>=nChar ) break;
        nIn -= len;
      }
    }
    if( zCharSet ){
      sqlite3_free(azChar);
    }
  }
  sqlite3_result_text(context, (char*)zIn, nIn, SQLITE_TRANSIENT);
}

/* The core implementation of the CONCAT(...) and CONCAT_WS(SEP,...)
** functions.
**
** Return a string value that is the concatenation of all non-null
** entries in argv[].  Use zSep as the separator.
*/
static void concatFuncCore(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv,
  int nSep,
  const char *zSep
){
  i64 j, n = 0;
  int i;
  int bNotNull = 0;   /* True after at least NOT NULL argument seen */
  char *z;
  for(i=0; i<argc; i++){
    n += sqlite3_value_bytes(argv[i]);
  }
  n += (argc-1)*(i64)nSep;
  z = sqlite3_malloc64(n+1);
  if( z==0 ){
    sqlite3_result_error_nomem(context);
    return;
  }
  j = 0;
  for(i=0; i<argc; i++){
    if( sqlite3_value_type(argv[i])!=SQLITE_NULL ){
      int k = sqlite3_value_bytes(argv[i]);
      const char *v = (const char*)sqlite3_value_text(argv[i]);
      if( v!=0 ){
        if( bNotNull && nSep>0 ){
          memcpy(&z[j], zSep, nSep);
          j += nSep;
        }
        memcpy(&z[j], v, k);
        j += k;
        bNotNull = 1;
      }
    }
  }
  z[j] = 0;
  assert( j<=n );
  sqlite3_result_text64(context, z, j, sqlite3_free, SQLITE_UTF8);
}

/*
** The CONCAT(...) function.  Generate a string result that is the
** concatentation of all non-null arguments.
*/
static void concatFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  concatFuncCore(context, argc, argv, 0, "");
}

/*
** The CONCAT_WS(separator, ...) function.
**
** Generate a string that is the concatenation of 2nd through the Nth
** argument.  Use the first argument (which must be non-NULL) as the
** separator.
*/
static void concatwsFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int nSep = sqlite3_value_bytes(argv[0]);
  const char *zSep = (const char*)sqlite3_value_text(argv[0]);
  if( zSep==0 ) return;
  concatFuncCore(context, argc-1, argv+1, nSep, zSep);
}


#ifdef SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION
/*
** The "unknown" function is automatically substituted in place of
** any unrecognized function name when doing an EXPLAIN or EXPLAIN QUERY PLAN
** when the SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION compile-time option is used.
** When the "sqlite3" command-line shell is built using this functionality,
** that allows an EXPLAIN or EXPLAIN QUERY PLAN for complex queries
** involving application-defined functions to be examined in a generic
** sqlite3 shell.
*/
static void unknownFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  /* no-op */
  (void)context;
  (void)argc;
  (void)argv;
}
#endif /*SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION*/


/* IMP: R-25361-16150 This function is omitted from SQLite by default. It
** is only available if the SQLITE_SOUNDEX compile-time option is used
** when SQLite is built.
*/
#ifdef SQLITE_SOUNDEX
/*
** Compute the soundex encoding of a word.
**
** IMP: R-59782-00072 The soundex(X) function returns a string that is the
** soundex encoding of the string X.
*/
static void soundexFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  char zResult[8];
  const u8 *zIn;
  int i, j;
  static const unsigned char iCode[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 1, 2, 3, 0, 1, 2, 0, 0, 2, 2, 4, 5, 5, 0,
    1, 2, 6, 2, 3, 0, 1, 0, 2, 0, 2, 0, 0, 0, 0, 0,
    0, 0, 1, 2, 3, 0, 1, 2, 0, 0, 2, 2, 4, 5, 5, 0,
    1, 2, 6, 2, 3, 0, 1, 0, 2, 0, 2, 0, 0, 0, 0, 0,
  };
  assert( argc==1 );
  zIn = (u8*)sqlite3_value_text(argv[0]);
  if( zIn==0 ) zIn = (u8*)"";
  for(i=0; zIn[i] && !sqlite3Isalpha(zIn[i]); i++){}
  if( zIn[i] ){
    u8 prevcode = iCode[zIn[i]&0x7f];
    zResult[0] = sqlite3Toupper(zIn[i]);
    for(j=1; j<4 && zIn[i]; i++){
      int code = iCode[zIn[i]&0x7f];
      if( code>0 ){
        if( code!=prevcode ){
          prevcode = code;
          zResult[j++] = code + '0';
        }
      }else{
        prevcode = 0;
      }
    }
    while( j<4 ){
      zResult[j++] = '0';
    }
    zResult[j] = 0;
    sqlite3_result_text(context, zResult, 4, SQLITE_TRANSIENT);
  }else{
    /* IMP: R-64894-50321 The string "?000" is returned if the argument
    ** is NULL or contains no ASCII alphabetic characters. */
    sqlite3_result_text(context, "?000", 4, SQLITE_STATIC);
  }
}
#endif /* SQLITE_SOUNDEX */

#ifndef SQLITE_OMIT_LOAD_EXTENSION
/*
** A function that loads a shared-library extension then returns NULL.
*/
static void loadExt(sqlite3_context *context, int argc, sqlite3_value **argv){
  const char *zFile = (const char *)sqlite3_value_text(argv[0]);
  const char *zProc;
  sqlite3 *db = sqlite3_context_db_handle(context);
  char *zErrMsg = 0;

  /* Disallow the load_extension() SQL function unless the SQLITE_LoadExtFunc
  ** flag is set.  See the sqlite3_enable_load_extension() API.
  */
  if( (db->flags & SQLITE_LoadExtFunc)==0 ){
    sqlite3_result_error(context, "not authorized", -1);
    return;
  }

  if( argc==2 ){
    zProc = (const char *)sqlite3_value_text(argv[1]);
  }else{
    zProc = 0;
  }
  if( zFile && sqlite3_load_extension(db, zFile, zProc, &zErrMsg) ){
    sqlite3_result_error(context, zErrMsg, -1);
    sqlite3_free(zErrMsg);
  }
}
#endif


/*
** An instance of the following structure holds the context of a
** sum() or avg() aggregate computation.
*/
typedef struct SumCtx SumCtx;
struct SumCtx {
  double rSum;      /* Running sum as as a double */
  double rErr;      /* Error term for Kahan-Babushka-Neumaier summation */
  i64 iSum;         /* Running sum as a signed integer */
  i64 cnt;          /* Number of elements summed */
  u8 approx;        /* True if any non-integer value was input to the sum */
  u8 ovrfl;         /* Integer overflow seen */
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  u8 decs;          /* True if summing decimals */
  decQuad decSum;   /* decQuad aggregation */
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
};

/*
** Do one step of the Kahan-Babushka-Neumaier summation.
**
** https://en.wikipedia.org/wiki/Kahan_summation_algorithm
**
** Variables are marked "volatile" to defeat c89 x86 floating point
** optimizations can mess up this algorithm.
*/
static void kahanBabuskaNeumaierStep(
  volatile SumCtx *pSum,
  volatile double r
){
  volatile double s = pSum->rSum;
  volatile double t = s + r;
  if( fabs(s) > fabs(r) ){
    pSum->rErr += (s - t) + r;
  }else{
    pSum->rErr += (r - t) + s;
  }
  pSum->rSum = t;
}

/*
** Add a (possibly large) integer to the running sum.
*/
static void kahanBabuskaNeumaierStepInt64(volatile SumCtx *pSum, i64 iVal){
  if( iVal<=-4503599627370496LL || iVal>=+4503599627370496LL ){
    i64 iBig, iSm;
    iSm = iVal % 16384;
    iBig = iVal - iSm;
    kahanBabuskaNeumaierStep(pSum, iBig);
    kahanBabuskaNeumaierStep(pSum, iSm);
  }else{
    kahanBabuskaNeumaierStep(pSum, (double)iVal);
  }
}

/*
** Initialize the Kahan-Babaska-Neumaier sum from a 64-bit integer
*/
static void kahanBabuskaNeumaierInit(
  volatile SumCtx *p,
  i64 iVal
){
  if( iVal<=-4503599627370496LL || iVal>=+4503599627370496LL ){
    i64 iSm = iVal % 16384;
    p->rSum = (double)(iVal - iSm);
    p->rErr = (double)iSm;
  }else{
    p->rSum = (double)iVal;
    p->rErr = 0.0;
  }
}

/*
** Routines used to compute the sum, average, and total.
**
** The SUM() function follows the (broken) SQL standard which means
** that it returns NULL if it sums over no inputs.  TOTAL returns
** 0.0 in that case.  In addition, TOTAL always returns a float where
** SUM might return an integer if it never encounters a floating point
** value.  TOTAL never fails, but SUM might throw an exception if
** it overflows an integer.
*/
static void sumStep(sqlite3_context *context, int argc, sqlite3_value **argv){
  SumCtx *p;
  int type;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  p = sqlite3_aggregate_context(context, sizeof(*p));
  type = sqlite3_value_numeric_type(argv[0]);
  if( p && type!=SQLITE_NULL ){
    p->cnt++;
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    if( type==SQLITE_DECIMAL ){
      intv_t v = *(intv_t*)sqlite3_value_interval(argv[0], SQLITE_DECIMAL);

      if( p->decs==0 ){
        p->decSum = v.u.dec;
        p->decs = 1;

        if( 0 ){
          char aaa[128];
          char bbb[128];

          decQuadToString( &p->decSum, aaa);
          decQuadToString( &v.u.dec, bbb);

          fprintf(stderr, "%s  = %s\n", aaa, bbb);
        }
      }else{
        decContext ctx;
        decQuad    res;

        dec_ctx_init( &ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);
        decQuadAdd( &res, &p->decSum, &v.u.dec, &ctx);

        if( dfp_conv_check_status(&ctx, "quad", "add(quads)") ){
          sqlite3_result_error(context, "decimal overflow", -1);
        }

        if( 0 ){
          char aaa[128];
          char bbb[128];
          char ccc[128];

          decQuadToString( &p->decSum, aaa);
          decQuadToString( &v.u.dec, bbb);
          decQuadToString( &res, ccc);

          fprintf(stderr, "%s + %s = %s\n", aaa, bbb, ccc);
        }

        p->decSum = res;
      }
    }else
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
    if( p->approx==0 ){
      if( type!=SQLITE_INTEGER ){
        kahanBabuskaNeumaierInit(p, p->iSum);
        p->approx = 1;
        kahanBabuskaNeumaierStep(p, sqlite3_value_double(argv[0]));
      }else{
        i64 x = p->iSum;
        if( sqlite3AddInt64(&x, sqlite3_value_int64(argv[0]))==0 ){
          p->iSum = x;
        }else{
          p->ovrfl = 1;
          kahanBabuskaNeumaierInit(p, p->iSum);
          p->approx = 1;
          kahanBabuskaNeumaierStepInt64(p, sqlite3_value_int64(argv[0]));
        }
      }
    }else{
      if( type==SQLITE_INTEGER ){
        kahanBabuskaNeumaierStepInt64(p, sqlite3_value_int64(argv[0]));
      }else{
        p->ovrfl = 0;
        kahanBabuskaNeumaierStep(p, sqlite3_value_double(argv[0]));
      }
    }
  }
}
#ifndef SQLITE_OMIT_WINDOWFUNC
static void sumInverse(sqlite3_context *context, int argc, sqlite3_value**argv){
  SumCtx *p;
  int type;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  p = sqlite3_aggregate_context(context, sizeof(*p));
  type = sqlite3_value_numeric_type(argv[0]);
  /* p is always non-NULL because sumStep() will have been called first
  ** to initialize it */
  if( ALWAYS(p) && type!=SQLITE_NULL ){
    assert( p->cnt>0 );
    p->cnt--;
    if( !p->approx ){
      if( sqlite3SubInt64(&p->iSum, sqlite3_value_int64(argv[0])) ){
        p->ovrfl = 1;
        p->approx = 1;
      }
    }else if( type==SQLITE_INTEGER ){
      i64 iVal = sqlite3_value_int64(argv[0]);
      if( iVal!=SMALLEST_INT64 ){
        kahanBabuskaNeumaierStepInt64(p, -iVal);
      }else{
        kahanBabuskaNeumaierStepInt64(p, LARGEST_INT64);
        kahanBabuskaNeumaierStepInt64(p, 1);
      }       
    }else{
      kahanBabuskaNeumaierStep(p, -sqlite3_value_double(argv[0]));
    }
  }
}
#else
# define sumInverse 0
#endif /* SQLITE_OMIT_WINDOWFUNC */
static void sumFinalize(sqlite3_context *context){
  SumCtx *p;
  p = sqlite3_aggregate_context(context, 0);
  if( p && p->cnt>0 ){
    if( p->approx ){
      if( p->ovrfl ){
        sqlite3_result_error(context,"integer overflow",-1);
      }else if( !sqlite3IsOverflow(p->rErr) ){
        sqlite3_result_double(context, p->rSum+p->rErr);
      }else{
        sqlite3_result_double(context, p->rSum);
      }
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    }else if( p->decs ){
       intv_t res;
       res.type = INTV_DECIMAL_TYPE;
       res.sign = 0;
       res.u.dec = p->decSum;
       sqlite3_result_interval(context, &res);
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
    }else{
      sqlite3_result_int64(context, p->iSum);
    }
  }
}
static void avgFinalize(sqlite3_context *context){
  SumCtx *p;
  p = sqlite3_aggregate_context(context, 0);
  if( p && p->cnt>0 ){
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    if( p->decs ){
      decContext ctx;
      decQuad denom;
      decQuad res;
      intv_t  tv;

      dec_ctx_init( &ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);
      decQuadFromInt32( &denom, p->cnt);
      decQuadDivide( &res, &p->decSum, &denom, &ctx);
      if( dfp_conv_check_status(&ctx, "quad", "divide(quad)") ){
        sqlite3_result_error(context, "decimal overflow", -1);
      }

      tv.type = INTV_DECIMAL_TYPE;
      tv.sign = 0;
      tv.u.dec = res;

      sqlite3_result_interval( context, &tv);
      return;
    }
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
    double r;
    if( p->approx ){
      r = p->rSum;
      if( !sqlite3IsOverflow(p->rErr) ) r += p->rErr;
    }else{
      r = (double)(p->iSum);
    }
    sqlite3_result_double(context, r/(double)p->cnt);
  }
}
static void totalFinalize(sqlite3_context *context){
  SumCtx *p;
  double r = 0.0;
  p = sqlite3_aggregate_context(context, 0);
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  if( p && p->decs ){
    intv_t res;
    res.type = INTV_DECIMAL_TYPE;
    res.sign = 0;
    res.u.dec = p->decSum;
    sqlite3_result_interval(context, &res);
    return;
  }
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  if( p ){
    if( p->approx ){
      r = p->rSum;
      if( !sqlite3IsOverflow(p->rErr) ) r += p->rErr;
    }else{
      r = (double)(p->iSum);
    }
  }
  sqlite3_result_double(context, r);
}

/*
** The following structure keeps track of state information for the
** count() aggregate function.
*/
typedef struct CountCtx CountCtx;
struct CountCtx {
  i64 n;
#ifdef SQLITE_DEBUG
  int bInverse;                   /* True if xInverse() ever called */
#endif
};

/*
** Routines to implement the count() aggregate function.
*/
static void countStep(sqlite3_context *context, int argc, sqlite3_value **argv){
  CountCtx *p;
  p = sqlite3_aggregate_context(context, sizeof(*p));
  if( (argc==0 || SQLITE_NULL!=sqlite3_value_type(argv[0])) && p ){
    p->n++;
  }

#ifndef SQLITE_OMIT_DEPRECATED
  /* The sqlite3_aggregate_count() function is deprecated.  But just to make
  ** sure it still operates correctly, verify that its count agrees with our
  ** internal count when using count(*) and when the total count can be
  ** expressed as a 32-bit integer. */
  assert( argc==1 || p==0 || p->n>0x7fffffff || p->bInverse
          || p->n==sqlite3_aggregate_count(context) );
#endif
}  
static void countFinalize(sqlite3_context *context){
  CountCtx *p;
  p = sqlite3_aggregate_context(context, 0);
  sqlite3_result_int64(context, p ? p->n : 0);
}
#ifndef SQLITE_OMIT_WINDOWFUNC
static void countInverse(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  CountCtx *p;
  p = sqlite3_aggregate_context(ctx, sizeof(*p));
  /* p is always non-NULL since countStep() will have been called first */
  if( (argc==0 || SQLITE_NULL!=sqlite3_value_type(argv[0])) && ALWAYS(p) ){
    p->n--;
#ifdef SQLITE_DEBUG
    p->bInverse = 1;
#endif
  }
}  
#else
# define countInverse 0
#endif /* SQLITE_OMIT_WINDOWFUNC */

/*
** Routines to implement min() and max() aggregate functions.
*/
static void minmaxStep(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **argv
){
  Mem *pArg  = (Mem *)argv[0];
  Mem *pBest;
  UNUSED_PARAMETER(NotUsed);

  pBest = (Mem *)sqlite3_aggregate_context(context, sizeof(*pBest));
  if( !pBest ) return;

  if( sqlite3_value_type(pArg)==SQLITE_NULL ){
    if( pBest->flags ) sqlite3SkipAccumulatorLoad(context);
  }else if( pBest->flags ){
    int max;
    int cmp;
    CollSeq *pColl = sqlite3GetFuncCollSeq(context);
    /* This step function is used for both the min() and max() aggregates,
    ** the only difference between the two being that the sense of the
    ** comparison is inverted. For the max() aggregate, the
    ** sqlite3_user_data() function returns (void *)-1. For min() it
    ** returns (void *)db, where db is the sqlite3* database pointer.
    ** Therefore the next statement sets variable 'max' to 1 for the max()
    ** aggregate, or 0 for min().
    */
    max = sqlite3_user_data(context)!=0;
    cmp = sqlite3MemCompare(pBest, pArg, pColl);
    if( (max && cmp<0) || (!max && cmp>0) ){
      sqlite3VdbeMemCopy(pBest, pArg);
    }else{
      sqlite3SkipAccumulatorLoad(context);
    }
  }else{
    pBest->db = sqlite3_context_db_handle(context);
    sqlite3VdbeMemCopy(pBest, pArg);
  }
}
static void minMaxValueFinalize(sqlite3_context *context, int bValue){
  sqlite3_value *pRes;
  pRes = (sqlite3_value *)sqlite3_aggregate_context(context, 0);
  if( pRes ){
    if( pRes->flags ){
      sqlite3_result_value(context, pRes);
    }
    if( bValue==0 ) sqlite3VdbeMemRelease(pRes);
  }
}
#ifndef SQLITE_OMIT_WINDOWFUNC
static void minMaxValue(sqlite3_context *context){
  minMaxValueFinalize(context, 1);
}
#else
# define minMaxValue 0
#endif /* SQLITE_OMIT_WINDOWFUNC */
static void minMaxFinalize(sqlite3_context *context){
  minMaxValueFinalize(context, 0);
}

/*
** group_concat(EXPR, ?SEPARATOR?)
** string_agg(EXPR, SEPARATOR)
**
** Content is accumulated in GroupConcatCtx.str with the SEPARATOR
** coming before the EXPR value, except for the first entry which
** omits the SEPARATOR.
**
** It is tragic that the SEPARATOR goes before the EXPR string.  The
** groupConcatInverse() implementation would have been easier if the
** SEPARATOR were appended after EXPR.  And the order is undocumented,
** so we could change it, in theory.  But the old behavior has been
** around for so long that we dare not, for fear of breaking something.
*/
typedef struct {
  StrAccum str;          /* The accumulated concatenation */
#ifndef SQLITE_OMIT_WINDOWFUNC
  int nAccum;            /* Number of strings presently concatenated */
  int nFirstSepLength;   /* Used to detect separator length change */
  /* If pnSepLengths!=0, refs an array of inter-string separator lengths,
  ** stored as actually incorporated into presently accumulated result.
  ** (Hence, its slots in use number nAccum-1 between method calls.)
  ** If pnSepLengths==0, nFirstSepLength is the length used throughout.
  */
  int *pnSepLengths;
#endif
} GroupConcatCtx;

static void groupConcatStep(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const char *zVal;
  GroupConcatCtx *pGCC;
  const char *zSep;
  int nVal, nSep;
  assert( argc==1 || argc==2 );
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ) return;
  pGCC = (GroupConcatCtx*)sqlite3_aggregate_context(context, sizeof(*pGCC));
  if( pGCC ){
    sqlite3 *db = sqlite3_context_db_handle(context);
    int firstTerm = pGCC->str.mxAlloc==0;
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    struct sqlclntstate *clnt = get_sql_clnt();

    pGCC->str.mxAlloc = (clnt && clnt->group_concat_mem_limit != 0) ?
      clnt->group_concat_mem_limit : db->aLimit[SQLITE_LIMIT_LENGTH];
#else /* defined(SQLITE_BUILDING_FOR_COMDB2) */
    pGCC->str.mxAlloc = db->aLimit[SQLITE_LIMIT_LENGTH];
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
    if( argc==1 ){
      if( !firstTerm ){
        sqlite3_str_appendchar(&pGCC->str, 1, ',');
      }
#ifndef SQLITE_OMIT_WINDOWFUNC
      else{
        pGCC->nFirstSepLength = 1;
      }
#endif
    }else if( !firstTerm ){
      zSep = (char*)sqlite3_value_text(argv[1]);
      nSep = sqlite3_value_bytes(argv[1]);
      if( zSep ){
        sqlite3_str_append(&pGCC->str, zSep, nSep);
      }
#ifndef SQLITE_OMIT_WINDOWFUNC
      else{
        nSep = 0;
      }
      if( nSep != pGCC->nFirstSepLength || pGCC->pnSepLengths != 0 ){
        int *pnsl = pGCC->pnSepLengths;
        if( pnsl == 0 ){
          /* First separator length variation seen, start tracking them. */
          pnsl = (int*)sqlite3_malloc64((pGCC->nAccum+1) * sizeof(int));
          if( pnsl!=0 ){
            int i = 0, nA = pGCC->nAccum-1;
            while( i<nA ) pnsl[i++] = pGCC->nFirstSepLength;
          }
        }else{
          pnsl = (int*)sqlite3_realloc64(pnsl, pGCC->nAccum * sizeof(int));
        }
        if( pnsl!=0 ){
          if( ALWAYS(pGCC->nAccum>0) ){
            pnsl[pGCC->nAccum-1] = nSep;
          }
          pGCC->pnSepLengths = pnsl;
        }else{
          sqlite3StrAccumSetError(&pGCC->str, SQLITE_NOMEM);
        }
      }
#endif
    }
#ifndef SQLITE_OMIT_WINDOWFUNC
    else{
      pGCC->nFirstSepLength = sqlite3_value_bytes(argv[1]);
    }
    pGCC->nAccum += 1;
#endif
    zVal = (char*)sqlite3_value_text(argv[0]);
    nVal = sqlite3_value_bytes(argv[0]);
    if( zVal ) sqlite3_str_append(&pGCC->str, zVal, nVal);
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    // NC: Attach an error message here as we do not want client to see the
    // default error message 'transaction too big' emitted for ERR_TRAN_TOO_BIG,
    // which SQLITE_TOOBIG later gets translated to.
    if( clnt && pGCC->str.accError==SQLITE_TOOBIG ){
      clnt->sqlite_errstr = "string or blob too big";
    }
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  }
}

#ifndef SQLITE_OMIT_WINDOWFUNC
static void groupConcatInverse(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  GroupConcatCtx *pGCC;
  assert( argc==1 || argc==2 );
  (void)argc;  /* Suppress unused parameter warning */
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ) return;
  pGCC = (GroupConcatCtx*)sqlite3_aggregate_context(context, sizeof(*pGCC));
  /* pGCC is always non-NULL since groupConcatStep() will have always
  ** run first to initialize it */
  if( ALWAYS(pGCC) ){
    int nVS;  /* Number of characters to remove */
    /* Must call sqlite3_value_text() to convert the argument into text prior
    ** to invoking sqlite3_value_bytes(), in case the text encoding is UTF16 */
    (void)sqlite3_value_text(argv[0]);
    nVS = sqlite3_value_bytes(argv[0]);
    pGCC->nAccum -= 1;
    if( pGCC->pnSepLengths!=0 ){
      assert(pGCC->nAccum >= 0);
      if( pGCC->nAccum>0 ){
        nVS += *pGCC->pnSepLengths;
        memmove(pGCC->pnSepLengths, pGCC->pnSepLengths+1,
               (pGCC->nAccum-1)*sizeof(int));
      }
    }else{
      /* If removing single accumulated string, harmlessly over-do. */
      nVS += pGCC->nFirstSepLength;
    }
    if( nVS>=(int)pGCC->str.nChar ){
      pGCC->str.nChar = 0;
    }else{
      pGCC->str.nChar -= nVS;
      memmove(pGCC->str.zText, &pGCC->str.zText[nVS], pGCC->str.nChar);
    }
    if( pGCC->str.nChar==0 ){
      pGCC->str.mxAlloc = 0;
      sqlite3_free(pGCC->pnSepLengths);
      pGCC->pnSepLengths = 0;
    }
  }
}
#else
# define groupConcatInverse 0
#endif /* SQLITE_OMIT_WINDOWFUNC */
static void groupConcatFinalize(sqlite3_context *context){
  GroupConcatCtx *pGCC
    = (GroupConcatCtx*)sqlite3_aggregate_context(context, 0);
  if( pGCC ){
    sqlite3ResultStrAccum(context, &pGCC->str);
#ifndef SQLITE_OMIT_WINDOWFUNC
    sqlite3_free(pGCC->pnSepLengths);
#endif
  }
}
#ifndef SQLITE_OMIT_WINDOWFUNC
static void groupConcatValue(sqlite3_context *context){
  GroupConcatCtx *pGCC
    = (GroupConcatCtx*)sqlite3_aggregate_context(context, 0);
  if( pGCC ){
    StrAccum *pAccum = &pGCC->str;
    if( pAccum->accError==SQLITE_TOOBIG ){
      sqlite3_result_error_toobig(context);
    }else if( pAccum->accError==SQLITE_NOMEM ){
      sqlite3_result_error_nomem(context);
    }else if( pGCC->nAccum>0 && pAccum->nChar==0 ){
      sqlite3_result_text(context, "", 1, SQLITE_STATIC);
    }else{   
      const char *zText = sqlite3_str_value(pAccum);
      sqlite3_result_text(context, zText, pAccum->nChar, SQLITE_TRANSIENT);
    }
  }
}
#else
# define groupConcatValue 0
#endif /* SQLITE_OMIT_WINDOWFUNC */

/*
** This routine does per-connection function registration.  Most
** of the built-in functions above are part of the global function set.
** This routine only deals with those that are not global.
*/
void sqlite3RegisterPerConnectionBuiltinFunctions(sqlite3 *db){
  int rc = sqlite3_overload_function(db, "MATCH", 2);
  assert( rc==SQLITE_NOMEM || rc==SQLITE_OK );
  if( rc==SQLITE_NOMEM ){
    sqlite3OomFault(db);
  }
}

/*
** Re-register the built-in LIKE functions.  The caseSensitive
** parameter determines whether or not the LIKE operator is case
** sensitive.
*/
void sqlite3RegisterLikeFunctions(sqlite3 *db, int caseSensitive){
  FuncDef *pDef;
  struct compareInfo *pInfo;
  int flags;
  int nArg;
  if( caseSensitive ){
    pInfo = (struct compareInfo*)&likeInfoAlt;
    flags = SQLITE_FUNC_LIKE | SQLITE_FUNC_CASE;
  }else{
    pInfo = (struct compareInfo*)&likeInfoNorm;
    flags = SQLITE_FUNC_LIKE;
  }
  for(nArg=2; nArg<=3; nArg++){
    sqlite3CreateFunc(db, "like", nArg, SQLITE_UTF8, pInfo, likeFunc, 
                      0, 0, 0, 0, 0);
    pDef = sqlite3FindFunction(db, "like", nArg, SQLITE_UTF8, 0);
    pDef->funcFlags |= flags;
    pDef->funcFlags &= ~SQLITE_FUNC_UNSAFE;
  }
}

/*
** pExpr points to an expression which implements a function.  If
** it is appropriate to apply the LIKE optimization to that function
** then set aWc[0] through aWc[2] to the wildcard characters and the
** escape character and then return TRUE.  If the function is not a
** LIKE-style function then return FALSE.
**
** The expression "a LIKE b ESCAPE c" is only considered a valid LIKE
** operator if c is a string literal that is exactly one byte in length.
** That one byte is stored in aWc[3].  aWc[3] is set to zero if there is
** no ESCAPE clause.
**
** *pIsNocase is set to true if uppercase and lowercase are equivalent for
** the function (default for LIKE).  If the function makes the distinction
** between uppercase and lowercase (as does GLOB) then *pIsNocase is set to
** false.
*/
int sqlite3IsLikeFunction(sqlite3 *db, Expr *pExpr, int *pIsNocase, char *aWc){
  FuncDef *pDef;
  int nExpr;
  assert( pExpr!=0 );
  assert( pExpr->op==TK_FUNCTION );
  assert( ExprUseXList(pExpr) );
  if( !pExpr->x.pList ){
    return 0;
  }
  nExpr = pExpr->x.pList->nExpr;
  assert( !ExprHasProperty(pExpr, EP_IntValue) );
  pDef = sqlite3FindFunction(db, pExpr->u.zToken, nExpr, SQLITE_UTF8, 0);
#ifdef SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION
  if( pDef==0 ) return 0;
#endif
  if( NEVER(pDef==0) || (pDef->funcFlags & SQLITE_FUNC_LIKE)==0 ){
    return 0;
  }

  /* The memcpy() statement assumes that the wildcard characters are
  ** the first three statements in the compareInfo structure.  The
  ** asserts() that follow verify that assumption
  */
  memcpy(aWc, pDef->pUserData, 3);
  assert( (char*)&likeInfoAlt == (char*)&likeInfoAlt.matchAll );
  assert( &((char*)&likeInfoAlt)[1] == (char*)&likeInfoAlt.matchOne );
  assert( &((char*)&likeInfoAlt)[2] == (char*)&likeInfoAlt.matchSet );

  if( nExpr<3 ){
    aWc[3] = 0;
  }else{
    Expr *pEscape = pExpr->x.pList->a[2].pExpr;
    char *zEscape;
    if( pEscape->op!=TK_STRING ) return 0;
    assert( !ExprHasProperty(pEscape, EP_IntValue) );
    zEscape = pEscape->u.zToken;
    if( zEscape[0]==0 || zEscape[1]!=0 ) return 0;
    if( zEscape[0]==aWc[0] ) return 0;
    if( zEscape[0]==aWc[1] ) return 0;
    aWc[3] = zEscape[0];
  }

  *pIsNocase = (pDef->funcFlags & SQLITE_FUNC_CASE)==0;
  return 1;
}

/* Mathematical Constants */
#ifndef M_PI
# define M_PI   3.141592653589793238462643383279502884
#endif
#ifndef M_LN10
# define M_LN10 2.302585092994045684017991454684364208
#endif
#ifndef M_LN2
# define M_LN2  0.693147180559945309417232121458176568
#endif


/* Extra math functions that require linking with -lm
*/
#ifdef SQLITE_ENABLE_MATH_FUNCTIONS
/*
** Implementation SQL functions:
**
**   ceil(X)
**   ceiling(X)
**   floor(X)
**
** The sqlite3_user_data() pointer is a pointer to the libm implementation
** of the underlying C function.
*/
static void ceilingFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert( argc==1 );
  switch( sqlite3_value_numeric_type(argv[0]) ){
    case SQLITE_INTEGER: {
       sqlite3_result_int64(context, sqlite3_value_int64(argv[0]));
       break;
    }
    case SQLITE_FLOAT: {
       double (*x)(double) = (double(*)(double))sqlite3_user_data(context);
       sqlite3_result_double(context, x(sqlite3_value_double(argv[0])));
       break;
    }
    default: {
       break;
    }
  }
}

/*
** On some systems, ceil() and floor() are intrinsic function.  You are
** unable to take a pointer to these functions.  Hence, we here wrap them
** in our own actual functions.
*/
static double xCeil(double x){ return ceil(x); }
static double xFloor(double x){ return floor(x); }

/*
** Some systems do not have log2() and log10() in their standard math
** libraries.
*/
#if defined(HAVE_LOG10) && HAVE_LOG10==0
# define log10(X) (0.4342944819032517867*log(X))
#endif
#if defined(HAVE_LOG2) && HAVE_LOG2==0
# define log2(X) (1.442695040888963456*log(X))
#endif


/*
** Implementation of SQL functions:
**
**   ln(X)       - natural logarithm
**   log(X)      - log X base 10
**   log10(X)    - log X base 10
**   log(B,X)    - log X base B
*/
static void logFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  double x, b, ans;
  assert( argc==1 || argc==2 );
  switch( sqlite3_value_numeric_type(argv[0]) ){
    case SQLITE_INTEGER:
    case SQLITE_FLOAT:
      x = sqlite3_value_double(argv[0]);
      if( x<=0.0 ) return;
      break;
    default:
      return;
  }
  if( argc==2 ){
    switch( sqlite3_value_numeric_type(argv[0]) ){
      case SQLITE_INTEGER:
      case SQLITE_FLOAT:
        b = log(x);
        if( b<=0.0 ) return;
        x = sqlite3_value_double(argv[1]);
        if( x<=0.0 ) return;
        break;
     default:
        return;
    }
    ans = log(x)/b;
  }else{
    switch( SQLITE_PTR_TO_INT(sqlite3_user_data(context)) ){
      case 1:
        ans = log10(x);
        break;
      case 2:
        ans = log2(x);
        break;
      default:
        ans = log(x);
        break;
    }
  }
  sqlite3_result_double(context, ans);
}

/*
** Functions to converts degrees to radians and radians to degrees.
*/
static double degToRad(double x){ return x*(M_PI/180.0); }
static double radToDeg(double x){ return x*(180.0/M_PI); }

/*
** Implementation of 1-argument SQL math functions:
**
**   exp(X)  - Compute e to the X-th power
*/
static void math1Func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int type0;
  double v0, ans;
  double (*x)(double);
  assert( argc==1 );
  type0 = sqlite3_value_numeric_type(argv[0]);
  if( type0!=SQLITE_INTEGER && type0!=SQLITE_FLOAT ) return;
  v0 = sqlite3_value_double(argv[0]);
  x = (double(*)(double))sqlite3_user_data(context);
  ans = x(v0);
  sqlite3_result_double(context, ans);
}

/*
** Implementation of 2-argument SQL math functions:
**
**   power(X,Y)  - Compute X to the Y-th power
*/
static void math2Func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int type0, type1;
  double v0, v1, ans;
  double (*x)(double,double);
  assert( argc==2 );
  type0 = sqlite3_value_numeric_type(argv[0]);
  if( type0!=SQLITE_INTEGER && type0!=SQLITE_FLOAT ) return;
  type1 = sqlite3_value_numeric_type(argv[1]);
  if( type1!=SQLITE_INTEGER && type1!=SQLITE_FLOAT ) return;
  v0 = sqlite3_value_double(argv[0]);
  v1 = sqlite3_value_double(argv[1]);
  x = (double(*)(double,double))sqlite3_user_data(context);
  ans = x(v0, v1);
  sqlite3_result_double(context, ans);
}

/*
** Implementation of 0-argument pi() function.
*/
static void piFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert( argc==0 );
  (void)argv;
  sqlite3_result_double(context, M_PI);
}

#endif /* SQLITE_ENABLE_MATH_FUNCTIONS */

/*
** Implementation of sign(X) function.
*/
static void signFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int type0;
  double x;
  UNUSED_PARAMETER(argc);
  assert( argc==1 );
  type0 = sqlite3_value_numeric_type(argv[0]);
  if( type0!=SQLITE_INTEGER && type0!=SQLITE_FLOAT ) return;
  x = sqlite3_value_double(argv[0]);
  sqlite3_result_int(context, x<0.0 ? -1 : x>0.0 ? +1 : 0);
}

#if defined(SQLITE_ENABLE_PERCENTILE)
/***********************************************************************
** This section implements the percentile(Y,P) SQL function and similar.
** Requirements:
**
**   (1)  The percentile(Y,P) function is an aggregate function taking
**        exactly two arguments.
**
**   (2)  If the P argument to percentile(Y,P) is not the same for every
**        row in the aggregate then an error is thrown.  The word "same"
**        in the previous sentence means that the value differ by less
**        than 0.001.
**
**   (3)  If the P argument to percentile(Y,P) evaluates to anything other
**        than a number in the range of 0.0 to 100.0 inclusive then an
**        error is thrown.
**
**   (4)  If any Y argument to percentile(Y,P) evaluates to a value that
**        is not NULL and is not numeric then an error is thrown.
**
**   (5)  If any Y argument to percentile(Y,P) evaluates to plus or minus
**        infinity then an error is thrown.  (SQLite always interprets NaN
**        values as NULL.)
**
**   (6)  Both Y and P in percentile(Y,P) can be arbitrary expressions,
**        including CASE WHEN expressions.
**
**   (7)  The percentile(Y,P) aggregate is able to handle inputs of at least
**        one million (1,000,000) rows.
**
**   (8)  If there are no non-NULL values for Y, then percentile(Y,P)
**        returns NULL.
**
**   (9)  If there is exactly one non-NULL value for Y, the percentile(Y,P)
**        returns the one Y value.
**
**  (10)  If there N non-NULL values of Y where N is two or more and
**        the Y values are ordered from least to greatest and a graph is
**        drawn from 0 to N-1 such that the height of the graph at J is
**        the J-th Y value and such that straight lines are drawn between
**        adjacent Y values, then the percentile(Y,P) function returns
**        the height of the graph at P*(N-1)/100.
**
**  (11)  The percentile(Y,P) function always returns either a floating
**        point number or NULL.
**
**  (12)  The percentile(Y,P) is implemented as a single C99 source-code
**        file that compiles into a shared-library or DLL that can be loaded
**        into SQLite using the sqlite3_load_extension() interface.
**
**  (13)  A separate median(Y) function is the equivalent percentile(Y,50).
**
**  (14)  A separate percentile_cont(Y,P) function is equivalent to
**        percentile(Y,P/100.0).  In other words, the fraction value in
**        the second argument is in the range of 0 to 1 instead of 0 to 100.
**
**  (15)  A separate percentile_disc(Y,P) function is like
**        percentile_cont(Y,P) except that instead of returning the weighted
**        average of the nearest two input values, it returns the next lower
**        value.  So the percentile_disc(Y,P) will always return a value
**        that was one of the inputs.
**
**  (16)  All of median(), percentile(Y,P), percentile_cont(Y,P) and
**        percentile_disc(Y,P) can be used as window functions.
**
** Differences from standard SQL:
**
**  *  The percentile_cont(X,P) function is equivalent to the following in
**     standard SQL:
**
**         (percentile_cont(P) WITHIN GROUP (ORDER BY X))
**
**     The SQLite syntax is much more compact.  The standard SQL syntax
**     is also supported if SQLite is compiled with the
**     -DSQLITE_ENABLE_ORDERED_SET_AGGREGATES option.
**
**  *  No median(X) function exists in the SQL standard.  App developers
**     are expected to write "percentile_cont(0.5)WITHIN GROUP(ORDER BY X)".
**
**  *  No percentile(Y,P) function exists in the SQL standard.  Instead of
**     percential(Y,P), developers must write this:
**     "percentile_cont(P/100.0) WITHIN GROUP (ORDER BY Y)".  Note that
**     the fraction parameter to percentile() goes from 0 to 100 whereas
**     the fraction parameter in SQL standard percentile_cont() goes from
**     0 to 1.
**
** Implementation notes as of 2024-08-31:
**
**  *  The regular aggregate-function versions of these routines work
**     by accumulating all values in an array of doubles, then sorting
**     that array using quicksort before computing the answer. Thus
**     the runtime is O(NlogN) where N is the number of rows of input.
**
**  *  For the window-function versions of these routines, the array of
**     inputs is sorted as soon as the first value is computed.  Thereafter,
**     the array is kept in sorted order using an insert-sort.  This
**     results in O(N*K) performance where K is the size of the window.
**     One can imagine alternative implementations that give O(N*logN*logK)
**     performance, but they require more complex logic and data structures.
**     The developers have elected to keep the asymptotically slower
**     algorithm for now, for simplicity, under the theory that window
**     functions are seldom used and when they are, the window size K is
**     often small.  The developers might revisit that decision later,
**     should the need arise.
*/

/* The following object is the group context for a single percentile()
** aggregate.  Remember all input Y values until the very end.
** Those values are accumulated in the Percentile.a[] array.
*/
typedef struct Percentile Percentile;
struct Percentile {
  u64 nAlloc;          /* Number of slots allocated for a[] */
  u64 nUsed;           /* Number of slots actually used in a[] */
  char bSorted;        /* True if a[] is already in sorted order */
  char bKeepSorted;    /* True if advantageous to keep a[] sorted */
  char bPctValid;      /* True if rPct is valid */
  double rPct;         /* Fraction.  0.0 to 1.0 */
  double *a;           /* Array of Y values */
};

/*
** Return TRUE if the input floating-point number is an infinity.
*/
static int percentIsInfinity(double r){
  sqlite3_uint64 u;
  assert( sizeof(u)==sizeof(r) );
  memcpy(&u, &r, sizeof(u));
  return ((u>>52)&0x7ff)==0x7ff;
}

/*
** Return TRUE if two doubles differ by 0.001 or less.
*/
static int percentSameValue(double a, double b){
  a -= b;
  return a>=-0.001 && a<=0.001;
}

/*
** Search p (which must have p->bSorted) looking for an entry with
** value y.  Return the index of that entry.
**
** If bExact is true, return -1 if the entry is not found.
**
** If bExact is false, return the index at which a new entry with
** value y should be insert in order to keep the values in sorted
** order.  The smallest return value in this case will be 0, and
** the largest return value will be p->nUsed.
*/
static i64 percentBinarySearch(Percentile *p, double y, int bExact){
  i64 iFirst = 0;                   /* First element of search range */
  i64 iLast = (i64)p->nUsed - 1;    /* Last element of search range */
  while( iLast>=iFirst ){
    i64 iMid = (iFirst+iLast)/2;
    double x = p->a[iMid];
    if( x<y ){
      iFirst = iMid + 1;
    }else if( x>y ){
      iLast = iMid - 1;
    }else{
      return iMid;
    }
  }
  if( bExact ) return -1;
  return iFirst;
}

/*
** Generate an error for a percentile function.
**
** The error format string must have exactly one occurrence of "%%s()"
** (with two '%' characters).  That substring will be replaced by the name
** of the function.
*/
static void percentError(sqlite3_context *pCtx, const char *zFormat, ...){
  char *zMsg1;
  char *zMsg2;
  va_list ap;

  va_start(ap, zFormat);
  zMsg1 = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  zMsg2 = zMsg1 ? sqlite3_mprintf(zMsg1, sqlite3VdbeFuncName(pCtx)) : 0;
  sqlite3_result_error(pCtx, zMsg2, -1);
  sqlite3_free(zMsg1);
  sqlite3_free(zMsg2);
}

/*
** The "step" function for percentile(Y,P) is called once for each
** input row.
*/
static void percentStep(sqlite3_context *pCtx, int argc, sqlite3_value **argv){
  Percentile *p;
  double rPct;
  int eType;
  double y;
  assert( argc==2 || argc==1 );

  if( argc==1 ){
    /* Requirement 13:  median(Y) is the same as percentile(Y,50). */
    rPct = 0.5;
  }else{
    /* P must be a number between 0 and 100 for percentile() or between
    ** 0.0 and 1.0 for percentile_cont() and percentile_disc().
    **
    ** The user-data is an integer which is 10 times the upper bound.
    */
    double mxFrac = (SQLITE_PTR_TO_INT(sqlite3_user_data(pCtx))&2)? 100.0 : 1.0;
    eType = sqlite3_value_numeric_type(argv[1]);
    rPct = sqlite3_value_double(argv[1])/mxFrac;
    if( (eType!=SQLITE_INTEGER && eType!=SQLITE_FLOAT)
     || rPct<0.0 || rPct>1.0
    ){
      percentError(pCtx, "the fraction argument to %%s()"
                        " is not between 0.0 and %.1f",
                        (double)mxFrac);
      return;
    }
  }

  /* Allocate the session context. */
  p = (Percentile*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p==0 ) return;

  /* Remember the P value.  Throw an error if the P value is different
  ** from any prior row, per Requirement (2). */
  if( !p->bPctValid ){
    p->rPct = rPct;
    p->bPctValid = 1;
  }else if( !percentSameValue(p->rPct,rPct) ){
    percentError(pCtx, "the fraction argument to %%s()"
                      " is not the same for all input rows");
    return;
  }

  /* Ignore rows for which Y is NULL */
  eType = sqlite3_value_type(argv[0]);
  if( eType==SQLITE_NULL ) return;

  /* If not NULL, then Y must be numeric.  Otherwise throw an error.
  ** Requirement 4 */
  if( eType!=SQLITE_INTEGER && eType!=SQLITE_FLOAT ){
    percentError(pCtx, "input to %%s() is not numeric");
    return;
  }

  /* Throw an error if the Y value is infinity or NaN */
  y = sqlite3_value_double(argv[0]);
  if( percentIsInfinity(y) ){
    percentError(pCtx, "Inf input to %%s()");
    return;
  }

  /* Allocate and store the Y */
  if( p->nUsed>=p->nAlloc ){
    u64 n = p->nAlloc*2 + 250;
    double *a = sqlite3_realloc64(p->a, sizeof(double)*n);
    if( a==0 ){
      sqlite3_free(p->a);
      memset(p, 0, sizeof(*p));
      sqlite3_result_error_nomem(pCtx);
      return;
    }
    p->nAlloc = n;
    p->a = a;
  }
  if( p->nUsed==0 ){
    p->a[p->nUsed++] = y;
    p->bSorted = 1;
  }else if( !p->bSorted || y>=p->a[p->nUsed-1] ){
    p->a[p->nUsed++] = y;
  }else if( p->bKeepSorted ){
    i64 i;
    i = percentBinarySearch(p, y, 0);
    if( i<(int)p->nUsed ){
      memmove(&p->a[i+1], &p->a[i], (p->nUsed-i)*sizeof(p->a[0]));
    }
    p->a[i] = y;
    p->nUsed++;
  }else{
    p->a[p->nUsed++] = y;
    p->bSorted = 0;
  }
}

/*
** Interchange two doubles.
*/
#define SWAP_DOUBLE(X,Y)  {double ttt=(X);(X)=(Y);(Y)=ttt;}

/*
** Sort an array of doubles.
**
** Algorithm: quicksort
**
** This is implemented separately rather than using the qsort() routine
** from the standard library because:
**
**    (1)  To avoid a dependency on qsort()
**    (2)  To avoid the function call to the comparison routine for each
**         comparison.
*/
static void percentSort(double *a, unsigned int n){
  int iLt;  /* Entries before a[iLt] are less than rPivot */
  int iGt;  /* Entries at or after a[iGt] are greater than rPivot */
  int i;         /* Loop counter */
  double rPivot; /* The pivot value */
  
  assert( n>=2 );
  if( a[0]>a[n-1] ){
    SWAP_DOUBLE(a[0],a[n-1])
  }
  if( n==2 ) return;
  iGt = n-1;
  i = n/2;
  if( a[0]>a[i] ){
    SWAP_DOUBLE(a[0],a[i])
  }else if( a[i]>a[iGt] ){
    SWAP_DOUBLE(a[i],a[iGt])
  }
  if( n==3 ) return;
  rPivot = a[i];
  iLt = i = 1;
  do{
    if( a[i]<rPivot ){
      if( i>iLt ) SWAP_DOUBLE(a[i],a[iLt])
      iLt++;
      i++;
    }else if( a[i]>rPivot ){
      do{
        iGt--;
      }while( iGt>i && a[iGt]>rPivot );
      SWAP_DOUBLE(a[i],a[iGt])
    }else{
      i++;
    }
  }while( i<iGt );
  if( iLt>=2 ) percentSort(a, iLt);
  if( n-iGt>=2 ) percentSort(a+iGt, n-iGt);
    
/* Uncomment for testing */
#if 0
  for(i=0; i<n-1; i++){
    assert( a[i]<=a[i+1] );
  }
#endif
}


/*
** The "inverse" function for percentile(Y,P) is called to remove a
** row that was previously inserted by "step".
*/
static void percentInverse(sqlite3_context *pCtx,int argc,sqlite3_value **argv){
  Percentile *p;
  int eType;
  double y;
  i64 i;
  assert( argc==2 || argc==1 );

  /* Allocate the session context. */
  p = (Percentile*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  assert( p!=0 );

  /* Ignore rows for which Y is NULL */
  eType = sqlite3_value_type(argv[0]);
  if( eType==SQLITE_NULL ) return;

  /* If not NULL, then Y must be numeric.  Otherwise throw an error.
  ** Requirement 4 */
  if( eType!=SQLITE_INTEGER && eType!=SQLITE_FLOAT ){
    return;
  }

  /* Ignore the Y value if it is infinity or NaN */
  y = sqlite3_value_double(argv[0]);
  if( percentIsInfinity(y) ){
    return;
  }
  if( p->bSorted==0 ){
    assert( p->nUsed>1 );
    percentSort(p->a, p->nUsed);
    p->bSorted = 1;
  }
  p->bKeepSorted = 1;

  /* Find and remove the row */
  i = percentBinarySearch(p, y, 1);
  if( i>=0 ){
    p->nUsed--;
    if( i<(int)p->nUsed ){
      memmove(&p->a[i], &p->a[i+1], (p->nUsed - i)*sizeof(p->a[0]));
    }
  }
}

/*
** Compute the final output of percentile().  Clean up all allocated
** memory if and only if bIsFinal is true.
*/
static void percentCompute(sqlite3_context *pCtx, int bIsFinal){
  Percentile *p;
  int settings = SQLITE_PTR_TO_INT(sqlite3_user_data(pCtx))&1; /* Discrete? */
  unsigned i1, i2;
  double v1, v2;
  double ix, vx;
  p = (Percentile*)sqlite3_aggregate_context(pCtx, 0);
  if( p==0 ) return;
  if( p->a==0 ) return;
  if( p->nUsed ){
    if( p->bSorted==0 ){
      assert( p->nUsed>1 );
      percentSort(p->a, p->nUsed);
      p->bSorted = 1;
    }
    ix = p->rPct*(p->nUsed-1);
    i1 = (unsigned)ix;
    if( settings & 1 ){
      vx = p->a[i1];
    }else{
      i2 = ix==(double)i1 || i1==p->nUsed-1 ? i1 : i1+1;
      v1 = p->a[i1];
      v2 = p->a[i2];
      vx = v1 + (v2-v1)*(ix-i1);
    }
    sqlite3_result_double(pCtx, vx);
  }
  if( bIsFinal ){
    sqlite3_free(p->a);
    memset(p, 0, sizeof(*p));
  }else{
    p->bKeepSorted = 1;
  }
}
static void percentFinal(sqlite3_context *pCtx){
  percentCompute(pCtx, 1);
}
static void percentValue(sqlite3_context *pCtx){
  percentCompute(pCtx, 0);
}
/****** End of percentile family of functions ******/
#endif /* SQLITE_ENABLE_PERCENTILE */

#if defined(SQLITE_DEBUG) || defined(SQLITE_ENABLE_FILESTAT)
/*
** Implementation of sqlite_filestat(SCHEMA).
**
** Return JSON text that describes low-level debug/diagnostic information
** about the sqlite3_file object associated with SCHEMA.
*/
static void filestatFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  const char *zDbName;
  sqlite3_str *pStr;
  Btree *pBtree;

  zDbName = (const char*)sqlite3_value_text(argv[0]);
  pBtree = sqlite3DbNameToBtree(db, zDbName);
  if( pBtree ){
    Pager *pPager;
    sqlite3_file *fd;
    int rc;
    sqlite3BtreeEnter(pBtree);
    pPager = sqlite3BtreePager(pBtree);
    assert( pPager!=0 );
    fd = sqlite3PagerFile(pPager);
    pStr = sqlite3_str_new(db);
    if( pStr==0 ){
      sqlite3_result_error_nomem(context);
    }else{
      sqlite3_str_append(pStr, "{\"db\":", 6);
      rc = sqlite3OsFileControl(fd, SQLITE_FCNTL_FILESTAT, pStr);
      if( rc ) sqlite3_str_append(pStr, "null", 4);
      fd = sqlite3PagerJrnlFile(pPager);
      if( fd && fd->pMethods!=0 ){
        sqlite3_str_appendall(pStr, ",\"journal\":");
        rc = sqlite3OsFileControl(fd, SQLITE_FCNTL_FILESTAT, pStr);
        if( rc ) sqlite3_str_append(pStr, "null", 4);
      }
      sqlite3_str_append(pStr, "}", 1);
      sqlite3_result_text(context, sqlite3_str_finish(pStr), -1,
                          sqlite3_free);
    }
    sqlite3BtreeLeave(pBtree);
  }else{
    sqlite3_result_text(context, "{}", 2, SQLITE_STATIC);
  }
}
#endif /* SQLITE_DEBUG || SQLITE_ENABLE_FILESTAT */

#ifdef SQLITE_DEBUG
/*
** Implementation of fpdecode(x,y,z) function.
**
** x is a real number that is to be decoded.  y is the precision.
** z is the maximum real precision.  Return a string that shows the
** results of the sqlite3FpDecode() function.
**
** Used for testing and debugging only, specifically testing and debugging
** of the sqlite3FpDecode() function.  This SQL function does not appear
** in production builds.  This function is not an API and is subject to
** modification or removal in future versions of SQLite.
*/
static void fpdecodeFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  FpDecode s;
  double x;
  int y, z;
  char zBuf[100];
  UNUSED_PARAMETER(argc);
  assert( argc==3 );
  x = sqlite3_value_double(argv[0]);
  y = sqlite3_value_int(argv[1]);
  z = sqlite3_value_int(argv[2]);
  if( z<=0 ) z = 1;
  sqlite3FpDecode(&s, x, y, z);
  if( s.isSpecial==2 ){
    sqlite3_snprintf(sizeof(zBuf), zBuf, "NaN");
  }else{
    sqlite3_snprintf(sizeof(zBuf), zBuf, "%c%.*s/%d", s.sign, s.n, s.z, s.iDP);
  }
  sqlite3_result_text(context, zBuf, -1, SQLITE_TRANSIENT);
}
#endif /* SQLITE_DEBUG */

#ifdef SQLITE_DEBUG
/*
** Implementation of parseuri(uri,flags) function.
**
** Required Arguments:
**    "uri"        The URI to parse.
**    "flags"      Bitmask of flags, as if to sqlite3_open_v2().
**
** Additional arguments beyond the first two make calls to
** sqlite3_uri_key() for integers and sqlite3_uri_parameter for
** anything else.
**
** The result is a string showing the results of calling sqlite3ParseUri().
**
** Used for testing and debugging only, specifically testing and debugging
** of the sqlite3ParseUri() function.  This SQL function does not appear
** in production builds.  This function is not an API and is subject to
** modification or removal in future versions of SQLite.
*/
static void parseuriFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  sqlite3_str *pResult;
  const char *zVfs;
  const char *zUri;
  unsigned int flgs;
  int rc;
  sqlite3_vfs *pVfs = 0;
  char *zFile = 0;
  char *zErr = 0;

  if( argc<2 ) return;
  pVfs = sqlite3_vfs_find(0);
  assert( pVfs );
  zVfs = pVfs->zName;
  zUri = (const char*)sqlite3_value_text(argv[0]);
  if( zUri==0 ) return;
  flgs = (unsigned int)sqlite3_value_int(argv[1]);
  rc = sqlite3ParseUri(zVfs, zUri, &flgs, &pVfs, &zFile, &zErr);
  pResult = sqlite3_str_new(0);
  if( pResult ){
    int i;
    sqlite3_str_appendf(pResult, "rc=%d", rc);
    sqlite3_str_appendf(pResult, ", flags=0x%x", flgs);
    sqlite3_str_appendf(pResult, ", vfs=%Q", pVfs ? pVfs->zName: 0);
    sqlite3_str_appendf(pResult, ", err=%Q", zErr);
    sqlite3_str_appendf(pResult, ", file=%Q", zFile);
    if( zFile ){
      const char *z = zFile;
      z += sqlite3Strlen30(z)+1;
      while( z[0] ){
        sqlite3_str_appendf(pResult, ", %Q", z);
        z += sqlite3Strlen30(z)+1;
      }
      for(i=2; i<argc; i++){
        const char *zArg;
        if( sqlite3_value_type(argv[i])==SQLITE_INTEGER ){
          int k = sqlite3_value_int(argv[i]);
          sqlite3_str_appendf(pResult, ", '%d:%q'",k,sqlite3_uri_key(zFile, k));
        }else if( (zArg = (const char*)sqlite3_value_text(argv[i]))!=0 ){
          sqlite3_str_appendf(pResult, ", '%q:%q'",
                 zArg, sqlite3_uri_parameter(zFile,zArg));
        }else{
          sqlite3_str_appendf(pResult, ", NULL");
        }
      }
    }
    sqlite3_result_text(ctx, sqlite3_str_finish(pResult), -1, sqlite3_free);
  }
  sqlite3_free_filename(zFile);
  sqlite3_free(zErr);
}
#endif /* SQLITE_DEBUG */

#if defined(SQLITE_BUILDING_FOR_COMDB2)
#if defined(SQLITE_BUILDING_FOR_COMDB2_DBGLOG)
static void dbglogCookieFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  uint64_t comdb2fastseed(int);
  sqlite3_result_int64(context, (i64)comdb2fastseed(4));
}

static void dbglogBeginFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  UNUSED_PARAMETER(argc);
  int dbglog_begin(const char *);
  sqlite3_result_int(context, dbglog_begin((const char *)sqlite3_value_text(argv[0])));
}

static void dbglogEndFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  unsigned long long cookie;
  void *mmapped;
  size_t size;
  int fd;
  int rc;
  int dbglog_mmap_dbglog_file(unsigned long long, void **, size_t *, int *);
  int dbglog_munmap_dbglog_file(unsigned long long, void *, size_t, int);

  UNUSED_PARAMETER(argc);
  cookie = (unsigned long long)sqlite3_value_int64(argv[0]);

  rc = dbglog_mmap_dbglog_file(cookie, &mmapped, &size, &fd);
  if (rc != 0)
      sqlite3_result_null(context);
  else {
      sqlite3_result_blob(context, mmapped, size, SQLITE_TRANSIENT);
      (void)dbglog_munmap_dbglog_file(cookie, mmapped, size, fd);
  }
}
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2_DBGLOG) */
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */

#if defined(SQLITE_BUILDING_FOR_COMDB2)
extern i64 comdb2_last_stmt_cost(void);
static void comdb2LastCostFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  i64 cost;
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  cost = comdb2_last_stmt_cost();
  sqlite3_result_int64(context, cost);
}

#endif

/*
** All of the FuncDef structures in the aBuiltinFunc[] array above
** to the global function hash table.  This occurs at start-time (as
** a consequence of calling sqlite3_initialize()).
**
** After this routine runs
*/
void sqlite3RegisterBuiltinFunctions(void){
  /*
  ** The following array holds FuncDef structures for all of the functions
  ** defined in this file.
  **
  ** The array cannot be constant since changes are made to the
  ** FuncDef.pHash elements at start-time.  The elements of this array
  ** are read-only after initialization is complete.
  **
  ** For peak efficiency, put the most frequently used function last.
  */
  static FuncDef aBuiltinFunc[] = {
/***** Functions only available with SQLITE_TESTCTRL_INTERNAL_FUNCTIONS *****/
#if !defined(SQLITE_UNTESTABLE)
    TEST_FUNC(implies_nonnull_row, 2, INLINEFUNC_implies_nonnull_row, 0),
    TEST_FUNC(expr_compare,        2, INLINEFUNC_expr_compare,        0),
    TEST_FUNC(expr_implies_expr,   2, INLINEFUNC_expr_implies_expr,   0),
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
    TEST_FUNC(affinity,            1, INLINEFUNC_affinity,            0),
#endif /* !defined(SQLITE_BUILDING_FOR_COMDB2) */
#endif /* !defined(SQLITE_UNTESTABLE) */
/***** Regular functions *****/
#ifdef SQLITE_SOUNDEX
    FUNCTION(soundex,            1, 0, 0, soundexFunc      ),
#endif
#ifndef SQLITE_OMIT_LOAD_EXTENSION
    SFUNCTION(load_extension,    1, 0, 0, loadExt          ),
    SFUNCTION(load_extension,    2, 0, 0, loadExt          ),
#endif
#ifndef SQLITE_OMIT_COMPILEOPTION_DIAGS
    DFUNCTION(sqlite_compileoption_used,1, 0, 0, compileoptionusedFunc  ),
    DFUNCTION(sqlite_compileoption_get, 1, 0, 0, compileoptiongetFunc  ),
#endif /* SQLITE_OMIT_COMPILEOPTION_DIAGS */
    INLINE_FUNC(unlikely,        1, INLINEFUNC_unlikely, SQLITE_FUNC_UNLIKELY),
    INLINE_FUNC(likelihood,      2, INLINEFUNC_unlikely, SQLITE_FUNC_UNLIKELY),
    INLINE_FUNC(likely,          1, INLINEFUNC_unlikely, SQLITE_FUNC_UNLIKELY),
#ifdef SQLITE_ENABLE_OFFSET_SQL_FUNC
    INLINE_FUNC(sqlite_offset,   1, INLINEFUNC_sqlite_offset, 0 ),
#endif
#if defined(SQLITE_DEBUG) || defined(SQLITE_ENABLE_FILESTAT)
    FUNCTION(sqlite_filestat,    1, 0, 0, filestatFunc     ),
#endif
    FUNCTION(ltrim,              1, 1, 0, trimFunc         ),
    FUNCTION(ltrim,              2, 1, 0, trimFunc         ),
    FUNCTION(rtrim,              1, 2, 0, trimFunc         ),
    FUNCTION(rtrim,              2, 2, 0, trimFunc         ),
    FUNCTION(trim,               1, 3, 0, trimFunc         ),
    FUNCTION(trim,               2, 3, 0, trimFunc         ),
    FUNCTION(min,               -3, 0, 1, minmaxFunc       ),
    WAGGREGATE(min, 1, 0, 1, minmaxStep, minMaxFinalize, minMaxValue, 0,
                                 SQLITE_FUNC_MINMAX|SQLITE_FUNC_ANYORDER ),
    FUNCTION(max,               -3, 1, 1, minmaxFunc       ),
    WAGGREGATE(max, 1, 1, 1, minmaxStep, minMaxFinalize, minMaxValue, 0,
                                 SQLITE_FUNC_MINMAX|SQLITE_FUNC_ANYORDER ),
    FUNCTION2(typeof,            1, 0, 0, typeofFunc,  SQLITE_FUNC_TYPEOF),
    FUNCTION2(subtype,           1, 0, 0, subtypeFunc,
                                           SQLITE_FUNC_TYPEOF|SQLITE_SUBTYPE),
    FUNCTION2(length,            1, 0, 0, lengthFunc,  SQLITE_FUNC_LENGTH),
    FUNCTION2(octet_length,      1, 0, 0, bytelengthFunc,SQLITE_FUNC_BYTELEN),
    FUNCTION(instr,              2, 0, 0, instrFunc        ),
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    VFUNCTION(sleep,             1, 0, 0, sleepFunc        ),
    VFUNCTION(usleep,            1, 0, 0, usleepFunc       ),
    VFUNCTION(comdb2_extract_table_names,
                                 1, 0, 0, tableNamesFunc   ),
    VFUNCTION(comdb2_normalize_sql,
                                 1, 0, 0, normalizeSqlFunc ),
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
    FUNCTION(printf,            -1, 0, 0, printfFunc       ),
    FUNCTION(format,            -1, 0, 0, printfFunc       ),
    FUNCTION(unicode,            1, 0, 0, unicodeFunc      ),
    FUNCTION(char,              -1, 0, 0, charFunc         ),
    FUNCTION(abs,                1, 0, 0, absFunc          ),
#ifdef SQLITE_DEBUG
    FUNCTION(fpdecode,           3, 0, 0, fpdecodeFunc     ),
    FUNCTION(parseuri,          -1, 0, 0, parseuriFunc     ),
#endif
#ifndef SQLITE_OMIT_FLOATING_POINT
    FUNCTION(round,              1, 0, 0, roundFunc        ),
    FUNCTION(round,              2, 0, 0, roundFunc        ),
#endif
    FUNCTION(upper,              1, 0, 0, upperFunc        ),
    FUNCTION(lower,              1, 0, 0, lowerFunc        ),
    FUNCTION(hex,                1, 0, 0, hexFunc          ),
    FUNCTION(unhex,              1, 0, 0, unhexFunc        ),
    FUNCTION(unhex,              2, 0, 0, unhexFunc        ),
    FUNCTION(concat,            -3, 0, 0, concatFunc       ),
    FUNCTION(concat_ws,         -4, 0, 0, concatwsFunc     ),
    INLINE_FUNC(ifnull,          2, INLINEFUNC_coalesce, 0 ),
    VFUNCTION(random,            0, 0, 0, randomFunc       ),
    VFUNCTION(randomblob,        1, 0, 0, randomBlob       ),
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    VFUNCTION(guid,              0, 0, 0, guidFunc         ),
    VFUNCTION(guid_str,          0, 0, 0, guidStrFunc      ),
    FUNCTION(guid,               1, 0, 0, guidFromStrFunc  ),
    FUNCTION(guid_str,           1, 0, 0, guidFromByteFunc ),
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
    FUNCTION(nullif,             2, 0, 1, nullifFunc       ),
    DFUNCTION(sqlite_version,    0, 0, 0, versionFunc      ),
    DFUNCTION(sqlite_source_id,  0, 0, 0, sourceidFunc     ),
    FUNCTION(sqlite_log,         2, 0, 0, errlogFunc       ),
    FUNCTION(unistr,             1, 0, 0, unistrFunc       ),
    FUNCTION(quote,              1, 0, 0, quoteFunc        ),
    FUNCTION(unistr_quote,       1, 1, 0, quoteFunc        ),
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
    VFUNCTION(last_insert_rowid, 0, 0, 0, last_insert_rowid),
    VFUNCTION(changes,           0, 0, 0, changes          ),
    VFUNCTION(total_changes,     0, 0, 0, total_changes    ),
#endif /* !defined(SQLITE_BUILDING_FOR_COMDB2) */
    FUNCTION(replace,            3, 0, 0, replaceFunc      ),
    FUNCTION(zeroblob,           1, 0, 0, zeroblobFunc     ),
    FUNCTION(substr,             2, 0, 0, substrFunc       ),
    FUNCTION(substr,             3, 0, 0, substrFunc       ),
    FUNCTION(substring,          2, 0, 0, substrFunc       ),
    FUNCTION(substring,          3, 0, 0, substrFunc       ),
    WAGGREGATE(sum,   1,0,0, sumStep, sumFinalize, sumFinalize, sumInverse, 0),
    WAGGREGATE(total, 1,0,0, sumStep,totalFinalize,totalFinalize,sumInverse, 0),
    WAGGREGATE(avg,   1,0,0, sumStep, avgFinalize, avgFinalize, sumInverse, 0),
    WAGGREGATE(count, 0,0,0, countStep,
        countFinalize, countFinalize, countInverse,
        SQLITE_FUNC_COUNT|SQLITE_FUNC_ANYORDER  ),
    WAGGREGATE(count, 1,0,0, countStep,
        countFinalize, countFinalize, countInverse, SQLITE_FUNC_ANYORDER ),
    WAGGREGATE(group_concat, 1, 0, 0, groupConcatStep,
        groupConcatFinalize, groupConcatValue, groupConcatInverse, 0),
    WAGGREGATE(group_concat, 2, 0, 0, groupConcatStep,
        groupConcatFinalize, groupConcatValue, groupConcatInverse, 0),
    WAGGREGATE(string_agg,   2, 0, 0, groupConcatStep,
        groupConcatFinalize, groupConcatValue, groupConcatInverse, 0),

#ifdef SQLITE_ENABLE_PERCENTILE
    WAGGREGATE(median,          1,   0,0, percentStep,
        percentFinal, percentValue, percentInverse,
        SQLITE_INNOCUOUS|SQLITE_SELFORDER1),
    WAGGREGATE(percentile,      2, 0x2,0, percentStep,
        percentFinal, percentValue, percentInverse,
        SQLITE_INNOCUOUS|SQLITE_SELFORDER1),
    WAGGREGATE(percentile_cont, 2,   0,0, percentStep,
        percentFinal, percentValue, percentInverse,
        SQLITE_INNOCUOUS|SQLITE_SELFORDER1),
    WAGGREGATE(percentile_disc, 2, 0x1,0, percentStep,
        percentFinal, percentValue, percentInverse,
        SQLITE_INNOCUOUS|SQLITE_SELFORDER1),
#endif /* SQLITE_ENABLE_PERCENTILE */
 
    LIKEFUNC(glob, 2, &globInfo, SQLITE_FUNC_LIKE|SQLITE_FUNC_CASE),
#ifdef SQLITE_CASE_SENSITIVE_LIKE
    LIKEFUNC(like, 2, &likeInfoAlt, SQLITE_FUNC_LIKE|SQLITE_FUNC_CASE),
    LIKEFUNC(like, 3, &likeInfoAlt, SQLITE_FUNC_LIKE|SQLITE_FUNC_CASE),
#else
    LIKEFUNC(like, 2, &likeInfoNorm, SQLITE_FUNC_LIKE),
    LIKEFUNC(like, 3, &likeInfoNorm, SQLITE_FUNC_LIKE),
#endif
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    FUNCTION(comdb2_double_to_blob, 1, 0, 0, comdb2DoubleToBlobFunc),
    FUNCTION(comdb2_blob_to_double, 1, 0, 0, comdb2BlobToDoubleFunc),
    FUNCTION(comdb2_sysinfo,        1, 0, 0, comdb2SysinfoFunc),
    FUNCTION(comdb2_ctxinfo,        1, 0, 0, comdb2CtxinfoFunc),
    FUNCTION(comdb2_version,        0, 0, 0, comdb2VersionFunc),
    FUNCTION(comdb2_semver,         0, 0, 0, comdb2SemVerFunc),
    FUNCTION(table_version,         1, 0, 0, tableVersionFunc),
    FUNCTION(partition_info,        2, 0, 0, partitionInfoFunc),
    FUNCTION(comdb2_host,           0, 0, 0, comdb2HostFunc),
    FUNCTION(comdb2_node,           0, 0, 0, comdb2HostFunc),
    FUNCTION(comdb2_port,           0, 0, 0, comdb2PortFunc),
    FUNCTION(comdb2_dbname,         0, 0, 0, comdb2DbnameFunc),
    FUNCTION(comdb2_prevquerycost,  0, 0, 0, comdb2PrevquerycostFunc),
    FUNCTION(comdb2_starttime,      0, 0, 0, comdb2StartTimeFunc),
    FUNCTION(comdb2_user,           0, 0, 0, comdb2UserFunc),
    FUNCTION(comdb2_last_cost,      0, 0, 0, comdb2LastCostFunc),
    FUNCTION(comdb2_snapshot_lsn,   0, 0, 0, comdb2SnapshotLsnFunc),
    FUNCTION(checksum_md5,          1, 0, 0, md5Func),
    FUNCTION(compress,              1, 0, 0, compressFunc),
    FUNCTION(uncompress,            1, 0, 0, uncompressFunc),
    FUNCTION(compress_zlib,         1, 0, 0, compressFunc),
    FUNCTION(uncompress_zlib,       1, 0, 0, uncompressFunc),
    FUNCTION(compress_gzip,         1, 0, 0, compressGzipFunc),
    FUNCTION(uncompress_gzip,       1, 0, 0, uncompressGzipFunc),
#if defined(SQLITE_BUILDING_FOR_COMDB2_DBGLOG)
    FUNCTION(dbglog_cookie,         0, 0, 0, dbglogCookieFunc),
    FUNCTION(dbglog_begin,          1, 0, 0, dbglogBeginFunc),
    FUNCTION(dbglog_end,            1, 0, 0, dbglogEndFunc),
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2_DBGLOG) */
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
#ifdef SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION
    FUNCTION(unknown,           -1, 0, 0, unknownFunc      ),
#endif
#ifdef SQLITE_ENABLE_MATH_FUNCTIONS
    MFUNCTION(ceil,              1, xCeil,     ceilingFunc ),
    MFUNCTION(ceiling,           1, xCeil,     ceilingFunc ),
    MFUNCTION(floor,             1, xFloor,    ceilingFunc ),
#if SQLITE_HAVE_C99_MATH_FUNCS
    MFUNCTION(trunc,             1, trunc,     ceilingFunc ),
#endif
    FUNCTION(ln,                 1, 0, 0,      logFunc     ),
    FUNCTION(log,                1, 1, 0,      logFunc     ),
    FUNCTION(log10,              1, 1, 0,      logFunc     ),
    FUNCTION(log2,               1, 2, 0,      logFunc     ),
    FUNCTION(log,                2, 0, 0,      logFunc     ),
    MFUNCTION(exp,               1, exp,       math1Func   ),
    MFUNCTION(pow,               2, pow,       math2Func   ),
    MFUNCTION(power,             2, pow,       math2Func   ),
    MFUNCTION(mod,               2, fmod,      math2Func   ),
    MFUNCTION(acos,              1, acos,      math1Func   ),
    MFUNCTION(asin,              1, asin,      math1Func   ),
    MFUNCTION(atan,              1, atan,      math1Func   ),
    MFUNCTION(atan2,             2, atan2,     math2Func   ),
    MFUNCTION(cos,               1, cos,       math1Func   ),
    MFUNCTION(sin,               1, sin,       math1Func   ),
    MFUNCTION(tan,               1, tan,       math1Func   ),
    MFUNCTION(cosh,              1, cosh,      math1Func   ),
    MFUNCTION(sinh,              1, sinh,      math1Func   ),
    MFUNCTION(tanh,              1, tanh,      math1Func   ),
#if SQLITE_HAVE_C99_MATH_FUNCS
    MFUNCTION(acosh,             1, acosh,     math1Func   ),
    MFUNCTION(asinh,             1, asinh,     math1Func   ),
    MFUNCTION(atanh,             1, atanh,     math1Func   ),
#endif
    MFUNCTION(sqrt,              1, sqrt,      math1Func   ),
    MFUNCTION(radians,           1, degToRad,  math1Func   ),
    MFUNCTION(degrees,           1, radToDeg,  math1Func   ),
    MFUNCTION(pi,                0, 0,         piFunc      ),
#endif /* SQLITE_ENABLE_MATH_FUNCTIONS */
    FUNCTION(sign,               1, 0, 0,      signFunc    ),
    INLINE_FUNC(coalesce,       -4, INLINEFUNC_coalesce, 0 ),
    INLINE_FUNC(iif,            -4, INLINEFUNC_iif,      0 ),
    INLINE_FUNC(if,             -4, INLINEFUNC_iif,      0 ),
  };
#ifndef SQLITE_OMIT_ALTERTABLE
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
  sqlite3AlterFunctions();
#endif /* !defined(SQLITE_BUILDING_FOR_COMDB2) */
#endif
  sqlite3WindowFunctions();
  sqlite3RegisterDateTimeFunctions();
  sqlite3RegisterJsonFunctions();
  sqlite3InsertBuiltinFuncs(aBuiltinFunc, ArraySize(aBuiltinFunc));

#if 0  /* Enable to print out how the built-in functions are hashed */
  {
    int i;
    FuncDef *p;
    for(i=0; i<SQLITE_FUNC_HASH_SZ; i++){
      printf("FUNC-HASH %02d:", i);
      for(p=sqlite3BuiltinFunctions.a[i]; p; p=p->u.pHash){
        int n = sqlite3Strlen30(p->zName);
        int h = p->zName[0] + n;
        assert( p->funcFlags & SQLITE_FUNC_BUILTIN );
        printf(" %s(%d)", p->zName, h);
      }
      printf("\n");
    }
  }
#endif
}

#if defined(SQLITE_BUILDING_FOR_COMDB2)
static int strptrcmp(const void *p1, const void *p2) {
      return strcmp(*(char *const *)p1, *(char *const *)p2);
}

void sqlite3GetAllBuiltinFunctions(void **data, int *tot)
{
    int cnt = 100;
    int num = 0;
    char **arr = malloc((cnt * sizeof(char *)));
    int i;
    FuncDef *p;
    for(i=0; i<SQLITE_FUNC_HASH_SZ; i++){
        for(p=sqlite3BuiltinFunctions.a[i]; p; p=p->u.pHash){
            if (num >= cnt) {
                cnt *= 2;
                arr = realloc(arr, cnt * sizeof(char*));
            }
            arr[num++] = sqlite3_mprintf("%s()", p->zName);
        }
    }
    arr[num++] = sqlite3_mprintf("sys.cmd.send()");
    arr[num++] = sqlite3_mprintf("sys.cmd.verify()");
    qsort(arr, num, sizeof(char *), strptrcmp);
    *tot = num;
    *data = arr;
}

void sqlite3FreeAllBuiltinFunctions(void **data, int tot)
{
    char **arr = (char **)data;
    for (int i = 0; i < tot; i++) {
        sqlite3_free(arr[i]);
    }
    free(arr);
}
#endif
