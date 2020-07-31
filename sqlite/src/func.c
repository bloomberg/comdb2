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
#include "vdbeInt.h"
#if defined(SQLITE_BUILDING_FOR_COMDB2)
#include <unistd.h>
#include <uuid/uuid.h>
#include <memcompare.c>
#include "comdb2.h"
#include "sql.h"
#include "bdb_int.h"
#include "md5.h"
#include "tohex.h"

pthread_mutex_t gbl_test_log_file_mtx = PTHREAD_MUTEX_INITIALIZER;
char *gbl_test_log_file = NULL;

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
    case SQLITE_NEXTSEQ:
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
    }else{
      zHaystack = sqlite3_value_text(argv[0]);
      zNeedle = sqlite3_value_text(argv[1]);
      isText = 1;
    }
    if( zNeedle==0 || (nHaystack && zHaystack==0) ) return;
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
}

/*
** Implementation of the printf() function.
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
  int negP2 = 0;

  assert( argc==3 || argc==2 );
  if( sqlite3_value_type(argv[1])==SQLITE_NULL
   || (argc==3 && sqlite3_value_type(argv[2])==SQLITE_NULL)
  ){
    return;
  }
  p0type = sqlite3_value_type(argv[0]);
  p1 = sqlite3_value_int(argv[1]);
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  if (p1 == 0) p1 = 1;
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
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
#ifdef SQLITE_SUBSTR_COMPATIBILITY
  /* If SUBSTR_COMPATIBILITY is defined then substr(X,0,N) work the same as
  ** as substr(X,1,N) - it returns the first N characters of X.  This
  ** is essentially a back-out of the bug-fix in check-in [5fc125d362df4b8]
  ** from 2009-02-02 for compatibility of applications that exploited the
  ** old buggy behavior. */
  if( p1==0 ) p1 = 1; /* <rdar://problem/6778339> */
#endif
  if( argc==3 ){
    p2 = sqlite3_value_int(argv[2]);
    if( p2<0 ){
      p2 = -p2;
      negP2 = 1;
    }
  }else{
    p2 = sqlite3_context_db_handle(context)->aLimit[SQLITE_LIMIT_LENGTH];
  }
  if( p1<0 ){
    p1 += len;
    if( p1<0 ){
      p2 += p1;
      if( p2<0 ) p2 = 0;
      p1 = 0;
    }
  }else if( p1>0 ){
    p1--;
  }else if( p2>0 ){
    p2--;
  }
  if( negP2 ){
    p1 -= p2;
    if( p1<0 ){
      p2 += p1;
      p1 = 0;
    }
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
    if( p1+p2>len ){
      p2 = len-p1;
      if( p2<0 ) p2 = 0;
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
    if( comdb2_sql_tick() )
      break;  
    /* We could also return error by doing
     * sqlite3_result_error(context, "Interrupted", -1); */
  }
  sqlite3_result_int(context, i);
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
  char *zErrMsg;
  if( sqlite3_value_type(argv[0])!=SQLITE_TEXT ) return;
  zSql = (const char *)sqlite3_value_text(argv[0]);
  if( !zSql ) return;
  db = sqlite3_context_db_handle(context);
  wasPrepareOnly = (db->flags&SQLITE_PREPARE_ONLY)!=0;
  db->flags |= SQLITE_PrepareOnly;
  memset(&sParse, 0, sizeof(Parse));
  sParse.db = db;
  sParse.prepFlags = SQLITE_PREPARE_ONLY|SQLITE_PREPARE_SRCLIST_ONLY;
  zErrMsg = 0;
  if( sqlite3RunParser(&sParse, zSql, &zErrMsg) ){
    if( zErrMsg ){
      sqlite3_result_error(context, zErrMsg, -1);
      sqlite3DbFree(db, zErrMsg);
    }
    goto done;
  }else if( zErrMsg ){ /* TODO: Is this ever possible? */
    sqlite3DbFree(db, zErrMsg);
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
  sqlite3ParserReset(&sParse);
}
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */

/*
** Implementation of the round() function
*/
#ifndef SQLITE_OMIT_FLOATING_POINT
static void roundFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
  int n = 0;
  double r;
  char *zBuf;
  assert( argc==1 || argc==2 );
  if( argc==2 ){
    if( SQLITE_NULL==sqlite3_value_type(argv[1]) ) return;
    n = sqlite3_value_int(argv[1]);
    if( n>30 ) n = 30;
    if( n<0 ) n = 0;
  }
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ) return;
  r = sqlite3_value_double(argv[0]);
  /* If Y==0 and X will fit in a 64-bit int,
  ** handle the rounding directly,
  ** otherwise use printf.
  */
  if( n==0 && r>=0 && r<LARGEST_INT64-1 ){
    r = (double)((sqlite_int64)(r+0.5));
  }else if( n==0 && r<0 && (-r)<LARGEST_INT64-1 ){
    r = -(double)((sqlite_int64)((-r)+0.5));
  }else{
    zBuf = sqlite3_mprintf("%.*f",n,r);
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
  testcase( nByte==db->aLimit[SQLITE_LIMIT_LENGTH]+1 );
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

  sqlite3_result_text(context, guid_str, GUID_STR_LENGTH, SQLITE_TRANSIENT);
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

  sqlite3_result_text(context, guid_str, GUID_STR_LENGTH, SQLITE_TRANSIENT);
}

static void comdb2TestLogFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert( argc==1 );
  if( sqlite3_value_type(argv[0])==SQLITE_TEXT ){
    const unsigned char *zMsg = sqlite3_value_text(argv[0]);
    if( zMsg ){
      Pthread_mutex_lock(&gbl_test_log_file_mtx);
      if( gbl_test_log_file!=NULL ){
        FILE *file = fopen(gbl_test_log_file, "a+");
        if( file!=NULL ){
          size_t nLen = strlen((char*)zMsg);
          size_t nRet = 0;
          if( nLen>0 ){
            nRet = fwrite(zMsg, sizeof(char), nLen, file);
          }
          fflush(file); fclose(file);
          sqlite3_result_int64(context, (i64)nRet);
        }
      }
      Pthread_mutex_unlock(&gbl_test_log_file_mtx);
    }
  }
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
  }else if( sqlite3_stricmp(zName, "sequence")==0 ){
    if( clnt ){
      sqlite3_result_int64(context, clnt->seqNo);
    }
  }else if( sqlite3_stricmp(zName, "priority")==0 ){
    if( clnt ){
      sqlite3_result_int64(context, clnt->priority);
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
** IMP: R-62073-11209 The changes() SQL function is a wrapper
** around the sqlite3_changes() C/C++ function and hence follows the same
** rules for counting changes.
*/
static void changes(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  sqlite3_result_int(context, sqlite3_changes(db));
}

/*
** Implementation of the total_changes() SQL function.  The return value is
** the same as the sqlite3_total_changes() API function.
*/
static void total_changes(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  /* IMP: R-52756-41993 This function is a wrapper around the
  ** sqlite3_total_changes() C/C++ interface. */
  sqlite3_result_int(context, sqlite3_total_changes(db));
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
** character is exactly one byte in size.  Also, provde the Utf8Read()
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
      while( (c=Utf8Read(zPattern)) == matchAll || c == matchOne ){
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
      if( c<=0x80 ){
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
  return patternCompare((u8*)zGlobPattern, (u8*)zString, &globInfo, '[');
}

/*
** The sqlite3_strlike() interface.  Return 0 on a match and non-zero for
** a miss - like strcmp().
*/
int sqlite3_strlike(const char *zPattern, const char *zStr, unsigned int esc){
  return patternCompare((u8*)zPattern, (u8*)zStr, &likeInfoNorm, esc);
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
** the build-in LIKE operator.  The first argument to the function is the
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
  zB = sqlite3_value_text(argv[0]);
  zA = sqlite3_value_text(argv[1]);

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
  assert( zB==sqlite3_value_text(argv[0]) );  /* Encoding did not change */

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
  }else{
    escape = pInfo->matchSet;
  }
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
** Implementation of the QUOTE() function.  This function takes a single
** argument.  If the argument is numeric, the return value is the same as
** the argument.  If the argument is NULL, the return value is the string
** "NULL".  Otherwise, the argument is enclosed in single quotes with
** single-quote escapes.
*/
static void quoteFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  switch( sqlite3_value_type(argv[0]) ){
    case SQLITE_FLOAT: {
      double r1, r2;
      char zBuf[50];
      r1 = sqlite3_value_double(argv[0]);
      sqlite3_snprintf(sizeof(zBuf), zBuf, "%!.15g", r1);
      sqlite3AtoF(zBuf, &r2, 20, SQLITE_UTF8);
      if( r1!=r2 ){
        sqlite3_snprintf(sizeof(zBuf), zBuf, "%!.20e", r1);
      }
      sqlite3_result_text(context, zBuf, -1, SQLITE_TRANSIENT);
      break;
    }
    case SQLITE_INTEGER: {
      sqlite3_result_value(context, argv[0]);
      break;
    }
    case SQLITE_BLOB: {
      char *zText = 0;
      char const *zBlob = sqlite3_value_blob(argv[0]);
      int nBlob = sqlite3_value_bytes(argv[0]);
      assert( zBlob==sqlite3_value_blob(argv[0]) ); /* No encoding change */
      zText = (char *)contextMalloc(context, (2*(i64)nBlob)+4); 
      if( zText ){
        int i;
        for(i=0; i<nBlob; i++){
          zText[(i*2)+2] = hexdigits[(zBlob[i]>>4)&0x0F];
          zText[(i*2)+3] = hexdigits[(zBlob[i])&0x0F];
        }
        zText[(nBlob*2)+2] = '\'';
        zText[(nBlob*2)+3] = '\0';
        zText[0] = 'X';
        zText[1] = '\'';
        sqlite3_result_text(context, zText, -1, SQLITE_TRANSIENT);
        sqlite3_free(zText);
      }
      break;
    }
    case SQLITE_TEXT: {
      int i,j;
      u64 n;
      const unsigned char *zArg = sqlite3_value_text(argv[0]);
      char *z;

      if( zArg==0 ) return;
      for(i=0, n=0; zArg[i]; i++){ if( zArg[i]=='\'' ) n++; }
      z = contextMalloc(context, ((i64)i)+((i64)n)+3);
      if( z ){
        z[0] = '\'';
        for(i=0, j=1; zArg[i]; i++){
          z[j++] = zArg[i];
          if( zArg[i]=='\'' ){
            z[j++] = '\'';
          }
        }
        z[j++] = '\'';
        z[j] = 0;
        sqlite3_result_text(context, z, j, sqlite3_free);
      }
      break;
    }
    default: {
      assert( sqlite3_value_type(argv[0])==SQLITE_NULL );
      sqlite3_result_text(context, "NULL", 4, SQLITE_STATIC);
      break;
    }
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
    sqlite3_result_text(context, zHex, n*2, sqlite3_free);
  }
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
    sqlite3_result_value(context, argv[0]);
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
  zOut = contextMalloc(context, (i64)nOut);
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
          zOut = sqlite3_realloc64(zOut, (int)nOut + (nOut - nStr - 1));
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
  int nIn;                          /* Number of bytes in input */
  int flags;                        /* 1: trimleft  2: trimright  3: trim */
  int i;                            /* Loop counter */
  unsigned char *aLen = 0;          /* Length of each character in zCharSet */
  unsigned char **azChar = 0;       /* Individual characters in zCharSet */
  int nChar;                        /* Number of characters in zCharSet */

  if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
    return;
  }
  zIn = sqlite3_value_text(argv[0]);
  if( zIn==0 ) return;
  nIn = sqlite3_value_bytes(argv[0]);
  assert( zIn==sqlite3_value_text(argv[0]) );
  if( argc==1 ){
    static const unsigned char lenOne[] = { 1 };
    static unsigned char * const azOne[] = { (u8*)" " };
    nChar = 1;
    aLen = (u8*)lenOne;
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
      azChar = contextMalloc(context, ((i64)nChar)*(sizeof(char*)+1));
      if( azChar==0 ){
        return;
      }
      aLen = (unsigned char*)&azChar[nChar];
      for(z=zCharSet, nChar=0; *z; nChar++){
        azChar[nChar] = (unsigned char *)z;
        SQLITE_SKIP_UTF8(z);
        aLen[nChar] = (u8)(z - azChar[nChar]);
      }
    }
  }
  if( nChar>0 ){
    flags = SQLITE_PTR_TO_INT(sqlite3_user_data(context));
    if( flags & 1 ){
      while( nIn>0 ){
        int len = 0;
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
        int len = 0;
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


#ifdef SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION
/*
** The "unknown" function is automatically substituted in place of
** any unrecognized function name when doing an EXPLAIN or EXPLAIN QUERY PLAN
** when the SQLITE_ENABLE_UNKNOWN_FUNCTION compile-time option is used.
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
  double rSum;      /* Floating point sum */
  i64 iSum;         /* Integer sum */   
  i64 cnt;          /* Number of elements summed */
  u8 overflow;      /* True if integer overflow seen */
  u8 approx;        /* True if non-integer value was input to the sum */
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  u8 decs;          /* True if summing decimals */
  decQuad decSum;   /* decQuad aggregation */
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
};

/*
** Routines used to compute the sum, average, and total.
**
** The SUM() function follows the (broken) SQL standard which means
** that it returns NULL if it sums over no inputs.  TOTAL returns
** 0.0 in that case.  In addition, TOTAL always returns a float where
** SUM might return an integer if it never encounters a floating point
** value.  TOTAL never fails, but SUM might through an exception if
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
    if( type==SQLITE_INTEGER ){
      i64 v = sqlite3_value_int64(argv[0]);
      p->rSum += v;
      if( (p->approx|p->overflow)==0 && sqlite3AddInt64(&p->iSum, v) ){
        p->approx = p->overflow = 1;
      }
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    }else if( type==SQLITE_DECIMAL ){
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
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
    }else{
      p->rSum += sqlite3_value_double(argv[0]);
      p->approx = 1;
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
    assert( type==SQLITE_INTEGER || p->approx );
    if( type==SQLITE_INTEGER && p->approx==0 ){
      i64 v = sqlite3_value_int64(argv[0]);
      p->rSum -= v;
      p->iSum -= v;
    }else{
      p->rSum -= sqlite3_value_double(argv[0]);
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
    if( p->overflow ){
      sqlite3_result_error(context,"integer overflow",-1);
    }else if( p->approx ){
      sqlite3_result_double(context, p->rSum);
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    }else if( p->decs){
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
    } else
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
    sqlite3_result_double(context, p->rSum/(double)p->cnt);
  }
}
static void totalFinalize(sqlite3_context *context){
  SumCtx *p;
  p = sqlite3_aggregate_context(context, 0);
  /* (double)0 In case of SQLITE_OMIT_FLOATING_POINT... */
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  if( p && p->decs ){
    intv_t res;
    res.type = INTV_DECIMAL_TYPE;
    res.sign = 0;
    res.u.dec = p->decSum;
    sqlite3_result_interval(context, &res);
  }
  else
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  sqlite3_result_double(context, p ? p->rSum : (double)0);
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
*/
static void groupConcatStep(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const char *zVal;
  StrAccum *pAccum;
  const char *zSep;
  int nVal, nSep;
  assert( argc==1 || argc==2 );
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ) return;
  pAccum = (StrAccum*)sqlite3_aggregate_context(context, sizeof(*pAccum));

  if( pAccum ){
    sqlite3 *db = sqlite3_context_db_handle(context);
    int firstTerm = pAccum->mxAlloc==0;
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    struct sqlclntstate *clnt = get_sql_clnt();

    pAccum->mxAlloc = (clnt && clnt->group_concat_mem_limit != 0) ?
      clnt->group_concat_mem_limit : db->aLimit[SQLITE_LIMIT_LENGTH];
#else /* defined(SQLITE_BUILDING_FOR_COMDB2) */
    pAccum->mxAlloc = db->aLimit[SQLITE_LIMIT_LENGTH];
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
    if( !firstTerm ){
      if( argc==2 ){
        zSep = (char*)sqlite3_value_text(argv[1]);
        nSep = sqlite3_value_bytes(argv[1]);
      }else{
        zSep = ",";
        nSep = 1;
      }
      if( zSep ) sqlite3_str_append(pAccum, zSep, nSep);
    }
    zVal = (char*)sqlite3_value_text(argv[0]);
    nVal = sqlite3_value_bytes(argv[0]);
    if( zVal ) sqlite3_str_append(pAccum, zVal, nVal);
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    // NC: Attach an error message here as we do not want client to see the
    // default error message 'transaction too big' emitted for ERR_TRAN_TOO_BIG,
    // which SQLITE_TOOBIG later gets translated to.
    if( clnt && pAccum->accError==SQLITE_TOOBIG ){
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
  int n;
  StrAccum *pAccum;
  assert( argc==1 || argc==2 );
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ) return;
  pAccum = (StrAccum*)sqlite3_aggregate_context(context, sizeof(*pAccum));
  /* pAccum is always non-NULL since groupConcatStep() will have always
  ** run frist to initialize it */
  if( ALWAYS(pAccum) ){
    n = sqlite3_value_bytes(argv[0]);
    if( argc==2 ){
      n += sqlite3_value_bytes(argv[1]);
    }else{
      n++;
    }
    if( n>=(int)pAccum->nChar ){
      pAccum->nChar = 0;
    }else{
      pAccum->nChar -= n;
      memmove(pAccum->zText, &pAccum->zText[n], pAccum->nChar);
    }
    if( pAccum->nChar==0 ) pAccum->mxAlloc = 0;
  }
}
#else
# define groupConcatInverse 0
#endif /* SQLITE_OMIT_WINDOWFUNC */
static void groupConcatFinalize(sqlite3_context *context){
  StrAccum *pAccum;
  pAccum = sqlite3_aggregate_context(context, 0);
  if( pAccum ){
    if( pAccum->accError==SQLITE_TOOBIG ){
      sqlite3_result_error_toobig(context);
    }else if( pAccum->accError==SQLITE_NOMEM ){
      sqlite3_result_error_nomem(context);
    }else{    
      sqlite3_result_text(context, sqlite3StrAccumFinish(pAccum), -1, 
                          sqlite3_free);
    }
  }
}
#ifndef SQLITE_OMIT_WINDOWFUNC
static void groupConcatValue(sqlite3_context *context){
  sqlite3_str *pAccum;
  pAccum = (sqlite3_str*)sqlite3_aggregate_context(context, 0);
  if( pAccum ){
    if( pAccum->accError==SQLITE_TOOBIG ){
      sqlite3_result_error_toobig(context);
    }else if( pAccum->accError==SQLITE_NOMEM ){
      sqlite3_result_error_nomem(context);
    }else{    
      const char *zText = sqlite3_str_value(pAccum);
      sqlite3_result_text(context, zText, -1, SQLITE_TRANSIENT);
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
** Set the LIKEOPT flag on the 2-argument function with the given name.
*/
static void setLikeOptFlag(sqlite3 *db, const char *zName, u8 flagVal){
  FuncDef *pDef;
  pDef = sqlite3FindFunction(db, zName, 2, SQLITE_UTF8, 0);
  if( ALWAYS(pDef) ){
    pDef->funcFlags |= flagVal;
  }
  pDef = sqlite3FindFunction(db, zName, 3, SQLITE_UTF8, 0);
  if( pDef ){
    pDef->funcFlags |= flagVal;
  }
}

/*
** Register the built-in LIKE and GLOB functions.  The caseSensitive
** parameter determines whether or not the LIKE operator is case
** sensitive.  GLOB is always case sensitive.
*/
void sqlite3RegisterLikeFunctions(sqlite3 *db, int caseSensitive){
  struct compareInfo *pInfo;
  if( caseSensitive ){
    pInfo = (struct compareInfo*)&likeInfoAlt;
  }else{
    pInfo = (struct compareInfo*)&likeInfoNorm;
  }
  sqlite3CreateFunc(db, "like", 2, SQLITE_UTF8, pInfo, likeFunc, 0, 0, 0, 0, 0);
  sqlite3CreateFunc(db, "like", 3, SQLITE_UTF8, pInfo, likeFunc, 0, 0, 0, 0, 0);
  sqlite3CreateFunc(db, "glob", 2, SQLITE_UTF8, 
      (struct compareInfo*)&globInfo, likeFunc, 0, 0, 0, 0, 0);
  setLikeOptFlag(db, "glob", SQLITE_FUNC_LIKE | SQLITE_FUNC_CASE);
  setLikeOptFlag(db, "like", 
      caseSensitive ? (SQLITE_FUNC_LIKE | SQLITE_FUNC_CASE) : SQLITE_FUNC_LIKE);
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
  if( pExpr->op!=TK_FUNCTION || !pExpr->x.pList ){
    return 0;
  }
  assert( !ExprHasProperty(pExpr, EP_xIsSelect) );
  nExpr = pExpr->x.pList->nExpr;
  pDef = sqlite3FindFunction(db, pExpr->u.zToken, nExpr, SQLITE_UTF8, 0);
  if( NEVER(pDef==0) || (pDef->funcFlags & SQLITE_FUNC_LIKE)==0 ){
    return 0;
  }
  if( nExpr<3 ){
    aWc[3] = 0;
  }else{
    Expr *pEscape = pExpr->x.pList->a[2].pExpr;
    char *zEscape;
    if( pEscape->op!=TK_STRING ) return 0;
    zEscape = pEscape->u.zToken;
    if( zEscape[0]==0 || zEscape[1]!=0 ) return 0;
    aWc[3] = zEscape[0];
  }

  /* The memcpy() statement assumes that the wildcard characters are
  ** the first three statements in the compareInfo structure.  The
  ** asserts() that follow verify that assumption
  */
  memcpy(aWc, pDef->pUserData, 3);
  assert( (char*)&likeInfoAlt == (char*)&likeInfoAlt.matchAll );
  assert( &((char*)&likeInfoAlt)[1] == (char*)&likeInfoAlt.matchOne );
  assert( &((char*)&likeInfoAlt)[2] == (char*)&likeInfoAlt.matchSet );
  *pIsNocase = (pDef->funcFlags & SQLITE_FUNC_CASE)==0;
  return 1;
}

#if defined(SQLITE_BUILDING_FOR_COMDB2)
#if defined(SQLITE_BUILDING_FOR_COMDB2_DBGLOG)
static void dbglogCookieFunc(
  sqlite3_context *context,
  int NotUsed,
  sqlite3_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  uint64_t comdb2fastseed(void);
  sqlite3_result_int64(context, (i64)comdb2fastseed());
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
#ifdef SQLITE_SOUNDEX
    FUNCTION(soundex,            1, 0, 0, soundexFunc      ),
#endif
#ifndef SQLITE_OMIT_LOAD_EXTENSION
    VFUNCTION(load_extension,    1, 0, 0, loadExt          ),
    VFUNCTION(load_extension,    2, 0, 0, loadExt          ),
#endif
#if SQLITE_USER_AUTHENTICATION
    FUNCTION(sqlite_crypt,       2, 0, 0, sqlite3CryptFunc ),
#endif
#ifndef SQLITE_OMIT_COMPILEOPTION_DIAGS
    DFUNCTION(sqlite_compileoption_used,1, 0, 0, compileoptionusedFunc  ),
    DFUNCTION(sqlite_compileoption_get, 1, 0, 0, compileoptiongetFunc  ),
#endif /* SQLITE_OMIT_COMPILEOPTION_DIAGS */
    FUNCTION2(unlikely,          1, 0, 0, noopFunc,  SQLITE_FUNC_UNLIKELY),
    FUNCTION2(likelihood,        2, 0, 0, noopFunc,  SQLITE_FUNC_UNLIKELY),
    FUNCTION2(likely,            1, 0, 0, noopFunc,  SQLITE_FUNC_UNLIKELY),
#ifdef SQLITE_DEBUG
    FUNCTION2(affinity,          1, 0, 0, noopFunc,  SQLITE_FUNC_AFFINITY),
#endif
#ifdef SQLITE_ENABLE_OFFSET_SQL_FUNC
    FUNCTION2(sqlite_offset,     1, 0, 0, noopFunc,  SQLITE_FUNC_OFFSET|
                                                     SQLITE_FUNC_TYPEOF),
#endif
    FUNCTION(ltrim,              1, 1, 0, trimFunc         ),
    FUNCTION(ltrim,              2, 1, 0, trimFunc         ),
    FUNCTION(rtrim,              1, 2, 0, trimFunc         ),
    FUNCTION(rtrim,              2, 2, 0, trimFunc         ),
    FUNCTION(trim,               1, 3, 0, trimFunc         ),
    FUNCTION(trim,               2, 3, 0, trimFunc         ),
    FUNCTION(min,               -1, 0, 1, minmaxFunc       ),
    FUNCTION(min,                0, 0, 1, 0                ),
    WAGGREGATE(min, 1, 0, 1, minmaxStep, minMaxFinalize, minMaxValue, 0,
                                          SQLITE_FUNC_MINMAX ),
    FUNCTION(max,               -1, 1, 1, minmaxFunc       ),
    FUNCTION(max,                0, 1, 1, 0                ),
    WAGGREGATE(max, 1, 1, 1, minmaxStep, minMaxFinalize, minMaxValue, 0,
                                          SQLITE_FUNC_MINMAX ),
    FUNCTION2(typeof,            1, 0, 0, typeofFunc,  SQLITE_FUNC_TYPEOF),
    FUNCTION2(length,            1, 0, 0, lengthFunc,  SQLITE_FUNC_LENGTH),
    FUNCTION(instr,              2, 0, 0, instrFunc        ),
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    VFUNCTION(sleep,             1, 0, 0, sleepFunc        ),
    VFUNCTION(comdb2_extract_table_names,
                                 1, 0, 0, tableNamesFunc   ),
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
    FUNCTION(printf,            -1, 0, 0, printfFunc       ),
    FUNCTION(unicode,            1, 0, 0, unicodeFunc      ),
    FUNCTION(char,              -1, 0, 0, charFunc         ),
    FUNCTION(abs,                1, 0, 0, absFunc          ),
#ifndef SQLITE_OMIT_FLOATING_POINT
    FUNCTION(round,              1, 0, 0, roundFunc        ),
    FUNCTION(round,              2, 0, 0, roundFunc        ),
#endif
    FUNCTION(upper,              1, 0, 0, upperFunc        ),
    FUNCTION(lower,              1, 0, 0, lowerFunc        ),
    FUNCTION(hex,                1, 0, 0, hexFunc          ),
    FUNCTION2(ifnull,            2, 0, 0, noopFunc,  SQLITE_FUNC_COALESCE),
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
    FUNCTION(quote,              1, 0, 0, quoteFunc        ),
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
    VFUNCTION(last_insert_rowid, 0, 0, 0, last_insert_rowid),
    VFUNCTION(changes,           0, 0, 0, changes          ),
    VFUNCTION(total_changes,     0, 0, 0, total_changes    ),
#endif /* !defined(SQLITE_BUILDING_FOR_COMDB2) */
    FUNCTION(replace,            3, 0, 0, replaceFunc      ),
    FUNCTION(zeroblob,           1, 0, 0, zeroblobFunc     ),
    FUNCTION(substr,             2, 0, 0, substrFunc       ),
    FUNCTION(substr,             3, 0, 0, substrFunc       ),
    WAGGREGATE(sum,   1,0,0, sumStep, sumFinalize, sumFinalize, sumInverse, 0),
    WAGGREGATE(total, 1,0,0, sumStep,totalFinalize,totalFinalize,sumInverse, 0),
    WAGGREGATE(avg,   1,0,0, sumStep, avgFinalize, avgFinalize, sumInverse, 0),
    WAGGREGATE(count, 0,0,0, countStep, 
        countFinalize, countFinalize, countInverse, SQLITE_FUNC_COUNT  ),
    WAGGREGATE(count, 1,0,0, countStep, 
        countFinalize, countFinalize, countInverse, 0  ),
    WAGGREGATE(group_concat, 1, 0, 0, groupConcatStep, 
        groupConcatFinalize, groupConcatValue, groupConcatInverse, 0),
    WAGGREGATE(group_concat, 2, 0, 0, groupConcatStep, 
        groupConcatFinalize, groupConcatValue, groupConcatInverse, 0),
  
    LIKEFUNC(glob, 2, &globInfo, SQLITE_FUNC_LIKE|SQLITE_FUNC_CASE),
#ifdef SQLITE_CASE_SENSITIVE_LIKE
    LIKEFUNC(like, 2, &likeInfoAlt, SQLITE_FUNC_LIKE|SQLITE_FUNC_CASE),
    LIKEFUNC(like, 3, &likeInfoAlt, SQLITE_FUNC_LIKE|SQLITE_FUNC_CASE),
#else
    LIKEFUNC(like, 2, &likeInfoNorm, SQLITE_FUNC_LIKE),
    LIKEFUNC(like, 3, &likeInfoNorm, SQLITE_FUNC_LIKE),
#endif
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    FUNCTION(comdb2_test_log,       1, 0, 0, comdb2TestLogFunc),
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

    FUNCTION(checksum_md5,          1, 0, 0, md5Func),
#if defined(SQLITE_BUILDING_FOR_COMDB2_DBGLOG)
    FUNCTION(dbglog_cookie,         0, 0, 0, dbglogCookieFunc),
    FUNCTION(dbglog_begin,          1, 0, 0, dbglogBeginFunc),
    FUNCTION(dbglog_end,            1, 0, 0, dbglogEndFunc),
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2_DBGLOG) */
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
#ifdef SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION
    FUNCTION(unknown,           -1, 0, 0, unknownFunc      ),
#endif
    FUNCTION(coalesce,           1, 0, 0, 0                ),
    FUNCTION(coalesce,           0, 0, 0, 0                ),
    FUNCTION2(coalesce,         -1, 0, 0, noopFunc,  SQLITE_FUNC_COALESCE),
  };
#ifndef SQLITE_OMIT_ALTERTABLE
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
  sqlite3AlterFunctions();
#endif /* !defined(SQLITE_BUILDING_FOR_COMDB2) */
#endif
  sqlite3WindowFunctions();
#if defined(SQLITE_ENABLE_STAT3) || defined(SQLITE_ENABLE_STAT4)
  sqlite3AnalyzeFunctions();
#endif
  sqlite3RegisterDateTimeFunctions();
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
        printf(" %s(%d)", p->zName, h);
      }
      printf("\n");
    }
  }
#endif
}
