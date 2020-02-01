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
#include <unistd.h>
#include "comdb2.h"

#include <memcompare.c>

/*
** Return the collating function associated with a function.
*/
static CollSeq *sqlitexGetFuncCollSeq(sqlitex_context *context){
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
static void sqlitexSkipAccumulatorLoad(sqlitex_context *context){
  context->skipFlag = 1;
}

/*
** Implementation of the non-aggregate min() and max() functions
*/
static void minmaxFunc(
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  int i;
  int mask;    /* 0 for min() or 0xffffffff for max() */
  int iBest;
  CollSeq *pColl;

  assert( argc>1 );
  mask = sqlitex_user_data(context)==0 ? 0 : -1;
  pColl = sqlitexGetFuncCollSeq(context);
  assert( pColl );
  assert( mask==-1 || mask==0 );
  iBest = 0;
  if( sqlitex_value_type(argv[0])==SQLITE_NULL ) return;
  for(i=1; i<argc; i++){
    if( sqlitex_value_type(argv[i])==SQLITE_NULL ) return;
    if( (sqlitexMemCompare(argv[iBest], argv[i], pColl)^mask)>=0 ){
      testcase( mask==0 );
      iBest = i;
    }
  }
  sqlitex_result_value(context, argv[iBest]);
}

/*
** Return the type of the argument.
*/
static void typeofFunc(
  sqlitex_context *context,
  int NotUsed,
  sqlitex_value **argv
){
  const char *z = 0;
  UNUSED_PARAMETER(NotUsed);
  switch( sqlitex_value_type(argv[0]) ){
    case SQLITE_INTEGER: z = "integer"; break;
    case SQLITE3_TEXT:    z = "text";    break;
    case SQLITE_FLOAT:   z = "real";    break;
    case SQLITE_BLOB:    z = "blob";    break;
    /* COMDB2 MODIFICATION */
    case SQLITE_DATETIMEUS:
    case SQLITE_DATETIME:       z = "datetime";    break;
    case SQLITE_INTERVAL_YM:    z = "interval_ym"; break;
    case SQLITE_INTERVAL_DSUS:
    case SQLITE_INTERVAL_DS:    z = "interval_ds"; break;
    case SQLITE_DECIMAL:        z = "decimal";     break;

    default:             z = "null";    break;
  }
  sqlitex_result_text(context, z, -1, SQLITEX_STATIC);
}


/*
** Implementation of the length() function
*/
static void lengthFunc(
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  int len;

  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  switch( sqlitex_value_type(argv[0]) ){
    /* COMDB2 MODIFICATION */
    case SQLITE_INTERVAL_YM:
    case SQLITE_INTERVAL_DS:
    case SQLITE_INTERVAL_DSUS:
    case SQLITE_DATETIME:
    case SQLITE_DATETIMEUS:
    case SQLITE_DECIMAL:

    case SQLITE_BLOB:
    case SQLITE_INTEGER:
    case SQLITE_FLOAT: {
      sqlitex_result_int(context, sqlitex_value_bytes(argv[0]));
      break;
    }
    case SQLITE3_TEXT: {
      const unsigned char *z = sqlitex_value_text(argv[0]);
      if( z==0 ) return;
      len = 0;
      while( *z ){
        len++;
        SQLITE_SKIP_UTF8(z);
      }
      sqlitex_result_int(context, len);
      break;
    }
    default: {
      sqlitex_result_null(context);
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
static void absFunc(sqlitex_context *context, int argc, sqlitex_value **argv){
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  switch( sqlitex_value_type(argv[0]) ){
    case SQLITE_INTEGER: {
      i64 iVal = sqlitex_value_int64(argv[0]);
      if( iVal<0 ){
        if( iVal==SMALLEST_INT64 ){
          /* IMP: R-31676-45509 If X is the integer -9223372036854775808
          ** then abs(X) throws an integer overflow error since there is no
          ** equivalent positive 64-bit two complement value. */
          sqlitex_result_error(context, "integer overflow", -1);
          return;
        }
        iVal = -iVal;
      } 
      sqlitex_result_int64(context, iVal);
      break;
    }
    case SQLITE_NULL: {
      /* IMP: R-37434-19929 Abs(X) returns NULL if X is NULL. */
      sqlitex_result_null(context);
      break;
    }
    case SQLITE_DECIMAL: {
      decContext ctx;
      intv_t *val = (intv_t*)sqlitex_value_interval( argv[0], SQLITE_DECIMAL);
      intv_t res_val;

      dec_ctx_init(&ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);

      decQuadAbs( &res_val.u.dec, &val->u.dec, &ctx);
      if(dfp_conv_check_status( &ctx, "quad", "abs(quad)"))
      {
          sqlitex_result_error(context, "decimal overflow", -1);
          break;
      }

      res_val.type = INTV_DECIMAL_TYPE;
      res_val.sign = 0;

      sqlitex_result_interval(context, &res_val);
      break;
    }
    default: {
      /* Because sqlitex_value_double() returns 0.0 if the argument is not
      ** something that can be converted into a number, we have:
      ** IMP: R-01992-00519 Abs(X) returns 0.0 if X is a string or blob
      ** that cannot be converted to a numeric value.
      */
      double rVal = sqlitex_value_double(argv[0]);
      if( rVal<0 ) rVal = -rVal;
      sqlitex_result_double(context, rVal);
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
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  const unsigned char *zHaystack;
  const unsigned char *zNeedle;
  int nHaystack;
  int nNeedle;
  int typeHaystack, typeNeedle;
  int N = 1;
  int isText;

  UNUSED_PARAMETER(argc);
  typeHaystack = sqlitex_value_type(argv[0]);
  typeNeedle = sqlitex_value_type(argv[1]);
  if( typeHaystack==SQLITE_NULL || typeNeedle==SQLITE_NULL ) return;
  nHaystack = sqlitex_value_bytes(argv[0]);
  nNeedle = sqlitex_value_bytes(argv[1]);
  if( typeHaystack==SQLITE_BLOB && typeNeedle==SQLITE_BLOB ){
    zHaystack = sqlitex_value_blob(argv[0]);
    zNeedle = sqlitex_value_blob(argv[1]);
    isText = 0;
  }else{
    zHaystack = sqlitex_value_text(argv[0]);
    zNeedle = sqlitex_value_text(argv[1]);
    isText = 1;
  }
  while( nNeedle<=nHaystack && memcmp(zHaystack, zNeedle, nNeedle)!=0 ){
    N++;
    do{
      nHaystack--;
      zHaystack++;
    }while( isText && (zHaystack[0]&0xc0)==0x80 );
  }
  if( nNeedle>nHaystack ) N = 0;
  sqlitex_result_int(context, N);
}

/*
** Implementation of the printf() function.
*/
static void printfFunc(
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  PrintfArguments x;
  StrAccum str;
  const char *zFormat;
  int n;

  if( argc>=1 && (zFormat = (const char*)sqlitex_value_text(argv[0]))!=0 ){
    x.nArg = argc-1;
    x.nUsed = 0;
    x.apArg = argv+1;
    sqlitexStrAccumInit(&str, 0, 0, SQLITE_MAX_LENGTH);
    str.db = sqlitex_context_db_handle(context);
    sqlitexXPrintf(&str, SQLITE_PRINTF_SQLFUNC, zFormat, &x);
    n = str.nChar;
    sqlitex_result_text(context, sqlitexStrAccumFinish(&str), n,
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
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  const unsigned char *z;
  const unsigned char *z2;
  int len;
  int p0type;
  i64 p1, p2;
  int negP2 = 0;

  assert( argc==3 || argc==2 );
  if( sqlitex_value_type(argv[1])==SQLITE_NULL
   || (argc==3 && sqlitex_value_type(argv[2])==SQLITE_NULL)
  ){
    return;
  }
  p0type = sqlitex_value_type(argv[0]);
  p1 = sqlitex_value_int(argv[1]);

  /* COMDB2 MODIFICATION
   * DRQS 22394086 */
  if (p1 == 0) p1 = 1;

  if( p0type==SQLITE_BLOB ){
    len = sqlitex_value_bytes(argv[0]);
    z = sqlitex_value_blob(argv[0]);
    if( z==0 ) return;
    assert( len==sqlitex_value_bytes(argv[0]) );
  }else{
    z = sqlitex_value_text(argv[0]);
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
    p2 = sqlitex_value_int(argv[2]);
    if( p2<0 ){
      p2 = -p2;
      negP2 = 1;
    }
  }else{
    p2 = sqlitex_context_db_handle(context)->aLimit[SQLITE_LIMIT_LENGTH];
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
    sqlitex_result_text64(context, (char*)z, z2-z, SQLITEX_TRANSIENT,
                          SQLITE_UTF8);
  }else{
    if( p1+p2>len ){
      p2 = len-p1;
      if( p2<0 ) p2 = 0;
    }
    sqlitex_result_blob64(context, (char*)&z[p1], (u64)p2, SQLITEX_TRANSIENT);
  }
}

/* COMDB2 MODIFICATION */
/*
** Implementation of the sleep() function
*/
static void sleepFunc(sqlitex_context *context, int argc, sqlitex_value *argv[]) {
  int n;
  int rc;
  if( argc != 1 ){
    sqlitex_result_int(context, -1);
    return;
  }
  n = sqlitex_value_int(argv[0]);
  if( n < 0 ){
    sqlitex_result_int(context, -1);
    return;
  }
  rc = sleep(n);
  sqlitex_result_int(context, n);
}

/*
** Implementation of the round() function
*/
#ifndef SQLITE_OMIT_FLOATING_POINT
static void roundFunc(sqlitex_context *context, int argc, sqlitex_value **argv){
  int n = 0;
  double r;
  char *zBuf;
  assert( argc==1 || argc==2 );
  if( argc==2 ){
    if( SQLITE_NULL==sqlitex_value_type(argv[1]) ) return;
    n = sqlitex_value_int(argv[1]);
    if( n>30 ) n = 30;
    if( n<0 ) n = 0;
  }
  if( sqlitex_value_type(argv[0])==SQLITE_NULL ) return;
  r = sqlitex_value_double(argv[0]);
  /* If Y==0 and X will fit in a 64-bit int,
  ** handle the rounding directly,
  ** otherwise use printf.
  */
  if( n==0 && r>=0 && r<LARGEST_INT64-1 ){
    r = (double)((sqlite_int64)(r+0.5));
  }else if( n==0 && r<0 && (-r)<LARGEST_INT64-1 ){
    r = -(double)((sqlite_int64)((-r)+0.5));
  }else{
    zBuf = sqlitex_mprintf("%.*f",n,r);
    if( zBuf==0 ){
      sqlitex_result_error_nomem(context);
      return;
    }
    sqlitexAtoF(zBuf, &r, sqlitexStrlen30(zBuf), SQLITE_UTF8);
    sqlitex_free(zBuf);
  }
  sqlitex_result_double(context, r);
}
#endif

/*
** Allocate nByte bytes of space using sqlitex_malloc(). If the
** allocation fails, call sqlitex_result_error_nomem() to notify
** the database handle that malloc() has failed and return NULL.
** If nByte is larger than the maximum string or blob length, then
** raise an SQLITE_TOOBIG exception and return NULL.
*/
static void *contextMalloc(sqlitex_context *context, i64 nByte){
  char *z;
  sqlitex *db = sqlitex_context_db_handle(context);
  assert( nByte>0 );
  testcase( nByte==db->aLimit[SQLITE_LIMIT_LENGTH] );
  testcase( nByte==db->aLimit[SQLITE_LIMIT_LENGTH]+1 );
  if( nByte>db->aLimit[SQLITE_LIMIT_LENGTH] ){
    sqlitex_result_error_toobig(context);
    z = 0;
  }else{
    z = sqlitexMalloc(nByte);
    if( !z ){
      sqlitex_result_error_nomem(context);
    }
  }
  return z;
}

/*
** Implementation of the upper() and lower() SQL functions.
*/
static void upperFunc(sqlitex_context *context, int argc, sqlitex_value **argv){
  char *z1;
  const char *z2;
  int i, n;
  UNUSED_PARAMETER(argc);
  z2 = (char*)sqlitex_value_text(argv[0]);
  n = sqlitex_value_bytes(argv[0]);
  /* Verify that the call to _bytes() does not invalidate the _text() pointer */
  assert( z2==(char*)sqlitex_value_text(argv[0]) );
  if( z2 ){
    z1 = contextMalloc(context, ((i64)n)+1);
    if( z1 ){
      for(i=0; i<n; i++){
        z1[i] = (char)sqlitexToupper(z2[i]);
      }
      sqlitex_result_text(context, z1, n, sqlitex_free);
    }
  }
}
static void lowerFunc(sqlitex_context *context, int argc, sqlitex_value **argv){
  char *z1;
  const char *z2;
  int i, n;
  UNUSED_PARAMETER(argc);
  z2 = (char*)sqlitex_value_text(argv[0]);
  n = sqlitex_value_bytes(argv[0]);
  /* Verify that the call to _bytes() does not invalidate the _text() pointer */
  assert( z2==(char*)sqlitex_value_text(argv[0]) );
  if( z2 ){
    z1 = contextMalloc(context, ((i64)n)+1);
    if( z1 ){
      for(i=0; i<n; i++){
        z1[i] = sqlitexTolower(z2[i]);
      }
      sqlitex_result_text(context, z1, n, sqlitex_free);
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
  sqlitex_context *context,
  int NotUsed,
  sqlitex_value **NotUsed2
){
  sqlite_int64 r;
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  sqlitex_randomness(sizeof(r), &r);
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
  sqlitex_result_int64(context, r);
}

/*
** Implementation of randomblob(N).  Return a random blob
** that is N bytes long.
*/
static void randomBlob(
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  int n;
  unsigned char *p;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  n = sqlitex_value_int(argv[0]);
  if( n<1 ){
    n = 1;
  }
  p = contextMalloc(context, n);
  if( p ){
    sqlitex_randomness(n, p);
    sqlitex_result_blob(context, (char*)p, n, sqlitex_free);
  }
}

/*
** Implementation of the comdb2_sysinfo() SQL function.  The return
** value depends on the class of system information being requested.
*/
static void comdb2SysinfoFunc(
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  const char *zName;
  assert( argc==1 );
  if( sqlitex_value_type(argv[0])!=SQLITE3_TEXT ){
    return;
  }
  zName = sqlitex_value_text(argv[0]);
  if( sqlitex_stricmp(zName, "pid")==0 ){
    sqlitex_result_int64(context, (sqlitex_int64)getpid());
  }
}

/*
** Implementation of the comdb2_version() SQL function.  The return
** value is the same as the stat value for version
*/
static void comdb2VersionFunc(
  sqlitex_context *context, 
  int NotUsed, 
  sqlitex_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  char zBuf[50];
  sqlitex_snprintf(sizeof(zBuf), zBuf, "XXXXXX");
  sqlitex_result_text(context, zBuf, -1, SQLITEX_TRANSIENT);
}

static void comdb2NodeFunc(
  sqlitex_context *context, 
  int NotUsed, 
  sqlitex_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  char zBuf[50];
  const char * node = getenv("HOSTNAME");
  sqlitex_snprintf(sizeof(zBuf), zBuf, "%s", node);
  sqlitex_result_text(context, zBuf, -1, SQLITEX_TRANSIENT);
}

/* COMDB2 MODIFICATIONS */
extern unsigned long long comdb2_table_version(const char *tablename);
/*
** Implementation of the table_version() SQL function.  This returns
** the comdb2 table version
*/
static void tableVersionFunc(
  sqlitex_context *context, 
  int argc, 
  sqlitex_value **argv
){
  const char *tablename;
  sqlite_int64 version;

  assert(argc==1);
  if(sqlitex_value_type(argv[0]) != SQLITE3_TEXT) {
    return;
  }
  
  tablename = sqlitex_value_text(argv[0]);

  version = comdb2_table_version(tablename);
  if (version<0) {
    return;
  }
  
  sqlitex_result_int64(context, version);
}

/* COMDB2 MODIFICATIONS */
extern char* comdb2_partition_info(const char *partition, const char *option);
/*
** Implementation of the table_version() SQL function.  This returns
** the comdb2 table version
*/
static void partitionInfoFunc(
  sqlitex_context *context, 
  int argc, 
  sqlitex_value **argv
){
  sqlitex      *db = sqlitex_context_db_handle(context);
  const char   *partition_name;
  const char   *option;
  char         *info;

  assert(argc==1);
  if(sqlitex_value_type(argv[0]) != SQLITE3_TEXT) {
    return;
  }
  partition_name = sqlitex_value_text(argv[0]);
  
  if(sqlitex_value_type(argv[1]) != SQLITE3_TEXT) {
    return;
  }
  option = sqlitex_value_text(argv[1]);

  info = comdb2_partition_info(partition_name, option);

  if(!info)
  {
    sqlitex_result_null(context);
  }
  else
  {
    sqlitex_result_text(context, info, -1, SQLITEX_TRANSIENT);

    if(info)
      free(info);
  }
}

/*
** Implementation of the last_insert_rowid() SQL function.  The return
** value is the same as the sqlitex_last_insert_rowid() API function.
*/
static void last_insert_rowid(
  sqlitex_context *context, 
  int NotUsed, 
  sqlitex_value **NotUsed2
){
  sqlitex *db = sqlitex_context_db_handle(context);
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  /* IMP: R-51513-12026 The last_insert_rowid() SQL function is a
  ** wrapper around the sqlitex_last_insert_rowid() C/C++ interface
  ** function. */
  sqlitex_result_int64(context, sqlitex_last_insert_rowid(db));
}

/*
** Implementation of the changes() SQL function.
**
** IMP: R-62073-11209 The changes() SQL function is a wrapper
** around the sqlitex_changes() C/C++ function and hence follows the same
** rules for counting changes.
*/
static void changes(
  sqlitex_context *context,
  int NotUsed,
  sqlitex_value **NotUsed2
){
  sqlitex *db = sqlitex_context_db_handle(context);
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  sqlitex_result_int(context, sqlitex_changes(db));
}

/*
** Implementation of the total_changes() SQL function.  The return value is
** the same as the sqlitex_total_changes() API function.
*/
static void total_changes(
  sqlitex_context *context,
  int NotUsed,
  sqlitex_value **NotUsed2
){
  sqlitex *db = sqlitex_context_db_handle(context);
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  /* IMP: R-52756-41993 This function is a wrapper around the
  ** sqlitex_total_changes() C/C++ interface. */
  sqlitex_result_int(context, sqlitex_total_changes(db));
}

/*
** A structure defining how to do GLOB-style comparisons.
*/
struct compareInfo {
  u8 matchAll;
  u8 matchOne;
  u8 matchSet;
  u8 noCase;
};

/*
** For LIKE and GLOB matching on EBCDIC machines, assume that every
** character is exactly one byte in size.  Also, all characters are
** able to participate in upper-case-to-lower-case mappings in EBCDIC
** whereas only characters less than 0x80 do in ASCII.
*/
#if defined(SQLITE_EBCDIC)
# define sqlitexUtf8Read(A)        (*((*A)++))
# define GlobUpperToLower(A)       A = sqlitexUpperToLower[A]
# define GlobUpperToLowerAscii(A)  A = sqlitexUpperToLower[A]
#else
# define GlobUpperToLower(A)       if( A<=0x7f ){ A = sqlitexUpperToLower[A]; }
# define GlobUpperToLowerAscii(A)  A = sqlitexUpperToLower[A]
#endif

static const struct compareInfo globInfo = { '*', '?', '[', 0 };
/* The correct SQL-92 behavior is for the LIKE operator to ignore
** case.  Thus  'a' LIKE 'A' would be true. */
static const struct compareInfo likeInfoNorm = { '%', '_',   0, 1 };
/* If SQLITE_CASE_SENSITIVE_LIKE is defined, then the LIKE operator
** is case sensitive causing 'a' LIKE 'A' to be false */
static const struct compareInfo likeInfoAlt = { '%', '_',   0, 0 };

/*
** Compare two UTF-8 strings for equality where the first string can
** potentially be a "glob" or "like" expression.  Return true (1) if they
** are the same and false (0) if they are different.
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
** The comments through this routine usually assume glob matching.
**
** This routine is usually quick, but can be N**2 in the worst case.
*/
static int patternCompare(
  const u8 *zPattern,              /* The glob pattern */
  const u8 *zString,               /* The string to compare against the glob */
  const struct compareInfo *pInfo, /* Information about how to do the compare */
  u32 esc                          /* The escape character */
){
  u32 c, c2;                       /* Next pattern and input string chars */
  u32 matchOne = pInfo->matchOne;  /* "?" or "_" */
  u32 matchAll = pInfo->matchAll;  /* "*" or "%" */
  u32 matchOther;                  /* "[" or the escape character */
  u8 noCase = pInfo->noCase;       /* True if uppercase==lowercase */
  const u8 *zEscaped = 0;          /* One past the last escaped input char */
  
  /* The GLOB operator does not have an ESCAPE clause.  And LIKE does not
  ** have the matchSet operator.  So we either have to look for one or
  ** the other, never both.  Hence the single variable matchOther is used
  ** to store the one we have to look for.
  */
  matchOther = esc ? esc : pInfo->matchSet;

  while( (c = sqlitexUtf8Read(&zPattern))!=0 ){
    if( c==matchAll ){  /* Match "*" */
      /* Skip over multiple "*" characters in the pattern.  If there
      ** are also "?" characters, skip those as well, but consume a
      ** single character of the input string for each "?" skipped */
      while( (c=sqlitexUtf8Read(&zPattern)) == matchAll
               || c == matchOne ){
        if( c==matchOne && sqlitexUtf8Read(&zString)==0 ){
          return 0;
        }
      }
      if( c==0 ){
        return 1;   /* "*" at the end of the pattern matches */
      }else if( c==matchOther ){
        if( esc ){
          c = sqlitexUtf8Read(&zPattern);
          if( c==0 ) return 0;
        }else{
          /* "[...]" immediately follows the "*".  We have to do a slow
          ** recursive search in this case, but it is an unusual case. */
          assert( matchOther<0x80 );  /* '[' is a single-byte character */
          while( *zString
                 && patternCompare(&zPattern[-1],zString,pInfo,esc)==0 ){
            SQLITE_SKIP_UTF8(zString);
          }
          return *zString!=0;
        }
      }

      /* At this point variable c contains the first character of the
      ** pattern string past the "*".  Search in the input string for the
      ** first matching character and recursively contine the match from
      ** that point.
      **
      ** For a case-insensitive search, set variable cx to be the same as
      ** c but in the other case and search the input string for either
      ** c or cx.
      */
      if( c<=0x80 ){
        u32 cx;
        if( noCase ){
          cx = sqlitexToupper(c);
          c = sqlitexTolower(c);
        }else{
          cx = c;
        }
        while( (c2 = *(zString++))!=0 ){
          if( c2!=c && c2!=cx ) continue;
          if( patternCompare(zPattern,zString,pInfo,esc) ) return 1;
        }
      }else{
        while( (c2 = sqlitexUtf8Read(&zString))!=0 ){
          if( c2!=c ) continue;
          if( patternCompare(zPattern,zString,pInfo,esc) ) return 1;
        }
      }
      return 0;
    }
    if( c==matchOther ){
      if( esc ){
        c = sqlitexUtf8Read(&zPattern);
        if( c==0 ) return 0;
        zEscaped = zPattern;
      }else{
        u32 prior_c = 0;
        int seen = 0;
        int invert = 0;
        c = sqlitexUtf8Read(&zString);
        if( c==0 ) return 0;
        c2 = sqlitexUtf8Read(&zPattern);
        if( c2=='^' ){
          invert = 1;
          c2 = sqlitexUtf8Read(&zPattern);
        }
        if( c2==']' ){
          if( c==']' ) seen = 1;
          c2 = sqlitexUtf8Read(&zPattern);
        }
        while( c2 && c2!=']' ){
          if( c2=='-' && zPattern[0]!=']' && zPattern[0]!=0 && prior_c>0 ){
            c2 = sqlitexUtf8Read(&zPattern);
            if( c>=prior_c && c<=c2 ) seen = 1;
            prior_c = 0;
          }else{
            if( c==c2 ){
              seen = 1;
            }
            prior_c = c2;
          }
          c2 = sqlitexUtf8Read(&zPattern);
        }
        if( c2==0 || (seen ^ invert)==0 ){
          return 0;
        }
        continue;
      }
    }
    c2 = sqlitexUtf8Read(&zString);
    if( c==c2 ) continue;
    if( noCase && c<0x80 && c2<0x80 && sqlitexTolower(c)==sqlitexTolower(c2) ){
      continue;
    }
    if( c==matchOne && zPattern!=zEscaped && c2!=0 ) continue;
    return 0;
  }
  return *zString==0;
}

/*
** The sqlitex_strglob() interface.
*/
int sqlitex_strglob(const char *zGlobPattern, const char *zString){
  return patternCompare((u8*)zGlobPattern, (u8*)zString, &globInfo, 0)==0;
}

/*
** Count the number of times that the LIKE operator (or GLOB which is
** just a variation of LIKE) gets called.  This is used for testing
** only.
*/
#ifdef SQLITE_TEST
int sqlitex_like_count = 0;
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
  sqlitex_context *context, 
  int argc, 
  sqlitex_value **argv
){
  const unsigned char *zA, *zB;
  u32 escape = 0;
  int nPat;
  sqlitex *db = sqlitex_context_db_handle(context);

#ifdef SQLITE_LIKE_DOESNT_MATCH_BLOBS
  if( sqlitex_value_type(argv[0])==SQLITE_BLOB
   || sqlitex_value_type(argv[1])==SQLITE_BLOB
  ){
#ifdef SQLITE_TEST
    sqlitex_like_count++;
#endif
    sqlitex_result_int(context, 0);
    return;
  }
#endif
  zB = sqlitex_value_text(argv[0]);
  zA = sqlitex_value_text(argv[1]);

  /* Limit the length of the LIKE or GLOB pattern to avoid problems
  ** of deep recursion and N*N behavior in patternCompare().
  */
  nPat = sqlitex_value_bytes(argv[0]);
  testcase( nPat==db->aLimit[SQLITE_LIMIT_LIKE_PATTERN_LENGTH] );
  testcase( nPat==db->aLimit[SQLITE_LIMIT_LIKE_PATTERN_LENGTH]+1 );
  if( nPat > db->aLimit[SQLITE_LIMIT_LIKE_PATTERN_LENGTH] ){
    sqlitex_result_error(context, "LIKE or GLOB pattern too complex", -1);
    return;
  }
  assert( zB==sqlitex_value_text(argv[0]) );  /* Encoding did not change */

  if( argc==3 ){
    /* The escape character string must consist of a single UTF-8 character.
    ** Otherwise, return an error.
    */
    const unsigned char *zEsc = sqlitex_value_text(argv[2]);
    if( zEsc==0 ) return;
    if( sqlitexUtf8CharLen((char*)zEsc, -1)!=1 ){
      sqlitex_result_error(context, 
          "ESCAPE expression must be a single character", -1);
      return;
    }
    escape = sqlitexUtf8Read(&zEsc);
  }
  if( zA && zB ){
    struct compareInfo *pInfo = sqlitex_user_data(context);
#ifdef SQLITE_TEST
    sqlitex_like_count++;
#endif
    
    sqlitex_result_int(context, patternCompare(zB, zA, pInfo, escape));
  }
}

/*
** Implementation of the NULLIF(x,y) function.  The result is the first
** argument if the arguments are different.  The result is NULL if the
** arguments are equal to each other.
*/
static void nullifFunc(
  sqlitex_context *context,
  int NotUsed,
  sqlitex_value **argv
){
  CollSeq *pColl = sqlitexGetFuncCollSeq(context);
  UNUSED_PARAMETER(NotUsed);
  if( sqlitexMemCompare(argv[0], argv[1], pColl)!=0 ){
    sqlitex_result_value(context, argv[0]);
  }
}

/*
** Implementation of the sqlite_version() function.  The result is the version
** of the SQLite library that is running.
*/
static void versionFunc(
  sqlitex_context *context,
  int NotUsed,
  sqlitex_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  /* IMP: R-48699-48617 This function is an SQL wrapper around the
  ** sqlitex_libversion() C-interface. */
  sqlitex_result_text(context, sqlitex_libversion(), -1, SQLITEX_STATIC);
}

/*
** Implementation of the sqlite_source_id() function. The result is a string
** that identifies the particular version of the source code used to build
** SQLite.
*/
static void sourceidFunc(
  sqlitex_context *context,
  int NotUsed,
  sqlitex_value **NotUsed2
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  /* IMP: R-24470-31136 This function is an SQL wrapper around the
  ** sqlitex_sourceid() C interface. */
  sqlitex_result_text(context, sqlitex_sourceid(), -1, SQLITEX_STATIC);
}

/*
** Implementation of the sqlite_log() function.  This is a wrapper around
** sqlitex_log().  The return value is NULL.  The function exists purely for
** its side-effects.
*/
static void errlogFunc(
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  UNUSED_PARAMETER(argc);
  UNUSED_PARAMETER(context);
  sqlitex_log(sqlitex_value_int(argv[0]), "%s", sqlitex_value_text(argv[1]));
}

/*
** Implementation of the sqlite_compileoption_used() function.
** The result is an integer that identifies if the compiler option
** was used to build SQLite.
*/
#ifndef SQLITE_OMIT_COMPILEOPTION_DIAGS
static void compileoptionusedFunc(
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  const char *zOptName;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  /* IMP: R-39564-36305 The sqlite_compileoption_used() SQL
  ** function is a wrapper around the sqlitex_compileoption_used() C/C++
  ** function.
  */
  if( (zOptName = (const char*)sqlitex_value_text(argv[0]))!=0 ){
    sqlitex_result_int(context, sqlitex_compileoption_used(zOptName));
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
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  int n;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  /* IMP: R-04922-24076 The sqlite_compileoption_get() SQL function
  ** is a wrapper around the sqlitex_compileoption_get() C/C++ function.
  */
  n = sqlitex_value_int(argv[0]);
  sqlitex_result_text(context, sqlitex_compileoption_get(n), -1, SQLITEX_STATIC);
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
static void quoteFunc(sqlitex_context *context, int argc, sqlitex_value **argv){
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  switch( sqlitex_value_type(argv[0]) ){
    case SQLITE_FLOAT: {
      double r1, r2;
      char zBuf[50];
      r1 = sqlitex_value_double(argv[0]);
      sqlitex_snprintf(sizeof(zBuf), zBuf, "%!.15g", r1);
      sqlitexAtoF(zBuf, &r2, 20, SQLITE_UTF8);
      if( r1!=r2 ){
        sqlitex_snprintf(sizeof(zBuf), zBuf, "%!.20e", r1);
      }
      sqlitex_result_text(context, zBuf, -1, SQLITEX_TRANSIENT);
      break;
    }
    case SQLITE_INTEGER: {
      sqlitex_result_value(context, argv[0]);
      break;
    }
    case SQLITE_BLOB: {
      char *zText = 0;
      char const *zBlob = sqlitex_value_blob(argv[0]);
      int nBlob = sqlitex_value_bytes(argv[0]);
      assert( zBlob==sqlitex_value_blob(argv[0]) ); /* No encoding change */
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
        sqlitex_result_text(context, zText, -1, SQLITEX_TRANSIENT);
        sqlitex_free(zText);
      }
      break;
    }
    case SQLITE3_TEXT: {
      int i,j;
      u64 n;
      const unsigned char *zArg = sqlitex_value_text(argv[0]);
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
        sqlitex_result_text(context, z, j, sqlitex_free);
      }
      break;
    }
    default: {
      assert( sqlitex_value_type(argv[0])==SQLITE_NULL );
      sqlitex_result_text(context, "NULL", 4, SQLITEX_STATIC);
      break;
    }
  }
}

/*
** The unicode() function.  Return the integer unicode code-point value
** for the first character of the input string. 
*/
static void unicodeFunc(
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  const unsigned char *z = sqlitex_value_text(argv[0]);
  (void)argc;
  if( z && z[0] ) sqlitex_result_int(context, sqlitexUtf8Read(&z));
}

/*
** The char() function takes zero or more arguments, each of which is
** an integer.  It constructs a string where each character of the string
** is the unicode character for the corresponding integer argument.
*/
static void charFunc(
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  unsigned char *z, *zOut;
  int i;
  zOut = z = sqlitex_malloc( argc*4+1 );
  if( z==0 ){
    sqlitex_result_error_nomem(context);
    return;
  }
  for(i=0; i<argc; i++){
    sqlitex_int64 x;
    unsigned c;
    x = sqlitex_value_int64(argv[i]);
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
  sqlitex_result_text64(context, (char*)z, zOut-z, sqlitex_free, SQLITE_UTF8);
}

/*
** The hex() function.  Interpret the argument as a blob.  Return
** a hexadecimal rendering as text.
*/
static void hexFunc(
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  int i, n;
  const unsigned char *pBlob;
  char *zHex, *z;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  pBlob = sqlitex_value_blob(argv[0]);
  n = sqlitex_value_bytes(argv[0]);
  assert( pBlob==sqlitex_value_blob(argv[0]) );  /* No encoding change */
  z = zHex = contextMalloc(context, ((i64)n)*2 + 1);
  if( zHex ){
    for(i=0; i<n; i++, pBlob++){
      unsigned char c = *pBlob;
      *(z++) = hexdigits[(c>>4)&0xf];
      *(z++) = hexdigits[c&0xf];
    }
    *z = 0;
    sqlitex_result_text(context, zHex, n*2, sqlitex_free);
  }
}

/*
** The zeroblob(N) function returns a zero-filled blob of size N bytes.
*/
static void zeroblobFunc(
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  i64 n;
  sqlitex *db = sqlitex_context_db_handle(context);
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  n = sqlitex_value_int64(argv[0]);
  testcase( n==db->aLimit[SQLITE_LIMIT_LENGTH] );
  testcase( n==db->aLimit[SQLITE_LIMIT_LENGTH]+1 );
  if( n>db->aLimit[SQLITE_LIMIT_LENGTH] ){
    sqlitex_result_error_toobig(context);
  }else{
    sqlitex_result_zeroblob(context, (int)n); /* IMP: R-00293-64994 */
  }
}

/*
** The replace() function.  Three arguments are all strings: call
** them A, B, and C. The result is also a string which is derived
** from A by replacing every occurrence of B with C.  The match
** must be exact.  Collating sequences are not used.
*/
static void replaceFunc(
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
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

  assert( argc==3 );
  UNUSED_PARAMETER(argc);
  zStr = sqlitex_value_text(argv[0]);
  if( zStr==0 ) return;
  nStr = sqlitex_value_bytes(argv[0]);
  assert( zStr==sqlitex_value_text(argv[0]) );  /* No encoding change */
  zPattern = sqlitex_value_text(argv[1]);
  if( zPattern==0 ){
    assert( sqlitex_value_type(argv[1])==SQLITE_NULL
            || sqlitex_context_db_handle(context)->mallocFailed );
    return;
  }
  if( zPattern[0]==0 ){
    assert( sqlitex_value_type(argv[1])!=SQLITE_NULL );
    sqlitex_result_value(context, argv[0]);
    return;
  }
  nPattern = sqlitex_value_bytes(argv[1]);
  assert( zPattern==sqlitex_value_text(argv[1]) );  /* No encoding change */
  zRep = sqlitex_value_text(argv[2]);
  if( zRep==0 ) return;
  nRep = sqlitex_value_bytes(argv[2]);
  assert( zRep==sqlitex_value_text(argv[2]) );
  nOut = nStr + 1;
  assert( nOut<SQLITE_MAX_LENGTH );
  zOut = contextMalloc(context, (i64)nOut);
  if( zOut==0 ){
    return;
  }
  loopLimit = nStr - nPattern;  
  for(i=j=0; i<=loopLimit; i++){
    if( zStr[i]!=zPattern[0] || memcmp(&zStr[i], zPattern, nPattern) ){
      zOut[j++] = zStr[i];
    }else{
      u8 *zOld;
      sqlitex *db = sqlitex_context_db_handle(context);
      nOut += nRep - nPattern;
      testcase( nOut-1==db->aLimit[SQLITE_LIMIT_LENGTH] );
      testcase( nOut-2==db->aLimit[SQLITE_LIMIT_LENGTH] );
      if( nOut-1>db->aLimit[SQLITE_LIMIT_LENGTH] ){
        sqlitex_result_error_toobig(context);
        sqlitex_free(zOut);
        return;
      }
      zOld = zOut;
      zOut = sqlitex_realloc(zOut, (int)nOut);
      if( zOut==0 ){
        sqlitex_result_error_nomem(context);
        sqlitex_free(zOld);
        return;
      }
      memcpy(&zOut[j], zRep, nRep);
      j += nRep;
      i += nPattern-1;
    }
  }
  assert( j+nStr-i+1==nOut );
  memcpy(&zOut[j], &zStr[i], nStr-i);
  j += nStr - i;
  assert( j<=nOut );
  zOut[j] = 0;
  sqlitex_result_text(context, (char*)zOut, j, sqlitex_free);
}

/*
** Implementation of the TRIM(), LTRIM(), and RTRIM() functions.
** The userdata is 0x1 for left trim, 0x2 for right trim, 0x3 for both.
*/
static void trimFunc(
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  const unsigned char *zIn;         /* Input string */
  const unsigned char *zCharSet;    /* Set of characters to trim */
  int nIn;                          /* Number of bytes in input */
  int flags;                        /* 1: trimleft  2: trimright  3: trim */
  int i;                            /* Loop counter */
  unsigned char *aLen = 0;          /* Length of each character in zCharSet */
  unsigned char **azChar = 0;       /* Individual characters in zCharSet */
  int nChar;                        /* Number of characters in zCharSet */

  if( sqlitex_value_type(argv[0])==SQLITE_NULL ){
    return;
  }
  zIn = sqlitex_value_text(argv[0]);
  if( zIn==0 ) return;
  nIn = sqlitex_value_bytes(argv[0]);
  assert( zIn==sqlitex_value_text(argv[0]) );
  if( argc==1 ){
    static const unsigned char lenOne[] = { 1 };
    static unsigned char * const azOne[] = { (u8*)" " };
    nChar = 1;
    aLen = (u8*)lenOne;
    azChar = (unsigned char **)azOne;
    zCharSet = 0;
  }else if( (zCharSet = sqlitex_value_text(argv[1]))==0 ){
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
    flags = SQLITE_PTR_TO_INT(sqlitex_user_data(context));
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
      sqlitex_free(azChar);
    }
  }
  sqlitex_result_text(context, (char*)zIn, nIn, SQLITEX_TRANSIENT);
}


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
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
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
  zIn = (u8*)sqlitex_value_text(argv[0]);
  if( zIn==0 ) zIn = (u8*)"";
  for(i=0; zIn[i] && !sqlitexIsalpha(zIn[i]); i++){}
  if( zIn[i] ){
    u8 prevcode = iCode[zIn[i]&0x7f];
    zResult[0] = sqlitexToupper(zIn[i]);
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
    sqlitex_result_text(context, zResult, 4, SQLITEX_TRANSIENT);
  }else{
    /* IMP: R-64894-50321 The string "?000" is returned if the argument
    ** is NULL or contains no ASCII alphabetic characters. */
    sqlitex_result_text(context, "?000", 4, SQLITEX_STATIC);
  }
}
#endif /* SQLITE_SOUNDEX */

#ifndef SQLITE_OMIT_LOAD_EXTENSION
/*
** A function that loads a shared-library extension then returns NULL.
*/
static void loadExt(sqlitex_context *context, int argc, sqlitex_value **argv){
  const char *zFile = (const char *)sqlitex_value_text(argv[0]);
  const char *zProc;
  sqlitex *db = sqlitex_context_db_handle(context);
  char *zErrMsg = 0;

  if( argc==2 ){
    zProc = (const char *)sqlitex_value_text(argv[1]);
  }else{
    zProc = 0;
  }
  if( zFile && sqlitex_load_extension(db, zFile, zProc, &zErrMsg) ){
    sqlitex_result_error(context, zErrMsg, -1);
    sqlitex_free(zErrMsg);
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
  u8 decs;          /* True if summing decimals */
  decQuad decSum;   /* decQuad aggregation */
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
static void sumStep(sqlitex_context *context, int argc, sqlitex_value **argv){
  SumCtx *p;
  int type;
  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  p = sqlitex_aggregate_context(context, sizeof(*p));
  type = sqlitex_value_numeric_type(argv[0]);
  if( p && type!=SQLITE_NULL ){
    p->cnt++;
    if( type==SQLITE_INTEGER ){
      i64 v = sqlitex_value_int64(argv[0]);
      p->rSum += v;
      if( (p->approx|p->overflow)==0 && sqlitexAddInt64(&p->iSum, v) ){
        p->overflow = 1;
      }
    }else if( type==SQLITE_DECIMAL){
       intv_t v = *(intv_t*)sqlitex_value_interval(argv[0], SQLITE_DECIMAL);

       if (p->decs == 0)
       {
          p->decSum = v.u.dec;
          p->decs = 1;

          if(0) {
             char aaa[128];
             char bbb[128];

             decQuadToString( &p->decSum, aaa);
             decQuadToString( &v.u.dec, bbb);

             fprintf(stderr, "%s  = %s\n",
                   aaa, bbb);
          }
       }
       else
       {
          decContext ctx;
          decQuad    res;
       
          dec_ctx_init( &ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);
          decQuadAdd( &res, &p->decSum, &v.u.dec, &ctx);

          if (dfp_conv_check_status(&ctx, "quad", "add(quads)"))
          {
             sqlitex_result_error(context, "decimal overflow", -1);
          }

          if(0){
             char aaa[128];
             char bbb[128];
             char ccc[128];

             decQuadToString( &p->decSum, aaa);
             decQuadToString( &v.u.dec, bbb);
             decQuadToString( &res, ccc);

             fprintf(stderr, "%s + %s = %s\n",
                   aaa, bbb, ccc);
          }

          p->decSum = res;
       }

    }else{
      p->rSum += sqlitex_value_double(argv[0]);
      p->approx = 1;
    }
  }
}
static void sumFinalize(sqlitex_context *context){
  SumCtx *p;
  p = sqlitex_aggregate_context(context, 0);
  if( p && p->cnt>0 ){
    if( p->overflow ){
      sqlitex_result_error(context,"integer overflow",-1);
    }else if( p->approx ){
      sqlitex_result_double(context, p->rSum);
    }else if( p->decs){
       intv_t res;
       res.type = INTV_DECIMAL_TYPE;
       res.sign = 0;
       res.u.dec = p->decSum;
       sqlitex_result_interval(context, &res);
    }else{
      sqlitex_result_int64(context, p->iSum);
    }
  }
}
static void avgFinalize(sqlitex_context *context){
  SumCtx *p;
  p = sqlitex_aggregate_context(context, 0);
  if( p && p->cnt>0 ){
     if( p->decs)
     {
        decContext ctx;
        decQuad denom;
        decQuad res;
        intv_t  tv;

        dec_ctx_init( &ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);
        decQuadFromInt32( &denom, p->cnt);
        decQuadDivide( &res, &p->decSum, &denom, &ctx);
        if (dfp_conv_check_status(&ctx, "quad", "divide(quad)"))
        {
           sqlitex_result_error(context, "decimal overflow", -1);
        }

        tv.type = INTV_DECIMAL_TYPE;
        tv.sign = 0;
        tv.u.dec = res;

        sqlitex_result_interval( context, &tv);
     }
     else
        sqlitex_result_double(context, p->rSum/(double)p->cnt);
  }
}
static void totalFinalize(sqlitex_context *context){
  SumCtx *p;
  p = sqlitex_aggregate_context(context, 0);
  /* (double)0 In case of SQLITE_OMIT_FLOATING_POINT... */
  if (p && p->decs)
  {
     intv_t res;
     res.type = INTV_DECIMAL_TYPE;
     res.sign = 0;
     res.u.dec = p->decSum;
     sqlitex_result_interval(context, &res);
  }
  else
     sqlitex_result_double(context, p ? p->rSum : (double)0);
}

/*
** The following structure keeps track of state information for the
** count() aggregate function.
*/
typedef struct CountCtx CountCtx;
struct CountCtx {
  i64 n;
};

/*
** Routines to implement the count() aggregate function.
*/
static void countStep(sqlitex_context *context, int argc, sqlitex_value **argv){
  CountCtx *p;
  p = sqlitex_aggregate_context(context, sizeof(*p));
  if( (argc==0 || SQLITE_NULL!=sqlitex_value_type(argv[0])) && p ){
    p->n++;
  }

#ifndef SQLITE_OMIT_DEPRECATED
  /* The sqlitex_aggregate_count() function is deprecated.  But just to make
  ** sure it still operates correctly, verify that its count agrees with our 
  ** internal count when using count(*) and when the total count can be
  ** expressed as a 32-bit integer. */
  assert( argc==1 || p==0 || p->n>0x7fffffff
          || p->n==sqlitex_aggregate_count(context) );
#endif
}   
static void countFinalize(sqlitex_context *context){
  CountCtx *p;
  p = sqlitex_aggregate_context(context, 0);
  sqlitex_result_int64(context, p ? p->n : 0);
}

/*
** Routines to implement min() and max() aggregate functions.
*/
static void minmaxStep(
  sqlitex_context *context, 
  int NotUsed, 
  sqlitex_value **argv
){
  Mem *pArg  = (Mem *)argv[0];
  Mem *pBest;
  UNUSED_PARAMETER(NotUsed);

  pBest = (Mem *)sqlitex_aggregate_context(context, sizeof(*pBest));
  if( !pBest ) return;

  if( sqlitex_value_type(argv[0])==SQLITE_NULL ){
    if( pBest->flags ) sqlitexSkipAccumulatorLoad(context);
  }else if( pBest->flags ){
    int max;
    int cmp;
    CollSeq *pColl = sqlitexGetFuncCollSeq(context);
    /* This step function is used for both the min() and max() aggregates,
    ** the only difference between the two being that the sense of the
    ** comparison is inverted. For the max() aggregate, the
    ** sqlitex_user_data() function returns (void *)-1. For min() it
    ** returns (void *)db, where db is the sqlitex* database pointer.
    ** Therefore the next statement sets variable 'max' to 1 for the max()
    ** aggregate, or 0 for min().
    */
    max = sqlitex_user_data(context)!=0;
    cmp = sqlitexMemCompare(pBest, pArg, pColl);
    if( (max && cmp<0) || (!max && cmp>0) ){
      sqlitexVdbeMemCopy(pBest, pArg);
    }else{
      sqlitexSkipAccumulatorLoad(context);
    }
  }else{
    pBest->db = sqlitex_context_db_handle(context);
    sqlitexVdbeMemCopy(pBest, pArg);
  }
}
static void minMaxFinalize(sqlitex_context *context){
  sqlitex_value *pRes;
  pRes = (sqlitex_value *)sqlitex_aggregate_context(context, 0);
  if( pRes ){
    if( pRes->flags ){
      sqlitex_result_value(context, pRes);
    }
    sqlitexVdbeMemRelease(pRes);
  }
}

/*
** group_concat(EXPR, ?SEPARATOR?)
*/
static void groupConcatStep(
  sqlitex_context *context,
  int argc,
  sqlitex_value **argv
){
  const char *zVal;
  StrAccum *pAccum;
  const char *zSep;
  int nVal, nSep;
  assert( argc==1 || argc==2 );
  if( sqlitex_value_type(argv[0])==SQLITE_NULL ) return;
  pAccum = (StrAccum*)sqlitex_aggregate_context(context, sizeof(*pAccum));

  if( pAccum ){
    sqlitex *db = sqlitex_context_db_handle(context);
    int firstTerm = pAccum->useMalloc==0;
    pAccum->useMalloc = 2;
    pAccum->mxAlloc = db->aLimit[SQLITE_LIMIT_LENGTH];
    if( !firstTerm ){
      if( argc==2 ){
        zSep = (char*)sqlitex_value_text(argv[1]);
        nSep = sqlitex_value_bytes(argv[1]);
      }else{
        zSep = ",";
        nSep = 1;
      }
      if( nSep ) sqlitexStrAccumAppend(pAccum, zSep, nSep);
    }
    zVal = (char*)sqlitex_value_text(argv[0]);
    nVal = sqlitex_value_bytes(argv[0]);
    if( zVal ) sqlitexStrAccumAppend(pAccum, zVal, nVal);
  }
}
static void groupConcatFinalize(sqlitex_context *context){
  StrAccum *pAccum;
  pAccum = sqlitex_aggregate_context(context, 0);
  if( pAccum ){
    if( pAccum->accError==STRACCUM_TOOBIG ){
      sqlitex_result_error_toobig(context);
    }else if( pAccum->accError==STRACCUM_NOMEM ){
      sqlitex_result_error_nomem(context);
    }else{    
      sqlitex_result_text(context, sqlitexStrAccumFinish(pAccum), -1, 
                          sqlitex_free);
    }
  }
}

/*
** This routine does per-connection function registration.  Most
** of the built-in functions above are part of the global function set.
** This routine only deals with those that are not global.
*/
void sqlitexRegisterBuiltinFunctions(sqlitex *db){
  int rc = sqlitex_overload_function(db, "MATCH", 2);
  assert( rc==SQLITE_NOMEM || rc==SQLITE_OK );
  if( rc==SQLITE_NOMEM ){
    db->mallocFailed = 1;
  }
}

/*
** Set the LIKEOPT flag on the 2-argument function with the given name.
*/
static void setLikeOptFlag(sqlitex *db, const char *zName, u8 flagVal){
  FuncDef *pDef;
  pDef = sqlitexFindFunction(db, zName, sqlitexStrlen30(zName),
                             2, SQLITE_UTF8, 0);
  if( ALWAYS(pDef) ){
    pDef->funcFlags |= flagVal;
  }
}

/*
** Register the built-in LIKE and GLOB functions.  The caseSensitive
** parameter determines whether or not the LIKE operator is case
** sensitive.  GLOB is always case sensitive.
*/
void sqlitexRegisterLikeFunctions(sqlitex *db, int caseSensitive){
  struct compareInfo *pInfo;
  if( caseSensitive ){
    pInfo = (struct compareInfo*)&likeInfoAlt;
  }else{
    pInfo = (struct compareInfo*)&likeInfoNorm;
  }
  sqlitexCreateFunc(db, "like", 2, SQLITE_UTF8, pInfo, likeFunc, 0, 0, 0);
  sqlitexCreateFunc(db, "like", 3, SQLITE_UTF8, pInfo, likeFunc, 0, 0, 0);
  sqlitexCreateFunc(db, "glob", 2, SQLITE_UTF8, 
      (struct compareInfo*)&globInfo, likeFunc, 0, 0, 0);
  setLikeOptFlag(db, "glob", SQLITE_FUNC_LIKE | SQLITE_FUNC_CASE);
  setLikeOptFlag(db, "like", 
      caseSensitive ? (SQLITE_FUNC_LIKE | SQLITE_FUNC_CASE) : SQLITE_FUNC_LIKE);
}

/*
** pExpr points to an expression which implements a function.  If
** it is appropriate to apply the LIKE optimization to that function
** then set aWc[0] through aWc[2] to the wildcard characters and
** return TRUE.  If the function is not a LIKE-style function then
** return FALSE.
**
** *pIsNocase is set to true if uppercase and lowercase are equivalent for
** the function (default for LIKE).  If the function makes the distinction
** between uppercase and lowercase (as does GLOB) then *pIsNocase is set to
** false.
*/
int sqlitexIsLikeFunction(sqlitex *db, Expr *pExpr, int *pIsNocase, char *aWc){
  FuncDef *pDef;
  if( pExpr->op!=TK_FUNCTION 
   || !pExpr->x.pList 
   || pExpr->x.pList->nExpr!=2
  ){
    return 0;
  }
  assert( !ExprHasProperty(pExpr, EP_xIsSelect) );
  pDef = sqlitexFindFunction(db, pExpr->u.zToken, 
                             sqlitexStrlen30(pExpr->u.zToken),
                             2, SQLITE_UTF8, 0);
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
  *pIsNocase = (pDef->funcFlags & SQLITE_FUNC_CASE)==0;
  return 1;
}

/*
** All of the FuncDef structures in the aBuiltinFunc[] array above
** to the global function hash table.  This occurs at start-time (as
** a consequence of calling sqlitex_initialize()).
**
** After this routine runs
*/
void sqlitexRegisterGlobalFunctions(void){
  /*
  ** The following array holds FuncDef structures for all of the functions
  ** defined in this file.
  **
  ** The array cannot be constant since changes are made to the
  ** FuncDef.pHash elements at start-time.  The elements of this array
  ** are read-only after initialization is complete.
  */
  static SQLITE_WSD FuncDef aBuiltinFunc[] = {
    FUNCTION(ltrim,              1, 1, 0, trimFunc         ),
    FUNCTION(ltrim,              2, 1, 0, trimFunc         ),
    FUNCTION(rtrim,              1, 2, 0, trimFunc         ),
    FUNCTION(rtrim,              2, 2, 0, trimFunc         ),
    FUNCTION(trim,               1, 3, 0, trimFunc         ),
    FUNCTION(trim,               2, 3, 0, trimFunc         ),
    FUNCTION(min,               -1, 0, 1, minmaxFunc       ),
    FUNCTION(min,                0, 0, 1, 0                ),
    AGGREGATE2(min,              1, 0, 1, minmaxStep,      minMaxFinalize,
                                          SQLITE_FUNC_MINMAX ),
    FUNCTION(max,               -1, 1, 1, minmaxFunc       ),
    FUNCTION(max,                0, 1, 1, 0                ),
    AGGREGATE2(max,              1, 1, 1, minmaxStep,      minMaxFinalize,
                                          SQLITE_FUNC_MINMAX ),
    FUNCTION2(typeof,            1, 0, 0, typeofFunc,  SQLITE_FUNC_TYPEOF),
    FUNCTION2(length,            1, 0, 0, lengthFunc,  SQLITE_FUNC_LENGTH),
    FUNCTION(instr,              2, 0, 0, instrFunc        ),
    FUNCTION(substr,             2, 0, 0, substrFunc       ),
    FUNCTION(sleep,              1, 0, 0, sleepFunc        ),
    FUNCTION(substr,             3, 0, 0, substrFunc       ),
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
    FUNCTION(coalesce,           1, 0, 0, 0                ),
    FUNCTION(coalesce,           0, 0, 0, 0                ),
    FUNCTION2(coalesce,         -1, 0, 0, noopFunc,  SQLITE_FUNC_COALESCE),
    FUNCTION(hex,                1, 0, 0, hexFunc          ),
    FUNCTION2(ifnull,            2, 0, 0, noopFunc,  SQLITE_FUNC_COALESCE),
    FUNCTION2(unlikely,          1, 0, 0, noopFunc,  SQLITE_FUNC_UNLIKELY),
    FUNCTION2(likelihood,        2, 0, 0, noopFunc,  SQLITE_FUNC_UNLIKELY),
    FUNCTION2(likely,            1, 0, 0, noopFunc,  SQLITE_FUNC_UNLIKELY),
    VFUNCTION(random,            0, 0, 0, randomFunc       ),
    VFUNCTION(randomblob,        1, 0, 0, randomBlob       ),
    FUNCTION(nullif,             2, 0, 1, nullifFunc       ),
    FUNCTION(sqlite_version,     0, 0, 0, versionFunc      ),
    FUNCTION(sqlite_source_id,   0, 0, 0, sourceidFunc     ),
    FUNCTION(sqlite_log,         2, 0, 0, errlogFunc       ),
#if SQLITE_USER_AUTHENTICATION
    FUNCTION(sqlite_crypt,       2, 0, 0, sqlitexCryptFunc ),
#endif
#ifndef SQLITE_OMIT_COMPILEOPTION_DIAGS
    FUNCTION(sqlite_compileoption_used,1, 0, 0, compileoptionusedFunc  ),
    FUNCTION(sqlite_compileoption_get, 1, 0, 0, compileoptiongetFunc  ),
#endif /* SQLITE_OMIT_COMPILEOPTION_DIAGS */
    FUNCTION(quote,              1, 0, 0, quoteFunc        ),
#ifndef SQLITE_BUILDING_FOR_COMDB2
    VFUNCTION(last_insert_rowid, 0, 0, 0, last_insert_rowid),
#endif
    VFUNCTION(changes,           0, 0, 0, changes          ),
    VFUNCTION(total_changes,     0, 0, 0, total_changes    ),
    FUNCTION(replace,            3, 0, 0, replaceFunc      ),
    FUNCTION(zeroblob,           1, 0, 0, zeroblobFunc     ),
  #ifdef SQLITE_SOUNDEX
    FUNCTION(soundex,            1, 0, 0, soundexFunc      ),
  #endif
  #ifndef SQLITE_OMIT_LOAD_EXTENSION
    FUNCTION(load_extension,     1, 0, 0, loadExt          ),
    FUNCTION(load_extension,     2, 0, 0, loadExt          ),
  #endif
    AGGREGATE(sum,               1, 0, 0, sumStep,         sumFinalize    ),
    AGGREGATE(total,             1, 0, 0, sumStep,         totalFinalize    ),
    AGGREGATE(avg,               1, 0, 0, sumStep,         avgFinalize    ),
    AGGREGATE2(count,            0, 0, 0, countStep,       countFinalize,
               SQLITE_FUNC_COUNT  ),
    AGGREGATE(count,             1, 0, 0, countStep,       countFinalize  ),
    AGGREGATE(group_concat,      1, 0, 0, groupConcatStep, groupConcatFinalize),
    AGGREGATE(group_concat,      2, 0, 0, groupConcatStep, groupConcatFinalize),
  
    LIKEFUNC(glob, 2, &globInfo, SQLITE_FUNC_LIKE|SQLITE_FUNC_CASE),
  #ifdef SQLITE_CASE_SENSITIVE_LIKE
    LIKEFUNC(like, 2, &likeInfoAlt, SQLITE_FUNC_LIKE|SQLITE_FUNC_CASE),
    LIKEFUNC(like, 3, &likeInfoAlt, SQLITE_FUNC_LIKE|SQLITE_FUNC_CASE),
  #else
    LIKEFUNC(like, 2, &likeInfoNorm, SQLITE_FUNC_LIKE),
    LIKEFUNC(like, 3, &likeInfoNorm, SQLITE_FUNC_LIKE),
  #endif
#ifdef SQLITE_BUILDING_FOR_COMDB2
    FUNCTION(comdb2_sysinfo,     1, 0, 0, comdb2SysinfoFunc),
    FUNCTION(comdb2_version,    0, 0, 0, comdb2VersionFunc),
    FUNCTION(table_version,     1, 0, 0, tableVersionFunc),
    FUNCTION(partition_info,     2, 0, 0, partitionInfoFunc),
    FUNCTION(comdb2_node,    0, 0, 0, comdb2NodeFunc),
#endif
  };

  int i;
  FuncDefHash *pHash = &GLOBAL(FuncDefHash, sqlitexGlobalFunctions);
  FuncDef *aFunc = (FuncDef*)&GLOBAL(FuncDef, aBuiltinFunc);

  for(i=0; i<ArraySize(aBuiltinFunc); i++){
    sqlitexFuncDefInsert(pHash, &aFunc[i]);
  }
  sqlite3RegisterDateTimeFunctions();
#ifndef SQLITE_OMIT_ALTERTABLE
  sqlitexAlterFunctions();
#endif
#if defined(SQLITE_ENABLE_STAT3) || defined(SQLITE_ENABLE_STAT4)
  sqlitexAnalyzeFunctions();
#endif
}
