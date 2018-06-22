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
** This file contains code use to manipulate "Mem" structure.  A "Mem"
** stores a single value in the VDBE.  Mem is an opaque structure visible
** only within the VDBE.  Interface routines refer to a Mem using the
** name sqlite_value
*/
#include "sqliteInt.h"
#include "vdbeInt.h"

/* COMDB2 MODIFICATION */
#include <arpa/inet.h>
#include <inttypes.h>
#include <flibc.h>
#include <strings.h>
#include <types.h>
#include <util.h>
#include "debug_switches.h"
#include "logmsg.h"

#ifdef SQLITE_DEBUG
/*
** Check invariants on a Mem object.
**
** This routine is intended for use inside of assert() statements, like
** this:    assert( sqlite3VdbeCheckMemInvariants(pMem) );
*/
int sqlite3VdbeCheckMemInvariants(Mem *p){
  /* If MEM_Dyn is set then Mem.xDel!=0.  
  ** Mem.xDel is might not be initialized if MEM_Dyn is clear.
  */
  /* COMDB2 MODIFICATION: these asserts dont work for comdb2 */
  //assert( (p->flags & MEM_Dyn)==0 || p->xDel!=0 );

  /* MEM_Dyn may only be set if Mem.szMalloc==0.  In this way we
  ** ensure that if Mem.szMalloc>0 then it is safe to do
  ** Mem.z = Mem.zMalloc without having to check Mem.flags&MEM_Dyn.
  ** That saves a few cycles in inner loops. */
  //assert( (p->flags & MEM_Dyn)==0 || p->szMalloc==0 );

  /* Cannot be both MEM_Int and MEM_Real at the same time */
  assert( (p->flags & (MEM_Int|MEM_Real))!=(MEM_Int|MEM_Real) );

  /* The szMalloc field holds the correct memory allocation size */
  /* COMDB2: TODO: fix this assert -- either to correct spec or explain why it
  is not needed
  assert( p->szMalloc==0
       || p->szMalloc==sqlite3DbMallocSize(p->db,p->zMalloc) ); */

  /* If p holds a string or blob, the Mem.z must point to exactly
  ** one of the following:
  **
  **   (1) Memory in Mem.zMalloc and managed by the Mem object
  **   (2) Memory to be freed using Mem.xDel
  **   (3) An ephemeral string or blob
  **   (4) A static string or blob
  if( (p->flags & (MEM_Str|MEM_Blob)) && p->n>0 ){
    assert(
      ((p->szMalloc>0 && p->z==p->zMalloc)? 1 : 0) +
      ((p->flags&MEM_Dyn)!=0 ? 1 : 0) +
      ((p->flags&MEM_Ephem)!=0 ? 1 : 0) +
      ((p->flags&MEM_Static)!=0 ? 1 : 0) >= 1
    );
  }
  */
  return 1;
}
#endif


/*
** If pMem is an object with a valid string representation, this routine
** ensures the internal encoding for the string representation is
** 'desiredEnc', one of SQLITE_UTF8, SQLITE_UTF16LE or SQLITE_UTF16BE.
**
** If pMem is not a string object, or the encoding of the string
** representation is already stored using the requested encoding, then this
** routine is a no-op.
**
** SQLITE_OK is returned if the conversion is successful (or not required).
** SQLITE_NOMEM may be returned if a malloc() fails during conversion
** between formats.
*/
int sqlite3VdbeChangeEncoding(Mem *pMem, int desiredEnc){
#ifndef SQLITE_OMIT_UTF16
  int rc;
#endif
  assert( (pMem->flags&MEM_RowSet)==0 );
  assert( desiredEnc==SQLITE_UTF8 || desiredEnc==SQLITE_UTF16LE
           || desiredEnc==SQLITE_UTF16BE );
  if( !(pMem->flags&MEM_Str) || pMem->enc==desiredEnc ){
    return SQLITE_OK;
  }
  assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );
#ifdef SQLITE_OMIT_UTF16
  return SQLITE_ERROR;
#else

  /* MemTranslate() may return SQLITE_OK or SQLITE_NOMEM. If NOMEM is returned,
  ** then the encoding of the value may not have changed.
  */
  rc = sqlite3VdbeMemTranslate(pMem, (u8)desiredEnc);
  assert(rc==SQLITE_OK    || rc==SQLITE_NOMEM);
  assert(rc==SQLITE_OK    || pMem->enc!=desiredEnc);
  assert(rc==SQLITE_NOMEM || pMem->enc==desiredEnc);
  return rc;
#endif
}

/*
** Make sure pMem->z points to a writable allocation of at least 
** min(n,32) bytes.
**
** If the bPreserve argument is true, then copy of the content of
** pMem->z into the new allocation.  pMem must be either a string or
** blob if bPreserve is true.  If bPreserve is false, any prior content
** in pMem->z is discarded.
*/
SQLITE_NOINLINE int sqlite3VdbeMemGrow(Mem *pMem, int n, int bPreserve){
  assert( sqlite3VdbeCheckMemInvariants(pMem) );
  assert( (pMem->flags&MEM_RowSet)==0 );
  testcase( pMem->db==0 );

  /* If the bPreserve flag is set to true, then the memory cell must already
  ** contain a valid string or blob value.  */
  assert( bPreserve==0 || pMem->flags&(MEM_Blob|MEM_Str) );
  testcase( bPreserve && pMem->z==0 );

  /* COMDB2: TODO: fix this assert
  assert( pMem->szMalloc==0
       || pMem->szMalloc==sqlite3DbMallocSize(pMem->db, pMem->zMalloc) ); */
  if( pMem->szMalloc<n ){
    if( n<32 ) n = 32;
    if( bPreserve && pMem->szMalloc>0 && pMem->z==pMem->zMalloc ){
      pMem->z = pMem->zMalloc = sqlite3DbReallocOrFree(pMem->db, pMem->z, n);
      bPreserve = 0;
    }else{
      if( pMem->szMalloc>0 ) sqlite3DbFree(pMem->db, pMem->zMalloc);
      pMem->zMalloc = sqlite3DbMallocRaw(pMem->db, n);
    }
    if( pMem->zMalloc==0 ){
      sqlite3VdbeMemSetNull(pMem);
      pMem->z = 0;
      pMem->szMalloc = 0;
      return SQLITE_NOMEM_BKPT;
    }else{
      pMem->szMalloc = sqlite3DbMallocSize(pMem->db, pMem->zMalloc);
    }
  }

  if( bPreserve && pMem->z && pMem->z!=pMem->zMalloc ){
    memcpy(pMem->zMalloc, pMem->z, pMem->n);

    /* COMDB2 MODIFICATION */
    if( pMem->flags&MEM_Xor ){
      xorbufcpy(pMem->zMalloc, pMem->zMalloc, pMem->n);
      pMem->flags &= ~MEM_Xor;
    }

  }
  if( (pMem->flags&MEM_Dyn)!=0 && pMem->xDel){ /* COMDB2 MODIFICATION */
    assert( pMem->xDel!=0 && pMem->xDel!=SQLITE_DYNAMIC );
    pMem->xDel((void *)(pMem->z));
  }

  pMem->z = pMem->zMalloc;
  pMem->flags &= ~(MEM_Dyn|MEM_Ephem|MEM_Static);
  return SQLITE_OK;
}

/*
** Change the pMem->zMalloc allocation to be at least szNew bytes.
** If pMem->zMalloc already meets or exceeds the requested size, this
** routine is a no-op.
**
** Any prior string or blob content in the pMem object may be discarded.
** The pMem->xDel destructor is called, if it exists.  Though MEM_Str
** and MEM_Blob values may be discarded, MEM_Int, MEM_Real, MEM_Datetime,
** MEM_Interval and MEM_Null values are preserved.
**
** Return SQLITE_OK on success or an error code (probably SQLITE_NOMEM)
** if unable to complete the resizing.
*/
int sqlite3VdbeMemClearAndResize(Mem *pMem, int szNew){
  assert( szNew>0 );
  assert( (pMem->flags & MEM_Dyn)==0 || pMem->szMalloc==0 );
  if( pMem->szMalloc<szNew ){
    return sqlite3VdbeMemGrow(pMem, szNew, 0);
  }
  assert( (pMem->flags & MEM_Dyn)==0 );
  pMem->z = pMem->zMalloc;
  pMem->flags &= (MEM_Null|MEM_Int|MEM_Real|MEM_Datetime|MEM_Interval);
  return SQLITE_OK;
}

/*
** Change pMem so that its MEM_Str or MEM_Blob value is stored in
** MEM.zMalloc, where it can be safely written.
**
** Return SQLITE_OK on success or SQLITE_NOMEM if malloc fails.
*/
int sqlite3VdbeMemMakeWriteable(Mem *pMem){
  assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );
  assert( (pMem->flags&MEM_RowSet)==0 );
  if( (pMem->flags & (MEM_Str|MEM_Blob))!=0 ){
    if( ExpandBlob(pMem) ) return SQLITE_NOMEM;
    if( pMem->szMalloc==0 || pMem->z!=pMem->zMalloc ){
      if( sqlite3VdbeMemGrow(pMem, pMem->n + 2, 1) ){
        return SQLITE_NOMEM_BKPT;
      }
      pMem->z[pMem->n] = 0;
      pMem->z[pMem->n+1] = 0;
      pMem->flags |= MEM_Term;
    }
  }
  pMem->flags &= ~MEM_Ephem;
#ifdef SQLITE_DEBUG
  pMem->pScopyFrom = 0;
#endif

  return SQLITE_OK;
}

/*
** If the given Mem* has a zero-filled tail, turn it into an ordinary
** blob stored in dynamically allocated space.
*/
#ifndef SQLITE_OMIT_INCRBLOB
int sqlite3VdbeMemExpandBlob(Mem *pMem){
  int nByte;
  assert( pMem->flags & MEM_Zero );
  assert( pMem->flags&MEM_Blob );
  assert( (pMem->flags&MEM_RowSet)==0 );
  assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );

  /* Set nByte to the number of bytes required to store the expanded blob. */
  nByte = pMem->n + pMem->u.nZero;
  if( nByte<=0 ){
    nByte = 1;
  }
  if( sqlite3VdbeMemGrow(pMem, nByte, 1) ){
    return SQLITE_NOMEM_BKPT;
  }

  memset(&pMem->z[pMem->n], 0, pMem->u.nZero);
  pMem->n += pMem->u.nZero;
  pMem->flags &= ~(MEM_Zero|MEM_Term);
  return SQLITE_OK;
}
#endif

/*
** It is already known that pMem contains an unterminated string.
** Add the zero terminator.
*/
static SQLITE_NOINLINE int vdbeMemAddTerminator(Mem *pMem){
  if( sqlite3VdbeMemGrow(pMem, pMem->n+2, 1) ){
    return SQLITE_NOMEM_BKPT;
  }
  pMem->z[pMem->n] = 0;
  pMem->z[pMem->n+1] = 0;
  pMem->flags |= MEM_Term;
  return SQLITE_OK;
}

/*
** Make sure the given Mem is \u0000 terminated.
*/
int sqlite3VdbeMemNulTerminate(Mem *pMem){
  assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );
  testcase( (pMem->flags & (MEM_Term|MEM_Str))==(MEM_Term|MEM_Str) );
  testcase( (pMem->flags & (MEM_Term|MEM_Str))==0 );
  if( (pMem->flags & (MEM_Term|MEM_Str))!=MEM_Str ){
    return SQLITE_OK;   /* Nothing to do */
  }else{
    return vdbeMemAddTerminator(pMem);
  }
}

/*
** Add MEM_Str to the set of representations for the given Mem.  Numbers
** are converted using sqlite3_snprintf().  Converting a BLOB to a string
** is a no-op.
**
** Existing representations MEM_Int and MEM_Real are invalidated if
** bForce is true but are retained if bForce is false.
**
** A MEM_Null value will never be passed to this function. This function is
** used for converting values to text for returning to the user (i.e. via
** sqlite3_value_text()), or for ensuring that values to be used as btree
** keys are strings. In the former case a NULL pointer is returned the
** user and the latter is an internal programming error.
*/
int sqlite3VdbeMemStringify(Mem *pMem, u8 enc, u8 bForce){
  int rc = SQLITE_OK;
  int fg = pMem->flags;
  const int nByte = 32;

  assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );
  assert( !(fg&MEM_Zero) );
  assert( !(fg&(MEM_Str|MEM_Blob)) );
  /* COMDB2 MODIFICATION */
  assert( fg&(MEM_Int|MEM_Real|MEM_Datetime|MEM_Interval) );
  assert( (pMem->flags&MEM_RowSet)==0 );
  assert( EIGHT_BYTE_ALIGNMENT(pMem) );


  if( sqlite3VdbeMemClearAndResize(pMem, nByte) ){
    pMem->enc = 0;
    return SQLITE_NOMEM_BKPT;
  }

  /* For a Real or Integer, use sqlite3_snprintf() to produce the UTF-8
  ** string representation of the value. Then, if the required encoding
  ** is UTF-16le or UTF-16be do a translation.
  ** 
  ** FIX ME: It would be better if sqlite3_snprintf() could do UTF-16.
  */
  /* COMDB2 MODIFICATION */
#ifdef SQLITE_BUILDING_FOR_COMDB2
  if( fg & MEM_Interval ){
    char tmp[64];
    int n = 0;
    if (pMem->du.tv.type == INTV_DECIMAL_TYPE) {
        rc = sqlite3DecimalToString(&pMem->du.tv.u.dec, tmp, sizeof(tmp));
        tmp[sizeof(tmp) - 1] = 0;
        n = strlen(tmp);
    } else {
        rc = intv_to_str(&pMem->du.tv, tmp, sizeof(tmp), &n);
    }
    if( rc!=0 ){
       logmsg(LOGMSG_ERROR, "interval_to_str failed type:%d need:%d\n", pMem->du.tv.type, n);
       return SQLITE_INTERNAL;
    }
    if( pMem->zMalloc ){
        sqlite3DbFree(pMem->db, pMem->zMalloc);
        pMem->zMalloc = NULL;
        pMem->szMalloc = 0;
        pMem->z = NULL;
    }
    pMem->n = n;
    char *z = sqlite3GlobalConfig.m.xMalloc(pMem->n+2);
    if( !z ) return SQLITE_NOMEM;
    memcpy(z, tmp, pMem->n);
    z[pMem->n] = 0;
    z[pMem->n+1] = 0;
    pMem->z = z;
    pMem->zMalloc = z;
    pMem->szMalloc = pMem->n + 2;
    pMem->enc = SQLITE_UTF8;
    pMem->flags |= MEM_Str | MEM_Term | MEM_Dyn;
    if( pMem->xDel ) pMem->xDel = 0;
    if( bForce ) pMem->flags &= ~MEM_Interval;
    sqlite3VdbeChangeEncoding(pMem, enc);

  }else if( fg & MEM_Datetime ){

    char    tmp[64];
    int     outdtsz;

    if( pMem->zMalloc ){
      sqlite3DbFree(pMem->db, pMem->zMalloc);
      pMem->zMalloc = NULL;
      pMem->szMalloc = 0;
      pMem->z = NULL;
    }

    if( convMem2ClientDatetimeStr(pMem, tmp, sizeof(tmp), &outdtsz) ){ 
      char *z;
      sqlite3ErrorWithMsg(pMem->db, SQLITE_CONV_ERROR,
            "can't convert datetime value to string");
      rc = SQLITE_CONV_ERROR;
      pMem->n = strlen("conv_error");
      z = sqlite3GlobalConfig.m.xMalloc(pMem->n+2);
      if( !z ) return SQLITE_NOMEM;
      memcpy(z, "conv_error", pMem->n);
      z[pMem->n] = 0;
      z[pMem->n+1] = 0;
      pMem->z = z;
      pMem->enc = SQLITE_UTF8;
      pMem->flags |= MEM_Str | MEM_Term | MEM_Dyn;
      if( pMem->xDel ) pMem->xDel = 0;
      if( bForce ) pMem->flags &= ~MEM_Datetime;
      sqlite3VdbeChangeEncoding(pMem, enc);
    }else{
      char *z;
      pMem->n = strlen(tmp);
      z = sqlite3GlobalConfig.m.xMalloc(pMem->n+2);
      if( !z ) return SQLITE_NOMEM;
      memcpy(z, tmp, pMem->n);
      z[pMem->n] = 0;
      z[pMem->n+1] = 0;
      pMem->z = z;
      pMem->zMalloc = z;
      pMem->szMalloc = pMem->n + 2;
      pMem->enc = SQLITE_UTF8;
      pMem->flags |= MEM_Str | MEM_Term | MEM_Dyn;
      if( pMem->xDel ) pMem->xDel = 0;
      if( bForce ) pMem->flags &= ~MEM_Datetime;
      sqlite3VdbeChangeEncoding(pMem, enc);
    }
  }
#else
  if (0);
#endif /* SQLITE_BUILDING_FOR_COMDB2 */
  else {
    if( fg & MEM_Int ){
      sqlite3_snprintf(nByte, pMem->z, "%lld", pMem->u.i);
    }else{
      assert( fg & MEM_Real );
      sqlite3_snprintf(nByte, pMem->z, "%!.15g", pMem->u.r);
    }
    pMem->n = sqlite3Strlen30(pMem->z);
    pMem->enc = SQLITE_UTF8;
    pMem->flags |= MEM_Str|MEM_Term;
    if( bForce ) pMem->flags &= ~(MEM_Int|MEM_Real);
    sqlite3VdbeChangeEncoding(pMem, enc);
    rc = SQLITE_OK;
  }
  /* COMDB2 MODIFICATION */
  return rc;
}

/*
** Memory cell pMem contains the context of an aggregate function.
** This routine calls the finalize method for that function.  The
** result of the aggregate is stored back into pMem.
**
** Return SQLITE_ERROR if the finalizer reports an error.  SQLITE_OK
** otherwise.
*/
int sqlite3VdbeMemFinalize(Mem *pMem, FuncDef *pFunc){
  int rc = SQLITE_OK;
  if( ALWAYS(pFunc && pFunc->xFinalize) ){
    sqlite3_context ctx;
    Mem t;
    assert( (pMem->flags & MEM_Null)!=0 || pFunc==pMem->u.pDef );
    assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );
    memset(&ctx, 0, sizeof(ctx));
    memset(&t, 0, sizeof(t));
    t.flags = MEM_Null;
    t.db = pMem->db;
    ctx.pOut = &t;
    ctx.pMem = pMem;
    ctx.pFunc = pFunc;
    pFunc->xFinalize(&ctx); /* IMP: R-24505-23230 */
    assert( (pMem->flags & MEM_Dyn)==0 );
    if( pMem->szMalloc>0 ) sqlite3DbFree(pMem->db, pMem->zMalloc);
    memcpy(pMem, &t, sizeof(t));
    rc = ctx.isError;
  }
  return rc;
}

/*
** If the memory cell contains a value that must be freed by
** invoking the external callback in Mem.xDel, then this routine
** will free that value.  It also sets Mem.flags to MEM_Null.
**
** This is a helper routine for sqlite3VdbeMemSetNull() and
** for sqlite3VdbeMemRelease().  Use those other routines as the
** entry point for releasing Mem resources.
*/
static SQLITE_NOINLINE void vdbeMemClearExternAndSetNull(Mem *p){
  assert( p->db==0 || sqlite3_mutex_held(p->db->mutex) );
  assert( VdbeMemDynamic(p) );
  if( p->flags&MEM_Agg ){
    sqlite3VdbeMemFinalize(p, p->u.pDef);
    assert( (p->flags & MEM_Agg)==0 );
    testcase( p->flags & MEM_Dyn );
  }
  if( p->flags&MEM_Dyn && p->xDel ){ /* COMDB2 MODIFICATION */
    assert( (p->flags&MEM_RowSet)==0 );
    assert( p->xDel!=SQLITE_DYNAMIC && p->xDel!=0 );
    p->xDel((void *)p->z);
  }else if( p->flags&MEM_RowSet ){
    sqlite3RowSetClear(p->u.pRowSet);
  }else if( p->flags&MEM_Frame ){
    VdbeFrame *pFrame = p->u.pFrame;
    pFrame->pParent = pFrame->v->pDelFrame;
    pFrame->v->pDelFrame = pFrame;
  }
  p->flags = MEM_Null;
}

/*
** Release memory held by the Mem p, both external memory cleared
** by p->xDel and memory in p->zMalloc.
**
** This is a helper routine invoked by sqlite3VdbeMemRelease() in
** the unusual case where there really is memory in p that needs
** to be freed.
*/
static SQLITE_NOINLINE void vdbeMemClear(Mem *p){
  if( VdbeMemDynamic(p) ){
    vdbeMemClearExternAndSetNull(p);
  }
  if( p->szMalloc ){
    sqlite3DbFree(p->db, p->zMalloc);
    p->szMalloc = 0;
  }
  p->z = 0;
}

/*
** Release any memory resources held by the Mem.  Both the memory that is
** free by Mem.xDel and the Mem.zMalloc allocation are freed.
**
** Use this routine prior to clean up prior to abandoning a Mem, or to
** reset a Mem back to its minimum memory utilization.
**
** Use sqlite3VdbeMemSetNull() to release just the Mem.xDel space
** prior to inserting new content into the Mem.
*/
void sqlite3VdbeMemRelease(Mem *p){
  assert( sqlite3VdbeCheckMemInvariants(p) );
  if( VdbeMemDynamic(p) || p->szMalloc ){
    vdbeMemClear(p);
  }
}

/*
** Convert a 64-bit IEEE double into a 64-bit signed integer.
** If the double is out of range of a 64-bit signed integer then
** return the closest available 64-bit signed integer.
*/
static i64 doubleToInt64(double r){
#ifdef SQLITE_OMIT_FLOATING_POINT
  /* When floating-point is omitted, double and int64 are the same thing */
  return r;
#else
  /*
  ** Many compilers we encounter do not define constants for the
  ** minimum and maximum 64-bit integers, or they define them
  ** inconsistently.  And many do not understand the "LL" notation.
  ** So we define our own static constants here using nothing
  ** larger than a 32-bit integer constant.
  */
  static const i64 maxInt = LARGEST_INT64;
  static const i64 minInt = SMALLEST_INT64;

  if( r<=(double)minInt ){
    return minInt;
  }else if( r>=(double)maxInt ){
    return maxInt;
  }else{
    return (i64)r;
  }
#endif
}

/*
** Return some kind of integer value which is the best we can do
** at representing the value that *pMem describes as an integer.
** If pMem is an integer, then the value is exact.  If pMem is
** a floating-point then the value returned is the integer part.
** If pMem is a string or blob, then we make an attempt to convert
** it into an integer and return that.  If pMem represents an
** an SQL-NULL value, return 0.
**
** If pMem represents a string value, its encoding might be changed.
*/
i64 sqlite3VdbeIntValue(Mem *pMem){
  int flags;
  assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );
  assert( EIGHT_BYTE_ALIGNMENT(pMem) );
  flags = pMem->flags;
  if( flags & MEM_Int ){
    return pMem->u.i;
  }else if( flags & MEM_Real ){
    return doubleToInt64(pMem->u.r);
  }
  /* COMDB2 MODIFICATION */
  else if( pMem->flags & MEM_Interval)
  {
    if(pMem->du.tv.type == INTV_YM_TYPE) 
    {
      return pMem->du.tv.sign*(i64)(pMem->du.tv.u.ym.years*12+
          pMem->du.tv.u.ym.months);
    }
    else if(pMem->du.tv.type == INTV_DS_TYPE || pMem->du.tv.type == INTV_DSUS_TYPE)
    {
      return pMem->du.tv.sign*(i64)(
          pMem->du.tv.u.ds.days*24*3600+
          pMem->du.tv.u.ds.hours*3600+
          pMem->du.tv.u.ds.mins*60+
          pMem->du.tv.u.ds.sec);
    }
    else 
    {
       logmsg(LOGMSG_ERROR, "%s:%d: Cannot convert a decimal to integer\n", __FILE__, __LINE__);
       return 0;
    }
  }
  else if( pMem->flags & MEM_Datetime)
  {
    return pMem->du.dt.dttz_sec;
  }else if( flags & (MEM_Str|MEM_Blob) ){
    i64 value = 0;
    assert( pMem->z || pMem->n==0 );
    sqlite3Atoi64(pMem->z, &value, pMem->n, pMem->enc);
    return value;
  }else{
    return 0;
  }
}

/*
** Return the best representation of pMem that we can get into a
** double.  If pMem is already a double or an integer, return its
** value.  If it is a string or blob, try to convert it to a double.
** If it is a NULL, return 0.0.
*/
double sqlite3VdbeRealValue(Mem *pMem){
  assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );
  assert( EIGHT_BYTE_ALIGNMENT(pMem) );
  if( pMem->flags & MEM_Real ){
    return pMem->u.r;
  }else if( pMem->flags & MEM_Int ){
    return (double)pMem->u.i;

   /* COMDB2 MODIFICATION */
  }
  else if( pMem->flags & MEM_Interval)
  {
    char tmp[128];
    switch( pMem->du.tv.type ){
    case INTV_YM_TYPE:
    case INTV_DS_TYPE:
    case INTV_DSUS_TYPE:
      return interval_to_double(&pMem->du.tv);
    default:
       decQuadToString(&pMem->du.tv.u.dec, tmp);
       return atof(tmp);
    }
  }else if( pMem->flags & MEM_Datetime ){
    return pMem->du.dt.dttz_sec + 
      pMem->du.dt.dttz_frac /
      (pMem->du.dt.dttz_prec == DTTZ_PREC_MSEC ? 1E3 : 1E6);
  }else if( pMem->flags & (MEM_Str|MEM_Blob) ){
    /* (double)0 In case of SQLITE_OMIT_FLOATING_POINT... */
    double val = (double)0;
    sqlite3AtoF(pMem->z, &val, pMem->n, pMem->enc);
    return val;
  }else{
    /* (double)0 In case of SQLITE_OMIT_FLOATING_POINT... */
    return (double)0;
  }
}

/*
** The MEM structure is already a MEM_Real.  Try to also make it a
** MEM_Int if we can.
*/
void sqlite3VdbeIntegerAffinity(Mem *pMem){
  i64 ix;
  assert( pMem->flags & MEM_Real );
  assert( (pMem->flags & MEM_RowSet)==0 );
  assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );
  assert( EIGHT_BYTE_ALIGNMENT(pMem) );

  ix = doubleToInt64(pMem->u.r);

  /* Only mark the value as an integer if
  **
  **    (1) the round-trip conversion real->int->real is a no-op, and
  **    (2) The integer is neither the largest nor the smallest
  **        possible integer (ticket #3922)
  **
  ** The second and third terms in the following conditional enforces
  ** the second condition under the assumption that addition overflow causes
  ** values to wrap around.
  */
  if( pMem->u.r==ix && ix>SMALLEST_INT64 && ix<LARGEST_INT64 ){
    pMem->u.i = ix;
    MemSetTypeFlag(pMem, MEM_Int);
  }
}

/*
** Convert pMem to type integer.  Invalidate any prior representations.
*/
int sqlite3VdbeMemIntegerify(Mem *pMem){
  assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );
  assert( (pMem->flags & MEM_RowSet)==0 );
  assert( EIGHT_BYTE_ALIGNMENT(pMem) );

  pMem->u.i = sqlite3VdbeIntValue(pMem);
  MemSetTypeFlag(pMem, MEM_Int);
  return SQLITE_OK;
}

/*
** Convert pMem so that it is of type MEM_Real.
** Invalidate any prior representations.
*/
int sqlite3VdbeMemRealify(Mem *pMem){
  assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );
  assert( EIGHT_BYTE_ALIGNMENT(pMem) );

  pMem->u.r = sqlite3VdbeRealValue(pMem);
  MemSetTypeFlag(pMem, MEM_Real);
  return SQLITE_OK;
}

/*
** Convert pMem so that it has types MEM_Real or MEM_Int or both.
** Invalidate any prior representations.
**
** Every effort is made to force the conversion, even if the input
** is a string that does not look completely like a number.  Convert
** as much of the string as we can and ignore the rest.
*/
int sqlite3VdbeMemNumerify(Mem *pMem){
  if( (pMem->flags & (MEM_Int|MEM_Real|MEM_Null))==0 ){
    assert( (pMem->flags & (MEM_Blob|MEM_Str|MEM_Interval|MEM_Datetime))!=0 );
    assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );
    /* TODO: COMDB2 MODIFICATION */
    /* string is NULL for cast(intervalds as string) */
    if( pMem->z && 0==sqlite3Atoi64(pMem->z, &pMem->u.i, pMem->n, pMem->enc) ){
      MemSetTypeFlag(pMem, MEM_Int);
    }else{
      pMem->u.r = sqlite3VdbeRealValue(pMem);
      MemSetTypeFlag(pMem, MEM_Real);
      sqlite3VdbeIntegerAffinity(pMem);
    }
  }
  assert( (pMem->flags & (MEM_Int|MEM_Real|MEM_Null))!=0 );
  pMem->flags &= ~(MEM_Str|MEM_Blob|MEM_Zero);
  return SQLITE_OK;
}

void sqlite3SetConversionError(void);


/*
** Cast the datatype of the value in pMem according to the affinity
** "aff".  Casting is different from applying affinity in that a cast
** is forced.  In other words, the value is converted into the desired
** affinity even if that results in loss of data.  This routine is
** used (for example) to implement the SQL "cast()" operator.
*/
int sqlite3VdbeMemCast(Vdbe *p, Mem *pMem, u8 aff, u8 encoding){
  int rc = SQLITE_OK, rc2;
  sqlite3 *db = p->db;

  if( pMem->flags & MEM_Null ) return rc;
  switch( aff ){
    case SQLITE_AFF_BLOB: {   /* Really a cast to BLOB */
      if( (pMem->flags & MEM_Blob)==0 ){
        sqlite3ValueApplyAffinity(pMem, SQLITE_AFF_TEXT, encoding);
        assert( pMem->flags & MEM_Str || pMem->db->mallocFailed );
        if( pMem->flags & MEM_Str ) MemSetTypeFlag(pMem, MEM_Blob);
      }else{
        pMem->flags &= ~(MEM_TypeMask&~MEM_Blob);
      }
      break;
    }
    case SQLITE_AFF_NUMERIC: {
      sqlite3VdbeMemNumerify(pMem);
      break;
    }
    case SQLITE_AFF_INTEGER: {
      sqlite3VdbeMemIntegerify(pMem);
      break;
    }
    case SQLITE_AFF_REAL:
    /* COMDB2 MODIFICATION */
    case SQLITE_AFF_SMALL: {
      sqlite3VdbeMemRealify(pMem);
      break;
    }
    case SQLITE_AFF_DATETIME: {
      rc2 = sqlite3VdbeMemDatetimefyTz(pMem, pMem->tz);
      if( rc2 && !debug_switch_ignore_datetime_cast_failures() ){
        sqlite3SetString(&p->zErrMsg, db, "cast to datetime failed");
        sqlite3SetConversionError();
        rc = rc2;
      }
      break;
    }
    case SQLITE_AFF_INTV_YE:
    case SQLITE_AFF_INTV_MO:
    case SQLITE_AFF_INTV_DY:
    case SQLITE_AFF_INTV_HO:
    case SQLITE_AFF_INTV_MI:
    case SQLITE_AFF_INTV_SE: {
      rc2 = sqlite3VdbeMemIntervalfy(pMem, aff);
      if( rc2 && !debug_switch_ignore_datetime_cast_failures() ){
        sqlite3SetString(&p->zErrMsg, db, "cast to interval failed");
        sqlite3SetConversionError();
        rc = rc2;
      }
      break;
    }
    case SQLITE_AFF_DECIMAL: {
      rc2 = sqlite3VdbeMemDecimalfy(pMem);
      if( rc2 && !debug_switch_ignore_datetime_cast_failures() ){
        sqlite3SetString(&p->zErrMsg, db, "cast to decimal failed");
        sqlite3SetConversionError();
        rc = rc2;
      }
      break;
    }
    default: {
      assert( aff==SQLITE_AFF_TEXT );
      assert( MEM_Str==(MEM_Blob>>3) );
      pMem->flags |= (pMem->flags&MEM_Blob)>>3;
      sqlite3ValueApplyAffinity(pMem, SQLITE_AFF_TEXT, encoding);
      assert( pMem->flags & MEM_Str || pMem->db->mallocFailed );
      //pMem->flags &= ~(MEM_Int|MEM_Real|MEM_Blob|MEM_Zero);
      if ( (pMem->flags & MEM_Str) & !pMem->db->mallocFailed )
        pMem->flags = MEM_Str | (~(MEM_AffMask|MEM_Zero));
      break;
    }
  }
  return rc;
}

/*
** Initialize bulk memory to be a consistent Mem object.
**
** The minimum amount of initialization feasible is performed.
*/
void sqlite3VdbeMemInit(Mem *pMem, sqlite3 *db, u16 flags){
  assert( (flags & ~MEM_TypeMask)==0 );
  pMem->flags = flags;
  pMem->db = db;
  pMem->szMalloc = 0;
}


/*
** Delete any previous value and set the value stored in *pMem to NULL.
**
** This routine calls the Mem.xDel destructor to dispose of values that
** require the destructor.  But it preserves the Mem.zMalloc memory allocation.
** To free all resources, use sqlite3VdbeMemRelease(), which both calls this
** routine to invoke the destructor and deallocates Mem.zMalloc.
**
** Use this routine to reset the Mem prior to insert a new value.
**
** Use sqlite3VdbeMemRelease() to complete erase the Mem prior to abandoning it.
*/
void sqlite3VdbeMemSetNull(Mem *pMem){
  if( VdbeMemDynamic(pMem) ){
    vdbeMemClearExternAndSetNull(pMem);
  }else{
    pMem->flags = MEM_Null;
  }
}
void sqlite3ValueSetNull(sqlite3_value *p){
  sqlite3VdbeMemSetNull((Mem*)p); 
}

/*
** Delete any previous value and set the value to be a BLOB of length
** n containing all zeros.
*/
void sqlite3VdbeMemSetZeroBlob(Mem *pMem, int n){
  sqlite3VdbeMemRelease(pMem);
  pMem->flags = MEM_Blob|MEM_Zero;
  pMem->n = 0;
  if( n<0 ) n = 0;
  pMem->u.nZero = n;
  pMem->enc = SQLITE_UTF8;
  pMem->z = 0;
}

/*
** The pMem is known to contain content that needs to be destroyed prior
** to a value change.  So invoke the destructor, then set the value to
** a 64-bit integer.
*/
static SQLITE_NOINLINE void vdbeReleaseAndSetInt64(Mem *pMem, i64 val){
  sqlite3VdbeMemSetNull(pMem);
  pMem->u.i = val;
  pMem->flags = MEM_Int;
}

/*
** Delete any previous value and set the value stored in *pMem to val,
** manifest type INTEGER.
*/
void sqlite3VdbeMemSetInt64(Mem *pMem, i64 val){
  if( VdbeMemDynamic(pMem) ){
    vdbeReleaseAndSetInt64(pMem, val);
  }else{
    pMem->u.i = val;
    pMem->flags = MEM_Int;
  }
}

#ifndef SQLITE_OMIT_FLOATING_POINT
/*
** Delete any previous value and set the value stored in *pMem to val,
** manifest type REAL.
*/
void sqlite3VdbeMemSetDouble(Mem *pMem, double val){
  sqlite3VdbeMemSetNull(pMem);
  if( !sqlite3IsNaN(val) ){
    pMem->u.r = val;
    pMem->flags = MEM_Real;
  }
}
#endif

/*
** Delete any previous value and set the value of pMem to be an
** empty boolean index.
*/
void sqlite3VdbeMemSetRowSet(Mem *pMem){
  sqlite3 *db = pMem->db;
  assert( db!=0 );
  assert( (pMem->flags & MEM_RowSet)==0 );
  sqlite3VdbeMemRelease(pMem);
  pMem->zMalloc = sqlite3DbMallocRawNN(db, 64);
  if( db->mallocFailed ){
    pMem->flags = MEM_Null;
    pMem->szMalloc = 0;
  }else{
    assert( pMem->zMalloc );
    pMem->szMalloc = sqlite3DbMallocSize(db, pMem->zMalloc);
    pMem->u.pRowSet = sqlite3RowSetInit(db, pMem->zMalloc, pMem->szMalloc);
    assert( pMem->u.pRowSet!=0 );
    pMem->flags = MEM_RowSet;
  }
}

/*
** Return true if the Mem object contains a TEXT or BLOB that is
** too large - whose size exceeds SQLITE_MAX_LENGTH.
*/
int sqlite3VdbeMemTooBig(Mem *p){
  assert( p->db!=0 );
  if( p->flags & (MEM_Str|MEM_Blob) ){
    int n = p->n;
    if( p->flags & MEM_Zero ){
      n += p->u.nZero;
    }
    return n>p->db->aLimit[SQLITE_LIMIT_LENGTH];
  }
  return 0; 
}

#ifdef SQLITE_DEBUG
/*
** This routine prepares a memory cell for modification by breaking
** its link to a shallow copy and by marking any current shallow
** copies of this cell as invalid.
**
** This is used for testing and debugging only - to make sure shallow
** copies are not misused.
*/
void sqlite3VdbeMemAboutToChange(Vdbe *pVdbe, Mem *pMem){
  int i;
  Mem *pX;
  for(i=0, pX=pVdbe->aMem; i<pVdbe->nMem; i++, pX++){
    if( pX->pScopyFrom==pMem ){
      pX->flags |= MEM_Undefined;
      pX->pScopyFrom = 0;
    }
  }
  pMem->pScopyFrom = 0;
}
#endif /* SQLITE_DEBUG */


/*
** Make an shallow copy of pFrom into pTo.  Prior contents of
** pTo are freed.  The pFrom->z field is not duplicated.  If
** pFrom->z is used, then pTo->z points to the same thing as pFrom->z
** and flags gets srcType (either MEM_Ephem or MEM_Static).
*/
static SQLITE_NOINLINE void vdbeClrCopy(Mem *pTo, const Mem *pFrom, int eType){
  vdbeMemClearExternAndSetNull(pTo);
  assert( !VdbeMemDynamic(pTo) );
  sqlite3VdbeMemShallowCopy(pTo, pFrom, eType);
}
void sqlite3VdbeMemShallowCopy(Mem *pTo, const Mem *pFrom, int srcType){
  assert( (pFrom->flags & MEM_RowSet)==0 );
  assert( pTo->db==pFrom->db );
  if( VdbeMemDynamic(pTo) ){ vdbeClrCopy(pTo,pFrom,srcType); return; }
  memcpy(pTo, pFrom, MEMCELLSIZE);
  if( (pFrom->flags&MEM_Static)==0 ){
    pTo->flags &= ~(MEM_Dyn|MEM_Static|MEM_Ephem);
    assert( srcType==MEM_Ephem || srcType==MEM_Static );
    pTo->flags |= srcType;
  }
}

/*
** Make a full copy of pFrom into pTo.  Prior contents of pTo are
** freed before the copy is made.
*/
int sqlite3VdbeMemCopy(Mem *pTo, const Mem *pFrom){
  int rc = SQLITE_OK;

  assert( (pFrom->flags & MEM_RowSet)==0 );
  if( VdbeMemDynamic(pTo) ) vdbeMemClearExternAndSetNull(pTo);
  memcpy(pTo, pFrom, MEMCELLSIZE);
  pTo->flags &= ~MEM_Dyn;
  if( pTo->flags&(MEM_Str|MEM_Blob) ){
    if( 0==(pFrom->flags&MEM_Static) ){
      pTo->flags |= MEM_Ephem;
      rc = sqlite3VdbeMemMakeWriteable(pTo);
    }
  }

  return rc;
}

/*
** Transfer the contents of pFrom to pTo. Any existing value in pTo is
** freed. If pFrom contains ephemeral data, a copy is made.
**
** pFrom contains an SQL NULL when this routine returns.
*/
void sqlite3VdbeMemMove(Mem *pTo, Mem *pFrom){
  assert( pFrom->db==0 || sqlite3_mutex_held(pFrom->db->mutex) );
  assert( pTo->db==0 || sqlite3_mutex_held(pTo->db->mutex) );
  assert( pFrom->db==0 || pTo->db==0 || pFrom->db==pTo->db );

  sqlite3VdbeMemRelease(pTo);
  memcpy(pTo, pFrom, sizeof(Mem));
  pFrom->flags = MEM_Null;
  pFrom->szMalloc = 0;
}

/*
** Change the value of a Mem to be a string or a BLOB.
**
** The memory management strategy depends on the value of the xDel
** parameter. If the value passed is SQLITE_TRANSIENT, then the 
** string is copied into a (possibly existing) buffer managed by the 
** Mem structure. Otherwise, any existing buffer is freed and the
** pointer copied.
**
** If the string is too large (if it exceeds the SQLITE_LIMIT_LENGTH
** size limit) then no memory allocation occurs.  If the string can be
** stored without allocating memory, then it is.  If a memory allocation
** is required to store the string, then value of pMem is unchanged.  In
** either case, SQLITE_TOOBIG is returned.
*/
int sqlite3VdbeMemSetStr(
  Mem *pMem,          /* Memory cell to set to string value */
  const char *z,      /* String pointer */
  int n,              /* Bytes in string, or negative */
  u8 enc,             /* Encoding of z.  0 for BLOBs */
  void (*xDel)(void*) /* Destructor function */
){
  int nByte = n;      /* New value for pMem->n */
  int iLimit;         /* Maximum allowed string or blob size */
  u16 flags = 0;      /* New value for pMem->flags */

  assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );
  assert( (pMem->flags & MEM_RowSet)==0 );

  /* If z is a NULL pointer, set pMem to contain an SQL NULL. */
  if( !z ){
    sqlite3VdbeMemSetNull(pMem);
    return SQLITE_OK;
  }

  if( pMem->db ){
    iLimit = pMem->db->aLimit[SQLITE_LIMIT_LENGTH];
  }else{
    iLimit = SQLITE_MAX_LENGTH;
  }
  flags = (enc==0?MEM_Blob:MEM_Str);
  if( nByte<0 ){
    assert( enc!=0 );
    if( enc==SQLITE_UTF8 ){
      nByte = sqlite3Strlen30(z);
      if( nByte>iLimit ) nByte = iLimit+1;
    }else{
      for(nByte=0; nByte<=iLimit && (z[nByte] | z[nByte+1]); nByte+=2){}
    }
    flags |= MEM_Term;
  }

  /* The following block sets the new values of Mem.z and Mem.xDel. It
  ** also sets a flag in local variable "flags" to indicate the memory
  ** management (one of MEM_Dyn or MEM_Static).
  */
  if( xDel==SQLITE_TRANSIENT ){
    int nAlloc = nByte;
    if( flags&MEM_Term ){
      nAlloc += (enc==SQLITE_UTF8?1:2);
    }
    if( nByte>iLimit ){
      return SQLITE_TOOBIG;
    }
    testcase( nAlloc==0 );
    testcase( nAlloc==31 );
    testcase( nAlloc==32 );
    if( sqlite3VdbeMemClearAndResize(pMem, MAX(nAlloc,32)) ){
      return SQLITE_NOMEM_BKPT;
    }
    memcpy(pMem->z, z, nAlloc);
  }else if( xDel==SQLITE_DYNAMIC ){
    sqlite3VdbeMemRelease(pMem);
    pMem->zMalloc = pMem->z = (char *)z;
    pMem->szMalloc = sqlite3DbMallocSize(pMem->db, pMem->zMalloc);
  }else{
    sqlite3VdbeMemRelease(pMem);
    pMem->z = (char *)z;
    pMem->xDel = xDel;
    flags |= ((xDel==SQLITE_STATIC)?MEM_Static:MEM_Dyn);
  }

  pMem->n = nByte;
  pMem->flags = flags;
  pMem->enc = (enc==0 ? SQLITE_UTF8 : enc);

#ifndef SQLITE_OMIT_UTF16
  if( pMem->enc!=SQLITE_UTF8 && sqlite3VdbeMemHandleBom(pMem) ){
    return SQLITE_NOMEM_BKPT;
  }
#endif

  if( nByte>iLimit ){
    return SQLITE_TOOBIG;
  }

  return SQLITE_OK;
}
 
/* COMDB2 MODIFICATION */
/*
** Change the value of a Mem to be a datetime.
*/
int sqlite3VdbeMemSetDatetime(
  Mem *pMem,          /* Memory cell to set to datetime value */
  dttz_t * dt,        /* datetime fields */
  const char * tz     /* tzname propagation, for conversion purposes */ 
){
  sqlite3VdbeMemSetNull(pMem);
  pMem->du.dt = *dt;
  pMem->tz = tz;
  pMem->flags = MEM_Datetime;
  return SQLITE_OK;
}

/*
** Change the value of a Mem to be an interval.
*/
int sqlite3VdbeMemSetInterval(
  Mem *pMem,          /* Memory cell to set to interval value */
  intv_t * tv         /* interval fields */
){
  sqlite3VdbeMemSetNull(pMem);
  pMem->du.tv = *tv;
  pMem->flags = MEM_Interval;
  return SQLITE_OK;
}

/*
** Delete any previous value and set the value stored in *pMem to val,
** manifest type Decimal.
*/
int sqlite3VdbeMemSetDecimal(Mem *pMem, decQuad *val){
  sqlite3VdbeMemSetNull(pMem);
  pMem->du.tv.u.dec = *val;
  pMem->du.tv.type = SQLITE_DECIMAL;
  pMem->flags = MEM_Interval;
  return SQLITE_OK;
}

/*
** Move data out of a btree key or data field and into a Mem structure.
** The data or key is taken from the entry that pCur is currently pointing
** to.  offset and amt determine what portion of the data or key to retrieve.
** key is true to get the key or false to get data.  The result is written
** into the pMem element.
**
** The pMem object must have been initialized.  This routine will use
** pMem->zMalloc to hold the content from the btree, if possible.  New
** pMem->zMalloc space will be allocated if necessary.  The calling routine
** is responsible for making sure that the pMem object is eventually
** destroyed.
**
** If this routine fails for any reason (malloc returns NULL or unable
** to read from the disk) then the pMem is left in an inconsistent state.
*/
static SQLITE_NOINLINE int vdbeMemFromBtreeResize(
  BtCursor *pCur,   /* Cursor pointing at record to retrieve. */
  u32 offset,       /* Offset from the start of data to return bytes from. */
  u32 amt,          /* Number of bytes to return. */
  int key,          /* If true, retrieve from the btree key, not data. */
  Mem *pMem         /* OUT: Return data in this Mem structure. */
){
  int rc;
  pMem->flags = MEM_Null;
  if( SQLITE_OK==(rc = sqlite3VdbeMemClearAndResize(pMem, amt+2)) ){
    if( key ){
      rc = sqlite3BtreeKey(pCur, offset, amt, pMem->z);
    }else{
      rc = sqlite3BtreeData(pCur, offset, amt, pMem->z);
    }
    if( rc==SQLITE_OK ){
      pMem->z[amt] = 0;
      pMem->z[amt+1] = 0;
      pMem->flags = MEM_Blob|MEM_Term;
      pMem->n = (int)amt;
    }else{
      sqlite3VdbeMemRelease(pMem);
    }
  }
  return rc;
}
int sqlite3VdbeMemFromBtree(
  BtCursor *pCur,   /* Cursor pointing at record to retrieve. */
  u32 offset,       /* Offset from the start of data to return bytes from. */
  u32 amt,          /* Number of bytes to return. */
  int key,          /* If true, retrieve from the btree key, not data. */
  Mem *pMem         /* OUT: Return data in this Mem structure. */
){
  char *zData;        /* Data from the btree layer */
  u32 available = 0;  /* Number of bytes available on the local btree page */
  int rc = SQLITE_OK; /* Return code */

  assert( sqlite3BtreeCursorIsValid(pCur) );
  assert( !VdbeMemDynamic(pMem) );

  /* Note: the calls to BtreeKeyFetch() and DataFetch() below assert() 
  ** that both the BtShared and database handle mutexes are held. */
  assert( (pMem->flags & MEM_RowSet)==0 );
  if( key ){
    zData = (char *)sqlite3BtreeKeyFetch(pCur, &available);
  }else{
    zData = (char *)sqlite3BtreeDataFetch(pCur, &available);
  }
  assert( zData!=0 );

  if( offset+amt<=available ){
    pMem->z = &zData[offset];
    pMem->flags = MEM_Blob|MEM_Ephem;
    pMem->n = (int)amt;
  }else{
    rc = vdbeMemFromBtreeResize(pCur, offset, amt, key, pMem);
  }

  return rc;
}

/*
** The pVal argument is known to be a value other than NULL.
** Convert it into a string with encoding enc and return a pointer
** to a zero-terminated version of that string.
*/
static SQLITE_NOINLINE const void *valueToText(sqlite3_value* pVal, u8 enc){
  assert( pVal!=0 );
  assert( pVal->db==0 || sqlite3_mutex_held(pVal->db->mutex) );
  assert( (enc&3)==(enc&~SQLITE_UTF16_ALIGNED) );
  assert( (pVal->flags & MEM_RowSet)==0 );
  assert( (pVal->flags & (MEM_Null))==0 );
  if( pVal->flags & (MEM_Blob|MEM_Str) ){
    pVal->flags |= MEM_Str;
    if( pVal->enc != (enc & ~SQLITE_UTF16_ALIGNED) ){
      sqlite3VdbeChangeEncoding(pVal, enc & ~SQLITE_UTF16_ALIGNED);
    }
    if( (enc & SQLITE_UTF16_ALIGNED)!=0 && 1==(1&SQLITE_PTR_TO_INT(pVal->z)) ){
      assert( (pVal->flags & (MEM_Ephem|MEM_Static))!=0 );
      if( sqlite3VdbeMemMakeWriteable(pVal)!=SQLITE_OK ){
        return 0;
      }
    }
    sqlite3VdbeMemNulTerminate(pVal); /* IMP: R-31275-44060 */
  }else{
    /* COMDB2 MODIFICATION */
    if (sqlite3VdbeMemStringify(pVal, enc, 0) != SQLITE_OK)
      return 0;
    assert( 0==(1&SQLITE_PTR_TO_INT(pVal->z)) );
  }
  assert(pVal->enc==(enc & ~SQLITE_UTF16_ALIGNED) || pVal->db==0
              || pVal->db->mallocFailed );
  if( pVal->enc==(enc & ~SQLITE_UTF16_ALIGNED) ){
    return pVal->z;
  }else{
    return 0;
  }
}

/* This function is only available internally, it is not part of the
** external API. It works in a similar way to sqlite3_value_text(),
** except the data returned is in the encoding specified by the second
** parameter, which must be one of SQLITE_UTF16BE, SQLITE_UTF16LE or
** SQLITE_UTF8.
**
** (2006-02-16:)  The enc value can be or-ed with SQLITE_UTF16_ALIGNED.
** If that is the case, then the result must be aligned on an even byte
** boundary.
*/
const void *sqlite3ValueText(sqlite3_value* pVal, u8 enc){
  if( !pVal ) return 0;
  assert( pVal->db==0 || sqlite3_mutex_held(pVal->db->mutex) );
  assert( (enc&3)==(enc&~SQLITE_UTF16_ALIGNED) );
  assert( (pVal->flags & MEM_RowSet)==0 );
  if( (pVal->flags&(MEM_Str|MEM_Term))==(MEM_Str|MEM_Term) && pVal->enc==enc ){
    return pVal->z;
  }
  if( pVal->flags&MEM_Null ){
    return 0;
  }
  return valueToText(pVal, enc);
}

/*
** Create a new sqlite3_value object.
*/
sqlite3_value *sqlite3ValueNew(sqlite3 *db){
  Mem *p = sqlite3DbMallocZero(db, sizeof(*p));
  if( p ){
    p->flags = MEM_Null;
    p->db = db;
  }
  return p;
}

/*
** Context object passed by sqlite3Stat4ProbeSetValue() through to 
** valueNew(). See comments above valueNew() for details.
*/
struct ValueNewStat4Ctx {
  Parse *pParse;
  Index *pIdx;
  UnpackedRecord **ppRec;
  int iVal;
};

/*
** Allocate and return a pointer to a new sqlite3_value object. If
** the second argument to this function is NULL, the object is allocated
** by calling sqlite3ValueNew().
**
** Otherwise, if the second argument is non-zero, then this function is 
** being called indirectly by sqlite3Stat4ProbeSetValue(). If it has not
** already been allocated, allocate the UnpackedRecord structure that 
** that function will return to its caller here. Then return a pointer to
** an sqlite3_value within the UnpackedRecord.a[] array.
*/
static sqlite3_value *valueNew(sqlite3 *db, struct ValueNewStat4Ctx *p){
#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
  if( p ){
    UnpackedRecord *pRec = p->ppRec[0];

    if( pRec==0 ){
      Index *pIdx = p->pIdx;      /* Index being probed */
      int nByte;                  /* Bytes of space to allocate */
      int i;                      /* Counter variable */
      int nCol = pIdx->nColumn;   /* Number of index columns including rowid */
  
      nByte = sizeof(Mem) * nCol + ROUND8(sizeof(UnpackedRecord));
      pRec = (UnpackedRecord*)sqlite3DbMallocZero(db, nByte);
      if( pRec ){
        pRec->pKeyInfo = sqlite3KeyInfoOfIndex(p->pParse, pIdx);
        if( pRec->pKeyInfo ){
          assert( pRec->pKeyInfo->nField+pRec->pKeyInfo->nXField==nCol );
          assert( pRec->pKeyInfo->enc==ENC(db) );
          pRec->aMem = (Mem *)((u8*)pRec + ROUND8(sizeof(UnpackedRecord)));
          for(i=0; i<nCol; i++){
            pRec->aMem[i].flags = MEM_Null;
            pRec->aMem[i].db = db;
          }
        }else{
          sqlite3DbFree(db, pRec);
          pRec = 0;
        }
      }
      if( pRec==0 ) return 0;
      p->ppRec[0] = pRec;
    }
  
    pRec->nField = p->iVal+1;
    return &pRec->aMem[p->iVal];
  }
#else
  UNUSED_PARAMETER(p);
#endif /* defined(SQLITE_ENABLE_STAT3_OR_STAT4) */
  return sqlite3ValueNew(db);
}

/*
** The expression object indicated by the second argument is guaranteed
** to be a scalar SQL function. If
**
**   * all function arguments are SQL literals,
**   * one of the SQLITE_FUNC_CONSTANT or _SLOCHNG function flags is set, and
**   * the SQLITE_FUNC_NEEDCOLL function flag is not set,
**
** then this routine attempts to invoke the SQL function. Assuming no
** error occurs, output parameter (*ppVal) is set to point to a value 
** object containing the result before returning SQLITE_OK.
**
** Affinity aff is applied to the result of the function before returning.
** If the result is a text value, the sqlite3_value object uses encoding 
** enc.
**
** If the conditions above are not met, this function returns SQLITE_OK
** and sets (*ppVal) to NULL. Or, if an error occurs, (*ppVal) is set to
** NULL and an SQLite error code returned.
*/
#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
static int valueFromFunction(
  sqlite3 *db,                    /* The database connection */
  Expr *p,                        /* The expression to evaluate */
  u8 enc,                         /* Encoding to use */
  u8 aff,                         /* Affinity to use */
  sqlite3_value **ppVal,          /* Write the new value here */
  struct ValueNewStat4Ctx *pCtx   /* Second argument for valueNew() */
){
  sqlite3_context ctx;            /* Context object for function invocation */
  sqlite3_value **apVal = 0;      /* Function arguments */
  int nVal = 0;                   /* Size of apVal[] array */
  FuncDef *pFunc = 0;             /* Function definition */
  sqlite3_value *pVal = 0;        /* New value */
  int rc = SQLITE_OK;             /* Return code */
  ExprList *pList = 0;            /* Function arguments */
  int i;                          /* Iterator variable */

  assert( pCtx!=0 );
  assert( (p->flags & EP_TokenOnly)==0 );
  pList = p->x.pList;
  if( pList ) nVal = pList->nExpr;
  pFunc = sqlite3FindFunction(db, p->u.zToken, nVal, enc, 0);
  assert( pFunc );
  if( (pFunc->funcFlags & (SQLITE_FUNC_CONSTANT|SQLITE_FUNC_SLOCHNG))==0 
   || (pFunc->funcFlags & SQLITE_FUNC_NEEDCOLL)
  ){
    return SQLITE_OK;
  }

  if( pList ){
    apVal = (sqlite3_value**)sqlite3DbMallocZero(db, sizeof(apVal[0]) * nVal);
    if( apVal==0 ){
      rc = SQLITE_NOMEM_BKPT;
      goto value_from_function_out;
    }
    for(i=0; i<nVal; i++){
      rc = sqlite3ValueFromExpr(db, pList->a[i].pExpr, enc, aff, &apVal[i]);
      if( apVal[i]==0 || rc!=SQLITE_OK ) goto value_from_function_out;
    }
  }

  pVal = valueNew(db, pCtx);
  if( pVal==0 ){
    rc = SQLITE_NOMEM_BKPT;
    goto value_from_function_out;
  }

  assert( pCtx->pParse->rc==SQLITE_OK );
  memset(&ctx, 0, sizeof(ctx));
  ctx.pOut = pVal;
  ctx.pFunc = pFunc;
  ctx.pVdbe = db->pVdbe;
  pFunc->xSFunc(&ctx, nVal, apVal);
  if( ctx.isError ){
    rc = ctx.isError;
    sqlite3ErrorMsg(pCtx->pParse, "%s", sqlite3_value_text(pVal));
  }else{
    sqlite3ValueApplyAffinity(pVal, aff, SQLITE_UTF8);
    assert( rc==SQLITE_OK );
    rc = sqlite3VdbeChangeEncoding(pVal, enc);
    if( rc==SQLITE_OK && sqlite3VdbeMemTooBig(pVal) ){
      rc = SQLITE_TOOBIG;
      pCtx->pParse->nErr++;
    }
  }
  pCtx->pParse->rc = rc;

 value_from_function_out:
  if( rc!=SQLITE_OK ){
    pVal = 0;
  }
  if( apVal ){
    for(i=0; i<nVal; i++){
      sqlite3ValueFree(apVal[i]);
    }
    sqlite3DbFree(db, apVal);
  }

  *ppVal = pVal;
  return rc;
}
#else
# define valueFromFunction(a,b,c,d,e,f) SQLITE_OK
#endif /* defined(SQLITE_ENABLE_STAT3_OR_STAT4) */

/*
** Extract a value from the supplied expression in the manner described
** above sqlite3ValueFromExpr(). Allocate the sqlite3_value object
** using valueNew().
**
** If pCtx is NULL and an error occurs after the sqlite3_value object
** has been allocated, it is freed before returning. Or, if pCtx is not
** NULL, it is assumed that the caller will free any allocated object
** in all cases.
*/
static int valueFromExpr(
  sqlite3 *db,                    /* The database connection */
  Expr *pExpr,                    /* The expression to evaluate */
  u8 enc,                         /* Encoding to use */
  u8 affinity,                    /* Affinity to use */
  sqlite3_value **ppVal,          /* Write the new value here */
  struct ValueNewStat4Ctx *pCtx   /* Second argument for valueNew() */
){
  int op;
  char *zVal = 0;
  sqlite3_value *pVal = 0;
  int negInt = 1;
  const char *zNeg = "";
  int rc = SQLITE_OK;
  Vdbe *p;

  /* COMDB2 MODIFICATION: first in list, we dont support more than one vdbe on a db */
  p = db->pVdbe;

  assert( pExpr!=0 );
  while( (op = pExpr->op)==TK_UPLUS || op==TK_SPAN ) pExpr = pExpr->pLeft;
  if( NEVER(op==TK_REGISTER) ) op = pExpr->op2;

  /* Compressed expressions only appear when parsing the DEFAULT clause
  ** on a table column definition, and hence only when pCtx==0.  This
  ** check ensures that an EP_TokenOnly expression is never passed down
  ** into valueFromFunction(). */
  assert( (pExpr->flags & EP_TokenOnly)==0 || pCtx==0 );

  if( op==TK_CAST ){
    u8 aff = sqlite3AffinityType(pExpr->u.zToken,0);
    rc = valueFromExpr(db, pExpr->pLeft, enc, aff, ppVal, pCtx);
    testcase( rc!=SQLITE_OK );
    if( *ppVal ){
      if (aff == SQLITE_AFF_DATETIME) {
        (*ppVal)->tz = p->tzname;
        (*ppVal)->dtprec = p->dtprec;
      }
      sqlite3VdbeMemCast(p, *ppVal, aff, SQLITE_UTF8);
      sqlite3ValueApplyAffinity(*ppVal, affinity, SQLITE_UTF8);
    }
    return rc;
  }

  /* Handle negative integers in a single step.  This is needed in the
  ** case when the value is -9223372036854775808.
  */
  if( op==TK_UMINUS
   && (pExpr->pLeft->op==TK_INTEGER || pExpr->pLeft->op==TK_FLOAT) ){
    pExpr = pExpr->pLeft;
    op = pExpr->op;
    negInt = -1;
    zNeg = "-";
  }

  if( op==TK_STRING || op==TK_FLOAT || op==TK_INTEGER ){
    pVal = valueNew(db, pCtx);
    if( pVal==0 ) goto no_mem;
    if( ExprHasProperty(pExpr, EP_IntValue) ){
      sqlite3VdbeMemSetInt64(pVal, (i64)pExpr->u.iValue*negInt);
    }else{
      zVal = sqlite3MPrintf(db, "%s%s", zNeg, pExpr->u.zToken);
      if( zVal==0 ) goto no_mem;
      sqlite3ValueSetStr(pVal, -1, zVal, SQLITE_UTF8, SQLITE_DYNAMIC);
    }
    if( (op==TK_INTEGER || op==TK_FLOAT ) && affinity==SQLITE_AFF_BLOB ){
      sqlite3ValueApplyAffinity(pVal, SQLITE_AFF_NUMERIC, SQLITE_UTF8);
    }else{
      sqlite3ValueApplyAffinity(pVal, affinity, SQLITE_UTF8);
    }
    if( pVal->flags & (MEM_Int|MEM_Real) ) pVal->flags &= ~MEM_Str;
    if( enc!=SQLITE_UTF8 ){
      rc = sqlite3VdbeChangeEncoding(pVal, enc);
    }
  }else if( op==TK_UMINUS  ){
    /* This branch happens for multiple negative signs.  Ex: -(-5) */
    if( SQLITE_OK==valueFromExpr(db,pExpr->pLeft,enc,affinity,&pVal,pCtx) 
     && pVal!=0
    ){
      sqlite3VdbeMemNumerify(pVal);
      if( pVal->flags & MEM_Real ){
        pVal->u.r = -pVal->u.r;
      }else if( pVal->u.i==SMALLEST_INT64 ){
        pVal->u.r = -(double)SMALLEST_INT64;
        MemSetTypeFlag(pVal, MEM_Real);
      }else{
        pVal->u.i = -pVal->u.i;
      }
      sqlite3ValueApplyAffinity(pVal, affinity, enc);
    }
  }else if( op==TK_NULL ){
    pVal = valueNew(db, pCtx);
    if( pVal==0 ) goto no_mem;
  }
#ifndef SQLITE_OMIT_BLOB_LITERAL
  else if( op==TK_BLOB ){
    int nVal;
    assert( pExpr->u.zToken[0]=='x' || pExpr->u.zToken[0]=='X' );
    assert( pExpr->u.zToken[1]=='\'' );
    pVal = valueNew(db, pCtx);
    if( !pVal ) goto no_mem;
    zVal = &pExpr->u.zToken[2];
    nVal = sqlite3Strlen30(zVal)-1;
    assert( zVal[nVal]=='\'' );
    sqlite3VdbeMemSetStr(pVal, sqlite3HexToBlob(db, zVal, nVal), nVal/2,
                         0, SQLITE_DYNAMIC);
  }
#endif

#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
  else if( op==TK_FUNCTION && pCtx!=0 ){
    rc = valueFromFunction(db, pExpr, enc, affinity, &pVal, pCtx);
  }
#endif

  *ppVal = pVal;
  return rc;

no_mem:
  sqlite3OomFault(db);
  sqlite3DbFree(db, zVal);
  assert( *ppVal==0 );
#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
  if( pCtx==0 ) sqlite3ValueFree(pVal);
#else
  assert( pCtx==0 ); sqlite3ValueFree(pVal);
#endif
  return SQLITE_NOMEM_BKPT;
}

/*
** Create a new sqlite3_value object, containing the value of pExpr.
**
** This only works for very simple expressions that consist of one constant
** token (i.e. "5", "5.1", "'a string'"). If the expression can
** be converted directly into a value, then the value is allocated and
** a pointer written to *ppVal. The caller is responsible for deallocating
** the value by passing it to sqlite3ValueFree() later on. If the expression
** cannot be converted to a value, then *ppVal is set to NULL.
*/
int sqlite3ValueFromExpr(
  sqlite3 *db,              /* The database connection */
  Expr *pExpr,              /* The expression to evaluate */
  u8 enc,                   /* Encoding to use */
  u8 affinity,              /* Affinity to use */
  sqlite3_value **ppVal     /* Write the new value here */
){
  return pExpr ? valueFromExpr(db, pExpr, enc, affinity, ppVal, 0) : 0;
}

#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
/*
** The implementation of the sqlite_record() function. This function accepts
** a single argument of any type. The return value is a formatted database 
** record (a blob) containing the argument value.
**
** This is used to convert the value stored in the 'sample' column of the
** sqlite_stat3 table to the record format SQLite uses internally.
*/
static void recordFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const int file_format = 1;
  u32 iSerial;                    /* Serial type */
  int nSerial;                    /* Bytes of space for iSerial as varint */
  u32 nVal;                       /* Bytes of space required for argv[0] */
  int nRet;
  sqlite3 *db;
  u8 *aRet;

  UNUSED_PARAMETER( argc );
  iSerial = sqlite3VdbeSerialType(argv[0], file_format, &nVal);
  nSerial = sqlite3VarintLen(iSerial);
  db = sqlite3_context_db_handle(context);

  nRet = 1 + nSerial + nVal;
  aRet = sqlite3DbMallocRawNN(db, nRet);
  if( aRet==0 ){
    sqlite3_result_error_nomem(context);
  }else{
    aRet[0] = nSerial+1;
    putVarint32(&aRet[1], iSerial);
    sqlite3VdbeSerialPut(&aRet[1+nSerial], argv[0], iSerial);
    sqlite3_result_blob(context, aRet, nRet, SQLITE_TRANSIENT);
    sqlite3DbFree(db, aRet);
  }
}

/*
** Register built-in functions used to help read ANALYZE data.
*/
void sqlite3AnalyzeFunctions(void){
  static FuncDef aAnalyzeTableFuncs[] = {
    FUNCTION(sqlite_record,   1, 0, 0, recordFunc),
  };
  sqlite3InsertBuiltinFuncs(aAnalyzeTableFuncs, ArraySize(aAnalyzeTableFuncs));
}


/*
** COMDB2 MODIFICATION
** There is no literal notation for datetime and interval. For these columns,
** try converting string literal to column's affinity for stat4 comparison.
*/
static int castExpr(
  sqlite3 *db,
  Expr *pExpr,
  u8 affinity,
  sqlite3_value **ppVal,
  struct ValueNewStat4Ctx *pAlloc
){
  dttz_t dt;
  intv_ds_t ds;
  int type, sign;
  uint64_t n0, n1;
  Vdbe *v = db->pVdbe;
  const char *z = pExpr->u.zToken;
  switch( affinity ){
    case SQLITE_AFF_DATETIME:
      if( str_to_dttz(z, strlen(z), v->tzname, &dt, v->dtprec)==0 ){
        sqlite3_value *pVal = valueNew(db, pAlloc);
        if ( pVal==NULL ){
          return SQLITE_NOMEM;
        }
        pVal->du.dt = dt;
        pVal->tz = v->tzname;
        pVal->dtprec = v->dtprec;
        pVal->flags = MEM_Datetime;
        *ppVal = pVal;
        return 0;
      }
      break;
    case SQLITE_AFF_INTV_MO:
      type = INTV_YM_TYPE;
      if(
        str_to_interval( z, strlen(z), &type, &n0, &n1, &ds, &sign )==0
        && type==0 // parsed into n0,n1
      ){
        sqlite3_value *pVal = valueNew(db, pAlloc);
        if ( pVal==NULL ){
          return SQLITE_NOMEM;
        }
        intv_t *tv = &pVal->du.tv;
        tv->u.ym.years = n0;
        tv->u.ym.months = n1;
        tv->sign = sign;
        tv->type = INTV_YM_TYPE;
        pVal->flags = MEM_Interval;
        *ppVal = pVal;
        return 0;
      }
      break;
    case SQLITE_AFF_INTV_SE:
      type = INTV_DSUS_TYPE;
      if(
        str_to_interval( z, strlen(z), &type, &n0, &n1, &ds, &sign )==0
        && type==1 // parsed into ds
      ){
        sqlite3_value *pVal = valueNew(db, pAlloc);
        if ( pVal==NULL ){
          return SQLITE_NOMEM;
        }
        intv_t *tv = &pVal->du.tv;
        tv->u.ds = ds;
        tv->sign = sign;
        tv->type = ds.prec == DTTZ_PREC_MSEC ? INTV_DS_TYPE : INTV_DSUS_TYPE;
        pVal->flags = MEM_Interval;
        *ppVal = pVal;
        return 0;
      }
      break;
  }
  return valueFromExpr(db, pExpr, ENC(db), affinity, ppVal, pAlloc);
}

/*
** Attempt to extract a value from pExpr and use it to construct *ppVal.
**
** If pAlloc is not NULL, then an UnpackedRecord object is created for
** pAlloc if one does not exist and the new value is added to the
** UnpackedRecord object.
**
** A value is extracted in the following cases:
**
**  * (pExpr==0). In this case the value is assumed to be an SQL NULL,
**
**  * The expression is a bound variable, and this is a reprepare, or
**
**  * The expression is a literal value.
**
** On success, *ppVal is made to point to the extracted value.  The caller
** is responsible for ensuring that the value is eventually freed.
*/
static int stat4ValueFromExpr(
  Parse *pParse,                  /* Parse context */
  Expr *pExpr,                    /* The expression to extract a value from */
  u8 affinity,                    /* Affinity to use */
  struct ValueNewStat4Ctx *pAlloc,/* How to allocate space.  Or NULL */
  sqlite3_value **ppVal           /* OUT: New value object (or NULL) */
){
  int rc = SQLITE_OK;
  sqlite3_value *pVal = 0;
  sqlite3 *db = pParse->db;

  /* Skip over any TK_COLLATE nodes */
  pExpr = sqlite3ExprSkipCollate(pExpr);

  if( !pExpr ){
    pVal = valueNew(db, pAlloc);
    if( pVal ){
      sqlite3VdbeMemSetNull((Mem*)pVal);
    }
  }else if( pExpr->op==TK_VARIABLE
        || NEVER(pExpr->op==TK_REGISTER && pExpr->op2==TK_VARIABLE)
  ){
    Vdbe *v;
    int iBindVar = pExpr->iColumn;
    sqlite3VdbeSetVarmask(pParse->pVdbe, iBindVar);
    if( (v = pParse->pReprepare)!=0 ){
      pVal = valueNew(db, pAlloc);
      if( pVal ){
        rc = sqlite3VdbeMemCopy((Mem*)pVal, &v->aVar[iBindVar-1]);
        if( rc==SQLITE_OK ){
          sqlite3ValueApplyAffinity(pVal, affinity, ENC(db));
        }
        pVal->db = pParse->db;
      }
    }
  }else{
#   if SQLITE_BUILDING_FOR_COMDB2
    if( pExpr->op==TK_STRING && affinity!=SQLITE_AFF_TEXT )
      rc = castExpr(db, pExpr, affinity, &pVal, pAlloc);
    else
#   endif
    rc = valueFromExpr(db, pExpr, ENC(db), affinity, &pVal, pAlloc);
  }

  assert( pVal==0 || pVal->db==db );
  *ppVal = pVal;
  return rc;
}

/*
** This function is used to allocate and populate UnpackedRecord 
** structures intended to be compared against sample index keys stored 
** in the sqlite_stat4 table.
**
** A single call to this function populates zero or more fields of the
** record starting with field iVal (fields are numbered from left to
** right starting with 0). A single field is populated if:
**
**  * (pExpr==0). In this case the value is assumed to be an SQL NULL,
**
**  * The expression is a bound variable, and this is a reprepare, or
**
**  * The sqlite3ValueFromExpr() function is able to extract a value 
**    from the expression (i.e. the expression is a literal value).
**
** Or, if pExpr is a TK_VECTOR, one field is populated for each of the
** vector components that match either of the two latter criteria listed
** above.
**
** Before any value is appended to the record, the affinity of the 
** corresponding column within index pIdx is applied to it. Before
** this function returns, output parameter *pnExtract is set to the
** number of values appended to the record.
**
** When this function is called, *ppRec must either point to an object
** allocated by an earlier call to this function, or must be NULL. If it
** is NULL and a value can be successfully extracted, a new UnpackedRecord
** is allocated (and *ppRec set to point to it) before returning.
**
** Unless an error is encountered, SQLITE_OK is returned. It is not an
** error if a value cannot be extracted from pExpr. If an error does
** occur, an SQLite error code is returned.
*/
int sqlite3Stat4ProbeSetValue(
  Parse *pParse,                  /* Parse context */
  Index *pIdx,                    /* Index being probed */
  UnpackedRecord **ppRec,         /* IN/OUT: Probe record */
  Expr *pExpr,                    /* The expression to extract a value from */
  int nElem,                      /* Maximum number of values to append */
  int iVal,                       /* Array element to populate */
  int *pnExtract                  /* OUT: Values appended to the record */
){
  int rc = SQLITE_OK;
  int nExtract = 0;

  if( pExpr==0 || pExpr->op!=TK_SELECT ){
    int i;
    struct ValueNewStat4Ctx alloc;

    alloc.pParse = pParse;
    alloc.pIdx = pIdx;
    alloc.ppRec = ppRec;

    for(i=0; i<nElem; i++){
      sqlite3_value *pVal = 0;
      Expr *pElem = (pExpr ? sqlite3VectorFieldSubexpr(pExpr, i) : 0);
      u8 aff = sqlite3IndexColumnAffinity(pParse->db, pIdx, iVal+i);
      alloc.iVal = iVal+i;
      rc = stat4ValueFromExpr(pParse, pElem, aff, &alloc, &pVal);
      if( !pVal ) break;
      nExtract++;
    }
  }

  *pnExtract = nExtract;
  return rc;
}

/*
** Attempt to extract a value from expression pExpr using the methods
** as described for sqlite3Stat4ProbeSetValue() above. 
**
** If successful, set *ppVal to point to a new value object and return 
** SQLITE_OK. If no value can be extracted, but no other error occurs
** (e.g. OOM), return SQLITE_OK and set *ppVal to NULL. Or, if an error
** does occur, return an SQLite error code. The final value of *ppVal
** is undefined in this case.
*/
int sqlite3Stat4ValueFromExpr(
  Parse *pParse,                  /* Parse context */
  Expr *pExpr,                    /* The expression to extract a value from */
  u8 affinity,                    /* Affinity to use */
  sqlite3_value **ppVal           /* OUT: New value object (or NULL) */
){
  return stat4ValueFromExpr(pParse, pExpr, affinity, 0, ppVal);
}

#include <serialget.c>

/*
** Extract the iCol-th column from the nRec-byte record in pRec.  Write
** the column value into *ppVal.  If *ppVal is initially NULL then a new
** sqlite3_value object is allocated.
**
** If *ppVal is initially NULL then the caller is responsible for 
** ensuring that the value written into *ppVal is eventually freed.
*/
int sqlite3Stat4Column(
  sqlite3 *db,                    /* Database handle */
  const void *pRec,               /* Pointer to buffer containing record */
  int nRec,                       /* Size of buffer pRec in bytes */
  int iCol,                       /* Column to extract */
  sqlite3_value **ppVal           /* OUT: Extracted value */
){
  u32 t;                          /* a column type code */
  int nHdr;                       /* Size of the header in the record */
  int iHdr;                       /* Next unread header byte */
  int iField;                     /* Next unread data byte */
  int szField;                    /* Size of the current data field */
  int i;                          /* Column index */
  u8 *a = (u8*)pRec;              /* Typecast byte array */
  Mem *pMem = *ppVal;             /* Write result into this Mem object */

  assert( iCol>0 );
  iHdr = getVarint32(a, nHdr);
  if( nHdr>nRec || iHdr>=nHdr ) return SQLITE_CORRUPT_BKPT;
  iField = nHdr;
  for(i=0; i<=iCol; i++){
    iHdr += getVarint32(&a[iHdr], t);
    testcase( iHdr==nHdr );
    testcase( iHdr==nHdr+1 );
    if( iHdr>nHdr ) return SQLITE_CORRUPT_BKPT;
    szField = sqlite3VdbeSerialTypeLen(t);
    iField += szField;
  }
  testcase( iField==nRec );
  testcase( iField==nRec+1 );
  if( iField>nRec ) return SQLITE_CORRUPT_BKPT;
  if( pMem==0 ){
    pMem = *ppVal = sqlite3ValueNew(db);
    if( pMem==0 ) return SQLITE_NOMEM_BKPT;
  }
  sqlite3VdbeSerialGet(&a[iField-szField], t, pMem);
  /* COMDB2 MODIFICATION */
  if(11 == t) /* see sqlite3VdbeSerialGet, 11 is datetime blob */
    pMem->tz = db->pVdbe->tzname;
  pMem->enc = ENC(db);
  return SQLITE_OK;
}

/*
** Unless it is NULL, the argument must be an UnpackedRecord object returned
** by an earlier call to sqlite3Stat4ProbeSetValue(). This call deletes
** the object.
*/
void sqlite3Stat4ProbeFree(UnpackedRecord *pRec){
  if( pRec ){
    int i;
    int nCol = pRec->pKeyInfo->nField+pRec->pKeyInfo->nXField;
    Mem *aMem = pRec->aMem;
    sqlite3 *db = aMem[0].db;
    for(i=0; i<nCol; i++){
      sqlite3VdbeMemRelease(&aMem[i]);
    }
    sqlite3KeyInfoUnref(pRec->pKeyInfo);
    sqlite3DbFree(db, pRec);
  }
}
#endif /* ifdef SQLITE_ENABLE_STAT4 */

/*
** Change the string value of an sqlite3_value object
*/
void sqlite3ValueSetStr(
  sqlite3_value *v,     /* Value to be set */
  int n,                /* Length of string z */
  const void *z,        /* Text of the new string */
  u8 enc,               /* Encoding to use */
  void (*xDel)(void*)   /* Destructor for the string */
){
  if( v ) sqlite3VdbeMemSetStr((Mem *)v, z, n, enc, xDel);
}

/*
** Free an sqlite3_value object
*/
void sqlite3ValueFree(sqlite3_value *v){
  if( !v ) return;
  sqlite3VdbeMemRelease((Mem *)v);
  sqlite3DbFree(((Mem*)v)->db, v);
}

/*
** The sqlite3ValueBytes() routine returns the number of bytes in the
** sqlite3_value object assuming that it uses the encoding "enc".
** The valueBytes() routine is a helper function.
*/
static SQLITE_NOINLINE int valueBytes(sqlite3_value *pVal, u8 enc){
  return valueToText(pVal, enc)!=0 ? pVal->n : 0;
}
int sqlite3ValueBytes(sqlite3_value *pVal, u8 enc){
  Mem *p = (Mem*)pVal;
  assert( (p->flags & MEM_Null)==0 || (p->flags & (MEM_Str|MEM_Blob))==0 );
  if( (p->flags & MEM_Str)!=0 && pVal->enc==enc ){
    return p->n;
  }
  if( (p->flags & MEM_Blob)!=0 ){
    if( p->flags & MEM_Zero ){
      return p->n + p->u.nZero;
    }else{
      return p->n;
    }
  }
  if( p->flags & MEM_Null ) return 0;
  return valueBytes(pVal, enc);
}

#ifdef SQLITE_BUILDING_FOR_COMDB2

/* COMDB2 MODIFICATION */

/*
** Add MEM_Datetime to the set of representations for the given Mem. 
*/
int sqlite3VdbeMemDatetimefyTz(Mem *pMem, const char *tz){
  int fg = pMem->flags;
  if( fg & MEM_Null )
    return SQLITE_OK;
  if( fg & MEM_Interval )
    return SQLITE_ERROR;
  if( pMem->flags & MEM_Blob )
    return SQLITE_ERROR; 
  if(pMem->flags & MEM_Real) {
    if(real_to_dttz(pMem->u.r, &pMem->du.dt, pMem->dtprec) != 0)
      return SQLITE_ERROR;
    pMem->flags = MEM_Datetime;
  }else if( pMem->flags & MEM_Int ){
    if (int_to_dttz(pMem->u.i, &pMem->du.dt, pMem->dtprec) != 0)
      return SQLITE_ERROR;
    pMem->flags = MEM_Datetime;
  }else if( pMem->flags & MEM_Str ){
    if (str_to_dttz(pMem->z, pMem->n, pMem->tz ? pMem->tz : tz,
     &pMem->du.dt, pMem->dtprec) != 0) {
      return SQLITE_ERROR;
    }
    pMem->flags = MEM_Datetime;
    /* all is good, get rid of the string, which is user-provided and partial
       many times */
    if( pMem->flags & MEM_Dyn ){
        sqlite3DbFree(pMem->db, pMem->z);
    }
    pMem->n = 0;
    pMem->z = 0;
    /*no MEM_Blob here*/
    pMem->flags &= ~(MEM_Str|MEM_Static|MEM_Dyn|MEM_Ephem);
  }
  return SQLITE_OK;
}

/*
** Add MEM_Datetime to the set of representations for the given Mem. 
*/
int sqlite3VdbeMemDatetimefy(Mem *pMem){
   return sqlite3VdbeMemDatetimefyTz(pMem, NULL);
}

/*
** Add MEM_Datetime to the set of representations for the given Mem. 
*/
int sqlite3VdbeMemDecimalfy(Mem *pMem)
{
   decContext     dfp_ctx;
   int            rc = SQLITE_OK;
   int            fg = pMem->flags;

   if(fg & MEM_Null) return rc;

   /* cannot convert a datetime to a decimal */
   if(fg & MEM_Datetime) return SQLITE_ERROR; 

   /* cannot convert a blob to a decimal */
   if(fg & MEM_Blob) return SQLITE_ERROR; 

   /* cannot convert a interval to a decimal */
   if(fg & MEM_Interval) return SQLITE_ERROR;

   /* TODO: we need a custom routine */
   if(fg & MEM_Real) return SQLITE_ERROR;

   if(fg & MEM_Int)
   {
      char str[32]; 
      void *ret = NULL;

      /* we can do better here, but we'll optimize later if need to */
      snprintf(str, sizeof(str), "%lld", pMem->u.i);

      dec_ctx_init(&dfp_ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);

      ret = decQuadFromString( &pMem->du.tv.u.dec, str, &dfp_ctx);

      if (ret)
      {
         if (dfp_conv_check_status( &dfp_ctx, "string", "quad"))
         {
            rc = SQLITE_ERROR;
         }
         else
         {
            pMem->du.tv.type = INTV_DECIMAL_TYPE;
            /* we ignore sign, it's embedded */
            pMem->du.tv.sign = 0;
            rc = SQLITE_OK;
         }
      }
      else
         rc = SQLITE_ERROR;
      if(0){
         char dbg[1024];

         decQuadToString( &pMem->du.tv.u.dec, dbg );
         printf("Parsed to quad %s\n", dbg);
      }
   }

   else if(fg & MEM_Str)
   {
      void *ret = NULL;
      
      dec_ctx_init(&dfp_ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);


      /* COMDB2 MODIFICATION. */
      sqlite3VdbeMemNulTerminate(pMem);

      ret = decQuadFromString( &pMem->du.tv.u.dec, pMem->z, &dfp_ctx);
      
      if (ret)
      {
         if (dfp_conv_check_status( &dfp_ctx, "string", "quad"))
         {
            rc = SQLITE_ERROR;
         }
         else
         {
            pMem->du.tv.type = INTV_DECIMAL_TYPE;
            /* we ignore sign, it's embedded */
            pMem->du.tv.sign = 0;
            rc = SQLITE_OK;
         }
      }
      else
         rc = SQLITE_ERROR;
      if(0){
         char dbg[1024];

         decQuadToString( &pMem->du.tv.u.dec, dbg );
         printf("Parsed to quad %s\n", dbg);
      }
   }

   sqlite3VdbeMemRelease(pMem);
   pMem->flags = MEM_Interval;

   return rc;
}


static char * skipsp(char *str){
    while (str && str[0] == ' ') str++;
    return str;
}

#include <alloca.h>

/*
** Add MEM_Interval to the set of representations for the given Mem. 
*/
int sqlite3VdbeMemIntervalfy(Mem *pMem, int type){
  uint64_t tmp, tmp2;
  int fg = pMem->flags;

  if( (fg & MEM_Interval) || (fg & MEM_Null) ) return SQLITE_OK;

  if( type == SQLITE_AFF_DECIMAL ){
    logmsg(LOGMSG_FATAL, "BUG, wrong function called, use Decimalfy %s:%d\n",
          __FILE__, __LINE__);
    abort();
  }

  bzero(&pMem->du, sizeof(pMem->du));
  if( pMem->flags & MEM_Int ){
    int_to_interval(pMem->u.i, &tmp, &tmp2, &pMem->du.tv.sign);
    goto num;
  }else if( pMem->flags & MEM_Real ){
    double_to_interval(pMem->u.r, &tmp, &tmp2, &pMem->du.tv.sign);
    goto num;
  }else if( pMem->flags & MEM_Str ){
    int intv;
    switch( type ){
      case SQLITE_AFF_INTV_DY:
      case SQLITE_AFF_INTV_HO:
      case SQLITE_AFF_INTV_MI:
      case SQLITE_AFF_INTV_SE: {
        intv = INTV_DSUS_TYPE;
        break;
      }
      case SQLITE_AFF_INTV_YE:
      case SQLITE_AFF_INTV_MO: {
        intv = INTV_YM_TYPE;
        break;
      }       
      default: {
          return SQLITE_ERROR;
      }
    }
    if( str_to_interval(pMem->z, pMem->n, &intv, &tmp, &tmp2,
     &pMem->du.tv.u.ds, &pMem->du.tv.sign) != 0 ){
      return SQLITE_ERROR;
    }
    if( intv == 0 ){
      switch( type ){
        case SQLITE_AFF_INTV_YE:
        case SQLITE_AFF_INTV_MO: {
          pMem->du.tv.type = INTV_YM_TYPE;
          pMem->du.tv.u.ym.years = tmp;
          pMem->du.tv.u.ym.months = tmp2;
          break;
        }
        default: {
          return SQLITE_ERROR;
        }
      }
    }else if( intv == 1) {
      switch( type ){
        case SQLITE_AFF_INTV_DY:
        case SQLITE_AFF_INTV_HO:
        case SQLITE_AFF_INTV_MI: {
          pMem->du.tv.type = INTV_DSUS_TYPE;
        }
        case SQLITE_AFF_INTV_SE: {
          pMem->du.tv.type = (pMem->du.tv.u.ds.prec == DTTZ_PREC_MSEC) ?
            INTV_DS_TYPE : INTV_DSUS_TYPE;
          break;
        }
        default: {
          return SQLITE_ERROR;
        }
      }
    } else if( intv == 2 ){
num:  switch( type ){
        case SQLITE_AFF_INTV_YE: {
          pMem->du.tv.type = INTV_YM_TYPE;
          pMem->du.tv.u.ym.years = tmp;
          break;
        }
        case SQLITE_AFF_INTV_MO: {
          pMem->du.tv.type = INTV_YM_TYPE;
          pMem->du.tv.u.ym.months = tmp;
          break;
        }
        case SQLITE_AFF_INTV_DY: {
          pMem->du.tv.type = INTV_DS_TYPE;
          _setIntervalDS( &pMem->du.tv.u.ds, 24*3600*tmp, 0);
          break;
        }
        case SQLITE_AFF_INTV_HO: {
          pMem->du.tv.type = INTV_DS_TYPE;
          _setIntervalDS( &pMem->du.tv.u.ds, 3600*tmp, 0);
          break;
        }
        case SQLITE_AFF_INTV_MI: {
          pMem->du.tv.type = INTV_DS_TYPE;
          _setIntervalDS( &pMem->du.tv.u.ds, 60*tmp, 0);
          break;
        }
        case SQLITE_AFF_INTV_SE: {
          if ((int)(tmp2 / 1E3) * 1000 == tmp2) {
            pMem->du.tv.type = INTV_DS_TYPE;
            _setIntervalDS(&pMem->du.tv.u.ds, tmp, tmp2 / 1000);
          } else {
            pMem->du.tv.type = INTV_DSUS_TYPE;
            _setIntervalDSUS(&pMem->du.tv.u.ds, tmp, tmp2);
          }
          break;
        }
        default: {
          return SQLITE_ERROR;
        }
      }
    }else{
      return SQLITE_ERROR;
    }
  }else{
    return SQLITE_ERROR;
  }
  pMem->flags &= ~(MEM_TypeMask);
  pMem->flags |= MEM_Interval;
  if (pMem->du.tv.type == INTV_YM_TYPE)
    _normalizeIntervalYM(&pMem->du.tv.u.ym);
  return SQLITE_OK;
}

/*
**  Operate on two intervals, res = a opcode b;
*/
int sqlite3VdbeMemIntervalAndInterval(
  const Mem *a,
  const Mem *b,
  int opcode,
  Mem * res
){
  bzero(res, sizeof(Mem));
  res->flags |= MEM_Interval;
  long long aa = 0, bb = 0;

  switch( a->du.tv.type ){
    case INTV_YM_TYPE: {
      aa = a->du.tv.sign*(int)(a->du.tv.u.ym.months + a->du.tv.u.ym.years*12);
      bb = b->du.tv.sign*(int)(b->du.tv.u.ym.months + b->du.tv.u.ym.years*12);

      if( opcode == OP_Add ){
        aa += bb;
      }else if(opcode == OP_Subtract ){
        aa -= bb;
      }else return SQLITE_ERROR;

      res->du.tv.type          = INTV_YM_TYPE;
      res->du.tv.sign          = (aa<0)?-1:1;
      res->du.tv.u.ym.months   = aa*res->du.tv.sign;
      res->du.tv.u.ym.years    = 0;

      _normalizeIntervalYM(&res->du.tv.u.ym);
      break;
    }
    case INTV_DS_TYPE: {
      if( b->du.tv.type == INTV_DS_TYPE ){
        aa = a->du.tv.sign*(long long)(a->du.tv.u.ds.frac +
                                       1000LL*           a->du.tv.u.ds.sec +
                                       1000LL*60*        a->du.tv.u.ds.mins +
                                       1000LL*3600*      a->du.tv.u.ds.hours +
                                       1000LL*3600*24*   a->du.tv.u.ds.days);
        bb = b->du.tv.sign*(long long)(b->du.tv.u.ds.frac +
                                       1000LL*           b->du.tv.u.ds.sec +
                                       1000LL*60*        b->du.tv.u.ds.mins +
                                       1000LL*3600*      b->du.tv.u.ds.hours +
                                       1000LL*3600*24*   b->du.tv.u.ds.days);

        if( opcode == OP_Add ){
          aa += bb;
        }else if( opcode == OP_Subtract ){
          aa -= bb;
        }else return SQLITE_ERROR;

        res->du.tv.type          = INTV_DS_TYPE;
        res->du.tv.sign          = (aa<0)?-1:1;
        aa = aa*res->du.tv.sign;
        _setIntervalDS(&res->du.tv.u.ds,
                       (((long long)aa)*res->du.tv.sign)/1000,
                       (int)((aa*res->du.tv.sign)%1000));
      }else{ // promote operand a to microsecond resolution
        aa = a->du.tv.sign*(long long)(
                                       1000LL*            a->du.tv.u.ds.frac +
                                       1000000LL*         a->du.tv.u.ds.sec +
                                       1000000LL*60*      a->du.tv.u.ds.mins +
                                       1000000LL*3600*    a->du.tv.u.ds.hours +
                                       1000000LL*3600*24* a->du.tv.u.ds.days);
        bb = b->du.tv.sign*(long long)(
                                       b->du.tv.u.ds.frac +
                                       1000000LL*         b->du.tv.u.ds.sec +
                                       1000000LL*60*      b->du.tv.u.ds.mins +
                                       1000000LL*3600*    b->du.tv.u.ds.hours +
                                       1000000LL*3600*24* b->du.tv.u.ds.days);

        if( opcode == OP_Add ){
          aa += bb;
        }else if( opcode == OP_Subtract ){
          aa -= bb;
        }else return SQLITE_ERROR;

        // promote the result of the expression to microsecond resolution
        res->du.tv.type          = INTV_DSUS_TYPE;
        res->du.tv.sign          = (aa<0)?-1:1;
        aa = aa*res->du.tv.sign;
        _setIntervalDSUS(&res->du.tv.u.ds,
                         (((long long)aa)*res->du.tv.sign)/1000000,
                         (int)((aa*res->du.tv.sign)%1000000));
      }

      break;
    }
    case INTV_DSUS_TYPE: {
      if( b->du.tv.type == INTV_DS_TYPE ){
        // promote operand b to microsecond resolution
        aa = a->du.tv.sign*(long long)(
                                       a->du.tv.u.ds.frac +
                                       1000000LL*         a->du.tv.u.ds.sec +
                                       1000000LL*60*      a->du.tv.u.ds.mins +
                                       1000000LL*3600*    a->du.tv.u.ds.hours +
                                       1000000LL*3600*24* a->du.tv.u.ds.days);
        bb = b->du.tv.sign*(long long)(
                                       1000LL*            b->du.tv.u.ds.frac +
                                       1000000LL*         b->du.tv.u.ds.sec +
                                       1000000LL*60*      b->du.tv.u.ds.mins +
                                       1000000LL*3600*    b->du.tv.u.ds.hours +
                                       1000000LL*3600*24* b->du.tv.u.ds.days);

        if (opcode == OP_Add ){
          aa += bb;
        }else if( opcode == OP_Subtract ){
          aa -= bb;
        }else return SQLITE_ERROR;

        // promote the result of the expression to microsecond resolution
        res->du.tv.type          = INTV_DSUS_TYPE;
        res->du.tv.sign          = (aa<0)?-1:1;
        aa = aa*res->du.tv.sign;
        _setIntervalDSUS(&res->du.tv.u.ds,
                         (((long long)aa)*res->du.tv.sign)/1000000,
                         (int)((aa*res->du.tv.sign)%1000000));
      }else{
        aa = a->du.tv.sign*(long long)(
                                       a->du.tv.u.ds.frac +
                                       1000000LL*         a->du.tv.u.ds.sec +
                                       1000000LL*60*      a->du.tv.u.ds.mins +
                                       1000000LL*3600*    a->du.tv.u.ds.hours +
                                       1000000LL*3600*24* a->du.tv.u.ds.days);
        bb = b->du.tv.sign*(long long)(
                                       b->du.tv.u.ds.frac +
                                       1000000LL*         b->du.tv.u.ds.sec +
                                       1000000LL*60*      b->du.tv.u.ds.mins +
                                       1000000LL*3600*    b->du.tv.u.ds.hours +
                                       1000000LL*3600*24* b->du.tv.u.ds.days);

        if( opcode == OP_Add ){
          aa += bb;
        }else if( opcode == OP_Subtract ){
          aa -= bb;
        }else return SQLITE_ERROR;

        res->du.tv.type          = INTV_DSUS_TYPE;
        res->du.tv.sign          = (aa<0)?-1:1;
        aa = aa*res->du.tv.sign;
        _setIntervalDSUS(&res->du.tv.u.ds,
                         (((long long)aa)*res->du.tv.sign)/1000000,
                         (int)((aa*res->du.tv.sign)%1000000));
      }

      break;
    }
    case INTV_DECIMAL_TYPE: {
        logmsg(LOGMSG_FATAL, 
              "BUG, wrong function called, use Decimalfy %s:%d\n",
              __FILE__, __LINE__);
        abort();
    }
  }
  return 0;
}

/*
**  Operate on interval and int, res = a opcode b;
*/
int sqlite3VdbeMemIntervalAndInt(
  const Mem *a,
  const Mem *b,
  int opcode,
  Mem * res
                                 ){
  bzero(res, sizeof(Mem));
  res->flags |= MEM_Interval;
  long long aa = 0, bb = 0;

  switch( a->du.tv.type ){
    case INTV_YM_TYPE: {
      aa = a->du.tv.sign*(int)(a->du.tv.u.ym.months + a->du.tv.u.ym.years*12);
      bb = b->u.i;

      if( opcode == OP_Multiply)  {
        aa *= bb;
      } else if (opcode == OP_Divide) {
        aa /= bb;
      } else return SQLITE_ERROR;

      res->du.tv.type          = INTV_YM_TYPE;
      res->du.tv.sign          = (aa<0)?-1:1;
      res->du.tv.u.ym.months   = aa*res->du.tv.sign;
      res->du.tv.u.ym.years    = 0;

      _normalizeIntervalYM(&res->du.tv.u.ym);
      break;
    }
    case INTV_DS_TYPE: {
      aa = a->du.tv.sign*(long long)(a->du.tv.u.ds.frac +
                                     1000LL*           a->du.tv.u.ds.sec +
                                     1000LL*60*        a->du.tv.u.ds.mins +
                                     1000LL*3600*      a->du.tv.u.ds.hours +
                                     1000LL*3600*24*   a->du.tv.u.ds.days);
      bb = b->u.i;

      if( opcode == OP_Multiply)  {
        aa *= bb;
      }else if (opcode == OP_Divide) {
        aa /= bb;
      }else return SQLITE_ERROR;

      res->du.tv.type          = INTV_DS_TYPE;
      res->du.tv.sign          = (aa<0)?-1:1;
      aa = aa*res->du.tv.sign;
      _setIntervalDS(&res->du.tv.u.ds,
                     (((long long)aa)*res->du.tv.sign)/1000,
                     (int)((aa*res->du.tv.sign)%1000));
      break;
    }
    case INTV_DSUS_TYPE: {
      aa = a->du.tv.sign*(long long)(
                                     a->du.tv.u.ds.frac +
                                     1000000LL*         a->du.tv.u.ds.sec +
                                     1000000LL*60*      a->du.tv.u.ds.mins +
                                     1000000LL*3600*    a->du.tv.u.ds.hours +
                                     1000000LL*3600*24* a->du.tv.u.ds.days);
      bb = b->u.i;

      if( opcode == OP_Multiply)  {
        aa *= bb;
      }else if (opcode == OP_Divide) {
        aa /= bb;
      }else return SQLITE_ERROR;

      res->du.tv.type          = INTV_DSUS_TYPE;
      res->du.tv.sign          = (aa<0)?-1:1;
      aa = aa*res->du.tv.sign;
      _setIntervalDSUS(&res->du.tv.u.ds,
                       (((long long)aa)*res->du.tv.sign)/1000000,
                       (int)((aa*res->du.tv.sign)%1000000));
      break;
    }
    case INTV_DECIMAL_TYPE: {
        logmsg(LOGMSG_FATAL, 
              "BUG, wrong function called, use Decimalfy %s:%d\n",
              __FILE__, __LINE__);
        abort();
    }
  }
  return 0;
}

/*
**  Operate on interval and int, res = a opcode b;
*/
int sqlite3VdbeMemIntAndInterval(
  const Mem *a,
  const Mem *b,
  int opcode,
  Mem * res
){
  bzero(res, sizeof(Mem));
  res->flags |= MEM_Interval;
  long long aa = 0, bb = 0;

  switch( b->du.tv.type ){
    case INTV_YM_TYPE: {
      aa = a->u.i;
      bb = b->du.tv.sign*(int)(b->du.tv.u.ym.months + b->du.tv.u.ym.years*12);

      if( opcode == OP_Multiply ){
        aa *= bb;
      }else return -1;

      res->du.tv.type          = INTV_YM_TYPE;
      res->du.tv.sign          = (aa<0)?-1:1;
      res->du.tv.u.ym.months   = aa*res->du.tv.sign;
      res->du.tv.u.ym.years    = 0;

      _normalizeIntervalYM(&res->du.tv.u.ym);
      break;
    }
    case INTV_DS_TYPE: {
      aa = a->u.i;
      bb = b->du.tv.sign*(long long)(b->du.tv.u.ds.frac +
                                     1000LL*           b->du.tv.u.ds.sec +
                                     1000LL*60*        b->du.tv.u.ds.mins +
                                     1000LL*3600*      b->du.tv.u.ds.hours +
                                     1000LL*3600*24*   b->du.tv.u.ds.days);

      if( opcode == OP_Multiply ){
        aa *= bb;
      }else return -1;

      res->du.tv.type          = INTV_DS_TYPE;
      res->du.tv.sign          = (aa<0)?-1:1;
      aa = aa*res->du.tv.sign;
      _setIntervalDS(&res->du.tv.u.ds,
                     (((long long)aa)*res->du.tv.sign)/1000,
                     (int)((aa*res->du.tv.sign)%1000));
      break;
    }
    case INTV_DSUS_TYPE: {
      aa = a->u.i;
      bb = b->du.tv.sign*(long long)(b->du.tv.u.ds.frac +
                                     1000000LL*         b->du.tv.u.ds.sec +
                                     1000000LL*60*      b->du.tv.u.ds.mins +
                                     1000000LL*3600*    b->du.tv.u.ds.hours +
                                     1000000LL*3600*24* b->du.tv.u.ds.days);

      if( opcode == OP_Multiply ){
        aa *= bb;
      }else return -1;

      res->du.tv.type          = INTV_DSUS_TYPE;
      res->du.tv.sign          = (aa<0)?-1:1;
      aa = aa*res->du.tv.sign;
      _setIntervalDSUS(&res->du.tv.u.ds,
                       (((long long)aa)*res->du.tv.sign)/1000000,
                       (int)((aa*res->du.tv.sign)%1000000));
      break;
    }
    case INTV_DECIMAL_TYPE: {
        logmsg(LOGMSG_FATAL, 
              "BUG, wrong function called, use Decimalfy %s:%d\n",
              __FILE__, __LINE__);
        abort();
    }
  }
  return 0;
}

/*
** Implement substraction of two datetimes
** The result is a day-second interval
*/
int sqlite3VdbeMemDatetimeAndDatetime(
  const Mem *a,
  const Mem *b,
  int opcode,
  Mem *res
){
  bzero(res, sizeof(Mem));
  res->flags |= MEM_Interval;
  if (opcode != OP_Subtract) return SQLITE_ERROR;
  const dttz_t *d1 = &a->du.dt;
  const dttz_t *d2 = &b->du.dt;
  sub_dttz_dttz(d1, d2, &res->du.tv);
  return 0;
}

static int _dttz_to_native_datetime(cdb2_client_datetime_t * cdt, const Mem *inp) {

    char    tmp[11];
    unsigned char buf[CLIENT_DATETIME_LEN];
    unsigned char *p_buf=buf, *p_buf_end=(p_buf+CLIENT_DATETIME_LEN);
    int     outnull = 0;
    int     outdtsz  = 0; 
    struct field_conv_opts_tz tzopts;

    /* provide the timezone to the conversion routines */
    bzero(&tzopts, sizeof(tzopts));
    tzopts.flags |= 2 /*FLD_CONV_TZONE*/;
    if( !inp->tz ) return SQLITE_ERROR;
    strncpy(tzopts.tzname, inp->tz, sizeof(tzopts.tzname));

    /* ugly, arghh */
    bzero(tmp, sizeof(tmp));

    /* limit range to [-9999-01-01T235959.000 GMT, 9999-12-31T000000.000 GMT] */
    if( !debug_switch_unlimited_datetime_range()
        && (inp->du.dt.dttz_sec < -377705030401ll
        || inp->du.dt.dttz_sec > 253402214400ll )){
      return -1;
    }

    tmp[0] = 8;
    *(long long*)&tmp[1] = flibc_htonll(inp->du.dt.dttz_sec);
    tmp[1] ^= 0x80; 
    *(unsigned short*)&tmp[9] = htons(
        inp->du.dt.dttz_prec == DTTZ_PREC_MSEC ?
        inp->du.dt.dttz_frac : inp->du.dt.dttz_frac / 1000);

    if(SERVER_DATETIME_to_CLIENT_DATETIME(&tmp, sizeof(tmp),  
              NULL, NULL,
              buf, sizeof(buf), &outnull, &outdtsz,
              (const struct field_conv_opts*)&tzopts, NULL))

            return SQLITE_ERROR;

    if(!(client_datetime_get(cdt,p_buf,p_buf_end)))
    {
        return SQLITE_ERROR;
    }
    
    return 0;
}

static int _dttz_to_native_datetimeus(cdb2_client_datetimeus_t * cdt, const Mem *inp) {

    char    tmp[13];
    unsigned char buf[CLIENT_DATETIME_LEN];
    unsigned char *p_buf=buf, *p_buf_end=(p_buf+CLIENT_DATETIME_LEN);
    int     outnull = 0;
    int     outdtsz  = 0; 
    struct field_conv_opts_tz tzopts;

    /* provide the timezone to the conversion routines */
    bzero(&tzopts, sizeof(tzopts));
    tzopts.flags |= 2 /*FLD_CONV_TZONE*/;
    if(!inp->tz) return SQLITE_ERROR;
    strncpy(tzopts.tzname, inp->tz, sizeof(tzopts.tzname));

    /* ugly, arghh */
    bzero(tmp, sizeof(tmp));

    /* limit range to [-9999-01-01T235959.000 GMT, 9999-12-31T000000.000 GMT] */
    if (!debug_switch_unlimited_datetime_range() && 
        (inp->du.dt.dttz_sec < -377705030401ll || 
         inp->du.dt.dttz_sec > 253402214400ll))
       return -1;

    tmp[0] = 8;
    *(long long*)&tmp[1] = flibc_htonll(inp->du.dt.dttz_sec);
    tmp[1] ^= 0x80; 
    *(unsigned short*)&tmp[9] = htonl(
        inp->du.dt.dttz_prec == DTTZ_PREC_USEC ?
        inp->du.dt.dttz_frac : inp->du.dt.dttz_frac * 1000);

    if(SERVER_DATETIMEUS_to_CLIENT_DATETIMEUS(&tmp, sizeof(tmp),  
              NULL, NULL,
              buf, sizeof(buf), &outnull, &outdtsz,
              (const struct field_conv_opts*)&tzopts, NULL))

            return SQLITE_ERROR;

    if(!(client_datetimeus_get(cdt,p_buf,p_buf_end)))
    {
        return SQLITE_ERROR;
    }
    
    return 0;
}

static int _native_datetime_to_dttz(cdb2_client_datetime_t * cdt, Mem * res) {

    char    tmp[11] = {0};
    unsigned char buf[CLIENT_DATETIME_LEN];
    unsigned char *p_buf=buf, *p_buf_end=(p_buf+CLIENT_DATETIME_LEN);
    int     outdtsz  = 0; 

    if(!(client_datetime_put(cdt,p_buf, p_buf_end)))
    {
        return SQLITE_ERROR;
    }

    if(CLIENT_DATETIME_to_SERVER_DATETIME(buf, sizeof(buf), 0, NULL, NULL,
                    tmp, sizeof(tmp), &outdtsz, NULL, NULL)) return SQLITE_ERROR;

    /* convert server datetime to native datetime */
    tmp[1] ^= 0x80;
    res->du.dt.dttz_sec   = flibc_ntohll(*(unsigned long long*)&tmp[1]);
    res->du.dt.dttz_frac  = ntohl(*(unsigned short*)&tmp[9]);
    res->du.dt.dttz_prec  = DTTZ_PREC_MSEC;

    return 0;
}

static int _native_datetimeus_to_dttz(cdb2_client_datetimeus_t * cdt, Mem * res) {

    char    tmp[13] = {0};
    unsigned char buf[CLIENT_DATETIME_LEN];
    unsigned char *p_buf=buf, *p_buf_end=(p_buf+CLIENT_DATETIME_LEN);
    int     outdtsz  = 0; 

    if(!(client_datetimeus_put(cdt,p_buf, p_buf_end)))
    {
        return SQLITE_ERROR;
    }

    if(CLIENT_DATETIMEUS_to_SERVER_DATETIMEUS(buf, sizeof(buf), 0, NULL, NULL,
                    tmp, sizeof(tmp), &outdtsz, NULL, NULL)) return SQLITE_ERROR;

    /* convert server datetime to native datetime */
    tmp[1] ^= 0x80;
    res->du.dt.dttz_sec   = flibc_ntohll(*(unsigned long long*)&tmp[1]);
    res->du.dt.dttz_frac  = ntohl(*(unsigned int*)&tmp[9]);
    res->du.dt.dttz_prec  = DTTZ_PREC_USEC;

    return 0;
}

/* "b" better be normalized */
int sqlite3VdbeMemDatetimeAndInterval(
  const Mem *a,
  const Mem *b,
  int opcode,
  Mem * res
){

  bzero(res, sizeof(Mem));
  res->flags |= MEM_Datetime;
  res->tz = a->tz; /* propagate tzname */

  /* operations */
  if( b->du.tv.type == INTV_YM_TYPE ){
    if( a->du.dt.dttz_prec == DTTZ_PREC_MSEC ){
      cdb2_client_datetime_t   cdt;

      /* convert server_datetime_t to cdb2_client_datetime_t */
      if( _dttz_to_native_datetime(&cdt, a) ) return SQLITE_ERROR;

      if( (opcode == OP_Add && b->du.tv.sign >0) || 
          (opcode == OP_Subtract && b->du.tv.sign <0) ){
        cdt.tm.tm_year  += b->du.tv.u.ym.years;
        cdt.tm.tm_mon += b->du.tv.u.ym.months;
      }else if( (opcode == OP_Add && b->du.tv.sign<0) || 
                 (opcode == OP_Subtract && b->du.tv.sign >0) ){
        cdt.tm.tm_year  -= b->du.tv.u.ym.years;
        cdt.tm.tm_mon -= b->du.tv.u.ym.months;
      }else return SQLITE_ERROR;

      /* convert cdb2_client_datetime_t to server_datetime_t */
      if( _native_datetime_to_dttz(&cdt, res) ) return SQLITE_ERROR;
    }else{
      cdb2_client_datetimeus_t   cdt;

      /* convert server_datetimeus_t to cdb2_client_datetimeus_t */
      if( _dttz_to_native_datetimeus(&cdt, a) ) return SQLITE_ERROR;

      if( (opcode == OP_Add && b->du.tv.sign >0) || 
          (opcode == OP_Subtract && b->du.tv.sign <0) ){
        cdt.tm.tm_year  += b->du.tv.u.ym.years;
        cdt.tm.tm_mon += b->du.tv.u.ym.months;
      }else if( (opcode == OP_Add && b->du.tv.sign<0) || 
                 (opcode == OP_Subtract && b->du.tv.sign >0) ){
        cdt.tm.tm_year  -= b->du.tv.u.ym.years;
        cdt.tm.tm_mon -= b->du.tv.u.ym.months;
      }else return SQLITE_ERROR;

      /* convert cdb2_client_datetimeus_t to server_datetimeus_t */
      if( _native_datetimeus_to_dttz(&cdt, res) ) return SQLITE_ERROR;
    }
    res->du.dt.dttz_conv = (a->du.dt.dttz_conv == DTTZ_CONV_NOW);
  }else if( b->du.tv.type == INTV_DS_TYPE || b->du.tv.type == INTV_DSUS_TYPE ){
    if( opcode == OP_Add ){
      add_dttz_intvds(&a->du.dt, &b->du.tv, &res->du.dt);
    }else if( opcode == OP_Subtract ){
      sub_dttz_intvds(&a->du.dt, &b->du.tv, &res->du.dt);
    }else{
      return SQLITE_ERROR;
    }
  }else{
    logmsg(LOGMSG_FATAL, 
          "BUG, wrong function called, use Decimalfy %s:%d\n",
          __FILE__, __LINE__);
    abort();
  }

  return 0;
}

int sqlite3VdbeMemIntervalAndDatetime(
  const Mem *a,
  const Mem *b,
  int opcode,
  Mem * res
){

  bzero(res, sizeof(Mem));
  res->flags |= MEM_Datetime;
  res->tz = b->tz; /* propagate tzname */

  if( a->du.tv.type == INTV_YM_TYPE ){
    if ( b->du.dt.dttz_prec == DTTZ_PREC_MSEC ){
      cdb2_client_datetime_t   cdt;

      /* convert server_datetime_t to cdb2_client_datetime_t */
      if( _dttz_to_native_datetime(&cdt, b) ) return SQLITE_ERROR;

      /* operations */
      if( opcode == OP_Add ){
        cdt.tm.tm_year  += a->du.tv.u.ym.years;
        cdt.tm.tm_mon += a->du.tv.u.ym.months;
      }else return SQLITE_ERROR;

      /* convert cdb2_client_datetime_t to server_datetime_t */
      if(_native_datetime_to_dttz(&cdt, res)) return SQLITE_ERROR;
    }else{
      cdb2_client_datetimeus_t   cdt;

      /* convert server_datetimeus_t to cdb2_client_datetimeus_t */
      if( _dttz_to_native_datetimeus(&cdt, b) ) return SQLITE_ERROR;

      /* operations */
      if( opcode == OP_Add ){
        cdt.tm.tm_year  += a->du.tv.u.ym.years;
        cdt.tm.tm_mon += a->du.tv.u.ym.months;
      }else return SQLITE_ERROR;

      /* convert cdb2_client_datetimeus_t to server_datetimeus_t */
      if( _native_datetimeus_to_dttz(&cdt, res) ) return SQLITE_ERROR;
    }
    res->du.dt.dttz_conv = (b->du.dt.dttz_conv == DTTZ_CONV_NOW);
  }else if( a->du.tv.type == INTV_DS_TYPE || a->du.tv.type == INTV_DSUS_TYPE ){
    if( opcode == OP_Add ){
      add_dttz_intvds(&b->du.dt, &a->du.tv, &res->du.dt);
    }else{
      return SQLITE_ERROR;
    }
  }else{
    logmsg(LOGMSG_DEBUG, "%s:%d: No arithmetics between decimal and datetime\n",
          __FILE__, __LINE__);
    return SQLITE_ERROR;
  }

  return 0;
}

/*
   Mem is an decimal type;
   Mem is anything;
 */
int sqliteVdbeMemDecimalBasicArithmetics(
  Mem *a,
  Mem *b,
  int opcode,
  Mem * res,
  int flipped
){
  decContext  ctx;
  void        *ret = NULL;
  int         rc = 0;
  Mem         bcopy;

  bzero(&bcopy, sizeof(Mem));
  bzero(res, sizeof(Mem));
  res->flags |= MEM_Interval;

  switch( b->flags & MEM_TypeMask ){
    case MEM_Int:
    case MEM_Str: {
      sqlite3VdbeMemCopy(&bcopy, b);
      b = &bcopy;
      
      rc = sqlite3VdbeMemDecimalfy(b);
      if( rc ){
        goto done;
      }

      /* fall through */
    }
    case MEM_Interval: {
      Mem *tmp;

      if( flipped ){
        tmp=b;
        b=a;
        a=tmp;
      }

      dec_ctx_init( &ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);
      
      switch( opcode ){
        case OP_Add: {
          ret = decQuadAdd((decQuad*)&res->du.tv.u.dec, 
                           (const decQuad*)&a->du.tv.u.dec, 
                           (const decQuad*)&b->du.tv.u.dec, 
                           &ctx);
          break;
        }
        case OP_Subtract: {
          ret = decQuadSubtract((decQuad*)&res->du.tv.u.dec, 
                                (const decQuad*)&a->du.tv.u.dec, 
                                (const decQuad*)&b->du.tv.u.dec, 
                                &ctx);
          break;
        }
        case OP_Divide: {
          ret = decQuadDivide((decQuad*)&res->du.tv.u.dec, 
                              (const decQuad*)&a->du.tv.u.dec, 
                              (const decQuad*)&b->du.tv.u.dec, 
                              &ctx);
          break;
        }
        case OP_Multiply: {
          ret = decQuadMultiply((decQuad*)&res->du.tv.u.dec, 
                                (const decQuad*)&a->du.tv.u.dec, 
                                (const decQuad*)&b->du.tv.u.dec, 
                                &ctx);
          break;
        }
        case OP_Remainder: {
          ret = decQuadRemainder((decQuad*)&res->du.tv.u.dec, 
                                 (const decQuad*)&a->du.tv.u.dec, 
                                 (const decQuad*)&b->du.tv.u.dec, 
                                 &ctx);
          break;
        }
        default: {
          rc = SQLITE_ERROR;
          goto done;
        }
      }

      if( !ret ){
        rc = SQLITE_ERROR;
        goto done;
      }else{
        if( dfp_conv_check_status( &ctx, "arithmetics", "quad") ){
          rc = SQLITE_ERROR;
          goto done;
        }
      }

      res->du.tv.type = INTV_DECIMAL_TYPE;
      res->du.tv.sign = 0;
      break;
    }

    default: {
      rc = SQLITE_ERROR;
      goto done;
    }
  }

done:
   sqlite3VdbeMemRelease(&bcopy);
   return rc;
}


#endif /* SQLITE_BUILDING_FOR_COMDB2 */
