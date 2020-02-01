/*
** 2001 September 15
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
** Memory allocation functions used throughout sqlite.
*/
#include "sqliteInt.h"
#include <stdarg.h>

/*
** Attempt to release up to n bytes of non-essential memory currently
** held by SQLite. An example of non-essential memory is memory used to
** cache database pages that are not currently in use.
*/
int sqlitex_release_memory(int n){
#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
  return sqlitexPcacheReleaseMemory(n);
#else
  /* IMPLEMENTATION-OF: R-34391-24921 The sqlitex_release_memory() routine
  ** is a no-op returning zero if SQLite is not compiled with
  ** SQLITE_ENABLE_MEMORY_MANAGEMENT. */
  UNUSED_PARAMETER(n);
  return 0;
#endif
}

/*
** An instance of the following object records the location of
** each unused scratch buffer.
*/
typedef struct ScratchFreeslot {
  struct ScratchFreeslot *pNext;   /* Next unused scratch buffer */
} ScratchFreeslot;

/*
** State information local to the memory allocation subsystem.
*/
static SQLITE_WSD struct Mem0Global {
  sqlitex_mutex *mutex;         /* Mutex to serialize access */

  /*
  ** The alarm callback and its arguments.  The mem0.mutex lock will
  ** be held while the callback is running.  Recursive calls into
  ** the memory subsystem are allowed, but no new callbacks will be
  ** issued.
  */
  sqlitex_int64 alarmThreshold;
  void (*alarmCallback)(void*, sqlitex_int64,int);
  void *alarmArg;

  /*
  ** Pointers to the end of sqlitexGlobalConfig.pScratch memory
  ** (so that a range test can be used to determine if an allocation
  ** being freed came from pScratch) and a pointer to the list of
  ** unused scratch allocations.
  */
  void *pScratchEnd;
  ScratchFreeslot *pScratchFree;
  u32 nScratchFree;

  /*
  ** True if heap is nearly "full" where "full" is defined by the
  ** sqlitex_soft_heap_limit() setting.
  */
  int nearlyFull;
} mem0 = { 0, 0, 0, 0, 0, 0, 0, 0 };

#define mem0 GLOBAL(struct Mem0Global, mem0)

/*
** This routine runs when the memory allocator sees that the
** total memory allocation is about to exceed the soft heap
** limit.
*/
static void softHeapLimitEnforcer(
  void *NotUsed, 
  sqlitex_int64 NotUsed2,
  int allocSize
){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  sqlitex_release_memory(allocSize);
}

/*
** Change the alarm callback
*/
static int sqlitexMemoryAlarm(
  void(*xCallback)(void *pArg, sqlitex_int64 used,int N),
  void *pArg,
  sqlitex_int64 iThreshold
){
  int nUsed;
  sqlitex_mutex_enter(mem0.mutex);
  mem0.alarmCallback = xCallback;
  mem0.alarmArg = pArg;
  mem0.alarmThreshold = iThreshold;
  nUsed = sqlitexStatusValue(SQLITE_STATUS_MEMORY_USED);
  mem0.nearlyFull = (iThreshold>0 && iThreshold<=nUsed);
  sqlitex_mutex_leave(mem0.mutex);
  return SQLITE_OK;
}

#ifndef SQLITE_OMIT_DEPRECATED
/*
** Deprecated external interface.  Internal/core SQLite code
** should call sqlitexMemoryAlarm.
*/
int sqlitex_memory_alarm(
  void(*xCallback)(void *pArg, sqlitex_int64 used,int N),
  void *pArg,
  sqlitex_int64 iThreshold
){
  return sqlitexMemoryAlarm(xCallback, pArg, iThreshold);
}
#endif

/*
** Set the soft heap-size limit for the library. Passing a zero or 
** negative value indicates no limit.
*/
sqlitex_int64 sqlitex_soft_heap_limit64(sqlitex_int64 n){
  sqlitex_int64 priorLimit;
  sqlitex_int64 excess;
#ifndef SQLITE_OMIT_AUTOINIT
  int rc = sqlitex_initialize();
  if( rc ) return -1;
#endif
  sqlitex_mutex_enter(mem0.mutex);
  priorLimit = mem0.alarmThreshold;
  sqlitex_mutex_leave(mem0.mutex);
  if( n<0 ) return priorLimit;
  if( n>0 ){
    sqlitexMemoryAlarm(softHeapLimitEnforcer, 0, n);
  }else{
    sqlitexMemoryAlarm(0, 0, 0);
  }
  excess = sqlitex_memory_used() - n;
  if( excess>0 ) sqlitex_release_memory((int)(excess & 0x7fffffff));
  return priorLimit;
}
void sqlitex_soft_heap_limit(int n){
  if( n<0 ) n = 0;
  sqlitex_soft_heap_limit64(n);
}

/*
** Initialize the memory allocation subsystem.
*/
int sqlitexMallocInit(void){
  if( sqlitexGlobalConfig.m.xMalloc==0 ){
    sqlitexMemSetDefault();
  }
  memset(&mem0, 0, sizeof(mem0));
  if( sqlitexGlobalConfig.bCoreMutex ){
    mem0.mutex = sqlitexMutexAlloc(SQLITE_MUTEX_STATIC_MEM);
  }
  if( sqlitexGlobalConfig.pScratch && sqlitexGlobalConfig.szScratch>=100
      && sqlitexGlobalConfig.nScratch>0 ){
    int i, n, sz;
    ScratchFreeslot *pSlot;
    sz = ROUNDDOWN8(sqlitexGlobalConfig.szScratch);
    sqlitexGlobalConfig.szScratch = sz;
    pSlot = (ScratchFreeslot*)sqlitexGlobalConfig.pScratch;
    n = sqlitexGlobalConfig.nScratch;
    mem0.pScratchFree = pSlot;
    mem0.nScratchFree = n;
    for(i=0; i<n-1; i++){
      pSlot->pNext = (ScratchFreeslot*)(sz+(char*)pSlot);
      pSlot = pSlot->pNext;
    }
    pSlot->pNext = 0;
    mem0.pScratchEnd = (void*)&pSlot[1];
  }else{
    mem0.pScratchEnd = 0;
    sqlitexGlobalConfig.pScratch = 0;
    sqlitexGlobalConfig.szScratch = 0;
    sqlitexGlobalConfig.nScratch = 0;
  }
  if( sqlitexGlobalConfig.pPage==0 || sqlitexGlobalConfig.szPage<512
      || sqlitexGlobalConfig.nPage<1 ){
    sqlitexGlobalConfig.pPage = 0;
    sqlitexGlobalConfig.szPage = 0;
    sqlitexGlobalConfig.nPage = 0;
  }
  return sqlitexGlobalConfig.m.xInit(sqlitexGlobalConfig.m.pAppData);
}

/*
** Return true if the heap is currently under memory pressure - in other
** words if the amount of heap used is close to the limit set by
** sqlitex_soft_heap_limit().
*/
int sqlitexHeapNearlyFull(void){
  return mem0.nearlyFull;
}

/*
** Deinitialize the memory allocation subsystem.
*/
void sqlitexMallocEnd(void){
  if( sqlitexGlobalConfig.m.xShutdown ){
    sqlitexGlobalConfig.m.xShutdown(sqlitexGlobalConfig.m.pAppData);
  }
  memset(&mem0, 0, sizeof(mem0));
}

/*
** Return the amount of memory currently checked out.
*/
sqlitex_int64 sqlitex_memory_used(void){
  int n, mx;
  sqlitex_int64 res;
  sqlitex_status(SQLITE_STATUS_MEMORY_USED, &n, &mx, 0);
  res = (sqlitex_int64)n;  /* Work around bug in Borland C. Ticket #3216 */
  return res;
}

/*
** Return the maximum amount of memory that has ever been
** checked out since either the beginning of this process
** or since the most recent reset.
*/
sqlitex_int64 sqlitex_memory_highwater(int resetFlag){
  int n, mx;
  sqlitex_int64 res;
  sqlitex_status(SQLITE_STATUS_MEMORY_USED, &n, &mx, resetFlag);
  res = (sqlitex_int64)mx;  /* Work around bug in Borland C. Ticket #3216 */
  return res;
}

/*
** Trigger the alarm 
*/
static void sqlitexMallocAlarm(int nByte){
  void (*xCallback)(void*,sqlitex_int64,int);
  sqlitex_int64 nowUsed;
  void *pArg;
  if( mem0.alarmCallback==0 ) return;
  xCallback = mem0.alarmCallback;
  nowUsed = sqlitexStatusValue(SQLITE_STATUS_MEMORY_USED);
  pArg = mem0.alarmArg;
  mem0.alarmCallback = 0;
  sqlitex_mutex_leave(mem0.mutex);
  xCallback(pArg, nowUsed, nByte);
  sqlitex_mutex_enter(mem0.mutex);
  mem0.alarmCallback = xCallback;
  mem0.alarmArg = pArg;
}

/*
** Do a memory allocation with statistics and alarms.  Assume the
** lock is already held.
*/
static int mallocWithAlarm(int n, void **pp){
  int nFull;
  void *p;
  assert( sqlitex_mutex_held(mem0.mutex) );
  nFull = sqlitexGlobalConfig.m.xRoundup(n);
  sqlitexStatusSet(SQLITE_STATUS_MALLOC_SIZE, n);
  if( mem0.alarmCallback!=0 ){
    int nUsed = sqlitexStatusValue(SQLITE_STATUS_MEMORY_USED);
    if( nUsed >= mem0.alarmThreshold - nFull ){
      mem0.nearlyFull = 1;
      sqlitexMallocAlarm(nFull);
    }else{
      mem0.nearlyFull = 0;
    }
  }
  p = sqlitexGlobalConfig.m.xMalloc(nFull);
#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
  if( p==0 && mem0.alarmCallback ){
    sqlitexMallocAlarm(nFull);
    p = sqlitexGlobalConfig.m.xMalloc(nFull);
  }
#endif
  if( p ){
    nFull = sqlitexMallocSize(p);
    sqlitexStatusAdd(SQLITE_STATUS_MEMORY_USED, nFull);
    sqlitexStatusAdd(SQLITE_STATUS_MALLOC_COUNT, 1);
  }
  *pp = p;
  return nFull;
}

/*
** Allocate memory.  This routine is like sqlitex_malloc() except that it
** assumes the memory subsystem has already been initialized.
*/
void *sqlitexMalloc(u64 n){
  void *p;
  if( n==0 || n>=0x7fffff00 ){
    /* A memory allocation of a number of bytes which is near the maximum
    ** signed integer value might cause an integer overflow inside of the
    ** xMalloc().  Hence we limit the maximum size to 0x7fffff00, giving
    ** 255 bytes of overhead.  SQLite itself will never use anything near
    ** this amount.  The only way to reach the limit is with sqlitex_malloc() */
    p = 0;
  }else if( sqlitexGlobalConfig.bMemstat ){
    sqlitex_mutex_enter(mem0.mutex);
    mallocWithAlarm((int)n, &p);
    sqlitex_mutex_leave(mem0.mutex);
  }else{
    p = sqlitexGlobalConfig.m.xMalloc((int)n);
  }
  assert( EIGHT_BYTE_ALIGNMENT(p) );  /* IMP: R-11148-40995 */
  return p;
}

/*
** This version of the memory allocation is for use by the application.
** First make sure the memory subsystem is initialized, then do the
** allocation.
*/
void *sqlitex_malloc(int n){
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlitex_initialize() ) return 0;
#endif
  return n<=0 ? 0 : sqlitexMalloc(n);
}
void *sqlitex_malloc64(sqlitex_uint64 n){
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlitex_initialize() ) return 0;
#endif
  return sqlitexMalloc(n);
}

/*
** Each thread may only have a single outstanding allocation from
** xScratchMalloc().  We verify this constraint in the single-threaded
** case by setting scratchAllocOut to 1 when an allocation
** is outstanding clearing it when the allocation is freed.
*/
#if SQLITE_THREADSAFE==0 && !defined(NDEBUG)
static int scratchAllocOut = 0;
#endif


/*
** Allocate memory that is to be used and released right away.
** This routine is similar to alloca() in that it is not intended
** for situations where the memory might be held long-term.  This
** routine is intended to get memory to old large transient data
** structures that would not normally fit on the stack of an
** embedded processor.
*/
void *sqlitexScratchMalloc(int n){
  void *p;
  assert( n>0 );

  sqlitex_mutex_enter(mem0.mutex);
  sqlitexStatusSet(SQLITE_STATUS_SCRATCH_SIZE, n);
  if( mem0.nScratchFree && sqlitexGlobalConfig.szScratch>=n ){
    p = mem0.pScratchFree;
    mem0.pScratchFree = mem0.pScratchFree->pNext;
    mem0.nScratchFree--;
    sqlitexStatusAdd(SQLITE_STATUS_SCRATCH_USED, 1);
    sqlitex_mutex_leave(mem0.mutex);
  }else{
    sqlitex_mutex_leave(mem0.mutex);
    p = sqlitexMalloc(n);
    if( sqlitexGlobalConfig.bMemstat && p ){
      sqlitex_mutex_enter(mem0.mutex);
      sqlitexStatusAdd(SQLITE_STATUS_SCRATCH_OVERFLOW, sqlitexMallocSize(p));
      sqlitex_mutex_leave(mem0.mutex);
    }
    sqlitexMemdebugSetType(p, MEMTYPE_SCRATCH);
  }
  assert( sqlitex_mutex_notheld(mem0.mutex) );


#if SQLITE_THREADSAFE==0 && !defined(NDEBUG)
  /* EVIDENCE-OF: R-12970-05880 SQLite will not use more than one scratch
  ** buffers per thread.
  **
  ** This can only be checked in single-threaded mode.
  */
  assert( scratchAllocOut==0 );
  if( p ) scratchAllocOut++;
#endif

  return p;
}
void sqlitexScratchFree(void *p){
  if( p ){

#if SQLITE_THREADSAFE==0 && !defined(NDEBUG)
    /* Verify that no more than two scratch allocation per thread
    ** is outstanding at one time.  (This is only checked in the
    ** single-threaded case since checking in the multi-threaded case
    ** would be much more complicated.) */
    assert( scratchAllocOut>=1 && scratchAllocOut<=2 );
    scratchAllocOut--;
#endif

    if( p>=sqlitexGlobalConfig.pScratch && p<mem0.pScratchEnd ){
      /* Release memory from the SQLITE_CONFIG_SCRATCH allocation */
      ScratchFreeslot *pSlot;
      pSlot = (ScratchFreeslot*)p;
      sqlitex_mutex_enter(mem0.mutex);
      pSlot->pNext = mem0.pScratchFree;
      mem0.pScratchFree = pSlot;
      mem0.nScratchFree++;
      assert( mem0.nScratchFree <= (u32)sqlitexGlobalConfig.nScratch );
      sqlitexStatusAdd(SQLITE_STATUS_SCRATCH_USED, -1);
      sqlitex_mutex_leave(mem0.mutex);
    }else{
      /* Release memory back to the heap */
      assert( sqlitexMemdebugHasType(p, MEMTYPE_SCRATCH) );
      assert( sqlitexMemdebugNoType(p, (u8)~MEMTYPE_SCRATCH) );
      sqlitexMemdebugSetType(p, MEMTYPE_HEAP);
      if( sqlitexGlobalConfig.bMemstat ){
        int iSize = sqlitexMallocSize(p);
        sqlitex_mutex_enter(mem0.mutex);
        sqlitexStatusAdd(SQLITE_STATUS_SCRATCH_OVERFLOW, -iSize);
        sqlitexStatusAdd(SQLITE_STATUS_MEMORY_USED, -iSize);
        sqlitexStatusAdd(SQLITE_STATUS_MALLOC_COUNT, -1);
        sqlitexGlobalConfig.m.xFree(p);
        sqlitex_mutex_leave(mem0.mutex);
      }else{
        sqlitexGlobalConfig.m.xFree(p);
      }
    }
  }
}

/*
** TRUE if p is a lookaside memory allocation from db
*/
#ifndef SQLITE_OMIT_LOOKASIDE
static int isLookaside(sqlitex *db, void *p){
  return p>=db->lookaside.pStart && p<db->lookaside.pEnd;
}
#else
#define isLookaside(A,B) 0
#endif

/*
** Return the size of a memory allocation previously obtained from
** sqlitexMalloc() or sqlitex_malloc().
*/
int sqlitexMallocSize(void *p){
  assert( sqlitexMemdebugHasType(p, MEMTYPE_HEAP) );
  return sqlitexGlobalConfig.m.xSize(p);
}
int sqlitexDbMallocSize(sqlitex *db, void *p){
  if( db==0 ){
    assert( sqlitexMemdebugNoType(p, (u8)~MEMTYPE_HEAP) );
    assert( sqlitexMemdebugHasType(p, MEMTYPE_HEAP) );
    return sqlitexMallocSize(p);
  }else{
    assert( sqlitex_mutex_held(db->mutex) );
    if( isLookaside(db, p) ){
      return db->lookaside.sz;
    }else{
      assert( sqlitexMemdebugHasType(p, (MEMTYPE_LOOKASIDE|MEMTYPE_HEAP)) );
      assert( sqlitexMemdebugNoType(p, (u8)~(MEMTYPE_LOOKASIDE|MEMTYPE_HEAP)) );
      return sqlitexGlobalConfig.m.xSize(p);
    }
  }
}
sqlitex_uint64 sqlitex_msize(void *p){
  assert( sqlitexMemdebugNoType(p, (u8)~MEMTYPE_HEAP) );
  assert( sqlitexMemdebugHasType(p, MEMTYPE_HEAP) );
  return (sqlitex_uint64)sqlitexGlobalConfig.m.xSize(p);
}

/*
** Free memory previously obtained from sqlitexMalloc().
*/
void sqlitex_free(void *p){
  if( p==0 ) return;  /* IMP: R-49053-54554 */
  assert( sqlitexMemdebugHasType(p, MEMTYPE_HEAP) );
  assert( sqlitexMemdebugNoType(p, (u8)~MEMTYPE_HEAP) );
  if( sqlitexGlobalConfig.bMemstat ){
    sqlitex_mutex_enter(mem0.mutex);
    sqlitexStatusAdd(SQLITE_STATUS_MEMORY_USED, -sqlitexMallocSize(p));
    sqlitexStatusAdd(SQLITE_STATUS_MALLOC_COUNT, -1);
    sqlitexGlobalConfig.m.xFree(p);
    sqlitex_mutex_leave(mem0.mutex);
  }else{
    sqlitexGlobalConfig.m.xFree(p);
  }
}

/*
** Add the size of memory allocation "p" to the count in
** *db->pnBytesFreed.
*/
static SQLITE_NOINLINE void measureAllocationSize(sqlitex *db, void *p){
  *db->pnBytesFreed += sqlitexDbMallocSize(db,p);
}

/*
** Free memory that might be associated with a particular database
** connection.
*/
void sqlitexDbFree(sqlitex *db, void *p){
  assert( db==0 || sqlitex_mutex_held(db->mutex) );
  if( p==0 ) return;
  if( db ){
    if( db->pnBytesFreed ){
      measureAllocationSize(db, p);
      return;
    }
    if( isLookaside(db, p) ){
      LookasideSlot *pBuf = (LookasideSlot*)p;
#if SQLITE_DEBUG
      /* Trash all content in the buffer being freed */
      memset(p, 0xaa, db->lookaside.sz);
#endif
      pBuf->pNext = db->lookaside.pFree;
      db->lookaside.pFree = pBuf;
      db->lookaside.nOut--;
      return;
    }
  }
  assert( sqlitexMemdebugHasType(p, (MEMTYPE_LOOKASIDE|MEMTYPE_HEAP)) );
  assert( sqlitexMemdebugNoType(p, (u8)~(MEMTYPE_LOOKASIDE|MEMTYPE_HEAP)) );
  assert( db!=0 || sqlitexMemdebugNoType(p, MEMTYPE_LOOKASIDE) );
  sqlitexMemdebugSetType(p, MEMTYPE_HEAP);
  sqlitex_free(p);
}

/*
** Change the size of an existing memory allocation
*/
void *sqlitexRealloc(void *pOld, u64 nBytes){
  int nOld, nNew, nDiff;
  void *pNew;
  assert( sqlitexMemdebugHasType(pOld, MEMTYPE_HEAP) );
  assert( sqlitexMemdebugNoType(pOld, (u8)~MEMTYPE_HEAP) );
  if( pOld==0 ){
    return sqlitexMalloc(nBytes); /* IMP: R-04300-56712 */
  }
  if( nBytes==0 ){
    sqlitex_free(pOld); /* IMP: R-26507-47431 */
    return 0;
  }
  if( nBytes>=0x7fffff00 ){
    /* The 0x7ffff00 limit term is explained in comments on sqlitexMalloc() */
    return 0;
  }
  nOld = sqlitexMallocSize(pOld);
  /* IMPLEMENTATION-OF: R-46199-30249 SQLite guarantees that the second
  ** argument to xRealloc is always a value returned by a prior call to
  ** xRoundup. */
  nNew = sqlitexGlobalConfig.m.xRoundup((int)nBytes);
  if( nOld==nNew ){
    pNew = pOld;
  }else if( sqlitexGlobalConfig.bMemstat ){
    sqlitex_mutex_enter(mem0.mutex);
    sqlitexStatusSet(SQLITE_STATUS_MALLOC_SIZE, (int)nBytes);
    nDiff = nNew - nOld;
    if( sqlitexStatusValue(SQLITE_STATUS_MEMORY_USED) >= 
          mem0.alarmThreshold-nDiff ){
      sqlitexMallocAlarm(nDiff);
    }
    pNew = sqlitexGlobalConfig.m.xRealloc(pOld, nNew);
    if( pNew==0 && mem0.alarmCallback ){
      sqlitexMallocAlarm((int)nBytes);
      pNew = sqlitexGlobalConfig.m.xRealloc(pOld, nNew);
    }
    if( pNew ){
      nNew = sqlitexMallocSize(pNew);
      sqlitexStatusAdd(SQLITE_STATUS_MEMORY_USED, nNew-nOld);
    }
    sqlitex_mutex_leave(mem0.mutex);
  }else{
    pNew = sqlitexGlobalConfig.m.xRealloc(pOld, nNew);
  }
  assert( EIGHT_BYTE_ALIGNMENT(pNew) ); /* IMP: R-11148-40995 */
  return pNew;
}

/*
** The public interface to sqlitexRealloc.  Make sure that the memory
** subsystem is initialized prior to invoking sqliteRealloc.
*/
void *sqlitex_realloc(void *pOld, int n){
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlitex_initialize() ) return 0;
#endif
  if( n<0 ) n = 0;  /* IMP: R-26507-47431 */
  return sqlitexRealloc(pOld, n);
}
void *sqlitex_realloc64(void *pOld, sqlitex_uint64 n){
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlitex_initialize() ) return 0;
#endif
  return sqlitexRealloc(pOld, n);
}


/*
** Allocate and zero memory.
*/ 
void *sqlitexMallocZero(u64 n){
  void *p = sqlitexMalloc(n);
  if( p ){
    memset(p, 0, (size_t)n);
  }
  return p;
}

/*
** Allocate and zero memory.  If the allocation fails, make
** the mallocFailed flag in the connection pointer.
*/
void *sqlitexDbMallocZero(sqlitex *db, u64 n){
  void *p = sqlitexDbMallocRaw(db, n);
  if( p ){
    memset(p, 0, (size_t)n);
  }
  return p;
}

/*
** Allocate memory.  If the allocation fails, make
** the mallocFailed flag in the connection pointer.
**
** If db!=0 and db->mallocFailed is true (indicating a prior malloc
** failure on the same database connection) then always return 0.
** Hence for a particular database connection, once malloc starts
** failing, it fails consistently until mallocFailed is reset.
** This is an important assumption.  There are many places in the
** code that do things like this:
**
**         int *a = (int*)sqlitexDbMallocRaw(db, 100);
**         int *b = (int*)sqlitexDbMallocRaw(db, 200);
**         if( b ) a[10] = 9;
**
** In other words, if a subsequent malloc (ex: "b") worked, it is assumed
** that all prior mallocs (ex: "a") worked too.
*/
void *sqlitexDbMallocRaw(sqlitex *db, u64 n){
  void *p;
  assert( db==0 || sqlitex_mutex_held(db->mutex) );
  assert( db==0 || db->pnBytesFreed==0 );
#ifndef SQLITE_OMIT_LOOKASIDE
  if( db ){
    LookasideSlot *pBuf;
    if( db->mallocFailed ){
      return 0;
    }
    if( db->lookaside.bEnabled ){
      if( n>db->lookaside.sz ){
        db->lookaside.anStat[1]++;
      }else if( (pBuf = db->lookaside.pFree)==0 ){
        db->lookaside.anStat[2]++;
      }else{
        db->lookaside.pFree = pBuf->pNext;
        db->lookaside.nOut++;
        db->lookaside.anStat[0]++;
        if( db->lookaside.nOut>db->lookaside.mxOut ){
          db->lookaside.mxOut = db->lookaside.nOut;
        }
        return (void*)pBuf;
      }
    }
  }
#else
  if( db && db->mallocFailed ){
    return 0;
  }
#endif
  p = sqlitexMalloc(n);
  if( !p && db ){
    db->mallocFailed = 1;
  }
  sqlitexMemdebugSetType(p, 
         (db && db->lookaside.bEnabled) ? MEMTYPE_LOOKASIDE : MEMTYPE_HEAP);
  return p;
}

/*
** Resize the block of memory pointed to by p to n bytes. If the
** resize fails, set the mallocFailed flag in the connection object.
*/
void *sqlitexDbRealloc(sqlitex *db, void *p, u64 n){
  void *pNew = 0;
  assert( db!=0 );
  assert( sqlitex_mutex_held(db->mutex) );
  if( db->mallocFailed==0 ){
    if( p==0 ){
      return sqlitexDbMallocRaw(db, n);
    }
    if( isLookaside(db, p) ){
      if( n<=db->lookaside.sz ){
        return p;
      }
      pNew = sqlitexDbMallocRaw(db, n);
      if( pNew ){
        memcpy(pNew, p, db->lookaside.sz);
        sqlitexDbFree(db, p);
      }
    }else{
      assert( sqlitexMemdebugHasType(p, (MEMTYPE_LOOKASIDE|MEMTYPE_HEAP)) );
      assert( sqlitexMemdebugNoType(p, (u8)~(MEMTYPE_LOOKASIDE|MEMTYPE_HEAP)) );
      sqlitexMemdebugSetType(p, MEMTYPE_HEAP);
      pNew = sqlitex_realloc64(p, n);
      if( !pNew ){
        db->mallocFailed = 1;
      }
      sqlitexMemdebugSetType(pNew,
            (db->lookaside.bEnabled ? MEMTYPE_LOOKASIDE : MEMTYPE_HEAP));
    }
  }
  return pNew;
}

/*
** Attempt to reallocate p.  If the reallocation fails, then free p
** and set the mallocFailed flag in the database connection.
*/
void *sqlitexDbReallocOrFree(sqlitex *db, void *p, u64 n){
  void *pNew;
  pNew = sqlitexDbRealloc(db, p, n);
  if( !pNew ){
    sqlitexDbFree(db, p);
  }
  return pNew;
}

/*
** Make a copy of a string in memory obtained from sqliteMalloc(). These 
** functions call sqlitexMallocRaw() directly instead of sqliteMalloc(). This
** is because when memory debugging is turned on, these two functions are 
** called via macros that record the current file and line number in the
** ThreadData structure.
*/
char *sqlitexDbStrDup(sqlitex *db, const char *z){
  char *zNew;
  size_t n;
  if( z==0 ){
    return 0;
  }
  n = sqlitexStrlen30(z) + 1;
  assert( (n&0x7fffffff)==n );
  zNew = sqlitexDbMallocRaw(db, (int)n);
  if( zNew ){
    memcpy(zNew, z, n);
  }
  return zNew;
}
char *sqlitexDbStrNDup(sqlitex *db, const char *z, u64 n){
  char *zNew;
  if( z==0 ){
    return 0;
  }
  assert( (n&0x7fffffff)==n );
  zNew = sqlitexDbMallocRaw(db, n+1);
  if( zNew ){
    memcpy(zNew, z, (size_t)n);
    zNew[n] = 0;
  }
  return zNew;
}

/*
** Create a string from the zFromat argument and the va_list that follows.
** Store the string in memory obtained from sqliteMalloc() and make *pz
** point to that string.
*/
void sqlitexSetString(char **pz, sqlitex *db, const char *zFormat, ...){
  va_list ap;
  char *z;

  va_start(ap, zFormat);
  z = sqlitexVMPrintf(db, zFormat, ap);
  va_end(ap);
  sqlitexDbFree(db, *pz);
  *pz = z;
}

/*
** Take actions at the end of an API call to indicate an OOM error
*/
static SQLITE_NOINLINE int apiOomError(sqlitex *db){
  db->mallocFailed = 0;
  sqlitexError(db, SQLITE_NOMEM);
  return SQLITE_NOMEM;
}

/*
** This function must be called before exiting any API function (i.e. 
** returning control to the user) that has called sqlitex_malloc or
** sqlitex_realloc.
**
** The returned value is normally a copy of the second argument to this
** function. However, if a malloc() failure has occurred since the previous
** invocation SQLITE_NOMEM is returned instead. 
**
** If the first argument, db, is not NULL and a malloc() error has occurred,
** then the connection error-code (the value returned by sqlitex_errcode())
** is set to SQLITE_NOMEM.
*/
int sqlitexApiExit(sqlitex* db, int rc){
  /* If the db handle is not NULL, then we must hold the connection handle
  ** mutex here. Otherwise the read (and possible write) of db->mallocFailed 
  ** is unsafe, as is the call to sqlitexError().
  */
  assert( !db || sqlitex_mutex_held(db->mutex) );
  if( db==0 ) return rc & 0xff;
  if( db->mallocFailed || rc==SQLITE_IOERR_NOMEM ){
    return apiOomError(db);
  }
  return rc & db->errMask;
}
