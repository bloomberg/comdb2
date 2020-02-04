/*
** 2008 November 05
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
** This file implements the default page cache implementation (the
** sqlitex_pcache interface). It also contains part of the implementation
** of the SQLITE_CONFIG_PAGECACHE and sqlitex_release_memory() features.
** If the default page cache implementation is overridden, then neither of
** these two features are available.
*/

#include "sqliteInt.h"

typedef struct PCache1 PCache1;
typedef struct PgHdr1 PgHdr1;
typedef struct PgFreeslot PgFreeslot;
typedef struct PGroup PGroup;

/* Each page cache (or PCache) belongs to a PGroup.  A PGroup is a set 
** of one or more PCaches that are able to recycle each other's unpinned
** pages when they are under memory pressure.  A PGroup is an instance of
** the following object.
**
** This page cache implementation works in one of two modes:
**
**   (1)  Every PCache is the sole member of its own PGroup.  There is
**        one PGroup per PCache.
**
**   (2)  There is a single global PGroup that all PCaches are a member
**        of.
**
** Mode 1 uses more memory (since PCache instances are not able to rob
** unused pages from other PCaches) but it also operates without a mutex,
** and is therefore often faster.  Mode 2 requires a mutex in order to be
** threadsafe, but recycles pages more efficiently.
**
** For mode (1), PGroup.mutex is NULL.  For mode (2) there is only a single
** PGroup which is the pcache1.grp global variable and its mutex is
** SQLITE_MUTEX_STATIC_LRU.
*/
struct PGroup {
  sqlitex_mutex *mutex;          /* MUTEX_STATIC_LRU or NULL */
  unsigned int nMaxPage;         /* Sum of nMax for purgeable caches */
  unsigned int nMinPage;         /* Sum of nMin for purgeable caches */
  unsigned int mxPinned;         /* nMaxpage + 10 - nMinPage */
  unsigned int nCurrentPage;     /* Number of purgeable pages allocated */
  PgHdr1 *pLruHead, *pLruTail;   /* LRU list of unpinned pages */
};

/* Each page cache is an instance of the following object.  Every
** open database file (including each in-memory database and each
** temporary or transient database) has a single page cache which
** is an instance of this object.
**
** Pointers to structures of this type are cast and returned as 
** opaque sqlitex_pcache* handles.
*/
struct PCache1 {
  /* Cache configuration parameters. Page size (szPage) and the purgeable
  ** flag (bPurgeable) are set when the cache is created. nMax may be 
  ** modified at any time by a call to the pcache1Cachesize() method.
  ** The PGroup mutex must be held when accessing nMax.
  */
  PGroup *pGroup;                     /* PGroup this cache belongs to */
  int szPage;                         /* Size of allocated pages in bytes */
  int szExtra;                        /* Size of extra space in bytes */
  int bPurgeable;                     /* True if cache is purgeable */
  unsigned int nMin;                  /* Minimum number of pages reserved */
  unsigned int nMax;                  /* Configured "cache_size" value */
  unsigned int n90pct;                /* nMax*9/10 */
  unsigned int iMaxKey;               /* Largest key seen since xTruncate() */

  /* Hash table of all pages. The following variables may only be accessed
  ** when the accessor is holding the PGroup mutex.
  */
  unsigned int nRecyclable;           /* Number of pages in the LRU list */
  unsigned int nPage;                 /* Total number of pages in apHash */
  unsigned int nHash;                 /* Number of slots in apHash[] */
  PgHdr1 **apHash;                    /* Hash table for fast lookup by key */
};

/*
** Each cache entry is represented by an instance of the following 
** structure. Unless SQLITE_PCACHE_SEPARATE_HEADER is defined, a buffer of
** PgHdr1.pCache->szPage bytes is allocated directly before this structure 
** in memory.
*/
struct PgHdr1 {
  sqlitex_pcache_page page;
  unsigned int iKey;             /* Key value (page number) */
  u8 isPinned;                   /* Page in use, not on the LRU list */
  PgHdr1 *pNext;                 /* Next in hash table chain */
  PCache1 *pCache;               /* Cache that currently owns this page */
  PgHdr1 *pLruNext;              /* Next in LRU list of unpinned pages */
  PgHdr1 *pLruPrev;              /* Previous in LRU list of unpinned pages */
};

/*
** Free slots in the allocator used to divide up the buffer provided using
** the SQLITE_CONFIG_PAGECACHE mechanism.
*/
struct PgFreeslot {
  PgFreeslot *pNext;  /* Next free slot */
};

/*
** Global data used by this cache.
*/
static SQLITE_WSD struct PCacheGlobal {
  PGroup grp;                    /* The global PGroup for mode (2) */

  /* Variables related to SQLITE_CONFIG_PAGECACHE settings.  The
  ** szSlot, nSlot, pStart, pEnd, nReserve, and isInit values are all
  ** fixed at sqlitex_initialize() time and do not require mutex protection.
  ** The nFreeSlot and pFree values do require mutex protection.
  */
  int isInit;                    /* True if initialized */
  int szSlot;                    /* Size of each free slot */
  int nSlot;                     /* The number of pcache slots */
  int nReserve;                  /* Try to keep nFreeSlot above this */
  void *pStart, *pEnd;           /* Bounds of pagecache malloc range */
  /* Above requires no mutex.  Use mutex below for variable that follow. */
  sqlitex_mutex *mutex;          /* Mutex for accessing the following: */
  PgFreeslot *pFree;             /* Free page blocks */
  int nFreeSlot;                 /* Number of unused pcache slots */
  /* The following value requires a mutex to change.  We skip the mutex on
  ** reading because (1) most platforms read a 32-bit integer atomically and
  ** (2) even if an incorrect value is read, no great harm is done since this
  ** is really just an optimization. */
  int bUnderPressure;            /* True if low on PAGECACHE memory */
} pcache1_g;

/*
** All code in this file should access the global structure above via the
** alias "pcache1". This ensures that the WSD emulation is used when
** compiling for systems that do not support real WSD.
*/
#define pcache1 (GLOBAL(struct PCacheGlobal, pcache1_g))

/*
** Macros to enter and leave the PCache LRU mutex.
*/
#define pcache1EnterMutex(X) sqlitex_mutex_enter((X)->mutex)
#define pcache1LeaveMutex(X) sqlitex_mutex_leave((X)->mutex)

/******************************************************************************/
/******** Page Allocation/SQLITE_CONFIG_PCACHE Related Functions **************/

/*
** This function is called during initialization if a static buffer is 
** supplied to use for the page-cache by passing the SQLITE_CONFIG_PAGECACHE
** verb to sqlitex_config(). Parameter pBuf points to an allocation large
** enough to contain 'n' buffers of 'sz' bytes each.
**
** This routine is called from sqlitex_initialize() and so it is guaranteed
** to be serialized already.  There is no need for further mutexing.
*/
void sqlitexPCacheBufferSetup(void *pBuf, int sz, int n){
  if( pcache1.isInit ){
    PgFreeslot *p;
    sz = ROUNDDOWN8(sz);
    pcache1.szSlot = sz;
    pcache1.nSlot = pcache1.nFreeSlot = n;
    pcache1.nReserve = n>90 ? 10 : (n/10 + 1);
    pcache1.pStart = pBuf;
    pcache1.pFree = 0;
    pcache1.bUnderPressure = 0;
    while( n-- ){
      p = (PgFreeslot*)pBuf;
      p->pNext = pcache1.pFree;
      pcache1.pFree = p;
      pBuf = (void*)&((char*)pBuf)[sz];
    }
    pcache1.pEnd = pBuf;
  }
}

/*
** Malloc function used within this file to allocate space from the buffer
** configured using sqlitex_config(SQLITE_CONFIG_PAGECACHE) option. If no 
** such buffer exists or there is no space left in it, this function falls 
** back to sqlitexMalloc().
**
** Multiple threads can run this routine at the same time.  Global variables
** in pcache1 need to be protected via mutex.
*/
static void *pcache1Alloc(int nByte){
  void *p = 0;
  assert( sqlitex_mutex_notheld(pcache1.grp.mutex) );
  sqlitexStatusSet(SQLITE_STATUS_PAGECACHE_SIZE, nByte);
  if( nByte<=pcache1.szSlot ){
    sqlitex_mutex_enter(pcache1.mutex);
    p = (PgHdr1 *)pcache1.pFree;
    if( p ){
      pcache1.pFree = pcache1.pFree->pNext;
      pcache1.nFreeSlot--;
      pcache1.bUnderPressure = pcache1.nFreeSlot<pcache1.nReserve;
      assert( pcache1.nFreeSlot>=0 );
      sqlitexStatusAdd(SQLITE_STATUS_PAGECACHE_USED, 1);
    }
    sqlitex_mutex_leave(pcache1.mutex);
  }
  if( p==0 ){
    /* Memory is not available in the SQLITE_CONFIG_PAGECACHE pool.  Get
    ** it from sqlitexMalloc instead.
    */
    p = sqlitexMalloc(nByte);
#ifndef SQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS
    if( p ){
      int sz = sqlitexMallocSize(p);
      sqlitex_mutex_enter(pcache1.mutex);
      sqlitexStatusAdd(SQLITE_STATUS_PAGECACHE_OVERFLOW, sz);
      sqlitex_mutex_leave(pcache1.mutex);
    }
#endif
    sqlitexMemdebugSetType(p, MEMTYPE_PCACHE);
  }
  return p;
}

/*
** Free an allocated buffer obtained from pcache1Alloc().
*/
static int pcache1Free(void *p){
  int nFreed = 0;
  if( p==0 ) return 0;
  if( p>=pcache1.pStart && p<pcache1.pEnd ){
    PgFreeslot *pSlot;
    sqlitex_mutex_enter(pcache1.mutex);
    sqlitexStatusAdd(SQLITE_STATUS_PAGECACHE_USED, -1);
    pSlot = (PgFreeslot*)p;
    pSlot->pNext = pcache1.pFree;
    pcache1.pFree = pSlot;
    pcache1.nFreeSlot++;
    pcache1.bUnderPressure = pcache1.nFreeSlot<pcache1.nReserve;
    assert( pcache1.nFreeSlot<=pcache1.nSlot );
    sqlitex_mutex_leave(pcache1.mutex);
  }else{
    assert( sqlitexMemdebugHasType(p, MEMTYPE_PCACHE) );
    sqlitexMemdebugSetType(p, MEMTYPE_HEAP);
    nFreed = sqlitexMallocSize(p);
#ifndef SQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS
    sqlitex_mutex_enter(pcache1.mutex);
    sqlitexStatusAdd(SQLITE_STATUS_PAGECACHE_OVERFLOW, -nFreed);
    sqlitex_mutex_leave(pcache1.mutex);
#endif
    sqlitex_free(p);
  }
  return nFreed;
}

#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
/*
** Return the size of a pcache allocation
*/
static int pcache1MemSize(void *p){
  if( p>=pcache1.pStart && p<pcache1.pEnd ){
    return pcache1.szSlot;
  }else{
    int iSize;
    assert( sqlitexMemdebugHasType(p, MEMTYPE_PCACHE) );
    sqlitexMemdebugSetType(p, MEMTYPE_HEAP);
    iSize = sqlitexMallocSize(p);
    sqlitexMemdebugSetType(p, MEMTYPE_PCACHE);
    return iSize;
  }
}
#endif /* SQLITE_ENABLE_MEMORY_MANAGEMENT */

/*
** Allocate a new page object initially associated with cache pCache.
*/
static PgHdr1 *pcache1AllocPage(PCache1 *pCache){
  PgHdr1 *p = 0;
  void *pPg;

  /* The group mutex must be released before pcache1Alloc() is called. This
  ** is because it may call sqlitex_release_memory(), which assumes that 
  ** this mutex is not held. */
  assert( sqlitex_mutex_held(pCache->pGroup->mutex) );
  pcache1LeaveMutex(pCache->pGroup);
#ifdef SQLITE_PCACHE_SEPARATE_HEADER
  pPg = pcache1Alloc(pCache->szPage);
  p = sqlitexMalloc(sizeof(PgHdr1) + pCache->szExtra);
  if( !pPg || !p ){
    pcache1Free(pPg);
    sqlitex_free(p);
    pPg = 0;
  }
#else
  pPg = pcache1Alloc(ROUND8(sizeof(PgHdr1)) + pCache->szPage + pCache->szExtra);
  p = (PgHdr1 *)&((u8 *)pPg)[pCache->szPage];
#endif
  pcache1EnterMutex(pCache->pGroup);

  if( pPg ){
    p->page.pBuf = pPg;
    p->page.pExtra = &p[1];
    if( pCache->bPurgeable ){
      pCache->pGroup->nCurrentPage++;
    }
    return p;
  }
  return 0;
}

/*
** Free a page object allocated by pcache1AllocPage().
**
** The pointer is allowed to be NULL, which is prudent.  But it turns out
** that the current implementation happens to never call this routine
** with a NULL pointer, so we mark the NULL test with ALWAYS().
*/
static void pcache1FreePage(PgHdr1 *p){
  if( ALWAYS(p) ){
    PCache1 *pCache = p->pCache;
    assert( sqlitex_mutex_held(p->pCache->pGroup->mutex) );
    pcache1Free(p->page.pBuf);
#ifdef SQLITE_PCACHE_SEPARATE_HEADER
    sqlitex_free(p);
#endif
    if( pCache->bPurgeable ){
      pCache->pGroup->nCurrentPage--;
    }
  }
}

/*
** Malloc function used by SQLite to obtain space from the buffer configured
** using sqlitex_config(SQLITE_CONFIG_PAGECACHE) option. If no such buffer
** exists, this function falls back to sqlitexMalloc().
*/
void *sqlitexPageMalloc(int sz){
  return pcache1Alloc(sz);
}

/*
** Free an allocated buffer obtained from sqlitexPageMalloc().
*/
void sqlitexPageFree(void *p){
  pcache1Free(p);
}


/*
** Return true if it desirable to avoid allocating a new page cache
** entry.
**
** If memory was allocated specifically to the page cache using
** SQLITE_CONFIG_PAGECACHE but that memory has all been used, then
** it is desirable to avoid allocating a new page cache entry because
** presumably SQLITE_CONFIG_PAGECACHE was suppose to be sufficient
** for all page cache needs and we should not need to spill the
** allocation onto the heap.
**
** Or, the heap is used for all page cache memory but the heap is
** under memory pressure, then again it is desirable to avoid
** allocating a new page cache entry in order to avoid stressing
** the heap even further.
*/
static int pcache1UnderMemoryPressure(PCache1 *pCache){
  if( pcache1.nSlot && (pCache->szPage+pCache->szExtra)<=pcache1.szSlot ){
    return pcache1.bUnderPressure;
  }else{
    return sqlitexHeapNearlyFull();
  }
}

/******************************************************************************/
/******** General Implementation Functions ************************************/

/*
** This function is used to resize the hash table used by the cache passed
** as the first argument.
**
** The PCache mutex must be held when this function is called.
*/
static void pcache1ResizeHash(PCache1 *p){
  PgHdr1 **apNew;
  unsigned int nNew;
  unsigned int i;

  assert( sqlitex_mutex_held(p->pGroup->mutex) );

  nNew = p->nHash*2;
  if( nNew<256 ){
    nNew = 256;
  }

  pcache1LeaveMutex(p->pGroup);
  if( p->nHash ){ sqlitexBeginBenignMalloc(); }
  apNew = (PgHdr1 **)sqlitexMallocZero(sizeof(PgHdr1 *)*nNew);
  if( p->nHash ){ sqlitexEndBenignMalloc(); }
  pcache1EnterMutex(p->pGroup);
  if( apNew ){
    for(i=0; i<p->nHash; i++){
      PgHdr1 *pPage;
      PgHdr1 *pNext = p->apHash[i];
      while( (pPage = pNext)!=0 ){
        unsigned int h = pPage->iKey % nNew;
        pNext = pPage->pNext;
        pPage->pNext = apNew[h];
        apNew[h] = pPage;
      }
    }
    sqlitex_free(p->apHash);
    p->apHash = apNew;
    p->nHash = nNew;
  }
}

/*
** This function is used internally to remove the page pPage from the 
** PGroup LRU list, if is part of it. If pPage is not part of the PGroup
** LRU list, then this function is a no-op.
**
** The PGroup mutex must be held when this function is called.
*/
static void pcache1PinPage(PgHdr1 *pPage){
  PCache1 *pCache;
  PGroup *pGroup;

  assert( pPage!=0 );
  assert( pPage->isPinned==0 );
  pCache = pPage->pCache;
  pGroup = pCache->pGroup;
  assert( pPage->pLruNext || pPage==pGroup->pLruTail );
  assert( pPage->pLruPrev || pPage==pGroup->pLruHead );
  assert( sqlitex_mutex_held(pGroup->mutex) );
  if( pPage->pLruPrev ){
    pPage->pLruPrev->pLruNext = pPage->pLruNext;
  }else{
    pGroup->pLruHead = pPage->pLruNext;
  }
  if( pPage->pLruNext ){
    pPage->pLruNext->pLruPrev = pPage->pLruPrev;
  }else{
    pGroup->pLruTail = pPage->pLruPrev;
  }
  pPage->pLruNext = 0;
  pPage->pLruPrev = 0;
  pPage->isPinned = 1;
  pCache->nRecyclable--;
}


/*
** Remove the page supplied as an argument from the hash table 
** (PCache1.apHash structure) that it is currently stored in.
**
** The PGroup mutex must be held when this function is called.
*/
static void pcache1RemoveFromHash(PgHdr1 *pPage){
  unsigned int h;
  PCache1 *pCache = pPage->pCache;
  PgHdr1 **pp;

  assert( sqlitex_mutex_held(pCache->pGroup->mutex) );
  h = pPage->iKey % pCache->nHash;
  for(pp=&pCache->apHash[h]; (*pp)!=pPage; pp=&(*pp)->pNext);
  *pp = (*pp)->pNext;

  pCache->nPage--;
}

/*
** If there are currently more than nMaxPage pages allocated, try
** to recycle pages to reduce the number allocated to nMaxPage.
*/
static void pcache1EnforceMaxPage(PGroup *pGroup){
  assert( sqlitex_mutex_held(pGroup->mutex) );
  while( pGroup->nCurrentPage>pGroup->nMaxPage && pGroup->pLruTail ){
    PgHdr1 *p = pGroup->pLruTail;
    assert( p->pCache->pGroup==pGroup );
    assert( p->isPinned==0 );
    pcache1PinPage(p);
    pcache1RemoveFromHash(p);
    pcache1FreePage(p);
  }
}

/*
** Discard all pages from cache pCache with a page number (key value) 
** greater than or equal to iLimit. Any pinned pages that meet this 
** criteria are unpinned before they are discarded.
**
** The PCache mutex must be held when this function is called.
*/
static void pcache1TruncateUnsafe(
  PCache1 *pCache,             /* The cache to truncate */
  unsigned int iLimit          /* Drop pages with this pgno or larger */
){
  TESTONLY( unsigned int nPage = 0; )  /* To assert pCache->nPage is correct */
  unsigned int h;
  assert( sqlitex_mutex_held(pCache->pGroup->mutex) );
  for(h=0; h<pCache->nHash; h++){
    PgHdr1 **pp = &pCache->apHash[h]; 
    PgHdr1 *pPage;
    while( (pPage = *pp)!=0 ){
      if( pPage->iKey>=iLimit ){
        pCache->nPage--;
        *pp = pPage->pNext;
        if( !pPage->isPinned ) pcache1PinPage(pPage);
        pcache1FreePage(pPage);
      }else{
        pp = &pPage->pNext;
        TESTONLY( nPage++; )
      }
    }
  }
  assert( pCache->nPage==nPage );
}

/******************************************************************************/
/******** sqlitex_pcache Methods **********************************************/

/*
** Implementation of the sqlitex_pcache.xInit method.
*/
static int pcache1Init(void *NotUsed){
  UNUSED_PARAMETER(NotUsed);
  assert( pcache1.isInit==0 );
  memset(&pcache1, 0, sizeof(pcache1));
  if( sqlitexGlobalConfig.bCoreMutex ){
    pcache1.grp.mutex = sqlitex_mutex_alloc(SQLITE_MUTEX_STATIC_LRU);
    pcache1.mutex = sqlitex_mutex_alloc(SQLITE_MUTEX_STATIC_PMEM);
  }
  pcache1.grp.mxPinned = 10;
  pcache1.isInit = 1;
  return SQLITE_OK;
}

/*
** Implementation of the sqlitex_pcache.xShutdown method.
** Note that the static mutex allocated in xInit does 
** not need to be freed.
*/
static void pcache1Shutdown(void *NotUsed){
  UNUSED_PARAMETER(NotUsed);
  assert( pcache1.isInit!=0 );
  memset(&pcache1, 0, sizeof(pcache1));
}

/* forward declaration */
static void pcache1Destroy(sqlitex_pcache *p);

/*
** Implementation of the sqlitex_pcache.xCreate method.
**
** Allocate a new cache.
*/
static sqlitex_pcache *pcache1Create(int szPage, int szExtra, int bPurgeable){
  PCache1 *pCache;      /* The newly created page cache */
  PGroup *pGroup;       /* The group the new page cache will belong to */
  int sz;               /* Bytes of memory required to allocate the new cache */

  /*
  ** The separateCache variable is true if each PCache has its own private
  ** PGroup.  In other words, separateCache is true for mode (1) where no
  ** mutexing is required.
  **
  **   *  Always use a unified cache (mode-2) if ENABLE_MEMORY_MANAGEMENT
  **
  **   *  Always use a unified cache in single-threaded applications
  **
  **   *  Otherwise (if multi-threaded and ENABLE_MEMORY_MANAGEMENT is off)
  **      use separate caches (mode-1)
  */
#if defined(SQLITE_ENABLE_MEMORY_MANAGEMENT) || SQLITE_THREADSAFE==0
  const int separateCache = 0;
#else
  int separateCache = sqlitexGlobalConfig.bCoreMutex>0;
#endif

  assert( (szPage & (szPage-1))==0 && szPage>=512 && szPage<=65536 );
  assert( szExtra < 300 );

  sz = sizeof(PCache1) + sizeof(PGroup)*separateCache;
  pCache = (PCache1 *)sqlitexMallocZero(sz);
  if( pCache ){
    if( separateCache ){
      pGroup = (PGroup*)&pCache[1];
      pGroup->mxPinned = 10;
    }else{
      pGroup = &pcache1.grp;
    }
    pCache->pGroup = pGroup;
    pCache->szPage = szPage;
    pCache->szExtra = szExtra;
    pCache->bPurgeable = (bPurgeable ? 1 : 0);
    pcache1EnterMutex(pGroup);
    pcache1ResizeHash(pCache);
    if( bPurgeable ){
      pCache->nMin = 10;
      pGroup->nMinPage += pCache->nMin;
      pGroup->mxPinned = pGroup->nMaxPage + 10 - pGroup->nMinPage;
    }
    pcache1LeaveMutex(pGroup);
    if( pCache->nHash==0 ){
      pcache1Destroy((sqlitex_pcache*)pCache);
      pCache = 0;
    }
  }
  return (sqlitex_pcache *)pCache;
}

/*
** Implementation of the sqlitex_pcache.xCachesize method. 
**
** Configure the cache_size limit for a cache.
*/
static void pcache1Cachesize(sqlitex_pcache *p, int nMax){
  PCache1 *pCache = (PCache1 *)p;
  if( pCache->bPurgeable ){
    PGroup *pGroup = pCache->pGroup;
    pcache1EnterMutex(pGroup);
    pGroup->nMaxPage += (nMax - pCache->nMax);
    pGroup->mxPinned = pGroup->nMaxPage + 10 - pGroup->nMinPage;
    pCache->nMax = nMax;
    pCache->n90pct = pCache->nMax*9/10;
    pcache1EnforceMaxPage(pGroup);
    pcache1LeaveMutex(pGroup);
  }
}

/*
** Implementation of the sqlitex_pcache.xShrink method. 
**
** Free up as much memory as possible.
*/
static void pcache1Shrink(sqlitex_pcache *p){
  PCache1 *pCache = (PCache1*)p;
  if( pCache->bPurgeable ){
    PGroup *pGroup = pCache->pGroup;
    int savedMaxPage;
    pcache1EnterMutex(pGroup);
    savedMaxPage = pGroup->nMaxPage;
    pGroup->nMaxPage = 0;
    pcache1EnforceMaxPage(pGroup);
    pGroup->nMaxPage = savedMaxPage;
    pcache1LeaveMutex(pGroup);
  }
}

/*
** Implementation of the sqlitex_pcache.xPagecount method. 
*/
static int pcache1Pagecount(sqlitex_pcache *p){
  int n;
  PCache1 *pCache = (PCache1*)p;
  pcache1EnterMutex(pCache->pGroup);
  n = pCache->nPage;
  pcache1LeaveMutex(pCache->pGroup);
  return n;
}


/*
** Implement steps 3, 4, and 5 of the pcache1Fetch() algorithm described
** in the header of the pcache1Fetch() procedure.
**
** This steps are broken out into a separate procedure because they are
** usually not needed, and by avoiding the stack initialization required
** for these steps, the main pcache1Fetch() procedure can run faster.
*/
static SQLITE_NOINLINE PgHdr1 *pcache1FetchStage2(
  PCache1 *pCache, 
  unsigned int iKey, 
  int createFlag
){
  unsigned int nPinned;
  PGroup *pGroup = pCache->pGroup;
  PgHdr1 *pPage = 0;

  /* Step 3: Abort if createFlag is 1 but the cache is nearly full */
  assert( pCache->nPage >= pCache->nRecyclable );
  nPinned = pCache->nPage - pCache->nRecyclable;
  assert( pGroup->mxPinned == pGroup->nMaxPage + 10 - pGroup->nMinPage );
  assert( pCache->n90pct == pCache->nMax*9/10 );
  if( createFlag==1 && (
        nPinned>=pGroup->mxPinned
     || nPinned>=pCache->n90pct
     || (pcache1UnderMemoryPressure(pCache) && pCache->nRecyclable<nPinned)
  )){
    return 0;
  }

  if( pCache->nPage>=pCache->nHash ) pcache1ResizeHash(pCache);
  assert( pCache->nHash>0 && pCache->apHash );

  /* Step 4. Try to recycle a page. */
  if( pCache->bPurgeable && pGroup->pLruTail && (
         (pCache->nPage+1>=pCache->nMax)
      || pGroup->nCurrentPage>=pGroup->nMaxPage
      || pcache1UnderMemoryPressure(pCache)
  )){
    PCache1 *pOther;
    pPage = pGroup->pLruTail;
    assert( pPage->isPinned==0 );
    pcache1RemoveFromHash(pPage);
    pcache1PinPage(pPage);
    pOther = pPage->pCache;

    /* We want to verify that szPage and szExtra are the same for pOther
    ** and pCache.  Assert that we can verify this by comparing sums. */
    assert( (pCache->szPage & (pCache->szPage-1))==0 && pCache->szPage>=512 );
    assert( pCache->szExtra<512 );
    assert( (pOther->szPage & (pOther->szPage-1))==0 && pOther->szPage>=512 );
    assert( pOther->szExtra<512 );

    if( pOther->szPage+pOther->szExtra != pCache->szPage+pCache->szExtra ){
      pcache1FreePage(pPage);
      pPage = 0;
    }else{
      pGroup->nCurrentPage -= (pOther->bPurgeable - pCache->bPurgeable);
    }
  }

  /* Step 5. If a usable page buffer has still not been found, 
  ** attempt to allocate a new one. 
  */
  if( !pPage ){
    if( createFlag==1 ) sqlitexBeginBenignMalloc();
    pPage = pcache1AllocPage(pCache);
    if( createFlag==1 ) sqlitexEndBenignMalloc();
  }

  if( pPage ){
    unsigned int h = iKey % pCache->nHash;
    pCache->nPage++;
    pPage->iKey = iKey;
    pPage->pNext = pCache->apHash[h];
    pPage->pCache = pCache;
    pPage->pLruPrev = 0;
    pPage->pLruNext = 0;
    pPage->isPinned = 1;
    *(void **)pPage->page.pExtra = 0;
    pCache->apHash[h] = pPage;
    if( iKey>pCache->iMaxKey ){
      pCache->iMaxKey = iKey;
    }
  }
  return pPage;
}

/*
** Implementation of the sqlitex_pcache.xFetch method. 
**
** Fetch a page by key value.
**
** Whether or not a new page may be allocated by this function depends on
** the value of the createFlag argument.  0 means do not allocate a new
** page.  1 means allocate a new page if space is easily available.  2 
** means to try really hard to allocate a new page.
**
** For a non-purgeable cache (a cache used as the storage for an in-memory
** database) there is really no difference between createFlag 1 and 2.  So
** the calling function (pcache.c) will never have a createFlag of 1 on
** a non-purgeable cache.
**
** There are three different approaches to obtaining space for a page,
** depending on the value of parameter createFlag (which may be 0, 1 or 2).
**
**   1. Regardless of the value of createFlag, the cache is searched for a 
**      copy of the requested page. If one is found, it is returned.
**
**   2. If createFlag==0 and the page is not already in the cache, NULL is
**      returned.
**
**   3. If createFlag is 1, and the page is not already in the cache, then
**      return NULL (do not allocate a new page) if any of the following
**      conditions are true:
**
**       (a) the number of pages pinned by the cache is greater than
**           PCache1.nMax, or
**
**       (b) the number of pages pinned by the cache is greater than
**           the sum of nMax for all purgeable caches, less the sum of 
**           nMin for all other purgeable caches, or
**
**   4. If none of the first three conditions apply and the cache is marked
**      as purgeable, and if one of the following is true:
**
**       (a) The number of pages allocated for the cache is already 
**           PCache1.nMax, or
**
**       (b) The number of pages allocated for all purgeable caches is
**           already equal to or greater than the sum of nMax for all
**           purgeable caches,
**
**       (c) The system is under memory pressure and wants to avoid
**           unnecessary pages cache entry allocations
**
**      then attempt to recycle a page from the LRU list. If it is the right
**      size, return the recycled buffer. Otherwise, free the buffer and
**      proceed to step 5. 
**
**   5. Otherwise, allocate and return a new page buffer.
*/
static sqlitex_pcache_page *pcache1Fetch(
  sqlitex_pcache *p, 
  unsigned int iKey, 
  int createFlag
){
  PCache1 *pCache = (PCache1 *)p;
  PgHdr1 *pPage = 0;

  assert( offsetof(PgHdr1,page)==0 );
  assert( pCache->bPurgeable || createFlag!=1 );
  assert( pCache->bPurgeable || pCache->nMin==0 );
  assert( pCache->bPurgeable==0 || pCache->nMin==10 );
  assert( pCache->nMin==0 || pCache->bPurgeable );
  assert( pCache->nHash>0 );
  pcache1EnterMutex(pCache->pGroup);

  /* Step 1: Search the hash table for an existing entry. */
  pPage = pCache->apHash[iKey % pCache->nHash];
  while( pPage && pPage->iKey!=iKey ){ pPage = pPage->pNext; }

  /* Step 2: Abort if no existing page is found and createFlag is 0 */
  if( pPage ){
    if( !pPage->isPinned ) pcache1PinPage(pPage);
  }else if( createFlag ){
    /* Steps 3, 4, and 5 implemented by this subroutine */
    pPage = pcache1FetchStage2(pCache, iKey, createFlag);
  }
  assert( pPage==0 || pCache->iMaxKey>=iKey );
  pcache1LeaveMutex(pCache->pGroup);
  return (sqlitex_pcache_page*)pPage;
}


/*
** Implementation of the sqlitex_pcache.xUnpin method.
**
** Mark a page as unpinned (eligible for asynchronous recycling).
*/
static void pcache1Unpin(
  sqlitex_pcache *p, 
  sqlitex_pcache_page *pPg, 
  int reuseUnlikely
){
  PCache1 *pCache = (PCache1 *)p;
  PgHdr1 *pPage = (PgHdr1 *)pPg;
  PGroup *pGroup = pCache->pGroup;
 
  assert( pPage->pCache==pCache );
  pcache1EnterMutex(pGroup);

  /* It is an error to call this function if the page is already 
  ** part of the PGroup LRU list.
  */
  assert( pPage->pLruPrev==0 && pPage->pLruNext==0 );
  assert( pGroup->pLruHead!=pPage && pGroup->pLruTail!=pPage );
  assert( pPage->isPinned==1 );

  if( reuseUnlikely || pGroup->nCurrentPage>pGroup->nMaxPage ){
    pcache1RemoveFromHash(pPage);
    pcache1FreePage(pPage);
  }else{
    /* Add the page to the PGroup LRU list. */
    if( pGroup->pLruHead ){
      pGroup->pLruHead->pLruPrev = pPage;
      pPage->pLruNext = pGroup->pLruHead;
      pGroup->pLruHead = pPage;
    }else{
      pGroup->pLruTail = pPage;
      pGroup->pLruHead = pPage;
    }
    pCache->nRecyclable++;
    pPage->isPinned = 0;
  }

  pcache1LeaveMutex(pCache->pGroup);
}

/*
** Implementation of the sqlitex_pcache.xRekey method. 
*/
static void pcache1Rekey(
  sqlitex_pcache *p,
  sqlitex_pcache_page *pPg,
  unsigned int iOld,
  unsigned int iNew
){
  PCache1 *pCache = (PCache1 *)p;
  PgHdr1 *pPage = (PgHdr1 *)pPg;
  PgHdr1 **pp;
  unsigned int h; 
  assert( pPage->iKey==iOld );
  assert( pPage->pCache==pCache );

  pcache1EnterMutex(pCache->pGroup);

  h = iOld%pCache->nHash;
  pp = &pCache->apHash[h];
  while( (*pp)!=pPage ){
    pp = &(*pp)->pNext;
  }
  *pp = pPage->pNext;

  h = iNew%pCache->nHash;
  pPage->iKey = iNew;
  pPage->pNext = pCache->apHash[h];
  pCache->apHash[h] = pPage;
  if( iNew>pCache->iMaxKey ){
    pCache->iMaxKey = iNew;
  }

  pcache1LeaveMutex(pCache->pGroup);
}

/*
** Implementation of the sqlitex_pcache.xTruncate method. 
**
** Discard all unpinned pages in the cache with a page number equal to
** or greater than parameter iLimit. Any pinned pages with a page number
** equal to or greater than iLimit are implicitly unpinned.
*/
static void pcache1Truncate(sqlitex_pcache *p, unsigned int iLimit){
  PCache1 *pCache = (PCache1 *)p;
  pcache1EnterMutex(pCache->pGroup);
  if( iLimit<=pCache->iMaxKey ){
    pcache1TruncateUnsafe(pCache, iLimit);
    pCache->iMaxKey = iLimit-1;
  }
  pcache1LeaveMutex(pCache->pGroup);
}

/*
** Implementation of the sqlitex_pcache.xDestroy method. 
**
** Destroy a cache allocated using pcache1Create().
*/
static void pcache1Destroy(sqlitex_pcache *p){
  PCache1 *pCache = (PCache1 *)p;
  PGroup *pGroup = pCache->pGroup;
  assert( pCache->bPurgeable || (pCache->nMax==0 && pCache->nMin==0) );
  pcache1EnterMutex(pGroup);
  pcache1TruncateUnsafe(pCache, 0);
  assert( pGroup->nMaxPage >= pCache->nMax );
  pGroup->nMaxPage -= pCache->nMax;
  assert( pGroup->nMinPage >= pCache->nMin );
  pGroup->nMinPage -= pCache->nMin;
  pGroup->mxPinned = pGroup->nMaxPage + 10 - pGroup->nMinPage;
  pcache1EnforceMaxPage(pGroup);
  pcache1LeaveMutex(pGroup);
  sqlitex_free(pCache->apHash);
  sqlitex_free(pCache);
}

/*
** This function is called during initialization (sqlitex_initialize()) to
** install the default pluggable cache module, assuming the user has not
** already provided an alternative.
*/
void sqlitexPCacheSetDefault(void){
  static const sqlitex_pcache_methods2 defaultMethods = {
    1,                       /* iVersion */
    0,                       /* pArg */
    pcache1Init,             /* xInit */
    pcache1Shutdown,         /* xShutdown */
    pcache1Create,           /* xCreate */
    pcache1Cachesize,        /* xCachesize */
    pcache1Pagecount,        /* xPagecount */
    pcache1Fetch,            /* xFetch */
    pcache1Unpin,            /* xUnpin */
    pcache1Rekey,            /* xRekey */
    pcache1Truncate,         /* xTruncate */
    pcache1Destroy,          /* xDestroy */
    pcache1Shrink            /* xShrink */
  };
  sqlitex_config(SQLITE_CONFIG_PCACHE2, &defaultMethods);
}

/*
** Return the size of the header on each page of this PCACHE implementation.
*/
int sqlitexHeaderSizePcache1(void){ return ROUND8(sizeof(PgHdr1)); }

#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
/*
** This function is called to free superfluous dynamically allocated memory
** held by the pager system. Memory in use by any SQLite pager allocated
** by the current thread may be sqlitex_free()ed.
**
** nReq is the number of bytes of memory required. Once this much has
** been released, the function returns. The return value is the total number 
** of bytes of memory released.
*/
int sqlitexPcacheReleaseMemory(int nReq){
  int nFree = 0;
  assert( sqlitex_mutex_notheld(pcache1.grp.mutex) );
  assert( sqlitex_mutex_notheld(pcache1.mutex) );
  if( pcache1.pStart==0 ){
    PgHdr1 *p;
    pcache1EnterMutex(&pcache1.grp);
    while( (nReq<0 || nFree<nReq) && ((p=pcache1.grp.pLruTail)!=0) ){
      nFree += pcache1MemSize(p->page.pBuf);
#ifdef SQLITE_PCACHE_SEPARATE_HEADER
      nFree += sqlitexMemSize(p);
#endif
      assert( p->isPinned==0 );
      pcache1PinPage(p);
      pcache1RemoveFromHash(p);
      pcache1FreePage(p);
    }
    pcache1LeaveMutex(&pcache1.grp);
  }
  return nFree;
}
#endif /* SQLITE_ENABLE_MEMORY_MANAGEMENT */

#ifdef SQLITE_TEST
/*
** This function is used by test procedures to inspect the internal state
** of the global cache.
*/
void sqlitexPcacheStats(
  int *pnCurrent,      /* OUT: Total number of pages cached */
  int *pnMax,          /* OUT: Global maximum cache size */
  int *pnMin,          /* OUT: Sum of PCache1.nMin for purgeable caches */
  int *pnRecyclable    /* OUT: Total number of pages available for recycling */
){
  PgHdr1 *p;
  int nRecyclable = 0;
  for(p=pcache1.grp.pLruHead; p; p=p->pLruNext){
    assert( p->isPinned==0 );
    nRecyclable++;
  }
  *pnCurrent = pcache1.grp.nCurrentPage;
  *pnMax = (int)pcache1.grp.nMaxPage;
  *pnMin = (int)pcache1.grp.nMinPage;
  *pnRecyclable = nRecyclable;
}
#endif
