/*
** 2008 August 05
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file implements that page cache.
*/
#include "sqliteInt.h"

/*
** A complete page cache is an instance of this structure.
*/
struct PCache {
  PgHdr *pDirty, *pDirtyTail;         /* List of dirty pages in LRU order */
  PgHdr *pSynced;                     /* Last synced page in dirty page list */
  int nRef;                           /* Number of referenced pages */
  int szCache;                        /* Configured cache size */
  int szPage;                         /* Size of every page in this cache */
  int szExtra;                        /* Size of extra space for each page */
  u8 bPurgeable;                      /* True if pages are on backing store */
  u8 eCreate;                         /* eCreate value for for xFetch() */
  int (*xStress)(void*,PgHdr*);       /* Call to try make a page clean */
  void *pStress;                      /* Argument to xStress */
  sqlitex_pcache *pCache;             /* Pluggable cache module */
  PgHdr *pPage1;                      /* Reference to page 1 */
};

/********************************** Linked List Management ********************/

/* Allowed values for second argument to pcacheManageDirtyList() */
#define PCACHE_DIRTYLIST_REMOVE   1    /* Remove pPage from dirty list */
#define PCACHE_DIRTYLIST_ADD      2    /* Add pPage to the dirty list */
#define PCACHE_DIRTYLIST_FRONT    3    /* Move pPage to the front of the list */

/*
** Manage pPage's participation on the dirty list.  Bits of the addRemove
** argument determines what operation to do.  The 0x01 bit means first
** remove pPage from the dirty list.  The 0x02 means add pPage back to
** the dirty list.  Doing both moves pPage to the front of the dirty list.
*/
static void pcacheManageDirtyList(PgHdr *pPage, u8 addRemove){
  PCache *p = pPage->pCache;

  if( addRemove & PCACHE_DIRTYLIST_REMOVE ){
    assert( pPage->pDirtyNext || pPage==p->pDirtyTail );
    assert( pPage->pDirtyPrev || pPage==p->pDirty );
  
    /* Update the PCache1.pSynced variable if necessary. */
    if( p->pSynced==pPage ){
      PgHdr *pSynced = pPage->pDirtyPrev;
      while( pSynced && (pSynced->flags&PGHDR_NEED_SYNC) ){
        pSynced = pSynced->pDirtyPrev;
      }
      p->pSynced = pSynced;
    }
  
    if( pPage->pDirtyNext ){
      pPage->pDirtyNext->pDirtyPrev = pPage->pDirtyPrev;
    }else{
      assert( pPage==p->pDirtyTail );
      p->pDirtyTail = pPage->pDirtyPrev;
    }
    if( pPage->pDirtyPrev ){
      pPage->pDirtyPrev->pDirtyNext = pPage->pDirtyNext;
    }else{
      assert( pPage==p->pDirty );
      p->pDirty = pPage->pDirtyNext;
      if( p->pDirty==0 && p->bPurgeable ){
        assert( p->eCreate==1 );
        p->eCreate = 2;
      }
    }
    pPage->pDirtyNext = 0;
    pPage->pDirtyPrev = 0;
  }
  if( addRemove & PCACHE_DIRTYLIST_ADD ){
    assert( pPage->pDirtyNext==0 && pPage->pDirtyPrev==0 && p->pDirty!=pPage );
  
    pPage->pDirtyNext = p->pDirty;
    if( pPage->pDirtyNext ){
      assert( pPage->pDirtyNext->pDirtyPrev==0 );
      pPage->pDirtyNext->pDirtyPrev = pPage;
    }else{
      p->pDirtyTail = pPage;
      if( p->bPurgeable ){
        assert( p->eCreate==2 );
        p->eCreate = 1;
      }
    }
    p->pDirty = pPage;
    if( !p->pSynced && 0==(pPage->flags&PGHDR_NEED_SYNC) ){
      p->pSynced = pPage;
    }
  }
}

/*
** Wrapper around the pluggable caches xUnpin method. If the cache is
** being used for an in-memory database, this function is a no-op.
*/
static void pcacheUnpin(PgHdr *p){
  if( p->pCache->bPurgeable ){
    if( p->pgno==1 ){
      p->pCache->pPage1 = 0;
    }
    sqlitexGlobalConfig.pcache2.xUnpin(p->pCache->pCache, p->pPage, 0);
  }
}

/*
** Compute the number of pages of cache requested.  p->szCache is the
** cache size requested by the "PRAGMA cache_size" statement.
**
**
*/
static int numberOfCachePages(PCache *p){
  if( p->szCache>=0 ){
    /* IMPLEMENTATION-OF: R-42059-47211 If the argument N is positive then the
    ** suggested cache size is set to N. */
    return p->szCache;
  }else{
    /* IMPLEMENTATION-OF: R-61436-13639 If the argument N is negative, then
    ** the number of cache pages is adjusted to use approximately abs(N*1024)
    ** bytes of memory. */
    return (int)((-1024*(i64)p->szCache)/(p->szPage+p->szExtra));
  }
}

/*************************************************** General Interfaces ******
**
** Initialize and shutdown the page cache subsystem. Neither of these 
** functions are threadsafe.
*/
int sqlitexPcacheInitialize(void){
  if( sqlitexGlobalConfig.pcache2.xInit==0 ){
    /* IMPLEMENTATION-OF: R-26801-64137 If the xInit() method is NULL, then the
    ** built-in default page cache is used instead of the application defined
    ** page cache. */
    sqlitexPCacheSetDefault();
  }
  return sqlitexGlobalConfig.pcache2.xInit(sqlitexGlobalConfig.pcache2.pArg);
}
void sqlitexPcacheShutdown(void){
  if( sqlitexGlobalConfig.pcache2.xShutdown ){
    /* IMPLEMENTATION-OF: R-26000-56589 The xShutdown() method may be NULL. */
    sqlitexGlobalConfig.pcache2.xShutdown(sqlitexGlobalConfig.pcache2.pArg);
  }
}

/*
** Return the size in bytes of a PCache object.
*/
int sqlitexPcacheSize(void){ return sizeof(PCache); }

/*
** Create a new PCache object. Storage space to hold the object
** has already been allocated and is passed in as the p pointer. 
** The caller discovers how much space needs to be allocated by 
** calling sqlitexPcacheSize().
*/
int sqlitexPcacheOpen(
  int szPage,                  /* Size of every page */
  int szExtra,                 /* Extra space associated with each page */
  int bPurgeable,              /* True if pages are on backing store */
  int (*xStress)(void*,PgHdr*),/* Call to try to make pages clean */
  void *pStress,               /* Argument to xStress */
  PCache *p                    /* Preallocated space for the PCache */
){
  memset(p, 0, sizeof(PCache));
  p->szPage = 1;
  p->szExtra = szExtra;
  p->bPurgeable = bPurgeable;
  p->eCreate = 2;
  p->xStress = xStress;
  p->pStress = pStress;
  p->szCache = 100;
  return sqlitexPcacheSetPageSize(p, szPage);
}

/*
** Change the page size for PCache object. The caller must ensure that there
** are no outstanding page references when this function is called.
*/
int sqlitexPcacheSetPageSize(PCache *pCache, int szPage){
  assert( pCache->nRef==0 && pCache->pDirty==0 );
  if( pCache->szPage ){
    sqlitex_pcache *pNew;
    pNew = sqlitexGlobalConfig.pcache2.xCreate(
                szPage, pCache->szExtra + ROUND8(sizeof(PgHdr)),
                pCache->bPurgeable
    );
    if( pNew==0 ) return SQLITE_NOMEM;
    sqlitexGlobalConfig.pcache2.xCachesize(pNew, numberOfCachePages(pCache));
    if( pCache->pCache ){
      sqlitexGlobalConfig.pcache2.xDestroy(pCache->pCache);
    }
    pCache->pCache = pNew;
    pCache->pPage1 = 0;
    pCache->szPage = szPage;
  }
  return SQLITE_OK;
}

/*
** Try to obtain a page from the cache.
**
** This routine returns a pointer to an sqlitex_pcache_page object if
** such an object is already in cache, or if a new one is created.
** This routine returns a NULL pointer if the object was not in cache
** and could not be created.
**
** The createFlags should be 0 to check for existing pages and should
** be 3 (not 1, but 3) to try to create a new page.
**
** If the createFlag is 0, then NULL is always returned if the page
** is not already in the cache.  If createFlag is 1, then a new page
** is created only if that can be done without spilling dirty pages
** and without exceeding the cache size limit.
**
** The caller needs to invoke sqlitexPcacheFetchFinish() to properly
** initialize the sqlitex_pcache_page object and convert it into a
** PgHdr object.  The sqlitexPcacheFetch() and sqlitexPcacheFetchFinish()
** routines are split this way for performance reasons. When separated
** they can both (usually) operate without having to push values to
** the stack on entry and pop them back off on exit, which saves a
** lot of pushing and popping.
*/
sqlitex_pcache_page *sqlitexPcacheFetch(
  PCache *pCache,       /* Obtain the page from this cache */
  Pgno pgno,            /* Page number to obtain */
  int createFlag        /* If true, create page if it does not exist already */
){
  int eCreate;

  assert( pCache!=0 );
  assert( pCache->pCache!=0 );
  assert( createFlag==3 || createFlag==0 );
  assert( pgno>0 );

  /* eCreate defines what to do if the page does not exist.
  **    0     Do not allocate a new page.  (createFlag==0)
  **    1     Allocate a new page if doing so is inexpensive.
  **          (createFlag==1 AND bPurgeable AND pDirty)
  **    2     Allocate a new page even it doing so is difficult.
  **          (createFlag==1 AND !(bPurgeable AND pDirty)
  */
  eCreate = createFlag & pCache->eCreate;
  assert( eCreate==0 || eCreate==1 || eCreate==2 );
  assert( createFlag==0 || pCache->eCreate==eCreate );
  assert( createFlag==0 || eCreate==1+(!pCache->bPurgeable||!pCache->pDirty) );
  return sqlitexGlobalConfig.pcache2.xFetch(pCache->pCache, pgno, eCreate);
}

/*
** If the sqlitexPcacheFetch() routine is unable to allocate a new
** page because new clean pages are available for reuse and the cache
** size limit has been reached, then this routine can be invoked to 
** try harder to allocate a page.  This routine might invoke the stress
** callback to spill dirty pages to the journal.  It will then try to
** allocate the new page and will only fail to allocate a new page on
** an OOM error.
**
** This routine should be invoked only after sqlitexPcacheFetch() fails.
*/
int sqlitexPcacheFetchStress(
  PCache *pCache,                 /* Obtain the page from this cache */
  Pgno pgno,                      /* Page number to obtain */
  sqlitex_pcache_page **ppPage    /* Write result here */
){
  PgHdr *pPg;
  if( pCache->eCreate==2 ) return 0;


  /* Find a dirty page to write-out and recycle. First try to find a 
  ** page that does not require a journal-sync (one with PGHDR_NEED_SYNC
  ** cleared), but if that is not possible settle for any other 
  ** unreferenced dirty page.
  */
  for(pPg=pCache->pSynced; 
      pPg && (pPg->nRef || (pPg->flags&PGHDR_NEED_SYNC)); 
      pPg=pPg->pDirtyPrev
  );
  pCache->pSynced = pPg;
  if( !pPg ){
    for(pPg=pCache->pDirtyTail; pPg && pPg->nRef; pPg=pPg->pDirtyPrev);
  }
  if( pPg ){
    int rc;
#ifdef SQLITE_LOG_CACHE_SPILL
    sqlitex_log(SQLITE_FULL, 
                "spill page %d making room for %d - cache used: %d/%d",
                pPg->pgno, pgno,
                sqlitexGlobalConfig.pcache.xPagecount(pCache->pCache),
                numberOfCachePages(pCache));
#endif
    rc = pCache->xStress(pCache->pStress, pPg);
    if( rc!=SQLITE_OK && rc!=SQLITE_BUSY ){
      return rc;
    }
  }
  *ppPage = sqlitexGlobalConfig.pcache2.xFetch(pCache->pCache, pgno, 2);
  return *ppPage==0 ? SQLITE_NOMEM : SQLITE_OK;
}

/*
** This is a helper routine for sqlitexPcacheFetchFinish()
**
** In the uncommon case where the page being fetched has not been
** initialized, this routine is invoked to do the initialization.
** This routine is broken out into a separate function since it
** requires extra stack manipulation that can be avoided in the common
** case.
*/
static SQLITE_NOINLINE PgHdr *pcacheFetchFinishWithInit(
  PCache *pCache,             /* Obtain the page from this cache */
  Pgno pgno,                  /* Page number obtained */
  sqlitex_pcache_page *pPage  /* Page obtained by prior PcacheFetch() call */
){
  PgHdr *pPgHdr;
  assert( pPage!=0 );
  pPgHdr = (PgHdr*)pPage->pExtra;
  assert( pPgHdr->pPage==0 );
 memset(pPgHdr, 0, sizeof(PgHdr));
  pPgHdr->pPage = pPage;
  pPgHdr->pData = pPage->pBuf;
  pPgHdr->pExtra = (void *)&pPgHdr[1];
  memset(pPgHdr->pExtra, 0, pCache->szExtra);
  pPgHdr->pCache = pCache;
  pPgHdr->pgno = pgno;
  return sqlitexPcacheFetchFinish(pCache,pgno,pPage);
}

/*
** This routine converts the sqlitex_pcache_page object returned by
** sqlitexPcacheFetch() into an initialized PgHdr object.  This routine
** must be called after sqlitexPcacheFetch() in order to get a usable
** result.
*/
PgHdr *sqlitexPcacheFetchFinish(
  PCache *pCache,             /* Obtain the page from this cache */
  Pgno pgno,                  /* Page number obtained */
  sqlitex_pcache_page *pPage  /* Page obtained by prior PcacheFetch() call */
){
  PgHdr *pPgHdr;

  if( pPage==0 ) return 0;
  pPgHdr = (PgHdr *)pPage->pExtra;

  if( !pPgHdr->pPage ){
    return pcacheFetchFinishWithInit(pCache, pgno, pPage);
  }
  if( 0==pPgHdr->nRef ){
    pCache->nRef++;
  }
  pPgHdr->nRef++;
  if( pgno==1 ){
    pCache->pPage1 = pPgHdr;
  }
  return pPgHdr;
}

/*
** Decrement the reference count on a page. If the page is clean and the
** reference count drops to 0, then it is made eligible for recycling.
*/
void SQLITE_NOINLINE sqlitexPcacheRelease(PgHdr *p){
  assert( p->nRef>0 );
  p->nRef--;
  if( p->nRef==0 ){
    p->pCache->nRef--;
    if( (p->flags&PGHDR_DIRTY)==0 ){
      pcacheUnpin(p);
    }else if( p->pDirtyPrev!=0 ){
      /* Move the page to the head of the dirty list. */
      pcacheManageDirtyList(p, PCACHE_DIRTYLIST_FRONT);
    }
  }
}

/*
** Increase the reference count of a supplied page by 1.
*/
void sqlitexPcacheRef(PgHdr *p){
  assert(p->nRef>0);
  p->nRef++;
}

/*
** Drop a page from the cache. There must be exactly one reference to the
** page. This function deletes that reference, so after it returns the
** page pointed to by p is invalid.
*/
void sqlitexPcacheDrop(PgHdr *p){
  assert( p->nRef==1 );
  if( p->flags&PGHDR_DIRTY ){
    pcacheManageDirtyList(p, PCACHE_DIRTYLIST_REMOVE);
  }
  p->pCache->nRef--;
  if( p->pgno==1 ){
    p->pCache->pPage1 = 0;
  }
  sqlitexGlobalConfig.pcache2.xUnpin(p->pCache->pCache, p->pPage, 1);
}

/*
** Make sure the page is marked as dirty. If it isn't dirty already,
** make it so.
*/
void sqlitexPcacheMakeDirty(PgHdr *p){
  p->flags &= ~PGHDR_DONT_WRITE;
  assert( p->nRef>0 );
  if( 0==(p->flags & PGHDR_DIRTY) ){
    p->flags |= PGHDR_DIRTY;
    pcacheManageDirtyList(p, PCACHE_DIRTYLIST_ADD);
  }
}

/*
** Make sure the page is marked as clean. If it isn't clean already,
** make it so.
*/
void sqlitexPcacheMakeClean(PgHdr *p){
  if( (p->flags & PGHDR_DIRTY) ){
    pcacheManageDirtyList(p, PCACHE_DIRTYLIST_REMOVE);
    p->flags &= ~(PGHDR_DIRTY|PGHDR_NEED_SYNC);
    if( p->nRef==0 ){
      pcacheUnpin(p);
    }
  }
}

/*
** Make every page in the cache clean.
*/
void sqlitexPcacheCleanAll(PCache *pCache){
  PgHdr *p;
  while( (p = pCache->pDirty)!=0 ){
    sqlitexPcacheMakeClean(p);
  }
}

/*
** Clear the PGHDR_NEED_SYNC flag from all dirty pages.
*/
void sqlitexPcacheClearSyncFlags(PCache *pCache){
  PgHdr *p;
  for(p=pCache->pDirty; p; p=p->pDirtyNext){
    p->flags &= ~PGHDR_NEED_SYNC;
  }
  pCache->pSynced = pCache->pDirtyTail;
}

/*
** Change the page number of page p to newPgno. 
*/
void sqlitexPcacheMove(PgHdr *p, Pgno newPgno){
  PCache *pCache = p->pCache;
  assert( p->nRef>0 );
  assert( newPgno>0 );
  sqlitexGlobalConfig.pcache2.xRekey(pCache->pCache, p->pPage, p->pgno,newPgno);
  p->pgno = newPgno;
  if( (p->flags&PGHDR_DIRTY) && (p->flags&PGHDR_NEED_SYNC) ){
    pcacheManageDirtyList(p, PCACHE_DIRTYLIST_FRONT);
  }
}

/*
** Drop every cache entry whose page number is greater than "pgno". The
** caller must ensure that there are no outstanding references to any pages
** other than page 1 with a page number greater than pgno.
**
** If there is a reference to page 1 and the pgno parameter passed to this
** function is 0, then the data area associated with page 1 is zeroed, but
** the page object is not dropped.
*/
void sqlitexPcacheTruncate(PCache *pCache, Pgno pgno){
  if( pCache->pCache ){
    PgHdr *p;
    PgHdr *pNext;
    for(p=pCache->pDirty; p; p=pNext){
      pNext = p->pDirtyNext;
      /* This routine never gets call with a positive pgno except right
      ** after sqlitexPcacheCleanAll().  So if there are dirty pages,
      ** it must be that pgno==0.
      */
      assert( p->pgno>0 );
      if( ALWAYS(p->pgno>pgno) ){
        assert( p->flags&PGHDR_DIRTY );
        sqlitexPcacheMakeClean(p);
      }
    }
    if( pgno==0 && pCache->pPage1 ){
      memset(pCache->pPage1->pData, 0, pCache->szPage);
      pgno = 1;
    }
    sqlitexGlobalConfig.pcache2.xTruncate(pCache->pCache, pgno+1);
  }
}

/*
** Close a cache.
*/
void sqlitexPcacheClose(PCache *pCache){
  assert( pCache->pCache!=0 );
  sqlitexGlobalConfig.pcache2.xDestroy(pCache->pCache);
}

/* 
** Discard the contents of the cache.
*/
void sqlitexPcacheClear(PCache *pCache){
  sqlitexPcacheTruncate(pCache, 0);
}

/*
** Merge two lists of pages connected by pDirty and in pgno order.
** Do not both fixing the pDirtyPrev pointers.
*/
static PgHdr *pcacheMergeDirtyList(PgHdr *pA, PgHdr *pB){
  PgHdr result, *pTail;
  pTail = &result;
  while( pA && pB ){
    if( pA->pgno<pB->pgno ){
      pTail->pDirty = pA;
      pTail = pA;
      pA = pA->pDirty;
    }else{
      pTail->pDirty = pB;
      pTail = pB;
      pB = pB->pDirty;
    }
  }
  if( pA ){
    pTail->pDirty = pA;
  }else if( pB ){
    pTail->pDirty = pB;
  }else{
    pTail->pDirty = 0;
  }
  return result.pDirty;
}

/*
** Sort the list of pages in accending order by pgno.  Pages are
** connected by pDirty pointers.  The pDirtyPrev pointers are
** corrupted by this sort.
**
** Since there cannot be more than 2^31 distinct pages in a database,
** there cannot be more than 31 buckets required by the merge sorter.
** One extra bucket is added to catch overflow in case something
** ever changes to make the previous sentence incorrect.
*/
#define N_SORT_BUCKET  32
static PgHdr *pcacheSortDirtyList(PgHdr *pIn){
  PgHdr *a[N_SORT_BUCKET], *p;
  int i;
  memset(a, 0, sizeof(a));
  while( pIn ){
    p = pIn;
    pIn = p->pDirty;
    p->pDirty = 0;
    for(i=0; ALWAYS(i<N_SORT_BUCKET-1); i++){
      if( a[i]==0 ){
        a[i] = p;
        break;
      }else{
        p = pcacheMergeDirtyList(a[i], p);
        a[i] = 0;
      }
    }
    if( NEVER(i==N_SORT_BUCKET-1) ){
      /* To get here, there need to be 2^(N_SORT_BUCKET) elements in
      ** the input list.  But that is impossible.
      */
      a[i] = pcacheMergeDirtyList(a[i], p);
    }
  }
  p = a[0];
  for(i=1; i<N_SORT_BUCKET; i++){
    p = pcacheMergeDirtyList(p, a[i]);
  }
  return p;
}

/*
** Return a list of all dirty pages in the cache, sorted by page number.
*/
PgHdr *sqlitexPcacheDirtyList(PCache *pCache){
  PgHdr *p;
  for(p=pCache->pDirty; p; p=p->pDirtyNext){
    p->pDirty = p->pDirtyNext;
  }
  return pcacheSortDirtyList(pCache->pDirty);
}

/* 
** Return the total number of referenced pages held by the cache.
*/
int sqlitexPcacheRefCount(PCache *pCache){
  return pCache->nRef;
}

/*
** Return the number of references to the page supplied as an argument.
*/
int sqlitexPcachePageRefcount(PgHdr *p){
  return p->nRef;
}

/* 
** Return the total number of pages in the cache.
*/
int sqlitexPcachePagecount(PCache *pCache){
  assert( pCache->pCache!=0 );
  return sqlitexGlobalConfig.pcache2.xPagecount(pCache->pCache);
}

#ifdef SQLITE_TEST
/*
** Get the suggested cache-size value.
*/
int sqlitexPcacheGetCachesize(PCache *pCache){
  return numberOfCachePages(pCache);
}
#endif

/*
** Set the suggested cache-size value.
*/
void sqlitexPcacheSetCachesize(PCache *pCache, int mxPage){
  assert( pCache->pCache!=0 );
  pCache->szCache = mxPage;
  sqlitexGlobalConfig.pcache2.xCachesize(pCache->pCache,
                                         numberOfCachePages(pCache));
}

/*
** Free up as much memory as possible from the page cache.
*/
void sqlitexPcacheShrink(PCache *pCache){
  assert( pCache->pCache!=0 );
  sqlitexGlobalConfig.pcache2.xShrink(pCache->pCache);
}

/*
** Return the size of the header added by this middleware layer
** in the page-cache hierarchy.
*/
int sqlitexHeaderSizePcache(void){ return ROUND8(sizeof(PgHdr)); }


#if defined(SQLITE_CHECK_PAGES) || defined(SQLITE_DEBUG)
/*
** For all dirty pages currently in the cache, invoke the specified
** callback. This is only used if the SQLITE_CHECK_PAGES macro is
** defined.
*/
void sqlitexPcacheIterateDirty(PCache *pCache, void (*xIter)(PgHdr *)){
  PgHdr *pDirty;
  for(pDirty=pCache->pDirty; pDirty; pDirty=pDirty->pDirtyNext){
    xIter(pDirty);
  }
}
#endif
