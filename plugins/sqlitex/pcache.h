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
** This header file defines the interface that the sqlite page cache
** subsystem. 
*/

#ifndef _PCACHE_H_

typedef struct PgHdr PgHdr;
typedef struct PCache PCache;

/*
** Every page in the cache is controlled by an instance of the following
** structure.
*/
struct PgHdr {
  sqlitex_pcache_page *pPage;    /* Pcache object page handle */
  void *pData;                   /* Page data */
  void *pExtra;                  /* Extra content */
  PgHdr *pDirty;                 /* Transient list of dirty pages */
  Pager *pPager;                 /* The pager this page is part of */
  Pgno pgno;                     /* Page number for this page */
#ifdef SQLITE_CHECK_PAGES
  u32 pageHash;                  /* Hash of page content */
#endif
  u16 flags;                     /* PGHDR flags defined below */

  /**********************************************************************
  ** Elements above are public.  All that follows is private to pcache.c
  ** and should not be accessed by other modules.
  */
  i16 nRef;                      /* Number of users of this page */
  PCache *pCache;                /* Cache that owns this page */

  PgHdr *pDirtyNext;             /* Next element in list of dirty pages */
  PgHdr *pDirtyPrev;             /* Previous element in list of dirty pages */
};

/* Bit values for PgHdr.flags */
#define PGHDR_DIRTY             0x002  /* Page has changed */
#define PGHDR_NEED_SYNC         0x004  /* Fsync the rollback journal before
                                       ** writing this page to the database */
#define PGHDR_NEED_READ         0x008  /* Content is unread */
#define PGHDR_REUSE_UNLIKELY    0x010  /* A hint that reuse is unlikely */
#define PGHDR_DONT_WRITE        0x020  /* Do not write content to disk */

#define PGHDR_MMAP              0x040  /* This is an mmap page object */

/* Initialize and shutdown the page cache subsystem */
int sqlitexPcacheInitialize(void);
void sqlitexPcacheShutdown(void);

/* Page cache buffer management:
** These routines implement SQLITE_CONFIG_PAGECACHE.
*/
void sqlitexPCacheBufferSetup(void *, int sz, int n);

/* Create a new pager cache.
** Under memory stress, invoke xStress to try to make pages clean.
** Only clean and unpinned pages can be reclaimed.
*/
int sqlitexPcacheOpen(
  int szPage,                    /* Size of every page */
  int szExtra,                   /* Extra space associated with each page */
  int bPurgeable,                /* True if pages are on backing store */
  int (*xStress)(void*, PgHdr*), /* Call to try to make pages clean */
  void *pStress,                 /* Argument to xStress */
  PCache *pToInit                /* Preallocated space for the PCache */
);

/* Modify the page-size after the cache has been created. */
int sqlitexPcacheSetPageSize(PCache *, int);

/* Return the size in bytes of a PCache object.  Used to preallocate
** storage space.
*/
int sqlitexPcacheSize(void);

/* One release per successful fetch.  Page is pinned until released.
** Reference counted. 
*/
sqlitex_pcache_page *sqlitexPcacheFetch(PCache*, Pgno, int createFlag);
int sqlitexPcacheFetchStress(PCache*, Pgno, sqlitex_pcache_page**);
PgHdr *sqlitexPcacheFetchFinish(PCache*, Pgno, sqlitex_pcache_page *pPage);
void sqlitexPcacheRelease(PgHdr*);

void sqlitexPcacheDrop(PgHdr*);         /* Remove page from cache */
void sqlitexPcacheMakeDirty(PgHdr*);    /* Make sure page is marked dirty */
void sqlitexPcacheMakeClean(PgHdr*);    /* Mark a single page as clean */
void sqlitexPcacheCleanAll(PCache*);    /* Mark all dirty list pages as clean */

/* Change a page number.  Used by incr-vacuum. */
void sqlitexPcacheMove(PgHdr*, Pgno);

/* Remove all pages with pgno>x.  Reset the cache if x==0 */
void sqlitexPcacheTruncate(PCache*, Pgno x);

/* Get a list of all dirty pages in the cache, sorted by page number */
PgHdr *sqlitexPcacheDirtyList(PCache*);

/* Reset and close the cache object */
void sqlitexPcacheClose(PCache*);

/* Clear flags from pages of the page cache */
void sqlitexPcacheClearSyncFlags(PCache *);

/* Discard the contents of the cache */
void sqlitexPcacheClear(PCache*);

/* Return the total number of outstanding page references */
int sqlitexPcacheRefCount(PCache*);

/* Increment the reference count of an existing page */
void sqlitexPcacheRef(PgHdr*);

int sqlitexPcachePageRefcount(PgHdr*);

/* Return the total number of pages stored in the cache */
int sqlitexPcachePagecount(PCache*);

#if defined(SQLITE_CHECK_PAGES) || defined(SQLITE_DEBUG)
/* Iterate through all dirty pages currently stored in the cache. This
** interface is only available if SQLITE_CHECK_PAGES is defined when the 
** library is built.
*/
void sqlitexPcacheIterateDirty(PCache *pCache, void (*xIter)(PgHdr *));
#endif

/* Set and get the suggested cache-size for the specified pager-cache.
**
** If no global maximum is configured, then the system attempts to limit
** the total number of pages cached by purgeable pager-caches to the sum
** of the suggested cache-sizes.
*/
void sqlitexPcacheSetCachesize(PCache *, int);
#ifdef SQLITE_TEST
int sqlitexPcacheGetCachesize(PCache *);
#endif

/* Free up as much memory as possible from the page cache */
void sqlitexPcacheShrink(PCache*);

#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
/* Try to return memory used by the pcache module to the main memory heap */
int sqlitexPcacheReleaseMemory(int);
#endif

#ifdef SQLITE_TEST
void sqlitexPcacheStats(int*,int*,int*,int*);
#endif

void sqlitexPCacheSetDefault(void);

/* Return the header size */
int sqlitexHeaderSizePcache(void);
int sqlitexHeaderSizePcache1(void);

#endif /* _PCACHE_H_ */
