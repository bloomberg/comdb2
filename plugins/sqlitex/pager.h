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
** This header file defines the interface that the sqlite page cache
** subsystem.  The page cache subsystem reads and writes a file a page
** at a time and provides a journal for rollback.
*/

#ifndef _PAGER_H_
#define _PAGER_H_

/*
** Default maximum size for persistent journal files. A negative 
** value means no limit. This value may be overridden using the 
** sqlitexPagerJournalSizeLimit() API. See also "PRAGMA journal_size_limit".
*/
#ifndef SQLITE_DEFAULT_JOURNAL_SIZE_LIMIT
  #define SQLITE_DEFAULT_JOURNAL_SIZE_LIMIT -1
#endif

/*
** The type used to represent a page number.  The first page in a file
** is called page 1.  0 is used to represent "not a page".
*/
typedef u32 Pgno;

/*
** Each open file is managed by a separate instance of the "Pager" structure.
*/
typedef struct Pager Pager;

/*
** Handle type for pages.
*/
typedef struct PgHdr DbPage;

/*
** Page number PAGER_MJ_PGNO is never used in an SQLite database (it is
** reserved for working around a windows/posix incompatibility). It is
** used in the journal to signify that the remainder of the journal file 
** is devoted to storing a master journal name - there are no more pages to
** roll back. See comments for function writeMasterJournal() in pager.c 
** for details.
*/
#define PAGER_MJ_PGNO(x) ((Pgno)((PENDING_BYTE/((x)->pageSize))+1))

/*
** Allowed values for the flags parameter to sqlitexPagerOpen().
**
** NOTE: These values must match the corresponding BTREE_ values in btree.h.
*/
#define PAGER_OMIT_JOURNAL  0x0001    /* Do not use a rollback journal */
#define PAGER_MEMORY        0x0002    /* In-memory database */

/*
** Valid values for the second argument to sqlite3PagerLockingMode().
*/
#define PAGER_LOCKINGMODE_QUERY      -1
#define PAGER_LOCKINGMODE_NORMAL      0
#define PAGER_LOCKINGMODE_EXCLUSIVE   1

/*
** Numeric constants that encode the journalmode.  
*/
#define PAGER_JOURNALMODE_QUERY     (-1)  /* Query the value of journalmode */
#define PAGER_JOURNALMODE_DELETE      0   /* Commit by deleting journal file */
#define PAGER_JOURNALMODE_PERSIST     1   /* Commit by zeroing journal header */
#define PAGER_JOURNALMODE_OFF         2   /* Journal omitted.  */
#define PAGER_JOURNALMODE_TRUNCATE    3   /* Commit by truncating journal */
#define PAGER_JOURNALMODE_MEMORY      4   /* In-memory journal file */
#define PAGER_JOURNALMODE_WAL         5   /* Use write-ahead logging */

/*
** Flags that make up the mask passed to sqlitexPagerAcquire().
*/
#define PAGER_GET_NOCONTENT     0x01  /* Do not load data from disk */
#define PAGER_GET_READONLY      0x02  /* Read-only page is acceptable */

/*
** Flags for sqlitexPagerSetFlags()
*/
#define PAGER_SYNCHRONOUS_OFF       0x01  /* PRAGMA synchronous=OFF */
#define PAGER_SYNCHRONOUS_NORMAL    0x02  /* PRAGMA synchronous=NORMAL */
#define PAGER_SYNCHRONOUS_FULL      0x03  /* PRAGMA synchronous=FULL */
#define PAGER_SYNCHRONOUS_MASK      0x03  /* Mask for three values above */
#define PAGER_FULLFSYNC             0x04  /* PRAGMA fullfsync=ON */
#define PAGER_CKPT_FULLFSYNC        0x08  /* PRAGMA checkpoint_fullfsync=ON */
#define PAGER_CACHESPILL            0x10  /* PRAGMA cache_spill=ON */
#define PAGER_FLAGS_MASK            0x1c  /* All above except SYNCHRONOUS */

/*
** The remainder of this file contains the declarations of the functions
** that make up the Pager sub-system API. See source code comments for 
** a detailed description of each routine.
*/

/* Open and close a Pager connection. */ 
int sqlitexPagerOpen(
  sqlitex_vfs*,
  Pager **ppPager,
  const char*,
  int,
  int,
  int,
  void(*)(DbPage*)
);
int sqlitexPagerClose(Pager *pPager);
int sqlitexPagerReadFileheader(Pager*, int, unsigned char*);

/* Functions used to configure a Pager object. */
void sqlitexPagerSetBusyhandler(Pager*, int(*)(void *), void *);
int sqlitexPagerSetPagesize(Pager*, u32*, int);
int sqlitexPagerMaxPageCount(Pager*, int);
void sqlitexPagerSetCachesize(Pager*, int);
void sqlitexPagerSetMmapLimit(Pager *, sqlitex_int64);
void sqlite3PagerShrink(Pager*);
void sqlitexPagerSetFlags(Pager*,unsigned);
int sqlite3PagerLockingMode(Pager *, int);
int sqlite3PagerSetJournalMode(Pager *, int);
int sqlite3PagerGetJournalMode(Pager*);
int sqlite3PagerOkToChangeJournalMode(Pager*);
i64 sqlitexPagerJournalSizeLimit(Pager *, i64);
sqlitex_backup **sqlitexPagerBackupPtr(Pager*);

/* Functions used to obtain and release page references. */ 
int sqlitexPagerAcquire(Pager *pPager, Pgno pgno, DbPage **ppPage, int clrFlag);
#define sqlitexPagerGet(A,B,C) sqlitexPagerAcquire(A,B,C,0)
DbPage *sqlitexPagerLookup(Pager *pPager, Pgno pgno);
void sqlitexPagerRef(DbPage*);
void sqlitexPagerUnref(DbPage*);
void sqlitexPagerUnrefNotNull(DbPage*);

/* Operations on page references. */
int sqlitexPagerWrite(DbPage*);
void sqlitexPagerDontWrite(DbPage*);
int sqlitexPagerMovepage(Pager*,DbPage*,Pgno,int);
int sqlitexPagerPageRefcount(DbPage*);
void *sqlitexPagerGetData(DbPage *); 
void *sqlitexPagerGetExtra(DbPage *); 

/* Functions used to manage pager transactions and savepoints. */
void sqlitexPagerPagecount(Pager*, int*);
int sqlitexPagerBegin(Pager*, int exFlag, int);
int sqlitexPagerCommitPhaseOne(Pager*,const char *zMaster, int);
int sqlitexPagerExclusiveLock(Pager*);
int sqlitexPagerSync(Pager *pPager, const char *zMaster);
int sqlitexPagerCommitPhaseTwo(Pager*);
int sqlitexPagerRollback(Pager*);
int sqlitexPagerOpenSavepoint(Pager *pPager, int n);
int sqlitexPagerSavepoint(Pager *pPager, int op, int iSavepoint);
int sqlitexPagerSharedLock(Pager *pPager);

#ifndef SQLITE_OMIT_WAL
  int sqlitexPagerCheckpoint(Pager *pPager, int, int*, int*);
  int sqlitexPagerWalSupported(Pager *pPager);
  int sqlitexPagerWalCallback(Pager *pPager);
  int sqlitexPagerOpenWal(Pager *pPager, int *pisOpen);
  int sqlitexPagerCloseWal(Pager *pPager);
#endif

#ifdef SQLITE_ENABLE_ZIPVFS
  int sqlitexPagerWalFramesize(Pager *pPager);
#endif

/* Functions used to query pager state and configuration. */
u8 sqlitexPagerIsreadonly(Pager*);
u32 sqlitexPagerDataVersion(Pager*);
int sqlitexPagerRefcount(Pager*);
int sqlite3PagerMemUsed(Pager*);
const char *sqlite3PagerFilename(Pager*, int);
const sqlitex_vfs *sqlitexPagerVfs(Pager*);
sqlitex_file *sqlite3PagerFile(Pager*);
const char *sqlitexPagerJournalname(Pager*);
int sqlitexPagerNosync(Pager*);
void *sqlitexPagerTempSpace(Pager*);
int sqlitexPagerIsMemdb(Pager*);
void sqlite3PagerCacheStat(Pager *, int, int, int *);
void sqlitexPagerClearCache(Pager *);
int sqlitexSectorSize(sqlitex_file *);

/* Functions used to truncate the database file. */
void sqlitexPagerTruncateImage(Pager*,Pgno);

void sqlitexPagerRekey(DbPage*, Pgno, u16);

#if defined(SQLITE_HAS_CODEC) && !defined(SQLITE_OMIT_WAL)
void *sqlitexPagerCodec(DbPage *);
#endif

/* Functions to support testing and debugging. */
#if !defined(NDEBUG) || defined(SQLITE_TEST)
  Pgno sqlitexPagerPagenumber(DbPage*);
  int sqlitexPagerIswriteable(DbPage*);
#endif
#ifdef SQLITE_TEST
  int *sqlitexPagerStats(Pager*);
  void sqlitexPagerRefdump(Pager*);
  void disable_simulated_io_errors(void);
  void enable_simulated_io_errors(void);
#else
# define disable_simulated_io_errors()
# define enable_simulated_io_errors()
#endif

#endif /* _PAGER_H_ */
