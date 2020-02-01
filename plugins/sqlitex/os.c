/*
** 2005 November 29
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This file contains OS interface code that is common to all
** architectures.
*/
#define _SQLITE_OS_C_ 1
#include "sqliteInt.h"
#undef _SQLITE_OS_C_

/*
** The default SQLite sqlitex_vfs implementations do not allocate
** memory (actually, os_unix.c allocates a small amount of memory
** from within OsOpen()), but some third-party implementations may.
** So we test the effects of a malloc() failing and the sqlitexOsXXX()
** function returning SQLITE_IOERR_NOMEM using the DO_OS_MALLOC_TEST macro.
**
** The following functions are instrumented for malloc() failure 
** testing:
**
**     sqlitexOsRead()
**     sqlitexOsWrite()
**     sqlitexOsSync()
**     sqlitexOsFileSize()
**     sqlitexOsLock()
**     sqlitexOsCheckReservedLock()
**     sqlitexOsFileControl()
**     sqlitexOsShmMap()
**     sqlitexOsOpen()
**     sqlitexOsDelete()
**     sqlitexOsAccess()
**     sqlitexOsFullPathname()
**
*/
#if defined(SQLITE_TEST)
int sqlitex_memdebug_vfs_oom_test = 1;
  #define DO_OS_MALLOC_TEST(x)                                       \
  if (sqlitex_memdebug_vfs_oom_test && (!x || !sqlitexIsMemJournal(x))) {  \
    void *pTstAlloc = sqlitexMalloc(10);                             \
    if (!pTstAlloc) return SQLITE_IOERR_NOMEM;                       \
    sqlitex_free(pTstAlloc);                                         \
  }
#else
  #define DO_OS_MALLOC_TEST(x)
#endif

/*
** The following routines are convenience wrappers around methods
** of the sqlitex_file object.  This is mostly just syntactic sugar. All
** of this would be completely automatic if SQLite were coded using
** C++ instead of plain old C.
*/
int sqlitexOsClose(sqlitex_file *pId){
  int rc = SQLITE_OK;
  if( pId->pMethods ){
    rc = pId->pMethods->xClose(pId);
    pId->pMethods = 0;
  }
  return rc;
}
int sqlitexOsRead(sqlitex_file *id, void *pBuf, int amt, i64 offset){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xRead(id, pBuf, amt, offset);
}
int sqlitexOsWrite(sqlitex_file *id, const void *pBuf, int amt, i64 offset){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xWrite(id, pBuf, amt, offset);
}
int sqlitexOsTruncate(sqlitex_file *id, i64 size){
  return id->pMethods->xTruncate(id, size);
}
int sqlitexOsSync(sqlitex_file *id, int flags){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xSync(id, flags);
}
int sqlitexOsFileSize(sqlitex_file *id, i64 *pSize){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xFileSize(id, pSize);
}
int sqlitexOsLock(sqlitex_file *id, int lockType){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xLock(id, lockType);
}
int sqlitexOsUnlock(sqlitex_file *id, int lockType){
  return id->pMethods->xUnlock(id, lockType);
}
int sqlitexOsCheckReservedLock(sqlitex_file *id, int *pResOut){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xCheckReservedLock(id, pResOut);
}

/*
** Use sqlitexOsFileControl() when we are doing something that might fail
** and we need to know about the failures.  Use sqlitexOsFileControlHint()
** when simply tossing information over the wall to the VFS and we do not
** really care if the VFS receives and understands the information since it
** is only a hint and can be safely ignored.  The sqlitexOsFileControlHint()
** routine has no return value since the return value would be meaningless.
*/
int sqlitexOsFileControl(sqlitex_file *id, int op, void *pArg){
#ifdef SQLITE_TEST
  if( op!=SQLITE_FCNTL_COMMIT_PHASETWO ){
    /* Faults are not injected into COMMIT_PHASETWO because, assuming SQLite
    ** is using a regular VFS, it is called after the corresponding 
    ** transaction has been committed. Injecting a fault at this point 
    ** confuses the test scripts - the COMMIT comand returns SQLITE_NOMEM
    ** but the transaction is committed anyway.
    **
    ** The core must call OsFileControl() though, not OsFileControlHint(),
    ** as if a custom VFS (e.g. zipvfs) returns an error here, it probably
    ** means the commit really has failed and an error should be returned
    ** to the user.  */
    DO_OS_MALLOC_TEST(id);
  }
#endif
  return id->pMethods->xFileControl(id, op, pArg);
}
void sqlitexOsFileControlHint(sqlitex_file *id, int op, void *pArg){
  (void)id->pMethods->xFileControl(id, op, pArg);
}

int sqlitexOsSectorSize(sqlitex_file *id){
  int (*xSectorSize)(sqlitex_file*) = id->pMethods->xSectorSize;
  return (xSectorSize ? xSectorSize(id) : SQLITE_DEFAULT_SECTOR_SIZE);
}
int sqlitexOsDeviceCharacteristics(sqlitex_file *id){
  return id->pMethods->xDeviceCharacteristics(id);
}
int sqlitexOsShmLock(sqlitex_file *id, int offset, int n, int flags){
  return id->pMethods->xShmLock(id, offset, n, flags);
}
void sqlitexOsShmBarrier(sqlitex_file *id){
  id->pMethods->xShmBarrier(id);
}
int sqlitexOsShmUnmap(sqlitex_file *id, int deleteFlag){
  return id->pMethods->xShmUnmap(id, deleteFlag);
}
int sqlitexOsShmMap(
  sqlitex_file *id,               /* Database file handle */
  int iPage,
  int pgsz,
  int bExtend,                    /* True to extend file if necessary */
  void volatile **pp              /* OUT: Pointer to mapping */
){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xShmMap(id, iPage, pgsz, bExtend, pp);
}

#if SQLITE_MAX_MMAP_SIZE>0
/* The real implementation of xFetch and xUnfetch */
int sqlitexOsFetch(sqlitex_file *id, i64 iOff, int iAmt, void **pp){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xFetch(id, iOff, iAmt, pp);
}
int sqlitexOsUnfetch(sqlitex_file *id, i64 iOff, void *p){
  return id->pMethods->xUnfetch(id, iOff, p);
}
#else
/* No-op stubs to use when memory-mapped I/O is disabled */
int sqlitexOsFetch(sqlitex_file *id, i64 iOff, int iAmt, void **pp){
  *pp = 0;
  return SQLITE_OK;
}
int sqlitexOsUnfetch(sqlitex_file *id, i64 iOff, void *p){
  return SQLITE_OK;
}
#endif

/*
** The next group of routines are convenience wrappers around the
** VFS methods.
*/
int sqlitexOsOpen(
  sqlitex_vfs *pVfs, 
  const char *zPath, 
  sqlitex_file *pFile, 
  int flags, 
  int *pFlagsOut
){
  int rc;
  DO_OS_MALLOC_TEST(0);
  /* 0x87f7f is a mask of SQLITE_OPEN_ flags that are valid to be passed
  ** down into the VFS layer.  Some SQLITE_OPEN_ flags (for example,
  ** SQLITE_OPEN_FULLMUTEX or SQLITE_OPEN_SHAREDCACHE) are blocked before
  ** reaching the VFS. */
  rc = pVfs->xOpen(pVfs, zPath, pFile, flags & 0x87f7f, pFlagsOut);
  assert( rc==SQLITE_OK || pFile->pMethods==0 );
  return rc;
}
int sqlitexOsDelete(sqlitex_vfs *pVfs, const char *zPath, int dirSync){
  DO_OS_MALLOC_TEST(0);
  assert( dirSync==0 || dirSync==1 );
  return pVfs->xDelete(pVfs, zPath, dirSync);
}
int sqlitexOsAccess(
  sqlitex_vfs *pVfs, 
  const char *zPath, 
  int flags, 
  int *pResOut
){
  DO_OS_MALLOC_TEST(0);
  return pVfs->xAccess(pVfs, zPath, flags, pResOut);
}
int sqlitexOsFullPathname(
  sqlitex_vfs *pVfs, 
  const char *zPath, 
  int nPathOut, 
  char *zPathOut
){
  DO_OS_MALLOC_TEST(0);
  zPathOut[0] = 0;
  return pVfs->xFullPathname(pVfs, zPath, nPathOut, zPathOut);
}
#ifndef SQLITE_OMIT_LOAD_EXTENSION
void *sqlitexOsDlOpen(sqlitex_vfs *pVfs, const char *zPath){
  return pVfs->xDlOpen(pVfs, zPath);
}
void sqlitexOsDlError(sqlitex_vfs *pVfs, int nByte, char *zBufOut){
  pVfs->xDlError(pVfs, nByte, zBufOut);
}
void (*sqlitexOsDlSym(sqlitex_vfs *pVfs, void *pHdle, const char *zSym))(void){
  return pVfs->xDlSym(pVfs, pHdle, zSym);
}
void sqlitexOsDlClose(sqlitex_vfs *pVfs, void *pHandle){
  pVfs->xDlClose(pVfs, pHandle);
}
#endif /* SQLITE_OMIT_LOAD_EXTENSION */
int sqlitexOsRandomness(sqlitex_vfs *pVfs, int nByte, char *zBufOut){
  return pVfs->xRandomness(pVfs, nByte, zBufOut);
}
int sqlitexOsSleep(sqlitex_vfs *pVfs, int nMicro){
  return pVfs->xSleep(pVfs, nMicro);
}
int sqlitexOsCurrentTimeInt64(sqlitex_vfs *pVfs, sqlitex_int64 *pTimeOut){
  int rc;
  /* IMPLEMENTATION-OF: R-49045-42493 SQLite will use the xCurrentTimeInt64()
  ** method to get the current date and time if that method is available
  ** (if iVersion is 2 or greater and the function pointer is not NULL) and
  ** will fall back to xCurrentTime() if xCurrentTimeInt64() is
  ** unavailable.
  */
  if( pVfs->iVersion>=2 && pVfs->xCurrentTimeInt64 ){
    rc = pVfs->xCurrentTimeInt64(pVfs, pTimeOut);
  }else{
    double r;
    rc = pVfs->xCurrentTime(pVfs, &r);
    *pTimeOut = (sqlitex_int64)(r*86400000.0);
  }
  return rc;
}

int sqlitexOsOpenMalloc(
  sqlitex_vfs *pVfs, 
  const char *zFile, 
  sqlitex_file **ppFile, 
  int flags,
  int *pOutFlags
){
  int rc = SQLITE_NOMEM;
  sqlitex_file *pFile;
  pFile = (sqlitex_file *)sqlitexMallocZero(pVfs->szOsFile);
  if( pFile ){
    rc = sqlitexOsOpen(pVfs, zFile, pFile, flags, pOutFlags);
    if( rc!=SQLITE_OK ){
      sqlitex_free(pFile);
    }else{
      *ppFile = pFile;
    }
  }
  return rc;
}
int sqlitexOsCloseFree(sqlitex_file *pFile){
  int rc = SQLITE_OK;
  assert( pFile );
  rc = sqlitexOsClose(pFile);
  sqlitex_free(pFile);
  return rc;
}

/*
** This function is a wrapper around the OS specific implementation of
** sqlitex_os_init(). The purpose of the wrapper is to provide the
** ability to simulate a malloc failure, so that the handling of an
** error in sqlitex_os_init() by the upper layers can be tested.
*/
int sqlitexOsInit(void){
  void *p = sqlitex_malloc(10);
  if( p==0 ) return SQLITE_NOMEM;
  sqlitex_free(p);
  return sqlitex_os_init();
}

/*
** The list of all registered VFS implementations.
*/
static sqlitex_vfs * SQLITE_WSD vfsList = 0;
#define vfsList GLOBAL(sqlitex_vfs *, vfsList)

/*
** Locate a VFS by name.  If no name is given, simply return the
** first VFS on the list.
*/
sqlitex_vfs *sqlitex_vfs_find(const char *zVfs){
  sqlitex_vfs *pVfs = 0;
#if SQLITE_THREADSAFE
  sqlitex_mutex *mutex;
#endif
#ifndef SQLITE_OMIT_AUTOINIT
  int rc = sqlitex_initialize();
  if( rc ) return 0;
#endif
#if SQLITE_THREADSAFE
  mutex = sqlitexMutexAlloc(SQLITE_MUTEX_STATIC_MASTER);
#endif
  sqlitex_mutex_enter(mutex);
  for(pVfs = vfsList; pVfs; pVfs=pVfs->pNext){
    if( zVfs==0 ) break;
    if( strcmp(zVfs, pVfs->zName)==0 ) break;
  }
  sqlitex_mutex_leave(mutex);
  return pVfs;
}

/*
** Unlink a VFS from the linked list
*/
static void vfsUnlink(sqlitex_vfs *pVfs){
  assert( sqlitex_mutex_held(sqlitexMutexAlloc(SQLITE_MUTEX_STATIC_MASTER)) );
  if( pVfs==0 ){
    /* No-op */
  }else if( vfsList==pVfs ){
    vfsList = pVfs->pNext;
  }else if( vfsList ){
    sqlitex_vfs *p = vfsList;
    while( p->pNext && p->pNext!=pVfs ){
      p = p->pNext;
    }
    if( p->pNext==pVfs ){
      p->pNext = pVfs->pNext;
    }
  }
}

/*
** Register a VFS with the system.  It is harmless to register the same
** VFS multiple times.  The new VFS becomes the default if makeDflt is
** true.
*/
int sqlitex_vfs_register(sqlitex_vfs *pVfs, int makeDflt){
  MUTEX_LOGIC(sqlitex_mutex *mutex;)
#ifndef SQLITE_OMIT_AUTOINIT
  int rc = sqlitex_initialize();
  if( rc ) return rc;
#endif
#ifdef SQLITE_ENABLE_API_ARMOR
  if( pVfs==0 ) return SQLITE_MISUSE_BKPT;
#endif

  MUTEX_LOGIC( mutex = sqlitexMutexAlloc(SQLITE_MUTEX_STATIC_MASTER); )
  sqlitex_mutex_enter(mutex);
  vfsUnlink(pVfs);
  if( makeDflt || vfsList==0 ){
    pVfs->pNext = vfsList;
    vfsList = pVfs;
  }else{
    pVfs->pNext = vfsList->pNext;
    vfsList->pNext = pVfs;
  }
  assert(vfsList);
  sqlitex_mutex_leave(mutex);
  return SQLITE_OK;
}

/*
** Unregister a VFS so that it is no longer accessible.
*/
int sqlitex_vfs_unregister(sqlitex_vfs *pVfs){
#if SQLITE_THREADSAFE
  sqlitex_mutex *mutex = sqlitexMutexAlloc(SQLITE_MUTEX_STATIC_MASTER);
#endif
  sqlitex_mutex_enter(mutex);
  vfsUnlink(pVfs);
  sqlitex_mutex_leave(mutex);
  return SQLITE_OK;
}
