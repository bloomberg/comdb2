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
** Main file for the SQLite library.  The routines in this file
** implement the programmer interface to the library.  Routines in
** other files are for internal use by SQLite and should not be
** accessed by users of the library.
*/
#include "sqliteInt.h"
#include "sqlglue.h"

#ifdef SQLITE_ENABLE_FTS3
# include "fts3.h"
#endif
#ifdef SQLITE_ENABLE_RTREE
# include "rtree.h"
#endif
#ifdef SQLITE_ENABLE_ICU
# include "sqliteicu.h"
#endif
/* COMDB2 MODIFICATION */
#ifdef SQLITE_BUILDING_FOR_COMDB2
# include "comdb2systbl.h"
# ifdef SQLITE_ENABLE_SERIES
#  include "series.h"
# endif
#endif

/* COMDB2 MODIFICATION */
#include <paulbitchk.h>

#ifndef SQLITE_AMALGAMATION
/* IMPLEMENTATION-OF: R-46656-45156 The sqlitex_version[] string constant
** contains the text of SQLITE_VERSION macro. 
*/
const char sqlitex_version[] = SQLITE_VERSION;
#endif

/* IMPLEMENTATION-OF: R-53536-42575 The sqlitex_libversion() function returns
** a pointer to the to the sqlitex_version[] string constant. 
*/
const char *sqlitex_libversion(void){ return sqlitex_version; }

/* IMPLEMENTATION-OF: R-63124-39300 The sqlitex_sourceid() function returns a
** pointer to a string constant whose value is the same as the
** SQLITEX_SOURCE_ID C preprocessor macro. 
*/
const char *sqlitex_sourceid(void){ return SQLITEX_SOURCE_ID; }

/* IMPLEMENTATION-OF: R-35210-63508 The sqlitex_libversion_number() function
** returns an integer equal to SQLITE_VERSION_NUMBER.
*/
int sqlitex_libversion_number(void){ return SQLITE_VERSION_NUMBER; }

/* IMPLEMENTATION-OF: R-20790-14025 The sqlitex_threadsafe() function returns
** zero if and only if SQLite was compiled with mutexing code omitted due to
** the SQLITE_THREADSAFE compile-time option being set to 0.
*/
int sqlitex_threadsafe(void){ return SQLITE_THREADSAFE; }

#if !defined(SQLITE_OMIT_TRACE) && defined(SQLITE_ENABLE_IOTRACE)
/*
** If the following function pointer is not NULL and if
** SQLITE_ENABLE_IOTRACE is enabled, then messages describing
** I/O active are written using this function.  These messages
** are intended for debugging activity only.
*/
/* not-private */ void (*sqlitexIoTrace)(const char*, ...) = 0;
#endif

/*
** If the following global variable points to a string which is the
** name of a directory, then that directory will be used to store
** temporary files.
**
** See also the "PRAGMA temp_store_directory" SQL command.
*/
char *sqlitex_temp_directory = 0;

/*
** If the following global variable points to a string which is the
** name of a directory, then that directory will be used to store
** all database files specified with a relative pathname.
**
** See also the "PRAGMA data_store_directory" SQL command.
*/
char *sqlitex_data_directory = 0;


/* COMDB2 MODIFICATION */
static int sqlitexPCacheBufferSetup(void *dummy, ...) { return 0; }
static int sqlitexPcacheInitialize(void) { return 0; }
static int sqlitexPcacheShutdown(void) { return 0; }
static int sqlitexPCacheSetDefault(void) { return 0; }
int sqlitexHeaderSizeBtree(void) { return 0; }
int sqlitexHeaderSizePcache(void){ return 0; } 
int sqlitexHeaderSizePcache1(void){ return 0; } 

/*
** Initialize SQLite.  
**
** This routine must be called to initialize the memory allocation,
** VFS, and mutex subsystems prior to doing any serious work with
** SQLite.  But as long as you do not compile with SQLITE_OMIT_AUTOINIT
** this routine will be called automatically by key routines such as
** sqlitex_open().  
**
** This routine is a no-op except on its very first call for the process,
** or for the first call after a call to sqlitex_shutdown.
**
** The first thread to call this routine runs the initialization to
** completion.  If subsequent threads call this routine before the first
** thread has finished the initialization process, then the subsequent
** threads must block until the first thread finishes with the initialization.
**
** The first thread might call this routine recursively.  Recursive
** calls to this routine should not block, of course.  Otherwise the
** initialization process would never complete.
**
** Let X be the first thread to enter this routine.  Let Y be some other
** thread.  Then while the initial invocation of this routine by X is
** incomplete, it is required that:
**
**    *  Calls to this routine from Y must block until the outer-most
**       call by X completes.
**
**    *  Recursive calls to this routine from thread X return immediately
**       without blocking.
*/
int sqlitex_initialize(void){
  MUTEX_LOGIC( sqlitex_mutex *pMaster; )       /* The main static mutex */
  int rc;                                      /* Result code */
#ifdef SQLITE_EXTRA_INIT
  int bRunExtraInit = 0;                       /* Extra initialization needed */
#endif

#ifdef SQLITE_OMIT_WSD
  rc = sqlitex_wsd_init(4096, 24);
  if( rc!=SQLITE_OK ){
    return rc;
  }
#endif

  /* If SQLite is already completely initialized, then this call
  ** to sqlitex_initialize() should be a no-op.  But the initialization
  ** must be complete.  So isInit must not be set until the very end
  ** of this routine.
  */
  if( sqlitexGlobalConfig.isInit ) return SQLITE_OK;

  /* Make sure the mutex subsystem is initialized.  If unable to 
  ** initialize the mutex subsystem, return early with the error.
  ** If the system is so sick that we are unable to allocate a mutex,
  ** there is not much SQLite is going to be able to do.
  **
  ** The mutex subsystem must take care of serializing its own
  ** initialization.
  */
  rc = sqlitexMutexInit();
  if( rc ) return rc;

  /* Initialize the malloc() system and the recursive pInitMutex mutex.
  ** This operation is protected by the STATIC_MASTER mutex.  Note that
  ** MutexAlloc() is called for a static mutex prior to initializing the
  ** malloc subsystem - this implies that the allocation of a static
  ** mutex must not require support from the malloc subsystem.
  */
  MUTEX_LOGIC( pMaster = sqlitexMutexAlloc(SQLITE_MUTEX_STATIC_MASTER); )
  sqlitex_mutex_enter(pMaster);
  sqlitexGlobalConfig.isMutexInit = 1;
  if( !sqlitexGlobalConfig.isMallocInit ){
    rc = sqlitexMallocInit();
  }
  if( rc==SQLITE_OK ){
    sqlitexGlobalConfig.isMallocInit = 1;
    if( !sqlitexGlobalConfig.pInitMutex ){
      sqlitexGlobalConfig.pInitMutex =
           sqlitexMutexAlloc(SQLITE_MUTEX_RECURSIVE);
      if( sqlitexGlobalConfig.bCoreMutex && !sqlitexGlobalConfig.pInitMutex ){
        rc = SQLITE_NOMEM;
      }
    }
  }
  if( rc==SQLITE_OK ){
    sqlitexGlobalConfig.nRefInitMutex++;
  }
  sqlitex_mutex_leave(pMaster);

  /* If rc is not SQLITE_OK at this point, then either the malloc
  ** subsystem could not be initialized or the system failed to allocate
  ** the pInitMutex mutex. Return an error in either case.  */
  if( rc!=SQLITE_OK ){
    return rc;
  }

  /* Do the rest of the initialization under the recursive mutex so
  ** that we will be able to handle recursive calls into
  ** sqlitex_initialize().  The recursive calls normally come through
  ** sqlitex_os_init() when it invokes sqlitex_vfs_register(), but other
  ** recursive calls might also be possible.
  **
  ** IMPLEMENTATION-OF: R-00140-37445 SQLite automatically serializes calls
  ** to the xInit method, so the xInit method need not be threadsafe.
  **
  ** The following mutex is what serializes access to the appdef pcache xInit
  ** methods.  The sqlitex_pcache_methods.xInit() all is embedded in the
  ** call to sqlitexPcacheInitialize().
  */
  sqlitex_mutex_enter(sqlitexGlobalConfig.pInitMutex);
  if( sqlitexGlobalConfig.isInit==0 && sqlitexGlobalConfig.inProgress==0 ){
    FuncDefHash *pHash = &GLOBAL(FuncDefHash, sqlitexGlobalFunctions);
    sqlitexGlobalConfig.inProgress = 1;
    memset(pHash, 0, sizeof(sqlitexGlobalFunctions));
    sqlitexRegisterGlobalFunctions();
    if( sqlitexGlobalConfig.isPCacheInit==0 ){
      rc = sqlitexPcacheInitialize();
    }
    if( rc==SQLITE_OK ){
      sqlitexGlobalConfig.isPCacheInit = 1;
      rc = sqlitexOsInit();
    }
    if( rc==SQLITE_OK ){
      sqlitexPCacheBufferSetup( sqlitexGlobalConfig.pPage, 
          sqlitexGlobalConfig.szPage, sqlitexGlobalConfig.nPage);
      sqlitexGlobalConfig.isInit = 1;
#ifdef SQLITE_EXTRA_INIT
      bRunExtraInit = 1;
#endif
    }
    sqlitexGlobalConfig.inProgress = 0;
  }
  sqlitex_mutex_leave(sqlitexGlobalConfig.pInitMutex);

  /* Go back under the static mutex and clean up the recursive
  ** mutex to prevent a resource leak.
  */
  sqlitex_mutex_enter(pMaster);
  sqlitexGlobalConfig.nRefInitMutex--;
  if( sqlitexGlobalConfig.nRefInitMutex<=0 ){
    assert( sqlitexGlobalConfig.nRefInitMutex==0 );
    sqlitex_mutex_free(sqlitexGlobalConfig.pInitMutex);
    sqlitexGlobalConfig.pInitMutex = 0;
  }
  sqlitex_mutex_leave(pMaster);

  /* The following is just a sanity check to make sure SQLite has
  ** been compiled correctly.  It is important to run this code, but
  ** we don't want to run it too often and soak up CPU cycles for no
  ** reason.  So we run it once during initialization.
  */
#ifndef NDEBUG
#ifndef SQLITE_OMIT_FLOATING_POINT
  /* This section of code's only "output" is via assert() statements. */
  if ( rc==SQLITE_OK ){
    u64 x = (((u64)1)<<63)-1;
    double y;
    assert(sizeof(x)==8);
    assert(sizeof(x)==sizeof(y));
    memcpy(&y, &x, 8);
    assert( sqlitexIsNaN(y) );
  }
#endif
#endif

  /* Do extra initialization steps requested by the SQLITE_EXTRA_INIT
  ** compile-time option.
  */
#ifdef SQLITE_EXTRA_INIT
  if( bRunExtraInit ){
    int SQLITE_EXTRA_INIT(const char*);
    rc = SQLITE_EXTRA_INIT(0);
  }
#endif

  return rc;
}

/*
** Undo the effects of sqlitex_initialize().  Must not be called while
** there are outstanding database connections or memory allocations or
** while any part of SQLite is otherwise in use in any thread.  This
** routine is not threadsafe.  But it is safe to invoke this routine
** on when SQLite is already shut down.  If SQLite is already shut down
** when this routine is invoked, then this routine is a harmless no-op.
*/
int sqlitex_shutdown(void){
#ifdef SQLITE_OMIT_WSD
  int rc = sqlitex_wsd_init(4096, 24);
  if( rc!=SQLITE_OK ){
    return rc;
  }
#endif

  if( sqlitexGlobalConfig.isInit ){
#ifdef SQLITE_EXTRA_SHUTDOWN
    void SQLITE_EXTRA_SHUTDOWN(void);
    SQLITE_EXTRA_SHUTDOWN();
#endif
    sqlitex_os_end();
    sqlitex_reset_auto_extension();
    sqlitexGlobalConfig.isInit = 0;
  }
  if( sqlitexGlobalConfig.isPCacheInit ){
    sqlitexPcacheShutdown();
    sqlitexGlobalConfig.isPCacheInit = 0;
  }
  if( sqlitexGlobalConfig.isMallocInit ){
    sqlitexMallocEnd();
    sqlitexGlobalConfig.isMallocInit = 0;

#ifndef SQLITE_OMIT_SHUTDOWN_DIRECTORIES
    /* The heap subsystem has now been shutdown and these values are supposed
    ** to be NULL or point to memory that was obtained from sqlitex_malloc(),
    ** which would rely on that heap subsystem; therefore, make sure these
    ** values cannot refer to heap memory that was just invalidated when the
    ** heap subsystem was shutdown.  This is only done if the current call to
    ** this function resulted in the heap subsystem actually being shutdown.
    */
    sqlitex_data_directory = 0;
    sqlitex_temp_directory = 0;
#endif
  }
  if( sqlitexGlobalConfig.isMutexInit ){
    sqlitexMutexEnd();
    sqlitexGlobalConfig.isMutexInit = 0;
  }

  return SQLITE_OK;
}

/*
** This API allows applications to modify the global configuration of
** the SQLite library at run-time.
**
** This routine should only be called when there are no outstanding
** database connections or memory allocations.  This routine is not
** threadsafe.  Failure to heed these warnings can lead to unpredictable
** behavior.
*/
int sqlitex_config(int op, ...){
  va_list ap;
  int rc = SQLITE_OK;

  /* sqlitex_config() shall return SQLITE_MISUSE if it is invoked while
  ** the SQLite library is in use. */
  if( sqlitexGlobalConfig.isInit ) return SQLITE_MISUSE_BKPT;

  va_start(ap, op);
  switch( op ){

    /* Mutex configuration options are only available in a threadsafe
    ** compile.
    */
#if defined(SQLITE_THREADSAFE) && SQLITE_THREADSAFE>0  /* IMP: R-54466-46756 */
    case SQLITE_CONFIG_SINGLETHREAD: {
      /* EVIDENCE-OF: R-02748-19096 This option sets the threading mode to
      ** Single-thread. */
      sqlitexGlobalConfig.bCoreMutex = 0;  /* Disable mutex on core */
      sqlitexGlobalConfig.bFullMutex = 0;  /* Disable mutex on connections */
      break;
    }
#endif
#if defined(SQLITE_THREADSAFE) && SQLITE_THREADSAFE>0 /* IMP: R-20520-54086 */
    case SQLITE_CONFIG_MULTITHREAD: {
      /* EVIDENCE-OF: R-14374-42468 This option sets the threading mode to
      ** Multi-thread. */
      sqlitexGlobalConfig.bCoreMutex = 1;  /* Enable mutex on core */
      sqlitexGlobalConfig.bFullMutex = 0;  /* Disable mutex on connections */
      break;
    }
#endif
#if defined(SQLITE_THREADSAFE) && SQLITE_THREADSAFE>0 /* IMP: R-59593-21810 */
    case SQLITE_CONFIG_SERIALIZED: {
      /* EVIDENCE-OF: R-41220-51800 This option sets the threading mode to
      ** Serialized. */
      sqlitexGlobalConfig.bCoreMutex = 1;  /* Enable mutex on core */
      sqlitexGlobalConfig.bFullMutex = 1;  /* Enable mutex on connections */
      break;
    }
#endif
#if defined(SQLITE_THREADSAFE) && SQLITE_THREADSAFE>0 /* IMP: R-63666-48755 */
    case SQLITE_CONFIG_MUTEX: {
      /* Specify an alternative mutex implementation */
      sqlitexGlobalConfig.mutex = *va_arg(ap, sqlitex_mutex_methods*);
      break;
    }
#endif
#if defined(SQLITE_THREADSAFE) && SQLITE_THREADSAFE>0 /* IMP: R-14450-37597 */
    case SQLITE_CONFIG_GETMUTEX: {
      /* Retrieve the current mutex implementation */
      *va_arg(ap, sqlitex_mutex_methods*) = sqlitexGlobalConfig.mutex;
      break;
    }
#endif

    case SQLITE_CONFIG_MALLOC: {
      /* EVIDENCE-OF: R-55594-21030 The SQLITE_CONFIG_MALLOC option takes a
      ** single argument which is a pointer to an instance of the
      ** sqlitex_mem_methods structure. The argument specifies alternative
      ** low-level memory allocation routines to be used in place of the memory
      ** allocation routines built into SQLite. */
      sqlitexGlobalConfig.m = *va_arg(ap, sqlitex_mem_methods*);
      break;
    }
    case SQLITE_CONFIG_GETMALLOC: {
      /* EVIDENCE-OF: R-51213-46414 The SQLITE_CONFIG_GETMALLOC option takes a
      ** single argument which is a pointer to an instance of the
      ** sqlitex_mem_methods structure. The sqlitex_mem_methods structure is
      ** filled with the currently defined memory allocation routines. */
      if( sqlitexGlobalConfig.m.xMalloc==0 ) sqlitexMemSetDefault();
      *va_arg(ap, sqlitex_mem_methods*) = sqlitexGlobalConfig.m;
      break;
    }
    case SQLITE_CONFIG_MEMSTATUS: {
      /* EVIDENCE-OF: R-61275-35157 The SQLITE_CONFIG_MEMSTATUS option takes
      ** single argument of type int, interpreted as a boolean, which enables
      ** or disables the collection of memory allocation statistics. */
      sqlitexGlobalConfig.bMemstat = va_arg(ap, int);
      break;
    }
    case SQLITE_CONFIG_SCRATCH: {
      /* EVIDENCE-OF: R-08404-60887 There are three arguments to
      ** SQLITE_CONFIG_SCRATCH: A pointer an 8-byte aligned memory buffer from
      ** which the scratch allocations will be drawn, the size of each scratch
      ** allocation (sz), and the maximum number of scratch allocations (N). */
      sqlitexGlobalConfig.pScratch = va_arg(ap, void*);
      sqlitexGlobalConfig.szScratch = va_arg(ap, int);
      sqlitexGlobalConfig.nScratch = va_arg(ap, int);
      break;
    }
    case SQLITE_CONFIG_PAGECACHE: {
      /* EVIDENCE-OF: R-31408-40510 There are three arguments to
      ** SQLITE_CONFIG_PAGECACHE: A pointer to 8-byte aligned memory, the size
      ** of each page buffer (sz), and the number of pages (N). */
      sqlitexGlobalConfig.pPage = va_arg(ap, void*);
      sqlitexGlobalConfig.szPage = va_arg(ap, int);
      sqlitexGlobalConfig.nPage = va_arg(ap, int);
      break;
    }
    case SQLITE_CONFIG_PCACHE_HDRSZ: {
      /* EVIDENCE-OF: R-39100-27317 The SQLITE_CONFIG_PCACHE_HDRSZ option takes
      ** a single parameter which is a pointer to an integer and writes into
      ** that integer the number of extra bytes per page required for each page
      ** in SQLITE_CONFIG_PAGECACHE. */
      *va_arg(ap, int*) = 
          sqlitexHeaderSizeBtree() +
          sqlitexHeaderSizePcache() +
          sqlitexHeaderSizePcache1();
      break;
    }

    case SQLITE_CONFIG_PCACHE: {
      /* no-op */
      break;
    }
    case SQLITE_CONFIG_GETPCACHE: {
      /* now an error */
      rc = SQLITE_ERROR;
      break;
    }

    case SQLITE_CONFIG_PCACHE2: {
      /* EVIDENCE-OF: R-63325-48378 The SQLITE_CONFIG_PCACHE2 option takes a
      ** single argument which is a pointer to an sqlitex_pcache_methods2
      ** object. This object specifies the interface to a custom page cache
      ** implementation. */
      sqlitexGlobalConfig.pcache2 = *va_arg(ap, sqlitex_pcache_methods2*);
      break;
    }
    case SQLITE_CONFIG_GETPCACHE2: {
      /* EVIDENCE-OF: R-22035-46182 The SQLITE_CONFIG_GETPCACHE2 option takes a
      ** single argument which is a pointer to an sqlitex_pcache_methods2
      ** object. SQLite copies of the current page cache implementation into
      ** that object. */
      if( sqlitexGlobalConfig.pcache2.xInit==0 ){
        sqlitexPCacheSetDefault();
      }
      *va_arg(ap, sqlitex_pcache_methods2*) = sqlitexGlobalConfig.pcache2;
      break;
    }

/* EVIDENCE-OF: R-06626-12911 The SQLITE_CONFIG_HEAP option is only
** available if SQLite is compiled with either SQLITE_ENABLE_MEMSYS3 or
** SQLITE_ENABLE_MEMSYS5 and returns SQLITE_ERROR if invoked otherwise. */
#if defined(SQLITE_ENABLE_MEMSYS3) || defined(SQLITE_ENABLE_MEMSYS5)
    case SQLITE_CONFIG_HEAP: {
      /* EVIDENCE-OF: R-19854-42126 There are three arguments to
      ** SQLITE_CONFIG_HEAP: An 8-byte aligned pointer to the memory, the
      ** number of bytes in the memory buffer, and the minimum allocation size.
      */
      sqlitexGlobalConfig.pHeap = va_arg(ap, void*);
      sqlitexGlobalConfig.nHeap = va_arg(ap, int);
      sqlitexGlobalConfig.mnReq = va_arg(ap, int);

      if( sqlitexGlobalConfig.mnReq<1 ){
        sqlitexGlobalConfig.mnReq = 1;
      }else if( sqlitexGlobalConfig.mnReq>(1<<12) ){
        /* cap min request size at 2^12 */
        sqlitexGlobalConfig.mnReq = (1<<12);
      }

      if( sqlitexGlobalConfig.pHeap==0 ){
        /* EVIDENCE-OF: R-49920-60189 If the first pointer (the memory pointer)
        ** is NULL, then SQLite reverts to using its default memory allocator
        ** (the system malloc() implementation), undoing any prior invocation of
        ** SQLITE_CONFIG_MALLOC.
        **
        ** Setting sqlitexGlobalConfig.m to all zeros will cause malloc to
        ** revert to its default implementation when sqlitex_initialize() is run
        */
        memset(&sqlitexGlobalConfig.m, 0, sizeof(sqlitexGlobalConfig.m));
      }else{
        /* EVIDENCE-OF: R-61006-08918 If the memory pointer is not NULL then the
        ** alternative memory allocator is engaged to handle all of SQLites
        ** memory allocation needs. */
#ifdef SQLITE_ENABLE_MEMSYS3
        sqlitexGlobalConfig.m = *sqlitexMemGetMemsys3();
#endif
#ifdef SQLITE_ENABLE_MEMSYS5
        sqlitexGlobalConfig.m = *sqlitexMemGetMemsys5();
#endif
      }
      break;
    }
#endif

    case SQLITE_CONFIG_LOOKASIDE: {
      sqlitexGlobalConfig.szLookaside = va_arg(ap, int);
      sqlitexGlobalConfig.nLookaside = va_arg(ap, int);
      break;
    }
    
    /* Record a pointer to the logger function and its first argument.
    ** The default is NULL.  Logging is disabled if the function pointer is
    ** NULL.
    */
    case SQLITE_CONFIG_LOG: {
      /* MSVC is picky about pulling func ptrs from va lists.
      ** http://support.microsoft.com/kb/47961
      ** sqlitexGlobalConfig.xLog = va_arg(ap, void(*)(void*,int,const char*));
      */
      typedef void(*LOGFUNC_t)(void*,int,const char*);
      sqlitexGlobalConfig.xLog = va_arg(ap, LOGFUNC_t);
      sqlitexGlobalConfig.pLogArg = va_arg(ap, void*);
      break;
    }

    /* EVIDENCE-OF: R-55548-33817 The compile-time setting for URI filenames
    ** can be changed at start-time using the
    ** sqlitex_config(SQLITE_CONFIG_URI,1) or
    ** sqlitex_config(SQLITE_CONFIG_URI,0) configuration calls.
    */
    case SQLITE_CONFIG_URI: {
      /* EVIDENCE-OF: R-25451-61125 The SQLITE_CONFIG_URI option takes a single
      ** argument of type int. If non-zero, then URI handling is globally
      ** enabled. If the parameter is zero, then URI handling is globally
      ** disabled. */
      sqlitexGlobalConfig.bOpenUri = va_arg(ap, int);
      break;
    }

    case SQLITE_CONFIG_COVERING_INDEX_SCAN: {
      /* EVIDENCE-OF: R-36592-02772 The SQLITE_CONFIG_COVERING_INDEX_SCAN
      ** option takes a single integer argument which is interpreted as a
      ** boolean in order to enable or disable the use of covering indices for
      ** full table scans in the query optimizer. */
      sqlitexGlobalConfig.bUseCis = va_arg(ap, int);
      break;
    }

#ifdef SQLITE_ENABLE_SQLLOG
    case SQLITE_CONFIG_SQLLOG: {
      typedef void(*SQLLOGFUNC_t)(void*, sqlitex*, const char*, int);
      sqlitexGlobalConfig.xSqllog = va_arg(ap, SQLLOGFUNC_t);
      sqlitexGlobalConfig.pSqllogArg = va_arg(ap, void *);
      break;
    }
#endif

    case SQLITE_CONFIG_MMAP_SIZE: {
      /* EVIDENCE-OF: R-58063-38258 SQLITE_CONFIG_MMAP_SIZE takes two 64-bit
      ** integer (sqlitex_int64) values that are the default mmap size limit
      ** (the default setting for PRAGMA mmap_size) and the maximum allowed
      ** mmap size limit. */
      sqlitex_int64 szMmap = va_arg(ap, sqlitex_int64);
      sqlitex_int64 mxMmap = va_arg(ap, sqlitex_int64);
      /* EVIDENCE-OF: R-53367-43190 If either argument to this option is
      ** negative, then that argument is changed to its compile-time default.
      **
      ** EVIDENCE-OF: R-34993-45031 The maximum allowed mmap size will be
      ** silently truncated if necessary so that it does not exceed the
      ** compile-time maximum mmap size set by the SQLITE_MAX_MMAP_SIZE
      ** compile-time option.
      */
      if( mxMmap<0 || mxMmap>SQLITE_MAX_MMAP_SIZE ){
        mxMmap = SQLITE_MAX_MMAP_SIZE;
      }
      if( szMmap<0 ) szMmap = SQLITE_DEFAULT_MMAP_SIZE;
      if( szMmap>mxMmap) szMmap = mxMmap;
      sqlitexGlobalConfig.mxMmap = mxMmap;
      sqlitexGlobalConfig.szMmap = szMmap;
      break;
    }

#if SQLITE_OS_WIN && defined(SQLITE_WIN32_MALLOC) /* IMP: R-04780-55815 */
    case SQLITE_CONFIG_WIN32_HEAPSIZE: {
      /* EVIDENCE-OF: R-34926-03360 SQLITE_CONFIG_WIN32_HEAPSIZE takes a 32-bit
      ** unsigned integer value that specifies the maximum size of the created
      ** heap. */
      sqlitexGlobalConfig.nHeap = va_arg(ap, int);
      break;
    }
#endif

    case SQLITE_CONFIG_PMASZ: {
      sqlitexGlobalConfig.szPma = va_arg(ap, unsigned int);
      break;
    }

    default: {
      rc = SQLITE_ERROR;
      break;
    }
  }
  va_end(ap);
  return rc;
}

/*
** Set up the lookaside buffers for a database connection.
** Return SQLITE_OK on success.  
** If lookaside is already active, return SQLITE_BUSY.
**
** The sz parameter is the number of bytes in each lookaside slot.
** The cnt parameter is the number of slots.  If pStart is NULL the
** space for the lookaside memory is obtained from sqlitex_malloc().
** If pStart is not NULL then it is sz*cnt bytes of memory to use for
** the lookaside memory.
*/
static int setupLookaside(sqlitex *db, void *pBuf, int sz, int cnt){
  void *pStart;
  if( db->lookaside.nOut ){
    return SQLITE_BUSY;
  }
  /* Free any existing lookaside buffer for this handle before
  ** allocating a new one so we don't have to have space for 
  ** both at the same time.
  */
  if( db->lookaside.bMalloced ){
    sqlitex_free(db->lookaside.pStart);
  }
  /* The size of a lookaside slot after ROUNDDOWN8 needs to be larger
  ** than a pointer to be useful.
  */
  sz = ROUNDDOWN8(sz);  /* IMP: R-33038-09382 */
  if( sz<=(int)sizeof(LookasideSlot*) ) sz = 0;
  if( cnt<0 ) cnt = 0;
  if( sz==0 || cnt==0 ){
    sz = 0;
    pStart = 0;
  }else if( pBuf==0 ){
    sqlitexBeginBenignMalloc();
    pStart = sqlitexMalloc( sz*cnt );  /* IMP: R-61949-35727 */
    sqlitexEndBenignMalloc();
    if( pStart ) cnt = sqlitexMallocSize(pStart)/sz;
  }else{
    pStart = pBuf;
  }
  db->lookaside.pStart = pStart;
  db->lookaside.pFree = 0;
  db->lookaside.sz = (u16)sz;
  if( pStart ){
    int i;
    LookasideSlot *p;
    assert( sz > (int)sizeof(LookasideSlot*) );
    p = (LookasideSlot*)pStart;
    for(i=cnt-1; i>=0; i--){
      p->pNext = db->lookaside.pFree;
      db->lookaside.pFree = p;
      p = (LookasideSlot*)&((u8*)p)[sz];
    }
    db->lookaside.pEnd = p;
    db->lookaside.bEnabled = 1;
    db->lookaside.bMalloced = pBuf==0 ?1:0;
  }else{
    db->lookaside.pStart = db;
    db->lookaside.pEnd = db;
    db->lookaside.bEnabled = 0;
    db->lookaside.bMalloced = 0;
  }
  return SQLITE_OK;
}

/*
** Return the mutex associated with a database connection.
*/
sqlitex_mutex *sqlitex_db_mutex(sqlitex *db){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
  return db->mutex;
}

/*
** Free up as much memory as we can from the given database
** connection.
*/
int sqlitex_db_release_memory(sqlitex *db){
  int i;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif
  sqlitex_mutex_enter(db->mutex);
  sqlite3BtreeEnterAll(db);
  for(i=0; i<db->nDb; i++){
    Btree *pBt = db->aDb[i].pBt;
    if( pBt ){
      Pager *pPager = sqlite3BtreePager(pBt);
      sqlite3PagerShrink(pPager);
    }
  }
  sqlite3BtreeLeaveAll(db);
  sqlitex_mutex_leave(db->mutex);
  return SQLITE_OK;
}

/*
** Configuration settings for an individual database connection
*/
int sqlitex_db_config(sqlitex *db, int op, ...){
  va_list ap;
  int rc;
  va_start(ap, op);
  switch( op ){
    case SQLITE_DBCONFIG_LOOKASIDE: {
      void *pBuf = va_arg(ap, void*); /* IMP: R-26835-10964 */
      int sz = va_arg(ap, int);       /* IMP: R-47871-25994 */
      int cnt = va_arg(ap, int);      /* IMP: R-04460-53386 */
      rc = setupLookaside(db, pBuf, sz, cnt);
      break;
    }
    default: {
      static const struct {
        int op;      /* The opcode */
        u32 mask;    /* Mask of the bit in sqlitex.flags to set/clear */
      } aFlagOp[] = {
        { SQLITE_DBCONFIG_ENABLE_FKEY,    SQLITE_ForeignKeys    },
        { SQLITE_DBCONFIG_ENABLE_TRIGGER, SQLITE_EnableTrigger  },
      };
      unsigned int i;
      rc = SQLITE_ERROR; /* IMP: R-42790-23372 */
      for(i=0; i<ArraySize(aFlagOp); i++){
        if( aFlagOp[i].op==op ){
          int onoff = va_arg(ap, int);
          int *pRes = va_arg(ap, int*);
          int oldFlags = db->flags;
          if( onoff>0 ){
            db->flags |= aFlagOp[i].mask;
          }else if( onoff==0 ){
            db->flags &= ~aFlagOp[i].mask;
          }
          if( oldFlags!=db->flags ){
            sqlitexExpirePreparedStatements(db);
          }
          if( pRes ){
            *pRes = (db->flags & aFlagOp[i].mask)!=0;
          }
          rc = SQLITE_OK;
          break;
        }
      }
      break;
    }
  }
  va_end(ap);
  return rc;
}


/*
** Return true if the buffer z[0..n-1] contains all spaces.
*/
static int allSpaces(const char *z, int n){
  while( n>0 && z[n-1]==' ' ){ n--; }
  return n==0;
}

/*
** This is the default collating function named "BINARY" which is always
** available.
**
** If the padFlag argument is not NULL then space padding at the end
** of strings is ignored.  This implements the RTRIM collation.
*/
static int binCollFunc(
  void *padFlag,
  int nKey1, const void *pKey1,
  int nKey2, const void *pKey2
){
  int rc, n;
  n = nKey1<nKey2 ? nKey1 : nKey2;
  /* EVIDENCE-OF: R-65033-28449 The built-in BINARY collation compares
  ** strings byte by byte using the memcmp() function from the standard C
  ** library. */
  rc = memcmp(pKey1, pKey2, n);
  if( rc==0 ){
    if( padFlag
     && allSpaces(((char*)pKey1)+n, nKey1-n)
     && allSpaces(((char*)pKey2)+n, nKey2-n)
    ){
      /* EVIDENCE-OF: R-31624-24737 RTRIM is like BINARY except that extra
      ** spaces at the end of either string do not change the result. In other
      ** words, strings will compare equal to one another as long as they
      ** differ only in the number of spaces at the end.
      */
    }else{
      rc = nKey1 - nKey2;
    }
  }
  return rc;
}

/*
** Another built-in collating sequence: NOCASE. 
**
** This collating sequence is intended to be used for "case independent
** comparison". SQLite's knowledge of upper and lower case equivalents
** extends only to the 26 characters used in the English language.
**
** At the moment there is only a UTF-8 implementation.
*/
static int nocaseCollatingFunc(
  void *NotUsed,
  int nKey1, const void *pKey1,
  int nKey2, const void *pKey2
){
  int r = sqlitexStrNICmp(
      (const char *)pKey1, (const char *)pKey2, (nKey1<nKey2)?nKey1:nKey2);
  UNUSED_PARAMETER(NotUsed);
  if( 0==r ){
    r = nKey1-nKey2;
  }
  return r;
}

/*
** Dummy collating sequence used by datacopy index
*/
static int datacopyCollatingFunc(
  void *NotUsed,
  int nKey1, const void *pKey1,
  int nKey2, const void *pKey2
){
  return 0;
}

/*
** Return the ROWID of the most recent insert
*/
sqlite_int64 sqlitex_last_insert_rowid(sqlitex *db){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
  return db->lastRowid;
}

/*
** Return the number of changes in the most recent call to sqlitex_exec().
*/
int sqlitex_changes(sqlitex *db){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
  return db->nChange;
}

/*
** Return the number of changes since the database handle was opened.
*/
int sqlitex_total_changes(sqlitex *db){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
  return db->nTotalChange;
}

/*
** Close all open savepoints. This function only manipulates fields of the
** database handle object, it does not close any savepoints that may be open
** at the b-tree/pager level.
*/
void sqlitexCloseSavepoints(sqlitex *db){
  while( db->pSavepoint ){
    Savepoint *pTmp = db->pSavepoint;
    db->pSavepoint = pTmp->pNext;
    sqlitexDbFree(db, pTmp);
  }
  db->nSavepoint = 0;
  db->nStatement = 0;
  db->isTransactionSavepoint = 0;
}

/*
** Invoke the destructor function associated with FuncDef p, if any. Except,
** if this is not the last copy of the function, do not invoke it. Multiple
** copies of a single function are created when create_function() is called
** with SQLITE_ANY as the encoding.
*/
static void functionDestroy(sqlitex *db, FuncDef *p){
  FuncDestructor *pDestructor = p->pDestructor;
  if( pDestructor ){
    pDestructor->nRef--;
    if( pDestructor->nRef==0 ){
      pDestructor->xDestroy(pDestructor->pUserData);
      sqlitexDbFree(db, pDestructor);
    }
  }
}

/*
** Disconnect all sqlitex_vtab objects that belong to database connection
** db. This is called when db is being closed.
*/
static void disconnectAllVtab(sqlitex *db){
#ifndef SQLITE_OMIT_VIRTUALTABLE
  int i;
  HashElem *p;
  sqlite3BtreeEnterAll(db);
  for(i=0; i<db->nDb; i++){
    Schema *pSchema = db->aDb[i].pSchema;
    if( db->aDb[i].pSchema ){
      for(p=sqliteHashFirst(&pSchema->tblHash); p; p=sqliteHashNext(p)){
        Table *pTab = (Table *)sqliteHashData(p);
        if( IsVirtual(pTab) ) sqlitexVtabDisconnect(db, pTab);
      }
    }
  }
  for(p=sqliteHashFirst(&db->aModule); p; p=sqliteHashNext(p)){
    Module *pMod = (Module *)sqliteHashData(p);
    if( pMod->pEpoTab ){
      sqlitexVtabDisconnect(db, pMod->pEpoTab);
    }
  }
  sqlitexVtabUnlockList(db);
  sqlite3BtreeLeaveAll(db);
#else
  UNUSED_PARAMETER(db);
#endif
}

/*
** Return TRUE if database connection db has unfinalized prepared
** statements or unfinished sqlitex_backup objects.  
*/
static int connectionIsBusy(sqlitex *db){
  int j;
  assert( sqlitex_mutex_held(db->mutex) );
  if( db->pVdbe ) return 1;
  for(j=0; j<db->nDb; j++){
    Btree *pBt = db->aDb[j].pBt;
    if( pBt && sqlite3BtreeIsInBackup(pBt) ) return 1;
  }
  return 0;
}

/*
** Close an existing SQLite database
*/
static int sqlitexClose(sqlitex *db, int forceZombie){
  if( !db ){
    /* EVIDENCE-OF: R-63257-11740 Calling sqlitex_close() or
    ** sqlitex_close_v2() with a NULL pointer argument is a harmless no-op. */
    return SQLITE_OK;
  }
  if( !sqlitexSafetyCheckSickOrOk(db) ){
    return SQLITE_MISUSE_BKPT;
  }
  sqlitex_mutex_enter(db->mutex);

  /* Force xDisconnect calls on all virtual tables */
  disconnectAllVtab(db);

  /* If a transaction is open, the disconnectAllVtab() call above
  ** will not have called the xDisconnect() method on any virtual
  ** tables in the db->aVTrans[] array. The following sqlitexVtabRollback()
  ** call will do so. We need to do this before the check for active
  ** SQL statements below, as the v-table implementation may be storing
  ** some prepared statements internally.
  */
  sqlitexVtabRollback(db);

  /* Legacy behavior (sqlitex_close() behavior) is to return
  ** SQLITE_BUSY if the connection can not be closed immediately.
  */
  if( !forceZombie && connectionIsBusy(db) ){
    /* COMDB2 MODIFICATION */
    printf("%s:%d SQLITE_BUSY\n", __FILE__, __LINE__);
    cheap_stack_trace();

    sqlitexErrorWithMsg(db, SQLITE_BUSY, "unable to close due to unfinalized "
       "statements or unfinished backups");
    sqlitex_mutex_leave(db->mutex);
    return SQLITE_BUSY;
  }

#ifdef SQLITE_ENABLE_SQLLOG
  if( sqlitexGlobalConfig.xSqllog ){
    /* Closing the handle. Fourth parameter is passed the value 2. */
    sqlitexGlobalConfig.xSqllog(sqlitexGlobalConfig.pSqllogArg, db, 0, 2);
  }
#endif

  /* Convert the connection into a zombie and then close it.
  */
  db->magic = SQLITE_MAGIC_ZOMBIE;
  sqlitexLeaveMutexAndCloseZombie(db);
  return SQLITE_OK;
}

/*
** Two variations on the public interface for closing a database
** connection. The sqlitex_close() version returns SQLITE_BUSY and
** leaves the connection option if there are unfinalized prepared
** statements or unfinished sqlitex_backups.  The sqlitex_close_v2()
** version forces the connection to become a zombie if there are
** unclosed resources, and arranges for deallocation when the last
** prepare statement or sqlitex_backup closes.
*/
int sqlitex_close(sqlitex *db){ return sqlitexClose(db,0); }
int sqlitex_close_v2(sqlitex *db){ return sqlitexClose(db,1); }


/*
** Close the mutex on database connection db.
**
** Furthermore, if database connection db is a zombie (meaning that there
** has been a prior call to sqlitex_close(db) or sqlitex_close_v2(db)) and
** every sqlitex_stmt has now been finalized and every sqlitex_backup has
** finished, then free all resources.
*/
void sqlitexLeaveMutexAndCloseZombie(sqlitex *db){
  HashElem *i;                    /* Hash table iterator */
  int j;

  /* If there are outstanding sqlitex_stmt or sqlitex_backup objects
  ** or if the connection has not yet been closed by sqlitex_close_v2(),
  ** then just leave the mutex and return.
  */
  if( db->magic!=SQLITE_MAGIC_ZOMBIE || connectionIsBusy(db) ){
    sqlitex_mutex_leave(db->mutex);
    return;
  }

  /* If we reach this point, it means that the database connection has
  ** closed all sqlitex_stmt and sqlitex_backup objects and has been
  ** passed to sqlitex_close (meaning that it is a zombie).  Therefore,
  ** go ahead and free all resources.
  */

  /* If a transaction is open, roll it back. This also ensures that if
  ** any database schemas have been modified by an uncommitted transaction
  ** they are reset. And that the required b-tree mutex is held to make
  ** the pager rollback and schema reset an atomic operation. */
  sqlitexRollbackAll(db, SQLITE_OK);

  /* Free any outstanding Savepoint structures. */
  sqlitexCloseSavepoints(db);

  /* Close all database connections */
  for(j=0; j<db->nDb; j++){
    struct Db *pDb = &db->aDb[j];
    if( pDb->pBt ){
      sqlite3BtreeClose(pDb->pBt);
      pDb->pBt = 0;
      if( j!=1 ){
        pDb->pSchema = 0;
      }
    }
  }
  /* Clear the TEMP schema separately and last */
  if( db->aDb[1].pSchema ){
    sqlitexSchemaClear(db->aDb[1].pSchema);
  }
  sqlitexVtabUnlockList(db);

  /* Free up the array of auxiliary databases */
  sqlitexCollapseDatabaseArray(db);
  assert( db->nDb<=2 );
  assert( db->aDb==db->aDbStatic );

  /* Tell the code in notify.c that the connection no longer holds any
  ** locks and does not require any further unlock-notify callbacks.
  */
  sqlitexConnectionClosed(db);

  for(j=0; j<ArraySize(db->aFunc.a); j++){
    FuncDef *pNext, *pHash, *p;
    for(p=db->aFunc.a[j]; p; p=pHash){
      pHash = p->pHash;
      while( p ){
        functionDestroy(db, p);
        pNext = p->pNext;
        sqlitexDbFree(db, p);
        p = pNext;
      }
    }
  }
  for(i=sqliteHashFirst(&db->aCollSeq); i; i=sqliteHashNext(i)){
    CollSeq *pColl = (CollSeq *)sqliteHashData(i);
    /* Invoke any destructors registered for collation sequence user data. */
    for(j=0; j<3; j++){
      if( pColl[j].xDel ){
        pColl[j].xDel(pColl[j].pUser);
      }
    }
    sqlitexDbFree(db, pColl);
  }
  sqlitexHashClear(&db->aCollSeq);
#ifndef SQLITE_OMIT_VIRTUALTABLE
  for(i=sqliteHashFirst(&db->aModule); i; i=sqliteHashNext(i)){
    Module *pMod = (Module *)sqliteHashData(i);
    if( pMod->xDestroy ){
      pMod->xDestroy(pMod->pAux);
    }
    sqlitexVtabEponymousTableClear(db, pMod);
    sqlitexDbFree(db, pMod);
  }
  sqlitexHashClear(&db->aModule);
#endif

  sqlitexError(db, SQLITE_OK); /* Deallocates any cached error strings. */
  sqlitexValueFree(db->pErr);
  sqlitexCloseExtensions(db);
#if SQLITE_USER_AUTHENTICATION
  sqlitex_free(db->auth.zAuthUser);
  sqlitex_free(db->auth.zAuthPW);
#endif

  db->magic = SQLITE_MAGIC_ERROR;

  /* The temp-database schema is allocated differently from the other schema
  ** objects (using sqliteMalloc() directly, instead of sqlite3BtreeSchema()).
  ** So it needs to be freed here. Todo: Why not roll the temp schema into
  ** the same sqliteMalloc() as the one that allocates the database 
  ** structure?
  */
  sqlitexDbFree(db, db->aDb[1].pSchema);
  sqlitex_mutex_leave(db->mutex);
  db->magic = SQLITE_MAGIC_CLOSED;
  sqlitex_mutex_free(db->mutex);
  assert( db->lookaside.nOut==0 );  /* Fails on a lookaside memory leak */
  if( db->lookaside.bMalloced ){
    sqlitex_free(db->lookaside.pStart);
  }
  sqlitex_free(db);
}

/*
** Rollback all database files.  If tripCode is not SQLITE_OK, then
** any write cursors are invalidated ("tripped" - as in "tripping a circuit
** breaker") and made to return tripCode if there are any further
** attempts to use that cursor.  Read cursors remain open and valid
** but are "saved" in case the table pages are moved around.
*/
void sqlitexRollbackAll(sqlitex *db, int tripCode){
  int i;
  int inTrans = 0;
  int schemaChange;
  assert( sqlitex_mutex_held(db->mutex) );
  sqlitexBeginBenignMalloc();

  /* Obtain all b-tree mutexes before making any calls to BtreeRollback(). 
  ** This is important in case the transaction being rolled back has
  ** modified the database schema. If the b-tree mutexes are not taken
  ** here, then another shared-cache connection might sneak in between
  ** the database rollback and schema reset, which can cause false
  ** corruption reports in some cases.  */
  sqlite3BtreeEnterAll(db);
  schemaChange = (db->flags & SQLITE_InternChanges)!=0 && db->init.busy==0;

  for(i=0; i<db->nDb; i++){
    Btree *p = db->aDb[i].pBt;
    if( p ){
      if( sqlite3BtreeIsInTrans(p) ){
        inTrans = 1;
      }
      sqlite3BtreeRollback(p, tripCode, !schemaChange);
    }
  }
  sqlitexVtabRollback(db);
  sqlitexEndBenignMalloc();

  if( (db->flags&SQLITE_InternChanges)!=0 && db->init.busy==0 ){
    sqlitexExpirePreparedStatements(db);
    sqlitexResetAllSchemasOfConnection(db);
  }
  sqlite3BtreeLeaveAll(db);

  /* Any deferred constraint violations have now been resolved. */
  db->nDeferredCons = 0;
  db->nDeferredImmCons = 0;
  db->flags &= ~SQLITE_DeferFKs;

  /* If one has been configured, invoke the rollback-hook callback */
  if( db->xRollbackCallback && (inTrans || !db->autoCommit) ){
    db->xRollbackCallback(db->pRollbackArg);
  }
}

/*
** Return a static string containing the name corresponding to the error code
** specified in the argument.
*/
#if (defined(SQLITE_DEBUG) && SQLITE_OS_WIN) || defined(SQLITE_TEST)
const char *sqlitexErrName(int rc){
  const char *zName = 0;
  int i, origRc = rc;
  for(i=0; i<2 && zName==0; i++, rc &= 0xff){
    switch( rc ){
      case SQLITE_OK:                 zName = "SQLITE_OK";                break;
      case SQLITE_ERROR:              zName = "SQLITE_ERROR";             break;
      case SQLITE_INTERNAL:           zName = "SQLITE_INTERNAL";          break;
      case SQLITE_PERM:               zName = "SQLITE_PERM";              break;
      case SQLITE_ABORT:              zName = "SQLITE_ABORT";             break;
      case SQLITE_ABORT_ROLLBACK:     zName = "SQLITE_ABORT_ROLLBACK";    break;
      case SQLITE_BUSY:               zName = "SQLITE_BUSY";              break;
      case SQLITE_BUSY_RECOVERY:      zName = "SQLITE_BUSY_RECOVERY";     break;
      case SQLITE_BUSY_SNAPSHOT:      zName = "SQLITE_BUSY_SNAPSHOT";     break;
      case SQLITE_LOCKED:             zName = "SQLITE_LOCKED";            break;
      case SQLITE_LOCKED_SHAREDCACHE: zName = "SQLITE_LOCKED_SHAREDCACHE";break;
      case SQLITE_NOMEM:              zName = "SQLITE_NOMEM";             break;
      case SQLITE_READONLY:           zName = "SQLITE_READONLY";          break;
      case SQLITE_READONLY_RECOVERY:  zName = "SQLITE_READONLY_RECOVERY"; break;
      case SQLITE_READONLY_CANTLOCK:  zName = "SQLITE_READONLY_CANTLOCK"; break;
      case SQLITE_READONLY_ROLLBACK:  zName = "SQLITE_READONLY_ROLLBACK"; break;
      case SQLITE_READONLY_DBMOVED:   zName = "SQLITE_READONLY_DBMOVED";  break;
      case SQLITE_INTERRUPT:          zName = "SQLITE_INTERRUPT";         break;
      case SQLITE_IOERR:              zName = "SQLITE_IOERR";             break;
      case SQLITE_IOERR_READ:         zName = "SQLITE_IOERR_READ";        break;
      case SQLITE_IOERR_SHORT_READ:   zName = "SQLITE_IOERR_SHORT_READ";  break;
      case SQLITE_IOERR_WRITE:        zName = "SQLITE_IOERR_WRITE";       break;
      case SQLITE_IOERR_FSYNC:        zName = "SQLITE_IOERR_FSYNC";       break;
      case SQLITE_IOERR_DIR_FSYNC:    zName = "SQLITE_IOERR_DIR_FSYNC";   break;
      case SQLITE_IOERR_TRUNCATE:     zName = "SQLITE_IOERR_TRUNCATE";    break;
      case SQLITE_IOERR_FSTAT:        zName = "SQLITE_IOERR_FSTAT";       break;
      case SQLITE_IOERR_UNLOCK:       zName = "SQLITE_IOERR_UNLOCK";      break;
      case SQLITE_IOERR_RDLOCK:       zName = "SQLITE_IOERR_RDLOCK";      break;
      case SQLITE_IOERR_DELETE:       zName = "SQLITE_IOERR_DELETE";      break;
      case SQLITE_IOERR_NOMEM:        zName = "SQLITE_IOERR_NOMEM";       break;
      case SQLITE_IOERR_ACCESS:       zName = "SQLITE_IOERR_ACCESS";      break;
      case SQLITE_IOERR_CHECKRESERVEDLOCK:
                                zName = "SQLITE_IOERR_CHECKRESERVEDLOCK"; break;
      case SQLITE_IOERR_LOCK:         zName = "SQLITE_IOERR_LOCK";        break;
      case SQLITE_IOERR_CLOSE:        zName = "SQLITE_IOERR_CLOSE";       break;
      case SQLITE_IOERR_DIR_CLOSE:    zName = "SQLITE_IOERR_DIR_CLOSE";   break;
      case SQLITE_IOERR_SHMOPEN:      zName = "SQLITE_IOERR_SHMOPEN";     break;
      case SQLITE_IOERR_SHMSIZE:      zName = "SQLITE_IOERR_SHMSIZE";     break;
      case SQLITE_IOERR_SHMLOCK:      zName = "SQLITE_IOERR_SHMLOCK";     break;
      case SQLITE_IOERR_SHMMAP:       zName = "SQLITE_IOERR_SHMMAP";      break;
      case SQLITE_IOERR_SEEK:         zName = "SQLITE_IOERR_SEEK";        break;
      case SQLITE_IOERR_DELETE_NOENT: zName = "SQLITE_IOERR_DELETE_NOENT";break;
      case SQLITE_IOERR_MMAP:         zName = "SQLITE_IOERR_MMAP";        break;
      case SQLITE_IOERR_GETTEMPPATH:  zName = "SQLITE_IOERR_GETTEMPPATH"; break;
      case SQLITE_IOERR_CONVPATH:     zName = "SQLITE_IOERR_CONVPATH";    break;
      case SQLITE_CORRUPT:            zName = "SQLITE_CORRUPT";           break;
      case SQLITE_CORRUPT_VTAB:       zName = "SQLITE_CORRUPT_VTAB";      break;
      case SQLITE_NOTFOUND:           zName = "SQLITE_NOTFOUND";          break;
      case SQLITE_FULL:               zName = "SQLITE_FULL";              break;
      case SQLITE_CANTOPEN:           zName = "SQLITE_CANTOPEN";          break;
      case SQLITE_CANTOPEN_NOTEMPDIR: zName = "SQLITE_CANTOPEN_NOTEMPDIR";break;
      case SQLITE_CANTOPEN_ISDIR:     zName = "SQLITE_CANTOPEN_ISDIR";    break;
      case SQLITE_CANTOPEN_FULLPATH:  zName = "SQLITE_CANTOPEN_FULLPATH"; break;
      case SQLITE_CANTOPEN_CONVPATH:  zName = "SQLITE_CANTOPEN_CONVPATH"; break;
      case SQLITE_PROTOCOL:           zName = "SQLITE_PROTOCOL";          break;
      case SQLITE_EMPTY:              zName = "SQLITE_EMPTY";             break;
      case SQLITE_SCHEMA:             zName = "SQLITE_SCHEMA";            break;
      case SQLITE_TOOBIG:             zName = "SQLITE_TOOBIG";            break;
      case SQLITE_CONSTRAINT:         zName = "SQLITE_CONSTRAINT";        break;
      case SQLITE_CONSTRAINT_UNIQUE:  zName = "SQLITE_CONSTRAINT_UNIQUE"; break;
      case SQLITE_CONSTRAINT_TRIGGER: zName = "SQLITE_CONSTRAINT_TRIGGER";break;
      case SQLITE_CONSTRAINT_FOREIGNKEY:
                                zName = "SQLITE_CONSTRAINT_FOREIGNKEY";   break;
      case SQLITE_CONSTRAINT_CHECK:   zName = "SQLITE_CONSTRAINT_CHECK";  break;
      case SQLITE_CONSTRAINT_PRIMARYKEY:
                                zName = "SQLITE_CONSTRAINT_PRIMARYKEY";   break;
      case SQLITE_CONSTRAINT_NOTNULL: zName = "SQLITE_CONSTRAINT_NOTNULL";break;
      case SQLITE_CONSTRAINT_COMMITHOOK:
                                zName = "SQLITE_CONSTRAINT_COMMITHOOK";   break;
      case SQLITE_CONSTRAINT_VTAB:    zName = "SQLITE_CONSTRAINT_VTAB";   break;
      case SQLITE_CONSTRAINT_FUNCTION:
                                zName = "SQLITE_CONSTRAINT_FUNCTION";     break;
      case SQLITE_CONSTRAINT_ROWID:   zName = "SQLITE_CONSTRAINT_ROWID";  break;
      case SQLITE_MISMATCH:           zName = "SQLITE_MISMATCH";          break;
      case SQLITE_MISUSE:             zName = "SQLITE_MISUSE";            break;
      case SQLITE_NOLFS:              zName = "SQLITE_NOLFS";             break;
      case SQLITE_AUTH:               zName = "SQLITE_AUTH";              break;
      case SQLITE_FORMAT:             zName = "SQLITE_FORMAT";            break;
      case SQLITE_RANGE:              zName = "SQLITE_RANGE";             break;
      case SQLITE_NOTADB:             zName = "SQLITE_NOTADB";            break;
      case SQLITE_ROW:                zName = "SQLITE_ROW";               break;
      case SQLITE_NOTICE:             zName = "SQLITE_NOTICE";            break;
      case SQLITE_NOTICE_RECOVER_WAL: zName = "SQLITE_NOTICE_RECOVER_WAL";break;
      case SQLITE_NOTICE_RECOVER_ROLLBACK:
                                zName = "SQLITE_NOTICE_RECOVER_ROLLBACK"; break;
      case SQLITE_WARNING:            zName = "SQLITE_WARNING";           break;
      case SQLITE_WARNING_AUTOINDEX:  zName = "SQLITE_WARNING_AUTOINDEX"; break;
      case SQLITE_DONEX:               zName = "SQLITE_DONEX";              break;
    }
  }
  if( zName==0 ){
    static char zBuf[50];
    sqlitex_snprintf(sizeof(zBuf), zBuf, "SQLITE_UNKNOWN(%d)", origRc);
    zName = zBuf;
  }
  return zName;
}
#endif

/*
** Return a static string that describes the kind of error specified in the
** argument.
*/
const char *sqlitexErrStr(int rc){
  static const char* const aMsg[] = {
    /* SQLITE_OK          */ "not an error",
    /* SQLITE_ERROR       */ "SQL logic error or missing database",
    /* SQLITE_INTERNAL    */ 0,
    /* SQLITE_PERM        */ "access permission denied",
    /* SQLITE_ABORT       */ "callback requested query abort",
    /* SQLITE_BUSY        */ "database is locked",
    /* SQLITE_LOCKED      */ "database table is locked",
    /* SQLITE_NOMEM       */ "out of memory",
    /* SQLITE_READONLY    */ "attempt to write a readonly database",
    /* SQLITE_INTERRUPT   */ "interrupted",
    /* SQLITE_IOERR       */ "disk I/O error",
    /* SQLITE_CORRUPT     */ "database disk image is malformed",
    /* SQLITE_NOTFOUND    */ "unknown operation",
    /* SQLITE_FULL        */ "database or disk is full",
    /* SQLITE_CANTOPEN    */ "unable to open database file",
    /* SQLITE_PROTOCOL    */ "locking protocol",
    /* SQLITE_EMPTY       */ "table contains no data",
    /* SQLITE_SCHEMA      */ "database schema has changed",
    /* SQLITE_TOOBIG      */ "string or blob too big",
    /* SQLITE_CONSTRAINT  */ "constraint failed",
    /* SQLITE_MISMATCH    */ "datatype mismatch",
    /* SQLITE_MISUSE      */ "library routine called out of sequence",
    /* SQLITE_NOLFS       */ "large file support is disabled",
    /* SQLITE_AUTH        */ "authorization denied",
    /* SQLITE_FORMAT      */ "auxiliary database format error",
    /* SQLITE_RANGE       */ "bind or column index out of range",
    /* SQLITE_NOTADB      */ "file is encrypted or is not a database",

    /* COMDB2 MODIFICATION */
    /* SQLITE_DEADLOCK    */ "transaction has been aborted due to deadlock",
  };
  const char *zErr = "unknown error";
  switch( rc ){
    case SQLITE_ABORT_ROLLBACK: {
      zErr = "abort due to ROLLBACK";
      break;
    }
    default: {
      rc &= 0xff;
      if( ALWAYS(rc>=0) && rc<ArraySize(aMsg) && aMsg[rc]!=0 ){
        zErr = aMsg[rc];
      }
      break;
    }
  }
  return zErr;
}

/*
** This routine implements a busy callback that sleeps and tries
** again until a timeout value is reached.  The timeout value is
** an integer number of milliseconds passed in as the first
** argument.
*/
static int sqliteDefaultBusyCallback(
 void *ptr,               /* Database connection */
 int count                /* Number of times table has been busy */
){
#if SQLITE_OS_WIN || HAVE_USLEEP
  static const u8 delays[] =
     { 1, 2, 5, 10, 15, 20, 25, 25,  25,  50,  50, 100 };
  static const u8 totals[] =
     { 0, 1, 3,  8, 18, 33, 53, 78, 103, 128, 178, 228 };
# define NDELAY ArraySize(delays)
  sqlitex *db = (sqlitex *)ptr;
  int timeout = db->busyTimeout;
  int delay, prior;

  assert( count>=0 );
  if( count < NDELAY ){
    delay = delays[count];
    prior = totals[count];
  }else{
    delay = delays[NDELAY-1];
    prior = totals[NDELAY-1] + delay*(count-(NDELAY-1));
  }
  if( prior + delay > timeout ){
    delay = timeout - prior;
    if( delay<=0 ) return 0;
  }
  sqlitexOsSleep(db->pVfs, delay*1000);
  return 1;
#else
  sqlitex *db = (sqlitex *)ptr;
  int timeout = ((sqlitex *)ptr)->busyTimeout;
  if( (count+1)*1000 > timeout ){
    return 0;
  }
  sqlitexOsSleep(db->pVfs, 1000000);
  return 1;
#endif
}

/*
** Invoke the given busy handler.
**
** This routine is called when an operation failed with a lock.
** If this routine returns non-zero, the lock is retried.  If it
** returns 0, the operation aborts with an SQLITE_BUSY error.
*/
int sqlitexInvokeBusyHandler(BusyHandler *p){
  int rc;
  if( NEVER(p==0) || p->xFunc==0 || p->nBusy<0 ) return 0;
  rc = p->xFunc(p->pArg, p->nBusy);
  if( rc==0 ){
    p->nBusy = -1;
  }else{
    p->nBusy++;
  }
  return rc; 
}

/*
** This routine sets the busy callback for an Sqlite database to the
** given callback function with the given argument.
*/
int sqlitex_busy_handler(
  sqlitex *db,
  int (*xBusy)(void*,int),
  void *pArg
){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif
  sqlitex_mutex_enter(db->mutex);
  db->busyHandler.xFunc = xBusy;
  db->busyHandler.pArg = pArg;
  db->busyHandler.nBusy = 0;
  db->busyTimeout = 0;
  sqlitex_mutex_leave(db->mutex);
  return SQLITE_OK;
}

#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
/*
** This routine sets the progress callback for an Sqlite database to the
** given callback function with the given argument. The progress callback will
** be invoked every nOps opcodes.
*/
void sqlitex_progress_handler(
  sqlitex *db, 
  int nOps,
  int (*xProgress)(void*), 
  void *pArg
){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return;
  }
#endif
  sqlitex_mutex_enter(db->mutex);
  if( nOps>0 ){
    db->xProgress = xProgress;
    db->nProgressOps = (unsigned)nOps;
    db->pProgressArg = pArg;
  }else{
    db->xProgress = 0;
    db->nProgressOps = 0;
    db->pProgressArg = 0;
  }
  sqlitex_mutex_leave(db->mutex);
}
#endif


/*
** This routine installs a default busy handler that waits for the
** specified number of milliseconds before returning 0.
*/
int sqlitex_busy_timeout(sqlitex *db, int ms){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif
  if( ms>0 ){
    sqlitex_busy_handler(db, sqliteDefaultBusyCallback, (void*)db);
    db->busyTimeout = ms;
  }else{
    sqlitex_busy_handler(db, 0, 0);
  }
  return SQLITE_OK;
}

/*
** Cause any pending operation to stop at its earliest opportunity.
*/
void sqlitex_interrupt(sqlitex *db){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return;
  }
#endif
  db->u1.isInterrupted = 1;
}


/*
** This function is exactly the same as sqlitex_create_function(), except
** that it is designed to be called by internal code. The difference is
** that if a malloc() fails in sqlitex_create_function(), an error code
** is returned and the mallocFailed flag cleared. 
*/
int sqlitexCreateFunc(
  sqlitex *db,
  const char *zFunctionName,
  int nArg,
  int enc,
  void *pUserData,
  void (*xFunc)(sqlitex_context*,int,sqlitex_value **),
  void (*xStep)(sqlitex_context*,int,sqlitex_value **),
  void (*xFinal)(sqlitex_context*),
  FuncDestructor *pDestructor
){
  FuncDef *p;
  int nName;
  int extraFlags;

  assert( sqlitex_mutex_held(db->mutex) );
  if( zFunctionName==0 ||
      (xFunc && (xFinal || xStep)) || 
      (!xFunc && (xFinal && !xStep)) ||
      (!xFunc && (!xFinal && xStep)) ||
      (nArg<-1 || nArg>SQLITE_MAX_FUNCTION_ARG) ||
      (255<(nName = sqlitexStrlen30( zFunctionName))) ){
    return SQLITE_MISUSE_BKPT;
  }

  assert( SQLITE_FUNC_CONSTANT==SQLITE_DETERMINISTIC );
  extraFlags = enc &  SQLITE_DETERMINISTIC;
  enc &= (SQLITE_FUNC_ENCMASK|SQLITE_ANY);
  
#ifndef SQLITE_OMIT_UTF16
  /* If SQLITE_UTF16 is specified as the encoding type, transform this
  ** to one of SQLITE_UTF16LE or SQLITE_UTF16BE using the
  ** SQLITE_UTF16NATIVE macro. SQLITE_UTF16 is not used internally.
  **
  ** If SQLITE_ANY is specified, add three versions of the function
  ** to the hash table.
  */
  if( enc==SQLITE_UTF16 ){
    enc = SQLITE_UTF16NATIVE;
  }else if( enc==SQLITE_ANY ){
    int rc;
    rc = sqlitexCreateFunc(db, zFunctionName, nArg, SQLITE_UTF8|extraFlags,
         pUserData, xFunc, xStep, xFinal, pDestructor);
    if( rc==SQLITE_OK ){
      rc = sqlitexCreateFunc(db, zFunctionName, nArg, SQLITE_UTF16LE|extraFlags,
          pUserData, xFunc, xStep, xFinal, pDestructor);
    }
    if( rc!=SQLITE_OK ){
      return rc;
    }
    enc = SQLITE_UTF16BE;
  }
#else
  enc = SQLITE_UTF8;
#endif
  
  /* Check if an existing function is being overridden or deleted. If so,
  ** and there are active VMs, then return SQLITE_BUSY. If a function
  ** is being overridden/deleted but there are no active VMs, allow the
  ** operation to continue but invalidate all precompiled statements.
  */
  p = sqlitexFindFunction(db, zFunctionName, nName, nArg, (u8)enc, 0);
  if( p && (p->funcFlags & SQLITE_FUNC_ENCMASK)==enc && p->nArg==nArg ){
    if( db->nVdbeActive ){
      /* COMDB2 MODIFICATION */
      printf("%s:%d SQLITE_BUSY\n", __FILE__, __LINE__);
      cheap_stack_trace();

      sqlitexErrorWithMsg(db, SQLITE_BUSY, 
        "unable to delete/modify user-function due to active statements");
      assert( !db->mallocFailed );
      return SQLITE_BUSY;
    }else{
      sqlitexExpirePreparedStatements(db);
    }
  }

  p = sqlitexFindFunction(db, zFunctionName, nName, nArg, (u8)enc, 1);
  assert(p || db->mallocFailed);
  if( !p ){
    return SQLITE_NOMEM;
  }

  /* If an older version of the function with a configured destructor is
  ** being replaced invoke the destructor function here. */
  functionDestroy(db, p);

  if( pDestructor ){
    pDestructor->nRef++;
  }
  p->pDestructor = pDestructor;
  p->funcFlags = (p->funcFlags & SQLITE_FUNC_ENCMASK) | extraFlags;
  testcase( p->funcFlags & SQLITE_DETERMINISTIC );
  p->xFunc = xFunc;
  p->xStep = xStep;
  p->xFinalize = xFinal;
  p->pUserData = pUserData;
  p->nArg = (u16)nArg;
  return SQLITE_OK;
}

/*
** Create new user functions.
*/
int sqlitex_create_function(
  sqlitex *db,
  const char *zFunc,
  int nArg,
  int enc,
  void *p,
  void (*xFunc)(sqlitex_context*,int,sqlitex_value **),
  void (*xStep)(sqlitex_context*,int,sqlitex_value **),
  void (*xFinal)(sqlitex_context*)
){
  return sqlitex_create_function_v2(db, zFunc, nArg, enc, p, xFunc, xStep,
                                    xFinal, 0);
}

int sqlitex_create_function_v2(
  sqlitex *db,
  const char *zFunc,
  int nArg,
  int enc,
  void *p,
  void (*xFunc)(sqlitex_context*,int,sqlitex_value **),
  void (*xStep)(sqlitex_context*,int,sqlitex_value **),
  void (*xFinal)(sqlitex_context*),
  void (*xDestroy)(void *)
){
  int rc = SQLITE_ERROR;
  FuncDestructor *pArg = 0;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    return SQLITE_MISUSE_BKPT;
  }
#endif
  sqlitex_mutex_enter(db->mutex);
  if( xDestroy ){
    pArg = (FuncDestructor *)sqlitexDbMallocZero(db, sizeof(FuncDestructor));
    if( !pArg ){
      xDestroy(p);
      goto out;
    }
    pArg->xDestroy = xDestroy;
    pArg->pUserData = p;
  }
  rc = sqlitexCreateFunc(db, zFunc, nArg, enc, p, xFunc, xStep, xFinal, pArg);
  if( pArg && pArg->nRef==0 ){
    assert( rc!=SQLITE_OK );
    xDestroy(p);
    sqlitexDbFree(db, pArg);
  }

 out:
  rc = sqlitexApiExit(db, rc);
  sqlitex_mutex_leave(db->mutex);
  return rc;
}

#ifndef SQLITE_OMIT_UTF16
int sqlitex_create_function16(
  sqlitex *db,
  const void *zFunctionName,
  int nArg,
  int eTextRep,
  void *p,
  void (*xFunc)(sqlitex_context*,int,sqlitex_value**),
  void (*xStep)(sqlitex_context*,int,sqlitex_value**),
  void (*xFinal)(sqlitex_context*)
){
  int rc;
  char *zFunc8;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) || zFunctionName==0 ) return SQLITE_MISUSE_BKPT;
#endif
  sqlitex_mutex_enter(db->mutex);
  assert( !db->mallocFailed );
  zFunc8 = sqlitexUtf16to8(db, zFunctionName, -1, SQLITE_UTF16NATIVE);
  rc = sqlitexCreateFunc(db, zFunc8, nArg, eTextRep, p, xFunc, xStep, xFinal,0);
  sqlitexDbFree(db, zFunc8);
  rc = sqlitexApiExit(db, rc);
  sqlitex_mutex_leave(db->mutex);
  return rc;
}
#endif


/*
** Declare that a function has been overloaded by a virtual table.
**
** If the function already exists as a regular global function, then
** this routine is a no-op.  If the function does not exist, then create
** a new one that always throws a run-time error.  
**
** When virtual tables intend to provide an overloaded function, they
** should call this routine to make sure the global function exists.
** A global function must exist in order for name resolution to work
** properly.
*/
int sqlitex_overload_function(
  sqlitex *db,
  const char *zName,
  int nArg
){
  int nName = sqlitexStrlen30(zName);
  int rc = SQLITE_OK;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) || zName==0 || nArg<-2 ){
    return SQLITE_MISUSE_BKPT;
  }
#endif
  sqlitex_mutex_enter(db->mutex);
  if( sqlitexFindFunction(db, zName, nName, nArg, SQLITE_UTF8, 0)==0 ){
    rc = sqlitexCreateFunc(db, zName, nArg, SQLITE_UTF8,
                           0, sqlitexInvalidFunction, 0, 0, 0);
  }
  rc = sqlitexApiExit(db, rc);
  sqlitex_mutex_leave(db->mutex);
  return rc;
}

#ifndef SQLITE_OMIT_TRACE
/*
** Register a trace function.  The pArg from the previously registered trace
** is returned.  
**
** A NULL trace function means that no tracing is executes.  A non-NULL
** trace is a pointer to a function that is invoked at the start of each
** SQL statement.
*/
void *sqlitex_trace(sqlitex *db, void (*xTrace)(void*,const char*), void *pArg){
  void *pOld;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
  sqlitex_mutex_enter(db->mutex);
  pOld = db->pTraceArg;
  db->xTrace = xTrace;
  db->pTraceArg = pArg;
  sqlitex_mutex_leave(db->mutex);
  return pOld;
}
/*
** Register a profile function.  The pArg from the previously registered 
** profile function is returned.  
**
** A NULL profile function means that no profiling is executes.  A non-NULL
** profile is a pointer to a function that is invoked at the conclusion of
** each SQL statement that is run.
*/
void *sqlitex_profile(
  sqlitex *db,
  void (*xProfile)(void*,const char*,sqlite_uint64),
  void *pArg
){
  void *pOld;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
  sqlitex_mutex_enter(db->mutex);
  pOld = db->pProfileArg;
  db->xProfile = xProfile;
  db->pProfileArg = pArg;
  sqlitex_mutex_leave(db->mutex);
  return pOld;
}
#endif /* SQLITE_OMIT_TRACE */

/*
** Register a function to be invoked when a transaction commits.
** If the invoked function returns non-zero, then the commit becomes a
** rollback.
*/
void *sqlitex_commit_hook(
  sqlitex *db,              /* Attach the hook to this database */
  int (*xCallback)(void*),  /* Function to invoke on each commit */
  void *pArg                /* Argument to the function */
){
  void *pOld;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
  sqlitex_mutex_enter(db->mutex);
  pOld = db->pCommitArg;
  db->xCommitCallback = xCallback;
  db->pCommitArg = pArg;
  sqlitex_mutex_leave(db->mutex);
  return pOld;
}

/*
** Register a callback to be invoked each time a row is updated,
** inserted or deleted using this database connection.
*/
void *sqlitex_update_hook(
  sqlitex *db,              /* Attach the hook to this database */
  void (*xCallback)(void*,int,char const *,char const *,sqlite_int64),
  void *pArg                /* Argument to the function */
){
  void *pRet;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
  sqlitex_mutex_enter(db->mutex);
  pRet = db->pUpdateArg;
  db->xUpdateCallback = xCallback;
  db->pUpdateArg = pArg;
  sqlitex_mutex_leave(db->mutex);
  return pRet;
}

/*
** Register a callback to be invoked each time a transaction is rolled
** back by this database connection.
*/
void *sqlitex_rollback_hook(
  sqlitex *db,              /* Attach the hook to this database */
  void (*xCallback)(void*), /* Callback function */
  void *pArg                /* Argument to the function */
){
  void *pRet;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
  sqlitex_mutex_enter(db->mutex);
  pRet = db->pRollbackArg;
  db->xRollbackCallback = xCallback;
  db->pRollbackArg = pArg;
  sqlitex_mutex_leave(db->mutex);
  return pRet;
}

#ifndef SQLITE_OMIT_WAL
/*
** The sqlitex_wal_hook() callback registered by sqlitex_wal_autocheckpoint().
** Invoke sqlitex_wal_checkpoint if the number of frames in the log file
** is greater than sqlitex.pWalArg cast to an integer (the value configured by
** wal_autocheckpoint()).
*/ 
int sqlitexWalDefaultHook(
  void *pClientData,     /* Argument */
  sqlitex *db,           /* Connection */
  const char *zDb,       /* Database */
  int nFrame             /* Size of WAL */
){
  if( nFrame>=SQLITE_PTR_TO_INT(pClientData) ){
    sqlitexBeginBenignMalloc();
    sqlitex_wal_checkpoint(db, zDb);
    sqlitexEndBenignMalloc();
  }
  return SQLITE_OK;
}
#endif /* SQLITE_OMIT_WAL */

/*
** Configure an sqlitex_wal_hook() callback to automatically checkpoint
** a database after committing a transaction if there are nFrame or
** more frames in the log file. Passing zero or a negative value as the
** nFrame parameter disables automatic checkpoints entirely.
**
** The callback registered by this function replaces any existing callback
** registered using sqlitex_wal_hook(). Likewise, registering a callback
** using sqlitex_wal_hook() disables the automatic checkpoint mechanism
** configured by this function.
*/
int sqlitex_wal_autocheckpoint(sqlitex *db, int nFrame){
#ifdef SQLITE_OMIT_WAL
  UNUSED_PARAMETER(db);
  UNUSED_PARAMETER(nFrame);
#else
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif
  if( nFrame>0 ){
    sqlitex_wal_hook(db, sqlitexWalDefaultHook, SQLITE_INT_TO_PTR(nFrame));
  }else{
    sqlitex_wal_hook(db, 0, 0);
  }
#endif
  return SQLITE_OK;
}

/*
** Register a callback to be invoked each time a transaction is written
** into the write-ahead-log by this database connection.
*/
void *sqlitex_wal_hook(
  sqlitex *db,                    /* Attach the hook to this db handle */
  int(*xCallback)(void *, sqlitex*, const char*, int),
  void *pArg                      /* First argument passed to xCallback() */
){
#ifndef SQLITE_OMIT_WAL
  void *pRet;
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
  sqlitex_mutex_enter(db->mutex);
  pRet = db->pWalArg;
  db->xWalCallback = xCallback;
  db->pWalArg = pArg;
  sqlitex_mutex_leave(db->mutex);
  return pRet;
#else
  return 0;
#endif
}

/*
** Checkpoint database zDb.
*/
int sqlitex_wal_checkpoint_v2(
  sqlitex *db,                    /* Database handle */
  const char *zDb,                /* Name of attached database (or NULL) */
  int eMode,                      /* SQLITE_CHECKPOINT_* value */
  int *pnLog,                     /* OUT: Size of WAL log in frames */
  int *pnCkpt                     /* OUT: Total number of frames checkpointed */
){
#ifdef SQLITE_OMIT_WAL
  return SQLITE_OK;
#else
  int rc;                         /* Return code */
  int iDb = SQLITE_MAX_ATTACHED;  /* sqlitex.aDb[] index of db to checkpoint */

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif

  /* Initialize the output variables to -1 in case an error occurs. */
  if( pnLog ) *pnLog = -1;
  if( pnCkpt ) *pnCkpt = -1;

  assert( SQLITE_CHECKPOINT_PASSIVE==0 );
  assert( SQLITE_CHECKPOINT_FULL==1 );
  assert( SQLITE_CHECKPOINT_RESTART==2 );
  assert( SQLITE_CHECKPOINT_TRUNCATE==3 );
  if( eMode<SQLITE_CHECKPOINT_PASSIVE || eMode>SQLITE_CHECKPOINT_TRUNCATE ){
    /* EVIDENCE-OF: R-03996-12088 The M parameter must be a valid checkpoint
    ** mode: */
    return SQLITE_MISUSE;
  }

  sqlitex_mutex_enter(db->mutex);
  if( zDb && zDb[0] ){
    iDb = sqlitexFindDbName(db, zDb);
  }
  if( iDb<0 ){
    rc = SQLITE_ERROR;
    sqlitexErrorWithMsg(db, SQLITE_ERROR, "unknown database: %s", zDb);
  }else{
    db->busyHandler.nBusy = 0;
    rc = sqlitexCheckpoint(db, iDb, eMode, pnLog, pnCkpt);
    sqlitexError(db, rc);
  }
  rc = sqlitexApiExit(db, rc);
  sqlitex_mutex_leave(db->mutex);
  return rc;
#endif
}


/*
** Checkpoint database zDb. If zDb is NULL, or if the buffer zDb points
** to contains a zero-length string, all attached databases are 
** checkpointed.
*/
int sqlitex_wal_checkpoint(sqlitex *db, const char *zDb){
  /* EVIDENCE-OF: R-41613-20553 The sqlitex_wal_checkpoint(D,X) is equivalent to
  ** sqlitex_wal_checkpoint_v2(D,X,SQLITE_CHECKPOINT_PASSIVE,0,0). */
  return sqlitex_wal_checkpoint_v2(db,zDb,SQLITE_CHECKPOINT_PASSIVE,0,0);
}

#ifndef SQLITE_OMIT_WAL
/*
** Run a checkpoint on database iDb. This is a no-op if database iDb is
** not currently open in WAL mode.
**
** If a transaction is open on the database being checkpointed, this 
** function returns SQLITE_LOCKED and a checkpoint is not attempted. If 
** an error occurs while running the checkpoint, an SQLite error code is 
** returned (i.e. SQLITE_IOERR). Otherwise, SQLITE_OK.
**
** The mutex on database handle db should be held by the caller. The mutex
** associated with the specific b-tree being checkpointed is taken by
** this function while the checkpoint is running.
**
** If iDb is passed SQLITE_MAX_ATTACHED, then all attached databases are
** checkpointed. If an error is encountered it is returned immediately -
** no attempt is made to checkpoint any remaining databases.
**
** Parameter eMode is one of SQLITE_CHECKPOINT_PASSIVE, FULL or RESTART.
*/
int sqlitexCheckpoint(sqlitex *db, int iDb, int eMode, int *pnLog, int *pnCkpt){
  int rc = SQLITE_OK;             /* Return code */
  int i;                          /* Used to iterate through attached dbs */
  int bBusy = 0;                  /* True if SQLITE_BUSY has been encountered */

  assert( sqlitex_mutex_held(db->mutex) );
  assert( !pnLog || *pnLog==-1 );
  assert( !pnCkpt || *pnCkpt==-1 );

  for(i=0; i<db->nDb && rc==SQLITE_OK; i++){
    if( i==iDb || iDb==SQLITE_MAX_ATTACHED ){
      rc = sqlite3BtreeCheckpoint(db->aDb[i].pBt, eMode, pnLog, pnCkpt);
      pnLog = 0;
      pnCkpt = 0;
      if( rc==SQLITE_BUSY ){
        bBusy = 1;
        rc = SQLITE_OK;
      }
    }
  }

  return (rc==SQLITE_OK && bBusy) ? SQLITE_BUSY : rc;
}
#endif /* SQLITE_OMIT_WAL */

/*
** This function returns true if main-memory should be used instead of
** a temporary file for transient pager files and statement journals.
** The value returned depends on the value of db->temp_store (runtime
** parameter) and the compile time value of SQLITE_TEMP_STORE. The
** following table describes the relationship between these two values
** and this functions return value.
**
**   SQLITE_TEMP_STORE     db->temp_store     Location of temporary database
**   -----------------     --------------     ------------------------------
**   0                     any                file      (return 0)
**   1                     1                  file      (return 0)
**   1                     2                  memory    (return 1)
**   1                     0                  file      (return 0)
**   2                     1                  file      (return 0)
**   2                     2                  memory    (return 1)
**   2                     0                  memory    (return 1)
**   3                     any                memory    (return 1)
*/
int sqlitexTempInMemory(const sqlitex *db){
#if SQLITE_TEMP_STORE==1
  return ( db->temp_store==2 );
#endif
#if SQLITE_TEMP_STORE==2
  return ( db->temp_store!=1 );
#endif
#if SQLITE_TEMP_STORE==3
  return 1;
#endif
#if SQLITE_TEMP_STORE<1 || SQLITE_TEMP_STORE>3
  return 0;
#endif
}

/*
** Return UTF-8 encoded English language explanation of the most recent
** error.
*/
const char *sqlitex_errmsg(sqlitex *db){
  const char *z;
  if( !db ){
    return sqlitexErrStr(SQLITE_NOMEM);
  }
  if( !sqlitexSafetyCheckSickOrOk(db) ){
    return sqlitexErrStr(SQLITE_MISUSE_BKPT);
  }
  sqlitex_mutex_enter(db->mutex);
  if( db->mallocFailed ){
    z = sqlitexErrStr(SQLITE_NOMEM);
  }else{
    testcase( db->pErr==0 );
    z = (char*)sqlitex_value_text(db->pErr);
    assert( !db->mallocFailed );
    if( z==0 ){
      z = sqlitexErrStr(db->errCode);
    }
  }
  sqlitex_mutex_leave(db->mutex);
  return z;
}

#ifndef SQLITE_OMIT_UTF16
/*
** Return UTF-16 encoded English language explanation of the most recent
** error.
*/
const void *sqlitex_errmsg16(sqlitex *db){
  static const u16 outOfMem[] = {
    'o', 'u', 't', ' ', 'o', 'f', ' ', 'm', 'e', 'm', 'o', 'r', 'y', 0
  };
  static const u16 misuse[] = {
    'l', 'i', 'b', 'r', 'a', 'r', 'y', ' ', 
    'r', 'o', 'u', 't', 'i', 'n', 'e', ' ', 
    'c', 'a', 'l', 'l', 'e', 'd', ' ', 
    'o', 'u', 't', ' ', 
    'o', 'f', ' ', 
    's', 'e', 'q', 'u', 'e', 'n', 'c', 'e', 0
  };

  const void *z;
  if( !db ){
    return (void *)outOfMem;
  }
  if( !sqlitexSafetyCheckSickOrOk(db) ){
    return (void *)misuse;
  }
  sqlitex_mutex_enter(db->mutex);
  if( db->mallocFailed ){
    z = (void *)outOfMem;
  }else{
    z = sqlitex_value_text16(db->pErr);
    if( z==0 ){
      sqlitexErrorWithMsg(db, db->errCode, sqlitexErrStr(db->errCode));
      z = sqlitex_value_text16(db->pErr);
    }
    /* A malloc() may have failed within the call to sqlitex_value_text16()
    ** above. If this is the case, then the db->mallocFailed flag needs to
    ** be cleared before returning. Do this directly, instead of via
    ** sqlitexApiExit(), to avoid setting the database handle error message.
    */
    db->mallocFailed = 0;
  }
  sqlitex_mutex_leave(db->mutex);
  return z;
}
#endif /* SQLITE_OMIT_UTF16 */

/*
** Return the most recent error code generated by an SQLite routine. If NULL is
** passed to this function, we assume a malloc() failed during sqlitex_open().
*/
int sqlitex_errcode(sqlitex *db){
  if( db && !sqlitexSafetyCheckSickOrOk(db) ){
    return SQLITE_MISUSE_BKPT;
  }
  if( !db || db->mallocFailed ){
    return SQLITE_NOMEM;
  }
  return db->errCode & db->errMask;
}
int sqlitex_extended_errcode(sqlitex *db){
  if( db && !sqlitexSafetyCheckSickOrOk(db) ){
    return SQLITE_MISUSE_BKPT;
  }
  if( !db || db->mallocFailed ){
    return SQLITE_NOMEM;
  }
  return db->errCode;
}

/*
** Return a string that describes the kind of error specified in the
** argument.  For now, this simply calls the internal sqlitexErrStr()
** function.
*/
const char *sqlitex_errstr(int rc){
  return sqlitexErrStr(rc);
}

/*
** Create a new collating function for database "db".  The name is zName
** and the encoding is enc.
*/
static int createCollation(
  sqlitex* db,
  const char *zName, 
  u8 enc,
  void* pCtx,
  int(*xCompare)(void*,int,const void*,int,const void*),
  void(*xDel)(void*)
){
  CollSeq *pColl;
  int enc2;
  
  assert( sqlitex_mutex_held(db->mutex) );

  /* If SQLITE_UTF16 is specified as the encoding type, transform this
  ** to one of SQLITE_UTF16LE or SQLITE_UTF16BE using the
  ** SQLITE_UTF16NATIVE macro. SQLITE_UTF16 is not used internally.
  */
  enc2 = enc;
  testcase( enc2==SQLITE_UTF16 );
  testcase( enc2==SQLITE_UTF16_ALIGNED );
  if( enc2==SQLITE_UTF16 || enc2==SQLITE_UTF16_ALIGNED ){
    enc2 = SQLITE_UTF16NATIVE;
  }
  if( enc2<SQLITE_UTF8 || enc2>SQLITE_UTF16BE ){
    return SQLITE_MISUSE_BKPT;
  }

  /* Check if this call is removing or replacing an existing collation 
  ** sequence. If so, and there are active VMs, return busy. If there
  ** are no active VMs, invalidate any pre-compiled statements.
  */
  pColl = sqlitexFindCollSeq(db, (u8)enc2, zName, 0);
  if( pColl && pColl->xCmp ){
    if( db->nVdbeActive ){
      /* COMDB2 MODIFICATION */
      printf("%s:%d SQLITE_BUSY\n", __FILE__, __LINE__);
      cheap_stack_trace();

      sqlitexErrorWithMsg(db, SQLITE_BUSY, 
        "unable to delete/modify collation sequence due to active statements");
      return SQLITE_BUSY;
    }
    sqlitexExpirePreparedStatements(db);

    /* If collation sequence pColl was created directly by a call to
    ** sqlitex_create_collation, and not generated by synthCollSeq(),
    ** then any copies made by synthCollSeq() need to be invalidated.
    ** Also, collation destructor - CollSeq.xDel() - function may need
    ** to be called.
    */ 
    if( (pColl->enc & ~SQLITE_UTF16_ALIGNED)==enc2 ){
      CollSeq *aColl = sqlitexHashFind(&db->aCollSeq, zName);
      int j;
      for(j=0; j<3; j++){
        CollSeq *p = &aColl[j];
        if( p->enc==pColl->enc ){
          if( p->xDel ){
            p->xDel(p->pUser);
          }
          p->xCmp = 0;
        }
      }
    }
  }

  pColl = sqlitexFindCollSeq(db, (u8)enc2, zName, 1);
  if( pColl==0 ) return SQLITE_NOMEM;
  pColl->xCmp = xCompare;
  pColl->pUser = pCtx;
  pColl->xDel = xDel;
  pColl->enc = (u8)(enc2 | (enc & SQLITE_UTF16_ALIGNED));
  sqlitexError(db, SQLITE_OK);
  return SQLITE_OK;
}


/*
** This array defines hard upper bounds on limit values.  The
** initializer must be kept in sync with the SQLITE_LIMIT_*
** #defines in sqlitex.h.
*/
static const int aHardLimit[] = {
  SQLITE_MAX_LENGTH,
  SQLITE_MAX_SQL_LENGTH,
  SQLITE_MAX_COLUMN,
  SQLITE_MAX_EXPR_DEPTH,
  SQLITE_MAX_COMPOUND_SELECT,
  SQLITE_MAX_VDBE_OP,
  SQLITE_MAX_FUNCTION_ARG,
  SQLITE_MAX_ATTACHED,
  SQLITE_MAX_LIKE_PATTERN_LENGTH,
  SQLITE_MAX_VARIABLE_NUMBER,      /* IMP: R-38091-32352 */
  SQLITE_MAX_TRIGGER_DEPTH,
  SQLITE_MAX_WORKER_THREADS,
};

/*
** Make sure the hard limits are set to reasonable values
*/
#if SQLITE_MAX_LENGTH<100
# error SQLITE_MAX_LENGTH must be at least 100
#endif
#if SQLITE_MAX_SQL_LENGTH<100
# error SQLITE_MAX_SQL_LENGTH must be at least 100
#endif
#if SQLITE_MAX_SQL_LENGTH>SQLITE_MAX_LENGTH
# error SQLITE_MAX_SQL_LENGTH must not be greater than SQLITE_MAX_LENGTH
#endif
#if SQLITE_MAX_COMPOUND_SELECT<2
# error SQLITE_MAX_COMPOUND_SELECT must be at least 2
#endif
#if SQLITE_MAX_VDBE_OP<40
# error SQLITE_MAX_VDBE_OP must be at least 40
#endif
#if SQLITE_MAX_FUNCTION_ARG<0 || SQLITE_MAX_FUNCTION_ARG>1000
# error SQLITE_MAX_FUNCTION_ARG must be between 0 and 1000
#endif
#if SQLITE_MAX_ATTACHED<0 || SQLITE_MAX_ATTACHED>125
# error SQLITE_MAX_ATTACHED must be between 0 and 125
#endif
#if SQLITE_MAX_LIKE_PATTERN_LENGTH<1
# error SQLITE_MAX_LIKE_PATTERN_LENGTH must be at least 1
#endif
#if SQLITE_MAX_COLUMN>32767
# error SQLITE_MAX_COLUMN must not exceed 32767
#endif
#if SQLITE_MAX_TRIGGER_DEPTH<1
# error SQLITE_MAX_TRIGGER_DEPTH must be at least 1
#endif
#if SQLITE_MAX_WORKER_THREADS<0 || SQLITE_MAX_WORKER_THREADS>50
# error SQLITE_MAX_WORKER_THREADS must be between 0 and 50
#endif


/*
** Change the value of a limit.  Report the old value.
** If an invalid limit index is supplied, report -1.
** Make no changes but still report the old value if the
** new limit is negative.
**
** A new lower limit does not shrink existing constructs.
** It merely prevents new constructs that exceed the limit
** from forming.
*/
int sqlitex_limit(sqlitex *db, int limitId, int newLimit){
  int oldLimit;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return -1;
  }
#endif

  /* EVIDENCE-OF: R-30189-54097 For each limit category SQLITE_LIMIT_NAME
  ** there is a hard upper bound set at compile-time by a C preprocessor
  ** macro called SQLITE_MAX_NAME. (The "_LIMIT_" in the name is changed to
  ** "_MAX_".)
  */
  assert( aHardLimit[SQLITE_LIMIT_LENGTH]==SQLITE_MAX_LENGTH );
  assert( aHardLimit[SQLITE_LIMIT_SQL_LENGTH]==SQLITE_MAX_SQL_LENGTH );
  assert( aHardLimit[SQLITE_LIMIT_COLUMN]==SQLITE_MAX_COLUMN );
  assert( aHardLimit[SQLITE_LIMIT_EXPR_DEPTH]==SQLITE_MAX_EXPR_DEPTH );
  assert( aHardLimit[SQLITE_LIMIT_COMPOUND_SELECT]==SQLITE_MAX_COMPOUND_SELECT);
  assert( aHardLimit[SQLITE_LIMIT_VDBE_OP]==SQLITE_MAX_VDBE_OP );
  assert( aHardLimit[SQLITE_LIMIT_FUNCTION_ARG]==SQLITE_MAX_FUNCTION_ARG );
  assert( aHardLimit[SQLITE_LIMIT_ATTACHED]==SQLITE_MAX_ATTACHED );
  assert( aHardLimit[SQLITE_LIMIT_LIKE_PATTERN_LENGTH]==
                                               SQLITE_MAX_LIKE_PATTERN_LENGTH );
  assert( aHardLimit[SQLITE_LIMIT_VARIABLE_NUMBER]==SQLITE_MAX_VARIABLE_NUMBER);
  assert( aHardLimit[SQLITE_LIMIT_TRIGGER_DEPTH]==SQLITE_MAX_TRIGGER_DEPTH );
  assert( aHardLimit[SQLITE_LIMIT_WORKER_THREADS]==SQLITE_MAX_WORKER_THREADS );
  assert( SQLITE_LIMIT_WORKER_THREADS==(SQLITE_N_LIMIT-1) );


  if( limitId<0 || limitId>=SQLITE_N_LIMIT ){
    return -1;
  }
  oldLimit = db->aLimit[limitId];
  if( newLimit>=0 ){                   /* IMP: R-52476-28732 */
    if( newLimit>aHardLimit[limitId] ){
      newLimit = aHardLimit[limitId];  /* IMP: R-51463-25634 */
    }
    db->aLimit[limitId] = newLimit;
  }
  return oldLimit;                     /* IMP: R-53341-35419 */
}

/*
** This function is used to parse both URIs and non-URI filenames passed by the
** user to API functions sqlitex_open() or sqlitex_open_v2(), and for database
** URIs specified as part of ATTACH statements.
**
** The first argument to this function is the name of the VFS to use (or
** a NULL to signify the default VFS) if the URI does not contain a "vfs=xxx"
** query parameter. The second argument contains the URI (or non-URI filename)
** itself. When this function is called the *pFlags variable should contain
** the default flags to open the database handle with. The value stored in
** *pFlags may be updated before returning if the URI filename contains 
** "cache=xxx" or "mode=xxx" query parameters.
**
** If successful, SQLITE_OK is returned. In this case *ppVfs is set to point to
** the VFS that should be used to open the database file. *pzFile is set to
** point to a buffer containing the name of the file to open. It is the 
** responsibility of the caller to eventually call sqlitex_free() to release
** this buffer.
**
** If an error occurs, then an SQLite error code is returned and *pzErrMsg
** may be set to point to a buffer containing an English language error 
** message. It is the responsibility of the caller to eventually release
** this buffer by calling sqlitex_free().
*/
int sqlitexParseUri(
  const char *zDefaultVfs,        /* VFS to use if no "vfs=xxx" query option */
  const char *zUri,               /* Nul-terminated URI to parse */
  unsigned int *pFlags,           /* IN/OUT: SQLITE_OPEN_XXX flags */
  sqlitex_vfs **ppVfs,            /* OUT: VFS to use */ 
  char **pzFile,                  /* OUT: Filename component of URI */
  char **pzErrMsg                 /* OUT: Error message (if rc!=SQLITE_OK) */
){
  int rc = SQLITE_OK;
  unsigned int flags = *pFlags;
  const char *zVfs = zDefaultVfs;
  char *zFile;
  char c;
  int nUri = sqlitexStrlen30(zUri);

  assert( *pzErrMsg==0 );

  if( ((flags & SQLITE_OPEN_URI)             /* IMP: R-48725-32206 */
            || sqlitexGlobalConfig.bOpenUri) /* IMP: R-51689-46548 */
   && nUri>=5 && memcmp(zUri, "file:", 5)==0 /* IMP: R-57884-37496 */
  ){
    char *zOpt;
    int eState;                   /* Parser state when parsing URI */
    int iIn;                      /* Input character index */
    int iOut = 0;                 /* Output character index */
    int nByte = nUri+2;           /* Bytes of space to allocate */

    /* Make sure the SQLITE_OPEN_URI flag is set to indicate to the VFS xOpen 
    ** method that there may be extra parameters following the file-name.  */
    flags |= SQLITE_OPEN_URI;

    for(iIn=0; iIn<nUri; iIn++) nByte += (zUri[iIn]=='&');
    zFile = sqlitex_malloc(nByte);
    if( !zFile ) return SQLITE_NOMEM;

    iIn = 5;
#ifdef SQLITE_ALLOW_URI_AUTHORITY
    if( strncmp(zUri+5, "///", 3)==0 ){
      iIn = 7;
      /* The following condition causes URIs with five leading / characters
      ** like file://///host/path to be converted into UNCs like //host/path.
      ** The correct URI for that UNC has only two or four leading / characters
      ** file://host/path or file:////host/path.  But 5 leading slashes is a 
      ** common error, we are told, so we handle it as a special case. */
      if( strncmp(zUri+7, "///", 3)==0 ){ iIn++; }
    }else if( strncmp(zUri+5, "//localhost/", 12)==0 ){
      iIn = 16;
    }
#else
    /* Discard the scheme and authority segments of the URI. */
    if( zUri[5]=='/' && zUri[6]=='/' ){
      iIn = 7;
      while( zUri[iIn] && zUri[iIn]!='/' ) iIn++;
      if( iIn!=7 && (iIn!=16 || memcmp("localhost", &zUri[7], 9)) ){
        *pzErrMsg = sqlitex_mprintf("invalid uri authority: %.*s", 
            iIn-7, &zUri[7]);
        rc = SQLITE_ERROR;
        goto parse_uri_out;
      }
    }
#endif

    /* Copy the filename and any query parameters into the zFile buffer. 
    ** Decode %HH escape codes along the way. 
    **
    ** Within this loop, variable eState may be set to 0, 1 or 2, depending
    ** on the parsing context. As follows:
    **
    **   0: Parsing file-name.
    **   1: Parsing name section of a name=value query parameter.
    **   2: Parsing value section of a name=value query parameter.
    */
    eState = 0;
    while( (c = zUri[iIn])!=0 && c!='#' ){
      iIn++;
      if( c=='%' 
       && sqlitexIsxdigit(zUri[iIn]) 
       && sqlitexIsxdigit(zUri[iIn+1]) 
      ){
        int octet = (sqlitexHexToInt(zUri[iIn++]) << 4);
        octet += sqlitexHexToInt(zUri[iIn++]);

        assert( octet>=0 && octet<256 );
        if( octet==0 ){
          /* This branch is taken when "%00" appears within the URI. In this
          ** case we ignore all text in the remainder of the path, name or
          ** value currently being parsed. So ignore the current character
          ** and skip to the next "?", "=" or "&", as appropriate. */
          while( (c = zUri[iIn])!=0 && c!='#' 
              && (eState!=0 || c!='?')
              && (eState!=1 || (c!='=' && c!='&'))
              && (eState!=2 || c!='&')
          ){
            iIn++;
          }
          continue;
        }
        c = octet;
      }else if( eState==1 && (c=='&' || c=='=') ){
        if( zFile[iOut-1]==0 ){
          /* An empty option name. Ignore this option altogether. */
          while( zUri[iIn] && zUri[iIn]!='#' && zUri[iIn-1]!='&' ) iIn++;
          continue;
        }
        if( c=='&' ){
          zFile[iOut++] = '\0';
        }else{
          eState = 2;
        }
        c = 0;
      }else if( (eState==0 && c=='?') || (eState==2 && c=='&') ){
        c = 0;
        eState = 1;
      }
      zFile[iOut++] = c;
    }
    if( eState==1 ) zFile[iOut++] = '\0';
    zFile[iOut++] = '\0';
    zFile[iOut++] = '\0';

    /* Check if there were any options specified that should be interpreted 
    ** here. Options that are interpreted here include "vfs" and those that
    ** correspond to flags that may be passed to the sqlitex_open_v2()
    ** method. */
    zOpt = &zFile[sqlitexStrlen30(zFile)+1];
    while( zOpt[0] ){
      int nOpt = sqlitexStrlen30(zOpt);
      char *zVal = &zOpt[nOpt+1];
      int nVal = sqlitexStrlen30(zVal);

      if( nOpt==3 && memcmp("vfs", zOpt, 3)==0 ){
        zVfs = zVal;
      }else{
        struct OpenMode {
          const char *z;
          int mode;
        } *aMode = 0;
        char *zModeType = 0;
        int mask = 0;
        int limit = 0;

        if( nOpt==5 && memcmp("cache", zOpt, 5)==0 ){
          static struct OpenMode aCacheMode[] = {
            { "shared",  SQLITE_OPEN_SHAREDCACHE },
            { "private", SQLITE_OPEN_PRIVATECACHE },
            { 0, 0 }
          };

          mask = SQLITE_OPEN_SHAREDCACHE|SQLITE_OPEN_PRIVATECACHE;
          aMode = aCacheMode;
          limit = mask;
          zModeType = "cache";
        }
        if( nOpt==4 && memcmp("mode", zOpt, 4)==0 ){
          static struct OpenMode aOpenMode[] = {
            { "ro",  SQLITE_OPEN_READONLY },
            { "rw",  SQLITE_OPEN_READWRITE }, 
            { "rwc", SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE },
            { "memory", SQLITE_OPEN_MEMORY },
            { 0, 0 }
          };

          mask = SQLITE_OPEN_READONLY | SQLITE_OPEN_READWRITE
                   | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY;
          aMode = aOpenMode;
          limit = mask & flags;
          zModeType = "access";
        }

        if( aMode ){
          int i;
          int mode = 0;
          for(i=0; aMode[i].z; i++){
            const char *z = aMode[i].z;
            if( nVal==sqlitexStrlen30(z) && 0==memcmp(zVal, z, nVal) ){
              mode = aMode[i].mode;
              break;
            }
          }
          if( mode==0 ){
            *pzErrMsg = sqlitex_mprintf("no such %s mode: %s", zModeType, zVal);
            rc = SQLITE_ERROR;
            goto parse_uri_out;
          }
          if( (mode & ~SQLITE_OPEN_MEMORY)>limit ){
            *pzErrMsg = sqlitex_mprintf("%s mode not allowed: %s",
                                        zModeType, zVal);
            rc = SQLITE_PERM;
            goto parse_uri_out;
          }
          flags = (flags & ~mask) | mode;
        }
      }

      zOpt = &zVal[nVal+1];
    }

  }else{
    zFile = sqlitex_malloc(nUri+2);
    if( !zFile ) return SQLITE_NOMEM;
    memcpy(zFile, zUri, nUri);
    zFile[nUri] = '\0';
    zFile[nUri+1] = '\0';
    flags &= ~SQLITE_OPEN_URI;
  }

  *ppVfs = sqlitex_vfs_find(zVfs);
  if( *ppVfs==0 ){
    *pzErrMsg = sqlitex_mprintf("no such vfs: %s", zVfs);
    rc = SQLITE_ERROR;
  }
 parse_uri_out:
  if( rc!=SQLITE_OK ){
    sqlitex_free(zFile);
    zFile = 0;
  }
  *pFlags = flags;
  *pzFile = zFile;
  return rc;
}

static void register_lua_sfuncs(sqlitex *db, struct sqlthdstate *thd)
{
  char **funcs;
  int num_funcs;
  get_sfuncs(&funcs, &num_funcs);
  for (int i = 0; i < num_funcs; ++i) {
    lua_func_arg_t *arg = malloc(sizeof(lua_func_arg_t));
    arg->thd = thd;
    arg->name = funcs[i];
    sqlitex_create_function_v2(db, funcs[i], -1, SQLITE_UTF8, arg, (void *)lua_func,
                               NULL, NULL, free);
  }
}

static void register_lua_afuncs(sqlitex *db, struct sqlthdstate *thd)
{
  char **funcs;
  int num_funcs;
  get_afuncs(&funcs, &num_funcs);
  for (int i = 0; i < num_funcs; ++i) {
    lua_func_arg_t *arg = malloc(sizeof(lua_func_arg_t));
    arg->thd = thd;
    arg->name = funcs[i];
    sqlitex_create_function_v2(db, funcs[i], -1, SQLITE_UTF8, arg, NULL,
                               (void *)lua_step, (void *)lua_final, free);
  }
}

/*
** This routine does the work of opening a database on behalf of
** sqlitex_open() and sqlitex_open16(). The database filename "zFilename"  
** is UTF-8 encoded.
*/
static int openDatabase(
  const char *zFilename, /* Database filename UTF-8 encoded */
  sqlitex **ppDb,        /* OUT: Returned database handle */
  unsigned int flags,    /* Operational flags */
  const char *zVfs,      /* Name of the VFS to use */
  struct sqlthdstate *thd
){
  sqlitex *db;                    /* Store allocated handle here */
  int rc;                         /* Return code */
  int isThreadsafe;               /* True for threadsafe connections */
  char *zOpen = 0;                /* Filename argument to pass to BtreeOpen() */
  char *zErrMsg = 0;              /* Error message from sqlitexParseUri() */

#ifdef SQLITE_ENABLE_API_ARMOR
  if( ppDb==0 ) return SQLITE_MISUSE_BKPT;
#endif
  *ppDb = 0;
#ifndef SQLITE_OMIT_AUTOINIT
  rc = sqlitex_initialize();
  if( rc ) return rc;
#endif

  /* Only allow sensible combinations of bits in the flags argument.  
  ** Throw an error if any non-sense combination is used.  If we
  ** do not block illegal combinations here, it could trigger
  ** assert() statements in deeper layers.  Sensible combinations
  ** are:
  **
  **  1:  SQLITE_OPEN_READONLY
  **  2:  SQLITE_OPEN_READWRITE
  **  6:  SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
  */
  assert( SQLITE_OPEN_READONLY  == 0x01 );
  assert( SQLITE_OPEN_READWRITE == 0x02 );
  assert( SQLITE_OPEN_CREATE    == 0x04 );
  testcase( (1<<(flags&7))==0x02 ); /* READONLY */
  testcase( (1<<(flags&7))==0x04 ); /* READWRITE */
  testcase( (1<<(flags&7))==0x40 ); /* READWRITE | CREATE */
  if( ((1<<(flags&7)) & 0x46)==0 ){
    return SQLITE_MISUSE_BKPT;  /* IMP: R-65497-44594 */
  }

  if( sqlitexGlobalConfig.bCoreMutex==0 ){
    isThreadsafe = 0;
  }else if( flags & SQLITE_OPEN_NOMUTEX ){
    isThreadsafe = 0;
  }else if( flags & SQLITE_OPEN_FULLMUTEX ){
    isThreadsafe = 1;
  }else{
    isThreadsafe = sqlitexGlobalConfig.bFullMutex;
  }
  if( flags & SQLITE_OPEN_PRIVATECACHE ){
    flags &= ~SQLITE_OPEN_SHAREDCACHE;
  }else if( sqlitexGlobalConfig.sharedCacheEnabled ){
    flags |= SQLITE_OPEN_SHAREDCACHE;
  }

  /* Remove harmful bits from the flags parameter
  **
  ** The SQLITE_OPEN_NOMUTEX and SQLITE_OPEN_FULLMUTEX flags were
  ** dealt with in the previous code block.  Besides these, the only
  ** valid input flags for sqlitex_open_v2() are SQLITE_OPEN_READONLY,
  ** SQLITE_OPEN_READWRITE, SQLITE_OPEN_CREATE, SQLITE_OPEN_SHAREDCACHE,
  ** SQLITE_OPEN_PRIVATECACHE, and some reserved bits.  Silently mask
  ** off all other flags.
  */
  flags &=  ~( SQLITE_OPEN_DELETEONCLOSE |
               SQLITE_OPEN_EXCLUSIVE |
               SQLITE_OPEN_MAIN_DB |
               SQLITE_OPEN_TEMP_DB | 
               SQLITE_OPEN_TRANSIENT_DB | 
               SQLITE_OPEN_MAIN_JOURNAL | 
               SQLITE_OPEN_TEMP_JOURNAL | 
               SQLITE_OPEN_SUBJOURNAL | 
               SQLITE_OPEN_MASTER_JOURNAL |
               SQLITE_OPEN_NOMUTEX |
               SQLITE_OPEN_FULLMUTEX |
               SQLITE_OPEN_WAL
             );

  /* Allocate the sqlite data structure */
  db = sqlitexMallocZero( sizeof(sqlitex) );
  if( db==0 ) goto opendb_out;
  if( isThreadsafe ){
    db->mutex = sqlitexMutexAlloc(SQLITE_MUTEX_RECURSIVE);
    if( db->mutex==0 ){
      sqlitex_free(db);
      db = 0;
      goto opendb_out;
    }
  }
  sqlitex_mutex_enter(db->mutex);
  db->errMask = 0xff;
  db->nDb = 2;
  db->magic = SQLITE_MAGIC_BUSY;
  db->aDb = db->aDbStatic;

  assert( sizeof(db->aLimit)==sizeof(aHardLimit) );
  memcpy(db->aLimit, aHardLimit, sizeof(db->aLimit));
  db->aLimit[SQLITE_LIMIT_WORKER_THREADS] = SQLITE_DEFAULT_WORKER_THREADS;
  db->autoCommit = 1;
  db->nextAutovac = -1;
  db->szMmap = sqlitexGlobalConfig.szMmap;
  db->nextPagesize = 0;
  db->nMaxSorterMmap = 0x7FFFFFFF;
  db->flags |= SQLITE_ShortColNames | SQLITE_EnableTrigger | SQLITE_CacheSpill
#if !defined(SQLITE_DEFAULT_AUTOMATIC_INDEX) || SQLITE_DEFAULT_AUTOMATIC_INDEX
                 | SQLITE_AutoIndex
#endif
#if SQLITE_DEFAULT_CKPTFULLFSYNC
                 | SQLITE_CkptFullFSync
#endif
#if SQLITE_DEFAULT_FILE_FORMAT<4
                 | SQLITE_LegacyFileFmt
#endif
#ifdef SQLITE_ENABLE_LOAD_EXTENSION
                 | SQLITE_LoadExtension
#endif
#if SQLITE_DEFAULT_RECURSIVE_TRIGGERS
                 | SQLITE_RecTriggers
#endif
#if defined(SQLITE_DEFAULT_FOREIGN_KEYS) && SQLITE_DEFAULT_FOREIGN_KEYS
                 | SQLITE_ForeignKeys
#endif
#if defined(SQLITE_REVERSE_UNORDERED_SELECTS)
                 | SQLITE_ReverseOrder
#endif
      ;
  sqlitexHashInit(&db->aCollSeq);
#ifndef SQLITE_OMIT_VIRTUALTABLE
  sqlitexHashInit(&db->aModule);
#endif

  /* Add the default collation sequence BINARY. BINARY works for both UTF-8
  ** and UTF-16, so add a version for each to avoid any unnecessary
  ** conversions. The only error that can occur here is a malloc() failure.
  **
  ** EVIDENCE-OF: R-52786-44878 SQLite defines three built-in collating
  ** functions:
  */
  createCollation(db, "BINARY", SQLITE_UTF8, 0, binCollFunc, 0);
  createCollation(db, "BINARY", SQLITE_UTF16BE, 0, binCollFunc, 0);
  createCollation(db, "BINARY", SQLITE_UTF16LE, 0, binCollFunc, 0);
  createCollation(db, "NOCASE", SQLITE_UTF8, 0, nocaseCollatingFunc, 0);
  createCollation(db, "RTRIM", SQLITE_UTF8, (void*)1, binCollFunc, 0);
  if( db->mallocFailed ){
    goto opendb_out;
  }
  /* EVIDENCE-OF: R-08308-17224 The default collating function for all
  ** strings is BINARY. 
  */
  db->pDfltColl = sqlitexFindCollSeq(db, SQLITE_UTF8, "BINARY", 0);
  assert( db->pDfltColl!=0 );

  /* Parse the filename/URI argument. */
  /* COMDB2 MODIFICATION */
  createCollation(db, "DATACOPY", SQLITE_UTF8, 0, datacopyCollatingFunc, 0);

  db->openFlags = flags;
  rc = sqlitexParseUri(zVfs, zFilename, &flags, &db->pVfs, &zOpen, &zErrMsg);
  if( rc!=SQLITE_OK ){
    if( rc==SQLITE_NOMEM ) db->mallocFailed = 1;
    sqlitexErrorWithMsg(db, rc, zErrMsg ? "%s" : 0, zErrMsg);
    sqlitex_free(zErrMsg);
    goto opendb_out;
  }

  /* Open the backend database driver */
  rc = sqlite3BtreeOpen(db->pVfs, zOpen, db, &db->aDb[0].pBt, 0,
                        flags | SQLITE_OPEN_MAIN_DB);
  if( rc!=SQLITE_OK ){
    if( rc==SQLITE_IOERR_NOMEM ){
      rc = SQLITE_NOMEM;
    }
    sqlitexError(db, rc);
    goto opendb_out;
  }
  sqlite3BtreeEnter(db->aDb[0].pBt);
  db->aDb[0].pSchema = sqlitexSchemaGet(db, db->aDb[0].pBt);
  if( !db->mallocFailed ) ENC(db) = SCHEMA_ENC(db);
  sqlite3BtreeLeave(db->aDb[0].pBt);
  db->aDb[1].pSchema = sqlitexSchemaGet(db, 0);

  /* The default safety_level for the main database is 'full'; for the temp
  ** database it is 'NONE'. This matches the pager layer defaults.  
  */
  db->aDb[0].zName = "main";
  db->aDb[0].safety_level = 3;
  db->aDb[1].zName = "temp";
  db->aDb[1].safety_level = 1;

  db->magic = SQLITE_MAGIC_OPEN;
  if( db->mallocFailed ){
    goto opendb_out;
  }

  /* Register all built-in functions, but do not attempt to read the
  ** database schema yet. This is delayed until the first time the database
  ** is accessed.
  */
  sqlitexError(db, SQLITE_OK);
  sqlitexRegisterBuiltinFunctions(db);

  /* Load automatic extensions - extensions that have been registered
  ** using the sqlitex_automatic_extension() API.
  */
  rc = sqlitex_errcode(db);
  if( rc==SQLITE_OK ){
    sqlitexAutoLoadExtensions(db);
    rc = sqlitex_errcode(db);
    if( rc!=SQLITE_OK ){
      goto opendb_out;
    }
  }

#ifdef SQLITE_ENABLE_FTS1
  if( !db->mallocFailed ){
    extern int sqlitexFts1Init(sqlitex*);
    rc = sqlitexFts1Init(db);
  }
#endif

#ifdef SQLITE_ENABLE_FTS2
  if( !db->mallocFailed && rc==SQLITE_OK ){
    extern int sqlitexFts2Init(sqlitex*);
    rc = sqlitexFts2Init(db);
  }
#endif

#ifdef SQLITE_ENABLE_FTS3
  if( !db->mallocFailed && rc==SQLITE_OK ){
    rc = sqlitexFts3Init(db);
  }
#endif

#ifdef SQLITE_ENABLE_ICU
  if( !db->mallocFailed && rc==SQLITE_OK ){
    rc = sqlitexIcuInit(db);
  }
#endif

#ifdef SQLITE_ENABLE_RTREE
  if( !db->mallocFailed && rc==SQLITE_OK ){
    rc = sqlitexRtreeInit(db);
  }
#endif

#ifdef SQLITE_ENABLE_DBSTAT_VTAB
  if( !db->mallocFailed && rc==SQLITE_OK ){
    rc = sqlitexDbstatRegister(db);
  }
#endif

/* COMDB2 MODIFICATION */
#ifdef SQLITE_BUILDING_FOR_COMDB2
  if( !db->mallocFailed && rc==SQLITE_OK ){
    rc = comdb2SystblInitX(db);
  }

# ifdef SQLITE_ENABLE_SERIES
  if( !db->mallocFailed && rc==SQLITE_OK ){
    rc = sqlitexSeriesInit(db);
  }
# endif
#endif

  /* -DSQLITE_DEFAULT_LOCKING_MODE=1 makes EXCLUSIVE the default locking
  ** mode.  -DSQLITE_DEFAULT_LOCKING_MODE=0 make NORMAL the default locking
  ** mode.  Doing nothing at all also makes NORMAL the default.
  */
#ifdef SQLITE_DEFAULT_LOCKING_MODE
  db->dfltLockMode = SQLITE_DEFAULT_LOCKING_MODE;
  sqlite3PagerLockingMode(sqlite3BtreePager(db->aDb[0].pBt),
                          SQLITE_DEFAULT_LOCKING_MODE);
#endif

  if( rc ) sqlitexError(db, rc);

  /* Enable the lookaside-malloc subsystem */
  setupLookaside(db, 0, sqlitexGlobalConfig.szLookaside,
                        sqlitexGlobalConfig.nLookaside);

  sqlitex_wal_autocheckpoint(db, SQLITE_DEFAULT_WAL_AUTOCHECKPOINT);

opendb_out:
  sqlitex_free(zOpen);
  if( db ){
    assert( db->mutex!=0 || isThreadsafe==0
           || sqlitexGlobalConfig.bFullMutex==0 );
    sqlitex_mutex_leave(db->mutex);
  }
  rc = sqlitex_errcode(db);
  assert( db!=0 || rc==SQLITE_NOMEM );
  if( rc==SQLITE_NOMEM ){
    sqlitex_close(db);
    db = 0;
  }else if( rc!=SQLITE_OK ){
    db->magic = SQLITE_MAGIC_SICK;
  }
  *ppDb = db;
#ifdef SQLITE_ENABLE_SQLLOG
  if( sqlitexGlobalConfig.xSqllog ){
    /* Opening a db handle. Fourth parameter is passed 0. */
    void *pArg = sqlitexGlobalConfig.pSqllogArg;
    sqlitexGlobalConfig.xSqllog(pArg, db, zFilename, 0);
  }
#endif

  /* COMDB2 MODIFICATION */
#ifdef SQLITE_BUILDING_FOR_COMDB2
  static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
  pthread_mutex_lock(&mutex);
  /* these modify global structures */
  register_date_functionsX(db); 
  register_lua_sfuncs(db, thd);
  register_lua_afuncs(db, thd);
  db->should_fingerprint = 0;
  pthread_mutex_unlock(&mutex);
#endif

  return sqlitexApiExit(0, rc);
}

/*
** Open a new database handle.
*/
int sqlitex_open(
  const char *zFilename, 
  sqlitex **ppDb,
  struct sqlthdstate *thd
  ){
    int rc;

#ifdef DEBUG_SQLITE_MEMORY
    extern void sqlite_init_start(void);
    sqlite_init_start();
#endif

    rc = openDatabase(zFilename, ppDb,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 0, thd);

#ifdef DEBUG_SQLITE_MEMORY
    extern void sqlite_init_end(void);
    sqlite_init_end();
#endif

    return rc;
}
int sqlitex_open_v2(
  const char *filename,   /* Database filename (UTF-8) */
  sqlitex **ppDb,         /* OUT: SQLite db handle */
  int flags,              /* Flags */
  const char *zVfs        /* Name of VFS module to use */
){
  return openDatabase(filename, ppDb, (unsigned int)flags, zVfs, NULL);
}

#ifndef SQLITE_OMIT_UTF16
/*
** Open a new database handle.
*/
int sqlitex_open16(
  const void *zFilename, 
  sqlitex **ppDb
){
  char const *zFilename8;   /* zFilename encoded in UTF-8 instead of UTF-16 */
  sqlitex_value *pVal;
  int rc;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( ppDb==0 ) return SQLITE_MISUSE_BKPT;
#endif
  *ppDb = 0;
#ifndef SQLITE_OMIT_AUTOINIT
  rc = sqlitex_initialize();
  if( rc ) return rc;
#endif
  if( zFilename==0 ) zFilename = "\000\000";
  pVal = sqlitexValueNew(0);
  sqlitexValueSetStr(pVal, -1, zFilename, SQLITE_UTF16NATIVE, SQLITEX_STATIC);
  zFilename8 = sqlitexValueText(pVal, SQLITE_UTF8);
  if( zFilename8 ){
    rc = openDatabase(zFilename8, ppDb,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    assert( *ppDb || rc==SQLITE_NOMEM );
    if( rc==SQLITE_OK && !DbHasProperty(*ppDb, 0, DB_SchemaLoaded) ){
      SCHEMA_ENC(*ppDb) = ENC(*ppDb) = SQLITE_UTF16NATIVE;
    }
  }else{
    rc = SQLITE_NOMEM;
  }
  sqlitexValueFree(pVal);

  return sqlitexApiExit(0, rc);
}
#endif /* SQLITE_OMIT_UTF16 */

/*
** Register a new collation sequence with the database handle db.
*/
int sqlitex_create_collation(
  sqlitex* db, 
  const char *zName, 
  int enc, 
  void* pCtx,
  int(*xCompare)(void*,int,const void*,int,const void*)
){
  return sqlitex_create_collation_v2(db, zName, enc, pCtx, xCompare, 0);
}

/*
** Register a new collation sequence with the database handle db.
*/
int sqlitex_create_collation_v2(
  sqlitex* db, 
  const char *zName, 
  int enc, 
  void* pCtx,
  int(*xCompare)(void*,int,const void*,int,const void*),
  void(*xDel)(void*)
){
  int rc;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) || zName==0 ) return SQLITE_MISUSE_BKPT;
#endif
  sqlitex_mutex_enter(db->mutex);
  assert( !db->mallocFailed );
  rc = createCollation(db, zName, (u8)enc, pCtx, xCompare, xDel);
  rc = sqlitexApiExit(db, rc);
  sqlitex_mutex_leave(db->mutex);
  return rc;
}

#ifndef SQLITE_OMIT_UTF16
/*
** Register a new collation sequence with the database handle db.
*/
int sqlitex_create_collation16(
  sqlitex* db, 
  const void *zName,
  int enc, 
  void* pCtx,
  int(*xCompare)(void*,int,const void*,int,const void*)
){
  int rc = SQLITE_OK;
  char *zName8;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) || zName==0 ) return SQLITE_MISUSE_BKPT;
#endif
  sqlitex_mutex_enter(db->mutex);
  assert( !db->mallocFailed );
  zName8 = sqlitexUtf16to8(db, zName, -1, SQLITE_UTF16NATIVE);
  if( zName8 ){
    rc = createCollation(db, zName8, (u8)enc, pCtx, xCompare, 0);
    sqlitexDbFree(db, zName8);
  }
  rc = sqlitexApiExit(db, rc);
  sqlitex_mutex_leave(db->mutex);
  return rc;
}
#endif /* SQLITE_OMIT_UTF16 */

/*
** Register a collation sequence factory callback with the database handle
** db. Replace any previously installed collation sequence factory.
*/
int sqlitex_collation_needed(
  sqlitex *db, 
  void *pCollNeededArg, 
  void(*xCollNeeded)(void*,sqlitex*,int eTextRep,const char*)
){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif
  sqlitex_mutex_enter(db->mutex);
  db->xCollNeeded = xCollNeeded;
  db->xCollNeeded16 = 0;
  db->pCollNeededArg = pCollNeededArg;
  sqlitex_mutex_leave(db->mutex);
  return SQLITE_OK;
}

#ifndef SQLITE_OMIT_UTF16
/*
** Register a collation sequence factory callback with the database handle
** db. Replace any previously installed collation sequence factory.
*/
int sqlitex_collation_needed16(
  sqlitex *db, 
  void *pCollNeededArg, 
  void(*xCollNeeded16)(void*,sqlitex*,int eTextRep,const void*)
){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif
  sqlitex_mutex_enter(db->mutex);
  db->xCollNeeded = 0;
  db->xCollNeeded16 = xCollNeeded16;
  db->pCollNeededArg = pCollNeededArg;
  sqlitex_mutex_leave(db->mutex);
  return SQLITE_OK;
}
#endif /* SQLITE_OMIT_UTF16 */

#ifndef SQLITE_OMIT_DEPRECATED
/*
** This function is now an anachronism. It used to be used to recover from a
** malloc() failure, but SQLite now does this automatically.
*/
int sqlitex_global_recover(void){
  return SQLITE_OK;
}
#endif

/*
** Test to see whether or not the database connection is in autocommit
** mode.  Return TRUE if it is and FALSE if not.  Autocommit mode is on
** by default.  Autocommit is disabled by a BEGIN statement and reenabled
** by the next COMMIT or ROLLBACK.
*/
int sqlitex_get_autocommit(sqlitex *db){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
  return db->autoCommit;
}

/*
** The following routines are substitutes for constants SQLITE_CORRUPT,
** SQLITE_MISUSE, SQLITE_CANTOPEN, SQLITE_IOERR and possibly other error
** constants.  They serve two purposes:
**
**   1.  Serve as a convenient place to set a breakpoint in a debugger
**       to detect when version error conditions occurs.
**
**   2.  Invoke sqlitex_log() to provide the source code location where
**       a low-level error is first detected.
*/
int sqlitexCorruptError(int lineno){
  testcase( sqlitexGlobalConfig.xLog!=0 );
  sqlitex_log(SQLITE_CORRUPT,
              "database corruption at line %d of [%.10s]",
              lineno, 20+sqlitex_sourceid());
  return SQLITE_CORRUPT;
}
int sqlitexMisuseError(int lineno){
  testcase( sqlitexGlobalConfig.xLog!=0 );
  sqlitex_log(SQLITE_MISUSE, 
              "misuse at line %d of [%.10s]",
              lineno, 20+sqlitex_sourceid());
  return SQLITE_MISUSE;
}
int sqlitexCantopenError(int lineno){
  testcase( sqlitexGlobalConfig.xLog!=0 );
  sqlitex_log(SQLITE_CANTOPEN, 
              "cannot open file at line %d of [%.10s]",
              lineno, 20+sqlitex_sourceid());
  return SQLITE_CANTOPEN;
}


#ifndef SQLITE_OMIT_DEPRECATED
/*
** This is a convenience routine that makes sure that all thread-specific
** data for this thread has been deallocated.
**
** SQLite no longer uses thread-specific data so this routine is now a
** no-op.  It is retained for historical compatibility.
*/
void sqlitex_thread_cleanup(void){
}
#endif

/*
** Return meta information about a specific column of a database table.
** See comment in sqlitex.h (sqlite.h.in) for details.
*/
int sqlitex_table_column_metadata(
  sqlitex *db,                /* Connection handle */
  const char *zDbName,        /* Database name or NULL */
  const char *zTableName,     /* Table name */
  const char *zColumnName,    /* Column name */
  char const **pzDataType,    /* OUTPUT: Declared data type */
  char const **pzCollSeq,     /* OUTPUT: Collation sequence name */
  int *pNotNull,              /* OUTPUT: True if NOT NULL constraint exists */
  int *pPrimaryKey,           /* OUTPUT: True if column part of PK */
  int *pAutoinc               /* OUTPUT: True if column is auto-increment */
){
  int rc;
  char *zErrMsg = 0;
  Table *pTab = 0;
  Column *pCol = 0;
  int iCol = 0;
  char const *zDataType = 0;
  char const *zCollSeq = 0;
  int notnull = 0;
  int primarykey = 0;
  int autoinc = 0;


#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) || zTableName==0 ){
    return SQLITE_MISUSE_BKPT;
  }
#endif

  /* Ensure the database schema has been loaded */
  sqlitex_mutex_enter(db->mutex);
  sqlite3BtreeEnterAll(db);
  rc = sqlitexInit(db, &zErrMsg);
  if( SQLITE_OK!=rc ){
    goto error_out;
  }

  /* Locate the table in question */
  pTab = sqlitexFindTable(db, zTableName, zDbName);
  if( !pTab || pTab->pSelect ){
    pTab = 0;
    goto error_out;
  }

  /* Find the column for which info is requested */
  if( zColumnName==0 ){
    /* Query for existance of table only */
  }else{
    for(iCol=0; iCol<pTab->nCol; iCol++){
      pCol = &pTab->aCol[iCol];
      if( 0==sqlitexStrICmp(pCol->zName, zColumnName) ){
        break;
      }
    }
    if( iCol==pTab->nCol ){
      if( HasRowid(pTab) && sqlitexIsRowid(zColumnName) ){
        iCol = pTab->iPKey;
        pCol = iCol>=0 ? &pTab->aCol[iCol] : 0;
      }else{
        pTab = 0;
        goto error_out;
      }
    }
  }

  /* The following block stores the meta information that will be returned
  ** to the caller in local variables zDataType, zCollSeq, notnull, primarykey
  ** and autoinc. At this point there are two possibilities:
  ** 
  **     1. The specified column name was rowid", "oid" or "_rowid_" 
  **        and there is no explicitly declared IPK column. 
  **
  **     2. The table is not a view and the column name identified an 
  **        explicitly declared column. Copy meta information from *pCol.
  */ 
  if( pCol ){
    zDataType = pCol->zType;
    zCollSeq = pCol->zColl;
    notnull = pCol->notNull!=0;
    primarykey  = (pCol->colFlags & COLFLAG_PRIMKEY)!=0;
    autoinc = pTab->iPKey==iCol && (pTab->tabFlags & TF_Autoincrement)!=0;
  }else{
    zDataType = "INTEGER";
    primarykey = 1;
  }
  if( !zCollSeq ){
    zCollSeq = "BINARY";
  }

error_out:
  sqlite3BtreeLeaveAll(db);

  /* Whether the function call succeeded or failed, set the output parameters
  ** to whatever their local counterparts contain. If an error did occur,
  ** this has the effect of zeroing all output parameters.
  */
  if( pzDataType ) *pzDataType = zDataType;
  if( pzCollSeq ) *pzCollSeq = zCollSeq;
  if( pNotNull ) *pNotNull = notnull;
  if( pPrimaryKey ) *pPrimaryKey = primarykey;
  if( pAutoinc ) *pAutoinc = autoinc;

  if( SQLITE_OK==rc && !pTab ){
    sqlitexDbFree(db, zErrMsg);
    zErrMsg = sqlitexMPrintf(db, "no such table column: %s.%s", zTableName,
        zColumnName);
    rc = SQLITE_ERROR;
  }
  sqlitexErrorWithMsg(db, rc, (zErrMsg?"%s":0), zErrMsg);
  sqlitexDbFree(db, zErrMsg);
  rc = sqlitexApiExit(db, rc);
  sqlitex_mutex_leave(db->mutex);
  return rc;
}

/*
** Sleep for a little while.  Return the amount of time slept.
*/
int sqlitex_sleep(int ms){
  sqlitex_vfs *pVfs;
  int rc;
  pVfs = sqlitex_vfs_find(0);
  if( pVfs==0 ) return 0;

  /* This function works in milliseconds, but the underlying OsSleep() 
  ** API uses microseconds. Hence the 1000's.
  */
  rc = (sqlitexOsSleep(pVfs, 1000*ms)/1000);
  return rc;
}

/*
** Enable or disable the extended result codes.
*/
int sqlitex_extended_result_codes(sqlitex *db, int onoff){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif
  sqlitex_mutex_enter(db->mutex);
  db->errMask = onoff ? 0xffffffff : 0xff;
  sqlitex_mutex_leave(db->mutex);
  return SQLITE_OK;
}

/*
** Invoke the xFileControl method on a particular database.
*/
int sqlitex_file_control(sqlitex *db, const char *zDbName, int op, void *pArg){
  int rc = SQLITE_ERROR;
  Btree *pBtree;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif
  sqlitex_mutex_enter(db->mutex);
  pBtree = sqlitexDbNameToBtree(db, zDbName);
  if( pBtree ){
    Pager *pPager;
    sqlitex_file *fd;
    sqlite3BtreeEnter(pBtree);
    pPager = sqlite3BtreePager(pBtree);
    assert( pPager!=0 );
    fd = sqlite3PagerFile(pPager);
    assert( fd!=0 );
    if( op==SQLITE_FCNTL_FILE_POINTER ){
      *(sqlitex_file**)pArg = fd;
      rc = SQLITE_OK;
    }else if( fd->pMethods ){
      rc = sqlitexOsFileControl(fd, op, pArg);
    }else{
      rc = SQLITE_NOTFOUND;
    }
    sqlite3BtreeLeave(pBtree);
  }
  sqlitex_mutex_leave(db->mutex);
  return rc;
}

/*
** Interface to the testing logic.
*/
int sqlitex_test_control(int op, ...){
  int rc = 0;
#ifndef SQLITE_OMIT_BUILTIN_TEST
  va_list ap;
  va_start(ap, op);
  switch( op ){

    /*
    ** Save the current state of the PRNG.
    */
    case SQLITE_TESTCTRL_PRNG_SAVE: {
      sqlitexPrngSaveState();
      break;
    }

    /*
    ** Restore the state of the PRNG to the last state saved using
    ** PRNG_SAVE.  If PRNG_SAVE has never before been called, then
    ** this verb acts like PRNG_RESET.
    */
    case SQLITE_TESTCTRL_PRNG_RESTORE: {
      sqlitexPrngRestoreState();
      break;
    }

    /*
    ** Reset the PRNG back to its uninitialized state.  The next call
    ** to sqlitex_randomness() will reseed the PRNG using a single call
    ** to the xRandomness method of the default VFS.
    */
    case SQLITE_TESTCTRL_PRNG_RESET: {
      sqlitex_randomness(0,0);
      break;
    }

    /*
    **  sqlitex_test_control(BITVEC_TEST, size, program)
    **
    ** Run a test against a Bitvec object of size.  The program argument
    ** is an array of integers that defines the test.  Return -1 on a
    ** memory allocation error, 0 on success, or non-zero for an error.
    ** See the sqlitexBitvecBuiltinTest() for additional information.
    */
    case SQLITE_TESTCTRL_BITVEC_TEST: {
      int sz = va_arg(ap, int);
      int *aProg = va_arg(ap, int*);
      rc = sqlitexBitvecBuiltinTest(sz, aProg);
      break;
    }

    /*
    **  sqlitex_test_control(FAULT_INSTALL, xCallback)
    **
    ** Arrange to invoke xCallback() whenever sqlitexFaultSim() is called,
    ** if xCallback is not NULL.
    **
    ** As a test of the fault simulator mechanism itself, sqlitexFaultSim(0)
    ** is called immediately after installing the new callback and the return
    ** value from sqlitexFaultSim(0) becomes the return from
    ** sqlitex_test_control().
    */
    case SQLITE_TESTCTRL_FAULT_INSTALL: {
      /* MSVC is picky about pulling func ptrs from va lists.
      ** http://support.microsoft.com/kb/47961
      ** sqlitexGlobalConfig.xTestCallback = va_arg(ap, int(*)(int));
      */
      typedef int(*TESTCALLBACKFUNC_t)(int);
      sqlitexGlobalConfig.xTestCallback = va_arg(ap, TESTCALLBACKFUNC_t);
      rc = sqlitexFaultSim(0);
      break;
    }

    /*
    **  sqlitex_test_control(BENIGN_MALLOC_HOOKS, xBegin, xEnd)
    **
    ** Register hooks to call to indicate which malloc() failures 
    ** are benign.
    */
    case SQLITE_TESTCTRL_BENIGN_MALLOC_HOOKS: {
      typedef void (*void_function)(void);
      void_function xBenignBegin;
      void_function xBenignEnd;
      xBenignBegin = va_arg(ap, void_function);
      xBenignEnd = va_arg(ap, void_function);
      sqlitexBenignMallocHooks(xBenignBegin, xBenignEnd);
      break;
    }

    /*
    **  sqlitex_test_control(SQLITE_TESTCTRL_PENDING_BYTE, unsigned int X)
    **
    ** Set the PENDING byte to the value in the argument, if X>0.
    ** Make no changes if X==0.  Return the value of the pending byte
    ** as it existing before this routine was called.
    **
    ** IMPORTANT:  Changing the PENDING byte from 0x40000000 results in
    ** an incompatible database file format.  Changing the PENDING byte
    ** while any database connection is open results in undefined and
    ** deleterious behavior.
    */
    case SQLITE_TESTCTRL_PENDING_BYTE: {
      rc = PENDING_BYTE;
#ifndef SQLITE_OMIT_WSD
      {
        unsigned int newVal = va_arg(ap, unsigned int);
        if( newVal ) sqlitexPendingByte = newVal;
      }
#endif
      break;
    }

    /*
    **  sqlitex_test_control(SQLITE_TESTCTRL_ASSERT, int X)
    **
    ** This action provides a run-time test to see whether or not
    ** assert() was enabled at compile-time.  If X is true and assert()
    ** is enabled, then the return value is true.  If X is true and
    ** assert() is disabled, then the return value is zero.  If X is
    ** false and assert() is enabled, then the assertion fires and the
    ** process aborts.  If X is false and assert() is disabled, then the
    ** return value is zero.
    */
    case SQLITE_TESTCTRL_ASSERT: {
      volatile int x = 0;
      assert( (x = va_arg(ap,int))!=0 );
      rc = x;
      break;
    }


    /*
    **  sqlitex_test_control(SQLITE_TESTCTRL_ALWAYS, int X)
    **
    ** This action provides a run-time test to see how the ALWAYS and
    ** NEVER macros were defined at compile-time.
    **
    ** The return value is ALWAYS(X).  
    **
    ** The recommended test is X==2.  If the return value is 2, that means
    ** ALWAYS() and NEVER() are both no-op pass-through macros, which is the
    ** default setting.  If the return value is 1, then ALWAYS() is either
    ** hard-coded to true or else it asserts if its argument is false.
    ** The first behavior (hard-coded to true) is the case if
    ** SQLITE_TESTCTRL_ASSERT shows that assert() is disabled and the second
    ** behavior (assert if the argument to ALWAYS() is false) is the case if
    ** SQLITE_TESTCTRL_ASSERT shows that assert() is enabled.
    **
    ** The run-time test procedure might look something like this:
    **
    **    if( sqlitex_test_control(SQLITE_TESTCTRL_ALWAYS, 2)==2 ){
    **      // ALWAYS() and NEVER() are no-op pass-through macros
    **    }else if( sqlitex_test_control(SQLITE_TESTCTRL_ASSERT, 1) ){
    **      // ALWAYS(x) asserts that x is true. NEVER(x) asserts x is false.
    **    }else{
    **      // ALWAYS(x) is a constant 1.  NEVER(x) is a constant 0.
    **    }
    */
    case SQLITE_TESTCTRL_ALWAYS: {
      int x = va_arg(ap,int);
      rc = ALWAYS(x);
      break;
    }

    /*
    **   sqlitex_test_control(SQLITE_TESTCTRL_BYTEORDER);
    **
    ** The integer returned reveals the byte-order of the computer on which
    ** SQLite is running:
    **
    **       1     big-endian,    determined at run-time
    **      10     little-endian, determined at run-time
    **  432101     big-endian,    determined at compile-time
    **  123410     little-endian, determined at compile-time
    */ 
    case SQLITE_TESTCTRL_BYTEORDER: {
      rc = SQLITE_BYTEORDER*100 + SQLITE_LITTLEENDIAN*10 + SQLITE_BIGENDIAN;
      break;
    }

    /*   sqlitex_test_control(SQLITE_TESTCTRL_RESERVE, sqlitex *db, int N)
    **
    ** Set the nReserve size to N for the main database on the database
    ** connection db.
    */
    case SQLITE_TESTCTRL_RESERVE: {
      sqlitex *db = va_arg(ap, sqlitex*);
      int x = va_arg(ap,int);
      sqlitex_mutex_enter(db->mutex);
      sqlite3BtreeSetPageSize(db->aDb[0].pBt, 0, x, 0);
      sqlitex_mutex_leave(db->mutex);
      break;
    }

    /*  sqlitex_test_control(SQLITE_TESTCTRL_OPTIMIZATIONS, sqlitex *db, int N)
    **
    ** Enable or disable various optimizations for testing purposes.  The 
    ** argument N is a bitmask of optimizations to be disabled.  For normal
    ** operation N should be 0.  The idea is that a test program (like the
    ** SQL Logic Test or SLT test module) can run the same SQL multiple times
    ** with various optimizations disabled to verify that the same answer
    ** is obtained in every case.
    */
    case SQLITE_TESTCTRL_OPTIMIZATIONS: {
      sqlitex *db = va_arg(ap, sqlitex*);
      db->dbOptFlags = (u16)(va_arg(ap, int) & 0xffff);
      break;
    }

#ifdef SQLITE_N_KEYWORD
    /* sqlitex_test_control(SQLITE_TESTCTRL_ISKEYWORD, const char *zWord)
    **
    ** If zWord is a keyword recognized by the parser, then return the
    ** number of keywords.  Or if zWord is not a keyword, return 0.
    ** 
    ** This test feature is only available in the amalgamation since
    ** the SQLITE_N_KEYWORD macro is not defined in this file if SQLite
    ** is built using separate source files.
    */
    case SQLITE_TESTCTRL_ISKEYWORD: {
      const char *zWord = va_arg(ap, const char*);
      int n = sqlitexStrlen30(zWord);
      rc = (sqlitexKeywordCode((u8*)zWord, n)!=TK_ID) ? SQLITE_N_KEYWORD : 0;
      break;
    }
#endif 

    /* sqlitex_test_control(SQLITE_TESTCTRL_SCRATCHMALLOC, sz, &pNew, pFree);
    **
    ** Pass pFree into sqlitexScratchFree(). 
    ** If sz>0 then allocate a scratch buffer into pNew.  
    */
    case SQLITE_TESTCTRL_SCRATCHMALLOC: {
      void *pFree, **ppNew;
      int sz;
      sz = va_arg(ap, int);
      ppNew = va_arg(ap, void**);
      pFree = va_arg(ap, void*);
      if( sz ) *ppNew = sqlitexScratchMalloc(sz);
      sqlitexScratchFree(pFree);
      break;
    }

    /*   sqlitex_test_control(SQLITE_TESTCTRL_LOCALTIME_FAULT, int onoff);
    **
    ** If parameter onoff is non-zero, configure the wrappers so that all
    ** subsequent calls to localtime() and variants fail. If onoff is zero,
    ** undo this setting.
    */
    case SQLITE_TESTCTRL_LOCALTIME_FAULT: {
      sqlitexGlobalConfig.bLocaltimeFault = va_arg(ap, int);
      break;
    }

    /*   sqlitex_test_control(SQLITE_TESTCTRL_NEVER_CORRUPT, int);
    **
    ** Set or clear a flag that indicates that the database file is always well-
    ** formed and never corrupt.  This flag is clear by default, indicating that
    ** database files might have arbitrary corruption.  Setting the flag during
    ** testing causes certain assert() statements in the code to be activated
    ** that demonstrat invariants on well-formed database files.
    */
    case SQLITE_TESTCTRL_NEVER_CORRUPT: {
      sqlitexGlobalConfig.neverCorrupt = va_arg(ap, int);
      break;
    }


    /*   sqlitex_test_control(SQLITE_TESTCTRL_VDBE_COVERAGE, xCallback, ptr);
    **
    ** Set the VDBE coverage callback function to xCallback with context 
    ** pointer ptr.
    */
    case SQLITE_TESTCTRL_VDBE_COVERAGE: {
#ifdef SQLITE_VDBE_COVERAGE
      typedef void (*branch_callback)(void*,int,u8,u8);
      sqlitexGlobalConfig.xVdbeBranch = va_arg(ap,branch_callback);
      sqlitexGlobalConfig.pVdbeBranchArg = va_arg(ap,void*);
#endif
      break;
    }

    /*   sqlitex_test_control(SQLITE_TESTCTRL_SORTER_MMAP, db, nMax); */
    case SQLITE_TESTCTRL_SORTER_MMAP: {
      sqlitex *db = va_arg(ap, sqlitex*);
      db->nMaxSorterMmap = va_arg(ap, int);
      break;
    }

    /*   sqlitex_test_control(SQLITE_TESTCTRL_ISINIT);
    **
    ** Return SQLITE_OK if SQLite has been initialized and SQLITE_ERROR if
    ** not.
    */
    case SQLITE_TESTCTRL_ISINIT: {
      if( sqlitexGlobalConfig.isInit==0 ) rc = SQLITE_ERROR;
      break;
    }

    /*  sqlitex_test_control(SQLITE_TESTCTRL_IMPOSTER, db, dbName, onOff, tnum);
    **
    ** This test control is used to create imposter tables.  "db" is a pointer
    ** to the database connection.  dbName is the database name (ex: "main" or
    ** "temp") which will receive the imposter.  "onOff" turns imposter mode on
    ** or off.  "tnum" is the root page of the b-tree to which the imposter
    ** table should connect.
    **
    ** Enable imposter mode only when the schema has already been parsed.  Then
    ** run a single CREATE TABLE statement to construct the imposter table in
    ** the parsed schema.  Then turn imposter mode back off again.
    **
    ** If onOff==0 and tnum>0 then reset the schema for all databases, causing
    ** the schema to be reparsed the next time it is needed.  This has the
    ** effect of erasing all imposter tables.
    */
    case SQLITE_TESTCTRL_IMPOSTER: {
      sqlitex *db = va_arg(ap, sqlitex*);
      sqlitex_mutex_enter(db->mutex);
      db->init.iDb = sqlitexFindDbName(db, va_arg(ap,const char*));
      db->init.busy = db->init.imposterTable = va_arg(ap,int);
      db->init.newTnum = va_arg(ap,int);
      if( db->init.busy==0 && db->init.newTnum>0 ){
        sqlitexResetAllSchemasOfConnection(db);
      }
      sqlitex_mutex_leave(db->mutex);
      break;
    }
  }
  va_end(ap);
#endif /* SQLITE_OMIT_BUILTIN_TEST */
  return rc;
}

/*
** This is a utility routine, useful to VFS implementations, that checks
** to see if a database file was a URI that contained a specific query 
** parameter, and if so obtains the value of the query parameter.
**
** The zFilename argument is the filename pointer passed into the xOpen()
** method of a VFS implementation.  The zParam argument is the name of the
** query parameter we seek.  This routine returns the value of the zParam
** parameter if it exists.  If the parameter does not exist, this routine
** returns a NULL pointer.
*/
const char *sqlitex_uri_parameter(const char *zFilename, const char *zParam){
  if( zFilename==0 || zParam==0 ) return 0;
  zFilename += sqlitexStrlen30(zFilename) + 1;
  while( zFilename[0] ){
    int x = strcmp(zFilename, zParam);
    zFilename += sqlitexStrlen30(zFilename) + 1;
    if( x==0 ) return zFilename;
    zFilename += sqlitexStrlen30(zFilename) + 1;
  }
  return 0;
}

/*
** Return a boolean value for a query parameter.
*/
int sqlitex_uri_boolean(const char *zFilename, const char *zParam, int bDflt){
  const char *z = sqlitex_uri_parameter(zFilename, zParam);
  bDflt = bDflt!=0;
  return z ? sqlitexGetBoolean(z, bDflt) : bDflt;
}

/*
** Return a 64-bit integer value for a query parameter.
*/
sqlitex_int64 sqlitex_uri_int64(
  const char *zFilename,    /* Filename as passed to xOpen */
  const char *zParam,       /* URI parameter sought */
  sqlitex_int64 bDflt       /* return if parameter is missing */
){
  const char *z = sqlitex_uri_parameter(zFilename, zParam);
  sqlitex_int64 v;
  if( z && sqlitexDecOrHexToI64(z, &v)==SQLITE_OK ){
    bDflt = v;
  }
  return bDflt;
}

/*
** Return the Btree pointer identified by zDbName.  Return NULL if not found.
*/
Btree *sqlitexDbNameToBtree(sqlitex *db, const char *zDbName){
  int i;
  for(i=0; i<db->nDb; i++){
    if( db->aDb[i].pBt
     && (zDbName==0 || sqlitexStrICmp(zDbName, db->aDb[i].zName)==0)
    ){
      return db->aDb[i].pBt;
    }
  }
  return 0;
}

/*
** Return the filename of the database associated with a database
** connection.
*/
const char *sqlitex_db_filename(sqlitex *db, const char *zDbName){
  Btree *pBt;
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
  pBt = sqlitexDbNameToBtree(db, zDbName);
  return pBt ? sqlite3BtreeGetFilename(pBt) : 0;
}

/*
** Return 1 if database is read-only or 0 if read/write.  Return -1 if
** no such database exists.
*/
int sqlitex_db_readonly(sqlitex *db, const char *zDbName){
  Btree *pBt;
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlitexSafetyCheckOk(db) ){
    (void)SQLITE_MISUSE_BKPT;
    return -1;
  }
#endif
  pBt = sqlitexDbNameToBtree(db, zDbName);
  return pBt ? sqlite3BtreeIsReadonly(pBt) : -1;
}
