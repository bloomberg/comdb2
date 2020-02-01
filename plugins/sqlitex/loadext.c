/*
** 2006 June 7
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used to dynamically load extensions into
** the SQLite library.
*/

#ifndef SQLITE_CORE
  #define SQLITE_CORE 1  /* Disable the API redefinition in sqlitexext.h */
#endif
#include "sqlitexext.h"
#include "sqliteInt.h"
#include <string.h>

#ifndef SQLITE_OMIT_LOAD_EXTENSION

/*
** Some API routines are omitted when various features are
** excluded from a build of SQLite.  Substitute a NULL pointer
** for any missing APIs.
*/
#ifndef SQLITE_ENABLE_COLUMN_METADATA
# define sqlitex_column_database_name   0
# define sqlitex_column_database_name16 0
# define sqlitex_column_table_name      0
# define sqlitex_column_table_name16    0
# define sqlitex_column_origin_name     0
# define sqlitex_column_origin_name16   0
#endif

#ifdef SQLITE_OMIT_AUTHORIZATION
# define sqlitex_set_authorizer         0
#endif

#ifdef SQLITE_OMIT_UTF16
# define sqlitex_bind_text16            0
# define sqlitex_collation_needed16     0
# define sqlitex_column_decltype16      0
# define sqlitex_column_name16          0
# define sqlitex_column_text16          0
# define sqlitex_complete16             0
# define sqlitex_create_collation16     0
# define sqlitex_create_function16      0
# define sqlitex_errmsg16               0
# define sqlitex_open16                 0
# define sqlitex_prepare16              0
# define sqlitex_prepare16_v2           0
# define sqlitex_result_error16         0
# define sqlitex_result_text16          0
# define sqlitex_result_text16be        0
# define sqlitex_result_text16le        0
# define sqlitex_value_text16           0
# define sqlitex_value_text16be         0
# define sqlitex_value_text16le         0
# define sqlitex_column_database_name16 0
# define sqlitex_column_table_name16    0
# define sqlitex_column_origin_name16   0
#endif

#ifdef SQLITE_OMIT_COMPLETE
# define sqlitex_complete 0
# define sqlitex_complete16 0
#endif

#ifdef SQLITE_OMIT_DECLTYPE
# define sqlitex_column_decltype16      0
# define sqlitex_column_decltype        0
#endif

#ifdef SQLITE_OMIT_PROGRESS_CALLBACK
# define sqlitex_progress_handler 0
#endif

#ifdef SQLITE_OMIT_VIRTUALTABLE
# define sqlitex_create_module 0
# define sqlitex_create_module_v2 0
# define sqlitex_declare_vtab 0
# define sqlitex_vtab_config 0
# define sqlitex_vtab_on_conflict 0
#endif

#ifdef SQLITE_OMIT_SHARED_CACHE
# define sqlitex_enable_shared_cache 0
#endif

#ifdef SQLITE_OMIT_TRACE
# define sqlitex_profile       0
# define sqlitex_trace         0
#endif

#ifdef SQLITE_OMIT_GET_TABLE
# define sqlitex_free_table    0
# define sqlitex_get_table     0
#endif

#ifdef SQLITE_OMIT_INCRBLOB
#define sqlitex_bind_zeroblob  0
#define sqlitex_blob_bytes     0
#define sqlitex_blob_close     0
#define sqlitex_blob_open      0
#define sqlitex_blob_read      0
#define sqlitex_blob_write     0
#define sqlitex_blob_reopen    0
#endif

/*
** The following structure contains pointers to all SQLite API routines.
** A pointer to this structure is passed into extensions when they are
** loaded so that the extension can make calls back into the SQLite
** library.
**
** When adding new APIs, add them to the bottom of this structure
** in order to preserve backwards compatibility.
**
** Extensions that use newer APIs should first call the
** sqlitex_libversion_number() to make sure that the API they
** intend to use is supported by the library.  Extensions should
** also check to make sure that the pointer to the function is
** not NULL before calling it.
*/
static const sqlitex_api_routines sqlitexApis = {
  sqlitex_aggregate_context,
#ifndef SQLITE_OMIT_DEPRECATED
  sqlitex_aggregate_count,
#else
  0,
#endif
  sqlitex_bind_blob,
  sqlitex_bind_double,
  sqlitex_bind_int,
  sqlitex_bind_int64,
  sqlitex_bind_null,
  sqlitex_bind_parameter_count,
  sqlitex_bind_parameter_index,
  sqlitex_bind_parameter_name,
  sqlitex_bind_text,
  sqlitex_bind_text16,
  sqlitex_bind_value,
  sqlitex_busy_handler,
  sqlitex_busy_timeout,
  sqlitex_changes,
  sqlitex_close,
  sqlitex_collation_needed,
  sqlitex_collation_needed16,
  sqlitex_column_blob,
  sqlitex_column_bytes,
  sqlitex_column_bytes16,
  sqlitex_column_count,
  sqlitex_column_database_name,
  sqlitex_column_database_name16,
  sqlitex_column_decltype,
  sqlitex_column_decltype16,
  sqlitex_column_double,
  sqlitex_column_int,
  sqlitex_column_int64,
  sqlitex_column_name,
  sqlitex_column_name16,
  sqlitex_column_origin_name,
  sqlitex_column_origin_name16,
  sqlitex_column_table_name,
  sqlitex_column_table_name16,
  sqlitex_column_text,
  sqlitex_column_text16,
  sqlitex_column_type,
  sqlitex_column_value,
  sqlitex_commit_hook,
  sqlitex_complete,
  sqlitex_complete16,
  sqlitex_create_collation,
  sqlitex_create_collation16,
  sqlitex_create_function,
  sqlitex_create_function16,
  sqlitex_create_module,
  sqlitex_data_count,
  sqlitex_db_handle,
  sqlitex_declare_vtab,
  sqlitex_enable_shared_cache,
  sqlitex_errcode,
  sqlitex_errmsg,
  sqlitex_errmsg16,
  sqlitex_exec,
#ifndef SQLITE_OMIT_DEPRECATED
  sqlitex_expired,
#else
  0,
#endif
  sqlitex_finalize,
  sqlitex_free,
  sqlitex_free_table,
  sqlitex_get_autocommit,
  sqlitex_get_auxdata,
  sqlitex_get_table,
  0,     /* Was sqlitex_global_recover(), but that function is deprecated */
  sqlitex_interrupt,
#ifndef SQLITE_BUILDING_FOR_COMDB2
  sqlitex_last_insert_rowid,
#else
  0,
#endif
  sqlitex_libversion,
  sqlitex_libversion_number,
  sqlitex_malloc,
  sqlitex_mprintf,
  sqlitex_open,
  sqlitex_open16,
  sqlitex_prepare,
  sqlitex_prepare16,
  sqlitex_profile,
  sqlitex_progress_handler,
  sqlitex_realloc,
  sqlitex_reset,
  sqlitex_result_blob,
  sqlitex_result_double,
  sqlitex_result_error,
  sqlitex_result_error16,
  sqlitex_result_int,
  sqlitex_result_int64,
  sqlitex_result_null,
  sqlitex_result_text,
  sqlitex_result_text16,
  sqlitex_result_text16be,
  sqlitex_result_text16le,
  sqlitex_result_value,
  sqlitex_rollback_hook,
  sqlitex_set_authorizer,
  sqlitex_set_auxdata,
  sqlitex_snprintf,
  sqlitex_step,
  sqlitex_table_column_metadata,
#ifndef SQLITE_OMIT_DEPRECATED
  sqlitex_thread_cleanup,
#else
  0,
#endif
  sqlitex_total_changes,
  sqlitex_trace,
#ifndef SQLITE_OMIT_DEPRECATED
  sqlitex_transfer_bindings,
#else
  0,
#endif
  sqlitex_update_hook,
  sqlitex_user_data,
  sqlitex_value_blob,
  sqlitex_value_bytes,
  sqlitex_value_bytes16,
  sqlitex_value_double,
  sqlitex_value_int,
  sqlitex_value_int64,
  sqlitex_value_numeric_type,
  sqlitex_value_text,
  sqlitex_value_text16,
  sqlitex_value_text16be,
  sqlitex_value_text16le,
  sqlitex_value_type,
  sqlitex_vmprintf,
  /*
  ** The original API set ends here.  All extensions can call any
  ** of the APIs above provided that the pointer is not NULL.  But
  ** before calling APIs that follow, extension should check the
  ** sqlitex_libversion_number() to make sure they are dealing with
  ** a library that is new enough to support that API.
  *************************************************************************
  */
  sqlitex_overload_function,

  /*
  ** Added after 3.3.13
  */
  sqlitex_prepare_v2,
  sqlitex_prepare16_v2,
  sqlitex_clear_bindings,

  /*
  ** Added for 3.4.1
  */
  sqlitex_create_module_v2,

  /*
  ** Added for 3.5.0
  */
  sqlitex_bind_zeroblob,
  sqlitex_blob_bytes,
  sqlitex_blob_close,
  sqlitex_blob_open,
  sqlitex_blob_read,
  sqlitex_blob_write,
  sqlitex_create_collation_v2,
  sqlitex_file_control,
  sqlitex_memory_highwater,
  sqlitex_memory_used,
#ifdef SQLITE_MUTEX_OMIT
  0, 
  0, 
  0,
  0,
  0,
#else
  sqlitex_mutex_alloc,
  sqlitex_mutex_enter,
  sqlitex_mutex_free,
  sqlitex_mutex_leave,
  sqlitex_mutex_try,
#endif
  sqlitex_open_v2,
  sqlitex_release_memory,
  sqlitex_result_error_nomem,
  sqlitex_result_error_toobig,
  sqlitex_sleep,
  sqlitex_soft_heap_limit,
  sqlitex_vfs_find,
  sqlitex_vfs_register,
  sqlitex_vfs_unregister,

  /*
  ** Added for 3.5.8
  */
  sqlitex_threadsafe,
  sqlitex_result_zeroblob,
  sqlitex_result_error_code,
  sqlitex_test_control,
  sqlitex_randomness,
  sqlitex_context_db_handle,

  /*
  ** Added for 3.6.0
  */
  sqlitex_extended_result_codes,
  sqlitex_limit,
  sqlitex_next_stmt,
  sqlitex_sql,
  sqlitex_status,

  /*
  ** Added for 3.7.4
  */
#ifndef SQLITE_BUILDING_FOR_COMDB2
  sqlitex_backup_finish,
  sqlitex_backup_init,
  sqlitex_backup_pagecount,
  sqlitex_backup_remaining,
  sqlitex_backup_step,
#else
  0,
  0,
  0,
  0,
  0,
#endif
#ifndef SQLITE_OMIT_COMPILEOPTION_DIAGS
  sqlitex_compileoption_get,
  sqlitex_compileoption_used,
#else
  0,
  0,
#endif
  sqlitex_create_function_v2,
  sqlitex_db_config,
  sqlitex_db_mutex,
  sqlitex_db_status,
  sqlitex_extended_errcode,
  sqlitex_log,
  sqlitex_soft_heap_limit64,
  sqlitex_sourceid,
  sqlitex_stmt_status,
  sqlitex_strnicmp,
#ifdef SQLITE_ENABLE_UNLOCK_NOTIFY
  sqlitex_unlock_notify,
#else
  0,
#endif
#ifndef SQLITE_OMIT_WAL
  sqlitex_wal_autocheckpoint,
  sqlitex_wal_checkpoint,
  sqlitex_wal_hook,
#else
  0,
  0,
  0,
#endif
  sqlitex_blob_reopen,
  sqlitex_vtab_config,
  sqlitex_vtab_on_conflict,
  sqlitex_close_v2,
  sqlitex_db_filename,
  sqlitex_db_readonly,
  sqlitex_db_release_memory,
  sqlitex_errstr,
  sqlitex_stmt_busy,
  sqlitex_stmt_readonly,
  sqlitex_stricmp,
  sqlitex_uri_boolean,
  sqlitex_uri_int64,
  sqlitex_uri_parameter,
  sqlitex_vsnprintf,
  sqlitex_wal_checkpoint_v2,
  /* Version 3.8.7 and later */
  sqlitex_auto_extension,
  sqlitex_bind_blob64,
  sqlitex_bind_text64,
  sqlitex_cancel_auto_extension,
  sqlitex_load_extension,
  sqlitex_malloc64,
  sqlitex_msize,
  sqlitex_realloc64,
  sqlitex_reset_auto_extension,
  sqlitex_result_blob64,
  sqlitex_result_text64,
  sqlitex_strglob,
  /* Backported: Version 3.26.0 and later */
#ifdef SQLITE_ENABLE_NORMALIZE
  sqlitex_normalized_sql
#else
  0
#endif
};

/*
** Attempt to load an SQLite extension library contained in the file
** zFile.  The entry point is zProc.  zProc may be 0 in which case a
** default entry point name (sqlitex_extension_init) is used.  Use
** of the default name is recommended.
**
** Return SQLITE_OK on success and SQLITE_ERROR if something goes wrong.
**
** If an error occurs and pzErrMsg is not 0, then fill *pzErrMsg with 
** error message text.  The calling function should free this memory
** by calling sqlitexDbFree(db, ).
*/
static int sqlitexLoadExtension(
  sqlitex *db,          /* Load the extension into this database connection */
  const char *zFile,    /* Name of the shared library containing extension */
  const char *zProc,    /* Entry point.  Use "sqlitex_extension_init" if 0 */
  char **pzErrMsg       /* Put error message here if not 0 */
){
  sqlitex_vfs *pVfs = db->pVfs;
  void *handle;
  int (*xInit)(sqlitex*,char**,const sqlitex_api_routines*);
  char *zErrmsg = 0;
  const char *zEntry;
  char *zAltEntry = 0;
  void **aHandle;
  int nMsg = 300 + sqlitexStrlen30(zFile);
  int ii;

  /* Shared library endings to try if zFile cannot be loaded as written */
  static const char *azEndings[] = {
#if SQLITE_OS_WIN
     "dll"   
#elif defined(__APPLE__)
     "dylib"
#else
     "so"
#endif
  };


  if( pzErrMsg ) *pzErrMsg = 0;

  /* Ticket #1863.  To avoid a creating security problems for older
  ** applications that relink against newer versions of SQLite, the
  ** ability to run load_extension is turned off by default.  One
  ** must call sqlitex_enable_load_extension() to turn on extension
  ** loading.  Otherwise you get the following error.
  */
  if( (db->flags & SQLITE_LoadExtension)==0 ){
    if( pzErrMsg ){
      *pzErrMsg = sqlitex_mprintf("not authorized");
    }
    return SQLITE_ERROR;
  }

  zEntry = zProc ? zProc : "sqlitex_extension_init";

  handle = sqlitexOsDlOpen(pVfs, zFile);
#if SQLITE_OS_UNIX || SQLITE_OS_WIN
  for(ii=0; ii<ArraySize(azEndings) && handle==0; ii++){
    char *zAltFile = sqlitex_mprintf("%s.%s", zFile, azEndings[ii]);
    if( zAltFile==0 ) return SQLITE_NOMEM;
    handle = sqlitexOsDlOpen(pVfs, zAltFile);
    sqlitex_free(zAltFile);
  }
#endif
  if( handle==0 ){
    if( pzErrMsg ){
      *pzErrMsg = zErrmsg = sqlitex_malloc(nMsg);
      if( zErrmsg ){
        sqlitex_snprintf(nMsg, zErrmsg, 
            "unable to open shared library [%s]", zFile);
        sqlitexOsDlError(pVfs, nMsg-1, zErrmsg);
      }
    }
    return SQLITE_ERROR;
  }
  xInit = (int(*)(sqlitex*,char**,const sqlitex_api_routines*))
                   sqlitexOsDlSym(pVfs, handle, zEntry);

  /* If no entry point was specified and the default legacy
  ** entry point name "sqlitex_extension_init" was not found, then
  ** construct an entry point name "sqlitex_X_init" where the X is
  ** replaced by the lowercase value of every ASCII alphabetic 
  ** character in the filename after the last "/" upto the first ".",
  ** and eliding the first three characters if they are "lib".  
  ** Examples:
  **
  **    /usr/local/lib/libExample5.4.3.so ==>  sqlitex_example_init
  **    C:/lib/mathfuncs.dll              ==>  sqlitex_mathfuncs_init
  */
  if( xInit==0 && zProc==0 ){
    int iFile, iEntry, c;
    int ncFile = sqlitexStrlen30(zFile);
    zAltEntry = sqlitex_malloc(ncFile+30);
    if( zAltEntry==0 ){
      sqlitexOsDlClose(pVfs, handle);
      return SQLITE_NOMEM;
    }
    memcpy(zAltEntry, "sqlitex_", 8);
    for(iFile=ncFile-1; iFile>=0 && zFile[iFile]!='/'; iFile--){}
    iFile++;
    if( sqlitex_strnicmp(zFile+iFile, "lib", 3)==0 ) iFile += 3;
    for(iEntry=8; (c = zFile[iFile])!=0 && c!='.'; iFile++){
      if( sqlitexIsalpha(c) ){
        zAltEntry[iEntry++] = (char)sqlitexUpperToLower[(unsigned)c];
      }
    }
    memcpy(zAltEntry+iEntry, "_init", 6);
    zEntry = zAltEntry;
    xInit = (int(*)(sqlitex*,char**,const sqlitex_api_routines*))
                     sqlitexOsDlSym(pVfs, handle, zEntry);
  }
  if( xInit==0 ){
    if( pzErrMsg ){
      nMsg += sqlitexStrlen30(zEntry);
      *pzErrMsg = zErrmsg = sqlitex_malloc(nMsg);
      if( zErrmsg ){
        sqlitex_snprintf(nMsg, zErrmsg,
            "no entry point [%s] in shared library [%s]", zEntry, zFile);
        sqlitexOsDlError(pVfs, nMsg-1, zErrmsg);
      }
    }
    sqlitexOsDlClose(pVfs, handle);
    sqlitex_free(zAltEntry);
    return SQLITE_ERROR;
  }
  sqlitex_free(zAltEntry);
  if( xInit(db, &zErrmsg, &sqlitexApis) ){
    if( pzErrMsg ){
      *pzErrMsg = sqlitex_mprintf("error during initialization: %s", zErrmsg);
    }
    sqlitex_free(zErrmsg);
    sqlitexOsDlClose(pVfs, handle);
    return SQLITE_ERROR;
  }

  /* Append the new shared library handle to the db->aExtension array. */
  aHandle = sqlitexDbMallocZero(db, sizeof(handle)*(db->nExtension+1));
  if( aHandle==0 ){
    return SQLITE_NOMEM;
  }
  if( db->nExtension>0 ){
    memcpy(aHandle, db->aExtension, sizeof(handle)*db->nExtension);
  }
  sqlitexDbFree(db, db->aExtension);
  db->aExtension = aHandle;

  db->aExtension[db->nExtension++] = handle;
  return SQLITE_OK;
}
int sqlitex_load_extension(
  sqlitex *db,          /* Load the extension into this database connection */
  const char *zFile,    /* Name of the shared library containing extension */
  const char *zProc,    /* Entry point.  Use "sqlitex_extension_init" if 0 */
  char **pzErrMsg       /* Put error message here if not 0 */
){
  int rc;
  sqlitex_mutex_enter(db->mutex);
  rc = sqlitexLoadExtension(db, zFile, zProc, pzErrMsg);
  rc = sqlitexApiExit(db, rc);
  sqlitex_mutex_leave(db->mutex);
  return rc;
}

/*
** Call this routine when the database connection is closing in order
** to clean up loaded extensions
*/
void sqlitexCloseExtensions(sqlitex *db){
  int i;
  assert( sqlitex_mutex_held(db->mutex) );
  for(i=0; i<db->nExtension; i++){
    sqlitexOsDlClose(db->pVfs, db->aExtension[i]);
  }
  sqlitexDbFree(db, db->aExtension);
}

/*
** Enable or disable extension loading.  Extension loading is disabled by
** default so as not to open security holes in older applications.
*/
int sqlitex_enable_load_extension(sqlitex *db, int onoff){
  sqlitex_mutex_enter(db->mutex);
  if( onoff ){
    db->flags |= SQLITE_LoadExtension;
  }else{
    db->flags &= ~SQLITE_LoadExtension;
  }
  sqlitex_mutex_leave(db->mutex);
  return SQLITE_OK;
}

#endif /* SQLITE_OMIT_LOAD_EXTENSION */

/*
** The auto-extension code added regardless of whether or not extension
** loading is supported.  We need a dummy sqlitexApis pointer for that
** code if regular extension loading is not available.  This is that
** dummy pointer.
*/
#ifdef SQLITE_OMIT_LOAD_EXTENSION
static const sqlitex_api_routines sqlitexApis = { 0 };
#endif


/*
** The following object holds the list of automatically loaded
** extensions.
**
** This list is shared across threads.  The SQLITE_MUTEX_STATIC_MASTER
** mutex must be held while accessing this list.
*/
typedef struct sqlitexAutoExtList sqlitexAutoExtList;
static SQLITE_WSD struct sqlitexAutoExtList {
  int nExt;              /* Number of entries in aExt[] */          
  void (**aExt)(void);   /* Pointers to the extension init functions */
} sqlitexAutoext = { 0, 0 };

/* The "wsdAutoext" macro will resolve to the autoextension
** state vector.  If writable static data is unsupported on the target,
** we have to locate the state vector at run-time.  In the more common
** case where writable static data is supported, wsdStat can refer directly
** to the "sqlitexAutoext" state vector declared above.
*/
#ifdef SQLITE_OMIT_WSD
# define wsdAutoextInit \
  sqlitexAutoExtList *x = &GLOBAL(sqlitexAutoExtList,sqlitexAutoext)
# define wsdAutoext x[0]
#else
# define wsdAutoextInit
# define wsdAutoext sqlitexAutoext
#endif


/*
** Register a statically linked extension that is automatically
** loaded by every new database connection.
*/
int sqlitex_auto_extension(void (*xInit)(void)){
  int rc = SQLITE_OK;
#ifndef SQLITE_OMIT_AUTOINIT
  rc = sqlitex_initialize();
  if( rc ){
    return rc;
  }else
#endif
  {
    int i;
#if SQLITE_THREADSAFE
    sqlitex_mutex *mutex = sqlitexMutexAlloc(SQLITE_MUTEX_STATIC_MASTER);
#endif
    wsdAutoextInit;
    sqlitex_mutex_enter(mutex);
    for(i=0; i<wsdAutoext.nExt; i++){
      if( wsdAutoext.aExt[i]==xInit ) break;
    }
    if( i==wsdAutoext.nExt ){
      int nByte = (wsdAutoext.nExt+1)*sizeof(wsdAutoext.aExt[0]);
      void (**aNew)(void);
      aNew = sqlitex_realloc(wsdAutoext.aExt, nByte);
      if( aNew==0 ){
        rc = SQLITE_NOMEM;
      }else{
        wsdAutoext.aExt = aNew;
        wsdAutoext.aExt[wsdAutoext.nExt] = xInit;
        wsdAutoext.nExt++;
      }
    }
    sqlitex_mutex_leave(mutex);
    assert( (rc&0xff)==rc );
    return rc;
  }
}

/*
** Cancel a prior call to sqlitex_auto_extension.  Remove xInit from the
** set of routines that is invoked for each new database connection, if it
** is currently on the list.  If xInit is not on the list, then this
** routine is a no-op.
**
** Return 1 if xInit was found on the list and removed.  Return 0 if xInit
** was not on the list.
*/
int sqlitex_cancel_auto_extension(void (*xInit)(void)){
#if SQLITE_THREADSAFE
  sqlitex_mutex *mutex = sqlitexMutexAlloc(SQLITE_MUTEX_STATIC_MASTER);
#endif
  int i;
  int n = 0;
  wsdAutoextInit;
  sqlitex_mutex_enter(mutex);
  for(i=wsdAutoext.nExt-1; i>=0; i--){
    if( wsdAutoext.aExt[i]==xInit ){
      wsdAutoext.nExt--;
      wsdAutoext.aExt[i] = wsdAutoext.aExt[wsdAutoext.nExt];
      n++;
      break;
    }
  }
  sqlitex_mutex_leave(mutex);
  return n;
}

/*
** Reset the automatic extension loading mechanism.
*/
void sqlitex_reset_auto_extension(void){
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlitex_initialize()==SQLITE_OK )
#endif
  {
#if SQLITE_THREADSAFE
    sqlitex_mutex *mutex = sqlitexMutexAlloc(SQLITE_MUTEX_STATIC_MASTER);
#endif
    wsdAutoextInit;
    sqlitex_mutex_enter(mutex);
    sqlitex_free(wsdAutoext.aExt);
    wsdAutoext.aExt = 0;
    wsdAutoext.nExt = 0;
    sqlitex_mutex_leave(mutex);
  }
}

/*
** Load all automatic extensions.
**
** If anything goes wrong, set an error in the database connection.
*/
void sqlitexAutoLoadExtensions(sqlitex *db){
  int i;
  int go = 1;
  int rc;
  int (*xInit)(sqlitex*,char**,const sqlitex_api_routines*);

  wsdAutoextInit;
  if( wsdAutoext.nExt==0 ){
    /* Common case: early out without every having to acquire a mutex */
    return;
  }
  for(i=0; go; i++){
    char *zErrmsg;
#if SQLITE_THREADSAFE
    sqlitex_mutex *mutex = sqlitexMutexAlloc(SQLITE_MUTEX_STATIC_MASTER);
#endif
    sqlitex_mutex_enter(mutex);
    if( i>=wsdAutoext.nExt ){
      xInit = 0;
      go = 0;
    }else{
      xInit = (int(*)(sqlitex*,char**,const sqlitex_api_routines*))
              wsdAutoext.aExt[i];
    }
    sqlitex_mutex_leave(mutex);
    zErrmsg = 0;
    if( xInit && (rc = xInit(db, &zErrmsg, &sqlitexApis))!=0 ){
      sqlitexErrorWithMsg(db, rc,
            "automatic extension loading failed: %s", zErrmsg);
      go = 0;
    }
    sqlitex_free(zErrmsg);
  }
}
