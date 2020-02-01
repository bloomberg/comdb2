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
** This header file defines the SQLite interface for use by
** shared libraries that want to be imported as extensions into
** an SQLite instance.  Shared libraries that intend to be loaded
** as extensions by SQLite should #include this file instead of 
** sqlitex.h.
*/
#ifndef _SQLITE3EXT_H_
#define _SQLITE3EXT_H_
#include "sqlitex.h"

typedef struct sqlitex_api_routines sqlitex_api_routines;

/*
** The following structure holds pointers to all of the SQLite API
** routines.
**
** WARNING:  In order to maintain backwards compatibility, add new
** interfaces to the end of this structure only.  If you insert new
** interfaces in the middle of this structure, then older different
** versions of SQLite will not be able to load each other's shared
** libraries!
*/
struct sqlitex_api_routines {
  void * (*aggregate_context)(sqlitex_context*,int nBytes);
  int  (*aggregate_count)(sqlitex_context*);
  int  (*bind_blob)(sqlitex_stmt*,int,const void*,int n,void(*)(void*));
  int  (*bind_double)(sqlitex_stmt*,int,double);
  int  (*bind_int)(sqlitex_stmt*,int,int);
  int  (*bind_int64)(sqlitex_stmt*,int,sqlite_int64);
  int  (*bind_null)(sqlitex_stmt*,int);
  int  (*bind_parameter_count)(sqlitex_stmt*);
  int  (*bind_parameter_index)(sqlitex_stmt*,const char*zName);
  const char * (*bind_parameter_name)(sqlitex_stmt*,int);
  int  (*bind_text)(sqlitex_stmt*,int,const char*,int n,void(*)(void*));
  int  (*bind_text16)(sqlitex_stmt*,int,const void*,int,void(*)(void*));
  int  (*bind_value)(sqlitex_stmt*,int,const sqlitex_value*);
  int  (*busy_handler)(sqlitex*,int(*)(void*,int),void*);
  int  (*busy_timeout)(sqlitex*,int ms);
  int  (*changes)(sqlitex*);
  int  (*close)(sqlitex*);
  int  (*collation_needed)(sqlitex*,void*,void(*)(void*,sqlitex*,
                           int eTextRep,const char*));
  int  (*collation_needed16)(sqlitex*,void*,void(*)(void*,sqlitex*,
                             int eTextRep,const void*));
  const void * (*column_blob)(sqlitex_stmt*,int iCol);
  int  (*column_bytes)(sqlitex_stmt*,int iCol);
  int  (*column_bytes16)(sqlitex_stmt*,int iCol);
  int  (*column_count)(sqlitex_stmt*pStmt);
  const char * (*column_database_name)(sqlitex_stmt*,int);
  const void * (*column_database_name16)(sqlitex_stmt*,int);
  const char * (*column_decltype)(sqlitex_stmt*,int i);
  const void * (*column_decltype16)(sqlitex_stmt*,int);
  double  (*column_double)(sqlitex_stmt*,int iCol);
  int  (*column_int)(sqlitex_stmt*,int iCol);
  sqlite_int64  (*column_int64)(sqlitex_stmt*,int iCol);
  const char * (*column_name)(sqlitex_stmt*,int);
  const void * (*column_name16)(sqlitex_stmt*,int);
  const char * (*column_origin_name)(sqlitex_stmt*,int);
  const void * (*column_origin_name16)(sqlitex_stmt*,int);
  const char * (*column_table_name)(sqlitex_stmt*,int);
  const void * (*column_table_name16)(sqlitex_stmt*,int);
  const unsigned char * (*column_text)(sqlitex_stmt*,int iCol);
  const void * (*column_text16)(sqlitex_stmt*,int iCol);
  int  (*column_type)(sqlitex_stmt*,int iCol);
  sqlitex_value* (*column_value)(sqlitex_stmt*,int iCol);
  void * (*commit_hook)(sqlitex*,int(*)(void*),void*);
  int  (*complete)(const char*sql);
  int  (*complete16)(const void*sql);
  int  (*create_collation)(sqlitex*,const char*,int,void*,
                           int(*)(void*,int,const void*,int,const void*));
  int  (*create_collation16)(sqlitex*,const void*,int,void*,
                             int(*)(void*,int,const void*,int,const void*));
  int  (*create_function)(sqlitex*,const char*,int,int,void*,
                          void (*xFunc)(sqlitex_context*,int,sqlitex_value**),
                          void (*xStep)(sqlitex_context*,int,sqlitex_value**),
                          void (*xFinal)(sqlitex_context*));
  int  (*create_function16)(sqlitex*,const void*,int,int,void*,
                            void (*xFunc)(sqlitex_context*,int,sqlitex_value**),
                            void (*xStep)(sqlitex_context*,int,sqlitex_value**),
                            void (*xFinal)(sqlitex_context*));
  int (*create_module)(sqlitex*,const char*,const sqlitex_module*,void*);
  int  (*data_count)(sqlitex_stmt*pStmt);
  sqlitex * (*db_handle)(sqlitex_stmt*);
  int (*declare_vtab)(sqlitex*,const char*);
  int  (*enable_shared_cache)(int);
  int  (*errcode)(sqlitex*db);
  const char * (*errmsg)(sqlitex*);
  const void * (*errmsg16)(sqlitex*);
  int  (*exec)(sqlitex*,const char*,sqlitex_callback,void*,char**);
  int  (*expired)(sqlitex_stmt*);
  int  (*finalize)(sqlitex_stmt*pStmt);
  void  (*free)(void*);
  void  (*free_table)(char**result);
  int  (*get_autocommit)(sqlitex*);
  void * (*get_auxdata)(sqlitex_context*,int);
  int  (*get_table)(sqlitex*,const char*,char***,int*,int*,char**);
  int  (*global_recover)(void);
  void  (*interruptx)(sqlitex*);
  sqlite_int64  (*last_insert_rowid)(sqlitex*);
  const char * (*libversion)(void);
  int  (*libversion_number)(void);
  void *(*malloc)(int);
  char * (*mprintf)(const char*,...);
  int  (*open)(const char*,sqlitex**,struct sqlthdstate*);
  int  (*open16)(const void*,sqlitex**);
  int  (*prepare)(sqlitex*,const char*,int,sqlitex_stmt**,const char**);
  int  (*prepare16)(sqlitex*,const void*,int,sqlitex_stmt**,const void**);
  void * (*profile)(sqlitex*,void(*)(void*,const char*,sqlite_uint64),void*);
  void  (*progress_handler)(sqlitex*,int,int(*)(void*),void*);
  void *(*realloc)(void*,int);
  int  (*reset)(sqlitex_stmt*pStmt);
  void  (*result_blob)(sqlitex_context*,const void*,int,void(*)(void*));
  void  (*result_double)(sqlitex_context*,double);
  void  (*result_error)(sqlitex_context*,const char*,int);
  void  (*result_error16)(sqlitex_context*,const void*,int);
  void  (*result_int)(sqlitex_context*,int);
  void  (*result_int64)(sqlitex_context*,sqlite_int64);
  void  (*result_null)(sqlitex_context*);
  void  (*result_text)(sqlitex_context*,const char*,int,void(*)(void*));
  void  (*result_text16)(sqlitex_context*,const void*,int,void(*)(void*));
  void  (*result_text16be)(sqlitex_context*,const void*,int,void(*)(void*));
  void  (*result_text16le)(sqlitex_context*,const void*,int,void(*)(void*));
  void  (*result_value)(sqlitex_context*,sqlitex_value*);
  void * (*rollback_hook)(sqlitex*,void(*)(void*),void*);
  int  (*set_authorizer)(sqlitex*,int(*)(void*,int,const char*,const char*,
                         const char*,const char*),void*);
  void  (*set_auxdata)(sqlitex_context*,int,void*,void (*)(void*));
  char * (*snprintf)(int,char*,const char*,...);
  int  (*step)(sqlitex_stmt*);
  int  (*table_column_metadata)(sqlitex*,const char*,const char*,const char*,
                                char const**,char const**,int*,int*,int*);
  void  (*thread_cleanup)(void);
  int  (*total_changes)(sqlitex*);
  void * (*trace)(sqlitex*,void(*xTrace)(void*,const char*),void*);
  int  (*transfer_bindings)(sqlitex_stmt*,sqlitex_stmt*);
  void * (*update_hook)(sqlitex*,void(*)(void*,int ,char const*,char const*,
                                         sqlite_int64),void*);
  void * (*user_data)(sqlitex_context*);
  const void * (*value_blob)(sqlitex_value*);
  int  (*value_bytes)(sqlitex_value*);
  int  (*value_bytes16)(sqlitex_value*);
  double  (*value_double)(sqlitex_value*);
  int  (*value_int)(sqlitex_value*);
  sqlite_int64  (*value_int64)(sqlitex_value*);
  int  (*value_numeric_type)(sqlitex_value*);
  const unsigned char * (*value_text)(sqlitex_value*);
  const void * (*value_text16)(sqlitex_value*);
  const void * (*value_text16be)(sqlitex_value*);
  const void * (*value_text16le)(sqlitex_value*);
  int  (*value_type)(sqlitex_value*);
  char *(*vmprintf)(const char*,va_list);
  /* Added ??? */
  int (*overload_function)(sqlitex*, const char *zFuncName, int nArg);
  /* Added by 3.3.13 */
  int (*prepare_v2)(sqlitex*,const char*,int,sqlitex_stmt**,const char**);
  int (*prepare16_v2)(sqlitex*,const void*,int,sqlitex_stmt**,const void**);
  int (*clear_bindings)(sqlitex_stmt*);
  /* Added by 3.4.1 */
  int (*create_module_v2)(sqlitex*,const char*,const sqlitex_module*,void*,
                          void (*xDestroy)(void *));
  /* Added by 3.5.0 */
  int (*bind_zeroblob)(sqlitex_stmt*,int,int);
  int (*blob_bytes)(sqlitex_blob*);
  int (*blob_close)(sqlitex_blob*);
  int (*blob_open)(sqlitex*,const char*,const char*,const char*,sqlitex_int64,
                   int,sqlitex_blob**);
  int (*blob_read)(sqlitex_blob*,void*,int,int);
  int (*blob_write)(sqlitex_blob*,const void*,int,int);
  int (*create_collation_v2)(sqlitex*,const char*,int,void*,
                             int(*)(void*,int,const void*,int,const void*),
                             void(*)(void*));
  int (*file_control)(sqlitex*,const char*,int,void*);
  sqlitex_int64 (*memory_highwater)(int);
  sqlitex_int64 (*memory_used)(void);
  sqlitex_mutex *(*mutex_alloc)(int);
  void (*mutex_enter)(sqlitex_mutex*);
  void (*mutex_free)(sqlitex_mutex*);
  void (*mutex_leave)(sqlitex_mutex*);
  int (*mutex_try)(sqlitex_mutex*);
  int (*open_v2)(const char*,sqlitex**,int,const char*);
  int (*release_memory)(int);
  void (*result_error_nomem)(sqlitex_context*);
  void (*result_error_toobig)(sqlitex_context*);
  int (*sleep)(int);
  void (*soft_heap_limit)(int);
  sqlitex_vfs *(*vfs_find)(const char*);
  int (*vfs_register)(sqlitex_vfs*,int);
  int (*vfs_unregister)(sqlitex_vfs*);
  int (*xthreadsafe)(void);
  void (*result_zeroblob)(sqlitex_context*,int);
  void (*result_error_code)(sqlitex_context*,int);
  int (*test_control)(int, ...);
  void (*randomness)(int,void*);
  sqlitex *(*context_db_handle)(sqlitex_context*);
  int (*extended_result_codes)(sqlitex*,int);
  int (*limit)(sqlitex*,int,int);
  sqlitex_stmt *(*next_stmt)(sqlitex*,sqlitex_stmt*);
  const char *(*sql)(sqlitex_stmt*);
  int (*status)(int,int*,int*,int);
  int (*backup_finish)(sqlitex_backup*);
  sqlitex_backup *(*backup_init)(sqlitex*,const char*,sqlitex*,const char*);
  int (*backup_pagecount)(sqlitex_backup*);
  int (*backup_remaining)(sqlitex_backup*);
  int (*backup_step)(sqlitex_backup*,int);
  const char *(*compileoption_get)(int);
  int (*compileoption_used)(const char*);
  int (*create_function_v2)(sqlitex*,const char*,int,int,void*,
                            void (*xFunc)(sqlitex_context*,int,sqlitex_value**),
                            void (*xStep)(sqlitex_context*,int,sqlitex_value**),
                            void (*xFinal)(sqlitex_context*),
                            void(*xDestroy)(void*));
  int (*db_config)(sqlitex*,int,...);
  sqlitex_mutex *(*db_mutex)(sqlitex*);
  int (*db_status)(sqlitex*,int,int*,int*,int);
  int (*extended_errcode)(sqlitex*);
  void (*log)(int,const char*,...);
  sqlitex_int64 (*soft_heap_limit64)(sqlitex_int64);
  const char *(*sourceid)(void);
  int (*stmt_status)(sqlitex_stmt*,int,int);
  int (*strnicmp)(const char*,const char*,int);
  int (*unlock_notify)(sqlitex*,void(*)(void**,int),void*);
  int (*wal_autocheckpoint)(sqlitex*,int);
  int (*wal_checkpoint)(sqlitex*,const char*);
  void *(*wal_hook)(sqlitex*,int(*)(void*,sqlitex*,const char*,int),void*);
  int (*blob_reopen)(sqlitex_blob*,sqlitex_int64);
  int (*vtab_config)(sqlitex*,int op,...);
  int (*vtab_on_conflict)(sqlitex*);
  /* Version 3.7.16 and later */
  int (*close_v2)(sqlitex*);
  const char *(*db_filename)(sqlitex*,const char*);
  int (*db_readonly)(sqlitex*,const char*);
  int (*db_release_memory)(sqlitex*);
  const char *(*errstr)(int);
  int (*stmt_busy)(sqlitex_stmt*);
  int (*stmt_readonly)(sqlitex_stmt*);
  int (*stricmp)(const char*,const char*);
  int (*uri_boolean)(const char*,const char*,int);
  sqlitex_int64 (*uri_int64)(const char*,const char*,sqlitex_int64);
  const char *(*uri_parameter)(const char*,const char*);
  char *(*vsnprintf)(int,char*,const char*,va_list);
  int (*wal_checkpoint_v2)(sqlitex*,const char*,int,int*,int*);
  /* Version 3.8.7 and later */
  int (*auto_extension)(void(*)(void));
  int (*bind_blob64)(sqlitex_stmt*,int,const void*,sqlitex_uint64,
                     void(*)(void*));
  int (*bind_text64)(sqlitex_stmt*,int,const char*,sqlitex_uint64,
                      void(*)(void*),unsigned char);
  int (*cancel_auto_extension)(void(*)(void));
  int (*load_extension)(sqlitex*,const char*,const char*,char**);
  void *(*malloc64)(sqlitex_uint64);
  sqlitex_uint64 (*msize)(void*);
  void *(*realloc64)(void*,sqlitex_uint64);
  void (*reset_auto_extension)(void);
  void (*result_blob64)(sqlitex_context*,const void*,sqlitex_uint64,
                        void(*)(void*));
  void (*result_text64)(sqlitex_context*,const char*,sqlitex_uint64,
                         void(*)(void*), unsigned char);
  int (*strglob)(const char*,const char*);
  /* Backported: Version 3.26.0 and later */
  const char *(*normalized_sql)(sqlitex_stmt*);
};

/*
** The following macros redefine the API routines so that they are
** redirected through the global sqlitex_api structure.
**
** This header file is also used by the loadext.c source file
** (part of the main SQLite library - not an extension) so that
** it can get access to the sqlitex_api_routines structure
** definition.  But the main library does not want to redefine
** the API.  So the redefinition macros are only valid if the
** SQLITE_CORE macros is undefined.
*/
#ifndef SQLITE_CORE
#define sqlitex_aggregate_context      sqlitex_api->aggregate_context
#ifndef SQLITE_OMIT_DEPRECATED
#define sqlitex_aggregate_count        sqlitex_api->aggregate_count
#endif
#define sqlitex_bind_blob              sqlitex_api->bind_blob
#define sqlitex_bind_double            sqlitex_api->bind_double
#define sqlitex_bind_int               sqlitex_api->bind_int
#define sqlitex_bind_int64             sqlitex_api->bind_int64
#define sqlitex_bind_null              sqlitex_api->bind_null
#define sqlitex_bind_parameter_count   sqlitex_api->bind_parameter_count
#define sqlitex_bind_parameter_index   sqlitex_api->bind_parameter_index
#define sqlitex_bind_parameter_name    sqlitex_api->bind_parameter_name
#define sqlitex_bind_text              sqlitex_api->bind_text
#define sqlitex_bind_text16            sqlitex_api->bind_text16
#define sqlitex_bind_value             sqlitex_api->bind_value
#define sqlitex_busy_handler           sqlitex_api->busy_handler
#define sqlitex_busy_timeout           sqlitex_api->busy_timeout
#define sqlitex_changes                sqlitex_api->changes
#define sqlitex_close                  sqlitex_api->close
#define sqlitex_collation_needed       sqlitex_api->collation_needed
#define sqlitex_collation_needed16     sqlitex_api->collation_needed16
#define sqlitex_column_blob            sqlitex_api->column_blob
#define sqlitex_column_bytes           sqlitex_api->column_bytes
#define sqlitex_column_bytes16         sqlitex_api->column_bytes16
#define sqlitex_column_count           sqlitex_api->column_count
#define sqlitex_column_database_name   sqlitex_api->column_database_name
#define sqlitex_column_database_name16 sqlitex_api->column_database_name16
#define sqlitex_column_decltype        sqlitex_api->column_decltype
#define sqlitex_column_decltype16      sqlitex_api->column_decltype16
#define sqlitex_column_double          sqlitex_api->column_double
#define sqlitex_column_int             sqlitex_api->column_int
#define sqlitex_column_int64           sqlitex_api->column_int64
#define sqlitex_column_name            sqlitex_api->column_name
#define sqlitex_column_name16          sqlitex_api->column_name16
#define sqlitex_column_origin_name     sqlitex_api->column_origin_name
#define sqlitex_column_origin_name16   sqlitex_api->column_origin_name16
#define sqlitex_column_table_name      sqlitex_api->column_table_name
#define sqlitex_column_table_name16    sqlitex_api->column_table_name16
#define sqlitex_column_text            sqlitex_api->column_text
#define sqlitex_column_text16          sqlitex_api->column_text16
#define sqlitex_column_type            sqlitex_api->column_type
#define sqlitex_column_value           sqlitex_api->column_value
#define sqlitex_commit_hook            sqlitex_api->commit_hook
#define sqlitex_complete               sqlitex_api->complete
#define sqlitex_complete16             sqlitex_api->complete16
#define sqlitex_create_collation       sqlitex_api->create_collation
#define sqlitex_create_collation16     sqlitex_api->create_collation16
#define sqlitex_create_function        sqlitex_api->create_function
#define sqlitex_create_function16      sqlitex_api->create_function16
#define sqlitex_create_module          sqlitex_api->create_module
#define sqlitex_create_module_v2       sqlitex_api->create_module_v2
#define sqlitex_data_count             sqlitex_api->data_count
#define sqlitex_db_handle              sqlitex_api->db_handle
#define sqlitex_declare_vtab           sqlitex_api->declare_vtab
#define sqlitex_enable_shared_cache    sqlitex_api->enable_shared_cache
#define sqlitex_errcode                sqlitex_api->errcode
#define sqlitex_errmsg                 sqlitex_api->errmsg
#define sqlitex_errmsg16               sqlitex_api->errmsg16
#define sqlitex_exec                   sqlitex_api->exec
#ifndef SQLITE_OMIT_DEPRECATED
#define sqlitex_expired                sqlitex_api->expired
#endif
#define sqlitex_finalize               sqlitex_api->finalize
#define sqlitex_free                   sqlitex_api->free
#define sqlitex_free_table             sqlitex_api->free_table
#define sqlitex_get_autocommit         sqlitex_api->get_autocommit
#define sqlitex_get_auxdata            sqlitex_api->get_auxdata
#define sqlitex_get_table              sqlitex_api->get_table
#ifndef SQLITE_OMIT_DEPRECATED
#define sqlitex_global_recover         sqlitex_api->global_recover
#endif
#define sqlitex_interrupt              sqlitex_api->interruptx
#define sqlitex_last_insert_rowid      sqlitex_api->last_insert_rowid
#define sqlitex_libversion             sqlitex_api->libversion
#define sqlitex_libversion_number      sqlitex_api->libversion_number
#define sqlitex_malloc                 sqlitex_api->malloc
#define sqlitex_mprintf                sqlitex_api->mprintf
#define sqlitex_open                   sqlitex_api->open
#define sqlitex_open16                 sqlitex_api->open16
#define sqlitex_prepare                sqlitex_api->prepare
#define sqlitex_prepare16              sqlitex_api->prepare16
#define sqlitex_prepare_v2             sqlitex_api->prepare_v2
#define sqlitex_prepare16_v2           sqlitex_api->prepare16_v2
#define sqlitex_profile                sqlitex_api->profile
#define sqlitex_progress_handler       sqlitex_api->progress_handler
#define sqlitex_realloc                sqlitex_api->realloc
#define sqlitex_reset                  sqlitex_api->reset
#define sqlitex_result_blob            sqlitex_api->result_blob
#define sqlitex_result_double          sqlitex_api->result_double
#define sqlitex_result_error           sqlitex_api->result_error
#define sqlitex_result_error16         sqlitex_api->result_error16
#define sqlitex_result_int             sqlitex_api->result_int
#define sqlitex_result_int64           sqlitex_api->result_int64
#define sqlitex_result_null            sqlitex_api->result_null
#define sqlitex_result_text            sqlitex_api->result_text
#define sqlitex_result_text16          sqlitex_api->result_text16
#define sqlitex_result_text16be        sqlitex_api->result_text16be
#define sqlitex_result_text16le        sqlitex_api->result_text16le
#define sqlitex_result_value           sqlitex_api->result_value
#define sqlitex_rollback_hook          sqlitex_api->rollback_hook
#define sqlitex_set_authorizer         sqlitex_api->set_authorizer
#define sqlitex_set_auxdata            sqlitex_api->set_auxdata
#define sqlitex_snprintf               sqlitex_api->snprintf
#define sqlitex_step                   sqlitex_api->step
#define sqlitex_table_column_metadata  sqlitex_api->table_column_metadata
#define sqlitex_thread_cleanup         sqlitex_api->thread_cleanup
#define sqlitex_total_changes          sqlitex_api->total_changes
#define sqlitex_trace                  sqlitex_api->trace
#ifndef SQLITE_OMIT_DEPRECATED
#define sqlitex_transfer_bindings      sqlitex_api->transfer_bindings
#endif
#define sqlitex_update_hook            sqlitex_api->update_hook
#define sqlitex_user_data              sqlitex_api->user_data
#define sqlitex_value_blob             sqlitex_api->value_blob
#define sqlitex_value_bytes            sqlitex_api->value_bytes
#define sqlitex_value_bytes16          sqlitex_api->value_bytes16
#define sqlitex_value_double           sqlitex_api->value_double
#define sqlitex_value_int              sqlitex_api->value_int
#define sqlitex_value_int64            sqlitex_api->value_int64
#define sqlitex_value_numeric_type     sqlitex_api->value_numeric_type
#define sqlitex_value_text             sqlitex_api->value_text
#define sqlitex_value_text16           sqlitex_api->value_text16
#define sqlitex_value_text16be         sqlitex_api->value_text16be
#define sqlitex_value_text16le         sqlitex_api->value_text16le
#define sqlitex_value_type             sqlitex_api->value_type
#define sqlitex_vmprintf               sqlitex_api->vmprintf
#define sqlitex_overload_function      sqlitex_api->overload_function
#define sqlitex_prepare_v2             sqlitex_api->prepare_v2
#define sqlitex_prepare16_v2           sqlitex_api->prepare16_v2
#define sqlitex_clear_bindings         sqlitex_api->clear_bindings
#define sqlitex_bind_zeroblob          sqlitex_api->bind_zeroblob
#define sqlitex_blob_bytes             sqlitex_api->blob_bytes
#define sqlitex_blob_close             sqlitex_api->blob_close
#define sqlitex_blob_open              sqlitex_api->blob_open
#define sqlitex_blob_read              sqlitex_api->blob_read
#define sqlitex_blob_write             sqlitex_api->blob_write
#define sqlitex_create_collation_v2    sqlitex_api->create_collation_v2
#define sqlitex_file_control           sqlitex_api->file_control
#define sqlitex_memory_highwater       sqlitex_api->memory_highwater
#define sqlitex_memory_used            sqlitex_api->memory_used
#define sqlitex_mutex_alloc            sqlitex_api->mutex_alloc
#define sqlitex_mutex_enter            sqlitex_api->mutex_enter
#define sqlitex_mutex_free             sqlitex_api->mutex_free
#define sqlitex_mutex_leave            sqlitex_api->mutex_leave
#define sqlitex_mutex_try              sqlitex_api->mutex_try
#define sqlitex_open_v2                sqlitex_api->open_v2
#define sqlitex_release_memory         sqlitex_api->release_memory
#define sqlitex_result_error_nomem     sqlitex_api->result_error_nomem
#define sqlitex_result_error_toobig    sqlitex_api->result_error_toobig
#define sqlitex_sleep                  sqlitex_api->sleep
#define sqlitex_soft_heap_limit        sqlitex_api->soft_heap_limit
#define sqlitex_vfs_find               sqlitex_api->vfs_find
#define sqlitex_vfs_register           sqlitex_api->vfs_register
#define sqlitex_vfs_unregister         sqlitex_api->vfs_unregister
#define sqlitex_threadsafe             sqlitex_api->xthreadsafe
#define sqlitex_result_zeroblob        sqlitex_api->result_zeroblob
#define sqlitex_result_error_code      sqlitex_api->result_error_code
#define sqlitex_test_control           sqlitex_api->test_control
#define sqlitex_randomness             sqlitex_api->randomness
#define sqlitex_context_db_handle      sqlitex_api->context_db_handle
#define sqlitex_extended_result_codes  sqlitex_api->extended_result_codes
#define sqlitex_limit                  sqlitex_api->limit
#define sqlitex_next_stmt              sqlitex_api->next_stmt
#define sqlitex_sql                    sqlitex_api->sql
#define sqlitex_status                 sqlitex_api->status
#define sqlitex_backup_finish          sqlitex_api->backup_finish
#define sqlitex_backup_init            sqlitex_api->backup_init
#define sqlitex_backup_pagecount       sqlitex_api->backup_pagecount
#define sqlitex_backup_remaining       sqlitex_api->backup_remaining
#define sqlitex_backup_step            sqlitex_api->backup_step
#define sqlitex_compileoption_get      sqlitex_api->compileoption_get
#define sqlitex_compileoption_used     sqlitex_api->compileoption_used
#define sqlitex_create_function_v2     sqlitex_api->create_function_v2
#define sqlitex_db_config              sqlitex_api->db_config
#define sqlitex_db_mutex               sqlitex_api->db_mutex
#define sqlitex_db_status              sqlitex_api->db_status
#define sqlitex_extended_errcode       sqlitex_api->extended_errcode
#define sqlitex_log                    sqlitex_api->log
#define sqlitex_soft_heap_limit64      sqlitex_api->soft_heap_limit64
#define sqlitex_sourceid               sqlitex_api->sourceid
#define sqlitex_stmt_status            sqlitex_api->stmt_status
#define sqlitex_strnicmp               sqlitex_api->strnicmp
#define sqlitex_unlock_notify          sqlitex_api->unlock_notify
#define sqlitex_wal_autocheckpoint     sqlitex_api->wal_autocheckpoint
#define sqlitex_wal_checkpoint         sqlitex_api->wal_checkpoint
#define sqlitex_wal_hook               sqlitex_api->wal_hook
#define sqlitex_blob_reopen            sqlitex_api->blob_reopen
#define sqlitex_vtab_config            sqlitex_api->vtab_config
#define sqlitex_vtab_on_conflict       sqlitex_api->vtab_on_conflict
/* Version 3.7.16 and later */
#define sqlitex_close_v2               sqlitex_api->close_v2
#define sqlitex_db_filename            sqlitex_api->db_filename
#define sqlitex_db_readonly            sqlitex_api->db_readonly
#define sqlitex_db_release_memory      sqlitex_api->db_release_memory
#define sqlitex_errstr                 sqlitex_api->errstr
#define sqlitex_stmt_busy              sqlitex_api->stmt_busy
#define sqlitex_stmt_readonly          sqlitex_api->stmt_readonly
#define sqlitex_stricmp                sqlitex_api->stricmp
#define sqlitex_uri_boolean            sqlitex_api->uri_boolean
#define sqlitex_uri_int64              sqlitex_api->uri_int64
#define sqlitex_uri_parameter          sqlitex_api->uri_parameter
#define sqlitex_uri_vsnprintf          sqlitex_api->vsnprintf
#define sqlitex_wal_checkpoint_v2      sqlitex_api->wal_checkpoint_v2
/* Version 3.8.7 and later */
#define sqlitex_auto_extension         sqlitex_api->auto_extension
#define sqlitex_bind_blob64            sqlitex_api->bind_blob64
#define sqlitex_bind_text64            sqlitex_api->bind_text64
#define sqlitex_cancel_auto_extension  sqlitex_api->cancel_auto_extension
#define sqlitex_load_extension         sqlitex_api->load_extension
#define sqlitex_malloc64               sqlitex_api->malloc64
#define sqlitex_msize                  sqlitex_api->msize
#define sqlitex_realloc64              sqlitex_api->realloc64
#define sqlitex_reset_auto_extension   sqlitex_api->reset_auto_extension
#define sqlitex_result_blob64          sqlitex_api->result_blob64
#define sqlitex_result_text64          sqlitex_api->result_text64
#define sqlitex_strglob                sqlitex_api->strglob
/* Backported: Version 3.26.0 and later */
#define sqlitex_normalized_sql         sqlitex_api->normalized_sql
#endif /* SQLITE_CORE */

#ifndef SQLITE_CORE
  /* This case when the file really is being compiled as a loadable 
  ** extension */
# define SQLITE_EXTENSION_INIT1     const sqlitex_api_routines *sqlitex_api=0;
# define SQLITE_EXTENSION_INIT2(v)  sqlitex_api=v;
# define SQLITE_EXTENSION_INIT3     \
    extern const sqlitex_api_routines *sqlitex_api;
#else
  /* This case when the file is being statically linked into the 
  ** application */
# define SQLITE_EXTENSION_INIT1     /*no-op*/
# define SQLITE_EXTENSION_INIT2(v)  (void)v; /* unused parameter */
# define SQLITE_EXTENSION_INIT3     /*no-op*/
#endif

#endif /* _SQLITE3EXT_H_ */
