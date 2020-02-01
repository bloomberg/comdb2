/*
** 2007 August 14
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains the C functions that implement mutexes.
**
** This file contains code that is common across all mutex implementations.
*/
#include "sqliteInt.h"

#if defined(SQLITE_DEBUG) && !defined(SQLITE_MUTEX_OMIT)
/*
** For debugging purposes, record when the mutex subsystem is initialized
** and uninitialized so that we can assert() if there is an attempt to
** allocate a mutex while the system is uninitialized.
*/
static SQLITE_WSD int mutexIsInit = 0;
#endif /* SQLITE_DEBUG */


#ifndef SQLITE_MUTEX_OMIT
/*
** Initialize the mutex system.
*/
int sqlitexMutexInit(void){ 
  int rc = SQLITE_OK;
  if( !sqlitexGlobalConfig.mutex.xMutexAlloc ){
    /* If the xMutexAlloc method has not been set, then the user did not
    ** install a mutex implementation via sqlitex_config() prior to 
    ** sqlitex_initialize() being called. This block copies pointers to
    ** the default implementation into the sqlitexGlobalConfig structure.
    */
    sqlitex_mutex_methods const *pFrom;
    sqlitex_mutex_methods *pTo = &sqlitexGlobalConfig.mutex;

    if( sqlitexGlobalConfig.bCoreMutex ){
      pFrom = sqlitexDefaultMutex();
    }else{
      pFrom = sqlitexNoopMutex();
    }
    memcpy(pTo, pFrom, offsetof(sqlitex_mutex_methods, xMutexAlloc));
    memcpy(&pTo->xMutexFree, &pFrom->xMutexFree,
           sizeof(*pTo) - offsetof(sqlitex_mutex_methods, xMutexFree));
    pTo->xMutexAlloc = pFrom->xMutexAlloc;
  }
  rc = sqlitexGlobalConfig.mutex.xMutexInit();

#ifdef SQLITE_DEBUG
  GLOBAL(int, mutexIsInit) = 1;
#endif

  return rc;
}

/*
** Shutdown the mutex system. This call frees resources allocated by
** sqlitexMutexInit().
*/
int sqlitexMutexEnd(void){
  int rc = SQLITE_OK;
  if( sqlitexGlobalConfig.mutex.xMutexEnd ){
    rc = sqlitexGlobalConfig.mutex.xMutexEnd();
  }

#ifdef SQLITE_DEBUG
  GLOBAL(int, mutexIsInit) = 0;
#endif

  return rc;
}

/*
** Retrieve a pointer to a static mutex or allocate a new dynamic one.
*/
sqlitex_mutex *sqlitex_mutex_alloc(int id){
#ifndef SQLITE_OMIT_AUTOINIT
  if( id<=SQLITE_MUTEX_RECURSIVE && sqlitex_initialize() ) return 0;
  if( id>SQLITE_MUTEX_RECURSIVE && sqlitexMutexInit() ) return 0;
#endif
  return sqlitexGlobalConfig.mutex.xMutexAlloc(id);
}

sqlitex_mutex *sqlitexMutexAlloc(int id){
  if( !sqlitexGlobalConfig.bCoreMutex ){
    return 0;
  }
  assert( GLOBAL(int, mutexIsInit) );
  return sqlitexGlobalConfig.mutex.xMutexAlloc(id);
}

/*
** Free a dynamic mutex.
*/
void sqlitex_mutex_free(sqlitex_mutex *p){
  if( p ){
    sqlitexGlobalConfig.mutex.xMutexFree(p);
  }
}

/*
** Obtain the mutex p. If some other thread already has the mutex, block
** until it can be obtained.
*/
void sqlitex_mutex_enter(sqlitex_mutex *p){
  if( p ){
    sqlitexGlobalConfig.mutex.xMutexEnter(p);
  }
}

/*
** Obtain the mutex p. If successful, return SQLITE_OK. Otherwise, if another
** thread holds the mutex and it cannot be obtained, return SQLITE_BUSY.
*/
int sqlitex_mutex_try(sqlitex_mutex *p){
  int rc = SQLITE_OK;
  if( p ){
    return sqlitexGlobalConfig.mutex.xMutexTry(p);
  }
  return rc;
}

/*
** The sqlitex_mutex_leave() routine exits a mutex that was previously
** entered by the same thread.  The behavior is undefined if the mutex 
** is not currently entered. If a NULL pointer is passed as an argument
** this function is a no-op.
*/
void sqlitex_mutex_leave(sqlitex_mutex *p){
  if( p ){
    sqlitexGlobalConfig.mutex.xMutexLeave(p);
  }
}

#ifndef NDEBUG
/*
** The sqlitex_mutex_held() and sqlitex_mutex_notheld() routine are
** intended for use inside assert() statements.
*/
int sqlitex_mutex_held(sqlitex_mutex *p){
  return p==0 || sqlitexGlobalConfig.mutex.xMutexHeld(p);
}
int sqlitex_mutex_notheld(sqlitex_mutex *p){
  return p==0 || sqlitexGlobalConfig.mutex.xMutexNotheld(p);
}
#endif

#endif /* !defined(SQLITE_MUTEX_OMIT) */
