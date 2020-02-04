/*
** 2008 October 07
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
** This implementation in this file does not provide any mutual
** exclusion and is thus suitable for use only in applications
** that use SQLite in a single thread.  The routines defined
** here are place-holders.  Applications can substitute working
** mutex routines at start-time using the
**
**     sqlitex_config(SQLITE_CONFIG_MUTEX,...)
**
** interface.
**
** If compiled with SQLITE_DEBUG, then additional logic is inserted
** that does error checking on mutexes to make sure they are being
** called correctly.
*/
#include "sqliteInt.h"

#ifndef SQLITE_MUTEX_OMIT

#ifndef SQLITE_DEBUG
/*
** Stub routines for all mutex methods.
**
** This routines provide no mutual exclusion or error checking.
*/
static int noopMutexInit(void){ return SQLITE_OK; }
static int noopMutexEnd(void){ return SQLITE_OK; }
static sqlitex_mutex *noopMutexAlloc(int id){ 
  UNUSED_PARAMETER(id);
  return (sqlitex_mutex*)8; 
}
static void noopMutexFree(sqlitex_mutex *p){ UNUSED_PARAMETER(p); return; }
static void noopMutexEnter(sqlitex_mutex *p){ UNUSED_PARAMETER(p); return; }
static int noopMutexTry(sqlitex_mutex *p){
  UNUSED_PARAMETER(p);
  return SQLITE_OK;
}
static void noopMutexLeave(sqlitex_mutex *p){ UNUSED_PARAMETER(p); return; }

sqlitex_mutex_methods const *sqlitexNoopMutex(void){
  static const sqlitex_mutex_methods sMutex = {
    noopMutexInit,
    noopMutexEnd,
    noopMutexAlloc,
    noopMutexFree,
    noopMutexEnter,
    noopMutexTry,
    noopMutexLeave,

    0,
    0,
  };

  return &sMutex;
}
#endif /* !SQLITE_DEBUG */

#ifdef SQLITE_DEBUG
/*
** In this implementation, error checking is provided for testing
** and debugging purposes.  The mutexes still do not provide any
** mutual exclusion.
*/

/*
** The mutex object
*/
typedef struct sqlitex_debug_mutex {
  int id;     /* The mutex type */
  int cnt;    /* Number of entries without a matching leave */
} sqlitex_debug_mutex;

/*
** The sqlitex_mutex_held() and sqlitex_mutex_notheld() routine are
** intended for use inside assert() statements.
*/
static int debugMutexHeld(sqlitex_mutex *pX){
  sqlitex_debug_mutex *p = (sqlitex_debug_mutex*)pX;
  return p==0 || p->cnt>0;
}
static int debugMutexNotheld(sqlitex_mutex *pX){
  sqlitex_debug_mutex *p = (sqlitex_debug_mutex*)pX;
  return p==0 || p->cnt==0;
}

/*
** Initialize and deinitialize the mutex subsystem.
*/
static int debugMutexInit(void){ return SQLITE_OK; }
static int debugMutexEnd(void){ return SQLITE_OK; }

/*
** The sqlitex_mutex_alloc() routine allocates a new
** mutex and returns a pointer to it.  If it returns NULL
** that means that a mutex could not be allocated. 
*/
static sqlitex_mutex *debugMutexAlloc(int id){
  static sqlitex_debug_mutex aStatic[SQLITE_MUTEX_STATIC_APP3 - 1];
  sqlitex_debug_mutex *pNew = 0;
  switch( id ){
    case SQLITE_MUTEX_FAST:
    case SQLITE_MUTEX_RECURSIVE: {
      pNew = sqlitexMalloc(sizeof(*pNew));
      if( pNew ){
        pNew->id = id;
        pNew->cnt = 0;
      }
      break;
    }
    default: {
#ifdef SQLITE_ENABLE_API_ARMOR
      if( id-2<0 || id-2>=ArraySize(aStatic) ){
        (void)SQLITE_MISUSE_BKPT;
        return 0;
      }
#endif
      pNew = &aStatic[id-2];
      pNew->id = id;
      break;
    }
  }
  return (sqlitex_mutex*)pNew;
}

/*
** This routine deallocates a previously allocated mutex.
*/
static void debugMutexFree(sqlitex_mutex *pX){
  sqlitex_debug_mutex *p = (sqlitex_debug_mutex*)pX;
  assert( p->cnt==0 );
  if( p->id==SQLITE_MUTEX_RECURSIVE || p->id==SQLITE_MUTEX_FAST ){
    sqlitex_free(p);
  }else{
#ifdef SQLITE_ENABLE_API_ARMOR
    (void)SQLITE_MISUSE_BKPT;
#endif
  }
}

/*
** The sqlitex_mutex_enter() and sqlitex_mutex_try() routines attempt
** to enter a mutex.  If another thread is already within the mutex,
** sqlitex_mutex_enter() will block and sqlitex_mutex_try() will return
** SQLITE_BUSY.  The sqlitex_mutex_try() interface returns SQLITE_OK
** upon successful entry.  Mutexes created using SQLITE_MUTEX_RECURSIVE can
** be entered multiple times by the same thread.  In such cases the,
** mutex must be exited an equal number of times before another thread
** can enter.  If the same thread tries to enter any other kind of mutex
** more than once, the behavior is undefined.
*/
static void debugMutexEnter(sqlitex_mutex *pX){
  sqlitex_debug_mutex *p = (sqlitex_debug_mutex*)pX;
  assert( p->id==SQLITE_MUTEX_RECURSIVE || debugMutexNotheld(pX) );
  p->cnt++;
}
static int debugMutexTry(sqlitex_mutex *pX){
  sqlitex_debug_mutex *p = (sqlitex_debug_mutex*)pX;
  assert( p->id==SQLITE_MUTEX_RECURSIVE || debugMutexNotheld(pX) );
  p->cnt++;
  return SQLITE_OK;
}

/*
** The sqlitex_mutex_leave() routine exits a mutex that was
** previously entered by the same thread.  The behavior
** is undefined if the mutex is not currently entered or
** is not currently allocated.  SQLite will never do either.
*/
static void debugMutexLeave(sqlitex_mutex *pX){
  sqlitex_debug_mutex *p = (sqlitex_debug_mutex*)pX;
  assert( debugMutexHeld(pX) );
  p->cnt--;
  assert( p->id==SQLITE_MUTEX_RECURSIVE || debugMutexNotheld(pX) );
}

sqlitex_mutex_methods const *sqlitexNoopMutex(void){
  static const sqlitex_mutex_methods sMutex = {
    debugMutexInit,
    debugMutexEnd,
    debugMutexAlloc,
    debugMutexFree,
    debugMutexEnter,
    debugMutexTry,
    debugMutexLeave,

    debugMutexHeld,
    debugMutexNotheld
  };

  return &sMutex;
}
#endif /* SQLITE_DEBUG */

/*
** If compiled with SQLITE_MUTEX_NOOP, then the no-op mutex implementation
** is used regardless of the run-time threadsafety setting.
*/
#ifdef SQLITE_MUTEX_NOOP
sqlitex_mutex_methods const *sqlitexDefaultMutex(void){
  return sqlitexNoopMutex();
}
#endif /* defined(SQLITE_MUTEX_NOOP) */
#endif /* !defined(SQLITE_MUTEX_OMIT) */
