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
** This file contains code to implement a pseudo-random number
** generator (PRNG) for SQLite.
**
** Random numbers are used by some of the database backends in order
** to generate random integer keys for tables or random filenames.
*/
#include "sqliteInt.h"


/* All threads share a single random number generator.
** This structure is the current state of the generator.
*/
static SQLITE_WSD struct sqlitexPrngType {
  unsigned char isInit;          /* True if initialized */
  unsigned char i, j;            /* State variables */
  unsigned char s[256];          /* State variables */
} sqlitexPrng;

/*
** Return N random bytes.
*/
void sqlitex_randomness(int N, void *pBuf){
  unsigned char t;
  unsigned char *zBuf = pBuf;

  /* The "wsdPrng" macro will resolve to the pseudo-random number generator
  ** state vector.  If writable static data is unsupported on the target,
  ** we have to locate the state vector at run-time.  In the more common
  ** case where writable static data is supported, wsdPrng can refer directly
  ** to the "sqlitexPrng" state vector declared above.
  */
#ifdef SQLITE_OMIT_WSD
  struct sqlitexPrngType *p = &GLOBAL(struct sqlitexPrngType, sqlitexPrng);
# define wsdPrng p[0]
#else
# define wsdPrng sqlitexPrng
#endif

#if SQLITE_THREADSAFE
  sqlitex_mutex *mutex;
#endif

#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlitex_initialize() ) return;
#endif

#if SQLITE_THREADSAFE
  mutex = sqlitexMutexAlloc(SQLITE_MUTEX_STATIC_PRNG);
#endif

  sqlitex_mutex_enter(mutex);
  if( N<=0 || pBuf==0 ){
    wsdPrng.isInit = 0;
    sqlitex_mutex_leave(mutex);
    return;
  }

  /* Initialize the state of the random number generator once,
  ** the first time this routine is called.  The seed value does
  ** not need to contain a lot of randomness since we are not
  ** trying to do secure encryption or anything like that...
  **
  ** Nothing in this file or anywhere else in SQLite does any kind of
  ** encryption.  The RC4 algorithm is being used as a PRNG (pseudo-random
  ** number generator) not as an encryption device.
  */
  if( !wsdPrng.isInit ){
    int i;
    char k[256];
    wsdPrng.j = 0;
    wsdPrng.i = 0;
    sqlitexOsRandomness(sqlitex_vfs_find(0), 256, k);
    for(i=0; i<256; i++){
      wsdPrng.s[i] = (u8)i;
    }
    for(i=0; i<256; i++){
      wsdPrng.j += wsdPrng.s[i] + k[i];
      t = wsdPrng.s[wsdPrng.j];
      wsdPrng.s[wsdPrng.j] = wsdPrng.s[i];
      wsdPrng.s[i] = t;
    }
    wsdPrng.isInit = 1;
  }

  assert( N>0 );
  do{
    wsdPrng.i++;
    t = wsdPrng.s[wsdPrng.i];
    wsdPrng.j += t;
    wsdPrng.s[wsdPrng.i] = wsdPrng.s[wsdPrng.j];
    wsdPrng.s[wsdPrng.j] = t;
    t += wsdPrng.s[wsdPrng.i];
    *(zBuf++) = wsdPrng.s[t];
  }while( --N );
  sqlitex_mutex_leave(mutex);
}

#ifndef SQLITE_OMIT_BUILTIN_TEST
/*
** For testing purposes, we sometimes want to preserve the state of
** PRNG and restore the PRNG to its saved state at a later time, or
** to reset the PRNG to its initial state.  These routines accomplish
** those tasks.
**
** The sqlitex_test_control() interface calls these routines to
** control the PRNG.
*/
static SQLITE_WSD struct sqlitexPrngType sqlitexSavedPrng;
void sqlitexPrngSaveState(void){
  memcpy(
    &GLOBAL(struct sqlitexPrngType, sqlitexSavedPrng),
    &GLOBAL(struct sqlitexPrngType, sqlitexPrng),
    sizeof(sqlitexPrng)
  );
}
void sqlitexPrngRestoreState(void){
  memcpy(
    &GLOBAL(struct sqlitexPrngType, sqlitexPrng),
    &GLOBAL(struct sqlitexPrngType, sqlitexSavedPrng),
    sizeof(sqlitexPrng)
  );
}
#endif /* SQLITE_OMIT_BUILTIN_TEST */
