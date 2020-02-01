/*
** 2008 October 28
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains a no-op memory allocation drivers for use when
** SQLITE_ZERO_MALLOC is defined.  The allocation drivers implemented
** here always fail.  SQLite will not operate with these drivers.  These
** are merely placeholders.  Real drivers must be substituted using
** sqlitex_config() before SQLite will operate.
*/
#include "sqliteInt.h"

/*
** This version of the memory allocator is the default.  It is
** used when no other memory allocator is specified using compile-time
** macros.
*/
#ifdef SQLITE_ZERO_MALLOC

/*
** No-op versions of all memory allocation routines
*/
static void *sqlitexMemMalloc(int nByte){ return 0; }
static void sqlitexMemFree(void *pPrior){ return; }
static void *sqlitexMemRealloc(void *pPrior, int nByte){ return 0; }
static int sqlitexMemSize(void *pPrior){ return 0; }
static int sqlitexMemRoundup(int n){ return n; }
static int sqlitexMemInit(void *NotUsed){ return SQLITE_OK; }
static void sqlitexMemShutdown(void *NotUsed){ return; }

/*
** This routine is the only routine in this file with external linkage.
**
** Populate the low-level memory allocation function pointers in
** sqlitexGlobalConfig.m with pointers to the routines in this file.
*/
void sqlitexMemSetDefault(void){
  static const sqlitex_mem_methods defaultMethods = {
     sqlitexMemMalloc,
     sqlitexMemFree,
     sqlitexMemRealloc,
     sqlitexMemSize,
     sqlitexMemRoundup,
     sqlitexMemInit,
     sqlitexMemShutdown,
     0
  };
  sqlitex_config(SQLITE_CONFIG_MALLOC, &defaultMethods);
}

#endif /* SQLITE_ZERO_MALLOC */
