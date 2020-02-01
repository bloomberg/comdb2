/*
** 2004 May 22
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
** This file contains macros and a little bit of code that is common to
** all of the platform-specific files (os_*.c) and is #included into those
** files.
**
** This file should be #included by the os_*.c files only.  It is not a
** general purpose header file.
*/
#ifndef _OS_COMMON_H_
#define _OS_COMMON_H_

/*
** At least two bugs have slipped in because we changed the MEMORY_DEBUG
** macro to SQLITE_DEBUG and some older makefiles have not yet made the
** switch.  The following code should catch this problem at compile-time.
*/
#ifdef MEMORY_DEBUG
# error "The MEMORY_DEBUG macro is obsolete.  Use SQLITE_DEBUG instead."
#endif

#if defined(SQLITE_TEST) && defined(SQLITE_DEBUG)
# ifndef SQLITE_DEBUG_OS_TRACE
#   define SQLITE_DEBUG_OS_TRACE 0
# endif
  int sqlitexOSTrace = SQLITE_DEBUG_OS_TRACE;
# define OSTRACE(X)          if( sqlitexOSTrace ) sqlite3DebugPrintf X
#else
# define OSTRACE(X)
#endif

/*
** Macros for performance tracing.  Normally turned off.  Only works
** on i486 hardware.
*/
#ifdef SQLITE_PERFORMANCE_TRACE

/* 
** hwtime.h contains inline assembler code for implementing 
** high-performance timing routines.
*/
#include "hwtime.h"

static sqlite_uint64 g_start;
static sqlite_uint64 g_elapsed;
#define TIMER_START       g_start=sqlitexHwtime()
#define TIMER_END         g_elapsed=sqlitexHwtime()-g_start
#define TIMER_ELAPSED     g_elapsed
#else
#define TIMER_START
#define TIMER_END
#define TIMER_ELAPSED     ((sqlite_uint64)0)
#endif

/*
** If we compile with the SQLITE_TEST macro set, then the following block
** of code will give us the ability to simulate a disk I/O error.  This
** is used for testing the I/O recovery logic.
*/
#ifdef SQLITE_TEST
int sqlitex_io_error_hit = 0;            /* Total number of I/O Errors */
int sqlitex_io_error_hardhit = 0;        /* Number of non-benign errors */
int sqlitex_io_error_pending = 0;        /* Count down to first I/O error */
int sqlitex_io_error_persist = 0;        /* True if I/O errors persist */
int sqlitex_io_error_benign = 0;         /* True if errors are benign */
int sqlitex_diskfull_pending = 0;
int sqlitex_diskfull = 0;
#define SimulateIOErrorBenign(X) sqlitex_io_error_benign=(X)
#define SimulateIOError(CODE)  \
  if( (sqlitex_io_error_persist && sqlitex_io_error_hit) \
       || sqlitex_io_error_pending-- == 1 )  \
              { local_ioerr(); CODE; }
static void local_ioerr(){
  IOTRACE(("IOERR\n"));
  sqlitex_io_error_hit++;
  if( !sqlitex_io_error_benign ) sqlitex_io_error_hardhit++;
}
#define SimulateDiskfullError(CODE) \
   if( sqlitex_diskfull_pending ){ \
     if( sqlitex_diskfull_pending == 1 ){ \
       local_ioerr(); \
       sqlitex_diskfull = 1; \
       sqlitex_io_error_hit = 1; \
       CODE; \
     }else{ \
       sqlitex_diskfull_pending--; \
     } \
   }
#else
#define SimulateIOErrorBenign(X)
#define SimulateIOError(A)
#define SimulateDiskfullError(A)
#endif

/*
** When testing, keep a count of the number of open files.
*/
#ifdef SQLITE_TEST
int sqlitex_open_file_count = 0;
#define OpenCounter(X)  sqlitex_open_file_count+=(X)
#else
#define OpenCounter(X)
#endif

#endif /* !defined(_OS_COMMON_H_) */
