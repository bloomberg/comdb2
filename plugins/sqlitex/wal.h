/*
** 2010 February 1
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This header file defines the interface to the write-ahead logging 
** system. Refer to the comments below and the header comment attached to 
** the implementation of each function in log.c for further details.
*/

#ifndef _WAL_H_
#define _WAL_H_

#include "sqliteInt.h"

/* Additional values that can be added to the sync_flags argument of
** sqlitexWalFrames():
*/
#define WAL_SYNC_TRANSACTIONS  0x20   /* Sync at the end of each transaction */
#define SQLITE_SYNC_MASK       0x13   /* Mask off the SQLITE_SYNC_* values */

#ifdef SQLITE_OMIT_WAL
# define sqlitexWalOpen(x,y,z)                   0
# define sqlitexWalLimit(x,y)
# define sqlitexWalClose(w,x,y,z)                0
# define sqlitexWalBeginReadTransaction(y,z)     0
# define sqlitexWalEndReadTransaction(z)
# define sqlitexWalDbsize(y)                     0
# define sqlitexWalBeginWriteTransaction(y)      0
# define sqlitexWalEndWriteTransaction(x)        0
# define sqlitexWalUndo(x,y,z)                   0
# define sqlitexWalSavepoint(y,z)
# define sqlitexWalSavepointUndo(y,z)            0
# define sqlitexWalFrames(u,v,w,x,y,z)           0
# define sqlitexWalCheckpoint(r,s,t,u,v,w,x,y,z) 0
# define sqlitexWalCallback(z)                   0
# define sqlitexWalExclusiveMode(y,z)            0
# define sqlitexWalHeapMemory(z)                 0
# define sqlitexWalFramesize(z)                  0
# define sqlitexWalFindFrame(x,y,z)              0
#else

#define WAL_SAVEPOINT_NDATA 4

/* Connection to a write-ahead log (WAL) file. 
** There is one object of this type for each pager. 
*/
typedef struct Wal Wal;

/* Open and close a connection to a write-ahead log. */
int sqlitexWalOpen(sqlitex_vfs*, sqlitex_file*, const char *, int, i64, Wal**);
int sqlitexWalClose(Wal *pWal, int sync_flags, int, u8 *);

/* Set the limiting size of a WAL file. */
void sqlitexWalLimit(Wal*, i64);

/* Used by readers to open (lock) and close (unlock) a snapshot.  A 
** snapshot is like a read-transaction.  It is the state of the database
** at an instant in time.  sqlitexWalOpenSnapshot gets a read lock and
** preserves the current state even if the other threads or processes
** write to or checkpoint the WAL.  sqlitexWalCloseSnapshot() closes the
** transaction and releases the lock.
*/
int sqlitexWalBeginReadTransaction(Wal *pWal, int *);
void sqlitexWalEndReadTransaction(Wal *pWal);

/* Read a page from the write-ahead log, if it is present. */
int sqlitexWalFindFrame(Wal *, Pgno, u32 *);
int sqlitexWalReadFrame(Wal *, u32, int, u8 *);

/* If the WAL is not empty, return the size of the database. */
Pgno sqlitexWalDbsize(Wal *pWal);

/* Obtain or release the WRITER lock. */
int sqlitexWalBeginWriteTransaction(Wal *pWal);
int sqlitexWalEndWriteTransaction(Wal *pWal);

/* Undo any frames written (but not committed) to the log */
int sqlitexWalUndo(Wal *pWal, int (*xUndo)(void *, Pgno), void *pUndoCtx);

/* Return an integer that records the current (uncommitted) write
** position in the WAL */
void sqlitexWalSavepoint(Wal *pWal, u32 *aWalData);

/* Move the write position of the WAL back to iFrame.  Called in
** response to a ROLLBACK TO command. */
int sqlitexWalSavepointUndo(Wal *pWal, u32 *aWalData);

/* Write a frame or frames to the log. */
int sqlitexWalFrames(Wal *pWal, int, PgHdr *, Pgno, int, int);

/* Copy pages from the log to the database file */ 
int sqlitexWalCheckpoint(
  Wal *pWal,                      /* Write-ahead log connection */
  int eMode,                      /* One of PASSIVE, FULL and RESTART */
  int (*xBusy)(void*),            /* Function to call when busy */
  void *pBusyArg,                 /* Context argument for xBusyHandler */
  int sync_flags,                 /* Flags to sync db file with (or 0) */
  int nBuf,                       /* Size of buffer nBuf */
  u8 *zBuf,                       /* Temporary buffer to use */
  int *pnLog,                     /* OUT: Number of frames in WAL */
  int *pnCkpt                     /* OUT: Number of backfilled frames in WAL */
);

/* Return the value to pass to a sqlitex_wal_hook callback, the
** number of frames in the WAL at the point of the last commit since
** sqlitexWalCallback() was called.  If no commits have occurred since
** the last call, then return 0.
*/
int sqlitexWalCallback(Wal *pWal);

/* Tell the wal layer that an EXCLUSIVE lock has been obtained (or released)
** by the pager layer on the database file.
*/
int sqlitexWalExclusiveMode(Wal *pWal, int op);

/* Return true if the argument is non-NULL and the WAL module is using
** heap-memory for the wal-index. Otherwise, if the argument is NULL or the
** WAL module is using shared-memory, return false. 
*/
int sqlitexWalHeapMemory(Wal *pWal);

#ifdef SQLITE_ENABLE_ZIPVFS
/* If the WAL file is not empty, return the number of bytes of content
** stored in each frame (i.e. the db page-size when the WAL was created).
*/
int sqlitexWalFramesize(Wal *pWal);
#endif

#endif /* ifndef SQLITE_OMIT_WAL */
#endif /* _WAL_H_ */
