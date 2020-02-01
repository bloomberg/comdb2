/*
** 2007 August 22
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
** This file implements a special kind of sqlitex_file object used
** by SQLite to create journal files if the atomic-write optimization
** is enabled.
**
** The distinctive characteristic of this sqlitex_file is that the
** actual on disk file is created lazily. When the file is created,
** the caller specifies a buffer size for an in-memory buffer to
** be used to service read() and write() requests. The actual file
** on disk is not created or populated until either:
**
**   1) The in-memory representation grows too large for the allocated 
**      buffer, or
**   2) The sqlitexJournalCreate() function is called.
*/

/* COMDB2 MODIFICATION */
//static int i_am_not_an_empty_file;

#ifdef SQLITE_ENABLE_ATOMIC_WRITE
#include "sqliteInt.h"


/*
** A JournalFile object is a subclass of sqlitex_file used by
** as an open file handle for journal files.
*/
struct JournalFile {
  sqlitex_io_methods *pMethod;    /* I/O methods on journal files */
  int nBuf;                       /* Size of zBuf[] in bytes */
  char *zBuf;                     /* Space to buffer journal writes */
  int iSize;                      /* Amount of zBuf[] currently used */
  int flags;                      /* xOpen flags */
  sqlitex_vfs *pVfs;              /* The "real" underlying VFS */
  sqlitex_file *pReal;            /* The "real" underlying file descriptor */
  const char *zJournal;           /* Name of the journal file */
};
typedef struct JournalFile JournalFile;

/*
** If it does not already exists, create and populate the on-disk file 
** for JournalFile p.
*/
static int createFile(JournalFile *p){
  int rc = SQLITE_OK;
  if( !p->pReal ){
    sqlitex_file *pReal = (sqlitex_file *)&p[1];
    rc = sqlitexOsOpen(p->pVfs, p->zJournal, pReal, p->flags, 0);
    if( rc==SQLITE_OK ){
      p->pReal = pReal;
      if( p->iSize>0 ){
        assert(p->iSize<=p->nBuf);
        rc = sqlitexOsWrite(p->pReal, p->zBuf, p->iSize, 0);
      }
      if( rc!=SQLITE_OK ){
        /* If an error occurred while writing to the file, close it before
        ** returning. This way, SQLite uses the in-memory journal data to 
        ** roll back changes made to the internal page-cache before this
        ** function was called.  */
        sqlitexOsClose(pReal);
        p->pReal = 0;
      }
    }
  }
  return rc;
}

/*
** Close the file.
*/
static int jrnlClose(sqlitex_file *pJfd){
  JournalFile *p = (JournalFile *)pJfd;
  if( p->pReal ){
    sqlitexOsClose(p->pReal);
  }
  sqlitex_free(p->zBuf);
  return SQLITE_OK;
}

/*
** Read data from the file.
*/
static int jrnlRead(
  sqlitex_file *pJfd,    /* The journal file from which to read */
  void *zBuf,            /* Put the results here */
  int iAmt,              /* Number of bytes to read */
  sqlite_int64 iOfst     /* Begin reading at this offset */
){
  int rc = SQLITE_OK;
  JournalFile *p = (JournalFile *)pJfd;
  if( p->pReal ){
    rc = sqlitexOsRead(p->pReal, zBuf, iAmt, iOfst);
  }else if( (iAmt+iOfst)>p->iSize ){
    rc = SQLITE_IOERR_SHORT_READ;
  }else{
    memcpy(zBuf, &p->zBuf[iOfst], iAmt);
  }
  return rc;
}

/*
** Write data to the file.
*/
static int jrnlWrite(
  sqlitex_file *pJfd,    /* The journal file into which to write */
  const void *zBuf,      /* Take data to be written from here */
  int iAmt,              /* Number of bytes to write */
  sqlite_int64 iOfst     /* Begin writing at this offset into the file */
){
  int rc = SQLITE_OK;
  JournalFile *p = (JournalFile *)pJfd;
  if( !p->pReal && (iOfst+iAmt)>p->nBuf ){
    rc = createFile(p);
  }
  if( rc==SQLITE_OK ){
    if( p->pReal ){
      rc = sqlitexOsWrite(p->pReal, zBuf, iAmt, iOfst);
    }else{
      memcpy(&p->zBuf[iOfst], zBuf, iAmt);
      if( p->iSize<(iOfst+iAmt) ){
        p->iSize = (iOfst+iAmt);
      }
    }
  }
  return rc;
}

/*
** Truncate the file.
*/
static int jrnlTruncate(sqlitex_file *pJfd, sqlite_int64 size){
  int rc = SQLITE_OK;
  JournalFile *p = (JournalFile *)pJfd;
  if( p->pReal ){
    rc = sqlitexOsTruncate(p->pReal, size);
  }else if( size<p->iSize ){
    p->iSize = size;
  }
  return rc;
}

/*
** Sync the file.
*/
static int jrnlSync(sqlitex_file *pJfd, int flags){
  int rc;
  JournalFile *p = (JournalFile *)pJfd;
  if( p->pReal ){
    rc = sqlitexOsSync(p->pReal, flags);
  }else{
    rc = SQLITE_OK;
  }
  return rc;
}

/*
** Query the size of the file in bytes.
*/
static int jrnlFileSize(sqlitex_file *pJfd, sqlite_int64 *pSize){
  int rc = SQLITE_OK;
  JournalFile *p = (JournalFile *)pJfd;
  if( p->pReal ){
    rc = sqlitexOsFileSize(p->pReal, pSize);
  }else{
    *pSize = (sqlite_int64) p->iSize;
  }
  return rc;
}

/*
** Table of methods for JournalFile sqlitex_file object.
*/
static struct sqlitex_io_methods JournalFileMethods = {
  1,             /* iVersion */
  jrnlClose,     /* xClose */
  jrnlRead,      /* xRead */
  jrnlWrite,     /* xWrite */
  jrnlTruncate,  /* xTruncate */
  jrnlSync,      /* xSync */
  jrnlFileSize,  /* xFileSize */
  0,             /* xLock */
  0,             /* xUnlock */
  0,             /* xCheckReservedLock */
  0,             /* xFileControl */
  0,             /* xSectorSize */
  0,             /* xDeviceCharacteristics */
  0,             /* xShmMap */
  0,             /* xShmLock */
  0,             /* xShmBarrier */
  0              /* xShmUnmap */
};

/* 
** Open a journal file.
*/
int sqlitexJournalOpen(
  sqlitex_vfs *pVfs,         /* The VFS to use for actual file I/O */
  const char *zName,         /* Name of the journal file */
  sqlitex_file *pJfd,        /* Preallocated, blank file handle */
  int flags,                 /* Opening flags */
  int nBuf                   /* Bytes buffered before opening the file */
){
  JournalFile *p = (JournalFile *)pJfd;
  memset(p, 0, sqlitexJournalSize(pVfs));
  if( nBuf>0 ){
    p->zBuf = sqlitexMallocZero(nBuf);
    if( !p->zBuf ){
      return SQLITE_NOMEM;
    }
  }else{
    return sqlitexOsOpen(pVfs, zName, pJfd, flags, 0);
  }
  p->pMethod = &JournalFileMethods;
  p->nBuf = nBuf;
  p->flags = flags;
  p->zJournal = zName;
  p->pVfs = pVfs;
  return SQLITE_OK;
}

/*
** If the argument p points to a JournalFile structure, and the underlying
** file has not yet been created, create it now.
*/
int sqlitexJournalCreate(sqlitex_file *p){
  if( p->pMethods!=&JournalFileMethods ){
    return SQLITE_OK;
  }
  return createFile((JournalFile *)p);
}

/*
** The file-handle passed as the only argument is guaranteed to be an open
** file. It may or may not be of class JournalFile. If the file is a
** JournalFile, and the underlying file on disk has not yet been opened,
** return 0. Otherwise, return 1.
*/
int sqlitexJournalExists(sqlitex_file *p){
  return (p->pMethods!=&JournalFileMethods || ((JournalFile *)p)->pReal!=0);
}

/* 
** Return the number of bytes required to store a JournalFile that uses vfs
** pVfs to create the underlying on-disk files.
*/
int sqlitexJournalSize(sqlitex_vfs *pVfs){
  return (pVfs->szOsFile+sizeof(JournalFile));
}
#endif
