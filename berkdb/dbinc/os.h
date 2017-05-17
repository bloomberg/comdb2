/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1997-2003
 *	Sleepycat Software.  All rights reserved.
 *
 * $Id: os.h,v 11.18 2003/03/11 14:59:29 bostic Exp $
 */

#ifndef _DB_OS_H_
#define	_DB_OS_H_

#if defined(__cplusplus)
extern "C" {
#endif

/*
 * Flags understood by __os_open.
 */
#define	DB_OSO_CREATE	0x0001		/* POSIX: O_CREAT */
#define	DB_OSO_DIRECT	0x0002		/* Don't buffer the file in the OS. */
#define	DB_OSO_EXCL	0x0004		/* POSIX: O_EXCL */
#define	DB_OSO_LOG	0x0008		/* Opening a log file. */
#define	DB_OSO_RDONLY	0x0010		/* POSIX: O_RDONLY */
#define	DB_OSO_REGION	0x0020		/* Opening a region file. */
#define	DB_OSO_SEQ	0x0040		/* Expected sequential access. */
#define	DB_OSO_TEMP	0x0080		/* Remove after last close. */
#define	DB_OSO_TRUNC	0x0100		/* POSIX: O_TRUNC */
#define DB_OSO_OSYNC	0x0200		/* O_SYNC */

/*
 * Seek options understood by __os_seek.
 */
typedef enum {
	DB_OS_SEEK_CUR,			/* POSIX: SEEK_CUR */
	DB_OS_SEEK_END,			/* POSIX: SEEK_END */
	DB_OS_SEEK_SET			/* POSIX: SEEK_SET */
} DB_OS_SEEK;

/*
 * We group certain seek/write calls into a single function so that we
 * can use pread(2)/pwrite(2) where they're available.
 */
#define	DB_IO_READ	1
#define	DB_IO_WRITE	2

/* DB filehandle. */
struct __fh_t {
	/*
	 * The file-handle mutex is only used to protect the handle/fd
	 * across seek and read/write pairs, it does not protect the
	 * the reference count, or any other fields in the structure.
	 */
	DB_MUTEX  *mutexp;		/* Mutex to lock. */

	int	  ref;			/* Reference count. */

#if defined(DB_WIN32)
	HANDLE	  handle;		/* Windows/32 file handle. */
#endif
	int	  fd;			/* POSIX file descriptor. */

	char	*name;			/* File name (ref DB_FH_UNLINK) */

	/*
	 * Last seek statistics, used for zero-filling on filesystems
	 * that don't support it directly.
	 */
	u_int32_t pgno;
	u_int32_t pgsize;
	u_int32_t offset;

#define	DB_FH_NOSYNC	0x01		/* Handle doesn't need to be sync'd. */
#define	DB_FH_OPENED	0x02		/* Handle is valid. */
#define	DB_FH_UNLINK	0x04		/* Unlink on close */
#define	DB_FH_TEMP   	0x08		/* Unlink on close */
#define DB_FH_DIRECT    0x10		/* On linux use to do direct IO */
#define DB_FH_SYNC      0x20
	u_int8_t flags;
};

#if defined(__cplusplus)
}
#endif

#include "dbinc_auto/os_ext.h"
#endif /* !_DB_OS_H_ */
