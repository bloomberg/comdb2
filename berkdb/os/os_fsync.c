/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1997-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: os_fsync.c,v 11.18 2003/02/16 15:53:55 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <fcntl.h>			/* XXX: Required by __hp3000s900 */
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#endif

#include "db_int.h"
#include "logmsg.h"

#ifdef	HAVE_VXWORKS
#include "ioLib.h"

#define	fsync(fd)	__vx_fsync(fd);

int
__vx_fsync(fd)
	int fd;
{
	int ret;

	/*
	 * The results of ioctl are driver dependent.  Some will return the
	 * number of bytes sync'ed.  Only if it returns 'ERROR' should we
	 * flag it.
	 */
	if ((ret = ioctl(fd, FIOSYNC, 0)) != ERROR)
		return (0);
	return (ret);
}
#endif

#ifdef __hp3000s900
#define	fsync(fd)	__mpe_fsync(fd);

int
__mpe_fsync(fd)
	int fd;
{
	extern FCONTROL(short, short, void *);

	FCONTROL(_MPE_FILENO(fd), 2, NULL);	/* Flush the buffers */
	FCONTROL(_MPE_FILENO(fd), 6, NULL);	/* Write the EOF */
	return (0);
}
#endif

extern void (*__berkdb_trace_func)(const char *);
static int __berkdb_fsync_alarm_ms = 0;
static long long *__berkdb_num_fsyncs = 0;
static void (*fsync_callback)(int fd) = 0;

/* defined in os_rw.c */
uint64_t bb_berkdb_fasttime(void);

void
__berkdb_set_num_fsyncs(long long *n)
{
	__berkdb_num_fsyncs = n;
}

void
__berkdb_register_fsync_callback(void (*callback) (int fd))
{
	fsync_callback = callback;
}

void
berk_fsync_alarm_ms(int x)
{
	logmsg(LOGMSG_INFO, "__berkdb_fsync_alarm_ms = %d\n", x);
	__berkdb_fsync_alarm_ms = x;
}

/*
 * __os_fsync --
 *	Flush a file descriptor.
 *
 * PUBLIC: int __os_fsync __P((DB_ENV *, DB_FH *));
 */
int
__os_fsync(dbenv, fhp)
	DB_ENV *dbenv;
	DB_FH *fhp;
{
	int ret, retries, ckalmn;
	uint64_t x1 = 0, x2 = 0;

	ckalmn = __berkdb_fsync_alarm_ms;

	/*
	 * Do nothing if the file descriptor has been marked as not requiring
	 * any sync to disk.
	 */

	/* we enable O_DSYNC for directio files, so dont fsync them */
#if 0
	if (F_ISSET(fhp, DB_OSO_DIRECT))
		return (0);
#endif
	if (F_ISSET(fhp, DB_FH_NOSYNC))
		return (0);

	if (F_ISSET(fhp, DB_FH_TEMP))
		return (0);

#ifdef _LINUX_SOURCE
	if (F_ISSET(fhp, DB_FH_DIRECT))
		return (0);
	if (F_ISSET(fhp, DB_FH_SYNC))
		return (0);
#endif

	/* Check for illegal usage. */
	DB_ASSERT(F_ISSET(fhp, DB_FH_OPENED) && fhp->fd != -1);

	retries = 0;

	if (ckalmn)
		x1 = bb_berkdb_fasttime();

#if defined(_AIX)
#define	FDATASYNC	0x00400000

	/* fdatasync on aix */
	do {
		ret = fsync_range(fhp->fd, FDATASYNC, 0, 0);
	} while (ret != 0 &&
	    ((ret = __os_get_errno()) == EINTR || ret == EBUSY) &&
	    ++retries < DB_RETRY);

#elif defined (__sun) || defined(__linux__)

	/* use fdatasync on solaris and linux */

	do {
		ret = fdatasync(fhp->fd);
	} while (ret != 0 &&
	    ((ret = __os_get_errno()) == EINTR || ret == EBUSY) &&
	    ++retries < DB_RETRY);

#else

	do {
		ret = DB_GLOBAL(j_fsync) != NULL ?
		    DB_GLOBAL(j_fsync)(fhp->fd) : fsync(fhp->fd);
	} while (ret != 0 &&
	    ((ret = __os_get_errno()) == EINTR || ret == EBUSY) &&
	    ++retries < DB_RETRY);
#endif

	if (ckalmn)
		x2 = bb_berkdb_fasttime();


	if ((x2 - x1) > M2U(ckalmn) && __berkdb_trace_func) {
		char s[80];

		snprintf(s, sizeof(s), "LONG FSYNC FD=%d %d ms\n",
		    fhp->fd, U2M(x2 - x1));
		__berkdb_trace_func(s);
	}

	if (__berkdb_num_fsyncs)
		(*__berkdb_num_fsyncs)++;

	if (fsync_callback)
		fsync_callback(fhp->fd);


	if (ret != 0)
		__db_err(dbenv, "fsync %s", strerror(ret));

	return (ret);

}
