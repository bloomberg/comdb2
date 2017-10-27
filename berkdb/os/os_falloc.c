/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1998-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: os_falloc.c,v 11.32 2014/09/32 12:27:03 bostic Exp $";
#endif /* not lint */


#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#ifdef __linux__
#include <sys/syscall.h>
#include <linux/falloc.h>
#endif
#endif /* NO_SYSTEM_INCLUDES */

#include "db_int.h"


/*
 * __os_fallocate --
 *	    Preallocate an open file, Linux style. Caller ensures fhp->fd is
 *      open. Returns 0 on success, __os_get_errno() on fail.
 *
 * PUBLIC: int __os_fallocate __P((DB_ENV *, off_t, off_t, DB_FH *));
 */
int
__os_fallocate(dbenv, offset, len, fhp)
	DB_ENV *dbenv;
	off_t offset, len;
	DB_FH *fhp;
{
	int ret = 0;

#ifdef __linux__
	/*
	 * We use FALLOC_FL_KEEP_SIZE because we want the same behavior
	 * for file size as before. Also we need to use syscall because
	 * this can't be compiled on systems without falloc headers.
	 */
	if (dbenv->attr.preallocate_on_writes &&
	    fhp != NULL &&
	    syscall(SYS_fallocate, fhp->fd, FALLOC_FL_KEEP_SIZE,
	    offset, len) == -1) {
		ret = __os_get_errno();
		__db_err(dbenv, "syscall(SYS_fallocate): %s", strerror(ret));
	}
#endif /* else do nothing */

	return (ret);
}
