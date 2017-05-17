/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1997-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: os_rename.c,v 11.15 2003/05/05 19:55:04 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <string.h>
#endif

#include "db_int.h"

/*
 * __os_rename --
 *	Rename a file.  If flags is non-zero, then errors are OK and we
 * should not output an error message.
 *
 * PUBLIC: int __os_rename __P((DB_ENV *,
 * PUBLIC:    const char *, const char *, u_int32_t));
 */
int
___os_rename(dbenv, old, new, flags)
	DB_ENV *dbenv;
	const char *old, *new;
	u_int32_t flags;
{
	int ret, retries;

	retries = 0;
	do {
		ret = DB_GLOBAL(j_rename) != NULL ?
		    DB_GLOBAL(j_rename)(old, new) : rename(old, new);
	} while (ret != 0 &&
	    ((ret = __os_get_errno()) == EINTR || ret == EBUSY) &&
	    ++retries < DB_RETRY);

	if (ret != 0 && flags == 0)
		__db_err(dbenv, "rename %s %s: %s", old, new, strerror(ret));
	return (ret);
}
