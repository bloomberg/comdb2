/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: db_byteorder.c,v 11.9 2003/01/08 04:07:38 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#endif

#include "db_int.h"
#include "dbinc/db_swap.h"

/*
 * __db_byteorder --
 *	Return if we need to do byte swapping, checking for illegal
 *	values.
 *
 * PUBLIC: int __db_byteorder __P((DB_ENV *, int));
 */
int
__db_byteorder(dbenv, lorder)
	DB_ENV *dbenv;
	int lorder;
{
	int is_bigendian = __db_isbigendian();
	switch (lorder) {
	case 0:
		break;
	case 1234:
		if (is_bigendian)
			return (DB_SWAPBYTES);
		break;
	case 4321:
		if (!is_bigendian)
			return (DB_SWAPBYTES);
		break;
	default:
		__db_err(dbenv,
	    "unsupported byte order, only big and little-endian supported");
		return (EINVAL);
	}
	return (0);
}
