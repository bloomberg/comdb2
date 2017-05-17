/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1997-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: raise.c,v 11.7 2003/01/08 04:06:41 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <signal.h>
#include <unistd.h>
#endif

/*
 * raise --
 *	Send a signal to the current process.
 *
 * PUBLIC: #ifndef HAVE_RAISE
 * PUBLIC: int raise __P((int));
 * PUBLIC: #endif
 */
int
raise(s)
	int s;
{
	/*
	 * Do not use __os_id(), as it may not return the process ID -- any
	 * system with kill(3) probably has getpid(3).
	 */
	return (kill(getpid(), s));
}
