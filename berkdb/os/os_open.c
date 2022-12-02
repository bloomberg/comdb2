/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1997-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: os_open.c,v 11.48 2003/09/10 00:27:29 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#ifdef HAVE_SYS_FCNTL_H
#include <sys/fcntl.h>
#endif

#include <fcntl.h>
#include <string.h>
#endif

#include "db_int.h"

#ifdef HAVE_QNX
static int __os_region_open __P((DB_ENV *, const char *, int, int, DB_FH **));
#endif

#if defined (UFID_HASH_DEBUG)
#include <logmsg.h>
#endif

/*
 * __os_have_direct --
 *	Check to see if we support direct I/O.
 *
 * PUBLIC: int __os_have_direct __P((void));
 */
int
__os_have_direct()
{
	int ret;

	ret = 0;

#ifdef HAVE_O_DIRECT
	ret = 1;
#endif
#if defined(HAVE_DIRECTIO) && defined(DIRECTIO_ON)
	ret = 1;
#endif
	return (ret);
}

/*
 * __os_open --
 *	Open a file.
 *
 * PUBLIC: int __os_open
 * PUBLIC:     __P((DB_ENV *, const char *, u_int32_t, int, DB_FH **));
 */
int
___os_open(dbenv, name, flags, mode, fhpp)
	DB_ENV *dbenv;
	const char *name;
	u_int32_t flags;
	int mode;
	DB_FH **fhpp;
{
	return (__os_open_extend(dbenv, name, 0, 0, flags, mode, fhpp));
}

int gbl_force_direct_io = 1;
int gbl_wal_osync = 0;
/*
 * __os_open_extend --
 *	Open a file descriptor (including page size and log size information).
 *
 * PUBLIC: int __os_open_extend __P((DB_ENV *,
 * PUBLIC:     const char *, u_int32_t, u_int32_t, u_int32_t, int, DB_FH **));
 */

int
___os_open_extend(dbenv, name, log_size, page_size, flags, mode, fhpp)
	DB_ENV *dbenv;
	const char *name;
	u_int32_t log_size, page_size, flags;
	int mode;
	DB_FH **fhpp;
{
	DB_FH *fhp;
	int oflags, ret;

	COMPQUIET(log_size, 0);
	COMPQUIET(page_size, 0);

	*fhpp = NULL;
	oflags = 0;

    if (gbl_force_direct_io && (F_ISSET(dbenv, DB_ENV_DIRECT_DB)) && !(LF_ISSET(DB_OSO_LOG)))
        LF_SET(DB_OSO_DIRECT);

#define	OKFLAGS								\
	(DB_OSO_CREATE | DB_OSO_DIRECT | DB_OSO_EXCL | DB_OSO_LOG |	\
	 DB_OSO_RDONLY | DB_OSO_REGION | DB_OSO_SEQ | DB_OSO_TEMP |	\
	 DB_OSO_TRUNC | DB_OSO_OSYNC)
	if ((ret = __db_fchk(dbenv, "__os_open", flags, OKFLAGS)) != 0)
		return (ret);

#if defined(O_BINARY)
	/*
	 * If there's a binary-mode open flag, set it, we never want any
	 * kind of translation.  Some systems do translations by default,
	 * e.g., with Cygwin, the default mode for an open() is set by the
	 * mode of the mount that underlies the file.
	 */
	oflags |= O_BINARY;
#endif

#if defined (UFID_HASH_DEBUG)
	if (!strstr(name, "logs/log")) {
		logmsg(LOGMSG_USER, "%s opening %s\n", __func__, name);
	}
#endif
	/*
	 * DB requires the POSIX 1003.1 semantic that two files opened at the
	 * same time with DB_OSO_CREATE/O_CREAT and DB_OSO_EXCL/O_EXCL flags
	 * set return an EEXIST failure in at least one.
	 */
	if (LF_ISSET(DB_OSO_CREATE)) {
#if defined (UFID_HASH_DEBUG)
		logmsg(LOGMSG_USER, "%s set create flag for %s\n", __func__, name);
#endif
		 oflags |= O_CREAT;
	}

	if (LF_ISSET(DB_OSO_EXCL))
		 oflags |= O_EXCL;


	/* they dont even give us any dbenv flag to turn this thing on in this
	 * release anyway. */
#if 0
	/* newer berkdbs allow dsync.  i think dsync works these days. */
#if defined(O_DSYNC)
	if (LF_ISSET(DB_OSO_LOG))
		oflags |= O_DSYNC;
#endif
#endif


	if (LF_ISSET(DB_OSO_RDONLY))
		oflags |= O_RDONLY;
	else
		oflags |= O_RDWR;

	if (LF_ISSET(DB_OSO_TRUNC)) {
#if defined (UFID_HASH_DEBUG)
		logmsg(LOGMSG_USER, "%s truncating %s\n", __func__, name);
#endif
		 oflags |= O_TRUNC;
	}


	/*
	 * make directio imply dsync, which will disable fsync.  also
	 * make directio imply "concurrent io" on aix (solaris is already
	 * concurrent).
	 */
#if defined(HAVE_O_DIRECT)
	if (LF_ISSET(DB_OSO_DIRECT) && !LF_ISSET(DB_OSO_LOG)) {
		oflags |= O_DSYNC;

		/* 
		 * disable O_CIO for the testsuite & tools as these
		 * will not work.  I've symlink'd os_open.c from the
		 * tools directory to force a recompile
		 */
#if defined(_AIX) && !defined (TESTSUITE)
		oflags |= O_CIO;
#elif defined(_LINUX_SOURCE)
		oflags |= (O_DIRECT | O_SYNC);
#else
		oflags |= O_DIRECT;
#endif
	}

	/* if they didn't request direct on this file, it's a log file, and the
	 * environment wants direct logs, request sync io, but not directio */
	if (F_ISSET(dbenv, DB_ENV_DIRECT_LOG) && LF_ISSET(DB_OSO_LOG) && gbl_wal_osync) {
		/* don't do O_DIRECT for logs */
#if defined(_AIX) && !defined (TESTSUITE)
		oflags |= O_DSYNC;
#elif defined(_LINUX_SOURCE)
		oflags |= O_SYNC;
#endif
	}
#endif

	if (LF_ISSET(DB_OSO_OSYNC))
		oflags |= O_SYNC;

#ifdef HAVE_QNX
	if (LF_ISSET(DB_OSO_REGION))
		return (__os_qnx_region_open(dbenv, name, oflags, mode, fhpp));
#endif

	/* Open the file. */
	if ((ret = __os_openhandle(dbenv, name, oflags, mode, &fhp)) != 0)
		return (ret);

#ifdef _LINUX_SOURCE
	if (LF_ISSET(DB_OSO_CREATE) && LF_ISSET(DB_OSO_LOG))
		(void)__os_fallocate(dbenv, 0, dbenv->lg_size, fhp);
#endif


#if defined(HAVE_DIRECTIO) && defined(DIRECTIO_ON)
	/*
	 * The Solaris C library includes directio, but you have to set special
	 * compile flags to #define DIRECTIO_ON.  Require both in order to call
	 * directio.
	 */
	if (LF_ISSET(DB_OSO_DIRECT)) {
#	    if defined(__APPLE__)
		fcntl(fhp->fd, F_SETFL, F_NOCACHE);
#	    else
		(void)directio(fhp->fd, DIRECTIO_ON);
#	    endif
	}
#endif

	/*
	 * Delete any temporary file.
	 *
	 * !!!
	 * There's a race here, where we've created a file and we crash before
	 * we can unlink it.  Temporary files aren't common in DB, regardless,
	 * it's not a security problem because the file is empty.  There's no
	 * reasonable way to avoid the race (playing signal games isn't worth
	 * the portability nightmare), so we just live with it.
	 */
	if (LF_ISSET(DB_OSO_TEMP)) {
#if defined(HAVE_UNLINK_WITH_OPEN_FAILURE) || defined(CONFIG_TEST)
		if ((ret = __os_strdup(dbenv, name, &fhp->name)) != 0) {
			(void)__os_closehandle(dbenv, fhp);
			(void)__os_unlink(dbenv, name);
			return (ret);
		}
		F_SET(fhp, DB_FH_UNLINK);
#else
		(void)__os_unlink(dbenv, name);
#endif
	}


	*fhpp = fhp;
	return (0);
}

#ifdef HAVE_QNX
/*
 * __os_qnx_region_open --
 *	Open a shared memory region file using POSIX shm_open.
 */
static int
__os_qnx_region_open(dbenv, name, oflags, mode, fhpp)
	DB_ENV *dbenv;
	const char *name;
	int oflags;
	int mode;
	DB_FH **fhpp;
{
	DB_FH *fhp;
	int ret;
	char *newname;

	if ((ret = __os_calloc(dbenv, 1, sizeof(DB_FH), fhpp)) != 0)
		return (ret);
	fhp = *fhpp;

	if ((ret = __os_shmname(dbenv, name, &newname)) != 0)
		goto err;

	/*
	 * Once we have created the object, we don't need the name
	 * anymore.  Other callers of this will convert themselves.
	 */
	fhp->fd = shm_open(newname, oflags, mode);
	__os_free(dbenv, newname);

	if (fhp->fd == -1) {
		ret = __os_get_errno();
		goto err;
	}

	F_SET(fhp, DB_FH_OPENED);

#ifdef HAVE_FCNTL_F_SETFD
	/* Deny file descriptor acces to any child process. */
	if (fcntl(fhp->fd, F_SETFD, 1) == -1) {
		ret = __os_get_errno();
		__db_err(dbenv, "fcntl(F_SETFD): %s", strerror(ret));
		goto err;
	}
#endif

err:	if (ret != 0) {
		(void)__os_closehandle(dbenv, fhp);
		*fhpp = NULL;
	}

	return (ret);
}

/*
 * __os_shmname --
 *	Translate a pathname into a shm_open memory object name.
 *
 * PUBLIC: #ifdef HAVE_QNX
 * PUBLIC: int __os_shmname __P((DB_ENV *, const char *, char **));
 * PUBLIC: #endif
 */
int
__os_shmname(dbenv, name, newnamep)
	DB_ENV *dbenv;
	const char *name;
	char **newnamep;
{
	int ret;
	size_t size;
	char *p, *q, *tmpname;

	*newnamep = NULL;

	/*
	 * POSIX states that the name for a shared memory object
	 * may begin with a slash '/' and support for subsequent
	 * slashes is implementation-dependent.  The one implementation
	 * we know of right now, QNX, forbids subsequent slashes.
	 * We don't want to be parsing pathnames for '.' and '..' in
	 * the middle.  In order to allow easy conversion, just take
	 * the last component as the shared memory name.  This limits
	 * the namespace a bit, but makes our job a lot easier.
	 *
	 * We should not be modifying user memory, so we use our own.
	 * Caller is responsible for freeing the memory we give them.
	 */
	if ((ret = __os_strdup(dbenv, name, &tmpname)) != 0)
		return (ret);
	/*
	 * Skip over filename component.
	 * We set that separator to '\0' so that we can do another
	 * __db_rpath.  However, we immediately set it then to ':'
	 * so that we end up with the tailing directory:filename.
	 * We require a home directory component.  Return an error
	 * if there isn't one.
	 */
	p = __db_rpath(tmpname);
	if (p == NULL)
		return (EINVAL);
	if (p != tmpname) {
		*p = '\0';
		q = p;
		p = __db_rpath(tmpname);
		*q = ':';
	}
	if (p != NULL) {
		/*
		 * If we have a path component, copy and return it.
		 */
		ret = __os_strdup(dbenv, p, newnamep);
		__os_free(dbenv, tmpname);
		return (ret);
	}

	/*
	 * We were given just a directory name with no path components.
	 * Add a leading slash, and copy the remainder.
	 */
	size = strlen(tmpname) + 2;
	if ((ret = __os_malloc(dbenv, size, &p)) != 0)
		return (ret);
	p[0] = '/';
	memcpy(&p[1], tmpname, size - 1);
	__os_free(dbenv, tmpname);
	*newnamep = p;
	return (0);
}
#endif
