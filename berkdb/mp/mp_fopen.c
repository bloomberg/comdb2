/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: mp_fopen.c,v 11.120 2003/11/07 18:45:15 ubell Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#ifdef HAVE_RPC
#include <rpc/rpc.h>
#endif
#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_shash.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "locks_wrap.h"

#ifdef HAVE_RPC
#include "dbinc_auto/db_server.h"
#include "dbinc_auto/rpc_client_ext.h"
#endif

static int __memp_fclose_pp __P((DB_MPOOLFILE *, u_int32_t));
static int __memp_fopen_pp __P((DB_MPOOLFILE *,
		const char *, u_int32_t, int, size_t));
static int __memp_get_clear_len __P((DB_MPOOLFILE *, u_int32_t *));
static int __memp_get_flags __P((DB_MPOOLFILE *, u_int32_t *));
static int __memp_get_lsn_offset __P((DB_MPOOLFILE *, int32_t *));
static int __memp_get_maxsize __P((DB_MPOOLFILE *, u_int32_t *, u_int32_t *));
static int __memp_set_maxsize __P((DB_MPOOLFILE *, u_int32_t, u_int32_t));
static int __memp_get_pgcookie __P((DB_MPOOLFILE *, DBT *));
static int __memp_get_priority __P((DB_MPOOLFILE *, DB_CACHE_PRIORITY *));
static int __memp_set_priority __P((DB_MPOOLFILE *, DB_CACHE_PRIORITY));

/*
 * __memp_fcreate_pp --
 *	DB_ENV->memp_fcreate pre/post processing.
 *
 * PUBLIC: int __memp_fcreate_pp __P((DB_ENV *, DB_MPOOLFILE **, u_int32_t));
 */
int
__memp_fcreate_pp(dbenv, retp, flags)
	DB_ENV *dbenv;
	DB_MPOOLFILE **retp;
	u_int32_t flags;
{
	int rep_check, ret;

	PANIC_CHECK(dbenv);

	/* Validate arguments. */
	if ((ret = __db_fchk(dbenv, "DB_ENV->memp_fcreate", flags, 0)) != 0)
		return (ret);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __memp_fcreate(dbenv, retp);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __memp_fcreate --
 *	DB_ENV->memp_fcreate.
 *
 * PUBLIC: int __memp_fcreate __P((DB_ENV *, DB_MPOOLFILE **));
 */
int
__memp_fcreate(dbenv, retp)
	DB_ENV *dbenv;
	DB_MPOOLFILE **retp;
{
	DB_MPOOLFILE *dbmfp;
	pthread_mutex_t *reclk;
	int ret, i;

	/* Allocate and initialize the per-process structure. */
	if ((ret = __os_calloc(dbenv, 1, sizeof(DB_MPOOLFILE), &dbmfp)) != 0)
		return (ret);

	dbmfp->ref = 1;
	dbmfp->lsn_offset = -1;
	dbmfp->dbenv = dbenv;
	dbmfp->mfp = INVALID_ROFF;

	/* Initialize the recovery page lock array. */
	if (dbenv->mp_recovery_pages > 0) {
		dbmfp->rec_idx = 1;

		if ((ret = __os_calloc(dbenv, dbenv->mp_recovery_pages + 1,
			    sizeof(pthread_mutex_t), &reclk)) != 0)
			return (ret);

		for (i = 0; i <= dbenv->mp_recovery_pages; i++) {
			Pthread_mutex_init(&reclk[i], NULL);
		}

		dbmfp->recp_lk_array = reclk;

		/* Initialize the index lock. */
		Pthread_mutex_init(&dbmfp->recp_idx_lk, NULL);
	}

#ifdef HAVE_RPC
	if (F_ISSET(dbenv, DB_ENV_RPCCLIENT)) {
		dbmfp->get_clear_len = __dbcl_memp_get_clear_len;
		dbmfp->set_clear_len = __dbcl_memp_set_clear_len;
		dbmfp->get_fileid = __dbcl_memp_get_fileid;
		dbmfp->set_fileid = __dbcl_memp_set_fileid;
		dbmfp->get_flags = __dbcl_memp_get_flags;
		dbmfp->set_flags = __dbcl_memp_set_flags;
		dbmfp->get_ftype = __dbcl_memp_get_ftype;
		dbmfp->set_ftype = __dbcl_memp_set_ftype;
		dbmfp->get_lsn_offset = __dbcl_memp_get_lsn_offset;
		dbmfp->set_lsn_offset = __dbcl_memp_set_lsn_offset;
		dbmfp->get_maxsize = __dbcl_memp_get_maxsize;
		dbmfp->set_maxsize = __dbcl_memp_set_maxsize;
		dbmfp->get_pgcookie = __dbcl_memp_get_pgcookie;
		dbmfp->set_pgcookie = __dbcl_memp_set_pgcookie;
		dbmfp->get_priority = __dbcl_memp_get_priority;
		dbmfp->set_priority = __dbcl_memp_set_priority;

		dbmfp->get = __dbcl_memp_fget;
		dbmfp->open = __dbcl_memp_fopen;
		dbmfp->put = __dbcl_memp_fput;
		dbmfp->set = __dbcl_memp_fset;
		dbmfp->sync = __dbcl_memp_fsync;
	} else
#endif
	{
		dbmfp->get_clear_len = __memp_get_clear_len;
		dbmfp->set_clear_len = __memp_set_clear_len;
		dbmfp->get_fileid = __memp_get_fileid;
		dbmfp->set_fileid = __memp_set_fileid;
		dbmfp->get_flags = __memp_get_flags;
		dbmfp->set_flags = __memp_set_flags;
		dbmfp->get_ftype = __memp_get_ftype;
		dbmfp->set_ftype = __memp_set_ftype;
		dbmfp->get_lsn_offset = __memp_get_lsn_offset;
		dbmfp->set_lsn_offset = __memp_set_lsn_offset;
		dbmfp->get_maxsize = __memp_get_maxsize;
		dbmfp->set_maxsize = __memp_set_maxsize;
		dbmfp->get_pgcookie = __memp_get_pgcookie;
		dbmfp->set_pgcookie = __memp_set_pgcookie;
		dbmfp->get_priority = __memp_get_priority;
		dbmfp->set_priority = __memp_set_priority;

		dbmfp->get = __memp_fget_pp;
		dbmfp->open = __memp_fopen_pp;
		dbmfp->put = __memp_fput_pp;
		dbmfp->set = __memp_fset_pp;
		dbmfp->sync = __memp_fsync_pp;
	}
	dbmfp->close = __memp_fclose_pp;

	*retp = dbmfp;
	return (0);
}

/*
 * __memp_get_clear_len --
 *	Get the clear length.
 */
static int
__memp_get_clear_len(dbmfp, clear_lenp)
	DB_MPOOLFILE *dbmfp;
	u_int32_t *clear_lenp;
{
	*clear_lenp = dbmfp->clear_len;
	return (0);
}

/*
 * __memp_set_clear_len --
 *	DB_MPOOLFILE->set_clear_len.
 *
 * PUBLIC: int __memp_set_clear_len __P((DB_MPOOLFILE *, u_int32_t));
 */
int
__memp_set_clear_len(dbmfp, clear_len)
	DB_MPOOLFILE *dbmfp;
	u_int32_t clear_len;
{
	MPF_ILLEGAL_AFTER_OPEN(dbmfp, "DB_MPOOLFILE->set_clear_len");

	dbmfp->clear_len = clear_len;
	return (0);
}

/*
 * __memp_get_fileid --
 *	DB_MPOOLFILE->get_fileid.
 *
 * PUBLIC: int __memp_get_fileid __P((DB_MPOOLFILE *, u_int8_t *));
 */
int
__memp_get_fileid(dbmfp, fileid)
	DB_MPOOLFILE *dbmfp;
	u_int8_t *fileid;
{
	if (!F_ISSET(dbmfp, MP_FILEID_SET)) {
		__db_err(dbmfp->dbenv, "get_fileid: file ID not set");
		return (EINVAL);
	}

	memcpy(fileid, dbmfp->fileid, DB_FILE_ID_LEN);
	return (0);
}

/*
 * __memp_set_fileid --
 *	DB_MPOOLFILE->set_fileid.
 *
 * PUBLIC: int __memp_set_fileid __P((DB_MPOOLFILE *, u_int8_t *));
 */
int
__memp_set_fileid(dbmfp, fileid)
	DB_MPOOLFILE *dbmfp;
	u_int8_t *fileid;
{
	MPF_ILLEGAL_AFTER_OPEN(dbmfp, "DB_MPOOLFILE->set_fileid");

	memcpy(dbmfp->fileid, fileid, DB_FILE_ID_LEN);
	F_SET(dbmfp, MP_FILEID_SET);

	return (0);
}

/*
 * __memp_get_flags --
 *	Get the DB_MPOOLFILE flags;
 */
static int
__memp_get_flags(dbmfp, flagsp)
	DB_MPOOLFILE *dbmfp;
	u_int32_t *flagsp;
{
	MPOOLFILE *mfp;

	mfp = dbmfp->mfp;

	*flagsp = 0;

	if (mfp == NULL)
		*flagsp = FLD_ISSET(dbmfp->config_flags,
		    DB_MPOOL_NOFILE | DB_MPOOL_UNLINK);
	else {
		if (mfp->no_backing_file)
			FLD_SET(*flagsp, DB_MPOOL_NOFILE);
		if (mfp->unlink_on_close)
			FLD_SET(*flagsp, DB_MPOOL_UNLINK);
	}
	return (0);
}

/*
 * __memp_set_flags --
 *	Set the DB_MPOOLFILE flags;
 *
 * PUBLIC: int __memp_set_flags __P((DB_MPOOLFILE *, u_int32_t, int));
 */
int
__memp_set_flags(dbmfp, flags, onoff)
	DB_MPOOLFILE *dbmfp;
	u_int32_t flags;
	int onoff;
{
	DB_ENV *dbenv;
	MPOOLFILE *mfp;
	int ret;

	dbenv = dbmfp->dbenv;
	mfp = dbmfp->mfp;

#define	OKFLAGS	(DB_MPOOL_NOFILE | DB_MPOOL_UNLINK)
	if ((ret =
	    __db_fchk(dbenv, "DB_MPOOLFILE->set_flags", flags, OKFLAGS)) != 0)
		return (ret);

	switch (flags) {
	case 0:
		break;
	case DB_MPOOL_NOFILE:
		if (mfp == NULL)
			if (onoff)
				FLD_SET(dbmfp->config_flags, DB_MPOOL_NOFILE);
			else
				FLD_CLR(dbmfp->config_flags, DB_MPOOL_NOFILE);
		else
			mfp->no_backing_file = onoff;
		break;
	case DB_MPOOL_UNLINK:
		if (mfp == NULL)
			if (onoff)
				FLD_SET(dbmfp->config_flags, DB_MPOOL_UNLINK);
			else
				FLD_CLR(dbmfp->config_flags, DB_MPOOL_UNLINK);
		else
			mfp->unlink_on_close = onoff;
		break;
	}
	return (0);
}

/*
 * __memp_get_ftype --
 *	Get the file type (as registered).
 *
 * PUBLIC: int __memp_get_ftype __P((DB_MPOOLFILE *, int *));
 */
int
__memp_get_ftype(dbmfp, ftypep)
	DB_MPOOLFILE *dbmfp;
	int *ftypep;
{
	*ftypep = dbmfp->ftype;
	return (0);
}

/*
 * __memp_set_ftype --
 *	DB_MPOOLFILE->set_ftype.
 *
 * PUBLIC: int __memp_set_ftype __P((DB_MPOOLFILE *, int));
 */
int
__memp_set_ftype(dbmfp, ftype)
	DB_MPOOLFILE *dbmfp;
	int ftype;
{
	MPF_ILLEGAL_AFTER_OPEN(dbmfp, "DB_MPOOLFILE->set_ftype");

	dbmfp->ftype = ftype;
	return (0);
}

/*
 * __memp_get_lsn_offset --
 *	Get the page's LSN offset.
 */
static int
__memp_get_lsn_offset(dbmfp, lsn_offsetp)
	DB_MPOOLFILE *dbmfp;
	int32_t *lsn_offsetp;
{
	*lsn_offsetp = dbmfp->lsn_offset;
	return (0);
}

/*
 * __memp_set_lsn_offset --
 *	Set the page's LSN offset.
 *
 * PUBLIC: int __memp_set_lsn_offset __P((DB_MPOOLFILE *, int32_t));
 */
int
__memp_set_lsn_offset(dbmfp, lsn_offset)
	DB_MPOOLFILE *dbmfp;
	int32_t lsn_offset;
{
	MPF_ILLEGAL_AFTER_OPEN(dbmfp, "DB_MPOOLFILE->set_lsn_offset");

	dbmfp->lsn_offset = lsn_offset;
	return (0);
}

/*
 * __memp_get_maxsize --
 *	Get the file's maximum size.
 */
static int
__memp_get_maxsize(dbmfp, gbytesp, bytesp)
	DB_MPOOLFILE *dbmfp;
	u_int32_t *gbytesp, *bytesp;
{
	DB_ENV *dbenv;
	DB_MPOOL *dbmp;
	MPOOLFILE *mfp;

	if ((mfp = dbmfp->mfp) == NULL) {
		*gbytesp = dbmfp->gbytes;
		*bytesp = dbmfp->bytes;
	} else {
		dbenv = dbmfp->dbenv;
		dbmp = dbenv->mp_handle;

		R_LOCK(dbenv, dbmp->reginfo);
		*gbytesp = mfp->maxpgno / (GIGABYTE / mfp->stat.st_pagesize);
		*bytesp = (mfp->maxpgno %
		    (GIGABYTE / mfp->stat.st_pagesize)) * mfp->stat.st_pagesize;
		R_UNLOCK(dbenv, dbmp->reginfo);
	}

	return (0);
}

/*
 * __memp_set_maxsize --
 *	Set the files's maximum size.
 */
static int
__memp_set_maxsize(dbmfp, gbytes, bytes)
	DB_MPOOLFILE *dbmfp;
	u_int32_t gbytes, bytes;
{
	DB_ENV *dbenv;
	DB_MPOOL *dbmp;
	MPOOLFILE *mfp;

	if ((mfp = dbmfp->mfp) == NULL) {
		dbmfp->gbytes = gbytes;
		dbmfp->bytes = bytes;
	} else {
		dbenv = dbmfp->dbenv;
		dbmp = dbenv->mp_handle;

		R_LOCK(dbenv, dbmp->reginfo);
		mfp->maxpgno = gbytes * (GIGABYTE / mfp->stat.st_pagesize);
		mfp->maxpgno += (bytes +
		    mfp->stat.st_pagesize - 1) / mfp->stat.st_pagesize;
		R_UNLOCK(dbenv, dbmp->reginfo);
	}

	return (0);
}

/*
 * __memp_get_pgcookie --
 *	Get the pgin/pgout cookie.
 */
static int
__memp_get_pgcookie(dbmfp, pgcookie)
	DB_MPOOLFILE *dbmfp;
	DBT *pgcookie;
{
	if (dbmfp->pgcookie == NULL) {
		pgcookie->size = 0;
		pgcookie->data = "";
	} else
		memcpy(pgcookie, dbmfp->pgcookie, sizeof(DBT));
	return (0);
}

/*
 * __memp_set_pgcookie --
 *	Set the pgin/pgout cookie.
 *
 * PUBLIC: int __memp_set_pgcookie __P((DB_MPOOLFILE *, DBT *));
 */
int
__memp_set_pgcookie(dbmfp, pgcookie)
	DB_MPOOLFILE *dbmfp;
	DBT *pgcookie;
{
	DB_ENV *dbenv;
	DBT *cookie;
	int ret;

	MPF_ILLEGAL_AFTER_OPEN(dbmfp, "DB_MPOOLFILE->set_pgcookie");
	dbenv = dbmfp->dbenv;

	if ((ret = __os_calloc(dbenv, 1, sizeof(*cookie), &cookie)) != 0)
		return (ret);
	if ((ret = __os_malloc(dbenv, pgcookie->size, &cookie->data)) != 0) {
		(void)__os_free(dbenv, cookie);
		return (ret);
	}

	memcpy(cookie->data, pgcookie->data, pgcookie->size);
	cookie->size = pgcookie->size;

	dbmfp->pgcookie = cookie;
	return (0);
}

/*
 * __memp_get_priority --
 *	Set the cache priority for pages from this file.
 */
static int
__memp_get_priority(dbmfp, priorityp)
	DB_MPOOLFILE *dbmfp;
	DB_CACHE_PRIORITY *priorityp;
{
	switch (dbmfp->priority) {
	case MPOOL_PRI_VERY_LOW:
		*priorityp = DB_PRIORITY_VERY_LOW;
		break;
	case MPOOL_PRI_LOW:
		*priorityp = DB_PRIORITY_LOW;
		break;
	case MPOOL_PRI_DEFAULT:
		*priorityp = DB_PRIORITY_DEFAULT;
		break;
	case MPOOL_PRI_HIGH:
		*priorityp = DB_PRIORITY_HIGH;
		break;
	case MPOOL_PRI_VERY_HIGH:
		*priorityp = DB_PRIORITY_VERY_HIGH;
		break;
	case MPOOL_PRI_INDEX:
		*priorityp = DB_PRIORITY_INDEX;
		break;
	default:
		__db_err(dbmfp->dbenv,
		    "DB_MPOOLFILE->get_priority: unknown priority value: %d",
		    dbmfp->priority);
		return (EINVAL);
	}

	return (0);
}

/*
 * __memp_set_priority --
 *	Set the cache priority for pages from this file.
 */
static int
__memp_set_priority(dbmfp, priority)
	DB_MPOOLFILE *dbmfp;
	DB_CACHE_PRIORITY priority;
{
	switch (priority) {
	case DB_PRIORITY_VERY_LOW:
		dbmfp->priority = MPOOL_PRI_VERY_LOW;
		break;
	case DB_PRIORITY_LOW:
		dbmfp->priority = MPOOL_PRI_LOW;
		break;
	case DB_PRIORITY_DEFAULT:
		dbmfp->priority = MPOOL_PRI_DEFAULT;
		break;
	case DB_PRIORITY_HIGH:
		dbmfp->priority = MPOOL_PRI_HIGH;
		break;
	case DB_PRIORITY_VERY_HIGH:
		dbmfp->priority = MPOOL_PRI_VERY_HIGH;
		break;
	case DB_PRIORITY_INDEX:
		dbmfp->priority = MPOOL_PRI_INDEX;
		break;
	default:
		__db_err(dbmfp->dbenv,
		    "DB_MPOOLFILE->set_priority: unknown priority value: %d",
		    priority);
		return (EINVAL);
	}

	/* Update the underlying file if we've already opened it. */
	if (dbmfp->mfp != NULL)
		dbmfp->mfp->priority = priority;

	return (0);
}

/*
 * __memp_fopen_pp --
 *	DB_MPOOLFILE->open pre/post processing.
 */
static int
__memp_fopen_pp(dbmfp, path, flags, mode, pagesize)
	DB_MPOOLFILE *dbmfp;
	const char *path;
	u_int32_t flags;
	int mode;
	size_t pagesize;
{
	DB_ENV *dbenv;
	int rep_check, ret;

	dbenv = dbmfp->dbenv;

	PANIC_CHECK(dbenv);

	/* Validate arguments. */
	if ((ret = __db_fchk(dbenv, "DB_MPOOLFILE->open", flags,
	    DB_CREATE | DB_DIRECT | DB_EXTENT |
	    DB_NOMMAP | DB_ODDFILESIZE | DB_RDONLY | DB_TRUNCATE)) != 0)
		return (ret);

	/*
	 * Require a non-zero, power-of-two pagesize, smaller than the
	 * clear length.
	 */
	if (pagesize == 0 || !POWER_OF_TWO(pagesize)) {
		__db_err(dbenv,
		    "DB_MPOOLFILE->open: page sizes must be a power-of-2");
		return (EINVAL);
	}
	if (dbmfp->clear_len > pagesize) {
		__db_err(dbenv,
		    "DB_MPOOLFILE->open: clear length larger than page size");
		return (EINVAL);
	}

	/* Read-only checks, and local flag. */
	if (LF_ISSET(DB_RDONLY) && path == NULL) {
		__db_err(dbenv,
		    "DB_MPOOLFILE->open: temporary files can't be readonly");
		return (EINVAL);
	}

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	ret = __memp_fopen(dbmfp, NULL, path, flags, mode, pagesize);
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __memp_fopen --
 *	DB_MPOOLFILE->open.
 *
 * PUBLIC: int __memp_fopen __P((DB_MPOOLFILE *,
 * PUBLIC:     MPOOLFILE *, const char *, u_int32_t, int, size_t));
 */
int
__memp_fopen(dbmfp, mfp, path, flags, mode, pagesize)
	DB_MPOOLFILE *dbmfp;
	MPOOLFILE *mfp;
	const char *path;
	u_int32_t flags;
	int mode;
	size_t pagesize;
{
	DB_ENV *dbenv;
	DB_MPOOL *dbmp;
	DB_MPOOLFILE *tmp_dbmfp;
	MPOOL *mp;
	db_pgno_t last_pgno;
	size_t maxmap;
	u_int32_t mbytes, bytes, oflags;
	int refinc, ret, i;
	char *rpath, *recp_path, *recp_ext;
	void *p;

	dbenv = dbmfp->dbenv;
	dbmp = dbenv->mp_handle;
	mp = dbmp->reginfo[0].primary;
	refinc = ret = 0;
	rpath = recp_path = NULL;
	recp_ext = DB_RECV_EXTENSION;
	/*
	 * If it's a temporary file, delay the open until we actually need
	 * to write the file, and we know we can't join any existing files.
	 */
	if (path == NULL)
		goto alloc;

	/*
	 * If our caller knows what mfp we're using, increment the ref count,
	 * no need to search.
	 *
	 * We don't need to acquire a lock other than the mfp itself, because
	 * we know there's another reference and it's not going away.
	 */
	if (mfp != NULL) {
		MUTEX_LOCK(dbenv, &mfp->mutex);
		++mfp->mpf_cnt;
		refinc = 1;
		MUTEX_UNLOCK(dbenv, &mfp->mutex);
	}

	/*
	 * Get the real name for this file and open it.  If it's a Queue extent
	 * file, it may not exist, and that's OK.
	 */
	oflags = 0;
	if (LF_ISSET(DB_CREATE))
		oflags |= DB_OSO_CREATE;
	if (LF_ISSET(DB_DIRECT))
		oflags |= DB_OSO_DIRECT;
	if (LF_ISSET(DB_RDONLY)) {
		F_SET(dbmfp, MP_READONLY);
		oflags |= DB_OSO_RDONLY;
	}
	if (LF_ISSET(DB_OSYNC))
		oflags |= DB_OSO_OSYNC;
	if ((ret =
		__db_appname(dbenv, DB_APP_DATA, path, 0, NULL, &rpath)) != 0)
		 goto err;

	/*
	 * Supply a page size so os_open can decide whether to turn buffering
	 * off if the DB_DIRECT_DB flag is set.
	 */
	if ((ret = __os_open_extend(dbenv, rpath,
		    0, (u_int32_t)pagesize, oflags, mode, &dbmfp->fhp)) != 0) {
		if (!LF_ISSET(DB_EXTENT))
			 __db_err(dbenv, "%s: %s", rpath, db_strerror(ret));

		goto err;
	}

	/*
	 * Open the recovery-page file handle if the user has enabled this
	 * option.
	 */
	if (dbenv->mp_recovery_pages) {
		if ((ret = __os_calloc(dbenv, 1,
			    strlen(rpath) + strlen(recp_ext) + 1,
			    &recp_path)) != 0) {
			__db_err(dbenv, "Error allocating %s file: %s",
			    recp_ext, db_strerror(ret));
			goto err;
		}

		/* Recovery-page filename. */
		sprintf(recp_path, "%s%s", rpath, recp_ext);

		/* Open the recovery page file.  */
		if ((ret = __os_open_extend(dbenv, recp_path,
			    0, (u_int32_t)pagesize,
			    oflags | DB_OSO_CREATE |DB_OSO_OSYNC,
			    mode, &dbmfp->recp)) != 0) {
			__db_err(dbenv, "%s: %s", recp_path, db_strerror(ret));
			goto err;
		}

		/* Initialize recp->mutexp. */
		if (F_ISSET(dbenv, DB_ENV_THREAD) &&
		    (ret = __db_mutex_setup(dbenv, dbmp->reginfo,
			    &dbmfp->recp->mutexp,
			    MUTEX_ALLOC |MUTEX_THREAD)) != 0)
			 goto err;
	}
	/*
	 * Cache file handles are shared, and have mutexes to protect the
	 * underlying file handle across seek and read/write calls.
	 */
	dbmfp->fhp->ref = 1;
	if (F_ISSET(dbenv, DB_ENV_THREAD) &&
	    (ret = __db_mutex_setup(dbenv, dbmp->reginfo,
	    &dbmfp->fhp->mutexp, MUTEX_ALLOC | MUTEX_THREAD)) != 0)
		goto err;

	/*
	 * Figure out the file's size.
	 *
	 * !!!
	 * We can't use off_t's here, or in any code in the mainline library
	 * for that matter.  (We have to use them in the os stubs, of course,
	 * as there are system calls that take them as arguments.)  The reason
	 * is some customers build in environments where an off_t is 32-bits,
	 * but still run where offsets are 64-bits, and they pay us a lot of
	 * money.
	 */
	if ((ret = __os_ioinfo(
	    dbenv, rpath, dbmfp->fhp, &mbytes, &bytes, NULL)) != 0) {
		__db_err(dbenv, "%s: %s", rpath, db_strerror(ret));
		goto err;
	}

	/*
	 * Get the file id if we weren't given one.  Generated file id's
	 * don't use timestamps, otherwise there'd be no chance of any
	 * other process joining the party.
	 */
	if (!F_ISSET(dbmfp, MP_FILEID_SET) &&
	    (ret = __os_fileid(dbenv, rpath, 0, dbmfp->fileid)) != 0)
		goto err;

	if (mfp != NULL) 
		goto check_map;

	/*
	 * If not creating a temporary file, walk the list of MPOOLFILE's,
	 * looking for a matching file.  Files backed by temporary files
	 * or previously removed files can't match.
	 *
	 * DB_TRUNCATE support.
	 *
	 * The fileID is a filesystem unique number (e.g., a UNIX dev/inode
	 * pair) plus a timestamp.  If files are removed and created in less
	 * than a second, the fileID can be repeated.  The problem with
	 * repetition happens when the file that previously had the fileID
	 * value still has pages in the pool, since we don't want to use them
	 * to satisfy requests for the new file.
	 *
	 * Because the DB_TRUNCATE flag reuses the dev/inode pair, repeated
	 * opens with that flag set guarantees matching fileIDs when the
	 * machine can open a file and then re-open with truncate within a
	 * second.  For this reason, we pass that flag down, and, if we find
	 * a matching entry, we ensure that it's never found again, and we
	 * create a new entry for the current request.
	 */
	R_LOCK(dbenv, dbmp->reginfo);
	for (mfp = SH_TAILQ_FIRST(&mp->mpfq, __mpoolfile);
	    mfp != NULL; mfp = SH_TAILQ_NEXT(mfp, q, __mpoolfile)) {
		/* Skip dead files and temporary files. */
		if (mfp->deadfile || F_ISSET(mfp, MP_TEMP))
			continue;

		/* Skip non-matching files. */
		if (memcmp(dbmfp->fileid, R_ADDR(dbmp->reginfo,
		    mfp->fileid_off), DB_FILE_ID_LEN) != 0)
			continue;

		/*
		 * If the file is being truncated, remove it from the system
		 * and create a new entry.
		 *
		 * !!!
		 * We should be able to set mfp to NULL and break out of the
		 * loop, but I like the idea of checking all the entries.
		 */
		if (LF_ISSET(DB_TRUNCATE)) {
			MUTEX_LOCK(dbenv, &mfp->mutex);
			mfp->deadfile = 1;
			MUTEX_UNLOCK(dbenv, &mfp->mutex);
			continue;
		}

		/*
		 * Some things about a file cannot be changed: the clear length,
		 * page size, or lSN location.
		 *
		 * The file type can change if the application's pre- and post-
		 * processing needs change.  For example, an application that
		 * created a hash subdatabase in a database that was previously
		 * all btree.
		 *
		 * !!!
		 * We do not check to see if the pgcookie information changed,
		 * or update it if it is.
		 */
		if (dbmfp->clear_len != mfp->clear_len ||
		    pagesize != mfp->stat.st_pagesize ||
		    dbmfp->lsn_offset != mfp->lsn_off) {
			__db_err(dbenv,
		    "%s: clear length, page size or LSN location changed",
			    path);
			R_UNLOCK(dbenv, dbmp->reginfo);
			ret = EINVAL;
			goto err;
		}

		/*
		 * Check to see if this file has died while we waited.
		 *
		 * We normally don't lock the deadfile field when we read it as
		 * we only care if the field is zero or non-zero.  We do lock
		 * on read when searching for a matching MPOOLFILE so that two
		 * threads of control don't race between setting the deadfile
		 * bit and incrementing the reference count, that is, a thread
		 * of control decrementing the reference count and then setting
		 * deadfile because the reference count is 0 blocks us finding
		 * the file without knowing it's about to be marked dead.
		 */
		MUTEX_LOCK(dbenv, &mfp->mutex);
		if (mfp->deadfile) {
			MUTEX_UNLOCK(dbenv, &mfp->mutex);
			continue;
		}
		++mfp->mpf_cnt;
		refinc = 1;
		MUTEX_UNLOCK(dbenv, &mfp->mutex);

		if (dbmfp->ftype != 0)
			mfp->ftype = dbmfp->ftype;

		break;
	}
	R_UNLOCK(dbenv, dbmp->reginfo);

	if (mfp != NULL)
		goto check_map;

alloc:	/* Allocate and initialize a new MPOOLFILE. */
	if ((ret = __memp_alloc(
	    dbmp, dbmp->reginfo, NULL, sizeof(MPOOLFILE), NULL, &mfp)) != 0)
		goto err;
	memset(mfp, 0, sizeof(MPOOLFILE));
	mfp->mpf_cnt = 1;
	mfp->ftype = dbmfp->ftype;
	mfp->stat.st_pagesize = pagesize;
	mfp->lsn_off = dbmfp->lsn_offset;
	mfp->clear_len = dbmfp->clear_len;
	mfp->priority = dbmfp->priority;
	mfp->flushed = 0;
	LIST_INIT(&mfp->dbmpf_list);

	if (dbmfp->gbytes != 0 || dbmfp->bytes != 0) {
		mfp->maxpgno =
		    dbmfp->gbytes * (GIGABYTE / mfp->stat.st_pagesize);
		mfp->maxpgno += (dbmfp->bytes +
		    mfp->stat.st_pagesize - 1) / mfp->stat.st_pagesize;
	}
	if (FLD_ISSET(dbmfp->config_flags, DB_MPOOL_NOFILE))
		mfp->no_backing_file = 1;
	if (FLD_ISSET(dbmfp->config_flags, DB_MPOOL_UNLINK))
		mfp->unlink_on_close = 1;

	if (LF_ISSET(DB_TXN_NOT_DURABLE))
		F_SET(mfp, MP_NOT_DURABLE);
	if (LF_ISSET(DB_DIRECT))
		F_SET(mfp, MP_DIRECT);
	if (LF_ISSET(DB_EXTENT))
		F_SET(mfp, MP_EXTENT);
	F_SET(mfp, MP_CAN_MMAP);

	if (path == NULL)
		F_SET(mfp, MP_TEMP);
	else {
		/*
		 * Don't permit files that aren't a multiple of the pagesize,
		 * and find the number of the last page in the file, all the
		 * time being careful not to overflow 32 bits.
		 *
		 * During verify or recovery, we might have to cope with a
		 * truncated file; if the file size is not a multiple of the
		 * page size, round down to a page, we'll take care of the
		 * partial page outside the mpool system.
		 */
		if (bytes % pagesize != 0) {
			if (LF_ISSET(DB_ODDFILESIZE))
				bytes -= (u_int32_t)(bytes % pagesize);
			else {
				__db_err(dbenv,
		    "%s: file size not a multiple of the pagesize", rpath);
				ret = EINVAL;
				goto err;
			}
		}

		/* 
		 * Notify if the on disk preallocation max is not a multiple
		 * of the page size.
		 */
		if (dbenv->attr.preallocate_max % pagesize != 0) {
			__db_err(dbenv,
		    "%s: preallocate_max is not a multiple of the pagesize", 
				 rpath);
		}
		mfp->prealloc_max = dbenv->attr.preallocate_max / pagesize;

		/* 
		 * Notify if the max io size is not a multiple of the
		 * page size.
		 */
		if (dbenv->attr.sgio_max % pagesize != 0) {
			__db_err(dbenv,
		    "%s: sgio_max is not a multiple of the pagesize", 
				 rpath);
		}

		/*
		 * If the user specifies DB_MPOOL_LAST or DB_MPOOL_NEW on a
		 * page get, we have to increment the last page in the file.
		 * Figure it out and save it away.
		 *
		 * Note correction: page numbers are zero-based, not 1-based.
		 */
		last_pgno = (db_pgno_t)(mbytes * (MEGABYTE / pagesize));
		last_pgno += (db_pgno_t)(bytes / pagesize);
		if (last_pgno != 0)
			--last_pgno;
		mfp->orig_last_pgno = mfp->last_pgno = last_pgno;
		mfp->alloc_pgno = mfp->orig_last_pgno;

		/* Copy the file path into shared memory. */
		if ((ret = __memp_alloc(dbmp, dbmp->reginfo,
		    NULL, strlen(path) + 1, &mfp->path_off, &p)) != 0)
			goto err;
		memcpy(p, path, strlen(path) + 1);

		/* Copy the file identification string into shared memory. */
		if ((ret = __memp_alloc(dbmp, dbmp->reginfo,
		    NULL, DB_FILE_ID_LEN, &mfp->fileid_off, &p)) != 0)
			goto err;
		memcpy(p, dbmfp->fileid, DB_FILE_ID_LEN);
	}

	/* Copy the page cookie into shared memory. */
	if (dbmfp->pgcookie == NULL || dbmfp->pgcookie->size == 0) {
		mfp->pgcookie_len = 0;
		mfp->pgcookie_off = 0;
	} else {
		if ((ret = __memp_alloc(dbmp, dbmp->reginfo,
		    NULL, dbmfp->pgcookie->size, &mfp->pgcookie_off, &p)) != 0)
			goto err;
		memcpy(p, dbmfp->pgcookie->data, dbmfp->pgcookie->size);
		mfp->pgcookie_len = dbmfp->pgcookie->size;
	}

	/*
	 * Prepend the MPOOLFILE to the list of MPOOLFILE's.
	 */
	R_LOCK(dbenv, dbmp->reginfo);
	ret = __db_mutex_setup(dbenv, dbmp->reginfo, &mfp->mutex,
	    MUTEX_NO_RLOCK);
	if (ret == 0)
		SH_TAILQ_INSERT_HEAD(&mp->mpfq, mfp, q, __mpoolfile);
	R_UNLOCK(dbenv, dbmp->reginfo);
	if (ret != 0)
		goto err;

check_map:
	/*
	 * We need to verify that all handles open a file either durable or not
	 * durable.  This needs to be cross process and cross sub-databases, so
	 * mpool is the place to do it.
	 */
	if (!LF_ISSET(DB_RDONLY) &&
	     !LF_ISSET(DB_TXN_NOT_DURABLE) != !F_ISSET(mfp, MP_NOT_DURABLE)) {
		__db_err(dbenv,
	     "Cannot open DURABLE and NOT DURABLE handles in the same file");
		ret = EINVAL;
		goto err;
	}
	/*
	 * All paths to here have initialized the mfp variable to reference
	 * the selected (or allocated) MPOOLFILE.
	 */
	dbmfp->mfp = mfp;

	/*
	 * Set the priority in the non-allocate case.
	 */
	mfp->priority = dbmfp->priority;


	/*
	 * Check to see if we can mmap the file.  If a file:
	 *	+ isn't temporary
	 *	+ is read-only
	 *	+ doesn't require any pgin/pgout support
	 *	+ the DB_NOMMAP flag wasn't set (in either the file open or
	 *	  the environment in which it was opened)
	 *	+ and is less than mp_mmapsize bytes in size
	 *
	 * we can mmap it instead of reading/writing buffers.  Don't do error
	 * checking based on the mmap call failure.  We want to do normal I/O
	 * on the file if the reason we failed was because the file was on an
	 * NFS mounted partition, and we can fail in buffer I/O just as easily
	 * as here.
	 *
	 * We'd like to test to see if the file is too big to mmap.  Since we
	 * don't know what size or type off_t's or size_t's are, or the largest
	 * unsigned integral type is, or what random insanity the local C
	 * compiler will perpetrate, doing the comparison in a portable way is
	 * flatly impossible.  Hope that mmap fails if the file is too large.
	 */
#define	DB_MAXMMAPSIZE	(10 * 1024 * 1024)	/* 10 MB. */
	if (F_ISSET(mfp, MP_CAN_MMAP)) {
		maxmap = dbenv->mp_mmapsize == 0 ?
		    DB_MAXMMAPSIZE : dbenv->mp_mmapsize;
		if (path == NULL)
			F_CLR(mfp, MP_CAN_MMAP);
		else if (!F_ISSET(dbmfp, MP_READONLY))
			F_CLR(mfp, MP_CAN_MMAP);
		else if (dbmfp->ftype != 0)
			F_CLR(mfp, MP_CAN_MMAP);
		else if (LF_ISSET(DB_NOMMAP) || F_ISSET(dbenv, DB_ENV_NOMMAP))
			F_CLR(mfp, MP_CAN_MMAP);
		else if (mbytes > maxmap / MEGABYTE ||
		    (mbytes == maxmap / MEGABYTE && bytes >= maxmap % MEGABYTE))
			F_CLR(mfp, MP_CAN_MMAP);

		dbmfp->addr = NULL;
		if (F_ISSET(mfp, MP_CAN_MMAP)) {
			dbmfp->len = (size_t)mbytes * MEGABYTE + bytes;
			if (__os_mapfile(dbenv, rpath,
			    dbmfp->fhp, dbmfp->len, 1, &dbmfp->addr) != 0) {
				dbmfp->addr = NULL;
				F_CLR(mfp, MP_CAN_MMAP);
			}
		}
	}

	F_SET(dbmfp, MP_OPEN_CALLED);

	/*
	 * Share the underlying file descriptor if that's possible.
	 *
	 * Add the file to the process' list of DB_MPOOLFILEs.
	 */
	MUTEX_THREAD_LOCK(dbenv, dbmp->mutexp);

	for (tmp_dbmfp = TAILQ_FIRST(&dbmp->dbmfq);
	    tmp_dbmfp != NULL; tmp_dbmfp = TAILQ_NEXT(tmp_dbmfp, q))
		if (dbmfp->mfp == tmp_dbmfp->mfp &&
		    (F_ISSET(dbmfp, MP_READONLY) ||
		    !F_ISSET(tmp_dbmfp, MP_READONLY))) {
			if (dbmfp->fhp->mutexp != NULL)
				__db_mutex_free(
				    dbenv, dbmp->reginfo, dbmfp->fhp->mutexp);
			(void)__os_closehandle(dbenv, dbmfp->fhp);

			++tmp_dbmfp->fhp->ref;
			dbmfp->fhp = tmp_dbmfp->fhp;

			if (dbmfp->recp != NULL) {
				if (dbmfp->recp->mutexp != NULL)
					__db_mutex_free(dbenv, dbmp->reginfo,
					    dbmfp->recp->mutexp);
				__os_closehandle(dbenv, dbmfp->recp);
				dbmfp->recp = tmp_dbmfp->recp;

				for (i = 0; i <= dbenv->mp_recovery_pages; i++)
					Pthread_mutex_destroy(&dbmfp->
					    recp_lk_array[i]);

				__os_free(dbenv, dbmfp->recp_lk_array);
				dbmfp->recp_lk_array = tmp_dbmfp->recp_lk_array;
			}
			break;
		}

	TAILQ_INSERT_TAIL(&dbmp->dbmfq, dbmfp, q);
	LIST_INSERT_HEAD(&mfp->dbmpf_list, dbmfp, mpfq);

	MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);

	if (0) {
err:		if (refinc) {
			MUTEX_LOCK(dbenv, &mfp->mutex);
			--mfp->mpf_cnt;
			MUTEX_UNLOCK(dbenv, &mfp->mutex);
		}

	}
	if (rpath != NULL)
		__os_free(dbenv, rpath);
	if (recp_path != NULL)
		__os_free(dbenv, recp_path);
	return (ret);
}

/*
 * __memp_last_pgno --
 *	Return the page number of the last page in the file.
 *
 * !!!
 * Undocumented interface: DB private.
 *
 * PUBLIC: void __memp_last_pgno __P((DB_MPOOLFILE *, db_pgno_t *));
 */
void
__memp_last_pgno(dbmfp, pgnoaddr)
	DB_MPOOLFILE *dbmfp;
	db_pgno_t *pgnoaddr;
{
	DB_ENV *dbenv;
	DB_MPOOL *dbmp;

	dbenv = dbmfp->dbenv;
	dbmp = dbenv->mp_handle;

	R_LOCK(dbenv, dbmp->reginfo);
	*pgnoaddr = dbmfp->mfp->last_pgno;
	R_UNLOCK(dbenv, dbmp->reginfo);
}

/*
 * memp_fclose_pp --
 *	DB_MPOOLFILE->close pre/post processing.
 */
static int
__memp_fclose_pp(dbmfp, flags)
	DB_MPOOLFILE *dbmfp;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	int rep_check, ret, t_ret;

	dbenv = dbmfp->dbenv;

	/*
	 * Validate arguments, but as a handle destructor, we can't fail.
	 *
	 * !!!
	 * DB_MPOOL_DISCARD: Undocumented flag: DB private.
	 */
	ret = __db_fchk(dbenv, "DB_MPOOLFILE->close", flags, DB_MPOOL_DISCARD);

	rep_check = IS_ENV_REPLICATED(dbenv) ? 1 : 0;
	if (rep_check)
		__env_rep_enter(dbenv);
	if ((t_ret = __memp_fclose(dbmfp, flags)) != 0 && ret == 0)
		ret = t_ret;
	if (rep_check)
		__env_rep_exit(dbenv);
	return (ret);
}

/*
 * __memp_fclose --
 *	DB_MPOOLFILE->close.
 *
 * PUBLIC: int __memp_fclose __P((DB_MPOOLFILE *, u_int32_t));
 */
int
__memp_fclose(dbmfp, flags)
	DB_MPOOLFILE *dbmfp;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	DB_MPOOL *dbmp;
	MPOOLFILE *mfp;
	char *rpath;
	u_int32_t ref;
	int deleted, ret, t_ret, i;

	dbenv = dbmfp->dbenv;
	dbmp = dbenv->mp_handle;
	ret = 0;

	/*
	 * Remove the DB_MPOOLFILE from the process' list.
	 *
	 * It's possible the underlying mpool cache may never have been created.
	 * In that case, all we have is a structure, discard it.
	 *
	 * It's possible the DB_MPOOLFILE was never added to the DB_MPOOLFILE
	 * file list, check the MP_OPEN_CALLED flag to be sure.
	 */
	if (dbmp == NULL)
		goto done;

	MUTEX_THREAD_LOCK(dbenv, dbmp->mutexp);

	DB_ASSERT(dbmfp->ref >= 1);
	if ((ref = --dbmfp->ref) == 0 && F_ISSET(dbmfp, MP_OPEN_CALLED)) {
		TAILQ_REMOVE(&dbmp->dbmfq, dbmfp, q);
		LIST_REMOVE(dbmfp, mpfq);
	}

	/*
	 * Decrement the file descriptor's ref count -- if we're the last ref,
	 * we'll discard the file descriptor.
	 */
	if (ref == 0 && dbmfp->fhp != NULL && --dbmfp->fhp->ref > 0) {
		dbmfp->fhp = NULL;
		dbmfp->recp = NULL;
	}

	MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);
	if (ref != 0)
		return (0);

	/* Complain if pinned blocks never returned. */
	if (dbmfp->pinref != 0) {
		__db_err(dbenv, "%s: close: %lu blocks left pinned",
		    __memp_fn(dbmfp), (u_long)dbmfp->pinref);
		ret = __db_panic(dbenv, DB_RUNRECOVERY);
	}

	/* Discard any mmap information. */
	if (dbmfp->addr != NULL &&
	    (ret = __os_unmapfile(dbenv, dbmfp->addr, dbmfp->len)) != 0)
		__db_err(dbenv, "%s: %s", __memp_fn(dbmfp), db_strerror(ret));

	/*
	 * Close the file and discard the descriptor structure; temporary
	 * files may not yet have been created.
	 */
	if (dbmfp->fhp != NULL) {
		if (dbmfp->fhp->mutexp != NULL) {
			__db_mutex_free(
			    dbenv, dbmp->reginfo, dbmfp->fhp->mutexp);
			dbmfp->fhp->mutexp = NULL;
		}
		if ((t_ret = __os_closehandle(dbenv, dbmfp->fhp)) != 0) {
			__db_err(dbenv, "%s: %s",
			    __memp_fn(dbmfp), db_strerror(t_ret));
			if (ret == 0)
				ret = t_ret;
		}
		dbmfp->fhp = NULL;
	}

	/* Close the recovery-page file.  */
	if (dbmfp->recp != NULL) {
		if (dbmfp->recp->mutexp != NULL) {
			__db_mutex_free(dbenv, dbmp->reginfo,
			    dbmfp->recp->mutexp);
			dbmfp->recp->mutexp = NULL;
		}
		if ((t_ret = __os_closehandle(dbenv, dbmfp->recp)) != 0) {
			__db_err(dbenv, "%s: %s",
			    __memp_fn(dbmfp), db_strerror(t_ret));
			if (ret == 0)
				ret = t_ret;
		}
		dbmfp->recp = NULL;

		for (i = 0; i <= dbenv->mp_recovery_pages; i++)
			Pthread_mutex_destroy(&dbmfp->recp_lk_array[i]);

		__os_free(dbenv, dbmfp->recp_lk_array);
		dbmfp->recp_lk_array = NULL;
	}

	/*
	 * Discard our reference on the underlying MPOOLFILE, and close it
	 * if it's no longer useful to anyone.  It possible the open of the
	 * file never happened or wasn't successful, in which case, mpf will
	 * be NULL and MP_OPEN_CALLED will not be set.
	 */
	mfp = dbmfp->mfp;
	DB_ASSERT((F_ISSET(dbmfp, MP_OPEN_CALLED) && mfp != NULL) ||
	    (!F_ISSET(dbmfp, MP_OPEN_CALLED) && mfp == NULL));
	if (!F_ISSET(dbmfp, MP_OPEN_CALLED))
		goto done;

	/*
	 * If it's a temp file, all outstanding references belong to unflushed
	 * buffers.  (A temp file can only be referenced by one DB_MPOOLFILE).
	 * We don't care about preserving any of those buffers, so mark the
	 * MPOOLFILE as dead so that even the dirty ones just get discarded
	 * when we try to flush them.
	 */
	deleted = 0;
	MUTEX_LOCK(dbenv, &mfp->mutex);
	if (--mfp->mpf_cnt == 0 || LF_ISSET(DB_MPOOL_DISCARD)) {
		if (LF_ISSET(DB_MPOOL_DISCARD) ||
		    F_ISSET(mfp, MP_TEMP) || mfp->unlink_on_close)
			mfp->deadfile = 1;
		if (mfp->unlink_on_close) {
			if ((t_ret = __db_appname(dbmp->dbenv,
			    DB_APP_DATA, R_ADDR(dbmp->reginfo,
			    mfp->path_off), 0, NULL, &rpath)) != 0 && ret == 0)
				ret = t_ret;
			if (t_ret == 0) {
				if ((t_ret = __os_unlink(
				    dbmp->dbenv, rpath) != 0) && ret == 0)
					ret = t_ret;
				__os_free(dbenv, rpath);
			}
		}
		if (mfp->block_cnt == 0) {
			if ((t_ret =
			    __memp_mf_discard(dbmp, mfp)) != 0 && ret == 0)
				ret = t_ret;
			deleted = 1;
		}
	}
	if (deleted == 0)
		MUTEX_UNLOCK(dbenv, &mfp->mutex);

done:	/* Discard the DB_MPOOLFILE structure. */
	if (dbmfp->pgcookie != NULL) {
		__os_free(dbenv, dbmfp->pgcookie->data);
		__os_free(dbenv, dbmfp->pgcookie);
	}
	__os_free(dbenv, dbmfp);

	return (ret);
}

/*
 * __memp_mf_sync --
 *	 sync an MPOOLFILE.  Should only be used when
 * the file is not already open in this process.
 *
 * PUBLIC: int __memp_mf_sync __P((DB_MPOOL *, MPOOLFILE *));
 */
int
__memp_mf_sync(dbmp, mfp)
	DB_MPOOL *dbmp;
	MPOOLFILE *mfp;
{
	DB_ENV *dbenv;
	DB_FH *fhp;
	int ret, t_ret;
	char *rpath;

	dbenv = dbmp->dbenv;

	if ((ret = __db_appname(dbenv, DB_APP_DATA,
	    R_ADDR(dbmp->reginfo, mfp->path_off), 0, NULL, &rpath)) == 0) {
		if ((ret = __os_open(dbenv, rpath, 0, 0, &fhp)) == 0) {
			ret = __os_fsync(dbenv, fhp);
			if ((t_ret =
			    __os_closehandle(dbenv, fhp)) != 0 && ret == 0)
				ret = t_ret;
		}
		__os_free(dbenv, rpath);
	}

	return (ret);
}

/*
 * __memp_mf_discard --
 *	Discard an MPOOLFILE.
 *
 * PUBLIC: int __memp_mf_discard __P((DB_MPOOL *, MPOOLFILE *));
 */
int
__memp_mf_discard(dbmp, mfp)
	DB_MPOOL *dbmp;
	MPOOLFILE *mfp;
{
	DB_ENV *dbenv;
	DB_MPOOL_STAT *sp;
	MPOOL *mp;
	int ret;

	dbenv = dbmp->dbenv;
	mp = dbmp->reginfo[0].primary;
	ret = 0;

	/*
	 * Expects caller to be holding the MPOOLFILE mutex.
	 *
	 * When discarding a file, we have to flush writes from it to disk.
	 * The scenario is that dirty buffers from this file need to be
	 * flushed to satisfy a future checkpoint, but when the checkpoint
	 * calls mpool sync, the sync code won't know anything about them.
	 */
	if (mfp->file_written && !mfp->deadfile)
		ret = __memp_mf_sync(dbmp, mfp);

	/*
	 * We have to release the MPOOLFILE lock before acquiring the region
	 * lock so that we don't deadlock.  Make sure nobody ever looks at
	 * this structure again.
	 */
	mfp->deadfile = 1;

	/* Discard the mutex we're holding. */
	MUTEX_UNLOCK(dbenv, &mfp->mutex);

	/* Delete from the list of MPOOLFILEs. */
	R_LOCK(dbenv, dbmp->reginfo);
	SH_TAILQ_REMOVE(&mp->mpfq, mfp, q, __mpoolfile);

	/* Copy the statistics into the region. */
	sp = &mp->stat;
	sp->st_cache_hit += mfp->stat.st_cache_hit;
	sp->st_cache_miss += mfp->stat.st_cache_miss;
	sp->st_cache_ihit += mfp->stat.st_cache_ihit;
	sp->st_cache_imiss += mfp->stat.st_cache_imiss;
	sp->st_cache_lhit += mfp->stat.st_cache_lhit;
	sp->st_cache_lmiss += mfp->stat.st_cache_lmiss;
	sp->st_map += mfp->stat.st_map;
	sp->st_page_create += mfp->stat.st_page_create;
	sp->st_page_in += mfp->stat.st_page_in;
	sp->st_page_out += mfp->stat.st_page_out;
	sp->st_ro_merges += mfp->stat.st_ro_merges;
	sp->st_rw_merges += mfp->stat.st_rw_merges;

	/* Clear the mutex this MPOOLFILE recorded. */
	__db_shlocks_clear(&mfp->mutex, dbmp->reginfo,
	    (REGMAINT *)R_ADDR(dbmp->reginfo, mp->maint_off));

	/* Free the space. */
	if (mfp->path_off != 0)
		__db_shalloc_free(dbmp->reginfo[0].addr,
		    R_ADDR(dbmp->reginfo, mfp->path_off));
	if (mfp->fileid_off != 0)
		__db_shalloc_free(dbmp->reginfo[0].addr,
		    R_ADDR(dbmp->reginfo, mfp->fileid_off));
	if (mfp->pgcookie_off != 0)
		__db_shalloc_free(dbmp->reginfo[0].addr,
		    R_ADDR(dbmp->reginfo, mfp->pgcookie_off));
	__db_shalloc_free(dbmp->reginfo[0].addr, mfp);

	R_UNLOCK(dbenv, dbmp->reginfo);

	return (ret);
}

/*
 * __memp_fn --
 *	On errors we print whatever is available as the file name.
 *
 * PUBLIC: char * __memp_fn __P((DB_MPOOLFILE *));
 */
char *
__memp_fn(dbmfp)
	DB_MPOOLFILE *dbmfp;
{
	return (__memp_fns(dbmfp->dbenv->mp_handle, dbmfp->mfp));
}

/*
 * __memp_fns --
 *	On errors we print whatever is available as the file name.
 *
 * PUBLIC: char * __memp_fns __P((DB_MPOOL *, MPOOLFILE *));
 *
 */
char *
__memp_fns(dbmp, mfp)
	DB_MPOOL *dbmp;
	MPOOLFILE *mfp;
{
	if (mfp->path_off == 0)
		return ((char *)"temporary");

	return ((char *)R_ADDR(dbmp->reginfo, mfp->path_off));
}
