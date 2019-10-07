/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
#include "db_config.h"
#include "pthread.h"

#ifndef lint
static const char revid[] = "$Id: mp_bh.c,v 11.86 2003/07/02 20:02:37 mjc Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <alloca.h>
#include <limits.h>
#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>

#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_shash.h"
#include "dbinc/mp.h"
#include "dbinc/log.h"
#include "dbinc/db_page.h"
#include "dbinc/db_swap.h"
#include "dbinc/hmac.h"
#include "dbinc_auto/txn_auto.h"
#include "dbinc/txn.h"

#include <sys/types.h>
#include <dirent.h>

#include "logmsg.h"
#include "locks_wrap.h"
#include "thread_stats.h"
#include "comdb2_atomic.h"

char *bdb_trans(const char infile[], char outfile[]);
extern int gbl_test_badwrite_intvl;

static int __memp_pgwrite
__P((DB_ENV *, DB_MPOOLFILE *, DB_MPOOL_HASH *, BH *));

static int __memp_pgwrite_int
__P((DB_ENV *, DB_MPOOLFILE *, DB_MPOOL_HASH *, BH *, int));

static int __memp_pgwrite_multi
__P((DB_ENV *, DB_MPOOLFILE *, DB_MPOOL_HASH **, BH **, int, int));

/*
 * __memp_bhwrite --
 *	Write the page associated with a given buffer header.
 *
 * PUBLIC: int __memp_bhwrite __P((DB_MPOOL *,
 * PUBLIC:      DB_MPOOL_HASH *, MPOOLFILE *, BH *, int));
 */
int
__memp_bhwrite(dbmp, hp, mfp, bhp, open_extents)
	DB_MPOOL *dbmp;
	DB_MPOOL_HASH *hp;
	MPOOLFILE *mfp;
	BH *bhp;
	int open_extents;
{
	return (__memp_bhwrite_multi(dbmp, &hp, mfp, &bhp, 1, open_extents));
}

/*
 * __memp_bhwrite_multi --
 *	Write the page associated with a given buffer header.
 *
 * PUBLIC: int __memp_bhwrite_multi __P((DB_MPOOL *,
 * PUBLIC:      DB_MPOOL_HASH **, MPOOLFILE *, BH **, int, int));
 */
int
__memp_bhwrite_multi(dbmp, hps, mfp, bhps, numpages, open_extents)
	DB_MPOOL *dbmp;
	DB_MPOOL_HASH **hps;
	MPOOLFILE *mfp;
	BH **bhps;
	int numpages;
	int open_extents;
{
	DB_ENV *dbenv;
	DB_MPOOLFILE *dbmfp;
	DB_MPREG *mpreg;
	int ret;

	dbenv = dbmp->dbenv;

	/*
	 * If the file has been removed or is a closed temporary file
	 * or it's an old queue extent, we're done -- the page-write
	 * function knows how to handle the fact that we don't have
	 * (or need!) any real file descriptor information.
	 */
	if (mfp == NULL || mfp->deadfile)
		return (__memp_pgwrite_multi(dbenv, NULL, hps, 
					     bhps, numpages, 1));

	/*
	 * Walk the process' DB_MPOOLFILE list and find a file descriptor for
	 * the file.  We also check that the descriptor is open for writing.
	 */
	MUTEX_THREAD_LOCK(dbenv, dbmp->mutexp);
	for (dbmfp = TAILQ_FIRST(&dbmp->dbmfq);
	    dbmfp != NULL; dbmfp = TAILQ_NEXT(dbmfp, q))
		if (dbmfp->mfp == mfp && !F_ISSET(dbmfp, MP_READONLY)) {
			++dbmfp->ref;
			break;
		}
	MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);

	if (dbmfp != NULL) {
		/*
		 * Temporary files may not have been created.  We only handle
		 * temporary files in this path, because only the process that
		 * created a temporary file will ever flush buffers to it.
		 */
		if (dbmfp->fhp == NULL) {
			/* We may not be allowed to create backing files. */
			if (mfp->no_backing_file)
				return (EPERM);

			MUTEX_THREAD_LOCK(dbenv, dbmp->mutexp);
			if (dbmfp->fhp == NULL)
				ret = __db_appname(dbenv, DB_APP_TMP, NULL,
				    F_ISSET(dbenv, DB_ENV_DIRECT_DB) ?
				    DB_OSO_DIRECT : 0, &dbmfp->fhp, NULL);
			else
				ret = 0;
			MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);
			if (ret != 0) {
				__db_err(dbenv,
				    "unable to create temporary backing file");
				return (ret);
			}
		}

		goto pgwrite;
	}

	/*
	 * There's no file handle for this file in our process.
	 *
	 * !!!
	 * It's the caller's choice if we're going to open extent files.
	 */
	if (!open_extents && F_ISSET(mfp, MP_EXTENT))
		return (EPERM);

	/*
	 * !!!
	 * Don't try to attach to temporary files.  There are two problems in
	 * trying to do that.  First, if we have different privileges than the
	 * process that "owns" the temporary file, we might create the backing
	 * disk file such that the owning process couldn't read/write its own
	 * buffers, e.g., memp_trickle running as root creating a file owned
	 * as root, mode 600.  Second, if the temporary file has already been
	 * created, we don't have any way of finding out what its real name is,
	 * and, even if we did, it was already unlinked (so that it won't be
	 * left if the process dies horribly).  This decision causes a problem,
	 * however: if the temporary file consumes the entire buffer cache,
	 * and the owner doesn't flush the buffers to disk, we could end up
	 * with resource starvation, and the memp_trickle thread couldn't do
	 * anything about it.  That's a pretty unlikely scenario, though.
	 *
	 * Note we should never get here when the temporary file in question
	 * has already been closed in another process, in which case it should
	 * be marked dead.
	 */
	if (F_ISSET(mfp, MP_TEMP))
		return (EPERM);

	/*
	 * It's not a page from a file we've opened.  If the file requires
	 * input/output processing, see if this process has ever registered
	 * information as to how to write this type of file.  If not, there's
	 * nothing we can do.
	 */
	if (mfp->ftype != 0) {
		MUTEX_THREAD_LOCK(dbenv, dbmp->mutexp);
		for (mpreg = LIST_FIRST(&dbmp->dbregq);
		    mpreg != NULL; mpreg = LIST_NEXT(mpreg, q))
			if (mpreg->ftype == mfp->ftype)
				break;
		MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);
		if (mpreg == NULL)
			return (EPERM);
	}

	/*
	 * Try and open the file, attaching to the underlying shared area.
	 * Ignore any error, assume it's a permissions problem.
	 *
	 * XXX
	 * There's no negative cache, so we may repeatedly try and open files
	 * that we have previously tried (and failed) to open.
	 */
	if ((ret = __memp_fcreate(dbenv, &dbmfp)) != 0)
		return (ret);
	if ((ret = __memp_fopen(dbmfp, mfp,
	    R_ADDR(dbmp->reginfo, mfp->path_off),
	    0, 0, mfp->stat.st_pagesize)) != 0) {
		(void)__memp_fclose(dbmfp, 0);
		return (ret);
	}

pgwrite:
	ret = __memp_pgwrite_multi(dbenv, dbmfp, hps, bhps, numpages, 1);

	/*
	 * Discard our reference, and, if we're the last reference, make sure
	 * the file eventually gets closed.
	 */
	MUTEX_THREAD_LOCK(dbenv, dbmp->mutexp);
	if (dbmfp->ref == 1)
		F_SET(dbmfp, MP_FLUSH);
	else
		--dbmfp->ref;
	MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);

	return (ret);
}

/*
 * __dir_pgread_multi --
 *  Read pages from a file without going through the buffer-pool.
 *
 * PUBLIC: int __dir_pgread_multi __P((DB_MPOOLFILE *, db_pgno_t pgno, int *numpages, u_int8_t *pages ));
 */
int
__dir_pgread_multi(dbmfp, pgno, numpages, pages)
	DB_MPOOLFILE *dbmfp;
	db_pgno_t pgno;
	int *numpages;
	u_int8_t *pages;
{
	DB_ENV *dbenv;
	MPOOLFILE *mfp;
	size_t nr, pagesize;
	int cntpage;
	int ret, idx;

	cntpage = *numpages;
	*numpages = 0;

	dbenv = dbmfp->dbenv;
	mfp = dbmfp->mfp;
	pagesize = mfp->stat.st_pagesize;

	for (idx = 0; cntpage--; idx += pagesize) {
		nr = 0;

		if ((ret = __os_io(dbenv, DB_IO_READ,
			    dbmfp->fhp, pgno, pagesize, &pages[idx], &nr)) != 0)
			return (ret);

		if (nr < pagesize) {
			return (*numpages) ? 0 : DB_PAGE_NOTFOUND;
		}

		++mfp->stat.st_page_in;

		if ((ret = mfp->ftype == 0 ? 0 :
			__dir_pg(dbmfp, pgno, &pages[idx], 1)) != 0)
			return (ret);

		(*numpages)++;
	}
	return 0;
}

/*
 * __dir_pgread --
 *  Read a page from a file without going through the buffer-pool.
 *
 * PUBLIC: int __dir_pgread __P((DB_MPOOLFILE *, db_pgno_t pgno, u_int8_t *page ));
 */
int
__dir_pgread(dbmfp, pgno, page)
	DB_MPOOLFILE *dbmfp;
	db_pgno_t pgno;
	u_int8_t *page;
{
	int numpages;

	numpages = 1;
	return __dir_pgread_multi(dbmfp, pgno, &numpages, page);
}

/*
 * __memp_recover_page --
 *  Search the recovery-page cache for the latest version of this page.
 */
static int
__memp_recover_page(dbmfp, hp, bhp, pgno)
	DB_MPOOLFILE *dbmfp;
	DB_MPOOL_HASH *hp;
	BH *bhp;
	db_pgno_t pgno;
{

	DB_ENV *dbenv;
	DB_LSN page_lsn, largest_lsn;
	DB_MPREG *mpreg;
	MPOOLFILE *mfp;
	DB_MPOOL *dbmp;
	MPOOL *c_mp;
	size_t nr, pagesize;
	DB_PGINFO duminfo = { 0 }, *pginfo;
	PAGE *pagep;
	int ret, i, pgidx, free_buf, ftype;
	u_int32_t n_cache;
	db_pgno_t inpg;

	dbenv = dbmfp->dbenv;
	mfp = dbmfp->mfp;
	pagesize = mfp->stat.st_pagesize;
	dbmp = dbenv->mp_handle;
	n_cache = NCACHE(dbmp->reginfo[0].primary, bhp->mf_offset, bhp->pgno);
	c_mp = dbmp->reginfo[n_cache].primary;

	pgidx = -1;
	free_buf = 0;

	ZERO_LSN(largest_lsn);

	/* Short circuit if there's no recover page file. */
	if (dbmfp->recp == NULL)
		return DB_PAGE_NOTFOUND;

	pginfo = &duminfo;
	ftype = mfp->ftype;

	MUTEX_THREAD_LOCK(dbenv, dbmp->mutexp);

	/* Get the page-cookie. */
	for (mpreg = LIST_FIRST(&dbmp->dbregq);
	    mpreg != NULL; mpreg = LIST_NEXT(mpreg, q)) {
		if (ftype != mpreg->ftype)
			continue;
		if (mfp->pgcookie_len > 0) {
			pginfo = (DB_PGINFO *)R_ADDR(dbmp->reginfo,
			    mfp->pgcookie_off);
		}
		break;
	}

	MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);

	/* Create a temporary pagecache. */
	if (pagesize <= 4096) {
		pagep = (PAGE *)alloca(pagesize);
	} else {
		if ((ret = __os_malloc(dbenv, pagesize, &pagep)) != 0)
			return (ret);
		free_buf = 1;
	}

	/* If this is page-0, just read the meta page. */
	for (i = 0; i <= dbenv->mp_recovery_pages; i++) {
		/* Lock out other threads */
		Pthread_mutex_lock(&dbmfp->recp_lk_array[i]);

		/* Read page. */
		if ((ret = __os_io(dbenv, DB_IO_READ,
		    dbmfp->recp, i, pagesize,
		    (u_int8_t *)pagep, &nr)) != 0) {
			Pthread_mutex_unlock(&dbmfp->recp_lk_array[i]);
			break;
		}

		Pthread_mutex_unlock(&dbmfp->recp_lk_array[i]);

		/* Verify length. */
		if (nr < pagesize)
			break;

		/* Check if this is the correct page. */
		if (F_ISSET(pginfo, DB_AM_SWAP)) {
			P_32_COPYSWAP(&PGNO(pagep), &inpg);
			P_32_COPYSWAP(&LSN(pagep).file, &page_lsn.file);
			P_32_COPYSWAP(&LSN(pagep).offset, &page_lsn.offset);
		} else {
			inpg = PGNO(pagep);
			page_lsn.file = LSN(pagep).file;
			page_lsn.offset = LSN(pagep).offset;
		}

		/* Skip if the page number is wrong. */
		if (inpg != pgno)
			continue;

		/* Found something: copy it into bhp. */
		if (log_compare(&page_lsn, &largest_lsn) > 0) {
			pgidx = i;
			memcpy(&largest_lsn, &page_lsn, sizeof(page_lsn));
			memcpy(bhp->buf, pagep, pagesize);
		}
	}

	if (free_buf)
		free(pagep);

	if (pgidx < 0)
		return DB_PAGE_NOTFOUND;

	ATOMIC_ADD32(hp->hash_page_dirty, 1);
	ATOMIC_ADD32(c_mp->stat.st_page_dirty, 1);
	F_SET(bhp, BH_DIRTY);
	F_CLR(bhp, BH_TRASH);

	/* The page was clean before getting in here, so update the LSN. */
    if (dbenv->tx_perfect_ckp)
		bhp->first_dirty_tx_begin_lsn = __txn_get_first_dirty_begin_lsn(largest_lsn);

	logmsg(LOGMSG_INFO, "Found recovery page %d lsn %d:%d at idx %d\n",
	    pgno, largest_lsn.file, largest_lsn.offset, pgidx);

	return 0;
}


/*
 * __memp_pgread --
 *	Read a page from a file.
 *
 * PUBLIC: int __memp_pgread __P((DB_MPOOLFILE *, DB_MPOOL_HASH *, BH *, int, int));
 */
int
__memp_pgread(dbmfp, hp, bhp, can_create, is_recovery_page)
	DB_MPOOLFILE *dbmfp;
	DB_MPOOL_HASH *hp;
	BH *bhp;
	int can_create;
	int is_recovery_page;
{
	DB_ENV *dbenv;
	MPOOLFILE *mfp;
	DB_MUTEX *mutexp;
	size_t len, nr, pagesize;
	int ret, try_recover;

	mutexp = &hp->hash_mutex;
	dbenv = dbmfp->dbenv;
	mfp = dbmfp->mfp;
	pagesize = mfp->stat.st_pagesize;
	try_recover = 0;

	/* We should never be called with a dirty or a locked buffer. */
	DB_ASSERT(!F_ISSET(bhp, BH_DIRTY | BH_DIRTY_CREATE | BH_LOCKED));

	/* Lock the buffer and swap the hash bucket lock for the buffer lock. */
	F_SET(bhp, BH_LOCKED | BH_TRASH);
	MUTEX_LOCK(dbenv, &bhp->mutex);
	MUTEX_UNLOCK(dbenv, mutexp);

	/*
	 * Temporary files may not yet have been created.  We don't create
	 * them now, we create them when the pages have to be flushed.
	 */
	nr = 0;
	if (dbmfp->fhp != NULL)
		if ((ret = __os_io(dbenv, DB_IO_READ,
		    dbmfp->fhp, bhp->pgno, pagesize, bhp->buf, &nr)) != 0)
			goto err;

	/*
	 * The page may not exist; if it doesn't, nr may well be 0, but we
	 * expect the underlying OS calls not to return an error code in
	 * this case.
	 */
	if (nr < pagesize) {
		/*
		 * If this is a read_recovery_page fget, try to
		 * restore from the recovery page log */
		if (is_recovery_page) {
			goto recover_page;
		}

		/*
		 * Don't output error messages for short reads.  In particular,
		 * DB recovery processing may request pages never written to
		 * disk or for which only some part have been written to disk,
		 * in which case we won't find the page.  The caller must know
		 * how to handle the error.
		 */
		if (can_create == 0) {
			ret = DB_PAGE_NOTFOUND;
			goto err;
		}

		/* Clear any bytes that need to be cleared. */
		len = mfp->clear_len == 0 ? pagesize : mfp->clear_len;
		memset(bhp->buf, 0, len);

#if defined(DIAGNOSTIC) || defined(UMRW)
		/*
		 * If we're running in diagnostic mode, corrupt any bytes on
		 * the page that are unknown quantities for the caller.
		 */
		if (len < pagesize)
			memset(bhp->buf + len, CLEAR_BYTE, pagesize - len);
#endif
		++mfp->stat.st_page_create;
	} else
		++mfp->stat.st_page_in;

	if (0) {
recover_page:
		if (__memp_recover_page(dbmfp, hp, bhp, bhp->pgno)) {
			/* This is a catastrophic error: panic the database. */
			__db_err(dbenv,
			    "checksum error: page %lu: catastrophic "
			    "recovery required", (u_long) bhp->pgno);
			return (__db_panic(dbenv, DB_RUNRECOVERY));
		}
		++try_recover;
	}

	/* Call any pgin function. */
	ret = mfp->ftype == 0 ? 0 : __memp_pg(dbmfp, bhp, 1);

	/* Search the recovery page cache on cksum error. */
	if (ret == DB_SEARCH_PGCACHE && 0 == try_recover)
		goto recover_page;

	/* Panic if it didn't work otherwise . */
	if (ret)
		return (__db_panic(dbenv, DB_RUNRECOVERY));

	/* Unlock the buffer and reacquire the hash bucket lock. */
err:	MUTEX_UNLOCK(dbenv, &bhp->mutex);
	MUTEX_LOCK(dbenv, mutexp);

	/*
	 * If no errors occurred, the data is now valid, clear the BH_TRASH
	 * flag; regardless, clear the lock bit and let other threads proceed.
	 */
	F_CLR(bhp, BH_LOCKED);
	if (ret == 0)
		F_CLR(bhp, BH_TRASH);


	return (ret);
}

#if 0
struct logfile {
	int lsn;
	char *fname;
	int fd;
	int used;
};
#endif

int gbl_verify_lsn_written = 0;

pthread_mutex_t verifylk = PTHREAD_MUTEX_INITIALIZER;

int berkdb_verify_lsn_written_to_disk(DB_ENV *dbenv, DB_LSN *lsn,
    int check_checkpoint);

#include <limits.h>

int
berkdb_verify_page_lsn_written_to_disk(DB_ENV *dbenv, DB_LSN *lsn)
{
	DIR *d;
	int filenum = 0;
	struct dirent *ent;
	char dir[PATH_MAX];
	bdb_trans(dbenv->db_home, dir);

	Pthread_mutex_lock(&verifylk);
	d = opendir(dir);
	if (d == NULL) {
		__db_err(dbenv, "Can't get directory listing");
		Pthread_mutex_unlock(&verifylk);
		return 1;
	}

	ent = readdir(d);
	while (ent) {
		int n;

		if (strncmp(ent->d_name, "log.", 4) == 0) {
			errno = 0;
			n = strtol(ent->d_name + 4, NULL, 10);

			if (n > 0 && errno == 0) {
				if (n > filenum)
					filenum = n;
			}
		}
		ent = readdir(d);
	}
	closedir(d);

	Pthread_mutex_unlock(&verifylk);

	/* guaranteed written */
	if (lsn->file < filenum)
		return 0;

	return berkdb_verify_lsn_written_to_disk(dbenv, lsn, 0);
}


int
berkdb_verify_lsn_written_to_disk(DB_ENV *dbenv, DB_LSN *lsn,
    int check_checkpoint)
{
	DB_FH *fh;
	char *logname;
	int rc;
	HDR hdr = { 0 };
	uint8_t *logent = NULL;
	u_int32_t type = 0;


	if (dbenv->lg_handle == NULL)
		return 0;

	rc = __log_name(dbenv->lg_handle, lsn->file, &logname, &fh,
	    DB_OSO_RDONLY | DB_OSO_SEQ);
	if (rc) {
		__db_err(dbenv, "can't open log for %u:%u\n", lsn->file,
		    lsn->offset);
		return 1;
	}
	__os_free(dbenv, logname);
	rc = __os_seek(dbenv, fh, 0, 0, lsn->offset, 0, DB_OS_SEEK_SET);
	if (rc) {
		__db_err(dbenv, "rc %d on seek to %u in log %u\n", rc,
		    lsn->offset, lsn->file);
		__os_closehandle(dbenv, fh);
		return 1;
	}
	int is_hmac;
	size_t hdrsz;

	if (CRYPTO_ON(dbenv)) {
		hdrsz = HDR_CRYPTO_SZ;
		is_hmac = 1;
	} else {
		hdrsz = HDR_NORMAL_SZ;
		is_hmac = 0;
	}
	rc = read(fh->fd, &hdr, hdrsz);
	if (rc != hdrsz) {
		__db_err(dbenv,
		    "error reading log record header at %u:%u errno:%d expect:%d got:%d\n",
		    lsn->file, lsn->offset, errno, (int)hdrsz, rc);
		__os_closehandle(dbenv, fh);
		return 1;
	}
	if (LOG_SWAPPED())
		__log_hdrswap(&hdr, CRYPTO_ON(dbenv));

	if (hdr.len == 0) {
		/* lsn is invalid - eg: zeroed file */
		__db_err(dbenv, "invalid log record header at %u:%u\n",
		    lsn->file, lsn->offset);
		__os_closehandle(dbenv, fh);
		return 1;
	}

	u_int32_t len = hdr.len - hdrsz;

	rc = __os_malloc(dbenv, len, &logent);
	if (rc) {
		__db_err(dbenv, "__os_malloc(%d) rc %d %s\n", len, errno,
		    strerror(errno));
		__os_closehandle(dbenv, fh);
		return 1;
	}

	rc = read(fh->fd, logent, len);
	if (rc != len) {
		__db_err(dbenv,
		    "error reading log record at %u:%u errno:%d expect:%u got:%d\n",
		    lsn->file, lsn->offset, errno, len, rc);
		__os_closehandle(dbenv, fh);
		if (logent)
			__os_free(dbenv, logent);
		return 1;
	}
	__os_closehandle(dbenv, fh);

	if (__db_check_chksum(dbenv, dbenv->crypto_handle, hdr.chksum, logent,
		len, is_hmac)) {
		__db_err(dbenv, "checksum failed at %u:%u\n", lsn->file,
		    lsn->offset);
		if (logent)
			__os_free(dbenv, logent);
		return 1;
	}

	LOGCOPY_32(&type, logent);
	/* check that the checkpoint lsn is valid and readable */
	if (type == DB___txn_ckp && check_checkpoint) {
		__txn_ckp_args *ckp = NULL;

		rc = __txn_ckp_read(dbenv, logent, &ckp);
		if (rc) {
			logmsg(LOGMSG_ERROR, 
                "%s: %u:%u record was a checkpoint, but couldn't read it?\n",
			    __func__, lsn->file, lsn->offset);
			if (logent)
				__os_free(dbenv, logent);
			return 1;
		}

		rc = berkdb_verify_lsn_written_to_disk(dbenv, &ckp->ckp_lsn, 0);

		if (ckp)
			__os_free(dbenv, ckp);

		if (logent)
			__os_free(dbenv, logent);
		return rc;
	}

	if (logent)
		__os_free(dbenv, logent);
	return 0;
}

/*
 * __memp_pgwrite --
 *	Write a page to a file.
 */
static int
__memp_pgwrite(dbenv, dbmfp, hp, bhp)
	DB_ENV *dbenv;
	DB_MPOOLFILE *dbmfp;
	DB_MPOOL_HASH *hp;
	BH *bhp;
{
	return __memp_pgwrite_int(dbenv, dbmfp, hp, bhp, 1);
}

/*
 * __memp_pgwrite_int --
 *  Write a page to a file.  Setting wrrec = 1 also writes it to the recovery 
 *  page log.
 */
static int
__memp_pgwrite_int(dbenv, dbmfp, hp, bhp, wrrec)
	DB_ENV *dbenv;
	DB_MPOOLFILE *dbmfp;
	DB_MPOOL_HASH *hp;
	BH *bhp;
	int wrrec;
{
	return __memp_pgwrite_multi(dbenv, dbmfp, &hp, &bhp, 1, wrrec);
}


/*
 * __memp_pgwrite_multi --
 *  Write multiple pages to a file.  Setting wrrec = 1 also writes it to 
 * the recovery page log.
 */
static int
__memp_pgwrite_multi(dbenv, dbmfp, hps, bhps, numpages, wrrec)
	DB_ENV *dbenv;
	DB_MPOOLFILE *dbmfp;
	DB_MPOOL_HASH **hps;
	BH **bhps;
	int numpages, wrrec;
{
	DB_LSN tmplsn, maxlsn;
	MPOOLFILE *mfp;
	DB_MPOOL_HASH *hp;
	BH *bhp = NULL;
	u_int8_t **bparray;
	size_t nw;
	int *callpgin, *reclk;
	DB_MPOOL *dbmp;
	MPOOL *c_mp;
	u_int32_t n_cache;
	int ret, i, idx;

	mfp = dbmfp == NULL ? NULL : dbmfp->mfp;
	ret = 0;
	idx = -1;

	/* We should at least have one one buffer to write out. */
	DB_ASSERT(numpages >= 1 && bhps[0] != NULL && hps[0] != NULL);

	/*
	 * We should never be called with a clean or trash buffer.
	 * The sync code does call us with already locked buffers.
	 */
	DB_ASSERT(F_ISSET(bhps[0], BH_DIRTY));
	DB_ASSERT(!F_ISSET(bhps[0], BH_TRASH));

#ifdef DIAGNOSTIC
	/* Do the same checks for the rest of the buffers */
	for (i = 1; i < numpages; i++) {
		bhp = bhps[i];
		hp = hps[i];

		/* Make sure the buffes are contigious */
		DB_ASSERT(bhps[i]->pgno - 1 == bhps[i - 1]->pgno);

		DB_ASSERT(F_ISSET(bhp, BH_DIRTY));
		DB_ASSERT(!F_ISSET(bhp, BH_TRASH));

	}
#endif

	if ((ret = __os_calloc(dbenv, numpages, sizeof(int), &callpgin)) != 0)
		return (ret);
	if ((ret = __os_calloc(dbenv, numpages, sizeof(int), &reclk)) != 0) {
		__os_free(dbenv, callpgin);
		return (ret);
	}
	if ((ret = __os_malloc(dbenv, numpages * sizeof(u_int8_t *), &bparray))
	    != 0) {
		__os_free(dbenv, callpgin);
		__os_free(dbenv, reclk);
		return (ret);
	}

	for (i = 0; i < numpages; i++) {
		bhp = bhps[i];
		hp = hps[i];

		/*
		 * If we have not already traded the hash bucket lock
		 * for the buffer lock, do so now.
		 */
		if (!F_ISSET(bhp, BH_LOCKED)) {
			F_SET(bhp, BH_LOCKED);
			MUTEX_LOCK(dbenv, &bhp->mutex);
			MUTEX_UNLOCK(dbenv, &hp->hash_mutex);
		}
	}

	/*
	 * It's possible that the underlying file doesn't exist, either
	 * because of an outright removal or because it was a temporary
	 * file that's been closed.
	 *
	 * !!!
	 * Once we pass this point, we know that dbmfp and mfp aren't NULL,
	 * and that we have a valid file reference.
	 */
	if (mfp == NULL || mfp->deadfile)
		goto file_dead;

	/*
	 * If the page is in a file for which we have LSN information, we have
	 * to ensure the appropriate log records are on disk.
	 */
	if (LOGGING_ON(dbenv) && mfp->lsn_off != -1) {
		bhp = bhps[0];

		/* Some knowledge of LSN outside of the log module is ok. */
		if (!F_ISSET(bhp, BH_CALLPGIN))
			memcpy(&maxlsn, bhp->buf + mfp->lsn_off,
			    sizeof(DB_LSN));

		for (i = 1; i < numpages; i++) {
			bhp = bhps[i];
			if (F_ISSET(bhp, BH_CALLPGIN))
				continue;

			memcpy(&tmplsn,
			    bhp->buf + mfp->lsn_off, sizeof(DB_LSN));
			if (log_compare(&tmplsn, &maxlsn) > 0)
				memcpy(&maxlsn, &tmplsn, sizeof(DB_LSN));
		}

		if ((ret = __log_flush(dbenv, &maxlsn)) != 0)
			goto err;
	}
#ifdef DIAGNOSTIC
	/*
	 * Verify write-ahead logging semantics.
	 *
	 * !!!
	 * One special case.  There is a single field on the meta-data page,
	 * the last-page-number-in-the-file field, for which we do not log
	 * changes.  If the page was originally created in a database that
	 * didn't have logging turned on, we can see a page marked dirty but
	 * for which no corresponding log record has been written.  However,
	 * the only way that a page can be created for which there isn't a
	 * previous log record and valid LSN is when the page was created
	 * without logging turned on, and so we check for that special-case
	 * LSN value.
	 */
	if (LOGGING_ON(dbenv) && !IS_NOT_LOGGED_LSN(LSN(bhp->buf))) {
		/*
		 * There is a potential race here.  If we are in the midst of
		 * Switching log files, it's possible we could test against the
		 * old file and the new offset in the log region's LSN.  If we
		 * fail the first test, acquire the log mutex and check again.
		 */
		DB_LOG *dblp;
		DB_MUTEX *mtx;
		LOG *lp;

		dblp = dbenv->lg_handle;
		lp = dblp->reginfo.primary;
		if (log_compare(&lp->s_lsn, &LSN(bhp->buf)) <= 0) {
			mtx = R_ADDR(&dblp->reginfo, lp->flush_mutex_off);
			MUTEX_LOCK(dbenv, mtx);
			DB_ASSERT(log_compare(&lp->s_lsn, &LSN(bhp->buf)) > 0);
			MUTEX_UNLOCK(dbenv, mtx);
		}
	}
#endif

	if (gbl_verify_lsn_written && LOGGING_ON(dbenv)) {
		for (i = 0; i < numpages; i++) {
			bhp = bhps[i];
			if (!IS_NOT_LOGGED_LSN(LSN(bhp->buf)) &&
			    !(LSN(bhp->buf).file == 0 &&
				LSN(bhp->buf).offset == 0) &&
			    !(LSN(bhp->buf).file == 1 &&
				LSN(bhp->buf).offset == 0) &&
			    (bhp->pgno != 0)) {

				DB_LSN chklsn;

				memcpy(&chklsn, bhp->buf + mfp->lsn_off,
				    sizeof(DB_LSN));

				if (berkdb_verify_page_lsn_written_to_disk
				    (dbenv, &chklsn)) {
					__db_err(dbenv,
					    "Failed verify %u:%u for "
					    "\"%s\" page %d pgsize %d\n",
					    (unsigned)LSN(bhp->buf).file,
					    (unsigned)LSN(bhp->buf).offset,
					    dbmfp->fhp->name ? dbmfp->fhp->
					    name : "???", (int)bhp->pgno,
					    (int)dbmfp->fhp->pgsize);
					abort();
				}
			}
		}
	}


	/*
	 * Call any pgout function.  We set the callpgin flag so that we flag
	 * that the contents of the buffer will need to be passed through pgin
	 * before they are reused.
	 */
	for (i = 0; i < numpages; i++) {
		bhp = bhps[i];

		if (mfp->ftype != 0 && !F_ISSET(bhp, BH_CALLPGIN)) {
			callpgin[i] = 1;
			if ((ret = __memp_pg(dbmfp, bhp, 0)) != 0)
				goto err;
		}
	}

	/* Recovery-page logging.  */
	for (i = 0; i < numpages; i++) {
		bhp = bhps[i];

		if (wrrec && dbenv->mp_recovery_pages > 0) {
			/* Meta page is always at idx 0. */
			if (0 == bhp->pgno)
				idx = 0;

			/* Get the next recovery-page index. */
			else {
				Pthread_mutex_lock(&dbmfp->recp_idx_lk);
				idx = dbmfp->rec_idx;
				dbmfp->rec_idx = (dbmfp->rec_idx %
				    dbenv->mp_recovery_pages) + 1;
				Pthread_mutex_unlock(&dbmfp->recp_idx_lk);
			}

			/* Lock out other threads */
			Pthread_mutex_lock(&dbmfp->recp_lk_array[idx]);
			/* Hack in case we're writing out the meta page. */
			reclk[i] = idx + 1;

			ret = __os_io(dbenv, DB_IO_WRITE, dbmfp->recp,
			    idx, mfp->stat.st_pagesize, bhp->buf, &nw);
			if (ret != 0) {
				__db_err(dbenv,
				    "%s: write failed for "
				    "recovery page %lu to %lu",
				    __memp_fn(dbmfp), (u_long) bhp->pgno,
				    (u_long) idx);
				goto err;
			}
		}
	}

	/* If bad-write testing is enabled, do a short-write then abort. */
	if (!IS_RECOVERING(dbenv) && gbl_test_badwrite_intvl > 0 &&
	    !(rand() % gbl_test_badwrite_intvl)) {
		for (i = 0; i < numpages; i++) {
			bhp = bhps[i];
			if (bhp->pgno == 0)
				continue;


			int ln = rand() % (mfp->stat.st_pagesize - 1);

			logmsg(LOGMSG_USER, "Writing a bad page for page %d "
			    "(recovery idx %d) and exiting.\n", bhp->pgno, idx);

			__os_io_partial(dbenv, DB_IO_WRITE, dbmfp->fhp,
			    bhp->pgno, mfp->stat.st_pagesize, ln,
			    bhp->buf, &nw);
			sleep(1);
			_exit(1);
		}
	}




	/* Get page offsets. */
	for (i = 0; i < numpages; i++) {
		bhp = bhps[i];
		bparray[i] = bhp->buf;
	}

	/* Write the page. */
	if ((ret = __os_iov(dbenv, DB_IO_WRITE, dbmfp->fhp,
		    bhps[0]->pgno, mfp->stat.st_pagesize,
		    bparray, numpages, &nw)) != 0) {
		__db_err(dbenv, "%s: writev failed for page %lu",
		    __memp_fn(dbmfp), (u_long) bhp->pgno);
		goto err;
	}


	/* Fsync datafiles before reusing indexes. */
	if (0 == idx || 1 == idx)
		__os_fsync(dbenv, dbmfp->fhp);

	mfp->file_written = 1;
	mfp->stat.st_page_out += numpages;
	mfp->stat.st_rw_merges += numpages - 1;

err:
file_dead:
	/* Unlock the recovery-lock. */
	for (i = 0; reclk[i] && i < numpages; i++)
		Pthread_mutex_unlock(&dbmfp->recp_lk_array[reclk[i] - 1]);

	/*
	 * !!!
	 * Once we pass this point, dbmfp and mfp may be NULL, we may not have
	 * a valid file reference.
	 *
	 * Unlock the buffer and reacquire the hash lock.
	 */
	for (i = 0; i < numpages; i++) {
		bhp = bhps[i];
		hp = hps[i];

		MUTEX_UNLOCK(dbenv, &bhp->mutex);
		MUTEX_LOCK(dbenv, &hp->hash_mutex);

		/*
		 * If we rewrote the page, it will need processing by the pgin
		 * routine before reuse.
		 */
		if (callpgin[i])
			F_SET(bhp, BH_CALLPGIN);
	}

	/*
	 * Update the hash bucket statistics, reset the flags.
	 * If we were successful, the page is no longer dirty.
	 */
	if (ret == 0) {
		dbmp = dbenv->mp_handle;
		for (i = 0; i < numpages; i++) {
			bhp = bhps[i];
			hp = hps[i];

			DB_ASSERT(hp->hash_page_dirty != 0);

			/* Best to do this in lock step with hash_page_dirty */
			n_cache = NCACHE(dbmp->reginfo[0].primary,
			    bhp->mf_offset, bhp->pgno);
			c_mp = dbmp->reginfo[n_cache].primary;
			ATOMIC_ADD32(hp->hash_page_dirty, -1);
			ATOMIC_ADD32(c_mp->stat.st_page_dirty, -1);

			if (dbenv->tx_perfect_ckp) {
				/* Clear first_dirty_lsn. */
				MAX_LSN(bhp->first_dirty_tx_begin_lsn);
			}

			F_CLR(bhp, BH_DIRTY | BH_DIRTY_CREATE);
		}


	}

	/* Regardless, clear any sync wait-for count and remove our lock. */
	for (i = 0; i < numpages; i++) {
		bhp = bhps[i];

		bhp->ref_sync = 0;
		F_CLR(bhp, BH_LOCKED);
	}

	__os_free(dbenv, callpgin);
	__os_free(dbenv, reclk);
	__os_free(dbenv, bparray);

	return (ret);
}

/*
 * Bloomberg hack - record a hit to memp_fget, with the time taken
 */
static void
bb_memp_pg_hit(uint64_t start_time_us)
{
	uint64_t time_diff = bb_berkdb_fasttime() - start_time_us;
	struct berkdb_thread_stats *stats;

	stats = bb_berkdb_get_thread_stats();
	stats->n_memp_pgs++;
	stats->memp_pg_time_us += time_diff;

	stats = bb_berkdb_get_process_stats();
	stats->n_memp_pgs++;
	stats->memp_pg_time_us += time_diff;
}


/*
 * __dir_pg --
 *	Call the pgin/pgout routine.
 *
 * PUBLIC: int __dir_pg __P((DB_MPOOLFILE *, db_pgno_t, u_int8_t *, int));
 */
int
__dir_pg(dbmfp, pgno, buf, is_pgin)
	DB_MPOOLFILE *dbmfp;
	db_pgno_t pgno;
	u_int8_t *buf;
	int is_pgin;
{
	DBT dbt, *dbtp;
	DB_ENV *dbenv;
	DB_MPOOL *dbmp;
	DB_MPREG *mpreg;
	MPOOLFILE *mfp;
	int ftype, ret;

	uint64_t start_time_us = 0;

	dbenv = dbmfp->dbenv;
	dbmp = dbenv->mp_handle;
	mfp = dbmfp->mfp;

	if (gbl_bb_berkdb_enable_memp_pg_timing)
		start_time_us = bb_berkdb_fasttime();

	MUTEX_THREAD_LOCK(dbenv, dbmp->mutexp);

	ftype = mfp->ftype;
	for (mpreg = LIST_FIRST(&dbmp->dbregq);
	    mpreg != NULL; mpreg = LIST_NEXT(mpreg, q)) {
		if (ftype != mpreg->ftype)
			continue;
		if (mfp->pgcookie_len == 0)
			dbtp = NULL;
		else {
			dbt.size = mfp->pgcookie_len;
			dbt.data = R_ADDR(dbmp->reginfo, mfp->pgcookie_off);
			dbtp = &dbt;
		}
		MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);

		if (is_pgin) {
			if (mpreg->pgin != NULL &&
			    (ret = mpreg->pgin(dbenv, pgno, buf, dbtp)) != 0)
				goto err;
		} else
		    if (mpreg->pgout != NULL &&
		    (ret = mpreg->pgout(dbenv, pgno, buf, dbtp)) != 0)
			goto err;
		break;
	}

	if (mpreg == NULL)
		MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);

	if (gbl_bb_berkdb_enable_memp_pg_timing)
		bb_memp_pg_hit(start_time_us);
	return (0);

err:	MUTEX_THREAD_UNLOCK(dbenv, dbmp->mutexp);
	__db_err(dbenv, "%s: %s failed for page %lu",
	    __memp_fn(dbmfp), is_pgin ? "pgin" : "pgout", (u_long) pgno);
	if (gbl_bb_berkdb_enable_memp_pg_timing)
		bb_memp_pg_hit(start_time_us);
	return (ret);
}




/*
 * __memp_pg --
 *	Call the pgin/pgout routine.
 *
 * PUBLIC: int __memp_pg __P((DB_MPOOLFILE *, BH *, int));
 */
int
__memp_pg(dbmfp, bhp, is_pgin)
	DB_MPOOLFILE *dbmfp;
	BH *bhp;
	int is_pgin;
{
	return __dir_pg(dbmfp, bhp->pgno, bhp->buf, is_pgin);
}

/*
 * __memp_bhfree --
 *	Free a bucket header and its referenced data.
 *
 * PUBLIC: void __memp_bhfree __P((DB_MPOOL *, DB_MPOOL_HASH *, BH *, int));
 */
void
__memp_bhfree(dbmp, hp, bhp, free_mem)
	DB_MPOOL *dbmp;
	DB_MPOOL_HASH *hp;
	BH *bhp;
	int free_mem;
{
	DB_ENV *dbenv;
	MPOOL *c_mp, *mp;
	MPOOLFILE *mfp;
	u_int32_t n_cache;

	/*
	 * Assumes the hash bucket is locked and the MPOOL is not.
	 */
	dbenv = dbmp->dbenv;
	mp = dbmp->reginfo[0].primary;
	n_cache = NCACHE(mp, bhp->mf_offset, bhp->pgno);

	/*
	 * Delete the buffer header from the hash bucket queue and reset
	 * the hash bucket's priority, if necessary.
	 */
	SH_TAILQ_REMOVE(&hp->hash_bucket, bhp, hq, __bh);
	if (bhp->priority == hp->hash_priority)
		hp->hash_priority =
		    SH_TAILQ_FIRST(&hp->hash_bucket, __bh) == NULL ?
		    0 : SH_TAILQ_FIRST(&hp->hash_bucket, __bh)->priority;

	/*
	 * Discard the hash bucket's mutex, it's no longer needed, and
	 * we don't want to be holding it when acquiring other locks.
	 */
	MUTEX_UNLOCK(dbenv, &hp->hash_mutex);

	/*
	 * Find the underlying MPOOLFILE and decrement its reference count.
	 * If this is its last reference, remove it.
	 */
	mfp = R_ADDR(dbmp->reginfo, bhp->mf_offset);
	MUTEX_LOCK(dbenv, &mfp->mutex);
	if (--mfp->block_cnt == 0 && mfp->mpf_cnt == 0)
		(void)__memp_mf_discard(dbmp, mfp);
	else
		MUTEX_UNLOCK(dbenv, &mfp->mutex);

	R_LOCK(dbenv, &dbmp->reginfo[n_cache]);

	/*
	 * Clear the mutex this buffer recorded; requires the region lock
	 * be held.
	 */
	__db_shlocks_clear(&bhp->mutex, &dbmp->reginfo[n_cache],
	    (REGMAINT *)R_ADDR(&dbmp->reginfo[n_cache], mp->maint_off));

	/*
	 * If we're not reusing the buffer immediately, free the buffer header
	 * and data for real.
	 */
	if (free_mem) {
		__db_shalloc_free(dbmp->reginfo[n_cache].addr, bhp);
		c_mp = dbmp->reginfo[n_cache].primary;
		c_mp->stat.st_pages--;
	}
	R_UNLOCK(dbenv, &dbmp->reginfo[n_cache]);
}
