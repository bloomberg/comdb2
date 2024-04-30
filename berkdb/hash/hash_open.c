/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
/*
 * Copyright (c) 1990, 1993, 1994
 *	Margo Seltzer.  All rights reserved.
 */
/*
 * Copyright (c) 1990, 1993, 1994
 *	The Regents of the University of California.  All rights reserved.
 *
 * This code is derived from software contributed to Berkeley by
 * Margo Seltzer.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: hash_open.c,v 11.185 2003/07/17 01:39:17 margo Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <stdlib.h>
#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/hash.h"
#include "dbinc/log.h"
#include "dbinc/db_shash.h"
#include "dbinc/lock.h"
#include "dbinc/mp.h"
#include "dbinc/db_swap.h"
#include "dbinc/btree.h"
#include "dbinc/fop.h"

static db_pgno_t __ham_init_meta __P((DB *, HMETA *, db_pgno_t, DB_LSN *));

/*
 * __ham_open --
 *
 * PUBLIC: int __ham_open __P((DB *,
 * PUBLIC:     DB_TXN *, const char * name, db_pgno_t, u_int32_t));
 */
int
__ham_open(dbp, txn, name, base_pgno, flags)
	DB *dbp;
	DB_TXN *txn;
	const char *name;
	db_pgno_t base_pgno;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	HASH_CURSOR *hcp;
	HASH *hashp;
	int ret, t_ret;

	dbenv = dbp->dbenv;
	dbc = NULL;
	mpf = dbp->mpf;

	/*
	 * Get a cursor.  If DB_CREATE is specified, we may be creating
	 * pages, and to do that safely in CDB we need a write cursor.
	 * In STD_LOCKING mode, we'll synchronize using the meta page
	 * lock instead.
	 */
	if ((ret = __db_cursor(dbp,
	    txn, &dbc, LF_ISSET(DB_CREATE) && CDB_LOCKING(dbenv) ?
	    DB_WRITECURSOR : 0)) != 0)
		return (ret);

	hcp = (HASH_CURSOR *)dbc->internal;
	hashp = dbp->h_internal;
	hashp->meta_pgno = base_pgno;
	if ((ret = __ham_get_meta(dbc)) != 0)
		goto err1;

	/* Initialize the hdr structure.  */
	if (hcp->hdr->dbmeta.magic == DB_HASHMAGIC) {
		/* File exists, verify the data in the header. */
		if (hashp->h_hash == NULL)
			hashp->h_hash = hcp->hdr->dbmeta.version < 5
			? __ham_func4 : __ham_func5;
		if (!F_ISSET(dbp, DB_AM_RDONLY) && !IS_RECOVERING(dbenv) &&
		    hashp->h_hash(dbp,
		    CHARKEY, sizeof(CHARKEY)) != hcp->hdr->h_charkey) {
			__db_err(dbp->dbenv,
			    "hash: incompatible hash function");
			ret = EINVAL;
			goto err2;
		}
		if (F_ISSET(&hcp->hdr->dbmeta, DB_HASH_DUP))
			F_SET(dbp, DB_AM_DUP);
		if (F_ISSET(&hcp->hdr->dbmeta, DB_HASH_DUPSORT))
			F_SET(dbp, DB_AM_DUPSORT);
		if (F_ISSET(&hcp->hdr->dbmeta, DB_HASH_SUBDB))
			F_SET(dbp, DB_AM_SUBDB);

		/*
		 * We must initialize last_pgno, it could be stale.
		 * We update this without holding the meta page write
		 * locked.  This is ok since two threads in the code
		 * must be setting it to the same value.  SR #7159.
		 */
		if (!F_ISSET(dbp, DB_AM_RDONLY) &&
		    dbp->meta_pgno == PGNO_BASE_MD) {
			__memp_last_pgno(mpf, &hcp->hdr->dbmeta.last_pgno);
			F_SET(hcp, H_DIRTY);
		}
	} else if (!IS_RECOVERING(dbenv) && !F_ISSET(dbp, DB_AM_RECOVER)) {
		__db_err(dbp->dbenv,
		    "%s: Invalid hash meta page %d", name, base_pgno);
		ret = EINVAL;
	}

err2:	/* Release the meta data page */
	if ((t_ret = __ham_release_meta(dbc)) != 0 && ret == 0)
		ret = t_ret;
err1:	if ((t_ret  = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}

/*
 * __ham_metachk --
 *
 * PUBLIC: int __ham_metachk __P((DB *, const char *, HMETA *));
 */
int
__ham_metachk(dbp, name, hashm)
	DB *dbp;
	const char *name;
	HMETA *hashm;
{
	DB_ENV *dbenv;
	u_int32_t vers;
	int ret;

	dbenv = dbp->dbenv;

	/*
	 * At this point, all we know is that the magic number is for a Hash.
	 * Check the version, the database may be out of date.
	 */
	vers = hashm->dbmeta.version;
	if (F_ISSET(dbp, DB_AM_SWAP))
		M_32_SWAP(vers);
	switch (vers) {
	case 4:
	case 5:
	case 6:
		__db_err(dbenv,
		    "%s: hash version %lu requires a version upgrade",
		    name, (u_long)vers);
		return (DB_OLD_VERSION);
	case 7:
	case 8:
		break;
	default:
		__db_err(dbenv,
		    "%s: unsupported hash version: %lu", name, (u_long)vers);
		return (EINVAL);
	}

	/* Swap the page if we need to. */
	if (F_ISSET(dbp, DB_AM_SWAP) && (ret = __ham_mswap((PAGE *)hashm)) != 0)
		return (ret);

	/* Check the type. */
	if (dbp->type != DB_HASH && dbp->type != DB_UNKNOWN)
		return (EINVAL);
	dbp->type = DB_HASH;
	DB_ILLEGAL_METHOD(dbp, DB_OK_HASH);

	/*
	 * Check application info against metadata info, and set info, flags,
	 * and type based on metadata info.
	 */
	if ((ret = __db_fchk(dbenv,
	    "DB->open", hashm->dbmeta.flags,
	    DB_HASH_DUP | DB_HASH_SUBDB | DB_HASH_DUPSORT)) != 0)
		return (ret);

	if (F_ISSET(&hashm->dbmeta, DB_HASH_DUP))
		F_SET(dbp, DB_AM_DUP);
	else
		if (F_ISSET(dbp, DB_AM_DUP)) {
			__db_err(dbenv,
		"%s: DB_DUP specified to open method but not set in database",
			    name);
			return (EINVAL);
		}

	if (F_ISSET(&hashm->dbmeta, DB_HASH_SUBDB))
		F_SET(dbp, DB_AM_SUBDB);
	else
		if (F_ISSET(dbp, DB_AM_SUBDB)) {
			__db_err(dbenv,
	    "%s: multiple databases specified but not supported in file",
			name);
			return (EINVAL);
		}

	if (F_ISSET(&hashm->dbmeta, DB_HASH_DUPSORT)) {
		if (dbp->dup_compare == NULL)
			dbp->dup_compare = __bam_defcmp;
	} else
		if (dbp->dup_compare != NULL) {
			__db_err(dbenv,
		"%s: duplicate sort function specified but not set in database",
			    name);
			return (EINVAL);
		}

	/* Set the page size. */
	dbp->pgsize = hashm->dbmeta.pagesize;

	/* Copy the file's ID. */
	memcpy(dbp->fileid, hashm->dbmeta.uid, DB_FILE_ID_LEN);

	return (0);
}

/*
 * __ham_init_meta --
 *
 * Initialize a hash meta-data page.  We assume that the meta-data page is
 * contiguous with the initial buckets that we create.  If that turns out
 * to be false, we'll fix it up later.  Return the initial number of buckets
 * allocated.
 */
static db_pgno_t
__ham_init_meta(dbp, meta, pgno, lsnp)
	DB *dbp;
	HMETA *meta;
	db_pgno_t pgno;
	DB_LSN *lsnp;
{
	HASH *hashp;
	db_pgno_t nbuckets;
	int i;
	int32_t l2;

	hashp = dbp->h_internal;
	if (hashp->h_hash == NULL)
		hashp->h_hash = DB_HASHVERSION < 5 ? __ham_func4 : __ham_func5;

	if (hashp->h_nelem != 0 && hashp->h_ffactor != 0) {
		hashp->h_nelem = (hashp->h_nelem - 1) / hashp->h_ffactor + 1;
		l2 = __db_log2(hashp->h_nelem > 2 ? hashp->h_nelem : 2);
	} else
		l2 = 1;
	nbuckets = (db_pgno_t)(1 << l2);

	memset(meta, 0, sizeof(HMETA));
	meta->dbmeta.lsn = *lsnp;
	meta->dbmeta.pgno = pgno;
	meta->dbmeta.magic = DB_HASHMAGIC;
	meta->dbmeta.version = DB_HASHVERSION;
	meta->dbmeta.pagesize = dbp->pgsize;
	if (F_ISSET(dbp, DB_AM_CHKSUM))
		FLD_SET(meta->dbmeta.metaflags, DBMETA_CHKSUM);
	if (F_ISSET(dbp, DB_AM_ENCRYPT)) {
		meta->dbmeta.encrypt_alg =
		   ((DB_CIPHER *)dbp->dbenv->crypto_handle)->alg;
		DB_ASSERT(meta->dbmeta.encrypt_alg != 0);
		meta->crypto_magic = meta->dbmeta.magic;
	}
	PGTYPE(&meta->dbmeta) = P_HASHMETA;
	meta->dbmeta.free = PGNO_INVALID;
	meta->dbmeta.last_pgno = pgno;
	meta->max_bucket = nbuckets - 1;
	meta->high_mask = nbuckets - 1;
	meta->low_mask = (nbuckets >> 1) - 1;
	meta->ffactor = hashp->h_ffactor;
	meta->h_charkey = hashp->h_hash(dbp, CHARKEY, sizeof(CHARKEY));
	memcpy(meta->dbmeta.uid, dbp->fileid, DB_FILE_ID_LEN);

	if (F_ISSET(dbp, DB_AM_DUP))
		F_SET(&meta->dbmeta, DB_HASH_DUP);
	if (F_ISSET(dbp, DB_AM_SUBDB))
		F_SET(&meta->dbmeta, DB_HASH_SUBDB);
	if (dbp->dup_compare != NULL)
		F_SET(&meta->dbmeta, DB_HASH_DUPSORT);

	/*
	 * Create the first and second buckets pages so that we have the
	 * page numbers for them and we can store that page number in the
	 * meta-data header (spares[0]).
	 */
	meta->spares[0] = pgno + 1;

	/* Fill in the last fields of the meta data page. */
	for (i = 1; i <= l2; i++)
		meta->spares[i] = meta->spares[0];
	for (; i < NCACHED; i++)
		meta->spares[i] = PGNO_INVALID;

	return (nbuckets);
}

/*
 * __ham_new_file --
 *	Create the necessary pages to begin a new database file.  If name
 * is NULL, then this is an unnamed file, the mpf has been set in the dbp
 * and we simply create the pages using mpool.  In this case, we don't log
 * because we never have to redo an unnamed create and the undo simply
 * frees resources.
 *
 * This code appears more complex than it is because of the two cases (named
 * and unnamed).  The way to read the code is that for each page being created,
 * there are three parts: 1) a "get page" chunk (which either uses malloc'd
 * memory or calls __memp_fget), 2) the initialization, and 3) the "put page"
 * chunk which either does a fop write or an __memp_fput.
 *
 * PUBLIC: int __ham_new_file __P((DB *, DB_TXN *, DB_FH *, const char *));
 */
int
__ham_new_file(dbp, txn, fhp, name)
	DB *dbp;
	DB_TXN *txn;
	DB_FH *fhp;
	const char *name;
{
	DB_ENV *dbenv;
	DB_LSN lsn;
	DB_MPOOLFILE *mpf;
	DB_PGINFO pginfo;
	DBT pdbt;
	HMETA *meta;
	PAGE *page;
	int ret;
	db_pgno_t lpgno;
	void *buf;

	dbenv = dbp->dbenv;
	mpf = dbp->mpf;
	meta = NULL;
	page = NULL;
	memset(&pdbt, 0, sizeof(pdbt));

	/* Build meta-data page. */
	if (name == NULL) {
		lpgno = PGNO_BASE_MD;
		ret = __memp_fget(mpf, &lpgno, DB_MPOOL_CREATE, &meta);
	} else {
		pginfo.db_pagesize = dbp->pgsize;
		pginfo.type = dbp->type;
		pginfo.flags =
		    F_ISSET(dbp, (DB_AM_CHKSUM | DB_AM_ENCRYPT | DB_AM_SWAP));
		pdbt.data = &pginfo;
		pdbt.size = sizeof(pginfo);
		ret = __os_calloc(dbp->dbenv, 1, dbp->pgsize, &buf);
		meta = (HMETA *)buf;
	}
	if (ret != 0)
		return (ret);

	LSN_NOT_LOGGED(lsn);
	lpgno = __ham_init_meta(dbp, meta, PGNO_BASE_MD, &lsn);
	meta->dbmeta.last_pgno = lpgno;

	if (name == NULL)
		ret = __memp_fput(mpf, meta, DB_MPOOL_DIRTY);
	else {
		if ((ret = __db_pgout(dbenv, PGNO_BASE_MD, meta, &pdbt)) != 0)
			goto err;
		ret = __fop_write(dbenv, txn, name,
		    DB_APP_DATA, fhp, dbp->pgsize, 0, 0, buf, dbp->pgsize, 1,
		    F_ISSET(dbp, DB_AM_NOT_DURABLE) ? DB_LOG_NOT_DURABLE : 0);
	}
	if (ret != 0)
		goto err;
	meta = NULL;

	/* Now allocate the final hash bucket. */
	if (name == NULL) {
		if ((ret =
		    __memp_fget(mpf, &lpgno, DB_MPOOL_CREATE, &page)) != 0)
			goto err;
	} else {
#ifdef DIAGNOSTIC
		memset(buf, dbp->pgsize, 0);
#endif
		page = (PAGE *)buf;
	}

	P_INIT(page, dbp->pgsize, lpgno, PGNO_INVALID, PGNO_INVALID, 0, P_HASH);
	LSN_NOT_LOGGED(page->lsn);

	if (name == NULL)
		ret = __memp_fput(mpf, page, DB_MPOOL_DIRTY);
	else {
		if ((ret = __db_pgout(dbenv, lpgno, buf, &pdbt)) != 0)
			goto err;
		ret = __fop_write(dbenv, txn, name, DB_APP_DATA,
		    fhp, dbp->pgsize, lpgno, 0, buf, dbp->pgsize, 1,
		    F_ISSET(dbp, DB_AM_NOT_DURABLE) ? DB_LOG_NOT_DURABLE : 0);
	}
	if (ret != 0)
		goto err;
	page = NULL;

err:	if (name != NULL)
		__os_free(dbenv, buf);
	else {
		if (meta != NULL)
			(void)__memp_fput(mpf, meta, 0);
		if (page != NULL)
			(void)__memp_fput(mpf, page, 0);
	}
	return (ret);
}

/*
 * __ham_new_subdb --
 *	Create the necessary pages to begin a new subdatabase.
 *
 * PUBLIC: int __ham_new_subdb __P((DB *, DB *, DB_TXN *));
 */
int
__ham_new_subdb(mdbp, dbp, txn)
	DB *mdbp, *dbp;
	DB_TXN *txn;
{
	DBC *dbc;
	DB_ENV *dbenv;
	DB_LOCK metalock, mmlock;
	DB_LSN lsn;
	DB_MPOOLFILE *mpf;
	DBMETA *mmeta;
	HMETA *meta;
	PAGE *h;
	int i, ret, t_ret;
	db_pgno_t lpgno, mpgno;

	dbenv = mdbp->dbenv;
	mpf = mdbp->mpf;
	dbc = NULL;
	meta = NULL;
	mmeta = NULL;
	LOCK_INIT(metalock);
	LOCK_INIT(mmlock);

	if ((ret = __db_cursor(mdbp, txn,
	    &dbc, CDB_LOCKING(dbenv) ?  DB_WRITECURSOR : 0)) != 0)
		return (ret);

	/* Get and lock the new meta data page. */
	if ((ret = __db_lget(dbc,
	    0, dbp->meta_pgno, DB_LOCK_WRITE, 0, &metalock)) != 0)
		goto err;
	if ((ret =
	    PAGEGET(dbc, mpf, &dbp->meta_pgno, DB_MPOOL_CREATE, &meta)) != 0)
		goto err;

	/* Initialize the new meta-data page. */
	lsn = meta->dbmeta.lsn;
	lpgno = __ham_init_meta(dbp, meta, dbp->meta_pgno, &lsn);

	/*
	 * We are about to allocate a set of contiguous buckets (lpgno
	 * worth).  We need to get the master meta-data page to figure
	 * out where these pages are and to allocate them.  So, lock and
	 * get the master meta data page.
	 */
	mpgno = PGNO_BASE_MD;
	if ((ret = __db_lget(dbc, 0, mpgno, DB_LOCK_WRITE, 0, &mmlock)) != 0)
		goto err;
	if ((ret = PAGEGET(dbc, mpf, &mpgno, 0, &mmeta)) != 0)
		goto err;

	/*
	 * Now update the hash meta-data page to reflect where the first
	 * set of buckets are actually located.
	 */
	meta->spares[0] = mmeta->last_pgno + 1;
	for (i = 0; i < NCACHED && meta->spares[i] != PGNO_INVALID; i++)
		meta->spares[i] = meta->spares[0];

	/* The new meta data page is now complete; log it. */
	if ((ret = __db_log_page(mdbp,
	    txn, &meta->dbmeta.lsn, dbp->meta_pgno, (PAGE *)meta)) != 0)
		goto err;

	/* Reflect the group allocation. */
	if (DBENV_LOGGING(dbenv))
		if ((ret = __ham_groupalloc_log(mdbp, txn,
		    &LSN(mmeta), 0, &LSN(mmeta),
		    meta->spares[0], meta->max_bucket + 1, mmeta->free)) != 0)
			goto err;

	/* Release the new meta-data page. */
	if ((ret = PAGEPUT(dbc, mpf, meta, DB_MPOOL_DIRTY)) != 0)
		goto err;
	meta = NULL;

	lpgno += mmeta->last_pgno;

	/* Now allocate the final hash bucket. */
	if ((ret = PAGEGET(dbc, mpf, &lpgno, DB_MPOOL_CREATE, &h)) != 0)
		goto err;

	mmeta->last_pgno = lpgno;
	P_INIT(h, dbp->pgsize, lpgno, PGNO_INVALID, PGNO_INVALID, 0, P_HASH);
	LSN(h) = LSN(mmeta);
	if ((ret = PAGEPUT(dbc, mpf, h, DB_MPOOL_DIRTY)) != 0)
		goto err;

	/* Now put the master-metadata page back. */
	if ((ret = PAGEPUT(dbc, mpf, mmeta, DB_MPOOL_DIRTY)) != 0)
		goto err;
	mmeta = NULL;

err:
	if (mmeta != NULL)
		if ((t_ret = PAGEPUT(dbc, mpf, mmeta, 0)) != 0 && ret == 0)
			ret = t_ret;
	if (LOCK_ISSET(mmlock))
		if ((t_ret = __LPUT(dbc, mmlock)) != 0 && ret == 0)
			ret = t_ret;
	if (meta != NULL)
		if ((t_ret = PAGEPUT(dbc, mpf, meta, 0)) != 0 && ret == 0)
			ret = t_ret;
	if (LOCK_ISSET(metalock))
		if ((t_ret = __LPUT(dbc, metalock)) != 0 && ret == 0)
			ret = t_ret;
	if (dbc != NULL)
		if ((t_ret = __db_c_close(dbc)) != 0 && ret == 0)
			ret = t_ret;
	return (ret);
}
