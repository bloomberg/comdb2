/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
/*
 * Copyright (c) 1990, 1993, 1994, 1995, 1996
 *	Keith Bostic.  All rights reserved.
 */
/*
 * Copyright (c) 1990, 1993, 1994, 1995
 *	The Regents of the University of California.  All rights reserved.
 *
 * This code is derived from software contributed to Berkeley by
 * Mike Olson.
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
static const char revid[] = "$Id: db_meta.c,v 11.77 2003/09/09 16:42:06 ubell Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/lock.h"
#include "dbinc/mp.h"
#include "dbinc/db_am.h"
#include "dbinc/db_swap.h"
#include "dbinc/log.h"
#include "dbinc/txn.h"

#include <unistd.h>
#include <stdlib.h>
#include <logmsg.h>

#if defined (UFID_HASH_DEBUG)
void comdb2_cheapstack_sym(FILE *f, char *fmt, ...);
#endif

/* definition in malloc.h clashes with dlmalloc */
extern void *memalign(size_t boundary, size_t size);

static void __db_init_meta __P((DB *, void *, db_pgno_t, u_int32_t));

/*
 * __db_init_meta --
 *	Helper function for __db_new that initializes the important fields in
 * a meta-data page (used instead of P_INIT).  We need to make sure that we
 * retain the page number and LSN of the existing page.
 */
static void
__db_init_meta(dbp, p, pgno, pgtype)
	DB *dbp;
	void *p;
	db_pgno_t pgno;
	u_int32_t pgtype;
{
	DB_LSN save_lsn;
	DBMETA *meta;

	meta = (DBMETA *)p;
	save_lsn = meta->lsn;
	memset(meta, 0, sizeof(DBMETA));
	meta->lsn = save_lsn;
	meta->pagesize = dbp->pgsize;
	if (F_ISSET(dbp, DB_AM_CHKSUM))
		FLD_SET(meta->metaflags, DBMETA_CHKSUM);
	meta->pgno = pgno;
	PGTYPE(meta) = (u_int8_t)pgtype;
}

int gbl_core_on_sparse_file = 0;
int gbl_check_sparse_files = 0;

#if defined (UFID_HASH_DEBUG)
#define CHECK_ALLOC_PAGE_LSN(x) do { \
	if (x.file == 0 && x.offset == 1) { \
		logmsg(LOGMSG_USER, "Invalid page lsn for pgalloc\n"); \
		__log_flush(dbc->dbp->dbenv, NULL); \
		abort(); \
	} \
} while(0);
#else
#define CHECK_ALLOC_PAGE_LSN(x) 
#endif

/* We already have the metapage locked. */
static int
__db_new_from_freelist(DBC *dbc, DBMETA *meta, u_int32_t type, PAGE **pagepp)
{
	DB *dbp;
	DB_LSN lsn;
	DB_MPOOLFILE *mpf;
	PAGE *h;
	db_pgno_t last, pgno, newnext;
	int ret;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	h = NULL;

	newnext = PGNO_INVALID;

	pgno = PGNO_BASE_MD;
	last = meta->last_pgno;
	if (meta->free == PGNO_INVALID) {
		__db_err(dbc->dbp->dbenv,
		    "__db_new_from_freelist called with no pages on freelist\n");
		ret = EINVAL;

		goto err;
	}

	pgno = meta->free;
	if ((ret = PAGEGET(dbc, mpf, &pgno, 0, &h)) != 0)
		goto err;

	/*
	 * We want to take the first page off the free list and
	 * then set meta->free to the that page's next_pgno, but
	 * we need to log the change first.
	 */
	newnext = h->next_pgno;
	lsn = h->lsn;

	/*
	 * Log the allocation before fetching the new page.  If we
	 * don't have room in the log then we don't want to tell
	 * mpool to extend the file.
	 */
	if (DBC_LOGGING(dbc)) {
		CHECK_ALLOC_PAGE_LSN(lsn);
		if ((ret = __db_pg_alloc_log(dbp, dbc->txn, &LSN(meta), 0,
		    &LSN(meta), PGNO_BASE_MD, &lsn, pgno,
		    (u_int32_t)type, newnext)) != 0)
			goto err;
	} else
		LSN_NOT_LOGGED(LSN(meta));

	meta->free = newnext;

	LSN(h) = LSN(meta);

	DB_ASSERT(TYPE(h) == P_INVALID);

	if (TYPE(h) != P_INVALID)
		return (__db_panic(dbp->dbenv, EINVAL));

	switch (type) {
	case P_BTREEMETA:
	case P_HASHMETA:
	case P_QAMMETA:
		__db_init_meta(dbp, h, h->pgno, type);
		break;
	default:
		P_INIT(h, dbp->pgsize,
		    h->pgno, PGNO_INVALID, PGNO_INVALID, 0, type);
		break;
	}

	*pagepp = h;
	return (0);

err:
	if (h != NULL)
		PAGEPUT(dbc, mpf, h, 0);
	return (ret);
}

int
__db_next_freepage(DB *dbp, db_pgno_t * pg)
{
	PAGE *h;
	int rc;
	DB_MPOOLFILE *mpf;

	mpf = dbp->mpf;

	rc = __memp_fget(mpf, pg, 0, &h);
	if (rc)
		return rc;

	if (TYPE(h) != P_INVALID) {
		fprintf(stderr,
		    "encountered non-invalid page %u (type %d) while reading free list\n",
		    *pg, PGTYPE(h));
		return EINVAL;
	}

	*pg = NEXT_PGNO(h);

	rc = __memp_fput(mpf, h, 0);
	return rc;
}


int
__db_dump_freelist(DB *dbp, db_pgno_t first)
{
	db_pgno_t pg;
	int rc = 0;
	int i;

	printf("freelist {\n");
	for (pg = first, i = 0; pg != PGNO_INVALID;
	    rc = __db_next_freepage(dbp, &pg), i++) {
		if (rc) {
			fprintf(stderr, "rc %d getting freelist page %u\n", rc,
			    pg);
			break;
		}
		printf("%u ", pg);
		if (i && i % 16 == 0)
			printf("\n");
	}
	printf("\n}\n");

	return rc;
}

int
__db_dump_freepages(DB *dbp, FILE *out)
{
    db_pgno_t pg = 0;
    DBMETA *meta = NULL;
    DB_MPOOLFILE *mpf;
    int rc = 0;
    int lastcr = 0;
    int i = 0;

    mpf = dbp->mpf;
    rc = __memp_fget(mpf, &pg, 0, &meta);
    if (rc) {
        fprintf(stderr, "Error getting metapage: %d\n", rc);
        return rc;
    }

    pg = (meta->free);
    __memp_fput(mpf, meta, 0);

    if (pg == PGNO_INVALID) {
        fprintf(out, "(EMPTY)\n");
        return 0;
    }

    fprintf(out, "freelist {\n");

    while(pg != PGNO_INVALID) {
        fprintf(out, "%6u ", pg);
        i++;
        if (i % 8 == 0) {
            printf("\n");
            lastcr = 1;
        }
        else
            lastcr = 0;
        rc = __db_next_freepage(dbp, &pg);
        if (rc) {
            fprintf(stderr, "rc %d getting freelist page %u\n", rc, pg);
            break;
        }
    }
    if (!lastcr) 
        fprintf(out, "\n");
    fprintf(out, "}\n");
    return rc;
}

/* for debugging */
int __lock_dump_region_int(DB_ENV *, const char *area, FILE *,
    int just_active_locks);



//Was used for tesing: static uint8_t pagebuf[1048576];

int __os_physwrite(DB_ENV *dbenv, DB_FH * fhp, void *addr, size_t len,
                   size_t * nwp);

#include <arpa/inet.h>

int __db_new_original(DBC *dbc, u_int32_t type, PAGE **pagepp);

/*
 * __db_new --
 *	Get a new page, preferably from the freelist.
 *
 * PUBLIC: int __db_new __P((DBC *, u_int32_t, PAGE **));
 */
int
__db_new(dbc, type, pagepp)
	DBC *dbc;
	u_int32_t type;
	PAGE **pagepp;
{
	DB *dbp;
	DB_LSN lsn;
	DB_MPOOLFILE *mpf;
	PAGE *h;
	DBMETA *meta;
	DB_LOCK metalock;
	db_pgno_t pgno;
	int extend, ret;
	int meta_flags;
	uint8_t *pagebuf = NULL;
	int i;
	db_pgno_t firstpage;
	int page_extent_size = 0;
	DB_TXN *t = NULL;

	meta = NULL;

	meta_flags = 0;
	dbp = dbc->dbp;
	mpf = dbp->mpf;
	h = NULL;

	page_extent_size = dbc->dbp->dbenv->page_extent_size;
	if (page_extent_size == 0 ||
	    dbc->dbp == ((DB_REP *)dbc->dbp->dbenv->rep_handle)->rep_db)
		return __db_new_original(dbc, type, pagepp);

	*pagepp = NULL;

	pgno = PGNO_BASE_MD;
	if ((ret = __db_lget(dbc,
		    LCK_ALWAYS, pgno, DB_LOCK_WRITE, 0, &metalock)) != 0)
		goto err;
	if ((ret = PAGEGET(dbc, mpf, &pgno, 0, &meta)) != 0)
		goto err;

	extend = (meta->free == PGNO_INVALID);
	meta_flags = DB_MPOOL_DIRTY;


	/* If there's pages on the freelist, call the original code.  Otherwise, extend the file. */
	if (extend) {
		/* linux requires buffers aligned on 512-boundary for direct IO */
#ifdef __linux__
		pagebuf = memalign(512, meta->pagesize * page_extent_size);
#else
		pagebuf = malloc(meta->pagesize * page_extent_size);
#endif

		ret = dbc->dbp->dbenv->txn_begin(dbc->dbp->dbenv, NULL, &t, 0);

		if (ret)
			goto err;

		firstpage = meta->last_pgno + 1;
		for (i = 0; i < page_extent_size; i++) {
			h = (PAGE *)(pagebuf + (meta->pagesize * i));
			pgno = meta->last_pgno + 1 + i;

			P_INIT(h, dbp->pgsize, pgno, PGNO_INVALID, PGNO_INVALID,
			    0, P_INVALID);

			if (DBC_LOGGING(dbc)) {
				DB_LSN pglsn;

				ZERO_LSN(pglsn);
				/* Log the page creation. */
				CHECK_ALLOC_PAGE_LSN(pglsn);
				ret =
				    __db_pg_alloc_log(dbc->dbp, t, &LSN(meta),
				    0, &LSN(meta), PGNO_BASE_MD, &pglsn, pgno,
				    (u_int32_t)P_INVALID, PGNO_INVALID);
				if (ret)
					goto err;
				h->lsn = meta->lsn;
			}

			/* keep track of the LSN, we'll need to flush to that point */
			lsn = h->lsn;
			meta->lsn = lsn;
		}


		/* Now "free" those pages: put them on the free list so subsequent __db_new calls
		 * find them. */
		if (DBC_LOGGING(dbc)) {
			PAGE *bp;
			DBT ldbt;

			memset(&ldbt, 0, sizeof(ldbt));
			for (i = page_extent_size - 1; i >= 0; i--) {
				h = (PAGE *)(pagebuf + (meta->pagesize * i));
				h->next_pgno = meta->free;
				pgno = meta->last_pgno + 1 + i;

				ldbt.data = h;
				ldbt.size = P_OVERHEAD(dbc->dbp);

				ret =
				    PAGEGET(dbc, mpf, &pgno, DB_MPOOL_CREATE,
				    &bp);
				if (ret)
					goto err;
				memcpy(bp, ldbt.data, ldbt.size);

#if defined (DEBUG_PGFREE_ZERO)
				if (h->pgno == 0) {
					__log_flush(dbc->dbp->dbenv, NULL);
					abort();
				}
#endif
				/* Now log the page being put on the freelist */
				ret =
				    __db_pg_free_log(dbc->dbp, t, &LSN(meta), 0,
				    h->pgno, &LSN(meta), PGNO_BASE_MD, &ldbt,
				    meta->free);
				if (ret)
					goto err;

				h->lsn = meta->lsn;
				bp->lsn = meta->lsn;

				/* We cheated and wrote it already, so the page shouldn't be dirty */
				ret = PAGEPUT(dbc, mpf, bp, 0);
				if (ret)
					goto err;

				meta->free = pgno;
			}
		}

		for (i = 0; i < page_extent_size; i++) {
			/* Cheat.  Pre-write the page.  The idea is to get the OS
			 * to allocate a nice contiguous extent. Since we are bypassing
			 * the buffer pool for the write we need to swap the page ourselves.
			 * We write the pages in one shot below, but prepare it here. */
			h = (PAGE *)(pagebuf + (meta->pagesize * i));

			ret = __dir_pg(mpf, h->pgno, (u_int8_t *)h, 0);
			if (ret)
				goto err;
		}

		meta->last_pgno += page_extent_size;

		/* We are about to force pages to disk.  We wrote log records above
		 * which must be forced to disk first.  We didn't bring pages into cache
		 * with __memp_get, so need to take care of flushing ourselves. */
		ret = __log_flush(dbc->dbp->dbenv, &lsn);
		if (ret)
			goto err;

		/* Use pwrite. The pages we want to write are not in the bufferpool, so no __memp_put.
		 * __os_physwrite does multiple writes.  I'd like a stronger hint to the OS that we
		 * (1) want sequential IO, and (2) we want it in a contiguous region on disk.
		 * fallocate() is an even stronger hint, but we'd need a linux kernel from this millennium
		 * for that. */
		ret =
		    pwrite(mpf->fhp->fd, pagebuf,
		    meta->pagesize * page_extent_size,
		    firstpage * meta->pagesize);
		if (ret != meta->pagesize * page_extent_size)
			goto err;

		/* Set the max page number known by the mpool.  This is
		 * normally done by PAGEGET(..., DB_MPOOL_NEW).
		 * We don't want these pages brought into the mpool, since they
		 * may not be needed, but want them available for reads later on
		 * So trick the mpool into believing the pages are there. */
		if (meta->last_pgno > mpf->mfp->last_pgno)
			mpf->mfp->last_pgno = meta->last_pgno;

		t->commit(t, 0);
		t = NULL;
	}

	/* We either added pages to the freelist, or already had some.  Either
	 * way, a page should be available. Get one. */
	ret = __db_new_from_freelist(dbc, meta, type, pagepp);
	if (ret)
		goto err;

	PAGEPUT(dbc, mpf, (PAGE *)meta, DB_MPOOL_DIRTY);

	(void)__TLPUT(dbc, metalock);
	if (pagebuf) {
		free(pagebuf);
        pagebuf = NULL;
    }

	/*
	 * If dirty reads are enabled and we are in a transaction, we could
	 * abort this allocation after the page(s) pointing to this
	 * one have their locks downgraded.  This would permit dirty readers
	 * to access this page which is ok, but they must be off the
	 * page when we abort.  This will also prevent updates happening
	 * to this page until we commit.
	 */
	if (F_ISSET(dbc->dbp, DB_AM_DIRTY) && dbc->txn != NULL) {
		if ((ret = __db_lget(dbc, 0,
			    h->pgno, DB_LOCK_WWRITE, 0, &metalock)) != 0)
			goto err;
	}

	if (extend)
		return DB_LOCK_DEADLOCK;
	else
		return (0);

err:
	if (t)
		t->abort(t);
	if (h != NULL)
		PAGEPUT(dbc, mpf, h, 0);
	if (meta != NULL)
		PAGEPUT(dbc, mpf, meta, meta_flags);
	(void)__TLPUT(dbc, metalock);
	if (pagebuf)
		__os_free(dbc->dbp->dbenv, pagebuf);
	return (ret);
}

int
__db_new_original(dbc, type, pagepp)
	DBC *dbc;
	u_int32_t type;
	PAGE **pagepp;
{
	DBMETA *meta;
	DB *dbp;
	DB_LOCK metalock;
	DB_LSN lsn;
	DB_MPOOLFILE *mpf;
	PAGE *h;
	db_pgno_t last, pgno, newnext;
	u_int32_t meta_flags;
	int extend, ret;

	u_int32_t mbytes, bytes, iosize;
	off_t sz;

	meta = NULL;

	meta_flags = 0;
	dbp = dbc->dbp;
	mpf = dbp->mpf;
	h = NULL;

	newnext = PGNO_INVALID;

	pgno = PGNO_BASE_MD;
	if ((ret = __db_lget(dbc,
		    LCK_ALWAYS, pgno, DB_LOCK_WRITE, 0, &metalock)) != 0)
		goto err;
	if ((ret = PAGEGET(dbc, mpf, &pgno, 0, &meta)) != 0)
		goto err;
	last = meta->last_pgno;
	if (meta->free == PGNO_INVALID) {
		last = pgno = meta->last_pgno + 1;
		ZERO_LSN(lsn);
		extend = 1;
	} else {
		pgno = meta->free;
		if ((ret = PAGEGET(dbc, mpf, &pgno, 0, &h)) != 0)
			goto err;

		/*
		 * We want to take the first page off the free list and
		 * then set meta->free to the that page's next_pgno, but
		 * we need to log the change first.
		 */
		newnext = h->next_pgno;
		lsn = h->lsn;
		extend = 0;
	}


	/* If this is a temp file, don't make this call */
	if (gbl_check_sparse_files && mpf->fhp) {
		/*
		 * Do a sanity check to make sure we aren't somehow
		 * creating a sparse file.  There have been a few
		 * cases of sparse dbs in the wild.
		 */
		__os_ioinfo(dbp->dbenv, NULL, mpf->fhp, &mbytes, &bytes,
		    &iosize);
		sz = mbytes * 1024 * 1024 + bytes;
		/*
		 * After we get a page, it can hang out in cache
		 * indefinitely, so there's no guarantee the next page
		 * is file size + 1.  We can do a very expensive check
		 * to see if all the pages between meta->last_pgno and
		 * pgno are in cache, but that seems really excessive.
		 * So for now we'll warn only if the gap between them
		 * is > cache size + 1 page.  The gaps I've seen have
		 * been either tiny (< 10 pages) or gigantic
		 * (gigabytes), so this seems good enough.
		 */
		if ((pgno * meta->pagesize) >
		    (sz + dbp->dbenv->mp_gbytes * 1024 * 1024 * 1024 +
			dbp->dbenv->mp_bytes)) {
			printf
			    ("(%d) file %s sz %u meta %u allocating page %u extend %d\n",
			    getpid(), dbp->fname ? dbp->fname : "???",
			    (db_pgno_t) sz / meta->pagesize, meta->last_pgno,
			    pgno, extend);

			if (gbl_core_on_sparse_file) {
				char cmd[100];

				snprintf(cmd, sizeof(cmd), "gcore %d", getpid());
				printf("%s\n", cmd);
				int lrc = system(cmd);
                if (lrc) {
                    logmsg(LOGMSG_ERROR, "%s:%d system() returns rc = %d\n",__FILE__,__LINE__, lrc);
				}
				gbl_core_on_sparse_file = 0;
			}
		}
	}

	/*
	 * Log the allocation before fetching the new page.  If we
	 * don't have room in the log then we don't want to tell
	 * mpool to extend the file.
	 */
	if (DBC_LOGGING(dbc)) {
		CHECK_ALLOC_PAGE_LSN(lsn);
		if ((ret = __db_pg_alloc_log(dbp, dbc->txn, &LSN(meta), 0,
			    &LSN(meta), PGNO_BASE_MD, &lsn, pgno,
			    (u_int32_t)type, newnext)) != 0)
			goto err;
	} else
		LSN_NOT_LOGGED(LSN(meta));

	meta_flags = DB_MPOOL_DIRTY;

	meta->free = newnext;

	if (extend == 1) {
		if ((ret = PAGEGET(dbc, mpf, &pgno, DB_MPOOL_NEW, &h)) != 0)
			goto err;
		DB_ASSERT(last == pgno);
		meta->last_pgno = pgno;
		ZERO_LSN(h->lsn);
		h->pgno = pgno;
	}
	LSN(h) = LSN(meta);

	DB_ASSERT(TYPE(h) == P_INVALID);

	if (TYPE(h) != P_INVALID)
		return (__db_panic(dbp->dbenv, EINVAL));

	PAGEPUT(dbc, mpf, (PAGE *)meta, DB_MPOOL_DIRTY);

	(void)__TLPUT(dbc, metalock);

	switch (type) {
	case P_BTREEMETA:
	case P_HASHMETA:
	case P_QAMMETA:
		__db_init_meta(dbp, h, h->pgno, type);
		break;
	default:
		P_INIT(h, dbp->pgsize,
		    h->pgno, PGNO_INVALID, PGNO_INVALID, 0, type);
		break;
	}

	/*
	 * If dirty reads are enabled and we are in a transaction, we could
	 * abort this allocation after the page(s) pointing to this
	 * one have their locks downgraded.  This would permit dirty readers
	 * to access this page which is ok, but they must be off the
	 * page when we abort.  This will also prevent updates happening
	 * to this page until we commit.
	 */
	if (F_ISSET(dbc->dbp, DB_AM_DIRTY) && dbc->txn != NULL) {
		if ((ret = __db_lget(dbc, 0,
		    h->pgno, DB_LOCK_WWRITE, 0, &metalock)) != 0)
			goto err;
	}
	*pagepp = h;
	return (0);

err:	if (h != NULL)
		PAGEPUT(dbc, mpf, h, 0);
	if (meta != NULL)
		PAGEPUT(dbc, mpf, meta, meta_flags);
	(void)__TLPUT(dbc, metalock);
	return (ret);
}
/*
 * __db_free --
 *	Add a page to the head of the freelist.
 *
 * PUBLIC: int __db_free __P((DBC *, PAGE *));
 */
int
__db_free(dbc, h)
	DBC *dbc;
	PAGE *h;
{
	DBMETA *meta;
	DB *dbp;
	DBT ddbt, ldbt;
	DB_LOCK metalock;
	DB_MPOOLFILE *mpf;
	db_pgno_t pgno;
	u_int32_t dirty_flag;
	int ret, t_ret;

	dbp = dbc->dbp;
	mpf = dbp->mpf;

	/*
	 * Retrieve the metadata page and insert the page at the head of
	 * the free list.  If either the lock get or page get routines
	 * fail, then we need to put the page with which we were called
	 * back because our caller assumes we take care of it.
	 */
	dirty_flag = 0;
	pgno = PGNO_BASE_MD;
	if ((ret = __db_lget(dbc,
	    LCK_ALWAYS, pgno, DB_LOCK_WRITE, 0, &metalock)) != 0)
		goto err;
	if ((ret = PAGEGET(dbc, mpf, &pgno, 0, &meta)) != 0) {
		(void)__TLPUT(dbc, metalock);
		goto err;
	}


	DB_ASSERT(h->pgno != meta->free);
	/* Log the change. */
	if (DBC_LOGGING(dbc)) {
		memset(&ldbt, 0, sizeof(ldbt));
		ldbt.data = h;
		ldbt.size = P_OVERHEAD(dbp);
		switch (TYPE(h)) {
		case P_HASH:
		case P_IBTREE:
		case P_IRECNO:
		case P_LBTREE:
		case P_LRECNO:
		case P_LDUP:
			if (h->entries > 0 || IS_PREFIX(h)) {
				if (IS_PREFIX(h))
					ldbt.size += sizeof(db_indx_t);
				ldbt.size += h->entries * sizeof(db_indx_t);
				ddbt.data = (u_int8_t *)h + h->hf_offset;
				ddbt.size = dbp->pgsize - h->hf_offset;
				ret = __db_pg_freedata_log(dbp, dbc->txn,
				     &LSN(meta), 0, h->pgno, &LSN(meta),
				     PGNO_BASE_MD, &ldbt, meta->free, &ddbt);
				break;
			}
			goto log;
		case P_HASHMETA:
			ldbt.size = sizeof(HMETA);
			goto log;
		case P_BTREEMETA:
			ldbt.size = sizeof(BTMETA);
			goto log;
		case P_OVERFLOW:
			ldbt.size += OV_LEN(h);
			goto log;
		default:
			DB_ASSERT(TYPE(h) != P_QAMDATA);

log:
#if defined (DEBUG_PGFREE_ZERO)
				if (h->pgno == 0) {
					__log_flush(dbc->dbp->dbenv, NULL);
					abort();
				}
#endif
				ret = __db_pg_free_log(dbp,
				dbc->txn, &LSN(meta), 0, h->pgno,
				&LSN(meta), PGNO_BASE_MD, &ldbt, meta->free);
		}
		if (ret != 0) {
			PAGEPUT(dbc, mpf, (PAGE *)meta, 0);
			(void)__TLPUT(dbc, metalock);
			goto err;
		}
	} else {
#if defined (UFID_HASH_DEBUG)
		logmsg(LOGMSG_USER, "logging not set for cursor, metalsn [%d:%d]\n",
				LSN(meta).file, LSN(meta).offset);
		logmsg(LOGMSG_USER, "logging-on=%d recover=%d rep-client=%d\n",
				LOGGING_ON((dbc)->dbp->dbenv), F_ISSET((dbc), DBC_RECOVER),
				IS_REP_CLIENT((dbc)->dbp->dbenv));
#endif
		LSN_NOT_LOGGED(LSN(meta));
	}
	LSN(h) = LSN(meta);
#if defined (UFID_HASH_DEBUG)
	if (IS_NOT_LOGGED_LSN(LSN(h))) {
		comdb2_cheapstack_sym(stderr, "%s setting not-logged for pg %d",
				__func__, h->pgno);
	}
#endif

	P_INIT(h, dbp->pgsize, h->pgno, PGNO_INVALID, meta->free, 0, P_INVALID);
#ifdef DIAGNOSTIC
	memset((u_int8_t *)
	    h + P_OVERHEAD(dbp), CLEAR_BYTE, dbp->pgsize - P_OVERHEAD(dbp));
#endif

	meta->free = h->pgno;

	/* Discard the metadata page. */
	if ((t_ret =
	    PAGEPUT(dbc, mpf, (PAGE *)meta, DB_MPOOL_DIRTY)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __TLPUT(dbc, metalock)) != 0 && ret == 0)
		ret = t_ret;

	/* Discard the caller's page reference. */
	dirty_flag = DB_MPOOL_DIRTY;
err:	if ((t_ret = PAGEPUT(dbc, mpf, h, dirty_flag)) != 0 && ret == 0)
		ret = t_ret;

	/*
	 * XXX
	 * We have to unlock the caller's page in the caller!
	 */
	return (ret);
}

#ifdef DEBUG
/*
 * __db_lprint --
 *	Print out the list of locks currently held by a cursor.
 *
 * PUBLIC: int __db_lprint __P((DBC *));
 */
int
__db_lprint(dbc)
	DBC *dbc;
{
	DB_ENV *dbenv;
	DB *dbp;
	DB_LOCKREQ req;

	dbp = dbc->dbp;
	dbenv = dbp->dbenv;

	if (LOCKING_ON(dbenv)) {
		req.op = DB_LOCK_DUMP;
		(void)__lock_vec(dbenv, dbc->locker, 0, &req, 1, NULL);
	}
	return (0);
}
#endif

/*
 * Implement the rules for transactional locking.  We can release the previous
 * lock if we are not in a transaction or COUPLE_ALWAYS is specifed (used in
 * record locking).  If we are doing dirty reads then we can release read locks
 * and down grade write locks.
 */
#define	DB_PUT_ACTION(dbc, action, lockp)				\
	    (((action == LCK_COUPLE || action == LCK_COUPLE_ALWAYS) &&	\
	    LOCK_ISSET(*lockp)) ?					\
	    (dbc->txn == NULL || action == LCK_COUPLE_ALWAYS ||		\
	    (F_ISSET(dbc, DBC_DIRTY_READ) &&				\
	    (lockp)->mode == DB_LOCK_DIRTY)) ? LCK_COUPLE :		\
	    (F_ISSET((dbc)->dbp, DB_AM_DIRTY) &&			\
	    (lockp)->mode == DB_LOCK_WRITE) ? LCK_DOWNGRADE : 0 : 0)

/*
 * __db_lget --
 *	The standard lock get call.
 *
 * PUBLIC: int __db_lget __P((DBC *,
 * PUBLIC:     int, db_pgno_t, db_lockmode_t, u_int32_t, DB_LOCK *));
 */
int
__db_lget(dbc, action, pgno, mode, lkflags, lockp)
	DBC *dbc;
	int action;
	db_pgno_t pgno;
	db_lockmode_t mode;
	u_int32_t lkflags;
	DB_LOCK *lockp;
{
	DB *dbp;
	DB_ENV *dbenv;
	DB_LOCKREQ couple[2], *reqp;
	DB_TXN *txn;
	int has_timeout, ret;

	dbp = dbc->dbp;
	dbenv = dbp->dbenv;
	txn = dbc->txn;

	/*
	 * We do not always check if we're configured for locking before
	 * calling __db_lget to acquire the lock.
	 */
	if (CDB_LOCKING(dbenv) || 
	    !LOCKING_ON(dbenv) || F_ISSET(dbc, DBC_COMPENSATE) ||
	    (F_ISSET(dbc, DBC_RECOVER) &&
	    (action != LCK_ROLLBACK || IS_REP_CLIENT(dbenv))) ||
	    (action != LCK_ALWAYS && F_ISSET(dbc, DBC_OPD))) {
		LOCK_INIT(*lockp);
		return (0);
	}

	dbc->lock.pgno = pgno;
	if (lkflags & DB_LOCK_RECORD)
		dbc->lock.type = DB_RECORD_LOCK;
	else
		dbc->lock.type = DB_PAGE_LOCK;
	lkflags &= ~DB_LOCK_RECORD;

	/*
	 * If the transaction enclosing this cursor has DB_LOCK_NOWAIT set,
	 * pass that along to the lock call.
	 */
	if (DB_NONBLOCK(dbc))
		lkflags |= DB_LOCK_NOWAIT;

	if (F_ISSET(dbc, DBC_DIRTY_READ) && mode == DB_LOCK_READ)
		mode = DB_LOCK_DIRTY;

	has_timeout = F_ISSET(dbc, DBC_RECOVER) ||
	    (txn != NULL && F_ISSET(txn, TXN_LOCKTIMEOUT));

	switch (DB_PUT_ACTION(dbc, action, lockp)) {
	case LCK_COUPLE:
lck_couple:	couple[0].op = has_timeout? DB_LOCK_GET_TIMEOUT : DB_LOCK_GET;
		couple[0].obj = &dbc->lock_dbt;
		couple[0].mode = mode;
		if (action == LCK_COUPLE_ALWAYS)
			action = LCK_COUPLE;
		UMRW_SET(couple[0].timeout);
		if (has_timeout)
			couple[0].timeout =
			     F_ISSET(dbc, DBC_RECOVER) ? 0 : txn->lock_timeout;
		if (action == LCK_COUPLE) {
			couple[1].op = DB_LOCK_PUT;
			couple[1].lock = *lockp;
		}

		ret = __lock_vec(dbenv, dbc->locker,
		    lkflags, couple, action == LCK_COUPLE ? 2 : 1, &reqp);
		if (ret == 0 || reqp == &couple[1])
			*lockp = couple[0].lock;
		break;
	case LCK_DOWNGRADE:
		if ((ret = __lock_downgrade(
		    dbenv, lockp, DB_LOCK_WWRITE, 0)) != 0)
			return (ret);
		/* FALL THROUGH */
	default:
		if (has_timeout)
			goto lck_couple;
		ret = __lock_get(dbenv,
		    dbc->locker, lkflags, &dbc->lock_dbt, mode, lockp);
		break;
	}

	return ((ret == DB_LOCK_NOTGRANTED &&
	     !F_ISSET(dbenv, DB_ENV_TIME_NOTGRANTED)) ? DB_LOCK_DEADLOCK : ret);
}

/*
 * __db_lput --
 *	The standard lock put call.
 *
 * PUBLIC: int __db_lput __P((DBC *, DB_LOCK *));
 */
int
__db_lput(dbc, lockp)
	DBC *dbc;
	DB_LOCK *lockp;
{
	DB_ENV *dbenv;
	int ret;

	dbenv = dbc->dbp->dbenv;

	switch (DB_PUT_ACTION(dbc, LCK_COUPLE, lockp)) {
	case LCK_COUPLE:
		ret = __lock_put(dbenv, lockp);
		break;
	case LCK_DOWNGRADE:
		ret = __lock_downgrade(dbenv, lockp, DB_LOCK_WWRITE, 0);
		break;
	default:
		ret = 0;
		break;
	}

	return (ret);
}

static volatile int count_freepages_abort = 0;

void
__berkdb_count_freeepages_abort(void)
{
	count_freepages_abort = 1;
}

unsigned int
__berkdb_count_freepages(int fd)
{
	PAGE *p;
	DBMETA *meta;
	uint8_t metabuf[512];
	uint8_t *buf = NULL;
	int pagesz;
	unsigned int npages = 0;

	count_freepages_abort = 0;

	meta = (DBMETA *)metabuf;
	if (read(fd, metabuf, 512) != 512)
		goto done;
	pagesz = ntohl(meta->pagesize);
	if (pagesz < 512 || pagesz > (64 * 1024))
		goto done;
	if (lseek(fd, 0, SEEK_SET) != 0)
		goto done;
	buf = malloc(pagesz);
	p = (PAGE *)buf;

	while (read(fd, buf, pagesz) == pagesz && !count_freepages_abort) {
		if (TYPE(p) == P_INVALID)
			npages++;
	}

done:
	if (buf)
		free(buf);
	return npages;
}
