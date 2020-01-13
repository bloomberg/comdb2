/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: db_pr.c,v 11.94 2003/06/30 17:19:46 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#endif

#include "tohex.h"
#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/btree.h"
#include "dbinc/hash.h"
#include "dbinc/mp.h"
#include "dbinc/qam.h"
#include "dbinc/db_verify.h"
#include "logmsg.h"

static int __db_bmeta __P((DB *, FILE *, BTMETA *, u_int32_t));
static int __db_hmeta __P((DB *, FILE *, HMETA *, u_int32_t));
static void __db_meta __P((DB *, DBMETA *, FILE *, FN const *, u_int32_t));
static const char *__db_pagetype_to_string __P((u_int32_t));
static void __db_prdb __P((DB *, FILE *));
static void __db_proff __P((void *, FILE *));
static int __db_prtree __P((DB *, FILE *, u_int32_t));
static int __db_qmeta __P((DB *, FILE *, QMETA *, u_int32_t));

/*
 * __db_loadme --
 *	A nice place to put a breakpoint.
 *
 * PUBLIC: void __db_loadme __P((void));
 */
void
__db_loadme()
{
	u_int32_t id;

	__os_id(&id);
}

/*
 * __db_dump --
 *	Dump the tree to a file.
 *
 * PUBLIC: int __db_dump __P((DB *, char *, char *));
 */
int
__db_dump(dbp, op, name)
	DB *dbp;
	char *op, *name;
{
	FILE *fp;
	u_int32_t flags;
	int ret;

	for (flags = 0; *op != '\0'; ++op)
		switch (*op) {
		case 'a':
			LF_SET(DB_PR_PAGE);
			break;
		case 'h':
			break;
		case 'r':
			LF_SET(DB_PR_RECOVERYTEST);
			break;
		default:
			return (EINVAL);
		}

	if (name == NULL)
		fp = stdout;
	else {
		if ((fp = fopen(name, "w")) == NULL)
			return (__os_get_errno());
	}

	__db_prdb(dbp, fp);

	logmsgf(LOGMSG_USER, fp, "%s\n", DB_LINE);

	ret = __db_prtree(dbp, fp, flags);

	(void)fflush(fp);
	if (name != NULL)
		(void)fclose(fp);

	return (ret);
}

/*
 * __db_inmemdbflags --
 *	Call a callback for printing or other handling of strings associated
 * with whatever in-memory DB structure flags are set.
 *
 * PUBLIC: void __db_inmemdbflags __P((u_int32_t, void *,
 * PUBLIC:     void (*)(u_int32_t, const FN *, void *)));
 */
void
__db_inmemdbflags(flags, cookie, callback)
	u_int32_t flags;
	void *cookie;
	void (*callback) __P((u_int32_t, const FN *, void *));
{
	static const FN fn[] = {
		{ DB_AM_CHKSUM,		"checksumming" },
		{ DB_AM_CL_WRITER,	"client replica writer" },
		{ DB_AM_COMPENSATE,	"created by compensating transaction" },
		{ DB_AM_CREATED,	"database created" },
		{ DB_AM_CREATED_MSTR,	"encompassing file created" },
		{ DB_AM_DBM_ERROR,	"dbm/ndbm error" },
		{ DB_AM_DELIMITER,	"variable length" },
		{ DB_AM_DIRTY,		"dirty reads" },
		{ DB_AM_DISCARD,	"discard cached pages" },
		{ DB_AM_DUP,		"duplicates" },
		{ DB_AM_DUPSORT,	"sorted duplicates" },
		{ DB_AM_ENCRYPT,	"encrypted" },
		{ DB_AM_FIXEDLEN,	"fixed-length records" },
		{ DB_AM_INMEM,		"in-memory" },
		{ DB_AM_IN_RENAME,	"file is being renamed" },
		{ DB_AM_OPEN_CALLED,	"DB->open called" },
		{ DB_AM_PAD,		"pad value" },
		{ DB_AM_PGDEF,		"default page size" },
		{ DB_AM_RDONLY,		"read-only" },
		{ DB_AM_RECNUM,		"Btree record numbers" },
		{ DB_AM_RECOVER,	"opened for recovery" },
		{ DB_AM_RENUMBER,	"renumber" },
		{ DB_AM_REVSPLITOFF,	"no reverse splits" },
		{ DB_AM_SECONDARY,	"secondary" },
		{ DB_AM_SNAPSHOT,	"load on open" },
		{ DB_AM_SUBDB,		"subdatabases" },
		{ DB_AM_SWAP,		"needswap" },
		{ DB_AM_TXN,		"transactional" },
		{ DB_AM_VERIFYING,	"verifier" },
		{ 0,			NULL }
	};

	callback(flags, fn, cookie);
}

/*
 * __db_prdb --
 *	Print out the DB structure information.
 */
static void
__db_prdb(dbp, fp)
	DB *dbp;
	FILE *fp;
{
	BTREE *bt;
	HASH *h;
	QUEUE *q;

	logmsgf(LOGMSG_USER, fp,
	    "In-memory DB structure:\n%s: %#lx",
	    __db_dbtype_to_string(dbp->type), (u_long)dbp->flags);
	__db_inmemdbflags(dbp->flags, fp, __db_prflags);
	logmsgf(LOGMSG_USER, fp, "\n");

	switch (dbp->type) {
	case DB_BTREE:
	case DB_RECNO:
		bt = dbp->bt_internal;
		logmsgf(LOGMSG_USER, fp, "bt_meta: %lu bt_root: %lu\n",
		    (u_long)bt->bt_meta, (u_long)bt->bt_root);
		logmsgf(LOGMSG_USER, fp, "bt_maxkey: %lu bt_minkey: %lu\n",
		    (u_long)bt->bt_maxkey, (u_long)bt->bt_minkey);
		logmsgf(LOGMSG_USER, fp, "bt_compare: %#lx bt_prefix: %#lx\n",
		    P_TO_ULONG(bt->bt_compare), P_TO_ULONG(bt->bt_prefix));
		logmsgf(LOGMSG_USER, fp, "bt_lpgno: %lu\n", (u_long)bt->bt_lpgno);
		if (dbp->type == DB_RECNO) {
			logmsgf(LOGMSG_USER, fp,
		    "re_pad: %#lx re_delim: %#lx re_len: %lu re_source: %s\n",
			    (u_long)bt->re_pad, (u_long)bt->re_delim,
			    (u_long)bt->re_len,
			    bt->re_source == NULL ? "" : bt->re_source);
			logmsgf(LOGMSG_USER, fp, "re_modified: %d re_eof: %d re_last: %lu\n",
			    bt->re_modified, bt->re_eof, (u_long)bt->re_last);
		}
		break;
	case DB_HASH:
		h = dbp->h_internal;
		logmsgf(LOGMSG_USER, fp, "meta_pgno: %lu\n", (u_long)h->meta_pgno);
		logmsgf(LOGMSG_USER, fp, "h_ffactor: %lu\n", (u_long)h->h_ffactor);
        logmsgf(LOGMSG_USER, fp, "h_nelem: %lu\n", (u_long)h->h_nelem);
		logmsgf(LOGMSG_USER, fp, "h_hash: %#lx\n", P_TO_ULONG(h->h_hash));
		break;
	case DB_QUEUE:
		q = dbp->q_internal;
		logmsgf(LOGMSG_USER, fp, "q_meta: %lu\n", (u_long)q->q_meta);
		logmsgf(LOGMSG_USER, fp, "q_root: %lu\n", (u_long)q->q_root);
		logmsgf(LOGMSG_USER, fp, "re_pad: %#lx re_len: %lu\n",
		    (u_long)q->re_pad, (u_long)q->re_len);
		logmsgf(LOGMSG_USER, fp, "rec_page: %lu\n", (u_long)q->rec_page);
		logmsgf(LOGMSG_USER, fp, "page_ext: %lu\n", (u_long)q->page_ext);
		break;
	case DB_UNKNOWN:
	default:
		break;
	}
}

/*
 * __db_prtree --
 *	Print out the entire tree.
 */
static int
__db_prtree(dbp, fp, flags)
	DB *dbp;
	FILE *fp;
	u_int32_t flags;
{
	DB_MPOOLFILE *mpf;
	PAGE *h;
	db_pgno_t i, last;
	int ret;

	mpf = dbp->mpf;

	if (dbp->type == DB_QUEUE)
		return (__db_prqueue(dbp, fp, flags));

	/*
	 * Find out the page number of the last page in the database, then
	 * dump each page.
	 */
	__memp_last_pgno(mpf, &last);
	for (i = 0; i <= last; ++i) {
		if ((ret = __memp_fget(mpf, &i, 0, &h)) != 0)
			return (ret);
		(void)__db_prpage(dbp, h, fp, flags);
		if ((ret = __memp_fput(mpf, h, 0)) != 0)
			return (ret);
	}

	return (0);
}

/*
 * __db_meta --
 *	Print out common metadata information.
 */
static void
__db_meta(dbp, dbmeta, fp, fn, flags)
	DB *dbp;
	DBMETA *dbmeta;
	FILE *fp;
	FN const *fn;
	u_int32_t flags;
{
	DB_MPOOLFILE *mpf;
	PAGE *h;
	db_pgno_t pgno;
	u_int8_t *p;
	int cnt, ret;
	const char *sep;

	mpf = dbp->mpf;

	logmsgf(LOGMSG_USER, fp, "\tmagic: %#lx\n", (u_long)dbmeta->magic);
	logmsgf(LOGMSG_USER, fp, "\tversion: %lu\n", (u_long)dbmeta->version);
	logmsgf(LOGMSG_USER, fp, "\tpagesize: %lu\n", (u_long)dbmeta->pagesize);
	logmsgf(LOGMSG_USER, fp, "\ttype: %lu\n", (u_long)PGTYPE(dbmeta));
	logmsgf(LOGMSG_USER, fp, "\tkeys: %lu\trecords: %lu\n",
	    (u_long)dbmeta->key_count, (u_long)dbmeta->record_count);

	if (!LF_ISSET(DB_PR_RECOVERYTEST)) {
		/*
		 * If we're doing recovery testing, don't display the free
		 * list, it may have changed and that makes the dump diff
		 * not work.
		 */
		logmsgf(LOGMSG_USER, fp, "\tfree list: %lu", (u_long)dbmeta->free);
		for (pgno = dbmeta->free,
		    cnt = 0, sep = ", "; pgno != PGNO_INVALID;) {
			if ((ret = __memp_fget(mpf, &pgno, 0, &h)) != 0) {
				logmsgf(LOGMSG_USER, fp,
			    "Unable to retrieve free-list page: %lu: %s\n",
				    (u_long)pgno, db_strerror(ret));
				break;
			}
			pgno = h->next_pgno;
			(void)__memp_fput(mpf, h, 0);
			logmsgf(LOGMSG_USER, fp, "%s%lu", sep, (u_long)pgno);
			if (++cnt % 10 == 0) {
				logmsgf(LOGMSG_USER, fp, "\n");
				cnt = 0;
				sep = "\t";
			} else
				sep = ", ";
		}
		logmsgf(LOGMSG_USER, fp, "\n");
		logmsgf(LOGMSG_USER, fp, "\tlast_pgno: %lu\n", (u_long)dbmeta->last_pgno);
	}

	if (fn != NULL) {
		logmsgf(LOGMSG_USER, fp, "\tflags: %#lx", (u_long)dbmeta->flags);
		__db_prflags(dbmeta->flags, fn, fp);
		logmsgf(LOGMSG_USER, fp, "\n");
	}

	logmsgf(LOGMSG_USER, fp, "\tuid: ");
	for (p = (u_int8_t *)dbmeta->uid,
	    cnt = 0; cnt < DB_FILE_ID_LEN; ++cnt) {
		logmsgf(LOGMSG_USER, fp, "%x", *p++);
		if (cnt < DB_FILE_ID_LEN - 1)
			logmsgf(LOGMSG_USER, fp, " ");
	}
	logmsgf(LOGMSG_USER, fp, "\n");
}

/*
 * __db_bmeta --
 *	Print out the btree meta-data page.
 */
static int
__db_bmeta(dbp, fp, h, flags)
	DB *dbp;
	FILE *fp;
	BTMETA *h;
	u_int32_t flags;
{
	static const FN mfn[] = {
		{ BTM_DUP,	"duplicates" },
		{ BTM_RECNO,	"recno" },
		{ BTM_RECNUM,	"btree:recnum" },
		{ BTM_FIXEDLEN,	"recno:fixed-length" },
		{ BTM_RENUMBER,	"recno:renumber" },
		{ BTM_SUBDB,	"multiple-databases" },
		{ 0,		NULL }
	};

	__db_meta(dbp, (DBMETA *)h, fp, mfn, flags);

	logmsgf(LOGMSG_USER, fp, "\tmaxkey: %lu minkey: %lu\n",
	    (u_long)h->maxkey, (u_long)h->minkey);
	if (dbp->type == DB_RECNO)
		logmsgf(LOGMSG_USER, fp, "\tre_len: %#lx re_pad: %lu\n",
		    (u_long)h->re_len, (u_long)h->re_pad);
	logmsgf(LOGMSG_USER, fp, "\troot: %lu\n", (u_long)h->root);

	return (0);
}

/*
 * __db_hmeta --
 *	Print out the hash meta-data page.
 */
static int
__db_hmeta(dbp, fp, h, flags)
	DB *dbp;
	FILE *fp;
	HMETA *h;
	u_int32_t flags;
{
	static const FN mfn[] = {
		{ DB_HASH_DUP,	 "duplicates" },
		{ DB_HASH_SUBDB, "multiple-databases" },
		{ 0,		 NULL }
	};
	int i;

	__db_meta(dbp, (DBMETA *)h, fp, mfn, flags);

	logmsgf(LOGMSG_USER, fp, "\tmax_bucket: %lu\n", (u_long)h->max_bucket);
	logmsgf(LOGMSG_USER, fp, "\thigh_mask: %#lx\n", (u_long)h->high_mask);
	logmsgf(LOGMSG_USER, fp, "\tlow_mask:  %#lx\n", (u_long)h->low_mask);
	logmsgf(LOGMSG_USER, fp, "\tffactor: %lu\n", (u_long)h->ffactor);
	logmsgf(LOGMSG_USER, fp, "\tnelem: %lu\n", (u_long)h->nelem);
	logmsgf(LOGMSG_USER, fp, "\th_charkey: %#lx\n", (u_long)h->h_charkey);
	logmsgf(LOGMSG_USER, fp, "\tspare points: ");
	for (i = 0; i < NCACHED; i++)
		logmsgf(LOGMSG_USER, fp, "%lu ", (u_long)h->spares[i]);
	logmsgf(LOGMSG_USER, fp, "\n");

	return (0);
}

/*
 * __db_qmeta --
 *	Print out the queue meta-data page.
 */
static int
__db_qmeta(dbp, fp, h, flags)
	DB *dbp;
	FILE *fp;
	QMETA *h;
	u_int32_t flags;
{
	__db_meta(dbp, (DBMETA *)h, fp, NULL, flags);

	logmsgf(LOGMSG_USER, fp, "\tfirst_recno: %lu\n", (u_long)h->first_recno);
	logmsgf(LOGMSG_USER, fp, "\tcur_recno: %lu\n", (u_long)h->cur_recno);
	logmsgf(LOGMSG_USER, fp, "\tre_len: %#lx re_pad: %lu\n",
	    (u_long)h->re_len, (u_long)h->re_pad);
	logmsgf(LOGMSG_USER, fp, "\trec_page: %lu\n", (u_long)h->rec_page);
	logmsgf(LOGMSG_USER, fp, "\tpage_ext: %lu\n", (u_long)h->page_ext);

	return (0);
}

/*
 * __db_prnpage
 *	-- Print out a specific page.
 *
 * PUBLIC: int __db_prnpage __P((DB *, db_pgno_t, FILE *));
 */
int
__db_prnpage(dbp, pgno, fp)
	DB *dbp;
	db_pgno_t pgno;
	FILE *fp;
{
	DB_MPOOLFILE *mpf;
	PAGE *h;
	int ret, t_ret;

	mpf = dbp->mpf;

	if ((ret = __memp_fget(mpf, &pgno, 0, &h)) != 0)
		return (ret);

	ret = __db_prpage(dbp, h, fp, DB_PR_PAGE);

	if ((t_ret = __memp_fput(mpf, h, 0)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}

/*
 * __db_prpage
 *	-- Print out a page.
 *
 * PUBLIC: int __db_prpage __P((DB *, PAGE *, FILE *, u_int32_t));
 */
int
__db_prpage(dbp, h, fp, flags)
	DB *dbp;
	PAGE *h;
	FILE *fp;
	u_int32_t flags;
{
	BINTERNAL *bi;
	BKEYDATA *bk;
	HOFFPAGE a_hkd;
	QAMDATA *qp, *qep;
	RINTERNAL *ri;
	db_indx_t dlen, len, i, *inp;
	db_pgno_t pgno;
	db_recno_t recno;
	u_int32_t pagesize, qlen;
	u_int8_t *ep, *hk, *p;
	int deleted, ret;
	const char *s;
	void *sp;

	/*
	 * If we're doing recovery testing and this page is P_INVALID,
	 * assume it's a page that's on the free list, and don't display it.
	 */
	if (LF_ISSET(DB_PR_RECOVERYTEST) && TYPE(h) == P_INVALID)
		return (0);

	s = __db_pagetype_to_string(TYPE(h));
	if (s == NULL) {
		logmsgf(LOGMSG_USER, fp, "ILLEGAL PAGE TYPE: page: %lu type: %lu\n",
		    (u_long)h->pgno, (u_long)TYPE(h));
		return (1);
	}

	/*
	 * !!!
	 * Find out the page size.  We don't want to do it the "right" way,
	 * by reading the value from the meta-data page, that's going to be
	 * slow.  Reach down into the mpool region.
	 */
	pagesize = (u_int32_t)dbp->mpf->mfp->stat.st_pagesize;

	/* Page number, page type. */
	logmsgf(LOGMSG_USER, fp, "page %lu: %s level: %lu",
	    (u_long)h->pgno, s, (u_long)h->level);

	/* Record count. */
	if (TYPE(h) == P_IBTREE ||
	    TYPE(h) == P_IRECNO || (TYPE(h) == P_LRECNO &&
	    h->pgno == ((BTREE *)dbp->bt_internal)->bt_root))
		logmsgf(LOGMSG_USER, fp, " records: %lu", (u_long)RE_NREC(h));

	/* LSN. */
	if (!LF_ISSET(DB_PR_RECOVERYTEST))
		logmsgf(LOGMSG_USER, fp, " (lsn.file: %lu lsn.offset: %lu)\n",
		    (u_long)LSN(h).file, (u_long)LSN(h).offset);

	switch (TYPE(h)) {
	case P_BTREEMETA:
		return (__db_bmeta(dbp, fp, (BTMETA *)h, flags));
	case P_HASHMETA:
		return (__db_hmeta(dbp, fp, (HMETA *)h, flags));
	case P_QAMMETA:
		return (__db_qmeta(dbp, fp, (QMETA *)h, flags));
	case P_QAMDATA:				/* Should be meta->start. */
		if (!LF_ISSET(DB_PR_PAGE))
			return (0);

		qlen = ((QUEUE *)dbp->q_internal)->re_len;
		recno = (h->pgno - 1) * QAM_RECNO_PER_PAGE(dbp) + 1;
		i = 0;
		qep = (QAMDATA *)((u_int8_t *)h + pagesize - qlen);
		for (qp = QAM_GET_RECORD(dbp, h, i); qp < qep;
		    recno++, i++, qp = QAM_GET_RECORD(dbp, h, i)) {
			if (!F_ISSET(qp, QAM_SET))
				continue;

			logmsgf(LOGMSG_USER, fp, "%s",
			    F_ISSET(qp, QAM_VALID) ? "\t" : "       D");
			logmsgf(LOGMSG_USER, fp, "[%03lu] %4lu ", (u_long)recno,
			    (u_long)((u_int8_t *)qp - (u_int8_t *)h));
			__db_pr(qp->data, qlen, fp);
		}
		return (0);
	default:
		break;
	}

	/* LSN. */
	if (LF_ISSET(DB_PR_RECOVERYTEST))
		logmsgf(LOGMSG_USER, fp, " (lsn.file: %lu lsn.offset: %lu)\n",
		    (u_long)LSN(h).file, (u_long)LSN(h).offset);

	s = "\t";
	if (TYPE(h) != P_IBTREE && TYPE(h) != P_IRECNO) {
		logmsgf(LOGMSG_USER, fp, "%sprev: %4lu next: %4lu",
		    s, (u_long)PREV_PGNO(h), (u_long)NEXT_PGNO(h));
		s = " ";
	}
	if (TYPE(h) == P_OVERFLOW) {
		logmsgf(LOGMSG_USER, fp, "%sref cnt: %4lu ", s, (u_long)OV_REF(h));
		__db_pr((u_int8_t *)h + P_OVERHEAD(dbp), OV_LEN(h), fp);
		return (0);
	}
	logmsgf(LOGMSG_USER, fp, "%sentries: %4lu", s, (u_long)NUM_ENT(h));
	logmsgf(LOGMSG_USER, fp, " offset: %4lu\n", (u_long)HOFFSET(h));

	if (TYPE(h) == P_INVALID || !LF_ISSET(DB_PR_PAGE))
		return (0);

	ret = 0;
	inp = P_INP(dbp, h);
	for (i = 0; i < NUM_ENT(h); i++) {
		if ((db_alignp_t)(P_ENTRY(dbp, h, i) - (u_int8_t *)h) <
		    (db_alignp_t)(P_OVERHEAD(dbp)) ||
		    (size_t)(P_ENTRY(dbp, h, i) - (u_int8_t *)h) >= pagesize) {
			logmsgf(LOGMSG_USER, fp,
			    "ILLEGAL PAGE OFFSET: indx: %lu of %lu\n",
			    (u_long)i, (u_long)inp[i]);
			ret = EINVAL;
			continue;
		}
		deleted = 0;
		switch (TYPE(h)) {
		case P_HASH:
		case P_IBTREE:
		case P_IRECNO:
			sp = P_ENTRY(dbp, h, i);
			break;
		case P_LBTREE:
			sp = P_ENTRY(dbp, h, i);
			deleted = i % 2 == 0 &&
			    B_DISSET(GET_BKEYDATA(dbp, h, i + O_INDX));
			break;
		case P_LDUP:
		case P_LRECNO:
			sp = P_ENTRY(dbp, h, i);
			deleted = B_DISSET(GET_BKEYDATA(dbp, h, i));
			break;
		default:
			goto type_err;
		}
		logmsgf(LOGMSG_USER, fp, "%s", deleted ? "       D" : "\t");
		logmsgf(LOGMSG_USER, fp, "[%03lu] %4lu ", (u_long)i, (u_long)inp[i]);
		switch (TYPE(h)) {
		case P_HASH:
			hk = sp;
			switch (HPAGE_PTYPE(hk)) {
			case H_OFFDUP:
				memcpy(&pgno,
				    HOFFDUP_PGNO(hk), sizeof(db_pgno_t));
				logmsgf(LOGMSG_USER, fp,
				    "%4lu [offpage dups]\n", (u_long)pgno);
				break;
			case H_DUPLICATE:
				/*
				 * If this is the first item on a page, then
				 * we cannot figure out how long it is, so
				 * we only print the first one in the duplicate
				 * set.
				 */
				if (i != 0)
					len = LEN_HKEYDATA(dbp, h, 0, i);
				else
					len = 1;

				logmsgf(LOGMSG_USER, fp, "Duplicates:\n");
				for (p = HKEYDATA_DATA(hk),
				    ep = p + len; p < ep;) {
					memcpy(&dlen, p, sizeof(db_indx_t));
					p += sizeof(db_indx_t);
					logmsgf(LOGMSG_USER, fp, "\t\t");
					__db_pr(p, dlen, fp);
					p += sizeof(db_indx_t) + dlen;
				}
				break;
			case H_KEYDATA:
				__db_pr(HKEYDATA_DATA(hk),
				    LEN_HKEYDATA(dbp, h, i == 0 ?
				    pagesize : 0, i), fp);
				break;
			case H_OFFPAGE:
				memcpy(&a_hkd, hk, HOFFPAGE_SIZE);
				logmsgf(LOGMSG_USER, fp,
				    "overflow: total len: %4lu page: %4lu\n",
				    (u_long)a_hkd.tlen, (u_long)a_hkd.pgno);
				break;
			default:
				logmsgf(LOGMSG_USER, fp, "ILLEGAL HASH PAGE TYPE: %lu\n",
				    (u_long)HPAGE_PTYPE(hk));
				ret = EINVAL;
				break;
			}
			break;
		case P_IBTREE:
			bi = sp;
			logmsgf(LOGMSG_USER, fp, "count: %4lu pgno: %4lu type: %4lu",
			    (u_long)bi->nrecs, (u_long)bi->pgno,
			    (u_long)bi->type);
			switch (B_TYPE(bi)) {
			case B_KEYDATA:
				__db_pr(bi->data, bi->len, fp);
				break;
			case B_DUPLICATE:
			case B_OVERFLOW:
				__db_proff(bi->data, fp);
				break;
			default:
				logmsgf(LOGMSG_USER, fp, "ILLEGAL BINTERNAL TYPE: %lu\n",
				    (u_long)B_TYPE(bi));
				ret = EINVAL;
				break;
			}
			break;
		case P_IRECNO:
			ri = sp;
			logmsgf(LOGMSG_USER, fp, "entries %4lu pgno %4lu\n",
			    (u_long)ri->nrecs, (u_long)ri->pgno);
			break;
		case P_LBTREE:
		case P_LDUP:
		case P_LRECNO:
			bk = sp;
			switch (B_TYPE(bk)) {
			case B_KEYDATA:
				__db_pr(bk->data, bk->len, fp);
				break;
			case B_DUPLICATE:
			case B_OVERFLOW:
				__db_proff(bk, fp);
				break;
			default:
				logmsgf(LOGMSG_USER, fp,
			    "ILLEGAL DUPLICATE/LBTREE/LRECNO TYPE: %lu\n",
				    (u_long)B_TYPE(bk));
				ret = EINVAL;
				break;
			}
			break;
		default:
type_err:		logmsgf(LOGMSG_USER, fp,
			    "ILLEGAL PAGE TYPE: %lu\n", (u_long)TYPE(h));
			ret = EINVAL;
			continue;
		}
	}
	(void)fflush(fp);
	return (ret);
}

/*
 * __db_pr --
 *	Print out a data element.
 *
 * PUBLIC: void __db_pr __P((u_int8_t *, u_int32_t, FILE *));
 */
void
__db_pr(p, len, fp)
	u_int8_t *p;
	u_int32_t len;
	FILE *fp;
{
	u_int32_t i;
	int lastch;

	logmsgf(LOGMSG_USER, fp, "len: %3lu", (u_long)len);
	lastch = '.';
	if (len != 0) {
		logmsgf(LOGMSG_USER, fp, " data: ");
		char temp[2*len+1];
		util_tohex(temp, (const char*)p, len);
		logmsgf(LOGMSG_USER, fp, "%s", temp);
		/* COMDB2_MODIFICATION: better output with util_tohex
		for (i = len <= 20 ? len : 20; i > 0; --i, ++p) {
			lastch = *p;
			if (isprint((int)*p) || *p == '\n')
				logmsgf(LOGMSG_USER, fp, "%c", *p);
			else
				logmsgf(LOGMSG_USER, fp, "0x%.2x", (u_int)*p);
		}
		if (len > 20) {
			logmsgf(LOGMSG_USER, fp, "...");
			lastch = '.';
		}
		 */
	}
	if (lastch != '\n')
		logmsgf(LOGMSG_USER, fp, "\n");
}

/*
 * __db_prdbt --
 *	Print out a DBT data element.
 *
 * PUBLIC: int __db_prdbt __P((DBT *, int, const char *, void *,
 * PUBLIC:     int (*)(void *, const void *), int, VRFY_DBINFO *));
 */
int
__db_prdbt(dbtp, checkprint, prefix, handle, callback, is_recno, vdp)
	DBT *dbtp;
	int checkprint;
	const char *prefix;
	void *handle;
	int (*callback) __P((void *, const void *));
	int is_recno;
	VRFY_DBINFO *vdp;
{
	static const u_char hex[] = "0123456789abcdef";
	db_recno_t recno;
	size_t len;
	int ret;
#define	DBTBUFLEN	100
	u_int8_t *p, *hp;
	char buf[DBTBUFLEN], hbuf[DBTBUFLEN];

	if (vdp != NULL) {
		/*
		 * If vdp is non-NULL, we might be the first key in the
		 * "fake" subdatabase used for key/data pairs we can't
		 * associate with a known subdb.
		 *
		 * Check and clear the SALVAGE_PRINTHEADER flag;  if
		 * it was set, print a subdatabase header.
		 */
		if (F_ISSET(vdp, SALVAGE_PRINTHEADER))
			(void)__db_prheader(NULL, "__OTHER__", 0, 0,
			    handle, callback, vdp, 0);
		F_CLR(vdp, SALVAGE_PRINTHEADER);
		F_SET(vdp, SALVAGE_PRINTFOOTER);

		/*
		 * Even if the printable flag wasn't set by our immediate
		 * caller, it may be set on a salvage-wide basis.
		 */
		if (F_ISSET(vdp, SALVAGE_PRINTABLE))
			checkprint = 1;
	}

	/*
	 * !!!
	 * This routine is the routine that dumps out items in the format
	 * used by db_dump(1) and db_load(1).  This means that the format
	 * cannot change.
	 */
	if (prefix != NULL && (ret = callback(handle, prefix)) != 0)
		return (ret);
	if (is_recno) {
		/*
		 * We're printing a record number, and this has to be done
		 * in a platform-independent way.  So we use the numeral in
		 * straight ASCII.
		 */
		(void)__ua_memcpy(&recno, dbtp->data, sizeof(recno));
		snprintf(buf, DBTBUFLEN, "%lu", (u_long)recno);

		/* If we're printing data as hex, print keys as hex too. */
		if (!checkprint) {
			for (len = strlen(buf), p = (u_int8_t *)buf,
			    hp = (u_int8_t *)hbuf; len-- > 0; ++p) {
				*hp++ = hex[(u_int8_t)(*p & 0xf0) >> 4];
				*hp++ = hex[*p & 0x0f];
			}
			*hp = '\0';
			ret = callback(handle, hbuf);
		} else
			ret = callback(handle, buf);

		if (ret != 0)
			return (ret);
	} else if (checkprint) {
		for (len = dbtp->size, p = dbtp->data; len--; ++p)
			if (isprint((int)*p)) {
				if (*p == '\\' &&
				    (ret = callback(handle, "\\")) != 0)
					return (ret);
				snprintf(buf, DBTBUFLEN, "%c", *p);
				if ((ret = callback(handle, buf)) != 0)
					return (ret);
			} else {
				snprintf(buf, DBTBUFLEN, "\\%c%c",
				    hex[(u_int8_t)(*p & 0xf0) >> 4],
				    hex[*p & 0x0f]);
				if ((ret = callback(handle, buf)) != 0)
					return (ret);
			}
	} else
		for (len = dbtp->size, p = dbtp->data; len--; ++p) {
			snprintf(buf, DBTBUFLEN, "%c%c",
			    hex[(u_int8_t)(*p & 0xf0) >> 4], hex[*p & 0x0f]);
			if ((ret = callback(handle, buf)) != 0)
				return (ret);
		}

	return (callback(handle, "\n"));
}

/*
 * __db_proff --
 *	Print out an off-page element.
 */
static void
__db_proff(vp, fp)
	void *vp;
	FILE *fp;
{
	BOVERFLOW *bo;

	bo = vp;
	switch (B_TYPE(bo)) {
	case B_OVERFLOW:
		logmsgf(LOGMSG_USER, fp, "overflow: total len: %4lu page: %4lu\n",
		    (u_long)bo->tlen, (u_long)bo->pgno);
		break;
	case B_DUPLICATE:
		logmsgf(LOGMSG_USER, fp, "duplicate: page: %4lu\n", (u_long)bo->pgno);
		break;
	default:
		/* NOTREACHED */
		break;
	}
}

/*
 * __db_prflags --
 *	Print out flags values.
 *
 * PUBLIC: void __db_prflags __P((u_int32_t, const FN *, void *));
 */
void
__db_prflags(flags, fn, vfp)
	u_int32_t flags;
	FN const *fn;
	void *vfp;
{
	FILE *fp;
	const FN *fnp;
	int found;
	const char *sep;

	/*
	 * We pass the FILE * through a void * so that we can use
	 * this function as as a callback.
	 */
	fp = (FILE *)vfp;

	sep = " (";
	for (found = 0, fnp = fn; fnp->mask != 0; ++fnp)
		if (LF_ISSET(fnp->mask)) {
			logmsgf(LOGMSG_USER, fp, "%s%s", sep, fnp->name);
			sep = ", ";
			found = 1;
		}
	if (found)
		logmsgf(LOGMSG_USER, fp, ")");
}

/*
 * __db_dbtype_to_string --
 *	Return the name of the database type.
 * PUBLIC: const char * __db_dbtype_to_string __P((DBTYPE));
 */
const char *
__db_dbtype_to_string(type)
	DBTYPE type;
{
	switch (type) {
	case DB_BTREE:
		return ("btree");
	case DB_HASH:
		return ("hash");
	case DB_RECNO:
		return ("recno");
	case DB_QUEUE:
		return ("queue");
	case DB_UNKNOWN:
	default:
		return ("UNKNOWN TYPE");
	}
	/* NOTREACHED */
}

/*
 * __db_pagetype_to_string --
 *	Return the name of the specified page type.
 */
static const char *
__db_pagetype_to_string(type)
	u_int32_t type;
{
	char *s;

	s = NULL;
	switch (type) {
	case P_BTREEMETA:
		s = "btree metadata";
		break;
	case P_LDUP:
		s = "duplicate";
		break;
	case P_HASH:
		s = "hash";
		break;
	case P_HASHMETA:
		s = "hash metadata";
		break;
	case P_IBTREE:
		s = "btree internal";
		break;
	case P_INVALID:
		s = "invalid";
		break;
	case P_IRECNO:
		s = "recno internal";
		break;
	case P_LBTREE:
		s = "btree leaf";
		break;
	case P_LRECNO:
		s = "recno leaf";
		break;
	case P_OVERFLOW:
		s = "overflow";
		break;
	case P_QAMMETA:
		s = "queue metadata";
		break;
	case P_QAMDATA:
		s = "queue";
		break;
	default:
		/* Just return a NULL. */
		break;
	}
	return (s);
}

/*
 * __db_prheader --
 *	Write out header information in the format expected by db_load.
 *
 * PUBLIC: int	__db_prheader __P((DB *, char *, int, int, void *,
 * PUBLIC:     int (*)(void *, const void *), VRFY_DBINFO *, db_pgno_t));
 */
int
__db_prheader(dbp, subname, pflag, keyflag, handle, callback, vdp, meta_pgno)
	DB *dbp;
	char *subname;
	int pflag, keyflag;
	void *handle;
	int (*callback) __P((void *, const void *));
	VRFY_DBINFO *vdp;
	db_pgno_t meta_pgno;
{
	DBT dbt;
	DB_BTREE_STAT *btsp;
	DB_ENV *dbenv;
	DB_HASH_STAT *hsp;
	DB_QUEUE_STAT *qsp;
	DBTYPE dbtype;
	VRFY_PAGEINFO *pip;
	size_t buflen;
	char *buf;
	int using_vdp, ret, t_ret;

	btsp = NULL;
	hsp = NULL;
	qsp = NULL;
	ret = 0;
	buf = NULL;
	COMPQUIET(buflen, 0);

	/*
	 * If dbp is NULL, then pip is guaranteed to be non-NULL; we only ever
	 * call __db_prheader with a NULL dbp from one case inside __db_prdbt,
	 * and this is a special subdatabase for "lost" items.  In this case
	 * we have a vdp (from which we'll get a pip).  In all other cases, we
	 * will have a non-NULL dbp (and vdp may or may not be NULL depending
	 * on whether we're salvaging).
	 */
	DB_ASSERT(dbp != NULL || vdp != NULL);

	if (dbp == NULL)
		dbenv = NULL;
	else
		dbenv = dbp->dbenv;

	/*
	 * If we've been passed a verifier statistics object, use that;  we're
	 * being called in a context where dbp->stat is unsafe.
	 *
	 * Also, the verifier may set the pflag on a per-salvage basis.  If so,
	 * respect that.
	 */
	if (vdp != NULL) {
		if ((ret = __db_vrfy_getpageinfo(vdp, meta_pgno, &pip)) != 0)
			return (ret);

		if (F_ISSET(vdp, SALVAGE_PRINTABLE))
			pflag = 1;
		using_vdp = 1;
	} else {
		pip = NULL;
		using_vdp = 0;
	}

	/*
	 * If dbp is NULL, make it a btree.  Otherwise, set dbtype to whatever
	 * appropriate type for the specified meta page, or the type of the dbp.
	 */
	if (dbp == NULL)
		dbtype = DB_BTREE;
	else if (using_vdp)
		switch (pip->type) {
		case P_BTREEMETA:
			if (F_ISSET(pip, VRFY_IS_RECNO))
				dbtype = DB_RECNO;
			else
				dbtype = DB_BTREE;
			break;
		case P_HASHMETA:
			dbtype = DB_HASH;
			break;
		case P_QAMMETA:
			dbtype = DB_QUEUE;
			break;
		default:
			/*
			 * If the meta page is of a bogus type, it's because
			 * we have a badly corrupt database.  (We must be in
			 * the verifier for pip to be non-NULL.) Pretend we're
			 * a Btree and salvage what we can.
			 */
			DB_ASSERT(F_ISSET(dbp, DB_AM_VERIFYING));
			dbtype = DB_BTREE;
			break;
	} else
		dbtype = dbp->type;

	if ((ret = callback(handle, "VERSION=3\n")) != 0)
		goto err;
	if (pflag) {
		if ((ret = callback(handle, "format=print\n")) != 0)
			goto err;
	} else if ((ret = callback(handle, "format=bytevalue\n")) != 0)
		goto err;

	/*
	 * 64 bytes is long enough, as a minimum bound, for any of the
	 * fields besides subname.  Subname uses __db_prdbt and therefore
	 * does not need buffer space here.
	 */
	buflen = 64;
	if ((ret = __os_malloc(dbenv, buflen, &buf)) != 0)
		goto err;
	if (subname != NULL) {
		snprintf(buf, buflen, "database=");
		if ((ret = callback(handle, buf)) != 0)
			goto err;
		memset(&dbt, 0, sizeof(dbt));
		dbt.data = subname;
		dbt.size = (u_int32_t)strlen(subname);
		if ((ret = __db_prdbt(&dbt,
		    1, NULL, handle, callback, 0, NULL)) != 0)
			goto err;
	}
	switch (dbtype) {
	case DB_BTREE:
		if ((ret = callback(handle, "type=btree\n")) != 0)
			goto err;
		if (using_vdp) {
			if (F_ISSET(pip, VRFY_HAS_RECNUMS))
				if ((ret =
				    callback(handle, "recnum=1\n")) != 0)
					goto err;
			if (pip->bt_maxkey != 0) {
				snprintf(buf, buflen,
				    "bt_maxkey=%lu\n", (u_long)pip->bt_maxkey);
				if ((ret = callback(handle, buf)) != 0)
					goto err;
			}
			if (pip->bt_minkey != 0 &&
			    pip->bt_minkey != DEFMINKEYPAGE) {
				snprintf(buf, buflen,
				    "bt_minkey=%lu\n", (u_long)pip->bt_minkey);
				if ((ret = callback(handle, buf)) != 0)
					goto err;
			}
			break;
		}
		if ((ret = __db_stat(dbp, &btsp, 0)) != 0) {
			__db_err(dbp->dbenv, "DB->stat: %s", db_strerror(ret));
			goto err;
		}
		if (F_ISSET(dbp, DB_AM_RECNUM))
			if ((ret = callback(handle, "recnum=1\n")) != 0)
				goto err;
		if (btsp->bt_maxkey != 0) {
			snprintf(buf, buflen,
			    "bt_maxkey=%lu\n", (u_long)btsp->bt_maxkey);
			if ((ret = callback(handle, buf)) != 0)
				goto err;
		}
		if (btsp->bt_minkey != 0 && btsp->bt_minkey != DEFMINKEYPAGE) {
			snprintf(buf, buflen,
			    "bt_minkey=%lu\n", (u_long)btsp->bt_minkey);
			if ((ret = callback(handle, buf)) != 0)
				goto err;
		}
		break;
	case DB_HASH:
		if ((ret = callback(handle, "type=hash\n")) != 0)
			goto err;
		if (using_vdp) {
			if (pip->h_ffactor != 0) {
				snprintf(buf, buflen,
				    "h_ffactor=%lu\n", (u_long)pip->h_ffactor);
				if ((ret = callback(handle, buf)) != 0)
					goto err;
			}
			if (pip->h_nelem != 0) {
				snprintf(buf, buflen,
				    "h_nelem=%lu\n", (u_long)pip->h_nelem);
				if ((ret = callback(handle, buf)) != 0)
					goto err;
			}
			break;
		}
		if ((ret = __db_stat(dbp, &hsp, 0)) != 0) {
			__db_err(dbp->dbenv, "DB->stat: %s", db_strerror(ret));
			goto err;
		}
		if (hsp->hash_ffactor != 0) {
			snprintf(buf, buflen,
			    "h_ffactor=%lu\n", (u_long)hsp->hash_ffactor);
			if ((ret = callback(handle, buf)) != 0)
				goto err;
		}
		if (hsp->hash_nkeys != 0) {
			snprintf(buf, buflen,
			    "h_nelem=%lu\n", (u_long)hsp->hash_nkeys);
			if ((ret = callback(handle, buf)) != 0)
				goto err;
		}
		break;
	case DB_QUEUE:
		if ((ret = callback(handle, "type=queue\n")) != 0)
			goto err;
		if (vdp != NULL) {
			snprintf(buf,
			    buflen, "re_len=%lu\n", (u_long)vdp->re_len);
			if ((ret = callback(handle, buf)) != 0)
				goto err;
			break;
		}
		if ((ret = __db_stat(dbp, &qsp, 0)) != 0) {
			__db_err(dbp->dbenv, "DB->stat: %s", db_strerror(ret));
			goto err;
		}
		snprintf(buf, buflen, "re_len=%lu\n", (u_long)qsp->qs_re_len);
		if ((ret = callback(handle, buf)) != 0)
			goto err;
		if (qsp->qs_re_pad != 0 && qsp->qs_re_pad != ' ') {
			snprintf(buf, buflen, "re_pad=%#x\n", qsp->qs_re_pad);
			if ((ret = callback(handle, buf)) != 0)
				goto err;
		}
		if (qsp->qs_extentsize != 0) {
			snprintf(buf, buflen,
			    "extentsize=%lu\n", (u_long)qsp->qs_extentsize);
			if ((ret = callback(handle, buf)) != 0)
				goto err;
		}
		break;
	case DB_RECNO:
		if ((ret = callback(handle, "type=recno\n")) != 0)
			goto err;
		if (using_vdp) {
			if (F_ISSET(pip, VRFY_IS_RRECNO))
				if ((ret =
				    callback(handle, "renumber=1\n")) != 0)
					goto err;
			if (pip->re_len > 0) {
				snprintf(buf, buflen,
				    "re_len=%lu\n", (u_long)pip->re_len);
				if ((ret = callback(handle, buf)) != 0)
					goto err;
			}
			break;
		}
		if ((ret = __db_stat(dbp, &btsp, 0)) != 0) {
			__db_err(dbp->dbenv, "DB->stat: %s", db_strerror(ret));
			goto err;
		}
		if (F_ISSET(dbp, DB_AM_RENUMBER))
			if ((ret = callback(handle, "renumber=1\n")) != 0)
				goto err;
		if (F_ISSET(dbp, DB_AM_FIXEDLEN)) {
			snprintf(buf, buflen,
			    "re_len=%lu\n", (u_long)btsp->bt_re_len);
			if ((ret = callback(handle, buf)) != 0)
				goto err;
		}
		if (btsp->bt_re_pad != 0 && btsp->bt_re_pad != ' ') {
			snprintf(buf, buflen, "re_pad=%#x\n", btsp->bt_re_pad);
			if ((ret = callback(handle, buf)) != 0)
				goto err;
		}
		break;
	case DB_UNKNOWN:
		DB_ASSERT(0);			/* Impossible. */
		__db_err(dbenv, "Impossible DB type in __db_prheader");
		ret = EINVAL;
		goto err;
	}

	if (using_vdp) {
		if (F_ISSET(pip, VRFY_HAS_DUPS))
			if ((ret = callback(handle, "duplicates=1\n")) != 0)
				goto err;
		if (F_ISSET(pip, VRFY_HAS_DUPSORT))
			if ((ret = callback(handle, "dupsort=1\n")) != 0)
				goto err;
		/* We should handle page size. XXX */
	} else {
		if (F_ISSET(dbp, DB_AM_CHKSUM))
			if ((ret = callback(handle, "chksum=1\n")) != 0)
				goto err;
		if (F_ISSET(dbp, DB_AM_DUP))
			if ((ret = callback(handle, "duplicates=1\n")) != 0)
				goto err;
		if (F_ISSET(dbp, DB_AM_DUPSORT))
			if ((ret = callback(handle, "dupsort=1\n")) != 0)
				goto err;
		if (!F_ISSET(dbp, DB_AM_PGDEF)) {
			snprintf(buf, buflen,
			    "db_pagesize=%lu\n", (u_long)dbp->pgsize);
			if ((ret = callback(handle, buf)) != 0)
				goto err;
		}
	}

	if (keyflag && (ret = callback(handle, "keys=1\n")) != 0)
		goto err;

	ret = callback(handle, "HEADER=END\n");

err:	if (using_vdp &&
	    (t_ret = __db_vrfy_putpageinfo(dbenv, vdp, pip)) != 0 && ret == 0)
		ret = t_ret;
	if (btsp != NULL)
		__os_ufree(dbenv, btsp);
	if (hsp != NULL)
		__os_ufree(dbenv, hsp);
	if (qsp != NULL)
		__os_ufree(dbenv, qsp);
	if (buf != NULL)
		__os_free(dbenv, buf);

	return (ret);
}

/*
 * __db_prfooter --
 *	Print the footer that marks the end of a DB dump.  This is trivial,
 *	but for consistency's sake we don't want to put its literal contents
 *	in multiple places.
 *
 * PUBLIC: int __db_prfooter __P((void *, int (*)(void *, const void *)));
 */
int
__db_prfooter(handle, callback)
	void *handle;
	int (*callback) __P((void *, const void *));
{
	return (callback(handle, "DATA=END\n"));
}

/*
 * __db_pr_callback --
 *	Callback function for using pr_* functions from C.
 *
 * PUBLIC: int  __db_pr_callback __P((void *, const void *));
 */
int
__db_pr_callback(handle, str_arg)
	void *handle;
	const void *str_arg;
{
	char *str;
	FILE *f;

	str = (char *)str_arg;
	f = (FILE *)handle;

	logmsgf(LOGMSG_USER, f, "%s", str);

	return (0);
}
