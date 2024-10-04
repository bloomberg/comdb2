#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <db.h>
#include <db_int.h>
#include <dbinc/db_swap.h>
#include <dbinc/db_page.h>
#include <dbinc/hash.h>
#include <dbinc/btree.h>
#include <dbinc/log.h>
#include <dbinc/mp.h>
#include <flibc.h>
#include <inttypes.h>
#include "dbinc_auto/dbreg_auto.h"
#include "dbinc_auto/dbreg_ext.h"
#include "dbinc_auto/hash_auto.h"
#include "dbinc_auto/hash_ext.h"
#include "dbinc_auto/mp_ext.h"

#include <crc32c.h>
#include <logmsg.h>

extern void swap_meta(DBMETA *m);
extern const char *type2str(int type);

void inspect_page(DB *, PAGE *);

void prefix_tocpu(DB *dbp, PAGE *page);

static void pg2cpu(DB *dbp, PAGE *p)
{
	if (!F_ISSET(dbp, DB_AM_SWAP))
		return;
	switch (TYPE(p)) {
	case P_HASHMETA:
	case P_BTREEMETA:
	case P_QAMMETA:
		swap_meta((DBMETA *)p);
		return;
	}
	p->lsn.file = 	flibc_intflip(p->lsn.file);
	p->lsn.offset =	flibc_intflip(p->lsn.offset);
	p->pgno = 	flibc_intflip(p->pgno);
	p->prev_pgno = 	flibc_intflip(p->prev_pgno);
	p->next_pgno = 	flibc_intflip(p->next_pgno);
	p->entries = 	flibc_shortflip(p->entries);
        p->hf_offset = 	flibc_shortflip(p->hf_offset);
	if (IS_PREFIX(p))
		prefix_tocpu(dbp, p);
        db_indx_t *inp = P_INP(dbp, p);
	if (ISLEAF(p)) {
		int i;
		for (i = 0; i < NUM_ENT(p); i++) {
			inp[i] = flibc_shortflip(inp[i]);
			BKEYDATA *bk = GET_BKEYDATA(dbp, p, i);
			BOVERFLOW *bo;
			switch (B_TYPE(bk)) {
			case B_KEYDATA:
				bk->len = flibc_shortflip(bk->len);
				break;
			case B_OVERFLOW:
				bo = (BOVERFLOW *)bk;
				bo->pgno = flibc_intflip(bo->pgno);
				bo->tlen = flibc_intflip(bo->tlen);
			}
		}
	} else {
		int i;

		for (i = 0; i < NUM_ENT(p); i++) {
			inp[i] = flibc_shortflip(inp[i]);
			BINTERNAL *bi = GET_BINTERNAL(dbp, p, i);

			bi->len = flibc_shortflip(bi->len);
			bi->pgno = flibc_intflip(bi->pgno);
			bi->nrecs = flibc_intflip(bi->nrecs);
		}
	}
}

static void
dometa(DB *dbp, PAGE *p)
{
	DBMETA *meta = (DBMETA *)p;
	BTMETA *bm;

	logmsg(LOGMSG_USER, "MAGIC:0x%x %s ENDIAN\n", meta->magic,
#if defined(__x86_64) || defined(__x86)
	    F_ISSET(dbp, DB_AM_SWAP) ? "BIG" : "LITTLE"
#else
	    F_ISSET(dbp, DB_AM_SWAP) ? "LITTLE" : "BIG"
#endif
	    );

	switch (TYPE(p)) {
	case P_HASHMETA:
		logmsg(LOGMSG_USER, "LAST_PAGE:%d\n", meta->last_pgno);
		break;
	case P_BTREEMETA:
		bm = (BTMETA *)meta;
		logmsg(LOGMSG_USER, "ROOT:%d\n", bm->root);
		logmsg(LOGMSG_USER, "LAST_PAGE:%d\n", meta->last_pgno);
		logmsg(LOGMSG_USER, "CHKSUM:%s\n", YESNO(F_ISSET(dbp, DB_AM_CHKSUM)));
		break;
	case P_QAMMETA:
		logmsg(LOGMSG_USER, "LAST_PAGE:%d\n", meta->last_pgno);
		break;
	default:
		abort();
	}
}

static void
process_meta(DB *dbp, PAGE *p)
{
	F_CLR(dbp, DB_AM_SWAP);
	DBMETA *meta = (DBMETA *)p;
	uint32_t magic = meta->magic;

again:	switch (magic) {
	case DB_BTREEMAGIC:
	case DB_HASHMAGIC:
	case DB_QAMMAGIC:
	case DB_RENAMEMAGIC:
		break;
	default:
		if (F_ISSET(dbp, DB_AM_SWAP))
			goto bad;
		F_SET(dbp, DB_AM_SWAP);
		magic = flibc_intflip(magic);
		goto again;
	}
	if (F_ISSET(dbp, DB_AM_SWAP))
		swap_meta(meta);
	if (FLD_ISSET(meta->metaflags, DB_AM_CHKSUM))
		F_SET(dbp, DB_AM_CHKSUM);
	dbp->pgsize = meta->pagesize;
	if (meta->pagesize > 64 * 1024)
		dbp->offset_bias = meta->pagesize / (64 * 1024);
	else
		dbp->offset_bias = 1;
	return;
bad:	fprintf(stderr, "BAD META PAGE\n");
	exit(EXIT_FAILURE);
}

#define MIN(A,B) ((A)<(B)?(A):(B))

extern void print_hex(uint8_t * b, unsigned l, int newline);

static void
inspect_internal_page(DB *dbp, PAGE *p)
{
	int i;

	for (i = 0; i < NUM_ENT(p); i++) {
		BINTERNAL *bi = GET_BINTERNAL(dbp, p, i);
        //internal page info: pgno, numrecs if available, subtree leftmost key
        logmsg(LOGMSG_USER, "%d. pgno:%u nrecs:%u lkey:", i, bi->pgno, bi->nrecs);
        print_hex(bi->data, MIN(bi->len, 32), 0);
        logmsg(LOGMSG_USER, "\n");
	}
}

void inspect_page_hdr(DB *, PAGE *);

static void
dopage(DB *dbp, PAGE *p)
{
	/* don't check checksum - pages in cache have that reset to
	 * 0 - it's populated at pageout time */
	// check_chksum(dbp, p);
	/* don't flip, already flipped by __memp_fget's callchain */

	uint8_t type = TYPE(p);

	logmsg(LOGMSG_USER, "PAGE TYPE: %s\n", type2str(type));
	logmsg(LOGMSG_USER, "PAGE LEVEL: %d\n", LEVEL(p));
	switch (type) {
	case P_HASHMETA:
	case P_BTREEMETA:
	case P_QAMMETA:
		dometa(dbp, p);
		break;
	case P_LBTREE:
		inspect_page(dbp, p);
		break;
	case P_IBTREE:
		inspect_internal_page(dbp, p);
		break;
	case P_INVALID:
		inspect_page_hdr(dbp, p);
		break;
	}
}

void
__pgdump(DB_ENV *dbenv, int32_t fileid, uint8_t *ufid, db_pgno_t pgno)
{
	int ret;
	DB *dbp;
	DB_MPOOLFILE *mpf;
	PAGE *pagep;

	/* No transaction because we should already have a dbp open. */
	if (fileid != -1)
		ret = __dbreg_id_to_db(dbenv, NULL, &dbp, fileid, 0, NULL, 0);
	else
		ret = __ufid_to_db(dbenv, NULL, &dbp, ufid, NULL);

	if (ret) {
		fprintf(stderr,
		    "pgdump> __dbreg_id_to_db %" PRIi32 " error=%d\n", fileid,
		    ret);
		return;
	}
	mpf = dbp->mpf;
	ret = __memp_fget(mpf, &pgno, 0, &pagep);
	if (ret) {
		fprintf(stderr,
		    "pgdump> __memp_fget %s pgno %" PRIu32 " %" PRIi32
		    " error=%d\n", dbp->fname, pgno, fileid, ret);
		return;
	}
	logmsg(LOGMSG_USER, "pgdump> %s id %" PRIi32 " page %" PRIu32 "\n", dbp->fname,
	    fileid, pgno);
	dopage(dbp, pagep);
	ret = __memp_fput(mpf, pagep, 0);
	if (ret) {
		fprintf(stderr,
		    "pgdump> mempfput %s pgno %d %" PRIi32 " error=%d\n",
		    dbp->fname, pgno, fileid, ret);
		return;
	}
}

struct pginfo {
	int32_t fileid;
	db_pgno_t pgno;
	uint8_t ufid[0];
};

void
__pgdump_reprec(DB_ENV *dbenv, DBT *dbt)
{
	int32_t fileid;
	db_pgno_t pgno;
	uint8_t *p = dbt->data;

	LOGCOPY_32(&fileid, p);
	p += sizeof(int32_t);
	LOGCOPY_32(&pgno, p);
	p += sizeof(int32_t);
	__pgdump(dbenv, fileid, (dbt->size == sizeof(struct pginfo) ? NULL : p), pgno);
}

void
__pgdumpall(DB_ENV *dbenv, int32_t fileid, uint8_t *ufid, db_pgno_t pgno)
{
	struct pginfo *pg;
    pg = malloc(sizeof(struct pginfo) + DB_FILE_ID_LEN);
    int send_ufid = (pgno == -1);
	DBT dbt = { 0 };
	/* dump locally */
	__pgdump(dbenv, fileid, ufid, pgno);
	LOGCOPY_32(&pg->fileid, &fileid);
	LOGCOPY_32(&pg->pgno, &pgno);
	if (send_ufid)
		memcpy(pg->ufid, ufid, DB_FILE_ID_LEN);

	/* tell other nodes to dump */
	dbt.data = &pg;
	dbt.size = sizeof(struct pginfo);
	if (send_ufid)
		dbt.size += DB_FILE_ID_LEN;

	__rep_send_message(dbenv, db_eid_broadcast, REP_PGDUMP_REQ, NULL, &dbt,
	    DB_REP_NOBUFFER, NULL);
	free(pg);
}

void
__pgtrash(DB_ENV *dbenv, int32_t fileid, db_pgno_t pgno)
{
	int ret;
	DB *dbp;
	DB_MPOOLFILE *mpf;
	PAGE *pagep;

	/* No transaction because we should already have a dbp open. */
	ret = __dbreg_id_to_db(dbenv, NULL, &dbp, fileid, 0, NULL, 0);

	if (ret) {
		fprintf(stderr,
		    "pgdump> __dbreg_id_to_db %" PRIi32 " error=%d\n", fileid,
		    ret);
		return;
	}
	mpf = dbp->mpf;
	ret = __memp_fget(mpf, &pgno, 0, &pagep);
	if (ret) {
		fprintf(stderr,
		    "pgdump> __memp_fget %s pgno %" PRIu32 " %" PRIi32
		    " error=%d\n", dbp->fname, pgno, fileid, ret);
		return;
	}
	if (mpf->fhp) {
		/*bzero(pagep, mpf->fhp->pgsize); */
		memset(pagep, 0, sizeof(DB_LSN));
		fprintf(stderr, "Trashed page %" PRIu32 " in %s.\n", pgno,
		    dbp->fname);
	} else {
		fprintf(stderr, "No filehandle for %s?\n", dbp->fname);
	}
	ret = __memp_fput(mpf, pagep, 0);
	if (ret) {
		fprintf(stderr,
		    "pgdump> mempfput %s pgno %d %" PRIi32 " error=%d\n",
		    dbp->fname, pgno, fileid, ret);
		return;
	}
}

