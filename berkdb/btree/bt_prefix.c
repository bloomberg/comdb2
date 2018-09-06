#include <db_config.h>
#include <db_int.h>
#include <dbinc/db_page.h>
#include <dbinc/db_shash.h>
#include <dbinc/log.h>
#include <dbinc/mp.h>
#include <dbinc/db_swap.h>
#include <dbinc/btree.h>
#include <btree/bt_prefix.h>
#include <btree/bt_cache.h>

#include <stdlib.h>
#include <stdarg.h>
#include <alloca.h>
#include <string.h>
#include <strings.h>
#include <signal.h>
#include <assert.h>
#include <comdb2rle.h>
#include <logmsg.h>

struct pfx_type_t {
	uint16_t npfx;		/* pfx size */
	uint16_t nrle;		/* rle size */
	uint16_t nsfx;		/* sfx size */

	uint8_t sfx[2];		/* 12 bits updateid + 4 bits stripe */
	uint8_t *rle;		/* will start at pfx + npfx */
	uint8_t pfx[1];
};


void
print_hex(uint8_t * b, unsigned l, int newline)
{
	static char map[] = "0123456789abcdef";
	int i;

	for (i = 0; i < l; ++i) {
		logmsg(LOGMSG_USER, "%c%c", map[b[i] >> 4], map[b[i] & 0x0f]);
	}
	if (newline)
		logmsg(LOGMSG_USER, "\n");
}

void
inspect_bk(BKEYDATA *bk)
{
	if (bk == NULL) {
		logmsg(LOGMSG_USER, "%s: bk is NULL\n", __func__);
		return;
	}
	db_indx_t len;
	u_int32_t tlen;
	db_pgno_t pgno;
	BOVERFLOW *bo;

	switch (B_TYPE(bk)) {
	case B_KEYDATA:
		ASSIGN_ALIGN(db_indx_t, len, bk->len);
		if (len > KEYBUF) {
			logmsg(LOGMSG_USER, "not printing - too big");
			break;
		}
		print_hex(bk->data, len, 0);
		logmsg(LOGMSG_USER, " [%s%s%s ]", B_PISSET(bk) ? "P" : " ",
		    B_RISSET(bk) ? "R" : " ", B_DISSET(bk) ? "X" : " ");
		break;
	case B_OVERFLOW:
		bo = (BOVERFLOW *)bk;
		ASSIGN_ALIGN(u_int32_t, tlen, bo->tlen);

		ASSIGN_ALIGN(db_pgno_t, pgno, bo->pgno);
		logmsg(LOGMSG_USER, " overflow len:%u pg:%d [  %sO]", tlen, pgno,
		    B_DISSET(bk) ? "X" : " ");
		break;
	default:
		logmsg(LOGMSG_USER,
                "huh weird type here -> type:%d B_TYPE:%d B_TYPE_NOCOMP:%d\n",
		    bk->type, B_TYPE(bk), B_TYPE_NOCOMP(bk));
		break;
	}
}

void
inspect_page_hdr(DB *dbp, PAGE *h)
{
	logmsg(LOGMSG_USER, "file:%s leaf:%s\n"
	    "lsn:%d-%d\t" "pgno:%d\t" "prev:%d\t" "next:%d\n"
	    "hoffset:%d\t" "type:(%u) %u\t" "freespace:%lu\t" "entries:%d\n"
	    "chksum:%s\t" "crypto:%s\t" "\n",
	    dbp->fname, YESNO(ISLEAF(h)),
	    LSN(h).file, LSN(h).offset, PGNO(h), PREV_PGNO(h), NEXT_PGNO(h),
	    HOFFSET(h), TYPE(h), PGTYPE(h), P_FREESPACE(dbp, h), NUM_ENT(h),
	    YESNO(F_ISSET((dbp), DB_AM_CHKSUM)), YESNO(F_ISSET((dbp),
		    DB_AM_ENCRYPT)));

	if (ISLEAF(h) && IS_PREFIX(h)) {
		pfx_t *pfx = pgpfx(dbp, h, alloca(KEYBUF), KEYBUF);

		logmsg(LOGMSG_USER, "key-compression:yes pfx:0x");
		if (pfx->nrle) {
			print_hex(pfx->rle, pfx->nrle, 0);
			logmsg(LOGMSG_USER, " (uncompressed:0x");
		}
		print_hex(pfx->pfx, pfx->npfx, 0);
		if (pfx->nrle)
			logmsg(LOGMSG_USER, ")");
		if (pfx->nsfx) {
			logmsg(LOGMSG_USER, " sfx:0x");
			print_hex(pfx->sfx, pfx->nsfx, 0);
		}
		logmsg(LOGMSG_USER, " ");
	} else {
		logmsg(LOGMSG_USER, "key-compression:no\n");
	}
}

void
inspect_page_dta(DB *dbp, PAGE *h)
{
	logmsg(LOGMSG_USER, "values:");
	for (int i = 0; i < NUM_ENT(h); ++i) {
		BKEYDATA *bk = GET_BKEYDATA(dbp, h, i);

		if (B_TYPE(bk) == B_KEYDATA) {
			uint8_t *a, *b;
			db_indx_t len;

			ASSIGN_ALIGN(db_indx_t, len, bk->len);
			a = (uint8_t *) bk;
			b = (uint8_t *) h;
			if (a < b || (a > b + dbp->pgsize) || len > KEYBUF) {
				logmsg(LOGMSG_USER,
                    "\nthis page don't smell right anymore @i=%u\n",
				    i);
				raise(SIGINT);
				break;
			}
		}
		logmsg(LOGMSG_USER, "\n%3d. ", i);
		inspect_bk(bk);
	}
}

void
inspect_page(DB *dbp, PAGE *h)
{
	DB db;

	if (dbp->fname == NULL) {	// got a dummy dbp
		db = *dbp;
		dbp = &db;
		// all new pages should have checksums
		F_SET(dbp, DB_AM_CHKSUM);
	}
	inspect_page_hdr(dbp, h);
	if (ISLEAF(h)) {
		inspect_page_dta(dbp, h);
	}
	logmsg(LOGMSG_USER, "\n");
}

int
compare_bk(DB *dbp, PAGE *pg1, BKEYDATA *bk1, PAGE *pg2, BKEYDATA *bk2)
{
#ifdef PARANOID
	uint8_t guard[32];

	memset(guard, 0xdb, 32);

	if (B_TYPE(bk1) != B_TYPE(bk2)) {
		logmsg(LOGMSG_ERROR, "%s types don't match\n", __func__);
		return 1;
	}

	switch (B_TYPE(bk1)) {
	case B_KEYDATA: {
		uint8_t buf1[KEYBUF];
		uint8_t buf2[KEYBUF];

		bk_decompress(dbp, pg1, &bk1, buf1, KEYBUF);
		bk_decompress(dbp, pg2, &bk2, buf2, KEYBUF);
		db_indx_t len1, len2;

		ASSIGN_ALIGN(db_indx_t, len1, bk1->len);
		ASSIGN_ALIGN(db_indx_t, len2, bk2->len);
		if (len1 == len2 && bk1->type == bk2->type) {
			int i;
			int rc = memcmp(bk1->data, bk2->data, len1);

			if (rc)
				logmsg(LOGMSG_ERROR, "%s data doesn't match\n",
				    __func__);


			for (i = 0; i < sizeof(guard); ++i) {
				if (guard[i] != 0xdb) {
					logmsg(LOGMSG_ERROR, "%s guard corrupt\n",
					    __func__);
					rc = 1;
				}
			}
			if (rc)
				raise(SIGINT);
			return rc;
		}
		logmsg(LOGMSG_ERROR, "%s len1:%d len2:%d bk1->type:%d bk2->type:%d\n",
		       __func__, len1, len2, bk1->type, bk2->type);
		break;
	}
	case B_OVERFLOW:{
		BOVERFLOW *bo1 = (BOVERFLOW *)bk1;
		BOVERFLOW *bo2 = (BOVERFLOW *)bk2;
		u_int32_t tlen1, tlen2;
		db_pgno_t pgno1, pgno2;
		ASSIGN_ALIGN(u_int32_t, tlen1, bo1->tlen);
		ASSIGN_ALIGN(u_int32_t, tlen2, bo2->tlen);

		ASSIGN_ALIGN(db_pgno_t, pgno1, bo1->pgno);
		ASSIGN_ALIGN(db_pgno_t, pgno2, bo2->pgno);
		if (tlen1 == tlen2 && pgno1 == pgno2)
			return 0;
		break;
	}
	}
	logmsg(LOGMSG_ERROR, "%s len and type don't match\n", __func__);
	return 1;
#else
	return 0;
#endif //PARANOID
}

/*
 * Test that the new page has all the old data.
 */
static int
pfx_verify(DB *dbp, PAGE *orgpg, PAGE *pfxpg)
{
#ifdef PARANOID
	db_indx_t l1, l2;
	BKEYDATA *bk1 = NULL, *bk2 = NULL;
	int i = -1, n = NUM_ENT(pfxpg);

	if (NUM_ENT(orgpg) != n) {
		logmsg(LOGMSG_ERROR, "NUM_ENT don't match");
		goto ugh;
	}
	for (i = 0; i < n; ++i) {
		bk1 = GET_BKEYDATA(dbp, orgpg, i);
		bk2 = GET_BKEYDATA(dbp, pfxpg, i);
		if (compare_bk(dbp, orgpg, bk1, pfxpg, bk2) != 0)
			goto ugh;
	}
	return 0;

ugh:	if (bk1)
		ASSIGN_ALIGN(db_indx_t, l1, bk1->len);
	if (bk2)
		ASSIGN_ALIGN(db_indx_t, l2, bk2->len);
    logmsg(LOGMSG_FATAL, "failed at i:%d bk1->len:%d bk2->len:%d "
	    "bk1->type:%d bk2->type:%d memcmp:%d\n", i,
	    bk1 ? l1 : -1, bk2 ? l2 : -1,
	    bk1 ? bk1->type : -1, bk2 ? bk2->type : -1,
	    (bk1 && bk2) ? memcmp(bk1->data, bk2->data, l1) : -1);
	logmsg(LOGMSG_FATAL, "bk1: ");
	inspect_bk(bk1);
	logmsg(LOGMSG_FATAL, "\n");
	logmsg(LOGMSG_FATAL, "bk2: ");
	inspect_bk(bk2);
    logmsg(LOGMSG_FATAL, "\n");
	logmsg(LOGMSG_FATAL, "org page:");
	inspect_page(dbp, orgpg);
	logmsg(LOGMSG_FATAL, "new page:");
	inspect_page(dbp, pfxpg);
	raise(SIGINT);
	abort();
	return 0;
#else
	return 0;
#endif //PARANOID
}

void
split_check(DB *dbp, PAGE *pp, PAGE *lp, PAGE *rp)
{
#ifdef PARANOID
	if (!IS_PREFIX(pp))
		return;

	BKEYDATA *bk1, *bk2;
	int b1, b2;

	for (b1 = 0; b1 < NUM_ENT(lp); ++b1) {
		bk1 = GET_BKEYDATA(dbp, pp, b1);
		bk2 = GET_BKEYDATA(dbp, lp, b1);
		if (compare_bk(dbp, pp, bk1, lp, bk2) != 0) {
			inspect_bk(bk1);
			inspect_bk(bk2);
			raise(SIGINT);
		}
	}
	int b0 = b1;

	for (b2 = 0; b2 < NUM_ENT(rp); ++b1, ++b2) {
		bk1 = GET_BKEYDATA(dbp, pp, b1);
		bk2 = GET_BKEYDATA(dbp, rp, b2);
		if (compare_bk(dbp, pp, bk1, rp, bk2) != 0) {
			inspect_bk(bk1);
			inspect_bk(bk2);
			raise(SIGINT);
		}
	}
#endif
}

// PUBLIC: void copy_prefix __P((DB *, PAGE *, BKEYDATA *));
void
copy_prefix(DB *dbp, PAGE *page, BKEYDATA *pfx)
{
	db_indx_t pfxlen;

	SET_PREFIX(page);
	ASSIGN_ALIGN(db_indx_t, pfxlen, pfx->len);
	db_indx_t len = BKEYDATA_SIZE_FLUFFLESS(pfxlen);
	db_indx_t t = dbp->pgsize - len;
	db_indx_t *indx = P_PREFIX(dbp, page);

	*indx = t;
	BKEYDATA *newpfx = P_PFXENTRY(dbp, page);

	memcpy(newpfx, pfx, len);
	HOFFSET(page) = t;
}

pfx_t *
pgpfx(DB *dbp, PAGE *h, void *buf, int bsz)
{
	if (!IS_PREFIX(h))
		return NULL;
	BKEYDATA *bk = P_PFXENTRY(dbp, h);

	if (COMP_ISRLE(bk) && COMP_ISSFX(bk))
		raise(SIGINT);

	db_indx_t len;

	ASSIGN_ALIGN(db_indx_t, len, bk->len);

	pfx_t *pfx = buf;

	bzero(pfx, sizeof(pfx_t));

	if (COMP_ISSFX(bk)) {
		pfx->nsfx = COMP_SFX_SZ(bk);
		assert(pfx->nsfx && pfx->nsfx <= sizeof(pfx->sfx));
		len -= pfx->nsfx;
		memcpy(pfx->sfx, &bk->data[len], pfx->nsfx);
	}
	if (COMP_ISRLE(bk)) {
		Comdb2RLE rle = {.in = bk->data,.insz = len,.out = pfx->pfx,
			bsz - offsetof(pfx_t, pfx)
		};
		if (decompressComdb2RLE(&rle) != 0)
			return NULL;

		pfx->npfx = rle.outsz;
		pfx->nrle = len;
		pfx->rle = pfx->pfx + pfx->npfx;
		memcpy(pfx->rle, bk->data, len);
	} else {
		pfx->npfx = len;
		memcpy(pfx->pfx, bk->data, len);
	}
	return pfx;
}

BKEYDATA *
bk_decompress_int(pfx_t * pfx, BKEYDATA *bk, void *buf)
{
	if (!B_PISSET(bk) && !B_RISSET(bk))
		return bk;
	db_indx_t bklen;

	ASSIGN_ALIGN(db_indx_t, bklen, bk->len);
	BKEYDATA *tmp = buf;

	bzero(tmp, sizeof(BKEYDATA));
	if (B_PISSET(bk)) {
		assert(pfx);
		assert(pfx->npfx);
		memcpy(tmp->data, pfx->pfx, pfx->npfx);
		tmp->len += pfx->npfx;
	}
	if (B_RISSET(bk)) {
		Comdb2RLE rle = {.in = bk->data,.insz = bklen,
			.out = tmp->data + tmp->len,.outsz = KEYBUF
		};
		if (decompressComdb2RLE(&rle) != 0)
			return NULL;

		tmp->len += rle.outsz;
	} else {
		memcpy(tmp->data + tmp->len, bk->data, bklen);
		tmp->len += bklen;
	}
	if (B_PISSET(bk) && pfx->nsfx) {
		memcpy(tmp->data + tmp->len, pfx->sfx, pfx->nsfx);
		tmp->len += pfx->nsfx;
	}
	tmp->type = B_TYPE_NOCOMP(bk);
	return tmp;
}

static int
find_pfx(DB *dbp, PAGE *h, pfx_t * pfx)
{
	int i, rc;
	db_indx_t n = NUM_ENT(h);
	BKEYDATA *first = GET_BKEYDATA(dbp, h, 0);
	BKEYDATA *last = GET_BKEYDATA(dbp, h, n - 2);
	pfx_t *oldpfx = NULL;

	if (IS_PREFIX(h)) {
		oldpfx = pgpfx(dbp, h, alloca(KEYBUF), KEYBUF);
		uint8_t *firstbuf = alloca(KEYBUF);
		uint8_t *lastbuf = alloca(KEYBUF);

		first = bk_decompress_int(oldpfx, first, firstbuf);
		last = bk_decompress_int(oldpfx, last, lastbuf);
	}
	db_indx_t min, firstlen, lastlen;

	ASSIGN_ALIGN(db_indx_t, firstlen, first->len);
	ASSIGN_ALIGN(db_indx_t, lastlen, last->len);
	min = firstlen < lastlen ? firstlen : lastlen;
	for (i = 0; i < min && first->data[i] == last->data[i]; ++i);
	if (i == 0
	    || (oldpfx && i == oldpfx->npfx
		&& (memcmp(first->data, oldpfx->pfx, i) == 0))
	    ) {
		rc = 1;
		goto out;
	}

	bzero(pfx, sizeof(pfx_t));
	pfx->npfx = i;
	memcpy(pfx->pfx, first->data, i);

	assert(dbp->compression_flags & DB_PFX_COMP);

	if (dbp->compression_flags & DB_SFX_COMP) {
		assert(!(dbp->compression_flags & DB_RLE_COMP));
		if (firstlen != lastlen) {
			logmsg(LOGMSG_FATAL, "huh");
			raise(SIGINT);
		}
		uint8_t m, *f, *s, *l;

		m = sizeof(pfx->sfx);
		f = s = first->data + firstlen;
		l = last->data + lastlen;
		while ((*(--f) == *(--l)) && m--);
		++f;
		pfx->nsfx = s - f;
		assert(pfx->nsfx <= sizeof(pfx->sfx));
		memcpy(pfx->sfx, f, pfx->nsfx);
	}
	if (dbp->compression_flags & DB_RLE_COMP) {
		assert(!(dbp->compression_flags & DB_SFX_COMP));
		pfx->rle = pfx->pfx + pfx->npfx;
		Comdb2RLE rle = {.in = first->data,.insz = pfx->npfx,
			.out = pfx->rle,.outsz = pfx->npfx / 2
		};
		if (compressComdb2RLE(&rle) == 0)
			pfx->nrle = rle.outsz;
	}
#if 0
	printf("first: 0x");
	print_hex(first->data, first->len, 1);
	printf(" last: 0x");
	print_hex(last->data, last->len, 1);
	printf("  pfx: 0x");
	print_hex(pfx->pfx, pfx->npfx, 0);
	if (pfx->nsfx) {
		printf("%.*s", 2 * (first->len - pfx->npfx - pfx->nsfx),
		    "----------------");
		print_hex(pfx->sfx, pfx->nsfx, 0);
	}
	if (pfx->nrle) {
		printf(" -> 0x");
		print_hex(pfx->rle, pfx->nrle, 0);
	}
	puts("");
#endif

	rc = 0;
out:	return rc;
}

uint64_t num_compressed;
uint64_t free_before_size;
uint64_t free_after_size;
uint64_t total_pgsz;


static int
pfx_log(DBC *dbc, PAGE *h, PAGE *c)
{
	++GET_BH_GEN(h);
	BKEYDATA *p;
	DB *dbp = dbc->dbp;
	DBT pcomp = { 0 }, ncomp;
	int ptype = 0, ntype;

	if (IS_PREFIX(h)) {
		p = P_PFXENTRY(dbp, h);
		ASSIGN_ALIGN_DIFF(u_int32_t, pcomp.size, db_indx_t, p->len);

		pcomp.data = p->data;
		ptype = p->type;
	}

	assert(IS_PREFIX(c));
	p = P_PFXENTRY(dbp, c);
	ASSIGN_ALIGN_DIFF(u_int32_t, ncomp.size, db_indx_t, p->len);
	ncomp.data = p->data;
	ntype = p->type;

	++num_compressed;
	total_pgsz += dbp->pgsize;
	free_before_size += P_FREESPACE(dbp, h);
	free_after_size += P_FREESPACE(dbp, c);

	return __bam_prefix_log(dbp, dbc->txn, &LSN(c), 0, PGNO(c), &LSN(c),
	    &pcomp, &ncomp, ptype, ntype);
}

static inline void
pfx_apply_(DB *dbp, PAGE *h, PAGE *c)
{
	memcpy(h, c, dbp->pgsize);
}

// PUBLIC: int pfx_apply __P((DB *, PAGE *, PAGE *));
int
pfx_apply(DB *dbp, PAGE *h, PAGE *c)
{
	int rc;

	if ((rc = pfx_verify(dbp, h, c) != 0))
		return rc;
	pfx_apply_(dbp, h, c);
	return 0;
}

static int
pfx_remove(DB *dbp, PAGE *src, PAGE *newpage)
{
	int i;

	memcpy(newpage, src, P_OVERHEAD(dbp));
	HOFFSET(newpage) = dbp->pgsize;
	CLR_PREFIX(newpage);

	db_indx_t n = NUM_ENT(src);
	db_indx_t t = HOFFSET(newpage);
	db_indx_t *indx = P_INP(dbp, newpage);
	uint8_t kbuf[KEYBUF], pbuf[KEYBUF];
	pfx_t *pfx = pgpfx(dbp, src, pbuf, KEYBUF);

	for (i = 0; i < n; ++i) {
		BKEYDATA *key = GET_BKEYDATA(dbp, src, i);
		BKEYDATA *p;

		switch (B_TYPE(key)) {
		case B_OVERFLOW:
			t -= BOVERFLOW_SIZE;
			indx[i] = t;
			p = GET_BKEYDATA(dbp, newpage, i);
			memcpy(p, key, BOVERFLOW_SIZE);
			break;
		case B_KEYDATA:
			key = bk_decompress_int(pfx, key, kbuf);
			t -= BKEYDATA_SIZE(key->len);
			indx[i] = t;
			p = GET_BKEYDATA(dbp, newpage, i);
			p->len = key->len;
			B_TSET(p, B_TYPE(key), B_DISSET(key), 0, 0);
			memcpy(p->data, key->data, key->len);
			break;
		default:
			logmsg(LOGMSG_ERROR, "%s: bad type:%d on page:%d at i:%d\n",
			    __func__, B_TYPE(key), PGNO(src), i);
			raise(SIGINT);
			break;
		}
	}
	HOFFSET(newpage) = t;
	return 0;
}

#define SPACE_CHECK_AND_ADJ() 	\
do {				\
	if ((t - at) < need) {	\
		rc = 1;		\
		goto out;	\
	}			\
	t -= need;		\
	at += sizeof(at);	\
	indx[i] = t;		\
	++NUM_ENT(newpage);	\
	HOFFSET(newpage) = t;	\
} while (0)

/* out->data initially points to a buffer which can be used for
 * compression. When this func returns, out->data can be the
 * compression buffer or just a pointer into key->data.
 */
static int
bk_compress_int(pfx_t * pfx, DBT *key, DBT *out)
{
	void *tmp = out->data;

	out->flags = 0;
	out->data = key->data;
	ASSIGN_ALIGN(u_int32_t, out->size, key->size);

	if (key->size > pfx->npfx + pfx->nsfx) {
		uint8_t *p = key->data;
		uint8_t *s = (uint8_t *)key->data + key->size - pfx->nsfx;

		if (memcmp(pfx->pfx, p, pfx->npfx) == 0 &&
		    memcmp(pfx->sfx, s, pfx->nsfx) == 0) {
			out->data = (uint8_t *)key->data + pfx->npfx;
			out->size = key->size - pfx->npfx - pfx->nsfx;
			out->flags |= B_PFX;
		}
	}
	/* if (pfx->nrle) { */
	Comdb2RLE rle = {.in = out->data,.insz = out->size,
		.out = tmp,.outsz = out->size - 1
	};
	if (compressComdb2RLE(&rle) == 0) {
		out->data = tmp;
		out->size = rle.outsz;
		out->flags |= B_RLE;
	}
	/* } */

	return 0;
}

int
bk_compress(DB *dbp, PAGE *h, DBT *key, DBT *out)
{
	pfx_t *pfx = pgpfx(dbp, h, alloca(KEYBUF), KEYBUF);

	return bk_compress_int(pfx, key, out);
}

int pfx_compress_pages(DB *dbp, PAGE *newpage, PAGE *head, pfx_t *pfx, int np, ...)
{
	int rc;
	db_indx_t i, ih, n, s, ip, t, at, *indx;
	PAGE *h;
	va_list pl;
	uint8_t rlebuf[KEYBUF];
	pfx_t *ppfx = NULL;
	uint8_t *buf = NULL;

	s = P_OVERHEAD(dbp);
	memcpy(newpage, head, s);

	BKEYDATA *bkpfx = alloca(pfx_bkeydata_size(pfx));
	copy_prefix(dbp, newpage, pfx_to_bkeydata(pfx, bkpfx));

	t = HOFFSET(newpage);
	indx = P_INP(dbp, newpage);
	/* offset 'at' which next indx will be written */
	at = (uint8_t *) indx - (uint8_t *) newpage + sizeof(at);
	NUM_ENT(newpage) = 0;

	va_start(pl, np);
	for (ip = 0; ip != np; ++ip) {
		h = va_arg(pl, PAGE *);
		n = NUM_ENT(h) + NUM_ENT(newpage);

		if (IS_PREFIX(h)) {
			if (buf == NULL)
				buf = alloca(KEYBUF);
			ppfx = pgpfx(dbp, h, alloca(KEYBUF), KEYBUF);
		}

		for (i = NUM_ENT(newpage), ih = 0; i < n; ++i, ++ih) {
			DBT from;
			void *to;
			int need;
			BKEYDATA *key;
			key = GET_BKEYDATA(dbp, h, ih);
			if (B_TYPE(key) == B_OVERFLOW) {
				from.data = key;
				from.size = need = BOVERFLOW_SIZE;
				SPACE_CHECK_AND_ADJ();
				to = GET_BKEYDATA(dbp, newpage, i);
			} else if (B_TYPE(key) == B_KEYDATA) {
				uint8_t pfxflg = 0, rleflg = 0;
				key = bk_decompress_int(ppfx, key, buf);
				if (ih % 2 != 0) { // only keys share prefix
					from.data = key->data;
					ASSIGN_ALIGN_DIFF(u_int32_t, from.size,
							db_indx_t, key->len);
				} else {
					DBT keydbt;
					keydbt.data = key->data;
					ASSIGN_ALIGN_DIFF(u_int32_t, keydbt.size,
							db_indx_t, key->len);
					from.data = rlebuf;
					bk_compress_int(pfx, &keydbt, &from);
					pfxflg = from.flags & B_PFX;
					rleflg = from.flags & B_RLE;
				}
				need = BKEYDATA_SIZE_FLUFFLESS(from.size);
				SPACE_CHECK_AND_ADJ();
				BKEYDATA *p = GET_BKEYDATA(dbp, newpage, i);
				ASSIGN_ALIGN_DIFF(db_indx_t, p->len, u_int32_t, from.size);
				B_TSET(p, B_TYPE(key), B_DISSET(key), pfxflg, rleflg);
				to = &p->data;
			} else {
				logmsg(LOGMSG_FATAL, "huh weird type here");
				raise(SIGINT);
			}
			memcpy(to, from.data, from.size);
		}
	}
	rc = 0;
out:
	va_end(pl);
	return rc;
}

static inline int
sufficient(DB *dbp, PAGE *h, PAGE *c, uint32_t need)
{
	return P_FREESPACE(dbp, c) > need ? 0 : 1;
}

static int
do_pfx_compress(DB *dbp, PAGE *h, PAGE *c, DBT *comp, uint8_t type)
{
	db_indx_t len = comp->size;
	BKEYDATA bk = {.type = type };
	pfx_t *pfx = alloca(KEYBUF);

	bzero(pfx, sizeof(pfx_t));
	if (COMP_ISSFX(&bk)) {
		pfx->nsfx = COMP_SFX_SZ(&bk);
		len -= pfx->nsfx;
		memcpy(pfx->sfx, (uint8_t *) comp->data + len, pfx->nsfx);
	}
	if (COMP_ISRLE(&bk)) {
		Comdb2RLE rle = {.in = comp->data,.insz = len,
			.out = pfx->pfx,.outsz = KEYBUF
		};
		if (decompressComdb2RLE(&rle) != 0)
			return 1;
		pfx->npfx = rle.outsz;
		pfx->nrle = len;
		pfx->rle = comp->data;
	} else {
		pfx->npfx = len;
		memcpy(pfx->pfx, comp->data, len);
	}

	int rc = pfx_compress_pages(dbp, c, h, pfx, 1, h);
	pfx_verify(dbp, h, c);
	return rc;
}

// PUBLIC: int pfx_compress_redo __P((DB *, PAGE *, PAGE *, __bam_prefix_args *));
int
pfx_compress_redo(DB *dbp, PAGE *h, PAGE *c, __bam_prefix_args * arg)
{
	return do_pfx_compress(dbp, h, c, &arg->newpfx, arg->ntype);
}

// PUBLIC: int pfx_compress_undo __P((DB *, PAGE *, PAGE *, __bam_prefix_args *));
int
pfx_compress_undo(DB *dbp, PAGE *h, PAGE *c, __bam_prefix_args * arg)
{
	if (arg->prvpfx.size)
		return do_pfx_compress(dbp, h, c, &arg->prvpfx, arg->ptype);
	else
		return pfx_remove(dbp, h, c);
}

// PUBLIC: int pfx_compress_pg __P((DBC *, PAGE *, uint32_t));
int
pfx_compress_pg(DBC *dbc, PAGE *h, uint32_t need)
{
	int rc;
	PAGE *c = NULL;
	DB *dbp = dbc->dbp;
	uint8_t buf[KEYBUF + 128];
	pfx_t *pfx = (pfx_t *) buf;

	if ((rc = find_pfx(dbp, h, pfx)) != 0)
		goto out;
	if ((rc = __os_calloc(dbp->dbenv, 1, dbp->pgsize, &c)) != 0)
		goto out;
	if ((rc = pfx_compress_pages(dbp, c, h, pfx, 1, h)) != 0)
		goto out;
	if ((rc = sufficient(dbp, h, c, need)) != 0)
		goto out;
	if ((rc = pfx_verify(dbp, h, c) != 0))
		goto out;
	if ((rc = pfx_log(dbc, h, c)) != 0)
		goto out;
	pfx_apply_(dbp, h, c);
	__memp_fset(dbp->mpf, h, DB_MPOOL_DIRTY);

	rc = 0;
out:	__os_free(dbp->dbenv, c);
	return rc;
}

int
bk_compressed(DB *dbp, PAGE *page, BKEYDATA *bk)
{
	if (IS_PREFIX(page) && B_TYPE(bk) == B_KEYDATA)
		/*
		 * Want actual flags, not
		 * just short circuit ||
		 */
		return B_PISSET(bk) | B_RISSET(bk);

	return 0;
}

/* just decompress - don't add prefix */
int
jdecompress(BKEYDATA *bk, uint8_t * outbuf, db_indx_t outlen, db_indx_t * sz)
{
	db_indx_t inlen;

	ASSIGN_ALIGN(db_indx_t, inlen, bk->len);
	if (B_RISSET(bk)) {
		Comdb2RLE rle = {.in = bk->data,.insz = inlen,.out = outbuf,
			.outsz = outlen
		};
		if (decompressComdb2RLE(&rle) != 0)
			return 1;
		*sz = rle.outsz;
	} else if (inlen > outlen) {
		return 1;
	} else {
		memcpy(outbuf, bk->data, inlen);
		*sz = inlen;
	}
	return 0;
}

int
bk_decompress(DB *dbp, PAGE *pg, BKEYDATA **bk, uint8_t * buf, size_t len)
{
	if (!bk_compressed(dbp, pg, *bk))
		return 0;
	pfx_t *pfx;

	if ((pfx = pgpfx(dbp, pg, alloca(KEYBUF), KEYBUF)) == NULL)
		return 1;

	BKEYDATA *tmp;

	if ((tmp = bk_decompress_int(pfx, *bk, buf)) == NULL)
		return 2;
	*bk = tmp;
	return 0;
}

// PUBLIC: int pfx_bulk_page __P((DBC *, uint8_t *, int32_t *, uint32_t ));
int
pfx_bulk_page(DBC *dbc, uint8_t * np, int32_t *offp, uint32_t space)
{
	BTREE_CURSOR *cp = (BTREE_CURSOR *)dbc->internal;
	DB *dbp = dbc->dbp;
	PAGE *pg = cp->page;
	uint8_t *start = np;
	db_indx_t i;
	db_indx_t n = NUM_ENT(pg);
	pfx_t *pfx = pgpfx(dbp, pg, alloca(KEYBUF), KEYBUF);

	for (i = cp->indx; i < n; ++i) {
		BKEYDATA *bk = GET_BKEYDATA(dbp, pg, i);

		if (B_DISSET(bk))
			continue;

		/* WE NEED AT LEAST THIS MUCH IN ANY CASE (for two offp-s) */
		if (space < (2 * sizeof(*offp)))
			break;
		space -= 2 * sizeof(*offp);

		uint32_t len = 0;
		uint8_t *cur = np;

		if (B_TYPE(bk) == B_OVERFLOW) {
			int ret;
			BOVERFLOW *bo = (BOVERFLOW *)bk;

			ASSIGN_ALIGN(uint32_t, len, bo->tlen);
			if (len > space)
				break;
			db_pgno_t pgno;

			ASSIGN_ALIGN(db_pgno_t, pgno, bo->pgno);
			if ((ret = __bam_bulk_overflow(dbc,
				    len, pgno, cur)) != 0) {
				logmsg(LOGMSG_ERROR,
                        "__bam_bulk_overflow rc:%d pg:%u indx:%u\n",
				    ret, PGNO(pg), i);
				return (ret);
			}
			cur += len;
			space -= len;
		} else if (B_TYPE(bk) == B_KEYDATA) {
			/* PUT THE PREFIX */
			if (B_PISSET(bk)) {
				if (pfx->npfx > space)
					break;
				memcpy(cur, pfx->pfx, pfx->npfx);
				cur += pfx->npfx;
				len += pfx->npfx;
				space -= pfx->npfx;
			}

			/* RLE DECOMPRESS */
			db_indx_t sz;

			if (jdecompress(bk, cur, space, &sz) != 0)
				break;
			cur += sz;
			len += sz;
			space -= sz;

			/* PUT THE SUFFIX */
			if (B_PISSET(bk) && pfx->nsfx) {
				if (pfx->nsfx > space)
					break;
				memcpy(cur, pfx->sfx, pfx->nsfx);
				cur += pfx->nsfx;
				len += pfx->nsfx;
				space -= pfx->nsfx;
			}
		} else {
			logmsg(LOGMSG_ERROR, "%s unknown type:%d pg:%u indx:%u\n",
			    __func__, B_TYPE(bk), PGNO(pg), i);
			return -1;
		}

		/* SET THE POINTERS */
		*offp-- = np - start;
		*offp-- = len;

		/* ALIGN FOR THE NEXT ENTRY */
		np = (uint8_t *) ALIGN((intptr_t)cur, sizeof(int));
		space -= (np - cur);
	}

	if (i < n) {
		/* Insufficient buffer for this page.
		 * Point to last key successfully copied */
		if (i % 2 == 0) {	/* was on a key   */
			i -= 2;
		} else {		/* was on a value */
			i -= 3;
			offp += 2;
		}
	}

	assert((void *)offp > (void *)np);
	*offp = -1;
	cp->indx = i;
	cp->pagelsn = LSN(pg);

	return 0;
}

void
prefix_tocpu(DB *dbp, PAGE *page)
{
	db_indx_t *pfxindex = P_PREFIX(dbp, page);

	P_16_SWAP(pfxindex);
	BKEYDATA *pfxentry = P_PFXENTRY(dbp, page);

	M_16_SWAP(pfxentry->len);
}

void
prefix_fromcpu(DB *dbp, PAGE *page)
{
	db_indx_t *pfxindex = P_PREFIX(dbp, page);
	BKEYDATA *pfxentry = P_PFXENTRY(dbp, page);

	P_16_SWAP(pfxindex);
	M_16_SWAP(pfxentry->len);
}

static int find_common_pfx(DB *dbp, PAGE *p1, PAGE *p2, pfx_t *pfx)
{
	int i, rc;
	pfx_t *rv, *pfx1, *pfx2;
	db_indx_t n, min, i1len, i2len;
	BKEYDATA *i1, *i2;
	u_int8_t *i1buf, *i2buf;

	/* Always zero the structure to protect callers from
	   getting junk data */
	bzero(pfx, sizeof(pfx_t));

	rv = pfx1 = pfx2 = NULL;
	if (PGNO(p2) == NEXT_PGNO(p1)) {
		n = NUM_ENT(p2);
		i1 = GET_BKEYDATA(dbp, p1, 0);
		i2 = GET_BKEYDATA(dbp, p2, n - 2);
	} else {
		n = NUM_ENT(p1);
		i1 = GET_BKEYDATA(dbp, p1, n - 2);
		i2 = GET_BKEYDATA(dbp, p2, 0);
	}

	if (IS_PREFIX(p1)) {
		pfx1 = pgpfx(dbp, p1, alloca(KEYBUF), KEYBUF);
		i1buf = alloca(KEYBUF);
		i1 = bk_decompress_int(pfx1, i1, i1buf);
	}

	if (IS_PREFIX(p2)) {
		pfx2 = pgpfx(dbp, p2, alloca(KEYBUF), KEYBUF);
		i2buf = alloca(KEYBUF);
		i2 = bk_decompress_int(pfx2, i2, i2buf);
	}

	ASSIGN_ALIGN(db_indx_t, i1len, i1->len);
	ASSIGN_ALIGN(db_indx_t, i2len, i2->len);
	min = i1len < i2len ? i1len : i2len;
	for (i = 0; i < min && i1->data[i] == i2->data[i]; ++i)
		;
	if (i == 0) {
		rc = 1;
		goto out;
	}

	/* Prefix */
	assert(dbp->compression_flags & DB_PFX_COMP);
	pfx->npfx = i;
	memcpy(pfx->pfx, i1->data, i);

	/* Suffix */
	if (dbp->compression_flags & DB_SFX_COMP) {
		assert(!(dbp->compression_flags & DB_RLE_COMP));
		if (i1len != i2len)
			abort();
		uint8_t m, *f, *s, *l;
		m = sizeof(pfx->sfx);
		f = s = i1->data + i1len;
		l = i2->data + i2len;
		while ((*(--f) == *(--l)) && m--)
			;
		++f;
		pfx->nsfx = s - f;
		assert(pfx->nsfx <= sizeof(pfx->sfx));
		memcpy(pfx->sfx, f, pfx->nsfx);
	}

	/* RLE */
	if (dbp->compression_flags & DB_RLE_COMP) {
		assert(!(dbp->compression_flags & DB_SFX_COMP));
		pfx->rle = pfx->pfx + pfx->npfx;
		Comdb2RLE rle = { .in = i1->data, .insz = pfx->npfx,
		    .out = pfx->rle, .outsz = pfx->npfx / 2 };
		if (compressComdb2RLE(&rle) == 0)
			pfx->nrle = rle.outsz;
	}
	rc = 0;
out:
	return rc;
}

/* Check if p2 fits in p1.

   (!) The code is based on the assumption that a page can be
       either RLE or SFX compressed but not both. If the assumption is broken,
       The code needs to change accordingly.
   (!) The code is not 100% accruate but gives 0 false positive error.
       that is, if 2 pages fit in 1 page, it may return 'cannot fit' or 'might fit'.
       however if 2 pages don't fit, it will never return 'can fit'.

   It is the caller's responsiblity to validate -
   1) p1 and p2 are btree leaves.
   2) p1 and p2 are adjacent (order does not matter).
   3) p1 and p2 are not empty.
   4) caller must hold write locks. */
int can_fit(DB *dbp, PAGE *p1, PAGE *p2, pfx_t *pfx)
{
	int rc;
	pfx_t *pfx1, *pfx2;
	db_indx_t extra1, extra2, n1, n2, need, have, len, newpfxlen;
	BKEYDATA *bk1, *bk2;

	/* If pages are compressed but the db is configured not to compress,
	   we return false. */
	if (!(dbp->compression_flags & DB_PFX_COMP))
		return BTPFX_FIT_NO;

	pfx1 = pgpfx(dbp, p1, alloca(KEYBUF), KEYBUF);
	pfx2 = pgpfx(dbp, p2, alloca(KEYBUF), KEYBUF);
	extra1 = extra2 = 0;
	rc = find_common_pfx(dbp, p1, p2, pfx);

	if (rc == 0) { /* have common prefix */
		/*
		   1) npfx changed, page is RLE'd - return MAYBE
		   2) npfx changed, page isn't RLE'd but will be RLE'd - do the math with error
		   3) npfx changed, page is SFX'd or will be SFX'd - do the math
		   4) npfx changed, the rest remains uncompressed - do the math
		   5) npfx unchange, nrle changed - return MAYBE
		   6) npfx unchange, nsfx changed - math
		   7) npfx unchange, nrle unchanged, nsfx unchanged - no action
		 */

		if (pfx1 != NULL) {
			if (pfx1->npfx != pfx->npfx) {
				if (pfx1->nrle > 0) { /* #1 */
					return BTPFX_FIT_MAYBE;
				} else if (pfx->nrle > 0) { /* #2 */
					extra1 += pfx->npfx - pfx1->npfx;
				} else if (pfx1->nsfx > 0 || pfx->nsfx > 0) { /* #3 */
					extra1 += pfx->npfx - pfx1->npfx;
					extra1 += pfx->nsfx - pfx1->nsfx;
				} else { /* #4 */
					extra1 += pfx->npfx - pfx1->npfx;
				}
			} else {
				if (pfx1->nrle != pfx->nrle) { /* #5 */
					return BTPFX_FIT_MAYBE;
				} else if (pfx1->nsfx != pfx->nsfx) { /* #6 */
					extra1 += pfx->npfx - pfx1->npfx;
					extra1 += pfx->nsfx - pfx1->nsfx;
				}
				/* #7 */
			}
		}
		if (pfx2 != NULL) {
			if (pfx2->npfx != pfx->npfx) {
				if (pfx2->nrle > 0) { /* #1 */
					return BTPFX_FIT_MAYBE;
				} else if (pfx->nrle > 0) { /* #2 */
					extra1 += pfx->npfx - pfx2->npfx;
				} else if (pfx2->nsfx > 0 || pfx->nsfx > 0) { /* #3 */
					extra1 += pfx->npfx - pfx2->npfx;
					extra1 += pfx->nsfx - pfx2->nsfx;
				} else { /* #4 */
					extra1 += pfx->npfx - pfx2->npfx;
				}
			} else {
				if (pfx2->nrle != pfx->nrle) { /* #5 */
					return BTPFX_FIT_MAYBE;
				} else if (pfx2->nsfx != pfx->nsfx) { /* #6 */
					extra1 += pfx->npfx - pfx2->npfx;
					extra1 += pfx->nsfx - pfx2->nsfx;
				}
				/* #7 */
			}
		}
	} else { /* do not have common prefix */
		/*
		   1) Page RLE'd - ret MAYBE
		   2) Pages will be uncompress - math
		 */
		if (pfx1 != NULL) {
			if (pfx1->nrle > 0) /* #1 */
				return BTPFX_FIT_MAYBE;
			/* #2 */
			extra1 += pfx1->npfx;
			extra1 += pfx1->nsfx;
		}

		if (pfx2 != NULL) {
			if (pfx2->nrle > 0) /* #1 */
				return BTPFX_FIT_MAYBE;
			/* #2 */
			extra2 += pfx2->npfx;
			extra2 += pfx2->nsfx;
		}
	}

	/* Keys only */
	if (extra1 > 0) {
		n1 = NUM_ENT(p1) >> 1;
		extra1 *= n1;
	}

	if (extra2 > 0) {
		n2 = NUM_ENT(p2) >> 1;
		extra2 *= n2;
	}

	/* Records may not share the compression.
	   So even if new compression requies less bytes,
	   we still prepare for the worst case. */
	need = dbp->pgsize - P_OVERHEAD(dbp) - P_FREESPACE(dbp, p2) + (extra2 > 0 ? extra2 : 0);
	have = P_FREESPACE(dbp, p1) - (extra1 > 0 ? extra1 : 0);

	if (rc == 0) { /* Again, have common prefix */
		newpfxlen = 0;
		if (pfx->nrle)
			newpfxlen = pfx->nrle;
		else if (pfx->npfx)
			newpfxlen = pfx->npfx;
		if (pfx->nsfx)
			newpfxlen += pfx->nsfx;
		newpfxlen = BKEYDATA_SIZE_FLUFFLESS(newpfxlen);

		if (!IS_PREFIX(p1)) /* make room for pfx entry */
			have -= newpfxlen + sizeof(db_indx_t);
		else { /* adjust pfx entry */
			bk1 = P_PFXENTRY(dbp, p1);
			ASSIGN_ALIGN(db_indx_t, len, bk1->len);
			have += len - newpfxlen;
		}

		if (IS_PREFIX(p2)) { /* no need to copy over. deduct from need */
			bk2 = P_PFXENTRY(dbp, p2);
			ASSIGN_ALIGN(db_indx_t, len, bk2->len);
			need -= len + sizeof(db_indx_t);
		}
	} else {
		if (IS_PREFIX(p1)) {
			bk1 = P_PFXENTRY(dbp, p1);
			ASSIGN_ALIGN(db_indx_t, len, bk1->len);
			need -= len + sizeof(db_indx_t);
		}

		bk2 = P_PFXENTRY(dbp, p2);
		if (bk2 != NULL) {
			ASSIGN_ALIGN(db_indx_t, len, bk2->len);
			need -= len + sizeof(db_indx_t);
		}
	}


	if (need > have)
		return BTPFX_FIT_NO;

	if (pfx1 != NULL
		&& pfx2 != NULL
		&& pfx->npfx == pfx1->npfx
		&& memcmp(pfx, pfx1, sizeof(pfx_t) + pfx->npfx) == 0
		&& pfx1->npfx == pfx2->npfx
		&& memcmp(pfx1, pfx2, sizeof(pfx_t) + pfx->npfx) == 0)
		return BTPFX_FIT_YES_MEMCPY;

	return BTPFX_FIT_YES_DECOMP;
}

/* Utilities for page compaction and potentially other callers. */
inline u_int32_t pfx_bkeydata_size(pfx_t *pfx)
{
	return BKEYDATA_SIZE_FLUFFLESS(pfx->npfx + pfx->nsfx);
}

BKEYDATA *pfx_to_bkeydata(const pfx_t *pfx, BKEYDATA *bk)
{
	bzero(bk, sizeof(BKEYDATA));
	db_indx_t npfx = pfx->npfx + pfx->nsfx;
	if (pfx->nrle) {
		COMP_SETRLE(bk);
		memcpy(bk->data, pfx->rle, pfx->nrle);
		bk->len = pfx->nrle;
	} else if (pfx->npfx) {
		memcpy(bk->data, pfx->pfx, pfx->npfx);
		bk->len = pfx->npfx;
	}
	if (pfx->nsfx) {
		COMP_SETSFX(bk, pfx->nsfx);
		memcpy(bk->data + bk->len, pfx->sfx, pfx->nsfx);
		bk->len += pfx->nsfx;
	}
	return bk;
}

int dbt_to_pfx(const DBT *comp, u_int8_t type, pfx_t *pfx)
{
	db_indx_t len = comp->size;
	BKEYDATA bk = {.type = type};
	bzero(pfx, sizeof(pfx_t));
	if (COMP_ISSFX(&bk)) {
		pfx->nsfx = COMP_SFX_SZ(&bk);
		len -= pfx->nsfx;
		memcpy(pfx->sfx, (uint8_t*)comp->data + len, pfx->nsfx);
	}
	if (COMP_ISRLE(&bk)) {
		Comdb2RLE rle = {.in = comp->data, .insz = len,
		    .out = pfx->pfx, .outsz = KEYBUF};
		if (decompressComdb2RLE(&rle) != 0)
			return 1;
		pfx->npfx = rle.outsz;
		pfx->nrle = len;
		pfx->rle = comp->data;
	} else {
		pfx->npfx = len;
		memcpy(pfx->pfx, comp->data, len);
	}
	return 0;
}
