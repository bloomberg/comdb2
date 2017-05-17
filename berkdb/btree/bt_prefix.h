#ifndef INCLUDE_BT_PREFIX_H
#define INCLUDE_BT_PREFIX_H

struct __db;
struct _db_page;
struct _bkeydata;

//support for paranoid checks
void split_check(struct __db *, struct _db_page *pp, struct _db_page *lp,
    struct _db_page *rp);

//for db_dup
int bk_compress(struct __db *, struct _db_page *, DBT *in, DBT *out);
int bk_decompress(struct __db *, struct _db_page *, struct _bkeydata **,
    uint8_t * buf, size_t len);
int bk_compressed(struct __db *, struct _db_page *, struct _bkeydata *);

//for split
typedef struct pfx_type_t pfx_t;
pfx_t *pgpfx(struct __db *, struct _db_page *, void *buf, int sz);
struct _bkeydata *bk_decompress_int(pfx_t *, struct _bkeydata *, void *buf);

void prefix_tocpu(struct __db *, struct _db_page *);
void prefix_fromcpu(struct __db *, struct _db_page *);

/* for pgcompact routines */
/* fit? also return new prefix in pfx if fits. */
int can_fit(DB *dbp, PAGE *p1, PAGE *p2, pfx_t *pfx);
/* compress pages. place it here because autogen headers don't have pfx_t definition. */
int pfx_compress_pages(DB *dbp, PAGE *newpage, PAGE *head, pfx_t *pfx, int n, ...);
/* return length */
u_int32_t pfx_bkeydata_size(pfx_t *pfx);
/* convert pfx_t to BKEYDATA */
BKEYDATA *pfx_to_bkeydata(const pfx_t *pfx, BKEYDATA *bk);
/* convert DBT to pfx_t. used during recovery */
int dbt_to_pfx(const DBT *dbt, u_int8_t type, pfx_t *pfx);

// SPEACIAL LOVE FOR SPARC
#ifdef _SUN_SOURCE

#ifdef STATIC_ASSERT
#undef STATIC_ASSERT
#endif

#ifdef PARANOID
#define STATIC_ASSERT(condition)	\
switch (0) {				\
	case 0: break;			\
	case (condition): break;	\
}
#else
#define STATIC_ASSERT(...)
#endif // end PARANOID

#define ASSIGN_ALIGN(type, to, from) 			\
do {							\
	STATIC_ASSERT(sizeof(type) == sizeof(to))	\
	STATIC_ASSERT(sizeof(to) == sizeof(from))	\
	memcpy(&to, &from, sizeof(type));		\
} while (0)

#define ASSIGN_ALIGN_DIFF(totype, to, fromtype, from)	\
do {							\
	fromtype fromtemp;				\
	ASSIGN_ALIGN(fromtype, fromtemp, from);		\
	totype totemp = fromtemp;			\
	ASSIGN_ALIGN(totype, to, totemp);		\
} while (0)
#else // not _SUN_SOURCE
#define ASSIGN_ALIGN(type, to, from) to = from
#define ASSIGN_ALIGN_DIFF(totype, to, fromtype, from) to = from
#endif // end _SUN_SOURCE

#define BTPFX_FIT_YES_MEMCPY     0 /* yes and can bulk copy */
#define BTPFX_FIT_YES_DECOMP     1 /* yes but need to decompress */
#define BTPFX_FIT_MAYBE          2 /* not able to determine unless decompression */
#define BTPFX_FIT_NO             3 /* no. no is no */

#endif // end INCLUDE_BT_PREFIX_H
