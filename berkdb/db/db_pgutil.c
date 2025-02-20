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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <crc32c.h>
#include <logmsg.h>

const char *type2str(int type)
{
	switch (type) {
	case P_INVALID: 	return "P_INVALID";
	case __P_DUPLICATE: 	return "__P_DUPLICATE";
	case P_HASH: 		return "P_HASH";
	case P_IBTREE: 		return "P_IBTREE";
	case P_IRECNO: 		return "P_IRECNO";
	case P_LBTREE: 		return "P_LBTREE";
	case P_LRECNO: 		return "P_LRECNO";
	case P_OVERFLOW:	return "P_OVERFLOW";
	case P_HASHMETA: 	return "P_HASHMETA";
	case P_BTREEMETA: 	return "P_BTREEMETA";
	case P_QAMMETA: 	return "P_QAMMETA";
	case P_QAMDATA: 	return "P_QAMDATA";
	case P_LDUP: 		return "P_LDUP";
	case P_PAGETYPE_MAX: 	return "P_PAGETYPE_MAX";
	default:		return "???";
	}
}

void swap_meta(DBMETA *m)
{
	uint8_t *p = (uint8_t *)m;
	SWAP32(p);      /* lsn.file */
	SWAP32(p);      /* lsn.offset */
	SWAP32(p);      /* pgno */
	SWAP32(p);      /* magic */
	SWAP32(p);      /* version */
	SWAP32(p);      /* pagesize */
	p += 4;         /* unused, page type, unused, unused */
	SWAP32(p);      /* free */
	SWAP32(p);      /* alloc_lsn part 1 */
	SWAP32(p);      /* alloc_lsn part 2 */
	SWAP32(p);      /* cached key count */
	SWAP32(p);      /* cached record count */
	SWAP32(p);      /* flags */
	p = (u_int8_t *)m + sizeof(DBMETA);
	SWAP32(p);              /* maxkey */
	SWAP32(p);              /* minkey */
	SWAP32(p);              /* re_len */
	SWAP32(p);              /* re_pad */
	SWAP32(p);              /* root */
	p += 92 * sizeof(u_int32_t); /* unused */
	SWAP32(p);              /* crypto_magic */
}

uint32_t *
getchksump(DB *dbp, PAGE *p)
{
	switch (TYPE(p)) {
	case P_HASH:
	case P_IBTREE:
	case P_IRECNO:
	case P_LBTREE:
	case P_LRECNO:
	case P_OVERFLOW:
		return (uint32_t*)P_CHKSUM(dbp, p);

	case P_HASHMETA:
		return (uint32_t*) &((HMETA *)p)->chksum;

	case P_BTREEMETA:
		return (uint32_t*) &((BTMETA *)p)->chksum;

	case P_QAMMETA:
		return (uint32_t*) &((QMETA *)p)->chksum;

	case P_QAMDATA:
		return (uint32_t*) &((QPAGE *)p)->chksum;

	case P_LDUP:
	case P_INVALID:
	case __P_DUPLICATE:
	case P_PAGETYPE_MAX:
	default:
		return NULL;
	}
}


int
getchksumsz(DB *dbp, PAGE *p)
{
	switch (TYPE(p)) {
	case P_HASHMETA:
	case P_BTREEMETA:
	case P_QAMMETA:
		return DBMETASIZE;
	default:
		return dbp->pgsize;
	}
}

void set_chksum(DB *dbp, PAGE *p)
{
	if (!F_ISSET(dbp, DB_AM_CHKSUM)) {
		return;
	}
	uint32_t *chksump = getchksump(dbp, p);
	if (chksump == NULL) {
		return;
	}
	uint32_t sz = getchksumsz(dbp, p);
	uint32_t chksum =
	    IS_CRC32C(p) ? crc32c((uint8_t *)p, sz) : __ham_func4(dbp, p, sz);
	if (F_ISSET(dbp, DB_AM_SWAP)) {
		chksum = flibc_intflip(chksum);
	}
	*chksump = chksum;
}

/* side-effect: will clear checksum on page */
int verify_chksum(DB *dbp, PAGE *p)
{
	if (!F_ISSET(dbp, DB_AM_CHKSUM)) {
		return 0;
	}
	uint32_t *chksump = getchksump(dbp, p);
	if (chksump == NULL) {
		return 0;
	}
	uint32_t sz = getchksumsz(dbp, p);
	uint32_t chksum = *chksump;
	if (F_ISSET(dbp, DB_AM_SWAP)) {
		chksum = flibc_intflip(chksum);
	}
	*chksump = 0;
	uint32_t computed_chksum =
	    IS_CRC32C(p) ? crc32c((uint8_t *)p, sz) : __ham_func4(dbp, p, sz);
	if (computed_chksum != chksum) {
		fprintf(stderr, "%s checksum mismatch crc32c:%d swapped:%d "
				"expected:%u computed:%u\n",
			__func__, IS_CRC32C(p), F_ISSET(dbp, DB_AM_SWAP),
			chksum, computed_chksum);
		return -1;
	}
	return 0;
}
