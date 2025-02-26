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

#define	SERIAL_INIT	0
static uint32_t fid_serial = SERIAL_INIT;

extern void set_chksum(DB *dbp, PAGE *p);
extern void swap_meta(DBMETA *m);
extern int verify_chksum(DB *dbp, PAGE *p);

int set_new_fileid(const char *fname, int unique_okay, DBMETA *page)
{
	uint8_t *fidp = page->uid;
	struct stat sb;
	size_t i;
	uint32_t tmp;
	uint8_t *p;

	/* Clear the buffer. */
	memset(fidp, 0, DB_FILE_ID_LEN);

	if (stat(fname, &sb) != 0) {
		perror("stat");
		return errno;
	}

	/*
	 * Initialize/increment the serial number we use to help avoid
	 * fileid collisions.  Note that we don't bother with locking;
	 * it's unpleasant to do from down in here, and if we race on
	 * this no real harm will be done, since the finished fileid
	 * has so many other components.
	 *
	 * We increment by 100000 on each call as a simple way of
	 * randomizing;  simply incrementing seems potentially less useful
	 * if pids are also simply incremented, since this is process-local
	 * and we may be one of a set of processes starting up.  100000
	 * pushes us out of pid space on most platforms, and has few
	 * interesting properties in base 2.
	 */
	if (fid_serial == SERIAL_INIT)
		fid_serial = getpid();
	else
		fid_serial += 100000;

	/*
	 * !!!
	 * Nothing is ever big enough -- on Sparc V9, st_ino, st_dev and the
	 * time_t types are all 8 bytes.  As DB_FILE_ID_LEN is only 20 bytes,
	 * we convert to a (potentially) smaller fixed-size type and use it.
	 *
	 * We don't worry about byte sexing or the actual variable sizes.
	 *
	 * When this routine is called from the DB access methods, it's only
	 * called once -- whatever ID is generated when a database is created
	 * is stored in the database file's metadata, and that is what is
	 * saved in the mpool region's information to uniquely identify the
	 * file.
	 *
	 * When called from the mpool layer this routine will be called each
	 * time a new thread of control wants to share the file, which makes
	 * things tougher.  As far as byte sexing goes, since the mpool region
	 * lives on a single host, there's no issue of that -- the entire
	 * region is byte sex dependent.  As far as variable sizes go, we make
	 * the simplifying assumption that 32-bit and 64-bit processes will
	 * get the same 32-bit values if we truncate any returned 64-bit value
	 * to a 32-bit value.  When we're called from the mpool layer, though,
	 * we need to be careful not to include anything that isn't
	 * reproducible for a given file, such as the timestamp or serial
	 * number.
	 */
	tmp = (uint32_t)sb.st_ino;
	for (p = (uint8_t *)&tmp, i = sizeof(uint32_t); i > 0; --i)
		*fidp++ = *p++;

	tmp = (uint32_t)sb.st_dev;
	for (p = (uint8_t *)&tmp, i = sizeof(uint32_t); i > 0; --i)
		*fidp++ = *p++;

	if (unique_okay) {
		/*
		 * We want the number of seconds, not the high-order 0 bits,
		 * so convert the returned time_t to a (potentially) smaller
		 * fixed-size type.
		 */
		tmp = (uint32_t)time(NULL);
		for (p = (uint8_t *)&tmp, i = sizeof(uint32_t); i > 0; --i)
			*fidp++ = *p++;

		for (p = (uint8_t *)&fid_serial, i = sizeof(uint32_t);
		    i > 0; --i)
			*fidp++ = *p++;
	}

	return (0);
}

static int copy_meta(DB *dbp, int in)
{
	uint8_t pagebuf[dbp->pgsize];
	memset(pagebuf, 0, dbp->pgsize);
	PAGE *p = (PAGE *)pagebuf;

	ssize_t n = pread(in, p, dbp->pgsize, 0);
	if (n != dbp->pgsize) {
		fprintf(stderr, "%s bad read n:%ld\n", __func__, n);
		return 1;
	}

	if (verify_chksum(dbp, p) != 0) {
		fprintf(stderr, "%s failed checksum\n", __func__);
		return 1;
	}

	set_new_fileid(dbp->fname, 1, (DBMETA *)p);
	LSN_NOT_LOGGED(LSN(p));
	set_chksum(dbp, p);

	n = pwrite(in, p, dbp->pgsize, 0);
	if (n != dbp->pgsize) {
		fprintf(stderr, "%s bad write n:%ld\n", __func__, n);
		return 1;
	}

	return 0;
}

static int copy_btree(DB *dbp, int in)
{
	int rc = copy_meta(dbp, in);
	if (rc != 0) {
		return -1;
	}
	uint8_t pagebuf[dbp->pgsize];
	memset(pagebuf, 0, dbp->pgsize);
	PAGE *p = (PAGE *)pagebuf;
	size_t count = 0;
	off_t offset = dbp->pgsize; // Advance for meta read
	ssize_t n;
	while ((n = pread(in, p, dbp->pgsize, offset)) == dbp->pgsize) {
		++count;
		if (verify_chksum(dbp, p) != 0) {
			fprintf(stderr, "%s failed checksum page:%lu\n",
				__func__, count);
			return -2;
		}
		LSN_NOT_LOGGED(LSN(p));
		set_chksum(dbp, p);

		n = pwrite(in, p, dbp->pgsize, offset);
		if (n != dbp->pgsize) {
			fprintf(stderr, "%s bad write n:%ld\n", __func__, n);
			return -2;
		}

		offset += dbp->pgsize;
	}
	if (n != 0) {
		perror("read");
		return -3;
	}
	return 0;
}

static int bless_meta(DB *dbp, int fd, char *fname)
{
	uint8_t metabuf[DBMETASIZE];
	ssize_t n;
	if ((n = pread(fd, metabuf, DBMETASIZE, 0)) != DBMETASIZE) {
		if (n == -1)
			perror("pread");
		else
			fprintf(stderr, "short pread n:%ld\n", n);
		return -1;
	}
	DBMETA *meta = (DBMETA *)metabuf;
	uint32_t magic = meta->magic;
again:
	switch (magic) {
	case DB_BTREEMAGIC:
	case DB_HASHMAGIC:
	case DB_QAMMAGIC:
	case DB_RENAMEMAGIC:
		break;
	default:
		if (F_ISSET(dbp, DB_AM_SWAP))
			return -2;
		F_SET(dbp, DB_AM_SWAP);
		magic = flibc_intflip(magic);
		goto again;
	}
	if (F_ISSET(dbp, DB_AM_SWAP)) {
		swap_meta(meta);
	}
	if (FLD_ISSET(meta->metaflags, DBMETA_CHKSUM)) {
		F_SET(dbp, DB_AM_CHKSUM);
	}
	if (meta->encrypt_alg != 0 ||
	    FLD_ISSET(meta->metaflags, DB_AM_ENCRYPT)) {
		fprintf(stderr, "cannot bulkimport encrypted b-trees\n");
		return -3;
	}
	dbp->pgsize = meta->pagesize;
	dbp->fname = fname;
	if (meta->pagesize > 64 * 1024)
		dbp->offset_bias = meta->pagesize / (64 * 1024);
	else
		dbp->offset_bias = 1;
	return 0;
}

int bless_btree(char * input_filename, char * output_filename)
{
	int in = open(input_filename, O_RDWR);
	if (in == -1) {
		perror(input_filename);
		return 1;
	}

	// Need to 'touch' the file because we will use its stat info
	int out = open(output_filename, O_CREAT | O_EXCL, 0666);
	int rc = out != -1 ? Close(out) : 1;
	if (rc) {
		perror(output_filename);
		return 1;
	}

	DB dbp = {0};
	rc = bless_meta(&dbp, in, input_filename);
	if (rc != 0) {
		fprintf(stderr, "bless_meta failed rc:%d\n", rc);
		return 1;
	}

	dbp.fname = output_filename;
	rc = copy_btree(&dbp, in);
	if (rc) {
		fprintf(stderr, "copy failed rc:%d\n", rc);
		return 1;
	}
	Close(in);

	rc = rename(input_filename, output_filename);
	if (rc) {
		fprintf(stderr, "rename failed with errno(%s)\n", strerror(errno));
		return 1;
	}

	return 0;
}
