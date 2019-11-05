/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1997-2003
 *	Sleepycat Software.  All rights reserved.
 */

static int __berkdb_write_alarm_ms;
static int __berkdb_read_alarm_ms;
static long long *__berkdb_num_read_ios = 0;
static long long *__berkdb_num_write_ios = 0;
static void (*read_callback) (int bytes) = 0;
static void (*write_callback) (int bytes) = 0;

int __slow_read_ns = 0;
int __slow_write_ns = 0;

void (*__berkdb_trace_func) (const char *) = 0;

void
__berkdb_set_num_read_ios(long long *n)
{
	__berkdb_num_read_ios = n;
}

void
__berkdb_set_num_write_ios(long long *n)
{
	__berkdb_num_write_ios = n;
}

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: os_rw.c,v 11.30 2003/05/23 21:19:05 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#endif

#include "db_int.h"
#include "dbinc/db_swap.h"
#include "thread_stats.h"
#include "printformats.h"
#include "mem_restore.h"

#include <poll.h>
#include "logmsg.h"
#include "locks_wrap.h"

uint64_t bb_berkdb_fasttime(void);

#ifdef HAVE_FILESYSTEM_NOTZERO
static int __os_zerofill __P((DB_ENV *, DB_FH *));
#endif
static int __os_physwrite __P((DB_ENV *, DB_FH *, void *, size_t, size_t *));

/* NOTE: __berkdb_direct_pread/__berkdb_direct_pwrite assume that read/writes
   are always multiples of 512, which is true for all cases in berkeley */

static pthread_key_t iobufkey;
static pthread_once_t once = PTHREAD_ONCE_INIT;
int gbl_verify_direct_io = 0;

void fsnapf(FILE *, void *, size_t);

struct iobuf {
	size_t sz;
	void *buf;
};

/*
** malloc overrides are disabled in this file in order to 
** 100% correctly free memalign()'d chunks.
*/
void
free_iobuf(void *p)
{
	struct iobuf *b = p;

	free(b->buf);
	free(b);
}

static inline void
init_iobuf(void)
{
	Pthread_key_create(&iobufkey, free_iobuf);
}

static void *
get_aligned_buffer(void *buf, size_t bufsz, int copy)
{
	struct iobuf *b;

	b = pthread_getspecific(iobufkey);
	if (b == NULL) {
		b = malloc(sizeof(struct iobuf));
		b->sz = bufsz;
#if ! defined  ( _SUN_SOURCE ) && ! defined ( _HP_SOURCE )
		if (posix_memalign(&b->buf, 512, bufsz))
			return NULL;
#else
		b->buf = memalign(512, bufsz);
		if (b->buf == NULL)
			return NULL;
#endif
		Pthread_setspecific(iobufkey, b);
	} else if (b->sz < bufsz) {
		free(b->buf);
		b->buf = NULL;

		b->sz = 0;
#if ! defined ( _SUN_SOURCE ) &&  ! defined ( _HP_SOURCE )
		if (posix_memalign(&b->buf, 512, bufsz))
			return NULL;
#else
		b->buf = memalign(512, bufsz);
		if (b->buf == NULL)
			return NULL;
#endif
		b->sz = bufsz;
	}
	if (copy)
		memcpy(b->buf, buf, bufsz);

	return b->buf;
}

static int
__berkdb_pread(int fd, void *buf, size_t bufsz, off_t offset, int direct)
{
	void *abuf;
	int rc;

	pthread_once(&once, init_iobuf);

	if (direct)
		abuf = get_aligned_buffer(buf, bufsz, 0);
	else
		abuf = buf;
	rc = pread(fd, abuf, bufsz, offset);
	if (rc > 0 && buf != abuf)
		memcpy(buf, abuf, rc);
	return rc;
}

static int
__berkdb_pwrite(DB_ENV *dbenv, int fd, void *buf, size_t bufsz,
    off_t offset, int direct)
{
	void *abuf;
	int rc;
	int nretries = 0;
	DB_LSN lsn_before, lsn_after;

	pthread_once(&once, init_iobuf);

	if (direct)
		abuf = get_aligned_buffer(buf, bufsz, 1);
	else
		abuf = buf;
again:
	LOGCOPY_TOLSN(&lsn_before, abuf);
	do {
		rc = pwrite(fd, abuf, bufsz, offset);
		if (rc == -1) {
            int err = errno;
			logmsg(LOGMSG_ERROR, 
				"pwrite fd %d sz %zu off %"PRId64" retry %d %d %s\n", fd,
				bufsz, (int64_t)offset, nretries, err, strerror(err));
			if (err != EINTR && err != EBUSY)
				poll(NULL, 0, 10);
	    }
		if (dbenv->attr.debug_enospc_chance) {
			int p = rand() % 100;

			if (p < dbenv->attr.debug_enospc_chance) {
				rc = -1;
				errno = ENOSPC;
			}
		}
	} while (rc == -1 && ++nretries < dbenv->attr.num_write_retries);
	return rc;
}

/*
 * __os_io_partial --
 *	Do a partial page io.  This is used to test recovery page logging.
 *
 * PUBLIC: int __os_io_partial __P((DB_ENV *,
 * PUBLIC:     int, DB_FH *, db_pgno_t, size_t, size_t, u_int8_t *, size_t *));
 */
int
__os_io_partial(dbenv, op, fhp, pgno, pagesize, parlen, buf, niop)
	DB_ENV *dbenv;
	int op;
	DB_FH *fhp;
	db_pgno_t pgno;
	size_t pagesize, parlen, *niop;
	u_int8_t *buf;
{
	int ret;
	struct timespec s, rem;
	int rc;

	if (op == DB_IO_READ && __slow_read_ns) {
		s.tv_sec = 0;
		s.tv_nsec = __slow_read_ns;
		rc = nanosleep(&s, &rem);
		while (rc == -1 && errno == -EINTR) {
			s = rem;
			rc = nanosleep(&s, &rem);
		}
	} else if (op == DB_IO_WRITE && __slow_write_ns) {
		s.tv_sec = 0;
		s.tv_nsec = __slow_write_ns;
		rc = nanosleep(&s, &rem);
		while (rc == -1 && errno == -EINTR) {
			s = rem;
			rc = nanosleep(&s, &rem);
		}
	}

	/* Check for illegal usage. */
	DB_ASSERT(F_ISSET(fhp, DB_FH_OPENED) && fhp->fd != -1);

	if (op == DB_IO_WRITE)
		__checkpoint_verify(dbenv);

#if defined(HAVE_PREAD) && defined(HAVE_PWRITE)
	switch (op) {
	case DB_IO_READ:
		if (DB_GLOBAL(j_read) != NULL)
			goto slow;


		if (__berkdb_read_alarm_ms) {
			uint64_t x1, x2;

			x1 = bb_berkdb_fasttime();

			*niop = __berkdb_pread(fhp->fd, buf, parlen,
				(off_t) pgno * pagesize, F_ISSET(fhp, DB_FH_DIRECT));

			x2 = bb_berkdb_fasttime();
			if (gbl_bb_berkdb_enable_thread_stats) {
				struct berkdb_thread_stats *p, *t;

				t = bb_berkdb_get_thread_stats();
				p = bb_berkdb_get_process_stats();
				p->n_preads++;
				p->pread_bytes += pagesize;
				p->pread_time_us += (x2 - x1);
				t->n_preads++;
				t->pread_bytes += pagesize;
				t->pread_time_us += (x2 - x1);
			}

			if ((x2 - x1) > M2U(__berkdb_read_alarm_ms) &&
			    __berkdb_trace_func) {
				char s[80];

				snprintf(s, sizeof(s),
				    "LONG PREAD (%d) %d ms fd %d\n",
				    (int)pagesize, U2M(x2 - x1), fhp->fd);
				__berkdb_trace_func(s);
			}
		} 
		else {
			*niop = __berkdb_pread(fhp->fd, buf, parlen,
					(off_t) pgno * pagesize, 
					F_ISSET(fhp, DB_FH_DIRECT));
		}
		if (__berkdb_num_read_ios)
			(*__berkdb_num_read_ios)++;
		if (read_callback)
			read_callback(pagesize);

		break;
	case DB_IO_WRITE:
		if (DB_GLOBAL(j_write) != NULL)
			goto slow;
#ifdef HAVE_FILESYSTEM_NOTZERO
		if (__os_fs_notzero())
			goto slow;
#endif

		if (__berkdb_write_alarm_ms) {
			uint64_t x1, x2;

			x1 = bb_berkdb_fasttime();

			*niop = __berkdb_pwrite(dbenv, fhp->fd, buf,
					parlen, (off_t) pgno * pagesize, 
					F_ISSET(fhp, DB_FH_DIRECT));

			x2 = bb_berkdb_fasttime();
			if (gbl_bb_berkdb_enable_thread_stats) {
				struct berkdb_thread_stats *p, *t;

				t = bb_berkdb_get_thread_stats();
				p = bb_berkdb_get_process_stats();
				p->n_pwrites++;
				p->pwrite_bytes += pagesize;
				p->pwrite_time_us += (x2 - x1);
				t->n_pwrites++;
				t->pwrite_bytes += pagesize;
				t->pwrite_time_us += (x2 - x1);
			}

			if ((x2 - x1) > M2U(__berkdb_write_alarm_ms) &&
			    __berkdb_trace_func) {
				char s[80];

				snprintf(s, sizeof(s),
				    "LONG PWRITE (%d) %d ms fd %d\n",
				    (int)pagesize, U2M(x2 - x1), fhp->fd);
				__berkdb_trace_func(s);
			}
		} else {
			*niop = __berkdb_pwrite(dbenv, fhp->fd, buf,
					parlen, (off_t) pgno * pagesize, 
					F_ISSET(fhp, DB_FH_DIRECT));
		}

		if (__berkdb_num_write_ios)
			(*__berkdb_num_write_ios)++;
		if (write_callback)
			write_callback(pagesize);

		break;
	}
	if (*niop == (size_t) parlen)
		return (0);
    // This is for testing partial writes, so we fully expect partial IO here.
slow:
#endif
	MUTEX_THREAD_LOCK(dbenv, fhp->mutexp);

	if ((ret = __os_seek(dbenv, fhp,
		    pagesize, pgno, 0, 0, DB_OS_SEEK_SET)) != 0)
		goto err;
	switch (op) {
	case DB_IO_READ:
		ret = __os_read(dbenv, fhp, buf, parlen, niop);
		break;
	case DB_IO_WRITE:
		ret = __os_write(dbenv, fhp, buf, parlen, niop);
		break;
	}

err:	MUTEX_THREAD_UNLOCK(dbenv, fhp->mutexp);

	return (ret);

}


/*
 * __os_io --
 *	Do an I/O.
 *
 * PUBLIC: int __os_io __P((DB_ENV *,
 * PUBLIC:     int, DB_FH *, db_pgno_t, size_t, u_int8_t *, size_t *));
 */
int
__os_io(dbenv, op, fhp, pgno, pagesize, buf, niop)
	DB_ENV *dbenv;
	int op;
	DB_FH *fhp;
	db_pgno_t pgno;
	size_t pagesize, *niop;
	u_int8_t *buf;
{
	int ret;
	struct timespec s, rem;
	int rc;

	if (op == DB_IO_READ && __slow_read_ns) {
		s.tv_sec = 0;
		s.tv_nsec = __slow_read_ns;
		rc = nanosleep(&s, &rem);
		while (rc == -1 && errno == -EINTR) {
			s = rem;
			rc = nanosleep(&s, &rem);
		}
	} else if (op == DB_IO_WRITE && __slow_write_ns) {
		s.tv_sec = 0;
		s.tv_nsec = __slow_write_ns;
		rc = nanosleep(&s, &rem);
		while (rc == -1 && errno == -EINTR) {
			s = rem;
			rc = nanosleep(&s, &rem);
		}
	}

	if (op == DB_IO_WRITE && dbenv->attr.check_zero_lsn_writes
	    && (dbenv->open_flags & DB_INIT_TXN)) {
		static const char zerobuf[32];

		/* 
		 * There's some events (aborts) that can legitimately
		 * write a zero LSN - check the first bunch of bytes
		 * in the buffer. 
		 */
		if (memcmp(buf, zerobuf, sizeof(zerobuf)) == 0) {
			if (fhp->name) {
				__db_err(dbenv,
				    "%s %s: zero LSN for page %u",
				    __func__, fhp->name, pgno);
			} else {
				__db_err(dbenv,
				    "%s fd %d: zero LSN for page %u",
				    __func__, fhp->fd, pgno);
			}
			if (dbenv->attr.abort_zero_lsn_writes)
				abort();
		}
	}

	/* Check for illegal usage. */
	DB_ASSERT(F_ISSET(fhp, DB_FH_OPENED) && fhp->fd != -1);

	if (op == DB_IO_WRITE)
		__checkpoint_verify(dbenv);

#if defined(HAVE_PREAD) && defined(HAVE_PWRITE)
	switch (op) {
	case DB_IO_READ:
		if (DB_GLOBAL(j_read) != NULL)
			goto slow;


		if (__berkdb_read_alarm_ms) {
			uint64_t x1, x2;

			x1 = bb_berkdb_fasttime();

			*niop = __berkdb_pread(fhp->fd, buf,
				    pagesize, (off_t) pgno * pagesize,
					F_ISSET(fhp, DB_FH_DIRECT));

			x2 = bb_berkdb_fasttime();
			if (gbl_bb_berkdb_enable_thread_stats) {
				struct berkdb_thread_stats *p, *t;

				t = bb_berkdb_get_thread_stats();
				p = bb_berkdb_get_process_stats();
				p->n_preads++;
				p->pread_bytes += pagesize;
				p->pread_time_us += (x2 - x1);
				t->n_preads++;
				t->pread_bytes += pagesize;
				t->pread_time_us += (x2 - x1);
			}

			if ((x2 - x1) > M2U(__berkdb_read_alarm_ms) &&
			    __berkdb_trace_func) {
				char s[80];

				snprintf(s, sizeof(s),
				    "LONG PREAD (%d) %d ms fd %d\n",
				    (int)pagesize, U2M(x2 - x1), fhp->fd);
				__berkdb_trace_func(s);
			}
		} else {
			*niop =
			    __berkdb_pread(fhp->fd, buf, pagesize,
			    (off_t) pgno * pagesize, F_ISSET(fhp, DB_FH_DIRECT));
		}

		if (__berkdb_num_read_ios)
			(*__berkdb_num_read_ios)++;
		if (read_callback)
			read_callback(pagesize);

		break;
	case DB_IO_WRITE:
		if (DB_GLOBAL(j_write) != NULL)
			goto slow;
#ifdef HAVE_FILESYSTEM_NOTZERO
		if (__os_fs_notzero())
			goto slow;
#endif

		if (__berkdb_write_alarm_ms) {
			uint64_t x1, x2;

			x1 = bb_berkdb_fasttime();

			*niop = __berkdb_pwrite(dbenv, fhp->fd, buf,
					pagesize, (off_t) pgno * pagesize, F_ISSET(fhp, DB_FH_DIRECT));

			x2 = bb_berkdb_fasttime();
			if (gbl_bb_berkdb_enable_thread_stats) {
				struct berkdb_thread_stats *p, *t;

				t = bb_berkdb_get_thread_stats();
				p = bb_berkdb_get_process_stats();
				p->n_pwrites++;
				p->pwrite_bytes += pagesize;
				p->pwrite_time_us += (x2 - x1);
				t->n_pwrites++;
				t->pwrite_bytes += pagesize;
				t->pwrite_time_us += (x2 - x1);
			}

			if ((x2 - x1) > M2U(__berkdb_write_alarm_ms) &&
			    __berkdb_trace_func) {
				char s[80];

				snprintf(s, sizeof(s),
				    "LONG PWRITE (%d) %d ms fd %d\n",
				    (int)pagesize, U2M(x2 - x1), fhp->fd);
				__berkdb_trace_func(s);
			}
		} else {
			*niop = __berkdb_pwrite(dbenv, fhp->fd, buf,
					pagesize, (off_t) pgno * pagesize,
					F_ISSET(fhp, DB_FH_DIRECT));
		}

		if (__berkdb_num_write_ios)
			(*__berkdb_num_write_ios)++;
		if (write_callback)
			write_callback(pagesize);

		break;
	}
	if (*niop == (size_t) pagesize)
		return (0);
	logmsg(LOGMSG_DEBUG, "%s: failed %s io: expected %zd got %zd\n", __func__, op == DB_IO_READ ? "read" : "write", pagesize, *niop);
    // try to do a seek + read/write
slow:
#endif
	MUTEX_THREAD_LOCK(dbenv, fhp->mutexp);

	if ((ret = __os_seek(dbenv, fhp,
		    pagesize, pgno, 0, 0, DB_OS_SEEK_SET)) != 0)
		goto err;
	switch (op) {
	case DB_IO_READ:
		ret = __os_read(dbenv, fhp, buf, pagesize, niop);
		break;
	case DB_IO_WRITE:
		ret = __os_write(dbenv, fhp, buf, pagesize, niop);
		break;
	}

err:	MUTEX_THREAD_UNLOCK(dbenv, fhp->mutexp);

	return (ret);

}

static int __berkdb_read(DB_ENV *dbenv, int fd, void *buf, size_t bufsz, int direct) {
	void *abuf;
	int rc;

	pthread_once(&once, init_iobuf);
	if (direct)
		abuf = get_aligned_buffer(buf, bufsz, 0);
	else
		abuf = buf;
	rc = read(fd, abuf, bufsz);
	if (rc > 0 && buf != abuf)
		memcpy(buf, abuf, rc);
	return rc;
}

static int __berkdb_write(DB_ENV *dbenv, int fd, void *buf, size_t bufsz, int direct) {
	void *abuf;
	int rc;
	int nretries = 0;

	pthread_once(&once, init_iobuf);
	if (direct)
		abuf = get_aligned_buffer(buf, bufsz, 1);
	else
		abuf = buf;
	rc = write(fd, abuf, bufsz);
	if (rc == -1) {
		logmsg(LOGMSG_ERROR, 
				"write fd %d sz %zu retry %d error %d %s\n", fd,
				bufsz, nretries, errno, strerror(errno));
	}
	return rc;
}

/*
 * __os_read --
 *	Read from a file handle.
 *
 * PUBLIC: int __os_read __P((DB_ENV *, DB_FH *, void *, size_t, size_t *));
 */
int
__os_read(dbenv, fhp, addr, len, nrp)
	DB_ENV *dbenv;
	DB_FH *fhp;
	void *addr;
	size_t len;
	size_t *nrp;
{
	size_t offset;
	ssize_t nr;
	int ret, retries;
	u_int8_t *taddr;

	/* Check for illegal usage. */
	DB_ASSERT(F_ISSET(fhp, DB_FH_OPENED) && fhp->fd != -1);

	__checkpoint_verify(dbenv);

	retries = 0;
	for (taddr = addr, offset = 0; offset < len; taddr += nr, offset += nr) {
retry:		if ((nr = DB_GLOBAL(j_read) != NULL ?
			DB_GLOBAL(j_read)(fhp->fd, taddr, len - offset) :
			 __berkdb_read(dbenv, fhp->fd, taddr, len - offset, 
				 F_ISSET(fhp, DB_FH_DIRECT))) < 0) {
			ret = __os_get_errno();
			if ((ret == EINTR || ret == EBUSY) && nr == -1 &&
			    ++retries < DB_RETRY)
				goto retry;
			__db_err(dbenv, "read: %p, %zu: %s",
			    taddr, len - offset, strerror(ret));
			return (ret);
		}
		if (nr == 0)
			break;
	}
	*nrp = taddr - (u_int8_t *)addr;
	return (0);
}

/*
 * __os_write --
 *	Write to a file handle.
 *
 * PUBLIC: int __os_write __P((DB_ENV *, DB_FH *, void *, size_t, size_t *));
 */
int
__os_write(dbenv, fhp, addr, len, nwp)
	DB_ENV *dbenv;
	DB_FH *fhp;
	void *addr;
	size_t len;
	size_t *nwp;
{
	/* Check for illegal usage. */
	DB_ASSERT(F_ISSET(fhp, DB_FH_OPENED) && fhp->fd != -1);

	__checkpoint_verify(dbenv);

#ifdef HAVE_FILESYSTEM_NOTZERO
	/* Zero-fill as necessary. */
	if (__os_fs_notzero()) {
		int ret;
		if ((ret = __os_zerofill(dbenv, fhp)) != 0)
			return (ret);
	}
#endif
	return (__os_physwrite(dbenv, fhp, addr, len, nwp));
}

/*
 * __os_physwrite --
 *	Physical write to a file handle.
 */
static int
__os_physwrite(dbenv, fhp, addr, len, nwp)
	DB_ENV *dbenv;
	DB_FH *fhp;
	void *addr;
	size_t len;
	size_t *nwp;
{
	size_t offset;
	ssize_t nw = 0;
	int ret, retries;
	u_int8_t *taddr;

#if defined(HAVE_FILESYSTEM_NOTZERO) && defined(DIAGNOSTIC)
	if (__os_fs_notzero()) {
		struct stat sb;
		off_t cur_off;

		DB_ASSERT(fstat(fhp->fd, &sb) != -1 &&
		    (cur_off = lseek(fhp->fd, (off_t) 0, SEEK_CUR)) != -1 &&
		    cur_off <= sb.st_size);
	}
#endif
	retries = 0;

	int debug_enospc = dbenv->attr.debug_enospc_chance;
	off_t off = 0;

	for (taddr = addr, offset = 0; offset < len; taddr += nw, offset += nw) {
		if (debug_enospc)
			off = lseek(fhp->fd, (off_t) 0, SEEK_CUR);

retry:
		nw = DB_GLOBAL(j_write) != NULL ?
			DB_GLOBAL(j_write)(fhp->fd, taddr, len - offset) :
			__berkdb_write(dbenv, fhp->fd, taddr, len - offset,
					F_ISSET(fhp, DB_FH_DIRECT));
		if (debug_enospc && off >= 0) {
			int p = rand() % 100;

			if (p < dbenv->attr.debug_enospc_chance && nw > 0) {
				off_t rc = lseek(fhp->fd, off, SEEK_SET);
				if (rc == off) {
					nw = -1;
					errno = ENOSPC;
				}
				else {
					/* We got positioned into the limbo somewhere (how?)
					 * so it's not safe to proceed. */
					__db_err(dbenv, "lseek: fd %d off %"PRId64", wanted %"PRId64"\n", fhp->fd,
							(int64_t)rc, (int64_t)off);
					return errno;
				}
			}
		}
		if (nw < 0) {
			ret = __os_get_errno();
			logmsg(LOGMSG_WARN, "%s write fd %d sz %d retry %d err %d %s\n",
					__func__, fhp->fd, (int) (len - offset), retries, ret, strerror(ret));
			if (++retries < dbenv->attr.num_write_retries) {
				if (ret != EINTR && ret != EBUSY)
					poll(NULL, 0, 10);
				goto retry;
			}
			return (ret);
		}
	}
	*nwp = len;
	return (0);
}

#if defined(HAVE_PREAD) && defined(HAVE_PWRITE)
static int
__berkdb_direct_preadv(int fd,
    size_t pagesize, u_int8_t **bufs, size_t nobufs, off_t offset)
{
	void *abuf;
	int rc, i;

	pthread_once(&once, init_iobuf);

	abuf = get_aligned_buffer(NULL, nobufs * pagesize, 0);

	rc = pread(fd, abuf, nobufs * pagesize, offset);
	if (rc > 0) {
		for (i = 0; i < nobufs; i++) {
			if ((i + 1) * pagesize > rc) {
				memcpy(bufs[i],
				    (uint8_t *) abuf + (i * pagesize),
				    rc - i * pagesize);
				break;
			}
			memcpy(bufs[i],
			    (uint8_t *) abuf + (i * pagesize), pagesize);
		}
	}
	return rc;
}

static int
__berkdb_direct_pwritev(DB_ENV *dbenv,
    int fd, size_t pagesize, u_int8_t **bufs, size_t nobufs, off_t offset)
{
	void *abuf;
	int rc, i;
	int nretries = 0;

	pthread_once(&once, init_iobuf);

	abuf = get_aligned_buffer(NULL, nobufs * pagesize, 0);

	for (i = 0; i < nobufs; i++) {
		memcpy((uint8_t *) abuf + (i * pagesize), bufs[i], pagesize);
	}

	do {
		rc = pwrite(fd, abuf, nobufs * pagesize, offset);
		int err = errno;
		if (rc == -1) {
			logmsg(LOGMSG_WARN, "pwrite fd %d sz %d off %"PRId64" retry %d error %d %s\n",
			    fd, (int)(nobufs * pagesize), (int64_t)offset, nretries, err, strerror(err));
				if (err != EINTR && err != EBUSY)
						poll(NULL, 0, 10);
		}
		if (dbenv->attr.debug_enospc_chance) {
			int p = rand() % 100;

			if (p < dbenv->attr.debug_enospc_chance) {
				rc = -1;
				errno = ENOSPC;
			}
		}
	} while (rc == -1 && ++nretries < dbenv->attr.num_write_retries);
	return rc;
}
#endif

/*
 * __os_iov --
 *      Write a vector of data. Useful for skipping mpool buffer headers.
 *
 * PUBLIC: int __os_iov __P((DB_ENV *, int, DB_FH *, db_pgno_t, size_t,
 * PUBLIC:     u_int8_t **, size_t, size_t *));
 */
int
__os_iov(dbenv, op, fhp, pgno, pagesize, bufs, nobufs, niop)
	DB_ENV *dbenv;
	int op;
	DB_FH *fhp;
	db_pgno_t pgno;
	size_t pagesize, nobufs, *niop;
	u_int8_t **bufs;
{
	int ret = 0, i;
	db_pgno_t c_pgno;
	size_t single_niop, max_bufs;
	struct timespec s, rem;
	int rc;


#if defined(HAVE_PREAD) && defined(HAVE_PWRITE)
	if (op == DB_IO_READ && __slow_read_ns) {
		s.tv_sec = 0;
		s.tv_nsec = __slow_read_ns;
		rc = nanosleep(&s, &rem);
		while (rc == -1 && errno == -EINTR) {
			s = rem;
			rc = nanosleep(&s, &rem);
		}
	} else if (op == DB_IO_WRITE && __slow_write_ns) {
		s.tv_sec = 0;
		s.tv_nsec = __slow_write_ns;
		rc = nanosleep(&s, &rem);
		while (rc == -1 && errno == -EINTR) {
			s = rem;
			rc = nanosleep(&s, &rem);
		}
	}

	if (!F_ISSET(fhp, DB_FH_DIRECT))
		goto slow;
	if (nobufs == 1)
		goto slow;

	if (op == DB_IO_WRITE && dbenv->attr.check_zero_lsn_writes
	    && (dbenv->open_flags & DB_INIT_TXN)) {
		static const char zerobuf[32];

		/* 
		 * There's some events (aborts) that can legitimately
		 * write a zero LSN - check the first bunch of bytes
		 * in the buffer. 
		 */
		for (i = 0; i < nobufs; i++) {
			if (memcmp(bufs[i], zerobuf, sizeof(zerobuf)) == 0)
				break;
		}
		if (i < nobufs) {
			if (fhp->name) {
				__db_err(dbenv,
				    "%s %s: zero LSN for page %u",
				    __func__, fhp->name, pgno + i);
			} else {
				__db_err(dbenv,
				    "%s fd %d: zero LSN for page %u",
				    __func__, fhp->fd, pgno + i);
			}
			if (dbenv->attr.abort_zero_lsn_writes)
				abort();
		}
	}

	/* Check for illegal usage. */
	DB_ASSERT(F_ISSET(fhp, DB_FH_OPENED) &&
	    fhp->fd != -1 && DB_GLOBAL(j_read) != NULL);

	uint64_t x1 = 0, x2;

	max_bufs = nobufs;
	*niop = 0;
	if (max_bufs > dbenv->attr.sgio_max / pagesize)
		max_bufs = dbenv->attr.sgio_max / pagesize;

	switch (op) {
	case DB_IO_READ:
		if (__berkdb_read_alarm_ms)
			x1 = bb_berkdb_fasttime();

		c_pgno = pgno;
		do {
			single_niop = __berkdb_direct_preadv(fhp->fd,
			    pagesize,
			    bufs, max_bufs, (off_t)(c_pgno * pagesize));
			*niop += single_niop;
			c_pgno += max_bufs;
			bufs += max_bufs;

			if (nobufs < (*niop / pagesize) + max_bufs)
				max_bufs = nobufs % max_bufs;

			if (__berkdb_num_read_ios)
				(*__berkdb_num_read_ios)++;

		} while (single_niop > 0 && *niop < nobufs * pagesize);

		if (__berkdb_read_alarm_ms) {
			x2 = bb_berkdb_fasttime();
			if (gbl_bb_berkdb_enable_thread_stats) {
				struct berkdb_thread_stats *p, *t;

				t = bb_berkdb_get_thread_stats();
				p = bb_berkdb_get_process_stats();
				p->n_preads++;
				p->pread_bytes += *niop;
				p->pread_time_us += (x2 - x1);
				t->n_preads++;
				t->pread_bytes += *niop;
				t->pread_time_us += (x2 - x1);
			}

			if ((x2 - x1) > M2U(__berkdb_read_alarm_ms) &&
			    __berkdb_trace_func) {
				char s[80];

				snprintf(s, sizeof(s),
				    "LONG PREADV (%d) %d ms "
				    "fd %d\n", (int)(*niop), U2M(x2 - x1), fhp->fd);
				__berkdb_trace_func(s);
			}
		}

		if (read_callback)
			read_callback(*niop);

		break;

	case DB_IO_WRITE:
		__checkpoint_verify(dbenv);

		if (__berkdb_write_alarm_ms)
			x1 = bb_berkdb_fasttime();

		c_pgno = pgno;
		do {
			single_niop = __berkdb_direct_pwritev(dbenv,
			    fhp->fd,
			    pagesize,
			    bufs, max_bufs, (off_t)(c_pgno * pagesize));
			*niop += single_niop;
			c_pgno += max_bufs;
			bufs += max_bufs;

			if (nobufs < (*niop / pagesize) + max_bufs)
				max_bufs = nobufs % max_bufs;

			if (__berkdb_num_write_ios)
				(*__berkdb_num_write_ios)++;


		} while (single_niop != 0 && *niop < nobufs * pagesize);


		if (__berkdb_write_alarm_ms) {
			x2 = bb_berkdb_fasttime();
			if (gbl_bb_berkdb_enable_thread_stats) {
				struct berkdb_thread_stats *p, *t;

				t = bb_berkdb_get_thread_stats();
				p = bb_berkdb_get_process_stats();
				p->n_pwrites++;
				p->pwrite_bytes += nobufs * pagesize;
				p->pwrite_time_us += (x2 - x1);
				t->n_pwrites++;
				t->pwrite_bytes += nobufs * pagesize;
				t->pwrite_time_us += (x2 - x1);
			}

			if ((x2 - x1) > M2U(__berkdb_write_alarm_ms)
			    && __berkdb_trace_func) {
				char s[80];

				snprintf(s, sizeof(s),
				    "LONG PWRITEV (%d) %d ms "
				    " fd %d\n",
				    (int)(nobufs * pagesize), U2M(x2 - x1), fhp->fd);
				__berkdb_trace_func(s);
			}
		}

		if (write_callback)
			write_callback(nobufs * pagesize);

		break;
	}

	if (*niop == (size_t)(pagesize * nobufs))
		return (0);
	logmsg(LOGMSG_DEBUG, "%s: failed %s io: expected %zd got %zd\n", __func__, op == DB_IO_READ ? "read" : "write", pagesize * nobufs, *niop);
    // iov - we failed to write the pages as a unit, fall through and try them individually
slow:
#endif
	/* TODO: __os_pwritev/__os_readv */
	*niop = 0;
	single_niop = 0;

	for (i = 0; i < nobufs; i++) {
		ret = __os_io(dbenv, op, fhp, pgno + i,
		    pagesize, bufs[i], &single_niop);
		*niop += single_niop;

		if (ret != 0)
			break;
	}

	return (ret);
}

/*
 * __os_truncate --
 *	Truncate a file
 *
 * PUBLIC: int __os_truncate __P((DB_ENV *, DB_FH *, off_t offset));
 */
int
__os_truncate(dbenv, fhp, offset)
	DB_ENV *dbenv;
	DB_FH *fhp;
    off_t offset;
{
	int rc;
	DB_ASSERT(F_ISSET(fhp, DB_FH_OPENED) && fhp->fd != -1);
	MUTEX_THREAD_LOCK(dbenv, fhp->mutexp);
	rc = ftruncate(fhp->fd, offset);
	MUTEX_THREAD_UNLOCK(dbenv, fhp->mutexp);
	if (rc == -1) {
		logmsg(LOGMSG_ERROR, "ftruncate(%u) %d %s\n",
			(unsigned int)offset, errno, strerror(errno));
		return -1;
	}
	return rc;
}

#ifdef HAVE_FILESYSTEM_NOTZERO
/*
 * __os_zerofill --
 *	Zero out bytes in the file.
 *
 *	Pages allocated by writing pages past end-of-file are not zeroed,
 *	on some systems.  Recovery could theoretically be fooled by a page
 *	showing up that contained garbage.  In order to avoid this, we
 *	have to write the pages out to disk, and flush them.  The reason
 *	for the flush is because if we don't sync, the allocation of another
 *	page subsequent to this one might reach the disk first, and if we
 *	crashed at the right moment, leave us with this page as the one
 *	allocated by writing a page past it in the file.
 */
static int
__os_zerofill(dbenv, fhp)
	DB_ENV *dbenv;
	DB_FH *fhp;
{
	off_t stat_offset, write_offset;
	size_t blen, nw;
	u_int32_t bytes, mbytes;
	int group_sync, need_free, ret;
	u_int8_t buf[8 * 1024], *bp;

	/* Calculate the byte offset of the next write. */
	write_offset = (off_t)fhp->pgno * fhp->pgsize + fhp->offset;

	/* Stat the file. */
	if ((ret = __os_ioinfo(dbenv, NULL, fhp, &mbytes, &bytes, NULL)) != 0)
		return (ret);
	stat_offset = (off_t)mbytes * MEGABYTE + bytes;

	/* Check if the file is large enough. */
	if (stat_offset >= write_offset)
		return (0);

	/* Get a large buffer if we're writing lots of data. */
#undef	ZF_LARGE_WRITE
#define	ZF_LARGE_WRITE	(64 * 1024)
	if (write_offset - stat_offset > ZF_LARGE_WRITE) {
		if ((ret = __os_calloc(dbenv, 1, ZF_LARGE_WRITE, &bp)) != 0)
			    return (ret);
		blen = ZF_LARGE_WRITE;
		need_free = 1;
	} else {
		bp = buf;
		blen = sizeof(buf);
		need_free = 0;
		memset(buf, 0, sizeof(buf));
	}

	/* Seek to the current end of the file. */
	if ((ret = __os_seek(
	    dbenv, fhp, MEGABYTE, mbytes, bytes, 0, DB_OS_SEEK_SET)) != 0)
		goto err;

	/*
	 * Hash is the only access method that allocates groups of pages.  Hash
	 * uses the existence of the last page in a group to signify the entire
	 * group is OK; so, write all the pages but the last one in the group,
	 * flush them to disk, then write the last one to disk and flush it.
	 */
	for (group_sync = 0; stat_offset < write_offset; group_sync = 1) {
		if (write_offset - stat_offset <= blen) {
			blen = (size_t)(write_offset - stat_offset);
			if (group_sync && (ret = __os_fsync(dbenv, fhp)) != 0)
				goto err;
		}
		if ((ret = __os_physwrite(dbenv, fhp, bp, blen, &nw)) != 0)
			goto err;
		stat_offset += blen;
	}
	if ((ret = __os_fsync(dbenv, fhp)) != 0)
		goto err;

	/* Seek back to where we started. */
	mbytes = (u_int32_t)(write_offset / MEGABYTE);
	bytes = (u_int32_t)(write_offset % MEGABYTE);
	ret = __os_seek(dbenv, fhp, MEGABYTE, mbytes, bytes, 0, DB_OS_SEEK_SET);

err:	if (need_free)
		__os_free(dbenv, bp);
	return (ret);
}
#endif



#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/time.h>
#include <pthread.h>
#include <time.h>
#include <poll.h>


#if _IBM_SOURCE
#include <sys/systemcfg.h>
#endif


uint64_t
bb_berkdb_fasttime(void)
{
#if defined(_AIX)

	timebasestruct_t hr_time;
	long long absolute_time = 0, hr_ustime = 0;

	read_real_time(&hr_time, TIMEBASE_SZ);
	time_base_to_time(&hr_time, TIMEBASE_SZ);
	absolute_time = (long long)hr_time.tb_high * 1000000000LL
	    + (long long)hr_time.tb_low;
	hr_ustime = absolute_time / 1000LL;

	return hr_ustime;

#elif defined(__sun)

	hrtime_t hr_time = 0;
	hrtime_t hr_ustime = 0;

	hr_time = gethrtime();
	hr_ustime = hr_time / 1000LL;

	return hr_ustime;

#elif defined(__linux__) || defined(__APPLE__)
	struct timeval tp;
	long long absolute_time;
	gettimeofday(&tp, NULL);

	absolute_time = (tp.tv_sec * 1000000LL) + tp.tv_usec;

	return absolute_time;

#elif defined(__hpux)

	hrtime_t hr_time = 0;
	hrtime_t hr_ustime = 0;

	hr_time = gethrtime();
	hr_ustime = hr_time / 1000LL;

	return hr_ustime;

#else
	#error "need a way to get fast time!"
#endif
}


void
berk_write_alarm_ms(int x)
{
	logmsg(LOGMSG_INFO, "__berkdb_write_alarm_ms = %d\n", x);
	__berkdb_write_alarm_ms = x;
}

void
berk_read_alarm_ms(int x)
{
	logmsg(LOGMSG_INFO, "__berkdb_read_alarm_ms = %d\n", x);
	__berkdb_read_alarm_ms = x;
}

void
berk_set_long_trace_func(void (*func) (const char *msg))
{
	__berkdb_trace_func = func;
}

int gbl_bb_berkdb_enable_thread_stats = 1;
int gbl_bb_berkdb_enable_lock_timing = 1;
int gbl_bb_berkdb_enable_memp_timing = 0;	/* disable by default now, 
						 * hopefully lock, pread, and 
						 * memp_pg timing will be enough */
int gbl_bb_berkdb_enable_memp_pg_timing = 1;
int gbl_bb_berkdb_enable_shalloc_timing = 1;
static int inited = 0;
static pthread_key_t berkdb_thread_stats_key;

void
bb_berkdb_thread_stats_init(void)
{
	Pthread_key_create(&berkdb_thread_stats_key, free);
	inited = 1;
}

struct berkdb_thread_stats *
bb_berkdb_get_thread_stats(void)
{
	static struct berkdb_thread_stats junk;
	struct berkdb_thread_stats *p;

#ifndef TESTSUITE
	if (inited) {
		p = pthread_getspecific(berkdb_thread_stats_key);
		if (!p) {
			p = calloc(1, sizeof(struct berkdb_thread_stats));
			Pthread_setspecific(berkdb_thread_stats_key, p);
		}
		if (!p)
			p = &junk;
	} else
#endif
	{
		p = &junk;
	}
	return p;
}

void
bb_berkdb_thread_stats_reset(void)
{
	bzero(bb_berkdb_get_thread_stats(),
	    sizeof(struct berkdb_thread_stats));
}

struct berkdb_thread_stats *
bb_berkdb_get_process_stats(void)
{
	static struct berkdb_thread_stats s = { 0 };
	return &s;
}

void bb_berkdb_reset_worst_lock_wait_time_us(void)
{
	bb_berkdb_get_process_stats()->worst_lock_wait_time_us = 0;
}

void
__berkdb_register_read_callback(void (*callback) (int bytes))
{
	read_callback = callback;
}

void
__berkdb_register_write_callback(void (*callback) (int bytes))
{
	write_callback = callback;
}
