#include "db_config.h"
#include "db_int.h"
#include "dbinc/btree.h"
#include "dbinc/mp.h"
#include "dbinc/log.h"
#include "dbinc/txn.h"
#include "dbinc/db_swap.h"
#include "dbinc/lock.h"
#include "dbinc/mutex.h"
#include "btree/bt_cache.h"
#include "dbinc/db_shash.h"
#include "dbinc/hmac.h"
#include "dbinc_auto/hmac_ext.h"

#define PAGE_VERSION_IS_GUARANTEED_TARGET(highest_checkpoint_lsn, smallest_logfile, target_lsn, pglsn) \
		(log_compare(&highest_checkpoint_lsn, &pglsn) > 0 || IS_NOT_LOGGED_LSN(pglsn) || (pglsn.file < smallest_logfile))

#define __mempv_debug_format(id, msg, ...) \
		"%s -- [ID %llu] " msg, __func__, id, ## __VA_ARGS__

#define __mempv_logmsg(lvl, id, msg, ...) \
		logmsg(lvl, __mempv_debug_format(id, msg, ## __VA_ARGS__))

extern char *optostr(int op);

extern int __txn_commit_map_get(DB_ENV *, u_int64_t, DB_LSN *);

extern int __mempv_cache_init(DB_ENV *, MEMPV_CACHE *cache);
extern int __mempv_cache_get(DB *dbp, MEMPV_CACHE *cache, u_int8_t file_id[DB_FILE_ID_LEN], db_pgno_t pgno, DB_LSN target_lsn, BH *bhp);
extern int __mempv_cache_put(DB *dbp, MEMPV_CACHE *cache, u_int8_t file_id[DB_FILE_ID_LEN], db_pgno_t pgno, BH *bhp, DB_LSN target_lsn);

typedef int (*recovery_func_t)(DB_ENV*, DBT*, DB_LSN*, db_recops, PAGE *);

static long long unsigned int gbl_caller_id = 0;
pthread_mutex_t caller_id_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * __mempv_init --
 *	Initialize versioned memory pool.
 *
 * PUBLIC: int __mempv_init
 * PUBLIC:	   __P((DB_ENV *));
 */
int __mempv_init(dbenv)
	DB_ENV *dbenv;
{
	DB_MEMPV *mempv;
	int ret;

	ret = __os_malloc(dbenv, sizeof(DB_MEMPV), &mempv);
	if (ret) {
		goto done;
	}

	if ((ret = __mempv_cache_init(dbenv, &(mempv->cache))), ret != 0) {
		goto done;
	}

	dbenv->mempv = mempv;

done:
	return ret;
}

/*
 * __mempv_destroy --
 *	Destroy versioned memory pool.
 *
 * PUBLIC: void __mempv_destroy
 * PUBLIC:	   __P((DB_ENV *));
 */
void __mempv_destroy(dbenv)
	DB_ENV *dbenv;
{
	__mempv_cache_destroy(&(dbenv->mempv->cache));
	__os_free(dbenv, dbenv->mempv);
	dbenv->mempv = NULL;
}

static int __mempv_read_log_record(void *ptr, recovery_func_t *apply, u_int64_t *utxnid,
		db_pgno_t pgno, u_int32_t *p_rectype, long long unsigned int caller_id) {
	int ret, utxnid_logged;
	u_int32_t rectype;
	char *data = ptr;

	ret = 0;

	LOGCOPY_32(&rectype, data);
	
	if ((utxnid_logged = normalize_rectype(&rectype)) != 1) {
		ret = 1;
		goto done;
	}
	if ((rectype > 1000 && rectype < 10000) || rectype > 11000) {
		rectype -= 1000; // For logs with ufid
	}

	*p_rectype = rectype;

	data += sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN);
	LOGCOPY_64(utxnid, data);

	switch (rectype) {
		case DB___db_addrem:
			*apply = __db_addrem_snap_recover;
			break;
		case DB___db_big:
			*apply = __db_big_snap_recover;
			break;
		case DB___db_ovref:
			*apply = __db_ovref_snap_recover;
			break;
		case DB___db_relink:
			*apply = __db_relink_snap_recover;
			break;
		case DB___db_pg_alloc:
			*apply = __db_pg_alloc_snap_recover;
			break;
		case DB___bam_split:
		   *apply = __bam_split_snap_recover;
		   break;
		case DB___bam_rsplit:
		   *apply = __bam_rsplit_snap_recover;
		   break;
		case DB___bam_repl:
		   *apply = __bam_repl_snap_recover;
		   break;
		case DB___bam_adj:
		   *apply = __bam_adj_snap_recover;
		   break;
		case DB___bam_cadjust:
		   *apply = __bam_cadjust_snap_recover;
		   break;
		case DB___bam_cdel: 
		   *apply = __bam_cdel_snap_recover;
		   break;
		case DB___bam_prefix:
		   *apply = __bam_prefix_snap_recover;
		   break;
		case DB___db_pg_freedata:
		   *apply = __db_pg_freedata_snap_recover;
		   break;
		case DB___db_pg_free:
		   *apply = __db_pg_free_snap_recover;
		   break;
		default:
			__mempv_logmsg(LOGMSG_ERROR, caller_id,
				"Op of log record is unrecognized %d\n", rectype);
			ret = 1;
			break;
	}
done:		
	return ret;
}

/*
 * __mempv_fget --
 * Gets a page from the file after unrolling all modifications 
 * to the page made by transactions that committed after the target lsn.
 * Callers should never write to these pages.
 *
 * This function never modifies the actual page. "Unrolling" is done 
 * on a copy of the page.
 *
 * mpf: Memory pool file.
 * dbp: Open db.
 * pgno: Page number.
 * target_lsn: Modifications to the page made by any transaction that committed after this LSN will be unwound. 
 * last_checkpoint_lsn: Checkpoint preceding the target LSN.
 * ret_page: This gets set to point to the page at the target version.
 * flags: See `memp_fget` flags.
 *
 * PUBLIC: int __mempv_fget
 * PUBLIC:	   __P((DB_MPOOLFILE *, DB *, db_pgno_t, DB_LSN, DB_LSN, void *, u_int32_t));
 */
int __mempv_fget(mpf, dbp, pgno, target_lsn, highest_checkpoint_lsn, ret_page, flags)
	DB_MPOOLFILE *mpf;
	DB *dbp;
	db_pgno_t pgno;
	DB_LSN target_lsn;
	DB_LSN highest_checkpoint_lsn;
	void *ret_page;
	u_int32_t flags;
{
	recovery_func_t apply;
	int add_to_cache, found, ret, mempv_debug;
	u_int64_t utxnid;
	int64_t smallest_logfile;
	DB_LOGC *logc;
	PAGE *page, *page_image;
	DB_LSN commit_lsn;
	DB_ENV *dbenv;
	BH *bhp;
	void *data_t;

	abort();

	ret = found = add_to_cache = 0;
	logc = NULL;
	page = page_image = NULL;
	bhp = NULL;
	data_t = NULL;
	*(void **)ret_page = NULL;
	DBT dbt = {0};
	dbt.flags = DB_DBT_REALLOC;
	dbenv = mpf->dbenv;
	mempv_debug = dbenv->attr.mempv_debug;
	Pthread_mutex_lock(&dbenv->txmap->txmap_mutexp);
	smallest_logfile = dbenv->txmap->smallest_logfile;
	Pthread_mutex_unlock(&dbenv->txmap->txmap_mutexp);

	Pthread_mutex_lock(&caller_id_mutex);
	const long long unsigned int caller_id = ++gbl_caller_id;
	Pthread_mutex_unlock(&caller_id_mutex);

	if ((ret = __memp_fget(mpf, &pgno, flags, &page)) != 0) {
		logmsg(LOGMSG_ERROR, "%s: Failed to get initial page version\n", __func__);
		goto err;
	}

	const DB_LSN initial_lsn = LSN(page);
	if (mempv_debug) {
		__mempv_logmsg(LOGMSG_USER, caller_id,
		"Page #%"PRIu32": initial LSN {%"PRIu32":%"PRIu32"} target LSN {%"PRIu32":%"PRIu32"} checkpoint LSN {%"PRIu32":%"PRIu32"}\n",
		pgno, initial_lsn.file, initial_lsn.offset, target_lsn.file, target_lsn.offset, highest_checkpoint_lsn.file,
		highest_checkpoint_lsn.offset);
	}

	if (PAGE_VERSION_IS_GUARANTEED_TARGET(highest_checkpoint_lsn, smallest_logfile, target_lsn, initial_lsn)) {
		if (mempv_debug) {
			__mempv_logmsg(LOGMSG_USER, caller_id,
				"Page's LSN (%"PRIu32":%"PRIu32") indicates that it is at the right version\n",
				initial_lsn.file, initial_lsn.offset);
		}
		found = 1;
		page_image = page;
		goto found_page;
	} else {
		__os_malloc(dbenv, SSZA(BH, buf) + dbp->pgsize, (void *) &bhp);
		page_image = (PAGE *) (((u_int8_t *) bhp) + SSZA(BH, buf) );

		if (!page_image) {
			__mempv_logmsg(LOGMSG_ERROR, caller_id, "Failed to allocate page image\n");
			ret = ENOMEM;
			goto err;
		}

		if (!__mempv_cache_get(dbp, &dbenv->mempv->cache, mpf->fileid, pgno, target_lsn, bhp)) {
			if (mempv_debug) {
				__mempv_logmsg(LOGMSG_USER, caller_id, "Found target version in cache with LSN %"PRIu32":%"PRIu32"\n",
					LSN(page_image).file, LSN(page_image).offset);
			}
			found = 1;

			if ((ret = __memp_fput(mpf, page, 0)) != 0) {
				__mempv_logmsg(LOGMSG_ERROR, caller_id,
					"Failed to return initial page version to base memory pool\n");
				goto err;
			}
		} else {
			memcpy(bhp, ((char*)page) - offsetof(BH, buf), offsetof(BH, buf) + dbp->pgsize);
			bhp->is_copy = 1; 

			if ((ret = __memp_fput(mpf, page, 0)) != 0) {
				__mempv_logmsg(LOGMSG_ERROR, caller_id,
					"Failed to return initial page version to base memory pool\n");
				goto err;
			}

			if ((ret = __log_cursor(dbenv, &logc)) != 0) {
				__mempv_logmsg(LOGMSG_ERROR, caller_id, "Failed to create log cursor\n");
				goto err;
			}
		}
	}

	DB_LSN current_lsn = initial_lsn;
	while (!found) 
	{
		if (PAGE_VERSION_IS_GUARANTEED_TARGET(highest_checkpoint_lsn, smallest_logfile, target_lsn, current_lsn)) {
			if (mempv_debug) {
				__mempv_logmsg(LOGMSG_USER, caller_id,
					"Page's LSN (%"PRIu32":%"PRIu32") indicates that it is at the right version\n",
					current_lsn.file, current_lsn.offset);
			}
			add_to_cache = 1;
			found = 1;
			break;
		}

		if (IS_ZERO_LSN(current_lsn)) {
			__mempv_logmsg(LOGMSG_ERROR, caller_id, "Page has zero LSN\n");
			ret = 1;
			goto err;
		}
		
		ret = __log_c_get(logc, &current_lsn, &dbt, DB_SET);
		if (ret || (dbt.size < sizeof(int))) {
			__mempv_logmsg(LOGMSG_ERROR, caller_id,
				"Failed to get log cursor at LSN %"PRIu32":%"PRIu32"\n",
				current_lsn.file, current_lsn.offset);
			ret = ret ? ret : 1;
			goto err;
		}

		u_int32_t rectype;
		if ((ret = __mempv_read_log_record(data_t != NULL ? data_t : dbt.data, &apply,
				&utxnid, PGNO(page_image), &rectype, caller_id)) != 0) {
			__mempv_logmsg(LOGMSG_ERROR, caller_id,
				"Failed to read log record at LSN %"PRIu32":%"PRIu32"\n", current_lsn.file, current_lsn.offset);
			goto err;
		}

		 // If the transaction that wrote this page committed before us, return this page.
		if (!__txn_commit_map_get(dbenv, utxnid, &commit_lsn) && (log_compare(&commit_lsn, &target_lsn) <= 0)) {
			if (mempv_debug) {
				__mempv_logmsg(LOGMSG_USER, caller_id,
					"Commit LSN %"PRIu32":%"PRIu32" of last updater (utxnid %"PRIu64") "
					"indicates that this page is the right version\n",
					commit_lsn.file, commit_lsn.offset, utxnid);
			}
			add_to_cache = 1;
			found = 1;
			break;
		}

		if (mempv_debug) {
			__mempv_logmsg(LOGMSG_USER, caller_id, "Rolling back '%s' operation recorded at %"PRIu32":%"PRIu32"\n",
				optostr(rectype), current_lsn.file, current_lsn.offset);
		}

		if((ret = apply(dbenv, &dbt, &current_lsn, DB_TXN_ABORT, page_image)) != 0) {
			__mempv_logmsg(LOGMSG_ERROR, caller_id,
				"Failed to roll back operation record at %"PRIu32":%"PRIu32"\n",
				current_lsn.file, current_lsn.offset);
			goto err;
		}

		current_lsn = LSN(page_image);
	}

found_page:
	*(void **)ret_page = (void *) page_image;

	if (add_to_cache == 1) {
	   __mempv_cache_put(dbp, &dbenv->mempv->cache, mpf->fileid, pgno, bhp, target_lsn);
	}
err:
	if (logc) {
		__log_c_close(logc);
	}
	if (dbt.data) {
		__os_free(dbenv, dbt.data);
		dbt.data = NULL;
	}
	if (ret != 0 && bhp != NULL) {
		__os_free(dbenv, bhp);
	}
	return ret;
}

/*
 * __mempv_fput --
 *	Release a page accessed with __mempv_fget.
 *
 * mpf: Memory pool file.
 * page: Page accessed with __mempv_fget
 * flags: See memp_fput flags.
 *
 * PUBLIC: int __mempv_fput
 * PUBLIC:	   __P((DB_MPOOLFILE *, void *, u_int32_t));
 */
int __mempv_fput(mpf, page, flags)
	DB_MPOOLFILE *mpf;
	void *page;
	u_int32_t flags;
{
	BH *bhp;
	DB_ENV *dbenv;
	int ret;

	dbenv = mpf->dbenv;
	ret = 0;

	if (page != NULL) {
		bhp = (BH *)((u_int8_t *)page - SSZA(BH, buf));

		if (bhp->is_copy == 1) {
			// I am a copy
			__os_free(dbenv, bhp);
		} else if ((ret = __memp_fput(mpf, page, 0)), ret != 0) {
			logmsg(LOGMSG_ERROR, "%s: Failed to return initial page version\n", __func__);
		}

		page = NULL;
	}
	return ret;
}
