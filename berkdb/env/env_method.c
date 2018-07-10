/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1999-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] =
    "$Id: env_method.c,v 11.113 2003/09/11 17:36:41 sue Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#ifdef HAVE_RPC
#include <rpc/rpc.h>
#endif

#include <string.h>
#endif

/*
 * This is the file that initializes the global array.  Do it this way because
 * people keep changing one without changing the other.  Having declaration and
 * initialization in one file will hopefully fix that.
 */
#define	DB_INITIALIZE_DB_GLOBALS	1

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/hmac.h"
#include "dbinc/db_shash.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"

#ifdef HAVE_RPC
#include "dbinc_auto/db_server.h"
#include "dbinc_auto/rpc_client_ext.h"
#endif

#include "logmsg.h"

static int __dbenv_init __P((DB_ENV *));
static void __dbenv_err __P((const DB_ENV *, int, const char *, ...));
static void __dbenv_errx __P((const DB_ENV *, const char *, ...));
static int __dbenv_get_home __P((DB_ENV *, const char **));
static int __dbenv_set_app_dispatch __P((DB_ENV *,
	int (*)(DB_ENV *, DBT *, DB_LSN *, db_recops)));
static int __dbenv_get_data_dirs __P((DB_ENV *, const char ***));
static int __dbenv_set_feedback __P((DB_ENV *, void (*)(DB_ENV *, int, int)));
static void __dbenv_map_flags __P((DB_ENV *, u_int32_t *, u_int32_t *));
static int __dbenv_get_flags __P((DB_ENV *, u_int32_t *));
static int __dbenv_set_rpc_server_noclnt
	__P((DB_ENV *, void *, const char *, long, long, u_int32_t));
static int __dbenv_get_shm_key __P((DB_ENV *, long *));
static int __dbenv_get_tas_spins __P((DB_ENV *, u_int32_t *));
static int __dbenv_get_tmp_dir __P((DB_ENV *, const char **));
static int __dbenv_get_verbose __P((DB_ENV *, u_int32_t, int *));
static int __dbenv_get_concurrent __P((DB_ENV *, int *));
static int __dbenv_set_lsn_chaining __P((DB_ENV *, int));
static int __dbenv_get_lsn_chaining __P((DB_ENV *, int *));
static int __dbenv_set_bulk_stops_on_page __P((DB_ENV *, int));
static int __dbenv_memp_dump_bufferpool_info __P((DB_ENV *, FILE *));
static int __dbenv_set_deadlock_override __P((DB_ENV *, u_int32_t));
static int __dbenv_set_tran_lowpri __P((DB_ENV *, u_int32_t));
static int __dbenv_set_num_recovery_processor_threads __P((DB_ENV *, int));
static int __dbenv_set_num_recovery_worker_threads __P((DB_ENV *, int));
static void __dbenv_set_recovery_memsize __P((DB_ENV *, int));
static int __dbenv_get_recovery_memsize __P((DB_ENV *));
static int __dbenv_get_rep_master __P((DB_ENV *, char **, u_int32_t *, u_int32_t *));
static int __dbenv_get_rep_eid __P((DB_ENV *, char **));
static int __dbenv_get_page_extent_size __P((DB_ENV *));
static void __dbenv_set_page_extent_size __P((DB_ENV *, u_int32_t));
static int __dbenv_set_recovery_lsn __P((DB_ENV *, DB_LSN *));
static void __dbenv_get_rep_verify_lsn __P((DB_ENV *, DB_LSN *, DB_LSN *));
static void __dbenv_set_durable_lsn __P((DB_ENV *, DB_LSN *, uint32_t));
static void __dbenv_get_durable_lsn __P((DB_ENV *, DB_LSN *, uint32_t *));
static int __dbenv_blobmem_yield __P((DB_ENV *));
static int __dbenv_set_comdb2_dirs __P((DB_ENV *, char *, char *, char *));
static int __dbenv_set_is_tmp_tbl __P((DB_ENV *, int));
static int __dbenv_set_use_sys_malloc __P((DB_ENV *, int));
static int __dbenv_trigger_subscribe __P((DB_ENV *, const char *,
	pthread_cond_t **, pthread_mutex_t **, const uint8_t **));
static int __dbenv_trigger_unsubscribe __P((DB_ENV *, const char *));
static int __dbenv_trigger_open __P((DB_ENV *, const char *));
static int __dbenv_trigger_close __P((DB_ENV *, const char *));

/*
 * db_env_create --
 *	DB_ENV constructor.
 *
 * EXTERN: int db_env_create __P((DB_ENV **, u_int32_t));
 */
int
db_env_create(dbenvpp, flags)
	DB_ENV **dbenvpp;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	int ret;

	/*
	 * !!!
	 * Our caller has not yet had the opportunity to reset the panic
	 * state or turn off mutex locking, and so we can neither check
	 * the panic state or acquire a mutex in the DB_ENV create path.
	 *
	 * !!!
	 * We can't call the flags-checking routines, we don't have an
	 * environment yet.
	 */
	if (flags != 0 && !LF_ISSET(DB_RPCCLIENT))
		return (EINVAL);
	if ((ret = __os_calloc(NULL, 1, sizeof(*dbenv), &dbenv)) != 0)
		return (ret);

#ifdef HAVE_RPC
	if (LF_ISSET(DB_RPCCLIENT))
		F_SET(dbenv, DB_ENV_RPCCLIENT);
#endif
	if ((ret = __dbenv_init(dbenv)) != 0) {
		__os_free(NULL, dbenv);
		return (ret);
	}

    dbenv->checkpoint = NULL;
	*dbenvpp = dbenv;
	return (0);
}

static comdb2bma berkdb_blobmem;
static DB_ENV *dbenv_being_initialized;
static pthread_once_t berkdb_blobmem_once = PTHREAD_ONCE_INIT;
static void
__berkdb_blobmem_init_once(void)
{
	extern size_t gbl_blobmem_cap;
	berkdb_blobmem = comdb2bma_create(0, gbl_blobmem_cap, "berkdb/blob", NULL);
	if (berkdb_blobmem == NULL) {
		__db_err(dbenv_being_initialized,
				"DB_ENV->berkdb_blobmem_init_once: error creating blob allocator");
		(void)__db_panic(dbenv_being_initialized, ENOMEM);
	}
}

/*
 * __dbenv_init --
 *	Initialize a DB_ENV structure.
 */
static int
__dbenv_init(dbenv)
	DB_ENV *dbenv;
{
	int ret;
	extern unsigned gbl_blob_sz_thresh_bytes;

	/*
	 * !!!
	 * Our caller has not yet had the opportunity to reset the panic
	 * state or turn off mutex locking, and so we can neither check
	 * the panic state or acquire a mutex in the DB_ENV create path.
	 *
	 * Set up methods that are the same in both normal and RPC
	 */
	dbenv->err = __dbenv_err;
	dbenv->errx = __dbenv_errx;
	dbenv->set_errcall = __dbenv_set_errcall;
	dbenv->get_errfile = __dbenv_get_errfile;
	dbenv->set_errfile = __dbenv_set_errfile;
	dbenv->get_errpfx = __dbenv_get_errpfx;
	dbenv->set_errpfx = __dbenv_set_errpfx;

#ifdef	HAVE_RPC
	if (F_ISSET(dbenv, DB_ENV_RPCCLIENT)) {
		dbenv->close = __dbcl_env_close_wrap;
		dbenv->dbremove = __dbcl_env_dbremove;
		dbenv->dbrename = __dbcl_env_dbrename;
		dbenv->get_home = __dbcl_env_get_home;
		dbenv->get_open_flags = __dbcl_env_get_open_flags;
		dbenv->open = __dbcl_env_open_wrap;
		dbenv->remove = __dbcl_env_remove;
		dbenv->set_alloc = __dbcl_env_alloc;
		dbenv->set_app_dispatch = __dbcl_set_app_dispatch;
		dbenv->get_data_dirs = __dbcl_get_data_dirs;
		dbenv->set_data_dir = __dbcl_set_data_dir;
		dbenv->get_encrypt_flags = __dbcl_env_get_encrypt_flags;
		dbenv->set_encrypt = __dbcl_env_encrypt;
		dbenv->set_feedback = __dbcl_env_set_feedback;
		dbenv->get_flags = __dbcl_env_get_flags;
		dbenv->set_flags = __dbcl_env_flags;
		dbenv->set_paniccall = __dbcl_env_paniccall;
		dbenv->set_rpc_server = __dbcl_envrpcserver;
		dbenv->get_shm_key = __dbcl_get_shm_key;
		dbenv->set_shm_key = __dbcl_set_shm_key;
		dbenv->get_tas_spins = __dbcl_get_tas_spins;
		dbenv->set_tas_spins = __dbcl_set_tas_spins;
		dbenv->get_timeout = __dbcl_get_timeout;
		dbenv->set_timeout = __dbcl_set_timeout;
		dbenv->get_tmp_dir = __dbcl_get_tmp_dir;
		dbenv->set_tmp_dir = __dbcl_set_tmp_dir;
		dbenv->get_verbose = __dbcl_get_verbose;
		dbenv->set_verbose = __dbcl_set_verbose;
	} else {
#endif
		dbenv->close = __dbenv_close_pp;
		dbenv->dbremove = __dbenv_dbremove_pp;
		dbenv->dbrename = __dbenv_dbrename_pp;
		dbenv->open = __dbenv_open;
		dbenv->remove = __dbenv_remove;
		dbenv->get_home = __dbenv_get_home;
		dbenv->get_open_flags = __dbenv_get_open_flags;
		dbenv->set_alloc = __dbenv_set_alloc;
		dbenv->set_app_dispatch = __dbenv_set_app_dispatch;
		dbenv->get_data_dirs = __dbenv_get_data_dirs;
		dbenv->set_data_dir = __dbenv_set_data_dir;
		dbenv->get_encrypt_flags = __dbenv_get_encrypt_flags;
		dbenv->set_encrypt = __dbenv_set_encrypt;
		dbenv->set_feedback = __dbenv_set_feedback;
		dbenv->get_flags = __dbenv_get_flags;
		dbenv->set_flags = __dbenv_set_flags;
		dbenv->set_paniccall = __dbenv_set_paniccall;
		dbenv->set_rpc_server = __dbenv_set_rpc_server_noclnt;
		dbenv->get_shm_key = __dbenv_get_shm_key;
		dbenv->set_shm_key = __dbenv_set_shm_key;
		dbenv->get_tas_spins = __dbenv_get_tas_spins;
		dbenv->set_tas_spins = __dbenv_set_tas_spins;
		dbenv->get_tmp_dir = __dbenv_get_tmp_dir;
		dbenv->set_tmp_dir = __dbenv_set_tmp_dir;
		dbenv->get_verbose = __dbenv_get_verbose;
		dbenv->get_concurrent = __dbenv_get_concurrent;
		dbenv->set_lsn_chaining = __dbenv_set_lsn_chaining;
		dbenv->get_lsn_chaining = __dbenv_get_lsn_chaining;
		dbenv->set_verbose = __dbenv_set_verbose;
		dbenv->set_bulk_stops_on_page = __dbenv_set_bulk_stops_on_page;

		dbenv->set_num_recovery_processor_threads =
		    __dbenv_set_num_recovery_processor_threads;
		dbenv->set_num_recovery_worker_threads =
		    __dbenv_set_num_recovery_worker_threads;
		dbenv->set_recovery_memsize = __dbenv_set_recovery_memsize;
		dbenv->get_recovery_memsize = __dbenv_get_recovery_memsize;
		dbenv->memp_dump_bufferpool_info =
		    __dbenv_memp_dump_bufferpool_info;
		dbenv->set_deadlock_override = __dbenv_set_deadlock_override;
		dbenv->set_tran_lowpri = __dbenv_set_tran_lowpri;
		dbenv->get_rep_master = __dbenv_get_rep_master;
		dbenv->get_rep_eid = __dbenv_get_rep_eid;
		dbenv->get_page_extent_size = __dbenv_get_page_extent_size;
		dbenv->set_page_extent_size = __dbenv_set_page_extent_size;
		dbenv->setattr = __dbenv_setattr;
		dbenv->getattr = __dbenv_getattr;
		dbenv->dumpattrs = __dbenv_dumpattrs;
		dbenv->get_recovery_lsn = __db_find_recovery_start;
		dbenv->set_recovery_lsn = __dbenv_set_recovery_lsn;
		dbenv->get_rep_verify_lsn = __dbenv_get_rep_verify_lsn;
		dbenv->set_durable_lsn = __dbenv_set_durable_lsn;
		dbenv->get_durable_lsn = __dbenv_get_durable_lsn;
#ifdef	HAVE_RPC
	}
#endif
	dbenv->shm_key = INVALID_REGION_SEGID;
	dbenv->db_ref = 0;

	__os_spin(dbenv);

	__log_dbenv_create(dbenv);	/* Subsystem specific. */
	__lock_dbenv_create(dbenv);
	__memp_dbenv_create(dbenv);
	if ((ret = __rep_dbenv_create(dbenv)) != 0)
		return (ret);
	__txn_dbenv_create(dbenv);

	/* comdb2-specific changes */
	dbenv->dbs = NULL;
	listc_init(&dbenv->regions, offsetof(struct __heap, lnk));
	dbenv->bulk_stops_on_page = 0;
	dbenv->maxdb = 0;
	__dbenv_attr_init(dbenv);
	dbenv->iomap_fd = NULL;
	dbenv->iomap = NULL;

	/* 
	 * berkdb must have its own blocking allocator. otherwise we may 
	 * have deadlocks.
	 * for example, if berkdb and bdb use the same allocator- 
	 * 1. writer A allocates a big buffer and exhausts the allocator
	 * 2. reader B holds a page lock
	 * 3. writer A tries to get the same page lock in write mode. it must 
	 *  wait for reader B to release the lock
	 * 4. reader B tries to allocate memory to retrieve the data. it must 
	 *  wait for writer A to free the buffer
	 */
	if (gbl_blob_sz_thresh_bytes != ~(0U)) {
		dbenv_being_initialized = dbenv;
		pthread_once(&berkdb_blobmem_once, __berkdb_blobmem_init_once);
	}
	dbenv->bma = berkdb_blobmem;
	dbenv->bmaszthresh = gbl_blob_sz_thresh_bytes;

	dbenv->blobmem_yield = __dbenv_blobmem_yield;


	dbenv->set_comdb2_dirs = __dbenv_set_comdb2_dirs;
	ZERO_LSN(dbenv->recovery_start_lsn);

	dbenv->set_is_tmp_tbl = __dbenv_set_is_tmp_tbl;
	dbenv->set_use_sys_malloc = __dbenv_set_use_sys_malloc;

	dbenv->trigger_subscribe = __dbenv_trigger_subscribe;
	dbenv->trigger_unsubscribe = __dbenv_trigger_unsubscribe;
	dbenv->trigger_open = __dbenv_trigger_open;
	dbenv->trigger_close = __dbenv_trigger_close;

	return (0);
}

/*
 * __dbenv_err --
 *	Error message, including the standard error string.
 */
static void
#ifdef STDC_HEADERS
__dbenv_err(const DB_ENV *dbenv, int error, const char *fmt, ...)
#else
__dbenv_err(dbenv, error, fmt, va_alist)
	const DB_ENV *dbenv;
	int error;
	const char *fmt;
	va_dcl
#endif
{
	DB_REAL_ERR(dbenv, error, 1, 1, fmt);
}

/*
 * __dbenv_errx --
 *	Error message.
 */
static void
#ifdef STDC_HEADERS
__dbenv_errx(const DB_ENV *dbenv, const char *fmt, ...)
#else
__dbenv_errx(dbenv, fmt, va_alist)
	const DB_ENV *dbenv;
	const char *fmt;
	va_dcl
#endif
{
	DB_REAL_ERR(dbenv, 0, 0, 1, fmt);
}

static int
__dbenv_get_home(dbenv, homep)
	DB_ENV *dbenv;
	const char **homep;
{
	ENV_ILLEGAL_BEFORE_OPEN(dbenv, "DB_ENV->get_home");
	*homep = dbenv->db_home;
	return (0);
}

/*
 * __dbenv_set_alloc --
 *	{DB_ENV,DB}->set_alloc.
 *
 * PUBLIC: int  __dbenv_set_alloc __P((DB_ENV *, void *(*)(size_t),
 * PUBLIC:          void *(*)(void *, size_t), void (*)(void *)));
 */
int
__dbenv_set_alloc(dbenv, mal_func, real_func, free_func)
	DB_ENV *dbenv;
	void *(*mal_func) __P((size_t));
	void *(*real_func) __P((void *, size_t));
	void (*free_func) __P((void *));
{
	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_alloc");

	dbenv->db_malloc = mal_func;
	dbenv->db_realloc = real_func;
	dbenv->db_free = free_func;
	return (0);
}

/*
 * __dbenv_set_app_dispatch --
 *	Set the transaction abort recover function.
 */
static int
__dbenv_set_app_dispatch(dbenv, app_dispatch)
	DB_ENV *dbenv;
	int (*app_dispatch) __P((DB_ENV *, DBT *, DB_LSN *, db_recops));
{
	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_app_dispatch");

	dbenv->app_dispatch = app_dispatch;
	return (0);
}

/*
 * __dbenv_get_encrypt_flags --
 *	{DB_ENV,DB}->get_encrypt_flags.
 *
 * PUBLIC: int __dbenv_get_encrypt_flags __P((DB_ENV *, u_int32_t *));
 */
int
__dbenv_get_encrypt_flags(dbenv, flagsp)
	DB_ENV *dbenv;
	u_int32_t *flagsp;
{
#ifdef HAVE_CRYPTO
	DB_CIPHER *db_cipher;

	db_cipher = dbenv->crypto_handle;
	if (db_cipher != NULL && db_cipher->alg == CIPHER_AES)
		*flagsp = DB_ENCRYPT_AES;
	else
		*flagsp = 0;
	return (0);
#else
	COMPQUIET(flagsp, 0);
	__db_err(dbenv,
	    "library build did not include support for cryptography");
	return (DB_OPNOTSUP);
#endif
}

/*
 * __dbenv_set_encrypt --
 *	DB_ENV->set_encrypt.
 *
 * PUBLIC: int __dbenv_set_encrypt __P((DB_ENV *, const char *, u_int32_t));
 */
int
__dbenv_set_encrypt(dbenv, passwd, flags)
	DB_ENV *dbenv;
	const char *passwd;
	u_int32_t flags;
{
#ifdef HAVE_CRYPTO
	DB_CIPHER *db_cipher;
	int ret;

	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_encrypt");
#define	OK_CRYPTO_FLAGS	(DB_ENCRYPT_AES)

	if (flags != 0 && LF_ISSET(~OK_CRYPTO_FLAGS))
		return (__db_ferr(dbenv, "DB_ENV->set_encrypt", 0));

	if (passwd == NULL || strlen(passwd) == 0) {
		__db_err(dbenv, "Empty password specified to set_encrypt");
		return (EINVAL);
	}
	if (!CRYPTO_ON(dbenv)) {
		if ((ret = __os_calloc(dbenv, 1, sizeof(DB_CIPHER), &db_cipher))
		    != 0)
			goto err;
		dbenv->crypto_handle = db_cipher;
	} else
		db_cipher = (DB_CIPHER *)dbenv->crypto_handle;

	if (dbenv->passwd != NULL)
		__os_free(dbenv, dbenv->passwd);
	if ((ret = __os_strdup(dbenv, passwd, &dbenv->passwd)) != 0) {
		__os_free(dbenv, db_cipher);
		goto err;
	}
	/*
	 * We're going to need this often enough to keep around
	 */
	dbenv->passwd_len = strlen(dbenv->passwd) + 1;
	/*
	 * The MAC key is for checksumming, and is separate from
	 * the algorithm.  So initialize it here, even if they
	 * are using CIPHER_ANY.
	 */
	__db_derive_mac((u_int8_t *)dbenv->passwd,
	    dbenv->passwd_len, db_cipher->mac_key);
	switch (flags) {
	case 0:
		F_SET(db_cipher, CIPHER_ANY);
		break;
	case DB_ENCRYPT_AES:
		if ((ret = __crypto_algsetup(dbenv, db_cipher, CIPHER_AES, 0))
		    != 0)
			goto err1;
		break;
	default:				/* Impossible. */
		break;
	}
	return (0);

err1:
	__os_free(dbenv, dbenv->passwd);
	__os_free(dbenv, db_cipher);
	dbenv->crypto_handle = NULL;
err:
	return (ret);
#else
	COMPQUIET(passwd, NULL);
	COMPQUIET(flags, 0);

	__db_err(dbenv,
	    "library build did not include support for cryptography");
	return (DB_OPNOTSUP);
#endif
}

static void
__dbenv_map_flags(dbenv, inflagsp, outflagsp)
	DB_ENV *dbenv;
	u_int32_t *inflagsp, *outflagsp;
{
	COMPQUIET(dbenv, NULL);

	if (FLD_ISSET(*inflagsp, DB_AUTO_COMMIT)) {
		FLD_SET(*outflagsp, DB_ENV_AUTO_COMMIT);
		FLD_CLR(*inflagsp, DB_AUTO_COMMIT);
	}
	if (FLD_ISSET(*inflagsp, DB_CDB_ALLDB)) {
		FLD_SET(*outflagsp, DB_ENV_CDB_ALLDB);
		FLD_CLR(*inflagsp, DB_CDB_ALLDB);
	}
	if (FLD_ISSET(*inflagsp, DB_DIRECT_DB)) {
		FLD_SET(*outflagsp, DB_ENV_DIRECT_DB);
		FLD_CLR(*inflagsp, DB_DIRECT_DB);
	}
	if (FLD_ISSET(*inflagsp, DB_DIRECT_LOG)) {
		FLD_SET(*outflagsp, DB_ENV_DIRECT_LOG);
		FLD_CLR(*inflagsp, DB_DIRECT_LOG);
	}
	if (FLD_ISSET(*inflagsp, DB_LOG_AUTOREMOVE)) {
		FLD_SET(*outflagsp, DB_ENV_LOG_AUTOREMOVE);
		FLD_CLR(*inflagsp, DB_LOG_AUTOREMOVE);
	}
	if (FLD_ISSET(*inflagsp, DB_NOLOCKING)) {
		FLD_SET(*outflagsp, DB_ENV_NOLOCKING);
		FLD_CLR(*inflagsp, DB_NOLOCKING);
	}
	if (FLD_ISSET(*inflagsp, DB_NOMMAP)) {
		FLD_SET(*outflagsp, DB_ENV_NOMMAP);
		FLD_CLR(*inflagsp, DB_NOMMAP);
	}
	if (FLD_ISSET(*inflagsp, DB_NOPANIC)) {
		FLD_SET(*outflagsp, DB_ENV_NOPANIC);
		FLD_CLR(*inflagsp, DB_NOPANIC);
	}
	if (FLD_ISSET(*inflagsp, DB_OVERWRITE)) {
		FLD_SET(*outflagsp, DB_ENV_OVERWRITE);
		FLD_CLR(*inflagsp, DB_OVERWRITE);
	}
	if (FLD_ISSET(*inflagsp, DB_REGION_INIT)) {
		FLD_SET(*outflagsp, DB_ENV_REGION_INIT);
		FLD_CLR(*inflagsp, DB_REGION_INIT);
	}
	if (FLD_ISSET(*inflagsp, DB_TIME_NOTGRANTED)) {
		FLD_SET(*outflagsp, DB_ENV_TIME_NOTGRANTED);
		FLD_CLR(*inflagsp, DB_TIME_NOTGRANTED);
	}
	if (FLD_ISSET(*inflagsp, DB_TXN_NOSYNC)) {
		FLD_SET(*outflagsp, DB_ENV_TXN_NOSYNC);
		FLD_CLR(*inflagsp, DB_TXN_NOSYNC);
	}
	if (FLD_ISSET(*inflagsp, DB_TXN_NOT_DURABLE)) {
		FLD_SET(*outflagsp, DB_ENV_TXN_NOT_DURABLE);
		FLD_CLR(*inflagsp, DB_TXN_NOT_DURABLE);
	}
	if (FLD_ISSET(*inflagsp, DB_TXN_WRITE_NOSYNC)) {
		FLD_SET(*outflagsp, DB_ENV_TXN_WRITE_NOSYNC);
		FLD_CLR(*inflagsp, DB_TXN_WRITE_NOSYNC);
	}
	if (FLD_ISSET(*inflagsp, DB_YIELDCPU)) {
		FLD_SET(*outflagsp, DB_ENV_YIELDCPU);
		FLD_CLR(*inflagsp, DB_YIELDCPU);
	}
	if (FLD_ISSET(*inflagsp, DB_ROWLOCKS)) {
		FLD_SET(*outflagsp, DB_ENV_ROWLOCKS);
		FLD_CLR(*inflagsp, DB_ROWLOCKS);
	}
	if (FLD_ISSET(*inflagsp, DB_OSYNC)) {
		FLD_SET(*outflagsp, DB_ENV_OSYNC);
		FLD_CLR(*inflagsp, DB_OSYNC);
	}
}

static int
__dbenv_get_flags(dbenv, flagsp)
	DB_ENV *dbenv;
	u_int32_t *flagsp;
{
	static const u_int32_t env_flags[] = {
		DB_AUTO_COMMIT,
		DB_CDB_ALLDB,
		DB_DIRECT_DB,
		DB_DIRECT_LOG,
		DB_LOG_AUTOREMOVE,
		DB_NOLOCKING,
		DB_NOMMAP,
		DB_NOPANIC,
		DB_OVERWRITE,
		DB_REGION_INIT,
		DB_TIME_NOTGRANTED,
		DB_TXN_NOSYNC,
		DB_TXN_NOT_DURABLE,
		DB_TXN_WRITE_NOSYNC,
		DB_YIELDCPU,
		0
	};
	u_int32_t f, flags, mapped_flag;
	int i;

	flags = 0;
	for (i = 0; (f = env_flags[i]) != 0; i++) {
		mapped_flag = 0;
		__dbenv_map_flags(dbenv, &f, &mapped_flag);
		DB_ASSERT(f == 0);
		if (F_ISSET(dbenv, mapped_flag) == mapped_flag)
			LF_SET(env_flags[i]);
	}

	/* Special cases */
	if (dbenv->reginfo != NULL &&
	    ((REGENV *)((REGINFO *)dbenv->reginfo)->primary)->envpanic != 0) {
		LF_SET(DB_PANIC_ENVIRONMENT);
	}

	*flagsp = flags;
	return (0);
}

/*
 * __dbenv_set_flags --
 *	DB_ENV->set_flags.
 *
 * PUBLIC: int  __dbenv_set_flags __P((DB_ENV *, u_int32_t, int));
 */
int
__dbenv_set_flags(dbenv, flags, on)
	DB_ENV *dbenv;
	u_int32_t flags;
	int on;
{
	u_int32_t mapped_flags;
	int ret;

	if (LF_ISSET(DB_DIRECT_DB)) {
		logmsg(LOGMSG_INFO, "setting direct_db\n");
	}
#define	OK_FLAGS							\
	(DB_AUTO_COMMIT | DB_CDB_ALLDB | DB_DIRECT_DB | DB_DIRECT_LOG |	\
	    DB_LOG_AUTOREMOVE | DB_NOLOCKING | DB_NOMMAP | DB_NOPANIC | \
	    DB_OVERWRITE | DB_PANIC_ENVIRONMENT | DB_REGION_INIT |      \
	    DB_TIME_NOTGRANTED | DB_TXN_NOSYNC | DB_TXN_NOT_DURABLE |   \
	    DB_TXN_WRITE_NOSYNC | DB_YIELDCPU | DB_ROWLOCKS | DB_OSYNC)

	if (LF_ISSET(~OK_FLAGS))
		return (__db_ferr(dbenv, "DB_ENV->set_flags", 0));
	if (on) {
		if ((ret = __db_fcchk(dbenv, "DB_ENV->set_flags",
		    flags, DB_TXN_NOSYNC, DB_TXN_NOT_DURABLE)) != 0)
			return (ret);
		if ((ret = __db_fcchk(dbenv, "DB_ENV->set_flags",
		    flags, DB_TXN_NOSYNC, DB_TXN_WRITE_NOSYNC)) != 0)
			return (ret);
		if ((ret = __db_fcchk(dbenv, "DB_ENV->set_flags",
		    flags, DB_TXN_NOT_DURABLE, DB_TXN_WRITE_NOSYNC)) != 0)
			return (ret);
		if (LF_ISSET(DB_DIRECT_DB |
		    DB_DIRECT_LOG) && __os_have_direct() == 0) {
			__db_err(dbenv,
	"DB_ENV->set_flags: direct I/O is not supported by this platform");
			return (EINVAL);
		}
	}

	if (LF_ISSET(DB_CDB_ALLDB))
		ENV_ILLEGAL_AFTER_OPEN(dbenv,
		    "DB_ENV->set_flags: DB_CDB_ALLDB");
	if (LF_ISSET(DB_PANIC_ENVIRONMENT)) {
		ENV_ILLEGAL_BEFORE_OPEN(dbenv,
		    "DB_ENV->set_flags: DB_PANIC_ENVIRONMENT");
		PANIC_SET(dbenv, on);
	}
	if (LF_ISSET(DB_REGION_INIT))
		ENV_ILLEGAL_AFTER_OPEN(dbenv,
		    "DB_ENV->set_flags: DB_REGION_INIT");

	mapped_flags = 0;
	__dbenv_map_flags(dbenv, &flags, &mapped_flags);
	if (on)
		F_SET(dbenv, mapped_flags);
	else
		F_CLR(dbenv, mapped_flags);
	return (0);
}

static int
__dbenv_get_data_dirs(dbenv, dirpp)
	DB_ENV *dbenv;
	const char ***dirpp;
{
	*dirpp = (const char **)dbenv->db_data_dir;
	return (0);
}

/*
 * __dbenv_set_data_dir --
 *	DB_ENV->set_dta_dir.
 *
 * PUBLIC: int  __dbenv_set_data_dir __P((DB_ENV *, const char *));
 */
int
__dbenv_set_data_dir(dbenv, dir)
	DB_ENV *dbenv;
	const char *dir;
{
	int ret;

	/*
	 * The array is NULL-terminated so it can be returned by get_data_dirs
	 * without a length.
	 */

#define	DATA_INIT_CNT	20			/* Start with 20 data slots. */
	if (dbenv->db_data_dir == NULL) {
		if ((ret = __os_calloc(dbenv, DATA_INIT_CNT,
		    sizeof(char **), &dbenv->db_data_dir)) != 0)
			return (ret);
		dbenv->data_cnt = DATA_INIT_CNT;
	} else if (dbenv->data_next == dbenv->data_cnt - 2) {
		dbenv->data_cnt *= 2;
		if ((ret = __os_realloc(dbenv,
		    (u_int)dbenv->data_cnt * sizeof(char **),
		    &dbenv->db_data_dir)) != 0)
			return (ret);
	}

	ret = __os_strdup(dbenv,
	    dir, &dbenv->db_data_dir[dbenv->data_next++]);
	dbenv->db_data_dir[dbenv->data_next] = NULL;
	return (ret);
}

/*
 * __dbenv_set_errcall --
 *	{DB_ENV,DB}->set_errcall.
 *
 * PUBLIC: void __dbenv_set_errcall
 * PUBLIC:          __P((DB_ENV *, void (*)(const char *, char *)));
 */
void
__dbenv_set_errcall(dbenv, errcall)
	DB_ENV *dbenv;
	void (*errcall) __P((const char *, char *));
{
	dbenv->db_errcall = errcall;
}

/*
 * __dbenv_get_errfile --
 *	{DB_ENV,DB}->get_errfile.
 *
 * PUBLIC: void __dbenv_get_errfile __P((DB_ENV *, FILE **));
 */
void
__dbenv_get_errfile(dbenv, errfilep)
	DB_ENV *dbenv;
	FILE **errfilep;
{
	*errfilep = dbenv->db_errfile;
}

/*
 * __dbenv_set_errfile --
 *	{DB_ENV,DB}->set_errfile.
 *
 * PUBLIC: void __dbenv_set_errfile __P((DB_ENV *, FILE *));
 */
void
__dbenv_set_errfile(dbenv, errfile)
	DB_ENV *dbenv;
	FILE *errfile;
{
	dbenv->db_errfile = errfile;
}

/*
 * __dbenv_get_errpfx --
 *	{DB_ENV,DB}->get_errpfx.
 *
 * PUBLIC: void __dbenv_get_errpfx __P((DB_ENV *, const char **));
 */
void
__dbenv_get_errpfx(dbenv, errpfxp)
	DB_ENV *dbenv;
	const char **errpfxp;
{
	*errpfxp = dbenv->db_errpfx;
}

/*
 * __dbenv_set_errpfx --
 *	{DB_ENV,DB}->set_errpfx.
 *
 * PUBLIC: void __dbenv_set_errpfx __P((DB_ENV *, const char *));
 */
void
__dbenv_set_errpfx(dbenv, errpfx)
	DB_ENV *dbenv;
	const char *errpfx;
{
	dbenv->db_errpfx = errpfx;
}

static int
__dbenv_set_feedback(dbenv, feedback)
	DB_ENV *dbenv;
	void (*feedback) __P((DB_ENV *, int, int));
{
	dbenv->db_feedback = feedback;
	return (0);
}

/*
 * __dbenv_set_paniccall --
 *	{DB_ENV,DB}->set_paniccall.
 *
 * PUBLIC: int  __dbenv_set_paniccall __P((DB_ENV *, void (*)(DB_ENV *, int)));
 */
int
__dbenv_set_paniccall(dbenv, paniccall)
	DB_ENV *dbenv;
	void (*paniccall) __P((DB_ENV *, int));
{
	dbenv->db_paniccall = paniccall;
	return (0);
}

static int
__dbenv_get_shm_key(dbenv, shm_keyp)
	DB_ENV *dbenv;
	long *shm_keyp;			/* !!!: really a key_t *. */
{
	*shm_keyp = dbenv->shm_key;
	return (0);
}

/*
 * __dbenv_set_shm_key --
 *	DB_ENV->set_shm_key.
 *
 * PUBLIC: int  __dbenv_set_shm_key __P((DB_ENV *, long));
 */
int
__dbenv_set_shm_key(dbenv, shm_key)
	DB_ENV *dbenv;
	long shm_key;			/* !!!: really a key_t. */
{
	ENV_ILLEGAL_AFTER_OPEN(dbenv, "DB_ENV->set_shm_key");

	dbenv->shm_key = shm_key;
	return (0);
}

static int
__dbenv_get_tas_spins(dbenv, tas_spinsp)
	DB_ENV *dbenv;
	u_int32_t *tas_spinsp;
{
	*tas_spinsp = dbenv->tas_spins;
	return (0);
}

/*
 * __dbenv_set_tas_spins --
 *	DB_ENV->set_tas_spins.
 *
 * PUBLIC: int  __dbenv_set_tas_spins __P((DB_ENV *, u_int32_t));
 */
int
__dbenv_set_tas_spins(dbenv, tas_spins)
	DB_ENV *dbenv;
	u_int32_t tas_spins;
{
	dbenv->tas_spins = tas_spins;
	return (0);
}

static int
__dbenv_get_tmp_dir(dbenv, dirp)
	DB_ENV *dbenv;
	const char **dirp;
{
	*dirp = dbenv->db_tmp_dir;
	return (0);
}

/*
 * __dbenv_set_tmp_dir --
 *	DB_ENV->set_tmp_dir.
 *
 * PUBLIC: int  __dbenv_set_tmp_dir __P((DB_ENV *, const char *));
 */
int
__dbenv_set_tmp_dir(dbenv, dir)
	DB_ENV *dbenv;
	const char *dir;
{
	if (dbenv->db_tmp_dir != NULL)
		__os_free(dbenv, dbenv->db_tmp_dir);
	return (__os_strdup(dbenv, dir, &dbenv->db_tmp_dir));
}

static int
__dbenv_set_lsn_chaining(dbenv, val)
    DB_ENV *dbenv;
    int val;
{
    dbenv->lsn_chain = val;
    return (0);
}

static int
__dbenv_get_lsn_chaining(dbenv, val)
    DB_ENV *dbenv;
    int *val;
{
    *val = dbenv->lsn_chain;
    return (0);
}

static int
__dbenv_get_concurrent(dbenv, val)
    DB_ENV *dbenv;
    int *val;
{
    if(dbenv->num_recovery_processor_threads > 0 &&
            dbenv->num_recovery_worker_threads > 0)
    {
        *val = 1;
    }
    else
    {
        *val = 0;
    }
    return (0);
}

static int
__dbenv_get_verbose(dbenv, which, onoffp)
	DB_ENV *dbenv;
	u_int32_t which;
	int *onoffp;
{
	switch (which) {
	case DB_VERB_CHKPOINT:
	case DB_VERB_DEADLOCK:
	case DB_VERB_RECOVERY:
	case DB_VERB_REPLICATION:
	case DB_VERB_WAITSFOR:
		*onoffp = FLD_ISSET(dbenv->verbose, which) ? 1 : 0;
		break;
	default:
		return (EINVAL);
	}
	return (0);
}

/*
 * __dbenv_set_verbose --
 *	DB_ENV->set_verbose.
 *
 * PUBLIC: int  __dbenv_set_verbose __P((DB_ENV *, u_int32_t, int));
 */
int
__dbenv_set_verbose(dbenv, which, on)
	DB_ENV *dbenv;
	u_int32_t which;
	int on;
{
	switch (which) {
	case DB_VERB_CHKPOINT:
	case DB_VERB_DEADLOCK:
	case DB_VERB_RECOVERY:
	case DB_VERB_REPLICATION:
	case DB_VERB_WAITSFOR:
		if (on)
			FLD_SET(dbenv->verbose, which);
		else
			FLD_CLR(dbenv->verbose, which);
		break;
	default:
		return (EINVAL);
	}
	return (0);
}

/*
 * __db_mi_env --
 *	Method illegally called with public environment.
 *
 * PUBLIC: int __db_mi_env __P((DB_ENV *, const char *));
 */
int
__db_mi_env(dbenv, name)
	DB_ENV *dbenv;
	const char *name;
{
	__db_err(dbenv, "%s: method not permitted in shared environment", name);
	return (EINVAL);
}

/*
 * __db_mi_open --
 *	Method illegally called after open.
 *
 * PUBLIC: int __db_mi_open __P((DB_ENV *, const char *, int));
 */
int
__db_mi_open(dbenv, name, after)
	DB_ENV *dbenv;
	const char *name;
	int after;
{
	__db_err(dbenv, "%s: method not permitted %s handle's open method",
	    name, after ? "after" : "before");
	return (EINVAL);
}

/*
 * __db_env_config --
 *	Method or function called without required configuration.
 *
 * PUBLIC: int __db_env_config __P((DB_ENV *, char *, u_int32_t));
 */
int
__db_env_config(dbenv, i, flags)
	DB_ENV *dbenv;
	char *i;
	u_int32_t flags;
{
	char *sub;

	switch (flags) {
	case DB_INIT_LOCK:
		sub = "locking";
		break;
	case DB_INIT_LOG:
		sub = "logging";
		break;
	case DB_INIT_MPOOL:
		sub = "memory pool";
		break;
	case DB_INIT_REP:
		sub = "replication";
		break;
	case DB_INIT_TXN:
		sub = "transaction";
		break;
	default:
		sub = "<unspecified>";
		break;
	}
	__db_err(dbenv,
    "%s interface requires an environment configured for the %s subsystem",
	    i, sub);
	return (EINVAL);
}

static int
__dbenv_set_rpc_server_noclnt(dbenv, cl, host, tsec, ssec, flags)
	DB_ENV *dbenv;
	void *cl;
	const char *host;
	long tsec, ssec;
	u_int32_t flags;
{
	COMPQUIET(host, NULL);
	COMPQUIET(cl, NULL);
	COMPQUIET(tsec, 0);
	COMPQUIET(ssec, 0);
	COMPQUIET(flags, 0);

	__db_err(dbenv,
	    "set_rpc_server method not permitted in non-RPC environment");
	return (DB_OPNOTSUP);
}

static int
__dbenv_memp_dump_bufferpool_info(dbenv, f)
	DB_ENV *dbenv;
	FILE *f;
{
	return __memp_dump_bufferpool_info(dbenv, f);
}

static int
__dbenv_get_rep_eid(dbenv, eid)
	DB_ENV *dbenv;
	char **eid;
{
	return __rep_get_eid(dbenv, eid);
}


static int
__dbenv_get_rep_master(dbenv, master, gen, egen)
	DB_ENV *dbenv;
	char **master;
    u_int32_t *gen;
	u_int32_t *egen;
{
	return __rep_get_master(dbenv, master, gen, egen);
}

static int
__dbenv_get_page_extent_size(dbenv)
	DB_ENV *dbenv;
{
	return dbenv->page_extent_size;
}

static void
__dbenv_set_page_extent_size(dbenv, npages)
	DB_ENV *dbenv;
	u_int32_t npages;
{
	dbenv->page_extent_size = npages;
}
static int
__dbenv_set_bulk_stops_on_page(dbenv, flag)
	DB_ENV *dbenv;
	int flag;
{
	dbenv->bulk_stops_on_page = flag;
	return 0;
}

static int
__dbenv_set_num_recovery_processor_threads(dbenv, num)
	DB_ENV *dbenv;
	int num;
{
	dbenv->num_recovery_processor_threads = num;
	if (dbenv->recovery_processors)
		thdpool_set_maxthds(dbenv->recovery_processors, num);
	return 0;
}

static int
__dbenv_set_num_recovery_worker_threads(dbenv, num)
	DB_ENV *dbenv;
	int num;
{
	dbenv->num_recovery_worker_threads = num;
	if (dbenv->recovery_workers)
		thdpool_set_maxthds(dbenv->recovery_workers, num);
	return 0;
}


static int
__dbenv_set_deadlock_override(dbenv, opt)
	DB_ENV *dbenv;
	u_int32_t opt;
{
	if (opt == DB_LOCK_MINWRITE_NOREAD) {
		dbenv->replicant_use_minwrite_noread = 1;
	} else if (opt == DB_LOCK_MINWRITE_EVER) {
		dbenv->master_use_minwrite_ever = 1;
	} else {
		return -1;
	}

	return 0;
}

static int
__dbenv_set_tran_lowpri(dbenv, txnid)
	DB_ENV *dbenv;
	u_int32_t txnid;
{
	return __lock_locker_set_lowpri(dbenv, txnid);
}

static void
__dbenv_set_recovery_memsize(dbenv, sz)
	DB_ENV *dbenv;
	int sz;
{
	dbenv->recovery_memsize = sz;
}

static int
__dbenv_get_recovery_memsize(dbenv)
	DB_ENV *dbenv;
{
	return dbenv->recovery_memsize;
}

/* TODO: return int and return error if attempted after open? */
static int
__dbenv_set_recovery_lsn(dbenv, lsn)
	DB_ENV *dbenv;
	DB_LSN *lsn;
{
	dbenv->recovery_start_lsn = *lsn;
	return 0;
}

static void
__dbenv_get_rep_verify_lsn(dbenv, current_lsn, start_lsn)
	DB_ENV *dbenv;
	DB_LSN *current_lsn;
	DB_LSN *start_lsn;
{
	if (dbenv->newest_rep_verify_tran_time) {
		*current_lsn = dbenv->rep_verify_current_lsn;
		*start_lsn = dbenv->rep_verify_start_lsn;
	} else {
		ZERO_LSN((*current_lsn));
		ZERO_LSN((*start_lsn));
	}
}

static int
__dbenv_set_is_tmp_tbl(dbenv, is_tmp_tbl)
	DB_ENV *dbenv;
	int is_tmp_tbl;
{
	dbenv->is_tmp_tbl = is_tmp_tbl;
	return 0;
}

static int
__dbenv_set_use_sys_malloc(dbenv, use_sys_malloc)
	DB_ENV *dbenv;
	int use_sys_malloc;
{
	dbenv->use_sys_malloc = use_sys_malloc;
	return 0;
}

static int
__dbenv_blobmem_yield(dbenv)
	DB_ENV *dbenv;
{
	return comdb2bma_yield(dbenv->bma);
}

static int
__dbenv_set_comdb2_dirs(dbenv, data_dir, txn_dir, tmp_dir)
	DB_ENV *dbenv;
	char *data_dir;
	char *txn_dir;
	char *tmp_dir;
{
	dbenv->comdb2_dirs.data_dir = strdup(data_dir);
	dbenv->comdb2_dirs.txn_dir = strdup(txn_dir);
	dbenv->comdb2_dirs.tmp_dir = strdup(tmp_dir);
	return 0;
}

pthread_mutex_t gbl_durable_lsn_lk = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t gbl_durable_lsn_cond = PTHREAD_COND_INITIALIZER;

static void
__dbenv_set_durable_lsn(dbenv, lsnp, generation)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	uint32_t generation;
{
	DB_REP *db_rep;

	db_rep = dbenv->rep_handle;
	extern int gbl_durable_set_trace;

	if (lsnp->file == 2147483647) {
		logmsg(LOGMSG_FATAL, "huh? setting file to 2147483647?\n");
		abort();
	}

	pthread_mutex_lock(&gbl_durable_lsn_lk);

	if (generation > dbenv->durable_generation &&
	    log_compare(lsnp, &dbenv->durable_lsn) < 0) {
		logmsg(LOGMSG_FATAL, "Aborting on reversing durable lsn\n");
		abort();
	}

	if (dbenv->durable_generation < generation ||
	    log_compare(&dbenv->durable_lsn, lsnp) <= 0) {

		dbenv->durable_generation = generation;
		dbenv->durable_lsn = *lsnp;

		if (gbl_durable_set_trace) {
			logmsg(LOGMSG_USER,
			       "Set durable lsn to [%d][%d] generation %u\n",
			       dbenv->durable_lsn.file,
			       dbenv->durable_lsn.offset,
			       dbenv->durable_generation);
		}

		if (lsnp->file == 0) {
			logmsg(LOGMSG_FATAL, "Aborting on attempt to set "
					     "durable lsn file to 0\n");
			abort();
		}
	} else {
		/* This can happen if two commit threads can race against each
		 * other */
		if (gbl_durable_set_trace) {
			logmsg(LOGMSG_USER,
			       "Blocked attempt to set durable lsn from "
			       "[%d][%d] gen %d to [%d][%d] gen %d\n",
			       dbenv->durable_lsn.file,
			       dbenv->durable_lsn.offset,
			       dbenv->durable_generation, lsnp->file,
			       lsnp->offset, generation);
		}
	}

    pthread_cond_broadcast(&gbl_durable_lsn_cond);
	pthread_mutex_unlock(&gbl_durable_lsn_lk);
}

static void
__dbenv_get_durable_lsn(dbenv, lsnp, generation)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	uint32_t *generation;
{
	DB_REP *db_rep = dbenv->rep_handle;
	pthread_mutex_lock(&gbl_durable_lsn_lk);
	*lsnp = dbenv->durable_lsn;
	*generation = dbenv->durable_generation;
	pthread_mutex_unlock(&gbl_durable_lsn_lk);
}

static int
__dbenv_trigger_subscribe(dbenv, fname, cond, lock, open)
	DB_ENV *dbenv;
	const char *fname;
	pthread_cond_t **cond;
	pthread_mutex_t **lock;
	const uint8_t **open;
{
	int rc = 1;
	struct __db_trigger_subscription *t;
	t = __db_get_trigger_subscription(fname);
	pthread_mutex_lock(&t->lock);
	if (t->open) {
		t->active = 1;
		*cond = &t->cond;
		*lock = &t->lock;
		*open = &t->open;
		rc = 0;
	}
	pthread_mutex_unlock(&t->lock);
	return rc;
}

static int
__dbenv_trigger_unsubscribe(dbenv, fname)
	DB_ENV *dbenv;
	const char *fname;
{
	/* trigger_lock should be held by caller */
	struct __db_trigger_subscription *t;
	t = __db_get_trigger_subscription(fname);
	t->active = 0;
	return 0;
}

static int
__dbenv_trigger_open(dbenv, fname)
	DB_ENV *dbenv;
	const char *fname;
{
	struct __db_trigger_subscription *t;
	t = __db_get_trigger_subscription(fname);
	pthread_mutex_lock(&t->lock);
	t->open = 1;
	pthread_cond_signal(&t->cond);
	pthread_mutex_unlock(&t->lock);
	return 0;
}

static int
__dbenv_trigger_close(dbenv, fname)
	DB_ENV *dbenv;
	const char *fname;
{
	struct __db_trigger_subscription *t;
	t = __db_get_trigger_subscription(fname);
	pthread_mutex_lock(&t->lock);
	t->open = 0;
	pthread_cond_signal(&t->cond);
	pthread_mutex_unlock(&t->lock);
	return 0;
}
