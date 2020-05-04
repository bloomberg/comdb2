#ifndef _DB_RECOVERY_INFO_H
#define _DB_RECOVERY_INFO_H

#include <dbinc/hash.h>

/* Determine if this thread should handle this dbp */
int __recthd_dispatch(DB_ENV *dbenv, DB *file_dbp);
int __recthd_fileop_dispatch(DB_ENV *dbenv, const char *f1, const char *f2,
		u_int8_t *fid);
int __recthd_dbreg_dispatch(DB_ENV *dbenv, u_int8_t *fid, db_recops op);

/* Map fileid to thread number */
typedef struct ufid_thd {
	u_int8_t fileid[DB_FILE_ID_LEN];
	int thd;
} ufid_thd_t;

typedef struct fop_thd {
	char *name;
	int thd;
} fop_thd_t;

/* Shared information for threaded recovery */
typedef struct recthd {
	
	/* Decides this threads td-id */
	int *count;

	/* Total threads */
	int total_threads;

	/* Completed backwards */
	int completed_backwards;

	/* Number of requests each thread has serviced */
	int *hitcounts;

	/* FILE-ID to thread map */
	pthread_mutex_t lk;
	pthread_cond_t cd;
	hash_t *fileid_hash;
	hash_t *fop_hash;

	/* Used for backward and forward roll */
	DB_ENV *dbenv;
	DB_LSN lsn;
	DB_LSN stop_lsn;
	DB_LSN outlsn;
    DB **dbpp;

	/* Txninfo */
	void *txninfo;
} recthd_t;

/* Thread local infomation */
typedef struct recovery_info {
	int threadnum;
	recthd_t *r;
} recovery_info_t;

extern __thread recovery_info_t *__recovery_info;

#endif
