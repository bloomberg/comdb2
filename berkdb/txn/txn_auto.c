/* Do not edit: automatically built by gen_rec.awk. */
/* (recently updated for linux) */
#include "db_config.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "db.h"
#include "db_int.h"
#include "dbinc/db_swap.h"
#include <alloca.h>
#include <fsnapf.h>
/* #define __txn_DEBUG to turn on debug trace */
#include "db_config.h"

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#if TIME_WITH_SYS_TIME
#include <sys/time.h>
#include <time.h>
#else
#if HAVE_SYS_TIME_H
#include <sys/time.h>
#else
#include <time.h>
#endif /* HAVE_SYS_TIME_H */
#endif /* TIME_WITH SYS_TIME */

#include <ctype.h>
#include <string.h>
#endif

#include <assert.h>

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_dispatch.h"
#include "dbinc/db_am.h"
#include "dbinc/log.h"
#include "dbinc/txn.h"

extern int gbl_snapisol;

/*
 * PUBLIC: int __txn_regop_log __P((DB_ENV *, DB_TXN *, DB_LSN *,
 * PUBLIC:     u_int64_t *, u_int32_t, u_int32_t, int32_t, const DBT *));
 */
static int
__txn_regop_log_int(dbenv, txnid, ret_lsnp, ret_contextp, flags,
   opcode, timestamp, locks, usr_ptr)
	DB_ENV *dbenv;
	DB_TXN *txnid;
	DB_LSN *ret_lsnp;
	u_int64_t *ret_contextp;
	u_int32_t flags;
	u_int32_t opcode;
	int32_t timestamp;
	const DBT *locks;
    void *usr_ptr;
{
	DBT logrec;
	DB_TXNLOGREC *lr;
	DB_LSN *lsnp, null_lsn;
	u_int32_t zero, uinttmp, rectype, txn_num;
	u_int npad;
	u_int8_t *bp;
	DB *dbp = NULL;
	int is_durable, ret;
	int used_malloc = 0;
	int off_context = -1;
#if 0
    u_int32_t gen = 0;
#endif

#ifdef __txn_DEBUG
	fprintf(stderr,"__txn_regop_log: begin\n");
#endif

	rectype = DB___txn_regop;
	npad = 0;

	is_durable = 1;
	if (LF_ISSET(DB_LOG_NOT_DURABLE) ||
	    F_ISSET(dbenv, DB_ENV_TXN_NOT_DURABLE)) {
		if (txnid == NULL)
			return (0);
		is_durable = 0;
	}
	if (txnid == NULL) {
		txn_num = 0;
		null_lsn.file = 0;
		null_lsn.offset = 0;
		lsnp = &null_lsn;
	}
	else
	{
		if (TAILQ_FIRST(&txnid->kids) != NULL &&
		    (ret = __txn_activekids(dbenv, rectype, txnid)) != 0)
			return (ret);
		txn_num = txnid->txnid;
		lsnp = &txnid->last_lsn;
	}

	logrec.size = sizeof(rectype) + sizeof(txn_num) + sizeof(DB_LSN)
	    + sizeof(u_int32_t)
	    + sizeof(u_int32_t)
	    + sizeof(u_int32_t) + (locks == NULL ? 0 : locks->size);
	if (CRYPTO_ON(dbenv)) {
		npad =
		    ((DB_CIPHER *)dbenv->crypto_handle)->adj_size(logrec.size);
		logrec.size += npad;
	}

	if (!is_durable && txnid != NULL)
	{
		if ((ret = __os_malloc(dbenv,
		    logrec.size + sizeof(DB_TXNLOGREC), &lr)) != 0)
			return (ret);
#ifdef DIAGNOSTIC
		goto do_malloc;
#else
		logrec.data = &lr->data;
#endif

	}
	else
	{
#ifdef DIAGNOSTIC
do_malloc:
#endif

		if (logrec.size > 4096)
		{
			if ((ret =
			    __os_malloc(dbenv, logrec.size, &logrec.data)) != 0) {
#ifdef DIAGNOSTIC
				if (!is_durable && txnid != NULL)
					(void)__os_free(dbenv, lr);
#endif

				return (ret);
			}
			used_malloc = 1;
		}
		else
		{
			used_malloc = 0;
			logrec.data = alloca(logrec.size);
		}
	}
	if (npad > 0)
		memset((u_int8_t *)logrec.data + logrec.size - npad, 0, npad);

	bp = logrec.data;

	LOGCOPY_32(bp, &rectype);
	bp += sizeof(rectype);

	LOGCOPY_32(bp, &txn_num);
	bp += sizeof(txn_num);

	LOGCOPY_FROMLSN(bp, lsnp);
	bp += sizeof(DB_LSN);

	uinttmp = (u_int32_t)opcode;
	LOGCOPY_32(bp, &uinttmp);
	bp += sizeof(uinttmp);

	uinttmp = (u_int32_t)timestamp;
	LOGCOPY_32(bp, &uinttmp);
	bp += sizeof(uinttmp);

	if (locks == NULL)
	{
		zero = 0;
		LOGCOPY_32(bp, &zero);
		bp += sizeof(u_int32_t);
	}
	else
	{
		LOGCOPY_32(bp, &locks->size);
		bp += sizeof(locks->size);
		memcpy(bp, locks->data, locks->size);
		bp += locks->size;
      if (gbl_snapisol)
      {
         /* save location in the log stream, if any */
         if (locks->size > 0)
         {
            off_context = (u_int8_t*)bp-((u_int8_t*)(logrec.data))-sizeof(unsigned long long);

            assert(off_context>=0);
         }
      }
	}

	DB_ASSERT((u_int32_t)(bp - (u_int8_t *)logrec.data) <= logrec.size);

#ifdef DIAGNOSTIC
	if (!is_durable && txnid != NULL) {
		 /*
		 * We set the debug bit if we are going
		 * to log non-durable transactions so
		 * they will be ignored by recovery.
		 */
		memcpy(lr->data, logrec.data, logrec.size);
		rectype |= DB_debug_FLAG;
		LOGCOPY_32(logrec.data, &rectype);
	}
#endif

	if (!is_durable && txnid != NULL) {
		ret = 0;
		STAILQ_INSERT_HEAD(&txnid->logs, lr, links);
#ifdef DIAGNOSTIC
		goto do_put;
#endif

	}
	else
	{
#ifdef DIAGNOSTIC
do_put:
#endif

		ret = __log_put_commit_context(dbenv,
		    ret_lsnp, ret_contextp, (DBT *)&logrec, flags | DB_LOG_NOCOPY, off_context, usr_ptr);
		if (ret == 0 && txnid != NULL)
			txnid->last_lsn = *ret_lsnp;
	}

	if (!is_durable)
		LSN_NOT_LOGGED(*ret_lsnp);
#ifdef LOG_DIAGNOSTIC
	if (ret != 0)
		(void)__txn_regop_print(dbenv,
		    (DBT *)&logrec, ret_lsnp, (db_recops)0 , NULL);
#endif

#ifndef DIAGNOSTIC
	if (is_durable || txnid == NULL)
#endif

		if (used_malloc)
			__os_free(dbenv, logrec.data);

	return (ret);
}


int
__txn_regop_log(dbenv, txnid, ret_lsnp, ret_contextp, flags,
    opcode, timestamp, locks)
	DB_ENV *dbenv;
	DB_TXN *txnid;
	DB_LSN *ret_lsnp;
	u_int64_t *ret_contextp;
	u_int32_t flags;
	u_int32_t opcode;
	int32_t timestamp;
	const DBT *locks;
{
   return __txn_regop_log_int(dbenv, txnid, ret_lsnp, ret_contextp, flags,
      opcode, timestamp, locks, 0);
}   

int
__txn_regop_log_commit(dbenv, txnid, ret_lsnp, ret_contextp, flags,
   opcode, timestamp, locks, usr_ptr)
	DB_ENV *dbenv;
	DB_TXN *txnid;
	DB_LSN *ret_lsnp;
	u_int64_t *ret_contextp;
	u_int32_t flags;
	u_int32_t opcode;
	int32_t timestamp;
	const DBT *locks;
	void *usr_ptr;
{
   return __txn_regop_log_int(dbenv, txnid, ret_lsnp, ret_contextp, flags,
      opcode, timestamp, locks, usr_ptr);
}   



#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_regop_getpgnos __P((DB_ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_regop_getpgnos(dbenv, rec, lsnp, notused1, summary)
	DB_ENV *dbenv;
	DBT *rec;
	DB_LSN *lsnp;
	db_recops notused1;
	void *summary;
{
	TXN_RECS *t;
	int ret;
	COMPQUIET(rec, NULL);
	COMPQUIET(notused1, DB_TXN_ABORT);

	t = (TXN_RECS *)summary;

	if ((ret = __rep_check_alloc(dbenv, t, 1)) != 0)
		return (ret);

	t->array[t->npages].flags = LSN_PAGE_NOLOCK;
	t->array[t->npages].lsn = *lsnp;
	t->array[t->npages].fid = DB_LOGFILEID_INVALID;
	memset(&t->array[t->npages].pgdesc, 0,
	    sizeof(t->array[t->npages].pgdesc));

	t->npages++;

	return (0);
}
#endif /* HAVE_REPLICATION */

#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_regop_getallpgnos __P((DB_ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__txn_regop_getallpgnos(dbenv, rec, lsnp, notused1, summary)
	DB_ENV *dbenv;
	DBT *rec;
	DB_LSN *lsnp;
	db_recops notused1;
	void *summary;
{
	TXN_RECS *t;
	__txn_regop_args *argp;
	int ret = 0;

	COMPQUIET(notused1, DB_TXN_ABORT);

	argp = NULL;
	t = (TXN_RECS *)summary;


err:	if (argp != NULL)
	__os_free(dbenv, argp);

	return (ret);
}
#endif /* HAVE_REPLICATION */

/*
 * PUBLIC: int __txn_regop_read_int __P((DB_ENV *, void *,
 * PUBLIC:     int do_pgswp,  __txn_regop_args **));
 */
int
__txn_regop_read_int(dbenv, recbuf, do_pgswp, argpp)
	DB_ENV *dbenv;
	void *recbuf;
	int do_pgswp;
	__txn_regop_args **argpp;
{
	__txn_regop_args *argp;
	u_int32_t uinttmp;
	u_int8_t *bp;
	DB *dbp = NULL;
	int ret;

#ifdef __txn_DEBUG
	fprintf(stderr,"__txn_regop_read_int: begin\n");
#endif

	if ((ret = __os_malloc(dbenv,
	    sizeof(__txn_regop_args) + sizeof(DB_TXN), &argp)) != 0)
		return (ret);
	argp->txnid = (DB_TXN *)&argp[1];

	bp = recbuf;
	LOGCOPY_32(&argp->type, bp);
	bp += sizeof(argp->type);

	LOGCOPY_32(&argp->txnid->txnid,  bp);
	bp += sizeof(argp->txnid->txnid);

	LOGCOPY_TOLSN(&argp->prev_lsn, bp);
	bp += sizeof(DB_LSN);

	LOGCOPY_32(&uinttmp, bp);
	argp->opcode = (u_int32_t)uinttmp;
	bp += sizeof(uinttmp);

	LOGCOPY_32(&uinttmp, bp);
	argp->timestamp = (int32_t)uinttmp;
	bp += sizeof(uinttmp);

	memset(&argp->locks, 0, sizeof(argp->locks));
	LOGCOPY_32(&argp->locks.size, bp);
	bp += sizeof(u_int32_t);
	argp->locks.data = bp;
	bp += argp->locks.size;

	*argpp = argp;
	return (0);
}

/*
 * PUBLIC: int __txn_regop_print __P((DB_ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
#include <dbinc/lock.h>

int
__txn_regop_print(dbenv, dbtp, lsnp, notused2, notused3)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *notused3;
{
	__txn_regop_args *argp;
    unsigned long long commit_context = 0;
	struct tm *lt;
	u_int32_t i;
	int ch;
	int ret;

	notused2 = DB_TXN_ABORT;
	notused3 = NULL;

	if ((ret = __txn_regop_read_int(dbenv, dbtp->data, 0, &argp)) != 0)
		return (ret);
	(void)printf(
	    "[%lu][%lu]__txn_regop%s: rec: %lu txnid %lx prevlsn [%lu][%lu]\n",
	    (u_long)lsnp->file,
	    (u_long)lsnp->offset,
	    (argp->type & DB_debug_FLAG) ? "_debug" : "",
	    (u_long)argp->type,
	    (u_long)argp->txnid->txnid,
	    (u_long)argp->prev_lsn.file,
	    (u_long)argp->prev_lsn.offset);
	(void)printf("\topcode: %lu\n", (u_long)argp->opcode);
	fflush(stdout);
	time_t timestamp = argp->timestamp;
    lt = localtime((time_t *)&timestamp);
    if (lt)
    {
        (void)printf(
                "\ttimestamp: %ld (%.24s, 20%02lu%02lu%02lu%02lu%02lu.%02lu)\n",
                (long)argp->timestamp, ctime(&timestamp),
                (u_long)lt->tm_year - 100, (u_long)lt->tm_mon+1,
                (u_long)lt->tm_mday, (u_long)lt->tm_hour,
                (u_long)lt->tm_min, (u_long)lt->tm_sec);
    }
    else
    {
        printf("\ttimestamp: %ld\n", (long)argp->timestamp);
    }
	fflush(stdout);
	(void)printf("\tlocks: \n");
    DB_LSN ignored;
    int pglogs;
    u_int32_t keycnt;
    __lock_get_list(dbenv, 0, LOCK_GET_LIST_PRINTLOCK, DB_LOCK_WRITE, 
            &argp->locks, &ignored, (void **)&pglogs, &keycnt, stdout);
	(void)printf("\n");
    if(argp->locks.size >= 8)
    {
        char *p = &((char *)argp->locks.data)[argp->locks.size - 8];
        memcpy(&commit_context, p, 8);
    }
    printf("\tcommit-context: 0x%llx\n", commit_context);
	(void)printf("\n");

    /* Print commit context */

	fflush(stdout);
	(void)printf("\n");
	__os_free(dbenv, argp);

	return (0);
}

/*
 * PUBLIC: int __txn_regop_read __P((DB_ENV *, void *,  __txn_regop_args **));
 */
int
__txn_regop_read(dbenv, recbuf, argpp)
	DB_ENV *dbenv;
	void *recbuf;
	__txn_regop_args **argpp;
{
	return __txn_regop_read_int (dbenv, recbuf, 1, argpp);
}

unsigned long long
__txn_regop_read_context(argp)
	__txn_regop_args *argp;
{
    unsigned long long commit_context = 0;

    if(argp->locks.size >= 8)
    {
        char *p = &((char *)argp->locks.data)[argp->locks.size - 8];
        memcpy(&commit_context, p, 8);
    }

    return commit_context;
}

/*
 * PUBLIC: int __txn_ckp_log __P((DB_ENV *, DB_TXN *, DB_LSN *,
 * PUBLIC:     u_int32_t, DB_LSN *, DB_LSN *, int32_t, u_int32_t));
 */
int
__txn_ckp_log(dbenv, txnid, ret_lsnp, flags,
    ckp_lsn, last_ckp, timestamp, rep_gen)
	DB_ENV *dbenv;
	DB_TXN *txnid;
	DB_LSN *ret_lsnp;
	u_int32_t flags;
	DB_LSN * ckp_lsn;
	DB_LSN * last_ckp;
	int32_t timestamp;
	u_int32_t rep_gen;
{
	DBT logrec;
	DB_TXNLOGREC *lr;
	DB_LSN *lsnp, null_lsn;
	u_int32_t uinttmp, rectype, txn_num;
	u_int npad;
	u_int8_t *bp;
	DB *dbp = NULL;
	int is_durable, ret;
	int used_malloc = 0;

    if (last_ckp->file == 0 && last_ckp->offset == 0 && ckp_lsn->file != 1 && ckp_lsn->offset != 28) {
        __db_err(dbenv, "Logging a non-first checkpoint with a 0:0 last checkpoint lsn\n");
        abort();
    }

#ifdef __txn_DEBUG
	fprintf(stderr,"__txn_ckp_log: begin\n");
#endif

	rectype = DB___txn_ckp;
	npad = 0;

	is_durable = 1;
	if (LF_ISSET(DB_LOG_NOT_DURABLE) ||
	    F_ISSET(dbenv, DB_ENV_TXN_NOT_DURABLE)) {
		if (txnid == NULL)
			return (0);
		is_durable = 0;
	}
	if (txnid == NULL) {
		txn_num = 0;
		null_lsn.file = 0;
		null_lsn.offset = 0;
		lsnp = &null_lsn;
	}
	else
	{
		if (TAILQ_FIRST(&txnid->kids) != NULL &&
		    (ret = __txn_activekids(dbenv, rectype, txnid)) != 0)
			return (ret);
		txn_num = txnid->txnid;
		lsnp = &txnid->last_lsn;
	}

	logrec.size = sizeof(rectype) + sizeof(txn_num) + sizeof(DB_LSN)
	    + sizeof(*ckp_lsn)
	    + sizeof(*last_ckp)
	    + sizeof(u_int32_t)
	    + sizeof(u_int32_t);
	if (CRYPTO_ON(dbenv)) {
		npad =
		    ((DB_CIPHER *)dbenv->crypto_handle)->adj_size(logrec.size);
		logrec.size += npad;
	}

	if (!is_durable && txnid != NULL)
	{
		if ((ret = __os_malloc(dbenv,
		    logrec.size + sizeof(DB_TXNLOGREC), &lr)) != 0)
			return (ret);
#ifdef DIAGNOSTIC
		goto do_malloc;
#else
		logrec.data = &lr->data;
#endif

	}
	else
	{
#ifdef DIAGNOSTIC
do_malloc:
#endif

		if (logrec.size > 4096)
		{
			if ((ret =
			    __os_malloc(dbenv, logrec.size, &logrec.data)) != 0) {
#ifdef DIAGNOSTIC
				if (!is_durable && txnid != NULL)
					(void)__os_free(dbenv, lr);
#endif

				return (ret);
			}
			used_malloc = 1;
		}
		else
		{
			used_malloc = 0;
			logrec.data = alloca(logrec.size);
		}
	}
	if (npad > 0)
		memset((u_int8_t *)logrec.data + logrec.size - npad, 0, npad);

	bp = logrec.data;

	LOGCOPY_32(bp, &rectype);
	bp += sizeof(rectype);

	LOGCOPY_32(bp, &txn_num);
	bp += sizeof(txn_num);

	LOGCOPY_FROMLSN(bp, lsnp);
	bp += sizeof(DB_LSN);

	if (ckp_lsn != NULL)
		LOGCOPY_FROMLSN(bp, ckp_lsn);
	else
		memset(bp, 0, sizeof(*ckp_lsn));
	bp += sizeof(*ckp_lsn);

	if (last_ckp != NULL)
		LOGCOPY_FROMLSN(bp, last_ckp);
	else
		memset(bp, 0, sizeof(*last_ckp));
	bp += sizeof(*last_ckp);

	uinttmp = (u_int32_t)timestamp;
	LOGCOPY_32(bp, &uinttmp);
	bp += sizeof(uinttmp);

	uinttmp = (u_int32_t)rep_gen;
	LOGCOPY_32(bp, &uinttmp);
	bp += sizeof(uinttmp);

	DB_ASSERT((u_int32_t)(bp - (u_int8_t *)logrec.data) <= logrec.size);

#ifdef DIAGNOSTIC
	if (!is_durable && txnid != NULL) {
		 /*
		 * We set the debug bit if we are going
		 * to log non-durable transactions so
		 * they will be ignored by recovery.
		 */
		memcpy(lr->data, logrec.data, logrec.size);
		rectype |= DB_debug_FLAG;
		LOGCOPY_32(logrec.data, &rectype);
	}
#endif

	if (!is_durable && txnid != NULL) {
		ret = 0;
		STAILQ_INSERT_HEAD(&txnid->logs, lr, links);
#ifdef DIAGNOSTIC
		goto do_put;
#endif

	}
	else
	{
#ifdef DIAGNOSTIC
do_put:
#endif

		ret = __log_put(dbenv,
		    ret_lsnp, (DBT *)&logrec, flags | DB_LOG_NOCOPY);
		if (ret == 0 && txnid != NULL)
			txnid->last_lsn = *ret_lsnp;
	}

	if (!is_durable)
		LSN_NOT_LOGGED(*ret_lsnp);
#ifdef LOG_DIAGNOSTIC
	if (ret != 0)
		(void)__txn_ckp_print(dbenv,
		    (DBT *)&logrec, ret_lsnp, (db_recops)0 , NULL);
#endif

#ifndef DIAGNOSTIC
	if (is_durable || txnid == NULL)
#endif

		if (used_malloc)
			__os_free(dbenv, logrec.data);

	return (ret);
}

#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_ckp_getallpgnos __P((DB_ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_ckp_getallpgnos(dbenv, rec, lsnp, notused1, summary)
	DB_ENV *dbenv;
	DBT *rec;
	DB_LSN *lsnp;
	db_recops notused1;
	void *summary;
{
    DB *dbp;
	TXN_RECS *t;
    __txn_ckp_args *argp;
    int ret = 0;

	COMPQUIET(notused1, DB_TXN_ABORT);

    argp = NULL;
	t = (TXN_RECS *)summary;


err:    if (argp != NULL)
    __os_free(dbenv, argp);

    return (ret);
}
#endif /* HAVE_REPLICATION */


#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_ckp_getpgnos __P((DB_ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_ckp_getpgnos(dbenv, rec, lsnp, notused1, summary)
	DB_ENV *dbenv;
	DBT *rec;
	DB_LSN *lsnp;
	db_recops notused1;
	void *summary;
{
	TXN_RECS *t;
	int ret;
	COMPQUIET(rec, NULL);
	COMPQUIET(notused1, DB_TXN_ABORT);

	t = (TXN_RECS *)summary;

	if ((ret = __rep_check_alloc(dbenv, t, 1)) != 0)
		return (ret);

	t->array[t->npages].flags = LSN_PAGE_NOLOCK;
	t->array[t->npages].lsn = *lsnp;
	t->array[t->npages].fid = DB_LOGFILEID_INVALID;
	memset(&t->array[t->npages].pgdesc, 0,
	    sizeof(t->array[t->npages].pgdesc));

	t->npages++;

	return (0);
}
#endif /* HAVE_REPLICATION */

/*
 * PUBLIC: int __txn_ckp_read_int __P((DB_ENV *, void *,
 * PUBLIC:     int do_pgswp,  __txn_ckp_args **));
 */
int
__txn_ckp_read_int(dbenv, recbuf, do_pgswp, argpp)
	DB_ENV *dbenv;
	void *recbuf;
	int do_pgswp;
	__txn_ckp_args **argpp;
{
	__txn_ckp_args *argp;
	u_int32_t uinttmp;
	u_int8_t *bp;
	DB *dbp = NULL;
	int ret;

#ifdef __txn_DEBUG
	fprintf(stderr,"__txn_ckp_read_int: begin\n");
#endif

	if ((ret = __os_malloc(dbenv,
	    sizeof(__txn_ckp_args) + sizeof(DB_TXN), &argp)) != 0)
		return (ret);
	argp->txnid = (DB_TXN *)&argp[1];

	bp = recbuf;
	LOGCOPY_32(&argp->type, bp);
	bp += sizeof(argp->type);

	LOGCOPY_32(&argp->txnid->txnid,  bp);
	bp += sizeof(argp->txnid->txnid);

	LOGCOPY_TOLSN(&argp->prev_lsn, bp);
	bp += sizeof(DB_LSN);

	LOGCOPY_TOLSN(&argp->ckp_lsn, bp);
	bp += sizeof(argp->ckp_lsn);

	LOGCOPY_TOLSN(&argp->last_ckp, bp);
	bp += sizeof(argp->last_ckp);

	LOGCOPY_32(&uinttmp, bp);
	argp->timestamp = (int32_t)uinttmp;
	bp += sizeof(uinttmp);

	LOGCOPY_32(&uinttmp, bp);
	argp->rep_gen = (u_int32_t)uinttmp;
	bp += sizeof(uinttmp);

	*argpp = argp;
	return (0);
}

/*
 * PUBLIC: int __txn_ckp_print __P((DB_ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_ckp_print(dbenv, dbtp, lsnp, notused2, notused3)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *notused3;
{
	__txn_ckp_args *argp;
	struct tm *lt;
	int ret;

	notused2 = DB_TXN_ABORT;
	notused3 = NULL;

	if ((ret = __txn_ckp_read_int(dbenv, dbtp->data, 0, &argp)) != 0)
		return (ret);
	(void)printf(
	    "[%lu][%lu]__txn_ckp%s: rec: %lu txnid %lx prevlsn [%lu][%lu]\n",
	    (u_long)lsnp->file,
	    (u_long)lsnp->offset,
	    (argp->type & DB_debug_FLAG) ? "_debug" : "",
	    (u_long)argp->type,
	    (u_long)argp->txnid->txnid,
	    (u_long)argp->prev_lsn.file,
	    (u_long)argp->prev_lsn.offset);
	(void)printf("\tckp_lsn: [%lu][%lu]\n",
	    (u_long)argp->ckp_lsn.file, (u_long)argp->ckp_lsn.offset);
	fflush(stdout);
	(void)printf("\tlast_ckp: [%lu][%lu]\n",
	    (u_long)argp->last_ckp.file, (u_long)argp->last_ckp.offset);
	fflush(stdout);
	time_t timestamp = argp->timestamp;
    lt = localtime((time_t *)&timestamp);
    if (lt)
    {
        (void)printf(
                "\ttimestamp: %ld (%.24s, 20%02lu%02lu%02lu%02lu%02lu.%02lu)\n",
                (long)argp->timestamp, ctime(&timestamp),
                (u_long)lt->tm_year - 100, (u_long)lt->tm_mon+1,
                (u_long)lt->tm_mday, (u_long)lt->tm_hour,
                (u_long)lt->tm_min, (u_long)lt->tm_sec);
    }
    else
    {
        printf("\ttimestamp: %ld\n", (long)argp->timestamp);
    }
	fflush(stdout);
	(void)printf("\trep_gen: %ld\n", (long)argp->rep_gen);
	fflush(stdout);
	(void)printf("\n");
	__os_free(dbenv, argp);

	return (0);
}

/*
 * PUBLIC: int __txn_ckp_read __P((DB_ENV *, void *,  __txn_ckp_args **));
 */
int
__txn_ckp_read(dbenv, recbuf, argpp)
	DB_ENV *dbenv;
	void *recbuf;
	__txn_ckp_args **argpp;
{
	return __txn_ckp_read_int (dbenv, recbuf, 1, argpp);
}

/*
 * PUBLIC: int __txn_child_log __P((DB_ENV *, DB_TXN *, DB_LSN *,
 * PUBLIC:     u_int32_t, u_int32_t, DB_LSN *));
 */
int
__txn_child_log(dbenv, txnid, ret_lsnp, flags,
    child, c_lsn)
	DB_ENV *dbenv;
	DB_TXN *txnid;
	DB_LSN *ret_lsnp;
	u_int32_t flags;
	u_int32_t child;
	DB_LSN * c_lsn;
{
	DBT logrec;
	DB_TXNLOGREC *lr;
	DB_LSN *lsnp, null_lsn;
	u_int32_t uinttmp, rectype, txn_num;
	u_int npad;
	u_int8_t *bp;
	DB *dbp = NULL;
	int is_durable, ret;
	int used_malloc = 0;

#ifdef __txn_DEBUG
	fprintf(stderr,"__txn_child_log: begin\n");
#endif

	rectype = DB___txn_child;
	npad = 0;

	is_durable = 1;
	if (LF_ISSET(DB_LOG_NOT_DURABLE) ||
	    F_ISSET(dbenv, DB_ENV_TXN_NOT_DURABLE)) {
		if (txnid == NULL)
			return (0);
		is_durable = 0;
	}
	if (txnid == NULL) {
		txn_num = 0;
		null_lsn.file = 0;
		null_lsn.offset = 0;
		lsnp = &null_lsn;
	}
	else
	{
		if (TAILQ_FIRST(&txnid->kids) != NULL &&
		    (ret = __txn_activekids(dbenv, rectype, txnid)) != 0)
			return (ret);
		txn_num = txnid->txnid;
		lsnp = &txnid->last_lsn;
	}

	logrec.size = sizeof(rectype) + sizeof(txn_num) + sizeof(DB_LSN)
	    + sizeof(u_int32_t)
	    + sizeof(*c_lsn);
	if (CRYPTO_ON(dbenv)) {
		npad =
		    ((DB_CIPHER *)dbenv->crypto_handle)->adj_size(logrec.size);
		logrec.size += npad;
	}

	if (!is_durable && txnid != NULL)
	{
		if ((ret = __os_malloc(dbenv,
		    logrec.size + sizeof(DB_TXNLOGREC), &lr)) != 0)
			return (ret);
#ifdef DIAGNOSTIC
		goto do_malloc;
#else
		logrec.data = &lr->data;
#endif

	}
	else
	{
#ifdef DIAGNOSTIC
do_malloc:
#endif

		if (logrec.size > 4096)
		{
			if ((ret =
			    __os_malloc(dbenv, logrec.size, &logrec.data)) != 0) {
#ifdef DIAGNOSTIC
				if (!is_durable && txnid != NULL)
					(void)__os_free(dbenv, lr);
#endif

				return (ret);
			}
			used_malloc = 1;
		}
		else
		{
			used_malloc = 0;
			logrec.data = alloca(logrec.size);
		}
	}
	if (npad > 0)
		memset((u_int8_t *)logrec.data + logrec.size - npad, 0, npad);

	bp = logrec.data;

	LOGCOPY_32(bp, &rectype);
	bp += sizeof(rectype);

	LOGCOPY_32(bp, &txn_num);
	bp += sizeof(txn_num);

	LOGCOPY_FROMLSN(bp, lsnp);
	bp += sizeof(DB_LSN);

	uinttmp = (u_int32_t)child;
	LOGCOPY_32(bp, &uinttmp);
	bp += sizeof(uinttmp);

	if (c_lsn != NULL)
		LOGCOPY_FROMLSN(bp, c_lsn);
	else
		memset(bp, 0, sizeof(*c_lsn));
	bp += sizeof(*c_lsn);

	DB_ASSERT((u_int32_t)(bp - (u_int8_t *)logrec.data) <= logrec.size);

#ifdef DIAGNOSTIC
	if (!is_durable && txnid != NULL) {
		 /*
		 * We set the debug bit if we are going
		 * to log non-durable transactions so
		 * they will be ignored by recovery.
		 */
		memcpy(lr->data, logrec.data, logrec.size);
		rectype |= DB_debug_FLAG;
		LOGCOPY_32(logrec.data, &rectype);
	}
#endif

	if (!is_durable && txnid != NULL) {
		ret = 0;
		STAILQ_INSERT_HEAD(&txnid->logs, lr, links);
#ifdef DIAGNOSTIC
		goto do_put;
#endif

	}
	else
	{
#ifdef DIAGNOSTIC
do_put:
#endif

		ret = __log_put(dbenv,
		    ret_lsnp, (DBT *)&logrec, flags | DB_LOG_NOCOPY);
		if (ret == 0 && txnid != NULL)
			txnid->last_lsn = *ret_lsnp;
	}

	if (!is_durable)
		LSN_NOT_LOGGED(*ret_lsnp);
#ifdef LOG_DIAGNOSTIC
	if (ret != 0)
		(void)__txn_child_print(dbenv,
		    (DBT *)&logrec, ret_lsnp, (db_recops)0 , NULL);
#endif

#ifndef DIAGNOSTIC
	if (is_durable || txnid == NULL)
#endif

		if (used_malloc)
			__os_free(dbenv, logrec.data);

	return (ret);
}

#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_child_getallpgnos __P((DB_ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__txn_child_getallpgnos(dbenv, rec, lsnp, notused1, summary)
	DB_ENV *dbenv;
	DBT *rec;
	DB_LSN *lsnp;
	db_recops notused1;
	void *summary;
{
    DB *dbp;
	TXN_RECS *t;
    __txn_child_args *argp;
    int ret = 0;

	COMPQUIET(notused1, DB_TXN_ABORT);

    argp = NULL;
	t = (TXN_RECS *)summary;


err:    if (argp != NULL)
    __os_free(dbenv, argp);

    return (ret);
}
#endif /* HAVE_REPLICATION */


#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_child_getpgnos __P((DB_ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_child_getpgnos(dbenv, rec, lsnp, notused1, summary)
	DB_ENV *dbenv;
	DBT *rec;
	DB_LSN *lsnp;
	db_recops notused1;
	void *summary;
{
	TXN_RECS *t;
	int ret;
	COMPQUIET(rec, NULL);
	COMPQUIET(notused1, DB_TXN_ABORT);

	t = (TXN_RECS *)summary;

	if ((ret = __rep_check_alloc(dbenv, t, 1)) != 0)
		return (ret);

	t->array[t->npages].flags = LSN_PAGE_NOLOCK;
	t->array[t->npages].lsn = *lsnp;
	t->array[t->npages].fid = DB_LOGFILEID_INVALID;
	memset(&t->array[t->npages].pgdesc, 0,
	    sizeof(t->array[t->npages].pgdesc));

	t->npages++;

	return (0);
}
#endif /* HAVE_REPLICATION */

/*
 * PUBLIC: int __txn_child_read_int __P((DB_ENV *, void *,
 * PUBLIC:     int do_pgswp,  __txn_child_args **));
 */
int
__txn_child_read_int(dbenv, recbuf, do_pgswp, argpp)
	DB_ENV *dbenv;
	void *recbuf;
	int do_pgswp;
	__txn_child_args **argpp;
{
	__txn_child_args *argp;
	u_int32_t uinttmp;
	u_int8_t *bp;
	DB *dbp = NULL;
	int ret;

#ifdef __txn_DEBUG
	fprintf(stderr,"__txn_child_read_int: begin\n");
#endif

	if ((ret = __os_malloc(dbenv,
	    sizeof(__txn_child_args) + sizeof(DB_TXN), &argp)) != 0)
		return (ret);
	argp->txnid = (DB_TXN *)&argp[1];

	bp = recbuf;
	LOGCOPY_32(&argp->type, bp);
	bp += sizeof(argp->type);

	LOGCOPY_32(&argp->txnid->txnid,  bp);
	bp += sizeof(argp->txnid->txnid);

	LOGCOPY_TOLSN(&argp->prev_lsn, bp);
	bp += sizeof(DB_LSN);

	LOGCOPY_32(&uinttmp, bp);
	argp->child = (u_int32_t)uinttmp;
	bp += sizeof(uinttmp);

	LOGCOPY_TOLSN(&argp->c_lsn, bp);
	bp += sizeof(argp->c_lsn);

	*argpp = argp;
	return (0);
}

/*
 * PUBLIC: int __txn_child_print __P((DB_ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_child_print(dbenv, dbtp, lsnp, notused2, notused3)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *notused3;
{
	__txn_child_args *argp;
	int ret;

	notused2 = DB_TXN_ABORT;
	notused3 = NULL;

	if ((ret = __txn_child_read_int(dbenv, dbtp->data, 0, &argp)) != 0)
		return (ret);
	(void)printf(
	    "[%lu][%lu]__txn_child%s: rec: %lu txnid %lx prevlsn [%lu][%lu]\n",
	    (u_long)lsnp->file,
	    (u_long)lsnp->offset,
	    (argp->type & DB_debug_FLAG) ? "_debug" : "",
	    (u_long)argp->type,
	    (u_long)argp->txnid->txnid,
	    (u_long)argp->prev_lsn.file,
	    (u_long)argp->prev_lsn.offset);
	(void)printf("\tchild: 0x%lx\n", (u_long)argp->child);
	fflush(stdout);
	(void)printf("\tc_lsn: [%lu][%lu]\n",
	    (u_long)argp->c_lsn.file, (u_long)argp->c_lsn.offset);
	fflush(stdout);
	(void)printf("\n");
	__os_free(dbenv, argp);

	return (0);
}

/*
 * PUBLIC: int __txn_child_read __P((DB_ENV *, void *,  __txn_child_args **));
 */
int
__txn_child_read(dbenv, recbuf, argpp)
	DB_ENV *dbenv;
	void *recbuf;
	__txn_child_args **argpp;
{
	return __txn_child_read_int (dbenv, recbuf, 1, argpp);
}

/*
 * PUBLIC: int __txn_xa_regop_log __P((DB_ENV *, DB_TXN *, DB_LSN *,
 * PUBLIC:     u_int32_t, u_int32_t, const DBT *, int32_t, u_int32_t, u_int32_t,
 * PUBLIC:     DB_LSN *, const DBT *));
 */
int
__txn_xa_regop_log(dbenv, txnid, ret_lsnp, flags,
    opcode, xid, formatID, gtrid, bqual, begin_lsn,
    locks)
	DB_ENV *dbenv;
	DB_TXN *txnid;
	DB_LSN *ret_lsnp;
	u_int32_t flags;
	u_int32_t opcode;
	const DBT *xid;
	int32_t formatID;
	u_int32_t gtrid;
	u_int32_t bqual;
	DB_LSN * begin_lsn;
	const DBT *locks;
{
	DBT logrec;
	DB_TXNLOGREC *lr;
	DB_LSN *lsnp, null_lsn;
	u_int32_t zero, uinttmp, rectype, txn_num;
	u_int npad;
	u_int8_t *bp;
	DB *dbp = NULL;
	int is_durable, ret;
	int used_malloc = 0;

#ifdef __txn_DEBUG
	fprintf(stderr,"__txn_xa_regop_log: begin\n");
#endif

	rectype = DB___txn_xa_regop;
	npad = 0;

	is_durable = 1;
	if (LF_ISSET(DB_LOG_NOT_DURABLE) ||
	    F_ISSET(dbenv, DB_ENV_TXN_NOT_DURABLE)) {
		if (txnid == NULL)
			return (0);
		is_durable = 0;
	}
	if (txnid == NULL) {
		txn_num = 0;
		null_lsn.file = 0;
		null_lsn.offset = 0;
		lsnp = &null_lsn;
	}
	else
	{
		if (TAILQ_FIRST(&txnid->kids) != NULL &&
		    (ret = __txn_activekids(dbenv, rectype, txnid)) != 0)
			return (ret);
		txn_num = txnid->txnid;
		lsnp = &txnid->last_lsn;
	}

	logrec.size = sizeof(rectype) + sizeof(txn_num) + sizeof(DB_LSN)
	    + sizeof(u_int32_t)
	    + sizeof(u_int32_t) + (xid == NULL ? 0 : xid->size)
	    + sizeof(u_int32_t)
	    + sizeof(u_int32_t)
	    + sizeof(u_int32_t)
	    + sizeof(*begin_lsn)
	    + sizeof(u_int32_t) + (locks == NULL ? 0 : locks->size);
	if (CRYPTO_ON(dbenv)) {
		npad =
		    ((DB_CIPHER *)dbenv->crypto_handle)->adj_size(logrec.size);
		logrec.size += npad;
	}

	if (!is_durable && txnid != NULL)
	{
		if ((ret = __os_malloc(dbenv,
		    logrec.size + sizeof(DB_TXNLOGREC), &lr)) != 0)
			return (ret);
#ifdef DIAGNOSTIC
		goto do_malloc;
#else
		logrec.data = &lr->data;
#endif

	}
	else
	{
#ifdef DIAGNOSTIC
do_malloc:
#endif

		if (logrec.size > 4096)
		{
			if ((ret =
			    __os_malloc(dbenv, logrec.size, &logrec.data)) != 0) {
#ifdef DIAGNOSTIC
				if (!is_durable && txnid != NULL)
					(void)__os_free(dbenv, lr);
#endif

				return (ret);
			}
			used_malloc = 1;
		}
		else
		{
			used_malloc = 0;
			logrec.data = alloca(logrec.size);
		}
	}
	if (npad > 0)
		memset((u_int8_t *)logrec.data + logrec.size - npad, 0, npad);

	bp = logrec.data;

	LOGCOPY_32(bp, &rectype);
	bp += sizeof(rectype);

	LOGCOPY_32(bp, &txn_num);
	bp += sizeof(txn_num);

	LOGCOPY_FROMLSN(bp, lsnp);
	bp += sizeof(DB_LSN);

	uinttmp = (u_int32_t)opcode;
	LOGCOPY_32(bp, &uinttmp);
	bp += sizeof(uinttmp);

	if (xid == NULL)
	{
		zero = 0;
		LOGCOPY_32(bp, &zero);
		bp += sizeof(u_int32_t);
	}
	else
	{
		LOGCOPY_32(bp, &xid->size);
		bp += sizeof(xid->size);
		memcpy(bp, xid->data, xid->size);
		bp += xid->size;
	}

	uinttmp = (u_int32_t)formatID;
	LOGCOPY_32(bp, &uinttmp);
	bp += sizeof(uinttmp);

	uinttmp = (u_int32_t)gtrid;
	LOGCOPY_32(bp, &uinttmp);
	bp += sizeof(uinttmp);

	uinttmp = (u_int32_t)bqual;
	LOGCOPY_32(bp, &uinttmp);
	bp += sizeof(uinttmp);

	if (begin_lsn != NULL)
		LOGCOPY_FROMLSN(bp, begin_lsn);
	else
		memset(bp, 0, sizeof(*begin_lsn));
	bp += sizeof(*begin_lsn);

	if (locks == NULL)
	{
		zero = 0;
		LOGCOPY_32(bp, &zero);
		bp += sizeof(u_int32_t);
	}
	else
	{
		LOGCOPY_32(bp, &locks->size);
		bp += sizeof(locks->size);
		memcpy(bp, locks->data, locks->size);
		bp += locks->size;
	}

	DB_ASSERT((u_int32_t)(bp - (u_int8_t *)logrec.data) <= logrec.size);

#ifdef DIAGNOSTIC
	if (!is_durable && txnid != NULL) {
		 /*
		 * We set the debug bit if we are going
		 * to log non-durable transactions so
		 * they will be ignored by recovery.
		 */
		memcpy(lr->data, logrec.data, logrec.size);
		rectype |= DB_debug_FLAG;
		LOGCOPY_32(logrec.data, &rectype);
	}
#endif

	if (!is_durable && txnid != NULL) {
		ret = 0;
		STAILQ_INSERT_HEAD(&txnid->logs, lr, links);
#ifdef DIAGNOSTIC
		goto do_put;
#endif

	}
	else
	{
#ifdef DIAGNOSTIC
do_put:
#endif

		ret = __log_put(dbenv,
		    ret_lsnp, (DBT *)&logrec, flags | DB_LOG_NOCOPY);
		if (ret == 0 && txnid != NULL)
			txnid->last_lsn = *ret_lsnp;
	}

	if (!is_durable)
		LSN_NOT_LOGGED(*ret_lsnp);
#ifdef LOG_DIAGNOSTIC
	if (ret != 0)
		(void)__txn_xa_regop_print(dbenv,
		    (DBT *)&logrec, ret_lsnp, (db_recops)0 , NULL);
#endif

#ifndef DIAGNOSTIC
	if (is_durable || txnid == NULL)
#endif

		if (used_malloc)
			__os_free(dbenv, logrec.data);

	return (ret);
}

#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_xa_regop_getpgnos __P((DB_ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__txn_xa_regop_getpgnos(dbenv, rec, lsnp, notused1, summary)
	DB_ENV *dbenv;
	DBT *rec;
	DB_LSN *lsnp;
	db_recops notused1;
	void *summary;
{
	TXN_RECS *t;
	int ret;
	COMPQUIET(rec, NULL);
	COMPQUIET(notused1, DB_TXN_ABORT);

	t = (TXN_RECS *)summary;

	if ((ret = __rep_check_alloc(dbenv, t, 1)) != 0)
		return (ret);

	t->array[t->npages].flags = LSN_PAGE_NOLOCK;
	t->array[t->npages].lsn = *lsnp;
	t->array[t->npages].fid = DB_LOGFILEID_INVALID;
	memset(&t->array[t->npages].pgdesc, 0,
	    sizeof(t->array[t->npages].pgdesc));

	t->npages++;

	return (0);
}
#endif /* HAVE_REPLICATION */

#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_xa_regop_getallpgnos __P((DB_ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__txn_xa_regop_getallpgnos(dbenv, rec, lsnp, notused1, summary)
	DB_ENV *dbenv;
	DBT *rec;
	DB_LSN *lsnp;
	db_recops notused1;
	void *summary;
{
    DB *dbp;
	TXN_RECS *t;
	__txn_xa_regop_args *argp;
	int ret = 0;

	COMPQUIET(notused1, DB_TXN_ABORT);

	argp = NULL;
	t = (TXN_RECS *)summary;


err:	if (argp != NULL)
	__os_free(dbenv, argp);

	return (ret);
}
#endif /* HAVE_REPLICATION */


/*
 * PUBLIC: int __txn_xa_regop_read_int __P((DB_ENV *, void *,
 * PUBLIC:     int do_pgswp,  __txn_xa_regop_args **));
 */
int
__txn_xa_regop_read_int(dbenv, recbuf, do_pgswp, argpp)
	DB_ENV *dbenv;
	void *recbuf;
	int do_pgswp;
	__txn_xa_regop_args **argpp;
{
	__txn_xa_regop_args *argp;
	u_int32_t uinttmp;
	u_int8_t *bp;
	DB *dbp = NULL;
	int ret;

#ifdef __txn_DEBUG
	fprintf(stderr,"__txn_xa_regop_read_int: begin\n");
#endif

	if ((ret = __os_malloc(dbenv,
	    sizeof(__txn_xa_regop_args) + sizeof(DB_TXN), &argp)) != 0)
		return (ret);
	argp->txnid = (DB_TXN *)&argp[1];

	bp = recbuf;
	LOGCOPY_32(&argp->type, bp);
	bp += sizeof(argp->type);

	LOGCOPY_32(&argp->txnid->txnid,  bp);
	bp += sizeof(argp->txnid->txnid);

	LOGCOPY_TOLSN(&argp->prev_lsn, bp);
	bp += sizeof(DB_LSN);

	LOGCOPY_32(&uinttmp, bp);
	argp->opcode = (u_int32_t)uinttmp;
	bp += sizeof(uinttmp);

	memset(&argp->xid, 0, sizeof(argp->xid));
	LOGCOPY_32(&argp->xid.size, bp);
	bp += sizeof(u_int32_t);
	argp->xid.data = bp;
	bp += argp->xid.size;

	LOGCOPY_32(&uinttmp, bp);
	argp->formatID = (int32_t)uinttmp;
	bp += sizeof(uinttmp);

	LOGCOPY_32(&uinttmp, bp);
	argp->gtrid = (u_int32_t)uinttmp;
	bp += sizeof(uinttmp);

	LOGCOPY_32(&uinttmp, bp);
	argp->bqual = (u_int32_t)uinttmp;
	bp += sizeof(uinttmp);

	LOGCOPY_TOLSN(&argp->begin_lsn, bp);
	bp += sizeof(argp->begin_lsn);

	memset(&argp->locks, 0, sizeof(argp->locks));
	LOGCOPY_32(&argp->locks.size, bp);
	bp += sizeof(u_int32_t);
	argp->locks.data = bp;
	bp += argp->locks.size;

	*argpp = argp;
	return (0);
}

/*
 * PUBLIC: int __txn_xa_regop_print __P((DB_ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_xa_regop_print(dbenv, dbtp, lsnp, notused2, notused3)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *notused3;
{
	__txn_xa_regop_args *argp;
	u_int32_t i;
	int ch;
	int ret;

	notused2 = DB_TXN_ABORT;
	notused3 = NULL;

	if ((ret = __txn_xa_regop_read_int(dbenv, dbtp->data, 0, &argp)) != 0)
		return (ret);
	(void)printf(
	    "[%lu][%lu]__txn_xa_regop%s: rec: %lu txnid %lx prevlsn [%lu][%lu]\n",
	    (u_long)lsnp->file,
	    (u_long)lsnp->offset,
	    (argp->type & DB_debug_FLAG) ? "_debug" : "",
	    (u_long)argp->type,
	    (u_long)argp->txnid->txnid,
	    (u_long)argp->prev_lsn.file,
	    (u_long)argp->prev_lsn.offset);
	(void)printf("\topcode: %lu\n", (u_long)argp->opcode);
	fflush(stdout);
	(void)printf("\txid: \n");
	fsnapf(stdout, argp->xid.data, argp->xid.size);
	fflush(stdout);
	(void)printf("\tformatID: %ld\n", (long)argp->formatID);
	fflush(stdout);
	(void)printf("\tgtrid: %u\n", argp->gtrid);
	fflush(stdout);
	(void)printf("\tbqual: %u\n", argp->bqual);
	fflush(stdout);
	(void)printf("\tbegin_lsn: [%lu][%lu]\n",
	    (u_long)argp->begin_lsn.file, (u_long)argp->begin_lsn.offset);
	fflush(stdout);
	(void)printf("\tlocks: \n");
	fsnapf(stdout, argp->locks.data, argp->locks.size);
	fflush(stdout);
	(void)printf("\n");
	__os_free(dbenv, argp);

	return (0);
}

/*
 * PUBLIC: int __txn_xa_regop_read __P((DB_ENV *, void *,
 * PUBLIC:      __txn_xa_regop_args **));
 */
int
__txn_xa_regop_read(dbenv, recbuf, argpp)
	DB_ENV *dbenv;
	void *recbuf;
	__txn_xa_regop_args **argpp;
{
	return __txn_xa_regop_read_int (dbenv, recbuf, 1, argpp);
}

/*
 * PUBLIC: int __txn_recycle_log __P((DB_ENV *, DB_TXN *, DB_LSN *,
 * PUBLIC:     u_int32_t, u_int32_t, u_int32_t));
 */
int
__txn_recycle_log(dbenv, txnid, ret_lsnp, flags,
    min, max)
	DB_ENV *dbenv;
	DB_TXN *txnid;
	DB_LSN *ret_lsnp;
	u_int32_t flags;
	u_int32_t min;
	u_int32_t max;
{
	DBT logrec;
	DB_TXNLOGREC *lr;
	DB_LSN *lsnp, null_lsn;
	u_int32_t uinttmp, rectype, txn_num;
	u_int npad;
	u_int8_t *bp;
	DB *dbp = NULL;
	int is_durable, ret;
	int used_malloc = 0;

#ifdef __txn_DEBUG
	fprintf(stderr,"__txn_recycle_log: begin\n");
#endif

	rectype = DB___txn_recycle;
	npad = 0;

	is_durable = 1;
	if (LF_ISSET(DB_LOG_NOT_DURABLE) ||
	    F_ISSET(dbenv, DB_ENV_TXN_NOT_DURABLE)) {
		if (txnid == NULL)
			return (0);
		is_durable = 0;
	}
	if (txnid == NULL) {
		txn_num = 0;
		null_lsn.file = 0;
		null_lsn.offset = 0;
		lsnp = &null_lsn;
	}
	else
	{
		if (TAILQ_FIRST(&txnid->kids) != NULL &&
		    (ret = __txn_activekids(dbenv, rectype, txnid)) != 0)
			return (ret);
		txn_num = txnid->txnid;
		lsnp = &txnid->last_lsn;
	}

	logrec.size = sizeof(rectype) + sizeof(txn_num) + sizeof(DB_LSN)
	    + sizeof(u_int32_t)
	    + sizeof(u_int32_t);
	if (CRYPTO_ON(dbenv)) {
		npad =
		    ((DB_CIPHER *)dbenv->crypto_handle)->adj_size(logrec.size);
		logrec.size += npad;
	}

	if (!is_durable && txnid != NULL)
	{
		if ((ret = __os_malloc(dbenv,
		    logrec.size + sizeof(DB_TXNLOGREC), &lr)) != 0)
			return (ret);
#ifdef DIAGNOSTIC
		goto do_malloc;
#else
		logrec.data = &lr->data;
#endif

	}
	else
	{
#ifdef DIAGNOSTIC
do_malloc:
#endif

		if (logrec.size > 4096)
		{
			if ((ret =
			    __os_malloc(dbenv, logrec.size, &logrec.data)) != 0) {
#ifdef DIAGNOSTIC
				if (!is_durable && txnid != NULL)
					(void)__os_free(dbenv, lr);
#endif

				return (ret);
			}
			used_malloc = 1;
		}
		else
		{
			used_malloc = 0;
			logrec.data = alloca(logrec.size);
		}
	}
	if (npad > 0)
		memset((u_int8_t *)logrec.data + logrec.size - npad, 0, npad);

	bp = logrec.data;

	LOGCOPY_32(bp, &rectype);
	bp += sizeof(rectype);

	LOGCOPY_32(bp, &txn_num);
	bp += sizeof(txn_num);

	LOGCOPY_FROMLSN(bp, lsnp);
	bp += sizeof(DB_LSN);

	uinttmp = (u_int32_t)min;
	LOGCOPY_32(bp, &uinttmp);
	bp += sizeof(uinttmp);

	uinttmp = (u_int32_t)max;
	LOGCOPY_32(bp, &uinttmp);
	bp += sizeof(uinttmp);

	DB_ASSERT((u_int32_t)(bp - (u_int8_t *)logrec.data) <= logrec.size);

#ifdef DIAGNOSTIC
	if (!is_durable && txnid != NULL) {
		 /*
		 * We set the debug bit if we are going
		 * to log non-durable transactions so
		 * they will be ignored by recovery.
		 */
		memcpy(lr->data, logrec.data, logrec.size);
		rectype |= DB_debug_FLAG;
		LOGCOPY_32(logrec.data, &rectype);
	}
#endif

	if (!is_durable && txnid != NULL) {
		ret = 0;
		STAILQ_INSERT_HEAD(&txnid->logs, lr, links);
#ifdef DIAGNOSTIC
		goto do_put;
#endif

	}
	else
	{
#ifdef DIAGNOSTIC
do_put:
#endif

		ret = __log_put(dbenv,
		    ret_lsnp, (DBT *)&logrec, flags | DB_LOG_NOCOPY);
		if (ret == 0 && txnid != NULL)
			txnid->last_lsn = *ret_lsnp;
	}

	if (!is_durable)
		LSN_NOT_LOGGED(*ret_lsnp);
#ifdef LOG_DIAGNOSTIC
	if (ret != 0)
		(void)__txn_recycle_print(dbenv,
		    (DBT *)&logrec, ret_lsnp, (db_recops)0 , NULL);
#endif

#ifndef DIAGNOSTIC
	if (is_durable || txnid == NULL)
#endif

		if (used_malloc)
			__os_free(dbenv, logrec.data);

	return (ret);
}

#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_recycle_getpgnos __P((DB_ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_recycle_getpgnos(dbenv, rec, lsnp, notused1, summary)
	DB_ENV *dbenv;
	DBT *rec;
	DB_LSN *lsnp;
	db_recops notused1;
	void *summary;
{
	TXN_RECS *t;
	int ret;
	COMPQUIET(rec, NULL);
	COMPQUIET(notused1, DB_TXN_ABORT);

	t = (TXN_RECS *)summary;

	if ((ret = __rep_check_alloc(dbenv, t, 1)) != 0)
		return (ret);

	t->array[t->npages].flags = LSN_PAGE_NOLOCK;
	t->array[t->npages].lsn = *lsnp;
	t->array[t->npages].fid = DB_LOGFILEID_INVALID;
	memset(&t->array[t->npages].pgdesc, 0,
	    sizeof(t->array[t->npages].pgdesc));

	t->npages++;

	return (0);
}
#endif /* HAVE_REPLICATION */

#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_recycle_getallpgnos __P((DB_ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__txn_recycle_getallpgnos(dbenv, rec, lsnp, notused1, summary)
	DB_ENV *dbenv;
	DBT *rec;
	DB_LSN *lsnp;
	db_recops notused1;
	void *summary;
{
    DB *dbp;
	TXN_RECS *t;
	__txn_recycle_args *argp;
	int ret = 0;

	COMPQUIET(notused1, DB_TXN_ABORT);

	argp = NULL;
	t = (TXN_RECS *)summary;


err:	if (argp != NULL)
	__os_free(dbenv, argp);

	return (ret);
}
#endif /* HAVE_REPLICATION */


/*
 * PUBLIC: int __txn_recycle_read_int __P((DB_ENV *, void *,
 * PUBLIC:     int do_pgswp,  __txn_recycle_args **));
 */
int
__txn_recycle_read_int(dbenv, recbuf, do_pgswp, argpp)
	DB_ENV *dbenv;
	void *recbuf;
	int do_pgswp;
	__txn_recycle_args **argpp;
{
	__txn_recycle_args *argp;
	u_int32_t uinttmp;
	u_int8_t *bp;
	DB *dbp = NULL;
	int ret;

#ifdef __txn_DEBUG
	fprintf(stderr,"__txn_recycle_read_int: begin\n");
#endif

	if ((ret = __os_malloc(dbenv,
	    sizeof(__txn_recycle_args) + sizeof(DB_TXN), &argp)) != 0)
		return (ret);
	argp->txnid = (DB_TXN *)&argp[1];

	bp = recbuf;
	LOGCOPY_32(&argp->type, bp);
	bp += sizeof(argp->type);

	LOGCOPY_32(&argp->txnid->txnid,  bp);
	bp += sizeof(argp->txnid->txnid);

	LOGCOPY_TOLSN(&argp->prev_lsn, bp);
	bp += sizeof(DB_LSN);

	LOGCOPY_32(&uinttmp, bp);
	argp->min = (u_int32_t)uinttmp;
	bp += sizeof(uinttmp);

	LOGCOPY_32(&uinttmp, bp);
	argp->max = (u_int32_t)uinttmp;
	bp += sizeof(uinttmp);

	*argpp = argp;
	return (0);
}

/*
 * PUBLIC: int __txn_recycle_print __P((DB_ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_recycle_print(dbenv, dbtp, lsnp, notused2, notused3)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *notused3;
{
	__txn_recycle_args *argp;
	int ret;

	notused2 = DB_TXN_ABORT;
	notused3 = NULL;

	if ((ret = __txn_recycle_read_int(dbenv, dbtp->data, 0, &argp)) != 0)
		return (ret);
	(void)printf(
	    "[%lu][%lu]__txn_recycle%s: rec: %lu txnid %lx prevlsn [%lu][%lu]\n",
	    (u_long)lsnp->file,
	    (u_long)lsnp->offset,
	    (argp->type & DB_debug_FLAG) ? "_debug" : "",
	    (u_long)argp->type,
	    (u_long)argp->txnid->txnid,
	    (u_long)argp->prev_lsn.file,
	    (u_long)argp->prev_lsn.offset);
	(void)printf("\tmin: %u\n", argp->min);
	fflush(stdout);
	(void)printf("\tmax: %u\n", argp->max);
	fflush(stdout);
	(void)printf("\n");
	__os_free(dbenv, argp);

	return (0);
}

/*
 * PUBLIC: int __txn_recycle_read __P((DB_ENV *, void *,
 * PUBLIC:      __txn_recycle_args **));
 */
int
__txn_recycle_read(dbenv, recbuf, argpp)
	DB_ENV *dbenv;
	void *recbuf;
	__txn_recycle_args **argpp;
{
	return __txn_recycle_read_int (dbenv, recbuf, 1, argpp);
}

//#define PRINTLOG_SANITY 0

/*
 * PUBLIC: int __txn_regop_rowlocks_log __P((DB_ENV *, DB_TXN *,
 * PUBLIC:     DB_LSN *, u_int32_t, u_int32_t, u_int64_t, DB_LSN *, DB_LSN *,
 * PUBLIC:     u_int64_t, u_int64_t, u_int32_t, u_int32_t, const DBT *,
 * PUBLIC:     const DBT *));
 */
int
__txn_regop_rowlocks_log(dbenv, txnid, ret_lsnp, ret_contextp, flags,
    opcode, ltranid, begin_lsn, last_commit_lsn, timestamp,
    lflags, generation, locks, rowlocks, usr_ptr)
	DB_ENV *dbenv;
	DB_TXN *txnid;
	DB_LSN *ret_lsnp;
	u_int64_t *ret_contextp;
	u_int32_t flags;
	u_int32_t opcode;
	u_int64_t ltranid;
	DB_LSN * begin_lsn;
	DB_LSN * last_commit_lsn;
	u_int64_t timestamp;
	u_int32_t lflags;
	u_int32_t generation;
	const DBT *locks;
	const DBT *rowlocks;
    void *usr_ptr;
{
	DBT logrec;
	DB_TXNLOGREC *lr;
	DB_LSN *lsnp, null_lsn;
	u_int32_t zero, uinttmp, rectype, txn_num;
	u_int64_t uint64tmp;
	u_int npad;
	u_int8_t *bp;
	DB *dbp = NULL;
	int is_durable, ret;
	int used_malloc = 0;
    int off_context = -1;

#ifdef __txn_DEBUG
	fprintf(stderr,"__txn_regop_rowlocks_log: begin\n");
#endif

	rectype = DB___txn_regop_rowlocks;
	npad = 0;

	is_durable = 1;
	if (LF_ISSET(DB_LOG_NOT_DURABLE) ||
	    F_ISSET(dbenv, DB_ENV_TXN_NOT_DURABLE)) {
		if (txnid == NULL)
			return (0);
		is_durable = 0;
	}
	if (txnid == NULL) {
		txn_num = 0;
		null_lsn.file = 0;
		null_lsn.offset = 0;
		lsnp = &null_lsn;
	}
	else
	{
		if (TAILQ_FIRST(&txnid->kids) != NULL &&
		    (ret = __txn_activekids(dbenv, rectype, txnid)) != 0)
			return (ret);
		txn_num = txnid->txnid;
		lsnp = &txnid->last_lsn;
	}

	logrec.size = sizeof(rectype) + sizeof(txn_num) + sizeof(DB_LSN)
	    + sizeof(u_int32_t)
	    + sizeof(u_int64_t)
	    + sizeof(*begin_lsn)
	    + sizeof(*last_commit_lsn)
	    + sizeof(u_int64_t)
	    + sizeof(u_int64_t)
	    + sizeof(u_int32_t)
	    + sizeof(u_int32_t)
	    + sizeof(u_int32_t) + (locks == NULL ? 0 : locks->size)
	    + sizeof(u_int32_t) + (rowlocks == NULL ? 0 : rowlocks->size);
	if (CRYPTO_ON(dbenv)) {
		npad =
		    ((DB_CIPHER *)dbenv->crypto_handle)->adj_size(logrec.size);
		logrec.size += npad;
	}

	if (!is_durable && txnid != NULL)
	{
		if ((ret = __os_malloc(dbenv,
		    logrec.size + sizeof(DB_TXNLOGREC), &lr)) != 0)
			return (ret);
#ifdef DIAGNOSTIC
		goto do_malloc;
#else
		logrec.data = &lr->data;
#endif

	}
	else
	{
#ifdef DIAGNOSTIC
do_malloc:
#endif

		if (logrec.size > 4096)
		{
			if ((ret =
			    __os_malloc(dbenv, logrec.size, &logrec.data)) != 0) {
#ifdef DIAGNOSTIC
				if (!is_durable && txnid != NULL)
					(void)__os_free(dbenv, lr);
#endif

				return (ret);
			}
			used_malloc = 1;
		}
		else
		{
			used_malloc = 0;
			logrec.data = alloca(logrec.size);
		}
	}
	if (npad > 0)
		memset((u_int8_t *)logrec.data + logrec.size - npad, 0, npad);

	bp = logrec.data;

	LOGCOPY_32(bp, &rectype);
	bp += sizeof(rectype);

	LOGCOPY_32(bp, &txn_num);
	bp += sizeof(txn_num);

	LOGCOPY_FROMLSN(bp, lsnp);
	bp += sizeof(DB_LSN);

	uinttmp = (u_int32_t)opcode;
	LOGCOPY_32(bp, &uinttmp);
	bp += sizeof(uinttmp);

	uint64tmp = (u_int64_t)ltranid;
	memcpy(bp, &uint64tmp, sizeof(u_int64_t));
	bp += sizeof(uint64tmp);

	if (begin_lsn != NULL)
		LOGCOPY_FROMLSN(bp, begin_lsn);
	else
		memset(bp, 0, sizeof(*begin_lsn));
	bp += sizeof(*begin_lsn);

	if (last_commit_lsn != NULL)
		LOGCOPY_FROMLSN(bp, last_commit_lsn);
	else
		memset(bp, 0, sizeof(*last_commit_lsn));
	bp += sizeof(*last_commit_lsn);

    off_context = (u_int8_t*)bp-((u_int8_t*)(logrec.data));

    memset(bp, 0, sizeof(u_int64_t));
    bp += sizeof(u_int64_t);

	uint64tmp = (u_int64_t)timestamp;
	LOGCOPY_64(bp, &uint64tmp);
	bp += sizeof(uint64tmp);

	uinttmp = (u_int32_t)lflags;
	LOGCOPY_32(bp, &uinttmp);
	bp += sizeof(uinttmp);

	if (lflags & DB_TXN_LOGICAL_GEN) {
		uinttmp = (u_int32_t)generation;
		LOGCOPY_32(bp, &uinttmp);
		bp += sizeof(uinttmp);
	}

#ifdef PRINTLOG_SANITY
    fprintf(stderr, "%s txnid %x begin_lsn [%d][%d] writing generation %u\n", 
            __func__, txnid, begin_lsn->file, begin_lsn->offset, generation);
#endif

	if (locks == NULL)
	{
#ifdef PRINTLOG_SANITY
        fprintf(stderr, "%s txnid %x begin_lsn [%d][%d] locks size is 0\n", 
                __func__, txnid, begin_lsn->file, begin_lsn->offset);
#endif
		zero = 0;
		LOGCOPY_32(bp, &zero);
		bp += sizeof(u_int32_t);
	}
	else
	{
#ifdef PRINTLOG_SANITY
        fprintf(stderr, "%s txnid %x begin_lsn [%d][%d] locks size is %u\n", 
                __func__, txnid, begin_lsn->file, begin_lsn->offset, locks->size);
#endif
		LOGCOPY_32(bp, &locks->size);
		bp += sizeof(locks->size);
		memcpy(bp, locks->data, locks->size);
		bp += locks->size;
	}

	if (rowlocks == NULL)
	{
#ifdef PRINTLOG_SANITY
        fprintf(stderr, "%s txnid %x begin_lsn [%d][%d] rowlocks size is %u\n", 
                __func__, txnid, begin_lsn->file, begin_lsn->offset, locks->size);
#endif
		zero = 0;
		LOGCOPY_32(bp, &zero);
		bp += sizeof(u_int32_t);
	}
	else
	{
#ifdef PRINTLOG_SANITY
        fprintf(stderr, "%s txnid %x begin_lsn [%d][%d] rowlocks size is %u\n", 
                __func__, txnid, begin_lsn->file, begin_lsn->offset, rowlocks->size);
#endif
		LOGCOPY_32(bp, &rowlocks->size);
		bp += sizeof(rowlocks->size);
		memcpy(bp, rowlocks->data, rowlocks->size);
		bp += rowlocks->size;
	}

	DB_ASSERT((u_int32_t)(bp - (u_int8_t *)logrec.data) <= logrec.size);

#ifdef DIAGNOSTIC
	if (!is_durable && txnid != NULL) {
		 /*
		 * We set the debug bit if we are going
		 * to log non-durable transactions so
		 * they will be ignored by recovery.
		 */
		memcpy(lr->data, logrec.data, logrec.size);
		rectype |= DB_debug_FLAG;
		LOGCOPY_32(logrec.data, &rectype);
	}
#endif

    if (lflags & DB_TXN_LOGICAL_COMMIT)
        flags |= DB_LOG_LOGICAL_COMMIT;

	if (!is_durable && txnid != NULL) {
		ret = 0;
		STAILQ_INSERT_HEAD(&txnid->logs, lr, links);
#ifdef DIAGNOSTIC
		goto do_put;
#endif

	}
	else
	{
#ifdef DIAGNOSTIC
do_put:
#endif

		ret = __log_put_commit_context(dbenv,
		    ret_lsnp, ret_contextp, (DBT *)&logrec, flags | DB_LOG_NOCOPY, off_context, usr_ptr);
		if (ret == 0 && txnid != NULL)
			txnid->last_lsn = *ret_lsnp;
	}

	if (!is_durable)
		LSN_NOT_LOGGED(*ret_lsnp);
#ifdef LOG_DIAGNOSTIC
	if (ret != 0)
		(void)__txn_regop_rowlocks_print(dbenv,
		    (DBT *)&logrec, ret_lsnp, (db_recops)0 , NULL);
#endif

#ifndef DIAGNOSTIC
	if (is_durable || txnid == NULL)
#endif

		if (used_malloc)
			__os_free(dbenv, logrec.data);

	return (ret);
}


#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_regop_rowlocks_getpgnos __P((DB_ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__txn_regop_rowlocks_getpgnos(dbenv, rec, lsnp, notused1, summary)
	DB_ENV *dbenv;
	DBT *rec;
	DB_LSN *lsnp;
	db_recops notused1;
	void *summary;
{
	TXN_RECS *t;
	int ret;
	COMPQUIET(rec, NULL);
	COMPQUIET(notused1, DB_TXN_ABORT);

	t = (TXN_RECS *)summary;

	if ((ret = __rep_check_alloc(dbenv, t, 1)) != 0)
		return (ret);

	t->array[t->npages].flags = LSN_PAGE_NOLOCK;
	t->array[t->npages].lsn = *lsnp;
	t->array[t->npages].fid = DB_LOGFILEID_INVALID;
	memset(&t->array[t->npages].pgdesc, 0,
	    sizeof(t->array[t->npages].pgdesc));

	t->npages++;

	return (0);
}
#endif /* HAVE_REPLICATION */

#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_regop_rowlocks_getallpgnos __P((DB_ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__txn_regop_rowlocks_getallpgnos(dbenv, rec, lsnp, notused1, summary)
	DB_ENV *dbenv;
	DBT *rec;
	DB_LSN *lsnp;
	db_recops notused1;
	void *summary;
{
	TXN_RECS *t;
	__txn_regop_rowlocks_args *argp;
	int ret = 0;

	COMPQUIET(notused1, DB_TXN_ABORT);

	argp = NULL;
	t = (TXN_RECS *)summary;


err:	if (argp != NULL)
	__os_free(dbenv, argp);

	return (ret);
}
#endif /* HAVE_REPLICATION */

/*
 * PUBLIC: int __txn_regop_rowlocks_read_int __P((DB_ENV *, void *,
 * PUBLIC:     int do_pgswp,  __txn_regop_rowlocks_args **));
 */
int
__txn_regop_rowlocks_read_int(dbenv, recbuf, do_pgswp, argpp)
	DB_ENV *dbenv;
	void *recbuf;
	int do_pgswp;
	__txn_regop_rowlocks_args **argpp;
{
	__txn_regop_rowlocks_args *argp;
	u_int32_t uinttmp;
	u_int64_t uint64tmp;
	u_int8_t *bp;
	DB *dbp = NULL;
	int ret;

#ifdef __txn_DEBUG
	fprintf(stderr,"__txn_regop_rowlocks_read_int: begin\n");
#endif

	if ((ret = __os_malloc(dbenv,
	    sizeof(__txn_regop_rowlocks_args) + sizeof(DB_TXN), &argp)) != 0)
		return (ret);
	argp->txnid = (DB_TXN *)&argp[1];

	bp = recbuf;
	LOGCOPY_32(&argp->type, bp);
	bp += sizeof(argp->type);

	LOGCOPY_32(&argp->txnid->txnid,  bp);
	bp += sizeof(argp->txnid->txnid);

	LOGCOPY_TOLSN(&argp->prev_lsn, bp);
	bp += sizeof(DB_LSN);

	LOGCOPY_32(&uinttmp, bp);
	argp->opcode = (u_int32_t)uinttmp;
	bp += sizeof(uinttmp);

	memcpy(&uint64tmp, bp, sizeof(u_int64_t));
	argp->ltranid = uint64tmp;
	bp += sizeof(uint64tmp);

	LOGCOPY_TOLSN(&argp->begin_lsn, bp);
	bp += sizeof(argp->begin_lsn);

	LOGCOPY_TOLSN(&argp->last_commit_lsn, bp);
	bp += sizeof(argp->last_commit_lsn);

	memcpy(&uint64tmp, bp, sizeof(u_int64_t));
	argp->context = uint64tmp;
	bp += sizeof(uint64tmp);

	LOGCOPY_64(&uint64tmp, bp);
	argp->timestamp = (u_int64_t)uint64tmp;
	bp += sizeof(uint64tmp);

	LOGCOPY_32(&uinttmp, bp);
	argp->lflags = (u_int32_t)uinttmp;
	bp += sizeof(uinttmp);

	if (argp->lflags & DB_TXN_LOGICAL_GEN) {
		LOGCOPY_32(&uinttmp, bp);
		argp->generation = (u_int32_t)uinttmp;
		bp += sizeof(uinttmp);
	} else {
		argp->generation = 0;
	}

	memset(&argp->locks, 0, sizeof(argp->locks));
	LOGCOPY_32(&argp->locks.size, bp);
	bp += sizeof(u_int32_t);
	argp->locks.data = bp;
	bp += argp->locks.size;

	memset(&argp->rowlocks, 0, sizeof(argp->rowlocks));
	LOGCOPY_32(&argp->rowlocks.size, bp);
	bp += sizeof(u_int32_t);
	argp->rowlocks.data = bp;
	bp += argp->rowlocks.size;

	*argpp = argp;
	return (0);
}

/*
 * PUBLIC: int __txn_regop_rowlocks_print __P((DB_ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__txn_regop_rowlocks_print(dbenv, dbtp, lsnp, notused2, notused3)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *notused3;
{
	__txn_regop_rowlocks_args *argp;
	struct tm *lt;
	u_int32_t i;
	int ch;
	int ret;

	notused2 = DB_TXN_ABORT;
	notused3 = NULL;

	if ((ret = __txn_regop_rowlocks_read_int(dbenv, dbtp->data, 0, &argp)) != 0)
		return (ret);
	(void)printf(
	    "[%lu][%lu]__txn_regop_rowlocks%s: rec: %lu txnid %lx prevlsn [%lu][%lu]\n",
	    (u_long)lsnp->file,
	    (u_long)lsnp->offset,
	    (argp->type & DB_debug_FLAG) ? "_debug" : "",
	    (u_long)argp->type,
	    (u_long)argp->txnid->txnid,
	    (u_long)argp->prev_lsn.file,
	    (u_long)argp->prev_lsn.offset);
	(void)printf("\topcode: %lu\n", (u_long)argp->opcode);
	fflush(stdout);
	(void)printf("\tltranid: %"PRIx64"\n", argp->ltranid);
	fflush(stdout);
	(void)printf("\tbegin_lsn: [%lu][%lu]\n",
	    (u_long)argp->begin_lsn.file, (u_long)argp->begin_lsn.offset);
	fflush(stdout);
	(void)printf("\tlast_commit_lsn: [%lu][%lu]\n",
	    (u_long)argp->last_commit_lsn.file, (u_long)argp->last_commit_lsn.offset);
	fflush(stdout);
	(void)printf("\tcontext: %lx\n", argp->context);
	fflush(stdout);
    time_t timestamp = argp->timestamp;
	lt = localtime((time_t *)&timestamp);
    if (lt)
    {
        (void)printf(
                "\ttimestamp: %ld (%.24s, 20%02lu%02lu%02lu%02lu%02lu.%02lu)\n",
                (long)argp->timestamp, ctime((time_t *)&argp->timestamp),
                (u_long)lt->tm_year - 100, (u_long)lt->tm_mon+1,
                (u_long)lt->tm_mday, (u_long)lt->tm_hour,
                (u_long)lt->tm_min, (u_long)lt->tm_sec);
    }
    else
    {
        (void)printf("\ttimestamp: %ld\n", (long)argp->timestamp);
    }
	fflush(stdout);
	(void)printf("\tgeneration: %u\n", argp->generation);
	(void)printf("\tlflags: 0x%08x ", argp->lflags);
	if (argp->lflags & DB_TXN_LOGICAL_BEGIN)
		printf("DB_TXN_LOGICAL_BEGIN ");
	if (argp->lflags & DB_TXN_LOGICAL_COMMIT)
		printf("DB_TXN_LOGICAL_COMMIT ");
	if (argp->lflags & DB_TXN_SCHEMA_LOCK)
		printf("DB_TXN_SCHEMA_LOCK ");
	if (argp->lflags & DB_TXN_LOGICAL_GEN)
		printf("DB_TXN_LOGICAL_GEN ");
	if (argp->lflags & DB_TXN_DONT_GET_REPO_MTX)
		printf("DB_TXN_DONT_GET_REPO_MTX ");
	printf("\n");

	fflush(stdout);
	(void)printf("\tlocks: \n");
    DB_LSN ignored;
    int pglogs;
    u_int32_t keycnt;
    __lock_get_list(dbenv, 0, LOCK_GET_LIST_PRINTLOCK, DB_LOCK_WRITE, &argp->locks, &ignored, (void **)&pglogs, &keycnt, stdout);
	fflush(stdout);
	(void)printf("\trowlocks: \n");
	fsnapf(stdout, argp->rowlocks.data, argp->rowlocks.size);
	fflush(stdout);
	(void)printf("\n");
	__os_free(dbenv, argp);

	return (0);
}

/*
 * PUBLIC: int __txn_regop_rowlocks_read __P((DB_ENV *, void *,
 * PUBLIC:      __txn_regop_rowlocks_args **));
 */
int
__txn_regop_rowlocks_read(dbenv, recbuf, argpp)
	DB_ENV *dbenv;
	void *recbuf;
	__txn_regop_rowlocks_args **argpp;
{
	return __txn_regop_rowlocks_read_int (dbenv, recbuf, 1, argpp);
}

/*
 * PUBLIC: int __txn_regop_gen_log __P((DB_ENV *, DB_TXN *, DB_LSN *,
 * PUBLIC:     u_int32_t, u_int32_t, u_int32_t, u_int64_t, u_int64_t,
 * PUBLIC:     const DBT *));
 */
int
__txn_regop_gen_log(dbenv, txnid, ret_lsnp, ret_contextp, flags,
    opcode, generation, timestamp, locks, usr_ptr)
	DB_ENV *dbenv;
	DB_TXN *txnid;
	DB_LSN *ret_lsnp;
	u_int64_t *ret_contextp;
	u_int32_t flags;
	u_int32_t opcode;
	u_int32_t generation;
	u_int64_t timestamp;
	const DBT *locks;
    void *usr_ptr;
{
	DBT logrec;
	DB_TXNLOGREC *lr;
	DB_LSN *lsnp, null_lsn;
	u_int32_t zero, uinttmp, rectype, txn_num;
	u_int64_t uint64tmp;
	u_int npad;
	u_int8_t *bp;
	DB *dbp = NULL;
	int is_durable, ret;
	int used_malloc = 0;
	int off_context = -1;

#ifdef __txn_DEBUG
	fprintf(stderr,"__txn_regop_gen_log: begin\n");
#endif

	rectype = DB___txn_regop_gen;
	npad = 0;

	is_durable = 1;
	if (LF_ISSET(DB_LOG_NOT_DURABLE) ||
	    F_ISSET(dbenv, DB_ENV_TXN_NOT_DURABLE)) {
		if (txnid == NULL)
			return (0);
		is_durable = 0;
	}
	if (txnid == NULL) {
		txn_num = 0;
		null_lsn.file = 0;
		null_lsn.offset = 0;
		lsnp = &null_lsn;
	}
	else
	{
		if (TAILQ_FIRST(&txnid->kids) != NULL &&
		    (ret = __txn_activekids(dbenv, rectype, txnid)) != 0)
			return (ret);
		txn_num = txnid->txnid;
		lsnp = &txnid->last_lsn;
	}

	logrec.size = sizeof(rectype) + sizeof(txn_num) + sizeof(DB_LSN)
	    + sizeof(u_int32_t)
	    + sizeof(u_int32_t)
	    + sizeof(u_int64_t)
	    + sizeof(u_int64_t)
	    + sizeof(u_int32_t) + (locks == NULL ? 0 : locks->size);
	if (CRYPTO_ON(dbenv)) {
		npad =
		    ((DB_CIPHER *)dbenv->crypto_handle)->adj_size(logrec.size);
		logrec.size += npad;
	}

	if (!is_durable && txnid != NULL)
	{
		if ((ret = __os_malloc(dbenv,
		    logrec.size + sizeof(DB_TXNLOGREC), &lr)) != 0)
			return (ret);
#ifdef DIAGNOSTIC
		goto do_malloc;
#else
		logrec.data = &lr->data;
#endif

	}
	else
	{
#ifdef DIAGNOSTIC
do_malloc:
#endif

		if (logrec.size > 4096)
		{
			if ((ret =
			    __os_malloc(dbenv, logrec.size, &logrec.data)) != 0) {
#ifdef DIAGNOSTIC
				if (!is_durable && txnid != NULL)
					(void)__os_free(dbenv, lr);
#endif

				return (ret);
			}
			used_malloc = 1;
		}
		else
		{
			used_malloc = 0;
			logrec.data = alloca(logrec.size);
		}
	}
	if (npad > 0)
		memset((u_int8_t *)logrec.data + logrec.size - npad, 0, npad);

	bp = logrec.data;

	LOGCOPY_32(bp, &rectype);
	bp += sizeof(rectype);

	LOGCOPY_32(bp, &txn_num);
	bp += sizeof(txn_num);

	LOGCOPY_FROMLSN(bp, lsnp);
	bp += sizeof(DB_LSN);

	uinttmp = (u_int32_t)opcode;
	LOGCOPY_32(bp, &uinttmp);
	bp += sizeof(uinttmp);

	uinttmp = (u_int32_t)generation;
	LOGCOPY_32(bp, &uinttmp);
	bp += sizeof(uinttmp);

    off_context = (u_int8_t*)bp-((u_int8_t*)(logrec.data));

    memset(bp, 0, sizeof(u_int64_t));
    bp += sizeof(u_int64_t);

	uint64tmp = (u_int64_t)timestamp;
	LOGCOPY_64(bp, &uint64tmp);
	bp += sizeof(uint64tmp);

	if (locks == NULL)
	{
		zero = 0;
		LOGCOPY_32(bp, &zero);
		bp += sizeof(u_int32_t);
	}
	else
	{
		LOGCOPY_32(bp, &locks->size);
		bp += sizeof(locks->size);
		memcpy(bp, locks->data, locks->size);
		bp += locks->size;
	}

	DB_ASSERT((u_int32_t)(bp - (u_int8_t *)logrec.data) <= logrec.size);

#ifdef DIAGNOSTIC
	if (!is_durable && txnid != NULL) {
		 /*
		 * We set the debug bit if we are going
		 * to log non-durable transactions so
		 * they will be ignored by recovery.
		 */
		memcpy(lr->data, logrec.data, logrec.size);
		rectype |= DB_debug_FLAG;
		LOGCOPY_32(logrec.data, &rectype);
	}
#endif

	if (!is_durable && txnid != NULL) {
		ret = 0;
		STAILQ_INSERT_HEAD(&txnid->logs, lr, links);
#ifdef DIAGNOSTIC
		goto do_put;
#endif

	}
	else
	{
#ifdef DIAGNOSTIC
do_put:
#endif

		ret = __log_put_commit_context(dbenv,
		    ret_lsnp, ret_contextp, (DBT *)&logrec, flags | DB_LOG_NOCOPY, off_context, usr_ptr);
		if (ret == 0 && txnid != NULL)
			txnid->last_lsn = *ret_lsnp;
	}

	if (!is_durable)
		LSN_NOT_LOGGED(*ret_lsnp);
#ifdef LOG_DIAGNOSTIC
	if (ret != 0)
		(void)__txn_regop_gen_print(dbenv,
		    (DBT *)&logrec, ret_lsnp, (db_recops)0 , NULL);
#endif

#ifndef DIAGNOSTIC
	if (is_durable || txnid == NULL)
#endif

		if (used_malloc)
			__os_free(dbenv, logrec.data);

	return (ret);
}

#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_regop_gen_getpgnos __P((DB_ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__txn_regop_gen_getpgnos(dbenv, rec, lsnp, notused1, summary)
	DB_ENV *dbenv;
	DBT *rec;
	DB_LSN *lsnp;
	db_recops notused1;
	void *summary;
{
	TXN_RECS *t;
	int ret;
	COMPQUIET(rec, NULL);
	COMPQUIET(notused1, DB_TXN_ABORT);

	t = (TXN_RECS *)summary;

	if ((ret = __rep_check_alloc(dbenv, t, 1)) != 0)
		return (ret);

	t->array[t->npages].flags = LSN_PAGE_NOLOCK;
	t->array[t->npages].lsn = *lsnp;
	t->array[t->npages].fid = DB_LOGFILEID_INVALID;
	memset(&t->array[t->npages].pgdesc, 0,
	    sizeof(t->array[t->npages].pgdesc));

	t->npages++;

	return (0);
}
#endif /* HAVE_REPLICATION */

#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_regop_gen_getallpgnos __P((DB_ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__txn_regop_gen_getallpgnos(dbenv, rec, lsnp, notused1, summary)
	DB_ENV *dbenv;
	DBT *rec;
	DB_LSN *lsnp;
	db_recops notused1;
	void *summary;
{
	TXN_RECS *t;
	__txn_regop_gen_args *argp;
	int ret = 0;

	COMPQUIET(notused1, DB_TXN_ABORT);

	argp = NULL;
	t = (TXN_RECS *)summary;


err:	if (argp != NULL)
	__os_free(dbenv, argp);

	return (ret);
}
#endif /* HAVE_REPLICATION */

/*
 * PUBLIC: int __txn_regop_gen_read_int __P((DB_ENV *, void *,
 * PUBLIC:     int do_pgswp,  __txn_regop_gen_args **));
 */
int
__txn_regop_gen_read_int(dbenv, recbuf, do_pgswp, argpp)
	DB_ENV *dbenv;
	void *recbuf;
	int do_pgswp;
	__txn_regop_gen_args **argpp;
{
	__txn_regop_gen_args *argp;
	u_int32_t uinttmp;
	u_int64_t uint64tmp;
	u_int8_t *bp;
	DB *dbp = NULL;
	int ret;

#ifdef __txn_DEBUG
	fprintf(stderr,"__txn_regop_gen_read_int: begin\n");
#endif

	if ((ret = __os_malloc(dbenv,
	    sizeof(__txn_regop_gen_args) + sizeof(DB_TXN), &argp)) != 0)
		return (ret);
	argp->txnid = (DB_TXN *)&argp[1];

	bp = recbuf;
	LOGCOPY_32(&argp->type, bp);
	bp += sizeof(argp->type);

	LOGCOPY_32(&argp->txnid->txnid,  bp);
	bp += sizeof(argp->txnid->txnid);

	LOGCOPY_TOLSN(&argp->prev_lsn, bp);
	bp += sizeof(DB_LSN);

	LOGCOPY_32(&uinttmp, bp);
	argp->opcode = (u_int32_t)uinttmp;
	bp += sizeof(uinttmp);

	LOGCOPY_32(&uinttmp, bp);
	argp->generation = (u_int32_t)uinttmp;
	bp += sizeof(uinttmp);

	memcpy(&uint64tmp, bp, sizeof(u_int64_t));
	argp->context = uint64tmp;
	bp += sizeof(uint64tmp);

	LOGCOPY_64(&uint64tmp, bp);
	argp->timestamp = (u_int64_t)uint64tmp;
	bp += sizeof(uint64tmp);

	memset(&argp->locks, 0, sizeof(argp->locks));
	LOGCOPY_32(&argp->locks.size, bp);
	bp += sizeof(u_int32_t);
	argp->locks.data = bp;
	bp += argp->locks.size;

	*argpp = argp;
	return (0);
}

/*
 * PUBLIC: int __txn_regop_gen_print __P((DB_ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_regop_gen_print(dbenv, dbtp, lsnp, notused2, notused3)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *notused3;
{
	__txn_regop_gen_args *argp;
	struct tm *lt;
	u_int32_t i;
	int ch;
	int ret;

	notused2 = DB_TXN_ABORT;
	notused3 = NULL;

	if ((ret = __txn_regop_gen_read_int(dbenv, dbtp->data, 0, &argp)) != 0)
		return (ret);
	(void)printf(
	    "[%lu][%lu]__txn_regop_gen%s: rec: %lu txnid %lx prevlsn [%lu][%lu]\n",
	    (u_long)lsnp->file,
	    (u_long)lsnp->offset,
	    (argp->type & DB_debug_FLAG) ? "_debug" : "",
	    (u_long)argp->type,
	    (u_long)argp->txnid->txnid,
	    (u_long)argp->prev_lsn.file,
	    (u_long)argp->prev_lsn.offset);
	(void)printf("\topcode: %lu\n", (u_long)argp->opcode);
	fflush(stdout);
	(void)printf("\tgeneration: %u\n", argp->generation);
    unsigned long long flipcontext;
    int *fliporig = (int *)&argp->context;
    int *flipptr = (int *)&flipcontext;
    flipptr[0] = htonl(fliporig[1]);
    flipptr[1] = htonl(fliporig[0]);
    fflush(stdout);
    (void)printf("\tcontext: %016lx %016llx\n", argp->context, flipcontext);
    fflush(stdout);
    lt = localtime((time_t *)&argp->timestamp);
    if (lt)
    {
        (void)printf(
                "\ttimestamp: %ld (%.24s, 20%02lu%02lu%02lu%02lu%02lu.%02lu)\n",
                (long)argp->timestamp, ctime((time_t *)&argp->timestamp),
                (u_long)lt->tm_year - 100, (u_long)lt->tm_mon+1,
                (u_long)lt->tm_mday, (u_long)lt->tm_hour,
                (u_long)lt->tm_min, (u_long)lt->tm_sec);
    }
    else
    {
        (void)printf("\ttimestamp: %ld\n", (long)argp->timestamp);
    }
	fflush(stdout);
	(void)printf("\tlocks: \n");

    DB_LSN ignored;
    int pglogs;
    u_int32_t keycnt;
    __lock_get_list(dbenv, 0, LOCK_GET_LIST_PRINTLOCK, DB_LOCK_WRITE, &argp->locks, &ignored, (void **)&pglogs, &keycnt, stdout);

	//fsnapf(stdout, argp->locks.data, argp->locks.size);
	fflush(stdout);
	(void)printf("\n");
	__os_free(dbenv, argp);

	return (0);
}

/*
 * PUBLIC: int __txn_regop_gen_read __P((DB_ENV *, void *,
 * PUBLIC:      __txn_regop_gen_args **));
 */
int
__txn_regop_gen_read(dbenv, recbuf, argpp)
	DB_ENV *dbenv;
	void *recbuf;
	__txn_regop_gen_args **argpp;
{
	return __txn_regop_gen_read_int (dbenv, recbuf, 1, argpp);
}

/*
 * PUBLIC: int __txn_init_print __P((DB_ENV *, int (***)(DB_ENV *,
 * PUBLIC:     DBT *, DB_LSN *, db_recops, void *), size_t *));
 */
int
__txn_init_print(dbenv, dtabp, dtabsizep)
	DB_ENV *dbenv;
	int (***dtabp)__P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
	size_t *dtabsizep;
{
	int ret;

	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_regop_print, DB___txn_regop)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_ckp_print, DB___txn_ckp)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_child_print, DB___txn_child)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_xa_regop_print, DB___txn_xa_regop)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_recycle_print, DB___txn_recycle)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_regop_rowlocks_print, DB___txn_regop_rowlocks)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_regop_gen_print, DB___txn_regop_gen)) != 0)
		return (ret);
	return (0);
}

#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_init_getpgnos __P((DB_ENV *, int (***)(DB_ENV *,
 * PUBLIC:     DBT *, DB_LSN *, db_recops, void *), size_t *));
 */
int
__txn_init_getpgnos(dbenv, dtabp, dtabsizep)
	DB_ENV *dbenv;
	int (***dtabp)__P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
	size_t *dtabsizep;
{
	int ret;

	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_regop_getpgnos, DB___txn_regop)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_ckp_getpgnos, DB___txn_ckp)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_child_getpgnos, DB___txn_child)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_xa_regop_getpgnos, DB___txn_xa_regop)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_recycle_getpgnos, DB___txn_recycle)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_regop_rowlocks_getpgnos, DB___txn_regop_rowlocks)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_regop_gen_getpgnos, DB___txn_regop_gen)) != 0)
		return (ret);
	return (0);
}
#endif /* HAVE_REPLICATION */

#ifdef HAVE_REPLICATION
/*
 * PUBLIC: int __txn_init_getallpgnos __P((DB_ENV *,
 * PUBLIC:     int (***)(DB_ENV *, DBT *, DB_LSN *, db_recops, void *),
 * PUBLIC:     size_t *));
 */
int
__txn_init_getallpgnos(dbenv, dtabp, dtabsizep)
	DB_ENV *dbenv;
	int (***dtabp)__P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
	size_t *dtabsizep;
{
	int ret;

	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_regop_getallpgnos, DB___txn_regop)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_ckp_getallpgnos, DB___txn_ckp)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_child_getallpgnos, DB___txn_child)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_xa_regop_getallpgnos, DB___txn_xa_regop)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_recycle_getallpgnos, DB___txn_recycle)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_regop_rowlocks_getallpgnos, DB___txn_regop_rowlocks)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_regop_gen_getallpgnos, DB___txn_regop_gen)) != 0)
		return (ret);
	return (0);
}
#endif /* HAVE_REPLICATION */

/*
 * PUBLIC: int __txn_init_recover __P((DB_ENV *, int (***)(DB_ENV *,
 * PUBLIC:     DBT *, DB_LSN *, db_recops, void *), size_t *));
 */
int
__txn_init_recover(dbenv, dtabp, dtabsizep)
	DB_ENV *dbenv;
	int (***dtabp)__P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
	size_t *dtabsizep;
{
	int ret;

	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_regop_recover, DB___txn_regop)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_ckp_recover, DB___txn_ckp)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_child_recover, DB___txn_child)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_xa_regop_recover, DB___txn_xa_regop)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_recycle_recover, DB___txn_recycle)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_regop_rowlocks_recover, DB___txn_regop_rowlocks)) != 0)
		return (ret);
	if ((ret = __db_add_recovery(dbenv, dtabp, dtabsizep,
	    __txn_regop_gen_recover, DB___txn_regop_gen)) != 0)
		return (ret);
	return (0);
}
