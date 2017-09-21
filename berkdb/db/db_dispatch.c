/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
/*
 * Copyright (c) 1995, 1996
 *	The President and Fellows of Harvard University.  All rights reserved.
 *
 * This code is derived from software contributed to Berkeley by
 * Margo Seltzer.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: db_dispatch.c,v 11.145 2003/09/10 20:31:18 ubell Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/hash.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/fop.h"
#include "dbinc/txn.h"
#include "dbinc/db_swap.h"

#include <pthread.h>

#include <logmsg.h>

static int __db_limbo_fix __P((DB *, DB_TXN *,
	DB_TXN *, DB_TXNLIST *, db_pgno_t *, DBMETA *, db_limbo_state));
static int __db_limbo_bucket __P((DB_ENV *,
	DB_TXN *, DB_TXNLIST *, db_limbo_state));
static int __db_limbo_move __P((DB_ENV *, DB_TXN *, DB_TXN *, DB_TXNLIST *));
static int __db_limbo_prepare __P((DB *, DB_TXN *, DB_TXNLIST *));
static int __db_lock_move __P((DB_ENV *,
	u_int8_t *, db_pgno_t, db_lockmode_t, DB_TXN *, DB_TXN *));
static int __db_txnlist_find_internal __P((DB_ENV *, void *, db_txnlist_type,
	u_int32_t, u_int8_t[DB_FILE_ID_LEN], DB_TXNLIST **, int));
static int __db_txnlist_pgnoadd __P((DB_ENV *, DB_TXNHEAD *,
	int32_t, u_int8_t[DB_FILE_ID_LEN], char *, db_pgno_t));


/* TODO: dispatch table for these? */

#include "dbinc/btree.h"

#include "dbinc_auto/btree_auto.h"
#include "dbinc_auto/db_auto.h"
#include "dbinc_auto/btree_auto.h"
#include "dbinc_auto/crdel_auto.h"
#include "dbinc_auto/db_auto.h"
#include "dbinc_auto/dbreg_auto.h"
#include "dbinc_auto/fileops_auto.h"
#include "dbinc_auto/hash_auto.h"
#include "dbinc_auto/qam_auto.h"
#include "dbinc_auto/txn_auto.h"

static int log_event_counts[10000] = {0};

void
dump_log_event_counts(void)
{
	int events[] = {
		DB___bam_split, DB___bam_rsplit, DB___bam_adj, DB___bam_cadjust,
		    DB___bam_cdel,
		DB___bam_repl, DB___bam_root, DB___bam_curadj, DB___bam_rcuradj,
		    DB___crdel_metasub,
		DB___db_addrem, DB___db_big, DB___db_ovref, DB___db_relink,
		    DB___db_debug,
		DB___db_noop, DB___db_pg_alloc, DB___db_pg_free, DB___db_cksum,
		    DB___db_pg_freedata,
		DB___db_pg_prepare, DB___db_pg_new, DB___dbreg_register,
		    DB___fop_create,
		DB___fop_remove, DB___fop_write, DB___fop_rename,
		    DB___fop_file_remove, DB___ham_insdel,
		DB___ham_newpage, DB___ham_splitdata, DB___ham_replace,
		    DB___ham_copypage,
		DB___ham_metagroup, DB___ham_groupalloc, DB___ham_curadj,
		    DB___ham_chgpg, DB___qam_incfirst,
		DB___qam_mvptr, DB___qam_del, DB___qam_add, DB___qam_delext,
		    DB___txn_regop,
		DB___txn_regop_gen, DB___txn_regop_rowlocks, DB___txn_ckp,
		    DB___txn_child, DB___txn_xa_regop,
		DB___txn_recycle
	};
	char *event_names[] = {
		"DB___bam_split", "DB___bam_rsplit", "DB___bam_adj",
		    "DB___bam_cadjust", "DB___bam_cdel",
		"DB___bam_repl", "DB___bam_root", "DB___bam_curadj",
		    "DB___bam_rcuradj", "DB___crdel_metasub",
		"DB___db_addrem", "DB___db_big", "DB___db_ovref",
		    "DB___db_relink", "DB___db_debug",
		"DB___db_noop", "DB___db_pg_alloc", "DB___db_pg_free",
		    "DB___db_cksum", "DB___db_pg_freedata",
		"DB___db_pg_prepare", "DB___db_pg_new", "DB___dbreg_register",
		    "DB___fop_create",
		"DB___fop_remove", "DB___fop_write", "DB___fop_rename",
		    "DB___fop_file_remove", "DB___ham_insdel",
		"DB___ham_newpage", "DB___ham_splitdata", "DB___ham_replace",
		    "DB___ham_copypage",
		"DB___ham_metagroup", "DB___ham_groupalloc", "DB___ham_curadj",
		    "DB___ham_chgpg", "DB___qam_incfirst",
		"DB___qam_mvptr", "DB___qam_del", "DB___qam_add",
		    "DB___qam_delext", "DB___txn_regop",
		"DB___txn_regop_gen", "DB___txn_regop_rowlocks", "DB___txn_ckp",
		    "DB___txn_child", "DB___txn_xa_regop",
		"DB___txn_recycle"
	};
	int i;

	for (i = 0; i < sizeof(events) / sizeof(events[0]); i++) {
		if (log_event_counts[events[i]])
			logmsg(LOGMSG_USER, "%-20s %d\n", event_names[i],
			    log_event_counts[events[i]]);
	}
}


static char *
optostr(int op)
{
	switch (op) {
	case DB___bam_split:
		return "DB___bam_split";
	case DB___bam_rsplit:
		return "DB___bam_rsplit";
	case DB___bam_adj:
		return "DB___bam_adj";
	case DB___bam_cadjust:
		return "DB___bam_cadjust";
	case DB___bam_cdel:
		return "DB___bam_cdel";
	case DB___bam_repl:
		return "DB___bam_repl";
	case DB___bam_root:
		return "DB___bam_root";
	case DB___bam_curadj:
		return "DB___bam_curadj";
	case DB___bam_rcuradj:
		return "DB___bam_rcuradj";
	case DB___crdel_metasub:
		return "DB___crdel_metasub";
	case DB___db_addrem:
		return "DB___db_addrem";
	case DB___db_big:
		return "DB___db_big";
	case DB___db_ovref:
		return "DB___db_ovref";
	case DB___db_relink:
		return "DB___db_relink";
	case DB___db_debug:
		return "DB___db_debug";
	case DB___db_noop:
		return "DB___db_noop";
	case DB___db_pg_alloc:
		return "DB___db_pg_alloc";
	case DB___db_pg_free:
		return "DB___db_pg_free";
	case DB___db_cksum:
		return "DB___db_cksum";
	case DB___db_pg_freedata:
		return "DB___db_pg_freedata";
	case DB___db_pg_prepare:
		return "DB___db_pg_prepare";
	case DB___db_pg_new:
		return "DB___db_pg_new";
	case DB___dbreg_register:
		return "DB___dbreg_register";
	case DB___fop_create:
		return "DB___fop_create";
	case DB___fop_remove:
		return "DB___fop_remove";
	case DB___fop_write:
		return "DB___fop_write";
	case DB___fop_rename:
		return "DB___fop_rename";
	case DB___fop_file_remove:
		return "DB___fop_file_remove";
	case DB___ham_insdel:
		return "DB___ham_insdel";
	case DB___ham_newpage:
		return "DB___ham_newpage";
	case DB___ham_splitdata:
		return "DB___ham_splitdata";
	case DB___ham_replace:
		return "DB___ham_replace";
	case DB___ham_copypage:
		return "DB___ham_copypage";
	case DB___ham_metagroup:
		return "DB___ham_metagroup";
	case DB___ham_groupalloc:
		return "DB___ham_groupalloc";
	case DB___ham_curadj:
		return "DB___ham_curadj";
	case DB___ham_chgpg:
		return "DB___ham_chgpg";
	case DB___qam_incfirst:
		return "DB___qam_incfirst";
	case DB___qam_mvptr:
		return "DB___qam_mvptr";
	case DB___qam_del:
		return "DB___qam_del";
	case DB___qam_add:
		return "DB___qam_add";
	case DB___qam_delext:
		return "DB___qam_delext";
	case DB___txn_regop:
		return "DB___txn_regop";
	case DB___txn_regop_gen:
		return "DB___txn_regop_gen";
	case DB___txn_regop_rowlocks:
		return "DB___txn_regop_rowlocks";
	case DB___txn_ckp:
		return "DB___txn_ckp";
	case DB___txn_child:
		return "DB___txn_child";
	case DB___txn_xa_regop:
		return "DB___txn_xa_regop";
	case DB___txn_recycle:
		return "DB___txn_recycle";
	default:
		return "???";
	};
}

u_int32_t
file_id_for_recovery_record(DB_ENV *env, DB_LSN *lsn, int rectype, DBT *dbt)
{
	int off = -1;
	u_int32_t fileid = UINT32_MAX;

        /* Skip custom log recs */
        if (rectype < 10000) {
            log_event_counts[rectype]++;
        }

        /*
	 * Depending on type of record, find the offset in the log
	 * record of the fileid.  Offsets are derived from reading the
	 * autogenerated read routine.  All records begin with a type
	 * + txnid + prev_lsn. Most, but not all records have the
	 * fileid next.  Some have an opcode.
	 */
	switch (rectype) {
	case DB___bam_split:
	case DB___bam_prefix:
	case DB___bam_rsplit:
	case DB___db_pg_alloc:
	case DB___bam_adj:
	case DB___bam_cadjust:
	case DB___bam_cdel:
	case DB___bam_repl:
	case DB___bam_root:
	case DB___bam_curadj:
	case DB___bam_rcuradj:
        case DB___bam_pgcompact:
        case DB___crdel_metasub:
	case DB___db_ovref:
	case DB___db_pg_free:
	case DB___db_pg_freedata:
	case DB___db_pg_prepare:
	case DB___db_pg_new:
	case DB___ham_splitdata:
	case DB___ham_replace:
	case DB___ham_copypage:
	case DB___ham_metagroup:
	case DB___ham_groupalloc:
	case DB___ham_curadj:
	case DB___ham_chgpg:
	case DB___qam_incfirst:
	case DB___qam_del:
	case DB___qam_add:
	case DB___qam_delext:
		off = sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN);
		break;

	case DB___db_addrem:
	case DB___db_big:
	case DB___db_relink:
	case DB___ham_insdel:
	case DB___ham_newpage:
	case DB___qam_mvptr:
		/*
		 * These records take an additional 'opcode' parameter
		 * before the fileid
		 */
		off =
		    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
		    sizeof(u_int32_t);
		break;

	case DB___dbreg_register:
		/*
		 * This includes the file name before the fileid, so
		 * it's variable.  dbregister operations don't include
		 * IO and would gain nothing from parallelism, so
		 * don't waste time parsing the record, just have
		 * thread 0 handle it.
		 */
	case DB___db_debug:
	case DB___db_noop:
		/* Don't care, no-op */
	case DB___db_cksum:
		/* Not a per-file opcode. */

	case DB___fop_create:
	case DB___fop_remove:
	case DB___fop_write:
	case DB___fop_rename:
	case DB___fop_file_remove:
		/*
		 * File ops deal file files that aren't in the
		 * registry yet and dno't have ids.  Some of these are
		 * slow operations (like deleting large files) and it
		 * would be nice to extract some parallelism out of
		 * it, but it's not safe to do.  For now, relegate it
		 * to thread 0.
		 */

	case DB___txn_regop:
	case DB___txn_regop_gen:
	case DB___txn_regop_rowlocks:
	case DB___txn_ckp:
	case DB___txn_child:
	case DB___txn_xa_regop:
	case DB___txn_recycle:
		/*
		 * Transaction opcodes are not per-file and should be
		 * serialized anyway.  They won't be part of a bigger
		 * transaction, and thus wouldn't run parallel with
		 * anything that may depend on them.  TODO: child
		 * transactions? do they work? do we care?
		 */
		break;

	default:
		/*
		 * Berkeley documents that all their log record types
		 * are < 10000.  If we get one, and we didn't handle
		 * it above, we have a huge problem. Otherwise it's
		 * out of ours.  None of our opcodes are part of
		 * bigger transactions except rowlocks.
		 */
		if (rectype < 10000) {
			logmsg(LOGMSG_FATAL, "got rectype %d, don't know how to handle it",
			    rectype);
			abort();
		}
		break;
	}

	/* Get fileid out of the record */
	if (off != -1)
		LOGCOPY_32(&fileid, (char *)dbt->data + off);

	/*
	 * Some records have nothing to do with files.  These always
	 * get returned as 0 and are all processed in the same thread
	 * (thread 0).
	 */

	return fileid;
}

/*
 * __db_dispatch --
 *
 * This is the transaction dispatch function used by the db access methods.
 * It is designed to handle the record format used by all the access
 * methods (the one automatically generated by the db_{h,log,read}.sh
 * scripts in the tools directory).  An application using a different
 * recovery paradigm will supply a different dispatch function to txn_open.
 *
 * PUBLIC: int __db_dispatch __P((DB_ENV *,
 * PUBLIC:     int (**)__P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *)),
 * PUBLIC:     size_t, DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_dispatch(dbenv, dtab, dtabsize, db, lsnp, redo, info)
	DB_ENV *dbenv;		/* The environment. */
	int (**dtab)__P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
	size_t dtabsize;	/* Size of the dtab. */
	DBT *db;		/* The log record upon which to dispatch. */
	DB_LSN *lsnp;		/* The lsn of the record being dispatched. */
	db_recops redo;		/* Redo this op (or undo it). */
	void *info;
{
	DB_LSN prev_lsn;
	u_int32_t rectype, txnid;
	int make_call, ret;

	LOGCOPY_32(&rectype, db->data);
	LOGCOPY_32(&txnid, (u_int8_t *)db->data + sizeof(rectype));
	make_call = ret = 0;

	/* If we don't have a dispatch table, it's hard to dispatch. */
	DB_ASSERT(dtab != NULL);

	/*
	 * If we find a record that is in the user's number space and they
	 * have specified a recovery routine, let them handle it.  If they
	 * didn't specify a recovery routine, then we expect that they've
	 * followed all our rules and registered new recovery functions.
	 */
	switch (redo) {
	case DB_TXN_LOGICAL_BACKWARD_ROLL:
		switch (rectype) {
		case DB___txn_regop:
		case DB___txn_regop_gen:
		case DB___txn_regop_rowlocks:
		case DB___txn_child:
			/* need to capture all transactions, as I am collecting
			 * committed transactions */
			make_call = 1;
			break;
		case DB___txn_recycle:
			/* to support transaction recycling we need to handle the
			 * cleanup of transactions when reaching the first log entry */
			fprintf(stderr,
			    "%s:%d TXN_RECYCLE unsupported yet at lsn %d:%d\n",
			    __FILE__, __LINE__, lsnp->file, lsnp->offset);
			return -1;
			break;
		default:
			/* capture our logical logging */
			if (rectype >= DB_user_BEGIN &&
			    dbenv->app_dispatch != NULL)
				make_call = 1;
			break;
		}
		break;

	case DB_TXN_GETALLPGNOS:
		if (rectype < DB_user_BEGIN)
			make_call = 1;
		break;
	case DB_TXN_ABORT:
	case DB_TXN_APPLY:
	case DB_TXN_PRINT:
		make_call = 1;
		break;
	case DB_TXN_OPENFILES:
		/*
		 * We collect all the transactions that have
		 * "begin" records, those with no previous LSN,
		 * so that we do not abort partial transactions.
		 * These are known to be undone, otherwise the
		 * log would not have been freeable.
		 *
		 * Any rowlocks commit should also be added to
		 * this list, as we'll be grabbing rowlocks as
		 * necessary during recovery.
		 */
		LOGCOPY_TOLSN(&prev_lsn, (u_int8_t *)db->data +
		    sizeof(rectype) + sizeof(txnid));
		if (txnid != 0 && prev_lsn.file == 0 && (ret =
			__db_txnlist_add(dbenv, info, txnid, TXN_OK,
			    NULL)) != 0)
			 return (ret);

		/* FALLTHROUGH */
	case DB_TXN_POPENFILES:
		if (rectype == DB___dbreg_register ||
		    rectype == DB___txn_child ||
		    rectype == DB___txn_ckp || rectype == DB___txn_recycle)
			return (dtab[rectype] (dbenv, db, lsnp, redo, info));
		break;
	case DB_TXN_BACKWARD_ROLL:
		/*
		 * Running full recovery in the backward pass.  If we've
		 * seen this txnid before and added to it our commit list,
		 * then we do nothing during this pass, unless this is a child
		 * commit record, in which case we need to process it.  If
		 * we've never seen it, then we call the appropriate recovery
		 * routine.
		 *
		 * We need to always undo DB___db_noop records, so that we
		 * properly handle any aborts before the file was closed.
		 */
		switch (rectype) {
		case DB___txn_regop:
		case DB___txn_regop_gen:
		case DB___txn_regop_rowlocks:
		case DB___txn_recycle:
		case DB___txn_ckp:
		case DB___db_noop:
		case DB___fop_file_remove:
		case DB___txn_child:
			make_call = 1;
			break;

		case DB___dbreg_register:
			if (txnid == 0) {
				make_call = 1;
				break;
			}
			/* FALLTHROUGH */
		default:
			if (txnid != 0 && (ret =
			    __db_txnlist_find(dbenv,
			    info, txnid)) != TXN_COMMIT && ret != TXN_IGNORE) {
				/*
				 * If not found then, this is an incomplete
				 * abort.
				 */
				if (ret == TXN_NOTFOUND)
					return (__db_txnlist_add(dbenv,
					    info, txnid, TXN_IGNORE, lsnp));
				make_call = 1;
				if (ret == TXN_OK &&
				    (ret = __db_txnlist_update(dbenv,
				    info, txnid,
				    rectype == DB___txn_xa_regop ?
				    TXN_PREPARE : TXN_ABORT, NULL)) != 0)
					return (ret);
			}
		}
		break;
	case DB_TXN_FORWARD_ROLL:
		/*
		 * In the forward pass, if we haven't seen the transaction,
		 * do nothing, else recover it.
		 *
		 * We need to always redo DB___db_noop records, so that we
		 * properly handle any commits after the file was closed.
		 */
		switch (rectype) {
		case DB___txn_recycle:
		case DB___txn_ckp:
		case DB___db_noop:
			make_call = 1;
			break;

		default:
			if (txnid != 0 && (ret = __db_txnlist_find(dbenv,
			    info, txnid)) == TXN_COMMIT)
				make_call = 1;
			else if (ret != TXN_IGNORE &&
			    (rectype == DB___ham_metagroup ||
			    rectype == DB___ham_groupalloc ||
			    rectype == DB___db_pg_alloc)) {
				/*
				 * Because we cannot undo file extensions
				 * all allocation records must be reprocessed
				 * during rollforward in case the file was
				 * just created.  It may not have been
				 * present during the backward pass.
				 */
				make_call = 1;
				redo = DB_TXN_BACKWARD_ALLOC;
			} else if (rectype == DB___dbreg_register) {
				/*
				 * This may be a transaction dbreg_register.
				 * If it is, we only make the call on a COMMIT,
				 * which we checked above. If it's not, then we
				 * should always make the call, because we need
				 * the file open information.
				 */
				if (txnid == 0)
					make_call = 1;
			}
		}
		break;
	case DB_TXN_GETPGNOS:
		/*
		 * If this is one of DB's own log records, we simply
		 * dispatch.
		 */
		if (rectype < DB_user_BEGIN) {
			make_call = 1;
			break;
		}

		/*
		 * If we're still here, this is a custom record in an
		 * application that's doing app-specific logging.  Such a
		 * record doesn't have a getpgno function for the user
		 * dispatch function to call--the getpgnos functions return
		 * which pages replication needs to lock using the TXN_RECS
		 * structure, which is private and not something we want to
		 * document.
		 *
		 * Thus, we leave any necessary locking for the app's
		 * recovery function to do during the upcoming
		 * DB_TXN_APPLY.  Fill in default getpgnos info (we need
		 * a stub entry for every log record that will get
		 * DB_TXN_APPLY'd) and return success.
		 */
		return (__db_default_getpgnos(dbenv, lsnp, info));
	case DB_TXN_BACKWARD_ALLOC:
	default:
		return (__db_unknown_flag(
		    dbenv, "__db_dispatch", (u_int32_t)redo));
	}

	/*
	 * The switch statement uses ret to receive the return value of
	 * __db_txnlist_find, which returns a large number of different
	 * statuses, none of which we will be returning.  For safety,
	 * let's reset this here in case we ever do a "return(ret)"
	 * below in the future.
	 */
	ret = 0;

	if (make_call) {
		/*
		 * If the debug flag is set then we are logging
		 * records for a non-durable update so that they
		 * may be examined for diagnostic purposes.
		 * So only make the call if we are printing,
		 * otherwise we need to extract the previous
		 * lsn so undo will work properly.
		 */
		if (rectype & DB_debug_FLAG) {
			if (redo == DB_TXN_PRINT)
				rectype &= ~DB_debug_FLAG;
			else {
				LOGCOPY_TOLSN(lsnp,
				    (u_int8_t *)db->data +
				    sizeof(rectype) + sizeof(txnid));
				return (0);
			}
		}
		if (rectype >= DB_user_BEGIN && dbenv->app_dispatch != NULL)
			return (dbenv->app_dispatch(dbenv, db, lsnp, redo));
		else {
			/*
			 * The size of the dtab table argument is the same as
			 * the standard table, use the standard table's size
			 * as our sanity check.
			 */
			if (rectype > dtabsize || dtab[rectype] == NULL) {
				__db_err(dbenv,
				    "Illegal record type %lu in log",
				    (u_long)rectype);
				return (EINVAL);
			}
			/* let's do this only on the replicants, for now */
			return (dtab[rectype](dbenv, db, lsnp, redo, info));
		}
	}

	return (0);
}

/*
 * __db_add_recovery --
 *
 * PUBLIC: int __db_add_recovery __P((DB_ENV *,
 * PUBLIC:   int (***)(DB_ENV *, DBT *, DB_LSN *, db_recops, void *), size_t *,
 * PUBLIC:   int (*)(DB_ENV *, DBT *, DB_LSN *, db_recops, void *), u_int32_t));
 */
int
__db_add_recovery(dbenv, dtab, dtabsize, func, ndx)
	DB_ENV *dbenv;
	int (***dtab) __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
	size_t *dtabsize;
	int (*func) __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
	u_int32_t ndx;
{
	size_t i, nsize;
	int ret;

	/* Check if we have to grow the table. */
	if (ndx >= *dtabsize) {
		nsize = ndx + 40;
		if ((ret =
		    __os_realloc(dbenv, nsize * sizeof((*dtab)[0]), dtab)) != 0)
			return (ret);
		for (i = *dtabsize; i < nsize; ++i)
			(*dtab)[i] = NULL;
		*dtabsize = nsize;
	}

	(*dtab)[ndx] = func;
	return (0);
}

/*
 * __db_txnlist_init --
 *	Initialize transaction linked list.
 *
 * PUBLIC: int __db_txnlist_init __P((DB_ENV *,
 * PUBLIC:     u_int32_t, u_int32_t, DB_LSN *, void *));
 */
int
__db_txnlist_init(dbenv, low_txn, hi_txn, trunc_lsn, retp)
	DB_ENV *dbenv;
	u_int32_t low_txn, hi_txn;
	DB_LSN *trunc_lsn;
	void *retp;
{
	DB_TXNHEAD *headp;
	u_int32_t size, tmp;
	int ret;

	/*
	 * Size a hash table.
	 *	If low is zero then we are being called during rollback
	 * and we need only one slot.
	 *	Hi maybe lower than low if we have recycled txnid's.
	 *	The numbers here are guesses about txn density, we can afford
	 * to look at a few entries in each slot.
	 */
	if (low_txn == 0)
		size = 1;
	else {
		if (hi_txn < low_txn) {
			tmp = hi_txn;
			hi_txn = low_txn;
			low_txn = tmp;
		}
		tmp = hi_txn - low_txn;
		/* See if we wrapped around. */
		if (tmp > (TXN_MAXIMUM - TXN_MINIMUM) / 2)
			tmp = (low_txn - TXN_MINIMUM) + (TXN_MAXIMUM - hi_txn);
		size = tmp / 5;
		if (size < 100)
			size = 100;
	}
	if ((ret = __os_malloc(dbenv,
	    sizeof(DB_TXNHEAD) + size * sizeof(headp->head), &headp)) != 0)
		return (ret);

	memset(headp, 0, sizeof(DB_TXNHEAD) + size * sizeof(headp->head));
	headp->maxid = hi_txn;
	headp->generation = 0;
	headp->nslots = size;
	headp->gen_alloc = 8;
	if ((ret = __os_malloc(dbenv, headp->gen_alloc *
	    sizeof(headp->gen_array[0]), &headp->gen_array)) != 0) {
		__os_free(dbenv, headp);
		return (ret);
	}
	headp->gen_array[0].generation = 0;
	headp->gen_array[0].txn_min = TXN_MINIMUM;
	headp->gen_array[0].txn_max = TXN_MAXIMUM;
	if (trunc_lsn != NULL) {
		headp->trunc_lsn = *trunc_lsn;
		headp->maxlsn = *trunc_lsn;
	} else {
		ZERO_LSN(headp->trunc_lsn);
		ZERO_LSN(headp->maxlsn);
	}
	ZERO_LSN(headp->ckplsn);

	*(void **)retp = headp;
	return (0);
}

/*
 * __db_txnlist_add --
 *	Add an element to our transaction linked list.
 *
 * PUBLIC: int __db_txnlist_add __P((DB_ENV *,
 * PUBLIC:     void *, u_int32_t, int32_t, DB_LSN *));
 */
int
__db_txnlist_add(dbenv, listp, txnid, status, lsn)
	DB_ENV *dbenv;
	void *listp;
	u_int32_t txnid;
	int32_t status;
	DB_LSN *lsn;
{
	DB_TXNHEAD *hp;
	DB_TXNLIST *elp;
	int ret;

	if ((ret = __os_malloc(dbenv, sizeof(DB_TXNLIST), &elp)) != 0)
		return (ret);

	hp = (DB_TXNHEAD *)listp;
	LIST_INSERT_HEAD(&hp->head[DB_TXNLIST_MASK(hp, txnid)], elp, links);

	elp->type = TXNLIST_TXNID;
	elp->u.t.txnid = txnid;
	elp->u.t.status = status;
	elp->u.t.generation = hp->generation;
	if (txnid > hp->maxid)
		hp->maxid = txnid;
	if (lsn != NULL && IS_ZERO_LSN(hp->maxlsn) && status == TXN_COMMIT)
		hp->maxlsn = *lsn;

	DB_ASSERT(lsn == NULL ||
	    status != TXN_COMMIT || log_compare(&hp->maxlsn, lsn) >= 0);

	return (0);
}

/*
 * __db_txnlist_remove --
 *	Remove an element from our transaction linked list.
 *
 * PUBLIC: int __db_txnlist_remove __P((DB_ENV *, void *, u_int32_t));
 */
int
__db_txnlist_remove(dbenv, listp, txnid)
	DB_ENV *dbenv;
	void *listp;
	u_int32_t txnid;
{
	DB_TXNLIST *entry;

	return (__db_txnlist_find_internal(dbenv,
	    listp, TXNLIST_TXNID, txnid,
	    NULL, &entry, 1) == TXN_NOTFOUND ? TXN_NOTFOUND : TXN_OK);
}

/*
 * __db_txnlist_ckp --
 *	Used to record the maximum checkpoint that will be retained
 * after recovery.  Typically this is simply the max checkpoint, but
 * if we are doing client replication recovery or timestamp-based
 * recovery, we are going to virtually truncate the log and we need
 * to retain the last checkpoint before the truncation point.
 *
 * PUBLIC: void __db_txnlist_ckp __P((DB_ENV *, void *, DB_LSN *));
 */
void
__db_txnlist_ckp(dbenv, listp, ckp_lsn)
	DB_ENV *dbenv;
	void *listp;
	DB_LSN *ckp_lsn;
{
	DB_TXNHEAD *hp;

	COMPQUIET(dbenv, NULL);

	hp = (DB_TXNHEAD *)listp;

	if (IS_ZERO_LSN(hp->ckplsn) && !IS_ZERO_LSN(hp->maxlsn) &&
	    log_compare(&hp->maxlsn, ckp_lsn) >= 0)
		hp->ckplsn = *ckp_lsn;
}

/*
 * __db_txnlist_end --
 *	Discard transaction linked list.
 *
 * PUBLIC: void __db_txnlist_end __P((DB_ENV *, void *));
 */
void
__db_txnlist_end(dbenv, listp)
	DB_ENV *dbenv;
	void *listp;
{
	DB_TXNHEAD *hp;
	DB_TXNLIST *p;
	u_int32_t i;

	if ((hp = (DB_TXNHEAD *)listp) == NULL)
		return;

	for (i = 0; i < hp->nslots; i++)
		while (hp != NULL && (p = LIST_FIRST(&hp->head[i])) != NULL) {
			LIST_REMOVE(p, links);
			switch (p->type) {
			case TXNLIST_LSN:
				__os_free(dbenv, p->u.l.lsn_array);
				break;
			case TXNLIST_DELETE:
			case TXNLIST_PGNO:
			case TXNLIST_TXNID:
			default:
				/*
				 * Possibly an incomplete DB_TXNLIST; just
				 * free it.
				 */
				break;
			}
			__os_free(dbenv, p);
		}

	if (hp->gen_array != NULL)
		__os_free(dbenv, hp->gen_array);
	__os_free(dbenv, listp);
}


/*
 * __db_txnlist_first--
 *	Returns the first transaction in the list.
 *
 * PUBLIC: void __db_txnlist_end __P((DB_ENV *, void *));
int
__db_txnlist_first(dbenv, listp, txnlistp, delete)
	DB_ENV *dbenv;
	void *listp;
	DB_TXNLIST **txnlistp;
	int delete;
{
	DB_TXNHEAD *hp;
	DB_TXNLIST *p;
	u_int32_t i;

	if ((hp = (DB_TXNHEAD *)listp) == NULL)
		return (TXN_NOTFOUND);

	for (i = 0; i < hp->nslots; i++)
   {
      for(p = LIST_FIRST(&hp->head[i]);!p; p = LIST_NEXT(&hp->head[i], p))
      {
      {
         if (p->type != TXNLIST_TXNID)
            continue;


         if (delete)
            LIST_REMOVE(p, links);
         
         if (txnlistp) 
            *txnlistp = p;
         return (TXN_OK);
		}
   }

   return (TXN_NOTFOUND);
}
 */

/*
 * __db_txnlist_find --
 *	Checks to see if a txnid with the current generation is in the
 *	txnid list.  This returns TXN_NOTFOUND if the item isn't in the
 *	list otherwise it returns (like __db_txnlist_find_internal)
 *	the status of the transaction.  A txnid of 0 means the record
 *	was generated while not in a transaction.
 *
 * PUBLIC: int __db_txnlist_find __P((DB_ENV *, void *, u_int32_t));
 */
int
__db_txnlist_find(dbenv, listp, txnid)
	DB_ENV *dbenv;
	void *listp;
	u_int32_t txnid;
{
	DB_TXNLIST *entry;

	if (txnid == 0)
		return (TXN_NOTFOUND);
	return (__db_txnlist_find_internal(dbenv, listp,
	    TXNLIST_TXNID, txnid, NULL, &entry, 0));
}

/*
 * __db_txnlist_update --
 *	Change the status of an existing transaction entry.
 *	Returns TXN_NOTFOUND if no such entry exists.
 *
 * PUBLIC: int __db_txnlist_update __P((DB_ENV *,
 * PUBLIC:     void *, u_int32_t, int32_t, DB_LSN *));
 */
int
__db_txnlist_update(dbenv, listp, txnid, status, lsn)
	DB_ENV *dbenv;
	void *listp;
	u_int32_t txnid;
	int32_t status;
	DB_LSN *lsn;
{
	DB_TXNHEAD *hp;
	DB_TXNLIST *elp;
	int ret;

	if (txnid == 0)
		return (TXN_NOTFOUND);
	hp = (DB_TXNHEAD *)listp;
	ret = __db_txnlist_find_internal(dbenv,
	    listp, TXNLIST_TXNID, txnid, NULL, &elp, 0);

	if (ret == TXN_NOTFOUND || ret == TXN_IGNORE)
		return (ret);
	elp->u.t.status = status;

	if (lsn != NULL && IS_ZERO_LSN(hp->maxlsn) && status == TXN_COMMIT)
		hp->maxlsn = *lsn;

	return (ret);
}

/*
 * __db_txnlist_find_internal --
 *	Find an entry on the transaction list.  If the entry is not there or
 *	the list pointer is not initialized we return TXN_NOTFOUND.  If the
 *	item is found, we return the status.  Currently we always call this
 *	with an initialized list pointer but checking for NULL keeps it general.
 */
static int
__db_txnlist_find_internal(dbenv, listp, type, txnid, uid, txnlistp, delete)
	DB_ENV *dbenv;
	void *listp;
	db_txnlist_type type;
	u_int32_t  txnid;
	u_int8_t uid[DB_FILE_ID_LEN];
	DB_TXNLIST **txnlistp;
	int delete;
{
	struct __db_headlink *head;
	DB_TXNHEAD *hp;
	DB_TXNLIST *p;
	u_int32_t generation, hash, i;
	int ret;

	if ((hp = (DB_TXNHEAD *)listp) == NULL)
		return (TXN_NOTFOUND);

	switch (type) {
	case TXNLIST_TXNID:
		hash = txnid;
		/* Find the most recent generation containing this ID */
		for (i = 0; i <= hp->generation; i++)
			/* The range may wrap around the end. */
			if (hp->gen_array[i].txn_min <
			    hp->gen_array[i].txn_max ?
			    (txnid >= hp->gen_array[i].txn_min &&
			    txnid <= hp->gen_array[i].txn_max) :
			    (txnid >= hp->gen_array[i].txn_min ||
			    txnid <= hp->gen_array[i].txn_max))
				break;
		DB_ASSERT(i <= hp->generation);
		generation = hp->gen_array[i].generation;
		break;
	case TXNLIST_PGNO:
		memcpy(&hash, uid, sizeof(hash));
		generation = 0;
		break;
	case TXNLIST_DELETE:
	case TXNLIST_LSN:
	default:
		DB_ASSERT(0);
		return (EINVAL);
	}

	head = &hp->head[DB_TXNLIST_MASK(hp, hash)];

	for (p = LIST_FIRST(head); p != NULL; p = LIST_NEXT(p, links)) {
		if (p->type != type)
			continue;
		switch (type) {
		case TXNLIST_TXNID:
			if (p->u.t.txnid != txnid ||
			    generation != p->u.t.generation)
				continue;
			ret = p->u.t.status;
			break;

		case TXNLIST_PGNO:
			if (memcmp(uid, p->u.p.uid, DB_FILE_ID_LEN) != 0)
				continue;

			ret = 0;
			break;
		case TXNLIST_DELETE:
		case TXNLIST_LSN:
		default:
			DB_ASSERT(0);
			ret = EINVAL;
		}
		if (delete == 1) {
			LIST_REMOVE(p, links);
			__os_free(dbenv, p);
		} else if (p != LIST_FIRST(head)) {
			/* Move it to head of list. */
			LIST_REMOVE(p, links);
			LIST_INSERT_HEAD(head, p, links);
		}
		*txnlistp = p;
		return (ret);
	}

	return (TXN_NOTFOUND);
}

/*
 * __db_txnlist_gen --
 *	Change the current generation number.
 *
 * PUBLIC: int __db_txnlist_gen __P((DB_ENV *,
 * PUBLIC:       void *, int, u_int32_t, u_int32_t));
 */
int
__db_txnlist_gen(dbenv, listp, incr, min, max)
	DB_ENV *dbenv;
	void *listp;
	int incr;
	u_int32_t min, max;
{
	DB_TXNHEAD *hp;
	int ret;

	/*
	 * During recovery generation numbers keep track of "restart"
	 * checkpoints and recycle records.  Restart checkpoints occur
	 * whenever we take a checkpoint and there are no outstanding
	 * transactions.  When that happens, we can reset transaction IDs
	 * back to TXNID_MINIMUM.  Currently we only do the reset
	 * at then end of recovery.  Recycle records occur when txnids
	 * are exhausted during runtime.  A free range of ids is identified
	 * and logged.  This code maintains a stack of ranges.  A txnid
	 * is given the generation number of the first range it falls into
	 * in the stack.
	 */
	hp = (DB_TXNHEAD *)listp;
	if (incr < 0) {
		--hp->generation;
		memmove(hp->gen_array, &hp->gen_array[1],
		    (hp->generation + 1) * sizeof(hp->gen_array[0]));
	} else {
		++hp->generation;
		if (hp->generation >= hp->gen_alloc) {
			hp->gen_alloc *= 2;
			if ((ret = __os_realloc(dbenv, hp->gen_alloc *
			    sizeof(hp->gen_array[0]), &hp->gen_array)) != 0)
				return (ret);
		}
		memmove(&hp->gen_array[1], &hp->gen_array[0],
		    hp->generation * sizeof(hp->gen_array[0]));
		hp->gen_array[0].generation = hp->generation;
		hp->gen_array[0].txn_min = min;
		hp->gen_array[0].txn_max = max;
	}
	return (0);
}

#define	TXN_BUBBLE(AP, MAX) {						\
	DB_LSN __tmp;							\
	u_int32_t __j;							\
									\
	for (__j = 0; __j < MAX - 1; __j++)				\
		if (log_compare(&AP[__j], &AP[__j + 1]) < 0) {		\
			__tmp = AP[__j];				\
			AP[__j] = AP[__j + 1];				\
			AP[__j + 1] = __tmp;				\
		}							\
}

/*
 * __db_txnlist_lsnadd --
 *	Add to or re-sort the transaction list lsn entry.  Note that since this
 *	is used during an abort, the __txn_undo code calls into the "recovery"
 *	subsystem explicitly, and there is only a single TXNLIST_LSN entry on
 *	the list.
 *
 * PUBLIC: int __db_txnlist_lsnadd __P((DB_ENV *, void *, DB_LSN *, u_int32_t));
 */
int
__db_txnlist_lsnadd(dbenv, listp, lsnp, flags)
	DB_ENV *dbenv;
	void *listp;
	DB_LSN *lsnp;
	u_int32_t flags;
{
	DB_TXNHEAD *hp;
	DB_TXNLIST *elp;
	u_int32_t i;
	int ret;

	hp = (DB_TXNHEAD *)listp;

	for (elp = LIST_FIRST(&hp->head[0]);
	    elp != NULL; elp = LIST_NEXT(elp, links))
		if (elp->type == TXNLIST_LSN)
			break;

	if (elp == NULL)
		return (DB_SURPRISE_KID);

	if (LF_ISSET(TXNLIST_NEW)) {
		if (elp->u.l.ntxns >= elp->u.l.maxn) {
			if ((ret = __os_realloc(dbenv,
			    2 * elp->u.l.maxn * sizeof(DB_LSN),
			    &elp->u.l.lsn_array)) != 0)
				return (ret);
			elp->u.l.maxn *= 2;
		}
		elp->u.l.lsn_array[elp->u.l.ntxns++] = *lsnp;
	} else
		/* Simply replace the 0th element. */
		elp->u.l.lsn_array[0] = *lsnp;

	/*
	 * If we just added a new entry and there may be NULL entries, so we
	 * have to do a complete bubble sort, not just trickle a changed entry
	 * around.
	 */
	for (i = 0; i < (!LF_ISSET(TXNLIST_NEW) ? 1 : elp->u.l.ntxns); i++)
		TXN_BUBBLE(elp->u.l.lsn_array, elp->u.l.ntxns);

	*lsnp = elp->u.l.lsn_array[0];

	return (0);
}

/*
 * __db_txnlist_lsninit --
 *	Initialize a transaction list with an lsn array entry.
 *
 * PUBLIC: int __db_txnlist_lsninit __P((DB_ENV *, DB_TXNHEAD *, DB_LSN *));
 */
int
__db_txnlist_lsninit(dbenv, hp, lsnp)
	DB_ENV *dbenv;
	DB_TXNHEAD *hp;
	DB_LSN *lsnp;
{
	DB_TXNLIST *elp;
	int ret;

	elp = NULL;

	if ((ret = __os_malloc(dbenv, sizeof(DB_TXNLIST), &elp)) != 0)
		goto err;
	LIST_INSERT_HEAD(&hp->head[0], elp, links);

	if ((ret = __os_malloc(dbenv,
	    12 * sizeof(DB_LSN), &elp->u.l.lsn_array)) != 0)
		goto err;
	elp->type = TXNLIST_LSN;
	elp->u.l.maxn = 12;
	elp->u.l.ntxns = 1;
	elp->u.l.lsn_array[0] = *lsnp;

	return (0);

err:	__db_txnlist_end(dbenv, hp);
	return (ret);
}

/*
 * __db_add_limbo -- add pages to the limbo list.
 *	Get the file information and call pgnoadd for each page.
 *
 * PUBLIC: int __db_add_limbo __P((DB_ENV *,
 * PUBLIC:      void *, int32_t, db_pgno_t, int32_t));
 */
int
__db_add_limbo(dbenv, info, fileid, pgno, count)
	DB_ENV *dbenv;
	void *info;
	int32_t fileid;
	db_pgno_t pgno;
	int32_t count;
{
	DB_LOG *dblp;
	FNAME *fnp;
	int ret;

	dblp = dbenv->lg_handle;
	if ((ret = __dbreg_id_to_fname(dblp, fileid, 0, &fnp)) != 0)
		return (ret);

	do {
		if ((ret =
		    __db_txnlist_pgnoadd(dbenv, info, fileid, fnp->ufid,
		    R_ADDR(&dblp->reginfo, fnp->name_off), pgno)) != 0)
			return (ret);
		pgno++;
	} while (--count != 0);

	return (0);
}

/*
 * __db_do_the_limbo -- move pages from limbo to free.
 *
 * Limbo processing is what ensures that we correctly handle and
 * recover from page allocations.  During recovery, for each database,
 * we process each in-question allocation, link them into the free list
 * and then write out the new meta-data page that contains the pointer
 * to the new beginning of the free list.  On an abort, we use our
 * standard __db_free mechanism in a compensating transaction which logs
 * the specific modifications to the free list.
 *
 * If we run out of log space during an abort, then we can't write the
 * compensating transaction, so we abandon the idea of a compenating
 * transaction, and go back to processing how we do during recovery.
 * The reason that this is not the norm is that it's expensive: it requires
 * that we flush any database with an in-question allocation.  Thus if
 * a compensating transaction fails, we never try to restart it.
 *
 * Since files may be open and closed within transactions (in particular,
 * the master database for subdatabases), we must be prepared to open
 * files during this process.  If there is a compensating transaction, we
 * can open the files in that transaction.  If this was an abort and there
 * is no compensating transaction, then we've got to perform these opens
 * in the context of the aborting transaction so that we do not deadlock.
 * During recovery, there's no locking, so this isn't an issue.
 *
 * What you want to keep in mind when reading this is that there are two
 * algorithms going on here:  ctxn == NULL, then we're either in recovery
 * or our compensating transaction has failed and we're doing the
 * "create list and write meta-data page" algorithm.  Otherwise, we're in
 * an abort and doing the "use compensating transaction" algorithm.
 *
 * PUBLIC: int __db_do_the_limbo __P((DB_ENV *,
 * PUBLIC:     DB_TXN *, DB_TXN *, DB_TXNHEAD *, db_limbo_state));
 */
int
__db_do_the_limbo(dbenv, ptxn, txn, hp, state)
	DB_ENV *dbenv;
	DB_TXN *ptxn, *txn;
	DB_TXNHEAD *hp;
	db_limbo_state state;
{
	DB_TXNLIST *elp;
	u_int32_t h;
	int ret;

	ret = 0;
	/*
	 * The slots correspond to hash buckets.  We've hashed the
	 * fileids into hash buckets and need to pick up all affected
	 * files. (There will only be a single slot for an abort.)
	 */
	for (h = 0; h < hp->nslots; h++) {
		if ((elp = LIST_FIRST(&hp->head[h])) == NULL)
			continue;
		if (ptxn != NULL) {
			if ((ret =
			    __db_limbo_move(dbenv, ptxn, txn, elp)) != 0)
				goto err;
		} else if ((ret =
		    __db_limbo_bucket(dbenv, txn, elp, state)) != 0)
			goto err;
	}

err:	if (ret != 0) {
		__db_err(dbenv, "Fatal error in abort of an allocation");
		ret = __db_panic(dbenv, ret);
	}

	return (ret);
}

/* Limbo support routines. */

/*
 * __db_lock_move --
 *	Move a lock from child to parent.
 */
static int
__db_lock_move(dbenv, fileid, pgno, mode, ptxn, txn)
	DB_ENV *dbenv;
	u_int8_t *fileid;
	db_pgno_t pgno;
	db_lockmode_t mode;
	DB_TXN *ptxn, *txn;
{
	DBT lock_dbt;
	DB_LOCK lock;
	DB_LOCK_ILOCK lock_obj;
	DB_LOCKREQ req;
	int ret;

	lock_obj.pgno = pgno;
	memcpy(lock_obj.fileid, fileid, DB_FILE_ID_LEN);
	lock_obj.type = DB_PAGE_LOCK;

	memset(&lock_dbt, 0, sizeof(lock_dbt));
	lock_dbt.data = &lock_obj;
	lock_dbt.size = sizeof(lock_obj);

    ret = __lock_get(dbenv, txn->txnid, 0, &lock_dbt, mode, &lock);
    if (ret) {
        fprintf(stderr, "%s:%d: error getting a lock we already have?  ret=%d\n", 
                __func__, __LINE__, ret);
        abort();
    }

    memset(&req, 0, sizeof(req));
	req.lock = lock;
	req.op = DB_LOCK_TRADE_COMP;
	ret = __lock_vec(dbenv, ptxn->txnid, 0, &req, 1, NULL);

    if (ret) {
        fprintf(stderr, "%s:%d: error trading locks?  ret=%d\n", 
                __func__, __LINE__, ret);
        abort();
    }

	return (ret);
}

/*
 * __db_limbo_move
 *	Move all of the locks to the parent.
 *	These locks need to be in the child's commit
 *	record in order for parallel replication to
 *	work.
 */
static int
__db_limbo_move(dbenv, ptxn, txn, elp)
	DB_ENV *dbenv;
	DB_TXN *ptxn, *txn;
	DB_TXNLIST *elp;
{
	int ret, i;
	db_pgno_t pgno;



	for (; elp != NULL; elp = LIST_NEXT(elp, links)) {
		if (elp->type != TXNLIST_PGNO || elp->u.p.locked == 1)
			continue;

		if ((ret = __db_lock_move(dbenv, elp->u.p.uid,
			PGNO_BASE_MD, DB_LOCK_WRITE, ptxn, txn)) != 0)
			return (ret);

		for (i = 0; i < elp->u.p.nentries; i++) {
			pgno = elp->u.p.pgno_array[i];

			if (pgno == PGNO_INVALID)
				continue;

			if ((ret = __db_lock_move(dbenv, elp->u.p.uid,
							pgno, DB_LOCK_WRITE, ptxn, txn)) != 0)
				return (ret);
		}

		elp->u.p.locked = 1;
	}

	return (0);
}
/*
 * __db_limbo_bucket
 *	Perform limbo processing for a single hash bucket in the txnlist.
 * txn is the transaction aborting in the case of an abort and ctxn is the
 * compensating transaction.
 */

#define	T_RESTORED(txn)       ((txn) != NULL && F_ISSET(txn, TXN_RESTORED))
static int
__db_limbo_bucket(dbenv, txn, elp, state)
	DB_ENV *dbenv;
	DB_TXN *txn;
	DB_TXNLIST *elp;
	db_limbo_state state;
{
	DB *dbp;
	DB_MPOOLFILE *mpf;
	DBMETA *meta;
	DB_TXN *ctxn, *t;
	db_pgno_t last_pgno, pgno;
	int dbp_created, in_retry, ret, t_ret;

	ctxn = NULL;
	in_retry = 0;
	meta = NULL;
	mpf = NULL;
	ret = 0;
	for (; elp != NULL; elp = LIST_NEXT(elp, links)) {
		if (elp->type != TXNLIST_PGNO)
			continue;
retry:		dbp_created = 0;

		/*
		 * Pick the transaction in which to potentially
		 * log compensations.
		 */
		if (state == LIMBO_PREPARE)
			ctxn = txn;
		else if (!in_retry && state != LIMBO_RECOVER &&
		    state != LIMBO_TIMESTAMP && !T_RESTORED(txn) &&
		    (ret = __txn_compensate_begin(dbenv, &ctxn)) != 0)
			return (ret);

		/*
		 * Either use the compensating transaction or
		 * the one passed in, which will be null if recovering.
		 */
		t = ctxn == NULL ? txn : ctxn;

		/* First try to get a dbp by fileid. */
		ret =
		    __dbreg_id_to_db(dbenv, t, &dbp, elp->u.p.fileid, 0, NULL,
		    0);


#if 0
		if (ret == DB_DELETED) {
			fprintf(stderr, "dropping update for fid %d\n!",
			    elp->u.p.fileid);
			__db_panic(dbenv, ret);
		}
#endif

		/*
		 * File is being destroyed.  No need to worry about
		 * dealing with recovery of allocations.
		 */
		if (ret == DB_DELETED ||
		    (ret == 0 && F_ISSET(dbp, DB_AM_DISCARD)))
			goto next;

		if (ret != 0) {
			if ((ret = db_create(&dbp, dbenv, 0)) != 0)
				goto err;

			/*
			 * This tells the system not to lock, which is always
			 * OK, whether this is an abort or recovery.
			 */
			F_SET(dbp, DB_AM_COMPENSATE);
			dbp_created = 1;

			/* It is ok if the file is nolonger there. */
			ret = __db_open(dbp,
			    t, elp->u.p.fname, NULL, DB_UNKNOWN,
			    DB_ODDFILESIZE, __db_omode("rw----"), PGNO_BASE_MD);
			if (ret == ENOENT)
				goto next;
		}

		/*
		 * Verify that we are opening the same file that we were
		 * referring to when we wrote this log record.
		 */
		if (memcmp(elp->u.p.uid, dbp->fileid, DB_FILE_ID_LEN) != 0)
			goto next;

		mpf = dbp->mpf;
		last_pgno = PGNO_INVALID;

		if (ctxn == NULL || state == LIMBO_COMPENSATE) {
			pgno = PGNO_BASE_MD;
			if ((ret = __memp_fget(mpf, &pgno, 0, &meta)) != 0)
				goto err;
			last_pgno = meta->free;
		}

		if (state == LIMBO_PREPARE) {
			if ((ret = __db_limbo_prepare(dbp, ctxn, elp)) != 0)
				goto err;
		} else
			ret = __db_limbo_fix(dbp,
			     txn, ctxn, elp, &last_pgno, meta, state);
		/*
		 * If we were doing compensating transactions, then we are
		 * going to hope this error was due to running out of space.
		 * We'll change modes (into the sync the file mode) and keep
		 * trying.  If we weren't doing compensating transactions,
		 * then this is a real error and we're sunk.
		 */
		if (ret != 0) {
			if (ret == DB_RUNRECOVERY || ctxn == NULL)
				goto err;
			in_retry = 1;
			goto retry;
		}

		if (state == LIMBO_PREPARE)
			ctxn = NULL;

		else if (ctxn != NULL) {
			/*
			 * We only force compensation at the end of recovery.
			 * We want the txn_commit to be logged so turn
			 * off the recovery flag briefly.
			 */
			if (state == LIMBO_COMPENSATE)
				F_CLR(
				    (DB_LOG *)dbenv->lg_handle, DBLOG_RECOVER);
			ret = __txn_commit(ctxn, DB_TXN_NOSYNC);
			ctxn = NULL;
			if (state == LIMBO_COMPENSATE)
				F_SET(
				    (DB_LOG *)dbenv->lg_handle, DBLOG_RECOVER);
			if (ret != 0)
				goto retry;
		}

		/*
		 * This is where we handle the case where we're explicitly
		 * putting together a free list.  We need to decide whether
		 * we have to write the meta-data page, and if we do, then
		 * we need to sync it as well.
		 */
		else if (last_pgno == meta->free) {
			/* No change to page; just put the page back. */
			if ((ret = __memp_fput(mpf, meta, 0)) != 0)
				goto err;
			meta = NULL;
		} else {
			/*
			 * These changes are unlogged so we cannot have the
			 * metapage pointing at pages that are not on disk.
			 * Therefore, we flush the new free list, then update
			 * the metapage.  We have to put the meta-data page
			 * first so that it isn't pinned when we try to sync.
			 */
			if (!IS_RECOVERING(dbenv) && !T_RESTORED(txn))
				__db_err(dbenv, "Flushing free list to disk");
			if ((ret = __memp_fput(mpf, meta, 0)) != 0)
				goto err;
			meta = NULL;
			if ((ret = __db_sync(dbp)) != 0)
				goto err;
			pgno = PGNO_BASE_MD;
			if ((ret = __memp_fget(mpf, &pgno, 0, &meta)) != 0)
				goto err;
			meta->free = last_pgno;
			if ((ret = __memp_fput(mpf, meta, DB_MPOOL_DIRTY)) != 0)
				goto err;
			meta = NULL;
		}

next:
		/*
		 * If we get here, either we have processed the list
		 * or the db file has been deleted or could not be opened.
		 */
		if (ctxn != NULL &&
		    (t_ret = __txn_abort(ctxn)) != 0 && ret == 0)
			ret = t_ret;

		if (dbp_created &&
		    (t_ret = __db_close(dbp, txn, DB_NOSYNC)) != 0 && ret == 0)
			ret = t_ret;
		dbp = NULL;
		if (state != LIMBO_PREPARE && state != LIMBO_TIMESTAMP) {
			__os_free(dbenv, elp->u.p.fname);
			__os_free(dbenv, elp->u.p.pgno_array);
		}
		if (ret == ENOENT)
			ret = 0;
		else if (ret != 0)
			goto err;
	}

err:	if (meta != NULL)
		(void)__memp_fput(mpf, meta, 0);
	return (ret);
}

/*
 * __db_limbo_fix --
 *	Process a single limbo entry which describes all the page allocations
 * for a single file.
 */
static int
__db_limbo_fix(dbp, txn, ctxn, elp, lastp, meta, state)
	DB *dbp;
	DB_TXN *txn;
	DB_TXN *ctxn;
	DB_TXNLIST *elp;
	db_pgno_t *lastp;
	DBMETA *meta;
	db_limbo_state state;
{
	DBC *dbc;
	DBT ldbt;
	DB_MPOOLFILE *mpf;
	DB_ENV *dbenv;
	PAGE *freep, *pagep;
	db_pgno_t next, pgno;
	u_int32_t i;
	extern int gbl_cmptxn_inherit_locks;
	int put_page, ret, t_ret, inherit;

	/*
	 * Loop through the entries for this txnlist element and
	 * either link them into the free list or write a compensating
	 * record for each.
	 */
	inherit = (gbl_cmptxn_inherit_locks && ctxn && txn);
	dbc = NULL;
	mpf = dbp->mpf;
	dbenv = dbp->dbenv;
	put_page = ret = 0;

	if (inherit && (ret = __db_lock_move(dbenv, elp->u.p.uid, 0, 
			DB_LOCK_WRITE, ctxn, txn)) != 0) {
		goto err;
	}

	for (i = 0; i < elp->u.p.nentries; i++) {
		pgno = elp->u.p.pgno_array[i];

		if (pgno == PGNO_INVALID)
			continue;

		if ((ret =
		    __memp_fget(mpf, &pgno, DB_MPOOL_CREATE, &pagep)) != 0) {
			if (ret != ENOSPC)
				goto err;
			continue;
		}
		put_page = 1;

		if (state == LIMBO_COMPENSATE || IS_ZERO_LSN(LSN(pagep))) {
			if (ctxn == NULL) {
				/*
				 * If this is a fatal recovery which
				 * spans a previous crash this page may
				 * be on the free list already.
				 */
				for (next = *lastp; next != 0; ) {
					if (next == pgno)
						break;
					if ((ret = __memp_fget(mpf,
					    &next, 0, &freep)) != 0)
						goto err;
					next = NEXT_PGNO(freep);
					if ((ret =
					    __memp_fput(mpf, freep, 0)) != 0)
						goto err;
				}

				if (next != pgno) {
					P_INIT(pagep, dbp->pgsize, pgno,
					    PGNO_INVALID, *lastp, 0, P_INVALID);
					/* Make the lsn non-zero but generic. */
					INIT_LSN(LSN(pagep));
					*lastp = pgno;
				}
			} else if (state == LIMBO_COMPENSATE) {
				/*
				 * Generate a log record for what we did
				 * on the LIMBO_TIMESTAMP pass.  All pages
				 * here are free so P_OVERHEAD is sufficent.
				 */
				ZERO_LSN(pagep->lsn);
				memset(&ldbt, 0, sizeof(ldbt));
				ldbt.data = pagep;
				ldbt.size = P_OVERHEAD(dbp);
				if ((ret = __db_pg_new_log(dbp, ctxn,
				     &LSN(meta), 0, pagep->pgno,
				     &LSN(meta), PGNO_BASE_MD,
				     &ldbt, pagep->next_pgno)) != 0)
					goto err;
			} else {

				if (inherit && (ret = __db_lock_move(dbenv, 
						elp->u.p.uid, pgno, DB_LOCK_WRITE, ctxn, 
						txn)) != 0) {
					goto err;
				}

				if (dbc == NULL && (ret =
				    __db_cursor(dbp, ctxn, &dbc, 0)) != 0)
						goto err;
				/*
				 * If the dbp is compensating (because we
				 * opened it), the dbc will automatically be
				 * marked compensating, but in case we didn't
				 * do the open, we have to mark it explicitly.
				 */
				F_SET(dbc, DBC_COMPENSATE);
				ret = __db_free(dbc, pagep);
				put_page = 0;
				/*
				 * On any error, we hope that the error was
				 * caused due to running out of space, and we
				 * switch modes, doing the processing where we
				 * sync out files instead of doing compensating
				 * transactions.  If this was a real error and
				 * not out of space, we assume that some other
				 * call will fail real soon.
				 */
				if (ret != 0) {
					/* Assume that this is out of space. */
					(void)__db_c_close(dbc);
					dbc = NULL;
					goto err;
				}
			}
		}
		else
			elp->u.p.pgno_array[i] = PGNO_INVALID;

		if (put_page == 1) {
			ret = __memp_fput(mpf, pagep, DB_MPOOL_DIRTY);
			put_page = 0;
		}
		if (ret != 0)
			goto err;
	}

err:	if (put_page &&
	    (t_ret = __memp_fput(mpf, pagep, DB_MPOOL_DIRTY)) != 0 && ret == 0)
		ret = t_ret;
	if (dbc != NULL && (t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

static int
__db_limbo_prepare(dbp, txn, elp)
	DB *dbp;
	DB_TXN *txn;
	DB_TXNLIST *elp;
{
	DB_LSN lsn;
	DB_MPOOLFILE *mpf;
	PAGE *pagep;
	db_pgno_t pgno;
	u_int32_t i;
	int ret, t_ret;

	/*
	 * Loop through the entries for this txnlist element and
	 * output a prepare record for them.
	 */
	pagep = NULL;
	ret = 0;
	mpf = dbp->mpf;

	for (i = 0; i < elp->u.p.nentries; i++) {
		pgno = elp->u.p.pgno_array[i];

		if ((ret =
		    __memp_fget(mpf, &pgno, DB_MPOOL_CREATE, &pagep)) != 0) {
			if (ret != ENOSPC)
				return (ret);
			continue;
		}

		if (IS_ZERO_LSN(LSN(pagep)))
			ret = __db_pg_prepare_log(dbp, txn, &lsn, 0, pgno);

		if ((t_ret = __memp_fput(mpf, pagep, 0)) != 0 && ret == 0)
			ret = t_ret;

		if (ret != 0)
			return (ret);
	}

	return (0);
}

#define	DB_TXNLIST_MAX_PGNO	8		/* A nice even number. */

/*
 * __db_txnlist_pgnoadd --
 *	Find the txnlist entry for a file and add this pgno, or add the list
 *	entry for the file and then add the pgno.
 */
static int
__db_txnlist_pgnoadd(dbenv, hp, fileid, uid, fname, pgno)
	DB_ENV *dbenv;
	DB_TXNHEAD *hp;
	int32_t fileid;
	u_int8_t uid[DB_FILE_ID_LEN];
	char *fname;
	db_pgno_t pgno;
{
	DB_TXNLIST *elp;
	size_t len;
	u_int32_t hash;
	int ret;

	elp = NULL;

	if (__db_txnlist_find_internal(dbenv, hp,
	    TXNLIST_PGNO, 0, uid, &elp, 0) != 0) {
		if ((ret =
		    __os_malloc(dbenv, sizeof(DB_TXNLIST), &elp)) != 0)
			goto err;
		memcpy(&hash, uid, sizeof(hash));
		LIST_INSERT_HEAD(
		    &hp->head[DB_TXNLIST_MASK(hp, hash)], elp, links);
		elp->u.p.fileid = fileid;
		memcpy(elp->u.p.uid, uid, DB_FILE_ID_LEN);

		len = strlen(fname) + 1;
		if ((ret = __os_malloc(dbenv, len, &elp->u.p.fname)) != 0)
			goto err;
		memcpy(elp->u.p.fname, fname, len);

		elp->u.p.maxentry = 0;
		elp->u.p.locked = 0;
		elp->type = TXNLIST_PGNO;
		if ((ret = __os_malloc(dbenv,
		    8 * sizeof(db_pgno_t), &elp->u.p.pgno_array)) != 0)
			goto err;
		elp->u.p.maxentry = DB_TXNLIST_MAX_PGNO;
		elp->u.p.nentries = 0;
	} else if (elp->u.p.nentries == elp->u.p.maxentry) {
		elp->u.p.maxentry <<= 1;
		if ((ret = __os_realloc(dbenv, elp->u.p.maxentry *
		    sizeof(db_pgno_t), &elp->u.p.pgno_array)) != 0)
			goto err;
	}

	elp->u.p.pgno_array[elp->u.p.nentries++] = pgno;

	return (0);

err:	__db_txnlist_end(dbenv, hp);
	return (ret);
}

#ifdef HAVE_REPLICATION
/*
 * __db_default_getpgnos --
 *	Fill in default getpgnos information for an application-specific
 * log record.
 *
 * PUBLIC: int __db_default_getpgnos __P((DB_ENV *, DB_LSN *lsnp, void *));
 */
int
__db_default_getpgnos(dbenv, lsnp, summary)
	DB_ENV *dbenv;
	DB_LSN *lsnp;
	void *summary;
{
	TXN_RECS *t;
	int ret;

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
#endif

#ifdef DEBUG
/*
 * __db_txnlist_print --
 *	Print out the transaction list.
 *
 * PUBLIC: void __db_txnlist_print __P((void *));
 */
void
__db_txnlist_print(listp)
	void *listp;
{
	DB_TXNHEAD *hp;
	DB_TXNLIST *p;
	u_int32_t i;
	char *txntype;

	hp = (DB_TXNHEAD *)listp;

	logmsg(LOGMSG_USER, "Maxid: %lu Generation: %lu\n",
	    (u_long)hp->maxid, (u_long)hp->generation);
	for (i = 0; i < hp->nslots; i++)
		for (p = LIST_FIRST(&hp->head[i]);
		    p != NULL; p = LIST_NEXT(p, links)) {
			if (p->type != TXNLIST_TXNID) {
				logmsg(LOGMSG_ERROR, "Unrecognized type: %d\n", p->type);
				continue;
			}
			switch (p->u.t.status) {
			case TXN_OK:
				txntype = "OK";
				break;
			case TXN_COMMIT:
				txntype = "commit";
				break;
			case TXN_PREPARE:
				txntype = "prepare";
				break;
			case TXN_ABORT:
				txntype = "abort";
				break;
			case TXN_NOTFOUND:
				txntype = "notfound";
				break;
			case TXN_IGNORE:
				txntype = "ignore";
				break;
			case TXN_EXPECTED:
				txntype = "expected";
				break;
			case TXN_UNEXPECTED:
				txntype = "unexpected";
				break;
			default:
				txntype = "UNKNOWN";
				break;
			}
			logmsg(LOGMSG_USER, "TXNID: %lx(%lu): %s\n",
			    (u_long)p->u.t.txnid,
			    (u_long)p->u.t.generation, txntype);
		}
}
#endif
