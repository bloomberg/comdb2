#include "build/db_config.h"
#include "list.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <plhash.h>
#include <assert.h>
#include <string.h>
#include <strings.h>
#include "build/db.h"
#include "build/db_int.h"
#include "dbinc/db_swap.h"
#include "dbinc/printlog_hooks.h"
#include "flibc.h"
#include <alloca.h>
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/txn.h"
#include "dbinc/btree.h"
#include "dbinc_auto/dbreg_auto.h"
#include "dbinc_auto/btree_auto.h"
#include "dbinc_auto/db_auto.h"

/* Include a reference to the start hook. */
printlog_start_t *printlog_start_hook;

/* Include a reference to the end hook. */
printlog_end_t *printlog_end_hook;

/* Character to end a line. */
char printlog_endline;

/* Sanity check the state of this. */
enum {
	DBPRINTLOG_UNINITED = 0,
	DBPRINTLOG_STARTED_LOG = 1,
	DBPRINTLOG_COMPLETED_LOG = 2
};

/* Global enable switch. */
static int enabled = 1;

/* Struct to hold our outstanding dbregs. */
typedef struct trak_dbreg {
	int32_t fileid;
	char *name;
} trak_dbreg_t;

/* Struct to hold outstanding transactions. */
typedef struct trak_txn {
	u_int32_t txnid;
	unsigned long long possible_genid;
	int possible_operation;
	int possible_fileid;
	int possible_pgno;
	int possible_index;
} trak_txn_t;

/* Struct to hold all of the current information. */
typedef struct curlog {
	int log_state;
	u_int32_t lsn_file;
	u_int32_t lsn_offset;
	u_int32_t type;
	u_int32_t txnid;
	u_int32_t prev_file;
	u_int32_t prev_offset;
	void *argp;
} curlog_t;

/* Keep track of our state when sniffing out genids. */
enum {
	NOOP = 0, DATA_ADD = 1, DATA_DEL = 2, INDEX_ADD = 3, INDEX_DEL = 4
};

/* A copy of important data from our current log. */
static curlog_t curlog = { 0 };

/* Typedef a function which prints these. */
typedef int (*log_print_f) (curlog_t * c);

/* Array of log_print functions. */
static log_print_f *l_print_f = NULL;

/* The maximum registered log-handler. */
static int max_log_print = 0;

/* Register a function which will print comdb2-specific info about a log. */
static inline int
comdb2_register_print(u_int32_t type, log_print_f f)
{
	/* Allocate a huge block the first time it's run. */
	if (NULL == l_print_f) {
		/* This should cover all known record types today. */
		l_print_f = calloc(11000, sizeof(*l_print_f));

		/* Set max log print. */
		max_log_print = 11000;
	}

	/* Resize if we've grown beyond the current maximum. */
	if (type >= max_log_print) {
		log_print_f *tmp_l;

		/* Allocate a new area. */
		tmp_l = calloc(2 * type, sizeof(*l_print_f));

		/* Copy. */
		memcpy(tmp_l, l_print_f, max_log_print * sizeof(*l_print_f));

		/* Set new max. */
		max_log_print = 2 * type;

		/* Free the old log_printf. */
		free(l_print_f);

		/* Set new l_print_f. */
		l_print_f = tmp_l;
	}

	/* Set the handler. */
	l_print_f[type] = f;

	return 0;
}

/* Accessor for the txn hash. */
static inline hash_t *
txn_hash(void)
{
	static hash_t *h = NULL;

	/* Create the first time. */
	if( NULL == h ) {
		h = hash_init( sizeof( int32_t ) );
	}

	/* Return static object. */
	return h;
}

/* Accessor for the dbreg hash. */
static inline hash_t *
dbreg_hash(void)
{
	static hash_t *h = NULL;

	/* Create the first time. */
	if (NULL == h) {
		h = hash_init(sizeof(int32_t));
	}

	/* Return static object. */
	return h;
}

/* Reset txn state. */
static inline void
txn_reset(trak_txn_t * txn)
{
	/* Reset my operation. */
	txn->possible_operation = NOOP;

	/* Reset possible genid information. */
	txn->possible_genid = 0;

	/* Reset my fileid. */
	txn->possible_fileid = -1;

	/* Reset my pgno. */
	txn->possible_pgno = -1;

	/* Reset my index. */
	txn->possible_index = -1;
}

/* Retrieve/Allocate memory for this txn. */
static inline trak_txn_t *
txn_retrieve(u_int32_t txnid)
{
	hash_t *h = txn_hash();
	trak_txn_t *txn;

	/* Find this transaction. */
	if ((txn = hash_find(h, &txnid)) != NULL) {
		return txn;
	}

	/* Allocate a new txnid. */
	txn = malloc(sizeof(*txn));

	/* Set my txnid. */
	txn->txnid = txnid;

	/* Reset txn state. */
	txn_reset(txn);

	/* Add to hash. */
	hash_add(h, txn);

	/* Return it. */
	return txn;
}

/* Disable the comdb2-specific logic (to save time, possibly). */
int
comdb2_disable_printlog(void)
{
	enabled = 0;
	return 0;
}

/* Cleanup routine which deletes the txn state info from a hash. */
static int
comdb2_txn_regop_print(curlog_t * c)
{
	__txn_regop_args *argp = (__txn_regop_args *) c->argp;
	unsigned long long commit_context = 0;
	hash_t *h = txn_hash();
	trak_txn_t *txn;

	/* Find this transaction. */
	if ((txn = hash_find(h, &c->txnid)) != NULL) {
		/* Delete from hash. */
		hash_del(h, txn);

		/* Free the memory. */
		free(txn);

	}

	if (argp->locks.size >= 8) {
		char *p = &((char *)argp->locks.data)[argp->locks.size - 8];

		memcpy(&commit_context, p, 8);
	}
	printf("Commit context: 0x%llx\n", commit_context);

	return 0;
}

/* 'dbreg_register' print function. */
static int
comdb2_dbreg_register_print(curlog_t * c)
{
	__dbreg_register_args *argp = (__dbreg_register_args *) c->argp;
	hash_t *h = dbreg_hash();
	trak_dbreg_t *dbreg;

	/* Find the id in the hash. */
	if (NULL == (dbreg = hash_find(h, &argp->fileid))) {
		/* Allocate it. */
		dbreg = (trak_dbreg_t *) calloc(1, sizeof(*dbreg));

		/* Set the id. */
		dbreg->fileid = argp->fileid;

		/* Add this to my hash. */
		hash_add(h, dbreg);
	}

	/* If there's already a filename, free it. */
	if (dbreg->name)
		free(dbreg->name);

	/* Allocate. */
	dbreg->name = malloc(argp->name.size + 1);

	/* Copy. */
	memcpy(dbreg->name, argp->name.data, argp->name.size);

	/* Null-terminate. */
	dbreg->name[argp->name.size] = '\0';

	/* Print a message. */
	printf("\tregistered '%s' as fileid %d.%c", dbreg->name, dbreg->fileid,
	    printlog_endline);

	return 0;
}

/* 'repl' print function. */
static int
comdb2_bam_repl_print(curlog_t * c)
{
	__bam_repl_args *argp = (__bam_repl_args *) c->argp;
	char data[32], *btree;
	hash_t *h = dbreg_hash();
	trak_dbreg_t *dbreg;

	/* Don't know the file, so create substitute text. */
	if (NULL == (dbreg = hash_find(h, &argp->fileid))) {
		snprintf(data, sizeof(data), "fileid-%d", argp->fileid);
		btree = data;
	}
	/* We have a real name. */
	else {
		btree = dbreg->name;
	}

	printf("\tupdate btree '%s' page %d index %d%c", btree, argp->pgno,
	    argp->indx, printlog_endline);

	return 0;
}

/* 'addrem' print function. */
static int
comdb2_db_addrem_print(curlog_t * c)
{
	__db_addrem_args *argp = (__db_addrem_args *) c->argp;
	unsigned long long genid_raw = 0, genid;
	hash_t *h = dbreg_hash();
	trak_dbreg_t *dbreg;
	trak_txn_t *txn;
	char data[32], *btree, *operation, *p, *gn;

	/* Don't know the file, so create substitute text. */
	if (NULL == (dbreg = hash_find(h, &argp->fileid))) {
		snprintf(data, sizeof(data), "fileid-%d", argp->fileid);
		btree = data;
	}
	/* We have a real name. */
	else {
		btree = dbreg->name;
	}

	/* See if this is a 'delete'. */
	if (DB_REM_DUP == argp->opcode) {
		operation = "delete from";
	}
	/* See if this is an 'add'. */
	else if (DB_ADD_DUP == argp->opcode) {
		operation = "add to";
	}
	/* Shouldn't get here. */
	else {
		operation = "unknown operation??";
	}

	/* Print message. */
	printf("\t%s btree '%s' page %d index %d%c", operation, btree,
	    argp->pgno, argp->indx, printlog_endline);


	/* Grab my txn object. */
	txn = txn_retrieve(argp->txnid->txnid);

	/* Grab a pointer to the final '.'. */
	if (!(p = strrchr(btree, '.'))) {
		return 0;
	}

	/*
	 * I don't know if this page is a leaf-node or an internal
	 * node.  To make this bulletproof, latch the probable-genid
	 * at the first add_dup, and then print it only if there is a
	 * sequential add_dup at the next higher index.
	 */

	/* Add-to datafile logic. */
	if (DB_ADD_DUP == argp->opcode &&
	    (0 == memcmp(p, ".datas", 6) || 0 == memcmp(p, ".blobs", 6))) {
		/* If the last operation wasn't a data-add, zero my state. */
		if (DATA_ADD != txn->possible_operation) {
			txn_reset(txn);
		}

		/* All even indexes are the keys. */
		if (0 == argp->indx % 2 && 8 == argp->dbt.size) {
			/* Set the possible operation. */
			txn->possible_operation = DATA_ADD;

			/* Copy the possible genid. */
			memcpy(&txn->possible_genid, argp->dbt.data, 8);

			/* Copy the fileid. */
			txn->possible_fileid = argp->fileid;

			/* Set the pgno. */
			txn->possible_pgno = argp->pgno;

			/* Set the index. */
			txn->possible_index = argp->indx;
		}

		/*
		 * This is the subsequent 'data' entry, the previous
		 * was the key.
		 */
		else if (DATA_ADD == txn->possible_operation &&
		    1 == argp->indx % 2 && txn->possible_genid &&
		    txn->possible_fileid == argp->fileid &&
		    txn->possible_pgno == argp->pgno &&
		    txn->possible_index + 1 == argp->indx) {
			/* Copy what I'm going to print. */
			genid_raw = txn->possible_genid;

			/* Reset my txn. */
			txn_reset(txn);
		}

		/* This was a miss- zero out my state. */
		else {
			txn_reset(txn);
		}
	}

	/* Delete from datafile logic. */
	if (DB_REM_DUP == argp->opcode &&
	    (0 == memcmp(p, ".datas", 6) || 0 == memcmp(p, ".blobs", 6))) {
		/* If the last operation wasn't a data-delete, zero my state. */
		if (DATA_DEL != txn->possible_operation) {
			txn_reset(txn);
		}

		/* We're deleting something- latch our possible genid. */
		if (0 == txn->possible_genid && 0 == argp->indx % 2 && 12 ==
		    argp->hdr.size) {
			/* Reset the possible operation. */
			txn->possible_operation = DATA_DEL;

			/* Get a pointer to the possible genid. */
			gn = &((char *)argp->hdr.data)[3];

			/* Copy to possible_del_genid. */
			memcpy(&txn->possible_genid, gn, 8);

			/* Grab the fileid. */
			txn->possible_fileid = argp->fileid;

			/* Grab the pgno. */
			txn->possible_pgno = argp->pgno;

			/* Grab the index. */
			txn->possible_index = argp->indx;
		}
		/*
		 * Second delete at the same fileid, pgno & index: we
		 * have a genid.
		 */
		else if (DATA_DEL == txn->possible_operation &&
		    txn->possible_genid && txn->possible_fileid == argp->fileid
		    && txn->possible_pgno == argp->pgno &&
		    txn->possible_index == argp->indx) {
			/* Copy what I'm going to print. */
			genid_raw = txn->possible_genid;
			txn_reset(txn);
		} else {
			txn_reset(txn);
		}
	}

	/* For index files, the second add or delete will contain the genid. */
	if (DB_ADD_DUP == argp->opcode && 0 == memcmp(p, ".index", 6)) {
		if (INDEX_ADD != txn->possible_operation) {
			txn_reset(txn);
		}

		/* All even indexes are the keys. */
		if (0 == argp->indx % 2) {
			/* Set the possible operation. */
			txn->possible_operation = INDEX_ADD;

			/* Copy the fileid. */
			txn->possible_fileid = argp->fileid;

			/* Set the pgno. */
			txn->possible_pgno = argp->pgno;

			/* Set the index. */
			txn->possible_index = argp->indx;
		}

		/* Subsequent add at the next index on the page. */
		else if (INDEX_ADD == txn->possible_operation &&
		    1 == argp->indx % 2 && txn->possible_fileid == argp->fileid
		    && txn->possible_pgno == argp->pgno &&
		    txn->possible_index + 1 == argp->indx &&
		    argp->dbt.size >= 8) {
			/* The genid is the first 8 bytes of the payload. */
			memcpy(&genid_raw, argp->dbt.data, 8);
			txn_reset(txn);
		} else {
			txn_reset(txn);
		}
	}

	/* For index files, the second add or delete will contain the genid. */
	if (DB_REM_DUP == argp->opcode && 0 == memcmp(p, ".index", 6)) {
		if (INDEX_DEL != txn->possible_operation) {
			txn_reset(txn);
		}

		/* We're deleting something- grab info about this addrem. */
		if (NOOP == txn->possible_operation) {
			/* Set operation. */
			txn->possible_operation = INDEX_DEL;

			/* Grab the fileid. */
			txn->possible_fileid = argp->fileid;

			/* Grab the pgno. */
			txn->possible_pgno = argp->pgno;

			/* Grab the index. */
			txn->possible_index = argp->indx;
		} else if (INDEX_DEL == txn->possible_operation &&
		    txn->possible_fileid == argp->fileid &&
		    txn->possible_pgno == argp->pgno &&
		    txn->possible_index == argp->indx && argp->hdr.size >= 12) {
			/* Get a pointer to the possible genid. */
			gn = &((char *)argp->hdr.data)[3];

			/* Copy to genid_raw. */
			memcpy(&genid_raw, gn, 8);

			/* Reset state. */
			txn_reset(txn);
		} else {
			txn_reset(txn);
		}
	}

	/* Print the genid if we have one. */
	if (genid_raw) {
		genid = flibc_ntohll(genid_raw);
		printf("\tgenid: %llu (0x%llx)%c", genid, genid,
		    printlog_endline);
	}

	return 0;
}

/* Called at the start of a new logfile. */
static int
comdb2_printlog_start_log(u_int32_t file, u_int32_t offset,
    u_int32_t type, u_int32_t txnid, u_int32_t prev_file,
    u_int32_t prev_offset, void *argp)
{
	/* Short circuit immediately if not enabled. */
	if (!enabled)
		return 0;

	/* Sanity check. */
	assert(curlog.log_state == DBPRINTLOG_UNINITED ||
	    curlog.log_state == DBPRINTLOG_COMPLETED_LOG);

	/* Zap the logfile. */
	bzero(&curlog, sizeof(curlog));

	/* Set our file and offset. */
	curlog.lsn_file = file;
	curlog.lsn_offset = offset;

	/* Set our 'type'. */
	curlog.type = type;

	/* Set our 'txnid'. */
	curlog.txnid = txnid;

	/* Set the prev file and offset. */
	curlog.prev_file = prev_file;
	curlog.prev_offset = prev_offset;

	/* Pointer to the argp record. */
	curlog.argp = argp;

	/* Update the state. */
	curlog.log_state = DBPRINTLOG_STARTED_LOG;

	return 0;
}

/* Called when we're through processing a logfile. */
int
comdb2_printlog_end_log(int file, int offset)
{
	/* Short circuit immediately if not enabled. */
	if (!enabled)
		return 0;

	assert(curlog.log_state == DBPRINTLOG_STARTED_LOG);

	/* Sanity check the lsn. */
	assert(curlog.lsn_file == file);
	assert(curlog.lsn_offset == offset);

	/* Print final information. */
	if (curlog.type <= max_log_print && NULL !=l_print_f[curlog.type]) {
		(*l_print_f[curlog.type]) (&curlog);
	}

	/* Update my state. */
	curlog.log_state = DBPRINTLOG_COMPLETED_LOG;
	return 0;
}

/* Initialize comdb2-specific print functions. */
int
comdb2_printlog_initialize(void)
{
	static int once = 0;

	/* If it's already been called, print a nasty message. */
	if (once) {
		printf("%s: called more than once?\n", __func__);
		return -1;
	}

	/* Set the printlog start hook. */
	printlog_start_hook = comdb2_printlog_start_log;

	/* Set the printlog end hook. */
	printlog_end_hook = comdb2_printlog_end_log;

	/* Log handlers. */
	comdb2_register_print(DB___dbreg_register, comdb2_dbreg_register_print);
	comdb2_register_print(DB___db_addrem, comdb2_db_addrem_print);
	comdb2_register_print(DB___bam_repl, comdb2_bam_repl_print);
	comdb2_register_print(DB___txn_regop, comdb2_txn_regop_print);
	comdb2_register_print(DB___txn_regop_rowlocks, comdb2_txn_regop_print);

	/* Set my already called flag. */
	once = 1;

	return 0;
}
