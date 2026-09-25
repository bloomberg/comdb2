/*
 * Classify direct schema-change converter transactions.
 *
 * A rebuilding schema change registers each private replacement file against
 * its exact build identity.  The base converter marks its transactions with
 * the same identity.  Physical writes then establish two independent facts:
 * whether the transaction wrote one of its own private files, and whether it
 * also wrote an unsafe public user file.
 *
 * This module only records classification state.  It does not change commit
 * records or commit-map behavior.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: txn_sc_skip.c,v 1.0 2026/09/15 00:00:00 comdb2 Exp $";
#endif

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_shash.h"
#include "dbinc/txn.h"
#include "logmsg.h"
#include "comdb2_atomic.h"

static u_int64_t sc_direct_copy_txns_marked = 0;
static u_int64_t sc_direct_copy_txns_matched = 0;
static u_int64_t sc_direct_copy_txns_unsafe = 0;

int
__sc_private_file_registry_init(dbenv)
	DB_ENV *dbenv;
{
	SC_PRIVATE_FILE_REGISTRY *reg;
	int ret;

	if ((ret = __os_calloc(dbenv, 1,
	    sizeof(SC_PRIVATE_FILE_REGISTRY), &reg)) != 0)
		goto err;

	reg->files = hash_init_o(offsetof(SC_PRIVATE_FILE, fileid),
	    DB_FILE_ID_LEN);
	if (reg->files == NULL) {
		__os_free(dbenv, reg);
		ret = ENOMEM;
		goto err;
	}

	Pthread_mutex_init(&reg->lk, NULL);
	dbenv->sc_private_files = reg;
	return (0);

err:
	logmsg(LOGMSG_ERROR,
	    "Failed to initialize schema-change replacement-file registry\n");
	return (ret);
}

static int
free_sc_private_file(void *obj, void *arg)
{
	__os_free((DB_ENV *)arg, obj);
	return (0);
}

int
__sc_private_file_registry_destroy(dbenv)
	DB_ENV *dbenv;
{
	SC_PRIVATE_FILE_REGISTRY *reg = dbenv->sc_private_files;

	if (reg == NULL)
		return (0);

	hash_for(reg->files, &free_sc_private_file, (void *)dbenv);
	hash_clear(reg->files);
	hash_free(reg->files);
	Pthread_mutex_destroy(&reg->lk);
	__os_free(dbenv, reg);
	dbenv->sc_private_files = NULL;
	return (0);
}

int
__sc_private_file_register(dbenv, fileid, build_id)
	DB_ENV *dbenv;
	const u_int8_t *fileid;
	const sc_build_id_t *build_id;
{
	SC_PRIVATE_FILE_REGISTRY *reg = dbenv->sc_private_files;
	SC_PRIVATE_FILE *file;
	int ret = 0;

	if (reg == NULL || fileid == NULL || sc_build_id_is_zero(build_id))
		return (EINVAL);

	Pthread_mutex_lock(&reg->lk);
	file = hash_find(reg->files, fileid);
	if (file != NULL) {
		if (!sc_build_id_equal(&file->build_id, build_id)) {
			logmsg(LOGMSG_ERROR,
			    "%s: file already belongs to another schema-change build\n",
			    __func__);
			ret = EEXIST;
		}
		goto done;
	}

	if ((ret = __os_calloc(dbenv, 1, sizeof(SC_PRIVATE_FILE), &file)) != 0) {
		ret = ENOMEM;
		goto done;
	}

	memcpy(file->fileid, fileid, DB_FILE_ID_LEN);
	file->build_id = *build_id;
	hash_add(reg->files, file);

done:
	Pthread_mutex_unlock(&reg->lk);
	return (ret);
}

int
__sc_private_file_lookup(dbenv, fileid, build_id_out)
	DB_ENV *dbenv;
	const u_int8_t *fileid;
	sc_build_id_t *build_id_out;
{
	SC_PRIVATE_FILE_REGISTRY *reg = dbenv->sc_private_files;
	SC_PRIVATE_FILE *file;
	int ret = DB_NOTFOUND;

	if (reg == NULL || fileid == NULL)
		return (DB_NOTFOUND);

	Pthread_mutex_lock(&reg->lk);
	file = hash_find(reg->files, fileid);
	if (file != NULL) {
		*build_id_out = file->build_id;
		ret = 0;
	}
	Pthread_mutex_unlock(&reg->lk);
	return (ret);
}

struct sc_private_find_arg {
	const sc_build_id_t *build_id;
	SC_PRIVATE_FILE *found;
};

static int
find_one_build_file(void *obj, void *arg)
{
	SC_PRIVATE_FILE *file = obj;
	struct sc_private_find_arg *find = arg;

	if (!sc_build_id_equal(&file->build_id, find->build_id))
		return (0);

	find->found = file;
	return (1);
}

int
__sc_private_file_unregister_build(dbenv, build_id)
	DB_ENV *dbenv;
	const sc_build_id_t *build_id;
{
	SC_PRIVATE_FILE_REGISTRY *reg = dbenv->sc_private_files;
	struct sc_private_find_arg find;

	if (reg == NULL || sc_build_id_is_zero(build_id))
		return (0);

	Pthread_mutex_lock(&reg->lk);
	find.build_id = build_id;
	for (;;) {
		find.found = NULL;
		hash_for(reg->files, &find_one_build_file, &find);
		if (find.found == NULL)
			break;
		hash_del(reg->files, find.found);
		__os_free(dbenv, find.found);
	}
	Pthread_mutex_unlock(&reg->lk);
	return (0);
}

void
__sc_private_registry_note_failure(dbenv)
	DB_ENV *dbenv;
{
	SC_PRIVATE_FILE_REGISTRY *reg = dbenv->sc_private_files;

	if (reg == NULL)
		return;

	Pthread_mutex_lock(&reg->lk);
	reg->failed_registrations++;
	Pthread_mutex_unlock(&reg->lk);
}

void
__sc_private_registry_stats(dbenv, available, nfiles, failures)
	DB_ENV *dbenv;
	int *available;
	u_int64_t *nfiles;
	u_int64_t *failures;
{
	SC_PRIVATE_FILE_REGISTRY *reg = dbenv->sc_private_files;

	if (reg == NULL) {
		*available = 0;
		*nfiles = 0;
		*failures = 0;
		return;
	}

	Pthread_mutex_lock(&reg->lk);
	*available = 1;
	*nfiles = (u_int64_t)hash_get_num_entries(reg->files);
	*failures = reg->failed_registrations;
	Pthread_mutex_unlock(&reg->lk);
}

void
__txn_set_sc_build(txnp, build_id)
	DB_TXN *txnp;
	const sc_build_id_t *build_id;
{
	if (txnp == NULL || sc_build_id_is_zero(build_id))
		return;

	if (txnp->parent != NULL) {
		static int warned = 0;

		if (!warned) {
			warned = 1;
			logmsg(LOGMSG_WARN,
			    "%s: converter transaction has a parent; "
			    "classification will not apply\n", __func__);
		}
		return;
	}

	txnp->sc_build_id = *build_id;
	txnp->sc_skip_commit_map = 0;
	txnp->sc_unsafe_public_write = 0;
	(void)ATOMIC_ADD64(sc_direct_copy_txns_marked, 1);
}

void
__txn_note_sc_file_write_int(txnp, dbp)
	DB_TXN *txnp;
	DB *dbp;
{
	sc_build_id_t file_build_id;

	if (__sc_private_file_lookup(txnp->mgrp->dbenv, dbp->fileid,
	    &file_build_id) != 0) {
		if (dbp->sc_is_user_file) {
			txnp->sc_unsafe_public_write = 1;
			(void)ATOMIC_ADD64(sc_direct_copy_txns_unsafe, 1);
		}
		return;
	}

	if (sc_build_id_equal(&file_build_id, &txnp->sc_build_id)) {
		if (!txnp->sc_skip_commit_map)
			(void)ATOMIC_ADD64(sc_direct_copy_txns_matched, 1);
		txnp->sc_skip_commit_map = 1;
	} else if (dbp->sc_is_user_file) {
		txnp->sc_unsafe_public_write = 1;
		(void)ATOMIC_ADD64(sc_direct_copy_txns_unsafe, 1);
	}
}

void
__sc_direct_copy_stats(marked, matched, unsafe)
	u_int64_t *marked;
	u_int64_t *matched;
	u_int64_t *unsafe;
{
	*marked = ATOMIC_LOAD64(sc_direct_copy_txns_marked);
	*matched = ATOMIC_LOAD64(sc_direct_copy_txns_matched);
	*unsafe = ATOMIC_LOAD64(sc_direct_copy_txns_unsafe);
}