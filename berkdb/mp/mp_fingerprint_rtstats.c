/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2026
 *	Bloomberg Finance L.P.  All rights reserved.
 */
#include "db_config.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "db_int.h"

#include "logmsg.h"
#include "sys_wrap.h"
#include "comdb2_atomic.h"

/*
 * Must match FINGERPRINTSZ (db/fingerprint.h). Kept as an independent
 * literal so berkdb/ never has to include db/ headers.
 */
#define FP_RTSTATS_KEYSZ 16

/*
 * Per-fingerprint runtime counters, gathered from low-level subsystems
 * (bufferpool page-ins today; other categories can be added as new fields
 * plus their own bb_berkdb_fingerprint_rtstats_bump_*() entry point). Deliberately
 * a separate structure/hash from db/db_fingerprint.c's gbl_fingerprint_hash:
 * a future write-side commit will need to attribute these on a machine
 * (e.g. a replication master applying a write schedule) that may never
 * have populated its own gbl_fingerprint_hash for that fingerprint.
 */
struct fingerprint_rtstats {
	unsigned char fingerprint[FP_RTSTATS_KEYSZ]; /* hash key, must be first */
	uint64_t n_pagein_read;    /* every successful __memp_fget_internal()
				    * return attributed to this fingerprint
				    * during SQL read execution (cache hit or
				    * miss) */
	uint64_t n_pagein_read_io; /* subset of n_pagein_read where the fetch
				    * required an actual disk read */
	uint64_t n_write_pagein_read;    /* same as n_pagein_read, but for page-ins
				    * generated while the master applies a write
				    * schedule for this fingerprint */
	uint64_t n_write_pagein_read_io; /* subset of n_write_pagein_read that
				    * required an actual disk read */
	uint64_t n_apply_pagein_read;    /* same, on a replicant redoing this
				    * fingerprint's writes from the log */
	uint64_t n_apply_pagein_read_io; /* subset that required a disk read */
	int has_main_entry;        /* did gbl_fingerprint_hash have this
				    * fingerprint when the entry was created?
				    * Diagnostic only, and never refreshed --
				    * the systable derives has_query_info from
				    * live membership instead. */
};

static hash_t *gbl_fingerprint_rtstats_hash;
static pthread_mutex_t gbl_fingerprint_rtstats_hash_mu = PTHREAD_MUTEX_INITIALIZER;

int gbl_fingerprint_rtstats_max_entries = 5000;

/*
 * Fallback bucket for runtime counters with no associated SQL fingerprint:
 * recovery, replication apply, internal bookkeeping, or fingerprinting
 * disabled globally. Zero-initialized static storage; never inserted into
 * gbl_fingerprint_rtstats_hash.
 */
static struct fingerprint_rtstats gbl_fingerprint_rtstats_nofingerprint;

static pthread_key_t fingerprint_rtstats_key;
/* Which counter pair bump_pagein() increments. Per-thread, not on the entry:
 * one entry is shared by all three roles at once, even on a single node. */
#define FP_RTSTATS_MODE_SQL	0
#define FP_RTSTATS_MODE_WRITE	1
#define FP_RTSTATS_MODE_APPLY	2
static pthread_key_t fingerprint_rtstats_mode_key;
static int fingerprint_rtstats_inited = 0;

void
bb_berkdb_fingerprint_rtstats_init(void)
{
	Pthread_key_create(&fingerprint_rtstats_key, NULL);
	Pthread_key_create(&fingerprint_rtstats_mode_key, NULL);

	Pthread_mutex_lock(&gbl_fingerprint_rtstats_hash_mu);
	if (gbl_fingerprint_rtstats_hash == NULL)
		gbl_fingerprint_rtstats_hash = hash_init(FP_RTSTATS_KEYSZ);
	Pthread_mutex_unlock(&gbl_fingerprint_rtstats_hash_mu);

	fingerprint_rtstats_inited = 1;
}

/* Find-or-create this fingerprint's entry and point the calling thread's TLS
 * at it, in the given mode. Shared by the three arming entry points below. */
static void
fingerprint_rtstats_set_internal(const unsigned char *fingerprint, size_t fplen,
    int has_main_entry, int mode)
{
	struct fingerprint_rtstats *t;

	if (!fingerprint_rtstats_inited || fingerprint == NULL || fplen != FP_RTSTATS_KEYSZ)
		return;

	/* Skip the lock on a no-op re-arm. Safe to cache: entries are never freed. */
	t = pthread_getspecific(fingerprint_rtstats_key);
	if (t != NULL && (int)(intptr_t)pthread_getspecific(fingerprint_rtstats_mode_key) == mode &&
	    memcmp(t->fingerprint, fingerprint, FP_RTSTATS_KEYSZ) == 0)
		return;

	Pthread_setspecific(fingerprint_rtstats_mode_key, (void *)(intptr_t)mode);

	Pthread_mutex_lock(&gbl_fingerprint_rtstats_hash_mu);
	if (gbl_fingerprint_rtstats_hash == NULL)
		gbl_fingerprint_rtstats_hash = hash_init(FP_RTSTATS_KEYSZ);

	t = hash_find(gbl_fingerprint_rtstats_hash, fingerprint);
	if (t == NULL) {
		if (hash_get_num_entries(gbl_fingerprint_rtstats_hash) >= gbl_fingerprint_rtstats_max_entries) {
			static int complain_once = 1;
			if (complain_once) {
				logmsg(LOGMSG_WARN,
				    "Stopped tracking fingerprint runtime stats, hit max #entries %d.\n",
				    gbl_fingerprint_rtstats_max_entries);
				complain_once = 0;
			}
			Pthread_mutex_unlock(&gbl_fingerprint_rtstats_hash_mu);
			Pthread_setspecific(fingerprint_rtstats_key, NULL);
			return;
		}

		t = calloc(1, sizeof(struct fingerprint_rtstats));
		if (t == NULL) {
			Pthread_mutex_unlock(&gbl_fingerprint_rtstats_hash_mu);
			Pthread_setspecific(fingerprint_rtstats_key, NULL);
			return;
		}
		memcpy(t->fingerprint, fingerprint, FP_RTSTATS_KEYSZ);
		t->has_main_entry = has_main_entry;
		hash_add(gbl_fingerprint_rtstats_hash, t);
	}
	Pthread_mutex_unlock(&gbl_fingerprint_rtstats_hash_mu);

	Pthread_setspecific(fingerprint_rtstats_key, t);
}

/* Read-side arm, once per statement: its fingerprint is known and cursor
 * traversal is about to begin. */
void
bb_berkdb_fingerprint_rtstats_set(const unsigned char *fingerprint, size_t fplen, int has_main_entry)
{
	fingerprint_rtstats_set_internal(fingerprint, fplen, has_main_entry, FP_RTSTATS_MODE_SQL);
}

/* Master write-side arm: the block-processor thread received an
 * OSQL_FINGERPRINT op and is about to apply that statement's write ops. */
void
bb_berkdb_fingerprint_rtstats_set_write(const unsigned char *fingerprint, size_t fplen, int has_main_entry)
{
	fingerprint_rtstats_set_internal(fingerprint, fplen, has_main_entry, FP_RTSTATS_MODE_WRITE);
}

/* Replicant apply-side arm, per tagged log record (rep_record.c). Callers pass
 * has_main_entry=0; the systable derives has_query_info live instead. */
void
bb_berkdb_fingerprint_rtstats_set_apply(const unsigned char *fingerprint, size_t fplen, int has_main_entry)
{
	fingerprint_rtstats_set_internal(fingerprint, fplen, has_main_entry, FP_RTSTATS_MODE_APPLY);
}

/* Called when the statement is done (or on error paths). */
void
bb_berkdb_fingerprint_rtstats_clear(void)
{
	if (!fingerprint_rtstats_inited)
		return;
	Pthread_setspecific(fingerprint_rtstats_key, NULL);
	Pthread_setspecific(fingerprint_rtstats_mode_key, (void *)(intptr_t)FP_RTSTATS_MODE_SQL);
}

/*
 * Called only from berkdb/mp/mp_fget.c, at each successful-return
 * point of __memp_fget_internal(). Lockless: only touches the per-entry
 * counters of whatever the TLS currently points to (or the static
 * NO-FINGERPRINT default). Atomic adds because many threads can share the
 * same fingerprint's stat entry concurrently.
 */
void
bb_berkdb_fingerprint_rtstats_bump_pagein(int did_io)
{
	struct fingerprint_rtstats *t = NULL;
	int mode = FP_RTSTATS_MODE_SQL;

	if (fingerprint_rtstats_inited) {
		t = pthread_getspecific(fingerprint_rtstats_key);
		mode = (int)(intptr_t)pthread_getspecific(fingerprint_rtstats_mode_key);
	}
	if (t == NULL)
		t = &gbl_fingerprint_rtstats_nofingerprint;

	switch (mode) {
	case FP_RTSTATS_MODE_WRITE:
		ATOMIC_ADD64(t->n_write_pagein_read, 1);
		if (did_io)
			ATOMIC_ADD64(t->n_write_pagein_read_io, 1);
		break;
	case FP_RTSTATS_MODE_APPLY:
		ATOMIC_ADD64(t->n_apply_pagein_read, 1);
		if (did_io)
			ATOMIC_ADD64(t->n_apply_pagein_read_io, 1);
		break;
	default:
		ATOMIC_ADD64(t->n_pagein_read, 1);
		if (did_io)
			ATOMIC_ADD64(t->n_pagein_read_io, 1);
		break;
	}
}

/* Writers bump these locklessly with ATOMIC_ADD64, so load them the same way. */
static void
fingerprint_rtstats_load_counts(const struct fingerprint_rtstats *t,
    uint64_t counts[BB_BERKDB_FP_RTSTATS_NCOUNTS])
{
	counts[0] = ATOMIC_LOAD64(t->n_pagein_read);
	counts[1] = ATOMIC_LOAD64(t->n_pagein_read_io);
	counts[2] = ATOMIC_LOAD64(t->n_write_pagein_read);
	counts[3] = ATOMIC_LOAD64(t->n_write_pagein_read_io);
	counts[4] = ATOMIC_LOAD64(t->n_apply_pagein_read);
	counts[5] = ATOMIC_LOAD64(t->n_apply_pagein_read_io);
}

/*
 * Snapshot read for reporting (e.g. the comdb2_fingerprints systable).
 * Returns 1 and fills in counts[] if an entry exists for this fingerprint,
 * 0 (with counts[] zeroed) otherwise.
 * Unlike bb_berkdb_fingerprint_rtstats_bump_pagein(), this takes the hash mutex --
 * it's called at query-time on the reporting path, not per-page-fetch.
 */
int
bb_berkdb_fingerprint_rtstats_get(const unsigned char *fingerprint, size_t fplen,
    uint64_t counts[BB_BERKDB_FP_RTSTATS_NCOUNTS])
{
	struct fingerprint_rtstats *t;
	int found = 0;

	memset(counts, 0, BB_BERKDB_FP_RTSTATS_NCOUNTS * sizeof(counts[0]));

	if (!fingerprint_rtstats_inited || fingerprint == NULL || fplen != FP_RTSTATS_KEYSZ)
		return 0;

	Pthread_mutex_lock(&gbl_fingerprint_rtstats_hash_mu);
	if (gbl_fingerprint_rtstats_hash != NULL) {
		t = hash_find(gbl_fingerprint_rtstats_hash, fingerprint);
		if (t != NULL) {
			fingerprint_rtstats_load_counts(t, counts);
			found = 1;
		}
	}
	Pthread_mutex_unlock(&gbl_fingerprint_rtstats_hash_mu);

	return found;
}

/* Enumerate every entry for reporting. fn runs under the hash mutex, so it must
 * not block or re-enter this subsystem. counts[] rather than a struct keeps the
 * layout out of the bdb/berkdb boundary. */
void
bb_berkdb_fingerprint_rtstats_foreach(bb_berkdb_fingerprint_rtstats_enum_fn fn, void *arg)
{
	struct fingerprint_rtstats *t;
	void *hash_cur;
	unsigned int hash_cur_buk;

	if (!fingerprint_rtstats_inited || fn == NULL)
		return;

	Pthread_mutex_lock(&gbl_fingerprint_rtstats_hash_mu);
	if (gbl_fingerprint_rtstats_hash != NULL) {
		t = hash_first(gbl_fingerprint_rtstats_hash, &hash_cur, &hash_cur_buk);
		while (t != NULL) {
			uint64_t counts[BB_BERKDB_FP_RTSTATS_NCOUNTS];
			fingerprint_rtstats_load_counts(t, counts);
			fn(t->fingerprint, counts, t->has_main_entry, arg);
			t = hash_next(gbl_fingerprint_rtstats_hash, &hash_cur, &hash_cur_buk);
		}
	}
	Pthread_mutex_unlock(&gbl_fingerprint_rtstats_hash_mu);
}
