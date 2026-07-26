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
 * plus their own bb_fingerprint_rtstats_bump_*() entry point). Deliberately
 * a separate structure/hash from db/db_fingerprint.c's gbl_fingerprint_hash:
 * a future write-side commit will need to attribute these on a machine
 * (e.g. a replication master applying a write schedule) that may never
 * have populated its own gbl_fingerprint_hash for that fingerprint.
 */
struct fingerprint_rtstats {
	unsigned char fingerprint[FP_RTSTATS_KEYSZ]; /* hash key, must be first */
	uint64_t n_pagein_read;    /* every successful __memp_fget_internal()
				    * return attributed to this fingerprint
				    * (cache hit or miss) */
	uint64_t n_pagein_read_io; /* subset of n_pagein_read where the fetch
				    * required an actual disk read */
	int has_main_entry;        /* 1 if gbl_fingerprint_hash already had an
				    * entry for this fingerprint when this
				    * stat entry was created (set once, at
				    * creation time, not refreshed). Lets a
				    * future write-side commit distinguish
				    * "this machine has full query info" from
				    * "we only know the fingerprint + counts".
				    * Unused by read-side logic. */
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
static int fingerprint_rtstats_inited = 0;

void
bb_fingerprint_rtstats_init(void)
{
	Pthread_key_create(&fingerprint_rtstats_key, NULL);

	Pthread_mutex_lock(&gbl_fingerprint_rtstats_hash_mu);
	if (gbl_fingerprint_rtstats_hash == NULL)
		gbl_fingerprint_rtstats_hash = hash_init(FP_RTSTATS_KEYSZ);
	Pthread_mutex_unlock(&gbl_fingerprint_rtstats_hash_mu);

	fingerprint_rtstats_inited = 1;
}

/*
 * Called once per statement, as soon as its fingerprint is known and
 * before cursor traversal begins. Finds-or-creates the stat entry for
 * this fingerprint and points the calling thread's TLS slot at it.
 */
void
bb_fingerprint_rtstats_set(const unsigned char *fingerprint, size_t fplen, int has_main_entry)
{
	struct fingerprint_rtstats *t;

	if (!fingerprint_rtstats_inited || fingerprint == NULL || fplen != FP_RTSTATS_KEYSZ)
		return;

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

/* Called when the statement is done (or on error paths). */
void
bb_fingerprint_rtstats_clear(void)
{
	if (!fingerprint_rtstats_inited)
		return;
	Pthread_setspecific(fingerprint_rtstats_key, NULL);
}

/*
 * Called only from berkdb/mp/mp_fget.c, at each successful-return
 * point of __memp_fget_internal(). Lockless: only touches the per-entry
 * counters of whatever the TLS currently points to (or the static
 * NO-FINGERPRINT default). Atomic adds because many threads can share the
 * same fingerprint's stat entry concurrently.
 */
void
bb_fingerprint_rtstats_bump_pagein(int did_io)
{
	struct fingerprint_rtstats *t = NULL;

	if (fingerprint_rtstats_inited)
		t = pthread_getspecific(fingerprint_rtstats_key);
	if (t == NULL)
		t = &gbl_fingerprint_rtstats_nofingerprint;

	ATOMIC_ADD64(t->n_pagein_read, 1);
	if (did_io)
		ATOMIC_ADD64(t->n_pagein_read_io, 1);
}

/*
 * Snapshot read for reporting (e.g. the comdb2_fingerprints systable).
 * Returns 1 and fills in *n_pagein_read / *n_pagein_read_io if an entry
 * exists for this fingerprint, 0 (with both outputs zeroed) otherwise.
 * Unlike bb_fingerprint_rtstats_bump_pagein(), this takes the hash mutex --
 * it's called at query-time on the reporting path, not per-page-fetch.
 */
int
bb_fingerprint_rtstats_get(const unsigned char *fingerprint, size_t fplen,
    uint64_t *n_pagein_read, uint64_t *n_pagein_read_io)
{
	struct fingerprint_rtstats *t;
	int found = 0;

	*n_pagein_read = 0;
	*n_pagein_read_io = 0;

	if (!fingerprint_rtstats_inited || fingerprint == NULL || fplen != FP_RTSTATS_KEYSZ)
		return 0;

	Pthread_mutex_lock(&gbl_fingerprint_rtstats_hash_mu);
	if (gbl_fingerprint_rtstats_hash != NULL) {
		t = hash_find(gbl_fingerprint_rtstats_hash, fingerprint);
		if (t != NULL) {
			/* Writer (bump_pagein) updates these locklessly with
			 * ATOMIC_ADD64, so load them atomically too. */
			*n_pagein_read = ATOMIC_LOAD64(t->n_pagein_read);
			*n_pagein_read_io = ATOMIC_LOAD64(t->n_pagein_read_io);
			found = 1;
		}
	}
	Pthread_mutex_unlock(&gbl_fingerprint_rtstats_hash_mu);

	return found;
}
