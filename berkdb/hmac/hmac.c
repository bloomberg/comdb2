/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2001-2003
 *	Sleepycat Software.  All rights reserved.
 *
 * Some parts of this code originally written by Adam Stubblefield,
 * astubble@rice.edu.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: hmac.c,v 1.26 2003/01/08 05:04:43 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"	/* for hash.h only */
#include "dbinc/hash.h"
#include "dbinc/hmac.h"

#include <stdlib.h>
#include <crc32c.h>

#define	HMAC_OUTPUT_SIZE	20
#define	HMAC_BLOCK_SIZE	64

static void __db_hmac __P((u_int8_t *, u_int8_t *, size_t, u_int8_t *));

static inline void
__db_chksum_int(data, data_len, mac_key, store)
	u_int8_t *data;
	size_t data_len;
	u_int8_t *mac_key;
	u_int8_t *store;
{
	/*
	 * Since the checksum might be on a page of data we are checksumming
	 * we might be overwriting after checksumming, we zero-out the
	 * checksum value so that we can have a known value there when
	 * we verify the checksum.
	 */

	/* Just a hash, no MAC */
	const size_t sumlen = sizeof(uint32_t);

	memset(store, 0, sumlen);
	uint32_t hash = gbl_crc32c ? crc32c(data, data_len)
	    : __ham_func4(NULL, data, data_len);

	memcpy(store, &hash, sumlen);
}

/*
 * __db_chksum --
 *	Create a MAC/SHA1 checksum.
 *
 * PUBLIC: void __db_chksum __P((u_int8_t *, size_t, u_int8_t *, u_int8_t *));
 */
void
__db_chksum(data, data_len, mac_key, store)
	u_int8_t *data;
	size_t data_len;
	u_int8_t *mac_key;
	u_int8_t *store;
{
	__db_chksum_int(data, data_len, mac_key, store);
}

/*
 * __db_chksum_no_crypto --
 *	Create a MAC/SHA1 checksum.
 *
 * PUBLIC: void __db_chksum_no_crypto __P((u_int8_t *, size_t, u_int8_t *));
 */
void
__db_chksum_no_crypto(data, data_len, store)
	u_int8_t *data;
	size_t data_len;
	u_int8_t *store;
{
	__db_chksum_int(data, data_len, NULL, store);
}

/*
 * __db_derive_mac --
 *	Create a MAC/SHA1 key.
 *
 * PUBLIC: void __db_derive_mac __P((u_int8_t *, size_t, u_int8_t *));
 */
void
__db_derive_mac(passwd, plen, mac_key)
	u_int8_t *passwd;
	size_t plen;
	u_int8_t *mac_key;
{
	SHA_CTX ctx;

	/* Compute the MAC key. mac_key must be 20 bytes. */
	SHA1_Init(&ctx);
	SHA1_Update(&ctx, passwd, plen);
	SHA1_Update(&ctx, (u_int8_t *)DB_MAC_MAGIC, strlen(DB_MAC_MAGIC));
	SHA1_Update(&ctx, passwd, plen);
	SHA1_Final(mac_key, &ctx);

	return;
}

/*
 * __db_check_chksum --
 *	Verify a checksum (use crypto if set in dbenv).
 *
 *	Return 0 on success, >0 (errno) on error, -1 on checksum mismatch.
 *
 * PUBLIC: int __db_check_chksum __P((DB_ENV *,
 * PUBLIC:     DB_CIPHER *, u_int8_t *, void *, size_t, int));
 */
int
__db_check_chksum(dbenv, db_cipher, chksum, data, data_len, is_hmac)
	DB_ENV *dbenv;
	DB_CIPHER *db_cipher;
	u_int8_t *chksum;
	void *data;
	size_t data_len;
	int is_hmac;
{
	return __db_check_chksum_algo(dbenv, db_cipher, chksum, data, data_len,
	    is_hmac, algo_both);
}

/*
 * __db_check_chksum_no_crypto --
 *	Verify a checksum w/o crypto.
 *
 *	Return 0 on success, >0 (errno) on error, -1 on checksum mismatch.
 *
 * PUBLIC:  int __db_check_chksum_no_crypto __P((DB_ENV *,
 * PUBLIC:	DB_CIPHER *, u_int8_t *, void *, size_t, int));
 */
int
__db_check_chksum_no_crypto(dbenv, db_cipher, chksum, data, data_len, is_hmac)
	DB_ENV *dbenv;
	DB_CIPHER *db_cipher;
	u_int8_t *chksum;
	void *data;
	size_t data_len;
	int is_hmac;
{
	return __db_check_chksum_algo(dbenv, db_cipher, chksum, data, data_len,
	    is_hmac, algo_both);
}

/*
 * __db_check_chksum_algo --
 *	Verify a checksum using algo provided.
 *
 *	Return 0 on success, >0 (errno) on error, -1 on checksum mismatch.
 *
 * PUBLIC:  int __db_check_chksum_algo __P((DB_ENV *,
 * PUBLIC:	DB_CIPHER *, u_int8_t *, void *, size_t, int, chksum_t));
 */
int
__db_check_chksum_algo(dbenv, db_cipher, chksum, data, data_len, is_hmac, algo)
	DB_ENV *dbenv;
	DB_CIPHER *db_cipher;
	u_int8_t *chksum;
	void *data;
	size_t data_len;
	int is_hmac;
	chksum_t algo;
{
	/*
	 * If we are just doing checksumming and not encryption, then checksum
	 * is 4 bytes.  Otherwise, it is DB_MAC_KEY size.  Check for illegal
	 * combinations of crypto/non-crypto checksums.
	 */
	if (is_hmac == 0 && db_cipher != NULL) {
		__db_err(dbenv,
		    "Unencrypted checksum with a supplied encryption key");
		return (EINVAL);
	}

	const size_t sum_len = sizeof(u_int32_t);
	uint32_t old, hash;
	memcpy(&old, chksum, sum_len);
	memset(chksum, 0, sum_len);

	/*
	 * algo_both is primarily for the log records which don't have
	 * a mechanism to specify which algorithm was used. We try the
	 * one which lrl specifies first. After the db is cut-over
	 * from one algo to the other, there will be a few log files
	 * which will have the checksum verifed twice (failing the new
	 * algo but passing the older one). But in the general case the
	 * first checksum will work.
	 */
	chksum_t use, first, second;
	first = second = algo;
	if (algo == algo_both) {
		if (gbl_crc32c) {
			first = algo_crc32c;
			second = algo_hash4;
		} else {
			first = algo_hash4;
			second = algo_crc32c;
		}
	}
	use = first;
again:	switch (use) {
	case algo_crc32c:
		hash = crc32c(data, (uint32_t)data_len);
		break;
	case algo_hash4:
		hash = __ham_func4(NULL, data, (u_int32_t)data_len);
		break;
	default:
		abort();
	}
	if (old == hash)
		return 0;
	if (use == second)
		return -1;
	use = second;
	goto again;
}
