/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 *
 * $Id: crypto.h,v 1.11 2003/04/28 14:27:26 bostic Exp $
 */

#ifndef	_DB_CRYPTO_H_
#define	_DB_CRYPTO_H_

/*
 * !!!
 * These are the internal representations of the algorithm flags.
 * They are used in both the DB_CIPHER structure and the CIPHER
 * structure so we can tell if users specified both passwd and alg
 * correctly.
 *
 * CIPHER_ANY is used when an app joins an existing env but doesn't
 * know the algorithm originally used.  This is only valid in the
 * DB_CIPHER structure until we open and can set the alg.
 */
/*
 * We store the algorithm in an 8-bit field on the meta-page.  So we
 * use a numeric value, not bit fields.
 * now we are limited to 8 algorithms before we cannot use bits and
 * need numeric values.  That should be plenty.  It is okay for the
 * CIPHER_ANY flag to go beyond that since that is never stored on disk.
 */

/*
 * This structure is per-process, not in shared memory.
 */
struct __db_cipher {
	u_int	(*adj_size) __P((size_t));
	int	(*close) __P((DB_ENV *, void *));
	int	(*decrypt) __P((DB_ENV *, void *, void *, u_int8_t *, size_t));
	int	(*encrypt) __P((DB_ENV *, void *, void *, u_int8_t *, size_t));
	int	(*init) __P((DB_ENV *, DB_CIPHER *));

	u_int8_t mac_key[DB_MAC_KEY];	/* MAC key. */
	void	*data;			/* Algorithm-specific information */

#define	CIPHER_AES	1		/* AES algorithm */
	u_int8_t	alg;		/* Algorithm used - See above */
	u_int8_t	spare[3];	/* Spares */

#define	CIPHER_ANY	0x00000001	/* Only for DB_CIPHER */
	u_int32_t	flags;		/* Other flags */
};

#ifdef HAVE_CRYPTO

#include <openssl/objects.h>
#include <openssl/aes.h>
#include <openssl/crypto.h>

/*
 * Shared ciphering structure
 * No DB_MUTEX needed because all information is read-only after creation.
 */
typedef struct __cipher {
	roff_t		passwd;		/* Offset to shared passwd */
	size_t		passwd_len;	/* Length of passwd */
	u_int32_t	flags;		/* Algorithm used - see above */
} CIPHER;

#define	DB_AES_KEYLEN	128	/* AES key length */
#define	DB_AES_CHUNK	16	/* AES byte unit size */

typedef struct __aes_cipher {
	AES_KEY	decrypt_key;	/* Decryption key instance */
	AES_KEY	encrypt_key;	/* Encryption key instance */
} AES_CIPHER;

#include "dbinc_auto/crypto_ext.h"
#endif /* HAVE_CRYPTO */
#endif /* !_DB_CRYPTO_H_ */
