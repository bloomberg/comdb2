/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2001-2003
 *	Sleepycat Software.  All rights reserved.
 *
 *
 * Some parts of this code originally written by Adam Stubblefield,
 * astubble@rice.edu.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: aes_method.c,v 1.18 2003/04/28 19:59:19 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <string.h>
#endif
#include <stdlib.h>

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/hmac.h"

static int __aes_derivekeys __P((DB_ENV *, DB_CIPHER *, u_int8_t *, size_t));

/*
 * __aes_setup --
 *	Setup AES functions.
 *
 * PUBLIC: int __aes_setup __P((DB_ENV *, DB_CIPHER *));
 */
int
__aes_setup(dbenv, db_cipher)
	DB_ENV *dbenv;
	DB_CIPHER *db_cipher;
{
	AES_CIPHER *aes_cipher;
	int ret;

	db_cipher->adj_size = __aes_adj_size;
	db_cipher->close = __aes_close;
	db_cipher->decrypt = __aes_decrypt;
	db_cipher->encrypt = __aes_encrypt;
	db_cipher->init = __aes_init;
	if ((ret = __os_calloc(dbenv, 1, sizeof(AES_CIPHER), &aes_cipher)) != 0)
		return (ret);
	db_cipher->data = aes_cipher;
	return (0);
}

/*
 * __aes_adj_size --
 *	Given a size, return an addition amount needed to meet the
 *	"chunk" needs of the algorithm.
 *
 * PUBLIC: u_int __aes_adj_size __P((size_t));
 */
u_int
__aes_adj_size(len)
	size_t len;
{
	if (len % DB_AES_CHUNK == 0)
		return (0);
	return (DB_AES_CHUNK - (len % DB_AES_CHUNK));
}

/*
 * __aes_close --
 *	Destroy the AES encryption instantiation.
 *
 * PUBLIC: int __aes_close __P((DB_ENV *, void *));
 */
int
__aes_close(dbenv, data)
	DB_ENV *dbenv;
	void *data;
{
	__os_free(dbenv, data);
	return (0);
}

/*
 * __aes_decrypt --
 *	Decrypt data with AES.
 *
 * PUBLIC: int __aes_decrypt __P((DB_ENV *, void *, void *,
 * PUBLIC:     u_int8_t *, size_t));
 */
int
__aes_decrypt(dbenv, aes_data, iv, cipher, cipher_len)
	DB_ENV *dbenv;
	void *aes_data;
	void *iv;
	u_int8_t *cipher;
	size_t cipher_len;
{
	AES_CIPHER *aes = (AES_CIPHER *)aes_data;
	if (aes == NULL || iv == NULL || cipher == NULL)
		return (EINVAL);
	if ((cipher_len % DB_AES_CHUNK) != 0)
		return (EINVAL);
	AES_cbc_encrypt(cipher, cipher, cipher_len, &aes->decrypt_key, iv, AES_DECRYPT);
	return (0);
}

/*
 * __aes_encrypt --
 *	Encrypt data with AES.
 *
 * PUBLIC: int __aes_encrypt __P((DB_ENV *, void *, void *,
 * PUBLIC:     u_int8_t *, size_t));
 */
int
__aes_encrypt(dbenv, aes_data, iv, data, data_len)
	DB_ENV *dbenv;
	void *aes_data;
	void *iv;
	u_int8_t *data;
	size_t data_len;
{
	AES_CIPHER *aes;
	int ret;

	aes = (AES_CIPHER *)aes_data;
	if (aes == NULL || data == NULL)
		return (EINVAL);
	if ((data_len % DB_AES_CHUNK) != 0)
		return (EINVAL);
	/*
	 * Generate the IV here.
	 * We don't do this outside of there because some encryption
	 * algorithms someone might add may not use IV's and we always
	 * want on here.
	 */
	uint8_t orig[DB_IV_BYTES], copy[DB_IV_BYTES];
	if ((ret = __db_generate_iv(dbenv, (uint32_t*)orig)) != 0)
		return (ret);
	memcpy(copy, orig, DB_IV_BYTES);
	AES_cbc_encrypt(data, data, data_len, &aes->encrypt_key, copy, AES_ENCRYPT);
	memcpy(iv, orig, DB_IV_BYTES);
	return (0);
}

/*
 * __aes_init --
 *	Initialize the AES encryption instantiation.
 *
 * PUBLIC: int __aes_init __P((DB_ENV *, DB_CIPHER *));
 */
int
__aes_init(dbenv, db_cipher)
	DB_ENV *dbenv;
	DB_CIPHER *db_cipher;
{
	return (__aes_derivekeys(dbenv, db_cipher, (u_int8_t *)dbenv->passwd,
	    dbenv->passwd_len));
}

static int
__aes_derivekeys(dbenv, db_cipher, passwd, plen)
	DB_ENV *dbenv;
	DB_CIPHER *db_cipher;
	u_int8_t *passwd;
	size_t plen;
{
	if (passwd == NULL) return (EINVAL);
	AES_CIPHER *aes = (AES_CIPHER *)db_cipher->data;

	SHA_CTX ctx;
	SHA1_Init(&ctx);
	SHA1_Update(&ctx, passwd, plen);
	SHA1_Update(&ctx, (u_int8_t *)DB_ENC_MAGIC, strlen(DB_ENC_MAGIC));
	SHA1_Update(&ctx, passwd, plen);
	uint8_t temp[DB_MAC_KEY];
	SHA1_Final(temp, &ctx);

	if (AES_set_encrypt_key(temp, DB_AES_KEYLEN, &aes->encrypt_key) < 0)
		return (EINVAL);
	if (AES_set_decrypt_key(temp, DB_AES_KEYLEN, &aes->decrypt_key) < 0)
		return (EINVAL);
	return (0);
}
