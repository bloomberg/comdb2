/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 *
 * $Id: hmac.h,v 1.4 2003/01/08 04:32:02 bostic Exp $
 */

#ifndef	_DB_HMAC_H_
#define	_DB_HMAC_H_

#include <openssl/sha.h>
/*
 * AES assumes the SHA1 checksumming (also called MAC)
 */
#define	DB_MAC_MAGIC	"mac derivation key magic value"
#define	DB_ENC_MAGIC	"encryption and decryption key value magic"

typedef enum {
	algo_both = 1,
	algo_hash4,
	algo_crc32c
} chksum_t;

#include "dbinc_auto/hmac_ext.h"
#endif /* !_DB_HMAC_H_ */
