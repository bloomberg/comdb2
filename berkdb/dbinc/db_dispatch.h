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
 *
 * $Id: db_dispatch.h,v 11.36 2003/09/04 19:01:13 bostic Exp $
 */

#ifndef _DB_DISPATCH_H_
#define	_DB_DISPATCH_H_

#include <plhash.h>
/*
 * Declarations and typedefs for the list of transaction IDs used during
 * recovery.  This is a generic list used to pass along whatever information
 * we need during recovery.
 */
typedef enum {
	TXNLIST_DELETE,
	TXNLIST_LSN,
	TXNLIST_PGNO,
	TXNLIST_TXNID
} db_txnlist_type;

#define	DB_TXNLIST_MASK(hp, n)  (n % hp->nslots)
struct __db_txnhead {
	u_int32_t maxid;	/* Maximum transaction id. */
	DB_LSN maxlsn;		/* Maximum commit lsn. */
	DB_LSN ckplsn;		/* LSN of last retained checkpoint. */
	DB_LSN trunc_lsn;	/* Lsn to which we are going to truncate;
				 * make sure we abort anyone after this. */
	u_int32_t generation;	/* Current generation number. */
	u_int32_t gen_alloc;	/* Number of generations allocated. */
	struct {
		u_int32_t generation;
		u_int32_t txn_min;
		u_int32_t txn_max;
	} *gen_array;		/* Array of txnids associated with a gen. */
	u_int nslots;
	hash_t *h;
	LIST_HEAD(__db_headlink, __db_txnlist) head[1];
};

union __txnlist_union {
    struct {
        u_int32_t txnid;
        u_int32_t generation;
        int32_t status;
    } t;
    struct {
        u_int32_t ntxns;
        u_int32_t maxn;
        DB_LSN *lsn_array;
    } l;
    struct {
        u_int32_t nentries;
        u_int32_t maxentry;
        int32_t locked;
        char *fname;
        int32_t fileid;
        db_pgno_t *pgno_array;
        u_int8_t uid[DB_FILE_ID_LEN];
    } p;
};

struct __db_txnlist {
	db_txnlist_type type;
	union __txnlist_union u;
	struct __db_txnlist *next;
	LIST_ENTRY(__db_txnlist) links;
};

/*
 * Flag value for __db_txnlist_lsnadd. Distinguish whether we are replacing
 * an entry in the transaction list or adding a new one.
 */
#define	TXNLIST_NEW	0x1

/*
 * States for limbo list processing.
 */

typedef enum {
	LIMBO_NORMAL,		/* Normal processing. */
	LIMBO_PREPARE,		/* We are preparing a transaction. */
	LIMBO_RECOVER,		/* We are in recovery. */
	LIMBO_TIMESTAMP,	/* We are recovering to a timestamp. */
	LIMBO_COMPENSATE	/* After recover to ts, generate log records. */
} db_limbo_state;

#endif /* !_DB_DISPATCH_H_ */
