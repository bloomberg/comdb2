/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */
/*
 * Copyright (c) 1990, 1993, 1994
 *	The Regents of the University of California.  All rights reserved.
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
 * $Id: db_swap.h,v 11.9 2003/01/08 04:31:32 bostic Exp $
 */

#ifndef _DB_SWAP_H_
#define	_DB_SWAP_H_

#if defined(__cplusplus)
extern "C" {
#endif

#if defined(_LINUX_SOURCE)
	#include <dbinc/db_swap.amd64.h>
#elif defined(_IBM_SOURCE)
	#include <dbinc/db_swap.big.h>
#elif defined(_SUN_SOURCE)
	#include <dbinc/db_swap.big.h>
#else
	#error "PROVIDE MACHINE BYTE FLIPPING"
#endif

#undef	SWAP32
#define	SWAP32(p) {							\
	P_32_SWAP(p);							\
	(p) += sizeof(u_int32_t);					\
}
#undef	SWAP16
#define	SWAP16(p) {							\
	P_16_SWAP(p);							\
	(p) += sizeof(u_int16_t);					\
}

#define	LOGCOPY_TOLSN(lsnp, p) do {				\
	LOGCOPY_32(&(lsnp)->file, (p));				\
	LOGCOPY_32(&(lsnp)->offset,				\
	    (u_int8_t *)(p) + sizeof(u_int32_t));		\
} while (0)

#define	LOGCOPY_FROMLSN(p, lsnp) do {				\
	LOGCOPY_32((p), &(lsnp)->file);				\
	LOGCOPY_32((u_int8_t *)(p) + sizeof(u_int32_t), &(lsnp)->offset);	\
} while (0)


#if defined(__cplusplus)
}
#endif

#endif /* !_DB_SWAP_H_ */
