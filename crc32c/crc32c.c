/*
   Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#include <stdio.h>
#include <string.h>
#include <stddef.h>

#include "crc32c.h"

/* Compute chksum using lookup tables (slicing by 8) */
#include "sb8.h"

#include <logmsg.h>

uint32_t crc32c_software(const uint8_t* buf, uint32_t sz, uint32_t crc)
{
	/* Process misaligned data byte at a time */
	intptr_t misaligned = (intptr_t)buf & (sizeof(intptr_t) - 1);
	unsigned adj = misaligned ? sizeof(intptr_t) - misaligned : 0;
	if (adj > sz) adj = sz;
	int i = 0;
	switch (adj) {
	case 7: crc = crc_tableil8_o32[(crc ^ buf[i]) & 0x000000FF] ^ (crc >> 8); ++i;
	case 6: crc = crc_tableil8_o32[(crc ^ buf[i]) & 0x000000FF] ^ (crc >> 8); ++i;
	case 5: crc = crc_tableil8_o32[(crc ^ buf[i]) & 0x000000FF] ^ (crc >> 8); ++i;
	case 4: crc = crc_tableil8_o32[(crc ^ buf[i]) & 0x000000FF] ^ (crc >> 8); ++i;
	case 3: crc = crc_tableil8_o32[(crc ^ buf[i]) & 0x000000FF] ^ (crc >> 8); ++i;
	case 2: crc = crc_tableil8_o32[(crc ^ buf[i]) & 0x000000FF] ^ (crc >> 8); ++i;
	case 1: crc = crc_tableil8_o32[(crc ^ buf[i]) & 0x000000FF] ^ (crc >> 8); ++i;
		sz -= adj;
		buf += i;
	}
	/* Process 8 bytes at a time */
	const uint8_t *end = buf + (sz & (~0x7));
	while (buf < end) {
		// read two little endian ints
		uint32_t u32a, u32b;
		u32a = ((uint32_t)buf[0]<<0) | ((uint32_t)buf[1]<<8) | ((uint32_t)buf[2]<<16) | ((uint32_t)buf[3]<<24);
		buf += 4;
		u32b = ((uint32_t)buf[0]<<0) | ((uint32_t)buf[1]<<8) | ((uint32_t)buf[2]<<16) | ((uint32_t)buf[3]<<24);
		buf += 4;
		crc ^= u32a;
		uint32_t term1 = crc_tableil8_o88[crc & 0x000000FF] ^ crc_tableil8_o80[(crc >> 8) & 0x000000FF];
		uint32_t term2 = crc >> 16;
		crc = term1 ^ crc_tableil8_o72[term2 & 0x000000FF] ^ crc_tableil8_o64[(term2 >> 8) & 0x000000FF];
		term1 = crc_tableil8_o56[u32b & 0x000000FF] ^ crc_tableil8_o48[(u32b >> 8) & 0x000000FF];
		term2 = u32b >> 16;
		crc = crc ^ term1 ^ crc_tableil8_o40[term2  & 0x000000FF] ^ crc_tableil8_o32[(term2 >> 8) & 0x000000FF];
	}
	/* Process the last 7 (or less) bytes */
	sz &= 0x7;
	i = 0;
	switch (sz) {
	case 7: crc = crc_tableil8_o32[(crc ^ buf[i]) & 0x000000FF] ^ (crc >> 8); ++i;
	case 6: crc = crc_tableil8_o32[(crc ^ buf[i]) & 0x000000FF] ^ (crc >> 8); ++i;
	case 5: crc = crc_tableil8_o32[(crc ^ buf[i]) & 0x000000FF] ^ (crc >> 8); ++i;
	case 4: crc = crc_tableil8_o32[(crc ^ buf[i]) & 0x000000FF] ^ (crc >> 8); ++i;
	case 3: crc = crc_tableil8_o32[(crc ^ buf[i]) & 0x000000FF] ^ (crc >> 8); ++i;
	case 2: crc = crc_tableil8_o32[(crc ^ buf[i]) & 0x000000FF] ^ (crc >> 8); ++i;
	case 1: crc = crc_tableil8_o32[(crc ^ buf[i]) & 0x000000FF] ^ (crc >> 8); ++i;
	}
	return crc;
}

#ifdef __x86_64__

#include <smmintrin.h>
#include <wmmintrin.h>

/* Fwd declare available methods to compute crc32c */
static uint32_t crc32c_sse_pcl(const uint8_t *buf, uint32_t sz, uint32_t crc);
static uint32_t crc32c_sse(const uint8_t *buf, uint32_t sz, uint32_t crc);

typedef uint32_t(*crc32c_t)(const uint8_t* data, uint32_t size, uint32_t crc);
static crc32c_t crc32c_func;

/* Vector type so that we can use pclmul */
typedef long long v2di __attribute__ ((vector_size(16)));

/* Select best method to compute crc32c */
#include <cpuid.h>
#ifdef __clang__
#define SSE4_2 bit_SSE42
#define PCLMUL bit_PCLMULQDQ
#else
#define SSE4_2 bit_SSE4_2
#define PCLMUL bit_PCLMUL
#endif
void crc32c_init(int v)
{
	uint32_t eax, ebx, ecx, edx;
	__cpuid(1, eax, ebx, ecx, edx);
	if (ecx & SSE4_2) {
		if (ecx & PCLMUL) {
			crc32c_func = crc32c_sse_pcl;
			if (v) {
				logmsg(LOGMSG_INFO, "SSE 4.2 + PCLMUL SUPPORT FOR CRC32C\n");
				logmsg(LOGMSG_INFO, "crc32c = crc32c_sse_pcl\n");
			}
		} else {
			crc32c_func = crc32c_sse;
			if (v) {
                logmsg(LOGMSG_INFO, "SSE 4.2 SUPPORT FOR CRC32C\n");
				logmsg(LOGMSG_INFO, "crc32c = crc32c_sse\n");
			}
		}
	}
	if (crc32c_func == NULL) {
		crc32c_func = crc32c_software;
		if (v) {
			logmsg(LOGMSG_INFO, "NO HARDWARE SUPPORT FOR CRC32C\n");
			logmsg(LOGMSG_INFO, "crc32c = crc32c_software\n");
		}
	}
}

uint32_t crc32c_comdb2(const uint8_t* buf, uint32_t sz)
{
	return crc32c_func(buf, sz, CRC32C_SEED);
}

/* Helper routines */
static inline uint32_t crc32c_1024_sse_int(const uint8_t *buf, uint32_t crc);
static inline uint32_t crc32c_until_aligned(const uint8_t **buf, uint32_t *sz, uint32_t crc);
static inline uint32_t crc32c_8s(const uint8_t *buf, uint32_t sz, uint32_t crc);

#define _1K 1024
#define _3K _1K * 3

#define REPEAT_2(x) x x
#define REPEAT_4(x) REPEAT_2(x) REPEAT_2(x)
#define REPEAT_8(x) REPEAT_4(x) REPEAT_4(x)
#define REPEAT_16(x) REPEAT_8(x) REPEAT_8(x)
#define REPEAT_32(x) REPEAT_16(x) REPEAT_16(x)
#define REPEAT_64(x) REPEAT_32(x) REPEAT_32(x)
#define REPEAT_42(x) REPEAT_32(x) REPEAT_8(x) REPEAT_2(x)
#define REPEAT_127(x) REPEAT_64(x) REPEAT_32(x) REPEAT_16(x) \
    REPEAT_8(x) REPEAT_4(x) REPEAT_2(x) x

// Intel White Paper: Fast CRC Computation for iSCSI Polynomial Using CRC32 Instruction
#define THREESOME			\
c1 = _mm_crc32_u64(c1, b1[i]);	\
c2 = _mm_crc32_u64(c2, b2[i]);	\
c3 = _mm_crc32_u64(c3, b3[i]);	\
++i;

/* Compute chksum processing 8 bytes at a time */
static inline uint32_t crc32c_8s(const uint8_t *buf, uint32_t sz, uint32_t crc)
{
	crc = crc32c_until_aligned(&buf, &sz, crc);
	const uint8_t *end = buf + sz;
	const uint64_t *b = (uint64_t *) buf;
	const uint64_t *e = b + (sz / 8);
	while (b < e) {
		crc = _mm_crc32_u64(crc, *b);
		++b;
	}
	buf = (uint8_t *) b;
	intptr_t diff = end - buf;
	int i = 0;
	switch (diff) {
	case 7: crc = _mm_crc32_u8(crc, buf[i]); ++i;
	case 6: crc = _mm_crc32_u8(crc, buf[i]); ++i;
	case 5: crc = _mm_crc32_u8(crc, buf[i]); ++i;
	case 4: crc = _mm_crc32_u8(crc, buf[i]); ++i;
	case 3: crc = _mm_crc32_u8(crc, buf[i]); ++i;
	case 2: crc = _mm_crc32_u8(crc, buf[i]); ++i;
	case 1: crc = _mm_crc32_u8(crc, buf[i]); ++i;
	}
	return crc;
}

/*
 * Compute chksum processing 1024 bytes at a time and using
 * lookup tables for recombination
 */
static uint32_t crc32c_sse(const uint8_t *buf, uint32_t sz, uint32_t crc)
{
	crc = crc32c_until_aligned(&buf, &sz, crc);

	while (sz >= 1024) {
		crc = crc32c_1024_sse_int(buf, crc);
		buf += 1024;
		sz -= 1024;
	}

	uint32_t i = sz % 1024;

	if (i > 0) {
		crc = crc32c_8s(buf, i, crc);
		buf += i;
		sz -= i;
	}

	return crc;
}

/*
 * Compute chksum processing 3072 bytes at a time and using
 * PCLMUL for recombination. Use SSE for processing input < 3K.
 */
static uint32_t crc32c_sse_pcl(const uint8_t *buf, uint32_t sz, uint32_t crc)
{
	crc = crc32c_until_aligned(&buf, &sz, crc);
	const uint64_t *b1, *b2, *b3;
	uint64_t c1, c2, c3;
	uint64_t out = crc;
	v2di x1 = {0}, x2 = {0};
	const v2di K = {0x1a0f717c4, 0x0170076fa};

	while (sz >= _3K) {
		b1 = (const uint64_t *) &buf[0];
		b2 = (const uint64_t *) &buf[1024];
		b3 = (const uint64_t *) &buf[2048];
		c1 = out;
		c2 = c3 = 0;
		int i = 0;

		REPEAT_127(THREESOME);

		// Combine three results
		x1[0] = _mm_crc32_u64(c1, b1[127]); // block 1 crc
		x2[0] = _mm_crc32_u64(c2, b2[127]); // block 2 crc

		x1 = _mm_clmulepi64_si128(x1, K, 0x00); // mul by K[0]
		x2 = _mm_clmulepi64_si128(x2, K, 0x10); // mul by K[1]
		x1 = _mm_xor_si128(x1, x2);

		out = x1[0];    // boring scalar operations
		out ^= b3[127];
		out = _mm_crc32_u64(c3, out);

		buf += _3K;
		sz -= _3K;
	}
	if (sz) out = crc32c_sse(buf, sz, out);
	return out;
}

/* Compute chksum 1 byte at a time until input is sizeof(intptr) aligned */
static inline
uint32_t crc32c_until_aligned(const uint8_t **buf_, uint32_t *sz_, uint32_t crc)
{
	const uint8_t *buf = *buf_;
	uint32_t sz = *sz_;
	intptr_t misaligned = (intptr_t)buf & (sizeof(intptr_t) - 1);
	unsigned adj = misaligned ? sizeof(intptr_t) - misaligned : 0;
	if (adj > sz) adj = sz;
	int i = 0;
	switch (adj) {
	case 7: crc = _mm_crc32_u8(crc, buf[i]); ++i;
	case 6: crc = _mm_crc32_u8(crc, buf[i]); ++i;
	case 5: crc = _mm_crc32_u8(crc, buf[i]); ++i;
	case 4: crc = _mm_crc32_u8(crc, buf[i]); ++i;
	case 3: crc = _mm_crc32_u8(crc, buf[i]); ++i;
	case 2: crc = _mm_crc32_u8(crc, buf[i]); ++i;
	case 1: crc = _mm_crc32_u8(crc, buf[i]); ++i;
		sz -= adj;
		*sz_ = sz;
		*buf_ = buf + i;
	}
	return crc;
}

/* Compute chksum for 1024 bytes using SSE & recombine using lookup tables */
#include "crc32c_1024.h"
static inline uint32_t crc32c_1024_sse_int(const uint8_t *buf, uint32_t crc)
{
	uint64_t c1, c2, c3, tmp;
	const uint64_t *b8 = (const uint64_t *) buf;
	const uint64_t *b1 = &b8[1];
	const uint64_t *b2 = &b8[43];
	const uint64_t *b3 = &b8[85];
	c2 = c3 = 0;

	c1 = _mm_crc32_u64(crc, b8[0]);
	int i = 0;
	REPEAT_42(THREESOME);

	// merge in c2
	tmp = b8[127];
	tmp ^= mul_table1_336[c2 & 0xFF];
	tmp ^= ((uint64_t) mul_table1_336[(c2 >> 8) & 0xFF]) << 8;
	tmp ^= ((uint64_t) mul_table1_336[(c2 >> 16) & 0xFF]) << 16;
	tmp ^= ((uint64_t) mul_table1_336[(c2 >> 24) & 0xFF]) << 24;

	// merge in c1
	tmp ^= mul_table1_672[c1 & 0xFF];
	tmp ^= ((uint64_t) mul_table1_672[(c1 >> 8) & 0xFF]) << 8;
	tmp ^= ((uint64_t) mul_table1_672[(c1 >> 16) & 0xFF]) << 16;
	tmp ^= ((uint64_t) mul_table1_672[(c1 >> 24) & 0xFF]) << 24;

	return _mm_crc32_u64(c3, tmp);
}

#endif // Intel only

#if defined(_HAS_CRC32_ARMV7) || defined(_HAS_CRC32_ARMV8)
#include <arm_acle.h>
#include <asm/hwcap.h>
#include <sys/auxv.h>

typedef uint32_t(*crc32c_t)(const uint8_t* data, uint32_t size, uint32_t crc);
static crc32c_t crc32c_func;

/* compute chksum for a small (<8) number of items word then half word then byte 
   doing loop is more expensive then chunking by word, half, and byte
   while (sz >= sizeof(const uint8_t)) {
       crc = __crc32cb(crc, *buf++);
       sz -= 1;
   }
 */

static inline uint32_t crc32c_process_small_arm(const uint8_t **buf_, uint32_t *totsz_, uint32_t sz, uint32_t crc)
{
    const uint8_t *buf = *buf_;

    if (sz & sizeof(uint32_t)) {
        crc = __crc32cw(crc, *(const uint32_t *)buf);
        buf += sizeof(uint32_t);
    }

    if (sz & sizeof(uint16_t)) {
        crc = __crc32ch(crc, *(const uint16_t *)buf);
        buf += sizeof(uint16_t);
    }

    if (sz & sizeof(const uint8_t)) {
        crc = __crc32cb(crc, *buf);
    }
    *buf_ = buf + sz;
    *totsz_ -= sz;

    return crc;
}

/* Compute chksum until input is sizeof(intptr) aligned */
static inline uint32_t crc32c_until_aligned_arm(const uint8_t **buf_, uint32_t *sz_, uint32_t crc)
{
    const uint8_t *buf = *buf_;
    uint32_t sz = *sz_;
    intptr_t misaligned = (intptr_t)buf & (sizeof(intptr_t) - 1);
    unsigned adj = misaligned ? sizeof(intptr_t) - misaligned : 0;
    if (adj > sz) adj = sz;
    return crc32c_process_small_arm(buf_, sz_, adj, crc);
}

static inline uint32_t crc32c_arm(const uint8_t* buf, uint32_t sz, uint32_t crc)
{
    // If we will need to process long buffers aligned we
    // should call it here: crc32c_until_aligned_arm(&buf, &sz, crc);

    while (sz >= sizeof(uint64_t)) {
        crc = __crc32cd(crc, *(const uint64_t *)buf);
        sz -= sizeof(uint64_t);
        buf += sizeof(uint64_t);
    }

    return crc32c_process_small_arm(&buf, &sz, sz, crc);
}

void crc32c_init(int v)
{
#if defined(_HAS_CRC32_ARMV7)
    int en = getauxval(AT_HWCAP) & HWCAP2_CRC32;
#else
    int en = getauxval(AT_HWCAP) & HWCAP_CRC32;
#endif
    if (en) {
        crc32c_func = crc32c_arm;
        if (v) {
            logmsg(LOGMSG_INFO, "ARM HW SUPPORT FOR CRC32C\n");
        }
    } else {
        crc32c_func = crc32c_software;
        if (v) {
            logmsg(LOGMSG_INFO, "NO HARDWARE SUPPORT FOR CRC32C\n");
            logmsg(LOGMSG_INFO, "crc32c = crc32c_software\n");
        }
    }
}

uint32_t crc32c_comdb2(const uint8_t* buf, uint32_t sz)
{
    return crc32c_func(buf, sz, CRC32C_SEED);
}

#endif
