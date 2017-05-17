#include "chksum.h"
#include "sb8.h"

uint32_t crc32c(const uint8_t* buf, uint32_t sz)
{
	uint32_t crc = 0;
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
		u32a = (buf[0]<<0) | (buf[1]<<8) | (buf[2]<<16) | (buf[3]<<24);
		buf += 4;
		u32b = (buf[0]<<0) | (buf[1]<<8) | (buf[2]<<16) | (buf[3]<<24);
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

uint32_t __ham_func4(const uint8_t *k, uint32_t len)
{
	uint32_t h, loop;

	if (len == 0)
		return (0);

#define	HASH4a	h = (h << 5) - h + *k++;
#define	HASH4b	h = (h << 5) + h + *k++;
#define	HASH4	HASH4b
	h = 0;

	loop = (len + 8 - 1) >> 3;
	switch (len & (8 - 1)) {
	case 0:
		do {
			HASH4;
	case 7:
			HASH4;
	case 6:
			HASH4;
	case 5:
			HASH4;
	case 4:
			HASH4;
	case 3:
			HASH4;
	case 2:
			HASH4;
	case 1:
			HASH4;
		} while (--loop);
	}
	return (h);
}
