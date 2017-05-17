#include <flibc.h>

#define P_16_COPYSWAP(x, y) (*(uint16_t *)(y) = flibc_shortflip(*(uint16_t *)(x)))
#define P_32_COPYSWAP(x, y) (*(uint32_t *)(y) = flibc_intflip(*(uint32_t *)(x)))
#define P_64_COPYSWAP(x, y) (*(uint64_t *)(y) = flibc_llflip(*(uint64_t *)(x)))

#define P_16_SWAP(x) P_16_COPYSWAP((x), (x))
#define P_32_SWAP(x) P_32_COPYSWAP((x), (x))
#define P_64_SWAP(x) P_64_COPYSWAP((x), (x))

#define M_16_SWAP(x) ((x) = flibc_shortflip((uint16_t)(x)))
#define M_32_SWAP(x) ((x) = flibc_intflip((uint32_t)(x)))
#define M_64_SWAP(x) ((x) = flibc_llflip((uint64_t)(x)))

#define LOG_SWAPPED() 0
#define __db_isbigendian() 1
#define	LOGCOPY_16(x, p) memcpy((x), (p), sizeof(u_int16_t))
#define	LOGCOPY_32(x, p) memcpy((x), (p), sizeof(u_int32_t))
#define	LOGCOPY_64(x, p) memcpy((x), (p), sizeof(u_int64_t))
