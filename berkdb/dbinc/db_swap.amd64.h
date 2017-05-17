#define P_16_COPYSWAP(x, y) (*(uint16_t *)(y) = __builtin_bswap16(*(uint16_t *)(x)))
#define P_32_COPYSWAP(x, y) (*(uint32_t *)(y) = __builtin_bswap32(*(uint32_t *)(x)))
#define P_64_COPYSWAP(x, y) (*(uint64_t *)(y) = __builtin_bswap64(*(uint64_t *)(x)))

#define P_16_SWAP(x) P_16_COPYSWAP((x), (x))
#define P_32_SWAP(x) P_32_COPYSWAP((x), (x))
#define P_64_SWAP(x) P_64_COPYSWAP((x), (x))

#define M_16_SWAP(x) ((x) = __builtin_bswap16((uint16_t)(x)))
#define M_32_SWAP(x) ((x) = __builtin_bswap32((uint32_t)(x)))
#define M_64_SWAP(x) ((x) = __builtin_bswap64((uint64_t)(x)))

#define LOG_SWAPPED() 1
#define __db_isbigendian() 0
#define	LOGCOPY_16(x, p) P_16_COPYSWAP((p), (x))
#define	LOGCOPY_32(x, p) P_32_COPYSWAP((p), (x))
#define	LOGCOPY_64(x, p) P_64_COPYSWAP((p), (x))
