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

#ifndef _INCLUDED_FLIBC_H
#define _INCLUDED_FLIBC_H

#include <sys/types.h>
#include <netinet/in.h>
#include <compile_time_assert.h>

/* When casting an floating point number to an integer the result is undefined
 * if the floating point number is out of the integer's range (and it does in
 * fact have different behavior across our types of machines). to protect
 * against this we used checks like the following before we cast floats to ints:
 * if(flt > UINT_MAX) return -1;
 * The trouble is that UINT_MAX is out of a float's range so while
 * UINT_MAX == 4294967295
 * (float)UINT_MAX == 4294967296.000000
 * This means that if flt == 4294967296.000000 it will pass the check above
 * and undefined behavior will result when we later try to convert it to an
 * unsigned int.
 *
 * For each #define from limits.h, these are the largest (in magnitude) values
 * that can be represented by doubles/floats that are still within the limits.h
 * #define.s
 *
 * To generate these values below that aren't directly from limits.h, the c99
 * functions nextafter() and nextafterf() were used to find the appropriate
 * value (ie FLIBC_FLT_UINT_MAX = nextafterf((float)UINT_MAX, 0.0)). */

#define FLIBC_DBL_ULLONG_MAX ((double)18446744073709549568ULL)
#define FLIBC_FLT_ULLONG_MAX ((float)18446742974197923840ULL)

#define FLIBC_DBL_LLONG_MAX ((double)9223372036854774784LL)
#define FLIBC_FLT_LLONG_MAX ((float)9223371487098961920LL)

#define FLIBC_DBL_LLONG_MIN ((double)LLONG_MIN)
#define FLIBC_FLT_LLONG_MIN ((float)LLONG_MIN)

#define FLIBC_DBL_UINT_MAX ((double)UINT_MAX)
#define FLIBC_FLT_UINT_MAX ((float)4294967040U)

#define FLIBC_DBL_INT_MAX ((double)INT_MAX)
#define FLIBC_FLT_INT_MAX ((float)2147483520)

#define FLIBC_DBL_INT_MIN ((double)INT_MIN)
#define FLIBC_FLT_INT_MIN ((float)INT_MIN)

#define FLIBC_DBL_USHRT_MAX ((double)USHRT_MAX)
#define FLIBC_FLT_USHRT_MAX ((float)USHRT_MAX)

#define FLIBC_DBL_SHRT_MAX ((double)SHRT_MAX)
#define FLIBC_FLT_SHRT_MAX ((float)SHRT_MAX)

#define FLIBC_DBL_SHRT_MIN ((double)SHRT_MIN)
#define FLIBC_FLT_SHRT_MIN ((float)SHRT_MIN)

/* the following functions are for converting long long ints, doubles and floats
 * between host and network order */

union twin {
    uint64_t u64;
    uint32_t u32[2];
    uint8_t u8[8];
};
typedef union twin twin_t;
BB_COMPILE_TIME_ASSERT(twin_size, sizeof(twin_t) == sizeof(uint64_t));

union shortswp {
    uint16_t u16;
    uint8_t u8[2];
};
typedef union shortswp shortswp_t;
BB_COMPILE_TIME_ASSERT(shortswp_size, sizeof(shortswp_t) == sizeof(uint16_t));

union intswp {
    uint32_t u32;
    uint8_t u8[4];
};
typedef union intswp intswp_t;
BB_COMPILE_TIME_ASSERT(intswp_size, sizeof(intswp_t) == sizeof(uint32_t));

int flibc_snprintf_dbl(char *p_str, const size_t str_buf_len, double d);

#if defined(_LINUX_SOURCE)
#include "flibc.amd64.h"
#elif defined(_IBM_SOURCE)
#include "flibc.powerpc.h"
#elif defined(_SUN_SOURCE)
#include "flibc.sparc.h"
#else
#error "PROVIDE BYTE FLIPPING ROUTINES"
#endif

static inline float flibc_floatflip(float in)
{
    uint32_t out = flibc_intflip(*(uint32_t *)(&in));
    return *(float *)&out;
}
static inline double flibc_dblflip(double in)
{
    uint64_t out = flibc_llflip(*(uint64_t *)(&in));
    return *(double *)(&out);
}
#define flibc_htonll(x) flibc_ntohll(x)
#define flibc_htonf(x) flibc_ntohf(x)
#define flibc_htond(x) flibc_ntohd(x)
#endif // _INCLUDED_FLIBC_H
