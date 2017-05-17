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

#ifndef INCLUDED_BB_STDINT_H
#define INCLUDED_BB_STDINT_H

/* Most types should have a type, limits, const, and fmtio
 */
/* If this file gets too long, we might want to split into four files:
 * this file, bb_stdint_limits.h, bb_stdint_const.h, bb_stdint_fmtio.h
 */

#ifdef __sun
#include <sys/types.h> /* Need this for _LP64 macro */
#endif

#ifdef __cplusplus
extern "C" {
#endif

/***********************************************************************/
/***                       Types                                     ***/
/***********************************************************************/

typedef char bbint8_t;
typedef unsigned char bbuint8_t;

typedef short bbint16_t;
typedef unsigned short bbuint16_t;

typedef int bbint32_t;
typedef unsigned int bbuint32_t;

#if defined(_LP64) || defined(__LP64__)
typedef int bbgint;
typedef unsigned int bbguint;
#else /* 32 bit */
typedef long bbgint;
typedef unsigned long bbguint;
#endif

#if defined(_LP64) || defined(__LP64__)
typedef long bbint64_t;
typedef unsigned long bbuint64_t;
#else /* 32 bit */
typedef long long bbint64_t;
typedef unsigned long long bbuint64_t;
#endif

typedef long bbintptr_t;
typedef unsigned long bbuintptr_t;

/* This is for fortran character string lengths when *into* fortran
   subroutines after the argument list. */
typedef long bbf77charstr_sz;

/***********************************************************************/
/***                       Limits                                    ***/
/***********************************************************************/

#define BBINT8_MIN (-128)
#define BBINT8_MAX (127)
#define BBUINT8_MAX (255U)

#define BBINT16_MIN (-32767 - 1)
#define BBINT16_MAX (32767)
#define BBUINT16_MAX (65535U)

#define BBINT32_MIN (-2147483647 - 1)
#define BBINT32_MAX (2147483647)
#define BBUINT32_MAX (4294967295U)

#if defined(_LP64) || defined(__LP64__)

#define BBINT64_MIN (-9223372036854775807L - 1)
#define BBINT64_MAX (9223372036854775807L)
#define BBUINT64_MAX (18446744073709551615UL)

#else /* 32 bit */

#define BBINT64_MIN (-9223372036854775807LL - 1)
#define BBINT64_MAX (9223372036854775807LL)
#define BBUINT64_MAX (18446744073709551615ULL)

#endif

/***********************************************************************/
/***                       CONST                                     ***/
/***********************************************************************/
/* The following macros create constants of the types.
 * The intent is that:
 *	Constants defined using these macros have a specific size and
 *	signedness.
 */
#define BB__CONCAT__(A, B) A /**/ B

#define BBINT8_C(c) (c)
#define BBUINT8_C(c) BB__CONCAT__(c, u)

#define BBINT16_C(c) (c)
#define BBUINT16_C(c) BB__CONCAT__(c, u)

#define BBINT32_C(c) (c)
#define BBUINT32_C(c) BB__CONCAT__(c, u)

#if defined(_LP64) || defined(__LP64__)
#define BBINT64_C(c) BB__CONCAT__(c, l)
#define BBUINT64_C(c) BB__CONCAT__(c, ul)
#else /* 32 bit */
#define BBINT64_C(c) BB__CONCAT__(c, ll)
#define BBUINT64_C(c) BB__CONCAT__(c, ull)
#endif

#undef BB__CONCAT__
/***********************************************************************/
/***                       Format IO                                 ***/
/***********************************************************************/
/* The form of the names of the macros is either "PRI" for printf specifiers
 * or "SCN" for scanf specifiers, followed by the conversion specifier letter
 * followed by the datatype size. For example, PRId32 is the macro for
 * the printf d conversion specifier with the flags for 32 bit datatype.
 *
 * Separate macros are given for printf and scanf because typically different
 * size flags must prefix the conversion specifier letter.
 *
 * An example using one of these macros:
 *
 *	uint64_t u;
 *	printf("u = %016" BBPRIx64 "\n", u);
 *
 */

/*------------------------------*/
/* Printf "d" conversion.       */
/*------------------------------*/
#define BBPRId8 "d"
#define BBPRId16 "d"

#define BBPRId32 "d"

#if defined(_LP64) || defined(__LP64__)
#define BBPRId64 "ld"
#else /* 32 bit */
#define BBPRId64 "lld"
#endif

/*------------------------------*/
/* Printf "i" conversion.       */
/*------------------------------*/
#define BBPRIi8 "i"
#define BBPRIi16 "i"

#define BBPRIi32 "i"

#if defined(_LP64) || defined(__LP64__)
#define BBPRIi64 "li"
#else /* 32 bit */
#define BBPRIi64 "lli"
#endif

/*------------------------------*/
/* Printf "o" conversion.       */
/*------------------------------*/
#define BBPRIo8 "o"
#define BBPRIo16 "o"

#define BBPRIo32 "o"

#if defined(_LP64) || defined(__LP64__)
#define BBPRIo64 "lo"
#else /* 32 bit */
#define BBPRIo64 "llo"
#endif

/*------------------------------*/
/* Printf "x" conversion.       */
/*------------------------------*/
#define BBPRIx8 "x"
#define BBPRIx16 "x"

#define BBPRIx32 "x"

#if defined(_LP64) || defined(__LP64__)
#define BBPRIx64 "lx"
#else /* 32 bit */
#define BBPRIx64 "llx"
#endif

/*------------------------------*/
/* Printf "X" conversion.       */
/*------------------------------*/
#define BBPRIX8 "X"
#define BBPRIX16 "X"

#define BBPRIX32 "X"

#if defined(_LP64) || defined(__LP64__)
#define BBPRIX64 "lX"
#else /* 32 bit */
#define BBPRIX64 "llX"
#endif

/*---------------------------------*/
/* Printf "u" conversion.          */
/*---------------------------------*/
#define BBPRIu8 "u"
#define BBPRIu16 "u"

#define BBPRIu32 "u"

#if defined(_LP64) || defined(__LP64__)
#define BBPRIu64 "lu"
#else /* 32 bit */
#define BBPRIu64 "llu"
#endif

/*---------------------------------*/
/* Scanf "d" conversion.          */
/*---------------------------------*/
#define BBSCNd16 "hd"

#define BBSCNd32 "d"

#if defined(_LP64) || defined(__LP64__)
#define BBSCNd64 "ld"
#else /* 32 bit  */
#define BBSCNd64 "lld"
#endif

/*---------------------------------*/
/* Scanf "i" conversion.          */
/*---------------------------------*/
#define BBSCNi16 "hi"

#define BBSCNi32 "i"

#if defined(_LP64) || defined(__LP64__)
#define BBSCNi64 "li"
#else /* 32 bit */
#define BBSCNi64 "lli"
#endif

/*---------------------------------*/
/* Scanf "o" conversion.          */
/*---------------------------------*/
#define BBSCNo16 "ho"

#define BBSCNo32 "o"

#if defined(_LP64) || defined(__LP64__)
#define BBSCNo64 "lo"
#else /* 32 bit  */
#define BBSCNo64 "llo"
#endif

/*---------------------------------*/
/* Scanf "u" conversion.          */
/*---------------------------------*/
#define BBSCNu16 "hu"

#define BBSCNu32 "u"

#if defined(_LP64) || defined(__LP64__)
#define BBSCNu64 "lu"
#else /* 32 bit */
#define BBSCNu64 "llu"
#endif

/*---------------------------------*/
/* Scanf "x" conversion.          */
/*---------------------------------*/
#define BBSCNx16 "hx"

#define BBSCNx32 "x"

#if defined(_LP64) || defined(__LP64__)
#define BBSCNx64 "lx"
#else
#define BBSCNx64 "llx"
#endif

/*
 * The following macros define I/O formats for intmax_t and uintmax_t.
 */
#if defined(_LP64) || defined(__LP64__)
#define BBPRIdMAX "ld"
#define BBPRIoMAX "lo"
#define BBPRIxMAX "lx"
#define BBPRIuMAX "lu"
#else /* 32 bit */
#define BBPRIdMAX "lld"
#define BBPRIoMAX "llo"
#define BBPRIxMAX "llx"
#define BBPRIuMAX "llu"
#endif

#if defined(_LP64) || defined(__LP64__)
#define BBSCNiMAX "li"
#define BBSCNdMAX "ld"
#define BBSCNoMAX "lo"
#define BBSCNxMAX "lx"
#else /* 32 bit */
#define BBSCNiMAX "lli"
#define BBSCNdMAX "lld"
#define BBSCNoMAX "llo"
#define BBSCNxMAX "llx"
#endif

#ifdef __cplusplus
}
#endif

#endif /* INCLUDED_BB_STDINT_H */
