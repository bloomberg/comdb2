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

#ifndef INCLUDED_BB_INTTYPES
#define INCLUDED_BB_INTTYPES

/* This is done because the dg's don't have inttypes.  Its a pity to
   put this in since the dg's are on their way out....this should be cleaned
   up once they are gone, but for now, the dg typedefs are being added as
   needed.
*/

#ifdef __DGUX__
typedef int intptr_t;
typedef unsigned int uintptr_t;
typedef unsigned int uint32_t;
typedef int int32_t;
typedef unsigned short uint16_t;
typedef short int16_t;
typedef unsigned char uint8_t;
#else
#include <inttypes.h>
#endif

/**
 * HP and Linux don't have standard definitions for longlong_t, u_longlong_t.
 * I'm defining them here (even if they don't *strictly* belong), rather than
 * creating yet another file.
 **/

#if defined(_HP_SOURCE) || defined(_LINUX_SOURCE)
typedef long long longlong_t;
typedef unsigned long long u_longlong_t;
#endif

#if defined(_LINUX_SOURCE)
typedef unsigned char uchar_t;
typedef unsigned short ushort_t;
typedef unsigned int uint_t;
typedef unsigned long ulong_t;
#endif

#endif
