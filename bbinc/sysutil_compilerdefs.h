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

#ifndef INCLUDED_SYSUTIL_COMPILERDEFS
#define INCLUDED_SYSUTIL_COMPILERDEFS

#ifndef __attribute_common__
#if defined(__cplusplus) && (defined(__GNUC__) || defined(__HP_aCC))
#define __attribute_common__ __attribute__((common))
#else
#define __attribute_common__
#endif
#endif

/* http://gcc.gnu.org/onlinedocs/gcc-4.1.1/gcc/Function-Attributes.html
 *
 * Usage:
 * __attribute_format__((printf, i, j))
 * where `i' is the 1-based index of the format string argument, and `j' is the
 * 1-based index of the first argument for the format string.  `j' may be 0
 * for va_list arguments, in which case the format string is just checked for
 * consistency.
 *
 * Example:
 * void ctrace(const char *format, ...) __attribute_format__((printf, 1, 2));
 */
#ifndef __attribute_format__
#ifdef __GNUC__
#define __attribute_format__(x) __attribute__((format x))
#else
#define __attribute_format__(x)
#endif
#endif

#ifndef __attribute_deprecated__
#ifdef __GNUC__
#define __attribute_deprecated__ __attribute__((deprecated))
#else
#define __attribute_deprecated__
#endif
#endif

#ifndef __attribute_unused__
#ifdef __GNUC__
#define __attribute_unused__ __attribute__((unused))
#else
#define __attribute_unused__
#endif
#endif

#endif
