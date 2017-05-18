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

/** @file str0.h Safe string manipulation routines. */

#ifndef INCLUDED_STR0_H
#define INCLUDED_STR0_H

#if defined(__sun) || defined(__hpux)
#define NEED_STRNLEN_DECL 1
#define NEED_STRNLEN_DEFN 1
#endif

#if defined(linux) || defined(__linux) || defined(__linux__)
#if defined(__USE_GNU)
#define NEED_STRNLEN_DECL 0
#else
#define NEED_STRNLEN_DECL 1
#endif
#define NEED_STRNLEN_DEFN 0
#endif

#include <stdarg.h> /* MUST be the 1st .h file (DG/UX bug) */
#include <stdlib.h>
#include <string.h>

#include <sysutil_compilerdefs.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Safe version of sprintf().
 *
 * This function will never overflow the
 * buffer passed to it (unlike sprintf), as long as the buffer size passed
 * in is correct. It is also guaranteed to zero-terminate the character string
 * generated (unlike snprintf()).
 *
 * @param buf Buffer to place the output in.
 * @param bufsize Buffer size
 * @param fmt printf-like format string and arguments
 *
 * @return Number of characters formatted (that is, the number of characters
 * that would have been written to the buffer if it were large enough).
 * Negative value upon error. See snprintf() for more details.
 *
 * @note Both DG/UX and HP-UX always return -1 if the buffer is too short.
 */

int snprintf0(char *buf, size_t bufsize, const char *fmt, ...)
    __attribute_format__((printf, 3, 4));

/** Safe version of strcpy(), more reliable than strncpy().
 *
 * This function will never overflow the
 * destination string (unlike strcpy), as long as the string size passed
 * in is correct. It is also guaranteed to zero-terminate the destination
 * string (unlike strncpy()).
 *
 * @param dst Destination string.
 * @param src Source string.
 * @param n Maximum number of characters to copy.
 *
 * @return The dst argument upon success. NULL upon errors. See strncpy() for
 * more details.
 */

char *strncpy0(char *dst, const char *src, size_t n);

/** Safe version of strcat. This function will never overflow the
 * destination string (unlike strcat), as long as the string size passed
 * in is correct.
 *
 * @param dst Destination string.
 * @param src Source string.
 * @param n Size of the buffer containing the destination string. The resulting
 * string will be at most n-1 characters plus a null terminator.
 *
 * @return The dst argument upon success. NULL upon errors. See strncpy() for
 * more details.
 *
 * @note This function has different semantics than strncat(). Unlike the
 * latter, it uses the 3rd argument to denote the total size of dst.
 */

char *strncat0(char *dst, const char *src, size_t n);

/** Concatenate formatted output to the end of the string.
 *
 * This function formats its arguments in a manner identical to sprintf()
 * and then attaches the result to the end of the output buffer. It is
 * basically similar in effect to an snprintf0() followed by strncat0().
 *
 * @param buf Buffer to attach the output to. It is expected to contain a valid
 * nul-terminated string.
 * @param bufsize Total buffer size.
 * @param fmt printf-like format string and arguments
 *
 * @return Number of characters formatted (that is, the number of characters
 * that would have been written to the buffer if it were large enough).
 * Negative value upon error. See snprintf() for more details.
 *
 * @note Both DG/UX and HP-UX always return -1 if the buffer is too short.
 */

int strncatf0(char *buf, size_t bufsize, const char *fmt, ...)
    __attribute_format__((printf, 3, 4));

/** Safe version of vsprintf. This function will never overflow the
 * destination string (unlike vsprintf), as long as the string size passed
 * in is correct.
 *
 * @param buf Buffer to place the output in.
 * @param bufsize Buffer size
 * @param fmt printf-like format string and arguments
 * @ap    vprintf-like list of variable argument list
 *
 * @return Number of characters formatted (that is, the number of characters
 * that would have been written to the buffer if it were large enough).
 * Negative value upon error. See snprintf() for more details.
 *
 * @note Both DG/UX and HP-UX always return -1 if the buffer is too short.
 */

int vsnprintf0(char *buf, size_t bufsize, const char *fmt, va_list ap)
    __attribute_format__((printf, 3, 0));

#if NEED_STRNLEN_DECL
/** Safe version of strlen().
 *
 * This function will search no further than the maximum number of bytes
 * specified. If NUL is not found, it'll return that maximum length as
 * the length.
 *
 * @param s Source string.
 * @param maxlen Maximum number of characters to search for NUL.
 *
 * @return Length of the string or maxlen, whichever is smaller.
 */

size_t strnlen(const char *s, size_t maxlen);
#endif

/** str0-style wrapper for strnlen().
 *
 * This function checks the s argument. If NULL, it prints a diagnostic
 * to stderr and returns 0. Otherwise it returns the result of calling
 * strnlen() with the same arguments.
 *
 * @param s Source string. If NULL, prints a diagnostic to stderr and
 * returns 0.
 * @param maxlen Maximum number of characters to search for NUL.
 *
 * @return Length of the string or maxlen, whichever is smaller.
 */

size_t strnlen0(const char *s, size_t maxlen);

#ifdef __cplusplus
}
#endif

#endif /* INCLUDED_STR0_H */
