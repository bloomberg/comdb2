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

#ifndef INCLUDED_SAFESTRERROR
#define INCLUDED_SAFESTRERROR

/*
 * This provides a thread safe wrapper around strerror.  The strerror()
 * situation appears to be a little sad.  strerror() unfortunately isn't
 * required to be reentrant by the POSIX standard, even though it would
 * seem likely that most implementations would just return a
 * pointer to a string literal.  However, some implementations work by
 * populating a static string and returning the string buffer - so are not
 * thread safe.  Allegedly some implementations do return pointers to
 * string literals given valid error numbers, and only format a string if
 * the error number is invalid.
 *
 * strerror_r() in theory is the solution to these problems, but itself is
 * not quite perfect as its interface raises a new problem.  If given a buffer
 * which is too short it can return ERANGE and not actually populate the
 * buffer, leading to problems like this:
 *
 * char buf[1];
 * strerror_r(errno, buf, sizeof(buf));
 * printf("Error is %s\n", b);
 * // Danger!  strerror_r probably returns ERANGE, and buf is uninitialised..
 *
 * Obviously buf needs to be larger than one byte, but there doesn't appear to
 * be any macro advertised as giving the minimum required length of the
 * buffer to provide.
 *
 * safestrerror() aims at providing a sane-as-possible wrapper for strerror_r()
 * which relieves everyone of the burden of trying to figure out the above
 * issues.  Just use STRERROR_BUFLEN as your buffer size, and call
 * safestrerror() to get your error string.
 */

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Recommended length of buffer to provide to safestrerror().  This length
 * seems to work O.K. on all four platforms at present (sun, ibm, hp, linux). */
#define STRERROR_BUFLEN 128

/* Safe (reentrant) strerror function, with error checking.  This function
 * takes an errno value as errnum and a buffer to populate as buf, with length
 * buflen bytes.  It always returns buf, which will always have been populated
 * with a null terminated string.  Behaviour is undefined unless buflen>=1.  In
 * general it is recommended that buflen be at least STRERROR_BUFLEN.
 *
 * The buffer is populated with the error number followed by the strerror_r()
 * error string, for example "20 Not a directory".  Why is this useful?
 * Occasionally I've come across error strings from which it has not been
 * immediately obvious which E macro is the ccorresponding macro - providing
 * the numericcal value as well as the text saves some time in these cases.
 */
char *safestrerror(int errnum, char *buf, size_t buflen);

/* Convenience macro (the _m is for macro) which ccalls safestrerror().
 *
 * Rather than this:
 *
 * char buf[STRERROR_BUFLEN];
 * fprintf(stderr, "Stuff went wrong: %s\n",
 *     safestrerror(errno, buf, sizeof(buf));
 *
 * Instead we can write this:
 *
 * char buf[STRERROR_BUFLEN];
 * fprintf(stderr, "Stuff went wrong: %s\n", strerror_m(errno, buf));
 *
 * Note that the second argument buf is expected to be the name of a char array,
 * and that the macro will use sizeof(buf) to deterine its size.
 */
#define strerror_m(errnum, buf) safestrerror((errnum), (buf), sizeof(buf))

#ifdef __cplusplus
}
#endif

#endif
