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

/** @file str0.c Safe string manipulation routines. */

#include <str0.h>

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <logmsg.h>

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
{
    int rc;
    va_list ap;

    if (buf == NULL || bufsize == 0) {
        logmsg(LOGMSG_ERROR, "snprintf0(%d): ERROR: bad buffer @%p(%zu)\n",
                (int)getpid(), buf, bufsize);
        return -1;
    }

    if (fmt == NULL) {
        logmsg(LOGMSG_ERROR, "snprintf0(%d): ERROR: NULL format\n", (int)getpid());
        return -1;
    }

    va_start(ap, fmt);
    rc = vsnprintf(buf, bufsize, fmt, ap);
    va_end(ap);
    buf[bufsize - 1] = '\0';
    return rc;
}

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

char *strncpy0(char *dst, const char *src, size_t n)
{

    if (dst == NULL) {
        logmsg(LOGMSG_ERROR, "strncpy0(%d): ERROR: NULL dst\n", (int)getpid());
        return NULL;
    }

    if (src == NULL) {
        logmsg(LOGMSG_ERROR, "strncpy0(%d): ERROR: NULL src\n", (int)getpid());
        return NULL;
    }

    if (n == 0) {
        logmsg(LOGMSG_ERROR, "strncpy0(%d): ERROR: bad buffer size: %zu\n",
                (int)getpid(), n);
        return NULL;
    }

    {
        const size_t bytesToCopy = n - 1;
        char *p = strncpy(dst, src, bytesToCopy);
        dst[bytesToCopy] = '\0';
        return p;
    }
}

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

char *strncat0(char *dst, const char *src, size_t n)
{
    size_t dst_len;

    if (dst == NULL) {
        logmsg(LOGMSG_ERROR, "strncat0(%d): ERROR: NULL dst\n", (int)getpid());
        return NULL;
    }

    if (src == NULL) {
        logmsg(LOGMSG_ERROR, "strncat0(%d): ERROR: NULL src\n", (int)getpid());
        return NULL;
    }

    dst_len = strlen(dst);

    if (n <= dst_len) {
        logmsg(LOGMSG_ERROR, "strncat0(%d): ERROR: bad buffer size: %zu\n",
                (int)getpid(), n);
        return NULL;
    }

    strncpy(dst + dst_len, src, n - dst_len);
    dst[n - 1] = '\0';
    return dst;
}

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
{
    int rc;
    va_list ap;
    size_t len;

    if (buf == NULL || bufsize == 0) {
        logmsg(LOGMSG_ERROR, "strncatf0(%d): ERROR: bad buffer @%p(%zu)\n",
                (int)getpid(), buf, bufsize);
        return -1;
    }

    if (fmt == NULL) {
        logmsg(LOGMSG_ERROR, "strncatf0(%d): ERROR: NULL format\n", (int)getpid());
        return -1;
    }

    len = strlen(buf);

    if (len >= bufsize) {
        logmsg(LOGMSG_ERROR, "strncatf0(%d): ERROR: string longer than buffer "
                        "size (%zu >= %zu)\n",
                (int)getpid(), len, bufsize);
        return -1;
    }

    va_start(ap, fmt);
    rc = vsnprintf(buf + len, bufsize - len, fmt, ap);
    va_end(ap);
    buf[bufsize - 1] = '\0';
    return rc;
}

/** Safe version of vsnprint. This function will never overflow the
 * destination string (unlike vsprintf) and will always be null-terminated,
 * as long as the string size passed in is correct.
 *
 * @param buf Buffer to place the output in.
 * @param bufsize Buffer size
 * @param fmt printf-like format string
 * @param ap vsprintf-like list of variable argument list
 *
 * @return Number of characters formatted (that is, the number of characters
 * that would have been written to the buffer if it were large enough).
 * Negative value upon error. See vsnprintf() for more details.
 *
 * @note Both DG/UX and HP-UX always return -1 if the buffer is too short.
 */

int vsnprintf0(char *buf, size_t bufsize, const char *fmt, va_list ap)
{
    int rc;

    if (!buf || !bufsize) {
        logmsg(LOGMSG_ERROR, "vsnprintf0(%d): ERROR: bad buffer @%p(%zu)\n",
                (int)getpid(), buf, bufsize);
        return -1;
    }

    if (!fmt) {
        logmsg(LOGMSG_ERROR, "vsnprintf0(%d): ERROR: NULL format\n", (int)getpid());
        return -1;
    }

    rc = vsnprintf(buf, bufsize, fmt, ap);
    buf[bufsize - 1] = '\0';
    return rc;
}

#if NEED_STRNLEN_DEFN
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

size_t strnlen(const char *s, size_t maxlen)
{
    const char *p = memchr(s, '\0', maxlen);
    return p ? (size_t)(p - s) : maxlen;
}
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

size_t strnlen0(const char *s, size_t maxlen)
{
    if (s == NULL) {
        logmsg(LOGMSG_ERROR, "strnlen0(%d): ERROR: NULL s with %d maxlen\n",
                (int)getpid(), (int)maxlen);
        return 0;
    }

    return strnlen(s, maxlen);
}

#ifdef TEST_APP

#define ARRAY_SIZE(a) (sizeof((a)) / sizeof((a)[0]))

static long long int eightbytes(void) { return 0; }

int main()
{
    size_t i;
    char buf[40]; /* output buffer */

    struct {
        char *src;     /* source string */
        size_t reslen; /* expected result length (no trailing 0) */
    } tst_data[] = {
        /* source strings */
        {"12345678911234567892", 20},                     /* small string */
        {"123456789112345678921234567893123456789", 39},  /* bufsize-1 */
        {"1234567891123456789212345678931234567894", 39}, /* bufsize */
        {"12345678911234567892123456789312345678941234567895", 39}, /* big */
    };

    struct {
        char *src;     /* source string */
        size_t maxlen; /* maximum length */
        size_t reslen; /* expected result length (no trailing 0) */
    } strnlen_tst_data[] = {
        /* source strings */
        {"", 200, 0},
        {"", 0, 0},
        {"x", 0, 0},
        {"x", 1, 1},
        {"xy", 1, 1},
        {"xy", 2, 2},
        {"xy", 3, 2},
        {"xyz", 1, 1},
        {"xyz", 2, 2},
        {"xyz", 3, 3},
        {"xyz", 4, 3},
    };

    for (i = 0; i < (sizeof(tst_data) / sizeof(tst_data[0])); i++) {
        buf[0] = '\0';

        if (!strncpy0(buf, tst_data[i].src, sizeof(buf))) {
            fprintf(stderr, "ERROR: strncpy0, test #%d failed\n", (int)i);
            exit(EXIT_FAILURE);
        }

        if (strlen(buf) != tst_data[i].reslen) {
            fprintf(stderr, "ERROR: strncpy0, test #%d:\n"
                            "  src: '%s'\n"
                            "  dst: '%s' (len: %zu, exp: %zu)\n",
                    (int)i, tst_data[i].src, buf, strlen(buf),
                    tst_data[i].reslen);
            exit(EXIT_FAILURE);
        }

        if (strncmp(buf, tst_data[i].src, tst_data[i].reslen)) {
            fprintf(stderr, "ERROR: strncpy0, test #%d:\n"
                            "  src: '%s'\n"
                            "  dst: '%s'\n",
                    (int)i, tst_data[i].src, buf);
            exit(EXIT_FAILURE);
        }

        buf[0] = '\0';

        if (snprintf0(buf, sizeof(buf), "%s", tst_data[i].src) < 0) {
            fprintf(stderr, "ERROR: snprintf0, test #%d failed\n", (int)i);
            exit(EXIT_FAILURE);
        }

        if (strlen(buf) != tst_data[i].reslen) {
            fprintf(stderr, "ERROR: snprintf0, test #%d:\n"
                            "  src: '%s'\n"
                            "  dst: '%s' (len: %zu, exp: %zu)\n",
                    (int)i, tst_data[i].src, buf, strlen(buf),
                    tst_data[i].reslen);
            exit(EXIT_FAILURE);
        }

        if (strncmp(buf, tst_data[i].src, tst_data[i].reslen)) {
            fprintf(stderr, "ERROR: snprintf0, test #%d:\n"
                            "  src: '%s'\n"
                            "  dst: '%s'\n",
                    (int)i, tst_data[i].src, buf);
            exit(EXIT_FAILURE);
        }
    }

    for (i = 0; i < ARRAY_SIZE(strnlen_tst_data); ++i) {
        size_t res;

        if ((res = strnlen(strnlen_tst_data[i].src,
                           strnlen_tst_data[i].maxlen)) !=
            strnlen_tst_data[i].reslen) {
            fprintf(stderr, "ERROR: strnlen, test #%d:\n"
                            "  src: '%s'\n"
                            "  maxlen: %d\n"
                            "  result: %d, exp: %d\n",
                    (int)i, strnlen_tst_data[i].src,
                    (int)strnlen_tst_data[i].maxlen, (int)res,
                    (int)strnlen_tst_data[i].reslen);
            exit(EXIT_FAILURE);
        }
    }

    fprintf(
        stderr,
        "Next message should be a complaint from strnlen0 that s is NULL.\n");
    if ((i = strnlen0(NULL, 42))) {
        fprintf(stderr, "ERROR: strnlen0(NULL, 42) returned %d instead of 0.\n",
                (int)i);
        exit(EXIT_FAILURE);
    }

    printf("all tests passed\n");
    printf("implementation sizes in bytes:\n"
           "int = %d\n"
           "long long int = %d\n"
           "fn() returning long long int = %d\n"
           "size_t = %d\n"
           "strlen() = %d\n"
           "strnlen() = %d\n"
           "strnlen0() = %d\n",
           (int)sizeof(int), (int)sizeof(long long int),
           (int)sizeof(eightbytes()), (int)sizeof(size_t),
           (int)sizeof(strlen("foo")), (int)sizeof(strnlen("foo", 100)),
           (int)sizeof(strnlen0("foo", 100)));
    exit(EXIT_SUCCESS);
}

#endif /* TEST_APP */
