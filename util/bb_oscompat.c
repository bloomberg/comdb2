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

/**
 * The idea of this module is to (where possible) provide a single interface to
 * library/OS calls that vary from system to system.  If this file gets too big,
 * we may want to split it out into bb_oscompat_<arch>.c.  I'm starting with one
 * common file because I don't think cscheckin/robocop can cope with the above
 *well.
 **/

#if defined(_SUN_SOURCE)
#ifndef _REENTRANT
#define _REENTRANT
#endif
#endif
#if defined(_LINUX_SOURCE)
#define __USE_MISC /* getservbyname_r() */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE /* required for "struct ucred" in GLIBC >= 2.8 */
#endif
#endif

/* get the standard ctime_r() on Sun, among other things
 * gcc defines this */
#ifndef _POSIX_PTHREAD_SEMANTICS
#define _POSIX_PTHREAD_SEMANTICS
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <bb_stdint.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <stdint.h>
#include <inttypes.h>

#if defined(_IBM_SOURCE)
#include <sys/procfs.h>
#include <procinfo.h>
#elif defined(__linux__)
#include <sys/time.h>
#include <asm/param.h> /* HZ */
#endif

#if defined(_SUN_SOURCE)
#include <ucred.h>
#elif defined(__linux__)
#include <linux/socket.h>
#endif

#include <bb_oscompat.h>
#include <sysutil_stdbool.h>
#include "logmsg.h"

/* helper routine to allocate a thread-specific buffer associated with a given
 * key
 * this would all be simpler if the pthread_once init routine took an argument
 * and the
 * key destructor function got the key as an argument */
static void *__compat_allocate_thread_buffer(pthread_once_t *once,
                                             void (*keyalloc_routine)(void),
                                             pthread_key_t *key, int bufsize)
{
    void *buf;
    int rc;

    rc = pthread_once(once, keyalloc_routine);
    if (rc != 0) {
        logmsgperror("__compat_allocate_thread_buffer:pthread_once");
        return NULL;
    }

    if ((buf = pthread_getspecific(*key)) == NULL) {
        buf = malloc(bufsize);
        if (buf == NULL) {
            logmsgperror("__compat_allocate_thread_buffer:malloc");
            return NULL;
        }

        rc = pthread_setspecific(*key, buf);
        if (rc != 0) {
            logmsgperror("__compat_allocate_thread_buffer:pthread_setspecific");
            free(buf);
            return NULL;
        }
    }

    return buf;
}

/* GETHOSTBYNAME/GETHOSTBYADDR: NATIVELY USES THREAD STORAGE ON IBM+HP */

#if defined(_SUN_SOURCE) || defined(_LINUX_SOURCE)

struct __compat_gethoststar_buffer {
    struct hostent hostent;
    char data[4096];
};

static pthread_once_t gethoststar_once = PTHREAD_ONCE_INIT;
static pthread_key_t gethoststar_key;

static void __compat_free_gethoststar_buffer(void *buf)
{
    int rc;
    rc = pthread_setspecific(gethoststar_key, NULL);
    if (rc != 0)
        logmsgperror("__compat_free_gethoststar_buffer:pthread_setspecific");
    free(buf);
}

static void __compat_allocate_gethoststar_key()
{
    int rc;
    rc = pthread_key_create(&gethoststar_key, __compat_free_gethoststar_buffer);
    if (rc != 0)
        logmsgperror("__compat_allocate_gethoststar_key:pthread_key_create");
}

static struct hostent *__compat_gethostbyname(const char *name)
{
    struct __compat_gethoststar_buffer *buf;

    buf = __compat_allocate_thread_buffer(
        &gethoststar_once, __compat_allocate_gethoststar_key, &gethoststar_key,
        sizeof(struct __compat_gethoststar_buffer));
    if (buf == NULL)
        return NULL;

#if defined(__APPLE__)
        // todo use getaddrinfo instead
        return gethostbyname(name);
#elif defined(_SUN_SOURCE)
    return gethostbyname_r(name, &buf->hostent, buf->data, sizeof(buf->data),
                           &h_errno);
#elif defined(_LINUX_SOURCE)
    struct hostent *result;
    gethostbyname_r(name, &buf->hostent, buf->data, sizeof(buf->data), &result,
                    &h_errno);
    return result;
#endif
}

#endif /* defined(_SUN_SOURCE) || defined(_LINUX_SOURCE) */

struct hostent *comdb2_gethostbyname(const char *name)
{
#if defined(_SUN_SOURCE) || defined(_LINUX_SOURCE)
    return __compat_gethostbyname(name);
#elif defined(_IBM_SOURCE) || defined(_HP_SOURCE)
    /* IBM/HP's gethostbyname() is already thread-safe
       Darwin uses thread-local storage for this */
    return gethostbyname(name);
#else
#error "unsupported architecture"
#endif
}


/* GETSERVBYNAME: NATIVELY USES THREAD STORAGE ON IBM/HP/macOS */

#if defined(_SUN_SOURCE) || defined(__linux__)

struct __compat_getservbyname_buffer {
    struct servent servent;
    char data[4096];
};

static pthread_once_t getservbyname_once = PTHREAD_ONCE_INIT;
static pthread_key_t getservbyname_key;

static void __compat_free_getservbyname_buffer(void *buf)
{
    int rc;
    rc = pthread_setspecific(getservbyname_key, NULL);
    if (rc != 0)
        logmsgperror("__compat_free_getservbyname_buffer:pthread_setspecific");
    free(buf);
}

static void __compat_allocate_getservbyname_key()
{
    int rc;
    rc = pthread_key_create(&getservbyname_key,
                            __compat_free_getservbyname_buffer);
    if (rc != 0)
        logmsgperror("__compat_allocate_getservbyname_key:pthread_key_create");
}

static struct servent *__compat_getservbyname(const char *name,
                                              const char *proto)
{
    struct __compat_getservbyname_buffer *buf;
    buf = __compat_allocate_thread_buffer(
        &getservbyname_once, __compat_allocate_getservbyname_key,
        &getservbyname_key, sizeof(struct __compat_getservbyname_buffer));
    if (buf == NULL)
        return NULL;

#if defined(_SUN_SOURCE)
    return getservbyname_r(name, proto, &buf->servent, buf->data,
                           sizeof(buf->data));
#elif defined(__linux__)
    struct servent *result;
    getservbyname_r(name, proto, &buf->servent, buf->data, sizeof(buf->data),
                    &result);
    return result;
#endif
}

#endif /* defined(_SUN_SOURCE) || defined(__linux__) */

struct servent *comdb2_getservbyname(const char *name, const char *proto)
{
#if defined(_SUN_SOURCE) || defined(__linux__)
    return __compat_getservbyname(name, proto);
#elif defined(_IBM_SOURCE) || defined(_HP_SOURCE) || defined(__APPLE__)
    /* IBM/HP's getservbyname() is already thread-safe */
    return getservbyname(name, proto);
#else
#error "unsupported architecture"
#endif
}

/* TMPNAM: NATIVELY USES THREAD STORAGE ON SOLARIS */
#if defined(_IBM_SOURCE) || defined(_HP_SOURCE)
struct __compat_tmpnam_buffer {
    char data[L_tmpnam];
};

/* 'PTHREAD_ONCE_INIT' initializer on AIX is missing
 * a set of braces which is making GCC compilation fail. */
/* apparently no longer necessary */
static pthread_once_t tmpnam_once = PTHREAD_ONCE_INIT;
static pthread_key_t tmpnam_key;

static void __compat_free_tmpnam_buffer(void *buf)
{
    int rc;
    rc = pthread_setspecific(tmpnam_key, NULL);
    if (rc != 0)
        logmsgperror("__compat_free_tmpnam_buffer:pthread_setspecific");
    free(buf);
}

static void __compat_allocate_tmpnam_key()
{
    int rc;
    rc = pthread_key_create(&tmpnam_key, __compat_free_tmpnam_buffer);
    if (rc != 0)
        logmsgperror("__compat_allocate_tmpnam_key:pthread_key_create");
}

static char *__compat_tmpnam(char *s)
{
    struct __compat_tmpnam_buffer *buf;

    buf = __compat_allocate_thread_buffer(
        &tmpnam_once, __compat_allocate_tmpnam_key, &tmpnam_key,
        sizeof(struct __compat_tmpnam_buffer));
    if (buf == NULL)
        return NULL;

    return tmpnam(buf->data);
}
#endif

#if !defined(_LINUX_SOURCE)
char *bb_tmpnam(char *s)
{
    if (s != NULL)
        return tmpnam(s);
    else {
#if defined(_SUN_SOURCE)
        /* Solaris's tmpnam(NULL) is already thread-safe */
        return tmpnam(NULL);
#elif defined(_IBM_SOURCE) || defined(_HP_SOURCE)
        return __compat_tmpnam(NULL);
#else
#error "unsupported architecture"
#endif
    }
}
#endif

int bb_readdir(DIR *d, void *buf, struct dirent **dent) {
#ifdef _LINUX_SOURCE
    struct dirent *rv;
    *dent = rv = readdir(d);
    if (rv == NULL)
        return errno;
    /* rv->d_reclen is the actual size of rv.
       It may not match sizeof(struct dirent). */
    memcpy(buf, rv, rv->d_reclen);
    return 0;
#else
    int rc;
    return readdir_r(d, buf, dent);
#endif
}

#ifdef BB_OSCOMPAT_TESTPROGRAM
#include <assert.h>
#include <signal.h>
#include <sys/wait.h>

/* expect threads to have different buffers but each thread to always reuse his
 */
static void *test_bb_gethostservstar_thread(void *v)
{
    struct hostent *prev_hentname = NULL, *prev_hentaddr = NULL;
    struct servent *prev_sent = NULL;
    char *fname;
    struct sockaddr_in addr;
    char buf[32];
    int ii;

    for (ii = 0; ii < 2; ii++) {
        struct hostent *hentname;
        struct servent *sent;

        /* GETHOSTBYNAME */
        hentname = comdb2_gethostbyname("sundev9");
        if (hentname == NULL) {
            fprintf(stderr, "comdb2_gethostbyname failed: h_errno=%d\n",
                    h_errno);
            return NULL;
        }

        memcpy((caddr_t)&addr.sin_addr, hentname->h_addr, hentname->h_length);
        inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf));
        fprintf(stderr, "tid %d iter %d:  %s (hent = %p)\n", pthread_self(), ii,
                buf, hentname);
        if (prev_hentname == NULL)
            prev_hentname = hentname;
        assert(prev_hentname == hentname); /* EXPECT SAME ADDRESS EACH TIME */

        /* GETSERVBYNAME */
        sent = comdb2_getservbyname("bigrcv", "tcp");
        if (sent == NULL) {
            fprintf(stderr, "comdb2_getservbyname failed\n");
            return NULL;
        }
        fprintf(stderr, "tid %d iter %d:  %s -> %d (sent = %p)\n",
                pthread_self(), ii, sent->s_name, ntohs(sent->s_port), sent);
        if (prev_sent == NULL)
            prev_sent = sent;
        assert(prev_sent == sent); /* EXPECT SAME ADDRESS EACH TIME */

        /* EXPECT GETSERVBYNAME TO USE DIFFERENT STORAGE */
        assert((void *)hentname != (void *)sent);

#ifndef _LINUX_SOURCE
        fname = bb_tmpnam(NULL);
        fprintf(stderr, "tid %d iter %d: %s (%p)\n", pthread_self(), ii, fname,
                fname);
#endif
    }

    /* prevent buffer free (thread exit) until all threads have started */
    sleep(1);

    return NULL;
}

static int test_bb_gethostservstar()
{
    pthread_t tids[3];
    int rc;
    int ii;

    for (ii = 0; ii < 3; ii++) {
        rc = pthread_create(&tids[ii], NULL, test_bb_gethostservstar_thread,
                            NULL);
        assert(rc == 0);
    }

    for (ii = 0; ii < 3; ii++)
        pthread_join(tids[ii], NULL);

    return 0;
}

int main(int argc, char **argv)
{
    char buffer[1024];

    printf("tmpnam(0)   %s\n", tmpnam(NULL));
    printf("tmpnam(buf) %s\n", tmpnam(buffer));

    test_bb_gethostservstar();

    return 0;
}

#endif
