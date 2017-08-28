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

#ifndef INCLUDED_BB_OSCOMPAT_H
#define INCLUDED_BB_OSCOMPAT_H

#include <stdint.h>
#include <sys/types.h>
#include <time.h>
#include <limits.h>
#include <dirent.h>


#ifdef __cplusplus
extern "C" {
#endif

#ifndef PATH_MAX
#ifdef MAXPATHLEN
#define PATH_MAX MAXPATHLEN
#else
#define PATH_MAX 1024
#endif
#endif

/**
 * this routine is thread-safe.  it uses pthread keys and gethostbyname_r
 *internally
 * on systems that don't natively provide a thread-safe gethostbyname().
 * if it returns NULL, the rcode is found in h_errno.
 * NOTE: for a given thread, the same buffer is reused with each call.  you
 *should save
 *       any data that needs to persist.  making a shallow copy is not
 *sufficient.
 **/
struct hostent *bb_gethostbyname(const char *name);

/**
 * Similar semantics to bb_gethostbyname.
 * This uses a different thread-safe buffer than bb_gethost*
 **/
struct servent *bb_getservbyname(const char *name, const char *proto);

#ifndef _LINUX_SOURCE
/**
 * Solaris-like thread-safe semantics on all systems.
 * If 's' is NULL, returns a pointer to thread-local storage.
 * (No Linux implementation because gnu ld gives us an annoying warning:
 *  warning: the use of `tmpnam' is dangerous, better use `mkstemp')
 */
char *bb_tmpnam(char *s);
#endif

int bb_readdir(DIR *d, void *buf, struct dirent **dent);

#ifdef __cplusplus
}
#endif

#endif
