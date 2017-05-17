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

#ifndef INCLUDED_FSNAPF
#define INCLUDED_FSNAPF

#include <stdio.h>

#if defined __cplusplus
extern "C" {
#endif

/* Callback print function for fsnap.  The first argument given is a context
 * pointer which gets passed through from fsnap.  The second argument is a
 * printf style format string.  This is followed by the format arguments.
 * This should return <0 on error in which case the output will stop.
 * Note that the callback is designed this way so that you can use fprintf
 * as the callback with a FILE* as the context. */
typedef int (*fsnap_callback_type)(void *context, const char *fmt, ...);

/* dump hex to a file.  for historical reasons len is an integer argument;
 * I don't want to change this as many things rely upon it. */
void fsnapf(FILE *fil, const void *buf, int len);

/* dump hex to a custom 'print' callback.  prefix is optional; pass NULL if
 * not required. */
void fsnapp(const char *prefix, const void *buf, size_t len,
            fsnap_callback_type callback, void *context);

/* prefix string */
void fsnap_prfx(FILE *fil, const char *prfx, const void *buf, size_t len);

#if defined __cplusplus
}
#endif

#endif
