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

/*
 * cheapstub.c
 *
 * stack_pc_getlist is not implemented on linux.  This causes crashes
 * periodically.  Until this is fixed, I'm stubbing these out.
 *
 * $Id: cheapstub.c 2010-09-24 14:05:38Z mhannum $
 */

#include <stdio.h>
#include <inttypes.h>
#include <ucontext.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#if defined(_LINUX_SOURCE) || defined(_SUN_SOURCE)
#  include <execinfo.h>
#endif

#include <logmsg.h>

/* Backtrace is only available in glibc */
#ifdef __GLIBC__
extern int backtrace(void **, int);
extern void backtrace_symbols_fd(void *const *, int, int);
#else
#define backtrace(A, B) 1
#define backtrace_symbols_fd(A, B, C)
#endif

static void cheapstub(FILE *f)
{
    if (f == NULL)
        f=stdout;
    pthread_t tid = pthread_self();
    const char size = 32;
    void *buf[size];
    int n = backtrace(buf, size);

    logmsgf(LOGMSG_USER, f,
            "tid=0x%lx(%u) stack trace, run addr2line -f -e <exe> on: \n", tid,
            (uint32_t)tid);
    for  (int i = 2; i < n; ++i) {
        logmsgf(LOGMSG_USER, f, "%p ", buf[i]);
    }
    logmsgf(LOGMSG_USER,  f, "\n");

#ifdef DEBUG
    char **funcs = backtrace_symbols(buf, n);
    if (funcs == NULL) 
        n = 0;

    int i;
    for  (i = 0; i < n; ++i) {
        char *name = funcs[i];

        while (*name && *name != '(') ++name;
        if (! (*name)) continue;
        ++name;
        int j = 0;
        while (name[j] && name[j++] != '+');
        name[j] = '\0';
        if (strcmp(name, "comdb2_linux_cheap_stack_trace") == 0) continue;
        if (strcmp(name, "cheap_stack_trace") == 0) continue;
        logmsgf(LOGMSG_USER, f, "0x%x(%u): %s\n", tid, tid, name);
    }
    free(funcs);
#endif
}

void comdb2_linux_cheap_stack_trace(void)
{
    cheapstub(NULL);
}

void comdb2_cheap_stack_trace_file(FILE *f)
{
    cheapstub(f);
}
