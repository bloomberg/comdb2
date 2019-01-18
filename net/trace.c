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

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <time.h>
#include <pthread.h>

#include "pthread_wrap.h"
#include "net.h"
#include "net_int.h"
#include "logmsg.h"

enum {
    NETTRCF_SUPPRESS = 1 /* suppress this trace if if distress mode */
};

static pthread_mutex_t trace_lock = PTHREAD_MUTEX_INITIALIZER;

static void host_node_vprintf(loglvl lvl, host_node_type *host_node_ptr, 
                              int flags, const char *fmt, va_list ap)
{
    struct netinfo_struct *netinfo_ptr = host_node_ptr->netinfo_ptr;

    /* While my net changes bed in, I want to see all trace even if in
     * "distress" mode.  Later perhaps we can redirect this to ctrace. */
    /*
    if((flags & NETTRCF_SUPPRESS) && host_node_ptr->distress)
       return;
    */

    Pthread_mutex_lock(&trace_lock);
    logmsg(lvl, "%08lx [%s %s%s fd %d] ", pthread_self(), netinfo_ptr->service,
           host_node_ptr->host, host_node_ptr->subnet, host_node_ptr->fd);
    logmsgv(lvl, fmt, ap);
    Pthread_mutex_unlock(&trace_lock);
}

void host_node_printf(loglvl lvl, host_node_type *host_node_ptr, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    host_node_vprintf(lvl, host_node_ptr, NETTRCF_SUPPRESS, fmt, ap);
    va_end(ap);
}

void host_node_errf(loglvl lvl, host_node_type *host_node_ptr, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    host_node_vprintf(lvl, host_node_ptr, 0, fmt, ap);
    va_end(ap);
}
