/*
   Copyright 2017 Bloomberg Finance L.P.

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

#ifndef __INCLUDED_COMDB2_PLUGIN_APPSOCK_H
#define __INCLUDED_COMDB2_PLUGIN_APPSOCK_H

enum {
    /* Cache the connection */
    APPSOCK_FLAG_CACHE_CONN = 1 << 1,
};

struct comdb2_appsock_arg {
    struct thr_handle *thr_self;
    struct dbenv *dbenv;
    struct dbtable *tab; /* Changed on the execution of 'use' */
    SBUF2 *sb;
    char *cmdline;
};
typedef struct comdb2_appsock_arg comdb2_appsock_arg_t;

struct comdb2_appsock {
    /* Used to perform case-sensitive lookup for the appsock handler */
    const char *name;
    /* Usage message */
    const char *usage;
    /* Execution count, incremented atomically */
    unsigned long long exec_count;
    /* appsock-specific flags */
    int flags;
    /* The handler function */
    int (*appsock_handler)(struct comdb2_appsock_arg *);
};
typedef struct comdb2_appsock comdb2_appsock_t;

#endif /* ! __INCLUDED_COMDB2_PLUGIN_APPSOCK_H */
