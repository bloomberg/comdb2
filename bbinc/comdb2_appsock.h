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

#ifndef __INCLUDED_COMDB2_APPSOCK_H
#define __INCLUDED_COMDB2_APPSOCK_H

enum {
    /* Whether it's an SQL handler? */
    APPSOCK_FLAG_IS_SQL = 1 << 0,
};

/* Return codes */
enum {
    APPSOCK_RETURN_OK = 0,
    APPSOCK_RETURN_ERR = 1,
    APPSOCK_RETURN_CONT = 2,
};

struct comdb2_appsock_arg {
    struct thr_handle *thr_self;
    struct dbenv *dbenv;
    struct dbtable *tab; /* Changed on the execution of 'use' */
    int conv_flags;      /* Changed on the execution of 'lendian' */
    SBUF2 *sb;
    char *cmdline;
    int *keepsocket;
    int error; /* internal error code */
    int admin;
};
typedef struct comdb2_appsock_arg comdb2_appsock_arg_t;

struct comdb2_appsock {
    /* Used to perform case-sensitive lookup for the appsock handler */
    const char *name;
    /* Usage message */
    const char *usage;
    /* Execution count, incremented atomically */
    uint32_t exec_count;
    /* appsock-specific flags */
    int flags;
    /* The handler function */
    int (*appsock_handler)(struct comdb2_appsock_arg *);
};
typedef struct comdb2_appsock comdb2_appsock_t;

#define APPSOCK_PLUGIN_DESC(X)                                                 \
    comdb2_appsock_t X##_plugin = {                                            \
        #X,                  /* Name */                                        \
        "",                  /* Usage info */                                  \
        0,                   /* Execution count */                             \
        0,                   /* Flags */                                       \
        handle_##X##_request /* Handler function */                            \
    }

#endif /* ! __INCLUDED_COMDB2_APPSOCK_H */
