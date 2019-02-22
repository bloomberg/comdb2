/*
   Copyright 2018 Bloomberg Finance L.P.

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

#include "comdb2_plugin.h"
#include "comdb2.h"
#include "comdb2_appsock.h"

extern int handle_remsql_request(comdb2_appsock_arg_t *arg);
extern int handle_remtran_request(comdb2_appsock_arg_t *arg);
extern int handle_alias_request(comdb2_appsock_arg_t *arg);

comdb2_appsock_t remsql_plugin = {
    "remsql",             /* Name */
    "",                   /* Usage info */
    0,                    /* Execution count */
    0,                    /* Flags */
    handle_remsql_request /* Handler function */
};

comdb2_appsock_t remtran_plugin = {
    "remtran",             /* Name */
    "",                    /* Usage info */
    0,                     /* Execution count */
    0,                     /* Flags */
    handle_remtran_request /* Handler function */
};

comdb2_appsock_t alias_plugin = {
    "alias",             /* Name */
    "",                  /* Usage info */
    0,                   /* Execution count */
    0,                   /* Flags */
    handle_alias_request /* Handler function */
};

#include "plugin.h"
