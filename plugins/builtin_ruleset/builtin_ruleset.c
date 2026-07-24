/*
   Copyright 2026 Bloomberg Finance L.P.

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

#include "comdb2.h"
#include "comdb2_plugin.h"
#include "comdb2_ruleset.h"
#include "comdb2_initializer.h"
#include "logmsg.h"

static int init_opcode_plugin(void *unused)
{
    return 0;
}

int add_ruleset(void)
{
#include "ruleset.h"
    int rc = comdb2_load_ruleset_buf(system_ruleset, &gbl_ruleset);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Couldn't load system ruleset\n");
        return -1;
    }
    return 0;
}

comdb2_initializer_t builtin_ruleset_plugin = {.post_lrl = add_ruleset, .post_recovery = NULL};

#define PLUGIN_DESC(X)                                                                                                 \
    {                                                                                                                  \
        #X,                               /* Plugin identifier */                                                      \
            #X " builtin ruleset plugin", /* Plugin description */                                                     \
            COMDB2_PLUGIN_INITIALIZER,    /* Plugin type */                                                            \
            1,                            /* Plugin version */                                                         \
            1,                            /* Plugin interface version */                                               \
            0,                            /* Plugin flags */                                                           \
            init_opcode_plugin,           /* Initialization function */                                                \
            NULL,                         /* Destroy function */                                                       \
            &builtin_ruleset_plugin       /* Plugin-specific data */                                                   \
    }

#include "plugin.h"
