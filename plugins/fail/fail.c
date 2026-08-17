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

#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "comdb2_plugin.h"
#include "comdb2_initializer.h"
#include "tunables.h"

int gbl_plugin_fail = 0;

static int fail_init(void *unused)
{
    REGISTER_TUNABLE("pluginfail", "Have a plugin fail at init time",
                     TUNABLE_INTEGER, &gbl_plugin_fail, INTERNAL | EXPERIMENTAL, NULL, NULL,
                     NULL, NULL);
    return 0;
}

static int fail_destroy()
{
    return 0;
}

static int fail_postlrl() {
    return gbl_plugin_fail;
}

static int fail_finalize()
{
    return 0;
}

comdb2_initializer_t fail_plugin = {.post_lrl = fail_postlrl, .post_recovery = fail_finalize};


#include "plugin.h"
