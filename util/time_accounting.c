/*
   Copyright 2019 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#include <pthread.h>
#include <sys/time.h>
#include "locks_wrap.h"
#include "comdb2_atomic.h"
#include "time_accounting.h"

#ifndef NDEBUG
const char *CHR_NAMES[] = {"ix_addk", "dat_add", "temp_table_saveop"};

uint64_t gbl_chron_times[CHR_MAX];

// add time accounting to appropriate slot
void accumulate_time(int el, int us)
{
    ATOMIC_ADD64(gbl_chron_times[el], us);
}

void reset_time_accounting(int el)
{
    XCHANGE64(gbl_chron_times[el], 0);
}

void print_time_accounting(int el)
{
    logmsg(LOGMSG_USER, "Timing information for %s: %"PRIu64"us\n", CHR_NAMES[el],
           gbl_chron_times[el]);
}

void print_all_time_accounting()
{
    extern int gbl_create_mode;
    if (gbl_create_mode) return;
    logmsg(LOGMSG_USER, "Timing information:\n");
    for (int i = 0; i < CHR_MAX; i++) {
        logmsg(LOGMSG_USER, "%s: %"PRIu64"us\n", CHR_NAMES[i], ATOMIC_LOAD64(gbl_chron_times[i]));
    }
}

#endif
